package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.*;

import static io.confluent.examples.streams.microservices.OrderCommand.OrderCommandResult.*;
import static java.util.Collections.singletonList;

public class OrderCommandSubService implements OrderCommand, Service {

    public final String UNIQUE_CONSUMER_GROUP_ID = getClass().getSimpleName() + ":" + UUID.randomUUID();
    private KafkaConsumer<Long, Order> consumer;
    private KafkaProducer<Long, Order> producer;
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    private volatile boolean running = false;
    //In a real implementation we would need to periodically purge old entries from this map.
    private Map<Long, PostCallback> outstandingRequests = new ConcurrentHashMap();

    interface PostCallback {
        void received(boolean succeeded);

        boolean succeeded();
    }

    private void startModule(String bootstrapServers) {
        startProducer(bootstrapServers);
        startConsumer(bootstrapServers);
        running = true;
        runConsumer();
    }

    @Override
    public OrderCommandResult putOrderAndWait(Order order) {
        //todo make this order.requestId()
        try {
            //Send the order synchronously
            producer.send(new ProducerRecord<>(Schemas.Topics.ORDERS.name(), order.getId(), order)).get();

            //Create a latch and pass it in the callback
            CountDownLatch latch = new CountDownLatch(1);

            //We use the orderId to identify the request. This assumes it is unique. In a real system we'd enforce a version or GUID
            PostCallback callback = callback(latch);
            outstandingRequests.put(order.getId(), callback);

            //Await the callback (called when the message is consumed)
            latch.await(10, TimeUnit.SECONDS);

            return latch.getCount() > 0 ? TIMED_OUT :
                    callback.succeeded() ? SUCCESS : FAILED_VALIDATION;

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return UNKNOWN_FAILURE;
    }

    private PostCallback callback(final CountDownLatch latch) {
        return new PostCallback() {
            boolean succeeded;

            @Override
            public synchronized void received(boolean succeeded) {
                this.succeeded = succeeded;
                latch.countDown();
            }

            @Override
            public synchronized boolean succeeded() {
                return succeeded;
            }
        };
    }

    @Override
    public void start(String bootstrapServers) {
        executorService.execute(() -> startModule(bootstrapServers));
        while (!running)
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        System.out.println("Started Service " + getClass().getSimpleName() + " producer is " + producer);
    }

    private void runConsumer() {
        try {
            consumer.subscribe(singletonList(Schemas.Topics.ORDERS.name()));

            //Check all incoming orders to see if they were
            while (running) {
                ConsumerRecords<Long, Order> records = consumer.poll(100);
                for (ConsumerRecord<Long, Order> record : records) {
                    Order order = record.value();
                    PostCallback callback = outstandingRequests.get(order.getId());
                    if (callback != null)
                        if (OrderType.VALIDATED.equals(order.getState()))
                            callback.received(true);
                        else if (OrderType.FAILED.equals(order.getState()))
                            callback.received(false);
                }
            }
        } finally {
            consumer.close();
        }
    }

    private void startProducer(String bootstrapServers) {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");

        producer = new KafkaProducer(producerConfig,
                Schemas.Topics.ORDER_VALIDATIONS.keySerde().serializer(),
                Schemas.Topics.ORDER_VALIDATIONS.valueSerde().serializer());

    }

    private void startConsumer(String bootstrapServers) {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, UNIQUE_CONSUMER_GROUP_ID);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        consumer = new KafkaConsumer(consumerConfig,
                Schemas.Topics.ORDERS.keySerde().deserializer(),
                Schemas.Topics.ORDERS.valueSerde().deserializer());
    }

    @Override
    public void stop() {
        running = false;
        try {
            executorService.awaitTermination(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            System.out.println("Failed to stop consumer " + getClass().getName() + " in 1000ms");
        }
        System.out.println(getClass().getName() + " was stopped");
        if (producer != null)
            producer.close();
    }

}
