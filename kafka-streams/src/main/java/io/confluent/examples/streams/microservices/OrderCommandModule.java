package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
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

import static java.util.Collections.singletonList;

public class OrderCommandModule implements OrderCommand, Service {

    public final String CONSUMER_GROUP_ID = getClass().getSimpleName() + ":" + UUID.randomUUID();
    private final String TRANSACTIONAL_ID = getClass().getSimpleName();
    private KafkaConsumer<Long, Order> consumer;
    private KafkaProducer<Long, Order> producer;
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    private volatile boolean running = false;
    private Map<Long, PostCallback> outstandingRequests = new ConcurrentHashMap();

    interface PostCallback {
        void done();
    }

    private void startModule(String bootstrapServers) {
        startProducer(bootstrapServers);
        startConsumer(bootstrapServers);
        running = true;
        runConsumer();
    }

    @Override
    public boolean putOrderAndWait(Order order) {
        try {
            producer.send(new ProducerRecord<>(Schemas.Topics.ORDERS.name(), order.getId(), order)).get();
            Object monitor = new Object();
            outstandingRequests.put(order.getId(), () -> monitor.notify());//todo make this order.requestId()
            synchronized (monitor) {
                monitor.wait(5000);
                return true;
            }
        } catch (InterruptedException e) {
            System.out.println("putOrderAndWait timed out for order " + order);
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return false;
    }


    @Override
    public void start(String bootstrapServers) {
        executorService.execute(() -> startModule(bootstrapServers));
        while (running == false)
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

            while (running) {
                ConsumerRecords<Long, Order> records = consumer.poll(100);
                for (ConsumerRecord<Long, Order> record : records) {
                    Order order = record.value();
                    PostCallback postCallback = outstandingRequests.get(order.getId());
                    if (postCallback != null)
                        postCallback.done();
                }
            }
        } finally {
            consumer.close();
        }
    }

    private void startProducer(String bootstrapServers) {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        producerConfig.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, TRANSACTIONAL_ID);
        //TODO work out how to enable idempotence without transactions??
//        producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");

        producer = new KafkaProducer(producerConfig,
                Schemas.Topics.ORDER_VALIDATIONS.keySerde().serializer(),
                Schemas.Topics.ORDER_VALIDATIONS.valueSerde().serializer());

    }

    private void startConsumer(String bootstrapServers) {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
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
