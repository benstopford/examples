package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderType;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.avro.microservices.OrderValidationResult;
import io.confluent.examples.streams.microservices.util.MicroserviceUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.FAIL;
import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.PASS;
import static io.confluent.examples.streams.avro.microservices.OrderValidationType.ORDER_DETAILS_CHECK;
import static io.confluent.examples.streams.microservices.Schemas.Topics;
import static java.util.Collections.singletonList;

public class OrderDetailsValidationService {
    public static final String CONSUMER_GROUP_ID = "OrderValidationService";
    private KafkaConsumer<Long, Order> consumer;
    private KafkaProducer<Long, OrderValidation> producer;
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    private boolean running = true;


    public void start(String bootstrapServers) {
        executorService.execute(() -> startService(bootstrapServers));
        System.out.println(getClass().getName() + " was started");
    }

    private void startService(String bootstrapServers) {
        startConsumer(bootstrapServers);
        startProducer(bootstrapServers);

        try {
            Map<TopicPartition, OffsetAndMetadata> consumedOffsets = new HashMap<>();
            consumer.subscribe(singletonList(Topics.ORDERS.name()));
            producer.initTransactions();

            while (running) {
                ConsumerRecords<Long, Order> records = consumer.poll(100);
                producer.beginTransaction();
                for (ConsumerRecord<Long, Order> record : records) {
                    Order order = record.value();
                    if (isValid(order))
                        producer.send(result(order, PASS));
                    else
                        producer.send(result(order, FAIL));

                    consumedOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()));
                }
                producer.sendOffsetsToTransaction(consumedOffsets, CONSUMER_GROUP_ID);
                producer.commitTransaction();
            }
        } finally {
            close();
        }
    }

    private ProducerRecord<Long, OrderValidation> result(Order order, OrderValidationResult passOrFail) {
        return new ProducerRecord<>(
                Topics.ORDER_VALIDATIONS.name(),
                order.getId(),
                new OrderValidation(order.getId(), ORDER_DETAILS_CHECK, passOrFail)
        );
    }

    private void startProducer(String bootstrapServers) {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfig.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "OrderDetailsServiceInstance1");
        producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");

        producer = new KafkaProducer(producerConfig,
                Topics.ORDER_VALIDATIONS.keySerde().serializer(),
                Topics.ORDER_VALIDATIONS.valueSerde().serializer());
    }

    private void startConsumer(String bootstrapServers) {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        consumer = new KafkaConsumer(consumerConfig,
                Topics.ORDERS.keySerde().deserializer(),
                Topics.ORDERS.valueSerde().deserializer());
    }

    private void close() {
        if (producer != null)
            producer.close();
        if (consumer != null)
            consumer.close();
    }

    public void stop() {
        running = false;
        try {
            executorService.awaitTermination(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            System.out.println("Failed to stop " + getClass().getName() + " in 1000ms");
        }
        System.out.println(getClass().getName() + " was stopped");
    }

    private boolean isValid(Order order) {
        if (order.getCustomerId() == null) return false;
        if (order.getQuantity() < 0) return false;
        if (order.getState() != OrderType.CREATED) return false;
        if (order.getPrice() < 0) return false;
        if (order.getProduct() == null) return false;
        return true;
    }

    public static void main(String[] args) throws Exception {
        OrderDetailsValidationService service = new OrderDetailsValidationService();
        service.startService(MicroserviceUtils.initSchemaRegistryAndGetBootstrapServers(args));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                service.close();
            } catch (Exception ignored) {
            }
        }));
    }
}