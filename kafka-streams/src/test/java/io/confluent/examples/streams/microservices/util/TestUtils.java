package io.confluent.examples.streams.microservices.util;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.avro.microservices.ProductType;
import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import io.confluent.examples.streams.microservices.Schemas;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.junit.ClassRule;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

public class TestUtils {

    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster(TestUtils.propsWith(
            //Set transactions to work with a single kafka broker.
            new KeyValue(KafkaConfig.TransactionsTopicReplicationFactorProp(), "1"),
            new KeyValue(KafkaConfig.TransactionsTopicMinISRProp(), "1"),
            new KeyValue(KafkaConfig.TransactionsTopicPartitionsProp(), "1")
    ));

    protected static Properties producerConfig(EmbeddedSingleNodeKafkaCluster cluster) {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        return producerConfig;
    }

    //TODO remove inventory specific method, do we really need this?
    protected static Properties inventoryConsumerProperties(EmbeddedSingleNodeKafkaCluster cluster) {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "inventory-test-reader");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        return consumerConfig;
    }


    public static <K, V> List<V> read(Schemas.Topic<K, V> topic, int numberToRead, String bootstrapServers) throws InterruptedException {
        return readKeyValues(topic, numberToRead, bootstrapServers).stream().map(kv -> kv.value).collect(Collectors.toList());
    }

    public static <K, V> List<K> readKeys(Schemas.Topic<K, V> topic, int numberToRead, String bootstrapServers) throws InterruptedException {
        return readKeyValues(topic, numberToRead, bootstrapServers).stream().map(kv -> kv.key).collect(Collectors.toList());
    }

    public static <K, V> List<KeyValue<K, V>> readKeyValues(Schemas.Topic<K, V> topic, int numberToRead, String bootstrapServers) throws InterruptedException {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "Test-Reader-" + String.valueOf(System.currentTimeMillis()));
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<K, V> consumer = new KafkaConsumer(consumerConfig, topic.keySerde().deserializer(), topic.valueSerde().deserializer());
        consumer.subscribe(singletonList(topic.name()));

        List<KeyValue<K, V>> actualValues = new ArrayList<>();
        org.apache.kafka.test.TestUtils.waitForCondition(() -> {
            ConsumerRecords<K, V> records = consumer.poll(100);
            for (ConsumerRecord<K, V> record : records) {
                actualValues.add(KeyValue.pair(record.key(), record.value()));
            }
            return actualValues.size() == numberToRead;
        }, 20000, "Timed out reading orders.");
        consumer.close();
        return actualValues;
    }

    public static <K, V> List<KeyValue<K, V>> readKeyValues2(int numberToRead, KafkaConsumer<K, V> consumer) throws InterruptedException {

        List<KeyValue<K, V>> actualValues = new ArrayList<>();
        org.apache.kafka.test.TestUtils.waitForCondition(() -> {
            ConsumerRecords<K, V> records = consumer.poll(100);
            for (ConsumerRecord<K, V> record : records) {
                actualValues.add(KeyValue.pair(record.key(), record.value()));
            }
            return actualValues.size() == numberToRead;
        }, 20000, "Timed out reading orders.");
        return actualValues;
    }

    public static <K, V> KafkaConsumer<K, V> createConsumer(Schemas.Topic<K, V> topic, String bootstrapServers) {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "Test-Reader-" + String.valueOf(System.currentTimeMillis()));
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<K, V> consumer = new KafkaConsumer(consumerConfig, topic.keySerde().deserializer(), topic.valueSerde().deserializer());
        consumer.subscribe(singletonList(topic.name()));
        return consumer;
    }

    static List<TopicTailer> tailers = new ArrayList();

    public static <K, V> void tailTopicToConsole(Schemas.Topic<K, V> topic, String bootstrapServers) {
        TopicTailer task = new TopicTailer(topic, bootstrapServers);
        tailers.add(task);
        Executors.newSingleThreadExecutor().execute(task);
    }

    public static void stopTailers() {
        tailers.stream().forEach((t) -> t.stop());
    }

    static class TopicTailer<K, V> implements Runnable {
        private boolean running = true;
        private boolean closed = false;
        private Schemas.Topic<K, V> topic;
        private String bootstrapServers;

        public TopicTailer(Schemas.Topic<K, V> topic, String bootstrapServers) {
            this.topic = topic;
            this.bootstrapServers = bootstrapServers;
        }

        @Override
        public void run() {
            Properties consumerConfig = new Properties();
            consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "Test-Reader-" + String.valueOf(System.currentTimeMillis()));
            consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            KafkaConsumer<K, V> consumer = new KafkaConsumer(consumerConfig, topic.keySerde().deserializer(), topic.valueSerde().deserializer());
            consumer.subscribe(singletonList(topic.name()));

            while (running) {
                ConsumerRecords<K, V> records = consumer.poll(100);
                for (ConsumerRecord<K, V> record : records) {
                    System.out.println(topic.name() + "->" + record.value());
                }
            }
            consumer.close();
            closed = true;
        }

        void stop() {
            running = false;
            while (!closed) try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
            }
        }

    }

    public static Properties propsWith(KeyValue... props) {
        Properties properties = new Properties();
        for (KeyValue kv : props) {
            properties.put(kv.key, kv.value);
        }
        return properties;
    }

    //TODO refactor these send messages into one
    public void sendOrders(List<Order> orders) {
        KafkaProducer<Long, Order> ordersProducer = new KafkaProducer(producerConfig(CLUSTER), Schemas.Topics.ORDERS.keySerde().serializer(), Schemas.Topics.ORDERS.valueSerde().serializer());
        for (Order order : orders)
            ordersProducer.send(new ProducerRecord(Schemas.Topics.ORDERS.name(), order.getId(), order));
        ordersProducer.close();
    }

    public void sendOrderValuations(List<OrderValidation> orderValidations) {
        KafkaProducer<Long, Order> ordersProducer = new KafkaProducer(producerConfig(CLUSTER), Schemas.Topics.ORDER_VALIDATIONS.keySerde().serializer(), Schemas.Topics.ORDER_VALIDATIONS.valueSerde().serializer());
        for (OrderValidation ov : orderValidations)
            ordersProducer.send(new ProducerRecord(Schemas.Topics.ORDER_VALIDATIONS.name(), ov.getOrderId(), ov));
        ordersProducer.close();
    }

    public void sendInventory(List<KeyValue<ProductType, Integer>> inventory, Schemas.Topic<ProductType, Integer> topic) {
        KafkaProducer<ProductType, Integer> stockProducer = new KafkaProducer<>(producerConfig(CLUSTER), topic.keySerde().serializer(), Schemas.Topics.WAREHOUSE_INVENTORY.valueSerde().serializer());
        for (KeyValue kv : inventory)
            stockProducer.send(new ProducerRecord(Schemas.Topics.WAREHOUSE_INVENTORY.name(), kv.key, kv.value));
        stockProducer.close();
    }

}
