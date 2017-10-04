package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.IntegrationTestUtils;
import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.test.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;

public class OrdersServiceTest {

    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();
    private static final String stockCountStoreName = "StockCountStore";
    private static String ordersTopic = OrdersService.ORDERS;
    private static String stockTopic = OrdersService.STOCK;
    private static String outputTopic = OrdersService.OUTPUT;
    private List<KeyValue<String, Integer>> inventory;
    private List<Order> orders;
    private List<Order> expected;
    private static SpecificAvroSerde<Order> ordersSerde;


    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(ordersTopic);
        CLUSTER.createTopic(outputTopic);
        ordersSerde = new SpecificAvroSerde<>();
        ordersSerde.configure(singletonMap(SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl()), false);
    }

    @Test
    public void shouldProcessOrdersWithSufficientStockAndFailOrdersWithInsufficientStock() throws Exception {

        //Given
        OrdersService orderService = new OrdersService();

        inventory = asList(
                new KeyValue<>("Underpants", 75),
                new KeyValue<>("Jumpers", 1)
        );
        sendInventory();

        orders = asList(
                new Order(0L, "Created", "Underpants", 3),
                new Order(1L, "Created", "Jumpers", 1),
                new Order(2L, "Created", "Jumpers", 1) //Should be rejected
        );
        sendOrders();


        //When
        orderService.startService(CLUSTER.bootstrapServers(), CLUSTER.schemaRegistryUrl());


        //Then the final order for Jumpers should have been 'rejected' as it's out of stock
        expected = asList(
                new Order(0L, "Validated", "Underpants", 3),
                new Order(1L, "Validated", "Jumpers", 1),
                new Order(2L, "Rejected", "Jumpers", 1)
        );
        assertThat(readOrders()).isEqualTo(expected);

        //And the inventory changelog should have recorded a reduction in stock values
        List<KeyValue<String, Long>> inventoryChangelog = readInventoryStateStore();
        assertThat(inventoryChangelog.get(2)).isEqualTo(new KeyValue("Underpants", 72L));
        assertThat(inventoryChangelog.get(3)).isEqualTo(new KeyValue("Jumpers", 0L));
    }

    private List<KeyValue<String, Long>> readInventoryStateStore() throws InterruptedException {
        return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(inventoryConsumerProperties(),
                ProcessorStateManager.storeChangelogTopic(OrdersService.ORDERS_SERVICE_APP_ID, stockCountStoreName), 4);
    }

    private List<Order> readOrders() throws InterruptedException {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "orders-test-reader");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KafkaConsumer<Long, Order> consumer = new KafkaConsumer(consumerConfig, Serdes.Long().deserializer(), ordersSerde.deserializer());
        consumer.subscribe(singletonList(outputTopic));

        List<Order> actualValues = new ArrayList<>();
        TestUtils.waitForCondition(() -> {
            ConsumerRecords<Long, Order> records = consumer.poll(100);
            for (ConsumerRecord<Long, Order> record : records) {
                actualValues.add(record.value());
            }
            return actualValues.size() == expected.size();
        }, 5000, "Timed out reading orders.");
        consumer.close();
        return actualValues;
    }

    private void sendInventory() {
        KafkaProducer<String, Integer> stockProducer = new KafkaProducer<>(producerConfig(), Serdes.String().serializer(), Serdes.Integer().serializer());
        for (KeyValue kv : inventory)
            stockProducer.send(new ProducerRecord(stockTopic, kv.key, kv.value));
    }

    private void sendOrders() {
        KafkaProducer<Long, Order> ordersProducer = new KafkaProducer(producerConfig(), Serdes.Long().serializer(), ordersSerde.serializer());
        for (Order order : orders)
            ordersProducer.send(new ProducerRecord(ordersTopic, order.getId(), order));
    }

    private Properties producerConfig() {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        return producerConfig;
    }

    private Properties inventoryConsumerProperties() {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "inventory-test-reader");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        return consumerConfig;
    }
}
