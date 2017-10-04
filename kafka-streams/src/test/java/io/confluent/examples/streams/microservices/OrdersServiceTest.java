package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.IntegrationTestUtils;
import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderType;
import io.confluent.examples.streams.avro.microservices.ProductType;
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

import static io.confluent.examples.streams.avro.microservices.OrderType.*;
import static io.confluent.examples.streams.avro.microservices.ProductType.*;
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
    private List<KeyValue<ProductType, Integer>> inventory;
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
    public void shouldProcessOrdersWithSufficientStockAndRejectOrdersWithInsufficientStock() throws Exception {

        //Given
        OrdersService orderService = new OrdersService();

        inventory = asList(
                new KeyValue<>(UNDERPANTS, 75),
                new KeyValue<>(JUMPERS, 1)
        );
        sendInventory();

        orders = asList(
                new Order(0L, CREATED, UNDERPANTS, 3),
                new Order(1L, CREATED, JUMPERS, 1),
                new Order(2L, CREATED, JUMPERS, 1)
        );
        sendOrders();


        //When
        orderService.startService(CLUSTER.bootstrapServers(), CLUSTER.schemaRegistryUrl());


        //Then the final order for Jumpers should have been 'rejected' as it's out of stock
        expected = asList(
                new Order(0L, CREATED, UNDERPANTS, 3),
                new Order(1L, CREATED, JUMPERS, 1),
                new Order(2L, CREATED, JUMPERS, 1),
                new Order(0L, VALIDATED, UNDERPANTS, 3),
                new Order(1L, VALIDATED, JUMPERS, 1),
                new Order(2L, INSUFFICIENT_STOCK, JUMPERS, 1)
        );
        assertThat(readOrders()).isEqualTo(expected);

        //And the reservations should have been incremented twice, once for each validated order
        List<KeyValue<ProductType, Long>> inventoryChangelog = readInventoryStateStore(2);
        assertThat(inventoryChangelog).isEqualTo(asList(
                new KeyValue(UNDERPANTS.toString(), 3L),
                new KeyValue(JUMPERS.toString(), 1L)
        ));
    }

    private List<KeyValue<ProductType, Long>> readInventoryStateStore(int numberOfRecordsToWaitFor) throws InterruptedException {
        return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(inventoryConsumerProperties(),
                ProcessorStateManager.storeChangelogTopic(OrdersService.ORDERS_SERVICE_APP_ID, stockCountStoreName), numberOfRecordsToWaitFor);
    }

    private List<Order> readOrders() throws InterruptedException {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "orders-test-reader");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KafkaConsumer<Long, Order> consumer = new KafkaConsumer(consumerConfig, Serdes.Long().deserializer(), ordersSerde.deserializer());
        consumer.subscribe(singletonList(ordersTopic));

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
        KafkaProducer<ProductType, Integer> stockProducer = new KafkaProducer<>(producerConfig(), OrdersService.productTypeSerde.serializer(), Serdes.Integer().serializer());
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
