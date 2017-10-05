package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.IntegrationTestUtils;
import io.confluent.examples.streams.avro.microservices.*;
import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import io.confluent.examples.streams.microservices.util.OrdersServiceTestUtils;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
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

public class InventoryServiceTest extends OrdersServiceTestUtils {

    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();
    private List<KeyValue<ProductType, Integer>> inventory;
    private List<Order> orders;
    private List<OrderValidation> expected;
    private static SpecificAvroSerde<Order> ordersSerde;


    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(Schemas.Topics.ORDERS);
        CLUSTER.createTopic(Schemas.Topics.ORDER_VALIDATIONS);
        ordersSerde = new SpecificAvroSerde<>();
        ordersSerde.configure(singletonMap(SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl()), false);
    }

    @Test
    public void shouldProcessOrdersWithSufficientStockAndRejectOrdersWithInsufficientStock() throws Exception {

        //Given
        InventoryService orderService = new InventoryService();

        inventory = asList(
                new KeyValue<>(UNDERPANTS, 75),
                new KeyValue<>(JUMPERS, 1)
        );
        sendInventory(inventory);

        orders = asList(
                new Order(0L, CREATED, UNDERPANTS, 3),
                new Order(1L, CREATED, JUMPERS, 1),
                new Order(2L, CREATED, JUMPERS, 1)
        );
        sendOrders(orders);


        //When
        orderService.startService(CLUSTER.bootstrapServers(), CLUSTER.schemaRegistryUrl());


        //Then the final order for Jumpers should have been 'rejected' as it's out of stock
        expected = asList(
                new OrderValidation(0L, OrderValidationType.INVENTORY_CHECK, OrderValidationResult.PASS),
                new OrderValidation(1L, OrderValidationType.INVENTORY_CHECK, OrderValidationResult.PASS),
                new OrderValidation(2L, OrderValidationType.INVENTORY_CHECK, OrderValidationResult.FAIL)
        );
        assertThat(readOrderValidations(expected.size())).isEqualTo(expected);

        //And the reservations should have been incremented twice, once for each validated order
        List<KeyValue<ProductType, Long>> inventoryChangelog = readInventoryStateStore(2);
        assertThat(inventoryChangelog).isEqualTo(asList(
                new KeyValue(UNDERPANTS.toString(), 3L),
                new KeyValue(JUMPERS.toString(), 1L)
        ));
    }

    private List<KeyValue<ProductType, Long>> readInventoryStateStore(int numberOfRecordsToWaitFor) throws InterruptedException {
        return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(inventoryConsumerProperties(),
                ProcessorStateManager.storeChangelogTopic(InventoryService.INVENTORY_SERVICE_APP_ID, InventoryService.RESERVED_STOCK_STORE_NAME), numberOfRecordsToWaitFor);
    }

    private List<Order> readOrderValidations(int numberToRead) throws InterruptedException {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "orders-test-reader");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KafkaConsumer<Long, Order> consumer = new KafkaConsumer(consumerConfig, Serdes.Long().deserializer(), ordersSerde.deserializer());
        consumer.subscribe(singletonList(Schemas.Topics.ORDER_VALIDATIONS));

        List<Order> actualValues = new ArrayList<>();
        TestUtils.waitForCondition(() -> {
            ConsumerRecords<Long, Order> records = consumer.poll(100);
            for (ConsumerRecord<Long, Order> record : records) {
                actualValues.add(record.value());
            }
            return actualValues.size() == numberToRead;
        }, 5000, "Timed out reading orders.");
        consumer.close();
        return actualValues;
    }

    private void sendInventory(List<KeyValue<ProductType, Integer>> inventory) {
        KafkaProducer<ProductType, Integer> stockProducer = new KafkaProducer<>(producerConfig(), InventoryService.PRODUCT_TYPE_SERDE.serializer(), Serdes.Integer().serializer());
        for (KeyValue kv : inventory)
            stockProducer.send(new ProducerRecord(Schemas.Topics.WAREHOUSE_INVENTORY, kv.key, kv.value));
    }

    private void sendOrders(List<Order> orders) {
        KafkaProducer<Long, Order> ordersProducer = new KafkaProducer(producerConfig(), Serdes.Long().serializer(), ordersSerde.serializer());
        for (Order order : orders)
            ordersProducer.send(new ProducerRecord(Schemas.Topics.ORDERS, order.getId(), order));
    }

}
