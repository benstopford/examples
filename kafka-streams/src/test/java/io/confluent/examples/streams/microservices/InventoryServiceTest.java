package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.IntegrationTestUtils;
import io.confluent.examples.streams.avro.microservices.*;
import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import io.confluent.examples.streams.microservices.util.TestUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static io.confluent.examples.streams.avro.microservices.OrderType.*;
import static io.confluent.examples.streams.avro.microservices.ProductType.*;
import static io.confluent.examples.streams.microservices.Schemas.*;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class InventoryServiceTest extends TestUtils {

    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();
    private List<KeyValue<ProductType, Integer>> inventory;
    private List<Order> orders;
    private List<OrderValidation> expected;


    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(Topics.ORDERS.name());
        CLUSTER.createTopic(Topics.ORDER_VALIDATIONS.name());
        System.out.println("running with schema registry: "+CLUSTER.schemaRegistryUrl());
        Schemas.configureSerdesWithSchemaRegistryUrl(CLUSTER.schemaRegistryUrl());
    }

    @Test
    public void shouldProcessOrdersWithSufficientStockAndRejectOrdersWithInsufficientStock() throws Exception {

        //Given
        InventoryService orderService = new InventoryService();

        inventory = asList(
                new KeyValue<>(UNDERPANTS, 75),
                new KeyValue<>(JUMPERS, 1)
        );
        sendInventory(inventory, Topics.WAREHOUSE_INVENTORY);

        orders = asList(
                new Order(0L, 1L, CREATED, UNDERPANTS, 3, 10.00d),
                new Order(1L, 2L, CREATED, JUMPERS, 1, 75.00d),
                new Order(2L, 2L, CREATED, JUMPERS, 1, 75.00d)
        );
        sendOrders(orders);


        //When
        orderService.startService(CLUSTER.bootstrapServers());


        //Then the final order for Jumpers should have been 'rejected' as it's out of stock
        expected = asList(
                new OrderValidation(0L, OrderValidationType.INVENTORY_CHECK, OrderValidationResult.PASS),
                new OrderValidation(1L, OrderValidationType.INVENTORY_CHECK, OrderValidationResult.PASS),
                new OrderValidation(2L, OrderValidationType.INVENTORY_CHECK, OrderValidationResult.FAIL)
        );
        assertThat(TestUtils.readOrderValidations(expected.size(), CLUSTER.bootstrapServers())).isEqualTo(expected);

        //And the reservations should have been incremented twice, once for each validated order
        List<KeyValue<ProductType, Long>> inventoryChangelog = readInventoryStateStore(2);
        assertThat(inventoryChangelog).isEqualTo(asList(
                new KeyValue(UNDERPANTS.toString(), 3L),
                new KeyValue(JUMPERS.toString(), 1L)
        ));
    }

    private List<KeyValue<ProductType, Long>> readInventoryStateStore(int numberOfRecordsToWaitFor) throws InterruptedException {
        return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(inventoryConsumerProperties(CLUSTER),
                ProcessorStateManager.storeChangelogTopic(InventoryService.INVENTORY_SERVICE_APP_ID, InventoryService.RESERVED_STOCK_STORE_NAME), numberOfRecordsToWaitFor);
    }


    private void sendInventory(List<KeyValue<ProductType, Integer>> inventory, Topic<ProductType, Integer> topic) {
        KafkaProducer<ProductType, Integer> stockProducer = new KafkaProducer<>(producerConfig(CLUSTER), topic.keySerde().serializer(), Topics.WAREHOUSE_INVENTORY.valueSerde().serializer());
        for (KeyValue kv : inventory)
            stockProducer.send(new ProducerRecord(Topics.WAREHOUSE_INVENTORY.name(), kv.key, kv.value));
    }

    private void sendOrders(List<Order> orders) {
        KafkaProducer<Long, Order> ordersProducer = new KafkaProducer(producerConfig(CLUSTER), Topics.ORDERS.keySerde().serializer(), Topics.ORDERS.valueSerde().serializer());
        for (Order order : orders)
            ordersProducer.send(new ProducerRecord(Topics.ORDERS.name(), order.getId(), order));
    }

}
