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

import static io.confluent.examples.streams.avro.microservices.OrderType.CREATED;
import static io.confluent.examples.streams.avro.microservices.ProductType.JUMPERS;
import static io.confluent.examples.streams.avro.microservices.ProductType.UNDERPANTS;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;

public class FraudServiceTest extends OrdersServiceTestUtils {

    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();
    private List<Order> orders;
    private List<OrderValidations> expected;
    private static SpecificAvroSerde<Order> ordersSerde;


    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(Schemas.Topics.ORDERS);
        CLUSTER.createTopic(Schemas.Topics.ORDER_VALIDATIONS);
        ordersSerde = new SpecificAvroSerde<>();
        ordersSerde.configure(singletonMap(SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl()), false);
    }

    @Test
    public void shouldValidateWhetherOrderAmountExceedsFraudLimitOverWindow() throws Exception {

        //TODO - add event time to this.

        //Given
        FraudService orderService = new FraudService();

        orders = asList(
                new Order(0L, 0L, CREATED, UNDERPANTS, 3, 5.00d),
                new Order(1L, 0L, CREATED, JUMPERS, 1, 75.00d), //customer 0 => pass
                new Order(2L, 1L, CREATED, JUMPERS, 1, 75.00d),
                new Order(3L, 1L, CREATED, JUMPERS, 1, 75.00d),
                new Order(4L, 1L, CREATED, JUMPERS, 50, 75.00d), //customer 1 => fail
                new Order(5L, 2L, CREATED, JUMPERS, 1, 75.00d),
                new Order(6L, 2L, CREATED, UNDERPANTS, 2000, 5.00d), //customer 2 => fail
                new Order(7L, 3L, CREATED, UNDERPANTS, 1, 5.00d)  //customer 3 => pass
        );
        sendOrders(orders);


        //When
        orderService.startService(CLUSTER.bootstrapServers(), CLUSTER.schemaRegistryUrl());

        //Then the final order for Jumpers should have been 'rejected' as it's out of stock
        expected = asList(
                new OrderValidations(0L, OrderValidationType.FRAUD_CHECK, OrderValidationResult.PASS),
                new OrderValidations(1L, OrderValidationType.FRAUD_CHECK, OrderValidationResult.PASS),
                new OrderValidations(2L, OrderValidationType.FRAUD_CHECK, OrderValidationResult.FAIL),
                new OrderValidations(3L, OrderValidationType.FRAUD_CHECK, OrderValidationResult.FAIL),
                new OrderValidations(4L, OrderValidationType.FRAUD_CHECK, OrderValidationResult.FAIL),
                new OrderValidations(5L, OrderValidationType.FRAUD_CHECK, OrderValidationResult.FAIL),
                new OrderValidations(6L, OrderValidationType.FRAUD_CHECK, OrderValidationResult.FAIL),
                new OrderValidations(7L, OrderValidationType.FRAUD_CHECK, OrderValidationResult.PASS)
        );
        assertThat(readOrderValidations(8)).isEqualTo(expected);
    }

    private List<KeyValue<ProductType, Long>> readInventoryStateStore(int numberOfRecordsToWaitFor) throws InterruptedException {
        return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(inventoryConsumerProperties(InventoryServiceTest.CLUSTER),
                ProcessorStateManager.storeChangelogTopic(InventoryService.INVENTORY_SERVICE_APP_ID, InventoryService.RESERVED_STOCK_STORE_NAME), numberOfRecordsToWaitFor);
    }

    private List<OrderValidations> readOrderValidations(int numberToRead) throws InterruptedException {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "orders-test-reader");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<Long, OrderValidations> consumer = new KafkaConsumer(consumerConfig, Serdes.Long().deserializer(), Schemas.SerdeBuilders
                .ORDER_VALIDATIONS.serde(CLUSTER.schemaRegistryUrl()).deserializer());
        consumer.subscribe(singletonList(Schemas.Topics.ORDER_VALIDATIONS));

        List<OrderValidations> actualValues = new ArrayList<>();
        TestUtils.waitForCondition(() -> {
            ConsumerRecords<Long, OrderValidations> records = consumer.poll(100);
            for (ConsumerRecord<Long, OrderValidations> record : records) {
                actualValues.add(record.value());
            }
            return actualValues.size() == numberToRead;
        }, 30000, "Timed out reading orders.");
        consumer.close();
        return actualValues;
    }

    private void sendOrders(List<Order> orders) {
        KafkaProducer<Long, Order> ordersProducer = new KafkaProducer(producerConfig(CLUSTER), Serdes.Long().serializer(), ordersSerde.serializer());
        for (Order order : orders)
            ordersProducer.send(new ProducerRecord(Schemas.Topics.ORDERS, order.getId(), order));
    }

}