package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.avro.microservices.OrderValidationResult;
import io.confluent.examples.streams.avro.microservices.OrderValidationType;
import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import io.confluent.examples.streams.microservices.util.TestUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static io.confluent.examples.streams.avro.microservices.OrderType.CREATED;
import static io.confluent.examples.streams.avro.microservices.OrderType.FAILED;
import static io.confluent.examples.streams.avro.microservices.ProductType.JUMPERS;
import static io.confluent.examples.streams.avro.microservices.ProductType.UNDERPANTS;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class OrdersServiceTest extends TestUtils {
    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();
    private List<Order> orders;
    private List<OrderValidation> ruleResults;
    private OrdersService ordersService;


    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(Schemas.Topics.ORDERS.name());
        CLUSTER.createTopic(Schemas.Topics.ORDER_VALIDATIONS.name());
        Schemas.configureSerdesWithSchemaRegistryUrl(CLUSTER.schemaRegistryUrl());
    }

    @Test
    public void shouldAggregateRuleSuccesses() throws Exception {

        //Given
        ordersService = new OrdersService();

        orders = asList(
                new Order(0L, 0L, CREATED, UNDERPANTS, 3, 5.00d),
                new Order(1L, 0L, CREATED, JUMPERS, 1, 75.00d)
        );
        sendOrders(orders);

        ruleResults = asList(
                new OrderValidation(0L, OrderValidationType.FRAUD_CHECK, OrderValidationResult.PASS),
                new OrderValidation(0L, OrderValidationType.ORDER_DETAILS_CHECK, OrderValidationResult.PASS),
                new OrderValidation(0L, OrderValidationType.INVENTORY_CHECK, OrderValidationResult.PASS),
                new OrderValidation(1L, OrderValidationType.FRAUD_CHECK, OrderValidationResult.PASS),
                new OrderValidation(1L, OrderValidationType.ORDER_DETAILS_CHECK, OrderValidationResult.FAIL),
                new OrderValidation(1L, OrderValidationType.INVENTORY_CHECK, OrderValidationResult.PASS)
        );
        sendOrderValuations(ruleResults);

        //When
        ordersService.start(CLUSTER.bootstrapServers());

        //Then there should be 4 order messages in total (2 original, 2 new)
        List<Order> finalOrders = TestUtils.readOrders(4, CLUSTER.bootstrapServers());
        assertThat(finalOrders.size()).isEqualTo(4);

        //And the first order should have been validated but the second should have failed
        assertThat(finalOrders).contains(
                new Order(0L, 0L, CREATED, UNDERPANTS, 3, 5.00d),
                new Order(1L, 0L, FAILED, JUMPERS, 1, 75.00d)
        );
    }

    //TODO generify send methods
    private void sendOrders(List<Order> orders) {
        KafkaProducer<Long, Order> ordersProducer = new KafkaProducer(producerConfig(CLUSTER), Schemas.Topics.ORDERS.keySerde().serializer(), Schemas.Topics.ORDERS.valueSerde().serializer());
        for (Order order : orders)
            ordersProducer.send(new ProducerRecord(Schemas.Topics.ORDERS.name(), order.getId(), order));
        ordersProducer.close();
    }

    private void sendOrderValuations(List<OrderValidation> orderValidations) {
        KafkaProducer<Long, Order> ordersProducer = new KafkaProducer(producerConfig(CLUSTER), Schemas.Topics.ORDER_VALIDATIONS.keySerde().serializer(), Schemas.Topics.ORDER_VALIDATIONS.valueSerde().serializer());
        for (OrderValidation ov : orderValidations)
            ordersProducer.send(new ProducerRecord(Schemas.Topics.ORDER_VALIDATIONS.name(), ov.getOrderId(), ov));
        ordersProducer.close();
    }

    @After
    public void tearDown() {
        ordersService.stop();
    }
}
