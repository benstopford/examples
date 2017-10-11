package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.avro.microservices.OrderValidationResult;
import io.confluent.examples.streams.avro.microservices.OrderValidationType;
import io.confluent.examples.streams.microservices.Schemas.Topics;
import io.confluent.examples.streams.microservices.util.TestUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static io.confluent.examples.streams.avro.microservices.OrderType.*;
import static io.confluent.examples.streams.avro.microservices.ProductType.JUMPERS;
import static io.confluent.examples.streams.avro.microservices.ProductType.UNDERPANTS;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class OrdersServiceTest extends TestUtils {
    private List<Order> orders;
    private List<OrderValidation> ruleResults;
    private OrdersService ordersService;


    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(Topics.ORDERS.name());
        CLUSTER.createTopic(Topics.ORDER_VALIDATIONS.name());
        Schemas.configureSerdesWithSchemaRegistryUrl(CLUSTER.schemaRegistryUrl());
    }

    @Test
    public void shouldAggregateRuleSuccesses() throws Exception {
        TestUtils.tailTopicToConsole(Topics.ORDER_VALIDATIONS, CLUSTER.bootstrapServers());
        TestUtils.tailTopicToConsole(Topics.ORDERS, CLUSTER.bootstrapServers());

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


        List<Order> finalOrders = TestUtils.read(Topics.ORDERS, 4, CLUSTER.bootstrapServers());
        assertThat(finalOrders.size()).isEqualTo(4);

        //And the first order should have been validated but the second should have failed
        assertThat(finalOrders).contains(
                new Order(0L, 0L, VALIDATED, UNDERPANTS, 3, 5.00d),
                new Order(1L, 0L, FAILED, JUMPERS, 1, 75.00d)
        );
    }


    @After
    public void tearDown() {
        ordersService.stop();
    }
}
