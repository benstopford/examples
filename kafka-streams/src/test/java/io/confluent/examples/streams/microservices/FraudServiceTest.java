package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.microservices.util.TestUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static io.confluent.examples.streams.avro.microservices.OrderType.CREATED;
import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.FAIL;
import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.PASS;
import static io.confluent.examples.streams.avro.microservices.OrderValidationType.FRAUD_CHECK;
import static io.confluent.examples.streams.avro.microservices.ProductType.JUMPERS;
import static io.confluent.examples.streams.avro.microservices.ProductType.UNDERPANTS;
import static io.confluent.examples.streams.microservices.Schemas.Topics;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class FraudServiceTest extends TestUtils {
    private List<Order> orders;
    private List<OrderValidation> expected;
    private FraudService fraudService;

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        if (!CLUSTER.isRunning())
            CLUSTER.start();

        CLUSTER.createTopic(Topics.ORDERS.name());
        CLUSTER.createTopic(Topics.ORDER_VALIDATIONS.name());
        System.out.println("running with schema registry: " + CLUSTER.schemaRegistryUrl());
        Schemas.configureSerdesWithSchemaRegistryUrl(CLUSTER.schemaRegistryUrl());
    }

    @After
    public void tearDown() throws Exception {
        fraudService.stop();
        CLUSTER.stop();
    }

    @Test
    public void shouldValidateWhetherOrderAmountExceedsFraudLimitOverWindow() throws Exception {

        //TODO - add event time to this.

        //Given
        fraudService = new FraudService();

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
        fraudService.start(CLUSTER.bootstrapServers());

        //Then the final order for Jumpers should have been 'rejected' as it's out of stock
        expected = asList(
                new OrderValidation(0L, FRAUD_CHECK, PASS),
                new OrderValidation(1L, FRAUD_CHECK, PASS),
                new OrderValidation(2L, FRAUD_CHECK, PASS),
                new OrderValidation(3L, FRAUD_CHECK, PASS),
                new OrderValidation(4L, FRAUD_CHECK, FAIL),
                new OrderValidation(5L, FRAUD_CHECK, PASS),
                new OrderValidation(6L, FRAUD_CHECK, FAIL),
                new OrderValidation(7L, FRAUD_CHECK, PASS)
        );
        assertThat(TestUtils.read(Topics.ORDER_VALIDATIONS, 8, CLUSTER.bootstrapServers())).isEqualTo(expected);
    }
}