package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.avro.microservices.OrderValidationResult;
import io.confluent.examples.streams.avro.microservices.OrderValidationType;
import io.confluent.examples.streams.microservices.util.TestUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static io.confluent.examples.streams.avro.microservices.OrderType.CREATED;
import static io.confluent.examples.streams.avro.microservices.ProductType.JUMPERS;
import static io.confluent.examples.streams.avro.microservices.ProductType.UNDERPANTS;
import static io.confluent.examples.streams.microservices.Schemas.Topics;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class OrderDetailsValidationServiceTest extends TestUtils {

    private List<Order> orders;
    private List<OrderValidation> expected;
    private OrderDetailsValidationService orderValService;


    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(Topics.ORDERS.name());
        CLUSTER.createTopic(Topics.ORDER_VALIDATIONS.name());
        Schemas.configureSerdesWithSchemaRegistryUrl(CLUSTER.schemaRegistryUrl());
    }

    @Test
    public void shouldPassValidOrder() throws Exception {

        //Given
        orderValService = new OrderDetailsValidationService();

        orders = asList(
                new Order(0L, 0L, CREATED, UNDERPANTS, 3, 5.00d), //should pass
                new Order(1L, 0L, CREATED, JUMPERS, -1, 75.00d) //should fail
        );
        sendOrders(orders);

        //When
        orderValService.start(CLUSTER.bootstrapServers());


        //Then the final order for Jumpers should have been 'rejected' as it's out of stock
        expected = asList(
                new OrderValidation(0L, OrderValidationType.ORDER_DETAILS_CHECK, OrderValidationResult.PASS),
                new OrderValidation(1L, OrderValidationType.ORDER_DETAILS_CHECK, OrderValidationResult.FAIL)
        );
        assertThat(TestUtils.read(Topics.ORDER_VALIDATIONS, 2, CLUSTER.bootstrapServers())).isEqualTo(expected);
    }

    @After
    public void tearDown(){
        orderValService.stop();
    }
}