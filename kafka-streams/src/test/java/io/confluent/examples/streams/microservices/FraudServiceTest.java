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
import static io.confluent.examples.streams.avro.microservices.ProductType.JUMPERS;
import static io.confluent.examples.streams.avro.microservices.ProductType.UNDERPANTS;
import static io.confluent.examples.streams.microservices.Schemas.Topics;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class FraudServiceTest extends TestUtils {

    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();
    private List<Order> orders;
    private List<OrderValidation> expected;
    private FraudService fraudService;


    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(Topics.ORDERS.name());
        CLUSTER.createTopic(Topics.ORDER_VALIDATIONS.name());
        System.out.println("running with schema registry: "+CLUSTER.schemaRegistryUrl());
        Schemas.configureSerdesWithSchemaRegistryUrl(CLUSTER.schemaRegistryUrl());
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
                new OrderValidation(0L, OrderValidationType.FRAUD_CHECK, OrderValidationResult.PASS),
                new OrderValidation(1L, OrderValidationType.FRAUD_CHECK, OrderValidationResult.PASS),
                new OrderValidation(2L, OrderValidationType.FRAUD_CHECK, OrderValidationResult.FAIL),
                new OrderValidation(3L, OrderValidationType.FRAUD_CHECK, OrderValidationResult.FAIL),
                new OrderValidation(4L, OrderValidationType.FRAUD_CHECK, OrderValidationResult.FAIL),
                new OrderValidation(5L, OrderValidationType.FRAUD_CHECK, OrderValidationResult.FAIL),
                new OrderValidation(6L, OrderValidationType.FRAUD_CHECK, OrderValidationResult.FAIL),
                new OrderValidation(7L, OrderValidationType.FRAUD_CHECK, OrderValidationResult.PASS)
        );
        assertThat(TestUtils.readOrderValidations(8, CLUSTER.bootstrapServers())).isEqualTo(expected);
    }

    private void sendOrders(List<Order> orders) {
        KafkaProducer<Long, Order> ordersProducer = new KafkaProducer(producerConfig(CLUSTER), Topics.ORDERS.keySerde().serializer(), Topics.ORDERS.valueSerde().serializer());
        for (Order order : orders)
            ordersProducer.send(new ProducerRecord(Topics.ORDERS.name(), order.getId(), order));
        ordersProducer.close();
    }

    @After
    public void tearDown(){
        fraudService.stop();
    }
}