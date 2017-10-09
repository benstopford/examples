package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.avro.microservices.OrderValidationResult;
import io.confluent.examples.streams.avro.microservices.OrderValidationType;
import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import io.confluent.examples.streams.microservices.util.TestUtils;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;
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

public class OrderDetailsValidationServiceTest extends TestUtils {

    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster(TestUtils.propsWith(
            //Set transactions to work with a single kafka broker.
            new KeyValue(KafkaConfig.TransactionsTopicReplicationFactorProp(), "1"),
            new KeyValue(KafkaConfig.TransactionsTopicMinISRProp(), "1"),
            new KeyValue(KafkaConfig.TransactionsTopicPartitionsProp(), "1")
    ));
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
        assertThat(TestUtils.readOrderValidations(2, CLUSTER.bootstrapServers())).isEqualTo(expected);
    }

    private void sendOrders(List<Order> orders) {
        KafkaProducer<Long, Order> ordersProducer = new KafkaProducer(producerConfig(CLUSTER), Topics.ORDERS.keySerde().serializer(), Topics.ORDERS.valueSerde().serializer());
        for (Order order : orders)
            ordersProducer.send(new ProducerRecord(Topics.ORDERS.name(), order.getId(), order));
        ordersProducer.close();
    }

    @After
    public void tearDown(){
        orderValService.stop();
    }
}