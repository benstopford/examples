package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.microservices.util.MicroserviceTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static io.confluent.examples.streams.avro.microservices.OrderType.CREATED;
import static io.confluent.examples.streams.avro.microservices.ProductType.UNDERPANTS;
import static org.assertj.core.api.Assertions.assertThat;

public class OrderQueryModuleTest extends MicroserviceTestUtils {
    OrderQueryModule service = new OrderQueryModule();

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(Schemas.Topics.ORDERS.name());
        System.out.println("running with schema registry: " + CLUSTER.schemaRegistryUrl());
        Schemas.configureSerdesWithSchemaRegistryUrl(CLUSTER.schemaRegistryUrl());
    }

    @After
    public void tearDown() {
        service.stop();
    }

    @Test
    public void shouldGetOrder() throws IOException, InterruptedException {
        //Given
        Order sent = new Order(1L, 1L, CREATED, UNDERPANTS, 3, 10.00d);
        service.start(CLUSTER.bootstrapServers());
        sendOrders(Arrays.asList(sent));

        //Wait for data to be available
        TestUtils.waitForCondition(() -> service.getOrder(1L) != null, 30000, "timed out reading state store");

        //When
        Order result = service.getOrder(1L);

        //Then
        assertThat(result).isEqualTo(sent);
    }
}
