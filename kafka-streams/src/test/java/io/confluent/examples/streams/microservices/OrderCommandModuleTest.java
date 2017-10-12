package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.microservices.util.TestUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static io.confluent.examples.streams.avro.microservices.OrderType.CREATED;
import static io.confluent.examples.streams.avro.microservices.ProductType.UNDERPANTS;
import static org.assertj.core.api.Assertions.assertThat;

public class OrderCommandModuleTest extends TestUtils {
    OrderCommandModule service = new OrderCommandModule();

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
    public void should() throws IOException {
        service.start(CLUSTER.bootstrapServers());
        boolean result = service.putOrderAndWait(new Order(0L, 1L, CREATED, UNDERPANTS, 3, 10.00d));
        assertThat(result).isTrue();
    }
}
