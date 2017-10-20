package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.microservices.orders.query.OrderQuerySubService;
import io.confluent.examples.streams.microservices.util.MicroserviceTestUtils;
import io.confluent.examples.streams.microservices.util.MicroserviceUtils;
import io.confluent.examples.streams.microservices.util.StubAsyncResponse;
import org.apache.kafka.streams.state.HostInfo;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static io.confluent.examples.streams.avro.microservices.OrderType.CREATED;
import static io.confluent.examples.streams.avro.microservices.OrderType.VALIDATED;
import static io.confluent.examples.streams.avro.microservices.ProductType.UNDERPANTS;
import static io.confluent.examples.streams.microservices.orders.beans.OrderBean.toBean;
import static io.confluent.examples.streams.microservices.orders.beans.OrderId.id;
import static java.util.Arrays.asList;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.assertj.core.api.Assertions.assertThat;

public class OrderQuerySubServiceTest extends MicroserviceTestUtils {
    OrderQuerySubService service;

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
        service = new OrderQuerySubService(new HostInfo("localhost", MicroserviceUtils.randomFreeLocalPort()));
        Order ceatedOrder = new Order(id(0L), 1L, CREATED, UNDERPANTS, 3, 10.00d);
        Order validatedOrder = new Order(id(1L), 1L, VALIDATED, UNDERPANTS, 3, 10.00d);

        //Given
        service.start(CLUSTER.bootstrapServers());
        sendOrders(asList(ceatedOrder));

        StubAsyncResponse stubAsyncResponse = new StubAsyncResponse();

        //When
        service.getOrder(id(1L), stubAsyncResponse);
        sendOrders(asList(validatedOrder));

        waitForCondition(() -> stubAsyncResponse.isDone(), 5000, "timed out reading state store");

        //Then
        assertThat(stubAsyncResponse.response).isEqualTo(toBean(validatedOrder));
    }
}
