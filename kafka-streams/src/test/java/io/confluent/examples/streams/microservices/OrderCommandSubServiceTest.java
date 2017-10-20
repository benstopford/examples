package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.microservices.Schemas.Topics;
import io.confluent.examples.streams.microservices.orders.command.OrderCommandSubService;
import io.confluent.examples.streams.microservices.util.MicroserviceTestUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.confluent.examples.streams.avro.microservices.OrderType.CREATED;
import static io.confluent.examples.streams.avro.microservices.ProductType.UNDERPANTS;
import static io.confluent.examples.streams.microservices.orders.beans.OrderId.id;
import static io.confluent.examples.streams.microservices.orders.command.OrderCommand.OrderCommandResult;
import static org.assertj.core.api.Assertions.assertThat;

public class OrderCommandSubServiceTest extends MicroserviceTestUtils {
    OrderCommandSubService service = new OrderCommandSubService();

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(Topics.ORDERS.name());
        System.out.println("running with schema registry: " + CLUSTER.schemaRegistryUrl());
        Schemas.configureSerdesWithSchemaRegistryUrl(CLUSTER.schemaRegistryUrl());
    }

    @After
    public void tearDown() {
        service.stop();
    }

    @Test
    public void shouldPutOrder() throws InterruptedException {
        service.start(CLUSTER.bootstrapServers());

        Order orderToPut = new Order(id(0L), 1L, CREATED, UNDERPANTS, 3, 10.00d);
        OrderCommandResult result = service.putOrder(orderToPut);
        assertThat(result).isEqualTo(OrderCommandResult.SUCCESS);

        //Make sure the order was submitted
        assertThat(read(Topics.ORDERS, 1, CLUSTER.bootstrapServers()).get(0))
                .isEqualTo(orderToPut);
    }
}
