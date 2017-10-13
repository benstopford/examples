package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.microservices.util.MicroserviceTestUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.confluent.examples.streams.avro.microservices.OrderType.*;
import static io.confluent.examples.streams.avro.microservices.ProductType.UNDERPANTS;
import static io.confluent.examples.streams.microservices.OrderCommand.OrderCommandResult;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class OrderCommandSubServiceTest extends MicroserviceTestUtils {
    OrderCommandSubService service = new OrderCommandSubService();

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
    public void shouldSubmitOrderAndReturnWhenValidated() throws IOException, InterruptedException {
        service.start(CLUSTER.bootstrapServers());
        CountDownLatch latch = new CountDownLatch(1);

        //putOrder and wait (using a separate thread)
        Executors.newSingleThreadExecutor().execute(() -> {
            OrderCommandResult result = service.putOrderAndWait(new Order(0L, 1L, CREATED, UNDERPANTS, 3, 10.00d));
            if (OrderCommandResult.SUCCESS.equals(result)) latch.countDown();
        });

        //Ensure the call doesn't return immediately
        Thread.sleep(100);
        assertThat(latch.getCount()).isEqualTo(1);

        //Send a message to denote that the order was validated, this should trigger the call to return
        MicroserviceTestUtils.sendOrders(asList(new Order(0L, 1L, VALIDATED, UNDERPANTS, 3, 10.00d)));

        //Await the call returning with Validated
        latch.await(10, TimeUnit.SECONDS);
        assertThat(latch.getCount()).isEqualTo(0);
    }

    @Test
    public void shouldTimeoutIfNoProcessing() throws IOException, InterruptedException {
        service.start(CLUSTER.bootstrapServers());
        OrderCommandResult result = service.putOrderAndWait(new Order(0L, 1L, CREATED, UNDERPANTS, 3, 10.00d));
        assertThat(result).isEqualTo(OrderCommandResult.TIMED_OUT);
    }

    @Test
    public void shouldSubmitOrderAndReturnWhenValidationFailed() throws IOException, InterruptedException {
        service.start(CLUSTER.bootstrapServers());
        CountDownLatch latch = new CountDownLatch(1);

        //putOrder and wait (using a separate thread)
        Executors.newSingleThreadExecutor().execute(() -> {
            OrderCommandResult result = service.putOrderAndWait(new Order(0L, 1L, CREATED, UNDERPANTS, 3, 10.00d));
            if (OrderCommandResult.FAILED_VALIDATION.equals(result)) latch.countDown();
        });

        //Ensure the call doesn't return immediately
        Thread.sleep(100);
        assertThat(latch.getCount()).isEqualTo(1);

        //Send a message to denote that the order was validated, this should trigger the call to return
        MicroserviceTestUtils.sendOrders(asList(new Order(0L, 1L, FAILED, UNDERPANTS, 3, 10.00d)));

        //Await the call returning with Validated
        latch.await(10, TimeUnit.SECONDS);
        assertThat(latch.getCount()).isEqualTo(0);
    }
}
