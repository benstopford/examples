package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderType;
import io.confluent.examples.streams.avro.microservices.ProductType;
import io.confluent.examples.streams.microservices.util.MicroserviceTestUtils;
import io.confluent.examples.streams.microservices.util.beans.OrderBean;
import io.confluent.examples.streams.microservices.util.beans.OrderId;
import org.apache.kafka.streams.state.HostInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ws.rs.ServerErrorException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.net.HttpURLConnection;
import java.util.Arrays;

import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.randomFreeLocalPort;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.Assert.fail;

public class OrdersServiceTest extends MicroserviceTestUtils {

    private int port;
    private OrdersService rest1;
    private OrdersService rest2;

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(Schemas.Topics.ORDERS.name());
        System.out.println("running with schema registry: " + CLUSTER.schemaRegistryUrl());
        Schemas.configureSerdesWithSchemaRegistryUrl(CLUSTER.schemaRegistryUrl());
    }

    @Before
    public void start() throws Exception {
        port = randomFreeLocalPort();
    }

    @After
    public void shutdown() throws Exception {
        if (rest1 != null)
            rest1.stop();
        if (rest2 != null)
            rest2.stop();
    }

    @Test
    public void shouldPostOrderAndGetItBack() throws Exception {
        OrderBean createdBean = new OrderBean(OrderId.id(1L), 2L, OrderType.CREATED, ProductType.JUMPERS, 10, 100d);

        final Client client = ClientBuilder.newClient();
        final String baseUrl = "http://localhost:" + port + "/orders";

        //Stub the underlying orders service
        rest1 = new OrdersService(
                new HostInfo("localhost", port)
        );
        rest1.start(CLUSTER.bootstrapServers());

        //When post order
        Response response = client.target(baseUrl + "/post")
                .request(APPLICATION_JSON_TYPE)
                .post(Entity.json(createdBean));

        //Then
        assertThat(response.getStatus()).isEqualTo(HttpURLConnection.HTTP_CREATED);

        //When GET
        OrderBean returnedBean = client.target(baseUrl + "/order/" + createdBean.getId())
                .request(APPLICATION_JSON_TYPE)
                .get(new GenericType<OrderBean>() {
                });
        //Then
        assertThat(returnedBean).isEqualTo(createdBean);
    }


    @Test
    public void shouldLinkToNextVersionOfRecordInResponse() throws Exception {
        OrderBean beanV1 = new OrderBean(OrderId.id(1L, 0), 2L, OrderType.CREATED, ProductType.JUMPERS, 10, 100d);
        Order orderV2 = new Order(OrderId.id(1L, 1), 2L, OrderType.VALIDATED, ProductType.JUMPERS, 10, 100d);
        OrderBean beanV2 = OrderBean.toBean(orderV2);

        final Client client = ClientBuilder.newClient();
        final String baseUrl = "http://localhost:" + port + "/orders";

        //Stub the underlying orders service
        rest1 = new OrdersService(
                new HostInfo("localhost", port)
        );
        rest1.start(CLUSTER.bootstrapServers());

        //When post order
        Response response = client.target(baseUrl + "/post")
                .request(APPLICATION_JSON_TYPE)
                .post(Entity.json(beanV1));

        //Simulate the order being validated
        MicroserviceTestUtils.sendOrders(Arrays.asList(orderV2));

        //Then the URI returned by the orginal post should point to the validated order
        OrderBean returnedBean = client.target(response.getLocation())
                .request(APPLICATION_JSON_TYPE)
                .get(new GenericType<OrderBean>() {
                });
        //Then
        assertThat(returnedBean).isEqualTo(beanV2);
    }

    @Test
    public void shouldTimeoutGetIfNoResponseIsFound() throws Exception {
        final Client client = ClientBuilder.newClient();
        final String baseUrl = "http://localhost:" + port + "/orders";


        //Start the rest interface
        rest1 = new OrdersService(
                new HostInfo("localhost", port)
        );
        rest1.start(CLUSTER.bootstrapServers());

        //When GET order should timeout
        try {
            client.target(baseUrl + "/order/" + OrderId.id(1))
                    .queryParam("timeout", 100) //Lower the request timeout
                    .request(APPLICATION_JSON_TYPE)
                    .get(new GenericType<OrderBean>() {
                    });
            fail("Request should have failed as materialized view has not been updated");
        } catch (ServerErrorException e) {
            assertThat(e.getMessage()).isEqualTo("HTTP 504 Gateway Timeout");
        }
    }

    @Test
    public void shouldGetOrderByIdWhenOnDifferentHost() throws Exception {
        OrderBean order = new OrderBean(OrderId.id(1L), 2L, OrderType.VALIDATED, ProductType.JUMPERS, 10, 100d);
        int port1 = randomFreeLocalPort();
        int port2 = randomFreeLocalPort();
        final Client client = ClientBuilder.newClient();

        //Given two rest servers on different ports
        rest1 = new OrdersService(new HostInfo("localhost", port1));
        rest2 = new OrdersService(new HostInfo("localhost", port2));
        rest1.start(CLUSTER.bootstrapServers());
        rest2.start(CLUSTER.bootstrapServers());

        //And one order
        client.target("http://localhost:" + port1 + "/orders/post")
                .request(APPLICATION_JSON_TYPE)
                .post(Entity.json(order));

        //When GET to rest1
        OrderBean returnedBean = client.target("http://localhost:" + port1 + "/orders/order/" + order.getId())
                .request(APPLICATION_JSON_TYPE)
                .get(new GenericType<OrderBean>() {
                });

        //Then we should get the order back
        assertThat(returnedBean).isEqualTo(new OrderBean(
                order.getId(),
                order.getCustomerId(),
                order.getState(),
                order.getProduct(),
                order.getQuantity(),
                order.getPrice()
        ));

        //When GET to rest2
        returnedBean = client.target("http://localhost:" + port2 + "/orders/order/" + order.getId())
                .request(APPLICATION_JSON_TYPE)
                .get(new GenericType<OrderBean>() {
                });

        //Then we should get the order back also
        assertThat(returnedBean).isEqualTo(new OrderBean(
                order.getId(),
                order.getCustomerId(),
                order.getState(),
                order.getProduct(),
                order.getQuantity(),
                order.getPrice()
        ));
    }
}
