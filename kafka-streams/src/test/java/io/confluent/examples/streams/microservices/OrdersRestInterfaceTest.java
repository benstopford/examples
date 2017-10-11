package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderType;
import io.confluent.examples.streams.avro.microservices.ProductType;
import io.confluent.examples.streams.microservices.rest.OrderBean;
import org.apache.kafka.streams.state.HostInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ServerSocket;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.Mockito.*;

public class OrdersRestInterfaceTest {

    private int port;
    private OrdersRestInterface rest;

    @Before
    public void start() throws Exception {
        port = randomFreeLocalPort();

    }

    @After
    public void shutdown() throws Exception {
        rest.stop();
    }

    @Test
    public void shouldPostOrderThenGetItBack() throws Exception {
        final Client client = ClientBuilder.newClient();
        final String baseUrl = "http://localhost:" + port + "/orders";

        //Stub the underlying orders service
        OrdersRequestResponse service = mock(OrdersRequestResponse.class);
        when(service.getOrder(1L)).thenReturn(new Order(1L, 2L, OrderType.VALIDATED, ProductType.JUMPERS, 10, 100d));
        when(service.putOrderAndWait(any(Order.class))).thenReturn(true);

        //Start the rest interface
        rest = new OrdersRestInterface(
                new HostInfo("localhost", port),
                service
        );
        rest.start();

        //When post order
        OrderBean inputOrder = new OrderBean(1L, 2L, OrderType.CREATED, ProductType.JUMPERS, 10, 100d);
        Response response = client.target(baseUrl + "/post")
                .request(APPLICATION_JSON_TYPE)
                .post(Entity.json(inputOrder));

        //Then
        assertThat(response.getStatus()).isEqualTo(HttpURLConnection.HTTP_CREATED);

        //When get order
        OrderBean returnedBean = client.target(baseUrl + "/order/1")
                .request(APPLICATION_JSON_TYPE)
                .get(new GenericType<OrderBean>() {
                });

        assertThat(returnedBean).isEqualTo(new OrderBean(
                inputOrder.getId(),
                inputOrder.getCustomerId(),
                OrderType.VALIDATED,
                inputOrder.getProduct(),
                inputOrder.getQuantity(),
                inputOrder.getPrice()
        ));
    }

    public static int randomFreeLocalPort() throws IOException {
        ServerSocket s = new ServerSocket(0);
        int port = s.getLocalPort();
        s.close();
        return port;
    }
}
