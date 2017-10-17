package io.confluent.examples.streams.microservices;

import avro.shaded.com.google.common.collect.Sets;
import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderType;
import io.confluent.examples.streams.avro.microservices.ProductType;
import io.confluent.examples.streams.interactivequeries.HostStoreInfo;
import io.confluent.examples.streams.microservices.orders.beans.OrderBean;
import io.confluent.examples.streams.microservices.orders.command.OrderCommand;
import io.confluent.examples.streams.microservices.orders.query.OrderQuery;
import io.confluent.examples.streams.microservices.orders.rest.OrdersRestInterface;
import org.apache.kafka.streams.state.HostInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.net.HttpURLConnection;

import static io.confluent.examples.streams.microservices.util.MicroserviceTestUtils.randomFreeLocalPort;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.Mockito.*;

public class OrdersRestInterfaceTest {

    private int port;
    private OrdersRestInterface rest1;
    private OrdersRestInterface rest2;

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
    public void shouldPostOrderThenGetItBack() throws Exception {
        final Client client = ClientBuilder.newClient();
        final String baseUrl = "http://localhost:" + port + "/orders";

        //Stub the underlying orders service
        OrderCommand command = mock(OrderCommand.class);
        when(command.putOrderAndWait(any(Order.class))).thenReturn(OrderCommand.OrderCommandResult.SUCCESS);
        OrderQuery query = mock(OrderQuery.class);
        when(query.getHostForOrderId(anyLong())).thenReturn(new HostStoreInfo("localhost", port, Sets.newHashSet("whatever")));
        when(query.getOrder(1L)).thenReturn(new Order(1L, 2L, OrderType.VALIDATED, ProductType.JUMPERS, 10, 100d));

        //Start the rest interface
        rest1 = new OrdersRestInterface(
                new HostInfo("localhost", port),
                command, query
        );
        rest1.start();

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

        //Then
        assertThat(returnedBean).isEqualTo(new OrderBean(
                inputOrder.getId(),
                inputOrder.getCustomerId(),
                OrderType.VALIDATED,
                inputOrder.getProduct(),
                inputOrder.getQuantity(),
                inputOrder.getPrice()
        ));
    }

    @Test
    public void shouldGetOrderByIdWhenOnDifferentHost() throws Exception {

        int port1 = randomFreeLocalPort();
        int port2 = randomFreeLocalPort();
        final Client client = ClientBuilder.newClient();
        final String baseUrl = "http://localhost:" + port1 + "/orders";

        //1st rest service will return null for the orderId and redirect to the second on port 2
        OrderQuery query1 = mock(OrderQuery.class);
        when(query1.getHostForOrderId(anyLong())).thenReturn(new HostStoreInfo("localhost", port2, Sets.newHashSet("whatever")));
        when(query1.getOrder(1L)).thenReturn(null);
        //Start the rest interface
        rest1 = new OrdersRestInterface(
                new HostInfo("localhost", port1),
                null, query1
        );
        rest1.start();

        //2nd rest service correctly returns the order
        OrderQuery query2 = mock(OrderQuery.class);
        when(query2.getHostForOrderId(anyLong())).thenReturn(new HostStoreInfo("localhost", port2, Sets.newHashSet("whatever")));
        Order order = new Order(1L, 2L, OrderType.VALIDATED, ProductType.JUMPERS, 10, 100d);
        when(query2.getOrder(1L)).thenReturn(order);

        rest2 = new OrdersRestInterface(
                new HostInfo("localhost", port2),
                null, query2
        );
        rest2.start();

        //When get order from rest1 (which doesn't have the order)
        OrderBean returnedBean = client.target(baseUrl + "/order/1")
                .request(APPLICATION_JSON_TYPE)
                .get(new GenericType<OrderBean>() {
                });

        //Then we should get it from rest2
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
