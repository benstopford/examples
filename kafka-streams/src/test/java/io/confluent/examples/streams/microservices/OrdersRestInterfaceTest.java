package io.confluent.examples.streams.microservices;

import avro.shaded.com.google.common.collect.Sets;
import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderType;
import io.confluent.examples.streams.avro.microservices.ProductType;
import io.confluent.examples.streams.interactivequeries.HostStoreInfo;
import io.confluent.examples.streams.microservices.orders.beans.OrderBean;
import io.confluent.examples.streams.microservices.orders.beans.OrderId;
import io.confluent.examples.streams.microservices.orders.command.OrderCommand;
import io.confluent.examples.streams.microservices.orders.query.OrderQuery;
import io.confluent.examples.streams.microservices.orders.rest.OrdersRestInterface;
import org.apache.kafka.streams.state.HostInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import javax.ws.rs.ServerErrorException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.net.HttpURLConnection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.randomFreeLocalPort;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.Assert.fail;
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
    public void shouldPostOrderAndGetItBackOnceButNotBeforeOrdersViewIsUpdated() throws Exception {
        OrderBean createdBean = new OrderBean(OrderId.id(1L), 2L, OrderType.CREATED, ProductType.JUMPERS, 10, 100d);
        OrderBean validatedBean = new OrderBean(OrderId.id(1L), 2L, OrderType.VALIDATED, ProductType.JUMPERS, 10, 100d);

        final Client client = ClientBuilder.newClient();
        final String baseUrl = "http://localhost:" + port + "/orders";

        //Stub the underlying orders service
        final ArgumentCaptor<AsyncResponse> captor = ArgumentCaptor.forClass(AsyncResponse.class);
        OrderCommand command = mock(OrderCommand.class);
        when(command.putOrder(any(Order.class))).thenReturn(OrderCommand.OrderCommandResult.SUCCESS);
        OrderQuery query = mock(OrderQuery.class);
        when(query.getHostForOrderId(anyString())).thenReturn(new HostStoreInfo("localhost", port, Sets.newHashSet("whatever")));


        //Start the rest interface
        rest1 = new OrdersRestInterface(
                new HostInfo("localhost", port),
                command, query
        );
        rest1.start();

        //When post order
        Response response = client.target(baseUrl + "/post")
                .request(APPLICATION_JSON_TYPE)
                .post(Entity.json(createdBean));

        //Then
        assertThat(response.getStatus()).isEqualTo(HttpURLConnection.HTTP_CREATED);

        //When GET
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Executors.newSingleThreadExecutor().execute(() -> {
            OrderBean returnedBean = client.target(baseUrl + "/order/1")
                    .request(APPLICATION_JSON_TYPE)
                    .get(new GenericType<OrderBean>() {
                    });
            //Then
            assertThat(returnedBean).isEqualTo(validatedBean);
            countDownLatch.countDown();
        });

        Thread.sleep(100);
        verify(query).getOrder(anyString(), captor.capture());

        //Synthesise the order hitting the materialized view
        captor.getValue().resume(validatedBean);

        //Await the GET completing
        countDownLatch.await(10, TimeUnit.SECONDS);
    }


    @Test
    public void shouldTimeoutGetIfNoResponseIsFound() throws Exception {
        final Client client = ClientBuilder.newClient();
        final String baseUrl = "http://localhost:" + port + "/orders";

        //Stub the underlying orders service
        OrderCommand command = mock(OrderCommand.class);
        when(command.putOrder(any(Order.class))).thenReturn(OrderCommand.OrderCommandResult.SUCCESS);
        OrderQuery query = mock(OrderQuery.class);
        when(query.getHostForOrderId(anyString())).thenReturn(new HostStoreInfo("localhost", port, Sets.newHashSet("whatever")));


        //Start the rest interface
        rest1 = new OrdersRestInterface(
                new HostInfo("localhost", port),
                command, query
        );
        rest1.start();

        //When post order
        OrderBean createdBean = new OrderBean(OrderId.id(1L), 2L, OrderType.CREATED, ProductType.JUMPERS, 10, 100d);
        Response response = client.target(baseUrl + "/post")
                .request(APPLICATION_JSON_TYPE)
                .post(Entity.json(createdBean));

        //Then
        assertThat(response.getStatus()).isEqualTo(HttpURLConnection.HTTP_CREATED);

        //When get order should timeout
        try {
            client.target(baseUrl + "/order/1")
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
        final String baseUrl = "http://localhost:" + port1 + "/orders";

        //1st rest service will return null for the orderId and redirect to the second on port 2
        HostStoreInfo hostPort2 = new HostStoreInfo("localhost", port2, Sets.newHashSet("whatever"));

        //1st rest service returns that the order lives on host with port 2
        rest1 = new OrdersRestInterface(
                new HostInfo("localhost", port1),
                null, immediatelyAnsweringQuery(hostPort2, null)
        );
        rest1.start();

        //2nd rest service correctly returns the order
        rest2 = new OrdersRestInterface(
                new HostInfo("localhost", port2),
                null, immediatelyAnsweringQuery(hostPort2, order)
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

    private OrderQuery immediatelyAnsweringQuery(final HostStoreInfo hostForOrderId, final OrderBean bean) {
        return new OrderQuery() {
            @Override
            public HostStoreInfo getHostForOrderId(String orderId) {
                return hostForOrderId;
            }

            @Override
            public void getOrder(String id, AsyncResponse asyncResponse) {
                System.out.println("calling response with bean");
                asyncResponse.resume(bean);
            }
        };
    }
}
