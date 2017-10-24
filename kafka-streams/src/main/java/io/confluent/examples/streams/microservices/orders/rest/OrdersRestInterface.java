package io.confluent.examples.streams.microservices.orders.rest;

import io.confluent.examples.streams.interactivequeries.HostStoreInfo;
import io.confluent.examples.streams.microservices.orders.beans.OrderBean;
import io.confluent.examples.streams.microservices.orders.beans.OrderId;
import io.confluent.examples.streams.microservices.orders.command.OrderCommand;
import io.confluent.examples.streams.microservices.orders.query.OrderQuery;
import org.apache.kafka.streams.state.HostInfo;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ManagedAsync;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import javax.ws.rs.*;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static org.apache.kafka.streams.state.StreamsMetadata.NOT_AVAILABLE;


@Path("orders")
public class OrdersRestInterface {
    public static final int LONG_POLL_TIMEOUT = 20000;
    private Server jettyServer;
    private HostInfo hostInfo;
    private OrderCommand command;
    private OrderQuery query;
    private final Client client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();

    public OrdersRestInterface(HostInfo hostInfo, OrderCommand orderCommand, OrderQuery query) {
        this.hostInfo = hostInfo;
        this.command = orderCommand;
        this.query = query;
    }

    @GET
    @ManagedAsync
    @Path("order/{id}")
    public void asyncGet(@PathParam("id") final String id, @Suspended final AsyncResponse asyncResponse) {
        setTimeout(asyncResponse);

        HostStoreInfo hostForKey = getKeyLocation(id, asyncResponse);

        if (hostForKey == null)
            return;
        else if (thisHost(hostForKey)) {
            System.out.println("running GET on this node");
            query.getOrder(id, asyncResponse);
        } else {
            System.out.println("Running get on a different node: " + hostForKey);
            fetchFromOtherHost(hostForKey, "orders/order/" + id, asyncResponse);
        }
    }

    private HostStoreInfo getKeyLocation(String id, AsyncResponse asyncResponse) {
        HostStoreInfo locationOfKey;
        while (isUnavailable(locationOfKey = query.getHostForOrderId(id))) {
            //The state store is not available. This can happen on startup.
            if (asyncResponse.isDone())
                return null;
            try {
                //If there is an outstanding request sleep a bit until the state store is available.
                Thread.sleep(Math.min(LONG_POLL_TIMEOUT, 500));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return locationOfKey;
    }

    private boolean isUnavailable(HostStoreInfo hostWithKey) {
        return NOT_AVAILABLE.host().equals(hostWithKey.getHost())
                && NOT_AVAILABLE.port() == hostWithKey.getPort();
    }


    private void setTimeout(@Suspended AsyncResponse asyncResponse) {
        asyncResponse.setTimeout(LONG_POLL_TIMEOUT, TimeUnit.MILLISECONDS);
        asyncResponse.setTimeoutHandler(resp -> resp.resume(
                Response.status(Response.Status.GATEWAY_TIMEOUT)
                        .entity("HTTP GET timed out after " + LONG_POLL_TIMEOUT + " ms")
                        .build()));
    }

    private boolean thisHost(final HostStoreInfo host) {
        return host.getHost().equals(hostInfo.host()) &&
                host.getPort() == hostInfo.port();
    }

    private void fetchFromOtherHost(final HostStoreInfo host, final String path, AsyncResponse asyncResponse) {
        try {
            OrderBean bean = client.target(String.format("http://%s:%d/%s", host.getHost(), host.getPort(), path))
                    .request(MediaType.APPLICATION_JSON_TYPE)
                    .get(new GenericType<OrderBean>() {
                    });
            asyncResponse.resume(bean);
        } catch (Exception swallowed) {
        }
    }

    @POST
    @Path("/post")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response submitOrder(OrderBean order) {

        OrderCommand.OrderCommandResult success = command.putOrder(OrderBean.fromBean(order));

        switch (success) {
            case SUCCESS:
                try {
                    return Response.created(new URI("/orders/order/" + OrderId.next(order.getId()))).entity(OrderId.next(order.getId()
                    )).build();
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                }
            default:
                return Response.status(HTTP_BAD_REQUEST).build();
        }
    }

    public void start() throws Exception {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        jettyServer = new Server(hostInfo.port());
        jettyServer.setHandler(context);

        ResourceConfig rc = new ResourceConfig();
        rc.register(this);
        rc.register(JacksonFeature.class);

        ServletContainer sc = new ServletContainer(rc);
        ServletHolder holder = new ServletHolder(sc);
        context.addServlet(holder, "/*");

        jettyServer.start();
        System.out.println("listening on " + jettyServer.getURI());
        System.out.println();
    }

    public void stop() throws Exception {
        if (jettyServer != null) {
            jettyServer.stop();
        }
    }
}
