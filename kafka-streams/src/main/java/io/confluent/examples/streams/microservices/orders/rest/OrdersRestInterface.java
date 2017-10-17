package io.confluent.examples.streams.microservices.orders.rest;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.interactivequeries.HostStoreInfo;
import io.confluent.examples.streams.microservices.orders.beans.OrderBean;
import io.confluent.examples.streams.microservices.orders.command.OrderCommand;
import io.confluent.examples.streams.microservices.orders.query.OrderQuery;
import org.apache.kafka.streams.state.HostInfo;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import javax.ws.rs.*;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static java.net.HttpURLConnection.*;


@Path("orders")
public class OrdersRestInterface {
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

    @GET()
    @Path("order/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public OrderBean song(@PathParam("id") final Long id) {
        HostStoreInfo host = query.getHostForOrderId(id);

        Order order = query.getOrder(id);
        if (order == null)
            if (!thisHost(host))
                order = fetchFromOtherHost(host, "orders/order/" + id);

        System.out.println("/order/id --> " + order);
        return order == null ? null : OrderBean.toBean(order);
    }

    private boolean thisHost(final HostStoreInfo host) {
        System.out.println("is that host " + host + " == " + hostInfo);
        return host.getHost().equals(hostInfo.host()) &&
                host.getPort() == hostInfo.port();
    }

    private Order fetchFromOtherHost(final HostStoreInfo host, final String path) {
        try {
            return client.target(String.format("http://%s:%d/%s", host.getHost(), host.getPort(), path))
                    .request(MediaType.APPLICATION_JSON_TYPE)
                    .get(new GenericType<Order>() {
                    });
        } catch (Exception swallowed) {
            return null;
        }
    }

    @POST
    @Path("/post")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response submitOrder(OrderBean order) {
        System.out.println("Running post of " + order);

        OrderCommand.OrderCommandResult success = command.putOrderAndWait(OrderBean.fromBean(order));
        System.out.println("Retuning from post with " + success);

        switch (success) {
            case SUCCESS:
                return Response.status(HTTP_CREATED).build();
            case TIMED_OUT:
                return Response.status(HTTP_GATEWAY_TIMEOUT).build();
            case FAILED_VALIDATION:
                return Response.status(HTTP_BAD_REQUEST).build();
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
