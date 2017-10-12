package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.interactivequeries.HostStoreInfo;
import io.confluent.examples.streams.microservices.rest.OrderBean;
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
import java.net.HttpURLConnection;


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

        Order order = thisHost(host) ?
                query.getOrder(id)
                : fetchFromOtherHost(host, "orders/order/" + id);

        System.out.println("/order/id --> " + order);
        return OrderBean.toBean(order);
    }

    private boolean thisHost(final HostStoreInfo host) {
        System.out.println("is that host " + host + " == " + hostInfo);
        return host.getHost().equals(hostInfo.host()) &&
                host.getPort() == hostInfo.port();
    }

    private Order fetchFromOtherHost(final HostStoreInfo host, final String path) {
        return client.target(String.format("http://%s:%d/%s", host.getHost(), host.getPort(), path))
                .request(MediaType.APPLICATION_JSON_TYPE)
                .get(new GenericType<Order>() {
                });
    }

    @POST
    @Path("/post")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response submitOrder(OrderBean order) {
        System.out.println("Running post of " + order);

        boolean success = command.putOrderAndWait(OrderBean.fromBean(order));
        if (success)
            return Response.status(HttpURLConnection.HTTP_CREATED).build();
        else
            return Response.status(HttpURLConnection.HTTP_GATEWAY_TIMEOUT).build();
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
