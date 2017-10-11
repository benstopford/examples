package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.microservices.rest.OrderBean;
import org.apache.kafka.streams.state.HostInfo;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.HttpURLConnection;


@Path("orders")
public class OrdersRestInterface {
    private Server jettyServer;
    private HostInfo hostInfo;
    private OrdersRequestResponse service;

    public OrdersRestInterface(HostInfo hostInfo, OrdersRequestResponse ordersRequestResponse) {
        this.hostInfo = hostInfo;
        this.service = ordersRequestResponse;
    }

    @GET()
    @Path("order/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public OrderBean song(@PathParam("id") final Long id) {
        Order order = service.getOrder(id);
        System.out.println("/order/id --> " + order);
        return OrderBean.toBean(order);
    }

    @POST
    @Path("/post")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response submitOrder(OrderBean order) {
        System.out.println("Running post of " + order);

        boolean success = service.putOrderAndWait(OrderBean.fromBean(order));
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
