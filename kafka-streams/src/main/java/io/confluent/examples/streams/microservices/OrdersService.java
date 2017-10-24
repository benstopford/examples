package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.interactivequeries.HostStoreInfo;
import io.confluent.examples.streams.interactivequeries.MetadataService;
import io.confluent.examples.streams.microservices.util.beans.OrderBean;
import io.confluent.examples.streams.microservices.util.beans.OrderId;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static io.confluent.examples.streams.microservices.Schemas.Topics.ORDERS;
import static io.confluent.examples.streams.microservices.Schemas.Topics.ORDER_VALIDATIONS;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.baseStreamsConfig;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.randomFreeLocalPort;
import static org.apache.kafka.streams.state.StreamsMetadata.NOT_AVAILABLE;

/**
 * This class provides a REST interface to access the Orders service.
 * It provides a wrapper around OrderCommand & OrderQuery interfaces (CQRS)
 * implemented through non-blocking IO.
 * <p>
 * Notably, if a request cannot be services by this node, the location
 * of the node with the appropriate key will be called.
 */
@Path("orders")
public class OrdersService implements Service {
    public static final String LONG_POLL_TIMEOUT = "10000";
    public static final String ORDERS_STORE_NAME = "orders-store";
    public final String SERVICE_APP_ID = getClass().getSimpleName();
    private final Client client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();
    private Server jettyServer;
    private HostInfo hostInfo;
    private KafkaStreams streams;
    private MetadataService metadataService;
    private KafkaProducer<String, Order> producer;
    //In a real implementation we would need to periodically purge old entries from this map.
    private Map<String, AsyncResponse> outstandingRequests = new ConcurrentHashMap<>();

    public OrdersService(HostInfo hostInfo) {
        this.hostInfo = hostInfo;
    }

    private KStreamBuilder createOrdersStore() {
        //Create a simple store of Orders by their Key
        KStreamBuilder builder = new KStreamBuilder();
        builder.stream(ORDERS.keySerde(), ORDERS.valueSerde(), ORDERS.name())
                .groupByKey(ORDERS.keySerde(), ORDERS.valueSerde())
                .reduce((agg, newVal) -> newVal, ORDERS_STORE_NAME)
                .toStream().foreach((id, order) -> maybeCompleteLongPollGet(id, order));

        return builder;
    }

    private void maybeCompleteLongPollGet(String id, Order order) {
        AsyncResponse callback = outstandingRequests.get(id);
        if (callback != null)
            callback.resume(OrderBean.toBean(order));
    }

    /**
     * Perform a Long-Poll get. This method will attempt to get the value for the passed key
     * blocking until the key is available or passed timeout is reached. Non-blocking IO is
     * used to implement this.
     *
     * @param id            - the key of the value to retrieve
     * @param timeout       - the timeout for the long-poll
     * @param asyncResponse - async response used to trigger the poll early should the appropriate value become available
     */
    @GET
    @ManagedAsync
    @Path("order/{id}")
    public void getWithTimeout(
            @PathParam("id") final String id,
            @QueryParam("timeout") @DefaultValue(LONG_POLL_TIMEOUT) Long timeout,
            @Suspended final AsyncResponse asyncResponse) {
        setTimeout(timeout, asyncResponse);

        HostStoreInfo hostForKey = getKeyLocationOrBlock(id, asyncResponse);

        if (hostForKey == null)
            //something went wrong and we couldn't determine which host the key is on
            return;

        //Retrieve locally or go remote
        if (thisHost(hostForKey)) {
            fetchLocal(id, asyncResponse);
        } else {
            System.out.println("Running GET on a different node: " + hostForKey);
            fetchFromOtherHost(hostForKey, "orders/order/" + id, asyncResponse);
        }
    }

    private void fetchLocal(String id, AsyncResponse asyncResponse) {
        System.out.println("running GET on this node");
        try {
            Order order = streams.store(ORDERS_STORE_NAME, QueryableStoreTypes.<String, Order>keyValueStore()).get(id);
            if (order == null) {
                System.out.println("Delaying get as order not there for id " + id);
                outstandingRequests.put(id, asyncResponse);
            } else {
                System.out.println("found order so resuming");
                asyncResponse.resume(OrderBean.toBean(order));
            }
        } catch (InvalidStateStoreException e) {
            //Store not ready so delay
            outstandingRequests.put(id, asyncResponse);
        }
    }

    /**
     * The key could be located in a state store on this node, or on a different node.
     * Check if location metadata is available and if it isn't, which can happen on startup
     * or during a rebalance, block until it is.
     */
    private HostStoreInfo getKeyLocationOrBlock(String id, AsyncResponse asyncResponse) {
        HostStoreInfo locationOfKey;
        while (locationMetadataIsUnavailable(locationOfKey = getHostForOrderId(id))) {
            //The metastore is not available. This can happen on startup.
            if (asyncResponse.isDone())
                //The response timed out so return
                return null;
            try {
                //Sleep a bit until metadata becomes available
                Thread.sleep(Math.min(Long.valueOf(LONG_POLL_TIMEOUT), 500));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return locationOfKey;
    }

    private boolean locationMetadataIsUnavailable(HostStoreInfo hostWithKey) {
        return NOT_AVAILABLE.host().equals(hostWithKey.getHost())
                && NOT_AVAILABLE.port() == hostWithKey.getPort();
    }

    private static void setTimeout(long timeout, AsyncResponse asyncResponse) {
        asyncResponse.setTimeout(timeout, TimeUnit.MILLISECONDS);
        asyncResponse.setTimeoutHandler(resp -> resp.resume(
                Response.status(Response.Status.GATEWAY_TIMEOUT)
                        .entity("HTTP GET timed out after " + timeout + " ms")
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

    /**
     * Post an order. This call will return once the order is successfully persisted to Kafka.
     *
     * @param order
     * @return
     */
    @POST
    @ManagedAsync
    @Path("/post")
    @Consumes(MediaType.APPLICATION_JSON)
    public void submitOrder(OrderBean order, @Suspended final AsyncResponse response) {
        Order bean = OrderBean.fromBean(order);
        producer.send(
                new ProducerRecord<>(ORDERS.name(), bean.getId(), bean),
                callback(response, bean.getId())
        );
    }

    @Override
    public void start(String bootstrapServers) {
        startProducer(bootstrapServers);
        startKStreams(bootstrapServers);
        startJetty();
        System.out.println("Started Service " + getClass().getSimpleName());
    }

    private void startKStreams(String bootstrapServers) {
        streams = new KafkaStreams(createOrdersStore(), config(bootstrapServers));
        streams.cleanUp(); //don't do this in prod as it clears your state stores
        metadataService = new MetadataService(streams);
        streams.start();
    }

    private void startJetty() {
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

        try {
            jettyServer.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println("Listening on " + jettyServer.getURI());
    }

    private Properties config(String bootstrapServers) {
        Properties props = baseStreamsConfig(bootstrapServers, "/tmp/kafka-streams", SERVICE_APP_ID);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, discoverableAddress());
        return props;
    }

    private String discoverableAddress() {
        return hostInfo.host() + ":" + hostInfo.port();
    }

    @Override
    public void stop() {
        if (streams != null) streams.close();
        if (producer != null) {
            producer.close();
        }
        if (jettyServer != null) {
            try {
                jettyServer.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public HostStoreInfo getHostForOrderId(String orderId) {
        return metadataService.streamsMetadataForStoreAndKey(ORDERS_STORE_NAME, orderId, Serdes.String().serializer());
    }

    private Callback callback(final AsyncResponse response, final String orderId) {
        return (recordMetadata, e) -> {
            if (e != null)
                response.resume(e);
            else
                try {
                    //Return the location of the newly created resource
                    String newId = OrderId.next(orderId);
                    Response uri = Response.created(new URI("/orders/order/" + newId))
                            .entity(newId)
                            .build();
                    response.resume(uri);
                } catch (URISyntaxException e2) {
                    e2.printStackTrace();
                }
        };
    }

    private void startProducer(String bootstrapServers) {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");

        producer = new KafkaProducer(producerConfig,
                ORDER_VALIDATIONS.keySerde().serializer(),
                ORDER_VALIDATIONS.valueSerde().serializer());

    }

    public static void main(String[] args) throws Exception {

        final String bootstrapServers = args.length > 1 ? args[1] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 2 ? args[2] : "http://localhost:8081";
        final String restHostname = args.length > 3 ? args[3] : "localhost";
        final String restPort = args.length > 4 ? args[4] : Integer.toString(randomFreeLocalPort());

        Schemas.configureSerdesWithSchemaRegistryUrl(schemaRegistryUrl);
        OrdersService service = new OrdersService(new HostInfo(restHostname, Integer.valueOf(restPort)));
        service.start(bootstrapServers);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                service.stop();
            } catch (Exception ignored) {
            }
        }));
    }
}
