package io.confluent.examples.streams.microservices.orders.query;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.interactivequeries.HostStoreInfo;
import io.confluent.examples.streams.interactivequeries.MetadataService;
import io.confluent.examples.streams.microservices.Service;
import io.confluent.examples.streams.microservices.orders.beans.OrderBean;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;

import javax.ws.rs.container.AsyncResponse;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static io.confluent.examples.streams.microservices.Schemas.Topics.ORDERS;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.baseStreamsConfig;


public class OrderQuerySubService implements Service, OrderQuery {
    public static final String ORDERS_STORE_NAME = "orders-store";
    public final String SERVICE_APP_ID = getClass().getSimpleName();
    private KafkaStreams streams;
    private MetadataService metadataService;
    //In a real implementation we would need to periodically purge old entries from this map.
    private Map<String, AsyncResponse> outstandingRequests = new ConcurrentHashMap<>();
    private HostInfo serverDiscoverableHostAddress;

    public OrderQuerySubService(HostInfo hostInfo) {
        serverDiscoverableHostAddress = hostInfo;
    }

    @Override
    public void start(String bootstrapServers) {
        streams = new KafkaStreams(createOrdersStore(), config(bootstrapServers));
        streams.cleanUp(); //don't do this in prod as it clears your state stores
        metadataService = new MetadataService(streams);
        streams.start();
        System.out.println("Started Service " + getClass().getSimpleName());
    }

    private Properties config(String bootstrapServers) {
        Properties props = baseStreamsConfig(bootstrapServers, "/tmp/kafka-streams", SERVICE_APP_ID);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, discoverableAddress());
        return props;
    }

    private String discoverableAddress() {
        return serverDiscoverableHostAddress.host() + ":" + serverDiscoverableHostAddress.port();
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

    @Override
    public void stop() {
        if (streams != null) streams.close();
    }

    @Override
    public HostStoreInfo getHostForOrderId(String orderId) {
        return metadataService.streamsMetadataForStoreAndKey(ORDERS_STORE_NAME, orderId, Serdes.String().serializer());
    }

    @Override
    public void getOrder(String id, AsyncResponse asyncResponse) {
        try {
            Order order = streams.store(ORDERS_STORE_NAME, QueryableStoreTypes.<String, Order>keyValueStore()).get(id);
            if (order == null) {
                outstandingRequests.put(id, asyncResponse);
            } else {
                asyncResponse.resume(OrderBean.toBean(order));
            }
        } catch (InvalidStateStoreException e) {
            //Store not ready so delay
            outstandingRequests.put(id, asyncResponse);
        }
    }
}