package io.confluent.examples.streams.microservices.orders.query;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.interactivequeries.HostStoreInfo;
import io.confluent.examples.streams.interactivequeries.MetadataService;
import io.confluent.examples.streams.microservices.Service;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.state.QueryableStoreTypes;

import static io.confluent.examples.streams.microservices.Schemas.Topics.ORDERS;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.streamsConfig;


public class OrderQuerySubService implements Service, OrderQuery {
    public static final String ORDERS_STORE_NAME = "orders-store";
    public final String SERVICE_APP_ID = getClass().getSimpleName();
    private KafkaStreams streams;
    private MetadataService metadataService;

    @Override
    public void start(String bootstrapServers) {
        streams = createOrdersStore(bootstrapServers, "/tmp/kafka-streams");
        streams.cleanUp(); //don't do this in prod as it clears your state stores
        metadataService = new MetadataService(streams);
        streams.start();
        System.out.println("Started Service " + getClass().getSimpleName());
    }

    private KafkaStreams createOrdersStore(final String bootstrapServers,
                                           final String stateDir) {
        //Create a simple store of Orders by their Key
        KStreamBuilder builder = new KStreamBuilder();
        builder.stream(ORDERS.keySerde(), ORDERS.valueSerde(), ORDERS.name())
                .groupByKey(ORDERS.keySerde(), ORDERS.valueSerde())
                .reduce((agg, newVal) -> newVal, ORDERS_STORE_NAME);

        return new KafkaStreams(builder, streamsConfig(bootstrapServers, stateDir, SERVICE_APP_ID));
    }

    @Override
    public void stop() {
        if (streams != null) streams.close();
    }

    @Override
    public HostStoreInfo getHostForOrderId(Long orderId) {
        return metadataService.streamsMetadataForStoreAndKey(ORDERS_STORE_NAME, orderId, Serdes.Long().serializer());
    }

    @Override
    public Order getOrder(Long id) {
        try {
            return streams.store(ORDERS_STORE_NAME, QueryableStoreTypes.<Long, Order>keyValueStore()).get(id);
        } catch (InvalidStateStoreException e) {
            System.out.println("No state store present at this time");
            return null;
        }
    }
}