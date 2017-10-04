package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static io.confluent.examples.streams.microservices.MicroserviceUtils.CustomRocksDBConfig;

public class OrdersService {
    static final String ORDERS = "orders";
    static final String STOCK = "stock";
    static String OUTPUT = "output"; //TODO this should go back to the orders topic
    static final String ORDERS_SERVICE_APP_ID = "orders-service";
    private static final String stockReservationStore = "StockCountStore";

    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081";

    public static void main(String[] args) throws Exception {
        if (args.length > 2) {
            throw new IllegalArgumentException("usage: ... " +
                    "[<bootstrap.servers> (optional, default: " + DEFAULT_BOOTSTRAP_SERVERS + ")] " +
                    "[<schema.registry.url> (optional, default: " + DEFAULT_SCHEMA_REGISTRY_URL + ")] ");
        }
        final String bootstrapServers = args.length > 1 ? args[1] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 2 ? args[2] : "http://localhost:8081";

        System.out.println("Connecting to Kafka cluster via bootstrap servers " + bootstrapServers);
        System.out.println("Connecting to Confluent schema registry at " + schemaRegistryUrl);
        final KafkaStreams streams = new OrdersService().startService(bootstrapServers, schemaRegistryUrl);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                streams.close();
            } catch (Exception ignored) {
            }
        }));
    }

    KafkaStreams startService(String bootstrapServers, String schemaRegistryUrl) {
        KafkaStreams streams = processOrders(bootstrapServers, schemaRegistryUrl, "/tmp/kafka-streams");
        streams.cleanUp(); //don't do this in prod as it clears your state stores
        streams.start();
        return streams;
    }

    private KafkaStreams processOrders(final String bootstrapServers,
                                       final String schemaRegistryUrl,
                                       final String stateDir) {

        //Latch onto instances of the orders and inventory topics
        KStreamBuilder builder = new KStreamBuilder();
        SpecificAvroSerde<Order> orderSerdes = orderSerdes(schemaRegistryUrl);
        KStream<Long, Order> orders = builder.stream(Serdes.Long(), orderSerdes, ORDERS);
        KStream<String, Integer> inventory = builder.stream(Serdes.String(), Serdes.Integer(), STOCK);

        //Create a store to reserve inventory whilst the order is processed
        StateStoreSupplier stockReservation = Stores.create(stockReservationStore)
                .withKeys(Serdes.String())
                .withValues(Serdes.Long())
                .persistent()
                .build();
        builder.addStateStore(stockReservation);

        KStream<Long, Order> orderOutputs = orders
                .transform(StockCheckTransformer::new, stockReservationStore);
        orderOutputs.to(Serdes.Long(), orderSerdes, OUTPUT);

        inventory.transform(StockTransferTransformer::new, stockReservationStore)
                .filter((key, value) -> value > 0);

        return new KafkaStreams(builder, streamsConfig(bootstrapServers, stateDir));

    }

    private static SpecificAvroSerde<Order> orderSerdes(String schemaRegistryUrl) {
        final SpecificAvroSerde<Order> orderSerde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig =
                Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        schemaRegistryUrl);
        orderSerde.configure(serdeConfig, false);
        return orderSerde;
    }

    private static Properties streamsConfig(String bootstrapServers, String stateDir) {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, ORDERS_SERVICE_APP_ID);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);
        // Workaround for a known issue with RocksDB in environments where you have only 1 cpu core.
        config.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, CustomRocksDBConfig.class);
        return config;
    }

    private static class StockTransferTransformer implements Transformer<String, Integer, KeyValue<String, Integer>> {
        private KeyValueStore<String, Long> stockCountStore;

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context) {
            stockCountStore = (KeyValueStore<String, Long>) context.getStateStore(stockReservationStore);
        }

        @Override
        public KeyValue<String, Integer> transform(final String product, final Integer stockCount) {
            Long existingStockCount = stockCountStore.get(product);
            if (existingStockCount == null) existingStockCount = 0L;
            stockCountStore.put(product, existingStockCount + stockCount);
            System.out.println("StockUpdate " + product + " stock count is now: " + stockCountStore.get(product));
            return KeyValue.pair(product, stockCount);
        }

        @Override
        public KeyValue<String, Integer> punctuate(long timestamp) {
            return null;
        }

        @Override
        public void close() {
        }
    }

    private static class StockCheckTransformer implements Transformer<Long, Order, KeyValue<Long, Order>> {
        private KeyValueStore<String, Long> stockCountStore;

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context) {
            stockCountStore = (KeyValueStore<String, Long>) context.getStateStore(stockReservationStore);

        }

        @Override
        public KeyValue<Long, Order> transform(final Long orderId, final Order order) {
            Order returnValue;

            Long stockCount = stockCountStore.get(order.getProduct());
            if (stockCount == null) stockCount = 0L;

            if (stockCount - order.getQuantity() >= 0) {
                stockCountStore.put(order.getProduct(), stockCount - order.getQuantity());
                System.out.println("Validating order " + order.getId() + " for product " + order.getProduct() + " as stock count is " + stockCount + " and order amount is " + order.getQuantity());
                returnValue = new Order(order.getId(), "Validated", order.getProduct(), order.getQuantity());
            } else {
                System.out.println("Rejecting order " + order.getId() + " for product " + order.getProduct() + " as stock count is " + stockCount + " and order amount is " + order.getQuantity());
                returnValue = new Order(order.getId(), "Rejected", order.getProduct(), order.getQuantity());
            }

            return KeyValue.pair(orderId, returnValue);
        }

        @Override
        public KeyValue<Long, Order> punctuate(long timestamp) {
            return null;
        }

        @Override
        public void close() {
        }
    }
}
