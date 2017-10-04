package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderType;
import io.confluent.examples.streams.avro.microservices.ProductType;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static io.confluent.examples.streams.avro.microservices.OrderType.*;
import static io.confluent.examples.streams.microservices.MicroserviceUtils.*;
import static io.confluent.examples.streams.microservices.MicroserviceUtils.CustomRocksDBConfig;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.*;

public class OrdersService {
    static final String ORDERS = "orders";
    static final String STOCK = "stock";
    static final ProductTypeSerde productTypeSerde = new ProductTypeSerde();
    static String OUTPUT = "output"; //TODO this should go back to the orders topic
    static final String ORDERS_SERVICE_APP_ID = "orders-service";
    private static final String stockReservationStore = "StockCountStore";

    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081";


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
        SpecificAvroSerde<Order> orderSerdes = orderSerde(schemaRegistryUrl);
        KStream<Long, Order> orders = builder.stream(Serdes.Long(), orderSerdes, ORDERS);
        KTable<ProductType, Integer> inventory = builder.table(productTypeSerde, Serdes.Integer(), STOCK);


        //Create a store to reserve inventory whilst the order is processed
        StateStoreSupplier stockReservation = Stores.create(stockReservationStore)
                .withKeys(productTypeSerde)
                .withValues(Serdes.Long())
                .persistent()
                .build();
        builder.addStateStore(stockReservation);


        //Rekey orders by product
        KStream<ProductType, Order> ordersByProduct = orders.selectKey((id, order) -> order.getProduct());

        //join to the published inventory, apply validation (which includes the local tally of reserved items), rekey back and output
        ordersByProduct
                .filter((id, order) -> CREATED.equals(order.getState()))
                .join(inventory, KeyValue::new, productTypeSerde, orderSerdes)
                .transform(OrderValidator::new, stockReservationStore)
                .selectKey((product, order) -> order.getId())
                .to(Serdes.Long(), orderSerdes, ORDERS);

        return new KafkaStreams(builder, streamsConfig(bootstrapServers, stateDir));

    }

    private static class OrderValidator implements Transformer<ProductType, KeyValue<Order, Integer>, KeyValue<ProductType, Order>> {
        private KeyValueStore<ProductType, Long> reservedStocksStore;

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context) {
            reservedStocksStore = (KeyValueStore<ProductType, Long>) context.getStateStore(stockReservationStore);

        }

        @Override
        public KeyValue<ProductType, Order> transform(final ProductType productId, final KeyValue<Order, Integer> orderAndStock) {
            Order validated;
            Order order = orderAndStock.key;
            Integer stockCount = orderAndStock.value;

            Long reserved = reservedStocksStore.get(order.getProduct());
            if (reserved == null) reserved = 0L;

            //If there is enough stock available (in the inventory but not reserved) validate the order
            if (stockCount - reserved - order.getQuantity() >= 0) {
                validated = setState(order, VALIDATED);
                //reserve the stock for this order
                reservedStocksStore.put(order.getProduct(), reserved + order.getQuantity());
            } else {
                validated = setState(order, INSUFFICIENT_STOCK);
            }

            System.out.println("Order set to " + order.getState() + " for product " + order.getProduct() + " as reserved count is " + reserved + " and stock count is " + stockCount + "and order amount is " + order.getQuantity());
            return KeyValue.pair(validated.getProduct(), validated);
        }

        private Order setState(Order order, OrderType state) {
            return new Order(order.getId(), state, order.getProduct(), order.getQuantity());
        }

        @Override
        public KeyValue<ProductType, Order> punctuate(long timestamp) {
            return null;
        }

        @Override
        public void close() {
        }
    }

    private static SpecificAvroSerde<Order> orderSerde(String schemaRegistryUrl) {
        SpecificAvroSerde<Order> orderSerde = new SpecificAvroSerde<>();
        orderSerde.configure(Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl), false);
        return orderSerde;
    }

    private static Properties streamsConfig(String bootstrapServers, String stateDir) {
        Properties config = new Properties();
        // Workaround for a known issue with RocksDB in environments where you have only 1 cpu core.
        config.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, CustomRocksDBConfig.class);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, ORDERS_SERVICE_APP_ID);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);
        return config;
    }

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
}
