package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderType;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.avro.microservices.ProductType;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.FAIL;
import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.PASS;
import static io.confluent.examples.streams.avro.microservices.OrderValidationType.INVENTORY_CHECK;
import static io.confluent.examples.streams.microservices.MicroserviceUtils.ProductTypeSerde;

public class InventoryService {
    public static final String INVENTORY_SERVICE_APP_ID = "inventory-service";
    public static final String RESERVED_STOCK_STORE_NAME = "StoreOfReservedStock";


    public static final ProductTypeSerde PRODUCT_TYPE_SERDE = new ProductTypeSerde();
    public static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081";


    //TODO next we need to decrement the reservation and the inventory when the order completes.
    //TODO orders could have multiple products, need order items in model

    KafkaStreams startService(String bootstrapServers, String schemaRegistryUrl) {
        KafkaStreams streams = processOrders(bootstrapServers, schemaRegistryUrl, "/tmp/kafka-streams");
        streams.cleanUp(); //don't do this in prod as it clears your state stores
        streams.start();
        return streams;
    }

    private KafkaStreams processOrders(final String bootstrapServers,
                                       final String schemaRegistryUrl,
                                       final String stateDir) {

        SpecificAvroSerde<Order> orderSerdes = Schemas.SerdeBuilders.ORDERS.serde(schemaRegistryUrl);
        SpecificAvroSerde<OrderValidation> orderValidationsSerdes = Schemas.SerdeBuilders.ORDER_VALIDATIONS.serde(schemaRegistryUrl);

        //Latch onto instances of the orders and inventory topics
        KStreamBuilder builder = new KStreamBuilder();
        KStream<Long, Order> orders = builder.stream(Serdes.Long(), orderSerdes, Schemas.Topics.ORDERS);
        KTable<ProductType, Integer> warehouseInventory = builder.table(PRODUCT_TYPE_SERDE, org.apache.kafka.common.serialization.Serdes.Integer(), Schemas.Topics.WAREHOUSE_INVENTORY);


        //Create a store to reserve inventory whilst the order is processed.
        //This will be prepopulated from Kafka before the service starts processing
        StateStoreSupplier reservedStock = Stores.create(RESERVED_STOCK_STORE_NAME)
                .withKeys(PRODUCT_TYPE_SERDE).withValues(org.apache.kafka.common.serialization.Serdes.Long())
                .persistent().build();
        builder.addStateStore(reservedStock);

        //The following steps could be written as a single statement but we split each step out for clarity

        //Change orders stream to be keyed by Product (so we can join with warehouse inventory)
        KStream<ProductType, Order> ordersByProduct = orders
                .selectKey((id, order) -> order.getProduct());

        //Filter out anything other than newly created orders
        KStream<ProductType, Order> newlyCreatedOrders = ordersByProduct
                .filter((id, order) -> OrderType.CREATED.equals(order.getState()));

        //Join Orders to Inventory so we will be able to compare each order to how much is in stock
        KStream<ProductType, KeyValue<Order, Integer>> ordersWithInventory = newlyCreatedOrders
                .join(warehouseInventory, KeyValue::new, PRODUCT_TYPE_SERDE, orderSerdes);

        //Validate the order based on how much stock we have both in the warehouse
        // inventory and the local store of reserved orders
        KStream<Long, OrderValidation> validatedOrders = ordersWithInventory
                .transform(InventoryValidator::new, RESERVED_STOCK_STORE_NAME);

        //Push the result into the Order Validations topic
        validatedOrders.to(org.apache.kafka.common.serialization.Serdes.Long(), orderValidationsSerdes, Schemas.Topics.ORDER_VALIDATIONS);

        return new KafkaStreams(builder, MicroserviceUtils.streamsConfig(bootstrapServers, stateDir));
    }

    private static class InventoryValidator implements Transformer<ProductType, KeyValue<Order, Integer>, KeyValue<Long, OrderValidation>> {
        private KeyValueStore<ProductType, Long> reservedStocksStore;

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context) {
            reservedStocksStore = (KeyValueStore<ProductType, Long>) context.getStateStore(RESERVED_STOCK_STORE_NAME);
        }

        @Override
        public KeyValue<Long, OrderValidation> transform(final ProductType productId, final KeyValue<Order, Integer> orderAndStock) {
            OrderValidation validated;
            Order order = orderAndStock.key;
            Integer warehouseStockCount = orderAndStock.value;

            Long reserved = reservedStocksStore.get(order.getProduct());
            if (reserved == null) reserved = 0L;

            //If there is enough stock available (considering both warehouse inventory and reserved stock) validate the order
            if (warehouseStockCount - reserved - order.getQuantity() >= 0) {
                reservedStocksStore.put(order.getProduct(), reserved + order.getQuantity());
                validated = new OrderValidation(order.getId(), INVENTORY_CHECK, PASS);
            } else {
                validated = new OrderValidation(order.getId(), INVENTORY_CHECK, FAIL);
            }

            System.out.println("Order set to " + order.getState() + " for product " + order.getProduct() + " as reserved count is " + reserved + " and stock count is " + warehouseStockCount + "and order amount is " + order.getQuantity());
            return KeyValue.pair(validated.getOrderId(), validated);
        }

        private Order setState(Order order, OrderType state) {
            return new Order(order.getId(), state, order.getProduct(), order.getQuantity());
        }

        @Override
        public KeyValue<Long, OrderValidation> punctuate(long timestamp) {
            return null;
        }

        @Override
        public void close() {
        }
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
        final KafkaStreams streams = new InventoryService().startService(bootstrapServers, schemaRegistryUrl);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                streams.close();
            } catch (Exception ignored) {
            }
        }));
    }
}
