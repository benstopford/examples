package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderType;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.avro.microservices.ProductType;
import io.confluent.examples.streams.microservices.Schemas.Topics;
import io.confluent.examples.streams.microservices.util.MicroserviceUtils;
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

public class InventoryService {
    public static final String INVENTORY_SERVICE_APP_ID = "inventory-service";
    public static final String RESERVED_STOCK_STORE_NAME = "store-of-reserved-stock";
    private KafkaStreams streams;


    //TODO next we need to decrement the reservation and the inventory when the order completes.
    //TODO orders could have multiple products, need order items in model
    //TODO should probablly have timestamps on all objects, validFrom, validTo (snapshots need clock for this?)

    //Next add validation that asserts inventory.warehouselocation.countrycode == order.deliveraddress.countrycode

    void startService(String bootstrapServers) {
        streams = processOrders(bootstrapServers, "/tmp/kafka-streams");
        streams.cleanUp(); //don't do this in prod as it clears your state stores
        streams.start();
    }

    private KafkaStreams processOrders(final String bootstrapServers,
                                       final String stateDir) {

        //Latch onto instances of the orders and inventory topics
        KStreamBuilder builder = new KStreamBuilder();
        KStream<Long, Order> orders = builder.stream(Topics.ORDERS.keySerde(), Topics.ORDERS.valueSerde(), Topics.ORDERS.name());
        KTable<ProductType, Integer> warehouseInventory = builder.table(Topics.WAREHOUSE_INVENTORY.keySerde(), Topics.WAREHOUSE_INVENTORY.valueSerde(), Topics.WAREHOUSE_INVENTORY.name());


        //Create a store to reserve inventory whilst the order is processed.
        //This will be prepopulated from Kafka before the service starts processing
        StateStoreSupplier reservedStock = Stores.create(RESERVED_STOCK_STORE_NAME)
                .withKeys(Topics.WAREHOUSE_INVENTORY.keySerde()).withValues(org.apache.kafka.common.serialization.Serdes.Long())
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
                .join(warehouseInventory, KeyValue::new, Topics.WAREHOUSE_INVENTORY.keySerde(), Topics.ORDERS.valueSerde());

        //Validate the order based on how much stock we have both in the warehouse
        // inventory and the local store of reserved orders
        KStream<Long, OrderValidation> validatedOrders = ordersWithInventory
                .transform(InventoryValidator::new, RESERVED_STOCK_STORE_NAME);

        //Push the result into the Order Validations topic
        validatedOrders.to(Topics.ORDER_VALIDATIONS.keySerde(), Topics.ORDER_VALIDATIONS.valueSerde(), Topics.ORDER_VALIDATIONS.name());

        return new KafkaStreams(builder, MicroserviceUtils.streamsConfig(bootstrapServers, stateDir, INVENTORY_SERVICE_APP_ID));
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

        @Override
        public KeyValue<Long, OrderValidation> punctuate(long timestamp) {
            return null;
        }

        @Override
        public void close() {
        }
    }

    public static void main(String[] args) throws Exception {
        InventoryService service = new InventoryService();
        service.startService(MicroserviceUtils.initSchemaRegistryAndGetBootstrapServers(args));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                service.close();
            } catch (Exception ignored) {
            }
        }));
    }

    private void close() {
        streams.close();
    }
}