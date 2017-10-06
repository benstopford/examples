package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.*;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.*;

public class FraudService {
    public static final String FRAUD_SERVICE_APP_ID = "fraud-service";
    public static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081";
    public static final int FRAUD_LIMIT = 2000;


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
        SpecificAvroSerde<OrderValidations> orderValidationsSerdes = Schemas.SerdeBuilders.ORDER_VALIDATIONS.serde(schemaRegistryUrl);

        //Latch onto instances of the orders and inventory topics
        KStreamBuilder builder = new KStreamBuilder();
        KStream<Long, Order> orders = builder.stream(Serdes.Long(), orderSerdes, Schemas.Topics.ORDERS);

        //The following steps could be written as a single statement but we split each step out for clarity

        //Create a lookup table for the total value of orders in a window
        KTable<Windowed<Long>, Double> totalsByCustomerTable = orders
                .filter((id, order) -> OrderType.CREATED.equals(order.getState()))
                .groupBy((id, order) -> order.getCustomerId(), Serdes.Long(), orderSerdes)
                .aggregate(
                        () -> 0D,
                        (custId, order, total) -> total + order.getQuantity() * order.getPrice(),
                        TimeWindows.of(60 * 1000L), //TODO - why doesn't it work if we make this big?
                        Serdes.Double(),
                        "total-order-value");

        //Convert to a stream to remove the window from the key
        KStream<Long, Double> totalsByCustomer = totalsByCustomerTable
                .toStream((windowedCustId, total) -> windowedCustId.key());

        //Rekey orders to be by customer id so we can join them to the total-value table
        KStream<Long, Order> ordersByCustId = orders.selectKey((id, order) -> order.getCustomerId());

        //Join the orders to the table to include the total-value
        KStream<Long, OrderValue> orderAndAmountByCust = ordersByCustId
                .join(totalsByCustomer, OrderValue::new
                        , JoinWindows.of(3000 * 1000L), Serdes.Long(), orderSerdes, Serdes.Double());

        //Branch anything over $2000 as a fraud check Fail
        orderAndAmountByCust.branch((id, orderValue) -> orderValue.getValue() >= FRAUD_LIMIT)[0]
                .mapValues((orderValue) -> new OrderValidations(orderValue.getOrder().getId(), OrderValidationType.FRAUD_CHECK, OrderValidationResult.FAIL))
                .to(Serdes.Long(), orderValidationsSerdes, Schemas.Topics.ORDER_VALIDATIONS);

        //Branch anything under $2000 as a fraud check Pass
        orderAndAmountByCust.branch((id, orderValue) -> orderValue.getValue() < FRAUD_LIMIT)[0]
                .mapValues((orderValue) -> new OrderValidations(orderValue.getOrder().getId(), OrderValidationType.FRAUD_CHECK, OrderValidationResult.PASS))
                .to(Serdes.Long(), orderValidationsSerdes, Schemas.Topics.ORDER_VALIDATIONS);

        return new KafkaStreams(builder, MicroserviceUtils.streamsConfig(bootstrapServers, stateDir, FRAUD_SERVICE_APP_ID));
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
        final KafkaStreams streams = new FraudService().startService(bootstrapServers, schemaRegistryUrl);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                streams.close();
            } catch (Exception ignored) {
            }
        }));
    }
}