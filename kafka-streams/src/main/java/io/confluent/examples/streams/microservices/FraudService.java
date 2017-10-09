package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.*;

import static io.confluent.examples.streams.microservices.MicroserviceUtils.parseArgs;
import static io.confluent.examples.streams.microservices.MicroserviceUtils.streamsConfig;
import static io.confluent.examples.streams.microservices.Schemas.Topics;

public class FraudService {
    public static final String FRAUD_SERVICE_APP_ID = "fraud-service";
    public static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081";
    public static final int FRAUD_LIMIT = 2000;
    private KafkaStreams streams;


    KafkaStreams startService(String bootstrapServers) {
        streams = processOrders(bootstrapServers, "/tmp/kafka-streams");
        streams.cleanUp(); //don't do this in prod as it clears your state stores
        streams.start();
        return streams;
    }

    private KafkaStreams processOrders(final String bootstrapServers,
                                       final String stateDir) {


        //Latch onto instances of the orders and inventory topics
        KStreamBuilder builder = new KStreamBuilder();
        KStream<Long, Order> orders = builder.stream(Topics.ORDERS.keySerde(), Topics.ORDERS.valueSerde(), Topics.ORDERS.name());

        //The following steps could be written as a single statement but we split each step out for clarity

        //Create a lookup table for the total value of orders in a window
        KTable<Windowed<Long>, Double> totalsByCustomerTable = orders
                .filter((id, order) -> OrderType.CREATED.equals(order.getState()))
                .groupBy((id, order) -> order.getCustomerId(), Topics.ORDERS.keySerde(), Topics.ORDERS.valueSerde())
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
                        , JoinWindows.of(3000 * 1000L), Topics.ORDERS.keySerde(), Topics.ORDERS.valueSerde(), Serdes.Double());

        //Branch anything over $2000 as a fraud check Fail
        orderAndAmountByCust.branch((id, orderValue) -> orderValue.getValue() >= FRAUD_LIMIT)[0]
                .mapValues((orderValue) -> new OrderValidation(orderValue.getOrder().getId(), OrderValidationType.FRAUD_CHECK, OrderValidationResult.FAIL))
                .to(Topics.ORDER_VALIDATIONS.keySerde(), Topics.ORDER_VALIDATIONS.valueSerde(), Topics.ORDER_VALIDATIONS.name());

        //Branch anything under $2000 as a fraud check Pass
        orderAndAmountByCust.branch((id, orderValue) -> orderValue.getValue() < FRAUD_LIMIT)[0]
                .mapValues((orderValue) -> new OrderValidation(orderValue.getOrder().getId(), OrderValidationType.FRAUD_CHECK, OrderValidationResult.PASS))
                .to(Topics.ORDER_VALIDATIONS.keySerde(), Topics.ORDER_VALIDATIONS.valueSerde(), Topics.ORDER_VALIDATIONS.name());

        return new KafkaStreams(builder, streamsConfig(bootstrapServers, stateDir, FRAUD_SERVICE_APP_ID));
    }


    public static void main(String[] args) throws Exception {
        final String bootstrapServers = parseArgs(args);
        FraudService service = new FraudService();
        service.startService(bootstrapServers);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                service.stop();
            } catch (Exception ignored) {
            }
        }));
    }

    public void stop() {
        if(streams != null) streams.close();
    }
}