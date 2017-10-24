package io.confluent.examples.streams.microservices.validation;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderType;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.avro.microservices.OrderValue;
import io.confluent.examples.streams.microservices.Schemas;
import io.confluent.examples.streams.microservices.Service;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.FAIL;
import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.PASS;
import static io.confluent.examples.streams.avro.microservices.OrderValidationType.FRAUD_CHECK;
import static io.confluent.examples.streams.microservices.Schemas.Topics.ORDERS;
import static io.confluent.examples.streams.microservices.Schemas.Topics.ORDER_VALIDATIONS;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.baseStreamsConfig;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.initSchemaRegistryAndGetBootstrapServers;


/**
 * This service searches for potentially fraudulent transactions by calculating the total value of orders for a
 * customer within a time period, then checks to see if this is over a configured limit.
 * <p>
 * i.e. if(SUM(order.value, 5Mins) > $5000) GroupBy customer -> Fail(orderId) else Pass(orderId)
 */
public class FraudService implements Service {
    public static final String FRAUD_SERVICE_APP_ID = "fraud-service";
    public static final int FRAUD_LIMIT = 2000;
    private KafkaStreams streams;

    @Override
    public void start(String bootstrapServers) {
        streams = processOrders(bootstrapServers, "/tmp/kafka-streams");
        streams.cleanUp(); //don't do this in prod as it clears your state stores
        streams.start();
        System.out.println("Started Service " + getClass().getSimpleName());
    }


    private KafkaStreams processOrders(final String bootstrapServers,
                                       final String stateDir) {

        //Latch onto instances of the orders and inventory topics
        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, Order> orders = builder.stream(ORDERS.keySerde(), ORDERS.valueSerde(), ORDERS.name())
                .filter((id, order) -> OrderType.CREATED.equals(order.getState()));

        //The following steps could be written as a single statement but we split each step out for clarity

        //Create an aggregate of the total value by customer and hold it with the order.
        //We use a trick to make this work: we disable caching so we get a complete changelog stream.
        KTable<Windowed<Long>, OrderValue> aggregate = orders
                .groupBy((id, order) -> order.getCustomerId(), Serdes.Long(), ORDERS.valueSerde())
                .aggregate(
                        OrderValue::new,
                        (custId, order, orderValue) ->
                                new OrderValue(order, orderValue.getValue() + order.getQuantity() * order.getPrice()),
                        TimeWindows.of(60 * 1000L),
                        Schemas.ORDER_VALUE_SERDE);

        //Ditch the windowing and rekey
        KStream<String, OrderValue> ordersWithTotals = aggregate
                .toStream((windowedKey, orderValue) -> windowedKey.key())
                .selectKey((id, orderValue) -> orderValue.getOrder().getId());

        //Now branch the stream into two, for pass and fail, based on whether the windowed total is over $2000
        KStream<String, OrderValue>[] forks = ordersWithTotals.branch(
                (id, orderValue) -> orderValue.getValue() >= FRAUD_LIMIT,
                (id, orderValue) -> orderValue.getValue() < FRAUD_LIMIT);

        forks[0].mapValues((orderValue) -> new OrderValidation(orderValue.getOrder().getId(), FRAUD_CHECK, FAIL))
                .to(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde(), ORDER_VALIDATIONS.name());

        forks[1].mapValues((pair) -> new OrderValidation(pair.getOrder().getId(), FRAUD_CHECK, PASS))
                .to(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde(), ORDER_VALIDATIONS.name());

        Properties props = baseStreamsConfig(bootstrapServers, stateDir, FRAUD_SERVICE_APP_ID);
        props.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0"); //disable caching to ensure we get a complete aggregate changelog
        return new KafkaStreams(builder, props);
    }

    public static void main(String[] args) throws Exception {
        final String bootstrapServers = initSchemaRegistryAndGetBootstrapServers(args);
        FraudService service = new FraudService();
        service.start(bootstrapServers);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                service.stop();
            } catch (Exception ignored) {
            }
        }));
    }

    @Override
    public void stop() {
        if (streams != null) streams.close();
    }
}