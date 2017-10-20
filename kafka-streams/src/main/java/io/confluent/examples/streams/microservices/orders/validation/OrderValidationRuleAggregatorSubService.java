package io.confluent.examples.streams.microservices.orders.validation;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderType;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.microservices.Service;
import io.confluent.examples.streams.microservices.orders.beans.OrderId;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;

import static io.confluent.examples.streams.avro.microservices.Order.newBuilder;
import static io.confluent.examples.streams.avro.microservices.OrderType.VALIDATED;
import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.FAIL;
import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.PASS;
import static io.confluent.examples.streams.microservices.Schemas.Topics.ORDERS;
import static io.confluent.examples.streams.microservices.Schemas.Topics.ORDER_VALIDATIONS;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.baseStreamsConfig;

public class OrderValidationRuleAggregatorSubService implements Service {
    public static final String ORDERS_SERVICE_APP_ID = "orders-service";
    private KafkaStreams streams;

    @Override
    public void start(String bootstrapServers) {
        streams = aggregateOrderValidations(bootstrapServers, "/tmp/kafka-streams");
        streams.cleanUp(); //don't do this in prod as it clears your state stores
        streams.start();
        System.out.println("Started Service " + getClass().getSimpleName());
    }

    private KafkaStreams aggregateOrderValidations(String bootstrapServers, String stateDir) {
        final int numberOfRules = 3; //TODO put into a ktable

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, OrderValidation> validations = builder.stream(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde(), ORDER_VALIDATIONS.name());
        KStream<String, Order> orders = builder.stream(ORDERS.keySerde(), ORDERS.valueSerde(), ORDERS.name())
                .filter((id, order) -> OrderType.CREATED.equals(order.getState()));

        //If all rules pass then validate the order
        validations.groupByKey(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde())
                .aggregate(
                        () -> 0L,
                        (id, result, total) -> PASS.equals(result.getValidationResult()) ? total + 1 : total,
                        TimeWindows.of(30 * 60 * 1000L), //TODO if the window is on the epoch then this is going to fail periodically unless it is sliding. is it sliding?
                        Serdes.Long()
                )
                //get rid of window
                .toStream((windowedKey, total) -> windowedKey.key())
                //filter where all rules passed
                .filter((k, total) -> total >= numberOfRules)
                //Join back to orders and output
                .join(orders, (id, order) -> newBuilder(order).setState(VALIDATED)
                        .setId(OrderId.next(order.getId()))
                        .build(), JoinWindows.of(3000 * 1000L), ORDERS.keySerde(), Serdes.Long(), ORDERS.valueSerde())
                .selectKey((k, v) -> v.getId())
                .to(ORDERS.keySerde(), ORDERS.valueSerde(), ORDERS.name());

        //If any rule fails then fail the order
        validations.filter((orderId, rule) -> FAIL.equals(rule.getValidationResult()))
                .join(orders, (aLong, order) ->
                                newBuilder(order)
                                        .setId(OrderId.next(order.getId()))
                                        .setState(OrderType.FAILED).build(),
                        JoinWindows.of(3000 * 1000L), ORDERS.keySerde(), ORDER_VALIDATIONS.valueSerde(), ORDERS.valueSerde())
                .groupByKey(ORDERS.keySerde(), ORDERS.valueSerde())
                .reduce((order, v1) -> order)
                .toStream().selectKey((k, v) -> v.getId())
                .to(ORDERS.keySerde(), ORDERS.valueSerde(), ORDERS.name());

        return new KafkaStreams(builder, baseStreamsConfig(bootstrapServers, stateDir, ORDERS_SERVICE_APP_ID));
    }

    @Override
    public void stop() {
        if (streams != null) streams.close();
    }
}
