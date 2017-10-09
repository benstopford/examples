package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderType;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.microservices.util.MicroserviceUtils;
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

public class OrdersService implements Service {
    public static final String ORDERS_SERVICE_APP_ID = "orders-service";
    private KafkaStreams streams;

    @Override
    public void start(String bootstrapServers) {
        streams = processOrders(bootstrapServers, "/tmp/kafka-streams");
        streams.cleanUp(); //don't do this in prod as it clears your state stores
        streams.start();
    }

    //TODO change validationresult.getpassed to something more explicit like validationresult

    private KafkaStreams processOrders(String bootstrapServers, String stateDir) {
        final int numberOfRules = 3; //todo put into a ktable

        KStreamBuilder builder = new KStreamBuilder();
        KStream<Long, OrderValidation> rules = builder.stream(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde(), ORDER_VALIDATIONS.name());
        KStream<Long, Order> orders = builder.stream(ORDERS.keySerde(), ORDERS.valueSerde(), ORDERS.name())
                .filter((id, order) -> OrderType.CREATED.equals(order.getState()));

        //If all rules pass then validate the order
        rules.groupByKey(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde())
                .aggregate(
                        () -> 0L,
                        (id, result, total) -> PASS.equals(result.getPassed()) ? total + 1 : total,
                        TimeWindows.of(60 * 1000L),
                        Serdes.Long()
                )
                .toStream((windowedKey, total) -> windowedKey.key()) //get rid of window
                .filter((k, total) -> total >= numberOfRules) //TODO push this config into a global KTable.
                .join(orders, (id, order) -> newBuilder(order).setState(VALIDATED).build(), JoinWindows.of(3000 * 1000L), ORDERS.keySerde(), Serdes.Long(), ORDERS.valueSerde())
                .to(ORDERS.keySerde(), ORDERS.valueSerde(), ORDERS.name());

        //If any rule fails then fail the order
        rules.filter((orderId, rule) -> FAIL.equals(rule.getPassed()))
                .join(orders, (aLong, order) -> newBuilder(order).setState(OrderType.FAILED).build(), JoinWindows.of(3000 * 1000L), ORDERS.keySerde(), ORDER_VALIDATIONS.valueSerde(), ORDERS.valueSerde())
                .groupByKey(ORDERS.keySerde(), ORDERS.valueSerde())
                .reduce((order, v1) -> order)
                .to(ORDERS.keySerde(), ORDERS.valueSerde(), ORDERS.name());

        return new KafkaStreams(builder, MicroserviceUtils.streamsConfig(bootstrapServers, stateDir, ORDERS_SERVICE_APP_ID));
    }

    @Override
    public void stop() {
        if (streams != null) streams.close();
    }
}
