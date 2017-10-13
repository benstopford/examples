package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderType;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.avro.microservices.OrderValue;
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
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.initSchemaRegistryAndGetBootstrapServers;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.streamsConfig;


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
        KStream<Long, Order> orders = builder.stream(ORDERS.keySerde(), ORDERS.valueSerde(), ORDERS.name())
                .filter((id, order) -> OrderType.CREATED.equals(order.getState()));

        //The following steps could be written as a single statement but we split each step out for clarity

        //Create an aggregate of the total value by customer and hold it with the order.
        //We use a trick to make this work: we disable caching so we get a complete changelog stream.
        KTable<Windowed<Long>, OrderValue> aggregate = orders
                .groupBy((id, order) -> order.getCustomerId(), ORDERS.keySerde(), ORDERS.valueSerde())
                .aggregate(
                        OrderValue::new,
                        (custId, order, orderValue) ->
                                new OrderValue(order, orderValue.getValue() + order.getQuantity() * order.getPrice()),
                        TimeWindows.of(60 * 1000L),
                        Schemas.ORDER_VALUE_SERDE);

        //Ditch the windowing and rekey
        KStream<Long, OrderValue> ordersWithTotals = aggregate
                .toStream((windowedKey, orderValue) -> windowedKey.key())
                .selectKey((id, orderValue) -> orderValue.getOrder().getId());

        //Now branch the stream into two, for pass and fail, based on whether the windowed total is over $2000
        KStream<Long, OrderValue>[] forks = ordersWithTotals.branch(
                (id, orderValue) -> orderValue.getValue() >= FRAUD_LIMIT,
                (id, orderValue) -> orderValue.getValue() < FRAUD_LIMIT);

        forks[0].mapValues((orderValue) -> new OrderValidation(orderValue.getOrder().getId(), FRAUD_CHECK, FAIL))
                .to(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde(), ORDER_VALIDATIONS.name());

        forks[1].mapValues((pair) -> new OrderValidation(pair.getOrder().getId(), FRAUD_CHECK, PASS))
                .to(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde(), ORDER_VALIDATIONS.name());

        Properties props = streamsConfig(bootstrapServers, stateDir, FRAUD_SERVICE_APP_ID);
        props.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0"); //disable caching to ensure we get a complete aggregate changelog
        return new KafkaStreams(builder, props);
    }

    /**
     * This version almost works but it creates duplicates. Keeping it for posterity :)
     *
     * Consider the test FraudServiceTest.thisCreatesDuplicates(). "Second Shot". After the second order we get 3 PASS's
     * (rather than just one). Each of the two duplicates arise for different reasons.
     * - The first duplicate is actually incorrect. The new order flows through the topology, joins to the not-yet-updated
     * totalByCustomer record (which hasn't updated yet) and outputs an incorrect result (it's still a pass but it's technically incorrect)
     * - The second duplicate comes when the totalsByCustomer is updated. This triggers the join again. What is somewhat
     * surprising is this join creates two outputs, for both new order Pass(order1) and the original Pass(order0). I think what is
     * happening is the join 'side' is switching to trigger  from the totalsByCustomer side, which then matches on
     * all existing orders in the stream with the same customer id. This creates the second duplicate (of order0)
     */
    private KafkaStreams processOrdersBroken(final String bootstrapServers,
                                             final String stateDir) {

        //Latch onto instances of the orders and inventory topics
        KStreamBuilder builder = new KStreamBuilder();
        KStream<Long, Order> orders = builder.stream(ORDERS.keySerde(), ORDERS.valueSerde(), ORDERS.name())
                .filter((id, order) -> OrderType.CREATED.equals(order.getState()));

        //The following steps could be written as a single statement but we split each step out for clarity

        //Create a lookup table for the total value of orders in a window
        KTable<Windowed<Long>, Double> totalsByCustomerTable = orders
                .groupBy((id, order) -> order.getCustomerId(), ORDERS.keySerde(), ORDERS.valueSerde())
                .aggregate(
                        () -> 0D,
                        (custId, order, total) -> total + order.getQuantity() * order.getPrice(),
                        TimeWindows.of(60 * 1000L), //TODO - why doesn't it work if we make this big?
                        Serdes.Double());

        //Convert to a stream to remove the window from the key
        KStream<Long, Double> totalsByCustomer = totalsByCustomerTable
                .toStream((windowedCustId, total) -> windowedCustId.key());

        //Rekey orders to be by customer id so we can join them to the total-value table
        KStream<Long, Order> ordersByCustId = orders.selectKey((id, order) -> order.getCustomerId());

        ordersByCustId.print("ordersByCustId");
        totalsByCustomer.print("totalsByCustomer");

        //Join the orders back to the table of totals
        KStream<Long, OrderValue> orderAndAmount = ordersByCustId  //TODO why does this create duplicates?
                .join(totalsByCustomer, OrderValue::new
                        , JoinWindows.of(3000 * 1000L), Serdes.Long(), Schemas.ORDER_VALUE_SERDE, Serdes.Double());

        orderAndAmount.print("orderAndAmount");

        //Now branch the stream into two, for pass and fail, based on whether the windowed total is over $2000
        orderAndAmount.branch((id, orderValue) -> orderValue.getValue() >= FRAUD_LIMIT)[0]
                .mapValues((orderValue) -> new OrderValidation(orderValue.getOrder().getId(), FRAUD_CHECK, FAIL))
                .to(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde(), ORDER_VALIDATIONS.name());

        orderAndAmount.branch((id, orderValue) -> orderValue.getValue() < FRAUD_LIMIT)[0]
                .mapValues((pair) -> new OrderValidation(pair.getOrder().getId(), FRAUD_CHECK, PASS))
                .to(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde(), ORDER_VALIDATIONS.name());

        return new KafkaStreams(builder, streamsConfig(bootstrapServers, stateDir, FRAUD_SERVICE_APP_ID));
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