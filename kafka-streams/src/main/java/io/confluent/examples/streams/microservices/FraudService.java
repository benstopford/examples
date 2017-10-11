package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderType;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.avro.microservices.OrderValue;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.*;

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
 * <p>
 * It turns out this is surprisingly tricky to do. I tried two different methods.
 * <p>
 * processOrdersV1 -> creates a windowed table of the total value of orders for each customer and then refers back to it.
 * This doesn't work for two reason:
 * (a) the table gets updated after the order is processed so you get an incorrect result.
 * (b) The change of key to customer id creates duplicates if there are multiple orders per customer.
 * <p>
 * processOrdersV2 -> tries to do it as a single stream, but orders are lost in the KTable as there is no way to
 * attach directly to the changelog stream and ensure you get every update.
 */
public class FraudService implements Service {
    public static final String FRAUD_SERVICE_APP_ID = "fraud-service";
    public static final int FRAUD_LIMIT = 2000;
    private KafkaStreams streams;

    @Override
    public void start(String bootstrapServers) {
        streams = processOrdersV1(bootstrapServers, "/tmp/kafka-streams");
        streams.cleanUp(); //don't do this in prod as it clears your state stores
        streams.start();
        System.out.println("Started Service " + getClass().getSimpleName());
    }

    /**
     * This version almost works but it creates duplicates.
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
    private KafkaStreams processOrdersV1(final String bootstrapServers,
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
                        (custId, order, total) -> total + order.getQuantity() * order.getPrice(), //TODO tomorrow: add the order id in here as a tuple including the order Id as a double,long tuple
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
                        , JoinWindows.of(3000 * 1000L), Serdes.Long(), Schemas.ORDER_VALUE_SERDE, Serdes.Double()); //todo tomorrow add a filter here that filters out any record that wasn't triggered by the appropriate order id

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

    /**
     * This approach attempts to do the whole thing as a single stream. We calculate the total value and keep it with the
     * order. It solves the duplicates problem. Unfortunately it doesn't work for a different reason: 'replaced' orders
     * (i.e. orders with the same customer id) get removed from the output stream by the aggregate. I can't find any
     * way to preserve them. see FraudServiceTest.shouldValidateWhetherOrderAmountExceedsFraudLimitOverWindow()
     */
    private KafkaStreams processOrdersV2(final String bootstrapServers,
                                         final String stateDir) {

        //Latch onto instances of the orders and inventory topics
        KStreamBuilder builder = new KStreamBuilder();
        KStream<Long, Order> orders = builder.stream(ORDERS.keySerde(), ORDERS.valueSerde(), ORDERS.name())
                .filter((id, order) -> OrderType.CREATED.equals(order.getState()));

        //The following steps could be written as a single statement but we split each step out for clarity

        //Create an aggregate of the total value by customer and hold it with the order.
        KTable<Windowed<Long>, OrderValue> aggregate = orders
                .groupBy((id, order) -> order.getCustomerId(), ORDERS.keySerde(), ORDERS.valueSerde())
                .aggregate(
                        () -> new OrderValue(),
                        (custId, order, orderValue) -> new OrderValue(order, orderValue.getValue() + order.getQuantity() * order.getPrice()),
                        TimeWindows.of(60 * 1000L),
                        Schemas.ORDER_VALUE_SERDE);

        //Ditch the windowing
        KStream<Long, OrderValue> ordersWithTotals = aggregate
                .toStream((windowedKey, orderValue) -> windowedKey.key());

        ordersWithTotals.print("ordersWithTotals");

        //Now branch the stream into two, for pass and fail, based on whether the windowed total is over $2000
        ordersWithTotals.branch((id, orderValue) -> orderValue.getValue() >= FRAUD_LIMIT)[0]
                .mapValues((orderValue) -> new OrderValidation(orderValue.getOrder().getId(), FRAUD_CHECK, FAIL))
                .to(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde(), ORDER_VALIDATIONS.name());

        ordersWithTotals.branch((id, orderValue) -> orderValue.getValue() < FRAUD_LIMIT)[0]
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