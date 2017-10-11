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


    /**
     * This test fails because the code creates duplicates. The problem is we're torn between two non-ideal situations.
     * Either (a) we make the up table custId->total a KTable. In this case there is a race between creating this table
     * and joining the table back to orders. As the table doesn't trigger we typically don't get any results at all with
     * this method.
     * <p>
     * Alternatively we convert it to a stream. In this case the problem is more subtle. When a customer value is updated
     * it updates the custId->total stream, but if we have multiple orders for the same customer all those orders orders
     * will be triggered, not just the order that created it. So you get duplicate outputs. Since you've lost the context
     * of which order triggered on that side, you can't do anything about it.
     * <p>
     * Probably the best solution is to use the processor API, although this is likely to have similar ordering issues.
     * <p>
     * Ahh. the answer is to compute the aggregate but keep the whole order inside the result. then you have a stream with
     * both the current aggregated count and the value. then you never need to join back to the orders stream!! ha ha.
     */


    private KafkaStreams processOrders(final String bootstrapServers,
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

        //Join the orders to the table to include the total-value
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