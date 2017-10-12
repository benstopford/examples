package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.ProductType;
import io.confluent.examples.streams.microservices.Schemas.Topics;
import io.confluent.examples.streams.microservices.util.TestUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.confluent.examples.streams.avro.microservices.OrderType.CREATED;
import static io.confluent.examples.streams.avro.microservices.OrderType.FAILED;
import static io.confluent.examples.streams.avro.microservices.ProductType.JUMPERS;
import static io.confluent.examples.streams.avro.microservices.ProductType.UNDERPANTS;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class TheTest extends TestUtils {
    private List<Service> services = new ArrayList<>();

    //TODO really all our order messages should have a incrementing version id


    @Before
    public void startEverythingElse() throws Exception {
        if (!CLUSTER.isRunning())
            CLUSTER.start();

        Topics.ALL.keySet().stream().forEach(name -> CLUSTER.createTopic(name));
        Schemas.configureSerdesWithSchemaRegistryUrl(CLUSTER.schemaRegistryUrl());

        services.add(new FraudService());
        services.add(new InventoryService());
        services.add(new OrderDetailsValidationService());
        services.add(new OrdersService());

        TestUtils.tailAllTopicsToConsole(CLUSTER.bootstrapServers());
    }

    @After
    public void tearDown() {
        services.stream().forEach(s -> s.stop());
        TestUtils.stopTailers();
        CLUSTER.stop();
    }

    //TODO test latency of processing for records when in a bactch of say 200 and end to end latency may stretch.

    @Test
    public void shouldProcessManyOrdersAcrossAllServices() throws InterruptedException {
        services.stream().forEach(s -> s.start(CLUSTER.bootstrapServers()));
        KafkaProducer<Long, Order> ordersProducer = new KafkaProducer(producerConfig(CLUSTER), Schemas.Topics.ORDERS.keySerde().serializer(), Schemas.Topics.ORDERS.valueSerde().serializer());

        //Prepare data
        List<KeyValue<ProductType, Integer>> inventory = asList(
                new KeyValue<>(UNDERPANTS, 75),
                new KeyValue<>(JUMPERS, 1)
        );
        sendInventory(inventory, Topics.WAREHOUSE_INVENTORY);

        //Set up consumers
        KafkaConsumer<Long, Order> consumer = TestUtils.createConsumer(Topics.ORDERS, CLUSTER.bootstrapServers());

        //First run outside of timing loop.
        Order order = new Order(0L, 0L, CREATED, UNDERPANTS, 3, -5.00d); //should fail details check
        ordersProducer.send(new ProducerRecord(Schemas.Topics.ORDERS.name(), order.getId(), order));

        List<KeyValue<Long, Order>> result = TestUtils.readKeyValues2(2, consumer);
        assertThat(result.stream().map(kv -> kv.value)).isEqualTo(asList(
                new Order(0L, 0L, CREATED, UNDERPANTS, 3, -5.00d),
                new Order(0L, 0L, FAILED, UNDERPANTS, 3, -5.00d)
        ));
        Thread.sleep(1000);

        System.out.println("Starting main run");

        for (int i = 1; i <= 10; i++) {
            long start = System.currentTimeMillis();

            order = new Order(1L * i, 0L, CREATED, UNDERPANTS, 4, -5.00d);
            ordersProducer.send(new ProducerRecord(Schemas.Topics.ORDERS.name(), order.getId(), order));
            result = TestUtils.readKeyValues2(2, consumer);

            System.out.println("Iter " + i + " Took " + (System.currentTimeMillis() - start));
            assertThat(result.stream().map(kv -> kv.value)).isEqualTo(asList(
                    new Order(1L * i, 0L, CREATED, UNDERPANTS, 4, -5.00d),
                    new Order(1L * i, 0L, FAILED, UNDERPANTS, 4, -5.00d)
            ));
        }


        consumer.close();
        ordersProducer.close();

    }
}
