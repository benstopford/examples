package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.OrderType;
import io.confluent.examples.streams.avro.microservices.ProductType;
import io.confluent.examples.streams.microservices.Schemas.Topics;
import io.confluent.examples.streams.microservices.util.MicroserviceTestUtils;
import io.confluent.examples.streams.microservices.util.beans.OrderBean;
import io.confluent.examples.streams.microservices.util.beans.OrderId;
import io.confluent.examples.streams.microservices.validation.FraudService;
import io.confluent.examples.streams.microservices.validation.InventoryService;
import io.confluent.examples.streams.microservices.validation.OrderDetailsService;
import io.confluent.examples.streams.microservices.validation.RuleAggregatorService;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.HostInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.confluent.examples.streams.avro.microservices.ProductType.JUMPERS;
import static io.confluent.examples.streams.avro.microservices.ProductType.UNDERPANTS;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.randomFreeLocalPort;
import static io.confluent.examples.streams.microservices.util.beans.OrderId.id;
import static io.confluent.examples.streams.microservices.util.beans.OrderId.next;
import static java.util.Arrays.asList;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class AcceptanceTest extends MicroserviceTestUtils {
    private volatile boolean loadTestRunning = true;
    private volatile int idCounter = 0;
    public final String restAddress = "localhost";
    private List<Service> services = new ArrayList<>();
    private static int restPort;
    private OrderBean returnedBean;

    @Test
    public void shouldCreateNewOrderAndGetBackValidatedOrder() throws Exception {
        final Client client = ClientBuilder.newClient();
        final String baseUrl = "http://localhost:" + restPort + "/orders";

        //Add inventory required by the inventory service with enough items in stock to pass validation
        List<KeyValue<ProductType, Integer>> inventory = asList(
                new KeyValue<>(UNDERPANTS, 75),
                new KeyValue<>(JUMPERS, 10)
        );
        sendInventory(inventory, Topics.WAREHOUSE_INVENTORY);

        //When we POST order
        OrderBean inputOrder = new OrderBean(OrderId.id(1L), 2L, OrderType.CREATED, ProductType.JUMPERS, 1, 1d);
        Response response = client.target(baseUrl + "/post").request(APPLICATION_JSON_TYPE).post(Entity.json(inputOrder));

        //The location passed back in the PUT is the location of the "post-validation" order.
        URI location = response.getLocation();

        //Then check it responds with ok
        assertThat(response.getStatus()).isEqualTo(HttpURLConnection.HTTP_CREATED);

        //Get the order back
        returnedBean = client.target(location)
                .request(APPLICATION_JSON_TYPE)
                .get(new GenericType<OrderBean>() {
                });

        assertThat(returnedBean.getId()).isEqualTo(OrderId.next(inputOrder.getId()));
        assertThat(returnedBean.getState()).isEqualTo(OrderType.VALIDATED);
    }

    @Test
    public void shouldProcessManyValidOrdersEndToEnd() throws Exception {
        final Client client = ClientBuilder.newClient();
        final String baseUrl = "http://localhost:" + restPort + "/orders";

        //Add inventory required by the inventory service
        List<KeyValue<ProductType, Integer>> inventory = asList(
                new KeyValue<>(UNDERPANTS, 75),
                new KeyValue<>(JUMPERS, 10)
        );
        sendInventory(inventory, Topics.WAREHOUSE_INVENTORY);

        //Send ten orders in succession
        for (long i = 0; i < 10; i++) {
            long start = System.currentTimeMillis();

            //When post order
            OrderBean inputOrder = new OrderBean(id(i), 2L, OrderType.CREATED, ProductType.JUMPERS, 1, 1d);
            Response response = client.target(baseUrl + "/post").request(APPLICATION_JSON_TYPE)
                    .post(Entity.json(inputOrder));

            URI location = response.getLocation();

            //Then check it responds with ok
            assertThat(response.getStatus()).isEqualTo(HttpURLConnection.HTTP_CREATED);

            //Get the order back
            returnedBean = client.target(location)
                    .request(APPLICATION_JSON_TYPE)
                    .get(new GenericType<OrderBean>() {
                    });

            System.out.println("Took " + (System.currentTimeMillis() - start));

            assertThat(returnedBean).isEqualTo(new OrderBean(
                    next(id(i)),
                    inputOrder.getCustomerId(),
                    OrderType.VALIDATED,
                    inputOrder.getProduct(),
                    inputOrder.getQuantity(),
                    inputOrder.getPrice()
            ));
        }
    }

    @Test
    public void shouldProcessManyInvalidOrdersEndToEnd() throws Exception {
        //Prepare data
        List<KeyValue<ProductType, Integer>> inventory = asList(
                new KeyValue<>(UNDERPANTS, 75000),
                new KeyValue<>(JUMPERS, 0) //***nothing in stock***
        );
        sendInventory(inventory, Topics.WAREHOUSE_INVENTORY);
        final Client client = ClientBuilder.newClient();
        final String baseUrl = "http://localhost:" + restPort + "/orders";


        //Send ten orders one after the other
        for (long i = 0; i < 10; i++) {
            long start = System.currentTimeMillis();

            //When post order
            OrderBean inputOrder = new OrderBean(id(i), 2L, OrderType.CREATED, ProductType.JUMPERS, 1, 1d);
            Response response = client.target(baseUrl + "/post").request(APPLICATION_JSON_TYPE)
                    .post(Entity.json(inputOrder));

            URI location = response.getLocation();

            //Then check it responds with ok
            assertThat(response.getStatus()).isEqualTo(HttpURLConnection.HTTP_CREATED);

            //Get the order back
            returnedBean = client.target(location)
                    .request(APPLICATION_JSON_TYPE)
                    .get(new GenericType<OrderBean>() {
                    });

            System.out.println("Took " + (System.currentTimeMillis() - start));

            assertThat(returnedBean).isEqualTo(new OrderBean(
                    next(id(i)),
                    inputOrder.getCustomerId(),
                    OrderType.FAILED,
                    inputOrder.getProduct(),
                    inputOrder.getQuantity(),
                    inputOrder.getPrice()
            ));
        }
    }

    @Test
    public void shouldHandleConcurrentRequests() throws Exception {
        final Client client = ClientBuilder.newClient();
        final String baseUrl = "http://localhost:" + restPort + "/orders";

        //Add inventory required by the inventory service
        List<KeyValue<ProductType, Integer>> inventory = asList(
                new KeyValue<>(UNDERPANTS, 75000000),
                new KeyValue<>(JUMPERS, 10000000)
        );
        sendInventory(inventory, Topics.WAREHOUSE_INVENTORY);

        ConcurrentLinkedQueue<Long> queue = new ConcurrentLinkedQueue<>();

        int threadCount = 10;
        ExecutorService executors = Executors.newFixedThreadPool(threadCount);

        //Warm up
        putAndGet(idCounter++, client, baseUrl);
        System.out.println("Starting");

        //Send ten orders one after the other
        for (long j = 0; j < threadCount; j++) {
            executors.execute(() -> {

                while (loadTestRunning) {
                    long start = System.currentTimeMillis();

                    int i = idCounter++;
                    OrderBean order = putAndGet(i, client, baseUrl);

                    long took = System.currentTimeMillis() - start;
                    queue.add(took);
                    System.out.println(i + " Took " + took);

                    assertThat(returnedBean).isEqualTo(new OrderBean(
                            next(id(i)),
                            order.getCustomerId(),
                            OrderType.VALIDATED,
                            order.getProduct(),
                            order.getQuantity(),
                            order.getPrice()
                    ));
                }
            });
        }

        //Run for some fixed time
        Thread.sleep(20 * 1000);
        loadTestRunning = false;
        executors.awaitTermination(5, TimeUnit.SECONDS);

        System.out.println("Number of gets processed in 20 secs " + queue.size());
        Optional<Long> total = queue.stream().reduce((a, b) -> a + b);
        System.out.println("Average duration of a get was " + total.get() / queue.size());
    }

    private OrderBean putAndGet(int i, Client client, String baseUrl) {
        OrderBean order = new OrderBean(id(i), 2L, OrderType.CREATED, ProductType.JUMPERS, 1, 1d);

        Response response = client.target(baseUrl + "/post").request(APPLICATION_JSON_TYPE)
                .post(Entity.json(order));

        returnedBean = client.target(response.getLocation())
                .request(APPLICATION_JSON_TYPE)
                .get(new GenericType<OrderBean>() {
                });
        return order;
    }

    @Before
    public void startEverythingElse() throws Exception {
        if (!CLUSTER.isRunning())
            CLUSTER.start();

        Topics.ALL.keySet().forEach(CLUSTER::createTopic);
        Schemas.configureSerdesWithSchemaRegistryUrl(CLUSTER.schemaRegistryUrl());
        restPort = randomFreeLocalPort();

        services.add(new FraudService());
        services.add(new InventoryService());
        services.add(new OrderDetailsService());
        services.add(new RuleAggregatorService());
        services.add(new OrdersService(new HostInfo(restAddress, restPort)));

        tailAllTopicsToConsole(CLUSTER.bootstrapServers());
        services.forEach(s -> s.start(CLUSTER.bootstrapServers()));
    }

    @After
    public void tearDown() throws Exception {
        services.forEach(Service::stop);
        stopTailers();
        CLUSTER.stop();
    }

}
