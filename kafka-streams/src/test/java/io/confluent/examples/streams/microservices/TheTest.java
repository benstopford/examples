package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.OrderType;
import io.confluent.examples.streams.avro.microservices.ProductType;
import io.confluent.examples.streams.microservices.Schemas.Topics;
import io.confluent.examples.streams.microservices.rest.OrderBean;
import io.confluent.examples.streams.microservices.util.MicroserviceTestUtils;
import org.apache.kafka.streams.KeyValue;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;

import static io.confluent.examples.streams.avro.microservices.ProductType.JUMPERS;
import static io.confluent.examples.streams.avro.microservices.ProductType.UNDERPANTS;
import static java.util.Arrays.asList;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static org.apache.kafka.test.TestUtils.waitForCondition;

public class TheTest extends MicroserviceTestUtils {
    public final String restAddress = "localhost";
    private List<Service> services = new ArrayList<>();
    private static int restPort;
    private OrderBean returnedBean;

    //TODO really all our order messages should have a incrementing version id
    //TODO test latency of processing for records when in a batch of say 200 and end to end latency may stretch.

    @Test
    public void shouldProcessOneOrderEndToEnd() throws Exception {
        final Client client = ClientBuilder.newClient();
        final String baseUrl = "http://localhost:" + restPort + "/orders";

        //Add inventory required by the inventory service
        List<KeyValue<ProductType, Integer>> inventory = asList(
                new KeyValue<>(UNDERPANTS, 75),
                new KeyValue<>(JUMPERS, 10)
        );
        sendInventory(inventory, Topics.WAREHOUSE_INVENTORY);

        //When post order
        OrderBean inputOrder = new OrderBean(1L, 2L, OrderType.CREATED, ProductType.JUMPERS, 1, 1d);
        Response response = client.target(baseUrl + "/post").request(APPLICATION_JSON_TYPE).post(Entity.json(inputOrder));

        //Then check it responds with ok
        AssertionsForClassTypes.assertThat(response.getStatus()).isEqualTo(HttpURLConnection.HTTP_CREATED);

        //Poll until we have an order
        waitForCondition(() -> {
            returnedBean = client.target(baseUrl + "/order/" + inputOrder.getId())
                    .request(APPLICATION_JSON_TYPE)
                    .get(new GenericType<OrderBean>() {
                    });

            return returnedBean != null && OrderType.VALIDATED.equals(returnedBean.getState());
        }, 30 * 1000, "timed out attempting to 'get' validated order order");

        AssertionsForClassTypes.assertThat(returnedBean).isEqualTo(new OrderBean(
                inputOrder.getId(),
                inputOrder.getCustomerId(),
                OrderType.VALIDATED,
                inputOrder.getProduct(),
                inputOrder.getQuantity(),
                inputOrder.getPrice()
        ));

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

        //Send ten orders one after the other
        for (long i = 0; i < 10; i++) {
            long start = System.currentTimeMillis();

            //When post order
            OrderBean inputOrder = new OrderBean(i, 2L, OrderType.CREATED, ProductType.JUMPERS, 1, 1d);
            Response response = client.target(baseUrl + "/post")
                    .request(APPLICATION_JSON_TYPE)
                    .post(Entity.json(inputOrder));

            //Then check it responds with ok
            AssertionsForClassTypes.assertThat(response.getStatus()).isEqualTo(HttpURLConnection.HTTP_CREATED);


            //Poll until we have an order
            waitForCondition(() -> {
                OrderBean returnedBean = client.target(baseUrl + "/order/" + inputOrder.getId())
                        .request(APPLICATION_JSON_TYPE)
                        .get(new GenericType<OrderBean>() {
                        });

                return returnedBean != null && OrderType.VALIDATED.equals(returnedBean.getState());
            }, 30 * 1000, "timed out attempting to 'get' validated order order");

            System.out.println("Took " + (System.currentTimeMillis() - start));

            OrderBean returnedBean = client.target(baseUrl + "/order/" + inputOrder.getId())
                    .request(APPLICATION_JSON_TYPE)
                    .get(new GenericType<OrderBean>() {
                    });

            AssertionsForClassTypes.assertThat(returnedBean).isEqualTo(new OrderBean(
                    i,
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


        for (long i = 0; i < 10; i++) {
            long start = System.currentTimeMillis();

            //When post order
            OrderBean inputOrder = new OrderBean(i, 2L, OrderType.CREATED, ProductType.JUMPERS, 1, 1d);
            Response response = client.target(baseUrl + "/post")
                    .request(APPLICATION_JSON_TYPE)
                    .post(Entity.json(inputOrder));

            //Then check it responds with ok
            AssertionsForClassTypes.assertThat(response.getStatus()).isEqualTo(HttpURLConnection.HTTP_BAD_REQUEST);


            //Poll until we have an order
            waitForCondition(() -> {
                returnedBean = client.target(baseUrl + "/order/" + inputOrder.getId())
                        .request(APPLICATION_JSON_TYPE)
                        .get(new GenericType<OrderBean>() {
                        });

                return returnedBean != null && OrderType.FAILED.equals(returnedBean.getState());
            }, 30 * 1000, "timed out attempting to 'get' validated order order");

            System.out.println("Took " + (System.currentTimeMillis() - start));

            AssertionsForClassTypes.assertThat(returnedBean).isEqualTo(new OrderBean(
                    i,
                    inputOrder.getCustomerId(),
                    OrderType.FAILED,
                    inputOrder.getProduct(),
                    inputOrder.getQuantity(),
                    inputOrder.getPrice()
            ));
        }
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
        services.add(new OrderDetailsValidationService());
        services.add(new OrdersService(restAddress, restPort));

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
