package io.confluent.examples.streams.microservices.orders;

import io.confluent.examples.streams.microservices.Schemas;
import io.confluent.examples.streams.microservices.Service;
import io.confluent.examples.streams.microservices.orders.command.OrderCommandSubService;
import io.confluent.examples.streams.microservices.orders.query.OrderQuerySubService;
import io.confluent.examples.streams.microservices.orders.rest.OrdersRestInterface;
import io.confluent.examples.streams.microservices.orders.validation.OrderValidationSubService;
import org.apache.kafka.streams.state.HostInfo;

import java.io.IOException;
import java.net.ServerSocket;

public class OrdersService implements Service {

    private OrderValidationSubService validation;
    private OrderQuerySubService queries;
    private OrderCommandSubService commands;
    private String host;
    private int restPort;
    private OrdersRestInterface restInterface;

    public OrdersService(String host, int restPort) {
        this.host = host;
        this.restPort = restPort;
    }

    @Override
    public void start(String bootstrapServers) {
        try {
            validation = new OrderValidationSubService();
            validation.start(bootstrapServers);

            commands = new OrderCommandSubService();
            commands.start(bootstrapServers);

            queries = new OrderQuerySubService();
            queries.start(bootstrapServers);

            restInterface = new OrdersRestInterface(new HostInfo(host, restPort), commands, queries);
            restInterface.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void stop() {
        try {
            if (validation != null)
                validation.stop();
            if (restInterface != null)
                restInterface.stop();
            if (commands != null)
                commands.stop();
            if (queries != null)
                queries.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {

        final String bootstrapServers = args.length > 1 ? args[1] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 2 ? args[2] : "http://localhost:8081";
        final String restHostname = args.length > 3 ? args[3] : "localhost";
        final String restPort = args.length > 4 ? args[4] : Integer.toString(randomFreeLocalPort());

        Schemas.configureSerdesWithSchemaRegistryUrl(schemaRegistryUrl);
        OrdersService service = new OrdersService(restHostname, Integer.valueOf(restPort));
        service.start(bootstrapServers);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                service.stop();
            } catch (Exception ignored) {
            }
        }));
    }

    public static int randomFreeLocalPort() throws IOException {
        ServerSocket s = new ServerSocket(0);
        int port = s.getLocalPort();
        s.close();
        return port;
    }
}
