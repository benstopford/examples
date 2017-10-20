package io.confluent.examples.streams.microservices.orders.command;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.microservices.Schemas;
import io.confluent.examples.streams.microservices.Service;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class OrderCommandSubService implements OrderCommand, Service {
    private KafkaProducer<String, Order> producer;

    @Override
    public OrderCommandResult putOrder(Order order) {
        try {
            //Send the order synchronously
            producer.send(new ProducerRecord<>(Schemas.Topics.ORDERS.name(), order.getId(), order)).get();
            return OrderCommandResult.SUCCESS;

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return OrderCommandResult.UNKNOWN_FAILURE;
    }

    @Override
    public void start(String bootstrapServers) {
        startProducer(bootstrapServers);
        System.out.println("Started Service " + getClass().getSimpleName());
    }

    private void startProducer(String bootstrapServers) {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");

        producer = new KafkaProducer(producerConfig,
                Schemas.Topics.ORDER_VALIDATIONS.keySerde().serializer(),
                Schemas.Topics.ORDER_VALIDATIONS.valueSerde().serializer());

    }

    @Override
    public void stop() {
        if (producer != null) {
            producer.close();
        }
        System.out.println(getClass().getSimpleName() + " was stopped");
    }

}
