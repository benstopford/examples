package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.avro.microservices.ProductType;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.confluent.examples.streams.microservices.MicroserviceUtils.*;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class Schemas {
    private static final Map<String, Topic> allTopics = new HashMap<>();

    public static class Topic<T, V> {
        private String name;
        private Serde<T> keySerde;
        private Serde<V> valueSerde;

        public Topic(String name, Serde<T> keySerde, Serde<V> valueSerde) {
            this.name = name;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
            allTopics.put(name, this);
        }

        public Serde<T> keySerde() {
            return keySerde;
        }

        public Serde<V> valueSerde() {
            return valueSerde;
        }

        public String name() {
            return name;
        }

        public String toString() {
            return name;
        }

    }

    public static class Topics {
        public static Topic<Long, Order> ORDERS;
        public static Topic<ProductType, Integer> WAREHOUSE_INVENTORY;
        public static Topic<Long, OrderValidation> ORDER_VALIDATIONS;
        static {
            createTopics();
        }

        private static void createTopics() {
            ORDERS = new Topic("orders", Serdes.Long(), new SpecificAvroSerde<Order>());
            ORDER_VALIDATIONS = new Topic("order-validations", Serdes.Long(), new SpecificAvroSerde<OrderValidation>());
            WAREHOUSE_INVENTORY = new Topic("warehouse-inventory", new ProductTypeSerde(), Serdes.Integer());
        }
    }

    public static void configureSerdesWithSchemaRegistryUrl(String url) {
        Topics.createTopics(); //wipe cached schema registry
        for (Topic topic : allTopics.values()) {
            configure(topic.keySerde(), url);
            configure(topic.valueSerde(), url);
        }
    }

    private static void configure(Serde serde, String url) {
        if (serde instanceof SpecificAvroSerde) {
            serde.configure(Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, url), false);
        }
    }
}
