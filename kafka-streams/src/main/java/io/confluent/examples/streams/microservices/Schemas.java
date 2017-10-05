package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;

import java.util.Collections;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class Schemas {

    public static class Topics {
        public static final String ORDERS = "orders";
        public static final String ORDER_VALIDATIONS = "order-validations";
        public static final String WAREHOUSE_INVENTORY = "warehouse-inventory";
    }

    public static class SerdeBuilders {
        public static final SchemaBuilder<Order> ORDERS = new SchemaBuilder<>();
        public static final SchemaBuilder<OrderValidation> ORDER_VALIDATIONS = new SchemaBuilder<>();
    }

    public static class SchemaBuilder<T extends SpecificRecord> {
        public SpecificAvroSerde<T> serde(String schemaRegistryUrl){
            SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
            serde.configure(Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl), false);
            return serde;
        }
    }


}
