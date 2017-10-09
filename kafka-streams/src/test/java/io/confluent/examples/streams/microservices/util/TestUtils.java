package io.confluent.examples.streams.microservices.util;

import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import io.confluent.examples.streams.microservices.Schemas;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KeyValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.util.Collections.singletonList;

public class TestUtils {
    protected Properties producerConfig(EmbeddedSingleNodeKafkaCluster cluster) {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        return producerConfig;
    }

    //TODO remove inventory specific method, do we really need this?
    protected Properties inventoryConsumerProperties(EmbeddedSingleNodeKafkaCluster cluster) {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "inventory-test-reader");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        return consumerConfig;
    }


    public static List<OrderValidation> readOrderValidations(int numberToRead, String bootstrapServers) throws InterruptedException {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "inventory-orders-test-reader");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KafkaConsumer<Long, OrderValidation> consumer = new KafkaConsumer(consumerConfig,
                Schemas.Topics.ORDER_VALIDATIONS.keySerde().deserializer(),
                Schemas.Topics.ORDER_VALIDATIONS.valueSerde().deserializer());
        consumer.subscribe(singletonList(Schemas.Topics.ORDER_VALIDATIONS.name()));

        List<OrderValidation> actualValues = new ArrayList<>();
        org.apache.kafka.test.TestUtils.waitForCondition(() -> {
            ConsumerRecords<Long, OrderValidation> records = consumer.poll(100);
            for (ConsumerRecord<Long, OrderValidation> record : records) {
                actualValues.add(record.value());
            }
            return actualValues.size() == numberToRead;
        }, 20000, "Timed out reading orders.");
        consumer.close();
        return actualValues;
    }

    public static Properties propsWith(KeyValue... props){
        Properties properties = new Properties();
        for(KeyValue kv: props){
            properties.put(kv.key, kv.value);
        }
        return properties;
    }
}
