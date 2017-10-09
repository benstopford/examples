package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.ProductType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.Options;

import java.util.Map;
import java.util.Properties;

class MicroserviceUtils {
    public static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081";

    static String parseArgs(String[] args) {
        if (args.length > 2) {
            throw new IllegalArgumentException("usage: ... " +
                    "[<bootstrap.servers> (optional, default: " + DEFAULT_BOOTSTRAP_SERVERS + ")] " +
                    "[<schema.registry.url> (optional, default: " + DEFAULT_SCHEMA_REGISTRY_URL + ")] ");
        }
        final String bootstrapServers = args.length > 1 ? args[1] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 2 ? args[2] : "http://localhost:8081";

        System.out.println("Connecting to Kafka cluster via bootstrap servers " + bootstrapServers);
        System.out.println("Connecting to Confluent schema registry at " + schemaRegistryUrl);
        Schemas.configureSerdesWithSchemaRegistryUrl(schemaRegistryUrl);
        return bootstrapServers;
    }

    static Properties streamsConfig(String bootstrapServers, String stateDir, String appId) {
        Properties config = new Properties();
        // Workaround for a known issue with RocksDB in environments where you have only 1 cpu core.
        config.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, CustomRocksDBConfig.class);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);
        return config;
    }

    public static class CustomRocksDBConfig implements RocksDBConfigSetter {

        @Override
        public void setConfig(final String storeName, final Options options, final Map<String, Object> configs) {
            // Workaround: We must ensure that the parallelism is set to >= 2.  There seems to be a known
            // issue with RocksDB where explicitly setting the parallelism to 1 causes issues (even though
            // 1 seems to be RocksDB's default for this configuration).
            int compactionParallelism = Math.max(Runtime.getRuntime().availableProcessors(), 2);
            // Set number of compaction threads (but not flush threads).
            options.setIncreaseParallelism(compactionParallelism);
        }
    }

    //TODO - how do I serialise an java Enum in streams without writing a serialiser myself?
    public static final class ProductTypeSerde implements Serde<ProductType> {

        @Override
        public void configure(Map<String, ?> map, boolean b) {
        }

        @Override
        public void close() {
        }

        @Override
        public Serializer<ProductType> serializer() {
            return new Serializer<ProductType>() {
                @Override
                public void configure(Map<String, ?> map, boolean b) {
                }

                @Override
                public byte[] serialize(String topic, ProductType pt) {
                    return pt.toString().getBytes();
                }

                @Override
                public void close() {
                }
            };
        }

        @Override
        public Deserializer<ProductType> deserializer() {
            return new Deserializer<ProductType>() {
                @Override
                public void configure(Map<String, ?> map, boolean b) {
                }

                @Override
                public ProductType deserialize(String topic, byte[] bytes) {
                    return ProductType.valueOf(new String(bytes));
                }

                @Override
                public void close() {
                }
            };
        }
    }
}
