package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.ProductType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.Options;

import java.util.Map;

class MicroserviceUtils {

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
