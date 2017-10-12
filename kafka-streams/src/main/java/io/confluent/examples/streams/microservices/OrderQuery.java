package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;

public interface OrderQuery {
    Order getOrder(Long id);
}
