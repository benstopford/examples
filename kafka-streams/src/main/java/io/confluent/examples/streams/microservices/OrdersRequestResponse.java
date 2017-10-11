package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;

public interface OrdersRequestResponse {
    Order getOrder(Long id);

    boolean putOrderAndWait(Order order);
}