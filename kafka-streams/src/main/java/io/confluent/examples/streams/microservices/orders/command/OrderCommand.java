package io.confluent.examples.streams.microservices.orders.command;

import io.confluent.examples.streams.avro.microservices.Order;

public interface OrderCommand {
    enum OrderCommandResult {SUCCESS, UNKNOWN_FAILURE}

    OrderCommandResult putOrder(Order order);
}