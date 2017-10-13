package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;

public interface OrderCommand {
    enum OrderCommandResult {SUCCESS, TIMED_OUT, FAILED_VALIDATION, UNKNOWN_FAILURE}

    OrderCommandResult putOrderAndWait(Order order);
}