package io.confluent.examples.streams.microservices.orders.query;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.interactivequeries.HostStoreInfo;

public interface OrderQuery {
    HostStoreInfo getHostForOrderId(Long orderId);
    Order getOrder(Long id);
}
