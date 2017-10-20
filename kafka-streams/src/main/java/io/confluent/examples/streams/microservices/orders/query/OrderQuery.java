package io.confluent.examples.streams.microservices.orders.query;

import io.confluent.examples.streams.interactivequeries.HostStoreInfo;

import javax.ws.rs.container.AsyncResponse;

public interface OrderQuery {
    HostStoreInfo
    getHostForOrderId(String orderId);

    void getOrder(String id, AsyncResponse asyncResponse);
}
