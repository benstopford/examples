package io.confluent.examples.streams.microservices.orders.beans;

public class OrderId {
    public static String id(long id, int version) {
        return "Order:" + id + ":" + version;
    }

    public static String id(long id) {
        return "Order:" + id + ":" + 0;
    }

    public static String next(String orderId) {
        String[] split = orderId.split(":");
        int version = Integer.valueOf(split[2]);
        return id(Long.valueOf(split[1]), version + 1);
    }
}