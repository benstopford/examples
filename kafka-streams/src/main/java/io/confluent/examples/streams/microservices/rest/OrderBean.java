package io.confluent.examples.streams.microservices.rest;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderType;
import io.confluent.examples.streams.avro.microservices.ProductType;

public class OrderBean {
    private long id;
    private long customerId;
    private OrderType state;
    private ProductType product;
    private int quantity;
    private double price;

    public OrderBean() {

    }

    public OrderBean(long id, long customerId, OrderType state, ProductType product, int quantity, double price) {
        this.id = id;
        this.customerId = customerId;
        this.state = state;
        this.product = product;
        this.quantity = quantity;
        this.price = price;
    }

    public long getId() {
        return id;
    }

    public long getCustomerId() {
        return customerId;
    }

    public OrderType getState() {
        return state;
    }

    public ProductType getProduct() {
        return product;
    }

    public int getQuantity() {
        return quantity;
    }

    public double getPrice() {
        return price;
    }

    public void setId(long id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OrderBean orderBean = (OrderBean) o;

        if (id != orderBean.id) return false;
        if (customerId != orderBean.customerId) return false;
        if (quantity != orderBean.quantity) return false;
        if (Double.compare(orderBean.price, price) != 0) return false;
        if (state != orderBean.state) return false;
        return product == orderBean.product;

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = (int) (id ^ (id >>> 32));
        result = 31 * result + (int) (customerId ^ (customerId >>> 32));
        result = 31 * result + (state != null ? state.hashCode() : 0);
        result = 31 * result + (product != null ? product.hashCode() : 0);
        result = 31 * result + quantity;
        temp = Double.doubleToLongBits(price);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "id=" + id +
                ", customerId=" + customerId +
                ", state=" + state +
                ", product=" + product +
                ", quantity=" + quantity +
                ", price=" + price +
                '}';
    }

    public static OrderBean toBean(Order order) {
        return new OrderBean(order.getId(),
                order.getCustomerId(),
                order.getState(),
                order.getProduct(),
                order.getQuantity(),
                order.getPrice());
    }

    public static Order fromBean(OrderBean order) {
        return new Order(order.getId(),
                order.getCustomerId(),
                order.getState(),
                order.getProduct(),
                order.getQuantity(),
                order.getPrice());
    }
}
