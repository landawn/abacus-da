package com.landawn.abacus.da.neo4j.model;

import java.util.Date;
import java.util.List;
import java.util.Objects;

import org.neo4j.ogm.annotation.GraphId;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Relationship;

@NodeEntity(label = "Order")
public class Order {
    @GraphId
    private Long id;
    @Relationship(type = "CONTAINS", direction = Relationship.OUTGOING)
    private List<OrderItem> orderItemList;
    private double amount;
    @Relationship(type = "HAS", direction = Relationship.OUTGOING)
    private Address shippingAddress;
    private Date date;

    /**
     * 
     *
     * @return 
     */
    public Long getId() {
        return id;
    }

    /**
     * 
     *
     * @param id 
     * @return 
     */
    public Order setId(Long id) {
        this.id = id;
        return this;
    }

    /**
     * 
     *
     * @return 
     */
    public List<OrderItem> getOrderItemList() {
        return orderItemList;
    }

    /**
     * 
     *
     * @param orderItemList 
     * @return 
     */
    public Order setOrderItemList(List<OrderItem> orderItemList) {
        this.orderItemList = orderItemList;
        return this;
    }

    /**
     * 
     *
     * @return 
     */
    public double getAmount() {
        return amount;
    }

    /**
     * 
     *
     * @param amount 
     * @return 
     */
    public Order setAmount(double amount) {
        this.amount = amount;
        return this;
    }

    /**
     * 
     *
     * @return 
     */
    public Address getShippingAddress() {
        return shippingAddress;
    }

    /**
     * 
     *
     * @param shippingAddress 
     * @return 
     */
    public Order setShippingAddress(Address shippingAddress) {
        this.shippingAddress = shippingAddress;
        return this;
    }

    /**
     * 
     *
     * @return 
     */
    public Date getDate() {
        return date;
    }

    /**
     * 
     *
     * @param date 
     * @return 
     */
    public Order setDate(Date date) {
        this.date = date;
        return this;
    }

    /**
     * 
     *
     * @return 
     */
    @Override
    public int hashCode() {
        return Objects.hash(amount, date, id, orderItemList, shippingAddress);
    }

    /**
     * 
     *
     * @param obj 
     * @return 
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Order other = (Order) obj;
        if (Double.doubleToLongBits(amount) != Double.doubleToLongBits(other.amount)) {
            return false;
        }
        if (!Objects.equals(date, other.date)) {
            return false;
        }
        if (!Objects.equals(id, other.id)) {
            return false;
        }
        if (!Objects.equals(orderItemList, other.orderItemList)) {
            return false;
        }
        if (!Objects.equals(shippingAddress, other.shippingAddress)) {
            return false;
        }
        return true;
    }

    /**
     * 
     *
     * @return 
     */
    @Override
    public String toString() {
        return "{id=" + id + ", orderItemList=" + orderItemList + ", amount=" + amount + ", shippingAddress=" + shippingAddress + ", date=" + date + "}";
    }

}
