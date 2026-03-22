package com.landawn.abacus.da.neo4j.model;

import java.util.Objects;

import org.neo4j.ogm.annotation.GraphId;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Relationship;

@NodeEntity(label = "OrderItem")
public class OrderItem {
    @GraphId
    private Long id;
    @Relationship(type = "OF", direction = Relationship.OUTGOING)
    private Product product;
    private int quantity;
    private double amount;

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
    public OrderItem setId(Long id) {
        this.id = id;
        return this;
    }

    /**
     * 
     *
     * @return 
     */
    public Product getProduct() {
        return product;
    }

    /**
     * 
     *
     * @param product 
     * @return 
     */
    public OrderItem setProduct(Product product) {
        this.product = product;
        return this;
    }

    /**
     * 
     *
     * @return 
     */
    public int getQuantity() {
        return quantity;
    }

    /**
     * 
     *
     * @param quantity 
     * @return 
     */
    public OrderItem setQuantity(int quantity) {
        this.quantity = quantity;
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
    public OrderItem setAmount(double amount) {
        this.amount = amount;
        return this;
    }

    /**
     * 
     *
     * @return 
     */
    @Override
    public int hashCode() {
        return Objects.hash(amount, id, product, quantity);
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
        OrderItem other = (OrderItem) obj;
        if (Double.doubleToLongBits(amount) != Double.doubleToLongBits(other.amount)) {
            return false;
        }
        if (!Objects.equals(id, other.id)) {
            return false;
        }
        if (!Objects.equals(product, other.product)) {
            return false;
        }
        if (quantity != other.quantity) {
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
        return "{id=" + id + ", product=" + product + ", quantity=" + quantity + ", amount=" + amount + "}";
    }
}
