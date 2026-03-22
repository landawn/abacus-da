package com.landawn.abacus.da.neo4j.model;

import java.util.Objects;

import org.neo4j.ogm.annotation.GraphId;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Relationship;

@NodeEntity(label = "Product")
public class Product {
    @GraphId
    private Long id;
    private String name;
    private double price;
    private String category;
    @Relationship(type = "SOLD_BY", direction = Relationship.OUTGOING)
    private Vendor vendor;

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
    public Product setId(Long id) {
        this.id = id;
        return this;
    }

    /**
     * 
     *
     * @return 
     */
    public String getName() {
        return name;
    }

    /**
     * 
     *
     * @param name 
     * @return 
     */
    public Product setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * 
     *
     * @return 
     */
    public double getPrice() {
        return price;
    }

    /**
     * 
     *
     * @param price 
     * @return 
     */
    public Product setPrice(double price) {
        this.price = price;
        return this;
    }

    /**
     * 
     *
     * @return 
     */
    public String getCategory() {
        return category;
    }

    /**
     * 
     *
     * @param category 
     * @return 
     */
    public Product setCategory(String category) {
        this.category = category;
        return this;
    }

    /**
     * 
     *
     * @return 
     */
    public Vendor getVendor() {
        return vendor;
    }

    /**
     * 
     *
     * @param vendor 
     * @return 
     */
    public Product setVendor(Vendor vendor) {
        this.vendor = vendor;
        return this;
    }

    /**
     * 
     *
     * @return 
     */
    @Override
    public int hashCode() {
        return Objects.hash(category, id, name, price, vendor);
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
        Product other = (Product) obj;
        if (!Objects.equals(category, other.category)) {
            return false;
        }
        if (!Objects.equals(id, other.id)) {
            return false;
        }
        if (!Objects.equals(name, other.name)) {
            return false;
        }
        if (Double.doubleToLongBits(price) != Double.doubleToLongBits(other.price)) {
            return false;
        }
        if (!Objects.equals(vendor, other.vendor)) {
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
        return "{id=" + id + ", name=" + name + ", price=" + price + ", category=" + category + ", vendor=" + vendor + "}";
    }
}
