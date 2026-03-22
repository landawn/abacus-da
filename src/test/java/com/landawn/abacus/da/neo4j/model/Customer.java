package com.landawn.abacus.da.neo4j.model;

import java.util.List;
import java.util.Objects;

import org.neo4j.ogm.annotation.GraphId;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Relationship;

@NodeEntity(label = "Customer")
public class Customer {
    @GraphId
    private Long id;
    private String firstName;
    private String lastName;

    @Relationship(type = "SHIPPING_TO", direction = Relationship.OUTGOING)
    private Address address;

    @Relationship(type = "OWNS", direction = Relationship.OUTGOING)
    private List<Order> orderList;

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
    public Customer setId(Long id) {
        this.id = id;
        return this;
    }

    /**
     * 
     *
     * @return 
     */
    public String getFirstName() {
        return firstName;
    }

    /**
     * 
     *
     * @param firstName 
     * @return 
     */
    public Customer setFirstName(String firstName) {
        this.firstName = firstName;
        return this;
    }

    /**
     * 
     *
     * @return 
     */
    public String getLastName() {
        return lastName;
    }

    /**
     * 
     *
     * @param lastName 
     * @return 
     */
    public Customer setLastName(String lastName) {
        this.lastName = lastName;
        return this;
    }

    /**
     * 
     *
     * @return 
     */
    public Address getAddress() {
        return address;
    }

    /**
     * 
     *
     * @param address 
     * @return 
     */
    public Customer setAddress(Address address) {
        this.address = address;
        return this;
    }

    /**
     * 
     *
     * @return 
     */
    public List<Order> getOrderList() {
        return orderList;
    }

    /**
     * 
     *
     * @param orderList 
     * @return 
     */
    public Customer setOrderList(List<Order> orderList) {
        this.orderList = orderList;
        return this;
    }

    /**
     * 
     *
     * @return 
     */
    @Override
    public int hashCode() {
        return Objects.hash(address, firstName, id, lastName, orderList);
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
        Customer other = (Customer) obj;
        if (!Objects.equals(address, other.address)) {
            return false;
        }
        if (!Objects.equals(firstName, other.firstName)) {
            return false;
        }
        if (!Objects.equals(id, other.id)) {
            return false;
        }
        if (!Objects.equals(lastName, other.lastName)) {
            return false;
        }
        if (!Objects.equals(orderList, other.orderList)) {
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
        return "{id=" + id + ", firstName=" + firstName + ", lastName=" + lastName + ", address=" + address + ", orderList=" + orderList + "}";
    }
}
