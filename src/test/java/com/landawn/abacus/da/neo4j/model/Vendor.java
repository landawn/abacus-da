package com.landawn.abacus.da.neo4j.model;

import java.util.Objects;

import org.neo4j.ogm.annotation.GraphId;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Relationship;

@NodeEntity(label = "Vendor")
public class Vendor {
    @GraphId
    private Long id;
    private String name;

    @Relationship(type = "WHERE", direction = Relationship.OUTGOING)
    private Address address;

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
    public Vendor setId(Long id) {
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
    public Vendor setName(String name) {
        this.name = name;
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
    public Vendor setAddress(Address address) {
        this.address = address;
        return this;
    }

    /**
     * 
     *
     * @return 
     */
    @Override
    public int hashCode() {
        return Objects.hash(address, id, name);
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
        Vendor other = (Vendor) obj;
        if (!Objects.equals(address, other.address)) {
            return false;
        }
        if (!Objects.equals(id, other.id)) {
            return false;
        }
        if (!Objects.equals(name, other.name)) {
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
        return "{id=" + id + ", name=" + name + ", address=" + address + "}";
    }

}
