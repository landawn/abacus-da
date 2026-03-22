package com.landawn.abacus.da.neo4j.model;

import java.util.Objects;

import org.neo4j.ogm.annotation.GraphId;
import org.neo4j.ogm.annotation.NodeEntity;

@NodeEntity(label = "Address")
public class Address {
    @GraphId
    private Long id;
    private String street;
    private String city;
    private String state;
    private String country;
    private int zipCode;

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
    public Address setId(Long id) {
        this.id = id;
        return this;
    }

    /**
     * 
     *
     * @return 
     */
    public String getStreet() {
        return street;
    }

    /**
     * 
     *
     * @param street 
     * @return 
     */
    public Address setStreet(String street) {
        this.street = street;
        return this;
    }

    /**
     * 
     *
     * @return 
     */
    public String getCity() {
        return city;
    }

    /**
     * 
     *
     * @param city 
     * @return 
     */
    public Address setCity(String city) {
        this.city = city;
        return this;
    }

    /**
     * 
     *
     * @return 
     */
    public String getState() {
        return state;
    }

    /**
     * 
     *
     * @param state 
     * @return 
     */
    public Address setState(String state) {
        this.state = state;
        return this;
    }

    /**
     * 
     *
     * @return 
     */
    public String getCountry() {
        return country;
    }

    /**
     * 
     *
     * @param country 
     * @return 
     */
    public Address setCountry(String country) {
        this.country = country;
        return this;
    }

    /**
     * 
     *
     * @return 
     */
    public int getZipCode() {
        return zipCode;
    }

    /**
     * 
     *
     * @param zipCode 
     * @return 
     */
    public Address setZipCode(int zipCode) {
        this.zipCode = zipCode;
        return this;
    }

    /**
     * 
     *
     * @return 
     */
    @Override
    public int hashCode() {
        return Objects.hash(city, country, id, state, street, zipCode);
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
        Address other = (Address) obj;
        if (!Objects.equals(city, other.city)) {
            return false;
        }
        if (!Objects.equals(country, other.country)) {
            return false;
        }
        if (!Objects.equals(id, other.id)) {
            return false;
        }
        if (!Objects.equals(state, other.state)) {
            return false;
        }
        if (!Objects.equals(street, other.street)) {
            return false;
        }
        if (zipCode != other.zipCode) {
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
        return "{id=" + id + ", street=" + street + ", city=" + city + ", state=" + state + ", country=" + country + ", zipCode=" + zipCode + "}";
    }

}
