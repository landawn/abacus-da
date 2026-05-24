package com.landawn.abacus.da.neo4j.model;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.ogm.annotation.GraphId;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Relationship;

@NodeEntity(label = "Movie")
public class Movie {

    @GraphId
    private Long id;
    private String title;
    private int released;

    @Relationship(type = "ACTS_IN", direction = Relationship.INCOMING)
    Set<Actor> actors = new HashSet<>();

    /**
     */
    public Movie() {
    }

    /**
     * 
     *
     * @param title 
     * @param year 
     */
    public Movie(String title, int year) {
        this.title = title;
        this.released = year;
    }

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
     */
    public void setId(Long id) {
        this.id = id;
    }

    /**
     * 
     *
     * @return 
     */
    public String getTitle() {
        return title;
    }

    /**
     * 
     *
     * @param title 
     */
    public void setTitle(String title) {
        this.title = title;
    }

    /**
     * 
     *
     * @return 
     */
    public int getReleased() {
        return released;
    }

    /**
     * 
     *
     * @param released 
     */
    public void setReleased(int released) {
        this.released = released;
    }

    /**
     * 
     *
     * @return 
     */
    public Set<Actor> getActors() {
        return actors;
    }

    /**
     * 
     *
     * @param actors 
     */
    public void setActors(Set<Actor> actors) {
        this.actors = actors;
    }

}