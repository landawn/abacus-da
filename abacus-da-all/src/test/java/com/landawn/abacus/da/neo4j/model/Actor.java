package com.landawn.abacus.da.neo4j.model;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.ogm.annotation.GraphId;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Relationship;

@NodeEntity(label = "Actor")
public class Actor {
    @GraphId
    private Long id;
    private String name;

    @Relationship(type = "ACTS_IN", direction = Relationship.OUTGOING)
    private Set<Movie> movies = new HashSet<>();

    /**
     */
    public Actor() {
    }

    /**
     * 
     *
     * @param name 
     */
    public Actor(String name) {
        this.name = name;
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
    public String getName() {
        return name;
    }

    /**
     * 
     *
     * @param name 
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * 
     *
     * @return 
     */
    public Set<Movie> getMovies() {
        return movies;
    }

    /**
     * 
     *
     * @param movies 
     */
    public void setMovies(Set<Movie> movies) {
        this.movies = movies;
    }

    /**
     * 
     *
     * @param movie 
     */
    public void actsIn(Movie movie) {
        movies.add(movie);
        movie.getActors().add(this);
    }
}
