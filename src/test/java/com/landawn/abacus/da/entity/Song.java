/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.entity;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public class Song {
    private UUID id;
    private String title;
    private String album;
    private String artist;
    private Set<String> tags;
    private java.nio.ByteBuffer data;

    /**
     * 
     *
     * @return 
     */
    public UUID getId() {
        return id;
    }

    /**
     * 
     *
     * @param id 
     */
    public void setId(UUID id) {
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
    public String getAlbum() {
        return album;
    }

    /**
     * 
     *
     * @param album 
     */
    public void setAlbum(String album) {
        this.album = album;
    }

    /**
     * 
     *
     * @return 
     */
    public String getArtist() {
        return artist;
    }

    /**
     * 
     *
     * @param artist 
     */
    public void setArtist(String artist) {
        this.artist = artist;
    }

    /**
     * 
     *
     * @return 
     */
    public Set<String> getTags() {
        return tags;
    }

    /**
     * 
     *
     * @param tags 
     */
    public void setTags(Set<String> tags) {
        this.tags = tags;
    }

    /**
     * 
     *
     * @return 
     */
    public java.nio.ByteBuffer getData() {
        return data;
    }

    /**
     * 
     *
     * @param data 
     */
    public void setData(java.nio.ByteBuffer data) {
        this.data = data;
    }

    /**
     * 
     *
     * @return 
     */
    @Override
    public int hashCode() {
        return Objects.hash(album, artist, data, id, tags, title);
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
        Song other = (Song) obj;
        if (!Objects.equals(album, other.album)) {
            return false;
        }
        if (!Objects.equals(artist, other.artist)) {
            return false;
        }
        if (!Objects.equals(data, other.data)) {
            return false;
        }
        if (!Objects.equals(id, other.id)) {
            return false;
        }
        if (!Objects.equals(tags, other.tags)) {
            return false;
        }
        if (!Objects.equals(title, other.title)) {
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
        return "{id=" + id + ", title=" + title + ", album=" + album + ", artist=" + artist + ", tags=" + tags + ", data=" + data + "}";
    }

}
