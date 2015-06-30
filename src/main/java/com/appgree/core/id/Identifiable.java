/*
 * Copyright 2013-2014 Appgree S.A. All rights reserved.
 */
package com.appgree.core.id;


/**
 * The Interface Identifiable represents a business object that can has a unique id so that can be stored in both relational and key-value
 * repositories.
 */
public interface Identifiable {

    /**
     * Gets the id.
     *
     * @return Getter for the id
     */
    public ObjectId getId();

    /**
     * Sets the id.
     *
     * @param id the new id
     */
    void setId(ObjectId id);

}
