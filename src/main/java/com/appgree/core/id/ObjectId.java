/*
 * Copyright 2013-2014 Appgree S.A. All rights reserved.
 */
package com.appgree.core.id;

/**
 * The Class ObjectId.
 *
 * @author Eduardo
 *
 *         This class represents a persistence independent id for business objects. This id might be used for protocol serialization and persistence
 *         on cache and database. ObjectId is immutable.
 */
public final class ObjectId implements Comparable<ObjectId> {

    /**
     * Empty id. This id is not valid for storage.
     */
    public static final ObjectId NULL = new ObjectId(0L);

    /**
     * String serialization of a NULL id.
     */
    public static final String VOID = "0";

    /**
     * Long value of the id. This value must be initialized at all times.
     */
    private final long longId;

    /**
     * String representation of the id. Generated using lazy initialization. Once created, it will never change.
     */
    private String stringId = null;

    /**
     * (private) Parameterized constructor.
     *
     * @param id long value for the id.
     */
    private ObjectId(long id) {
        this.longId = id;
        this.stringId = null;
    }

    /**
     * (private) Parameterized constructor.
     *
     * @param id string value for the id
     */
    private ObjectId(String id) {
        this.longId = Long.parseLong(id);
        this.stringId = id;
    }

    /**
     * To long.
     *
     * @return the long value of the id
     */
    public long toLong() {
        return longId;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        if (isNull()) {
            return VOID;
        }

        if (this.stringId == null) {
            this.stringId = String.valueOf(this.longId);
        }

        return this.stringId;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return (int) (this.longId ^ (this.longId >>> 32));
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null || !getClass().equals(obj.getClass())) {
            return false;
        }

        ObjectId objId = (ObjectId) obj;

        return this.longId == objId.longId;
    }

    /**
     * Factory method.
     *
     * @param id long value for the id
     * @return an ObjectId
     */
    public static ObjectId fromLong(long id) {
        return new ObjectId(id);
    }

    /**
     * Factory method.
     *
     * @param id String representation for the id
     * @return an ObjectId
     */
    public static ObjectId fromString(String id) {
        if (id == null || id.isEmpty() || id.equals(VOID) || "null".equals(id) || "nil".equals(id)) {
            return NULL;
        }

        return new ObjectId(id);
    }

    /**
     * Checks if is null.
     *
     * @return true if id is NULL
     */
    public boolean isNull() {
        return this.equals(NULL);
    }

    /**
     * Next id.
     *
     * @return an ObjectId with the next available value
     */
    public ObjectId nextId() {
        if (this.isNull()) {
            throw new NullPointerException("Trying to get the next id from NULL");
        }

        if (this.longId == Long.MAX_VALUE) {
            throw new ArithmeticException("Cannot increase the maximum number for an Id");
        }

        return ObjectId.fromLong(this.longId + 1);
    }

    /**
     * Checks if is null.
     *
     * @param id the id
     * @return true, if is null
     */
    public static boolean isNull(ObjectId id) {
        return id == null || id.isNull();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(ObjectId o) {
        return Long.compare(this.longId, o.toLong());
    }

}
