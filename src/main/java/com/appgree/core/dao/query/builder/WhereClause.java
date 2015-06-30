/*
 * Copyright 2013-2014 Appgree S.A. All rights reserved.
 */
package com.appgree.core.dao.query.builder;

/**
 * Inner class that represents a predicate for a conditional clause or query.
 */
public class WhereClause extends SQLClause {

    /** The Constant ORDER_BY. */
    private static final String ORDER_BY = " ORDER BY ";

    /** The Constant OR. */
    private static final String OR = " OR ";

    /** The Constant AND. */
    private static final String AND = " AND ";

    /**
     * Constructor with query.
     *
     * @param innerQuery the inner query
     */
    public WhereClause(SQLClause innerQuery) {
        super(innerQuery);
    }

    /**
     * Creates a WhereOperand for field using AND.
     *
     * @param field to be added to WHERE
     * @return WhereOperand instance
     */
    public WhereOperand and(String field) {
        if (this.whereClause.contains(LIMIT) || this.whereClause.contains(ORDER_BY)) {
            throw new IllegalArgumentException("Misplaced AND");
        }

        if (!this.whereClause.isEmpty()) {
            this.whereClause += AND;
        }

        return new WhereOperand(this, field);
    }

    /**
     * Creates a WhereOperand for field using AND.
     *
     * @param condition that must be fulfilled to be added to WHERE
     * @return WhereOperand instance
     */
    public WhereClause andTrue(String condition) {
        if (this.whereClause.contains(LIMIT) || this.whereClause.contains(ORDER_BY)) {
            throw new IllegalArgumentException("Misplaced AND");
        }

        if (!this.whereClause.isEmpty()) {
            this.whereClause += AND;
        }
        this.whereClause += "(" + condition + ")";

        return this;
    }

    /**
     * Creates a WhereOperand for field using OR.
     *
     * @param field to be added to WHERE
     * @return WhereOperand instance
     */
    public WhereOperand or(String field) {
        if (this.whereClause.contains(LIMIT) || this.whereClause.contains(ORDER_BY)) {
            throw new IllegalArgumentException("Misplaced AND");
        }

        if (!this.whereClause.isEmpty()) {
            this.whereClause += OR;
        }

        return new WhereOperand(this, field);
    }

    /**
     * Adds and ORDER BY clause.
     *
     * @param field to order by
     * @return this instance
     */
    public WhereClause orderBy(String field) {
        if (this.whereClause.contains(LIMIT)) {
            throw new IllegalArgumentException("Misplaced ORDER BY");
        }

        if (this.whereClause.isEmpty()) {
            this.whereClause = TRUE;
        }

        this.whereClause += ORDER_BY + field;

        return this;
    }

    /**
     * Sets the order to ASC.
     *
     * @return SQLClause instance
     */
    public SQLClause asc() {
        if (!this.whereClause.contains(ORDER_BY) || this.whereClause.contains(LIMIT)) {
            throw new IllegalArgumentException("Misplaced ASC");
        }
        return this;
    }

    /**
     * Sets the order to DESC.
     *
     * @return SQLClause instance
     */
    public SQLClause desc() {
        if (!this.whereClause.contains(ORDER_BY) || this.whereClause.contains(LIMIT)) {
            throw new IllegalArgumentException("Misplaced DESC");
        }

        this.whereClause += " DESC ";

        return this;
    }
}
