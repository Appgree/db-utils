/*
 * Copyright 2013-2014 Appgree S.A. All rights reserved.
 */
package com.appgree.core.dao.query.builder;


/**
 * Inner class that represents an incomplete SQL clause with the left operand of a WHERE condition.
 */
public class WhereOperand {

    /** The Constant IS_NOT_NULL. */
    private static final String IS_NOT_NULL = " IS NOT NULL";

    /** The Constant NOT_EQUAL. */
    private static final String NOT_EQUAL = " != ";

    /** The Constant ARGUMENT. */
    private static final String ARGUMENT = "?";

    /** The Constant LOWER_OR_EQUAL_THAN. */
    private static final String LOWER_OR_EQUAL_THAN = " <= ";

    /** The Constant LOWER_THAN. */
    private static final String LOWER_THAN = " < ";

    /** The Constant GREATER_OR_EQUAL_THAN. */
    private static final String GREATER_OR_EQUAL_THAN = " >= ";

    /** The Constant GREATER_THAN. */
    private static final String GREATER_THAN = " > ";

    /** The Constant EQUAL. */
    private static final String EQUAL = " = ";

    /** The Constant LIKE. */
    private static final String LIKE = " LIKE ";

    /** The inner query. */
    private SQLClause innerQuery;

    /**
     * Constructor with previous SQLClause and field.
     *
     * @param innerQuery the inner query
     * @param field to use in operand
     */
    public WhereOperand(SQLClause innerQuery, String field) {
        this.innerQuery = innerQuery;
        if (field != null) {
            this.innerQuery.whereClause += field;
        }
    }

    /**
     * Adds an EQUAL condition for a parameterized statement.
     *
     * @return SQLClause instance
     */
    public SQLClause equal() {
        return equal(null);
    }

    /**
     * Creates the condition predicate.
     *
     * @param value the value
     * @param operator the operator
     * @return the where clause
     */
    private WhereClause createConditionPredicate(Object value, String operator) {
        this.innerQuery.arguments.add(value);
        this.innerQuery.whereClause += operator + ARGUMENT;

        return new WhereClause(this.innerQuery);
    }

    /**
     * Adds an EQUAL condition for an actual statement.
     *
     * @param value the value
     * @return SQLClause instance
     */
    public WhereClause equal(Object value) {
        return createConditionPredicate(value, EQUAL);
    }

    /**
     * Adds a GREATER THAN condition for a parameterized statement.
     *
     * @return SQLClause instance
     */
    public WhereClause greaterThan() {
        return greaterThan(null);
    }

    /**
     * Adds a GREATER THAN condition for an actual statement.
     *
     * @param value the value
     * @return SQLClause instance
     */
    public WhereClause greaterThan(Object value) {
        return createConditionPredicate(value, GREATER_THAN);
    }

    /**
     * Adds a GREATER OR EQUAL THAN condition for a parameterized statement.
     *
     * @return SQLClause instance
     */
    public WhereClause greaterOrEqualThan() {
        return greaterOrEqualThan(null);
    }

    /**
     * Adds a GREATER OR EQUAL THAN condition for a actual statement.
     *
     * @param value the value
     * @return SQLClause instance
     */
    public WhereClause greaterOrEqualThan(Object value) {
        return createConditionPredicate(value, GREATER_OR_EQUAL_THAN);
    }

    /**
     * Adds a LOWER THAN condition for a parameterized statement.
     *
     * @return SQLClause instance
     */
    public WhereClause lowerThan() {
        return lowerThan(null);
    }

    /**
     * Adds a LOWER THAN condition for an actual statement.
     *
     * @param value the value
     * @return SQLClause instance
     */
    public WhereClause lowerThan(Object value) {
        return createConditionPredicate(value, LOWER_THAN);
    }

    /**
     * Adds a LOWER OR EQUAL THAN condition for a parameterized statement.
     *
     * @return SQLClause instance
     */
    public WhereClause lowerOrEqualThan() {
        return lowerOrEqualThan(null);
    }

    /**
     * Adds a LOWER OR EQUAL THAN condition for an actual statement.
     *
     * @param value the value
     * @return SQLClause instance
     */
    public WhereClause lowerOrEqualThan(Object value) {
        return createConditionPredicate(value, LOWER_OR_EQUAL_THAN);
    }

    /**
     * Adds a NOT EQUAL condition for a parameterized statement.
     *
     * @return SQLClause instance
     */
    public WhereClause notEqual() {
        return notEqual(null);
    }

    /**
     * Adds a NOT EQUAL condition for an actual statement.
     *
     * @param value the value
     * @return SQLClause instance
     */
    public WhereClause notEqual(Object value) {
        return createConditionPredicate(value, NOT_EQUAL);
    }

    /**
     * Like.
     *
     * @return the where clause
     */
    public WhereClause like() {
        return createConditionPredicate(null, LIKE);
    }

    /**
     * Like.
     *
     * @param value the value
     * @return the where clause
     */
    public WhereClause like(String value) {
        return createConditionPredicate(value, LIKE);
    }

    /**
     * Not null.
     *
     * @return the where clause
     */
    public WhereClause notNull() {
        this.innerQuery.whereClause += IS_NOT_NULL;

        return new WhereClause(innerQuery);
    }

}
