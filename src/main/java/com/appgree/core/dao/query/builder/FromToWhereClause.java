/*
 * Copyright 2013-2014 Appgree S.A. All rights reserved.
 */
package com.appgree.core.dao.query.builder;

import java.util.List;

/**
 * Inner class that represents a basic unconditional query. It can be use to JOIN and add conditions WHERE
 */
public class FromToWhereClause extends SQLClause {

    /**
     * Constructor with a select string and a table.
     *
     * @param select string
     * @param table name
     */
    public FromToWhereClause(String select, String table) {
        this(select + FROM + table);
    }

    /**
     * Constructor with a select string and a list of tables.
     *
     * @param select string
     * @param tables the tables
     */
    public FromToWhereClause(String select, List<String> tables) {
        this(select, DBQueryBuilder.generateCSVFromList(tables));
    }

    /**
     * Constructor with a select string.
     *
     * @param select string
     */
    FromToWhereClause(String select) {
        super(select);
    }

    /**
     * Adds a JOIN clause for a new table.
     *
     * @param table name
     * @return this instance
     */
    public FromToWhereClause join(String table) {
        if (table.contains(FROM)) {
            throw new IllegalArgumentException("Unexpected keyword FROM found in argument to join");
        }
        this.fromClause += " JOIN " + table;

        return this;
    }

    /**
     * Adds a WHERE operand for a field.
     *
     * @param field name
     * @return WhereOperand object
     */
    public WhereOperand where(String field) {
        return new WhereOperand(this, field);
    }

    /**
     * Adds a WHERE condition.
     *
     * @param condition as a WhereClause object
     * @return WhereClause instance
     */
    public WhereClause where(WhereClause condition) {
        if (!condition.fromClause.isEmpty()) {
            throw new IllegalArgumentException("Where condition is ill formed!");
        }
        this.whereClause += condition.whereClause;
        this.arguments.addAll(condition.arguments);
        if (condition.limitClause != null && !condition.limitClause.isEmpty() && (this.limitClause == null || this.limitClause.isEmpty())) {
            this.limitClause = condition.limitClause;
        }

        return new WhereClause(this);
    }

    /**
     * Adds a WHERE condition.
     *
     * @param condition as a String
     * @return WhereClause instance
     */
    public WhereClause whereTrue(String condition) {
        this.whereClause += condition;

        return new WhereClause(this);
    }

}
