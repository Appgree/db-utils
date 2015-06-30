/*
 * Copyright 2013-2014 Appgree S.A. All rights reserved.
 */
package com.appgree.core.dao.query.builder;

import java.util.Arrays;
import java.util.List;

/**
 * Inner class representing a select clause that requires a FROM.
 */
public class FromClause {

    /** The Constant COMMA. */
    static final String COMMA = ", ";

    /** The Constant COUNT. */
    public static final String COUNT = "COUNT(*)";

    /** The select clause. */
    private String selectClause;

    /**
     * Constructor that creates a select clause with field.
     *
     * @param field to be returned by the query
     */
    public FromClause(String field) {
        this.selectClause = field;
    }

    /**
     * Constructor that creates a select clause with a list of fields.
     *
     * @param fields to be returned by the query
     */
    public FromClause(List<String> fields) {
        this.selectClause = DBQueryBuilder.generateCSVFromList(fields);
    }

    /**
     * Creates a basic uncinditional sql query from a table.
     *
     * @param table name
     * @return FromToWhereClause object
     */
    public FromToWhereClause from(String table) {
        return new FromToWhereClause(this.selectClause, table);
    }

    /**
     * Creates a basic unconditional sql query from a list of tables.
     *
     * @param tables to use
     * @return FromToWhereClause object
     */
    public FromToWhereClause from(List<String> tables) {
        return new FromToWhereClause(this.selectClause, DBQueryBuilder.generateCSVFromList(tables));
    }

    /**
     * Creates a basic unconditional sql query from a list of tables.
     *
     * @param tables to use
     * @return FromToWhereClause object
     */
    public FromToWhereClause from(String[] tables) {
        return new FromToWhereClause(this.selectClause, DBQueryBuilder.generateCSVFromList(Arrays.asList(tables)));
    }

}
