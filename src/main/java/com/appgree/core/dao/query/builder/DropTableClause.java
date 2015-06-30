/*
 * Copyright 2013-2014 Appgree S.A. All rights reserved.
 */
package com.appgree.core.dao.query.builder;

/**
 * Inner class DROP TABLE clauses.
 */
public class DropTableClause extends SQLClause {

    /** The table. */
    private String table;

    /**
     * Instantiates a new drop clause.
     *
     * @param string the string
     */
    public DropTableClause(String string) {
        this.table = string;
    }

    /* (non-Javadoc)
     * @see com.appgree.core.dao.query.builder.SQLClause#toString()
     */
    @Override
    public String toString() {
        StringBuilder queryString = new StringBuilder("DROP TABLE ");
        queryString.append(this.table);
        
        return queryString.toString();
    }

}
