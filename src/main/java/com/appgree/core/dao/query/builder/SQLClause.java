/*
 * Copyright 2013-2014 Appgree S.A. All rights reserved.
 */
package com.appgree.core.dao.query.builder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.appgree.core.dao.processor.DataProcessor;

/**
 * Base class for executable clauses.
 */
public class SQLClause {

    /** The arguments. */
    protected List<Object> arguments;
    
    /** The last stmt. */
    private PreparedStatement lastStmt = null;
    
    /** The from clause. */
    protected String fromClause;
    
    /** The where clause. */
    protected String whereClause;
    
    /** The limit clause. */
    protected String limitClause;

    /** The Constant WHERE. */
    public static final String WHERE = " WHERE ";
    
    /** The Constant SELECT. */
    protected static final String SELECT = "SELECT ";
    
    /** The Constant FROM. */
    protected static final String FROM = " FROM ";
    
    /** The Constant TRUE. */
    protected static final String TRUE = "TRUE";
    
    /** The Constant LIMIT. */
    protected static final String LIMIT = " LIMIT ";

    /**
     * Instantiates a new SQL clause.
     *
     * @param fromClause the from clause
     * @param whereClause the where clause
     * @param limitClause the limit clause
     * @param arguments the arguments
     */
    public SQLClause(String fromClause, String whereClause, String limitClause, List<Object> arguments) {
        if (whereClause != null && (whereClause.contains(" SELECT ") || whereClause.contains(" FROM ") || whereClause.contains(" WHERE "))) {
            throw new IllegalArgumentException("Where condition cannot have SELECT, FROM or WHERE keywords");
        }
        this.fromClause = fromClause;
        this.whereClause = whereClause;
        this.limitClause = limitClause;
        this.arguments = arguments;
    }

    /**
     * Instantiates a new SQL clause.
     *
     * @param innerQuery the inner query
     */
    public SQLClause(SQLClause innerQuery) {
        this(innerQuery.fromClause, innerQuery.whereClause, innerQuery.limitClause, innerQuery.arguments);
    }

    /**
     * Instantiates a new SQL clause.
     */
    public SQLClause() {
        this("", "", "", new LinkedList<Object>());
    }

    /**
     * Instantiates a new SQL clause.
     *
     * @param string the string
     */
    public SQLClause(String string) {
        this(string, "", "", new LinkedList<Object>());
    }

    /**
     * Adds a LIMIT clause.
     *
     * @param limit the limit
     * @return a SQLClause instance
     */
    public SQLClause limit(Integer limit) {
        if (limit != null && limit <= 0) {
            return this;
        }

        this.arguments.add(limit);
        this.limitClause = "?";

        return this;
    }

    /**
     * Adds a LIMIT clause paginated.
     *
     * @param start the start
     * @param limit the limit
     * @return a SQLClause instance
     */
    public SQLClause limit(Integer start, Integer limit) {
        if (limit != null && limit <= 0 && start != null && start < 0) {
            return this;
        }

        this.arguments.add(start);
        this.arguments.add(limit);
        this.limitClause = "?, ?";

        return this;
    }

    /**
     * Executes query without parameters.
     *
     * @return A ResultSet
     * @throws SQLException the SQL exception
     */
    public ResultSet execute() throws SQLException {
        return execute((List<Object>) null);
    }

    /**
     * Adds the clause parameters to the prepared statement and executes query.
     *
     * @param parameters List of objects for the preparedStatement
     * @return A ResultSet
     * @throws SQLException the SQL exception
     */
    public ResultSet execute(List<Object> parameters) throws SQLException {
        if (lastStmt != null) {
            lastStmt.close();
        }
        List<Object> actualArgs = getActualPreparedStatementParams(parameters);
        lastStmt = DBQueryBuilder.execute(this.toString(), actualArgs);

        if (lastStmt == null) {
            return null;
        }

        return lastStmt.getResultSet();
    }

    /**
     * Adds the clause parameters to the prepared statement, executes query and processes the rows.
     *
     * @param <T> the generic type
     * @param processor to apply to every row
     * @return The count of processed rows
     * @throws Exception the exception
     */
    public <T> int execute(DataProcessor<T> processor) throws Exception {
        return DBQueryBuilder.executeWithProcessor(this.toString(), this.arguments, processor);
    }

    /**
     * Gets the actual prepared statement params.
     *
     * @param parameters the parameters
     * @return the actual prepared statement params
     * @throws SQLException the SQL exception
     */
    private List<Object> getActualPreparedStatementParams(List<Object> parameters) throws SQLException {
        if (parameters == null) {
            return this.arguments;
        }

        List<Object> actualArgs = new ArrayList<Object>(this.arguments);
        int index = 0;
        int provided = 0;
        for (Object param = this.arguments.get(index); index < this.arguments.size() && provided < parameters.size(); index++) {
            if (param == null) {
                actualArgs.set(index, parameters.get(provided++));
            }
        }

        if (index < this.arguments.size() || provided < parameters.size()) {
            throw new SQLException("Wrong number of arguments in call to preparedStatement");
        }
        return actualArgs;
    }

    /**
     * Closes the last executed statement.
     *
     * @throws SQLException the SQL exception
     */
    public void close() throws SQLException {
        if (lastStmt != null) {
            lastStmt.close();
        }
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        if (fromClause == null || fromClause.isEmpty() || fromClause.contains(SELECT) || !fromClause.contains(FROM)) {
            return "Malformed SQL statement: " + SELECT + fromClause + WHERE + whereClause;
        }

        String output = SELECT + fromClause;

        if (whereClause != null && !whereClause.isEmpty()) {
            output += WHERE + whereClause;
        }

        if (limitClause != null && !limitClause.isEmpty()) {
            output += LIMIT + limitClause;
        }

        return output;
    }

}
