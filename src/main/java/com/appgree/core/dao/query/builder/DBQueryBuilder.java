/*
 * Copyright 2013-2014 Appgree S.A. All rights reserved.
 */
package com.appgree.core.dao.query.builder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.appgree.core.dao.processor.DataProcessor;
import com.appgree.core.database.provider.DataBaseManager;


/**
 * Wrapper around basic SQL queries.
 */
public class DBQueryBuilder {

    /** The Constant ALL_FIELDS. */
    private static final String ALL_FIELDS = "*";
    /** The logger. */
    private static Logger logger = Logger.getLogger(DBQueryBuilder.class);

    /**
     * Creates a basic select unconditional statement.
     *
     * @param select clause
     * @return clause object to execute or add condition
     */
    public static FromToWhereClause selectFromString(String select) {
        return new FromToWhereClause(select);
    }

    /**
     * Creates a basic select clause. It requires a FROM clause
     *
     * @param field to be returned by the query
     * @return FromClause object to establish the table
     */
    public static FromClause select(String field) {
        return new FromClause(field);
    }

    /**
     * Creates a basic select clause. It requires a FROM clause
     *
     * @param fields to be returned by the query
     * @return FromClause object to establish the table
     */
    public static FromClause select(String[] fields) {
        return new FromClause(Arrays.asList(fields));
    }

    /**
     * Creates a basic select clause. It requires a FROM clause
     *
     * @param fields to be returned by the query
     * @return FromClause object to establish the table
     */
    public static FromClause select(List<String> fields) {
        return new FromClause(fields);
    }

    /**
     * Creates a basic select COUNT clause. It requires a FROM clause
     *
     * @return FromClause object to establish the table
     */
    public static FromClause selectCount() {
        return new FromClause(FromClause.COUNT);
    }

    /**
     * Starts a condition (WHERE) clause for a particular field.
     *
     * @param field the field
     * @return WhereOperand object
     */
    public static WhereOperand whereClause(String field) {
        return new WhereOperand(new SQLClause(), field);
    }

    /**
     * Executes a prepared statement with arguments.
     *
     * @param <T> the generic type
     * @param query string
     * @param arguments list
     * @param processor object to process every row
     * @return count of processed rows
     * @throws Exception the exception
     */
    public static <T> int executeWithProcessor(String query, List<Object> arguments, DataProcessor<T> processor) throws Exception {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        int processed = 0;
        try {
            Connection conn = null;
            conn = DataBaseManager.getInstance().getConnection();
            stmt = conn.prepareStatement(query);
            int index = 1;
            for (Object param : arguments) {
                stmt.setObject(index++, param);
            }
            if (!stmt.execute())
                return -1;

            rs = stmt.getResultSet();
            while (rs.next()) {
                processor.process(rs);
                processed++;
            }
        } catch (SQLException e) {
            logger.error("Exception caught while executing query: " + query + "\n" + e);

            throw e;
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }

        return processed;
    }

    /**
     * Executes a prepared statement with arguments.
     *
     * @param query string
     * @param arguments list
     * @return PreparedStatement object
     * @throws SQLException the SQL exception
     */
    public static PreparedStatement execute(String query, List<Object> arguments) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DataBaseManager.getInstance().getConnection();
            stmt = conn.prepareStatement(query);
            int index = 1;
            if (arguments != null) {
                for (Object param : arguments) {
                    stmt.setObject(index++, param);
                }
            }
            if (!stmt.execute())
                return null;

            return stmt;

        } catch (SQLException e) {
            logger.error("Exception caught while executing query: " + query + "\n" + e);

            throw e;
        }
    }


    /**
     * Execute.
     *
     * @param query the query
     * @return the prepared statement
     * @throws SQLException the SQL exception
     */
    public static PreparedStatement execute(String query) throws SQLException {
        return execute(query, null);
    }

    /**
     * Generate csv from list.
     *
     * @param fields the fields
     * @return the string
     */
    public static String generateCSVFromList(List<String> fields) {
        if (fields == null || fields.isEmpty()) {
            return ALL_FIELDS;
        }
        return StringUtils.join(fields, ", ");
    }

    /**
     * Creates a new table.
     *
     * @param string the string
     * @return the creates the clause
     */
    public static CreateTableClause createTable(String string) {
        return new CreateTableClause(string);
    }

    /**
     * Drop table.
     *
     * @param string the string
     * @return the SQL clause
     */
    public static SQLClause dropTable(String string) {
        return new DropTableClause(string);
    }

}
