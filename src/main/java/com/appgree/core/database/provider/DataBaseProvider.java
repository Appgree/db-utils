/*
 * Copyright 2013-2014 Appgree S.A. All rights reserved.
 */
package com.appgree.core.database.provider;

import java.sql.Connection;
import java.sql.SQLException;


/**
 * The Interface DataBaseProvider.
 */
public interface DataBaseProvider {

    /* main functions */
    /**
     * Gets the connection.
     *
     * @return the connection
     * @throws SQLException the SQL exception
     */
    public Connection getConnection() throws SQLException;

    /**
     * Roll back connection.
     *
     * @throws SQLException the SQL exception
     */
    public void rollBackConnection() throws SQLException;

    /**
     * Commit connection.
     *
     * @throws SQLException the SQL exception
     */
    public void commitConnection() throws SQLException;

    /**
     * Release connection.
     *
     * @throws SQLException the SQL exception
     */
    public void releaseConnection() throws SQLException;

    /* loading and ending funcitons */
    /**
     * Uninit.
     *
     * @throws Exception the exception
     */
    public void uninit() throws Exception;

    /**
     * THIS METHOD IS ONLY USED IN TESTING TO PASS A CONNECTION BETWEEN THREADS SO COMMITTING IS NOT NEEDED.
     *
     * @param conn the new connection
     * @throws SQLException the SQL exception
     */
    public void setConnection(Connection conn) throws SQLException;
}
