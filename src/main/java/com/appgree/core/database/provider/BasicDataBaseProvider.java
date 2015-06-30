/*
 * Copyright 2013-2014 Appgree S.A. All rights reserved.
 */
package com.appgree.core.database.provider;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.log4j.Logger;


/**
 * The Class BasicDataBaseProvider.
 */
public class BasicDataBaseProvider implements DataBaseProvider {

    /** The logger. */
    private static Logger logger = Logger.getLogger(BasicDataBaseProvider.class.getName());

    /** The connection string. */
    private static String connectionString;

    /* (non-Javadoc)
     * @see com.appgree.core.database.provider.DataBaseProvider#getConnection()
     */
    /**
     * Gets the connection.
     *
     * @return the connection
     * @throws SQLException the SQL exception
     */
    @Override
    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(connectionString);
    }

    /**
     * Inits the.
     *
     * @param driver the driver
     * @param connectionString the connection string
     * @throws Exception the exception
     */
    public void init(String driver, String connectionString) throws Exception {
        try {
            Class.forName(driver).newInstance();
            BasicDataBaseProvider.connectionString = connectionString;
        } catch (Exception e) {
            logger.error("Error loading jdbc driver", e);
            throw e;
        }

    }

    /* (non-Javadoc)
     * @see com.appgree.core.database.provider.DataBaseProvider#uninit()
     */
    /**
     * Uninit.
     *
     * @throws Exception the exception
     */
    @Override
    public void uninit() throws Exception {
        // nothing.
    }

    /* (non-Javadoc)
     * @see com.appgree.core.database.provider.DataBaseProvider#rollBackConnection()
     */
    /**
     * Roll back connection.
     *
     * @throws SQLException the SQL exception
     */
    @Override
    public void rollBackConnection() throws SQLException {
        // nothing.
    }

    /* (non-Javadoc)
     * @see com.appgree.core.database.provider.DataBaseProvider#commitConnection()
     */
    /**
     * Commit connection.
     *
     * @throws SQLException the SQL exception
     */
    @Override
    public void commitConnection() throws SQLException {
        // nothing.
    }

    /* (non-Javadoc)
     * @see com.appgree.core.database.provider.DataBaseProvider#releaseConnection()
     */
    /**
     * Release connection.
     *
     * @throws SQLException the SQL exception
     */
    @Override
    public void releaseConnection() throws SQLException {
        // nothing.
    }

    /* (non-Javadoc)
     * @see com.appgree.core.database.provider.DataBaseProvider#setConnection(java.sql.Connection)
     */
    /**
     * Sets the connection.
     *
     * @param conn the new connection
     * @throws SQLException the SQL exception
     */
    @Override
    public void setConnection(Connection conn) throws SQLException {
        // nothing.
    }
}
