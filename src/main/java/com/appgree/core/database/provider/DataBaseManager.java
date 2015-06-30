/*
 * Copyright 2013-2014 Appgree S.A. All rights reserved.
 */
package com.appgree.core.database.provider;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * The Class DataBaseManager.
 */
public class DataBaseManager implements DataBaseProvider {

    /** The Constant DEFAULT_MAX_N_BLOCKED_THREADS. */
    private static final int DEFAULT_MAX_N_BLOCKED_THREADS = 100;

    /**
     * Specialized exception to control number of concurrent blocked threads.
     */
    public class MaxNumberBlockedThreadsException extends SQLException {

        /**
         * Instantiates a new max number blocked threads exception.
         *
         * @param reason the reason
         */
        public MaxNumberBlockedThreadsException(String reason) {
            super(reason);
        }

        /** The Constant serialVersionUID. */
        private static final long serialVersionUID = 1L;
    }

    /** The provider. */
    private DataBaseProvider provider;

    /** The max n blocked threads. */
    private int maxNBlockedThreads = DEFAULT_MAX_N_BLOCKED_THREADS;

    /** The n blocked threads. */
    private AtomicInteger nBlockedThreads = new AtomicInteger();

    /** The query timeout. */
    private int queryTimeout = 10;

    // Singleton pattern
    /** The Constant instance. */
    private static final DataBaseManager instance = new DataBaseManager();

    // Private constructor prevents instantiation from other classes
    /**
     * Instantiates a new data base manager.
     */
    private DataBaseManager() {}

    // Returns the instance
    /**
     * Gets the single instance of DataBaseManager.
     *
     * @return single instance of DataBaseManager
     */
    public static DataBaseManager getInstance() {
        return instance;
    }

    /**
     * Initializes manager AND provider instance, no need to call init() again on the instance.
     *
     * @param provider the provider
     * @param maxNBlockedThreads the max n blocked threads
     * @param queryTimeout the query timeout
     * @throws Exception the exception
     */
    public void init(DataBaseProvider provider, int maxNBlockedThreads, int queryTimeout) throws Exception {
        this.provider = provider;
        this.maxNBlockedThreads = maxNBlockedThreads;
        this.queryTimeout = queryTimeout;
    }

    /* INTERFACE METHODS */

    /*
     * (non-Javadoc)
     * 
     * @see com.appgree.core.database.provider.DataBaseProvider#getConnection()
     */
    @Override
    public Connection getConnection() throws SQLException {

        int nBlockedThreads = this.nBlockedThreads.get();

        if (nBlockedThreads >= maxNBlockedThreads) {
            throw new MaxNumberBlockedThreadsException("Max number of concurrent blocked threads waiting for database reached. Current = "
                            + nBlockedThreads + " max = " + maxNBlockedThreads);
        }

        // increases counter
        this.nBlockedThreads.incrementAndGet();

        try {
            return new ConnectionWrapper(this.provider.getConnection(), this.queryTimeout);
        } finally {
            this.nBlockedThreads.decrementAndGet();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.appgree.core.database.provider.DataBaseProvider#uninit()
     */
    @Override
    public void uninit() throws Exception {
        if (this.provider != null) {
            this.provider.uninit();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.appgree.core.database.provider.DataBaseProvider#rollBackConnection()
     */
    @Override
    public void rollBackConnection() throws SQLException {
        this.provider.rollBackConnection();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.appgree.core.database.provider.DataBaseProvider#commitConnection()
     */
    @Override
    public void commitConnection() throws SQLException {
        this.provider.commitConnection();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.appgree.core.database.provider.DataBaseProvider#releaseConnection()
     */
    @Override
    public void releaseConnection() throws SQLException {
        this.provider.releaseConnection();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.appgree.core.database.provider.DataBaseProvider#setConnection(java.sql.Connection)
     */
    @Override
    public void setConnection(Connection conn) throws SQLException {
        this.provider.setConnection(conn);
    }

}
