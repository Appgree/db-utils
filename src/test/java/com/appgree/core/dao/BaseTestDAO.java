/*
 * Copyright 2013-2014 Appgree S.A. All rights reserved.
 */
package com.appgree.core.dao;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.appgree.core.database.provider.BasicDataBaseProvider;
import com.appgree.core.database.provider.DataBaseManager;
import com.appgree.core.id.ObjectId;


/**
 * This class represents the basic common functionality of all DAO tests.
 */
public class BaseTestDAO {

    /** The Constant ID_ONE. */
    protected static final ObjectId ID_ONE = ObjectId.fromLong(1);
    
    /** The Constant LEADER_ID. */
    protected static final ObjectId LEADER_ID = ObjectId.fromLong(2);

    /**
     * Inits the.
     *
     * @throws Exception the exception
     */
    @BeforeClass
    public static void init() throws Exception {
        final BasicDataBaseProvider provider = new BasicDataBaseProvider();
        provider.init("com.mysql.jdbc.Driver", "jdbc:mysql://localhost/<database>?user=<user>&password=<pwd>&useUnicode=true");
        DataBaseManager.getInstance().init(provider, 100, 100);
    }

    /**
     * Destroy.
     *
     * @throws Exception the exception
     */
    @AfterClass
    public static void destroy() throws Exception {
    }

    /**
     * Tear down.
     *
     * @throws Exception the exception
     */
    @After
    public void tearDown() throws Exception {
        DataBaseManager.getInstance().rollBackConnection();
    }

}
