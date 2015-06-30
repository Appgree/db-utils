/*
 * Copyright 2013-2014 Appgree S.A. All rights reserved.
 */
package com.appgree.core.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;

import com.appgree.core.id.Identifiable;


/**
 * The Interface StatementProcessor.
 *
 * @param <K> the key type
 */
public interface StatementProcessor<K extends Identifiable> {

    /**
     * Creates the.
     *
     * @param object the object
     * @param conn the conn
     * @return the prepared statement
     * @throws Exception the exception
     */
    public abstract PreparedStatement create(K object, Connection conn) throws Exception;

    /**
     * Sets the params.
     *
     * @param object the object
     * @param stmt the stmt
     * @throws Exception the exception
     */
    public void setParams(K object, PreparedStatement stmt) throws Exception;
}