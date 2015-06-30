/*
 * Copyright 2013-2014 Appgree S.A. All rights reserved.
 */
package com.appgree.core.dao.processor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * The Interface DataSerializer allows to convert from Object Oriented to Relational DB model.
 *
 * @param <T> the generic type
 */
public interface DataSerializer<T> {

    /**
     * Creates a ModelObject instance calling the specialization on the appropriate DAO. DO NOT USE resultSet.next() INSIDE.
     *
     * @param resultSet the result set
     * @return an instance of the object or null if not found
     * @throws Exception the exception
     */
    public T deserialize(ResultSet resultSet) throws Exception;

    /**
     * Insert an object's attributes into a prepared statement.
     *
     * @param object the object
     * @param stmt the PreparedStatement object
     * @throws Exception the exception
     */
    public void serialize(T object, PreparedStatement stmt) throws Exception;
}
