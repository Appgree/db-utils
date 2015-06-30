/*
 * Copyright 2013-2014 Appgree S.A. All rights reserved.
 */
package com.appgree.core.dao.processor;

import java.sql.ResultSet;

/**
 * The Interface DataProcessor returns any object from a table row. This can be used to perform operations on a ResultSet while iterating though the
 * db cursor.
 *
 * @param <T> the generic type
 */
public interface DataProcessor<T> {

    /**
     * Creates a ModelObject instance calling the specialization on the appropriate DAO. DO NOT USE resultSet.next() INSIDE.
     *
     * @param resultSet the result set
     * @return an instance of the object or null if not found
     * @throws Exception the exception
     */
    public T process(ResultSet resultSet) throws Exception;

}
