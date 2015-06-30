/*
 * Copyright 2013-2014 Appgree S.A. All rights reserved.
 */
package com.appgree.core.dao.processor;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.appgree.core.id.ObjectId;

/**
 * Extracts an ObjectId from the first long column of a resultSet. 
 * 
 */
public class ObjectIdProcessor implements DataProcessor<ObjectId> {

    /* (non-Javadoc)
     * @see com.appgree.core.dao.processor.DataProcessor#process(java.sql.ResultSet)
     */
    @Override
    public ObjectId process(ResultSet resultSet) throws SQLException {
        return ObjectId.fromLong(resultSet.getLong(1));
    }
}
