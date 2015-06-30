/*
 * Copyright 2013-2014 Appgree S.A. All rights reserved.
 */
package com.appgree.core.dao;

import java.lang.reflect.ParameterizedType;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.appgree.core.dao.processor.DataProcessor;
import com.appgree.core.dao.processor.DataSerializer;
import com.appgree.core.dao.query.builder.DBQueryBuilder;
import com.appgree.core.dao.query.builder.FromToWhereClause;
import com.appgree.core.dao.query.builder.SQLClause;
import com.appgree.core.dao.query.builder.WhereClause;
import com.appgree.core.database.provider.DataBaseManager;
import com.appgree.core.id.Identifiable;
import com.appgree.core.id.ObjectId;

/**
 * Represents a database DAO to be extended by child classes.
 *
 * @param <T> the generic type
 */
public abstract class BaseDAO<T extends Identifiable> implements DataSerializer<T>, DataProcessor<T> {

    /** The Constant ALIAS_A. */
    private static final String ALIAS_A = "A.";

    /** The Constant ON_DUPLICATE_KEYWORD. */
    private static final String ON_DUPLICATE_KEYWORD = " ON DUPLICATE KEY UPDATE ";

    /** The Constant SET_KEYWORD. */
    private static final String SET_KEYWORD = " SET ";

    /** The Constant UPDATE_KEYWORD. */
    private static final String UPDATE_KEYWORD = "UPDATE ";

    /** The Constant DELETE_FROM_KEYWORD. */
    private static final String DELETE_FROM_KEYWORD = "DELETE FROM ";

    /** The Constant ID_FIELD. */
    protected static final String ID_FIELD = "ID";

    /** The Constant EQUALS_EXPRESSION. */
    private static final String EQUALS_EXPRESSION = "=?";

    /** The Constant FROM_KEYWORD. */
    private static final String FROM_KEYWORD = " FROM ";

    /** The Constant WHERE_KEYWORD. */
    private static final String WHERE_KEYWORD = " WHERE ";

    /** The Constant MAX_ACCUM_BATCH. */
    protected static final int MAX_ACCUM_BATCH = 1000;

    /** The table name. */
    private String tableName;

    /** The insert clause. */
    private String insertClause = null;

    /** The update clause. */
    private String updateClause = null;

    /** The fields. */
    protected List<String> fields = new ArrayList<>();

    /** The insert ignore clause. */
    private String insertIgnoreClause;

    /** The registered instances. */
    private static Map<Class<? extends Identifiable>, BaseDAO<? extends Identifiable>> registeredInstances = new HashMap<>();

    /**
     * Instantiates a new base dao.
     *
     * @param tableName the table name
     */
    public BaseDAO(String tableName) {
        this.tableName = tableName;

        Class<T> clazz = getClassFromGenericInstance(this);
        registeredInstances.put(clazz, this);
    }

    /**
     * Gets the class from generic instance.
     *
     * @param <K> the key type
     * @param instance the instance
     * @return the class from generic instance
     */
    @SuppressWarnings("unchecked")
    private static <K extends Identifiable> Class<K> getClassFromGenericInstance(BaseDAO<K> instance) {
        return (Class<K>) ((ParameterizedType) instance.getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    /**
     * Fields.
     *
     * @return the list
     */
    protected List<String> allFields() {
        return fields;
    }

    /**
     * Fields to update on duplicate key.
     *
     * @return the list
     */
    protected List<String> fieldsToUpdate() {
        return fields;
    }

    /**
     * Gets the instance for class.
     *
     * @param <K> the key type
     * @param clazz the clazz
     * @return the instance for class
     */
    @SuppressWarnings("unchecked")
    public static final <K extends Identifiable> BaseDAO<K> getInstanceForClass(Class<K> clazz) {
        return (BaseDAO<K>) registeredInstances.get(clazz);
    }

    /**
     * Adds a new row to a table.
     *
     * @param object the object
     * @throws Exception the exception
     */
    public void add(T object) throws Exception {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DataBaseManager.getInstance().getConnection();
            // Obtains identifier
            if (ObjectId.isNull(object.getId())) {
                throw new SQLException("The object must have an valid identifier to be added");
            }
            // Inserts user
            stmt = conn.prepareStatement(insertClause(false));

            serialize(object, stmt);

            stmt.executeUpdate();
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    /**
     * Deletes a recrod from a table.
     *
     * @param id the id
     * @throws SQLException the SQL exception
     */
    public void delete(ObjectId id) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DataBaseManager.getInstance().getConnection();
            String query = deleteClause();
            stmt = conn.prepareStatement(query);
            stmt.setLong(1, id.toLong());
            stmt.executeUpdate();
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    /**
     * Delete clause.
     *
     * @return the string
     */
    protected String deleteClause() {
        return DELETE_FROM_KEYWORD + this.tableName + WHERE_KEYWORD + ID_FIELD + EQUALS_EXPRESSION;
    }

    /**
     * Find all records in a table matching the clause.
     *
     * @param sqlClause the sql clause
     * @return the list
     * @throws Exception the exception
     */
    protected List<T> findAll(SQLClause sqlClause) throws Exception {
        return findAllWithProcessor(sqlClause, this);
    }

    /**
     * Find all records in a table matching the clause and use a processor to return the value.
     *
     * @param <K> the key type
     * @param query the query
     * @param processor the processor
     * @return the list
     * @throws Exception the exception
     */
    protected <K> List<K> findAllWithProcessor(SQLClause query, DataProcessor<K> processor) throws Exception {
        ResultSet rs = null;
        try {
            rs = query.execute();
            if (rs == null) {
                return null;
            }

            ArrayList<K> ret = new ArrayList<>();
            while (rs.next()) {

                ret.add(processor.process(rs));
            }
            return ret;
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (query != null) {
                query.close();
            }
        }
    }

    /**
     * Adds or updates a row in a table using the object's id.
     *
     * @param object the object
     * @throws Exception the exception
     */
    public void addOrUpdate(T object) throws Exception {
        addOrUpdate(object, this.fieldsToUpdate());
    }

    /**
     * Adds the object or updates the specified fields on duplicate key.
     *
     * @param object the object
     * @param updateFields the update fields
     * @throws Exception the exception
     */
    public void addOrUpdate(T object, List<String> updateFields) throws Exception {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DataBaseManager.getInstance().getConnection();
            // Identifier is mandatory
            if (ObjectId.isNull(object.getId())) {
                throw new SQLException("The object must have a valid identifier to be added");
            }

            StatementProcessor<T> processor = new UpdateStatementBuilder(updateFields);
            stmt = processor.create(object, conn);
            processor.setParams(object, stmt);

            stmt.executeUpdate();

        } finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    /**
     * Adds the object or updates the specified fields on duplicate key.
     *
     * @param object the object
     * @param updateFields the update fields
     * @throws Exception the exception
     */
    public void addOrUpdate(T object, String[] updateFields) throws Exception {
        addOrUpdate(object, Arrays.asList(updateFields));
    }

    /**
     * Count all records in a table matching the clause.
     *
     * @param query the query
     * @return the int
     * @throws SQLException the SQL exception
     */
    public int countAll(WhereClause query) throws SQLException {
        ResultSet rs = null;
        try {
            SQLClause clause = DBQueryBuilder.selectCount().from(tableName).where(query);
            rs = clause.execute();
            if (rs == null || !rs.next()) {
                return 0;
            }

            return rs.getInt(1);

        } finally {
            if (rs != null) {
                rs.close();
            }
            if (query != null) {
                query.close();
            }
        }
    }

    /**
     * Find a row in a table by id.
     *
     * @param id the id
     * @return the t
     * @throws Exception the exception
     */
    public T findById(ObjectId id) throws Exception {
        return findObject(DBQueryBuilder.selectFromString(selectClause()).where(ALIAS_A + ID_FIELD).equal(id.toLong()).limit(1));
    }

    /**
     * Find a row in a table by id and uses a processor to calculate/process the result.
     *
     * @param <K> the key type
     * @param query the query
     * @param processor the processor
     * @return the k
     * @throws Exception the exception
     */
    protected <K> K findObjectWithProcessor(SQLClause query, DataProcessor<K> processor) throws Exception {
        ResultSet rs = null;
        try {
            rs = query.execute();
            if (rs == null || !rs.next()) {
                return null;
            }

            return processor.process(rs);
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (query != null) {
                query.close();
            }
        }
    }

    /**
     * Find a row in a table matching the clause.
     *
     * @param query the query
     * @return the t
     * @throws Exception the exception
     */
    public T findObject(SQLClause query) throws Exception {
        return findObjectWithProcessor(query, this);
    }

    /**
     * Process all records in a table matching a clause and returns the number of affected rows.
     *
     * @param clause the query
     * @param processor the insert to cache
     * @return the int
     * @throws Exception the exception
     */
    protected int countAllProcessed(SQLClause clause, DataProcessor<Boolean> processor) throws Exception {
        ResultSet rs = null;
        try {
            rs = clause.execute();
            if (rs == null) {
                return 0;
            }

            int counter = 0;
            while (rs.next()) {
                if (processor.process(rs)) {
                    counter++;
                }
            }

            return counter;
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (clause != null) {
                clause.close();
            }
        }

    }

    /**
     * Inserts a new row in a table.
     *
     * @param ignoreErrors the ignore errors
     * @return the string
     */
    protected String insertClause(boolean ignoreErrors) {
        if (ignoreErrors) {
            if (insertIgnoreClause == null) {
                synchronized (this) {
                    if (insertIgnoreClause == null) {
                        insertIgnoreClause = createInsertClause(true);
                    }
                }
            }
            return insertIgnoreClause;
        } else {
            if (insertClause == null) {
                synchronized (this) {
                    if (insertClause == null) {
                        insertClause = createInsertClause(false);
                    }
                }
            }
            return insertClause;
        }
    }

    private String createInsertClause(boolean ignoreErrors) {
        StringBuilder sb = new StringBuilder();
        List<String> fields = allFields();
        sb.append("INSERT ");
        if (ignoreErrors)
            sb.append("IGNORE ");
        sb.append("INTO ");
        sb.append(this.tableName).append(" (");
        StringBuilder values = new StringBuilder("(");
        int index = 0;
        for (String field : fields) {
            index++;
            values.append("?");
            if (index < fields.size()) {
                values.append(", ");
            }
            if (!ID_FIELD.endsWith(field)) {
                sb.append(field).append(", ");
            }
        }
        values.append(")");

        sb.append(ID_FIELD).append(") VALUES ").append(values);

        return sb.toString();
    }

    /**
     * Creates a select clause from the DAO's fields.
     *
     * @return the string
     */
    protected String selectClause() {
        return DBQueryBuilder.generateCSVFromList(this.fields) + FROM_KEYWORD + this.tableName + " A";
    }

    /**
     * Helper class to set objects to a prepared statement with null control
     *
     * @param stmt the stmt
     * @param column the column
     * @param value the value
     * @throws SQLException the SQL exception
     */
    protected void setNullable(PreparedStatement stmt, int column, int value) throws SQLException {
        if (value > 0) {
            stmt.setInt(column, value);
        } else {
            stmt.setNull(column, Types.INTEGER);
        }
    }

    /**
     * Helper class to set objects to a prepared statement with null control
     *
     * @param stmt the stmt
     * @param column the column
     * @param value the value
     * @throws SQLException the SQL exception
     */
    protected void setNullable(PreparedStatement stmt, int column, long value) throws SQLException {
        if (value > 0) {
            stmt.setLong(column, value);
        } else {
            stmt.setNull(column, Types.BIGINT);
        }
    }

    /**
     * Helper class to set objects to a prepared statement with null control
     *
     * @param stmt the stmt
     * @param column the column
     * @param value the value
     * @throws SQLException the SQL exception
     */
    protected void setNullable(PreparedStatement stmt, int column, Boolean value) throws SQLException {
        if (value != null) {
            stmt.setBoolean(column, value);
        } else {
            stmt.setNull(column, Types.BOOLEAN);
        }
    }

    /**
     * Helper class to set objects to a prepared statement with null control
     *
     * @param stmt the stmt
     * @param column the column
     * @param id the id
     * @throws SQLException the SQL exception
     */
    protected void setNullable(PreparedStatement stmt, int column, ObjectId id) throws SQLException {
        if (ObjectId.isNull(id)) {
            stmt.setNull(column, Types.BIGINT);
        } else {
            setNullable(stmt, column, id.toLong());
        }
    }

    /**
     * Update a row in a table by id.
     *
     * @param object the object
     * @throws Exception the exception
     */
    public void update(T object) throws Exception {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String query = null;
        try {
            conn = DataBaseManager.getInstance().getConnection();
            query = updateClause();
            stmt = conn.prepareStatement(query);

            serialize(object, stmt);

            stmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage() + " -> " + query);
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see com.appgree.core.dao.processor.DataProcessor#process(java.sql.ResultSet)
     */
    @Override
    public T process(ResultSet resultSet) throws Exception {
        return deserialize(resultSet);
    }

    /**
     * Create an update statement from a DAO's fields
     *
     * @return the string
     */
    protected String updateClause() {
        if (updateClause == null) {
            synchronized (this) {
                if (updateClause == null) {
                    List<String> fields = allFields();
                    // ID_USER=?, TMST=?, ID_DEVICE=?, ID_DEVICE_TYPE=?, VERSION=? WHERE ID = ?"
                    updateClause = UPDATE_KEYWORD + this.tableName + SET_KEYWORD;
                    int index = 0;
                    for (String field : fields) {
                        index++;
                        if (ID_FIELD.equals(field)) {
                            continue;
                        }
                        updateClause += field + EQUALS_EXPRESSION;
                        if (index < fields.size()) {
                            updateClause += ", ";
                        }
                    }

                    updateClause += WHERE_KEYWORD + ID_FIELD + EQUALS_EXPRESSION;
                }
            }
        }

        return updateClause;
    }

    /**
     * Gets the table name.
     *
     * @return the table name
     */
    protected final String getTableName() {
        return this.tableName;
    }

    /**
     * Select from table.
     *
     * @return the from to where clause
     */
    protected FromToWhereClause selectFromTable() {
        return DBQueryBuilder.selectFromString(selectClause());
    }

    /**
     * Add a list of objects as rows in a table.
     *
     * @param objects the objects
     * @param intermediateCommits the intermediate commits
     * @throws Exception the exception
     */
    public void addAll(List<T> objects, boolean intermediateCommits) throws Exception {
        processAll(objects, new InsertStatementBuilder(), intermediateCommits);
    }

    /**
     * Add or update a list of objects as rows in a table
     *
     * @param objects the objects
     * @param intermediateCommits if intermediate commits are allowed
     * @throws Exception the exception
     */
    public void addOrUpdateAll(List<T> objects, boolean intermediateCommits) throws Exception {
        addOrUpdateAll(objects, this.fieldsToUpdate(), intermediateCommits);
    }

    /**
     * Update selected fields in a table from a list of objects.
     *
     * @param objects the objects
     * @param updateFields the update fields
     * @param intermediateCommits if intermediate commits are allowed
     * @throws Exception the exception
     */
    public void addOrUpdateAll(List<T> objects, List<String> updateFields, boolean intermediateCommits) throws Exception {
        processAll(objects, new UpdateStatementBuilder(updateFields), intermediateCommits);
    }

    /**
     * Apply a prepared statement to a list of objects.
     *
     * @param <K> the key type
     * @param objects the objects
     * @param processor the processor
     * @param intermediateCommits if intermediate commits are allowed
     * @throws Exception the exception
     */
    public <K extends Identifiable> void processAll(List<K> objects, StatementProcessor<K> processor, boolean intermediateCommits)
                    throws Exception {
        if (objects == null) {
            return;
        }

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            int numAccum = 0;
            boolean releasedConnection = true;
            for (K object : objects) {
                // Skip null objects
                if (object == null) {
                    continue;
                }
                // Identifier is mandatory
                if (ObjectId.isNull(object.getId())) {
                    throw new SQLException("The object must have a valid identifier to be added");
                }

                if (releasedConnection) {
                    conn = DataBaseManager.getInstance().getConnection();

                    stmt = processor.create(object, conn);

                    releasedConnection = false;
                }

                processor.setParams(object, stmt);

                stmt.addBatch();

                if (++numAccum == MAX_ACCUM_BATCH) {
                    stmt.executeBatch();
                    if (intermediateCommits) {
                        stmt.close();
                        stmt = null;
                        DataBaseManager.getInstance().commitConnection();
                        releasedConnection = true;
                    }
                    numAccum = 0;
                }
            }
            if (numAccum != 0) {
                stmt.executeBatch();
                if (intermediateCommits) {
                    stmt.close();
                    stmt = null;
                    DataBaseManager.getInstance().commitConnection();
                }
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
    }


    /**
     * The Class InsertStatementBuilder.
     */
    public class InsertStatementBuilder implements StatementProcessor<T> {

        /*
         * (non-Javadoc)
         *
         * @see com.appgree.core.dao.StatementProcessor#create(com.appgree.core.id.Identifiable, java.sql.Connection)
         */
        /**
         * Creates the.
         *
         * @param object the object
         * @param conn the conn
         * @return the prepared statement
         * @throws SQLException the SQL exception
         */
        @Override
        public PreparedStatement create(T object, Connection conn) throws SQLException {
            String insertClause = insertClause(true);
            return conn.prepareStatement(insertClause);
        }

        /*
         * (non-Javadoc)
         *
         * @see com.appgree.core.dao.StatementProcessor#setParams(com.appgree.core.id.Identifiable, java.sql.PreparedStatement)
         */
        /**
         * Sets the params.
         *
         * @param object the object
         * @param stmt the stmt
         * @throws Exception the exception
         */
        @Override
        public void setParams(T object, PreparedStatement stmt) throws Exception  {
            serialize(object, stmt);
        }
    }

    /**
     * The Class UpdateStatementBuilder.
     */
    public class UpdateStatementBuilder implements StatementProcessor<T> {

        /** The fields. */
        private List<String> fields;

        /**
         * Instantiates a new update statement builder.
         *
         * @param updateFields the update fields
         */
        public UpdateStatementBuilder(List<String> updateFields) {
            this.fields = updateFields;
        }

        /*
         * (non-Javadoc)
         *
         * @see com.appgree.core.dao.StatementProcessor#create(com.appgree.core.id.Identifiable, java.sql.Connection)
         */
        /**
         * Creates the.
         *
         * @param object the object
         * @param conn the conn
         * @return the prepared statement
         * @throws SQLException the SQL exception
         */
        @Override
        public PreparedStatement create(T object, Connection conn) throws SQLException {
            StringBuilder query = new StringBuilder(insertClause(false));
            query.append(ON_DUPLICATE_KEYWORD);
            int i;
            for (i = 0; i < fields.size() - 1; i++) {
                query.append(fields.get(i));
                query.append("=VALUES(");
                query.append(fields.get(i));
                query.append("), ");
            }
            query.append(fields.get(i));
            query.append("=VALUES(");
            query.append(fields.get(i));
            query.append(")");

            // Inserts
            return conn.prepareStatement(query.toString());
        }

        /*
         * (non-Javadoc)
         *
         * @see com.appgree.core.dao.StatementProcessor#setParams(com.appgree.core.id.Identifiable, java.sql.PreparedStatement)
         */
        /**
         * Sets the params.
         *
         * @param object the object
         * @param stmt the stmt
         * @throws Exception the exception
         */
        @Override
        public void setParams(T object, PreparedStatement stmt) throws Exception {
            serialize(object, stmt);
        }

    }

}
