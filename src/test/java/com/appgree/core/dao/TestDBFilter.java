/*
 * Copyright 2013-2014 Appgree S.A. All rights reserved.
 */
package com.appgree.core.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.appgree.core.dao.processor.DataProcessor;
import com.appgree.core.dao.query.builder.DBQueryBuilder;
import com.appgree.core.dao.query.builder.SQLClause;
import com.appgree.core.database.provider.DataBaseManager;

/**
 * The Class TestDBFilter.
 */
public class TestDBFilter extends BaseTestDAO {

    
    private static final String TABLE_NAME = "MY_TABLE";

    @BeforeClass
    public static void populateTable() throws Exception {
        SQLClause createClause = DBQueryBuilder.createTable(TABLE_NAME).ifNotExists()
                        .withField("ID", Long.class).notNull()
                        .withField("FIRST_NAME", String.class, 100);
        createClause.execute();
        DBQueryBuilder.execute("INSERT INTO " + TABLE_NAME + " (ID, FIRST_NAME) VALUES (1, 'Jose')");
        DBQueryBuilder.execute("INSERT INTO " + TABLE_NAME + " (ID, FIRST_NAME) VALUES (2, 'Juan')");
        DBQueryBuilder.execute("INSERT INTO " + TABLE_NAME + " (ID, FIRST_NAME) VALUES (3, 'Miguel')");
        DBQueryBuilder.execute("INSERT INTO " + TABLE_NAME + " (ID, FIRST_NAME) VALUES (4, 'Ana')");
        DBQueryBuilder.execute("INSERT INTO " + TABLE_NAME + " (ID, FIRST_NAME) VALUES (5, 'Maria')");
        DBQueryBuilder.execute("INSERT INTO " + TABLE_NAME + " (ID, FIRST_NAME) VALUES (6, 'Andrea')");
        DBQueryBuilder.execute("INSERT INTO " + TABLE_NAME + " (ID, FIRST_NAME) VALUES (7, 'Israel')");
        DBQueryBuilder.execute("INSERT INTO " + TABLE_NAME + " (ID, FIRST_NAME) VALUES (8, 'David')");
        DBQueryBuilder.execute("INSERT INTO " + TABLE_NAME + " (ID, FIRST_NAME) VALUES (9, 'Javier')");
        DBQueryBuilder.execute("INSERT INTO " + TABLE_NAME + " (ID, FIRST_NAME) VALUES (10, 'Eduardo')");
    }

    @AfterClass
    public static void dropTable() throws SQLException {
        Connection conn = DataBaseManager.getInstance().getConnection();
        Assert.assertNotNull(conn);
        SQLClause dropClause = DBQueryBuilder.dropTable(TABLE_NAME);
        dropClause.execute();
    }    
    /**
     * Test select.
     */
    @Test
    public void testSelect() {
        assertEquals("SELECT ID FROM " + TABLE_NAME, DBQueryBuilder.select("ID").from(TABLE_NAME).toString());
        String fields[] = {"ID1", "ID2", "ID3", "ID4"};
        assertEquals("SELECT ID1, ID2, ID3, ID4 FROM " + TABLE_NAME, DBQueryBuilder.select(fields).from(TABLE_NAME).toString());
    }

    /**
     * Test from.
     */
    @Test
    public void testFrom() {
        assertEquals("SELECT ID FROM " + TABLE_NAME, DBQueryBuilder.select("ID").from(TABLE_NAME).toString());
        String tables[] = {"MY_TABLE1", "MY_TABLE2", "MY_TABLE3", "MY_TABLE4"};
        assertEquals("SELECT ID FROM MY_TABLE1, MY_TABLE2, MY_TABLE3, MY_TABLE4", DBQueryBuilder.select("ID").from(tables).toString());
        assertEquals("SELECT ID FROM MY_TABLE1 JOIN MY_TABLE2", DBQueryBuilder.select("ID").from("MY_TABLE1").join("MY_TABLE2").toString());
        assertEquals("SELECT COUNT(*) FROM MY_TABLE1", DBQueryBuilder.selectCount().from("MY_TABLE1").toString());
    }

    /**
     * Test where.
     */
    @Test
    public void testWhere() {
        assertEquals("SELECT ID FROM " + TABLE_NAME + " WHERE ID = ?", DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").equal(1).toString());
        assertEquals("SELECT ID FROM " + TABLE_NAME + " WHERE NAME = ?", DBQueryBuilder.select("ID").from(TABLE_NAME).where("NAME").equal("ME").toString());
        assertEquals("SELECT ID FROM " + TABLE_NAME + " WHERE ID > ?", DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").greaterThan(1).toString());
        assertEquals("SELECT ID FROM " + TABLE_NAME + " WHERE ID >= ?", DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").greaterOrEqualThan(1)
                        .toString());
        assertEquals("SELECT ID FROM " + TABLE_NAME + " WHERE ID < ?", DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").lowerThan(1).toString());
        assertEquals("SELECT ID FROM " + TABLE_NAME + " WHERE ID <= ?", DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").lowerOrEqualThan(1).toString());
    }

    /**
     * Test compound where.
     */
    @Test
    public void testCompoundWhere() {
        assertEquals("SELECT ID FROM " + TABLE_NAME + " WHERE ID = ? AND NAME = ?",
                        DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").equal(1).and("NAME").equal("ME").toString());
        assertEquals("SELECT ID FROM " + TABLE_NAME + " WHERE ID = ? OR NAME = ?", DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").equal(1).or("NAME")
                        .equal("ME").toString());
        assertEquals("SELECT ID FROM " + TABLE_NAME + " WHERE ID = ? ORDER BY ID",
                        DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").equal(1).orderBy("ID").toString());
        assertEquals("SELECT ID FROM " + TABLE_NAME + " WHERE ID = ? ORDER BY ID",
                        DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").equal(1).orderBy("ID").asc().toString());
        assertEquals("SELECT ID FROM " + TABLE_NAME + " WHERE ID = ? ORDER BY ID DESC ", DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").equal(1)
                        .orderBy("ID").desc().toString());
        assertEquals("SELECT ID FROM " + TABLE_NAME + " WHERE ID = ?", DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").equal(1).limit(0).toString());
        assertEquals("SELECT ID FROM " + TABLE_NAME + " WHERE ID = ? LIMIT ?", DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").equal(1).limit(2)
                        .toString());
        assertEquals("SELECT ID FROM " + TABLE_NAME + " WHERE ID = ? ORDER BY ID LIMIT ?", DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").equal(1)
                        .orderBy("ID").limit(2).toString());
    }

    /**
     * Test illegal where combinations.
     */
    @Test
    public void testIllegalWhereCombinations() {
        try {
            DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").equal(1).asc().toString();
            fail("Cannot use ASC before ORDER BY");
        } catch (Exception e) {
        }
        try {
            DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").equal(1).orderBy("ID").or("ID").toString();
            fail("Cannot use ORDER BY before OR");
        } catch (Exception e) {
        }
    }

    /**
     * Test prepared statement.
     *
     * @throws SQLException the SQL exception
     */
    @Test
    public void testPreparedStatement() throws SQLException {
        SQLClause query = null;
        ResultSet res = null;
        try {
            query = DBQueryBuilder.select("ID").from("WRONG_TABLE_NAME").where("ID").equal(1);
            res = query.execute();
            fail("Should have failed!");
        } catch (Exception e) {
        }
        try {
            query = DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").equal(1);
            res = assertExecuteQuery(query);
            Assert.assertEquals(res.getLong(1), 1);
            res.close();

            query = DBQueryBuilder.select("FIRST_NAME").from(TABLE_NAME).where("FIRST_NAME").equal("Juan");
            res = assertExecuteQuery(query);
            Assert.assertEquals(res.getString(1), "Juan");
            res.close();

            query = DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").equal(1).and("FIRST_NAME").equal("Juan");
            res = query.execute();
            Assert.assertNotNull(res);
            Assert.assertFalse(res.next());

            query = DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").equal(1).or("FIRST_NAME").equal("Juan");
            res = assertExecuteQuery(query);
            Assert.assertEquals(res.getLong(1), 1);
            res.close();

            query = DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").equal(1).orderBy("ID");
            res = assertExecuteQuery(query);
            Assert.assertEquals(res.getLong(1), 1);
            res.close();

            query = DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").equal(1).orderBy("ID").asc();
            res = assertExecuteQuery(query);
            Assert.assertEquals(res.getLong(1), 1);
            res.close();

            query = DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").equal(1).orderBy("ID").desc();
            res = assertExecuteQuery(query);
            Assert.assertEquals(res.getLong(1), 1);
            res.close();

            query = DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").equal(1).limit(1);
            res = assertExecuteQuery(query);
            Assert.assertEquals(res.getLong(1), 1);
            res.close();

            query = DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").greaterThan(1).limit(2);
            res = assertExecuteQuery(query);
            Assert.assertEquals(res.getLong(1), 2);
            res.close();

            query = DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").greaterOrEqualThan(1).limit(2);
            res = assertExecuteQuery(query);
            Assert.assertEquals(res.getLong(1), 1);
            res.close();

            query = DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").greaterThan(1).orderBy("ID").limit(2);
            res = assertExecuteQuery(query);
            Assert.assertEquals(res.getLong(1), 2);
            res.close();

            query = DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").greaterThan(1).orderBy("ID").desc().limit(2);
            res = assertExecuteQuery(query);
            Assert.assertNotSame(res.getLong(1), 2);
            res.close();

        } catch (SQLException e) {
            fail(e.getMessage());
        } finally {
            if (res != null) {
                res.close();
            }
            if (query != null) {
                query.close();
            }
        }

    }

    /**
     * Test reuse prepared statement.
     *
     * @throws SQLException the SQL exception
     */
    @Test
    public void testReusePreparedStatement() throws SQLException {
        SQLClause query = DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").greaterThan().orderBy("ID").limit(null);
        ResultSet res = null;
        try {
            Object[] args1 = {new Long(1), new Integer(2)};
            res = assertExecuteQuery(query, Arrays.asList(args1));
            Assert.assertEquals(res.getLong(1), 2);
            res.close();

            Object[] args2 = {new Long(9), new Integer(2)};
            res = assertExecuteQuery(query, Arrays.asList(args2));
            Assert.assertEquals(res.getLong(1), 10);
            res.close();

        } catch (Exception e) {
            fail(e.getMessage());
        } finally {
            if (res != null) {
                res.close();
            }
            if (query != null) {
                query.close();
            }
        }

        try {
            Object[] args3 = {new Long(9)};
            res = assertExecuteQuery(query, Arrays.asList(args3));
            fail("Wrong number of args in call to preparedStatement");
        } catch (SQLException e) {
        } finally {
            if (res != null) {
                res.close();
            }
            if (query != null) {
                query.close();
            }
        }

    }

    /**
     * Test execute with processor.
     *
     * @throws Exception the exception
     */
    @Test
    public void testExecuteWithProcessor() throws Exception {
        SQLClause query = null;
        try {
            query = DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").equal(1);
            Assert.assertEquals(query.execute(new AssertIdProcessor(1L)), 1);

            query = DBQueryBuilder.select("ID").from(TABLE_NAME).where("ID").greaterThan(1).orderBy("ID").desc().limit(5);
            Assert.assertEquals(query.execute(new AssertIdGreaterThanProcessor(1L)), 5);

        } catch (SQLException e) {
            fail(e.getMessage());
        } finally {
            if (query != null) {
                query.close();
            }
        }
    }

    /**
     * The Class AssertIdProcessor.
     */
    class AssertIdProcessor implements DataProcessor<Boolean> {

        /** The value. */
        private Object value;

        /**
         * Instantiates a new assert id processor.
         *
         * @param value the value
         */
        public AssertIdProcessor(Object value) {
            this.value = value;
        }

        /* (non-Javadoc)
         * @see com.appgree.core.dao.processor.DataProcessor#process(java.sql.ResultSet)
         */
        @Override
        public Boolean process(ResultSet resultSet) throws Exception {
            Assert.assertNotNull(resultSet);
            Object actualValue = resultSet.getObject(1);
            Assert.assertEquals(this.value, actualValue);

            return true;
        }

    }

    /**
     * The Class AssertIdGreaterThanProcessor.
     */
    class AssertIdGreaterThanProcessor implements DataProcessor<Boolean> {

        /** The value. */
        private Long value;

        /**
         * Instantiates a new assert id greater than processor.
         *
         * @param value the value
         */
        public AssertIdGreaterThanProcessor(Long value) {
            this.value = value;
        }

        /* (non-Javadoc)
         * @see com.appgree.core.dao.processor.DataProcessor#process(java.sql.ResultSet)
         */
        @Override
        public Boolean process(ResultSet resultSet) throws Exception {
            Assert.assertNotNull(resultSet);
            Long actualValue = resultSet.getLong(1);
            Assert.assertTrue(this.value < actualValue);

            return true;
        }

    }

    /**
     * Assert execute query.
     *
     * @param query the query
     * @param params the params
     * @return the result set
     * @throws SQLException the SQL exception
     */
    private ResultSet assertExecuteQuery(SQLClause query, List<Object> params) throws SQLException {
        ResultSet res;
        if (params == null) {
            res = query.execute();
        } else {
            res = query.execute(params);
        }
        Assert.assertNotNull(res);
        Assert.assertTrue(res.next());

        return res;
    }

    /**
     * Assert execute query.
     *
     * @param query the query
     * @return the result set
     * @throws SQLException the SQL exception
     */
    private ResultSet assertExecuteQuery(SQLClause query) throws SQLException {
        return assertExecuteQuery(query, null);
    }

}
