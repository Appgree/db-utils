/*
 * Copyright 2013-2014 Appgree S.A. All rights reserved.
 */
package com.appgree.core.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.appgree.core.dao.query.builder.DBQueryBuilder;
import com.appgree.core.dao.query.builder.SQLClause;
import com.appgree.core.database.provider.DataBaseManager;
import com.appgree.core.id.Identifiable;
import com.appgree.core.id.ObjectId;


/**
 * The Class TestBaseDAO.
 */
public class TestBaseDAO extends BaseTestDAO {

    /**
     * The Class ItemDAO.
     */
    public class ItemDAO extends BaseDAO<Item> {

        /**
         * Instantiates a new item dao.
         */
        public ItemDAO() {
            super("Item");
            this.fields.add("ID");
        }

        /* (non-Javadoc)
         * @see com.appgree.core.dao.processor.DataSerializer#deserialize(java.sql.ResultSet)
         */
        @Override
        public Item deserialize(ResultSet resultSet) throws Exception {
            long id = resultSet.getLong(1);
            
            return new Item(ObjectId.fromLong(id));
        }

        /* (non-Javadoc)
         * @see com.appgree.core.dao.processor.DataSerializer#serialize(java.lang.Object, java.sql.PreparedStatement)
         */
        @Override
        public void serialize(Item object, PreparedStatement stmt) throws Exception {
            stmt.setLong(1, object.getId().toLong());
        }

    }

    /**
     * The Class Item.
     */
    public class Item implements Identifiable {

        /* (non-Javadoc)
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            return result;
        }

        /* (non-Javadoc)
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Item other = (Item) obj;
            if (!getOuterType().equals(other.getOuterType()))
                return false;
            if (id == null) {
                if (other.id != null)
                    return false;
            } else if (!id.equals(other.id))
                return false;
            return true;
        }

        /** The id. */
        private ObjectId id;

        /**
         * Instantiates a new item.
         *
         * @param id the id
         */
        public Item(ObjectId id) {
            this.id = id;
        }

        /* (non-Javadoc)
         * @see com.appgree.core.id.Identifiable#getId()
         */
        @Override
        public ObjectId getId() {
            return id;
        }

        /* (non-Javadoc)
         * @see com.appgree.core.id.Identifiable#setId(com.appgree.core.id.ObjectId)
         */
        @Override
        public void setId(ObjectId id) {
            this.id = id;
        }

        private TestBaseDAO getOuterType() {
            return TestBaseDAO.this;
        }

    }
    
    @BeforeClass
    public static void populateTable() throws SQLException {
        SQLClause createClause = DBQueryBuilder.createTable("ITEM").ifNotExists().withField("ID", Long.class).notNull();
        createClause.execute();
    }

    @AfterClass
    public static void dropTable() throws SQLException {
        Connection conn = DataBaseManager.getInstance().getConnection();
        Assert.assertNotNull(conn);
        SQLClause dropClause = DBQueryBuilder.dropTable("ITEM");
        dropClause.execute();
    }
    
    /**
     * Test several.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSeveral() throws Exception {
        BaseDAO<Item> itemDao = new ItemDAO();
        BaseDAO<Item> anotherItemDao = BaseDAO.getInstanceForClass(Item.class);
        Assert.assertEquals(itemDao, anotherItemDao);

        Random random = new Random();
        final Item newItem = new Item(ObjectId.fromLong(random .nextLong()));
        itemDao.add(newItem);
        Item foundItem = itemDao.findById(newItem.getId());
        Assert.assertNotNull(foundItem);
        Assert.assertEquals(newItem, foundItem);
        
        ItemDAO sameDAO = (ItemDAO) BaseDAO.getInstanceForClass(Item.class);
        Item sameItem = sameDAO.findById(newItem.getId());
        Assert.assertEquals(foundItem, sameItem);
    }
}
