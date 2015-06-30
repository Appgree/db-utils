/*
 * Copyright 2013-2014 Appgree S.A. All rights reserved.
 */
package com.appgree.core.dao.query.builder;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;


/**
 * Inner class for CREATE TABLE clauses.
 */
public class CreateTableClause extends SQLClause{

    /**
     * The Enum FieldType.
     */
    enum FieldType {
        
        /** The int. */
        INT,
        
        /** The char. */
        CHAR,
        
        /** The varchar. */
        VARCHAR,
        
        /** The bool. */
        BOOL,
        
        /** The bigint. */
        BIGINT,
        
        /** The double. */
        DOUBLE,
        
        /** The unknown. */
        UNKNOWN
    }
    
    /**
     * The Class FieldDefinition stores table field type and atributes.
     */
    public class FieldDefinition {
        
        /** The type. */
        FieldType type;
        
        /** The length. */
        int length;
        
        /** The is primary key. */
        boolean isPrimaryKey;
        
        /** The is unique. */
        boolean isUnique;
        
        /** The is not null. */
        boolean isNotNull;
        
        /** The is auto increment. */
        boolean isAutoIncrement;
        
        /** The default value. */
        String defaultValue;
        
        /** The name. */
        private String name;

        /**
         * Instantiates a new field definition.
         *
         * @param fieldName the field name
         * @param clazz the clazz
         * @throws SQLFeatureNotSupportedException the SQL feature not supported exception
         */
        public FieldDefinition(String fieldName, Class<?> clazz) throws SQLFeatureNotSupportedException {
            if (clazz == null || clazz.isArray() || clazz.isAnnotation() || clazz.isAnonymousClass() || clazz.isInterface() || clazz.isLocalClass() || clazz.isMemberClass() || clazz.isSynthetic()) {
                throw new SQLFeatureNotSupportedException("Cannot convert class: " + clazz != null?clazz.getName():null + " to sql field");
            }

            this.name = fieldName;
            String typeName = clazz.getName().substring(clazz.getName().lastIndexOf(".") + 1).toLowerCase();
            switch(typeName) {
                case "byte":
                case "int":
                case "integer":
                    this.type = FieldType.INT;
                    break;
                case "long":
                    this.type = FieldType.BIGINT;
                    break;
                case "string":
                    this.type = FieldType.VARCHAR;
                    break;
                case "float":
                case "double":
                    this.type = FieldType.DOUBLE;
                    break;
                case "bool":
                case "boolean":
                    this.type = FieldType.BOOL;
                    break;
                default:
                    this.type = FieldType.UNKNOWN;
                    throw new SQLFeatureNotSupportedException("No SQL field type for class: " + clazz);
            }
        }

        /* (non-Javadoc)
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            StringBuilder fieldString = new StringBuilder(name);
            fieldString.append(" ");
            fieldString.append(type);
            if (length > 0) {
                fieldString.append("(");
                fieldString.append(length);
                fieldString.append(") ");
            }
            if (isNotNull) {
                fieldString.append(" NOT NULL");
            }
            if (!StringUtils.isEmpty(defaultValue)) {
                fieldString.append(" ");
                fieldString.append(defaultValue);
            }
            if (isAutoIncrement) {
                fieldString.append(" AUTO_INCREMENT");
            }
            if (isPrimaryKey) {
                fieldString.append("PRIMARY KEY (");
                fieldString.append(name);
                fieldString.append(")");
                if (fieldString.length() > 0) {
                    fieldString.append(", ");
                }
            }
            if (isUnique) {
                if (fieldString.length() > 0) {
                    fieldString.append(", ");
                }
                fieldString.append("UNIQUE ");
                fieldString.append(name);
                fieldString.append(" (");
                fieldString.append(name);
                fieldString.append(")");
            }
            
            return fieldString.toString();
        }
    }

    /** The table name. */
    private String tableName;
    
    /** The fields. */
    private Map<String, FieldDefinition> fields;
    
    /** The last field. */
    private String lastField;
    
    /** The check if exists. */
    private boolean checkIfExists;

    /**
     * Instantiates a new creates the clause.
     *
     * @param string the string
     */
    public CreateTableClause(String string) {
        this.fields = new HashMap<String, CreateTableClause.FieldDefinition>();
        this.setTableName(string);
    }

    /**
     * Adds an 'IF NOT EXISTS' clause.
     *
     * @return the creates the clause
     */
    public CreateTableClause ifNotExists() {
        this.checkIfExists = true;
        
        return this;
    }
    
    /**
     * Gets the table name.
     *
     * @return the table name
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Sets the table name.
     *
     * @param tableName the new table name
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Adds a new field to the table with a class.
     *
     * @param string the string
     * @param clazz the clazz
     * @return the creates the clause
     * @throws SQLException the SQL exception
     */
    public CreateTableClause withField(String string, Class<?> clazz) throws SQLException {
        if (fields != null && fields.containsKey(string)) {
            throw new SQLSyntaxErrorException("Duplicate field name: " + string);
        }
        fields.put(string, new FieldDefinition(string, clazz));
        lastField = string;
        
        return this;
    }

    /**
     * Adds a new field to the table with a class and length.
     *
     * @param string the string
     * @param clazz the clazz
     * @param length the length
     * @return the creates the clause
     * @throws SQLException the SQL exception
     */
    public CreateTableClause withField(String string, Class<?> clazz, int length) throws SQLException {
        withField(string, clazz);
        FieldDefinition field = getLastField();
        field.length = length;
        
        return this;
    }

    /**
     * Sets a field as primary key.
     *
     * @return the creates the clause
     * @throws SQLException the SQL exception
     */
    public CreateTableClause primaryKey() throws SQLException {
        FieldDefinition field = getLastField();
        field.isPrimaryKey = true;
        
        return this;        
    }

    /**
     * Set last field as Unique.
     *
     * @return the creates the clause
     * @throws SQLException the SQL exception
     */
    public CreateTableClause unique() throws SQLException {
        FieldDefinition field = getLastField();
        field.isUnique = true;
        
        return this;        
    }

    /**
     * Set last field as Not null.
     *
     * @return the creates the clause
     * @throws SQLException the SQL exception
     */
    public CreateTableClause notNull() throws SQLException {
        FieldDefinition field = getLastField();
        field.isNotNull = true;
        
        return this;        
    }
    
    private FieldDefinition getLastField() throws SQLException {
        if (StringUtils.isEmpty(lastField)) {
            throw new SQLSyntaxErrorException("Missing field for primary key! ");
        }
        FieldDefinition field = fields.get(lastField);
        return field;
    }

    /* (non-Javadoc)
     * @see com.appgree.core.dao.query.builder.SQLClause#toString()
     */
    @Override
    public String toString() {
        StringBuilder queryString = new StringBuilder("CREATE TABLE ");
        if (checkIfExists) {
            queryString.append("IF NOT EXISTS ");
        }
        queryString.append(tableName);
        queryString.append(" (");
        queryString.append(StringUtils.join(fields.values(), ", "));
        queryString.append(");");

        return queryString.toString();
    }

}
