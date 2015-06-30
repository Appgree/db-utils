# Appgree-db-utils

This is a yet another set of utility classes for database abstraction in Java. This library consists of 2 different tools:
 * Simple query builder. Java builder classes to create and execute SELECT queries
 * Base generic DAO class that implements CRUD DB operations for business objects
 
## License 

The software stands under Apache 2 License and comes with NO WARRANTY

## Features

 * Identifiable interface for business object stored in DB 
 * DBQueryBuilder utility class
 * DatabaseManager: singleton for connection management
 * BaseDAO: Generic abstract class for storing and retrieving Identifiable objects in the DB

## TODOs

 * DBQueryBuilder only supports CREATE and SELECT clauses and it's not meant to be complete.

## Usage

### DBQueryBuilder
 
 ```java
    SQLClause createClause = DBQueryBuilder.createTable(TABLE_NAME).ifNotExists()
                    .withField("ID", Long.class).notNull()
                    .withField("FIRST_NAME", String.class, 100);
    createClause.execute();
```

### DatabaseManager
 
 ```java
    final BasicDataBaseProvider provider = new BasicDataBaseProvider();
    provider.init("com.mysql.jdbc.Driver", "jdbc:mysql://localhost/database?user=<user>&password=<pwd>&useUnicode=true");
    DataBaseManager.getInstance().init(provider, 100, 100);
```

### BaseDAO
 
 ```java
    public class ItemDAO extends BaseDAO<Item> {
    
        public ItemDAO() {
            super("Item");
            this.fields.add("ID");
        }
        
        @Override
        public Item deserialize(ResultSet resultSet) throws Exception {
            long id = resultSet.getLong(1);
            
            return new Item(ObjectId.fromLong(id));
        }
        
        @Override
        public void serialize(Item object, PreparedStatement stmt) throws Exception {
            stmt.setLong(1, object.getId().toLong());
        }
    }
```


## Build

via Maven
