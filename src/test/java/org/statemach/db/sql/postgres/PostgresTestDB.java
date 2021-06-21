package org.statemach.db.sql.postgres;

import org.apache.commons.dbcp2.BasicDataSource;
import org.statemach.db.jdbc.JDBC;
import org.statemach.util.Java;

import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;

public class PostgresTestDB {

    static interface Config {
        static final String DB_ADDRESS  = "TEST_DB_ADDRESS";
        static final String DB_PORT     = "TEST_DB_PORT";
        static final String DB_NAME     = "TEST_DB_NAME";
        static final String DB_USERNAME = "TEST_DB_USERNAME";
        static final String DB_PASSWORD = "TEST_DB_PASSWORD";
        static final String DB_SCHEMA   = "TEST_DB_SCHEMA";
    }

    static interface SQL {
        static final String CHECK_SCHEMA               = Java.resource("CheckSchema.sql");
        static final String CREATE_FOREIGN_KEYS_FIRST  = Java.resource("CreateForeignKeysFirst.sql");
        static final String CREATE_FOREIGN_KEYS_SECOND = Java.resource("CreateForeignKeysSecond.sql");
        static final String CREATE_FOREIGN_KEYS_THIRD  = Java.resource("CreateForeignKeysThird.sql");
        static final String CREATE_SCHEMA              = Java.resource("CreateSchema.sql");
        static final String CREATE_TABLE_FIRST         = Java.resource("CreateTableFirst.sql");
        static final String CREATE_TABLE_SECOND        = Java.resource("CreateTableSecond.sql");
        static final String CREATE_TABLE_THIRD         = Java.resource("CreateTableThird.sql");
        static final String CREATE_TABLE_VERSION       = Java.resource("CreateTableVersion.sql");
        static final String DROP_SCHEMA                = Java.resource("DropSchema.sql");
        static final String INSERT_PRODUCT_VERSION     = Java.resource("InsertProductVersion.sql");
        static final String SELECT_PRODUCT_VERSION     = Java.resource("SelectProductVersion.sql");

    }

    public final static Map<String, String> config  = HashMap.ofAll(System.getenv());
    public final static BasicDataSource     pool    = createDBConnectionPool();
    public final static JDBC                jdbc    = new JDBC(pool);
    public final static String              schema  = config.getOrElse(Config.DB_SCHEMA, "test").toLowerCase();
    public final static String              product = "inventory-data";
    public final static String              version = "0.0.5";

    static boolean prepared = false;

    static BasicDataSource createDBConnectionPool() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(org.postgresql.Driver.class.getName());
        dataSource.setUrl("jdbc:postgresql://" +
                config.getOrElse(Config.DB_ADDRESS, "localhost") + ":" +
                config.getOrElse(Config.DB_PORT, "31703") + "/" +
                config.getOrElse(Config.DB_NAME, "inventory"));
        dataSource.setUsername(config.getOrElse(Config.DB_USERNAME, "admin"));
        dataSource.setPassword(config.getOrElse(Config.DB_PASSWORD, "M9bmiR8iuod9wFHskgFu"));
        return dataSource;
    }

    public static void setup() {
        if (prepared) {
            return;
        }

        prepared = true;
        if (checkSchema() && checkVersion()) {
            return;
        }
        createSchema();
    }

    static boolean checkSchema() {
        List<Boolean> hasSchema = jdbc.query(
                SQL.CHECK_SCHEMA,
                ps -> ps.setString(1, schema),
                rs -> rs.getBoolean(1));

        return hasSchema.getOrElse(false);
    }

    static boolean checkVersion() {
        List<Tuple2<String, String>> info = jdbc.query(
                Java.format(SQL.SELECT_PRODUCT_VERSION, schema),
                ps -> {},
                rs -> new Tuple2<>(rs.getString(1), rs.getString(2)));
        if (1 != info.size()) {
            return false;
        }
        return product.equals(info.get()._1) && version.equals(info.get()._2);
    }

    static void createSchema() {
        jdbc.execute(Java.format(SQL.DROP_SCHEMA, schema), ps -> {});
        jdbc.execute(Java.format(SQL.CREATE_SCHEMA, schema), ps -> {});
        jdbc.execute(Java.format(SQL.CREATE_TABLE_VERSION, schema), ps -> {});
        jdbc.execute(Java.format(SQL.CREATE_TABLE_FIRST, schema), ps -> {});
        jdbc.execute(Java.format(SQL.CREATE_TABLE_SECOND, schema), ps -> {});
        jdbc.execute(Java.format(SQL.CREATE_TABLE_THIRD, schema), ps -> {});
        jdbc.execute(Java.format(SQL.CREATE_FOREIGN_KEYS_FIRST, schema), ps -> {});
        jdbc.execute(Java.format(SQL.CREATE_FOREIGN_KEYS_SECOND, schema), ps -> {});
        jdbc.execute(Java.format(SQL.CREATE_FOREIGN_KEYS_THIRD, schema), ps -> {});
        jdbc.execute(Java.format(SQL.INSERT_PRODUCT_VERSION, schema), ps -> {
            ps.setString(1, product);
            ps.setString(2, version);
        });
    }
}
