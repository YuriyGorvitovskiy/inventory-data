package org.statemach.db.server;

import java.net.InetSocketAddress;
import java.util.function.Supplier;

import org.apache.commons.dbcp2.BasicDataSource;
import org.statemach.db.graphql.GraphQLHandler;
import org.statemach.db.jdbc.JDBC;
import org.statemach.db.jdbc.Vendor;
import org.statemach.db.rest.RestHandler;
import org.statemach.db.schema.Schema;
import org.statemach.db.sql.DataAccess;
import org.statemach.db.sql.SchemaAccess;
import org.statemach.db.sql.postgres.PostgresDataAccess;
import org.statemach.db.sql.postgres.PostgresSchemaAccess;
import org.statemach.db.version.VersionHandler;
import org.statemach.util.Http;
import org.statemach.util.Java;

import com.sun.net.httpserver.HttpServer;

import io.vavr.collection.HashMap;
import io.vavr.collection.Map;

public class Main {

    static interface Default {
        static final String DB_ADDRESS   = "localhost";
        static final String DB_PORT      = "5432";
        static final String DB_NAME      = "postgres";
        static final String DB_SCHEMA    = "public";
        static final String DB_MAX_TOTAL = "8";
        static final String DB_MAX_IDLE  = "8";
        static final String DB_MIN_IDLE  = "0";
        static final String HTTP_PORT    = "3702";
    }

    static interface Config {
        static final String DB_ADDRESS   = "DB_ADDRESS";
        static final String DB_PORT      = "DB_PORT";
        static final String DB_NAME      = "DB_NAME";
        static final String DB_USERNAME  = "DB_USERNAME";
        static final String DB_PASSWORD  = "DB_PASSWORD";
        static final String DB_SCHEMA    = "DB_SCHEMA";
        static final String DB_MAX_TOTAL = "DB_MAX_TOTAL";
        static final String DB_MAX_IDLE  = "DB_MAX_IDLE";
        static final String DB_MIN_IDLE  = "DB_MIN_IDLE";
        static final String HTTP_PORT    = "HTTP_PORT";
    }

    public static Supplier<Main> factory = () -> new Main(HashMap.ofAll(System.getenv()));

    final Map<String, String> config;

    Main(Map<String, String> config) {
        this.config = config;
    }

    JDBC configJDBC() {
        BasicDataSource dataSource = new BasicDataSource();

        String address  = config.getOrElse(Config.DB_ADDRESS, Default.DB_ADDRESS);
        String port     = config.getOrElse(Config.DB_PORT, Default.DB_PORT);
        String dbname   = config.getOrElse(Config.DB_NAME, Default.DB_NAME);
        String username = config.get(Config.DB_USERNAME)
            .getOrElseThrow(() -> new RuntimeException("Environment variable DB_USERNAME wasn't specified"));
        String password = config.get(Config.DB_PASSWORD)
            .getOrElseThrow(() -> new RuntimeException("Environment variable DB_PASSWORD wasn't specified"));
        int    maxTotal = Integer.parseInt(config.getOrElse(Config.DB_MAX_TOTAL, Default.DB_MAX_TOTAL));
        int    maxIdle  = Integer.parseInt(config.getOrElse(Config.DB_MAX_IDLE, Default.DB_MAX_IDLE));
        int    minIdle  = Integer.parseInt(config.getOrElse(Config.DB_MIN_IDLE, Default.DB_MIN_IDLE));

        dataSource.setDriverClassName(org.postgresql.Driver.class.getName());
        dataSource.setUrl(Java.format("jdbc:postgresql://${0}:${1}/${2}", address, port, dbname));
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setMaxTotal(maxTotal);
        dataSource.setMaxIdle(maxIdle);
        dataSource.setMinIdle(minIdle);

        return new JDBC(Vendor.POSTGRES, dataSource);
    }

    HttpServer build() throws Exception {
        String schemaName = config.getOrElse(Config.DB_SCHEMA, Default.DB_SCHEMA);

        JDBC         jdbc         = configJDBC();
        SchemaAccess schemaAccess = new PostgresSchemaAccess(jdbc, schemaName);
        Schema       schema       = Schema.from(schemaAccess);
        DataAccess   dataAccess   = PostgresDataAccess.of(jdbc, schemaName);

        HttpServer server = HttpServer.create();
        server.createContext("/", Http.errorHandler(new VersionHandler()));
        server.createContext("/rest", Http.errorHandler(RestHandler.of(schema, dataAccess)));
        server.createContext("/graphql", Http.errorHandler(GraphQLHandler.build(schema, schemaAccess, dataAccess)));

        return server;
    }

    public void run() {
        int port = Integer.parseInt(config.getOrElse(Config.HTTP_PORT, Default.HTTP_PORT));

        Java.soft(() -> {
            HttpServer server = build();
            server.bind(new InetSocketAddress(port), 0);
            server.start();
        });
    }

    public static void main(final String[] args) {
        factory.get().run();
    }
}
