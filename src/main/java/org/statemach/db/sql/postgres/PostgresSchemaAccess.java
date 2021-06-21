package org.statemach.db.sql.postgres;

import org.statemach.db.jdbc.JDBC;
import org.statemach.db.schema.ColumnInfo;
import org.statemach.db.schema.ForeignKey;
import org.statemach.db.schema.PrimaryKey;
import org.statemach.db.sql.SchemaAccess;
import org.statemach.util.Java;

import com.yg.util.DB;

import io.vavr.Tuple2;
import io.vavr.Tuple3;
import io.vavr.Tuple4;
import io.vavr.collection.List;
import io.vavr.collection.Map;

public class PostgresSchemaAccess implements SchemaAccess {
    static final String QUERY_FOR_ALL_FOREIGN_KEYS = Java.resource("QueryForAllForeignKeys.sql");
    static final String QUERY_FOR_ALL_PRIMARY_KEYS = Java.resource("QueryForAllPrimaryKeys.sql");
    static final String QEURY_FOR_ALL_TABLES       = Java.resource("QueryForAllTables.sql");

    public final JDBC   jdbc;
    public final String schemaName;

    public PostgresSchemaAccess(JDBC jdbc, String schemaName) {
        this.jdbc = jdbc;
        this.schemaName = schemaName;
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public Map<String, List<ColumnInfo>> getAllTables() {
        return DB.query(QEURY_FOR_ALL_TABLES,
                ps -> ps.setString(1, schemaName),
                rs -> new Tuple2<>(
                        rs.getString(1), // Table name
                        new ColumnInfo(
                                rs.getString(2),
                                PostgresDataType.getByName(rs.getString(3)))))
            .groupBy(t -> t._1)
            .mapValues(l -> l.map(c -> c._2));
    }

    @Override
    public List<PrimaryKey> getAllPrimaryKeys() {
        return jdbc.query(QUERY_FOR_ALL_PRIMARY_KEYS,
                ps -> ps.setString(1, schemaName),
                rs -> new Tuple3<>(rs.getString(1), rs.getString(2), rs.getString(3)))
            .groupBy(t -> t._1)
            .values()
            .map(l -> new PrimaryKey(l.get()._1, l.get()._2, l.map(t -> t._3)))
            .toList();
    }

    @Override
    public List<ForeignKey> getAllForeignKeys() {
        return jdbc.query(QUERY_FOR_ALL_FOREIGN_KEYS,
                ps -> ps.setString(1, schemaName),
                rs -> new Tuple4<>(rs.getString(1),
                        rs.getString(2),
                        rs.getString(3),
                        new ForeignKey.Match(rs.getString(4), rs.getString(5))))
            .groupBy(t -> t._1)
            .values()
            .map(l -> new ForeignKey(l.get()._1, l.get()._2, l.get()._3, l.map(t -> t._4)))
            .toList();
    }

}
