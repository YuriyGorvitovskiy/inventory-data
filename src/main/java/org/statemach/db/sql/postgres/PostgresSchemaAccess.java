package org.statemach.db.sql.postgres;

import org.statemach.db.jdbc.Extract;
import org.statemach.db.jdbc.JDBC;
import org.statemach.db.jdbc.Vendor;
import org.statemach.db.schema.ColumnInfo;
import org.statemach.db.schema.CompositeType;
import org.statemach.db.schema.ForeignKey;
import org.statemach.db.schema.PrimaryKey;
import org.statemach.db.sql.SQL;
import org.statemach.db.sql.SchemaAccess;
import org.statemach.util.Java;

import io.vavr.Tuple2;
import io.vavr.Tuple3;
import io.vavr.Tuple4;
import io.vavr.collection.List;
import io.vavr.collection.Map;

public class PostgresSchemaAccess implements SchemaAccess {
    static final String QUERY_FOR_ALL_FOREIGN_KEYS = Java.resource("QueryForAllForeignKeys.sql");
    static final String QUERY_FOR_ALL_PRIMARY_KEYS = Java.resource("QueryForAllPrimaryKeys.sql");
    static final String QEURY_FOR_ALL_TABLES       = Java.resource("QueryForAllTables.sql");
    static final String QUERY_FOR_ALL_TYPES        = Java.resource("QueryForAllTypes.sql");
    static final String CREATE_TYPE                = Java.resource("CreateCompositeType.sql");
    static final String DROP_TYPE                  = Java.resource("DropCompositeType.sql");

    public final JDBC   jdbc;
    public final String schemaName;

    public PostgresSchemaAccess(JDBC jdbc, String schemaName) {
        this.jdbc = jdbc;
        this.schemaName = schemaName;
    }

    @Override
    public Vendor getVendor() {
        return jdbc.getVendor();
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public Map<String, List<ColumnInfo>> getAllTables() {
        return jdbc.query(QEURY_FOR_ALL_TABLES,
                ps -> ps.setString(1, schemaName),
                rs -> new Tuple2<>(
                        rs.getString(1), // Table name
                        ColumnInfo.of(
                                rs.getString(2),
                                PostgresDataType.getByName(rs.getString(3)),
                                Extract.INTEGER.get(rs, 4))))
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

    @Override
    public List<CompositeType> getAllCompositeTypes() {
        return jdbc.query(QUERY_FOR_ALL_TYPES,
                ps -> ps.setString(1, schemaName),
                rs -> new Tuple2<>(
                        rs.getString(1), // Type name
                        ColumnInfo.of(
                                rs.getString(2),
                                PostgresDataType.getByName(rs.getString(3)),
                                Extract.INTEGER.get(rs, 4))))
            .groupBy(t -> t._1)
            .mapValues(l -> l.map(c -> c._2))
            .map(t -> new CompositeType(t._1, t._2))
            .toList();
    }

    @Override
    public void createCompositeType(CompositeType type) {
        String fields = type.fields
            .map(f -> SQL.INDENT + f.name + SQL.SPACE + f.type.name
                    + (f.size.isDefined() ? SQL.OPEN + f.size.get() + SQL.CLOSE : ""))
            .mkString(SQL.COMMA + SQL.NEXT_LINE);
        String sql    = Java.format(CREATE_TYPE, schemaName, type.name, fields);
        jdbc.execute(sql, ps -> {});
    }

    @Override
    public void dropCompositeType(String typeName) {
        String sql = Java.format(DROP_TYPE, schemaName, typeName);
        jdbc.execute(sql, ps -> {});
    }
}
