package org.statemach.db.schema;

import java.util.Objects;

import org.statemach.db.sql.SchemaAccess;
import org.statemach.util.Java;

import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;

public class Schema {
    public final String                 name;
    public final Map<String, TableInfo> tables;

    private final int hash;

    public Schema(String name, Map<String, TableInfo> tables) {
        this.name = name;
        this.tables = tables;

        this.hash = Objects.hash(name, tables);
    }

    public static Schema from(SchemaAccess access) {
        Map<String, Map<String, ColumnInfo>> columnsByNameByTable = access.getAllTables()
            .mapValues(l -> l.toMap(c -> c.name, c -> c));

        Map<String, PrimaryKey> primaryByTable = access.getAllPrimaryKeys()
            .toMap(p -> new Tuple2<>(p.table, p));

        List<ForeignKey> foreignKeys = access.getAllForeignKeys();

        Map<String, Map<String, ForeignKey>> incomingByNameByTable = foreignKeys
            .groupBy(f -> f.toTable)
            .mapValues(s -> s.toMap(f -> new Tuple2<>(f.name, f)));

        Map<String, Map<String, ForeignKey>> outgoingByNameByTable = foreignKeys
            .groupBy(f -> f.fromTable)
            .mapValues(s -> s.toMap(f -> new Tuple2<>(f.name, f)));

        Map<String, TableInfo> tablesByName = columnsByNameByTable
            .toMap(t -> new Tuple2<>(t._1,
                    new TableInfo(t._1,
                            t._2,
                            primaryByTable.get(t._1).getOrNull(),
                            incomingByNameByTable.get(t._1).getOrElse(HashMap.empty()),
                            outgoingByNameByTable.get(t._1).getOrElse(HashMap.empty()))));

        return new Schema(access.getSchemaName(), tablesByName);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object other) {
        return Java.equalsByFields(this, other, t -> t.name, t -> t.tables);
    }
}
