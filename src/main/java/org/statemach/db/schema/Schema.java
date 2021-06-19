package org.statemach.db.schema;

import org.statemach.db.sql.SchemaAccess;

import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;

public class Schema {
    public final String                 name;
    public final Map<String, TableInfo> tables;

    public Schema(String name, Map<String, TableInfo> tables) {
        this.name = name;
        this.tables = tables;
    }

    public static Schema from(SchemaAccess access) {
        Map<String, Map<String, ColumnInfo>> columnsByNameByTable = access.getTables()
            .mapValues(t -> t.toMap(c -> new Tuple2<>(c._1, new ColumnInfo(c._1, c._2))));

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
}
