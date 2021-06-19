package com.yg.inventory.model.db;

import com.yg.inventory.data.db.SchemaAccess;

import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;

public class Schema {
    public final Map<String, Table> tables;

    Schema(Map<String, Table> tables) {
        this.tables = tables;
    }

    public static Schema from(SchemaAccess access) {
        Map<String, Map<String, Column>> columnsByNameByTable = access.getTables()
            .mapValues(t -> t.toMap(c -> new Tuple2<>(c._1, new Column(c._1, c._2))));

        Map<String, PrimaryKey> primaryByTable = access.getAllPrimaryKeys()
            .toMap(p -> new Tuple2<>(p.table, p));

        Map<String, ForeignKey> foreignByName = access.getAllForeignKeys()
            .toMap(f -> new Tuple2<>(f.name, f));

        Map<String, Map<String, ForeignKey>> incomingByNameByTable = foreignByName.values()
            .groupBy(f -> f.toTable)
            .mapValues(s -> s.toMap(f -> new Tuple2<>(f.name, f)));

        Map<String, Map<String, ForeignKey>> outgoingByColumnByTable = foreignByName.values()
            .groupBy(f -> f.fromTable)
            .mapValues(s -> s.toMap(f -> new Tuple2<>(f.fromColumn, f)));

        Map<String, Table> tablesByName = columnsByNameByTable
            .toMap(t -> new Tuple2<>(t._1,
                    new Table(t._1,
                            t._2,
                            primaryByTable.get(t._1).getOrNull(),
                            incomingByNameByTable.get(t._1).getOrElse(HashMap.empty()),
                            outgoingByColumnByTable.get(t._1).getOrElse(HashMap.empty()))));

        return new Schema(tablesByName);
    }
}
