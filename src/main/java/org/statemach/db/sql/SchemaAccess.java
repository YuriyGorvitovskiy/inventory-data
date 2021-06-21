package org.statemach.db.sql;

import org.statemach.db.schema.ColumnInfo;
import org.statemach.db.schema.ForeignKey;
import org.statemach.db.schema.PrimaryKey;

import io.vavr.collection.List;
import io.vavr.collection.Map;

public interface SchemaAccess {

    String getSchemaName();

    Map<String, List<ColumnInfo>> getAllTables();

    List<PrimaryKey> getAllPrimaryKeys();

    List<ForeignKey> getAllForeignKeys();

}
