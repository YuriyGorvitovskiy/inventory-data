package org.statemach.db.sql;

import org.statemach.db.schema.ForeignKey;
import org.statemach.db.schema.PrimaryKey;

import com.yg.util.DB.DataType;

import io.vavr.collection.List;
import io.vavr.collection.Map;

public interface SchemaAccess {

    String getSchemaName();

    Map<String, Map<String, DataType>> getTables();

    List<PrimaryKey> getAllPrimaryKeys();

    List<ForeignKey> getAllForeignKeys();

}
