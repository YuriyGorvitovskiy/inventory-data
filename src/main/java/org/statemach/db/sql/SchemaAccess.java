package org.statemach.db.sql;

import org.statemach.db.jdbc.Vendor;
import org.statemach.db.schema.ColumnInfo;
import org.statemach.db.schema.CompositeType;
import org.statemach.db.schema.ForeignKey;
import org.statemach.db.schema.PrimaryKey;

import io.vavr.collection.List;
import io.vavr.collection.Map;

public interface SchemaAccess {

    Vendor getVendor();

    String getSchemaName();

    Map<String, List<ColumnInfo>> getAllTables();

    List<PrimaryKey> getAllPrimaryKeys();

    List<ForeignKey> getAllForeignKeys();

    List<CompositeType> getAllCompositeTypes();

    void createCompositeType(CompositeType type);

    void dropCompositeType(String typeName);

}
