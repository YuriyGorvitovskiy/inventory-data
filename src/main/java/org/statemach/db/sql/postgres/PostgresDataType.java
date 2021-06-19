package org.statemach.db.sql.postgres;

import org.statemach.db.schema.DataType;

public interface PostgresDataType {

    static final DataType<String> NAME = new DataType<>("name", String.class, PostgresExtract.STRING, PostgresInject.STRING);

}
