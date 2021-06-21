package org.statemach.db.sql.postgres;

import org.statemach.db.schema.DataType;

import io.vavr.collection.List;
import io.vavr.collection.Map;

public interface PostgresDataType {

    static final DataType BIGINT                      = new DataType("bigint");
    static final DataType BOOLEAN                     = new DataType("boolean");
    static final DataType CHARACTER                   = new DataType("character");
    static final DataType CHARACTER_VARYING           = new DataType("character varying");
    static final DataType DOUBLE_PRECISION            = new DataType("double precision");
    static final DataType INTEGER                     = new DataType("integer");
    static final DataType NAME                        = new DataType("name");
    static final DataType SMALLINT                    = new DataType("smallint");
    static final DataType TEXT                        = new DataType("text");
    static final DataType TIMESTAMP_WITHOUT_TIME_ZONE = new DataType("timestamp without time zone");
    static final DataType TSVECTOR                    = new DataType("tsvector");
    static final DataType UUID                        = new DataType("uuid");

    static final Map<String, DataType> BY_NAME = List.<DataType>of(
            BIGINT,
            BOOLEAN,
            CHARACTER,
            CHARACTER_VARYING,
            DOUBLE_PRECISION,
            INTEGER,
            NAME,
            SMALLINT,
            TEXT,
            TIMESTAMP_WITHOUT_TIME_ZONE,
            TSVECTOR,
            UUID)
        .toMap(dt -> dt.name, dt -> dt);

    static DataType getByName(String name) {
        return BY_NAME.get(name).getOrElse(() -> DataType.unsupported(name));
    }

}
