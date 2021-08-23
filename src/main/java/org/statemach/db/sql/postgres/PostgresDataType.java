package org.statemach.db.sql.postgres;

import java.sql.Types;

import org.statemach.db.jdbc.Extract;
import org.statemach.db.jdbc.Setter;
import org.statemach.db.jdbc.Transform;
import org.statemach.db.schema.DataType;

import io.vavr.collection.List;
import io.vavr.collection.Map;

public interface PostgresDataType {

    static final DataType BIGINT                  = DataType.of("bigint",
            Types.BIGINT,
            Transform.Str.LONG,
            Transform.Jsn.LONG,
            Setter.LONG,
            Extract.LONG);
    static final DataType BOOLEAN                 = DataType.of("boolean",
            Types.BOOLEAN,
            Transform.Str.BOOLEAN,
            Transform.Jsn.BOOLEAN,
            Setter.BOOLEAN,
            Extract.BOOLEAN);
    static final DataType CHARACTER               = DataType.of("character",
            Types.VARCHAR,
            Transform.Str.STRING,
            Transform.Jsn.STRING,
            Setter.STRING,
            Extract.STRING);
    static final DataType CHARACTER_VARYING       = DataType.of("character varying",
            Types.VARCHAR,
            Transform.Str.STRING,
            Transform.Jsn.STRING,
            Setter.STRING,
            Extract.STRING);
    static final DataType DOUBLE_PRECISION        = DataType.of("double precision",
            Types.DOUBLE,
            Transform.Str.DOUBLE,
            Transform.Jsn.DOUBLE,
            Setter.DOUBLE,
            Extract.DOUBLE);
    static final DataType INTEGER                 = DataType.of("integer",
            Types.INTEGER,
            Transform.Str.INTEGER,
            Transform.Jsn.INTEGER,
            Setter.INTEGER,
            Extract.INTEGER);
    static final DataType NAME                    = DataType.of("name",
            Types.VARCHAR,
            Transform.Str.STRING,
            Transform.Jsn.STRING,
            Setter.STRING,
            Extract.STRING);
    static final DataType SMALLINT                = DataType.of("smallint",
            Types.INTEGER,
            Transform.Str.INTEGER,
            Transform.Jsn.INTEGER,
            Setter.INTEGER,
            Extract.INTEGER);
    static final DataType TEXT                    = DataType.of("text",
            Types.VARCHAR,
            Transform.Str.STRING,
            Transform.Jsn.STRING,
            Setter.STRING,
            Extract.STRING);
    static final DataType TIMESTAMP_WITH_TIMEZONE = DataType.of("timestamp with time zone",
            Types.TIMESTAMP_WITH_TIMEZONE,
            Transform.Str.TIMESTAMP,
            Transform.Jsn.TIMESTAMP,
            Setter.TIMESTAMP,
            Extract.TIMESTAMP_AS_ISO8601);
    static final DataType TSVECTOR                = DataType.of("tsvector",
            Types.VARCHAR,
            Transform.Str.STRING,
            Transform.Jsn.STRING,
            Setter.STRING,
            null);
    static final DataType UUID                    = DataType.of("uuid",
            Types.OTHER,
            Transform.Str.UUID,
            Transform.Jsn.UUID,
            Setter.OBJECT,
            Extract.OBJECT_AS_UUID);

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
            TIMESTAMP_WITH_TIMEZONE,
            TSVECTOR,
            UUID)
        .toMap(dt -> dt.name, dt -> dt);

    static DataType getByName(String name) {
        return BY_NAME.get(name).getOrElse(() -> DataType.unsupported(name));
    }

}
