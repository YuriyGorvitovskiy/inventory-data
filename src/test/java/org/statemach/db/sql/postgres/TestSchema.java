package org.statemach.db.sql.postgres;

import org.statemach.db.schema.ColumnInfo;
import org.statemach.db.schema.DataType;
import org.statemach.db.schema.ForeignKey;
import org.statemach.db.schema.ForeignKey.Match;
import org.statemach.db.schema.PrimaryKey;
import org.statemach.db.schema.TableInfo;

import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.LinkedHashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.control.Option;

public interface TestSchema {

    static final String TABLE_NAME_FIRST   = "first";
    static final String TABLE_NAME_SECOND  = "second";
    static final String TABLE_NAME_THIRD   = "third";
    static final String TABLE_NAME_VERSION = "version";

    static final Option<PrimaryKey> PK_FIRST  = Option.of(new PrimaryKey("pk_first", "first", List.of("id")));
    static final Option<PrimaryKey> PK_SECOND = Option.of(new PrimaryKey("pk_second", "second", List.of("id")));
    static final Option<PrimaryKey> PK_THIRD  = Option.of(new PrimaryKey("pk_third", "third", List.of("name", "indx")));

    static final ForeignKey FK_FIRST_SECOND      = new ForeignKey("fk_first_second",
            TABLE_NAME_FIRST,
            TABLE_NAME_SECOND,
            List.of(new Match("second", "id")));
    static final ForeignKey FK_FIRST_THIRD       = new ForeignKey("fk_first_third",
            TABLE_NAME_FIRST,
            TABLE_NAME_THIRD,
            List.of(new Match("third_name", "name"), new Match("third_indx", "indx")));
    static final ForeignKey FK_SECOND_FIRST      = new ForeignKey("fk_second_first",
            TABLE_NAME_SECOND,
            TABLE_NAME_FIRST,
            List.of(new Match("first", "id")));
    static final ForeignKey FK_SECOND_SECOND_ONE = new ForeignKey("fk_second_second_one",
            TABLE_NAME_SECOND,
            TABLE_NAME_SECOND,
            List.of(new Match("one", "id")));
    static final ForeignKey FK_SECOND_SECOND_TWO = new ForeignKey("fk_second_second_two",
            TABLE_NAME_SECOND,
            TABLE_NAME_SECOND,
            List.of(new Match("two", "id")));
    static final ForeignKey FK_SECOND_THIRD      = new ForeignKey("fk_second_third",
            TABLE_NAME_SECOND,
            TABLE_NAME_THIRD,
            List.of(new Match("third_name", "name"), new Match("third_indx", "indx")));
    static final ForeignKey FK_THIRD_FIRST       = new ForeignKey("fk_third_first",
            TABLE_NAME_THIRD,
            TABLE_NAME_FIRST,
            List.of(new Match("first", "id")));
    static final ForeignKey FK_THIRD_SECOND      = new ForeignKey("fk_third_second",
            TABLE_NAME_THIRD,
            TABLE_NAME_SECOND,
            List.of(new Match("second", "id")));

    static final ColumnInfo COLUMN_FIRST_ID         = ColumnInfo.of("id", PostgresDataType.BIGINT);
    static final ColumnInfo COLUMN_FIRST_SECOND     = ColumnInfo.of("second", PostgresDataType.UUID);
    static final ColumnInfo COLUMN_FIRST_THIRD_NAME = ColumnInfo.of("third_name", PostgresDataType.NAME);
    static final ColumnInfo COLUMN_FIRST_THIRD_INDX = ColumnInfo.of("third_indx", PostgresDataType.INTEGER);
    static final ColumnInfo COLUMN_FIRST_FIXED      = ColumnInfo.of("fixed", PostgresDataType.CHARACTER, 256);
    static final ColumnInfo COLUMN_FIRST_VARYING    = ColumnInfo.of("varying", PostgresDataType.CHARACTER_VARYING, 256);
    static final ColumnInfo COLUMN_FIRST_UNLIMITED  = ColumnInfo.of("unlimited", PostgresDataType.TEXT);
    static final ColumnInfo COLUMN_FIRST_SEARCH     = ColumnInfo.of("search", PostgresDataType.TSVECTOR);

    static final ColumnInfo COLUMN_SECOND_ID         = ColumnInfo.of("id", PostgresDataType.UUID);
    static final ColumnInfo COLUMN_SECOND_FIRST      = ColumnInfo.of("first", PostgresDataType.BIGINT);
    static final ColumnInfo COLUMN_SECOND_ONE        = ColumnInfo.of("one", PostgresDataType.UUID);
    static final ColumnInfo COLUMN_SECOND_TWO        = ColumnInfo.of("two", PostgresDataType.UUID);
    static final ColumnInfo COLUMN_SECOND_THIRD_NAME = ColumnInfo.of("third_name", PostgresDataType.NAME);
    static final ColumnInfo COLUMN_SECOND_THIRD_INDX = ColumnInfo.of("third_indx", PostgresDataType.INTEGER);
    static final ColumnInfo COLUMN_SECOND_DOUBLE     = ColumnInfo.of("double", PostgresDataType.DOUBLE_PRECISION);
    static final ColumnInfo COLUMN_SECOND_INT        = ColumnInfo.of("int", PostgresDataType.INTEGER);
    static final ColumnInfo COLUMN_SECOND_SHORT      = ColumnInfo.of("short", PostgresDataType.SMALLINT);
    static final ColumnInfo COLUMN_SECOND_LONG       = ColumnInfo.of("long", PostgresDataType.BIGINT);

    static final ColumnInfo COLUMN_THIRD_NAME        = ColumnInfo.of("name", PostgresDataType.NAME);
    static final ColumnInfo COLUMN_THIRD_INDX        = ColumnInfo.of("indx", PostgresDataType.INTEGER);
    static final ColumnInfo COLUMN_THIRD_FIRST       = ColumnInfo.of("first", PostgresDataType.BIGINT);
    static final ColumnInfo COLUMN_THIRD_SECOND      = ColumnInfo.of("second", PostgresDataType.UUID);
    static final ColumnInfo COLUMN_THIRD_BOOL        = ColumnInfo.of("bool", PostgresDataType.BOOLEAN);
    static final ColumnInfo COLUMN_THIRD_TIME        = ColumnInfo.of("time", PostgresDataType.TIMESTAMP_WITHOUT_TIME_ZONE);
    static final ColumnInfo COLUMN_THIRD_UNSUPPORTED = ColumnInfo.of(
            "unsupported",
            DataType.unsupported("timestamp with time zone"));

    static final ColumnInfo COLUMN_VERSION_PRODUCT = ColumnInfo.of("product", PostgresDataType.CHARACTER_VARYING, 256);
    static final ColumnInfo COLUMN_VERSION_VERSION = ColumnInfo.of("version", PostgresDataType.CHARACTER_VARYING, 256);

    static final List<ColumnInfo> FIRST_COLUMNS   = List.of(
            COLUMN_FIRST_ID,
            COLUMN_FIRST_SECOND,
            COLUMN_FIRST_THIRD_NAME,
            COLUMN_FIRST_THIRD_INDX,
            COLUMN_FIRST_FIXED,
            COLUMN_FIRST_VARYING,
            COLUMN_FIRST_UNLIMITED,
            COLUMN_FIRST_SEARCH);
    static final List<ColumnInfo> SECOND_COLUMNS  = List.of(
            COLUMN_SECOND_ID,
            COLUMN_SECOND_FIRST,
            COLUMN_SECOND_ONE,
            COLUMN_SECOND_TWO,
            COLUMN_SECOND_THIRD_NAME,
            COLUMN_SECOND_THIRD_INDX,
            COLUMN_SECOND_DOUBLE,
            COLUMN_SECOND_INT,
            COLUMN_SECOND_SHORT,
            COLUMN_SECOND_LONG);
    static final List<ColumnInfo> THIRD_COLUMNS   = List.of(
            COLUMN_THIRD_NAME,
            COLUMN_THIRD_INDX,
            COLUMN_THIRD_FIRST,
            COLUMN_THIRD_SECOND,
            COLUMN_THIRD_BOOL,
            COLUMN_THIRD_TIME,
            COLUMN_THIRD_UNSUPPORTED);
    static final List<ColumnInfo> VERSION_COLUMNS = List.of(
            COLUMN_VERSION_PRODUCT,
            COLUMN_VERSION_VERSION);

    @SuppressWarnings("unchecked")
    static final Map<String, List<ColumnInfo>> ALL_TABLES = LinkedHashMap.ofEntries(
            new Tuple2<>(TABLE_NAME_FIRST, FIRST_COLUMNS),
            new Tuple2<>(TABLE_NAME_SECOND, SECOND_COLUMNS),
            new Tuple2<>(TABLE_NAME_THIRD, THIRD_COLUMNS),
            new Tuple2<>(TABLE_NAME_VERSION, VERSION_COLUMNS));

    static final List<PrimaryKey> ALL_PRIMARY_KEYS = List.of(PK_FIRST.get(), PK_SECOND.get(), PK_THIRD.get());

    static final List<ForeignKey> ALL_FOREIGN_KEYS = List.of(
            FK_FIRST_SECOND,
            FK_FIRST_THIRD,
            FK_SECOND_FIRST,
            FK_SECOND_SECOND_ONE,
            FK_SECOND_SECOND_TWO,
            FK_SECOND_THIRD,
            FK_THIRD_FIRST,
            FK_THIRD_SECOND);

    static final TableInfo TABLE_INFO_FIRST = new TableInfo(
            TABLE_NAME_FIRST,
            FIRST_COLUMNS.toMap(c -> c.name, c -> c),
            PK_FIRST,
            List.of(FK_SECOND_FIRST, FK_THIRD_FIRST).toMap(f -> f.name, f -> f),
            List.of(FK_FIRST_SECOND, FK_FIRST_THIRD).toMap(f -> f.name, f -> f));

    static final TableInfo TABLE_INFO_SECOND = new TableInfo(
            TABLE_NAME_SECOND,
            SECOND_COLUMNS.toMap(c -> c.name, c -> c),
            PK_SECOND,
            List.of(FK_FIRST_SECOND, FK_SECOND_SECOND_ONE, FK_SECOND_SECOND_TWO, FK_THIRD_SECOND).toMap(f -> f.name, f -> f),
            List.of(FK_SECOND_FIRST, FK_SECOND_SECOND_ONE, FK_SECOND_SECOND_TWO, FK_SECOND_THIRD).toMap(f -> f.name, f -> f));

    static final TableInfo TABLE_INFO_THIRD = new TableInfo(
            TABLE_NAME_THIRD,
            THIRD_COLUMNS.toMap(c -> c.name, c -> c),
            PK_THIRD,
            List.of(FK_FIRST_THIRD, FK_SECOND_THIRD).toMap(f -> f.name, f -> f),
            List.of(FK_THIRD_FIRST, FK_THIRD_SECOND).toMap(f -> f.name, f -> f));

    static final TableInfo TABLE_INFO_VERSION = new TableInfo(
            TABLE_NAME_VERSION,
            VERSION_COLUMNS.toMap(c -> c.name, c -> c),
            Option.none(),
            HashMap.empty(),
            HashMap.empty());

    static final Map<String, TableInfo> ALL_TABLE_INFO_MAP = List.of(
            TABLE_INFO_FIRST,
            TABLE_INFO_SECOND,
            TABLE_INFO_THIRD,
            TABLE_INFO_VERSION)
        .toMap(t -> t.name, t -> t);
}
