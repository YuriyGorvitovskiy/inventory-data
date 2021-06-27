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

public interface TestSchema {

    static final String TABLE_NAME_FIRST   = "first";
    static final String TABLE_NAME_SECOND  = "second";
    static final String TABLE_NAME_THIRD   = "third";
    static final String TABLE_NAME_VERSION = "version";

    static final PrimaryKey PK_FIRST  = new PrimaryKey("pk_first", "first", List.of("id"));
    static final PrimaryKey PK_SECOND = new PrimaryKey("pk_second", "second", List.of("id"));
    static final PrimaryKey PK_THIRD  = new PrimaryKey("pk_third", "third", List.of("name", "indx"));

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

    static final ColumnInfo COLUMN_FIRST_ID         = new ColumnInfo("id", PostgresDataType.BIGINT);
    static final ColumnInfo COLUMN_FIRST_SECOND     = new ColumnInfo("second", PostgresDataType.UUID);
    static final ColumnInfo COLUMN_FIRST_THIRD_NAME = new ColumnInfo("third_name", PostgresDataType.NAME);
    static final ColumnInfo COLUMN_FIRST_THIRD_INDX = new ColumnInfo("third_indx", PostgresDataType.INTEGER);
    static final ColumnInfo COLUMN_FIRST_FIXED      = new ColumnInfo("fixed", PostgresDataType.CHARACTER);
    static final ColumnInfo COLUMN_FIRST_VARYING    = new ColumnInfo("varying", PostgresDataType.CHARACTER_VARYING);
    static final ColumnInfo COLUMN_FIRST_UNLIMITED  = new ColumnInfo("unlimited", PostgresDataType.TEXT);
    static final ColumnInfo COLUMN_FIRST_SEARCH     = new ColumnInfo("search", PostgresDataType.TSVECTOR);

    static final ColumnInfo COLUMN_SECOND_ID         = new ColumnInfo("id", PostgresDataType.UUID);
    static final ColumnInfo COLUMN_SECOND_FIRST      = new ColumnInfo("first", PostgresDataType.BIGINT);
    static final ColumnInfo COLUMN_SECOND_ONE        = new ColumnInfo("one", PostgresDataType.UUID);
    static final ColumnInfo COLUMN_SECOND_TWO        = new ColumnInfo("two", PostgresDataType.UUID);
    static final ColumnInfo COLUMN_SECOND_THIRD_NAME = new ColumnInfo("third_name", PostgresDataType.NAME);
    static final ColumnInfo COLUMN_SECOND_THIRD_INDX = new ColumnInfo("third_indx", PostgresDataType.INTEGER);
    static final ColumnInfo COLUMN_SECOND_DOUBLE     = new ColumnInfo("double", PostgresDataType.DOUBLE_PRECISION);
    static final ColumnInfo COLUMN_SECOND_INT        = new ColumnInfo("int", PostgresDataType.INTEGER);
    static final ColumnInfo COLUMN_SECOND_SHORT      = new ColumnInfo("short", PostgresDataType.SMALLINT);
    static final ColumnInfo COLUMN_SECOND_LONG       = new ColumnInfo("long", PostgresDataType.BIGINT);

    static final ColumnInfo COLUMN_THIRD_NAME        = new ColumnInfo("name", PostgresDataType.NAME);
    static final ColumnInfo COLUMN_THIRD_INDX        = new ColumnInfo("indx", PostgresDataType.INTEGER);
    static final ColumnInfo COLUMN_THIRD_FIRST       = new ColumnInfo("first", PostgresDataType.BIGINT);
    static final ColumnInfo COLUMN_THIRD_SECOND      = new ColumnInfo("second", PostgresDataType.UUID);
    static final ColumnInfo COLUMN_THIRD_BOOL        = new ColumnInfo("bool", PostgresDataType.BOOLEAN);
    static final ColumnInfo COLUMN_THIRD_TIME        = new ColumnInfo("time", PostgresDataType.TIMESTAMP_WITHOUT_TIME_ZONE);
    static final ColumnInfo COLUMN_THIRD_UNSUPPORTED = new ColumnInfo(
            "unsupported",
            DataType.unsupported("timestamp with time zone"));

    static final ColumnInfo COLUMN_VERSION_PRODUCT = new ColumnInfo("product", PostgresDataType.CHARACTER_VARYING);
    static final ColumnInfo COLUMN_VERSION_VERSION = new ColumnInfo("version", PostgresDataType.CHARACTER_VARYING);

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

    static final List<PrimaryKey> ALL_PRIMARY_KEYS = List.of(PK_FIRST, PK_SECOND, PK_THIRD);

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
            null,
            HashMap.empty(),
            HashMap.empty());

    static final Map<String, TableInfo> ALL_TABLE_INFO_MAP = List.of(
            TABLE_INFO_FIRST,
            TABLE_INFO_SECOND,
            TABLE_INFO_THIRD,
            TABLE_INFO_VERSION)
        .toMap(t -> t.name, t -> t);
}
