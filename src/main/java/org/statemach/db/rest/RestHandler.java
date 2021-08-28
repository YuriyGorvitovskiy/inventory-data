package org.statemach.db.rest;

import org.statemach.db.jdbc.Extract;
import org.statemach.db.jdbc.Inject;
import org.statemach.db.schema.ColumnInfo;
import org.statemach.db.schema.DataType;
import org.statemach.db.schema.PrimaryKey;
import org.statemach.db.schema.Schema;
import org.statemach.db.schema.TableInfo;
import org.statemach.db.sql.Condition;
import org.statemach.db.sql.DataAccess;
import org.statemach.db.sql.From;
import org.statemach.db.sql.Join;
import org.statemach.db.sql.SQLBuilder;
import org.statemach.db.sql.Select;
import org.statemach.db.sql.TableLike;
import org.statemach.db.sql.View;
import org.statemach.db.sql.postgres.PostgresDataType;
import org.statemach.util.Http;
import org.statemach.util.Http.Error;
import org.statemach.util.Http.ErrorCode;
import org.statemach.util.Java;
import org.statemach.util.NodeLinkTree;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.control.Option;

public class RestHandler implements HttpHandler {

    static interface QueryParam {
        static final String SELECT      = "$select";
        static final String ORDER       = "$order";
        static final String SKIP        = "$skip";
        static final String LIMIT       = "$limit";
        static final String AUX_PREFIX  = "$";
        static final String DESC_PREFIX = "-";
        static final String ID_DIVIDER  = ":";
    }

    static final String ALIAS          = "t";
    static final int    DEFAULT_LIMIT  = 10;
    static final int    IN_PARAM_LIMIT = 7;

    final Schema     schema;
    final DataAccess dataAccess;
    final SQLBuilder sqlBuilder;

    public RestHandler(Schema schema,
                       DataAccess dataAccess,
                       SQLBuilder sqlBuilder) {
        this.schema = schema;
        this.dataAccess = dataAccess;
        this.sqlBuilder = sqlBuilder;
    }

    @Override
    public void handle(HttpExchange exchange) {
        switch (exchange.getRequestMethod().toUpperCase()) {
            case "GET":
                get(exchange);
                break;
            case "POST":
                insert(exchange);
                break;
            case "PUT":
                merge(exchange);
                break;
            case "PATCH":
                update(exchange);
                break;
            case "DELETE":
                delete(exchange);
                break;
            default:
                break;
        }
    }

    void get(HttpExchange exchange) {
        String path = Http.subContextPath(exchange);

        String[] items = path.split("/", 2);
        if (0 == items.length || Java.isEmpty(items[0])) {
            getListOfTables(exchange);
            return;
        }
        if (1 == items.length || Java.isEmpty(items[1])) {
            queryTable(exchange, items[0]);
            return;
        }
        getRow(exchange, items[0], items[1]);
    }

    void insert(HttpExchange exchange) {
        TableInfo                 table = getTable(Http.subContextPath(exchange));
        Map<String, List<String>> query = Http.queryParams(exchange);

        Map<String, Extract<?>> returning = getSelect(query, table);

        @SuppressWarnings("unchecked")
        Map<String, Object> entity = Http.extract(exchange, Map.class);
        Map<String, Inject> values = entity.flatMap(t -> getInject(table, t._1, t._2)).toMap(t -> t);

        Http.json(exchange, dataAccess.insert(table.name, values, returning));
    }

    void merge(HttpExchange exchange) {
        String path = Http.subContextPath(exchange);

        String[] items = path.split("/", 2);
        if (2 != items.length || Java.isEmpty(items[0]) || Java.isEmpty(items[1])) {
            throw new Http.Error(Http.ErrorCode.BAD_REQUEST, "Request path ${0} is incorrect. Please, provide table/id.", path);
        }

        TableInfo           table = getTable(items[0]);
        Map<String, Inject> id    = parseId(table, items[1]);

        Map<String, List<String>> query     = Http.queryParams(exchange);
        Map<String, Extract<?>>   returning = getSelect(query, table);

        @SuppressWarnings("unchecked")
        Map<String, Object> entity = Http.extract(exchange, Map.class);
        Map<String, Inject> values = entity.flatMap(t -> getInject(table, t._1, t._2)).toMap(t -> t);

        Http.json(exchange, dataAccess.merge(table.name, id, values, returning));
    }

    void update(HttpExchange exchange) {
        String path = Http.subContextPath(exchange);

        String[] items = path.split("/", 2);
        if (2 != items.length || Java.isEmpty(items[0]) || Java.isEmpty(items[1])) {
            throw new Http.Error(Http.ErrorCode.BAD_REQUEST, "Request path ${0} is incorrect. Please, provide table/id.", path);
        }

        TableInfo           table = getTable(items[0]);
        Map<String, Inject> id    = parseId(table, items[1]);

        Map<String, List<String>> query     = Http.queryParams(exchange);
        Map<String, Extract<?>>   returning = getSelect(query, table);

        @SuppressWarnings("unchecked")
        Map<String, Object> entity = Http.extract(exchange, Map.class);
        Map<String, Inject> values = entity.flatMap(t -> getInject(table, t._1, t._2)).toMap(t -> t);

        Http.json(exchange,
                dataAccess.update(table.name, id, values, returning)
                    .getOrElseThrow(() -> new Http.Error(Http.ErrorCode.NOT_FOUND,
                            "No item with id = ${0} is present in ${1} table",
                            id,
                            table)));
    }

    void delete(HttpExchange exchange) {
        String path = Http.subContextPath(exchange);

        String[] items = path.split("/", 2);
        if (2 != items.length || Java.isEmpty(items[0]) || Java.isEmpty(items[1])) {
            throw new Http.Error(Http.ErrorCode.BAD_REQUEST, "Request path ${0} is incorrect. Please, provide table/id.", path);
        }

        TableInfo           table = getTable(items[0]);
        Map<String, Inject> id    = parseId(table, items[1]);

        Map<String, List<String>> query     = Http.queryParams(exchange);
        Map<String, Extract<?>>   returning = getSelect(query, table);

        Http.json(exchange,
                dataAccess.delete(table.name, id, returning)
                    .getOrElseThrow(() -> new Http.Error(Http.ErrorCode.NOT_FOUND,
                            "No item with id = ${0} is present in ${1} table",
                            id,
                            table)));
    }

    Option<Tuple2<String, Inject>> getInject(TableInfo table, String columnName, Object value) {
        Option<ColumnInfo> column = table.columns.get(columnName);
        if (column.isEmpty()) {
            return Option.none();
        }

        DataType type = column.get().type;
        if (!type.isMutable) {
            return Option.none();
        }

        return Option.of(new Tuple2<>(column.get().name, type.injectJsonValue.prepare(value)));
    }

    void getListOfTables(HttpExchange exchange) {
        Http.json(exchange, schema.tables.mapValues(t -> t.columns.mapValues(c -> c.type.name)));
    }

    void queryTable(HttpExchange exchange, String tableName) {
        Map<String, List<String>> query = Http.queryParams(exchange);

        TableInfo             table     = getTable(tableName);
        Tuple2<Long, Integer> skipLimit = getSkipLimit(query);

        View<Tuple2<String, Extract<?>>> view = new View<>(
                "",
                NodeLinkTree.<String, From, Join>of(new From(TableLike.of(schema, table), ALIAS)),
                getConditions(query, table),
                getOrder(query, table),
                getSelect(query, table).map(t -> Select.of(ALIAS, t._1, t)).toList(),
                true,
                skipLimit._1,
                skipLimit._2);

        Http.json(exchange, dataAccess.query(List.empty(), view));
    }

    void getRow(HttpExchange exchange, String tableName, String id) {
        Map<String, List<String>> query = Http.queryParams(exchange);

        TableInfo table = schema.tables.get(tableName)
            .getOrElseThrow(() -> new Http.Error(
                    Http.ErrorCode.BAD_REQUEST,
                    "Table ${0} is not present",
                    tableName));

        Map<String, Inject>     primaryKey = parseId(table, id);
        Map<String, Extract<?>> select     = getSelect(query, table);
        Http.json(exchange,
                dataAccess.select(tableName, primaryKey, select)
                    .getOrElseThrow(() -> new Http.Error(Http.ErrorCode.NOT_FOUND,
                            "No item with id = ${0} is present in ${1} table",
                            id,
                            table)));
    }

    Map<String, Extract<?>> getSelect(Map<String, List<String>> query, TableInfo table) {
        Option<List<String>> param = query.get(QueryParam.SELECT);
        if (param.isEmpty()) {
            return table.columns.values()
                .flatMap(this::getExtract)
                .toMap(t -> t);

        }
        return param.get()
            .flatMap(s -> List.of(s.split(",")))
            .map(c -> getColumn(table, c))
            .map(c -> getExtract(c).getOrElseThrow(() -> new Http.Error(
                    Http.ErrorCode.BAD_REQUEST,
                    "Column ${0} is not supported for ${1} parameter",
                    c,
                    QueryParam.SELECT)))
            .toMap(t -> t);
    }

    Option<Tuple2<String, Extract<?>>> getExtract(ColumnInfo column) {
        return column.type.isExtractable
                ? Option.of(new Tuple2<>(column.name, column.type.extractJsonValue))
                : Option.none();
    }

    Condition getConditions(Map<String, List<String>> query, TableInfo table) {
        return sqlBuilder.and(query
            .filterKeys(k -> !k.startsWith(QueryParam.AUX_PREFIX))
            .map(t -> conditionFrom(table, t._1, t._2)));
    }

    List<Select<Boolean>> getOrder(Map<String, List<String>> query, TableInfo table) {
        Option<List<String>> param = query.get(QueryParam.ORDER);
        if (param.isEmpty()) {
            return table.primary.isDefined()
                    ? table.primary.get().columns.map(c -> Select.of(ALIAS, c, true))
                    : List.empty();
        }

        List<Select<Boolean>> orders = param.get()
            .flatMap(s -> List.of(s.split(",")))
            .map(c -> c.startsWith(QueryParam.DESC_PREFIX)
                    ? Select.of(ALIAS, c.substring(QueryParam.DESC_PREFIX.length()), false)
                    : Select.of(ALIAS, c, true));

        Option<String> nonPresentColumn = orders.map(s -> s.column).find(c -> !table.columns.containsKey(c));
        if (nonPresentColumn.isDefined()) {
            throw new Http.Error(Http.ErrorCode.BAD_REQUEST,
                    "Column ${0} is not present on requested table",
                    nonPresentColumn.get());
        }
        return orders;
    }

    Tuple2<Long, Integer> getSkipLimit(Map<String, List<String>> params) {
        return new Tuple2<>(
                params.get(QueryParam.SKIP)
                    .map(l -> Long.parseLong(l.get()))
                    .getOrElse(0L),
                params.get(QueryParam.LIMIT)
                    .map(l -> Integer.parseInt(l.get()))
                    .getOrElse(DEFAULT_LIMIT));
    }

    Map<String, Inject> parseId(TableInfo table, String id) {
        if (table.primary.isEmpty()) {
            return HashMap.empty();
        }
        PrimaryKey pk = table.primary.get();
        if (pk.columns.size() == 1) {
            return HashMap.of(pk.columns.get(), injectFrom(table, pk.columns.get(), id));
        }

        List<String> parts = List.of(id.split(QueryParam.ID_DIVIDER));
        if (parts.size() != pk.columns.size()) {
            throw new Error(ErrorCode.BAD_REQUEST,
                    "Can't parse id: ${0}. Expecting ${1} parts, separated by ${2}",
                    id,
                    QueryParam.ID_DIVIDER,
                    pk.columns.size());
        }
        return pk.columns.zip(parts)
            .map(t -> new Tuple2<>(t._1, injectFrom(table, t._1, t._2)))
            .toMap(t -> t);
    }

    Condition conditionFrom(TableInfo table, String columnName, List<String> values) {
        ColumnInfo column = getColumn(table, columnName);
        if (!column.type.isFilterable) {
            return Condition.NONE;
        }
        if (PostgresDataType.TSVECTOR == column.type) {
            return sqlBuilder.textSearch(Select.of(ALIAS, column.name), values);
        }

        if (values.isEmpty()) {
            return sqlBuilder.isNull(Select.of(ALIAS, column.name));
        }

        if (1 == values.size()) {
            return sqlBuilder.equal(Select.of(ALIAS, column.name), column.type.injectStringValue.prepare(values.get()));
        }

        if (IN_PARAM_LIMIT > values.size()) {
            return sqlBuilder.in(Select.of(ALIAS, column.name), values.map(column.type.injectStringValue::prepare));
        }
        return sqlBuilder.inArray(Select.of(ALIAS, column.name), column.type, values);
    }

    Inject injectFrom(TableInfo table, String columnName, String value) {
        ColumnInfo column = getColumn(table, columnName);
        return column.type.injectStringValue.prepare(value);
    }

    TableInfo getTable(String tableName) {
        return schema.tables.get(tableName)
            .getOrElseThrow(() -> new Http.Error(
                    Http.ErrorCode.BAD_REQUEST,
                    "Table ${0} is not present",
                    tableName));
    }

    ColumnInfo getColumn(TableInfo table, String columnName) {
        return table.columns.get(columnName)
            .getOrElseThrow(() -> new Http.Error(Http.ErrorCode.BAD_REQUEST,
                    "Column ${0} is not present on requested table",
                    columnName));
    }
}
