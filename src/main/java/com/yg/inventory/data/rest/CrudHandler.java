package com.yg.inventory.data.rest;

import java.util.UUID;
import java.util.function.Function;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import com.yg.inventory.data.db.DataAccess;
import com.yg.inventory.data.db.SchemaAccess;
import com.yg.util.DB;
import com.yg.util.DB.DataType;
import com.yg.util.DB.Inject;
import com.yg.util.Java;
import com.yg.util.Rest;
import com.yg.util.Rest.ErrorCode;

import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.control.Option;

public class CrudHandler implements HttpHandler {

    static interface QueryParam {
        static final String SELECT      = "$select";
        static final String ORDER       = "$order";
        static final String SKIP        = "$skip";
        static final String LIMIT       = "$limit";
        static final String AUX_PREFIX  = "$";
        static final String DESC_PREFIX = "-";
    }

    final SchemaAccess schema = new SchemaAccess();
    final DataAccess   data   = new DataAccess();

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
        String path = Rest.subContextPath(exchange);

        String[] items = path.split("/", 2);
        if (0 == items.length || Java.isEmpty(items[0])) {
            getListOfTables(exchange);
            return;
        }
        if (1 == items.length || Java.isEmpty(items[1])) {
            queryTable(exchange, items[0]);
            return;
        }
        getRow(exchange, items[0], parseId(items[1]));
    }

    void insert(HttpExchange exchange) {
        String                    table = Rest.subContextPath(exchange);
        Map<String, List<String>> query = Rest.queryParams(exchange);

        Map<String, DataType>               columns   = getColumns(table);
        List<Tuple2<String, DB.Extract<?>>> returning = getSelect(query, columns);

        @SuppressWarnings("unchecked")
        Map<String, Object>          entity = Rest.extract(exchange, Map.class);
        List<Tuple2<String, Inject>> insert = entity.flatMap(t -> getInject(columns, t._1, t._2)).toList();

        Rest.json(exchange, data.insert(table, insert, returning).get());
    }

    void merge(HttpExchange exchange) {
        String path = Rest.subContextPath(exchange);

        String[] items = path.split("/", 2);
        if (2 != items.length || Java.isEmpty(items[0]) || Java.isEmpty(items[1])) {
            throw new Rest.Error(Rest.ErrorCode.BAD_REQUEST, "Request path ${0} is incorrect. Please, provide table/id.", path);
        }

        String table = items[0];
        UUID   id    = parseId(items[1]);

        Map<String, List<String>>           query     = Rest.queryParams(exchange);
        Map<String, DataType>               columns   = getColumns(table);
        List<Tuple2<String, DB.Extract<?>>> returning = getSelect(query, columns);

        @SuppressWarnings("unchecked")
        Map<String, Object>          entity = Rest.extract(exchange, Map.class);
        List<Tuple2<String, Inject>> values = entity.flatMap(t -> getInject(columns, t._1, t._2)).toList();

        Rest.json(exchange, data.mergeById(table, id, values, returning).get());
    }

    void update(HttpExchange exchange) {
        String path = Rest.subContextPath(exchange);

        String[] items = path.split("/", 2);
        if (2 != items.length || Java.isEmpty(items[0]) || Java.isEmpty(items[1])) {
            throw new Rest.Error(Rest.ErrorCode.BAD_REQUEST, "Request path ${0} is incorrect. Please, provide table/id.", path);
        }

        String table = items[0];
        UUID   id    = parseId(items[1]);

        Map<String, List<String>>           query     = Rest.queryParams(exchange);
        Map<String, DataType>               columns   = getColumns(table);
        List<Tuple2<String, DB.Extract<?>>> returning = getSelect(query, columns);

        @SuppressWarnings("unchecked")
        Map<String, Object>          entity = Rest.extract(exchange, Map.class);
        List<Tuple2<String, Inject>> values = entity.flatMap(t -> getInject(columns, t._1, t._2)).toList();

        Rest.json(exchange,
                data.updateById(table, id, values, returning)
                    .getOrElseThrow(() -> new Rest.Error(Rest.ErrorCode.NOT_FOUND,
                            "No item with id = ${0} is present in ${1} table",
                            id,
                            table)));
    }

    void delete(HttpExchange exchange) {
        String path = Rest.subContextPath(exchange);

        String[] items = path.split("/", 2);
        if (2 != items.length || Java.isEmpty(items[0]) || Java.isEmpty(items[1])) {
            throw new Rest.Error(Rest.ErrorCode.BAD_REQUEST, "Request path ${0} is incorrect. Please, provide table/id.", path);
        }

        String table = items[0];
        UUID   id    = parseId(items[1]);

        Map<String, List<String>>           query     = Rest.queryParams(exchange);
        Map<String, DataType>               columns   = getColumns(table);
        List<Tuple2<String, DB.Extract<?>>> returning = getSelect(query, columns);

        Rest.json(exchange,
                data.deleteById(table, id, returning)
                    .getOrElseThrow(() -> new Rest.Error(Rest.ErrorCode.NOT_FOUND,
                            "No item with id = ${0} is present in ${1} table",
                            id,
                            table)));
    }

    Option<Tuple2<String, Inject>> getInject(Map<String, DataType> columns, String column, Object value) {
        Option<DataType> dt = columns.get(column);
        if (dt.isEmpty()) {
            return Option.none();
        }

        Option<Function<Object, Inject>> injector = DB.DATA_TYPE_INJECT.get(dt.get());
        if (dt.isEmpty()) {
            return Option.none();
        }

        return Option.of(new Tuple2<>(column, injector.get().apply(value)));
    }

    void getListOfTables(HttpExchange exchange) {
        Rest.json(exchange, schema.getTables());
    }

    void queryTable(HttpExchange exchange, String table) {
        Map<String, List<String>> query = Rest.queryParams(exchange);

        Map<String, DataType>               columns   = getColumns(table);
        List<Tuple2<String, DB.Extract<?>>> select    = getSelect(query, columns);
        Tuple2<String, DB.Inject>           condition = getConditions(query, columns);
        List<Tuple2<String, Boolean>>       order     = getOrder(query, columns);
        Tuple2<Long, Integer>               skipLimit = getSkipLimit(query);

        Rest.json(exchange, data.queryByCondition(table, select, condition, order, skipLimit));
    }

    public Map<String, DataType> getColumns(String table) {
        Map<String, DataType> columns = schema.getTableColumns(table);
        if (columns.isEmpty()) {
            throw new Rest.Error(Rest.ErrorCode.NOT_FOUND,
                    "Table ${0} was not found",
                    table);
        }
        return columns;
    }

    void getRow(HttpExchange exchange, String table, UUID id) {
        Map<String, List<String>>           query   = Rest.queryParams(exchange);
        Map<String, DataType>               columns = getColumns(table);
        List<Tuple2<String, DB.Extract<?>>> select  = getSelect(query, columns);

        Rest.json(exchange,
                data.queryById(table, select, id)
                    .getOrElseThrow(() -> new Rest.Error(Rest.ErrorCode.NOT_FOUND,
                            "No item with id = ${0} is present in ${1} table",
                            id,
                            table)));
    }

    List<Tuple2<String, DB.Extract<?>>> getSelect(Map<String, List<String>> query, Map<String, DataType> columns) {
        Option<List<String>> param = query.get(QueryParam.SELECT);

        return param.map(l -> l.flatMap(s -> List.of(s.split(","))))
            .getOrElse(columns.toList()
                .filter(t -> DB.DataType.SUPPORTED.contains(t._2))
                .map(t -> t._1))
            .map(c -> getSelectColumn(c, columns))
            .filter(t -> null != t._2)
            .toList();
    }

    Tuple2<String, DB.Extract<?>> getSelectColumn(String column, Map<String, DataType> columns) {
        DataType dataType = columns.get(column)
            .getOrElseThrow(() -> new Rest.Error(Rest.ErrorCode.BAD_REQUEST,
                    "Column ${0} is not present on requested table",
                    column));

        DB.Extract<?> extract = DB.DATA_TYPE_EXTRACT.get(dataType)
            .getOrElseThrow(() -> new Rest.Error(Rest.ErrorCode.BAD_REQUEST,
                    "Column ${0} is not supported for ${1} parameter",
                    column,
                    QueryParam.SELECT));

        return new Tuple2<>(column, extract);
    }

    Tuple2<String, DB.Inject> getConditions(Map<String, List<String>> query, Map<String, DataType> columns) {
        return DB.andSqlInjects(query
            .filterKeys(k -> !k.startsWith(QueryParam.AUX_PREFIX))
            .map(t -> DB.conditionSqlInject(
                    t._1,
                    columns.get(t._1)
                        .getOrElseThrow(() -> new Rest.Error(Rest.ErrorCode.BAD_REQUEST,
                                "Column ${0} is not present on requested table",
                                t._1)),
                    DB.DATA_TYPE_STRING_INJECT,
                    t._2)));
    }

    List<Tuple2<String, Boolean>> getOrder(Map<String, List<String>> query, Map<String, DataType> columns) {
        Option<List<String>> param = query.get(QueryParam.ORDER);
        if (param.isEmpty()) {
            return List.of(new Tuple2<>(columns.get()._1, true));
        }

        List<Tuple2<String, Boolean>> orders = param.get()
            .flatMap(s -> List.of(s.split(",")))
            .map(c -> c.startsWith(QueryParam.DESC_PREFIX)
                    ? new Tuple2<>(c.substring(QueryParam.DESC_PREFIX.length()), false)
                    : new Tuple2<>(c, true));

        Option<String> nonPresentColumn = orders.map(t -> t._1).find(c -> !columns.containsKey(c));
        if (nonPresentColumn.isDefined()) {
            throw new Rest.Error(ErrorCode.BAD_REQUEST,
                    "Column ${0} is not present on requested table",
                    nonPresentColumn.get());
        }
        return orders;
    }

    Tuple2<Long, Integer> getSkipLimit(Map<String, List<String>> query) {
        return new Tuple2<>(
                query.get(QueryParam.SKIP)
                    .map(l -> Long.parseLong(l.get()))
                    .getOrElse(0L),
                query.get(QueryParam.LIMIT)
                    .map(l -> Integer.parseInt(l.get()))
                    .getOrElse(10));
    }

    UUID parseId(String id) {
        return Java.soft(
                () -> UUID.fromString(id),
                ex -> new Rest.Error(ErrorCode.BAD_REQUEST, ex, "Can't parse id: ${0}", id));
    }
}
