package com.yg.inventory.data.rest;

import java.util.function.Function;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import com.yg.util.DB;
import com.yg.util.Java;
import com.yg.util.Rest;

import io.vavr.Tuple2;
import io.vavr.Tuple3;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Seq;
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

    final String QEURY_FOR_TABLE       = Java.resource("QueryForTable.sql");
    final String QEURY_FOR_COLUMNS     = Java.resource("QueryForColumns.sql");
    final String QEURY_ALL             = Java.resource("QueryAll.sql");
    final String QEURY_WITH_CONDITIONS = Java.resource("QueryWithConditions.sql");
    final String QEURY_BY_ID           = Java.resource("QueryById.sql");

    @Override
    public void handle(HttpExchange exchange) {
        switch (exchange.getRequestMethod().toUpperCase()) {
            case "GET":
                get(exchange);
                break;
            default:
                break;
        }
    }

    public void get(HttpExchange exchange) {
        String path = Rest.subContextPath(exchange);

        String[] items = path.split("/", 3);
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

    void getListOfTables(HttpExchange exchange) {
        Rest.json(exchange,
                DB.query(QEURY_FOR_TABLE,
                        ps -> {},
                        rs -> new Tuple3<String, String, String>(rs.getString(1), rs.getString(2), rs.getString(3)))
                    .groupBy(t -> t._1)
                    .mapValues(tl -> tl
                        .groupBy(t -> t._2)
                        .mapValues(cl -> cl.get()._3)));
    }

    void queryTable(HttpExchange exchange, String table) {
        Map<String, List<String>> query = Rest.queryParams(exchange);

        Map<String, String>                 columns   = getColumns(table);
        List<Tuple2<String, DB.Extract<?>>> select    = getSelect(query, columns);
        Tuple2<String, DB.Inject>           condition = getConditions(query, columns);
        List<Tuple2<String, Boolean>>       order     = getOrder(query, columns);
        Tuple2<Long, Integer>               skipLimit = getSkipLimit(query);

        String sql = Java.format(QEURY_WITH_CONDITIONS,
                select.map(t -> t._1).mkString(", "),
                table,
                condition._1,
                order.map(t -> t._1 + (t._2 ? " ASC" : " DESC")).mkString(", "));

        Rest.json(exchange,
                DB.query(sql,
                        ps -> {
                            int i = condition._2.set(ps, 1);
                            ps.setInt(i++, skipLimit._2);
                            ps.setLong(i++, skipLimit._1);
                        },
                        rs -> select.zipWithIndex()
                            .toLinkedMap(t -> Java.soft(() -> new Tuple2<>(t._1._1, t._1._2.get(rs, t._2 + 1))))));
    }

    void getRow(HttpExchange exchange, String table, String id) {
        Map<String, List<String>>           query   = Rest.queryParams(exchange);
        Map<String, String>                 columns = getColumns(table);
        List<Tuple2<String, DB.Extract<?>>> select  = getSelect(query, columns);

        Rest.json(exchange,
                DB.query(Java.format(QEURY_BY_ID, select.map(t -> t._1).mkString(", "), table),
                        ps -> ps.setLong(1, Long.parseLong(id)),
                        rs -> select.zipWithIndex()
                            .toLinkedMap(t -> Java.soft(() -> new Tuple2<>(t._1._1, t._1._2.get(rs, t._2 + 1)))))
                    .getOrElseThrow(() -> new Rest.Error(Rest.ErrorCode.NOT_FOUND,
                            "No item with id = ${0} is present in ${1} table",
                            id,
                            table)));
    }

    Map<String, String> getColumns(String table) {
        Map<String, String> columns = DB.query(QEURY_FOR_COLUMNS,
                ps -> ps.setString(1, table),
                rs -> new Tuple2<>(rs.getString(1), rs.getString(2)))
            .toLinkedMap(t -> t);

        if (columns.isEmpty()) {
            throw new Rest.Error(Rest.ErrorCode.NOT_FOUND,
                    "Table ${0} was not found",
                    table);
        }
        return columns;
    }

    List<Tuple2<String, DB.Extract<?>>> getSelect(Map<String, List<String>> query, Map<String, String> columns) {
        Option<List<String>> param = query.get(QueryParam.SELECT);

        return param.map(l -> l.flatMap(s -> List.of(s.split(","))))
            .getOrElse(columns.toList()
                .filter(t -> DB.DataType.SUPPORTED.contains(t._2))
                .map(t -> t._1))
            .map(c -> getSelectColumn(c, columns))
            .filter(t -> null != t._2)
            .toList();
    }

    Tuple2<String, DB.Extract<?>> getSelectColumn(String column, Map<String, String> columns) {
        String dataType = columns.get(column)
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

    Tuple2<String, DB.Inject> getConditions(Map<String, List<String>> query, Map<String, String> columns) {
        return andSqlInjects(query
            .filterKeys(k -> !k.startsWith(QueryParam.AUX_PREFIX))
            .map(t -> conditionSqlInject(
                    t._1,
                    columns.get(t._1)
                        .getOrElseThrow(() -> new Rest.Error(Rest.ErrorCode.BAD_REQUEST,
                                "Column ${0} is not present on requested table",
                                t._1)),
                    t._2)));
    }

    Tuple2<String, DB.Inject> andSqlInjects(Seq<Tuple2<String, DB.Inject>> conditions) {
        if (conditions.isEmpty()) {
            return new Tuple2<>("1 = 1", DB.Injects.NOTHING);
        }
        if (1 == conditions.size()) {
            return conditions.get();
        }
        return new Tuple2<>("(" + conditions.map(t -> t._1).mkString(") AND (") + ")", DB.fold(conditions.map(t -> t._2)));
    }

    Tuple2<String, DB.Inject> conditionSqlInject(String column, String dataType, List<String> values) {
        if (DB.DataType.TSVECTOR.equals(dataType)) {
            return new Tuple2<>("websearch_to_tsquery('english', ?) @@ " + column,
                    DB.Injects.Str.STRING.apply(values.mkString(" ")));
        }
        if (values.isEmpty()) {
            return new Tuple2<>(column + " IS NULL", DB.Injects.NOTHING);
        }
        Function<String, DB.Inject> inject = DB.DATA_TYPE_STRING_INJECT.get(dataType).get();
        if (1 == values.size()) {
            return new Tuple2<>(column + " = ?", inject.apply(values.get()));
        }
        return new Tuple2<>(column + " IN (" + Java.repeat("?", ", ", values.size()) + ")", DB.fold(values.map(inject)));
    }

    List<Tuple2<String, Boolean>> getOrder(Map<String, List<String>> query, Map<String, String> columns) {
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
            throw new Rest.Error(Rest.ErrorCode.BAD_REQUEST,
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
}
