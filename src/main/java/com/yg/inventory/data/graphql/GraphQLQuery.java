package com.yg.inventory.data.graphql;

import java.util.Collections;
import java.util.UUID;

import com.yg.inventory.data.db.Condition;
import com.yg.inventory.data.db.DataAccess;
import com.yg.inventory.data.db.Join;
import com.yg.inventory.data.db.SQL;
import com.yg.inventory.data.db.View;
import com.yg.inventory.model.db.Schema;
import com.yg.inventory.model.db.Table;
import com.yg.util.DB;
import com.yg.util.Java;
import com.yg.util.Tree;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.FieldCoordinates;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLType;
import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Set;
import io.vavr.control.Option;

public class GraphQLQuery {

    static interface Argument {
        static final String FILTER = "filter";
        static final String LIMIT  = "limit";
        static final String ORDER  = "order";
        static final String SKIP   = "skip";
    }

    static final String QUERY_TYPE     = "QueryType";
    static final String ID_COLUMN_NAME = "id";

    static final String CTE_FILTER_NAME = "filter";

    final Schema              schema;
    final DataAccess          dataAccess;
    final GraphQLQueryExtract extract;
    final GraphQLQueryFilter  filter;
    final GraphQLQueryOrder   order;

    GraphQLQuery(Schema schema,
                 DataAccess dataAccess,
                 GraphQLQueryExtract extract,
                 GraphQLQueryFilter filter,
                 GraphQLQueryOrder order) {
        this.schema = schema;
        this.dataAccess = dataAccess;
        this.extract = extract;
        this.filter = filter;
        this.order = order;
    }

    public static GraphQLQuery of(Schema schema, GraphQLNaming naming, DataAccess dataAccess) {
        return new GraphQLQuery(schema,
                dataAccess,
                new GraphQLQueryExtract(schema, naming),
                new GraphQLQueryFilter(schema, naming),
                new GraphQLQueryOrder(schema, naming));
    }

    public GraphQLObjectType buildQueryType() {
        return GraphQLObjectType.newObject()
            .name(QUERY_TYPE)
            .fields(schema.tables.keySet()
                .map(t -> extract.buildQueryField(t, t))
                .toJavaList())
            .build();
    }

    public List<GraphQLType> buildAddtionalTypes() {
        return extract.buildAllTypes()
            .appendAll(filter.buildAllTypes())
            .appendAll(order.buildAllTypes());
    }

    public List<Tuple2<FieldCoordinates, DataFetcher<?>>> buildAllFetchers() {
        return buildQueryFetchers();
    }

    List<Tuple2<FieldCoordinates, DataFetcher<?>>> buildQueryFetchers() {
        return schema.tables.values()
            .map(this::buildQueryFetcher)
            .toList();
    }

    Tuple2<FieldCoordinates, DataFetcher<?>> buildQueryFetcher(Table table) {
        return new Tuple2<>(
                FieldCoordinates.coordinates(QUERY_TYPE, table.name),
                e -> fetchQuery(table, e));
    }

    Object fetchQuery(Table table, DataFetchingEnvironment environment) throws Exception {
        return fetchQueryCommon(GraphQLField.of(environment), table, Option.none(), Option.none())
            .toJavaList();
    }

    List<Map<String, Object>> fetchSubQuery(List<Map<String, Object>> result, SubQuery q) {
        Set<UUID> ids = result.map(r -> (UUID) r.get(q.extract.name).get()).toSet();

        List<java.util.Map<String, Object>> subResult = fetchQueryCommon(
                q.field,
                q.table,
                Option.of(q.incoming.fromColumn),
                Option.of(new Tuple2<>(q.incoming.fromColumn, ids)));

        Map<UUID, List<java.util.Map<String, Object>>> subResultById = subResult
            .groupBy(r -> (UUID) r.get(q.incoming.fromColumn));

        return result.map(r -> putSubQueryResult(r, q, subResultById));
    }

    List<java.util.Map<String, Object>> fetchQueryCommon(GraphQLField field,
                                                         Table table,
                                                         Option<String> extraColumn,
                                                         Option<Tuple2<String, Set<UUID>>> columnNameWithIds) {
        Tuple2<List<Extract>, List<SubQuery>> selects = extract.parse(table, field.getSelectionSet(), extraColumn);
        List<Filter>                          filters = filter.parse(
                table,
                field.getArgument(Argument.FILTER),
                columnNameWithIds);

        List<OrderBy>         orders    = order.parse(table, field.getArgument(Argument.ORDER));
        Integer               skip      = Java.ifNull((Integer) field.getArgument(Argument.SKIP), 0);
        Integer               limit     = Java.ifNull((Integer) field.getArgument(Argument.LIMIT), 10);
        Tuple2<Long, Integer> skipLimit = new Tuple2<>(skip.longValue(), limit);

        Option<Tuple2<String, String>> nameCteAndColumn = Option.none();

        Tree<String, Table, ForeignKeyJoin> preparedJoins = filter.buildJoins(table, filters);
        List<View<Void>>                    views         = List.empty();
        if (filters.exists(f -> f.plural)) {
            views = views.append(buildFilterView(preparedJoins, filters));
            nameCteAndColumn = Option.of(new Tuple2<>(CTE_FILTER_NAME, ID_COLUMN_NAME));
            filters = List.empty();
            preparedJoins = Tree.of(table);
        }

        preparedJoins = order.buildJoins(preparedJoins, orders);
        preparedJoins = extract.buildJoins(preparedJoins, selects._1);
        View<DB.Extract<?>> extractView = buildExtractView(preparedJoins,
                nameCteAndColumn,
                selects._1,
                filters,
                orders,
                skipLimit);

        List<Map<String, Object>> subResult = dataAccess.queryByView(views, extractView);
        subResult = selects._2.foldLeft(subResult, this::fetchSubQuery);

        Map<String, List<String>> paths = selects._1.toMap(e -> new Tuple2<>(e.name, e.path))
            .merge(selects._2.toMap(q -> new Tuple2<>(q.name, q.path)));
        return subResult.map(r -> buildGraphQLResult(r, paths));
    }

    java.util.Map<String, Object> buildGraphQLResult(Map<String, Object> row, Map<String, List<String>> paths) {
        java.util.Map<String, Object> result = new java.util.HashMap<>();
        row.forEach(t -> putIntoMapTree(result, paths.get(t._1).get(), t._2));
        return result;
    }

    Map<String, Object> putSubQueryResult(Map<String, Object> row,
                                          SubQuery subQuery,
                                          Map<UUID, List<java.util.Map<String, Object>>> subResultById) {
        Option<?> key = row.get(subQuery.extract.name);
        if (key.isDefined()) {
            Option<List<java.util.Map<String, Object>>> list = subResultById.get((UUID) key.get());
            if (list.isDefined()) {
                return row.put(subQuery.name, list.get().toJavaList());
            }
        }
        return row.put(subQuery.name, Collections.emptyList());
    }

    void putIntoMapTree(java.util.Map<String, Object> tree, List<String> path, Object value) {
        @SuppressWarnings("unchecked")
        java.util.Map<String, Object> last = path
            .dropRight(1)
            .foldLeft(tree, (t, s) -> (java.util.Map<String, Object>) (t.computeIfAbsent(s, n -> new java.util.HashMap<>())));

        last.put(path.last(), value);
    }

    View<DB.Extract<?>> buildExtractView(Tree<String, Table, ForeignKeyJoin> preparedJoins,
                                         Option<Tuple2<String, String>> nameCteAndColumn,
                                         List<Extract> extracts,
                                         List<Filter> filters,
                                         List<OrderBy> orders,
                                         Tuple2<Long, Integer> skipLimit) {
        Tree<String, View.Node, Join> joins = preparedJoins
            .mapNodesWithIndex(1, (t, i) -> new View.Node(t.name, "q" + i))
            .mapLinksWithNodes(t -> buildJoin(t._1, t._2, t._3));

        var select = extract.buildExtracts(joins, extracts);
        var where  = filter.buildWhere(joins, filters);
        var sort   = order.buildOrders(joins, orders);

        if (nameCteAndColumn.isDefined()) {
            joins = Tree.<String, View.Node, Join>of(new View.Node(nameCteAndColumn.get()._1, "f"))
                .put("", buildCteJoin("f", nameCteAndColumn.get()._2, joins.getNode()), joins);
        }

        return new View<DB.Extract<?>>(
                "",
                joins,
                where,
                sort,
                select,
                false,
                skipLimit._1,
                skipLimit._2);
    }

    View<Void> buildFilterView(Tree<String, Table, ForeignKeyJoin> preparedJoins, List<Filter> filters) {
        Tree<String, View.Node, Join> joins = preparedJoins
            .mapNodesWithIndex(1, (t, i) -> new View.Node(t.name, "f" + i))
            .mapLinksWithNodes(t -> buildJoin(t._1, t._2, t._3));

        var select = List
            .of(new com.yg.inventory.data.db.Column<Void>(joins.getNode().alias, ID_COLUMN_NAME, ID_COLUMN_NAME, null));
        var where  = filter.buildWhere(joins, filters);
        return new View<Void>(
                CTE_FILTER_NAME,
                joins,
                where,
                List.empty(),
                select,
                true,
                0L,
                Integer.MAX_VALUE);
    }

    Tree<String, View.Node, Join> buildJoins(Tree<String, Table, ForeignKeyJoin> preparedJoins) {
        return preparedJoins
            .mapNodesWithIndex(1, (t, i) -> new View.Node(t.name, "f" + i))
            .mapLinksWithNodes(t -> buildJoin(t._1, t._2, t._3));
    }

    Join buildJoin(View.Node left, ForeignKeyJoin preparedJoin, View.Node right) {
        String leftColumn  = preparedJoin.outgoing ? preparedJoin.foreignKey.fromColumn : preparedJoin.foreignKey.toColumn;
        String rightColumn = preparedJoin.outgoing ? preparedJoin.foreignKey.toColumn : preparedJoin.foreignKey.fromColumn;
        return new Join(preparedJoin.joinKind,
                Condition.equal(
                        left.alias + SQL.DOT + leftColumn,
                        right.alias + SQL.DOT + rightColumn));
    }

    Join buildCteJoin(String cteAlias, String cteColumn, View.Node right) {
        return new Join(Join.Kind.INNER,
                Condition.equal(
                        cteAlias + SQL.DOT + cteColumn,
                        right.alias + SQL.DOT + ID_COLUMN_NAME));
    }
}
