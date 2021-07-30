package org.statemach.db.graphql;

import org.statemach.db.jdbc.Extract;
import org.statemach.db.schema.ColumnInfo;
import org.statemach.db.schema.CompositeType;
import org.statemach.db.schema.DataType;
import org.statemach.db.schema.ForeignKey;
import org.statemach.db.schema.PrimaryKey;
import org.statemach.db.schema.Schema;
import org.statemach.db.schema.TableInfo;
import org.statemach.db.sql.DataAccess;
import org.statemach.db.sql.From;
import org.statemach.db.sql.Join;
import org.statemach.db.sql.SQLBuilder;
import org.statemach.db.sql.SchemaAccess;
import org.statemach.db.sql.Select;
import org.statemach.db.sql.TableLike;
import org.statemach.db.sql.View;
import org.statemach.util.Java;
import org.statemach.util.NodeLinkTree;

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
    final SQLBuilder          sqlBuilder;
    final GraphQLNaming       naming;
    final GraphQLQueryExtract extract;
    final GraphQLQueryFilter  filter;
    final GraphQLQueryOrder   order;

    GraphQLQuery(Schema schema,
                 DataAccess dataAccess,
                 SQLBuilder sqlBuilder,
                 GraphQLNaming naming,
                 GraphQLQueryExtract extract,
                 GraphQLQueryFilter filter,
                 GraphQLQueryOrder order) {
        this.schema = schema;
        this.dataAccess = dataAccess;
        this.sqlBuilder = sqlBuilder;
        this.naming = naming;
        this.extract = extract;
        this.filter = filter;
        this.order = order;
    }

    public static GraphQLQuery of(Schema schema, GraphQLNaming naming, DataAccess dataAccess) {
        GraphQLMapping mapping = GraphQLMapping.of(schema.vendor);

        return new GraphQLQuery(schema,
                dataAccess,
                dataAccess.builder(),
                naming,
                new GraphQLQueryExtract(schema, naming, mapping),
                new GraphQLQueryFilter(schema, dataAccess.builder(), naming, mapping),
                new GraphQLQueryOrder(schema, naming));
    }

    public void instrumentSchema(SchemaAccess schemaAccess) {
        List<CompositeType> existing = schemaAccess.getAllCompositeTypes();
        List<CompositeType> required = requiredCompositeTypes();

        List<CompositeType> create = required.removeAll(existing);
        List<String>        delete = create.map(c -> c.name).retainAll(existing.map(c -> c.name));

        delete.forEach(c -> schemaAccess.dropCompositeType(c));
        create.forEach(c -> schemaAccess.createCompositeType(c));
    }

    public List<CompositeType> requiredCompositeTypes() {
        return schema.tables.values()
            .flatMap(t -> t.outgoing.values())
            .filter(f -> f.matchingColumns.size() > 1)
            .distinct()
            .flatMap(f -> requiredCompositeTypes(f))
            .toList();
    }

    public List<CompositeType> requiredCompositeTypes(ForeignKey fk) {
        TableInfo from = schema.tables.get(fk.fromTable).get();
        TableInfo to   = schema.tables.get(fk.toTable).get();

        return List.of(
                new CompositeType(naming.getFromType(fk), fk.matchingColumns.map(c -> from.columns.get(c.from).get())),
                new CompositeType(naming.getToType(fk), fk.matchingColumns.map(c -> to.columns.get(c.to).get())));
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

    Tuple2<FieldCoordinates, DataFetcher<?>> buildQueryFetcher(TableInfo table) {
        return new Tuple2<>(
                FieldCoordinates.coordinates(QUERY_TYPE, table.name),
                e -> fetchQuery(table, e));
    }

    Object fetchQuery(TableInfo table, DataFetchingEnvironment environment) throws Exception {
        return fetchQueryCommon(GraphQLField.of(environment), table, Option.none(), Option.none())
            .toJavaList();
    }

    List<Map<String, Object>> fetchSubQuery(List<Map<String, Object>> result, SubQuery q) {
        Set<Map<String, Object>> ids         = result
            .map(r -> q.extracts
                .map(e -> new Tuple2<>(e.name, r.get(e.name).get()))
                .toMap(t -> t))
            .toSet();
        List<String>             fromColumns = q.incoming.matchingColumns.map(m -> m.from);

        List<java.util.Map<String, Object>> subResult = fetchQueryCommon(
                q.field,
                q.table,
                Option.of(fromColumns),
                Option.of(new Tuple2<>(q.incoming, ids)));

        Map<List<Object>, List<java.util.Map<String, Object>>> subResultById = subResult
            .groupBy(r -> fromColumns.map(r::get));

        return result.map(r -> putSubQueryResult(r, q, subResultById));
    }

    List<java.util.Map<String, Object>> fetchQueryCommon(GraphQLField field,
                                                         TableInfo table,
                                                         Option<List<String>> extraColumn,
                                                         Option<Tuple2<ForeignKey, Set<Map<String, Object>>>> foreignKeyWithIds) {
        ExtractPortion selects = extract.parse(table, field.getSelectionSet(), extraColumn);
        List<Filter>   filters = filter.parse(table, field.getArgument(Argument.FILTER));

        List<OrderBy>         orders    = order.parse(table, field.getArgument(Argument.ORDER));
        Integer               skip      = Java.ifNull((Integer) field.getArgument(Argument.SKIP), 0);
        Integer               limit     = Java.ifNull((Integer) field.getArgument(Argument.LIMIT), 10);
        Tuple2<Long, Integer> skipLimit = new Tuple2<>(skip.longValue(), limit);

        Option<View<String>> cte = Option.none();

        NodeLinkTree<String, TableInfo, ForeignKeyJoin> preparedJoins = filter.buildJoins(table, filters);
        List<View<String>>                              views         = List.empty();
        if (filters.exists(f -> f.plural)) {
            cte = Option.of(buildFilterView(preparedJoins, filters, foreignKeyWithIds));
            views = views.append(cte.get());
            filters = List.empty();
            preparedJoins = NodeLinkTree.of(table);
            foreignKeyWithIds = Option.none();
        }

        preparedJoins = order.buildJoins(preparedJoins, orders);
        preparedJoins = extract.buildJoins(preparedJoins, selects.values);
        View<Tuple2<String, Extract<?>>> extractView = buildExtractView(preparedJoins,
                cte,
                foreignKeyWithIds,
                selects.values,
                filters,
                orders,
                skipLimit);

        List<Map<String, Object>> subResult = dataAccess.query(views, extractView);
        subResult = selects.queries.foldLeft(subResult, this::fetchSubQuery);

        Map<String, List<String>> paths = selects.values.toMap(e -> new Tuple2<>(e.name, e.path))
            .merge(selects.queries.toMap(q -> new Tuple2<>(q.name, q.path)));
        return subResult.map(r -> buildGraphQLResult(r, paths, selects.keys));
    }

    java.util.Map<String, Object> buildGraphQLResult(Map<String, Object> row,
                                                     Map<String, List<String>> paths,
                                                     List<ForeignKeyPath> keysPaths) {
        List<List<String>> nullPaths = keysPaths
            // If all keys for the path are null, we should drop path the result tree
            .filter(f -> f.extracts.find(e -> null != row.get(e.name).getOrNull()).isEmpty())
            .map(f -> f.path);

        List<List<String>> topNullPaths = nullPaths
            // Remove longer paths if shorter is present
            .filter(n -> nullPaths.find(p -> n != p && n.startsWith(p)).isEmpty());

        java.util.Map<String, Object> result = new java.util.HashMap<>();

        paths
            // Filter paths that not starts with any of Top Null Path
            .filter(t -> topNullPaths.find(n -> t._2.startsWith(n)).isEmpty())
            .forEach(t -> putIntoMapTree(result, t._2, row.get(t._1).getOrNull()));

        topNullPaths.forEach(p -> putIntoMapTree(result, p, null));
        return result;
    }

    Map<String, Object> putSubQueryResult(Map<String, Object> row,
                                          SubQuery subQuery,
                                          Map<List<Object>, List<java.util.Map<String, Object>>> subResultById) {
        List<Object>                                key  = subQuery.extracts.map(e -> row.get(e.name).get());
        Option<List<java.util.Map<String, Object>>> list = subResultById.get(key);
        return row.put(subQuery.name, list.getOrElse(List.empty()).toJavaList());
    }

    void putIntoMapTree(java.util.Map<String, Object> tree, List<String> path, Object value) {
        @SuppressWarnings("unchecked")
        java.util.Map<String, Object> last = path
            .dropRight(1)
            .foldLeft(tree, (t, s) -> (java.util.Map<String, Object>) (t.computeIfAbsent(s, n -> new java.util.HashMap<>())));

        last.put(path.last(), value);
    }

    View<Tuple2<String, Extract<?>>> buildExtractView(NodeLinkTree<String, TableInfo, ForeignKeyJoin> preparedJoins,
                                                      Option<View<String>> cte,
                                                      Option<Tuple2<ForeignKey, Set<Map<String, Object>>>> foreignKeyWithIds,
                                                      List<ExtractValue> extracts,
                                                      List<Filter> filters,
                                                      List<OrderBy> orders,
                                                      Tuple2<Long, Integer> skipLimit) {

        var joins = mapJoins(preparedJoins);

        var select = extract.buildExtracts(joins, extracts);
        var where  = filter.buildWhere(joins, filters);
        var sort   = order.buildOrders(joins, orders);

        joins = prependJoins(joins, cte.map(c -> new Tuple2<>(c, preparedJoins.node.primary.get())), foreignKeyWithIds);

        return new View<Tuple2<String, Extract<?>>>(
                "",
                joins,
                where,
                sort,
                select,
                false,
                skipLimit._1,
                skipLimit._2);
    }

    View<String> buildFilterView(NodeLinkTree<String, TableInfo, ForeignKeyJoin> preparedJoins,
                                 List<Filter> filters,
                                 Option<Tuple2<ForeignKey, Set<Map<String, Object>>>> foreignKeyWithIds) {
        PrimaryKey                       pk    = preparedJoins.node.primary.get();
        NodeLinkTree<String, From, Join> joins = prependJoins(mapJoins(preparedJoins), Option.none(), foreignKeyWithIds);

        var select = pk.columns.map(c -> Select.of(joins.getNode().alias, c, c));
        var where  = filter.buildWhere(joins, filters);
        return new View<>(
                CTE_FILTER_NAME,
                joins,
                where,
                List.empty(),
                select,
                true,
                0L,
                Integer.MAX_VALUE);
    }

    NodeLinkTree<String, From, Join> mapJoins(NodeLinkTree<String, TableInfo, ForeignKeyJoin> preparedJoins) {
        return preparedJoins
            .mapNodesWithIndex(1, (t, i) -> new From(TableLike.of(schema, t), "t" + i))
            .mapLinksWithNodes(t -> buildJoin(t._1, t._2, t._3));
    }

    NodeLinkTree<String, From, Join> prependJoins(NodeLinkTree<String, From, Join> joins,
                                                  Option<Tuple2<View<String>, PrimaryKey>> cteWithPrimaryKey,
                                                  Option<Tuple2<ForeignKey, Set<Map<String, Object>>>> foreignKeyWithIds) {

        if (cteWithPrimaryKey.isDefined()) {
            PrimaryKey pk = cteWithPrimaryKey.get()._2;
            joins = NodeLinkTree.<String, From, Join>of(new From(TableLike.of(cteWithPrimaryKey.get()._1), "t"))
                .put("", buildCteJoin("t", pk.columns.map(c -> new ForeignKey.Match(c, c)), joins.getNode()), joins);
        } else if (foreignKeyWithIds.isDefined()) {
            ForeignKey fk    = foreignKeyWithIds.get()._1;
            TableLike  array = buildForeignKeyToArray(fk, foreignKeyWithIds.get()._2);

            joins = NodeLinkTree.<String, From, Join>of(new From(array, "t"))
                .put("", buildToArrayJoin("t", fk.matchingColumns, joins.getNode()), joins);
        }
        return joins;
    }

    TableLike buildForeignKeyToArray(ForeignKey fk, Set<Map<String, Object>> ids) {
        TableInfo        table   = schema.tables.get(fk.toTable).get();
        List<ColumnInfo> columns = fk.matchingColumns.map(m -> table.columns.get(m.to).get());
        if (columns.size() == 1) {
            ColumnInfo column = columns.get();
            return sqlBuilder.arrayAsTable(columns.get(), ids.map(m -> m.get(column.name).getOrNull()));
        }

        DataType type = new DataType(naming.getToType(fk));
        return sqlBuilder.arrayAsTable(type, columns, ids);
    }

    NodeLinkTree<String, From, Join> buildJoins(NodeLinkTree<String, TableInfo, ForeignKeyJoin> preparedJoins) {
        return preparedJoins
            .mapNodesWithIndex(1, (t, i) -> new From(TableLike.of(schema, t), "f" + i))
            .mapLinksWithNodes(t -> buildJoin(t._1, t._2, t._3));
    }

    Join buildJoin(From left, ForeignKeyJoin preparedJoin, From right) {
        return new Join(preparedJoin.joinKind,
                sqlBuilder.and(preparedJoin.foreignKey.matchingColumns
                    .map(m -> sqlBuilder.equal(
                            Select.of(left.alias, preparedJoin.outgoing ? m.from : m.to),
                            Select.of(right.alias, preparedJoin.outgoing ? m.to : m.from)))));
    }

    Join buildCteJoin(String cteAlias, List<ForeignKey.Match> matchingColumns, From right) {
        return new Join(Join.Kind.INNER,
                sqlBuilder.and(matchingColumns
                    .map(m -> sqlBuilder.equal(
                            Select.of(cteAlias, m.from),
                            Select.of(right.alias, m.to)))));
    }

    Join buildToArrayJoin(String cteAlias, List<ForeignKey.Match> matchingColumns, From right) {
        return new Join(Join.Kind.INNER,
                sqlBuilder.and(matchingColumns
                    .map(m -> sqlBuilder.equal(
                            Select.of(cteAlias, m.to),
                            Select.of(right.alias, m.from)))));
    }
}
