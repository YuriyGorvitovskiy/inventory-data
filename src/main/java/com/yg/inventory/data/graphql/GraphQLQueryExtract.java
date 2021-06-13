package com.yg.inventory.data.graphql;

import com.yg.inventory.data.db.Join;
import com.yg.inventory.data.db.View;
import com.yg.inventory.model.db.Column;
import com.yg.inventory.model.db.ForeignKey;
import com.yg.inventory.model.db.Schema;
import com.yg.inventory.model.db.Table;
import com.yg.util.DB;
import com.yg.util.Tree;

import graphql.Scalars;
import graphql.schema.DataFetchingFieldSelectionSet;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLType;
import graphql.schema.SelectedField;
import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.control.Either;
import io.vavr.control.Option;

public class GraphQLQueryExtract {

    static final Map<DB.DataType, GraphQLScalarType> DATA_TYPE_TO_SCALAR = HashMap.ofEntries(
            new Tuple2<>(DB.DataType.BIGINT, Scalars.GraphQLString),
            new Tuple2<>(DB.DataType.BOOLEAN, Scalars.GraphQLBoolean),
            new Tuple2<>(DB.DataType.INTEGER, Scalars.GraphQLInt),
            new Tuple2<>(DB.DataType.UUID, Scalars.GraphQLID),
            new Tuple2<>(DB.DataType.VARCHAR, Scalars.GraphQLString));

    final Schema        schema;
    final GraphQLNaming naming;

    public GraphQLQueryExtract(Schema schema, GraphQLNaming naming) {
        this.schema = schema;
        this.naming = naming;
    }

    public List<GraphQLType> buildAllTypes() {
        return schema.tables.values()
            .map(this::buildType)
            .toList();
    }

    GraphQLType buildType(Table table) {
        return GraphQLObjectType.newObject()
            .name(naming.getExtractTypeName(table.name))
            .fields(buildScalarFields(table))
            .fields(buildOutgoingFields(table))
            .fields(buildIncomingFields(table))
            .build();
    }

    java.util.List<GraphQLFieldDefinition> buildScalarFields(Table table) {
        return table.columns.values()
            .filter(this::isExtractableColumn)
            .filter(c -> !table.outgoing.containsKey(c.name))
            .map(this::buildScalarField)
            .toJavaList();
    }

    java.util.List<GraphQLFieldDefinition> buildOutgoingFields(Table table) {
        return table.outgoing.values()
            .map(this::buildOutgoingField)
            .toJavaList();
    }

    java.util.List<GraphQLFieldDefinition> buildIncomingFields(Table table) {
        return table.incoming.values()
            .map(this::buildIncomingField)
            .toJavaList();
    }

    GraphQLFieldDefinition buildScalarField(Column column) {
        return GraphQLFieldDefinition.newFieldDefinition()
            .name(column.name)
            .type(DATA_TYPE_TO_SCALAR.get(column.type).get())
            .build();
    }

    GraphQLFieldDefinition buildOutgoingField(ForeignKey outgoing) {
        return GraphQLFieldDefinition.newFieldDefinition()
            .name(outgoing.fromColumn)
            .type(naming.getExtractTypeRef(outgoing.toTable))
            .build();
    }

    GraphQLFieldDefinition buildIncomingField(ForeignKey outgoing) {
        return buildQueryField(outgoing.name, outgoing.fromTable);
    }

    GraphQLFieldDefinition buildQueryField(String fieldName, String tableName) {
        return GraphQLFieldDefinition.newFieldDefinition()
            .name(fieldName)
            .type(GraphQLList.list(naming.getExtractTypeRef(tableName)))
            .argument(GraphQLArgument.newArgument()
                .name(GraphQLQuery.Argument.FILTER)
                .type(naming.getFilterTypeRef(tableName)))
            .argument(GraphQLArgument.newArgument()
                .name(GraphQLQuery.Argument.ORDER)
                .type(GraphQLList.list(naming.getOrderTypeRef(tableName))))
            .argument(GraphQLArgument.newArgument()
                .name(GraphQLQuery.Argument.SKIP)
                .type(Scalars.GraphQLInt))
            .argument(GraphQLArgument.newArgument()
                .name(GraphQLQuery.Argument.LIMIT)
                .type(Scalars.GraphQLInt))
            .build();
    }

    boolean isExtractableColumn(Column column) {
        return DB.DataType.TEXT_SEARCH_VECTOR != column.type;
    }

    public Tuple2<List<Extract>, List<SubQuery>> parse(Table table,
                                                       DataFetchingFieldSelectionSet selection,
                                                       Option<String> extraColumn) {
        List<Either<Extract, SubQuery>> result = parse(List.empty(), table, selection)
            .appendAll(parseExtraColumn(table, extraColumn));

        return result.partition(e -> e.isLeft())
            .map1(l -> l.map(e -> e.getLeft()).distinctBy(e -> e.name))
            .map2(l -> l.map(e -> e.get()));
    }

    Option<Either<Extract, SubQuery>> parseExtraColumn(Table table, Option<String> extraColumn) {
        if (extraColumn.isEmpty()) {
            return Option.none();
        }
        Column column = table.columns.get(extraColumn.get()).get();
        return Option.of(Either.left(Extract.of(List.of(column.name), column.type)));
    }

    List<Either<Extract, SubQuery>> parse(List<String> path, Table table, DataFetchingFieldSelectionSet selection) {
        return List.ofAll(selection.getImmediateFields())
            .flatMap(f -> parse(path, table, f));
    }

    List<Either<Extract, SubQuery>> parse(List<String> tablePath, Table table, SelectedField field) {
        String       name      = field.getName();
        List<String> fieldPath = tablePath.append(name);

        Option<ForeignKey> outgoing = table.outgoing.get(name);
        if (outgoing.isDefined()) {
            Option<Table> join = schema.tables.get(outgoing.get().toTable);
            if (join.isEmpty()) {
                return List.empty();
            }
            return parse(fieldPath, join.get(), field.getSelectionSet());
        }

        Option<Column> column = table.columns.get(name);
        if (column.isDefined()) {
            return List.of(Either.left(Extract.of(fieldPath, column.get().type)));
        }

        Option<ForeignKey> incoming = table.incoming.get(name);
        if (incoming.isDefined()) {
            column = table.columns.get(incoming.get().toColumn);
            Option<Table> join = schema.tables.get(incoming.get().fromTable);
            if (column.isEmpty() || join.isEmpty()) {
                return List.empty();
            }
            Extract  extract = Extract.of(tablePath.append(column.get().name), column.get().type);
            SubQuery query   = SubQuery.of(fieldPath, extract, incoming.get(), join.get(), field);
            return List.of(Either.left(extract), Either.right(query));

        }
        return List.empty();
    }

    public Tree<String, Table, ForeignKeyJoin> buildJoins(Tree<String, Table, ForeignKeyJoin> tree, List<Extract> extracts) {
        return extracts.foldLeft(tree,
                (t, e) -> t.putIfMissed(e.path.dropRight(1), (p, c) -> buildJoin(p, c, Join.Kind.LEFT)));
    }

    Tuple2<ForeignKeyJoin, Table> buildJoin(Table parent, String step, Join.Kind join) {
        ForeignKey outgoing = parent.outgoing.get(step).get();
        return new Tuple2<>(new ForeignKeyJoin(join, outgoing, true), schema.tables.get(outgoing.toTable).get());
    }

    public List<com.yg.inventory.data.db.Column<DB.Extract<?>>> buildExtracts(Tree<String, View.Node, Join> joinTree,
                                                                              List<Extract> extracts) {
        return extracts.map(e -> buildExtract(joinTree, e));
    }

    com.yg.inventory.data.db.Column<DB.Extract<?>> buildExtract(Tree<String, View.Node, Join> joinTree, Extract extract) {
        String alias  = joinTree.getNode(extract.path.dropRight(1)).get().alias;
        String column = extract.path.last();
        return new com.yg.inventory.data.db.Column<>(alias, column, extract.name, DB.DATA_TYPE_EXTRACT.get(extract.type).get());
    }

}
