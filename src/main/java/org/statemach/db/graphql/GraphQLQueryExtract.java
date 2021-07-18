package org.statemach.db.graphql;

import org.statemach.db.schema.ColumnInfo;
import org.statemach.db.schema.ForeignKey;
import org.statemach.db.schema.Schema;
import org.statemach.db.schema.TableInfo;
import org.statemach.db.sql.From;
import org.statemach.db.sql.Join;
import org.statemach.db.sql.Select;
import org.statemach.util.NodeLinkTree;

import graphql.Scalars;
import graphql.schema.DataFetchingFieldSelectionSet;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLType;
import graphql.schema.SelectedField;
import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.control.Either;
import io.vavr.control.Option;

public class GraphQLQueryExtract {

    final Schema         schema;
    final GraphQLNaming  naming;
    final GraphQLMapping mapping;

    public GraphQLQueryExtract(Schema schema, GraphQLNaming naming, GraphQLMapping mapping) {
        this.schema = schema;
        this.naming = naming;
        this.mapping = mapping;
    }

    public List<GraphQLType> buildAllTypes() {
        return schema.tables.values()
            .map(this::buildType)
            .toList();
    }

    GraphQLType buildType(TableInfo table) {
        return GraphQLObjectType.newObject()
            .name(naming.getExtractTypeName(table.name))
            .fields(buildScalarFields(table))
            .fields(buildOutgoingFields(table))
            .fields(buildIncomingFields(table))
            .build();
    }

    java.util.List<GraphQLFieldDefinition> buildScalarFields(TableInfo table) {
        return table.columns.values()
            .filter(this::isExtractableColumn)
            .map(c -> buildScalarField(table, c))
            .toJavaList();
    }

    java.util.List<GraphQLFieldDefinition> buildOutgoingFields(TableInfo table) {
        return table.outgoing.values()
            .map(this::buildOutgoingField)
            .toJavaList();
    }

    java.util.List<GraphQLFieldDefinition> buildIncomingFields(TableInfo table) {
        return table.incoming.values()
            .map(this::buildIncomingField)
            .toJavaList();
    }

    GraphQLFieldDefinition buildScalarField(TableInfo table, ColumnInfo column) {
        return GraphQLFieldDefinition.newFieldDefinition()
            .name(column.name)
            .type(mapping.scalar(table, column))
            .build();
    }

    GraphQLFieldDefinition buildOutgoingField(ForeignKey outgoing) {
        return GraphQLFieldDefinition.newFieldDefinition()
            .name(outgoing.name)
            .type(naming.getExtractTypeRef(outgoing.toTable))
            .build();
    }

    GraphQLFieldDefinition buildIncomingField(ForeignKey incoming) {
        return buildQueryField(naming.getReverseName(incoming.name), incoming.fromTable);
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

    boolean isExtractableColumn(ColumnInfo column) {
        return mapping.isExtractable(column.type);
    }

    public Tuple2<List<Extract>, List<SubQuery>> parse(TableInfo table,
                                                       DataFetchingFieldSelectionSet selection,
                                                       Option<List<String>> extraColumn) {
        List<Either<Extract, SubQuery>> result = parse(List.empty(), table, selection)
            .appendAll(parseExtraColumn(table, extraColumn));

        return result.partition(e -> e.isLeft())
            .map1(l -> l.map(e -> e.getLeft()).distinctBy(e -> e.name))
            .map2(l -> l.map(e -> e.get()));
    }

    List<Either<Extract, SubQuery>> parseExtraColumn(TableInfo table, Option<List<String>> extraColumn) {
        if (extraColumn.isEmpty()) {
            return List.empty();
        }
        List<ColumnInfo> columns = extraColumn.get().map(c -> table.columns.get(c).get());
        return columns.map(c -> Either.left(Extract.of(List.of(c.name), c.type)));
    }

    List<Either<Extract, SubQuery>> parse(List<String> path, TableInfo table, DataFetchingFieldSelectionSet selection) {
        return List.ofAll(selection.getImmediateFields())
            .flatMap(f -> parse(path, table, f));
    }

    List<Either<Extract, SubQuery>> parse(List<String> tablePath, TableInfo table, SelectedField field) {
        String       name      = field.getName();
        List<String> fieldPath = tablePath.append(name);

        Option<ForeignKey> outgoing = table.outgoing.get(name);
        if (outgoing.isDefined()) {
            Option<TableInfo> join = schema.tables.get(outgoing.get().toTable);
            if (join.isEmpty()) {
                return List.empty();
            }
            return parse(fieldPath, join.get(), field.getSelectionSet());
        }

        Option<ColumnInfo> column = table.columns.get(name);
        if (column.isDefined()) {
            return List.of(Either.left(Extract.of(fieldPath, column.get().type)));
        }

        Option<ForeignKey> incoming = table.incoming.get(naming.getReverseName(name));
        if (incoming.isDefined()) {
            Option<TableInfo> join = schema.tables.get(incoming.get().fromTable);
            if (column.isEmpty() || join.isEmpty()) {
                return List.empty();
            }
            List<Extract> extracts = incoming.get().matchingColumns
                .map(m -> {
                                           ColumnInfo c = table.columns.get(m.to).get();
                                           return Extract.of(tablePath.append(c.name), c.type);
                                       });
            SubQuery      query    = SubQuery.of(fieldPath, extracts, incoming.get(), join.get(), field);
            return extracts.map(Either::<Extract, SubQuery>left).append(Either.right(query));
        }
        return List.empty();
    }

    public NodeLinkTree<String, TableInfo, ForeignKeyJoin> buildJoins(NodeLinkTree<String, TableInfo, ForeignKeyJoin> tree,
                                                                      List<Extract> extracts) {
        return extracts.foldLeft(tree,
                (t, e) -> t.putIfMissed(e.path.dropRight(1), (p, c) -> buildJoin(p, c, Join.Kind.LEFT)));
    }

    Tuple2<ForeignKeyJoin, TableInfo> buildJoin(TableInfo parent, String step, Join.Kind join) {
        ForeignKey outgoing = parent.outgoing.get(step).get();
        return new Tuple2<>(new ForeignKeyJoin(join, outgoing, true), schema.tables.get(outgoing.toTable).get());
    }

    public List<Select<Tuple2<String, org.statemach.db.jdbc.Extract<?>>>> buildExtracts(NodeLinkTree<String, From, Join> joinTree,
                                                                                        List<Extract> extracts) {
        return extracts.map(e -> buildExtract(joinTree, e));
    }

    Select<Tuple2<String, org.statemach.db.jdbc.Extract<?>>> buildExtract(NodeLinkTree<String, From, Join> joinTree,
                                                                          Extract extract) {
        String alias  = joinTree.getNode(extract.path.dropRight(1)).get().alias;
        String column = extract.path.last();
        return Select.of(alias, column, new Tuple2<>(extract.name, mapping.extract(extract.type)));
    }

}
