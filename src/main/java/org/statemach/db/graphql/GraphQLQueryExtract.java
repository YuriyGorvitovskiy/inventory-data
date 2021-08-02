package org.statemach.db.graphql;

import org.statemach.db.jdbc.Extract;
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

    public ExtractPortion parse(TableInfo table,
                                DataFetchingFieldSelectionSet selection,
                                Option<List<String>> extraColumn) {
        return parse(List.empty(), table, selection)
            .append(parseExtraColumn(table, extraColumn))
            .distinctValues();
    }

    ExtractPortion parseExtraColumn(TableInfo table, Option<List<String>> extraColumn) {
        if (extraColumn.isEmpty()) {
            return ExtractPortion.EMPTY;
        }

        List<ColumnInfo> columns = extraColumn.get().map(c -> table.columns.get(c).get());
        return ExtractPortion.ofValues(columns.map(c -> ExtractValue.of(List.of(c.name), c.type)));
    }

    ExtractPortion parse(List<String> path, TableInfo table, DataFetchingFieldSelectionSet selection) {
        return List.ofAll(selection.getImmediateFields())
            .foldLeft(ExtractPortion.EMPTY, (a, f) -> a.append(parse(path, table, f)));
    }

    ExtractPortion parse(List<String> tablePath, TableInfo table, SelectedField field) {
        String       name      = field.getName();
        List<String> fieldPath = tablePath.append(name);

        Option<ForeignKey> outgoing = table.outgoing.get(name);
        if (outgoing.isDefined()) {
            TableInfo join = schema.tables.get(outgoing.get().toTable).get();
            return outgoingExtract(fieldPath, outgoing.get(), join)
                .append(parse(fieldPath, join, field.getSelectionSet()));
        }

        Option<ColumnInfo> column = table.columns.get(name);
        if (column.isDefined()) {
            return ExtractPortion.ofValue(ExtractValue.of(fieldPath, column.get().type));
        }

        Option<ForeignKey> incoming = table.incoming.get(naming.getForeignKey(name));
        TableInfo          join     = schema.tables.get(incoming.get().fromTable).get();
        return incomingExtract(fieldPath, incoming.get(), table, join, field);
    }

    ExtractPortion outgoingExtract(List<String> path, ForeignKey outgoing, TableInfo to) {
        return ExtractPortion.ofKey(path, foreignKeyExtracts(path, outgoing, to));
    }

    ExtractPortion incomingExtract(List<String> path,
                                   ForeignKey incoming,
                                   TableInfo to,
                                   TableInfo from,
                                   SelectedField field) {
        SubQuery query = SubQuery.of(path, foreignKeyExtracts(path.dropRight(1), incoming, to), incoming, from, field);
        return ExtractPortion.ofQuery(query);
    }

    List<ExtractValue> foreignKeyExtracts(List<String> path, ForeignKey foreignKey, TableInfo to) {
        return foreignKey.matchingColumns
            .map(m -> ExtractValue.of(path.append(m.to), to.columns.get(m.to).get().type));
    }

    public NodeLinkTree<String, TableInfo, ForeignKeyJoin> buildJoins(NodeLinkTree<String, TableInfo, ForeignKeyJoin> tree,
                                                                      List<ExtractValue> extracts) {
        return extracts.foldLeft(tree,
                (t, e) -> t.putIfMissed(e.path.dropRight(1), (p, c) -> buildJoin(p, c, Join.Kind.LEFT)));
    }

    Tuple2<ForeignKeyJoin, TableInfo> buildJoin(TableInfo parent, String step, Join.Kind join) {
        ForeignKey outgoing = parent.outgoing.get(step).get();
        return new Tuple2<>(new ForeignKeyJoin(join, outgoing, true), schema.tables.get(outgoing.toTable).get());
    }

    public List<Select<Tuple2<String, Extract<?>>>> buildExtracts(NodeLinkTree<String, From, Join> joinTree,
                                                                  List<ExtractValue> extracts) {
        return extracts.map(e -> buildExtract(joinTree, e));
    }

    Select<Tuple2<String, Extract<?>>> buildExtract(NodeLinkTree<String, From, Join> joinTree,
                                                    ExtractValue extract) {
        String alias  = joinTree.getNode(extract.path.dropRight(1)).get().alias;
        String column = extract.path.last();
        return Select.of(alias, column, new Tuple2<>(extract.name, mapping.extract(extract.type)));
    }

}
