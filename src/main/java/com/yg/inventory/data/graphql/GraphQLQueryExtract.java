package com.yg.inventory.data.graphql;

import com.yg.inventory.model.db.Column;
import com.yg.inventory.model.db.ForeignKey;
import com.yg.inventory.model.db.Schema;
import com.yg.inventory.model.db.Table;
import com.yg.util.DB;

import graphql.Scalars;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLType;
import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;

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
}
