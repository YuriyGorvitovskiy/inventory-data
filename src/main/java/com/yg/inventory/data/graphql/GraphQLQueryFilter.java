package com.yg.inventory.data.graphql;

import com.yg.inventory.model.db.Column;
import com.yg.inventory.model.db.ForeignKey;
import com.yg.inventory.model.db.Schema;
import com.yg.inventory.model.db.Table;
import com.yg.util.DB;

import graphql.Scalars;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLType;
import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;

public class GraphQLQueryFilter {

    static final Map<DB.DataType, GraphQLScalarType> DATA_TYPE_TO_SCALAR = HashMap.ofEntries(
            new Tuple2<>(DB.DataType.BIGINT, Scalars.GraphQLString),
            new Tuple2<>(DB.DataType.BOOLEAN, Scalars.GraphQLBoolean),
            new Tuple2<>(DB.DataType.INTEGER, Scalars.GraphQLInt),
            new Tuple2<>(DB.DataType.UUID, Scalars.GraphQLID),
            new Tuple2<>(DB.DataType.VARCHAR, Scalars.GraphQLString),
            new Tuple2<>(DB.DataType.TEXT_SEARCH_VECTOR, Scalars.GraphQLString));

    final Schema        schema;
    final GraphQLNaming naming;

    public GraphQLQueryFilter(Schema schema, GraphQLNaming naming) {
        this.schema = schema;
        this.naming = naming;
    }

    public List<GraphQLType> buildAllTypes() {
        return schema.tables.values()
            .map(this::buildTypeFor)
            .toList();
    }

    GraphQLType buildTypeFor(Table table) {
        return GraphQLInputObjectType.newInputObject()
            .name(naming.getFilterTypeName(table.name))
            .fields(buildScalarFields(table))
            .fields(buildOutgoingFields(table))
            .fields(buildIncomingFields(table))
            .build();
    }

    java.util.List<GraphQLInputObjectField> buildScalarFields(Table table) {
        return table.columns.values()
            .filter(c -> !table.outgoing.containsKey(c.name))
            .map(this::buildScalarField)
            .toJavaList();
    }

    java.util.List<GraphQLInputObjectField> buildOutgoingFields(Table table) {
        return table.outgoing.values()
            .map(this::buildOutgoingField)
            .toJavaList();
    }

    java.util.List<GraphQLInputObjectField> buildIncomingFields(Table table) {
        return table.incoming.values()
            .map(this::buildIncomingField)
            .toJavaList();
    }

    GraphQLInputObjectField buildScalarField(Column column) {
        return GraphQLInputObjectField.newInputObjectField()
            .name(column.name)
            .type(GraphQLList.list(DATA_TYPE_TO_SCALAR.get(column.type).get()))
            .build();
    }

    GraphQLInputObjectField buildOutgoingField(ForeignKey outgoing) {
        return GraphQLInputObjectField.newInputObjectField()
            .name(outgoing.fromColumn)
            .type(naming.getFilterTypeRef(outgoing.toTable))
            .build();
    }

    GraphQLInputObjectField buildIncomingField(ForeignKey outgoing) {
        return GraphQLInputObjectField.newInputObjectField()
            .name(outgoing.name)
            .type(naming.getFilterTypeRef(outgoing.fromTable))
            .build();
    }

}
