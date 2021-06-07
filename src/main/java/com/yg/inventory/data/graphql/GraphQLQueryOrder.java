package com.yg.inventory.data.graphql;

import com.yg.inventory.model.db.Column;
import com.yg.inventory.model.db.ForeignKey;
import com.yg.inventory.model.db.Schema;
import com.yg.inventory.model.db.Table;
import com.yg.util.DB;

import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import io.vavr.collection.List;
import io.vavr.control.Option;

public class GraphQLQueryOrder {

    static interface Sort {
        static final String ASC  = "ASC";
        static final String DESC = "DESC";
    }

    static final String               SORTING_ORDER_TYPE_NAME = "SortingOrder";
    static final GraphQLTypeReference SORTING_ORDER_TYPE_REF  = GraphQLTypeReference.typeRef(SORTING_ORDER_TYPE_NAME);

    final Schema        schema;
    final GraphQLNaming naming;

    public GraphQLQueryOrder(Schema schema, GraphQLNaming naming) {
        this.schema = schema;
        this.naming = naming;
    }

    public List<GraphQLType> buildAllTypes() {
        return schema.tables.values()
            .map(this::buildType)
            .append(buildSortingOrderEnum())
            .toList();
    }

    GraphQLType buildType(Table table) {
        return GraphQLInputObjectType.newInputObject()
            .name(naming.getOrderTypeName(table.name))
            .fields(buildFields(table))
            .build();
    }

    GraphQLType buildSortingOrderEnum() {
        return GraphQLEnumType.newEnum()
            .name(SORTING_ORDER_TYPE_NAME)
            .value(Sort.ASC)
            .value(Sort.DESC)
            .build();
    }

    java.util.List<GraphQLInputObjectField> buildFields(Table table) {
        return table.columns.values()
            .filter(this::isOrderableColumn)
            .map(c -> buildField(c, table.outgoing.get(c.name)))
            .toJavaList();
    }

    boolean isOrderableColumn(Column column) {
        return DB.DataType.TEXT_SEARCH_VECTOR != column.type;
    }

    GraphQLInputObjectField buildField(Column column, Option<ForeignKey> outgoing) {
        return GraphQLInputObjectField.newInputObjectField()
            .name(column.name)
            .type(outgoing.isDefined()
                    ? naming.getOrderTypeRef(outgoing.get().toTable)
                    : SORTING_ORDER_TYPE_REF)
            .build();
    }

}
