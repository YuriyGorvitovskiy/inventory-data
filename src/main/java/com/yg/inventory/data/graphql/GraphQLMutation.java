package com.yg.inventory.data.graphql;

import java.util.UUID;
import java.util.function.Function;

import com.yg.inventory.data.db.DataAccess;
import com.yg.inventory.model.db.Column;
import com.yg.inventory.model.db.Schema;
import com.yg.inventory.model.db.Table;
import com.yg.util.DB;
import com.yg.util.DB.Inject;

import graphql.Scalars;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingFieldSelectionSet;
import graphql.schema.FieldCoordinates;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLType;
import graphql.schema.SelectedField;
import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.control.Option;

public class GraphQLMutation {

    static interface Argument {
        static final String ID = "ID";
    }

    static final String MUTATION_TYPE = "MutationType";
    static final String ID_COLUMN     = "id";

    static final Map<DB.DataType, GraphQLScalarType> DATA_TYPE_TO_SCALAR = HashMap.ofEntries(
            new Tuple2<>(DB.DataType.BIGINT, Scalars.GraphQLString),
            new Tuple2<>(DB.DataType.BOOLEAN, Scalars.GraphQLBoolean),
            new Tuple2<>(DB.DataType.INTEGER, Scalars.GraphQLInt),
            new Tuple2<>(DB.DataType.UUID, Scalars.GraphQLID),
            new Tuple2<>(DB.DataType.VARCHAR, Scalars.GraphQLString),
            new Tuple2<>(DB.DataType.TEXT_SEARCH_VECTOR, Scalars.GraphQLString));

    final Schema        schema;
    final GraphQLNaming naming;
    final DataAccess    dataAccess;

    GraphQLMutation(Schema schema, GraphQLNaming naming, DataAccess dataAccess) {
        this.schema = schema;
        this.naming = naming;
        this.dataAccess = dataAccess;
    }

    public static GraphQLMutation of(Schema schema, GraphQLNaming naming, DataAccess dataAccess) {
        return new GraphQLMutation(schema, naming, dataAccess);
    }

    public GraphQLObjectType buildMutationType() {
        return GraphQLObjectType.newObject()
            .name(MUTATION_TYPE)
            .fields(schema.tables.values()
                .flatMap(this::buildMutationFields)
                .toJavaList())
            .build();
    }

    public List<GraphQLType> buildAddtionalTypes() {
        return schema.tables.values()
            .flatMap(this::buildTypes)
            .toList();
    }

    public List<Tuple2<FieldCoordinates, DataFetcher<?>>> buildAllFetchers() {
        return schema.tables.values()
            .flatMap(this::buildFetchers)
            .toList();
    }

    public List<Tuple2<FieldCoordinates, DataFetcher<?>>> buildFetchers(Table table) {
        return List.of(
                new Tuple2<>(FieldCoordinates.coordinates(MUTATION_TYPE, naming.getInsertMutationName(table.name)),
                        (DataFetcher<?>) (e -> fetchInsert(table, e))),
                new Tuple2<>(FieldCoordinates.coordinates(MUTATION_TYPE, naming.getUpsertMutationName(table.name)),
                        (DataFetcher<?>) (e -> fetchUpsert(table, e))),
                new Tuple2<>(FieldCoordinates.coordinates(MUTATION_TYPE, naming.getUpdateMutationName(table.name)),
                        (DataFetcher<?>) (e -> fetchUpsert(table, e))),
                new Tuple2<>(FieldCoordinates.coordinates(MUTATION_TYPE, naming.getDeleteMutationName(table.name)),
                        (DataFetcher<?>) (e -> fetchUpsert(table, e))));
    }

    List<GraphQLFieldDefinition> buildMutationFields(Table table) {
        return List.of(
                buildInsertMutations(table.name),
                buildUpsertMutations(table.name),
                buildUpdateMutations(table.name),
                buildDeleteMutations(table.name));
    }

    GraphQLFieldDefinition buildInsertMutations(String tableName) {
        return GraphQLFieldDefinition.newFieldDefinition()
            .name(naming.getInsertMutationName(tableName))
            .type(naming.getExtractTypeRef(tableName))
            .argument(GraphQLArgument.newArgument()
                .name(tableName)
                .type(naming.getInsertTypeRef(tableName)))
            .build();
    }

    GraphQLFieldDefinition buildUpsertMutations(String tableName) {
        return GraphQLFieldDefinition.newFieldDefinition()
            .name(naming.getUpsertMutationName(tableName))
            .type(naming.getExtractTypeRef(tableName))
            .argument(GraphQLArgument.newArgument()
                .name(Argument.ID)
                .type(Scalars.GraphQLID))
            .argument(GraphQLArgument.newArgument()
                .name(tableName)
                .type(naming.getUpdateTypeRef(tableName)))
            .build();
    }

    GraphQLFieldDefinition buildUpdateMutations(String tableName) {
        return GraphQLFieldDefinition.newFieldDefinition()
            .name(naming.getUpdateMutationName(tableName))
            .type(naming.getExtractTypeRef(tableName))
            .argument(GraphQLArgument.newArgument()
                .name(Argument.ID)
                .type(Scalars.GraphQLID))
            .argument(GraphQLArgument.newArgument()
                .name(tableName)
                .type(naming.getUpdateTypeRef(tableName)))
            .build();
    }

    GraphQLFieldDefinition buildDeleteMutations(String tableName) {
        return GraphQLFieldDefinition.newFieldDefinition()
            .name(naming.getDeleteMutationName(tableName))
            .type(naming.getExtractTypeRef(tableName))
            .argument(GraphQLArgument.newArgument()
                .name(Argument.ID)
                .type(Scalars.GraphQLID))
            .build();
    }

    List<GraphQLType> buildTypes(Table table) {
        return List.of(
                buildInsertType(table),
                buildUpdateType(table));
    }

    GraphQLType buildInsertType(Table table) {
        return GraphQLInputObjectType.newInputObject()
            .name(naming.getInsertTypeName(table.name))
            .fields(table.columns.values()
                .filter(this::isInsertableColumn)
                .map(this::buildMutableField)
                .toJavaList())
            .build();
    }

    GraphQLType buildUpdateType(Table table) {
        return GraphQLInputObjectType.newInputObject()
            .name(naming.getUpdateTypeName(table.name))
            .fields(table.columns.values()
                .filter(this::isUpdatableColumn)
                .map(this::buildMutableField)
                .toJavaList())
            .build();
    }

    GraphQLInputObjectField buildMutableField(Column column) {
        return GraphQLInputObjectField.newInputObjectField()
            .name(column.name)
            .type(DATA_TYPE_TO_SCALAR.get(column.type).get())
            .build();
    }

    boolean isInsertableColumn(Column column) {
        return DB.DataType.TEXT_SEARCH_VECTOR != column.type;
    }

    boolean isUpdatableColumn(Column column) {
        return isInsertableColumn(column) && ID_COLUMN.equalsIgnoreCase(column.name);
    }

    java.util.Map<String, Object> fetchInsert(Table table, DataFetchingEnvironment environment) throws Exception {
        java.util.Map<String, Object> entity = environment.getArgument(table.name);
        List<Tuple2<String, Inject>>  insert = HashMap.ofAll(entity).flatMap(t -> getInject(table, t._1, t._2)).toList();

        return dataAccess.insert(table.name, insert, returnFields(table, environment)).get().toJavaMap();
    }

    java.util.Map<String, Object> fetchUpsert(Table table, DataFetchingEnvironment environment) throws Exception {
        UUID                          id     = UUID.fromString(environment.getArgument(Argument.ID));
        java.util.Map<String, Object> entity = environment.getArgument(table.name);
        List<Tuple2<String, Inject>>  upsert = HashMap.ofAll(entity).flatMap(t -> getInject(table, t._1, t._2)).toList();

        return dataAccess.mergeById(table.name, id, upsert, returnFields(table, environment)).get().toJavaMap();
    }

    java.util.Map<String, Object> fetchUpdate(Table table, DataFetchingEnvironment environment) throws Exception {
        UUID                          id     = UUID.fromString(environment.getArgument(Argument.ID));
        java.util.Map<String, Object> entity = environment.getArgument(table.name);
        List<Tuple2<String, Inject>>  upsert = HashMap.ofAll(entity).flatMap(t -> getInject(table, t._1, t._2)).toList();

        return dataAccess.updateById(table.name, id, upsert, returnFields(table, environment)).get().toJavaMap();
    }

    java.util.Map<String, Object> fetchDelete(Table table, DataFetchingEnvironment environment) throws Exception {
        UUID id = UUID.fromString(environment.getArgument(Argument.ID));
        return dataAccess.deleteById(table.name, id, returnFields(table, environment)).get().toJavaMap();
    }

    Option<Tuple2<String, Inject>> getInject(Table table, String columnName, Object value) {
        Option<Column> column = table.columns.get(columnName);
        if (column.isEmpty()) {
            return Option.none();
        }

        Option<Function<Object, Inject>> injector = DB.DATA_TYPE_INJECT.get(column.get().type);
        if (injector.isEmpty()) {
            return Option.none();
        }

        return Option.of(new Tuple2<>(columnName, injector.get().apply(value)));
    }

    List<Tuple2<String, DB.Extract<?>>> returnFields(Table table, DataFetchingEnvironment environment) {
        DataFetchingFieldSelectionSet selectionSet = environment.getSelectionSet();
        return List.ofAll(selectionSet.getImmediateFields())
            .filter(f -> table.columns.containsKey(f.getName()))
            .map(SelectedField::getName)
            .append(ID_COLUMN)
            .distinct()
            .map(c -> new Tuple2<>(c, DB.DATA_TYPE_EXTRACT.get(table.columns.get(c).get().type).get()));
    }

}
