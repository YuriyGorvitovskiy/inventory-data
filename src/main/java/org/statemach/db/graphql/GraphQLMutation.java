package org.statemach.db.graphql;

import java.util.function.Function;

import org.statemach.db.jdbc.Extract;
import org.statemach.db.jdbc.Inject;
import org.statemach.db.schema.ColumnInfo;
import org.statemach.db.schema.PrimaryKey;
import org.statemach.db.schema.Schema;
import org.statemach.db.schema.TableInfo;
import org.statemach.db.sql.DataAccess;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingFieldSelectionSet;
import graphql.schema.FieldCoordinates;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLType;
import graphql.schema.SelectedField;
import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.control.Option;

public class GraphQLMutation {
    static final String MUTATION_TYPE = "MutationType";

    final Schema         schema;
    final GraphQLNaming  naming;
    final GraphQLMapping mapping;
    final DataAccess     dataAccess;

    GraphQLMutation(Schema schema, GraphQLNaming naming, GraphQLMapping mapping, DataAccess dataAccess) {
        this.schema = schema;
        this.naming = naming;
        this.mapping = mapping;
        this.dataAccess = dataAccess;
    }

    public static GraphQLMutation of(Schema schema, GraphQLNaming naming, DataAccess dataAccess) {
        return new GraphQLMutation(schema, naming, GraphQLMapping.of(schema.vendor), dataAccess);
    }

    public GraphQLObjectType buildMutationType() {
        return GraphQLObjectType.newObject()
            .name(MUTATION_TYPE)
            .fields(schema.tables.values()
                .filter(t -> t.primary.isDefined())
                .flatMap(this::buildMutationFields)
                .toJavaList())
            .build();
    }

    public List<GraphQLType> buildAddtionalTypes() {
        return schema.tables.values()
            .filter(t -> t.primary.isDefined())
            .flatMap(this::buildTypes)
            .toList();
    }

    public List<Tuple2<FieldCoordinates, DataFetcher<?>>> buildAllFetchers() {
        return schema.tables.values()
            .filter(t -> t.primary.isDefined())
            .flatMap(this::buildFetchers)
            .toList();
    }

    public List<Tuple2<FieldCoordinates, DataFetcher<?>>> buildFetchers(TableInfo table) {
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

    List<GraphQLFieldDefinition> buildMutationFields(TableInfo table) {
        return List.of(
                buildInsertMutation(table),
                buildUpsertMutation(table),
                buildUpdateMutation(table),
                buildDeleteMutation(table));
    }

    GraphQLFieldDefinition buildInsertMutation(TableInfo table) {
        return GraphQLFieldDefinition.newFieldDefinition()
            .name(naming.getInsertMutationName(table.name))
            .type(naming.getMutateTypeRef(table.name))
            .argument(GraphQLArgument.newArgument()
                .name(table.name)
                .type(naming.getInsertTypeRef(table.name)))
            .build();
    }

    GraphQLFieldDefinition buildUpsertMutation(TableInfo table) {
        return GraphQLFieldDefinition.newFieldDefinition()
            .name(naming.getUpsertMutationName(table.name))
            .type(naming.getMutateTypeRef(table.name))
            .arguments(buildPrimaryKeyArguments(table))
            .argument(GraphQLArgument.newArgument()
                .name(table.name)
                .type(naming.getUpdateTypeRef(table.name)))
            .build();
    }

    GraphQLFieldDefinition buildUpdateMutation(TableInfo table) {
        return GraphQLFieldDefinition.newFieldDefinition()
            .name(naming.getUpdateMutationName(table.name))
            .type(naming.getMutateTypeRef(table.name))
            .arguments(buildPrimaryKeyArguments(table))
            .argument(GraphQLArgument.newArgument()
                .name(table.name)
                .type(naming.getUpdateTypeRef(table.name)))
            .build();
    }

    GraphQLFieldDefinition buildDeleteMutation(TableInfo table) {

        return GraphQLFieldDefinition.newFieldDefinition()
            .name(naming.getDeleteMutationName(table.name))
            .type(naming.getMutateTypeRef(table.name))
            .arguments(buildPrimaryKeyArguments(table))
            .build();
    }

    java.util.List<GraphQLArgument> buildPrimaryKeyArguments(TableInfo table) {
        return table.primary.get().columns
            .map(n -> table.columns.get(n).get())
            .map(c -> GraphQLArgument.newArgument()
                .name(c.name)
                .type(mapping.scalar(table, c))
                .build())
            .toJavaList();
    }

    List<GraphQLType> buildTypes(TableInfo table) {
        return List.of(
                buildMutateType(table),
                buildInsertType(table),
                buildUpdateType(table));
    }

    GraphQLType buildMutateType(TableInfo table) {
        return GraphQLObjectType.newObject()
            .name(naming.getMutateTypeName(table.name))
            .fields(buildScalarFields(table))
            .build();
    }

    java.util.List<GraphQLFieldDefinition> buildScalarFields(TableInfo table) {
        return table.columns.values()
            .filter(c -> mapping.isExtractable(c.type))
            .map(c -> buildScalarField(table, c))
            .toJavaList();
    }

    GraphQLFieldDefinition buildScalarField(TableInfo table, ColumnInfo column) {
        return GraphQLFieldDefinition.newFieldDefinition()
            .name(column.name)
            .type(mapping.scalar(table, column))
            .build();
    }

    GraphQLType buildInsertType(TableInfo table) {
        return GraphQLInputObjectType.newInputObject()
            .name(naming.getInsertTypeName(table.name))
            .fields(table.columns.values()
                .filter(this::isInsertableColumn)
                .map(c -> buildMutableField(table, c))
                .toJavaList())
            .build();
    }

    GraphQLType buildUpdateType(TableInfo table) {
        return GraphQLInputObjectType.newInputObject()
            .name(naming.getUpdateTypeName(table.name))
            .fields(table.columns.values()
                .filter(c -> isUpdatableColumn(table.primary.get(), c))
                .map(c -> buildMutableField(table, c))
                .toJavaList())
            .build();
    }

    GraphQLInputObjectField buildMutableField(TableInfo table, ColumnInfo column) {
        return GraphQLInputObjectField.newInputObjectField()
            .name(column.name)
            .type(mapping.scalar(table, column))
            .build();
    }

    boolean isInsertableColumn(ColumnInfo column) {
        return mapping.isMutable(column.type);
    }

    boolean isUpdatableColumn(PrimaryKey pk, ColumnInfo column) {
        return isInsertableColumn(column) && !pk.columns.contains(column.name);
    }

    java.util.Map<String, Object> fetchInsert(TableInfo table, DataFetchingEnvironment environment) throws Exception {
        Map<String, Inject> entity = getEntity(table, environment);

        return dataAccess.insert(table.name, entity, returnFields(table, environment)).toJavaMap();
    }

    java.util.Map<String, Object> fetchUpsert(TableInfo table, DataFetchingEnvironment environment) throws Exception {
        Map<String, Inject> pk     = primaryKey(table, environment);
        Map<String, Inject> entity = getEntity(table, environment);

        return dataAccess.merge(table.name, pk, entity, returnFields(table, environment)).toJavaMap();
    }

    java.util.Map<String, Object> fetchUpdate(TableInfo table, DataFetchingEnvironment environment) throws Exception {
        Map<String, Inject> pk     = primaryKey(table, environment);
        Map<String, Inject> entity = getEntity(table, environment);

        return dataAccess.update(table.name, pk, entity, returnFields(table, environment)).get().toJavaMap();
    }

    java.util.Map<String, Object> fetchDelete(TableInfo table, DataFetchingEnvironment environment) throws Exception {
        Map<String, Inject> pk = primaryKey(table, environment);
        return dataAccess.delete(table.name, pk, returnFields(table, environment)).get().toJavaMap();
    }

    Map<String, Inject> primaryKey(TableInfo table, DataFetchingEnvironment environment) {
        return table.primary.isEmpty()
                ? HashMap.empty()
                : table.primary.get().columns.map(c -> columnInject(table, c, environment)).toMap(t -> t);
    }

    Tuple2<String, Inject> columnInject(TableInfo table, String columnName, DataFetchingEnvironment environment) {
        return getInject(table, columnName, environment.getArgument(columnName)).get();
    }

    Map<String, Inject> getEntity(TableInfo table, DataFetchingEnvironment environment) {
        java.util.Map<String, Object> entity = environment.getArgument(table.name);
        return HashMap.ofAll(entity)
            .flatMap(t -> getInject(table, t._1, t._2))
            .toLinkedMap(t -> t);
    }

    Option<Tuple2<String, Inject>> getInject(TableInfo table, String columnName, Object value) {
        ColumnInfo               column   = table.columns.get(columnName).get();
        Function<Object, Inject> injector = mapping.injectors.get(column.type).get();

        return Option.of(new Tuple2<>(columnName, injector.apply(value)));
    }

    Map<String, Extract<?>> returnFields(TableInfo table, DataFetchingEnvironment environment) {
        DataFetchingFieldSelectionSet selectionSet = environment.getSelectionSet();
        return List.ofAll(selectionSet.getImmediateFields())
            .filter(f -> table.columns.containsKey(f.getName()))
            .map(SelectedField::getName)
            .map(c -> new Tuple2<>(c, mapping.extract(table.columns.get(c).get().type)))
            .toLinkedMap(t -> t);
    }

}
