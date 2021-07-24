package org.statemach.db.graphql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "TEST_DATABASE", matches = "POSTGRES")
public class GraphQLHandler_Schema_PostgresTest extends GraphQLHandler_Common_PostgresTest {

    @Test
    void listTypes() {
        runTest("schema.types.gql", "schema.types.expect.json");
    }

    @Test
    void __Field_Types() {
        runTest("schema.__Field.gql", "schema.__Field.expect.json");
    }

    @Test
    void __Type_Types() {
        runTest("schema.__Type.gql", "schema.__Type.expect.json");
    }

    @Test
    void MutationType_Types() {
        runTest("schema.MutationType.gql", "schema.MutationType.expect.json");
    }

    @Test
    void QueryType_Types() {
        runTest("schema.QueryType.gql", "schema.QueryType.expect.json");
    }

    @Test
    void SortingOrder_Types() {
        runTest("schema.SortingOrder.gql", "schema.SortingOrder.expect.json");
    }

    @Test
    void __type_Types() {
        runTest("schema.__Type.gql", "schema.__Type.expect.json");
    }

    @Test
    void first_type() {
        runTest("schema.first.gql", "schema.first.expect.json");
    }

    @Test
    void second_type() {
        runTest("schema.second.gql", "schema.second.expect.json");
    }

    @Test
    void third_type() {
        runTest("schema.third.gql", "schema.third.expect.json");
    }

    @Test
    void version_type() {
        runTest("schema.version.gql", "schema.version.expect.json");
    }

    @Test
    void first_filter_type() {
        runTest("schema.first_filter.gql", "schema.first_filter.expect.json");
    }

    @Test
    void second_filter_type() {
        runTest("schema.second_filter.gql", "schema.second_filter.expect.json");
    }

    @Test
    void third_filter_type() {
        runTest("schema.third_filter.gql", "schema.third_filter.expect.json");
    }

    @Test
    void version_filter_type() {
        runTest("schema.version_filter.gql", "schema.version_filter.expect.json");
    }

    @Test
    void first_order_type() {
        runTest("schema.first_order.gql", "schema.first_order.expect.json");
    }

    @Test
    void second_order_type() {
        runTest("schema.second_order.gql", "schema.second_order.expect.json");
    }

    @Test
    void third_order_type() {
        runTest("schema.third_order.gql", "schema.third_order.expect.json");
    }

    @Test
    void version_order_type() {
        runTest("schema.version_order.gql", "schema.version_order.expect.json");
    }

    @Test
    void first_insert_type() {
        runTest("schema.first_insert.gql", "schema.first_insert.expect.json");
    }

    @Test
    void second_insert_type() {
        runTest("schema.second_insert.gql", "schema.second_insert.expect.json");
    }

    @Test
    void third_insert_type() {
        runTest("schema.third_insert.gql", "schema.third_insert.expect.json");
    }

    @Test
    void first_update_type() {
        runTest("schema.first_update.gql", "schema.first_update.expect.json");
    }

    @Test
    void second_update_type() {
        runTest("schema.second_update.gql", "schema.second_update.expect.json");
    }

    @Test
    void third_updatetype() {
        runTest("schema.third_update.gql", "schema.third_update.expect.json");
    }

}
