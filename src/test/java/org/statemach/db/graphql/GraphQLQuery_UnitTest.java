package org.statemach.db.graphql;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.statemach.db.schema.ColumnInfo;
import org.statemach.db.schema.CompositeType;
import org.statemach.db.schema.Schema;
import org.statemach.db.sql.DataAccess;
import org.statemach.db.sql.SQLBuilder;
import org.statemach.db.sql.SchemaAccess;

import io.vavr.collection.List;

public class GraphQLQuery_UnitTest {
    final String TYPE_NAME_1 = "Type1";
    final String TYPE_NAME_2 = "Type2";
    final String TYPE_NAME_3 = "Type3";

    final CompositeType TYPE_1 = new CompositeType(TYPE_NAME_1, List.empty());
    final CompositeType TYPE_2 = new CompositeType(TYPE_NAME_2, List.empty());
    final CompositeType TYPE_3 = new CompositeType(TYPE_NAME_3, List.empty());
    final CompositeType TYPE_4 = new CompositeType(TYPE_NAME_2, List.of(mock(ColumnInfo.class)));

    final Schema              schema       = mock(Schema.class);
    final SchemaAccess        schemaAccess = mock(SchemaAccess.class);
    final DataAccess          dataAccess   = mock(DataAccess.class);
    final SQLBuilder          sqlBuilder   = mock(SQLBuilder.class);
    final GraphQLNaming       naming       = mock(GraphQLNaming.class);
    final GraphQLQueryExtract extract      = mock(GraphQLQueryExtract.class);
    final GraphQLQueryFilter  filter       = mock(GraphQLQueryFilter.class);
    final GraphQLQueryOrder   order        = mock(GraphQLQueryOrder.class);
    final GraphQLQuery        subject      = spy(new GraphQLQuery(
            schema,
            dataAccess,
            sqlBuilder,
            naming,
            extract,
            filter,
            order));

    @Test
    void instrumentSchema() {
        // Verify
        doReturn(List.of(TYPE_1, TYPE_2)).when(schemaAccess).getAllCompositeTypes();
        doReturn(List.of(TYPE_1, TYPE_4)).when(subject).requiredCompositeTypes();

        // Execute
        subject.instrumentSchema(schemaAccess);

        // Verify
        InOrder order = inOrder(schemaAccess);
        order.verify(schemaAccess).getAllCompositeTypes();
        order.verify(schemaAccess).dropCompositeType(TYPE_NAME_2);
        order.verify(schemaAccess).createCompositeType(TYPE_4);
        order.verifyNoMoreInteractions();
    }
}
