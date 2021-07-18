package org.statemach.db.graphql;

import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingFieldSelectionSet;
import graphql.schema.SelectedField;
import io.vavr.control.Either;

public class GraphQLField {
    final Either<DataFetchingEnvironment, SelectedField> either;

    GraphQLField(Either<DataFetchingEnvironment, SelectedField> either) {
        this.either = either;
    }

    public static GraphQLField of(DataFetchingEnvironment environment) {
        return new GraphQLField(Either.left(environment));
    }

    public static GraphQLField of(SelectedField selected) {
        return new GraphQLField(Either.right(selected));
    }

    public DataFetchingFieldSelectionSet getSelectionSet() {
        return either.isLeft()
                ? either.getLeft().getSelectionSet()
                : either.get().getSelectionSet();
    }

    public Object getArgument(String name) {
        return either.isLeft()
                ? either.getLeft().getArgument(name)
                : either.get().getArguments().get(name);
    }
}
