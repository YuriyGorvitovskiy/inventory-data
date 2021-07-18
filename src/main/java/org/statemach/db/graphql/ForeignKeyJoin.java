package org.statemach.db.graphql;

import java.util.Objects;

import org.statemach.db.schema.ForeignKey;
import org.statemach.db.sql.Join;
import org.statemach.util.Java;

public class ForeignKeyJoin {
    public final Join.Kind  joinKind;
    public final ForeignKey foreignKey;
    public final boolean    outgoing;

    public ForeignKeyJoin(Join.Kind joinKind, ForeignKey foreignKey, boolean outgoing) {
        this.joinKind = joinKind;
        this.foreignKey = foreignKey;
        this.outgoing = outgoing;
    }

    @Override
    public int hashCode() {
        return Objects.hash(joinKind, foreignKey, outgoing);
    }

    @Override
    public boolean equals(Object other) {
        return Java.equalsByFields(this,
                other,
                t -> t.joinKind,
                t -> t.foreignKey,
                t -> t.outgoing);
    }

    @Override
    public String toString() {
        return "ForeignKeyJoin@{kind: " + joinKind +
                ", fk: " + foreignKey +
                ", dir: " + (outgoing ? "forward" : "reverse") +
                "}";
    }
}
