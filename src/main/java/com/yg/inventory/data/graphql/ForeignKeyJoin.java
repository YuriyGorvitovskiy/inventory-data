package com.yg.inventory.data.graphql;

import com.yg.inventory.data.db.Join;
import com.yg.inventory.model.db.ForeignKey;

public class ForeignKeyJoin {
    public final Join.Kind  joinKind;
    public final ForeignKey foreignKey;
    public final boolean    outgoing;

    public ForeignKeyJoin(Join.Kind joinKind, ForeignKey foreignKey, boolean outgoing) {
        this.joinKind = joinKind;
        this.foreignKey = foreignKey;
        this.outgoing = outgoing;
    }

}
