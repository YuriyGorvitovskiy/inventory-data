package com.yg.inventory.data.db;

public class Join {

    public static enum Kind {
        CROSS,
        INNER,
        LEFT,
        RIGHT,
        FULL,
    }

    public final Kind      kind;
    public final Condition condition;
    public final boolean   oneToOne;

    public Join(Kind kind, Condition condition, boolean oneToOne) {
        this.kind = kind;
        this.condition = condition;
        this.oneToOne = oneToOne;
    }

}
