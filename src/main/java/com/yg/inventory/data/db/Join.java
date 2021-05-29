package com.yg.inventory.data.db;

public class Join {
    static enum Kind {
        CROSS,
        INNER,
        LEFT,
        RIGHT,
        FULL,
    }

    final Kind   kind;
    final String leftColumn;
    final String rightColumn;

    public Join(Kind kind, String leftColumn, String rightColumn) {
        this.kind = kind;
        this.leftColumn = leftColumn;
        this.rightColumn = rightColumn;
    }

}
