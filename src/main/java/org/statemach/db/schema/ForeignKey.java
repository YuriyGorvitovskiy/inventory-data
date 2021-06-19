package org.statemach.db.schema;

import io.vavr.collection.List;

public class ForeignKey {

    public static class Match {
        public final String from;
        public final String to;

        public Match(String from, String to) {
            this.from = from;
            this.to = to;
        }
    }

    public final String      name;
    public final String      fromTable;
    public final String      toTable;
    public final List<Match> matchingColumns;

    public ForeignKey(String name, String fromTable, String toTable, List<Match> matchingColumns) {
        this.name = name;
        this.fromTable = fromTable;
        this.toTable = toTable;
        this.matchingColumns = matchingColumns;
    }
}