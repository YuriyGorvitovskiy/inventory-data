package org.statemach.db.schema;

import java.util.Objects;

import org.statemach.util.Java;

import io.vavr.collection.List;

public class ForeignKey {

    public static class Match {
        public final String from;
        public final String to;

        public Match(String from, String to) {
            this.from = from;
            this.to = to;
        }

        @Override
        public int hashCode() {
            return Objects.hash(from, to);
        }

        @Override
        public boolean equals(Object other) {
            return Java.equalsByFields(this, other, t -> t.from, t -> t.to);
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

    @Override
    public int hashCode() {
        return Objects.hash(name, fromTable, toTable, matchingColumns);
    }

    @Override
    public boolean equals(Object other) {
        return Java.equalsByFields(this, other, t -> t.name, t -> t.fromTable, t -> t.toTable, t -> t.matchingColumns);
    }

    @Override
    public String toString() {
        return "ForeignKey@{name: " + name +
                ",from: " + fromTable +
                ", to: " + toTable +
                "}";
    }
}