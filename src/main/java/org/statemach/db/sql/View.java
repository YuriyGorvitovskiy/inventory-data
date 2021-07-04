package org.statemach.db.sql;

import org.statemach.db.jdbc.Inject;
import org.statemach.util.NodeLinkTree;

import io.vavr.collection.List;

public class View<T> {

    public final String                           name;
    public final List<Select<T>>                  select;
    public final NodeLinkTree<String, From, Join> joins;
    public final Condition                        where;
    public final List<Select<Boolean>>            order;
    public final boolean                          distinct;
    public final Long                             skip;
    public final Integer                          limit;

    public View(String name,
                NodeLinkTree<String, From, Join> joins,
                Condition where,
                List<Select<Boolean>> order,
                List<Select<T>> select,
                boolean distinct,
                Long skip,
                Integer limit) {
        this.name = name;
        this.joins = joins;
        this.where = where;
        this.order = order;
        this.select = select;
        this.distinct = distinct;
        this.skip = skip;
        this.limit = limit;
    }

    public static NodeLinkTree<String, From, Join> alias(String aliasPrefix, NodeLinkTree<String, String, Join> joins) {
        return joins.mapNodesWithIndex(1, (n, i) -> new From(n, aliasPrefix + i));
    }

    public List<Inject> injects() {
        return injectJoins(joins).append(where.inject);
    }

    List<Inject> injectJoins(NodeLinkTree<String, From, Join> tree) {
        return tree.links
            .values()
            .flatMap(t -> List.of(t._1.condition.inject).appendAll(injectJoins(t._2)))
            .toList();
    }
}
