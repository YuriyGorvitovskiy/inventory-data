package com.yg.util;

import io.vavr.Tuple2;
import io.vavr.Tuple3;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.control.Option;

public class Tree<K, N, L> {

    public final N                                node;
    public final Map<K, Tuple2<L, Tree<K, N, L>>> links;

    public static <K, N, L> Tree<K, N, L> of(N node) {
        return new Tree<>(node, HashMap.empty());
    }

    public static <K, N, L> Tree<K, N, L> of(N node, Map<K, Tuple2<L, Tree<K, N, L>>> links) {
        return new Tree<>(node, links);
    }

    public static <K, N, L> Tree<K, N, L> of(N node, List<Tuple3<K, L, Tree<K, N, L>>> links) {
        return new Tree<>(node, links.toLinkedMap(t -> new Tuple2<>(t._1, new Tuple2<>(t._2, t._3))));
    }

    Tree(N node, Map<K, Tuple2<L, Tree<K, N, L>>> links) {
        this.node = node;
        this.links = links;
    }

    public Option<Tree<K, N, L>> getTree(K key) {
        return links.get(key).map(t -> t._2);
    }

    public Option<Tree<K, N, L>> getTree(List<K> path) {
        if (path.isEmpty()) {
            return Option.of(this);
        }
        Option<Tuple2<L, Tree<K, N, L>>> link = links.get(path.get());
        return link.flatMap(l -> l._2.getTree(path.drop(1)));
    }

    public N getNode() {
        return node;
    }

    public Option<N> getNode(K key) {
        return links.get(key).map(t -> t._2.node);
    }

    public Option<N> getNode(List<K> path) {
        if (path.isEmpty()) {
            return Option.of(node);
        }
        Option<Tuple2<L, Tree<K, N, L>>> link = links.get(path.get());
        return link.flatMap(l -> l._2.getNode(path.drop(1)));
    }

    public Option<L> getLink(List<K> path) {
        if (path.isEmpty()) {
            return Option.none();
        }
        Option<Tuple2<L, Tree<K, N, L>>> link = links.get(path.get());
        return link.flatMap(l -> 1 == path.size()
                ? Option.of(l._1)
                : l._2.getLink(path.drop(1)));
    }

    public Tree<K, N, L> setNode(N node) {
        return new Tree<>(node, links);
    }

    public Tree<K, N, L> setNode(List<K> path, N node) {
        return path.isEmpty()
                ? setNode(node)
                : setChild(path.get(), getTree(path.get()).get().setNode(path.drop(1), node));
    }

    public Tree<K, N, L> setLink(K key, L link) {
        Tree<K, N, L> child = links.get(key).get()._2;
        return new Tree<>(node, links.put(key, new Tuple2<>(link, child)));
    }

    public Tree<K, N, L> setLink(List<K> path, L link) {
        K key = path.last();
        path = path.dropRight(1);

        return path.isEmpty()
                ? setLink(key, link)
                : setChild(key, getTree(key).get().setLink(path, link));
    }

    public Tree<K, N, L> setChild(K key, Tree<K, N, L> child) {
        L link = links.get(key).get()._1;
        return new Tree<>(node, links.put(key, new Tuple2<>(link, child)));
    }

    public Tree<K, N, L> setChild(List<K> path, Tree<K, N, L> child) {
        K key = path.last();
        path = path.dropRight(1);

        return path.isEmpty()
                ? setChild(key, child)
                : setChild(key, getTree(key).get().setChild(path, child));
    }

    public Tree<K, N, L> put(K key, L link, Tree<K, N, L> child) {
        return new Tree<>(node, links.put(key, new Tuple2<>(link, child)));
    }

    public Tree<K, N, L> put(List<K> path, L link, Tree<K, N, L> child) {
        K key = path.get();
        path = path.drop(1);

        return path.isEmpty()
                ? put(key, link, child)
                : setChild(key, getTree(key).get().put(path, link, child));
    }
}
