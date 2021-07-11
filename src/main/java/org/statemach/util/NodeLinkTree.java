package org.statemach.util;

import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.Function;

import io.vavr.Tuple2;
import io.vavr.Tuple3;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Stream;
import io.vavr.control.Option;

public class NodeLinkTree<K, N, L> {

    public final N                                        node;
    public final Map<K, Tuple2<L, NodeLinkTree<K, N, L>>> links;

    public static <K, N, L> NodeLinkTree<K, N, L> of(N node) {
        return new NodeLinkTree<>(node, HashMap.empty());
    }

    public static <K, N, L> NodeLinkTree<K, N, L> of(N node, Map<K, Tuple2<L, NodeLinkTree<K, N, L>>> links) {
        return new NodeLinkTree<>(node, links);
    }

    public static <K, N, L> NodeLinkTree<K, N, L> of(N node, List<Tuple3<K, L, NodeLinkTree<K, N, L>>> links) {
        return new NodeLinkTree<>(node, links.toMap(t -> new Tuple2<>(t._1, new Tuple2<>(t._2, t._3))));
    }

    public static <K, N, L> NodeLinkTree<K, N, L> of(N node, List<K> path, BiFunction<N, K, Tuple2<L, N>> createLinkWithNode) {
        if (path.isEmpty()) {
            return of(node);
        }
        K                     key     = path.get();
        Tuple2<L, N>          created = createLinkWithNode.apply(node, key);
        NodeLinkTree<K, N, L> child   = of(created._2, path.drop(1), createLinkWithNode);
        return of(node, HashMap.of(key, new Tuple2<>(created._1, child)));
    }

    NodeLinkTree(N node, Map<K, Tuple2<L, NodeLinkTree<K, N, L>>> links) {
        this.node = node;
        this.links = links;
    }

    public Option<NodeLinkTree<K, N, L>> getTree(K key) {
        return links.get(key).map(t -> t._2);
    }

    public Option<NodeLinkTree<K, N, L>> getTree(List<K> path) {
        if (path.isEmpty()) {
            return Option.of(this);
        }
        Option<Tuple2<L, NodeLinkTree<K, N, L>>> link = links.get(path.get());
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
        Option<Tuple2<L, NodeLinkTree<K, N, L>>> link = links.get(path.get());
        return link.flatMap(l -> l._2.getNode(path.drop(1)));
    }

    public Option<L> getLink(List<K> path) {
        if (path.isEmpty()) {
            return Option.none();
        }
        Option<Tuple2<L, NodeLinkTree<K, N, L>>> link = links.get(path.get());
        return link.flatMap(l -> 1 == path.size()
                ? Option.of(l._1)
                : l._2.getLink(path.drop(1)));
    }

    public NodeLinkTree<K, N, L> setNode(N node) {
        return new NodeLinkTree<>(node, links);
    }

    public NodeLinkTree<K, N, L> setNode(List<K> path, N node) {
        return path.isEmpty()
                ? setNode(node)
                : setChild(path.get(), getTree(path.get()).get().setNode(path.drop(1), node));
    }

    public NodeLinkTree<K, N, L> setLink(K key, L link) {
        NodeLinkTree<K, N, L> child = links.get(key).get()._2;
        return new NodeLinkTree<>(node, links.put(key, new Tuple2<>(link, child)));
    }

    public NodeLinkTree<K, N, L> setLink(List<K> path, L link) {
        K key = path.last();
        path = path.dropRight(1);

        return path.isEmpty()
                ? setLink(key, link)
                : setChild(key, getTree(key).get().setLink(path, link));
    }

    public NodeLinkTree<K, N, L> setChild(K key, NodeLinkTree<K, N, L> child) {
        L link = links.get(key).get()._1;
        return new NodeLinkTree<>(node, links.put(key, new Tuple2<>(link, child)));
    }

    public NodeLinkTree<K, N, L> setChild(List<K> path, NodeLinkTree<K, N, L> child) {
        K key = path.last();
        path = path.dropRight(1);

        return path.isEmpty()
                ? setChild(key, child)
                : setChild(key, getTree(key).get().setChild(path, child));
    }

    public NodeLinkTree<K, N, L> put(K key, L link, NodeLinkTree<K, N, L> child) {
        return new NodeLinkTree<>(node, links.put(key, new Tuple2<>(link, child)));
    }

    public NodeLinkTree<K, N, L> put(List<K> path, L link, NodeLinkTree<K, N, L> child) {
        K key = path.get();
        path = path.drop(1);

        return path.isEmpty()
                ? put(key, link, child)
                : setChild(key, getTree(key).get().put(path, link, child));
    }

    public NodeLinkTree<K, N, L> putIfMissed(K key, BiFunction<N, K, Tuple2<L, N>> createMissingLinkWithNode) {
        Option<Tuple2<L, NodeLinkTree<K, N, L>>> childLink = links.get(key);
        if (childLink.isEmpty()) {
            Tuple2<L, N> created = createMissingLinkWithNode.apply(node, key);
            return this.put(key, created._1, of(created._2));
        }

        return this;
    }

    public NodeLinkTree<K, N, L> putIfMissed(List<K> path, BiFunction<N, K, Tuple2<L, N>> createMissingLinkWithNode) {
        if (path.isEmpty()) {
            return this;
        }

        K key = path.get();
        path = path.drop(1);

        Option<Tuple2<L, NodeLinkTree<K, N, L>>> childLink = links.get(key);
        if (childLink.isEmpty()) {
            Tuple2<L, N>          created   = createMissingLinkWithNode.apply(node, key);
            NodeLinkTree<K, N, L> childTree = of(created._2, path, createMissingLinkWithNode);
            return this.put(key, created._1, childTree);
        }

        NodeLinkTree<K, N, L> childTree = childLink.get()._2.putIfMissed(path, createMissingLinkWithNode);
        return childLink.get()._2 == childTree
                ? this
                : this.put(key, childLink.get()._1, childTree);
    }

    public NodeLinkTree<K, N, L> merge(NodeLinkTree<K, N, L> that, BiFunction<N, N, N> nodeCollision,
                                       BiFunction<L, L, L> linkCollision) {
        N   newNode  = nodeCollision.apply(this.node, that.node);
        var newLinks = this.links.merge(that.links,
                (l, r) -> null == l ? r
                        : null == r ? l
                                : new Tuple2<>(
                                        linkCollision.apply(l._1, r._1),
                                        l._2.merge(r._2, nodeCollision, linkCollision)));

        return of(newNode, newLinks);
    }

    public NodeLinkTree<K, N, L> merge(NodeLinkTree<K, N, L> that) {
        return this.merge(that, (l, r) -> l, (l, r) -> l);
    }

    public NodeLinkTree<K, N, L> mergeLinks(NodeLinkTree<K, N, L> that) {
        return this.merge(that, (l, r) -> l, (l, r) -> r);
    }

    public NodeLinkTree<K, N, L> mergeNode(NodeLinkTree<K, N, L> that) {
        return this.merge(that, (l, r) -> r, (l, r) -> l);
    }

    public NodeLinkTree<K, N, L> mergeAll(NodeLinkTree<K, N, L> that) {
        return this.merge(that, (l, r) -> r, (l, r) -> r);
    }

    public <R> NodeLinkTree<K, R, L> mapNodes(Function<N, R> mapping) {
        return new NodeLinkTree<>(
                mapping.apply(node),
                links.mapValues(v -> new Tuple2<>(v._1, v._2.mapNodes(mapping))));
    }

    public <R> NodeLinkTree<K, R, L> mapNodesWithIndex(int from, BiFunction<N, Integer, R> mapping) {
        return mapNodesWith(Stream.from(from), mapping);
    }

    public <R, I> NodeLinkTree<K, R, L> mapNodesWith(Iterable<I> that, BiFunction<N, I, R> mapping) {
        return mapNodesWith(that.iterator(), mapping);
    }

    public <R, I> NodeLinkTree<K, R, L> mapNodesWith(Iterator<I> it, BiFunction<N, I, R> mapping) {
        return new NodeLinkTree<>(
                mapping.apply(node, it.next()),
                links.mapValues(v -> new Tuple2<>(v._1, v._2.mapNodesWith(it, mapping))));
    }

    public <R> NodeLinkTree<K, N, R> mapLinks(Function<L, R> mapping) {
        return new NodeLinkTree<>(
                node,
                links.mapValues(v -> new Tuple2<>(mapping.apply(v._1), v._2.mapLinks(mapping))));
    }

    public <R> NodeLinkTree<K, N, R> mapLinksWithNodes(Function<Tuple3<N, L, N>, R> mapping) {
        return new NodeLinkTree<>(
                node,
                links.mapValues(v -> new Tuple2<>(mapping.apply(new Tuple3<>(node, v._1, v._2.node)),
                        v._2.mapLinksWithNodes(mapping))));
    }

    public <R> NodeLinkTree<K, N, R> mapLinksWithIndex(int from, BiFunction<L, Integer, R> mapping) {
        return mapLinksWith(Stream.from(from), mapping);
    }

    public <R, I> NodeLinkTree<K, N, R> mapLinksWith(Iterable<I> that, BiFunction<L, I, R> mapping) {
        return mapLinksWith(that.iterator(), mapping);
    }

    public <R, I> NodeLinkTree<K, N, R> mapLinksWith(Iterator<I> it, BiFunction<L, I, R> mapping) {
        return new NodeLinkTree<>(
                node,
                links.mapValues(v -> new Tuple2<>(mapping.apply(v._1, it.next()), v._2.mapLinksWith(it, mapping))));
    }
}
