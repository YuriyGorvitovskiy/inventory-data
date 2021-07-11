package org.statemach.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.BiFunction;

import org.junit.jupiter.api.Test;

import io.vavr.Tuple2;
import io.vavr.Tuple3;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.control.Option;

public class NodeLinkTree_UnitTest {

    @Test
    void of_node() {
        // Execute
        var result = NodeLinkTree.<String, Integer, Boolean>of(1);

        // Verify
        assertEquals(new NodeLinkTree<String, Integer, Boolean>(1, HashMap.empty()), result);
    }

    @Test
    void of_node_links_map() {
        // Setup
        var node2 = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node3 = NodeLinkTree.<String, Integer, Boolean>of(3);
        var links = HashMap.of(
                "a",
                new Tuple2<>(true, node2),
                "b",
                new Tuple2<>(false, node3));

        // Execute
        var result = NodeLinkTree.of(1, links);

        // Verify
        assertEquals(new NodeLinkTree<>(1, links), result);
    }

    @Test
    void of_node_links_list() {
        // Setup
        var node2 = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node3 = NodeLinkTree.<String, Integer, Boolean>of(3);
        var list  = List.of(
                new Tuple3<>("a", true, node2),
                new Tuple3<>("b", false, node3));
        var map   = HashMap.of(
                "a",
                new Tuple2<>(true, node2),
                "b",
                new Tuple2<>(false, node3));

        // Execute
        var result = NodeLinkTree.of(1, list);

        // Verify
        assertEquals(new NodeLinkTree<>(1, map), result);
    }

    @Test
    void of_node_links_function() {
        // Setup
        var                                                   path  = List.of("a", "b");
        BiFunction<Integer, String, Tuple2<Boolean, Integer>> links = (n, k) -> "a".equals(k)
                ? new Tuple2<>(true, 2)
                : new Tuple2<>(false, 3);

        var node3 = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2 = NodeLinkTree.of(2, List.of(new Tuple3<>("b", false, node3)));
        var node1 = NodeLinkTree.of(1, List.of(new Tuple3<>("a", true, node2)));

        var list = List.of(
                new Tuple3<>("a", true, node2),
                new Tuple3<>("b", false, node3));
        var map  = HashMap.of(
                "a",
                new Tuple2<>(true, node2),
                "b",
                new Tuple2<>(false, node3));

        // Execute
        var result = NodeLinkTree.of(1, path, links);

        // Verify
        assertEquals(node1, result);
    }

    @Test
    void getTree_key() {
        // Setup
        var node2 = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node3 = NodeLinkTree.<String, Integer, Boolean>of(3);
        var links = HashMap.of(
                "a",
                new Tuple2<>(true, node2),
                "b",
                new Tuple2<>(false, node3));

        var subject = NodeLinkTree.of(1, links);

        // Execute
        var result = subject.getTree("b");

        // Verify
        assertSame(node3, result.get());
    }

    @Test
    void getTree_key_missed() {
        // Setup
        var node2 = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node3 = NodeLinkTree.<String, Integer, Boolean>of(3);
        var links = HashMap.of(
                "a",
                new Tuple2<>(true, node2),
                "b",
                new Tuple2<>(false, node3));

        var subject = NodeLinkTree.of(1, links);

        // Execute
        var result = subject.getTree("c");

        // Verify
        assertSame(Option.none(), result);
    }

    @Test
    void getTree_path() {
        // Setup
        var path = List.of("a", "b");

        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", false, node3)));
        var subject = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node1)));

        // Execute
        var result = subject.getTree(path);

        // Verify
        assertEquals(node2, result.get());
    }

    @Test
    void getTree_path_missed() {
        // Setup
        var path = List.of("a", "d");

        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", false, node3)));
        var subject = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node1)));

        // Execute
        var result = subject.getTree(path);

        // Verify
        assertEquals(Option.none(), result);
    }

    @Test
    void getNode() {
        // Setup
        var node2 = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node3 = NodeLinkTree.<String, Integer, Boolean>of(3);
        var links = HashMap.of(
                "a",
                new Tuple2<>(true, node2),
                "b",
                new Tuple2<>(false, node3));

        var subject = NodeLinkTree.of(1, links);

        // Execute
        var result = subject.getNode();

        // Verify
        assertSame(1, result);
    }

    @Test
    void getNode_key() {
        // Setup
        var node2 = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node3 = NodeLinkTree.<String, Integer, Boolean>of(3);
        var links = HashMap.of(
                "a",
                new Tuple2<>(true, node2),
                "b",
                new Tuple2<>(false, node3));

        var subject = NodeLinkTree.of(1, links);

        // Execute
        var result = subject.getNode("b");

        // Verify
        assertSame(3, result.get());
    }

    @Test
    void getNode_key_missed() {
        // Setup
        var node2 = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node3 = NodeLinkTree.<String, Integer, Boolean>of(3);
        var links = HashMap.of(
                "a",
                new Tuple2<>(true, node2),
                "b",
                new Tuple2<>(false, node3));

        var subject = NodeLinkTree.of(1, links);

        // Execute
        var result = subject.getNode("c");

        // Verify
        assertSame(Option.none(), result);
    }

    @Test
    void getNode_path() {
        // Setup
        var path = List.of("a", "b");

        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", false, node3)));
        var subject = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node1)));

        // Execute
        var result = subject.getNode(path);

        // Verify
        assertEquals(2, result.get());
    }

    @Test
    void getNode_path_missed() {
        // Setup
        var path = List.of("a", "d");

        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", false, node3)));
        var subject = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node1)));

        // Execute
        var result = subject.getNode(path);

        // Verify
        assertEquals(Option.none(), result);
    }

    @Test
    void getLink_path() {
        // Setup
        var path = List.of("a", "b");

        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", false, node3)));
        var subject = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node1)));

        // Execute
        var result = subject.getLink(path);

        // Verify
        assertEquals(false, result.get());
    }

    @Test
    void getLink_path_missed() {
        // Setup
        var path = List.of("a", "d");

        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", false, node3)));
        var subject = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node1)));

        // Execute
        var result = subject.getLink(path);

        // Verify
        assertEquals(Option.none(), result);
    }

    @Test
    void getLink_path_empty() {
        // Setup
        var path = List.<String>empty();

        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", false, node3)));
        var subject = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node1)));

        // Execute
        var result = subject.getLink(path);

        // Verify
        assertEquals(Option.none(), result);
    }

    @Test
    void setNode() {
        // Setup
        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", false, node3)));
        var subject = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node1)));

        // Execute
        var result = subject.setNode(10);

        // Verify
        assertNotEquals(subject, result);
        assertEquals(10, result.getNode());
        assertEquals(node1, result.getTree("a").get());
        assertEquals(true, result.getLink(List.of("a")).get());
    }

    @Test
    void setNode_path() {
        // Setup
        var path = List.of("a", "b");

        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", false, node3)));
        var subject = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node1)));

        var expectNode2 = NodeLinkTree.<String, Integer, Boolean>of(10);
        var expectNode1 = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, expectNode2), new Tuple3<>("c", false, node3)));
        var expected    = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, expectNode1)));

        // Execute
        var result = subject.setNode(path, 10);

        // Verify
        assertNotEquals(subject, result);
        assertEquals(10, result.getNode(path).get());
        assertEquals(expected, result);
    }

    @Test
    void setLink_key() {
        // Setup
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.<String, Integer, Boolean>of(1);
        var subject = NodeLinkTree.of(0,
                List.of(
                        new Tuple3<>("a", true, node1),
                        new Tuple3<>("b", false, node2)));

        var expected = NodeLinkTree.of(0,
                List.of(
                        new Tuple3<>("a", true, node1),
                        new Tuple3<>("b", true, node2)));

        // Execute
        var result = subject.setLink("b", true);

        // Verify
        assertNotEquals(subject, result);
        assertEquals(true, result.getLink(List.of("a")).get());
        assertEquals(expected, result);
    }

    @Test
    void setLink_path() {
        // Setup
        var path = List.of("a", "b");

        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", false, node3)));
        var subject = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node1)));

        var expectNode1 = NodeLinkTree.of(1, List.of(new Tuple3<>("b", true, node2), new Tuple3<>("c", false, node3)));
        var expected    = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, expectNode1)));

        // Execute
        var result = subject.setLink(path, true);

        // Verify
        assertNotEquals(subject, result);
        assertEquals(true, result.getLink(path).get());
        assertEquals(expected, result);
    }

    @Test
    void setChild_key() {
        // Setup
        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.<String, Integer, Boolean>of(1);
        var subject = NodeLinkTree.of(0,
                List.of(
                        new Tuple3<>("a", true, node1),
                        new Tuple3<>("b", false, node2)));

        var expected = NodeLinkTree.of(0,
                List.of(
                        new Tuple3<>("a", true, node1),
                        new Tuple3<>("b", false, node3)));

        // Execute
        var result = subject.setChild("b", node3);

        // Verify
        assertNotEquals(subject, result);
        assertEquals(node3, result.getTree("b").get());
        assertEquals(expected, result);
    }

    @Test
    void setChild_path() {
        // Setup
        var path = List.of("a", "b");

        var node4   = NodeLinkTree.<String, Integer, Boolean>of(4);
        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", false, node3)));
        var subject = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node1)));

        var expectNode1 = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node4), new Tuple3<>("c", false, node3)));
        var expected    = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, expectNode1)));

        // Execute
        var result = subject.setChild(path, node4);

        // Verify
        assertNotEquals(subject, result);
        assertEquals(node4, result.getTree(path).get());
        assertEquals(expected, result);
    }

    @Test
    void put_key() {
        // Setup
        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.<String, Integer, Boolean>of(1);
        var subject = NodeLinkTree.of(0,
                List.of(
                        new Tuple3<>("a", true, node1),
                        new Tuple3<>("b", false, node2)));

        var expected = NodeLinkTree.of(0,
                List.of(
                        new Tuple3<>("a", true, node1),
                        new Tuple3<>("b", false, node2),
                        new Tuple3<>("c", true, node3)));

        // Execute
        var result = subject.put("c", true, node3);

        // Verify
        assertNotEquals(subject, result);
        assertEquals(node3, result.getTree("c").get());
        assertEquals(expected, result);
    }

    @Test
    void put_key_existing() {
        // Setup
        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.<String, Integer, Boolean>of(1);
        var subject = NodeLinkTree.of(0,
                List.of(
                        new Tuple3<>("a", true, node1),
                        new Tuple3<>("b", false, node2)));

        var expected = NodeLinkTree.of(0,
                List.of(
                        new Tuple3<>("a", true, node1),
                        new Tuple3<>("b", true, node3)));

        // Execute
        var result = subject.put("b", true, node3);

        // Verify
        assertNotEquals(subject, result);
        assertEquals(node3, result.getTree("b").get());
        assertEquals(expected, result);
    }

    @Test
    void put_path() {
        // Setup
        var path = List.of("a", "d");

        var node4   = NodeLinkTree.<String, Integer, Boolean>of(4);
        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", false, node3)));
        var subject = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node1)));

        var expectNode1 = NodeLinkTree.of(1,
                List.of(
                        new Tuple3<>("b", false, node2),
                        new Tuple3<>("c", false, node3),
                        new Tuple3<>("d", true, node4)));
        var expected    = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, expectNode1)));

        // Execute
        var result = subject.put(path, true, node4);

        // Verify
        assertNotEquals(subject, result);
        assertEquals(node4, result.getTree(path).get());
        assertEquals(expected, result);
    }

    @Test
    void put_path_existing() {
        // Setup
        var path = List.of("a", "c");

        var node4   = NodeLinkTree.<String, Integer, Boolean>of(4);
        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", false, node3)));
        var subject = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node1)));

        var expectNode1 = NodeLinkTree.of(1,
                List.of(
                        new Tuple3<>("b", false, node2),
                        new Tuple3<>("c", true, node4)));
        var expected    = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, expectNode1)));

        // Execute
        var result = subject.put(path, true, node4);

        // Verify
        assertNotEquals(subject, result);
        assertEquals(node4, result.getTree(path).get());
        assertEquals(expected, result);
    }

    @Test
    void putIfMissed_key() {
        // Setup
        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.<String, Integer, Boolean>of(1);
        var subject = NodeLinkTree.of(0,
                List.of(
                        new Tuple3<>("a", true, node1),
                        new Tuple3<>("b", false, node2)));

        var expected = NodeLinkTree.of(0,
                List.of(
                        new Tuple3<>("a", true, node1),
                        new Tuple3<>("b", false, node2),
                        new Tuple3<>("c", true, node3)));

        // Execute
        var result = subject.putIfMissed("c", (n, k) -> new Tuple2<>(true, 3));

        // Verify
        assertNotEquals(subject, result);
        assertEquals(node3, result.getTree("c").get());
        assertEquals(expected, result);
    }

    @Test
    void putIfMissed_key_existing() {
        // Setup
        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.<String, Integer, Boolean>of(1);
        var subject = NodeLinkTree.of(0,
                List.of(
                        new Tuple3<>("a", true, node1),
                        new Tuple3<>("b", false, node2)));

        // Execute
        var result = subject.putIfMissed("b", (n, k) -> new Tuple2<>(true, 3));

        // Verify
        assertEquals(subject, result);
        assertEquals(node2, result.getTree("b").get());
    }

    @Test
    void putIfMissed_path() {
        // Setup
        var path = List.of("a", "d");

        var node4   = NodeLinkTree.<String, Integer, Boolean>of(4);
        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", false, node3)));
        var subject = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node1)));

        var expectNode1 = NodeLinkTree.of(1,
                List.of(
                        new Tuple3<>("b", false, node2),
                        new Tuple3<>("c", false, node3),
                        new Tuple3<>("d", true, node4)));
        var expected    = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, expectNode1)));

        // Execute
        var result = subject.putIfMissed(path, (n, k) -> new Tuple2<>(true, 4));

        // Verify
        assertNotEquals(subject, result);
        assertEquals(node4, result.getTree(path).get());
        assertEquals(expected, result);
    }

    @Test
    void putIfMissed_path_existing() {
        // Setup
        var path = List.of("a", "c");

        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", false, node3)));
        var subject = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node1)));

        // Execute
        var result = subject.putIfMissed(path, (n, k) -> new Tuple2<>(true, 4));

        // Verify
        assertEquals(subject, result);
        assertEquals(node3, result.getTree(path).get());
    }

    @Test
    void merge() {
        // Setup
        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", false, node3)));
        var subject = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node1)));

        var node4 = NodeLinkTree.<String, Integer, Boolean>of(4);
        var node5 = NodeLinkTree.<String, Integer, Boolean>of(5);
        var node6 = NodeLinkTree.of(6, List.of(new Tuple3<>("d", true, node5), new Tuple3<>("c", true, node4)));
        var that  = NodeLinkTree.of(0, List.of(new Tuple3<>("a", false, node6)));

        var node7    = NodeLinkTree.of(1,
                List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", false, node3), new Tuple3<>("d", true, node5)));
        var expected = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node7)));

        // Execute
        var result = subject.merge(that);

        // Verify
        assertNotEquals(subject, result);
        assertEquals(expected, result);
    }

    @Test
    void mergeLinks() {
        // Setup
        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", false, node3)));
        var subject = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node1)));

        var node4 = NodeLinkTree.<String, Integer, Boolean>of(4);
        var node5 = NodeLinkTree.<String, Integer, Boolean>of(5);
        var node6 = NodeLinkTree.of(6, List.of(new Tuple3<>("d", true, node5), new Tuple3<>("c", true, node4)));
        var that  = NodeLinkTree.of(0, List.of(new Tuple3<>("a", false, node6)));

        var node7    = NodeLinkTree.of(1,
                List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", true, node3), new Tuple3<>("d", true, node5)));
        var expected = NodeLinkTree.of(0, List.of(new Tuple3<>("a", false, node7)));

        // Execute
        var result = subject.mergeLinks(that);

        // Verify
        assertNotEquals(subject, result);
        assertEquals(expected, result);
    }

    @Test
    void mergeNode() {
        // Setup
        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", false, node3)));
        var subject = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node1)));

        var node4 = NodeLinkTree.<String, Integer, Boolean>of(4);
        var node5 = NodeLinkTree.<String, Integer, Boolean>of(5);
        var node6 = NodeLinkTree.of(6, List.of(new Tuple3<>("d", true, node5), new Tuple3<>("c", true, node4)));
        var that  = NodeLinkTree.of(0, List.of(new Tuple3<>("a", false, node6)));

        var node7    = NodeLinkTree.of(6,
                List.of(
                        new Tuple3<>("b", false, node2),
                        new Tuple3<>("c", false, node4),
                        new Tuple3<>("d", true, node5)));
        var expected = NodeLinkTree.of(0,
                List.of(new Tuple3<>("a", true, node7)));

        // Execute
        var result = subject.mergeNode(that);

        // Verify
        assertNotEquals(subject, result);
        assertEquals(expected, result);
    }

    @Test
    void mergeAll() {
        // Setup
        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", false, node3)));
        var subject = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node1)));

        var node4 = NodeLinkTree.<String, Integer, Boolean>of(4);
        var node5 = NodeLinkTree.<String, Integer, Boolean>of(5);
        var node6 = NodeLinkTree.of(6, List.of(new Tuple3<>("d", true, node5), new Tuple3<>("c", true, node4)));
        var that  = NodeLinkTree.of(0, List.of(new Tuple3<>("a", false, node6)));

        var node7    = NodeLinkTree.of(6,
                List.of(
                        new Tuple3<>("b", false, node2),
                        new Tuple3<>("c", true, node4),
                        new Tuple3<>("d", true, node5)));
        var expected = NodeLinkTree.of(0,
                List.of(new Tuple3<>("a", false, node7)));

        // Execute
        var result = subject.mergeAll(that);

        // Verify
        assertNotEquals(subject, result);
        assertEquals(expected, result);
    }

    @Test
    void mapNodes() {
        // Setup
        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", false, node3)));
        var subject = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node1)));

        var mapped3  = NodeLinkTree.<String, String, Boolean>of("3");
        var mapped2  = NodeLinkTree.<String, String, Boolean>of("2");
        var mapped1  = NodeLinkTree.of("1", List.of(new Tuple3<>("b", false, mapped2), new Tuple3<>("c", false, mapped3)));
        var expected = NodeLinkTree.of("0", List.of(new Tuple3<>("a", true, mapped1)));

        // Execute
        var result = subject.mapNodes(n -> Integer.toString(n));

        // Verify
        assertNotEquals(subject, result);
        assertEquals(expected, result);
    }

    @Test
    void mapNodesWithIndex() {
        // Setup
        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", false, node3)));
        var subject = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node1)));

        var mapped3  = NodeLinkTree.<String, String, Boolean>of("3-4");
        var mapped2  = NodeLinkTree.<String, String, Boolean>of("2-3");
        var mapped1  = NodeLinkTree.of("1-2", List.of(new Tuple3<>("b", false, mapped2), new Tuple3<>("c", false, mapped3)));
        var expected = NodeLinkTree.of("0-1", List.of(new Tuple3<>("a", true, mapped1)));

        // Execute
        var result = subject.mapNodesWithIndex(1, (n, i) -> Integer.toString(n) + "-" + i);

        // Verify
        assertNotEquals(subject, result);
        assertEquals(expected, result);
    }

    @Test
    void mapLinks() {
        // Setup
        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", false, node3)));
        var subject = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node1)));

        var mapped3  = NodeLinkTree.<String, Integer, String>of(3);
        var mapped2  = NodeLinkTree.<String, Integer, String>of(2);
        var mapped1  = NodeLinkTree.of(1, List.of(new Tuple3<>("b", "true", mapped2), new Tuple3<>("c", "true", mapped3)));
        var expected = NodeLinkTree.of(0, List.of(new Tuple3<>("a", "false", mapped1)));

        // Execute
        var result = subject.mapLinks(l -> Boolean.toString(!l));

        // Verify
        assertNotEquals(subject, result);
        assertEquals(expected, result);
    }

    @Test
    void mapLinksWithNodes() {
        // Setup
        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", false, node3)));
        var subject = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node1)));

        var mapped3  = NodeLinkTree.<String, Integer, String>of(3);
        var mapped2  = NodeLinkTree.<String, Integer, String>of(2);
        var mapped1  = NodeLinkTree.of(1,
                List.of(new Tuple3<>("b", "1-true-2", mapped2), new Tuple3<>("c", "1-true-3", mapped3)));
        var expected = NodeLinkTree.of(0, List.of(new Tuple3<>("a", "0-false-1", mapped1)));

        // Execute
        var result = subject.mapLinksWithNodes(t -> t._1 + "-" + !t._2 + "-" + t._3);

        // Verify
        assertNotEquals(subject, result);
        assertEquals(expected, result);
    }

    @Test
    void mapLinksWithIndex() {
        // Setup
        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", false, node3)));
        var subject = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node1)));

        var mapped3  = NodeLinkTree.<String, Integer, String>of(3);
        var mapped2  = NodeLinkTree.<String, Integer, String>of(2);
        var mapped1  = NodeLinkTree.of(1,
                List.of(new Tuple3<>("b", "true-3", mapped2), new Tuple3<>("c", "true-4", mapped3)));
        var expected = NodeLinkTree.of(0, List.of(new Tuple3<>("a", "false-2", mapped1)));

        // Execute
        var result = subject.mapLinksWithIndex(2, (l, i) -> !l + "-" + i);

        // Verify
        assertNotEquals(subject, result);
        assertEquals(expected, result);
    }

    @Test
    void _hashCode() {
        // Setup
        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", false, node3)));
        var subject = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node1)));

        var node6   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node5   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node4   = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node5), new Tuple3<>("c", false, node6)));
        var similar = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node4)));

        // Execute
        int result = subject.hashCode();

        // Verify
        assertEquals(similar.hashCode(), result);
    }

    @Test
    void _toString() {
        // Setup
        var node3   = NodeLinkTree.<String, Integer, Boolean>of(3);
        var node2   = NodeLinkTree.<String, Integer, Boolean>of(2);
        var node1   = NodeLinkTree.of(1, List.of(new Tuple3<>("b", false, node2), new Tuple3<>("c", false, node3)));
        var subject = NodeLinkTree.of(0, List.of(new Tuple3<>("a", true, node1)));

        // Execute
        String result = subject.toString();

        // Verify
        assertTrue(result.contains("1"));
        assertTrue(result.contains("3"));
        assertTrue(result.contains("c"));
    }
}
