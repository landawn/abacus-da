package com.landawn.abacus.da.neo4j;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.ogm.annotation.GeneratedValue;
import org.neo4j.ogm.annotation.Id;
import org.neo4j.ogm.annotation.Labels;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Property;
import org.neo4j.ogm.cypher.query.SortOrder;
import org.neo4j.ogm.driver.Driver;
import org.neo4j.ogm.model.GraphModel;
import org.neo4j.ogm.request.DefaultRequest;
import org.neo4j.ogm.request.GraphModelRequest;
import org.neo4j.ogm.request.Request;
import org.neo4j.ogm.request.RowModelRequest;
import org.neo4j.ogm.request.Statement;
import org.neo4j.ogm.response.Response;
import org.neo4j.ogm.response.model.DefaultGraphModel;
import org.neo4j.ogm.response.model.NodeModel;
import org.neo4j.ogm.session.SessionFactory;
import org.neo4j.ogm.transaction.AbstractTransaction;

/**
 * 2026-11-03 review of {@link Neo4jExecutor}: what a {@code save} of an entity loaded by an earlier executor call writes,
 * now that a pooled session carries no mapping state (not even Neo4j-OGM's primary-index cache) into the next call.
 * Pins the {@code save} Javadoc against a real OGM {@link SessionFactory} whose {@link Driver} is mocked (no database):
 * a primary-index-only entity is MERGEd on its primary-index value and all its labels (so a label added since the load
 * no longer matches the stored node), a label removed since the load is never removed, and the MERGE replaces the node's
 * properties, read-only ones included. Inside one {@code run} session the same changes are an update by graph ID. Also
 * pins the {@code delete} Javadoc: an entity without a graph ID is reloaded by its primary-index value, and a delete
 * followed by a save of the same primary-index value creates the node again only across executor calls. And the
 * {@code loadAll(Class, Collection, SortOrder)} example: with an empty {@code SortOrder} the result follows the ids.
 */
public class Neo4jExecutorReview20261103Test {

    @NodeEntity
    public static class TaggedKeyedNode {
        @Id
        String uuid;

        String name;

        @Property(readOnly = true)
        String computed;

        @Labels
        List<String> labels = new ArrayList<>();

        public TaggedKeyedNode() {
        }
    }

    @NodeEntity
    public static class TaggedNativeNode {
        @Id
        @GeneratedValue
        Long id;

        String name;

        @Labels
        List<String> labels = new ArrayList<>();

        public TaggedNativeNode() {
        }
    }

    /** A native graph-ID field (by OGM's {@code Long id} naming rule) and a primary index. */
    @NodeEntity
    public static class DualIdNode {
        Long id;

        @Id
        String uuid;

        public DualIdNode() {
        }
    }

    private static final String KEYED_LABEL = Neo4jExecutorReview20261103Test.class.getSimpleName() + "$TaggedKeyedNode";
    private static final String NATIVE_LABEL = Neo4jExecutorReview20261103Test.class.getSimpleName() + "$TaggedNativeNode";

    private Request request;
    private Neo4jExecutor executor;
    private final List<Statement> statements = new ArrayList<>();

    @BeforeEach
    public void setUp() {
        final Driver driver = mock(Driver.class);
        when(driver.getTypeSystem()).thenCallRealMethod();
        when(driver.requiresTransaction()).thenReturn(true);
        when(driver.getTransactionFactorySupplier()).thenReturn(manager -> (type, bookmarks) -> new AbstractTransaction(manager) {
            @Override
            protected void rollback0() {
            }

            @Override
            protected void commit0() {
            }
        });
        when(driver.getExceptionTranslator()).thenReturn(e -> e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e));
        request = mock(Request.class);
        when(driver.request(any())).thenReturn(request);
        when(request.execute(any(DefaultRequest.class))).thenAnswer(invocation -> {
            statements.addAll(invocation.<DefaultRequest> getArgument(0).getStatements());

            return responseOf();
        });

        final String prefix = Neo4jExecutorReview20261103Test.class.getName() + "$";
        executor = new Neo4jExecutor(new SessionFactory(driver, prefix + "TaggedKeyedNode", prefix + "TaggedNativeNode", prefix + "DualIdNode"));
    }

    private void answerLoadsWith(final NodeModel node) {
        when(request.execute(any(GraphModelRequest.class))).thenAnswer(invocation -> responseOf(graphRow(node)));
    }

    /** The single statement of the last save, with its parameters. */
    private Statement onlySavedStatement() {
        assertEquals(1, statements.size(), () -> statements.stream().map(Statement::getStatement).toList().toString());

        return statements.get(0);
    }

    @Test
    public void testCrossCallSaveOfPrimaryIndexEntityWithAddedLabelMergesOnAllLabels() {
        answerLoadsWith(node(42L, Map.of("uuid", "u-1", "name", "old"), KEYED_LABEL));

        final TaggedKeyedNode loaded = executor.load(TaggedKeyedNode.class, "u-1");
        assertNotNull(loaded);
        loaded.labels.add("Vip");

        executor.save(loaded);

        final String cypher = onlySavedStatement().getStatement();
        // The MERGE pattern requires the new label too, which the stored node (graph ID 42) lacks: in Neo4j the pattern does
        // not match it, so a second node with uuid "u-1" is created (or a uniqueness constraint on uuid rejects the save).
        assertAll(() -> assertTrue(cypher.startsWith("UNWIND $rows as row MERGE (n:`" + KEYED_LABEL + "`:`Vip`{`uuid`: row.props.`uuid`})"), cypher),
                () -> assertFalse(cypher.contains("ID(n)=row.nodeId"), cypher));
    }

    @Test
    public void testSameSessionSaveOfPrimaryIndexEntityWithAddedLabelUpdatesTheLoadedNodeByGraphId() {
        answerLoadsWith(node(42L, Map.of("uuid", "u-1", "name", "old"), KEYED_LABEL));

        executor.run(session -> {
            final TaggedKeyedNode loaded = session.load(TaggedKeyedNode.class, "u-1");
            loaded.labels.add("Vip");
            session.save(loaded);
        });

        final Statement statement = onlySavedStatement();
        assertAll(() -> assertTrue(statement.getStatement().startsWith("UNWIND $rows as row MATCH (n) WHERE ID(n)=row.nodeId "), statement.getStatement()),
                () -> assertTrue(statement.getStatement().contains("SET n:`" + KEYED_LABEL + "`:`Vip`"), statement.getStatement()),
                () -> assertEquals(42L, firstRow(statement).get("nodeId")));
    }

    @Test
    public void testCrossCallSaveNeverRemovesALabelRemovedSinceTheLoad() {
        answerLoadsWith(node(7L, Map.of("name", "old"), NATIVE_LABEL, "Vip"));

        final TaggedNativeNode loadedNative = executor.load(TaggedNativeNode.class, 7L);
        assertEquals(List.of("Vip"), loadedNative.labels);
        loadedNative.labels.remove("Vip");
        executor.save(loadedNative);
        final String nativeCrossCall = onlySavedStatement().getStatement();
        statements.clear();

        executor.run(session -> {
            final TaggedNativeNode loaded = session.load(TaggedNativeNode.class, 7L);
            loaded.labels.remove("Vip");
            session.save(loaded);
        });
        final String nativeSameSession = onlySavedStatement().getStatement();
        statements.clear();

        answerLoadsWith(node(42L, Map.of("uuid", "u-1", "name", "old"), KEYED_LABEL, "Vip"));
        final TaggedKeyedNode loadedKeyed = executor.load(TaggedKeyedNode.class, "u-1");
        assertEquals(List.of("Vip"), loadedKeyed.labels);
        loadedKeyed.labels.remove("Vip");
        executor.save(loadedKeyed);
        final String keyedCrossCall = onlySavedStatement().getStatement();

        assertAll(
                // native graph ID: updated by ID, but without the loaded labels OGM cannot know that Vip was removed
                () -> assertTrue(nativeCrossCall.contains("WHERE ID(n)=row.nodeId"), nativeCrossCall),
                () -> assertFalse(nativeCrossCall.contains("REMOVE n:`Vip`"), nativeCrossCall),
                // primary index only: MERGE (n:<class label> {uuid}) still matches the node that has Vip, and nothing removes it
                () -> assertTrue(keyedCrossCall.contains("MERGE (n:`" + KEYED_LABEL + "`{"), keyedCrossCall),
                () -> assertFalse(keyedCrossCall.contains("REMOVE"), keyedCrossCall),
                // within one session the removal is written
                () -> assertTrue(nativeSameSession.contains("REMOVE n:`Vip`"), nativeSameSession));
    }

    @Test
    public void testCrossCallMergeReplacesAllPropertiesReadOnlyOnesIncluded() {
        answerLoadsWith(node(42L, Map.of("uuid", "u-1", "name", "old", "computed", "kept by the database"), KEYED_LABEL));

        final TaggedKeyedNode loaded = executor.load(TaggedKeyedNode.class, "u-1");
        assertEquals("kept by the database", loaded.computed);
        loaded.name = "new";

        executor.save(loaded);

        final Statement statement = onlySavedStatement();
        final Map<?, ?> props = (Map<?, ?>) firstRow(statement).get("props");
        // SET n=row.props replaces every property of the matched node: the read-only property, which OGM never writes, is
        // therefore removed (inside one session OGM updates by ID with SET n += row.props, which keeps it)
        assertAll(() -> assertTrue(statement.getStatement().contains("SET n=row.props"), statement.getStatement()),
                () -> assertEquals(Map.of("uuid", "u-1", "name", "new"), new LinkedHashMap<>(props)));
    }

    @Test
    public void testDeleteOfEntityWithNullGraphIdButPrimaryIndexValueReloadsAndDeletesIt() {
        answerLoadsWith(node(42L, Map.of("uuid", "u-1"), Neo4jExecutorReview20261103Test.class.getSimpleName() + "$DualIdNode"));
        final List<Map<String, Object>> deletes = new ArrayList<>();
        when(request.execute(any(RowModelRequest.class))).thenAnswer(invocation -> {
            deletes.add(new LinkedHashMap<>(invocation.<RowModelRequest> getArgument(0).getParameters()));

            return responseOf();
        });

        final DualIdNode neverLoaded = new DualIdNode();
        neverLoaded.uuid = "u-1";

        executor.delete(neverLoaded);

        // no graph ID, but OGM reloads the entity by its primary-index value and deletes the node it finds
        assertAll(() -> verify(request).execute(any(GraphModelRequest.class)), () -> assertEquals(1, deletes.size()),
                () -> assertEquals(42L, deletes.get(0).get("id")));
    }

    @Test
    public void testDeleteThenSaveOfSamePrimaryIndexValueCreatesTheNodeOnlyAcrossCalls() {
        answerLoadsWith(node(42L, Map.of("uuid", "u-1", "name", "old"), KEYED_LABEL));
        when(request.execute(any(RowModelRequest.class))).thenAnswer(invocation -> responseOf());

        executor.run(session -> {
            final TaggedKeyedNode loaded = session.load(TaggedKeyedNode.class, "u-1");
            session.delete(loaded);
            session.save(keyed("u-1", "new"));
        });
        final Statement sameSession = onlySavedStatement();
        statements.clear();

        executor.delete(executor.load(TaggedKeyedNode.class, "u-1"));
        executor.save(keyed("u-1", "new"));
        final String acrossCalls = onlySavedStatement().getStatement();

        assertAll(
                // Neo4j-OGM keeps the deleted node's graph ID 42 for "u-1" in the session: the save updates node 42, which is gone
                () -> assertTrue(sameSession.getStatement().contains("MATCH (n) WHERE ID(n)=row.nodeId"), sameSession.getStatement()),
                () -> assertEquals(42L, firstRow(sameSession).get("nodeId")),
                // a later executor call starts from an empty cache, so the node is created again
                () -> assertTrue(acrossCalls.contains("MERGE (n:`" + KEYED_LABEL + "`{`uuid`: row.props.`uuid`})"), acrossCalls));
    }

    @Test
    public void testLoadAllByIdsWithEmptySortOrderKeepsTheOrderOfTheIds() {
        // the database returns 7 before 9; the result follows the order of the ids argument instead
        when(request.execute(any(GraphModelRequest.class))).thenAnswer(
                invocation -> responseOf(graphRow(node(7L, Map.of("name", "seven"), NATIVE_LABEL)), graphRow(node(9L, Map.of("name", "nine"), NATIVE_LABEL))));

        final List<Long> loadedIds = executor.loadAll(TaggedNativeNode.class, List.of(9L, 7L), new SortOrder()).stream().map(n -> n.id).toList();

        assertEquals(List.of(9L, 7L), loadedIds);
    }

    private static TaggedKeyedNode keyed(final String uuid, final String name) {
        final TaggedKeyedNode node = new TaggedKeyedNode();
        node.uuid = uuid;
        node.name = name;

        return node;
    }

    private static Map<?, ?> firstRow(final Statement statement) {
        return (Map<?, ?>) ((List<?>) statement.getParameters().get("rows")).get(0);
    }

    private static NodeModel node(final long id, final Map<String, Object> properties, final String... labels) {
        final NodeModel node = new NodeModel(id);
        node.setLabels(labels);
        node.setProperties(properties);

        return node;
    }

    private static GraphModel graphRow(final NodeModel... nodes) {
        final DefaultGraphModel row = new DefaultGraphModel();

        for (final NodeModel node : nodes) {
            row.addNode(node);
        }

        return row;
    }

    @SafeVarargs
    private static <T> Response<T> responseOf(final T... rows) {
        final Deque<T> remaining = new ArrayDeque<>(Arrays.asList(rows));

        return new Response<>() {
            @Override
            public T next() {
                return remaining.poll();
            }

            @Override
            public void close() {
            }

            @Override
            public String[] columns() {
                return new String[] { "x" };
            }
        };
    }
}
