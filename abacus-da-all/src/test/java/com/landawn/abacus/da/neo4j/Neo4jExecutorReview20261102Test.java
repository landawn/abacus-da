package com.landawn.abacus.da.neo4j;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.ogm.annotation.Id;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.context.MappingContext;
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
import org.neo4j.ogm.session.Neo4jSession;
import org.neo4j.ogm.session.Session;
import org.neo4j.ogm.session.SessionFactory;
import org.neo4j.ogm.transaction.AbstractTransaction;

/**
 * 2026-11-02 review of {@link Neo4jExecutor}: a pooled session must not carry Neo4j-OGM's primary-index-to-graph-ID
 * cache (which {@code Session.clear()} keeps) into later operations. Runs against a real OGM {@link SessionFactory}
 * whose {@link Driver} is mocked (no database).
 */
public class Neo4jExecutorReview20261102Test {

    @NodeEntity
    public static class KeyedNode {
        @Id
        String uuid;

        String name;

        public KeyedNode() {
        }

        KeyedNode(final String uuid, final String name) {
            this.uuid = uuid;
            this.name = name;
        }
    }

    private static final String KEYED_NODE_LABEL = Neo4jExecutorReview20261102Test.class.getSimpleName() + "$KeyedNode";

    private Request request;
    private Neo4jExecutor executor;

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

        executor = new Neo4jExecutor(new SessionFactory(driver, Neo4jExecutorReview20261102Test.class.getName() + "$KeyedNode"));
    }

    /** Lets the pooled session load the node {@code u-1} under graph ID 42, as an earlier operation would. */
    private void loadU1AsGraphId42() {
        when(request.execute(any(GraphModelRequest.class))).thenAnswer(invocation -> responseOf(graphRow(keyedNodeModel(42L, "u-1", "old"))));

        final KeyedNode loaded = executor.load(KeyedNode.class, "u-1");

        assertNotNull(loaded);
        assertEquals("old", loaded.name);
    }

    @Test
    public void testSaveAfterAnEarlierLoadIsAMergeNotAnUpdateOfTheCachedGraphId() {
        loadU1AsGraphId42();

        final List<String> statements = new ArrayList<>();
        when(request.execute(any(DefaultRequest.class))).thenAnswer(invocation -> {
            for (final Statement statement : invocation.<DefaultRequest> getArgument(0).getStatements()) {
                statements.add(statement.getStatement());
            }

            return responseOf();
        });

        // e.g. the node was deleted meanwhile (by Cypher or another process) and is now created again
        executor.save(new KeyedNode("u-1", "new"));

        assertEquals(1, statements.size(), statements::toString);
        // HEAD reused the graph ID cached by the earlier load: "MATCH (n) WHERE ID(n)=row.nodeId ..." with nodeId 42,
        // which writes nothing once node 42 is gone, or overwrites another node that reused ID 42
        assertFalse(statements.get(0).contains("WHERE ID(n)=row.nodeId"), statements.get(0));
        assertTrue(statements.get(0).contains("MERGE (n:"), statements.get(0));
    }

    @Test
    public void testDeleteAfterAnEarlierLoadReloadsInsteadOfDeletingTheCachedGraphId() {
        loadU1AsGraphId42();

        // the node is gone now: the reload finds nothing
        when(request.execute(any(GraphModelRequest.class))).thenAnswer(invocation -> responseOf());
        when(request.execute(any(RowModelRequest.class))).thenAnswer(invocation -> responseOf());

        executor.delete(new KeyedNode("u-1", "any"));

        // HEAD sent "MATCH (n) WHERE ID(n) = $id ... DELETE" for the cached ID 42 without reloading
        verify(request, times(2)).execute(any(GraphModelRequest.class));
        verify(request, never()).execute(any(RowModelRequest.class));
    }

    @Test
    public void testNeo4jSessionIsStillPooledWithAnEmptyPrimaryIndexCache() throws Exception {
        loadU1AsGraphId42();

        final Session first = executor.call(session -> session);
        final Session second = executor.call(session -> session);

        assertSame(first, second);
        assertTrue(primaryIdToNativeId(first).isEmpty());
    }

    @Test
    public void testSessionWhoseCacheCannotBeClearedIsNotPooled() {
        // A Neo4jSession whose mapping context cannot be read (here: context() fails) keeps its cache, so it must be dropped
        // after the operation instead of being handed to the next one with stale graph IDs.
        final SessionFactory sessionFactory = mock(SessionFactory.class);
        final Neo4jSession first = mock(Neo4jSession.class);
        final Neo4jSession second = mock(Neo4jSession.class);
        when(first.context()).thenThrow(new IllegalStateException("no context"));
        when(second.context()).thenThrow(new IllegalStateException("no context"));
        when(sessionFactory.openSession()).thenReturn(first, second);
        final Neo4jExecutor unpooled = new Neo4jExecutor(sessionFactory);

        assertSame(first, unpooled.call(session -> session));
        // The failure is logged, not thrown; the next operation gets a fresh session.
        assertSame(second, unpooled.call(session -> session));
        verify(sessionFactory, times(2)).openSession();
        verify(first).clear();
    }

    @Test
    public void testSessionClearKeepsThePrimaryIndexCache() throws Exception {
        // pins the Neo4j-OGM behavior the fix compensates for: if OGM starts clearing it, the reflective clear is redundant
        final Session session = executor.sessionFactory().openSession();
        when(request.execute(any(GraphModelRequest.class))).thenAnswer(invocation -> responseOf(graphRow(keyedNodeModel(42L, "u-1", "old"))));

        session.load(KeyedNode.class, "u-1");
        session.clear();

        assertEquals(Arrays.asList(42L), new ArrayList<>(primaryIdToNativeId(session).values()));
    }

    private static Map<?, ?> primaryIdToNativeId(final Session session) throws Exception {
        final MappingContext context = ((Neo4jSession) session).context();
        final Field field = MappingContext.class.getDeclaredField("primaryIdToNativeId");
        field.setAccessible(true);

        return (Map<?, ?>) field.get(context);
    }

    private static NodeModel keyedNodeModel(final long id, final String uuid, final String name) {
        final NodeModel node = new NodeModel(id);
        node.setLabels(new String[] { KEYED_NODE_LABEL });
        node.setProperties(Map.of("uuid", uuid, "name", name));

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
