package com.landawn.abacus.da.neo4j;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.neo4j.ogm.model.Result;
import org.neo4j.ogm.session.Session;
import org.neo4j.ogm.session.SessionFactory;

import com.landawn.abacus.da.MockitoTestBase;

/**
 * Line coverage for the Neo4j stream and findOnly debug logs. The session is mocked.
 */
class Neo4jExecutorLineCoverageTest extends MockitoTestBase {

    private static final String CYPHER = "MATCH (p:Person {name: $name}) RETURN p.name AS name";

    @Mock
    private SessionFactory sessionFactory;

    @Mock
    private Session session;

    private Neo4jExecutor executor;

    @BeforeEach
    void setUp() {
        when(sessionFactory.openSession()).thenReturn(session);
        executor = new Neo4jExecutor(sessionFactory);
    }

    @Test
    void debugEnabledFindOnlyAndStreamsLogTheCypher() {
        final Map<String, Object> parameters = new LinkedHashMap<>();
        parameters.put("name", "Ada");
        final Map<String, Object> row = Map.of("name", "Ada");
        when(session.query(eq(CYPHER), same(parameters))).thenAnswer(invocation -> resultOf(row));
        when(session.query(eq(CYPHER), same(parameters), eq(true))).thenAnswer(invocation -> resultOf(row));
        when(session.query(eq(String.class), eq(CYPHER), same(parameters))).thenReturn(List.of("Ada"));
        when(session.queryForObject(eq(String.class), eq(CYPHER), same(parameters))).thenReturn("Ada");

        withDebug(Neo4jExecutor.class, () -> {
            assertEquals(List.of(row), executor.stream(CYPHER, parameters).toList());
            assertEquals(List.of(row), executor.stream(CYPHER, parameters, true).toList());
            assertEquals(List.of("Ada"), executor.stream(String.class, CYPHER, parameters).toList());
            assertEquals("Ada", executor.findOnly(String.class, CYPHER, parameters).get());
        });

        assertEquals(Map.of("name", "Ada"), parameters);
        verify(sessionFactory, times(1)).openSession();
        verify(session, times(4)).clear();
        verify(session).query(eq(CYPHER), same(parameters));
        verify(session).query(eq(CYPHER), same(parameters), eq(true));
        verify(session).query(eq(String.class), eq(CYPHER), same(parameters));
        verify(session).queryForObject(eq(String.class), eq(CYPHER), same(parameters));
    }

    private static Result resultOf(final Map<String, Object> row) {
        final Result result = mock(Result.class);
        when(result.iterator()).thenReturn(List.of(row).iterator());

        return result;
    }

    private static void withDebug(final Class<?> type, final Runnable action) {
        final java.util.logging.Logger jul = java.util.logging.Logger.getLogger(type.getName());
        final java.util.logging.Level previous = jul.getLevel();
        jul.setLevel(java.util.logging.Level.FINE);

        try {
            action.run();
        } finally {
            jul.setLevel(previous);
        }
    }
}
