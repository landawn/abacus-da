package com.landawn.abacus.da.cassandra.v3;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;

class CassandraExecutorTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("parameters")
    void parameterContainersBindInColumnOrderWithoutChangingCallerValues(final String scenario, final Object input, final Object[] expected) {
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.columns(DataType.bigint(), "id", "value");
            final Object[] snapshot = input instanceof Object[] array ? array.clone() : null;
            fixture.executor.prepareStatement("SELECT * FROM items WHERE id=? AND value=?", input);
            assertArrayEquals(expected, fixture.values.getFirst());
            if (snapshot != null) {
                assertArrayEquals(snapshot, (Object[]) input);
            }
        }
    }

    static Stream<Arguments> parameters() {
        final Map<String, Object> nullValue = new LinkedHashMap<>();
        nullValue.put("id", 1L);
        nullValue.put("value", null);
        return Stream.of(Arguments.of("array with conversion", new Object[] { 1, 2 }, new Object[] { 1L, 2L }),
                Arguments.of("list with conversion", List.of(1, 2), new Object[] { 1L, 2L }),
                Arguments.of("named map", Map.of("value", 2L, "id", 1L), new Object[] { 1L, 2L }),
                Arguments.of("explicit null", nullValue, new Object[] { 1L, null }),
                Arguments.of("bean properties", new CassandraBatchTest.Item(1, 2), new Object[] { 1L, 2L }));
    }

    @Test
    void singleParameterAcceptsNullAndUnwrapsOneElementContainers() {
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.columns(DataType.bigint(), "id");
            fixture.executor.prepareStatement("SELECT * FROM items WHERE id=?", (Object) null);
            fixture.executor.prepareStatement("SELECT * FROM items WHERE id=?", Collections.singletonList(null));
            fixture.executor.prepareStatement("SELECT * FROM items WHERE id=?", (Object) new Object[] { 7 });
            assertArrayEquals(new Object[] { null }, fixture.values.get(0));
            assertArrayEquals(new Object[] { null }, fixture.values.get(1));
            assertArrayEquals(new Object[] { 7L }, fixture.values.get(2));
        }
    }

    @Test
    void missingAndExcessParametersAreRejectedBeforeBinding() {
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.columns(DataType.bigint(), "id", "value");
            final String sql = "SELECT * FROM items WHERE id=? AND value=?";
            assertThrows(IllegalArgumentException.class, () -> fixture.executor.prepareStatement(sql));
            assertThrows(IllegalArgumentException.class, () -> fixture.executor.prepareStatement(sql, Map.of("id", 1L)));
            assertThrows(IllegalArgumentException.class, () -> fixture.executor.prepareStatement(sql, 1L));
            assertThrows(IllegalArgumentException.class, () -> fixture.executor.prepareStatement(sql, 1L, 2L, 3L));
            assertTrue(fixture.values.isEmpty());
        }
    }

    @Test
    void invalidColumnMetadataReportsTheMissingParameterIndex() {
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.columns(DataType.bigint(), "id", null);
            final String sql = "SELECT * FROM items WHERE id=? AND value=?";
            for (final Object input : List.of(Map.of("id", 1L), new CassandraBatchTest.Item(1, 2))) {
                final IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> fixture.executor.prepareStatement(sql, input));
                assertTrue(error.getMessage().contains("index 1"), error.getMessage());
            }
            assertTrue(fixture.values.isEmpty());
        }
    }

    @Test
    void parameterlessQueriesAcceptEmptyContainersButRejectActualValues() {
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.columns(DataType.bigint());
            final String sql = "SELECT * FROM items";
            for (final Object input : List.of(List.of(), Map.of(), new Object[0])) {
                fixture.executor.prepareStatement(sql, input);
                assertEquals(0, fixture.values.getLast().length);
            }
            assertThrows(IllegalArgumentException.class, () -> fixture.executor.prepareStatement(sql, List.of(1)));
            assertThrows(IllegalArgumentException.class, () -> fixture.executor.prepareStatement(sql, 1, 2));
            assertEquals(3, fixture.values.size());
        }
    }

    @Test
    void executeOverloadsReturnTheDriversResultAndExposeOwnedCluster() {
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.columns(DataType.bigint());
            final ResultSet result = mock(ResultSet.class);
            when(fixture.session.execute(any(Statement.class))).thenReturn(result);
            assertSame(fixture.cluster, fixture.executor.cluster());
            assertSame(result, fixture.executor.execute("SELECT * FROM items"));
            assertSame(result, fixture.executor.execute("SELECT * FROM items", Map.of()));
            final SimpleStatement direct = new SimpleStatement("SELECT * FROM items");
            assertSame(result, fixture.executor.execute(direct));
            verify(fixture.session).execute(direct);
        }
    }

    @Test
    void longQueriesArePreparedAgainInsteadOfRetainedInTheCache() {
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.columns(DataType.bigint());
            final String sql = "SELECT " + java.util.stream.IntStream.range(0, 300).mapToObj(i -> "c" + i).collect(java.util.stream.Collectors.joining(", "))
                    + " FROM items";
            fixture.executor.prepareStatement(sql);
            fixture.executor.prepareStatement(sql);
            verify(fixture.session, times(2)).prepare(anyString());
        }
    }

    @Test
    void closeReleasesTheClusterEvenWhenTheSessionFails() {
        final CassandraTestFixture fixture = new CassandraTestFixture();
        final IllegalStateException failure = new IllegalStateException("session close");
        doThrow(failure).when(fixture.session).close();
        assertSame(failure, assertThrows(IllegalStateException.class, fixture::close));
        verify(fixture.cluster).close();
    }

    @Test
    void closeDoesNotCloseAlreadyClosedDriverResources() {
        final CassandraTestFixture fixture = new CassandraTestFixture();
        when(fixture.session.isClosed()).thenReturn(true);
        when(fixture.cluster.isClosed()).thenReturn(true);
        fixture.close();
        verify(fixture.session, never()).close();
        verify(fixture.cluster, never()).close();
    }

    // ---- 2026-10-19 sliceK ----

    // The statement pool closes itself in its own JVM shutdown hook; CQL run afterwards (e.g. from an application's
    // shutdown hook) must still prepare and bind, just without caching.
    @Test
    void sliceK_closedStatementCacheIsTreatedAsAMiss() throws Exception {
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.columns(DataType.bigint(), "id");
            final String query = "SELECT * FROM items WHERE id = ?";
            fixture.executor.prepareStatement(query, 1L);
            fixture.executor.prepareStatement(query, 2L);
            verify(fixture.session, times(1)).prepare(query);

            final var poolField = CassandraExecutor.class.getDeclaredField("preparedStatementPool");
            poolField.setAccessible(true);
            ((com.landawn.abacus.pool.KeyedObjectPool<?, ?>) poolField.get(fixture.executor)).close();

            org.junit.jupiter.api.Assertions.assertAll(() -> fixture.executor.prepareStatement(query, 3L),
                    () -> fixture.executor.prepareStatement("SELECT * FROM items"),
                    () -> assertEquals(List.of(1L, 2L, 3L), fixture.values.stream().filter(v -> v.length == 1).map(v -> v[0]).toList()),
                    () -> verify(fixture.session, times(2)).prepare(query));
        }
    }

    // ---- 2026-10-27 sliceK ----

    // close() closed the session, the cluster and the statement pool in nested finally blocks, so a cluster close failure replaced a
    // session close failure, which was lost. The first failure is thrown with the later ones suppressed; every resource is still closed.
    @Test
    void sliceK_closeThrowsTheFirstFailureWithLaterOnesSuppressed() throws Exception {
        final CassandraTestFixture fixture = new CassandraTestFixture();
        final IllegalStateException sessionFailure = new IllegalStateException("session close");
        final IllegalStateException clusterFailure = new IllegalStateException("cluster close");
        doThrow(sessionFailure).when(fixture.session).close();
        doThrow(clusterFailure).when(fixture.cluster).close();

        final IllegalStateException thrown = assertThrows(IllegalStateException.class, fixture::close);

        org.junit.jupiter.api.Assertions.assertAll(() -> assertSame(sessionFailure, thrown),
                () -> assertArrayEquals(new Throwable[] { clusterFailure }, thrown.getSuppressed()), () -> verify(fixture.cluster).close(),
                () -> assertTrue(statementPool(fixture.executor).isClosed()));
    }

    /** Pins: a single close failure (the cluster's, or the session's) is thrown as is, and the statement pool is still closed. */
    @Test
    void sliceK_aSingleCloseFailureIsThrownAsIsAfterThePoolIsClosed() throws Exception {
        final CassandraTestFixture clusterFails = new CassandraTestFixture();
        final IllegalStateException clusterFailure = new IllegalStateException("cluster close");
        doThrow(clusterFailure).when(clusterFails.cluster).close();
        final CassandraTestFixture sessionFails = new CassandraTestFixture();
        final OutOfMemoryError sessionError = new OutOfMemoryError("session close");
        doThrow(sessionError).when(sessionFails.session).close();

        org.junit.jupiter.api.Assertions.assertAll(() -> {
            assertSame(clusterFailure, assertThrows(IllegalStateException.class, clusterFails::close));
            assertEquals(0, clusterFailure.getSuppressed().length);
            assertTrue(statementPool(clusterFails.executor).isClosed());
        }, () -> {
            assertSame(sessionError, assertThrows(OutOfMemoryError.class, sessionFails::close));
            verify(sessionFails.cluster).close();
            assertTrue(statementPool(sessionFails.executor).isClosed());
        });
    }

    private static com.landawn.abacus.pool.KeyedObjectPool<?, ?> statementPool(final CassandraExecutor executor) throws Exception {
        final var poolField = CassandraExecutor.class.getDeclaredField("preparedStatementPool");
        poolField.setAccessible(true);
        return (com.landawn.abacus.pool.KeyedObjectPool<?, ?>) poolField.get(executor);
    }

    // ---- 2026-10-27 r2K ----

    // Branch coverage of addCloseFailure's identity check: the same exception instance thrown by the session and the cluster is thrown
    // once, without itself as a suppressed exception (Throwable.addSuppressed(this) would throw IllegalArgumentException out of
    // close() before the statement pool is closed).
    @Test
    void r2K_theSameFailureFromTheSessionAndTheClusterIsThrownOnce() throws Exception {
        final CassandraTestFixture fixture = new CassandraTestFixture();
        final IllegalStateException failure = new IllegalStateException("close");
        doThrow(failure).when(fixture.session).close();
        doThrow(failure).when(fixture.cluster).close();

        final IllegalStateException thrown = assertThrows(IllegalStateException.class, fixture::close);

        org.junit.jupiter.api.Assertions.assertAll(() -> assertSame(failure, thrown), () -> assertEquals(0, thrown.getSuppressed().length),
                () -> verify(fixture.cluster).close(), () -> assertTrue(statementPool(fixture.executor).isClosed()));
    }

    // ---- 2026-10-27 fixD ----

    // fixD (finding F): close() has a third failure branch, the prepared-statement pool's close, which the tests above never reach
    // (the pool's own close does not fail). A session or cluster close failure is thrown with the pool's added to it as suppressed,
    // after the earlier ones; a pool close failure alone is thrown as is, after the session and the cluster are closed. The pool is a
    // private final field created by the constructor (no seam), so it is replaced by reflection with a mock whose close() throws.
    @Test
    void fixD_aStatementPoolCloseFailureIsSuppressedByAnEarlierOneOrThrownAlone() throws Exception {
        final CassandraTestFixture sessionAndPool = new CassandraTestFixture();
        final IllegalStateException sessionFailure = new IllegalStateException("session close");
        final IllegalStateException poolAfterSession = new IllegalStateException("pool close 1");
        doThrow(sessionFailure).when(sessionAndPool.session).close();
        failingStatementPool(sessionAndPool.executor, poolAfterSession);

        final CassandraTestFixture clusterAndPool = new CassandraTestFixture();
        final IllegalStateException clusterFailure = new IllegalStateException("cluster close");
        final IllegalStateException poolAfterCluster = new IllegalStateException("pool close 2");
        doThrow(clusterFailure).when(clusterAndPool.cluster).close();
        failingStatementPool(clusterAndPool.executor, poolAfterCluster);

        final CassandraTestFixture allThree = new CassandraTestFixture();
        final IllegalStateException firstFailure = new IllegalStateException("session close 3");
        final IllegalStateException secondFailure = new IllegalStateException("cluster close 3");
        final IllegalStateException thirdFailure = new IllegalStateException("pool close 3");
        doThrow(firstFailure).when(allThree.session).close();
        doThrow(secondFailure).when(allThree.cluster).close();
        failingStatementPool(allThree.executor, thirdFailure);

        final CassandraTestFixture poolOnly = new CassandraTestFixture();
        final IllegalStateException poolFailure = new IllegalStateException("pool close 4");
        failingStatementPool(poolOnly.executor, poolFailure);

        org.junit.jupiter.api.Assertions.assertAll(() -> {
            final IllegalStateException thrown = assertThrows(IllegalStateException.class, sessionAndPool::close);
            assertSame(sessionFailure, thrown);
            assertArrayEquals(new Throwable[] { poolAfterSession }, thrown.getSuppressed());
            verify(sessionAndPool.cluster).close();
        }, () -> {
            final IllegalStateException thrown = assertThrows(IllegalStateException.class, clusterAndPool::close);
            assertSame(clusterFailure, thrown);
            assertArrayEquals(new Throwable[] { poolAfterCluster }, thrown.getSuppressed());
            verify(clusterAndPool.session).close();
        }, () -> {
            final IllegalStateException thrown = assertThrows(IllegalStateException.class, allThree::close);
            assertSame(firstFailure, thrown);
            assertArrayEquals(new Throwable[] { secondFailure, thirdFailure }, thrown.getSuppressed());
        }, () -> {
            assertSame(poolFailure, assertThrows(IllegalStateException.class, poolOnly::close));
            assertEquals(0, poolFailure.getSuppressed().length);
            verify(poolOnly.session).close();
            verify(poolOnly.cluster).close();
        });
    }

    // Closes the executor's real statement pool and puts a mock in its place whose close() throws failure.
    private static void failingStatementPool(final CassandraExecutor executor, final RuntimeException failure) throws Exception {
        statementPool(executor).close();
        final com.landawn.abacus.pool.KeyedObjectPool<?, ?> pool = mock(com.landawn.abacus.pool.KeyedObjectPool.class);
        doThrow(failure).when(pool).close();
        final var poolField = CassandraExecutor.class.getDeclaredField("preparedStatementPool");
        poolField.setAccessible(true);
        poolField.set(executor, pool);
    }
}
