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
}
