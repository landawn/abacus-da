package com.landawn.abacus.da.cassandra.v3;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.landawn.abacus.annotation.Id;

class CassandraBatchTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("factories")
    void batchFactoriesPreserveBoundValuesOrderTypeAndSettings(final String scenario, final Function<CassandraExecutor, BatchStatement> factory,
            final Object[][] expected) throws Exception {
        final CassandraExecutor.StatementSettings settings = new CassandraExecutor.StatementSettings().consistency(ConsistencyLevel.LOCAL_QUORUM)
                .serialConsistency(ConsistencyLevel.LOCAL_SERIAL)
                .retryPolicy(DefaultRetryPolicy.INSTANCE)
                .readTimeoutMillis(3000)
                .fetchSize(128)
                .traceQuery(true);
        try (CassandraTestFixture fixture = new CassandraTestFixture(settings)) {
            fixture.columns(DataType.bigint(), "id", "value");
            final BatchStatement batch = factory.apply(fixture.executor);
            assertEquals(BatchStatement.Type.UNLOGGED, batchType(batch));
            assertEquals(fixture.statements, new ArrayList<>(batch.getStatements()));
            assertEquals(2, batch.size());
            for (int i = 0; i < expected.length; i++) {
                assertArrayEquals(expected[i], fixture.values.get(i));
            }
            assertEquals(ConsistencyLevel.LOCAL_QUORUM, batch.getConsistencyLevel());
            assertEquals(ConsistencyLevel.LOCAL_SERIAL, batch.getSerialConsistencyLevel());
            assertSame(DefaultRetryPolicy.INSTANCE, batch.getRetryPolicy());
            assertEquals(3000, batch.getReadTimeoutMillis());
            assertEquals(128, batch.getFetchSize());
            assertTrue(batch.isTracing());
        }
    }

    static Stream<Arguments> factories() {
        final List<Item> items = List.of(new Item(1, 10), new Item(2, 20));
        final List<Map<String, Object>> props = List.of(properties(1, 10), properties(2, 20));
        return Stream.of(
                Arguments.of("insert entities",
                        (Function<CassandraExecutor, BatchStatement>) e -> e.prepareBatchInsertStatement(items, BatchStatement.Type.UNLOGGED),
                        new Object[][] { { 1L, 10L }, { 2L, 20L } }),
                Arguments.of("insert properties",
                        (Function<CassandraExecutor, BatchStatement>) e -> e.prepareBatchInsertStatement(Item.class, props, BatchStatement.Type.UNLOGGED),
                        new Object[][] { { 1L, 10L }, { 2L, 20L } }),
                Arguments.of("update entities",
                        (Function<CassandraExecutor, BatchStatement>) e -> e.prepareBatchUpdateStatement(items, List.of("value"), BatchStatement.Type.UNLOGGED),
                        new Object[][] { { 10L, 1L }, { 20L, 2L } }),
                Arguments.of("update properties",
                        (Function<CassandraExecutor, BatchStatement>) e -> e.prepareBatchUpdateStatement(Item.class, props, BatchStatement.Type.UNLOGGED),
                        new Object[][] { { 10L, 1L }, { 20L, 2L } }),
                Arguments.of("update positional arrays",
                        (Function<CassandraExecutor, BatchStatement>) e -> e.prepareBatchUpdateStatement("UPDATE items SET value = ? WHERE id = ?",
                                List.of(new Object[] { 10L, 1L }, new Object[] { 20L, 2L }), BatchStatement.Type.UNLOGGED),
                        new Object[][] { { 10L, 1L }, { 20L, 2L } }));
    }

    @Test
    void defaultBatchIsLoggedAndRejectsInvalidElementsBeforePreparing() throws Exception {
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.columns(DataType.bigint(), "id", "value");
            assertEquals(BatchStatement.Type.LOGGED, batchType(fixture.executor.prepareBatchInsertStatement(List.of(new Item(1, 10)), null)));
            assertThrows(IllegalArgumentException.class, () -> fixture.executor.prepareBatchInsertStatement(List.of(), null));
            assertThrows(IllegalArgumentException.class, () -> fixture.executor.prepareBatchInsertStatement(Arrays.asList(new Item(1, 10), null), null));
            assertThrows(IllegalArgumentException.class, () -> fixture.executor.prepareBatchInsertStatement(Item.class, List.of(), null));
            assertThrows(IllegalArgumentException.class, () -> fixture.executor.prepareBatchUpdateStatement(List.of(new Item(1, 10)), List.of(), null));
            assertThrows(IllegalArgumentException.class, () -> fixture.executor.prepareBatchUpdateStatement(Item.class, List.of(), null));
            assertThrows(IllegalArgumentException.class, () -> fixture.executor.prepareBatchUpdateStatement("UPDATE items SET value=?", List.of(), null));
            assertEquals(1, fixture.values.size(), "Rejected batches must not prepare or bind partial work");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void configuredTracingCanBeEnabledAndDisabled(final boolean enabled) {
        try (CassandraTestFixture fixture = new CassandraTestFixture(new CassandraExecutor.StatementSettings().traceQuery(enabled))) {
            final Statement statement = new SimpleStatement("SELECT now() FROM system.local");
            statement.enableTracing();
            fixture.executor.configStatement(statement);
            assertEquals(enabled, statement.isTracing());
            assertThrows(IllegalArgumentException.class, () -> fixture.executor.configStatement(null));
        }
    }

    @Test
    void emptySettingsPreserveExistingStatementSettingsAndAcceptNull() {
        try (CassandraTestFixture fixture = new CassandraTestFixture(new CassandraExecutor.StatementSettings())) {
            final Statement statement = new SimpleStatement("SELECT now() FROM system.local");
            statement.setFetchSize(17);
            fixture.executor.configStatement(statement);
            assertEquals(17, statement.getFetchSize());
            assertFalse(statement.isTracing());
            fixture.executor.configStatement(null);
        }
    }

    private static Object batchType(final BatchStatement batch) throws Exception {
        // Driver 3 exposes no public type accessor; verify the type the real driver will encode.
        final var field = BatchStatement.class.getDeclaredField("batchType");
        field.setAccessible(true);
        return field.get(batch);
    }

    private static Map<String, Object> properties(final long id, final long value) {
        final Map<String, Object> result = new LinkedHashMap<>();
        result.put("id", id);
        result.put("value", value);
        return result;
    }

    public static class Item {
        @Id
        private long id;
        private long value;

        public Item() {
        }

        Item(final long id, final long value) {
            this.id = id;
            this.value = value;
        }

        public long getId() {
            return id;
        }

        public void setId(final long id) {
            this.id = id;
        }

        public long getValue() {
            return value;
        }

        public void setValue(final long value) {
            this.value = value;
        }
    }
}
