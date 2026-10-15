package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.mockito.Answers;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.da.TestBase;

public class CassandraBatchRegressionTest extends TestBase {

    @Test
    void everyBatchFactoryPreservesStatementOrderTypeAndSettings() throws Exception {
        final List<Item> items = List.of(new Item(1L, 10L), new Item(2L, 20L));
        final List<Map<String, Object>> properties = List.of(Map.of("id", 1L, "value", 10L), Map.of("id", 2L, "value", 20L));
        final List<Function<CassandraExecutor, BatchStatement>> factories = List.of(executor -> executor.prepareBatchInsertStatement(items, BatchType.UNLOGGED),
                executor -> executor.prepareBatchInsertStatement(Item.class, properties, BatchType.UNLOGGED),
                executor -> executor.prepareBatchUpdateStatement(items, List.of("value"), BatchType.UNLOGGED),
                executor -> executor.prepareBatchUpdateStatement(Item.class, properties, BatchType.UNLOGGED),
                executor -> (BatchStatement) executor.prepareBatchUpdateStatement("UPDATE items SET value = ? WHERE id = ?",
                        List.of(new Object[] { 10L, 1L }, new Object[] { 20L, 2L }), BatchType.UNLOGGED));

        for (final Function<CassandraExecutor, BatchStatement> factory : factories) {
            final List<BoundStatement> boundStatements = new ArrayList<>();
            final CassandraExecutor executor = executor(boundStatements);
            try {
                final BatchStatement batch = factory.apply(executor);
                final List<Object> statements = new ArrayList<>();
                batch.forEach(statements::add);
                assertEquals(boundStatements, statements);
                assertEquals(2, batch.size());
                assertEquals(BatchType.UNLOGGED, batch.getBatchType());
                assertEquals(ConsistencyLevel.LOCAL_QUORUM, batch.getConsistencyLevel());
                assertEquals(ConsistencyLevel.LOCAL_SERIAL, batch.getSerialConsistencyLevel());
                assertEquals(128, batch.getPageSize());
                // Driver 4.19.3's getTimeout() returns null even though setTimeout stores the value correctly.
                final var timeout = batch.getClass().getDeclaredField("timeout");
                timeout.setAccessible(true);
                assertEquals(Duration.ofSeconds(3), timeout.get(batch));
                assertTrue(batch.isTracing());
            } finally {
                executor.close();
            }
        }
    }

    @Test
    void emptyBatchIsRejectedAndSingleBatchDefaultsToLogged() {
        final CassandraExecutor executor = executor(new ArrayList<>());
        try {
            assertThrows(IllegalArgumentException.class,
                    () -> executor.prepareBatchUpdateStatement("UPDATE items SET value = ? WHERE id = ?", List.of(), null));
            final BatchStatement batch = (BatchStatement) executor.prepareBatchUpdateStatement("UPDATE items SET value = ? WHERE id = ?",
                    java.util.Collections.singletonList(new Object[] { 10L, 1L }), null);
            assertEquals(1, batch.size());
            assertEquals(BatchType.LOGGED, batch.getBatchType());
        } finally {
            executor.close();
        }
    }

    @Test
    void batchBuilderKeepsTheProtocolStatementLimit() {
        final BoundStatement shared = mock(BoundStatement.class, Answers.RETURNS_SELF);
        final CassandraExecutor executor = executor(shared);
        try {
            final List<Object[]> parameters = new ArrayList<>(65_536);
            for (int i = 0; i < 65_535; i++) {
                parameters.add(new Object[] { 10L, 1L });
            }

            assertEquals(65_535, ((BatchStatement) executor.prepareBatchUpdateStatement("UPDATE items SET value = ? WHERE id = ?", parameters, null)).size());

            parameters.add(new Object[] { 10L, 1L });
            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> executor.prepareBatchUpdateStatement("UPDATE items SET value = ? WHERE id = ?", parameters, null));
            assertTrue(failure.getMessage().contains("65535"), failure.getMessage());
        } finally {
            executor.close();
        }
    }

    private static CassandraExecutor executor(final BoundStatement shared) {
        // Bind every statement to one mock: a batch may repeat a statement, and 65,536 mocks would dominate the test.
        return executor(invocation -> shared);
    }

    private static CassandraExecutor executor(final Collection<BoundStatement> boundStatements) {
        return executor(invocation -> {
            final BoundStatement statement = mock(BoundStatement.class, Answers.RETURNS_SELF);
            boundStatements.add(statement);
            return statement;
        });
    }

    private static CassandraExecutor executor(final org.mockito.stubbing.Answer<BoundStatement> bind) {
        final CqlSession session = mock(CqlSession.class);
        final DriverContext context = mock(DriverContext.class);
        final PreparedStatement prepared = mock(PreparedStatement.class);
        final ColumnDefinitions columns = mock(ColumnDefinitions.class);
        final ColumnDefinition column = mock(ColumnDefinition.class);
        when(session.getContext()).thenReturn(context);
        when(context.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT);
        when(session.prepare(anyString())).thenReturn(prepared);
        when(prepared.getVariableDefinitions()).thenReturn(columns);
        when(columns.size()).thenReturn(2);
        when(columns.get(anyInt())).thenReturn(column);
        when(column.getType()).thenReturn(DataTypes.BIGINT);
        when(prepared.bind(any(Object[].class))).thenAnswer(bind);
        return new CassandraExecutor(session,
                new CassandraExecutor.StatementSettings().consistency(ConsistencyLevel.LOCAL_QUORUM)
                        .serialConsistency(ConsistencyLevel.LOCAL_SERIAL)
                        .fetchSize(128)
                        .timeout(Duration.ofSeconds(3))
                        .traceQuery(true));
    }

    public static final class Item {
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
