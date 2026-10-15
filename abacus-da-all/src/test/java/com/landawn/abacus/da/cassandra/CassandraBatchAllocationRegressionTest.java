package com.landawn.abacus.da.cassandra;

import static com.landawn.abacus.da.AllocationAssertions.assertLinearAllocation;
import static com.landawn.abacus.da.AllocationAssertions.assumeAllocationCountersAvailable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

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
import com.landawn.abacus.da.cassandra.CassandraBatchRegressionTest.Item;

public class CassandraBatchAllocationRegressionTest {
    @Test
    void allBatchFactoriesAvoidQuadraticStatementCopies() {
        assumeAllocationCountersAvailable();
        final CassandraExecutor executor = executor();
        final Item item = new Item(1L, 10L);
        final Map<String, Object> properties = Map.of("id", 1L, "value", 10L);
        try {
            assertAll(() -> check("entity inserts", size -> {
                final List<Item> input = Collections.nCopies(size, item);
                return () -> executor.prepareBatchInsertStatement(input, BatchType.UNLOGGED);
            }), () -> check("map inserts", size -> {
                final List<Map<String, Object>> input = Collections.nCopies(size, properties);
                return () -> executor.prepareBatchInsertStatement(Item.class, input, BatchType.UNLOGGED);
            }), () -> check("entity updates", size -> {
                final List<Item> input = Collections.nCopies(size, item);
                return () -> executor.prepareBatchUpdateStatement(input, List.of("value"), BatchType.UNLOGGED);
            }), () -> check("map updates", size -> {
                final List<Map<String, Object>> input = Collections.nCopies(size, properties);
                return () -> executor.prepareBatchUpdateStatement(Item.class, input, BatchType.UNLOGGED);
            }), () -> check("parameter updates", size -> {
                final List<Object[]> input = Collections.nCopies(size, new Object[] { 10L, 1L });
                return () -> (BatchStatement) executor.prepareBatchUpdateStatement("UPDATE items SET value = ? WHERE id = ?", input, BatchType.UNLOGGED);
            }));
        } finally {
            executor.close();
        }
    }

    private static void check(final String name, final IntFunction<Supplier<BatchStatement>> factory) {
        final Supplier<BatchStatement> small = factory.apply(512);
        final Supplier<BatchStatement> large = factory.apply(2048);
        assertEquals(512, small.get().size());
        assertEquals(2048, large.get().size());
        assertLinearAllocation(name, small, large);
    }

    private static CassandraExecutor executor() {
        // Non-recording stubs isolate executor/driver allocation from a mocking framework's invocation history.
        // Reuse one immutable bound statement; the real driver batch must still retain every occurrence.
        final BoundStatement bound = stub(BoundStatement.class, (proxy, method, args) -> {
            throw new AssertionError("Unexpected bound statement method: " + method.getName());
        });
        final ColumnDefinition column = stub(ColumnDefinition.class, (proxy, method, args) -> switch (method.getName()) {
            case "getType" -> DataTypes.BIGINT;
            default -> throw new AssertionError("Unexpected column method: " + method.getName());
        });
        final ColumnDefinitions columns = stub(ColumnDefinitions.class, (proxy, method, args) -> switch (method.getName()) {
            case "size" -> 2;
            case "get" -> column;
            default -> throw new AssertionError("Unexpected columns method: " + method.getName());
        });
        final PreparedStatement prepared = stub(PreparedStatement.class, (proxy, method, args) -> switch (method.getName()) {
            case "getVariableDefinitions" -> columns;
            case "bind" -> bound;
            default -> throw new AssertionError("Unexpected prepared statement method: " + method.getName());
        });
        final DriverContext context = stub(DriverContext.class, (proxy, method, args) -> switch (method.getName()) {
            case "getCodecRegistry" -> CodecRegistry.DEFAULT;
            default -> throw new AssertionError("Unexpected context method: " + method.getName());
        });
        final CqlSession session = stub(CqlSession.class, (proxy, method, args) -> switch (method.getName()) {
            case "getContext" -> context;
            case "prepare" -> prepared;
            case "isClosed" -> false;
            case "close" -> null;
            default -> throw new AssertionError("Unexpected session method: " + method.getName());
        });
        return new CassandraExecutor(session);
    }

    private static <T> T stub(final Class<T> type, final InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] { type }, handler));
    }
}
