package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.cql.SinglePageResultSet;

/**
 * {@link CassandraExecutorBase#list(Class, String, Object...)} documents a mutable result list. For a raw-row target
 * ({@code Row.class} or a supertype such as {@code Object.class}) the executors return the driver's {@code ResultSet.all()} list,
 * which both driver 3 and driver 4 return as the immutable {@code Collections.emptyList()} when there is no row.
 */
class CassandraExecutorBaseDeepReview2026ListMutabilityTest extends CassandraExecutorBaseTestSupport {

    // ---- 2026-10-27 sliceF ----

    // The real driver 4 result set (SinglePageResultSet over an empty page) through the v4 executor: list(Row.class, ...) and
    // list(Object.class, ...) returned Collections.emptyList(), whose add() throws UnsupportedOperationException.
    @Test
    void test_sliceF27_emptyRawRowListIsMutable() {
        final CqlSession session = mock(CqlSession.class);
        final DriverContext context = mock(DriverContext.class);
        when(session.getContext()).thenReturn(context);
        when(context.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT);
        final ColumnDefinitions noVariables = mock(ColumnDefinitions.class);
        when(noVariables.size()).thenReturn(0);
        final PreparedStatement prepared = mock(PreparedStatement.class);
        when(prepared.getVariableDefinitions()).thenReturn(noVariables);
        when(prepared.bind(any(Object[].class))).thenReturn(mock(BoundStatement.class));
        when(prepared.bind()).thenReturn(mock(BoundStatement.class));
        when(session.prepare(anyString())).thenReturn(prepared);
        final AsyncResultSet emptyPage = mock(AsyncResultSet.class);
        when(emptyPage.currentPage()).thenReturn(Collections.emptyList());
        when(emptyPage.hasMorePages()).thenReturn(false);
        when(session.execute(any(Statement.class))).thenAnswer(invocation -> new SinglePageResultSet(emptyPage));

        final CassandraExecutor executor = new CassandraExecutor(session);
        final List<Row> rows = executor.list(Row.class, "SELECT * FROM t");
        final List<Object> objects = executor.list(Object.class, "SELECT * FROM t");

        assertAll(() -> assertTrue(rows.isEmpty()), () -> assertDoesNotThrow(() -> rows.add(mock(Row.class))), () -> assertTrue(objects.isEmpty()),
                () -> assertDoesNotThrow(() -> objects.add("x")));
    }

    // The base guarantees it for any subclass hook: an empty list from toList(Class, RS) is replaced by a mutable one, a non-empty
    // one is returned as is.
    @Test
    void test_sliceF27_baseListReturnsMutableEmptyList() {
        final List<Object> nonEmpty = new ArrayList<>(List.of("a"));
        final TestCassandraExecutor emptyRows = new TestCassandraExecutor() {
            @Override
            @SuppressWarnings("unchecked")
            protected <T> List<T> toList(final Class<T> targetClass, final TestResultSet resultSet) {
                return (List<T>) Collections.emptyList();
            }
        };
        final TestCassandraExecutor someRows = new TestCassandraExecutor() {
            @Override
            @SuppressWarnings("unchecked")
            protected <T> List<T> toList(final Class<T> targetClass, final TestResultSet resultSet) {
                return (List<T>) nonEmpty;
            }
        };

        final List<String> empty = emptyRows.list(String.class, "SELECT name FROM t");

        assertAll(() -> assertDoesNotThrow(() -> empty.add("x")), () -> assertEquals(List.of("x"), empty),
                () -> assertSame(nonEmpty, someRows.list(Object.class, "SELECT name FROM t")));
    }
}
