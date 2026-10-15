package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.landawn.abacus.da.TestBase;

public class CassandraMemoryRegressionTest extends TestBase {

    @Test
    void advancedWrapperDoesNotRetainFirstPage() throws Exception {
        final AsyncResultSet firstPage = mock(AsyncResultSet.class);
        final AsyncResultSet secondPage = mock(AsyncResultSet.class);
        final Row firstRow = mock(Row.class);
        final Row secondRow = mock(Row.class);
        final ExecutionInfo firstInfo = mock(ExecutionInfo.class);
        final ExecutionInfo secondInfo = mock(ExecutionInfo.class);
        when(firstPage.currentPage()).thenReturn(List.of(firstRow));
        when(firstPage.hasMorePages()).thenReturn(true);
        when(firstPage.fetchNextPage()).thenReturn(CompletableFuture.completedFuture(secondPage));
        when(firstPage.getExecutionInfo()).thenReturn(firstInfo);
        when(secondPage.currentPage()).thenReturn(List.of(secondRow));
        when(secondPage.getExecutionInfo()).thenReturn(secondInfo);

        final ResultSet resultSet = ResultSets.wrap(firstPage);
        final Iterator<Row> rows = resultSet.iterator();
        assertSame(firstRow, rows.next());
        assertTrue(rows.hasNext());
        assertSame(secondRow, rows.next());
        assertFalse(rows.hasNext());
        assertSame(rows, resultSet.iterator());
        assertEquals(List.of(firstInfo, secondInfo), resultSet.getExecutionInfos());

        // Inspect strong references deterministically instead of relying on a GC request being honored.
        // The old anonymous wrapper kept its first page in a synthetic captured-argument field.
        for (final Field field : resultSet.getClass().getDeclaredFields()) {
            if (!Modifier.isStatic(field.getModifiers())) {
                field.setAccessible(true);
                assertNotSame(firstPage, field.get(resultSet), "Consumed first page retained by " + field.getName());
            }
        }
    }

    @Test
    void v4ListConvertsRowsBeforeFetchingTheNextPage() {
        assertEquals(List.of("first", "second"), CassandraExecutor.toList(v4Rows(), String.class));
    }

    @Test
    void v4DatasetConvertsRowsBeforeFetchingTheNextPage() {
        assertEquals(List.of("first", "second"), CassandraExecutor.extractData(v4Rows()).getColumn("value"));
    }

    @Test
    void v3ListConvertsRowsBeforeFetchingTheNextPage() {
        assertEquals(List.of("first", "second"), com.landawn.abacus.da.cassandra.v3.CassandraExecutor.toList(v3Rows(), String.class));
    }

    @Test
    void v3DatasetConvertsRowsBeforeFetchingTheNextPage() {
        assertEquals(List.of("first", "second"), com.landawn.abacus.da.cassandra.v3.CassandraExecutor.extractData(v3Rows()).getColumn("value"));
    }

    @Test
    void v4ConversionFailureLeavesSubsequentPagesUnread() {
        for (final boolean dataset : new boolean[] { false, true }) {
            final IllegalStateException failure = new IllegalStateException("decode failure");
            final ResultSet resultSet = v4Rows(failure);
            assertSame(failure, assertThrows(IllegalStateException.class, () -> {
                if (dataset) {
                    CassandraExecutor.extractData(resultSet);
                } else {
                    CassandraExecutor.toList(resultSet, String.class);
                }
            }));
            assertEquals("second", resultSet.iterator().next().getObject(0));
        }
    }

    @Test
    void v3ConversionFailureLeavesSubsequentPagesUnread() {
        for (final boolean dataset : new boolean[] { false, true }) {
            final IllegalStateException failure = new IllegalStateException("decode failure");
            final com.datastax.driver.core.ResultSet resultSet = v3Rows(failure);
            assertSame(failure, assertThrows(IllegalStateException.class, () -> {
                if (dataset) {
                    com.landawn.abacus.da.cassandra.v3.CassandraExecutor.extractData(resultSet);
                } else {
                    com.landawn.abacus.da.cassandra.v3.CassandraExecutor.toList(resultSet, String.class);
                }
            }));
            assertEquals("second", resultSet.iterator().next().getObject(0));
        }
    }

    @Test
    void negativeRemainingCountsOnlyDisableThePresizingHint() {
        // getAvailableWithoutFetching() is only used to presize the result; a faulty ResultSet reporting -1 must not
        // fail the conversion with ArrayList's IllegalArgumentException.
        final ResultSet v4List = v4Rows();
        final ResultSet v4Dataset = v4Rows();
        final com.datastax.driver.core.ResultSet v3List = v3Rows();
        final com.datastax.driver.core.ResultSet v3Dataset = v3Rows();
        when(v4List.getAvailableWithoutFetching()).thenReturn(-1);
        when(v4Dataset.getAvailableWithoutFetching()).thenReturn(-1);
        when(v3List.getAvailableWithoutFetching()).thenReturn(-1);
        when(v3Dataset.getAvailableWithoutFetching()).thenReturn(-1);

        assertEquals(List.of("first", "second"), CassandraExecutor.toList(v4List, String.class));
        assertEquals(List.of("first", "second"), CassandraExecutor.extractData(v4Dataset, String.class).getColumn("value"));
        assertEquals(List.of("first", "second"), com.landawn.abacus.da.cassandra.v3.CassandraExecutor.toList(v3List, String.class));
        assertEquals(List.of("first", "second"), com.landawn.abacus.da.cassandra.v3.CassandraExecutor.extractData(v3Dataset, String.class).getColumn("value"));
    }

    private static ResultSet v4Rows() {
        return v4Rows(null);
    }

    private static ResultSet v4Rows(final RuntimeException failure) {
        final ResultSet resultSet = mock(ResultSet.class);
        final ColumnDefinitions columns = mock(ColumnDefinitions.class);
        final ColumnDefinition column = mock(ColumnDefinition.class);
        when(resultSet.getColumnDefinitions()).thenReturn(columns);
        when(columns.size()).thenReturn(1);
        when(columns.get(0)).thenReturn(column);
        when(column.getName()).thenReturn(CqlIdentifier.fromInternal("value"));
        final AtomicInteger decoded = new AtomicInteger();
        final List<Row> rows = new ArrayList<>();
        for (final String value : List.of("first", "second")) {
            final Row row = mock(Row.class);
            when(row.getObject(0)).thenAnswer(invocation -> {
                decoded.incrementAndGet();
                if (failure != null && "first".equals(value)) {
                    throw failure;
                }
                return value;
            });
            rows.add(row);
        }
        final Iterator<Row> cursor = pagedRows(rows, decoded);
        when(resultSet.iterator()).thenReturn(cursor);
        when(resultSet.all()).thenAnswer(invocation -> drain(cursor));
        when(resultSet.getAvailableWithoutFetching()).thenReturn(1);
        return resultSet;
    }

    private static com.datastax.driver.core.ResultSet v3Rows() {
        return v3Rows(null);
    }

    private static com.datastax.driver.core.ResultSet v3Rows(final RuntimeException failure) {
        final com.datastax.driver.core.ResultSet resultSet = mock(com.datastax.driver.core.ResultSet.class);
        final com.datastax.driver.core.ColumnDefinitions columns = mock(com.datastax.driver.core.ColumnDefinitions.class);
        when(resultSet.getColumnDefinitions()).thenReturn(columns);
        when(columns.size()).thenReturn(1);
        when(columns.getName(0)).thenReturn("value");
        final AtomicInteger decoded = new AtomicInteger();
        final List<com.datastax.driver.core.Row> rows = new ArrayList<>();
        for (final String value : List.of("first", "second")) {
            final com.datastax.driver.core.Row row = mock(com.datastax.driver.core.Row.class);
            when(row.getObject(0)).thenAnswer(invocation -> {
                decoded.incrementAndGet();
                if (failure != null && "first".equals(value)) {
                    throw failure;
                }
                return value;
            });
            rows.add(row);
        }
        final Iterator<com.datastax.driver.core.Row> cursor = pagedRows(rows, decoded);
        when(resultSet.iterator()).thenReturn(cursor);
        when(resultSet.all()).thenAnswer(invocation -> drain(cursor));
        when(resultSet.getAvailableWithoutFetching()).thenReturn(1);
        return resultSet;
    }

    private static <T> Iterator<T> pagedRows(final List<T> rows, final AtomicInteger decoded) {
        return new Iterator<>() {
            private int position;

            @Override
            public boolean hasNext() {
                return position < rows.size();
            }

            @Override
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                assertEquals(position, decoded.get(), "Decode each page before retrieving another, without buffering all driver rows");
                return rows.get(position++);
            }
        };
    }

    private static <T> List<T> drain(final Iterator<T> rows) {
        final List<T> result = new ArrayList<>();
        rows.forEachRemaining(result::add);
        return result;
    }
}
