package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

class ResultSetsTest {

    @Test
    void appliedStatusTracksTheCurrentPageWithoutFetchingAhead() {
        final AsyncResultSet first = mock(AsyncResultSet.class);
        final AsyncResultSet second = mock(AsyncResultSet.class);
        final Row row = mock(Row.class);
        when(first.currentPage()).thenReturn(List.of());
        when(first.wasApplied()).thenReturn(true);
        when(first.hasMorePages()).thenReturn(true);
        when(first.fetchNextPage()).thenReturn(CompletableFuture.completedFuture(second));
        when(second.currentPage()).thenReturn(List.of(row));
        when(second.wasApplied()).thenReturn(false);
        final var result = ResultSets.wrap(first);
        assertTrue(result.wasApplied());
        verify(first, never()).fetchNextPage();
        assertSame(row, result.iterator().next());
        assertFalse(result.wasApplied());
        assertFalse(result.iterator().hasNext());
        verify(first).fetchNextPage();
    }
}
