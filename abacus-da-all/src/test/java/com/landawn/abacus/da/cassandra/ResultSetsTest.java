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

    // ---- 2026-10-19 sliceG ----

    /**
     * all() is pre-sized from the current page's remaining() (perf change): it must still drain every later page,
     * return a mutable list (also when empty), and tolerate a custom page that reports a negative remaining count.
     */
    @Test
    void allDrainsEveryPageIntoAMutableListAndToleratesANegativeRemainingCount_sliceG() {
        final AsyncResultSet first = mock(AsyncResultSet.class);
        final AsyncResultSet second = mock(AsyncResultSet.class);
        final AsyncResultSet empty = mock(AsyncResultSet.class);
        final Row r1 = mock(Row.class);
        final Row r2 = mock(Row.class);
        final Row r3 = mock(Row.class);
        when(first.currentPage()).thenReturn(List.of(r1));
        when(first.remaining()).thenReturn(-1);
        when(first.hasMorePages()).thenReturn(true);
        when(first.fetchNextPage()).thenReturn(CompletableFuture.completedFuture(second));
        when(second.currentPage()).thenReturn(List.of(r2, r3));
        when(empty.currentPage()).thenReturn(List.of());

        final List<Row> rows = ResultSets.wrap(first).all();
        final List<Row> none = ResultSets.wrap(empty).all();

        org.junit.jupiter.api.Assertions.assertAll(() -> org.junit.jupiter.api.Assertions.assertEquals(List.of(r1, r2, r3), rows),
                () -> assertTrue(rows.add(r1)), () -> assertTrue(none.isEmpty()), () -> assertTrue(none.add(r1)));
    }

    // ---- 2026-10-24 sliceG ----

    /** A one-row first page whose next page's future fails with {@code failure}. */
    private static AsyncResultSet sliceGPageThenFailure(final Throwable failure) {
        final AsyncResultSet first = mock(AsyncResultSet.class);
        when(first.currentPage()).thenReturn(List.of(mock(Row.class)));
        when(first.hasMorePages()).thenReturn(true);
        final CompletableFuture<AsyncResultSet> failed = new CompletableFuture<>();
        failed.completeExceptionally(failure);
        when(first.fetchNextPage()).thenReturn(failed);
        return first;
    }

    /**
     * A later page whose future fails with an Error must surface that Error itself, as the driver's synchronous
     * MultiPageResultSet does (and as a RuntimeException cause already did); it used to be wrapped in a plain
     * RuntimeException. A checked cause is still converted to a runtime exception.
     */
    @Test
    void laterPageErrorIsRethrownAsIsLikeTheDriversSyncResultSet_sliceG() {
        final AssertionError error = new AssertionError("page failed");
        final OutOfMemoryError oom = new OutOfMemoryError("page failed");
        final IllegalStateException runtime = new IllegalStateException("page failed");
        final java.io.IOException checked = new java.io.IOException("page failed");

        org.junit.jupiter.api.Assertions
                .assertAll(
                        () -> assertSame(error,
                                org.junit.jupiter.api.Assertions.assertThrows(AssertionError.class, () -> ResultSets.wrap(sliceGPageThenFailure(error)).all())),
                        () -> {
                            final java.util.Iterator<Row> iter = ResultSets.wrap(sliceGPageThenFailure(oom)).iterator();
                            iter.next(); // the first page's row is still delivered
                            assertSame(oom, org.junit.jupiter.api.Assertions.assertThrows(OutOfMemoryError.class, iter::hasNext));
                        },
                        () -> assertSame(runtime,
                                org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                                        () -> ResultSets.wrap(sliceGPageThenFailure(runtime)).all())),
                        () -> assertSame(checked, org.junit.jupiter.api.Assertions.assertThrows(com.landawn.abacus.exception.UncheckedIOException.class,
                                () -> ResultSets.wrap(sliceGPageThenFailure(checked)).all()).getCause()));
    }
}
