package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;

class ResultSetsDeepReview2026DriverThreadTest {

    @Test
    void fetchingAnotherPageFailsBeforeBlockingADriverThread() throws InterruptedException {
        final AsyncResultSet first = mock(AsyncResultSet.class);
        final AsyncResultSet second = mock(AsyncResultSet.class);
        when(first.currentPage()).thenReturn(List.of());
        when(first.hasMorePages()).thenReturn(true);
        when(first.fetchNextPage()).thenReturn(CompletableFuture.completedFuture(second));
        when(second.currentPage()).thenReturn(List.of());

        final ResultSet result = ResultSets.wrap(first);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread driverThread = new BlockingOperation.SafeThreadFactory().newThread(() -> {
            try {
                result.iterator().hasNext();
            } catch (final Throwable e) {
                failure.set(e);
            }
        });

        driverThread.start();
        driverThread.join(10_000);

        assertFalse(driverThread.isAlive());
        assertInstanceOf(IllegalStateException.class, failure.get());
        verify(first, never()).fetchNextPage();
    }
}
