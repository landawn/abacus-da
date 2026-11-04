/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.landawn.abacus.exception.UncheckedInterruptedException;

/**
 * Review 2026-11-03 (slice s10): pins the documented next-page failure rules of {@link ResultSets#wrap(AsyncResultSet)}.
 */
class ResultSetsReview20261103Test {

    private static ResultSet failingSecondPage(final CompletableFuture<AsyncResultSet> next) {
        final AsyncResultSet first = mock(AsyncResultSet.class);
        when(first.currentPage()).thenReturn(List.of());
        when(first.hasMorePages()).thenReturn(true);
        when(first.fetchNextPage()).thenReturn(next);
        return ResultSets.wrap(first);
    }

    @Test
    void nextPageFailureCausesAreUnwrapped() {
        final DriverTimeoutException timeout = new DriverTimeoutException("slow");
        final IOException checked = new IOException("checked");
        final CompletableFuture<AsyncResultSet> cancelled = new CompletableFuture<>();
        cancelled.cancel(false);

        assertAll(
                () -> assertSame(timeout,
                        assertThrows(DriverTimeoutException.class, () -> failingSecondPage(CompletableFuture.failedFuture(timeout)).iterator().hasNext())),
                () -> assertSame(checked,
                        assertThrows(RuntimeException.class, () -> failingSecondPage(CompletableFuture.failedFuture(checked)).all()).getCause()),
                () -> assertThrows(CancellationException.class, () -> failingSecondPage(cancelled).one()));
    }

    @Test
    void interruptedWaitThrowsUncheckedInterruptedExceptionAndKeepsTheFlag() {
        final ResultSet resultSet = failingSecondPage(new CompletableFuture<>());
        Thread.currentThread().interrupt();

        try {
            assertThrows(UncheckedInterruptedException.class, () -> resultSet.iterator().hasNext());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }
}
