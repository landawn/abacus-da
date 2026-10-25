package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.util.ContinuableFuture;

class AsyncCassandraExecutorBaseTest {

    @SuppressWarnings("unchecked")
    @Test
    void statementStreamsAreMappedLazilyAndReuseOneCursorAcrossFutureReads() throws Exception {
        final CassandraExecutorBase<String, Iterable<String>, Object, Object, Object> sync = mock(CassandraExecutorBase.class);
        final Iterable<String> rows = mock(Iterable.class);
        when(rows.iterator()).thenReturn(List.of("7", "8").iterator());
        final AtomicInteger conversions = new AtomicInteger();
        when(sync.createRowMapper(Long.class)).thenReturn(value -> {
            conversions.incrementAndGet();
            return Long.valueOf(value);
        });
        final Object statement = new Object();
        final TestAsync async = new TestAsync(sync, rows, statement);
        assertSame(sync, async.sync());
        final var future = async.stream(Long.class, statement);
        try (var stream = future.get()) {
            assertSame(stream, future.get());
            assertEquals(0, conversions.get());
            assertEquals(List.of(7L, 8L), stream.toList());
            assertEquals(2, conversions.get());
        }
        verify(rows).iterator();
        verify(sync).createRowMapper(Long.class);
    }

    @SuppressWarnings("unchecked")
    @Test
    void cursorCreationFailuresAreReplayedWithoutConsumingTheResultAgain() {
        final CassandraExecutorBase<String, Iterable<String>, Object, Object, Object> sync = mock(CassandraExecutorBase.class);
        final Iterable<String> rows = mock(Iterable.class);
        final IllegalStateException failure = new IllegalStateException("cursor unavailable");
        when(rows.iterator()).thenThrow(failure);
        final Object statement = new Object();
        final TestAsync async = new TestAsync(sync, rows, statement);
        final var future = async.stream(Long.class, statement);
        assertSame(failure, assertThrows(ExecutionException.class, future::get).getCause());
        assertSame(failure, assertThrows(ExecutionException.class, future::get).getCause());
        verify(rows).iterator();
    }

    // ---- 2026-10-24 r2 sliceG ----

    /**
     * Pins the class Javadoc: a repeated get() on a stream future returns the same, single-use Stream (the cursor is not read
     * again), so a second terminal operation on it throws IllegalStateException.
     */
    @SuppressWarnings("unchecked")
    @Test
    void r2sliceG_repeatedGetOfAStreamFutureReturnsTheSameSingleUseStream() throws Exception {
        final CassandraExecutorBase<String, Iterable<String>, Object, Object, Object> sync = mock(CassandraExecutorBase.class);
        final Iterable<String> rows = mock(Iterable.class);
        when(rows.iterator()).thenReturn(List.of("7", "8").iterator());
        when(sync.createRowMapper(Long.class)).thenReturn(Long::valueOf);
        final Object statement = new Object();
        final var future = new TestAsync(sync, rows, statement).stream(Long.class, statement);

        final var first = future.get();
        assertEquals(List.of(7L, 8L), first.toList());

        final var second = future.get();
        org.junit.jupiter.api.Assertions.assertAll(() -> assertSame(first, second), () -> assertThrows(IllegalStateException.class, second::toList),
                () -> verify(rows).iterator());
    }

    // Exercise the base extension contract; concrete driver facades override stream(Class, Statement).
    private static final class TestAsync extends AsyncCassandraExecutorBase<String, Iterable<String>, Object, Object, Object> {
        private final Iterable<String> rows;
        private final Object statement;

        private TestAsync(final CassandraExecutorBase<String, Iterable<String>, Object, Object, Object> sync, final Iterable<String> rows,
                final Object statement) {
            super(sync);
            this.rows = rows;
            this.statement = statement;
        }

        @Override
        public ContinuableFuture<Iterable<String>> execute(final Object statement) {
            assertSame(this.statement, statement);
            return ContinuableFuture.completed(rows);
        }

        @Override
        public ContinuableFuture<Iterable<String>> execute(final String query) {
            throw new UnsupportedOperationException("Only statement execution is configured by this fixture");
        }

        @Override
        public ContinuableFuture<Iterable<String>> execute(final String query, final Object... parameters) {
            throw new UnsupportedOperationException("Only statement execution is configured by this fixture");
        }

        @Override
        public ContinuableFuture<Iterable<String>> execute(final String query, final Map<String, Object> parameters) {
            throw new UnsupportedOperationException("Only statement execution is configured by this fixture");
        }
    }
}
