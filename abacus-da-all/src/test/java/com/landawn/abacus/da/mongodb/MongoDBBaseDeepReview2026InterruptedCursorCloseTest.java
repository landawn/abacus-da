package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingSupplier;

import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.Dataset;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;

/**
 * 2026-10-24 sliceT (cross-file, on the main agent's instruction): a cursor that the sync code closes itself must be closed
 * with the thread's interrupt status cleared. {@code cancel(true)} on an async executor future interrupts the worker running
 * the sync code, and a cursor closed on an interrupted thread skips {@code killCursors}, so the server cursor stayed open
 * until its idle timeout (proven live by slice R: +1 open cursor per cancelled {@code async.list}/{@code async.query}). The
 * status itself must survive the close, and close failures keep the try-with-resources semantics.
 */
public class MongoDBBaseDeepReview2026InterruptedCursorCloseTest {

    /** A row that makes {@link RecordingCursor#next()} throw. */
    private static final Object CURSOR_FAILURE = new Object();

    /**
     * 2026-10-24 verify4: creates one mock of every type the tests mock, before any test sets the interrupt status. WHY: the
     * build does not run Mockito as a {@code -javaagent}, so the JVM's first inline mock self-attaches Byte Buddy's agent
     * through an external process whose {@code waitFor()} throws InterruptedException on an interrupted thread; when this
     * class ran first in its JVM (for example on its own), both tests failed with MockitoInitializationException.
     */
    @BeforeAll
    static void initializeMockitoBeforeAnyInterrupt() {
        mock(MongoIterable.class);
        mock(FindIterable.class);
        mock(MongoCollection.class);
        mock(AsyncExecutor.class);
    }

    /** A cursor over fixed rows that records the thread's interrupt status when {@code close()} runs. */
    static final class RecordingCursor<T> implements MongoCursor<T> {
        private final List<?> rows;
        private final RuntimeException closeFailure;
        private int position;
        int closeCount;
        boolean interruptedAtClose;

        RecordingCursor(final List<?> rows, final RuntimeException closeFailure) {
            this.rows = rows;
            this.closeFailure = closeFailure;
        }

        @Override
        public void close() {
            closeCount++;
            interruptedAtClose = Thread.currentThread().isInterrupted();

            if (closeFailure != null) {
                throw closeFailure;
            }
        }

        @Override
        public boolean hasNext() {
            return position < rows.size();
        }

        @Override
        @SuppressWarnings("unchecked")
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            final Object row = rows.get(position++);

            if (row == CURSOR_FAILURE) {
                throw new MongoException("cursor failure");
            }

            return (T) row;
        }

        @Override
        public int available() {
            return rows.size() - position;
        }

        @Override
        public T tryNext() {
            return hasNext() ? next() : null;
        }

        @Override
        public ServerCursor getServerCursor() {
            return null;
        }

        @Override
        public ServerAddress getServerAddress() {
            return null;
        }
    }

    /** The cursors an iterable stub opened, in order. */
    private final List<RecordingCursor<?>> opened = new ArrayList<>();

    /**
     * Stubs {@code iterator()} of {@code iterable} to open a {@link RecordingCursor} over {@code rows}, and {@code into(...)} the
     * way the driver implements it (drain {@code iterator()} in a try-with-resources block).
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private <I extends MongoIterable<?>> I stub(final I iterable, final List<?> rows, final RuntimeException closeFailure) {
        doAnswer(invocation -> {
            final RecordingCursor<?> cursor = new RecordingCursor<>(rows, closeFailure);
            opened.add(cursor);
            return cursor;
        }).when(iterable).iterator();

        doAnswer(invocation -> {
            final Collection target = invocation.getArgument(0);

            try (MongoCursor<?> cursor = iterable.iterator()) {
                while (cursor.hasNext()) {
                    target.add(cursor.next());
                }
            }

            return target;
        }).when(iterable).into(any());

        return iterable;
    }

    @SuppressWarnings("unchecked")
    private MongoCollectionExecutor executorReturning(final List<?> rows) {
        final MongoCollection<Document> collection = mock(MongoCollection.class);
        final FindIterable<Document> findIterable = mock(FindIterable.class);
        when(collection.find(any(Bson.class))).thenReturn(findIterable);
        when(findIterable.projection(any())).thenReturn(findIterable);
        when(findIterable.sort(any())).thenReturn(findIterable);
        when(findIterable.skip(anyInt())).thenReturn(findIterable);
        when(findIterable.limit(anyInt())).thenReturn(findIterable);
        stub(findIterable, rows, null);

        return new MongoCollectionExecutor(collection, mock(AsyncExecutor.class));
    }

    /**
     * Runs {@code action} on this thread with its interrupt status set (as after {@code cancel(true)}) and asserts that the one
     * cursor it opened was closed once, with the status cleared, and that the status is set again afterwards.
     */
    private <T> T assertClosedUninterrupted(final ThrowingSupplier<T> action) throws Throwable {
        opened.clear();
        Thread.currentThread().interrupt();

        try {
            final T result = action.get();

            assertTrue(Thread.currentThread().isInterrupted(), "interrupt status restored");
            assertEquals(1, opened.size(), "cursors opened");
            assertEquals(1, opened.get(0).closeCount, "close calls");
            Assertions.assertFalse(opened.get(0).interruptedAtClose, "cursor closed on an interrupted thread");

            return result;
        } finally {
            Thread.interrupted();
        }
    }

    private static List<Document> rows() {
        return Arrays.asList(new Document("name", "n1").append("address", new Document("city", "Paris")), new Document("name", "n2"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void cursorsAreClosedWithTheInterruptStatusClearedAndRestored() {
        Assertions.assertAll(() -> {
            final List<NamedRow> result = assertClosedUninterrupted(() -> MongoDBBase.toList(stub(mock(MongoIterable.class), rows(), null), NamedRow.class));
            assertEquals("n2", result.get(1).getName());
        }, () -> {
            final Dataset ds = assertClosedUninterrupted(() -> MongoDBBase.extractData(stub(mock(MongoIterable.class), rows(), null)));
            assertEquals(Arrays.asList("n1", "n2"), ds.getColumn("name"));
        }, () -> {
            final Dataset ds = assertClosedUninterrupted(
                    () -> MongoDBBase.extractData(Collections.singletonList("name"), stub(mock(MongoIterable.class), rows(), null), Map.class));
            assertEquals(Arrays.asList("n1", "n2"), ds.getColumn("name"));
        }, () -> {
            // MongoCollectionExecutor.list: the single-field branch iterates the cursor itself.
            final List<String> names = assertClosedUninterrupted(
                    () -> executorReturning(rows()).list(Collections.singletonList("name"), new Document(), String.class));
            assertEquals(Arrays.asList("n1", "n2"), names);
        }, () -> {
            // MongoCollectionExecutor.query: a dotted column fetches every document before resolving the nested values.
            final Dataset ds = assertClosedUninterrupted(
                    () -> executorReturning(rows()).query(Arrays.asList("name", "address.city"), new Document(), Map.class));
            assertEquals(Arrays.asList("Paris", null), ds.getColumn("address.city"));
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    public void closeFailuresKeepTheTryWithResourcesSemantics() {
        final MongoException closeFailure = new MongoException("close failure");

        Assertions.assertAll(() -> {
            // A close failure after a complete read propagates, and the interrupt status still comes back.
            opened.clear();
            Thread.currentThread().interrupt();

            try {
                final MongoIterable<Object> iterable = stub(mock(MongoIterable.class), rows(), closeFailure);
                assertSame(closeFailure, assertThrows(MongoException.class, () -> MongoDBBase.toList(iterable, NamedRow.class)));
                assertTrue(Thread.currentThread().isInterrupted(), "interrupt status restored");
            } finally {
                Thread.interrupted();
            }
        }, () -> {
            // A close failure after a read failure is suppressed onto it.
            final MongoIterable<Object> iterable = stub(mock(MongoIterable.class), Arrays.asList(rows().get(0), CURSOR_FAILURE), closeFailure);
            final MongoException thrown = assertThrows(MongoException.class, () -> MongoDBBase.extractData(iterable));

            assertEquals("cursor failure", thrown.getMessage());
            assertEquals(Collections.singletonList(closeFailure), Arrays.asList(thrown.getSuppressed()));
        }, () -> {
            final MongoIterable<Object> iterable = stub(mock(MongoIterable.class), Arrays.asList(rows().get(0), CURSOR_FAILURE), closeFailure);
            final MongoException thrown = assertThrows(MongoException.class, () -> MongoDBBase.toList(iterable, NamedRow.class));

            assertEquals("cursor failure", thrown.getMessage());
            assertEquals(Collections.singletonList(closeFailure), Arrays.asList(thrown.getSuppressed()));
        });
    }

    public static class NamedRow {
        private String name;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }
}
