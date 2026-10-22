package com.landawn.abacus.da.mongodb;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;

/**
 * Stubs a mocked {@link MongoIterable} so that every way of reading it yields the same rows, as a driver iterable does:
 * {@code iterator()} / {@code cursor()} open a fresh {@link ListCursor} over the rows, and {@code into(target)} adds the rows
 * to {@code target} and returns it. Read paths that stream the cursor (e.g. {@code MongoDBBase.toList}) and paths that
 * still call {@code into} (e.g. {@code extractData}) therefore see identical results.
 */
final class MongoIterableStubs {

    /** A row value that makes the cursor's {@code next()} throw a {@link MongoException} at its position. */
    static final Object CURSOR_FAILURE = new Object() {
        @Override
        public String toString() {
            return "CURSOR_FAILURE";
        }
    };

    private MongoIterableStubs() {
    }

    /**
     * Stubs {@code iterator()}, {@code cursor()} and {@code into(...)} of {@code iterable} to yield {@code rows} (which may
     * contain {@code null} rows and {@link #CURSOR_FAILURE}).
     *
     * @return the cursors opened so far, in opening order (a live list), so tests can assert they were closed
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    static List<ListCursor<?>> stubRows(final MongoIterable<?> iterable, final List<?> rows) {
        final List<ListCursor<?>> opened = new ArrayList<>();

        doAnswer(invocation -> {
            final ListCursor<?> cursor = new ListCursor<>(rows);
            opened.add(cursor);
            return cursor;
        }).when(iterable).iterator();

        doAnswer(invocation -> {
            final ListCursor<?> cursor = new ListCursor<>(rows);
            opened.add(cursor);
            return cursor;
        }).when(iterable).cursor();

        doAnswer(invocation -> {
            final Collection target = invocation.getArgument(0);

            // As the driver's into(): drain a cursor (closed afterwards) into the target and return the target.
            try (ListCursor<?> cursor = new ListCursor<>(rows)) {
                opened.add(cursor);

                while (cursor.hasNext()) {
                    target.add(cursor.next());
                }
            }

            return target;
        }).when(iterable).into(any());

        return opened;
    }

    /** A {@link MongoCursor} over a list that records whether it was closed and how many rows were read. */
    static final class ListCursor<T> implements MongoCursor<T> {
        private final List<?> rows;
        private int position;
        private boolean closed;

        ListCursor(final List<?> rows) {
            this.rows = rows;
        }

        boolean isClosed() {
            return closed;
        }

        int rowsRead() {
            return position;
        }

        @Override
        public void close() {
            closed = true;
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
                throw new MongoException("cursor failure at row " + (position - 1));
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
}
