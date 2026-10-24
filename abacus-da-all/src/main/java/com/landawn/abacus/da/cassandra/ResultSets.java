/*
 * Copyright (C) 2024 HaiYang Li
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.landawn.abacus.da.cassandra;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.landawn.abacus.da.cs;
import com.landawn.abacus.exception.UncheckedInterruptedException;
import com.landawn.abacus.util.ExceptionUtil;
import com.landawn.abacus.util.ImmutableList;
import com.landawn.abacus.util.N;

/**
 * Internal utility class for working with Cassandra {@link ResultSet} and {@link AsyncResultSet}.
 *
 * <p>This package-private utility class provides helper methods for bridging between the
 * asynchronous and synchronous result types of the DataStax Java Driver (v4+, the
 * {@code com.datastax.oss.driver} APIs). It is intended for internal use by the
 * {@code com.landawn.abacus.da.cassandra} package.</p>
 *
 * <h2>Key Features</h2>
 * <ul>
 * <li><strong>Async-to-Sync Bridging:</strong> Wraps an {@link AsyncResultSet} as a {@link ResultSet}.</li>
 * <li><strong>Transparent Paging:</strong> Automatically fetches subsequent pages during iteration.</li>
 * <li><strong>Memory Efficient:</strong> Keeps only the current page in memory.</li>
 * </ul>
 *
 * <h2>Thread Safety</h2>
 * <p>The static methods on this class are thread-safe, but the {@link ResultSet} returned by
 * {@link #wrap(AsyncResultSet)} is not thread-safe and must be consumed from a single thread.</p>
 *
 * @see com.datastax.oss.driver.api.core.cql.ResultSet
 * @see com.datastax.oss.driver.api.core.cql.AsyncResultSet
 */
final class ResultSets {
    private ResultSets() {
        // utility class - no instances allowed
    }

    /**
     * Wraps an {@link AsyncResultSet} as a synchronous {@link ResultSet}.
     *
     * <p>The returned {@code ResultSet} iterates over the rows currently held by
     * {@code asyncResultSet} and, when the current page is exhausted, transparently blocks to
     * fetch the next page via {@link AsyncResultSet#fetchNextPage()}. The execution info of
     * every fetched page is accumulated and returned by {@link ResultSet#getExecutionInfos()}.</p>
     *
     * <h4>Behavior</h4>
     * <ul>
     * <li>{@link ResultSet#all()} consumes the iterator and therefore eagerly drains all
     * remaining pages.</li>
     * <li>{@link ResultSet#iterator()} returns the same stateful cursor on every call, matching
     * the driver's synchronous result-set implementations. A later call therefore continues
     * from the current position instead of replaying rows from the current page.</li>
     * <li>{@link ResultSet#isFullyFetched()} reflects whether the current page is the last one
     * known to the driver; it does not guarantee that all rows have already been iterated.</li>
     * <li>{@link ResultSet#getAvailableWithoutFetching()} returns the number of rows left in the
     * current page ({@link AsyncResultSet#remaining()}); it never triggers a fetch.</li>
     * <li>A failed next-page future is unwrapped from its {@link java.util.concurrent.ExecutionException}:
     * a {@link RuntimeException} or {@link Error} cause (for example the driver's timeout exception) is
     * rethrown as is, and a checked cause is converted into a runtime exception via
     * {@link com.landawn.abacus.util.ExceptionUtil}. If the wait is interrupted, the
     * current thread's interrupt status is restored and an {@link UncheckedInterruptedException} is thrown.</li>
     * <li>Consuming the result on a driver thread throws {@link IllegalStateException} when another page
     * is required. Rows already available in the current page can be consumed without triggering this check.</li>
     * </ul>
     *
     * <h4>Usage Example</h4>
     * <pre>{@code
     * CompletionStage<AsyncResultSet> stage = session.executeAsync(statement);
     * AsyncResultSet asyncRs = stage.toCompletableFuture().get();
     *
     * ResultSet rs = ResultSets.wrap(asyncRs);
     * for (Row row : rs) {
     *     String name = row.getString("name");
     *     int age = row.getInt("age");
     *     // ...
     * }
     * }</pre>
     *
     * @param asyncResultSet the async result set to wrap; must not be {@code null}
     * @return a {@link ResultSet} that lazily iterates over all rows produced by
     *         {@code asyncResultSet} and its subsequent pages
     * @throws IllegalArgumentException if {@code asyncResultSet} is {@code null}
     * @see com.datastax.oss.driver.api.core.cql.AsyncResultSet
     * @see com.datastax.oss.driver.api.core.cql.ResultSet
     */
    public static ResultSet wrap(final AsyncResultSet asyncResultSet) throws IllegalArgumentException {
        N.checkArgNotNull(asyncResultSet, cs.asyncResultSet);

        return new PagedResultSet(asyncResultSet);
    }

    // A named static wrapper avoids the synthetic capture of the initial page in an anonymous class.
    // Driver 4.19.3 consumes the encoded rows, but the captured page still retains its emptied queue's backing storage
    // and iterator state after advancing to later pages.
    private static final class PagedResultSet implements ResultSet {
        private AsyncResultSet currentResultSet;
        private final List<ExecutionInfo> executionInfos = new ArrayList<>(1);
        private Iterator<Row> currentRows;

        private final Iterator<Row> rowIterator = new Iterator<>() {
            /**
             * @return whether a row remains, fetching subsequent pages as necessary
             * @throws IllegalStateException if another page is required while running on a driver thread
             * @throws RuntimeException if fetching a subsequent page fails or its future is cancelled
             * @throws UncheckedInterruptedException if waiting for a subsequent page is interrupted
             */
            @Override
            public boolean hasNext() throws RuntimeException, UncheckedInterruptedException {
                while ((currentRows == null || !currentRows.hasNext()) && currentResultSet.hasMorePages()) {
                    try {
                        BlockingOperation.checkNotDriverThread();
                        currentResultSet = currentResultSet.fetchNextPage().toCompletableFuture().get();
                        executionInfos.add(currentResultSet.getExecutionInfo());
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw ExceptionUtil.toRuntimeException(e, true);
                    } catch (final ExecutionException e) {
                        // WHY: rethrow an Error cause as is, like the driver's synchronous MultiPageResultSet
                        // (CompletableFutures.getUninterruptibly) and like a first-page Error, which the async facade reports as
                        // the ExecutionException cause; toRuntimeException(e, true) wrapped it in a plain RuntimeException.
                        throw ExceptionUtil.toRuntimeException(e, true, true);
                    }

                    currentRows = N.iterate(currentResultSet.currentPage());
                }

                return currentRows != null && currentRows.hasNext();
            }

            /**
             * @return the next row, fetching another page if necessary
             * @throws IllegalStateException if another page is required while running on a driver thread
             * @throws RuntimeException if fetching a subsequent page fails or its future is cancelled
             * @throws UncheckedInterruptedException if waiting for a subsequent page is interrupted
             * @throws NoSuchElementException if all rows in all pages have been consumed
             */
            @Override
            public Row next() throws RuntimeException, UncheckedInterruptedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                return currentRows.next();
            }
        };

        private PagedResultSet(final AsyncResultSet asyncResultSet) {
            currentResultSet = asyncResultSet;
            currentRows = N.iterate(asyncResultSet.currentPage());
            executionInfos.add(asyncResultSet.getExecutionInfo());
        }

        @Override
        public ColumnDefinitions getColumnDefinitions() {
            return currentResultSet.getColumnDefinitions();
        }

        @Override
        public List<ExecutionInfo> getExecutionInfos() {
            return ImmutableList.copyOf(executionInfos);
        }

        @Override
        public boolean isFullyFetched() {
            return !currentResultSet.hasMorePages();
        }

        @Override
        public boolean wasApplied() {
            return currentResultSet.wasApplied();
        }

        /**
         * @return the number of rows left in the current page, as reported by {@link AsyncResultSet#remaining()}
         */
        @Override
        public int getAvailableWithoutFetching() {
            // Must not throw: the driver's default PagingIterable.all() (inherited by rs.map(f)) sizes its list with this
            // count. The current page's rows are consumed through currentPage(), whose iterator remaining() counts down,
            // exactly as the driver's own synchronous MultiPageResultSet reports it.
            return currentResultSet.remaining();
        }

        /**
         * @return all remaining rows
         * @throws IllegalStateException if another page is required while running on a driver thread
         * @throws RuntimeException if fetching a subsequent page fails or its future is cancelled
         * @throws UncheckedInterruptedException if waiting for a subsequent page is interrupted
         */
        @Override
        public List<Row> all() throws RuntimeException, UncheckedInterruptedException {
            // Pre-size for the rows already held by the current page, like the driver's default PagingIterable.all();
            // later pages still grow the list on demand. Math.max guards a custom AsyncResultSet reporting a negative count.
            final List<Row> rows = new ArrayList<>(Math.max(0, currentResultSet.remaining()));

            while (rowIterator.hasNext()) {
                rows.add(rowIterator.next());
            }

            return rows;
        }

        @Override
        public Iterator<Row> iterator() {
            return rowIterator;
        }
    }
}
