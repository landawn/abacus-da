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
     * <h2>Behavior</h2>
     * <ul>
     * <li>{@link ResultSet#all()} consumes the iterator and therefore eagerly drains all
     * remaining pages.</li>
     * <li>{@link ResultSet#iterator()} returns the same stateful cursor on every call, matching
     * the driver's synchronous result-set implementations. A later call therefore continues
     * from the current position instead of replaying rows from the current page.</li>
     * <li>{@link ResultSet#isFullyFetched()} reflects whether the current page is the last one
     * known to the driver; it does not guarantee that all rows have already been iterated.</li>
     * <li>{@link ResultSet#getAvailableWithoutFetching()} is <strong>not</strong> supported by
     * this wrapper and always throws {@link UnsupportedOperationException}.</li>
     * <li>Checked exceptions raised while fetching the next page (e.g.
     * {@link java.util.concurrent.ExecutionException} or {@link InterruptedException}) are
     * converted into runtime exceptions via {@link com.landawn.abacus.util.ExceptionUtil}.
     * If the wait is interrupted, the current thread's interrupt status is restored first.</li>
     * </ul>
     *
     * <h2>Usage Example</h2>
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
    public static ResultSet wrap(final AsyncResultSet asyncResultSet) {
        N.checkArgNotNull(asyncResultSet, "asyncResultSet");

        return new ResultSet() {
            private AsyncResultSet currentResultSet = asyncResultSet;
            private final List<ExecutionInfo> executionInfos = new ArrayList<>(1);
            private Iterator<Row> currentRows = N.iterate(asyncResultSet.currentPage());

            private final Iterator<Row> rowIterator = new Iterator<>() {
                @Override
                public boolean hasNext() {
                    while ((currentRows == null || !currentRows.hasNext()) && currentResultSet.hasMorePages()) {
                        try {
                            currentResultSet = currentResultSet.fetchNextPage().toCompletableFuture().get();
                            executionInfos.add(currentResultSet.getExecutionInfo());
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw ExceptionUtil.toRuntimeException(e, true);
                        } catch (final ExecutionException e) {
                            throw ExceptionUtil.toRuntimeException(e, true);
                        }

                        currentRows = N.iterate(currentResultSet.currentPage());
                    }

                    return currentRows != null && currentRows.hasNext();
                }

                @Override
                public Row next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }

                    return currentRows.next();
                }
            };

            {
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

            @Override
            public int getAvailableWithoutFetching() throws UnsupportedOperationException {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<Row> all() {
                return N.toList(iterator());
            }

            @Override
            public Iterator<Row> iterator() {
                return rowIterator;
            }
        };
    }
}
