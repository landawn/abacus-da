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
 * Utility class for working with Cassandra ResultSet objects and async result handling.
 * 
 * <p>This utility class provides helper methods for converting between different types of
 * Cassandra result sets, particularly for bridging between asynchronous and synchronous
 * result processing. It's designed to work with the modern Cassandra Java Driver's
 * asynchronous API while providing familiar synchronous interfaces.</p>
 * 
 * <h3>Key Features</h3>
 * <ul>
 * <li><strong>Async-to-Sync Bridging:</strong> Converts AsyncResultSet to synchronous ResultSet</li>
 * <li><strong>Lazy Loading:</strong> Supports pagination and lazy fetching of large result sets</li>
 * <li><strong>Memory Efficiency:</strong> Processes results page by page to minimize memory usage</li>
 * <li><strong>Error Handling:</strong> Proper exception handling for network and timeout issues</li>
 * </ul>
 * 
 * <h3>Use Cases</h3>
 * <ul>
 * <li>Converting async results for use with synchronous APIs</li>
 * <li>Processing large result sets that span multiple pages</li>
 * <li>Maintaining compatibility with existing synchronous code</li>
 * <li>Implementing custom result processing logic</li>
 * </ul>
 * 
 * <h3>Thread Safety</h3>
 * <p>This class is thread-safe as it contains only static utility methods and no mutable state.
 * However, the ResultSet instances created by these methods should be used from a single thread
 * unless otherwise documented.</p>
 * 
 * @see com.datastax.oss.driver.api.core.cql.ResultSet
 * @see com.datastax.oss.driver.api.core.cql.AsyncResultSet
 */
final class ResultSets {
    private ResultSets() {
        // utility class - no instances allowed
    }

    /**
     * Wraps an AsyncResultSet to provide a synchronous ResultSet interface.
     * 
     * <p>This method creates a synchronous wrapper around an AsyncResultSet that allows
     * traditional synchronous iteration while maintaining support for Cassandra's paging
     * mechanism. The wrapper handles automatic page fetching when the current page is
     * exhausted, providing seamless access to all results regardless of pagination.</p>
     * 
     * <h4>Key Features:</h4>
     * <ul>
     * <li><strong>Transparent Paging:</strong> Automatically fetches additional pages when needed</li>
     * <li><strong>Blocking Iteration:</strong> Iterator blocks until next page is available</li>
     * <li><strong>Memory Efficient:</strong> Only keeps current page in memory</li>
     * <li><strong>Exception Handling:</strong> Converts async exceptions to runtime exceptions</li>
     * </ul>
     * 
     * <h4>Performance Considerations:</h4>
     * <ul>
     * <li>First page is immediately available (no additional network call)</li>
     * <li>Subsequent pages trigger network requests and may cause blocking</li>
     * <li>Large result sets are processed page by page to minimize memory usage</li>
     * <li>Network timeouts and connection issues are propagated as runtime exceptions</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Execute async query
     * CompletionStage<AsyncResultSet> asyncStage = session.executeAsync(statement);
     * AsyncResultSet asyncResultSet = asyncStage.toCompletableFuture().get();
     * 
     * // Wrap for synchronous processing
     * ResultSet syncResultSet = ResultSets.wrap(asyncResultSet);
     * 
     * // Standard synchronous iteration
     * for (Row row : syncResultSet) {
     *     String name = row.getString("name");
     *     int age = row.getInt("age");
     *     // Process row...
     * }
     * 
     * // Or collect all results
     * List<Row> allRows = syncResultSet.all();   // Fetches all pages if needed
     * 
     * // Check if more pages are available
     * boolean hasMore = !syncResultSet.isFullyFetched();
     * }</pre>
     * 
     * <h4>Error Handling:</h4>
     * <p>Network errors, timeouts, and other async exceptions are caught and wrapped
     * in runtime exceptions during iteration. This ensures that synchronous code
     * doesn't need to deal with checked exceptions but can still handle errors
     * appropriately.</p>
     * 
     * @param asyncResultSet the AsyncResultSet to wrap for synchronous access
     * @return a synchronous ResultSet that provides access to all rows across all pages
     * @throws IllegalArgumentException if asyncResultSet is null
     * @see com.datastax.oss.driver.api.core.cql.AsyncResultSet
     * @see com.datastax.oss.driver.api.core.cql.ResultSet
     */
    public static ResultSet wrap(final AsyncResultSet asyncResultSet) {
        N.checkArgNotNull(asyncResultSet, "asyncResultSet");

        return new ResultSet() {
            private AsyncResultSet currentResultSet = asyncResultSet;
            private final List<ExecutionInfo> executionInfos = new ArrayList<>(1);

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
                return new Iterator<>() {
                    private Iterator<Row> rows = N.iterate(currentResultSet.currentPage());

                    @Override
                    public boolean hasNext() {
                        while ((rows == null || !rows.hasNext()) && currentResultSet.hasMorePages()) {
                            try {
                                currentResultSet = currentResultSet.fetchNextPage().toCompletableFuture().get();
                                executionInfos.add(currentResultSet.getExecutionInfo());
                            } catch (ExecutionException | InterruptedException e) {
                                throw ExceptionUtil.toRuntimeException(e, true);
                            }

                            rows = N.iterate(currentResultSet.currentPage());
                        }

                        return rows != null && rows.hasNext();
                    }

                    @Override
                    public Row next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }

                        return rows.next();
                    }
                };
            }
        };
    }
}
