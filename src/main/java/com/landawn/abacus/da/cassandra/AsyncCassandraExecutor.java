/*
 * Copyright (C) 2024 HaiYang Li
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.landawn.abacus.da.cassandra;

import java.util.Map;
import java.util.function.BiFunction;

import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.u.Nullable;
import com.landawn.abacus.util.u.Optional;
import com.landawn.abacus.util.stream.Stream;

/**
 * Asynchronous facade for {@link CassandraExecutor} backed by the DataStax OSS Java driver.
 *
 * <p>This executor exposes the same set of operations as {@link CassandraExecutor} but returns
 * each result wrapped in a {@link ContinuableFuture}, executing statements through the driver's
 * {@code executeAsync} API. Instances are obtained from {@link CassandraExecutor#async()}, and
 * the backing synchronous executor can be retrieved via {@link #sync()}.</p>
 *
 * @see CassandraExecutor
 * @see AsyncCassandraExecutorBase
 */
public final class AsyncCassandraExecutor extends AsyncCassandraExecutorBase<Row, ResultSet, Statement<?>, PreparedStatement, BatchType> {

    private final CassandraExecutor cassandraExecutor;

    AsyncCassandraExecutor(final CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
        this.cassandraExecutor = cassandraExecutor;
    }

    /**
     * Returns the underlying synchronous {@link CassandraExecutor} that backs this asynchronous facade.
     *
     * @return the wrapped synchronous {@link CassandraExecutor}
     */
    @Override
    public CassandraExecutor sync() {
        return cassandraExecutor;
    }

    @Override
    public ContinuableFuture<Stream<Object[]>> stream(final String query, final Object... parameters) {
        return super.stream(query, parameters);
    }

    /**
     * Asynchronously executes a CQL query and returns a future of a {@link Stream} mapped by a custom row mapper.
     *
     * @param <T> the type of objects in the returned stream
     * @param query the CQL query string
     * @param rowMapper a function that maps column definitions and rows to result objects
     * @param parameters the query parameters
     * @return a future that completes with a Stream of mapped objects
     */
    public <T> ContinuableFuture<Stream<T>> stream(final String query, final BiFunction<ColumnDefinitions, Row, T> rowMapper, final Object... parameters) {
        return execute(query, parameters).map(resultSet -> Stream.of(resultSet.iterator()).map(cassandraExecutor.createRowMapper(rowMapper)));
    }

    /**
     * Asynchronously executes a pre-configured CQL statement and returns a future of a {@link Stream} mapped by a custom row mapper.
     *
     * @param <T> the type of objects in the returned stream
     * @param statement the configured CQL statement to execute
     * @param rowMapper a function that maps column definitions and rows to result objects
     * @return a future that completes with a Stream of mapped objects
     */
    public <T> ContinuableFuture<Stream<T>> stream(final Statement<?> statement, final BiFunction<ColumnDefinitions, Row, T> rowMapper) {
        return execute(statement).map(resultSet -> Stream.of(resultSet.iterator()).map(cassandraExecutor.createRowMapper(rowMapper)));
    }

    @Override
    public <T> ContinuableFuture<Optional<T>> findFirst(final Class<T> targetClass, final String query, final Object... parameters) {
        return super.findFirst(targetClass, query, parameters);
    }

    @Override
    public <T> ContinuableFuture<Nullable<T>> queryForSingleValue(final Class<T> valueClass, final String query, final Object... parameters) {
        return super.queryForSingleValue(valueClass, query, parameters);
    }

    @Override
    public <T> ContinuableFuture<Optional<T>> queryForSingleNonNull(final Class<T> valueClass, final String query, final Object... parameters) {
        return super.queryForSingleNonNull(valueClass, query, parameters);
    }

    @Override
    public ContinuableFuture<ResultSet> execute(final String query) {
        return ContinuableFuture.wrap(cassandraExecutor.session().executeAsync(cassandraExecutor.prepareStatement(query)).toCompletableFuture())
                .map(ResultSets::wrap);
    }

    @Override
    public ContinuableFuture<ResultSet> execute(final String query, final Object... parameters) {
        return ContinuableFuture.wrap(cassandraExecutor.session().executeAsync(cassandraExecutor.prepareStatement(query, parameters)).toCompletableFuture())
                .map(ResultSets::wrap);
    }

    @Override
    public ContinuableFuture<ResultSet> execute(final String query, final Map<String, Object> parameters) {
        return ContinuableFuture.wrap(cassandraExecutor.session().executeAsync(cassandraExecutor.prepareStatement(query, parameters)).toCompletableFuture())
                .map(ResultSets::wrap);
    }

    @Override
    public ContinuableFuture<ResultSet> execute(final Statement<?> statement) {
        return ContinuableFuture.wrap(cassandraExecutor.session().executeAsync(statement).toCompletableFuture()).map(ResultSets::wrap);
    }
}
