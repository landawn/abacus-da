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

    /**
     * Package-private constructor invoked by {@link CassandraExecutor#async()}; not intended for
     * direct use.
     *
     * @param cassandraExecutor the synchronous executor to delegate to; must not be {@code null}
     */
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

    /**
     * Asynchronously executes the given CQL query and returns a future that completes with a
     * {@link Stream} where each row is exposed as an {@link Object} array of column values.
     *
     * @param query the CQL query to execute
     * @param parameters the positional query parameters
     * @return a future that completes with a Stream of {@code Object[]} rows
     */
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

    /**
     * Asynchronously executes the given CQL query and returns a future that completes with the
     * first row mapped to an instance of {@code targetClass}, or an empty {@link Optional} if
     * no row is returned.
     *
     * @param <T> the result type
     * @param targetClass the class to map the first row to
     * @param query the CQL query to execute
     * @param parameters the positional query parameters
     * @return a future that completes with an {@code Optional} of the first mapped row, or empty
     *         if no row exists
     */
    @Override
    public <T> ContinuableFuture<Optional<T>> findFirst(final Class<T> targetClass, final String query, final Object... parameters) {
        return super.findFirst(targetClass, query, parameters);
    }

    /**
     * Asynchronously executes the given CQL query and returns a future that completes with a
     * single value from the first column of the first row converted to {@code valueClass}, or
     * an empty {@link Nullable} if no row is returned. The value may be {@code null}.
     *
     * @param <T> the value type
     * @param valueClass the class to convert the single value to
     * @param query the CQL query to execute
     * @param parameters the positional query parameters
     * @return a future that completes with a {@code Nullable} holding the value (possibly
     *         {@code null}), or empty if no row exists
     */
    @Override
    public <T> ContinuableFuture<Nullable<T>> queryForSingleValue(final Class<T> valueClass, final String query, final Object... parameters) {
        return super.queryForSingleValue(valueClass, query, parameters);
    }

    /**
     * Asynchronously executes the given CQL query and returns a future that completes with a
     * non-{@code null} value from the first column of the first row converted to
     * {@code valueClass}, or an empty {@link Optional} if no row is returned or the value is
     * {@code null}.
     *
     * @param <T> the value type
     * @param valueClass the class to convert the single value to
     * @param query the CQL query to execute
     * @param parameters the positional query parameters
     * @return a future that completes with an {@code Optional} of the non-null value, or empty
     *         if no row exists or the value is {@code null}
     */
    @Override
    public <T> ContinuableFuture<Optional<T>> queryForSingleNonNull(final Class<T> valueClass, final String query, final Object... parameters) {
        return super.queryForSingleNonNull(valueClass, query, parameters);
    }

    /**
     * Asynchronously executes a parameterless CQL query. The driver's
     * {@link com.datastax.oss.driver.api.core.cql.AsyncResultSet AsyncResultSet} is wrapped via
     * {@link ResultSets#wrap(com.datastax.oss.driver.api.core.cql.AsyncResultSet)} so that callers
     * receive a familiar synchronous {@link ResultSet} interface.
     *
     * @param query the CQL query to execute
     * @return a future that completes with a synchronous-style {@link ResultSet}
     */
    @Override
    public ContinuableFuture<ResultSet> execute(final String query) {
        return ContinuableFuture.wrap(cassandraExecutor.session().executeAsync(cassandraExecutor.prepareStatement(query)).toCompletableFuture())
                .map(ResultSets::wrap);
    }

    /**
     * Asynchronously executes a CQL query with positional parameters. The driver's
     * {@link com.datastax.oss.driver.api.core.cql.AsyncResultSet AsyncResultSet} is wrapped via
     * {@link ResultSets#wrap(com.datastax.oss.driver.api.core.cql.AsyncResultSet)}.
     *
     * @param query the CQL query to execute
     * @param parameters the positional query parameters
     * @return a future that completes with a synchronous-style {@link ResultSet}
     */
    @Override
    public ContinuableFuture<ResultSet> execute(final String query, final Object... parameters) {
        return ContinuableFuture.wrap(cassandraExecutor.session().executeAsync(cassandraExecutor.prepareStatement(query, parameters)).toCompletableFuture())
                .map(ResultSets::wrap);
    }

    /**
     * Asynchronously executes a CQL query with named parameters. The driver's
     * {@link com.datastax.oss.driver.api.core.cql.AsyncResultSet AsyncResultSet} is wrapped via
     * {@link ResultSets#wrap(com.datastax.oss.driver.api.core.cql.AsyncResultSet)}.
     *
     * @param query the CQL query to execute
     * @param parameters a map of named parameter values keyed by parameter name
     * @return a future that completes with a synchronous-style {@link ResultSet}
     */
    @Override
    public ContinuableFuture<ResultSet> execute(final String query, final Map<String, Object> parameters) {
        return ContinuableFuture.wrap(cassandraExecutor.session().executeAsync(cassandraExecutor.prepareStatement(query, parameters)).toCompletableFuture())
                .map(ResultSets::wrap);
    }

    /**
     * Asynchronously executes a pre-built {@link Statement}. The driver's
     * {@link com.datastax.oss.driver.api.core.cql.AsyncResultSet AsyncResultSet} is wrapped via
     * {@link ResultSets#wrap(com.datastax.oss.driver.api.core.cql.AsyncResultSet)}.
     *
     * @param statement the CQL statement to execute
     * @return a future that completes with a synchronous-style {@link ResultSet}
     */
    @Override
    public ContinuableFuture<ResultSet> execute(final Statement<?> statement) {
        return ContinuableFuture.wrap(cassandraExecutor.session().executeAsync(statement).toCompletableFuture()).map(ResultSets::wrap);
    }
}
