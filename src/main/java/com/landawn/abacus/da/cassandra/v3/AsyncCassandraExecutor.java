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

package com.landawn.abacus.da.cassandra.v3;

import java.util.Map;
import java.util.function.BiFunction;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.landawn.abacus.da.cassandra.AsyncCassandraExecutorBase;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.u.Nullable;
import com.landawn.abacus.util.u.Optional;
import com.landawn.abacus.util.stream.Stream;

/**
 * Asynchronous facade for the Cassandra Driver 3.x {@link CassandraExecutor}.
 * @deprecated Use {@link com.landawn.abacus.da.cassandra.AsyncCassandraExecutor}
 *             for new applications (requires Cassandra Java Driver 4.x).
 */
public final class AsyncCassandraExecutor extends AsyncCassandraExecutorBase<Row, ResultSet, Statement, PreparedStatement, BatchStatement.Type> {

    private final CassandraExecutor cassandraExecutor;

    AsyncCassandraExecutor(final CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
        this.cassandraExecutor = cassandraExecutor;
    }

    @Override
    public CassandraExecutor sync() {
        return cassandraExecutor;
    }

    @Override
    public ContinuableFuture<Stream<Object[]>> stream(final String query, final Object... parameters) {
        return super.stream(query, parameters);
    }

    /**
     * Asynchronously executes the given CQL query and returns a future that completes with a
     * {@link Stream} of rows mapped by the supplied row mapper.
     *
     * @param <T> the result type produced by the row mapper
     * @param query the CQL query to execute
     * @param rowMapper function that converts the column definitions and each row into a result object
     * @param parameters the query parameters
     * @return a future that completes with a Stream of mapped results
     */
    public <T> ContinuableFuture<Stream<T>> stream(final String query, final BiFunction<ColumnDefinitions, Row, T> rowMapper, final Object... parameters) {
        return execute(query, parameters).map(resultSet -> Stream.of(resultSet.iterator()).map(cassandraExecutor.createRowMapper(rowMapper)));
    }

    /**
     * Asynchronously executes the given statement and returns a future that completes with a
     * {@link Stream} of rows mapped by the supplied row mapper.
     *
     * @param <T> the result type produced by the row mapper
     * @param statement the statement to execute
     * @param rowMapper function that converts the column definitions and each row into a result object
     * @return a future that completes with a Stream of mapped results
     */
    public <T> ContinuableFuture<Stream<T>> stream(final Statement statement, final BiFunction<ColumnDefinitions, Row, T> rowMapper) {
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
        return ContinuableFuture.wrap(cassandraExecutor.session().executeAsync(cassandraExecutor.prepareStatement(query)));
    }

    @Override
    public ContinuableFuture<ResultSet> execute(final String query, final Object... parameters) {
        return ContinuableFuture.wrap(cassandraExecutor.session().executeAsync(cassandraExecutor.prepareStatement(query, parameters)));
    }

    @Override
    public ContinuableFuture<ResultSet> execute(final String query, final Map<String, Object> parameters) {
        return ContinuableFuture.wrap(cassandraExecutor.session().executeAsync(cassandraExecutor.prepareStatement(query, parameters)));
    }

    @Override
    public ContinuableFuture<ResultSet> execute(final Statement statement) {
        return ContinuableFuture.wrap(cassandraExecutor.session().executeAsync(statement));
    }
}
