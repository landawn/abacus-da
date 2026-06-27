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
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.u.Nullable;
import com.landawn.abacus.util.u.Optional;
import com.landawn.abacus.util.stream.Stream;

/**
 * Asynchronous facade for the Cassandra Java Driver 3.x backed {@link CassandraExecutor}.
 *
 * <p>This executor exposes the same operations as {@link CassandraExecutor} but returns each
 * result wrapped in a {@link ContinuableFuture}, executing statements through the driver's
 * {@code executeAsync} API. Instances are obtained from {@link CassandraExecutor#async()}, and
 * the backing synchronous executor is available via {@link #sync()}.</p>
 *
 * @deprecated The Cassandra Java Driver 3.x is no longer maintained. Use
 *             {@link com.landawn.abacus.da.cassandra.AsyncCassandraExecutor} for new
 *             applications (which requires Cassandra Java Driver 4.x).
 * @see CassandraExecutor
 * @see com.landawn.abacus.da.cassandra.AsyncCassandraExecutor
 */
@Deprecated
public final class AsyncCassandraExecutor extends AsyncCassandraExecutorBase<Row, ResultSet, Statement, PreparedStatement, BatchStatement.Type> {

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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     * CassandraExecutor sync = async.sync();              // returns the backing synchronous executor
     * boolean roundTrips = sync == executor;              // returns true (same instance that created the facade)
     * boolean stable = async.sync() == async.sync();      // returns true (always the same instance)
     *
     * // Edge: blocking-style call when an async result is not desired.
     * List<User> users = async.sync().list(User.class, "SELECT * FROM users LIMIT ?", 10); // synchronous
     *
     * // Edge: the facade and the executor it wraps are different objects.
     * boolean distinct = (Object) async != (Object) sync; // returns true
     * }</pre>
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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: bind a positional parameter, then consume the streamed rows.
     * ContinuableFuture<Stream<Object[]>> future =
     *     async.stream("SELECT id, name FROM users WHERE status = ?", "active");
     * Stream<Object[]> rows = future.get();               // blocks until the query completes
     * rows.forEach(row -> System.out.println(row[0] + "=" + row[1]));
     *
     * // Typical: chain transformations off the future without blocking the caller.
     * ContinuableFuture<Long> count = async.stream("SELECT id FROM users").map(Stream::count); // future of the row count
     *
     * // Edge: no matching rows -> the future completes with an empty Stream.
     * long n = async.stream("SELECT id FROM users WHERE id = ?", -1).map(Stream::count).get(); // n == 0
     *
     * // Edge: the statement is prepared synchronously before the async call is issued, so a
     * // malformed query throws directly from async.stream(...) (not from a later get()).
     * async.stream("SELECT FROM");                        // throws the driver parse failure synchronously
     * }</pre>
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
     * Asynchronously executes the given CQL query and returns a future that completes with a
     * {@link Stream} of rows mapped by the supplied row mapper.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: map each row to a domain value using the column definitions and row.
     * BiFunction<ColumnDefinitions, Row, String> nameMapper = (defs, row) -> row.getString("name");
     * ContinuableFuture<Stream<String>> future =
     *     async.stream("SELECT name FROM users WHERE status = ?", nameMapper, "active");
     * List<String> names = future.get().toList();         // blocks, then collects mapped names
     *
     * // Typical: build a record from several columns.
     * ContinuableFuture<Stream<User>> users = async.stream("SELECT id, name FROM users",
     *     (defs, row) -> new User(row.getInt("id"), row.getString("name"))); // future of a Stream<User>
     *
     * // Edge: no matching rows -> the future completes with an empty Stream (mapper never runs).
     * long n = async.stream("SELECT name FROM users WHERE id = ?", nameMapper, -1)
     *               .map(Stream::count).get();             // n == 0
     *
     * // Edge: a mapper that throws makes terminal consumption fail when the Stream is iterated.
     * BiFunction<ColumnDefinitions, Row, String> boom = (defs, row) -> { throw new IllegalStateException(); };
     * async.stream("SELECT name FROM users", boom).get().forEach(x -> {}); // throws IllegalStateException on iteration
     * }</pre>
     *
     * @param <T> the result type produced by the row mapper
     * @param query the CQL query to execute
     * @param rowMapper function that converts the column definitions and each row into a result object
     * @param parameters the query parameters
     * @return a future that completes with a Stream of mapped results
     * @throws IllegalArgumentException if rowMapper is null
     */
    public <T> ContinuableFuture<Stream<T>> stream(final String query, final BiFunction<ColumnDefinitions, Row, T> rowMapper, final Object... parameters) {
        N.checkArgNotNull(rowMapper, "rowMapper");

        return execute(query, parameters).map(resultSet -> Stream.of(resultSet.iterator()).map(cassandraExecutor.createRowMapper(rowMapper)));
    }

    /**
     * Asynchronously executes the given statement and returns a future that completes with a
     * {@link Stream} of rows mapped by the supplied row mapper.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: run a fully-built SimpleStatement (driver 3.x) and map each row.
     * Statement stmt = new SimpleStatement("SELECT id, name FROM users WHERE status = ?", "active");
     * BiFunction<ColumnDefinitions, Row, User> mapper = (defs, row) -> new User(row.getInt("id"), row.getString("name"));
     * ContinuableFuture<Stream<User>> future = async.stream(stmt, mapper); // future of a Stream<User>
     * List<User> users = future.get().toList();                            // blocks, then collects mapped users
     *
     * // Typical: apply a per-statement fetch size, then count rows asynchronously.
     * Statement paged = new SimpleStatement("SELECT id FROM users").setFetchSize(500);
     * ContinuableFuture<Long> total = async.stream(paged, (defs, row) -> row.getInt("id")).map(Stream::count);
     *
     * // Edge: a statement matching no rows -> the future completes with an empty Stream.
     * Statement none = new SimpleStatement("SELECT id FROM users WHERE id = ?", -1);
     * long n = async.stream(none, (defs, row) -> row.getInt("id")).map(Stream::count).get(); // n == 0
     *
     * // Edge: a null statement makes the future complete exceptionally; get() rethrows it wrapped.
     * try {
     *     async.stream((Statement) null, mapper).get();   // throws ExecutionException (cause NullPointerException)
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is a NullPointerException
     * }
     * }</pre>
     *
     * @param <T> the result type produced by the row mapper
     * @param statement the statement to execute
     * @param rowMapper function that converts the column definitions and each row into a result object
     * @return a future that completes with a Stream of mapped results
     * @throws IllegalArgumentException if rowMapper is null
     */
    public <T> ContinuableFuture<Stream<T>> stream(final Statement statement, final BiFunction<ColumnDefinitions, Row, T> rowMapper) {
        N.checkArgNotNull(rowMapper, "rowMapper");

        return execute(statement).map(resultSet -> Stream.of(resultSet.iterator()).map(cassandraExecutor.createRowMapper(rowMapper)));
    }

    /**
     * Asynchronously executes the given CQL query and returns a future that completes with the
     * first row mapped to an instance of {@code targetClass}, or an empty {@link Optional} if
     * no row is returned.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: fetch and map the first matching row to an entity.
     * ContinuableFuture<Optional<User>> future =
     *     async.findFirst(User.class, "SELECT * FROM users WHERE id = ?", 42);
     * Optional<User> user = future.get();                   // blocks until the query completes
     * user.ifPresent(u -> System.out.println(u.getName())); // prints the name only when a row was found
     *
     * // Typical: provide a default off the future without blocking, via map.
     * ContinuableFuture<String> name = async.findFirst(User.class, "SELECT * FROM users WHERE id = ?", 42)
     *     .map(opt -> opt.map(User::getName).orElse("unknown")); // future of the name, or "unknown" if absent
     *
     * // Edge: no matching row -> the future completes with an empty Optional.
     * Optional<User> missing = async.findFirst(User.class, "SELECT * FROM users WHERE id = ?", -1).get();
     * boolean empty = missing.isEmpty();                  // returns true
     *
     * // Edge: the statement is prepared synchronously before the async call is issued, so a
     * // malformed query throws directly from async.findFirst(...) (not from a later get()).
     * async.findFirst(User.class, "SELECT FROM users");   // throws the driver parse failure synchronously
     * }</pre>
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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: read one scalar column from the first row.
     * ContinuableFuture<Nullable<String>> future =
     *     async.queryForSingleValue(String.class, "SELECT name FROM users WHERE id = ?", 42);
     * String name = future.get().orElse("unknown");       // blocks, then unwraps with a default
     *
     * // Typical: convert the column to a numeric type.
     * Nullable<Integer> age =
     *     async.queryForSingleValue(Integer.class, "SELECT age FROM users WHERE id = ?", 42).get();
     *
     * // Edge: no matching row -> the future completes with an empty Nullable.
     * Nullable<String> none =
     *     async.queryForSingleValue(String.class, "SELECT name FROM users WHERE id = ?", -1).get();
     * boolean empty = none.isEmpty();                      // returns true
     *
     * // Edge: a row exists but the column is SQL NULL -> present Nullable holding null (NOT empty).
     * Nullable<String> sqlNull =
     *     async.queryForSingleValue(String.class, "SELECT middle_name FROM users WHERE id = ?", 42).get();
     * boolean presentButNull = sqlNull.isPresent() && sqlNull.orElse("x") == null; // distinguishes null from "no row"
     * }</pre>
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
     * {@code valueClass}, or an empty {@link Optional} if no row is returned. If a row is returned
     * but the value is {@code null}, {@code get()} throws a {@link NullPointerException} (an
     * {@link Optional} cannot hold {@code null}).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: read a column that is expected to be non-null when the row exists.
     * ContinuableFuture<Optional<Long>> future =
     *     async.queryForSingleNonNull(Long.class, "SELECT COUNT(*) FROM users WHERE status = ?", "active");
     * long total = future.get().orElse(0L);               // blocks, then unwraps with a default
     *
     * // Typical: map the value off the future without blocking.
     * ContinuableFuture<String> label = async.queryForSingleNonNull(String.class, "SELECT name FROM users WHERE id = ?", 42)
     *     .map(opt -> opt.orElse("unknown")); // future of the name, or "unknown" if absent
     *
     * // Edge: no matching row -> the future completes with an empty Optional.
     * Optional<String> none = async.queryForSingleNonNull(String.class, "SELECT name FROM users WHERE id = ?", -1).get();
     * boolean empty = none.isEmpty();                      // returns true
     *
     * // Edge: a row exists but the column is SQL NULL -> Optional.of(null) throws NullPointerException.
     * // For this ContinuableFuture the mapping runs inside get(), so get() throws the
     * // NullPointerException directly (it is NOT wrapped in an ExecutionException).
     * Optional<String> ignored = async.queryForSingleNonNull(String.class, "SELECT middle_name FROM users WHERE id = ?", 42).get(); // throws NullPointerException
     * }</pre>
     *
     * @param <T> the value type
     * @param valueClass the class to convert the single value to
     * @param query the CQL query to execute
     * @param parameters the positional query parameters
     * @return a future that completes with an {@code Optional} of the non-null value, or empty
     *         if no row exists; if a row exists but the value is {@code null}, {@code get()} throws
     *         a {@link NullPointerException} directly
     */
    @Override
    public <T> ContinuableFuture<Optional<T>> queryForSingleNonNull(final Class<T> valueClass, final String query, final Object... parameters) {
        return super.queryForSingleNonNull(valueClass, query, parameters);
    }

    /**
     * Asynchronously executes a parameterless CQL query.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: fire a parameterless statement and iterate the ResultSet.
     * ContinuableFuture<ResultSet> future = async.execute("SELECT id, name FROM users");
     * ResultSet rs = future.get();                        // blocks until the statement completes
     * for (Row row : rs) {
     *     System.out.println(row.getInt("id")); // prints the id column of each returned row
     * }
     *
     * // Typical: a DDL/DML statement that returns no rows still completes with a (non-row) ResultSet.
     * async.execute("TRUNCATE users").get();              // blocks until applied
     *
     * // Edge: the statement is prepared synchronously before the async call is issued, so a
     * // malformed query throws directly from async.execute(...) (not from a later get()).
     * async.execute("SELECT FROM");                       // throws the driver parse failure synchronously
     *
     * // Edge: a null query throws NullPointerException synchronously from async.execute(...).
     * async.execute((String) null);                       // throws NullPointerException
     * }</pre>
     *
     * @param query the CQL query to execute
     * @return a future that completes with the {@link ResultSet} produced by the driver
     */
    @Override
    public ContinuableFuture<ResultSet> execute(final String query) {
        return ContinuableFuture.wrap(cassandraExecutor.session().executeAsync(cassandraExecutor.prepareStatement(query)));
    }

    /**
     * Asynchronously executes a CQL query with positional parameters.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: bind one positional parameter and consume the result rows.
     * ContinuableFuture<ResultSet> future =
     *     async.execute("SELECT id, name FROM users WHERE status = ?", "active");
     * ResultSet rs = future.get();                                  // blocks until the query completes
     * rs.forEach(row -> System.out.println(row.getString("name"))); // prints the name column of each row
     *
     * // Typical: bind several positional parameters for an INSERT.
     * async.execute("INSERT INTO users (id, name) VALUES (?, ?)", 1, "Alice").get(); // applied when get() returns
     *
     * // Note: the statement is prepared and bound synchronously; only the read round-trip is
     * // asynchronous. A failure on that round-trip (timeout, no host available) makes the
     * // future complete exceptionally and get() rethrows it wrapped in an ExecutionException.
     *
     * // Edge: because preparing/binding is synchronous, a null query throws
     * // IllegalArgumentException directly from async.execute(...) (not from a later get()).
     * async.execute((String) null, "active");             // throws IllegalArgumentException
     * }</pre>
     *
     * @param query the CQL query to execute
     * @param parameters the positional query parameters
     * @return a future that completes with the {@link ResultSet} produced by the driver
     */
    @Override
    public ContinuableFuture<ResultSet> execute(final String query, final Object... parameters) {
        return ContinuableFuture.wrap(cassandraExecutor.session().executeAsync(cassandraExecutor.prepareStatement(query, parameters)));
    }

    /**
     * Asynchronously executes a CQL query with named parameters.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: bind named parameters from a Map (CQL uses :name placeholders).
     * Map<String, Object> params = N.asMap("email", "alice@example.com");
     * ContinuableFuture<ResultSet> future =
     *     async.execute("SELECT * FROM users WHERE email = :email", params);
     * ResultSet rs = future.get();                        // blocks until the query completes
     *
     * // Typical: multiple named parameters for an INSERT.
     * Map<String, Object> row = N.asMap("id", 1, "name", "Alice");
     * async.execute("INSERT INTO users (id, name) VALUES (:id, :name)", row).get(); // applied when get() returns
     *
     * // Edge: named parameters are resolved synchronously while binding, so an empty map that
     * // omits a required :name throws IllegalArgumentException directly from async.execute(...)
     * // (not from a later get()).
     * async.execute("SELECT * FROM users WHERE email = :email", new HashMap<String, Object>()); // throws IllegalArgumentException
     * }</pre>
     *
     * @param query the CQL query to execute
     * @param parameters a map of named parameter values keyed by parameter name
     * @return a future that completes with the {@link ResultSet} produced by the driver
     */
    @Override
    public ContinuableFuture<ResultSet> execute(final String query, final Map<String, Object> parameters) {
        return ContinuableFuture.wrap(cassandraExecutor.session().executeAsync(cassandraExecutor.prepareStatement(query, parameters)));
    }

    /**
     * Asynchronously executes a pre-built {@link Statement}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: run a fully-built SimpleStatement (driver 3.x) and read the ResultSet.
     * Statement stmt = new SimpleStatement("SELECT id, name FROM users WHERE status = ?", "active");
     * ContinuableFuture<ResultSet> future = async.execute(stmt);
     * ResultSet rs = future.get();                        // blocks until the statement completes
     *
     * // Typical: apply per-statement options such as fetch size before executing.
     * Statement paged = new SimpleStatement("SELECT id FROM users").setFetchSize(500);
     * async.execute(paged).get().forEach(row -> System.out.println(row.getInt("id"))); // prints each id, blocking on get()
     *
     * // Edge: a statement matching no rows completes with an empty (but non-null) ResultSet.
     * Statement none = new SimpleStatement("SELECT id FROM users WHERE id = ?", -1);
     * boolean hasRows = async.execute(none).get().iterator().hasNext(); // returns false
     *
     * // Edge: a null statement makes the future complete exceptionally (cause NullPointerException).
     * try {
     *     async.execute((Statement) null).get();          // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is a NullPointerException
     * }
     * }</pre>
     *
     * @param statement the CQL statement to execute
     * @return a future that completes with the {@link ResultSet} produced by the driver
     */
    @Override
    public ContinuableFuture<ResultSet> execute(final Statement statement) {
        return ContinuableFuture.wrap(cassandraExecutor.session().executeAsync(statement));
    }
}
