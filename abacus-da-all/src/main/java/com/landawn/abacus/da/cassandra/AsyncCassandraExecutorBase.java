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

import static com.landawn.abacus.da.cassandra.CassandraExecutorBase.getKeyNameSet;
import static com.landawn.abacus.da.cassandra.CassandraExecutorBase.getKeyNames;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.landawn.abacus.query.AbstractQueryBuilder.SP;
import com.landawn.abacus.query.QueryUtil;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.util.Clazz;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.u.Nullable;
import com.landawn.abacus.util.u.Optional;
import com.landawn.abacus.util.u.OptionalBoolean;
import com.landawn.abacus.util.u.OptionalByte;
import com.landawn.abacus.util.u.OptionalChar;
import com.landawn.abacus.util.u.OptionalDouble;
import com.landawn.abacus.util.u.OptionalFloat;
import com.landawn.abacus.util.u.OptionalInt;
import com.landawn.abacus.util.u.OptionalLong;
import com.landawn.abacus.util.u.OptionalShort;
import com.landawn.abacus.util.stream.Stream;

/**
 * Abstract base class providing the asynchronous facade for Cassandra executors.
 *
 * <p>This class mirrors the API of {@link CassandraExecutorBase} but returns its results
 * wrapped in {@link ContinuableFuture}, allowing non-blocking execution. Each operation
 * delegates to the wrapped synchronous {@link CassandraExecutorBase} for CQL building and
 * result mapping, while the actual statement execution is performed asynchronously through
 * the abstract {@code execute(...)} methods implemented by concrete subclasses.</p>
 *
 * <p>An instance of this class is obtained from the synchronous executor (for example via
 * {@code CassandraExecutor.async()}) and can be converted back to its synchronous counterpart
 * through {@link #sync()}.</p>
 *
 * @param <RW> the row type for the Cassandra driver version
 * @param <RS> the result set type for the Cassandra driver version
 * @param <ST> the statement type for the Cassandra driver version
 * @param <PS> the prepared statement type for the Cassandra driver version
 * @param <BT> the batch type enum for the Cassandra driver version
 *
 * @see CassandraExecutorBase
 * @see AsyncCassandraExecutor
 */
public abstract class AsyncCassandraExecutorBase<RW, RS extends Iterable<RW>, ST, PS, BT> {

    /**
     * The wrapped synchronous executor that performs CQL building, row mapping, and (via the
     * concrete subclass) the actual driver-level statement execution.
     */
    protected final CassandraExecutorBase<RW, RS, ST, PS, BT> cassandraExecutor;

    /**
     * Creates a new asynchronous facade wrapping the given synchronous executor.
     *
     * @param cassandraExecutor the synchronous executor to delegate query building and result
     *                          mapping to; must not be {@code null}
     */
    protected AsyncCassandraExecutorBase(final CassandraExecutorBase<RW, RS, ST, PS, BT> cassandraExecutor) {
        this.cassandraExecutor = cassandraExecutor;
    }

    /**
     * Returns the underlying synchronous executor that backs this asynchronous facade.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutorBase<?, ?, ?, ?, ?> async = executor.async();
     *
     * // Typical: switch back to blocking calls when an async result is not needed.
     * CassandraExecutorBase<?, ?, ?, ?, ?> sync = async.sync(); // the backing synchronous executor
     *
     * // Typical: sync() is the same instance that created this facade.
     * boolean roundTrips = async.sync() == executor;            // returns true
     *
     * // Edge: sync() always returns the same instance.
     * boolean stable = async.sync() == async.sync();            // returns true
     *
     * // Edge: the facade and the executor it wraps are distinct objects.
     * boolean distinct = (Object) async != (Object) async.sync(); // returns true
     * }</pre>
     *
     * @return the wrapped synchronous {@link CassandraExecutorBase}
     */
    public CassandraExecutorBase<RW, RS, ST, PS, BT> sync() {
        return cassandraExecutor;
    }

    /**
     * Asynchronously retrieves a single entity by its primary key(s), selecting all mapped properties.
     *
     * <p>The {@code ids} are converted to a {@code WHERE} clause based on the primary key
     * definition of {@code targetClass}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: fetch by a single primary key, then block for the result.
     * Optional<User> user = async.get(User.class, 1L).get(); // present if a row with id=1 exists, else empty
     *
     * // Typical: react to completion without blocking the caller.
     * async.get(User.class, 1L).thenAccept(opt -> opt.ifPresent(System.out::println)); // prints the entity if present
     *
     * // Edge: no matching row -> the future completes with an empty Optional.
     * boolean none = async.get(User.class, -1L).get().isEmpty(); // returns true
     *
     * // Edge: more than one row matches -> the future completes exceptionally; get() rethrows it wrapped.
     * try {
     *     async.get(User.class, ambiguousKey).get(); // throws ExecutionException (cause DuplicateResultException)
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is a DuplicateResultException
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class to fetch
     * @param ids the primary key value(s) identifying the row
     * @return a future whose payload is an {@link Optional} containing the entity, or empty if no
     *         row matches; if more than one row matches the future completes exceptionally with
     *         {@link com.landawn.abacus.exception.DuplicateResultException}
     */
    public final <T> ContinuableFuture<Optional<T>> get(final Class<T> targetClass, final Object... ids) {
        return get(targetClass, null, ids);
    }

    /**
     * Asynchronously retrieves a single entity by its primary key(s), selecting only the
     * specified properties.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: project only a couple of columns when fetching by id.
     * Optional<User> user = async.get(User.class, Arrays.asList("id", "name"), 1L).get(); // only id & name populated
     *
     * // Typical: null selectPropNames selects all mapped properties (same as get(class, ids)).
     * Optional<User> full = async.get(User.class, (Collection<String>) null, 1L).get();
     *
     * // Edge: no matching row -> the future completes with an empty Optional.
     * boolean none = async.get(User.class, Arrays.asList("id"), -1L).get().isEmpty(); // returns true
     *
     * // Edge: more than one row matches -> the future completes exceptionally; get() rethrows it wrapped.
     * try {
     *     async.get(User.class, Arrays.asList("id"), ambiguousKey).get(); // throws ExecutionException (cause DuplicateResultException)
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is a DuplicateResultException
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class to fetch
     * @param selectPropNames the property names to include in the SELECT clause, or {@code null}
     *                       to select all mapped properties
     * @param ids the primary key value(s) identifying the row
     * @return a future whose payload is an {@link Optional} containing the entity, or empty if no
     *         row matches
     */
    public final <T> ContinuableFuture<Optional<T>> get(final Class<T> targetClass, final Collection<String> selectPropNames, final Object... ids) {
        return get(targetClass, selectPropNames, CassandraExecutorBase.idsToCondition(targetClass, ids));
    }

    /**
     * Asynchronously retrieves a single entity matching the supplied condition, selecting all
     * mapped properties.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: fetch the single row matching a condition.
     * Optional<User> user = async.get(User.class, Filters.eq("email", "a@b.com")).get();
     *
     * // Typical: combine the future with a default fallback.
     * User u = async.get(User.class, Filters.eq("id", 1L)).get().orElse(User.GUEST);
     *
     * // Edge: no matching row -> the future completes with an empty Optional.
     * boolean none = async.get(User.class, Filters.eq("id", -1L)).get().isEmpty(); // returns true
     *
     * // Edge: more than one row matches -> the future completes exceptionally; get() rethrows it wrapped.
     * try {
     *     async.get(User.class, Filters.eq("status", "active")).get(); // throws ExecutionException (cause DuplicateResultException)
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is a DuplicateResultException
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class to fetch
     * @param whereClause the WHERE condition identifying at most one row
     * @return a future whose payload is an {@link Optional} containing the entity, or empty if no
     *         row matches
     */
    public <T> ContinuableFuture<Optional<T>> get(final Class<T> targetClass, final Condition whereClause) {
        return get(targetClass, null, whereClause);
    }

    /**
     * Asynchronously retrieves a single entity matching the supplied condition, selecting only the
     * specified properties.
     *
     * <p>The underlying SELECT is limited to {@code 2} rows so that a duplicate-result error can
     * be raised when more than one row matches.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: project a subset of columns for the single matching row.
     * Optional<User> user = async.get(User.class, Arrays.asList("id", "name"), Filters.eq("email", "a@b.com")).get();
     *
     * // Typical: null selectPropNames selects all mapped properties.
     * Optional<User> full = async.get(User.class, null, Filters.eq("id", 1L)).get();
     *
     * // Edge: no matching row -> the future completes with an empty Optional.
     * boolean none = async.get(User.class, Arrays.asList("id"), Filters.eq("id", -1L)).get().isEmpty(); // returns true
     *
     * // Edge: more than one row matches -> the future completes exceptionally; get() rethrows it wrapped.
     * try {
     *     async.get(User.class, Arrays.asList("id"), Filters.eq("status", "active")).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is a DuplicateResultException
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class to fetch
     * @param selectPropNames the property names to include in the SELECT clause, or {@code null}
     *                       to select all mapped properties
     * @param whereClause the WHERE condition identifying at most one row
     * @return a future whose payload is an {@link Optional} containing the entity, or empty if no
     *         row matches
     */
    public <T> ContinuableFuture<Optional<T>> get(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP cp = cassandraExecutor.prepareQuery(targetClass, selectPropNames, whereClause, 2);

        return execute(cp).map(resultSet -> Optional.ofNullable(cassandraExecutor.fetchOnlyOne(targetClass, resultSet)));
    }

    /**
     * Asynchronously retrieves a single entity by its primary key(s), returning the entity
     * directly (or {@code null} when no row matches).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: fetch by primary key; payload is the entity itself (not an Optional).
     * User user = async.gett(User.class, 1L).get(); // the matching entity, or null
     *
     * // Typical: null-check the resolved payload.
     * User maybe = async.gett(User.class, 1L).get();
     * boolean exists = maybe != null; // true when a row with id=1 exists
     *
     * // Edge: no matching row -> the future completes with null.
     * User missing = async.gett(User.class, -1L).get(); // missing == null
     *
     * // Edge: more than one row matches -> the future completes exceptionally; get() rethrows it wrapped.
     * try {
     *     async.gett(User.class, ambiguousKey).get(); // throws ExecutionException (cause DuplicateResultException)
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is a DuplicateResultException
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class to fetch
     * @param ids the primary key value(s) identifying the row
     * @return a future whose payload is the entity instance, or {@code null} if no row matches
     */
    public final <T> ContinuableFuture<T> gett(final Class<T> targetClass, final Object... ids) {
        return gett(targetClass, null, ids);
    }

    /**
     * Asynchronously retrieves a single entity by its primary key(s), selecting only the
     * specified properties; returns the entity directly (or {@code null} when no row matches).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: fetch only a couple of columns by id.
     * User user = async.gett(User.class, Arrays.asList("id", "name"), 1L).get(); // entity with only id & name, or null
     *
     * // Typical: null selectPropNames selects all mapped properties.
     * User full = async.gett(User.class, (Collection<String>) null, 1L).get();
     *
     * // Edge: no matching row -> the future completes with null.
     * User missing = async.gett(User.class, Arrays.asList("id"), -1L).get(); // missing == null
     *
     * // Edge: more than one row matches -> the future completes exceptionally; get() rethrows it wrapped.
     * try {
     *     async.gett(User.class, Arrays.asList("id"), ambiguousKey).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is a DuplicateResultException
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class to fetch
     * @param selectPropNames the property names to include in the SELECT clause, or {@code null}
     *                       to select all mapped properties
     * @param ids the primary key value(s) identifying the row
     * @return a future whose payload is the entity instance, or {@code null} if no row matches
     */
    public final <T> ContinuableFuture<T> gett(final Class<T> targetClass, final Collection<String> selectPropNames, final Object... ids) {
        return gett(targetClass, selectPropNames, CassandraExecutorBase.idsToCondition(targetClass, ids));
    }

    /**
     * Asynchronously retrieves a single entity matching the supplied condition; returns the
     * entity directly (or {@code null} when no row matches).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: fetch the single row matching a condition; payload is the entity itself.
     * User user = async.gett(User.class, Filters.eq("email", "a@b.com")).get(); // the entity, or null
     *
     * // Typical: handle the resolved value in a continuation.
     * async.gett(User.class, Filters.eq("id", 1L)).thenAccept(u -> { if (u != null) process(u); });
     *
     * // Edge: no matching row -> the future completes with null.
     * User missing = async.gett(User.class, Filters.eq("id", -1L)).get(); // missing == null
     *
     * // Edge: more than one row matches -> the future completes exceptionally; get() rethrows it wrapped.
     * try {
     *     async.gett(User.class, Filters.eq("status", "active")).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is a DuplicateResultException
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class to fetch
     * @param whereClause the WHERE condition identifying at most one row
     * @return a future whose payload is the entity instance, or {@code null} if no row matches
     */
    public <T> ContinuableFuture<T> gett(final Class<T> targetClass, final Condition whereClause) {
        return gett(targetClass, null, whereClause);
    }

    /**
     * Asynchronously retrieves a single entity matching the supplied condition, selecting only
     * the specified properties; returns the entity directly (or {@code null} when no row matches).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: fetch a projected entity matching a condition.
     * User user = async.gett(User.class, Arrays.asList("id", "name"), Filters.eq("email", "a@b.com")).get();
     *
     * // Typical: null selectPropNames selects all mapped properties.
     * User full = async.gett(User.class, null, Filters.eq("id", 1L)).get();
     *
     * // Edge: no matching row -> the future completes with null.
     * User missing = async.gett(User.class, Arrays.asList("id"), Filters.eq("id", -1L)).get(); // missing == null
     *
     * // Edge: more than one row matches -> the future completes exceptionally; get() rethrows it wrapped.
     * try {
     *     async.gett(User.class, Arrays.asList("id"), Filters.eq("status", "active")).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is a DuplicateResultException
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class to fetch
     * @param selectPropNames the property names to include in the SELECT clause, or {@code null}
     *                       to select all mapped properties
     * @param whereClause the WHERE condition identifying at most one row
     * @return a future whose payload is the entity instance, or {@code null} if no row matches
     */
    public <T> ContinuableFuture<T> gett(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP cp = cassandraExecutor.prepareQuery(targetClass, selectPropNames, whereClause, 2);

        return execute(cp).map(resultSet -> cassandraExecutor.fetchOnlyOne(targetClass, resultSet));
    }

    /**
     * Asynchronously inserts the given entity using all of its non-{@code null} properties as
     * column values.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: fire-and-forget insert, then block to confirm completion.
     * User u = new User(1L, "Alice");
     * async.insert(u).get(); // returns the driver result set once applied
     *
     * // Typical: chain a follow-up action after the insert completes.
     * async.insert(new User(2L, "Bob")).thenRun(() -> log.info("inserted")); // runs after the INSERT completes
     *
     * // Edge: a null entity is rejected eagerly (synchronously), before any future is returned.
     * assertThrows(NullPointerException.class, () -> async.insert(null)); // throws NullPointerException
     *
     * // Edge: a CQL/driver failure (e.g. unknown table) surfaces as an ExecutionException on get().
     * try {
     *     async.insert(entityForMissingTable).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param entity the entity instance to insert
     * @return a future whose payload is the driver result set produced by the INSERT
     */
    public ContinuableFuture<RS> insert(final Object entity) {
        return execute(cassandraExecutor.prepareInsert(entity));
    }

    /**
     * Asynchronously inserts a new row into the table mapped by {@code targetClass} using the
     * supplied property name / value map.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: insert from a property map without constructing an entity.
     * Map<String, Object> props = new HashMap<>();
     * props.put("id", 1L);
     * props.put("name", "Alice");
     * async.insert(User.class, props).get(); // returns the driver result set once applied
     *
     * // Typical: build the map fluently for a partial-column insert.
     * async.insert(User.class, N.asMap("id", 2L, "name", "Bob")).get(); // returns the driver result set
     *
     * // Edge: an empty map is rejected eagerly (synchronously), before any future is returned.
     * assertThrows(IllegalArgumentException.class, () -> async.insert(User.class, new HashMap<>())); // throws IllegalArgumentException
     *
     * // Edge: a null props map is likewise rejected eagerly.
     * assertThrows(IllegalArgumentException.class, () -> async.insert(User.class, (Map<String, Object>) null)); // throws IllegalArgumentException
     * }</pre>
     *
     * @param targetClass the entity class identifying the target table and column mappings
     * @param props the column-name to column-value map to insert
     * @return a future whose payload is the driver result set produced by the INSERT
     * @throws IllegalArgumentException if {@code props} is {@code null} or empty
     */
    public ContinuableFuture<RS> insert(final Class<?> targetClass, final Map<String, Object> props) {
        return execute(cassandraExecutor.prepareInsert(targetClass, props));
    }

    /**
     * Asynchronously inserts a collection of entities as a single CQL batch.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: batch-insert several entities atomically (LOGGED batch).
     * List<User> users = Arrays.asList(new User(1L, "Alice"), new User(2L, "Bob"));
     * async.batchInsert(users, BatchType.LOGGED).get(); // returns the driver result set
     *
     * // Typical: an UNLOGGED batch for higher-throughput same-partition writes.
     * async.batchInsert(users, BatchType.UNLOGGED).get(); // returns the driver result set
     *
     * // Edge: no base-level empty-check; an empty list produces an empty batch (subclass-dependent).
     * async.batchInsert(Collections.emptyList(), BatchType.LOGGED).get(); // returns the (empty) batch result set
     * }</pre>
     *
     * @param entities the entities to insert
     * @param type the batch type (e.g. {@code LOGGED}, {@code UNLOGGED}, {@code COUNTER}) defined
     *             by the underlying driver
     * @return a future whose payload is the driver result set produced by the batch INSERT
     */
    public ContinuableFuture<RS> batchInsert(final Collection<?> entities, final BT type) {
        return execute(cassandraExecutor.prepareBatchInsertStatement(entities, type));
    }

    /**
     * Asynchronously inserts a collection of property maps as a single CQL batch against the
     * table mapped by {@code targetClass}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: batch-insert several rows from property maps.
     * List<Map<String, Object>> rows = Arrays.asList(
     *     N.asMap("id", 1L, "name", "Alice"),
     *     N.asMap("id", 2L, "name", "Bob"));
     * async.batchInsert(User.class, rows, BatchType.LOGGED).get(); // returns the driver result set
     *
     * // Typical: an UNLOGGED batch for same-partition writes.
     * async.batchInsert(User.class, rows, BatchType.UNLOGGED).get(); // returns the driver result set
     *
     * // Edge: no base-level empty-check; an empty list produces an empty batch (subclass-dependent).
     * async.batchInsert(User.class, Collections.emptyList(), BatchType.LOGGED).get(); // returns the (empty) batch result set
     * }</pre>
     *
     * @param targetClass the entity class identifying the target table and column mappings
     * @param propsList the list of column-name to column-value maps to insert
     * @param type the batch type (e.g. {@code LOGGED}, {@code UNLOGGED}, {@code COUNTER})
     * @return a future whose payload is the driver result set produced by the batch INSERT
     */
    public ContinuableFuture<RS> batchInsert(final Class<?> targetClass, final Collection<? extends Map<String, Object>> propsList, final BT type) {
        return execute(cassandraExecutor.prepareBatchInsertStatement(targetClass, propsList, type));
    }

    /**
     * Asynchronously updates the row identified by the entity's primary key, setting all
     * non-key properties of the entity.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: persist all non-key fields of a modified entity.
     * User u = async.gett(User.class, 1L).get();
     * u.setName("Alice Updated");
     * async.update(u).get(); // returns the driver result set
     *
     * // Typical: chain a continuation after the update completes.
     * async.update(u).thenRun(() -> log.info("updated")); // runs after the UPDATE completes
     *
     * // Edge: a null entity is rejected eagerly (synchronously), before any future is returned.
     * assertThrows(NullPointerException.class, () -> async.update((Object) null)); // throws NullPointerException
     *
     * // Edge: a CQL/driver failure surfaces as an ExecutionException on get().
     * try {
     *     async.update(entityForMissingTable).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param entity the entity whose primary-key fields are used in the WHERE clause and whose
     *               non-key fields supply the new values
     * @return a future whose payload is the driver result set produced by the UPDATE
     */
    public ContinuableFuture<RS> update(final Object entity) {
        final Class<?> entityClass = entity.getClass();
        final Set<String> keyNameSet = getKeyNameSet(entityClass);
        final Collection<String> updatePropNames = QueryUtil.getUpdatePropNames(entityClass, keyNameSet);

        return update(entity, updatePropNames);
    }

    /**
     * Asynchronously updates the row identified by the entity's primary key, setting only the
     * named properties.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: update only selected columns of an entity.
     * User u = async.gett(User.class, 1L).get();
     * u.setName("Alice Updated");
     * async.update(u, Arrays.asList("name")).get(); // returns the driver result set
     *
     * // Typical: update two named fields.
     * async.update(u, Arrays.asList("name", "email")).get(); // returns the driver result set
     *
     * // Edge: an empty propNamesToUpdate is rejected eagerly (synchronously), before any future is returned.
     * User e = new User(1L, "x");
     * assertThrows(IllegalArgumentException.class, () -> async.update(e, Collections.emptyList())); // throws IllegalArgumentException
     *
     * // Edge: a null propNamesToUpdate is likewise rejected eagerly.
     * assertThrows(IllegalArgumentException.class, () -> async.update(e, (Collection<String>) null)); // throws IllegalArgumentException
     * }</pre>
     *
     * @param entity the entity whose primary-key fields are used in the WHERE clause and whose
     *               selected fields supply the new values
     * @param propNamesToUpdate the names of the properties to update; must be non-empty
     * @return a future whose payload is the driver result set produced by the UPDATE
     * @throws IllegalArgumentException if {@code propNamesToUpdate} is {@code null} or empty
     */
    public ContinuableFuture<RS> update(final Object entity, final Collection<String> propNamesToUpdate) {
        N.checkArgument(N.notEmpty(propNamesToUpdate), "'propNamesToUpdate' can't be null or empty");

        return execute(cassandraExecutor.prepareUpdate(entity, propNamesToUpdate));
    }

    /**
     * Asynchronously updates rows matching {@code whereClause} on the table mapped by
     * {@code targetClass}, setting the supplied column values.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: set column values on the row(s) matching a condition.
     * Map<String, Object> props = N.asMap("name", "Alice Updated");
     * async.update(User.class, props, Filters.eq("id", 1L)).get(); // returns the driver result set
     *
     * // Typical: update multiple columns at once.
     * async.update(User.class, N.asMap("name", "x", "status", "active"), Filters.eq("id", 2L)).get(); // returns the driver result set
     *
     * // Edge: a condition matching no rows still completes normally (no row changed).
     * async.update(User.class, props, Filters.eq("id", -1L)).get(); // returns a result set; no row updated
     *
     * // Edge: a CQL/driver failure surfaces as an ExecutionException on get().
     * try {
     *     async.update(User.class, props, Filters.eq("missingCol", 1)).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param targetClass the entity class identifying the target table
     * @param props the column-name to new-value map to apply
     * @param whereClause the WHERE condition selecting rows to update
     * @return a future whose payload is the driver result set produced by the UPDATE
     */
    public ContinuableFuture<RS> update(final Class<?> targetClass, final Map<String, Object> props, final Condition whereClause) {
        return execute(cassandraExecutor.prepareUpdate(targetClass, props, whereClause));
    }

    /**
     * Asynchronously executes the supplied parameterized CQL update statement.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: run a parameterized UPDATE and block for completion.
     * async.update("UPDATE users SET name = ? WHERE id = ?", "Alice", 1L).get(); // returns the driver result set
     *
     * // Typical: this overload also runs INSERT/DELETE-style statements.
     * async.update("DELETE FROM users WHERE id = ?", 2L).get();
     *
     * // Edge: a statement affecting no rows still completes normally.
     * async.update("UPDATE users SET name = ? WHERE id = ?", "x", -1L).get(); // returns a result set; no row changed
     *
     * // Edge: a malformed query makes the future complete exceptionally; get() rethrows it wrapped.
     * try {
     *     async.update("UPDATE SET").get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver/parse failure
     * }
     * }</pre>
     *
     * @param query the parameterized CQL UPDATE / INSERT / DELETE statement
     * @param parameters the parameter values to bind
     * @return a future whose payload is the driver result set produced by the statement
     */
    public ContinuableFuture<RS> update(final String query, final Object... parameters) {
        return execute(query, parameters);
    }

    /**
     * Asynchronously updates a collection of entities as a single CQL batch, using each entity's
     * primary-key fields in the WHERE clause and all non-key fields as the SET targets.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: batch-update all non-key fields of several entities.
     * List<User> users = Arrays.asList(new User(1L, "A2"), new User(2L, "B2"));
     * async.batchUpdate(users, BatchType.LOGGED).get(); // returns the driver result set
     *
     * // Typical: an UNLOGGED batch for same-partition updates.
     * async.batchUpdate(users, BatchType.UNLOGGED).get(); // returns the driver result set
     *
     * // Edge: an empty collection is rejected eagerly (synchronously), before any future is returned.
     * assertThrows(IllegalArgumentException.class, () -> async.batchUpdate(Collections.emptyList(), BatchType.LOGGED)); // throws IllegalArgumentException
     *
     * // Edge: a null first element is also rejected eagerly.
     * assertThrows(IllegalArgumentException.class, () -> async.batchUpdate(Arrays.asList((User) null), BatchType.LOGGED)); // throws IllegalArgumentException
     * }</pre>
     *
     * @param entities the entities to update; must be non-empty and homogeneous
     * @param type the batch type (e.g. {@code LOGGED}, {@code UNLOGGED}, {@code COUNTER})
     * @return a future whose payload is the driver result set produced by the batch UPDATE
     * @throws IllegalArgumentException if {@code entities} is {@code null} / empty, or its first
     *         element is {@code null}
     */
    public ContinuableFuture<RS> batchUpdate(final Collection<?> entities, final BT type) {
        N.checkArgument(N.notEmpty(entities), "'entities' can't be null or empty.");

        final Object firstEntity = N.firstOrNullIfEmpty(entities);
        N.checkArgNotNull(firstEntity, "The first entity in the collection can't be null.");
        final Class<?> entityClass = firstEntity.getClass();
        final Set<String> keyNameSet = getKeyNameSet(entityClass);
        final Collection<String> updatePropNames = QueryUtil.getUpdatePropNames(entityClass, keyNameSet);

        return batchUpdate(entities, updatePropNames, type);
    }

    /**
     * Asynchronously updates a collection of entities as a single CQL batch, setting only the
     * named properties.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: batch-update only the named column(s) across entities.
     * List<User> users = Arrays.asList(new User(1L, "A2"), new User(2L, "B2"));
     * async.batchUpdate(users, Arrays.asList("name"), BatchType.LOGGED).get(); // returns the driver result set
     *
     * // Typical: update two named columns in one batch.
     * async.batchUpdate(users, Arrays.asList("name", "status"), BatchType.UNLOGGED).get(); // returns the driver result set
     *
     * // Edge: an empty propNamesToUpdate is rejected eagerly (synchronously), before any future is returned.
     * assertThrows(IllegalArgumentException.class, () -> async.batchUpdate(users, Collections.emptyList(), BatchType.LOGGED)); // throws IllegalArgumentException
     *
     * // Edge: a null propNamesToUpdate is likewise rejected eagerly.
     * assertThrows(IllegalArgumentException.class, () -> async.batchUpdate(users, (Collection<String>) null, BatchType.LOGGED)); // throws IllegalArgumentException
     * }</pre>
     *
     * @param entities the entities to update; must be non-empty
     * @param propNamesToUpdate the names of the properties to update; must be non-empty
     * @param type the batch type (e.g. {@code LOGGED}, {@code UNLOGGED}, {@code COUNTER})
     * @return a future whose payload is the driver result set produced by the batch UPDATE
     * @throws IllegalArgumentException if {@code propNamesToUpdate} is {@code null} or empty
     */
    public ContinuableFuture<RS> batchUpdate(final Collection<?> entities, final Collection<String> propNamesToUpdate, final BT type) {
        N.checkArgument(N.notEmpty(propNamesToUpdate), "'propNamesToUpdate' can't be null or empty");

        return execute(cassandraExecutor.prepareBatchUpdateStatement(entities, propNamesToUpdate, type));
    }

    /**
     * Asynchronously applies a list of property-map updates as a single CQL batch against the
     * table mapped by {@code targetClass}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: batch-apply per-row property maps (each map must carry its key column(s)).
     * List<Map<String, Object>> rows = Arrays.asList(
     *     N.asMap("id", 1L, "name", "A2"),
     *     N.asMap("id", 2L, "name", "B2"));
     * async.batchUpdate(User.class, rows, BatchType.LOGGED).get(); // returns the driver result set
     *
     * // Typical: an UNLOGGED batch for same-partition updates.
     * async.batchUpdate(User.class, rows, BatchType.UNLOGGED).get(); // returns the driver result set
     *
     * // Edge: no base-level empty-check; an empty list produces an empty batch (subclass-dependent).
     * async.batchUpdate(User.class, Collections.emptyList(), BatchType.LOGGED).get(); // returns the (empty) batch result set
     * }</pre>
     *
     * @param targetClass the entity class identifying the target table
     * @param propsList the list of column-name to new-value maps to apply
     * @param type the batch type (e.g. {@code LOGGED}, {@code UNLOGGED}, {@code COUNTER})
     * @return a future whose payload is the driver result set produced by the batch UPDATE
     */
    public ContinuableFuture<RS> batchUpdate(final Class<?> targetClass, final Collection<? extends Map<String, Object>> propsList, final BT type) {
        return execute(cassandraExecutor.prepareBatchUpdateStatement(targetClass, propsList, type));
    }

    /**
     * Asynchronously executes the same parameterized CQL statement once per row of parameters
     * as a single CQL batch.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: run one CQL statement many times in a batch, one parameter row per invocation.
     * List<Object[]> rows = Arrays.asList(new Object[] { "A2", 1L }, new Object[] { "B2", 2L });
     * async.batchUpdate("UPDATE users SET name = ? WHERE id = ?", rows, BatchType.LOGGED).get(); // returns the driver result set
     *
     * // Typical: an UNLOGGED batch of same-partition statements.
     * async.batchUpdate("UPDATE users SET status = ? WHERE id = ?", rows, BatchType.UNLOGGED).get();
     *
     * // Edge: no base-level empty-check; an empty parametersList produces an empty batch (subclass-dependent).
     * async.batchUpdate("UPDATE users SET name = ? WHERE id = ?", Collections.emptyList(), BatchType.LOGGED).get(); // returns the (empty) batch result set
     *
     * // Edge: a malformed query likewise completes the future exceptionally.
     * try {
     *     async.batchUpdate("UPDATE SET", rows, BatchType.LOGGED).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver/parse failure
     * }
     * }</pre>
     *
     * @param query the parameterized CQL statement to batch-execute
     * @param parametersList the parameter rows; each element supplies the bind values for one
     *                       invocation of {@code query}
     * @param type the batch type (e.g. {@code LOGGED}, {@code UNLOGGED}, {@code COUNTER})
     * @return a future whose payload is the driver result set produced by the batch execution
     */
    public ContinuableFuture<RS> batchUpdate(final String query, final Collection<?> parametersList, final BT type) {
        return execute(cassandraExecutor.prepareBatchUpdateStatement(query, parametersList, type));
    }

    /**
     * Asynchronously deletes the row identified by the entity's primary key.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: delete the row that this entity represents (by its primary key).
     * User u = new User(1L, "Alice");
     * async.delete(u).get(); // returns the driver result set
     *
     * // Typical: delete then chain a continuation.
     * async.delete(new User(2L, "Bob")).thenRun(() -> log.info("deleted")); // runs after the DELETE completes
     *
     * // Edge: a null entity is rejected eagerly (synchronously), before any future is returned.
     * assertThrows(NullPointerException.class, () -> async.delete((Object) null)); // throws NullPointerException
     *
     * // Edge: a CQL/driver failure surfaces as an ExecutionException on get().
     * try {
     *     async.delete(entityForMissingTable).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param entity the entity whose primary-key fields identify the row to delete
     * @return a future whose payload is the driver result set produced by the DELETE
     */
    public ContinuableFuture<RS> delete(final Object entity) {
        return delete(entity, null);
    }

    /**
     * Asynchronously deletes the row (or specific columns within it) identified by the entity's
     * primary key.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     * User u = new User(1L, "Alice");
     *
     * // Typical: delete only specific columns of the row (set them to null).
     * async.delete(u, Arrays.asList("name")).get(); // returns the driver result set
     *
     * // Typical: null propNamesToDelete deletes the entire row.
     * async.delete(u, (Collection<String>) null).get(); // returns the driver result set
     *
     * // Edge: a non-null but empty propNamesToDelete is rejected eagerly (synchronously).
     * assertThrows(IllegalArgumentException.class, () -> async.delete(u, Collections.emptyList())); // throws IllegalArgumentException
     *
     * // Edge: a null entity is rejected eagerly (synchronously), before any future is returned.
     * assertThrows(NullPointerException.class, () -> async.delete((Object) null, Arrays.asList("name"))); // throws NullPointerException
     * }</pre>
     *
     * @param entity the entity whose primary-key fields identify the target row
     * @param propNamesToDelete the columns to delete; {@code null} deletes the entire row, a
     *                          non-{@code null} value must be non-empty
     * @return a future whose payload is the driver result set produced by the DELETE
     * @throws IllegalArgumentException if {@code propNamesToDelete} is non-{@code null} and empty
     */
    public ContinuableFuture<RS> delete(final Object entity, final Collection<String> propNamesToDelete) {
        N.checkArgument(propNamesToDelete == null || N.notEmpty(propNamesToDelete), "'propNamesToDelete' can't be null or empty");

        return delete(entity.getClass(), propNamesToDelete, CassandraExecutorBase.entityToCondition(entity));
    }

    /**
     * Asynchronously deletes the row(s) identified by the supplied primary key value(s) from the
     * table mapped by {@code targetClass}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: delete by primary key.
     * async.delete(User.class, 1L).get(); // returns the driver result set
     *
     * // Typical: delete then react to completion.
     * async.delete(User.class, 2L).thenRun(() -> log.info("deleted")); // runs after the DELETE completes
     *
     * // Edge: deleting a non-existent key still completes normally.
     * async.delete(User.class, -1L).get(); // returns a result set; nothing removed
     *
     * // Edge: an id count that does not match the entity's key columns is rejected eagerly (synchronously).
     * assertThrows(IllegalArgumentException.class, () -> async.delete(User.class, 1L, 2L)); // throws IllegalArgumentException
     * }</pre>
     *
     * @param targetClass the entity class identifying the target table
     * @param ids the primary key value(s) identifying the row(s)
     * @return a future whose payload is the driver result set produced by the DELETE
     */
    public final ContinuableFuture<RS> delete(final Class<?> targetClass, final Object... ids) {
        return delete(targetClass, null, ids);
    }

    /**
     * Asynchronously deletes specific columns from the row(s) identified by the supplied primary
     * key value(s).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: clear specific column(s) of the row identified by its key.
     * async.delete(User.class, Arrays.asList("name"), 1L).get(); // returns the driver result set
     *
     * // Typical: null propNamesToDelete removes the entire row.
     * async.delete(User.class, (Collection<String>) null, 2L).get(); // returns the driver result set
     *
     * // Edge: a non-null but empty propNamesToDelete is rejected eagerly (synchronously).
     * assertThrows(IllegalArgumentException.class, () -> async.delete(User.class, Collections.emptyList(), 1L)); // throws IllegalArgumentException
     *
     * // Edge: an id count not matching the entity's key columns is also rejected eagerly.
     * assertThrows(IllegalArgumentException.class, () -> async.delete(User.class, Arrays.asList("name"), 1L, 2L)); // throws IllegalArgumentException
     * }</pre>
     *
     * @param targetClass the entity class identifying the target table
     * @param propNamesToDelete the columns to delete; {@code null} deletes the entire row, a
     *                          non-{@code null} value must be non-empty
     * @param ids the primary key value(s) identifying the row(s)
     * @return a future whose payload is the driver result set produced by the DELETE
     * @throws IllegalArgumentException if {@code propNamesToDelete} is non-{@code null} and empty
     */
    public final ContinuableFuture<RS> delete(final Class<?> targetClass, final Collection<String> propNamesToDelete, final Object... ids) {
        N.checkArgument(propNamesToDelete == null || N.notEmpty(propNamesToDelete), "'propNamesToDelete' can't be null or empty");

        return delete(targetClass, propNamesToDelete, CassandraExecutorBase.idsToCondition(targetClass, ids));
    }

    /**
     * Asynchronously deletes the row(s) matching {@code whereClause} from the table mapped by
     * {@code targetClass}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: delete the row(s) selected by a condition.
     * async.delete(User.class, Filters.eq("id", 1L)).get(); // returns the driver result set
     *
     * // Typical: delete then chain a continuation.
     * async.delete(User.class, Filters.eq("status", "inactive")).thenRun(() -> log.info("purged")); // runs after the DELETE completes
     *
     * // Edge: a condition matching no rows still completes normally.
     * async.delete(User.class, Filters.eq("id", -1L)).get(); // returns a result set; nothing removed
     *
     * // Edge: a CQL/driver failure surfaces as an ExecutionException on get().
     * try {
     *     async.delete(User.class, Filters.eq("missingCol", 1)).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param targetClass the entity class identifying the target table
     * @param whereClause the WHERE condition selecting rows to delete
     * @return a future whose payload is the driver result set produced by the DELETE
     */
    public ContinuableFuture<RS> delete(final Class<?> targetClass, final Condition whereClause) {
        return delete(targetClass, null, whereClause);
    }

    /**
     * Asynchronously deletes specific columns from the row(s) matching {@code whereClause} on the
     * table mapped by {@code targetClass}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: clear specific column(s) of the row(s) matching a condition.
     * async.delete(User.class, Arrays.asList("name"), Filters.eq("id", 1L)).get(); // returns the driver result set
     *
     * // Typical: null propNamesToDelete removes the whole matching row(s).
     * async.delete(User.class, null, Filters.eq("status", "inactive")).get(); // returns the driver result set
     *
     * // Edge: a non-null but empty propNamesToDelete is rejected eagerly (synchronously).
     * assertThrows(IllegalArgumentException.class,
     *     () -> async.delete(User.class, Collections.emptyList(), Filters.eq("id", 1L))); // throws IllegalArgumentException
     *
     * // Edge: a CQL/driver failure surfaces as an ExecutionException on get().
     * try {
     *     async.delete(User.class, Arrays.asList("name"), Filters.eq("missingCol", 1)).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param targetClass the entity class identifying the target table
     * @param propNamesToDelete the columns to delete; {@code null} deletes the entire row, a
     *                          non-{@code null} value must be non-empty
     * @param whereClause the WHERE condition selecting rows to delete
     * @return a future whose payload is the driver result set produced by the DELETE
     * @throws IllegalArgumentException if {@code propNamesToDelete} is non-{@code null} and empty
     */
    public ContinuableFuture<RS> delete(final Class<?> targetClass, final Collection<String> propNamesToDelete, final Condition whereClause) {
        N.checkArgument(propNamesToDelete == null || N.notEmpty(propNamesToDelete), "'propNamesToDelete' can't be null or empty");

        return execute(cassandraExecutor.prepareDelete(targetClass, propNamesToDelete, whereClause));
    }

    /**
     * Asynchronously deletes all of the supplied entities in a single DELETE whose WHERE clause
     * matches every entity's primary key.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: delete several entities (by their keys) in one DELETE.
     * List<User> users = Arrays.asList(new User(1L, "A"), new User(2L, "B"));
     * async.batchDelete(users).get(); // returns the driver result set
     *
     * // Typical: delete then react to completion.
     * async.batchDelete(users).thenRun(() -> log.info("batch deleted")); // runs after the DELETE completes
     *
     * // Edge: an empty collection is rejected eagerly (synchronously), before any future is returned.
     * assertThrows(IllegalArgumentException.class, () -> async.batchDelete(Collections.emptyList())); // throws IllegalArgumentException
     *
     * // Edge: a null first element is also rejected eagerly.
     * assertThrows(IllegalArgumentException.class, () -> async.batchDelete(Arrays.asList((User) null))); // throws IllegalArgumentException
     * }</pre>
     *
     * @param entities the entities to delete; must be non-empty and homogeneous
     * @return a future whose payload is the driver result set produced by the DELETE
     * @throws IllegalArgumentException if {@code entities} is {@code null} / empty, or its first
     *         element is {@code null}
     */
    public ContinuableFuture<RS> batchDelete(final Collection<?> entities) {
        N.checkArgument(N.notEmpty(entities), "'entities' can't be null or empty.");

        final Object firstEntity = N.firstOrNullIfEmpty(entities);
        N.checkArgNotNull(firstEntity, "The first entity in the collection can't be null.");
        final Class<?> entityClass = firstEntity.getClass();
        final Condition cond = CassandraExecutorBase.entityToCondition(entityClass, entities);

        return delete(entityClass, cond);
    }

    /**
     * Asynchronously deletes specific columns from all of the supplied entities in a single
     * DELETE whose WHERE clause matches every entity's primary key.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     * List<User> users = Arrays.asList(new User(1L, "A"), new User(2L, "B"));
     *
     * // Typical: clear specific column(s) of several rows in one DELETE.
     * async.batchDelete(users, Arrays.asList("name")).get(); // returns the driver result set
     *
     * // Typical: null propNamesToDelete removes the whole rows.
     * async.batchDelete(users, (Collection<String>) null).get(); // returns the driver result set
     *
     * // Edge: an empty entities collection is rejected eagerly (synchronously).
     * assertThrows(IllegalArgumentException.class, () -> async.batchDelete(Collections.emptyList(), Arrays.asList("name"))); // throws IllegalArgumentException
     *
     * // Edge: a non-null but empty propNamesToDelete is also rejected eagerly.
     * assertThrows(IllegalArgumentException.class, () -> async.batchDelete(users, Collections.emptyList())); // throws IllegalArgumentException
     * }</pre>
     *
     * @param entities the entities to delete from; must be non-empty and homogeneous
     * @param propNamesToDelete the columns to delete; {@code null} deletes the entire row, a
     *                          non-{@code null} value must be non-empty
     * @return a future whose payload is the driver result set produced by the DELETE
     * @throws IllegalArgumentException if {@code entities} is empty or its first element is
     *         {@code null}, or if {@code propNamesToDelete} is non-{@code null} and empty
     */
    public ContinuableFuture<RS> batchDelete(final Collection<?> entities, final Collection<String> propNamesToDelete) {
        N.checkArgument(N.notEmpty(entities), "'entities' can't be null or empty.");
        N.checkArgument(propNamesToDelete == null || N.notEmpty(propNamesToDelete), "'propNamesToDelete' can't be null or empty");

        final Object firstEntity = N.firstOrNullIfEmpty(entities);
        N.checkArgNotNull(firstEntity, "The first entity in the collection can't be null.");
        final Class<?> entityClass = firstEntity.getClass();
        final Condition cond = CassandraExecutorBase.entityToCondition(entityClass, entities);

        return delete(entityClass, propNamesToDelete, cond);
    }

    /**
     * Asynchronously checks whether any row matches the supplied primary key value(s).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: existence check by primary key.
     * boolean present = async.exists(User.class, 1L).get(); // true if a row with id=1 exists
     *
     * // Typical: branch on the resolved boolean in a continuation.
     * async.exists(User.class, 1L).thenAccept(found -> { if (found) log.info("present"); });
     *
     * // Edge: a non-existent key resolves to false.
     * boolean none = async.exists(User.class, -1L).get(); // returns false
     *
     * // Edge: an id count not matching the entity's key columns is rejected eagerly (synchronously).
     * assertThrows(IllegalArgumentException.class, () -> async.exists(User.class, 1L, 2L)); // throws IllegalArgumentException
     * }</pre>
     *
     * @param targetClass the entity class identifying the target table
     * @param ids the primary key value(s) to look up
     * @return a future whose payload is {@code true} if at least one matching row exists,
     *         {@code false} otherwise
     */
    public final ContinuableFuture<Boolean> exists(final Class<?> targetClass, final Object... ids) {
        return exists(targetClass, CassandraExecutorBase.idsToCondition(targetClass, ids));
    }

    /**
     * Asynchronously checks whether any row matches {@code whereClause} on the table mapped by
     * {@code targetClass}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: existence check by an arbitrary condition.
     * boolean present = async.exists(User.class, Filters.eq("email", "a@b.com")).get(); // true if any such row exists
     *
     * // Typical: use the result to decide whether to insert.
     * if (!async.exists(User.class, Filters.eq("id", 1L)).get()) { async.insert(new User(1L, "Alice")).get(); }
     *
     * // Edge: a condition matching no rows resolves to false.
     * boolean none = async.exists(User.class, Filters.eq("id", -1L)).get(); // returns false
     *
     * // Edge: a CQL/driver failure surfaces as an ExecutionException on get().
     * try {
     *     async.exists(User.class, Filters.eq("missingCol", 1)).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param targetClass the entity class identifying the target table
     * @param whereClause the WHERE condition to evaluate
     * @return a future whose payload is {@code true} if at least one matching row exists,
     *         {@code false} otherwise
     */
    public ContinuableFuture<Boolean> exists(final Class<?> targetClass, final Condition whereClause) {
        final SP cp = cassandraExecutor.prepareQuery(targetClass, getKeyNames(targetClass), whereClause, 1);

        return exists(cp.query(), cp.parameters().toArray());
    }

    /**
     * Asynchronously executes the supplied CQL query and returns whether it produced any row.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: run an explicit existence query (a SELECT limited to 1 is most efficient).
     * boolean present = async.exists("SELECT id FROM users WHERE id = ? LIMIT 1", 1L).get(); // true if a row exists
     *
     * // Typical: existence by a non-key column.
     * boolean anyActive = async.exists("SELECT id FROM users WHERE status = ? LIMIT 1", "active").get();
     *
     * // Edge: a query returning no rows resolves to false.
     * boolean none = async.exists("SELECT id FROM users WHERE id = ? LIMIT 1", -1L).get(); // returns false
     *
     * // Edge: a malformed query makes the future complete exceptionally; get() rethrows it wrapped.
     * try {
     *     async.exists("SELECT FROM").get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver/parse failure
     * }
     * }</pre>
     *
     * @param query the CQL query (typically a SELECT limited to 1)
     * @param parameters the parameter values to bind
     * @return a future whose payload is {@code true} if the query returned at least one row,
     *         {@code false} otherwise
     */
    public final ContinuableFuture<Boolean> exists(final String query, final Object... parameters) {
        return execute(query, parameters).map(CassandraExecutorBase.exists_mapper);
    }

    /**
     * Asynchronously counts the rows matching {@code whereClause} on the table mapped by
     * {@code targetClass}, by issuing a {@code SELECT COUNT(*)} against the table.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: count rows matching a condition.
     * long active = async.count(User.class, Filters.eq("status", "active")).get(); // number of active users
     *
     * // Typical: count then branch in a continuation.
     * async.count(User.class, Filters.eq("status", "active")).thenAccept(n -> log.info("active=" + n));
     *
     * // Edge: a condition matching no rows yields 0.
     * long none = async.count(User.class, Filters.eq("id", -1L)).get(); // returns 0
     *
     * // Edge: a CQL/driver failure surfaces as an ExecutionException on get().
     * try {
     *     async.count(User.class, Filters.eq("missingCol", 1)).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param targetClass the entity class identifying the target table
     * @param whereClause the WHERE condition to evaluate
     * @return a future whose payload is the number of matching rows
     */
    @SuppressWarnings("deprecation")
    public ContinuableFuture<Long> count(final Class<?> targetClass, final Condition whereClause) {
        final SP cp = cassandraExecutor.prepareQuery(targetClass, CassandraExecutorBase.COUNT_SELECT_PROP_NAMES, whereClause, 0);

        return count(cp.query(), cp.parameters().toArray());
    }

    /**
     * Asynchronously executes a CQL query whose first row's first {@code Long}-typed column is
     * taken as a count.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: run an explicit COUNT(*) query (deprecated; prefer queryForLong).
     * long active = async.count("SELECT COUNT(*) FROM users WHERE status = ?", "active").get(); // count of active users
     *
     * // Typical: preferred replacement.
     * long activePreferred = async.queryForLong("SELECT COUNT(*) FROM users WHERE status = ?", "active").get().orElse(0L);
     *
     * // Edge: a query producing no row (or a null value) yields 0.
     * long none = async.count("SELECT COUNT(*) FROM users WHERE id = ?", -1L).get(); // returns the count, 0 if none
     *
     * // Edge: a malformed query makes the future complete exceptionally; get() rethrows it wrapped.
     * try {
     *     async.count("SELECT COUNT FROM").get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver/parse failure
     * }
     * }</pre>
     *
     * @param query the CQL query to execute (typically a {@code SELECT COUNT(*) ...})
     * @param parameters the parameter values to bind
     * @return a future whose payload is the count of matching rows, or {@code 0} if the query
     *         returned no row or a {@code null} value
     * @deprecated Use {@link #queryForLong(String, Object...)} with {@code COUNT(*)} in the query instead.
     */
    @Deprecated
    public final ContinuableFuture<Long> count(final String query, final Object... parameters) {
        return queryForSingleValue(Long.class, query, parameters).map(CassandraExecutorBase.long_secondMapper);
    }

    /**
     * Asynchronously retrieves all entities matching {@code whereClause}, selecting all mapped
     * properties.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: list all matching entities, then block for the result.
     * List<User> users = async.list(User.class, Filters.eq("status", "active")).get();
     *
     * // Typical: derive a value off the future without blocking the caller.
     * ContinuableFuture<Integer> total = async.list(User.class, Filters.eq("status", "active")).map(List::size);
     *
     * // Edge: no matching rows -> the future completes with an empty list.
     * boolean empty = async.list(User.class, Filters.eq("id", -1L)).get().isEmpty(); // returns true
     *
     * // Edge: a CQL/driver failure surfaces as an ExecutionException on get().
     * try {
     *     async.list(User.class, Filters.eq("missingCol", 1)).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class to map result rows to
     * @param whereClause the WHERE condition selecting rows
     * @return a future whose payload is a {@link List} of mapped entities (empty if no row matches)
     */
    public <T> ContinuableFuture<List<T>> list(final Class<T> targetClass, final Condition whereClause) {
        return list(targetClass, null, whereClause);
    }

    /**
     * Asynchronously retrieves all entities matching {@code whereClause}, selecting only the
     * specified properties.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: list with a column projection.
     * List<User> users = async.list(User.class, Arrays.asList("id", "name"), Filters.eq("status", "active")).get();
     *
     * // Typical: null selectPropNames selects all mapped properties.
     * List<User> full = async.list(User.class, null, Filters.eq("status", "active")).get();
     *
     * // Edge: no matching rows -> the future completes with an empty list.
     * boolean empty = async.list(User.class, Arrays.asList("id"), Filters.eq("id", -1L)).get().isEmpty(); // returns true
     *
     * // Edge: a CQL/driver failure surfaces as an ExecutionException on get().
     * try {
     *     async.list(User.class, Arrays.asList("id"), Filters.eq("missingCol", 1)).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class to map result rows to
     * @param selectPropNames the property names to include in the SELECT clause, or {@code null}
     *                       to select all mapped properties
     * @param whereClause the WHERE condition selecting rows
     * @return a future whose payload is a {@link List} of mapped entities (empty if no row matches)
     */
    public <T> ContinuableFuture<List<T>> list(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP cp = cassandraExecutor.prepareQuery(targetClass, selectPropNames, whereClause);

        return list(targetClass, cp.query(), cp.parameters().toArray());
    }

    /**
     * Asynchronously executes the supplied CQL query and returns each row as a column-name to
     * column-value {@code Map}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: run a raw SELECT and read each row as a Map.
     * List<Map<String, Object>> rows = async.list("SELECT id, name FROM users WHERE status = ?", "active").get();
     * rows.forEach(row -> System.out.println(row.get("name"))); // prints each row's name
     *
     * // Typical: count the rows via the future.
     * ContinuableFuture<Integer> n = async.list("SELECT id FROM users").map(List::size);
     *
     * // Edge: no matching rows -> the future completes with an empty list.
     * boolean empty = async.list("SELECT id FROM users WHERE id = ?", -1L).get().isEmpty(); // returns true
     *
     * // Edge: a malformed query makes the future complete exceptionally; get() rethrows it wrapped.
     * try {
     *     async.list("SELECT FROM").get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver/parse failure
     * }
     * }</pre>
     *
     * @param query the parameterized CQL SELECT statement
     * @param parameters the parameter values to bind
     * @return a future whose payload is a {@link List} of result rows, each row as a
     *         {@code Map<String, Object>}
     */
    public final ContinuableFuture<List<Map<String, Object>>> list(final String query, final Object... parameters) {
        return list(Clazz.PROPS_MAP, query, parameters);
    }

    /**
     * Asynchronously executes the supplied CQL query and maps each row to an instance of
     * {@code targetClass}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: run a raw SELECT and map rows to entities.
     * List<User> users = async.list(User.class, "SELECT * FROM users WHERE status = ?", "active").get();
     *
     * // Typical: map rows to a simple value type (single-column query).
     * List<String> names = async.list(String.class, "SELECT name FROM users").get();
     *
     * // Edge: no matching rows -> the future completes with an empty list.
     * boolean empty = async.list(User.class, "SELECT * FROM users WHERE id = ?", -1L).get().isEmpty(); // returns true
     *
     * // Edge: a malformed query makes the future complete exceptionally; get() rethrows it wrapped.
     * try {
     *     async.list(User.class, "SELECT FROM").get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver/parse failure
     * }
     * }</pre>
     *
     * @param <T> the row type
     * @param targetClass the type each row should be mapped to
     * @param query the parameterized CQL SELECT statement
     * @param parameters the parameter values to bind
     * @return a future whose payload is a {@link List} of mapped rows (empty if no row matches)
     */
    public final <T> ContinuableFuture<List<T>> list(final Class<T> targetClass, final String query, final Object... parameters) {
        return execute(query, parameters).map(resultSet -> cassandraExecutor.toList(targetClass, resultSet));
    }

    /**
     * Asynchronously executes a query and returns the result as a {@link Dataset}, selecting all
     * mapped properties of {@code targetClass}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: load matching rows into a Dataset for column-oriented access.
     * Dataset ds = async.query(User.class, Filters.eq("status", "active")).get();
     * int rows = ds.size();
     *
     * // Typical: derive the row count off the future without blocking.
     * ContinuableFuture<Integer> count = async.query(User.class, Filters.eq("status", "active")).map(Dataset::size);
     *
     * // Edge: no matching rows -> the future completes with an empty Dataset.
     * boolean empty = async.query(User.class, Filters.eq("id", -1L)).get().isEmpty(); // returns true
     *
     * // Edge: a CQL/driver failure surfaces as an ExecutionException on get().
     * try {
     *     async.query(User.class, Filters.eq("missingCol", 1)).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param <T> the entity type used to resolve column/property names
     * @param targetClass the entity class identifying the target table and column mappings
     * @param whereClause the WHERE condition selecting rows
     * @return a future whose payload is a {@link Dataset} containing the result rows
     */
    public <T> ContinuableFuture<Dataset> query(final Class<T> targetClass, final Condition whereClause) {
        return query(targetClass, null, whereClause);
    }

    /**
     * Asynchronously executes a query and returns the result as a {@link Dataset}, selecting only
     * the specified properties.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: project specific columns into a Dataset.
     * Dataset ds = async.query(User.class, Arrays.asList("id", "name"), Filters.eq("status", "active")).get();
     *
     * // Typical: null selectPropNames selects all mapped properties.
     * Dataset full = async.query(User.class, null, Filters.eq("status", "active")).get();
     *
     * // Edge: no matching rows -> the future completes with an empty Dataset.
     * boolean empty = async.query(User.class, Arrays.asList("id"), Filters.eq("id", -1L)).get().isEmpty(); // returns true
     *
     * // Edge: a CQL/driver failure surfaces as an ExecutionException on get().
     * try {
     *     async.query(User.class, Arrays.asList("id"), Filters.eq("missingCol", 1)).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param <T> the entity type used to resolve column/property names
     * @param targetClass the entity class identifying the target table and column mappings
     * @param selectPropNames the property names to include in the SELECT clause, or {@code null}
     *                       to select all mapped properties
     * @param whereClause the WHERE condition selecting rows
     * @return a future whose payload is a {@link Dataset} containing the result rows
     */
    public <T> ContinuableFuture<Dataset> query(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP cp = cassandraExecutor.prepareQuery(targetClass, selectPropNames, whereClause);

        return query(targetClass, cp.query(), cp.parameters().toArray());
    }

    /**
     * Asynchronously executes the supplied CQL query and returns the result as a {@link Dataset}.
     *
     * <p>Each row is converted to a {@code Map<String, Object>} entry.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: run a raw SELECT and load the result into a Dataset.
     * Dataset ds = async.query("SELECT id, name FROM users WHERE status = ?", "active").get();
     *
     * // Typical: derive the row count off the future.
     * ContinuableFuture<Integer> rows = async.query("SELECT id FROM users").map(Dataset::size);
     *
     * // Edge: no matching rows -> the future completes with an empty Dataset.
     * boolean empty = async.query("SELECT id FROM users WHERE id = ?", -1L).get().isEmpty(); // returns true
     *
     * // Edge: a malformed query makes the future complete exceptionally; get() rethrows it wrapped.
     * try {
     *     async.query("SELECT FROM").get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver/parse failure
     * }
     * }</pre>
     *
     * @param query the parameterized CQL SELECT statement
     * @param parameters the parameter values to bind
     * @return a future whose payload is a {@link Dataset} containing the result rows
     */
    public final ContinuableFuture<Dataset> query(final String query, final Object... parameters) {
        return query(Map.class, query, parameters);
    }

    /**
     * Asynchronously executes the supplied CQL query and returns the result as a {@link Dataset}
     * whose columns are resolved via the property mapping of {@code targetClass}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: run a raw SELECT, interpreting columns via the entity's property mapping.
     * Dataset ds = async.query(User.class, "SELECT id, name FROM users WHERE status = ?", "active").get();
     *
     * // Typical: chain a transformation on the future.
     * ContinuableFuture<Integer> rows = async.query(User.class, "SELECT id FROM users").map(Dataset::size);
     *
     * // Edge: no matching rows -> the future completes with an empty Dataset.
     * boolean empty = async.query(User.class, "SELECT * FROM users WHERE id = ?", -1L).get().isEmpty(); // returns true
     *
     * // Edge: a malformed query makes the future complete exceptionally; get() rethrows it wrapped.
     * try {
     *     async.query(User.class, "SELECT FROM").get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver/parse failure
     * }
     * }</pre>
     *
     * @param targetClass the class whose property mapping is used to interpret result columns
     * @param query the parameterized CQL SELECT statement
     * @param parameters the parameter values to bind
     * @return a future whose payload is a {@link Dataset} containing the result rows
     */
    public final ContinuableFuture<Dataset> query(final Class<?> targetClass, final String query, final Object... parameters) {
        return execute(query, parameters).map(resultSet -> cassandraExecutor.extractData(targetClass, resultSet));
    }

    /**
     * Asynchronously executes a query and returns a lazy {@link Stream} of mapped entities,
     * selecting all mapped properties of {@code targetClass}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: stream matching entities, then collect to a list.
     * List<User> users = async.stream(User.class, Filters.eq("status", "active")).get().toList();
     *
     * // Typical: take only the first matching entity from the lazy stream.
     * Optional<User> first = async.stream(User.class, Filters.eq("status", "active")).get().first();
     *
     * // Edge: no matching rows -> the streamed result is empty.
     * boolean empty = async.stream(User.class, Filters.eq("id", -1L)).get().toList().isEmpty(); // returns true
     *
     * // Edge: a CQL/driver failure surfaces as an ExecutionException on get().
     * try {
     *     async.stream(User.class, Filters.eq("missingCol", 1)).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class to map result rows to
     * @param whereClause the WHERE condition selecting rows
     * @return a future whose payload is a {@link Stream} of mapped entities, backed by the
     *         underlying result-set iterator
     */
    public <T> ContinuableFuture<Stream<T>> stream(final Class<T> targetClass, final Condition whereClause) {
        return stream(targetClass, null, whereClause);
    }

    /**
     * Asynchronously executes a query and returns a lazy {@link Stream} of mapped entities,
     * selecting only the specified properties.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: stream with a column projection, then collect.
     * List<User> users = async.stream(User.class, Arrays.asList("id", "name"), Filters.eq("status", "active")).get().toList();
     *
     * // Typical: null selectPropNames selects all mapped properties.
     * Optional<User> first = async.stream(User.class, null, Filters.eq("status", "active")).get().first();
     *
     * // Edge: no matching rows -> the streamed result is empty.
     * boolean empty = async.stream(User.class, Arrays.asList("id"), Filters.eq("id", -1L)).get().toList().isEmpty(); // returns true
     *
     * // Edge: a CQL/driver failure surfaces as an ExecutionException on get().
     * try {
     *     async.stream(User.class, Arrays.asList("id"), Filters.eq("missingCol", 1)).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class to map result rows to
     * @param selectPropNames the property names to include in the SELECT clause, or {@code null}
     *                       to select all mapped properties
     * @param whereClause the WHERE condition selecting rows
     * @return a future whose payload is a {@link Stream} of mapped entities
     */
    public <T> ContinuableFuture<Stream<T>> stream(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP cp = cassandraExecutor.prepareQuery(targetClass, selectPropNames, whereClause);

        return stream(targetClass, cp.query(), cp.parameters().toArray());
    }

    /**
     * Asynchronously executes the supplied CQL query and returns each row as an {@code Object[]}
     * of column values.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: stream raw column arrays and print the first two columns.
     * async.stream("SELECT id, name FROM users WHERE status = ?", "active")
     *      .get().forEach(row -> System.out.println(row[0] + "=" + row[1]));
     *
     * // Typical: count rows by collecting the lazy stream.
     * long n = async.stream("SELECT id FROM users").get().toList().size(); // total rows
     *
     * // Edge: no matching rows -> the streamed result is empty.
     * boolean empty = async.stream("SELECT id FROM users WHERE id = ?", -1L).get().toList().isEmpty(); // returns true
     *
     * // Edge: a malformed query makes the future complete exceptionally; get() rethrows it wrapped.
     * try {
     *     async.stream("SELECT FROM").get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver/parse failure
     * }
     * }</pre>
     *
     * @param query the parameterized CQL SELECT statement
     * @param parameters the parameter values to bind
     * @return a future whose payload is a {@link Stream} of {@code Object[]} rows
     */
    public ContinuableFuture<Stream<Object[]>> stream(final String query, final Object... parameters) {
        return stream(Object[].class, query, parameters);
    }

    /**
     * Asynchronously executes the supplied CQL query and maps each row to an instance of
     * {@code targetClass}, returning a lazy {@link Stream}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: stream mapped entities from a raw query, then collect.
     * List<User> users = async.stream(User.class, "SELECT * FROM users WHERE status = ?", "active").get().toList();
     *
     * // Typical: stream a single-column query into value objects.
     * List<String> names = async.stream(String.class, "SELECT name FROM users").get().toList();
     *
     * // Edge: no matching rows -> the streamed result is empty.
     * boolean empty = async.stream(User.class, "SELECT * FROM users WHERE id = ?", -1L).get().toList().isEmpty(); // returns true
     *
     * // Edge: a malformed query makes the future complete exceptionally; get() rethrows it wrapped.
     * try {
     *     async.stream(User.class, "SELECT FROM").get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver/parse failure
     * }
     * }</pre>
     *
     * @param <T> the row type
     * @param targetClass the type each row should be mapped to
     * @param query the parameterized CQL SELECT statement
     * @param parameters the parameter values to bind
     * @return a future whose payload is a {@link Stream} of mapped rows
     */
    public final <T> ContinuableFuture<Stream<T>> stream(final Class<T> targetClass, final String query, final Object... parameters) {
        return execute(query, parameters).map(resultSet -> Stream.of(resultSet.iterator()).map(cassandraExecutor.createRowMapper(targetClass)));
    }

    /**
     * Asynchronously executes a pre-built CQL {@code statement} and maps each row to an instance
     * of {@code targetClass}, returning a lazy {@link Stream}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: run a fully-built driver statement and map rows to entities.
     * Statement<?> stmt = SimpleStatement.newInstance("SELECT * FROM users WHERE status = ?", "active");
     * List<User> users = async.stream(User.class, stmt).get().toList();
     *
     * // Typical: apply a per-statement page size, then take the first row.
     * Statement<?> paged = SimpleStatement.newInstance("SELECT * FROM users").setPageSize(500);
     * Optional<User> first = async.stream(User.class, paged).get().first();
     *
     * // Edge: a statement matching no rows -> the streamed result is empty.
     * Statement<?> none = SimpleStatement.newInstance("SELECT * FROM users WHERE id = ?", -1L);
     * boolean empty = async.stream(User.class, none).get().toList().isEmpty(); // returns true
     *
     * // Edge: a null statement makes the future complete exceptionally; get() rethrows it wrapped.
     * try {
     *     async.stream(User.class, (Statement<?>) null).get(); // throws ExecutionException (cause NullPointerException)
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is a NullPointerException
     * }
     * }</pre>
     *
     * @param <T> the row type
     * @param targetClass the type each row should be mapped to
     * @param statement the driver statement to execute
     * @return a future whose payload is a {@link Stream} of mapped rows
     */
    public <T> ContinuableFuture<Stream<T>> stream(final Class<T> targetClass, final ST statement) {
        return execute(statement).map(resultSet -> Stream.of(resultSet.iterator()).map(cassandraExecutor.createRowMapper(targetClass)));
    }

    /**
     * Asynchronously retrieves the first entity matching {@code whereClause}, selecting all
     * mapped properties of {@code targetClass}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: get the first row matching a (possibly multi-row) condition.
     * Optional<User> user = async.findFirst(User.class, Filters.eq("status", "active")).get();
     *
     * // Typical: provide a fallback when no row matches.
     * User u = async.findFirst(User.class, Filters.eq("status", "active")).get().orElse(User.GUEST);
     *
     * // Edge: no matching row -> the future completes with an empty Optional.
     * boolean none = async.findFirst(User.class, Filters.eq("id", -1L)).get().isEmpty(); // returns true
     *
     * // Edge: a CQL/driver failure surfaces as an ExecutionException on get().
     * try {
     *     async.findFirst(User.class, Filters.eq("missingCol", 1)).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class to map the first row to
     * @param whereClause the WHERE condition selecting rows
     * @return a future whose payload is an {@link Optional} containing the first mapped entity,
     *         or empty if no row matches
     */
    public <T> ContinuableFuture<Optional<T>> findFirst(final Class<T> targetClass, final Condition whereClause) {
        return findFirst(targetClass, null, whereClause);
    }

    /**
     * Asynchronously retrieves the first entity matching {@code whereClause}, selecting only the
     * specified properties; the underlying SELECT is limited to {@code 1} row.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: get the first matching row with a column projection.
     * Optional<User> user = async.findFirst(User.class, Arrays.asList("id", "name"), Filters.eq("status", "active")).get();
     *
     * // Typical: null selectPropNames selects all mapped properties.
     * Optional<User> full = async.findFirst(User.class, null, Filters.eq("status", "active")).get();
     *
     * // Edge: no matching row -> the future completes with an empty Optional.
     * boolean none = async.findFirst(User.class, Arrays.asList("id"), Filters.eq("id", -1L)).get().isEmpty(); // returns true
     *
     * // Edge: a CQL/driver failure surfaces as an ExecutionException on get().
     * try {
     *     async.findFirst(User.class, Arrays.asList("id"), Filters.eq("missingCol", 1)).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class to map the first row to
     * @param selectPropNames the property names to include in the SELECT clause, or {@code null}
     *                       to select all mapped properties
     * @param whereClause the WHERE condition selecting rows
     * @return a future whose payload is an {@link Optional} containing the first mapped entity,
     *         or empty if no row matches
     */
    public <T> ContinuableFuture<Optional<T>> findFirst(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP cp = cassandraExecutor.prepareQuery(targetClass, selectPropNames, whereClause, 1);

        return findFirst(targetClass, cp.query(), cp.parameters().toArray());
    }

    /**
     * Asynchronously executes the supplied CQL query and returns its first row as a column-name
     * to column-value {@code Map}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: read the first matching row as a Map.
     * Optional<Map<String, Object>> row = async.findFirst("SELECT id, name FROM users WHERE id = ?", 1L).get();
     * row.ifPresent(r -> System.out.println(r.get("name"))); // prints the name if a row was found
     *
     * // Typical: extract a single column from the first row.
     * Object name = async.findFirst("SELECT name FROM users WHERE id = ?", 1L).get().map(r -> r.get("name")).orElse(null);
     *
     * // Edge: no matching row -> the future completes with an empty Optional.
     * boolean none = async.findFirst("SELECT id FROM users WHERE id = ?", -1L).get().isEmpty(); // returns true
     *
     * // Edge: a malformed query makes the future complete exceptionally; get() rethrows it wrapped.
     * try {
     *     async.findFirst("SELECT FROM").get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver/parse failure
     * }
     * }</pre>
     *
     * @param query the parameterized CQL SELECT statement
     * @param parameters the parameter values to bind
     * @return a future whose payload is an {@link Optional} containing the first row as a
     *         {@code Map<String, Object>}, or empty if the query returned no row
     */
    public final ContinuableFuture<Optional<Map<String, Object>>> findFirst(final String query, final Object... parameters) {
        return findFirst(Clazz.PROPS_MAP, query, parameters);
    }

    /**
     * Asynchronously executes the supplied CQL query and maps its first row to an instance of
     * {@code targetClass}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: map the first matching row to an entity.
     * Optional<User> user = async.findFirst(User.class, "SELECT * FROM users WHERE id = ?", 1L).get();
     *
     * // Typical: map the first row to a value type (single-column query).
     * Optional<String> name = async.findFirst(String.class, "SELECT name FROM users WHERE id = ?", 1L).get();
     *
     * // Edge: no matching row -> the future completes with an empty Optional.
     * boolean none = async.findFirst(User.class, "SELECT * FROM users WHERE id = ?", -1L).get().isEmpty(); // returns true
     *
     * // Edge: a row whose mapped value is null cannot be held by Optional; get() throws
     * // NullPointerException directly (the completed future evaluates the mapping inline).
     * try {
     *     async.findFirst(String.class, "SELECT null_col FROM users WHERE id = ?", 1L).get(); // throws NullPointerException
     * } catch (NullPointerException ex) {
     *     // mapped first-row value was null
     * }
     * }</pre>
     *
     * @param <T> the row type
     * @param targetClass the type the first row should be mapped to
     * @param query the parameterized CQL SELECT statement
     * @param parameters the parameter values to bind
     * @return a future whose payload is an {@link Optional} containing the mapped first row, or
     *         empty if the query returned no row
     */
    public <T> ContinuableFuture<Optional<T>> findFirst(final Class<T> targetClass, final String query, final Object... parameters) {
        return execute(query, parameters).map(resultSet -> {
            final java.util.Iterator<RW> iter = resultSet.iterator();

            return iter.hasNext() ? Optional.of(cassandraExecutor.createRowMapper(targetClass).apply(iter.next())) : Optional.empty();
        });
    }

    /**
     * Asynchronously selects a single {@code boolean} value of the named property from the first
     * row matching {@code whereClause}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: read a boolean column from the first matching row.
     * boolean active = async.queryForBoolean(User.class, "active", Filters.eq("id", 1L)).get().orElse(false);
     *
     * // Typical: branch on presence of the value.
     * OptionalBoolean ob = async.queryForBoolean(User.class, "active", Filters.eq("id", 1L)).get();
     * if (ob.isPresent()) { useFlag(ob.getAsBoolean()); }
     *
     * // Edge: no matching row (or a null value) -> the future completes with an empty OptionalBoolean.
     * boolean empty = async.queryForBoolean(User.class, "active", Filters.eq("id", -1L)).get().isEmpty(); // returns true
     *
     * // Edge: a CQL/driver failure surfaces as an ExecutionException on get().
     * try {
     *     async.queryForBoolean(User.class, "missingCol", Filters.eq("id", 1L)).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class identifying the target table
     * @param propName the property whose value is selected
     * @param whereClause the WHERE condition selecting at most one row
     * @return a future whose payload is an {@link OptionalBoolean} holding the value, or empty if
     *         no row matches or the value is {@code null}
     */
    public <T> ContinuableFuture<OptionalBoolean> queryForBoolean(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, Boolean.class, propName, whereClause).map(CassandraExecutorBase.boolean_mapper);
    }

    /**
     * Asynchronously selects a single {@code char} value of the named property from the first
     * row matching {@code whereClause}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: read a char column from the first matching row.
     * char grade = async.queryForChar(User.class, "grade", Filters.eq("id", 1L)).get().orElse('-');
     *
     * // Typical: check presence first.
     * OptionalChar oc = async.queryForChar(User.class, "grade", Filters.eq("id", 1L)).get();
     * if (oc.isPresent()) { useGrade(oc.getAsChar()); }
     *
     * // Edge: no matching row (or a null value) -> the future completes with an empty OptionalChar.
     * boolean empty = async.queryForChar(User.class, "grade", Filters.eq("id", -1L)).get().isEmpty(); // returns true
     *
     * // Edge: a CQL/driver failure surfaces as an ExecutionException on get().
     * try {
     *     async.queryForChar(User.class, "missingCol", Filters.eq("id", 1L)).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class identifying the target table
     * @param propName the property whose value is selected
     * @param whereClause the WHERE condition selecting at most one row
     * @return a future whose payload is an {@link OptionalChar} holding the value, or empty if no
     *         row matches or the value is {@code null}
     */
    public <T> ContinuableFuture<OptionalChar> queryForChar(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, Character.class, propName, whereClause).map(CassandraExecutorBase.char_mapper);
    }

    /**
     * Asynchronously selects a single {@code byte} value of the named property from the first
     * row matching {@code whereClause}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: read a byte column from the first matching row.
     * byte level = async.queryForByte(User.class, "level", Filters.eq("id", 1L)).get().orElse((byte) 0);
     *
     * // Typical: check presence first.
     * OptionalByte ob = async.queryForByte(User.class, "level", Filters.eq("id", 1L)).get();
     * if (ob.isPresent()) { useLevel(ob.getAsByte()); }
     *
     * // Edge: no matching row (or a null value) -> the future completes with an empty OptionalByte.
     * boolean empty = async.queryForByte(User.class, "level", Filters.eq("id", -1L)).get().isEmpty(); // returns true
     *
     * // Edge: a CQL/driver failure surfaces as an ExecutionException on get().
     * try {
     *     async.queryForByte(User.class, "missingCol", Filters.eq("id", 1L)).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class identifying the target table
     * @param propName the property whose value is selected
     * @param whereClause the WHERE condition selecting at most one row
     * @return a future whose payload is an {@link OptionalByte} holding the value, or empty if no
     *         row matches or the value is {@code null}
     */
    public <T> ContinuableFuture<OptionalByte> queryForByte(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, Byte.class, propName, whereClause).map(CassandraExecutorBase.byte_mapper);
    }

    /**
     * Asynchronously selects a single {@code short} value of the named property from the first
     * row matching {@code whereClause}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: read a short column from the first matching row.
     * short cnt = async.queryForShort(User.class, "count", Filters.eq("id", 1L)).get().orElse((short) 0);
     *
     * // Typical: check presence first.
     * OptionalShort os = async.queryForShort(User.class, "count", Filters.eq("id", 1L)).get();
     * if (os.isPresent()) { useCount(os.getAsShort()); }
     *
     * // Edge: no matching row (or a null value) -> the future completes with an empty OptionalShort.
     * boolean empty = async.queryForShort(User.class, "count", Filters.eq("id", -1L)).get().isEmpty(); // returns true
     *
     * // Edge: a CQL/driver failure surfaces as an ExecutionException on get().
     * try {
     *     async.queryForShort(User.class, "missingCol", Filters.eq("id", 1L)).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class identifying the target table
     * @param propName the property whose value is selected
     * @param whereClause the WHERE condition selecting at most one row
     * @return a future whose payload is an {@link OptionalShort} holding the value, or empty if
     *         no row matches or the value is {@code null}
     */
    public <T> ContinuableFuture<OptionalShort> queryForShort(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, Short.class, propName, whereClause).map(CassandraExecutorBase.short_mapper);
    }

    /**
     * Asynchronously selects a single {@code int} value of the named property from the first
     * row matching {@code whereClause}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: read an int column from the first matching row.
     * int age = async.queryForInt(User.class, "age", Filters.eq("id", 1L)).get().orElse(0);
     *
     * // Typical: check presence first.
     * OptionalInt oi = async.queryForInt(User.class, "age", Filters.eq("id", 1L)).get();
     * if (oi.isPresent()) { useAge(oi.getAsInt()); }
     *
     * // Edge: no matching row (or a null value) -> the future completes with an empty OptionalInt.
     * boolean empty = async.queryForInt(User.class, "age", Filters.eq("id", -1L)).get().isEmpty(); // returns true
     *
     * // Edge: a CQL/driver failure surfaces as an ExecutionException on get().
     * try {
     *     async.queryForInt(User.class, "missingCol", Filters.eq("id", 1L)).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class identifying the target table
     * @param propName the property whose value is selected
     * @param whereClause the WHERE condition selecting at most one row
     * @return a future whose payload is an {@link OptionalInt} holding the value, or empty if no
     *         row matches or the value is {@code null}
     */
    public <T> ContinuableFuture<OptionalInt> queryForInt(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, Integer.class, propName, whereClause).map(CassandraExecutorBase.int_mapper);
    }

    /**
     * Asynchronously selects a single {@code long} value of the named property from the first
     * row matching {@code whereClause}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: read a long column from the first matching row.
     * long id = async.queryForLong(User.class, "id", Filters.eq("email", "a@b.com")).get().orElse(0L);
     *
     * // Typical: check presence first.
     * OptionalLong ol = async.queryForLong(User.class, "id", Filters.eq("email", "a@b.com")).get();
     * if (ol.isPresent()) { useId(ol.getAsLong()); }
     *
     * // Edge: no matching row (or a null value) -> the future completes with an empty OptionalLong.
     * boolean empty = async.queryForLong(User.class, "id", Filters.eq("id", -1L)).get().isEmpty(); // returns true
     *
     * // Edge: a CQL/driver failure surfaces as an ExecutionException on get().
     * try {
     *     async.queryForLong(User.class, "missingCol", Filters.eq("id", 1L)).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class identifying the target table
     * @param propName the property whose value is selected
     * @param whereClause the WHERE condition selecting at most one row
     * @return a future whose payload is an {@link OptionalLong} holding the value, or empty if no
     *         row matches or the value is {@code null}
     */
    public <T> ContinuableFuture<OptionalLong> queryForLong(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, Long.class, propName, whereClause).map(CassandraExecutorBase.long_mapper);
    }

    /**
     * Asynchronously selects a single {@code float} value of the named property from the first
     * row matching {@code whereClause}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: read a float column from the first matching row.
     * float score = async.queryForFloat(User.class, "score", Filters.eq("id", 1L)).get().orElse(0f);
     *
     * // Typical: check presence first.
     * OptionalFloat of = async.queryForFloat(User.class, "score", Filters.eq("id", 1L)).get();
     * if (of.isPresent()) { useScore(of.getAsFloat()); }
     *
     * // Edge: no matching row (or a null value) -> the future completes with an empty OptionalFloat.
     * boolean empty = async.queryForFloat(User.class, "score", Filters.eq("id", -1L)).get().isEmpty(); // returns true
     *
     * // Edge: a CQL/driver failure surfaces as an ExecutionException on get().
     * try {
     *     async.queryForFloat(User.class, "missingCol", Filters.eq("id", 1L)).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class identifying the target table
     * @param propName the property whose value is selected
     * @param whereClause the WHERE condition selecting at most one row
     * @return a future whose payload is an {@link OptionalFloat} holding the value, or empty if
     *         no row matches or the value is {@code null}
     */
    public <T> ContinuableFuture<OptionalFloat> queryForFloat(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, Float.class, propName, whereClause).map(CassandraExecutorBase.float_mapper);
    }

    /**
     * Asynchronously selects a single {@code double} value of the named property from the first
     * row matching {@code whereClause}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: read a double column from the first matching row.
     * double price = async.queryForDouble(User.class, "price", Filters.eq("id", 1L)).get().orElse(0d);
     *
     * // Typical: check presence first.
     * OptionalDouble od = async.queryForDouble(User.class, "price", Filters.eq("id", 1L)).get();
     * if (od.isPresent()) { usePrice(od.getAsDouble()); }
     *
     * // Edge: no matching row (or a null value) -> the future completes with an empty OptionalDouble.
     * boolean empty = async.queryForDouble(User.class, "price", Filters.eq("id", -1L)).get().isEmpty(); // returns true
     *
     * // Edge: a CQL/driver failure surfaces as an ExecutionException on get().
     * try {
     *     async.queryForDouble(User.class, "missingCol", Filters.eq("id", 1L)).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class identifying the target table
     * @param propName the property whose value is selected
     * @param whereClause the WHERE condition selecting at most one row
     * @return a future whose payload is an {@link OptionalDouble} holding the value, or empty if
     *         no row matches or the value is {@code null}
     */
    public <T> ContinuableFuture<OptionalDouble> queryForDouble(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, Double.class, propName, whereClause).map(CassandraExecutorBase.double_mapper);
    }

    /**
     * Asynchronously selects a single {@link String} value of the named property from the first
     * row matching {@code whereClause}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: read a string column from the first matching row.
     * String name = async.queryForString(User.class, "name", Filters.eq("id", 1L)).get().orElse("");
     *
     * // Typical: distinguish "no row" from "row with SQL NULL".
     * Nullable<String> nv = async.queryForString(User.class, "name", Filters.eq("id", 1L)).get();
     * boolean rowFound = nv.isPresent();                 // true if a row matched
     * boolean isSqlNull = nv.isPresent() && nv.isNull(); // true if the column value was SQL NULL
     *
     * // Edge: no matching row -> the future completes with an empty Nullable.
     * boolean empty = async.queryForString(User.class, "name", Filters.eq("id", -1L)).get().isEmpty(); // returns true
     *
     * // Edge: a CQL/driver failure surfaces as an ExecutionException on get().
     * try {
     *     async.queryForString(User.class, "missingCol", Filters.eq("id", 1L)).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class identifying the target table
     * @param propName the property whose value is selected
     * @param whereClause the WHERE condition selecting at most one row
     * @return a future whose payload is a {@link Nullable} holding the {@code String} value, or
     *         empty if no row matches; the value is {@code null} when the column is SQL NULL
     */
    public <T> ContinuableFuture<Nullable<String>> queryForString(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, String.class, propName, whereClause);
    }

    /**
     * Asynchronously selects a single {@link Date} value of the named property from the first
     * row matching {@code whereClause}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: read a date column from the first matching row.
     * Date created = async.queryForDate(User.class, "createdDate", Filters.eq("id", 1L)).get().orElse(null);
     *
     * // Typical: distinguish "no row" from "SQL NULL value".
     * Nullable<Date> nv = async.queryForDate(User.class, "createdDate", Filters.eq("id", 1L)).get();
     * boolean rowFound = nv.isPresent();
     *
     * // Edge: no matching row -> the future completes with an empty Nullable.
     * boolean empty = async.queryForDate(User.class, "createdDate", Filters.eq("id", -1L)).get().isEmpty(); // returns true
     *
     * // Edge: a CQL/driver failure surfaces as an ExecutionException on get().
     * try {
     *     async.queryForDate(User.class, "missingCol", Filters.eq("id", 1L)).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class identifying the target table
     * @param propName the property whose value is selected
     * @param whereClause the WHERE condition selecting at most one row
     * @return a future whose payload is a {@link Nullable} holding the {@code Date} value, or
     *         empty if no row matches
     */
    public <T> ContinuableFuture<Nullable<Date>> queryForDate(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, Date.class, propName, whereClause);
    }

    /**
     * Asynchronously selects a single date value of the named property, coerced to the requested
     * {@code Date} subtype, from the first row matching {@code whereClause}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: read a date column coerced to java.sql.Timestamp.
     * java.sql.Timestamp ts = async.queryForDate(User.class, java.sql.Timestamp.class, "createdDate", Filters.eq("id", 1L)).get().orElse(null);
     *
     * // Typical: coerce to java.sql.Date instead.
     * Nullable<java.sql.Date> d = async.queryForDate(User.class, java.sql.Date.class, "createdDate", Filters.eq("id", 1L)).get();
     *
     * // Edge: no matching row -> the future completes with an empty Nullable.
     * boolean empty = async.queryForDate(User.class, java.sql.Timestamp.class, "createdDate", Filters.eq("id", -1L)).get().isEmpty(); // returns true
     *
     * // Edge: a CQL/driver failure surfaces as an ExecutionException on get().
     * try {
     *     async.queryForDate(User.class, java.sql.Timestamp.class, "missingCol", Filters.eq("id", 1L)).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param <E> the concrete {@link Date} subtype to return (e.g. {@code java.sql.Timestamp})
     * @param targetClass the entity class identifying the target table
     * @param valueClass the {@code Date} subtype to convert the column value to
     * @param propName the property whose value is selected
     * @param whereClause the WHERE condition selecting at most one row
     * @return a future whose payload is a {@link Nullable} holding the typed date value, or empty
     *         if no row matches
     */
    public <T, E extends Date> ContinuableFuture<Nullable<E>> queryForDate(final Class<T> targetClass, final Class<E> valueClass, final String propName,
            final Condition whereClause) {
        return queryForSingleValue(targetClass, valueClass, propName, whereClause);
    }

    /**
     * Asynchronously selects a single column value of the named property, coerced to
     * {@code valueClass}, from the first row matching {@code whereClause}; the underlying SELECT
     * is limited to {@code 1} row.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: read a single column coerced to a chosen type.
     * Nullable<String> name = async.queryForSingleValue(User.class, String.class, "name", Filters.eq("id", 1L)).get();
     * String n = name.orElse("");
     *
     * // Typical: numeric coercion.
     * Long id = async.queryForSingleValue(User.class, Long.class, "id", Filters.eq("email", "a@b.com")).get().orElse(0L);
     *
     * // Edge: no matching row -> the future completes with an empty Nullable; a present-but-null value is allowed.
     * boolean empty = async.queryForSingleValue(User.class, String.class, "name", Filters.eq("id", -1L)).get().isEmpty(); // returns true
     *
     * // Edge: a CQL/driver failure surfaces as an ExecutionException on get().
     * try {
     *     async.queryForSingleValue(User.class, String.class, "missingCol", Filters.eq("id", 1L)).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param <V> the column-value type
     * @param targetClass the entity class identifying the target table
     * @param valueClass the type to convert the column value to
     * @param propName the property whose value is selected
     * @param whereClause the WHERE condition selecting at most one row
     * @return a future whose payload is a {@link Nullable} holding the value, or empty if no row
     *         matches; the value is {@code null} when the column is SQL NULL
     */
    public <T, V> ContinuableFuture<Nullable<V>> queryForSingleValue(final Class<T> targetClass, final Class<V> valueClass, final String propName,
            final Condition whereClause) {
        final SP cp = cassandraExecutor.prepareQuery(targetClass, List.of(propName), whereClause, 1);

        return queryForSingleValue(valueClass, cp.query(), cp.parameters().toArray());
    }

    /**
     * Asynchronously selects a single non-null column value of the named property, coerced to
     * {@code valueClass}, from the first row matching {@code whereClause}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: read a column known to be non-null when present.
     * Optional<String> name = async.queryForSingleNonNull(User.class, String.class, "name", Filters.eq("id", 1L)).get();
     * String n = name.orElse("");
     *
     * // Typical: numeric coercion of a required column.
     * Optional<Long> id = async.queryForSingleNonNull(User.class, Long.class, "id", Filters.eq("email", "a@b.com")).get();
     *
     * // Edge: no matching row -> the future completes with an empty Optional (no exception).
     * boolean empty = async.queryForSingleNonNull(User.class, String.class, "name", Filters.eq("id", -1L)).get().isEmpty(); // returns true
     *
     * // Edge: a row whose column value is SQL NULL cannot be held by Optional; get() throws
     * // NullPointerException directly (the completed future evaluates the mapping inline).
     * try {
     *     async.queryForSingleNonNull(User.class, String.class, "nullableCol", Filters.eq("id", 1L)).get(); // throws NullPointerException
     * } catch (NullPointerException ex) {
     *     // the selected column value was null
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param <V> the column-value type
     * @param targetClass the entity class identifying the target table
     * @param valueClass the type to convert the column value to
     * @param propName the property whose value is selected
     * @param whereClause the WHERE condition selecting at most one row
     * @return a future whose payload is an {@link Optional} holding the value, or empty if no row
     *         matches; the future completes exceptionally with a {@link NullPointerException} if
     *         a row was returned but the column value is {@code null}
     */
    public <T, V> ContinuableFuture<Optional<V>> queryForSingleNonNull(final Class<T> targetClass, final Class<V> valueClass, final String propName,
            final Condition whereClause) {
        final SP cp = cassandraExecutor.prepareQuery(targetClass, List.of(propName), whereClause, 1);

        return queryForSingleNonNull(valueClass, cp.query(), cp.parameters().toArray());
    }

    /**
     * Asynchronously executes the supplied CQL query and returns the first column of the first
     * row as a {@code boolean} value.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: read a boolean from a raw single-column query.
     * boolean active = async.queryForBoolean("SELECT active FROM users WHERE id = ?", 1L).get().orElse(false);
     *
     * // Typical: check presence first.
     * OptionalBoolean ob = async.queryForBoolean("SELECT active FROM users WHERE id = ?", 1L).get();
     * if (ob.isPresent()) { useFlag(ob.getAsBoolean()); }
     *
     * // Edge: no matching row (or a null value) -> the future completes with an empty OptionalBoolean.
     * boolean empty = async.queryForBoolean("SELECT active FROM users WHERE id = ?", -1L).get().isEmpty(); // returns true
     *
     * // Edge: a malformed query makes the future complete exceptionally; get() rethrows it wrapped.
     * try {
     *     async.queryForBoolean("SELECT FROM").get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver/parse failure
     * }
     * }</pre>
     *
     * @param query the parameterized CQL SELECT statement
     * @param parameters the parameter values to bind
     * @return a future whose payload is an {@link OptionalBoolean} holding the value, or empty if
     *         the query returned no row or the value is {@code null}
     */
    public final ContinuableFuture<OptionalBoolean> queryForBoolean(final String query, final Object... parameters) {
        return queryForSingleValue(Boolean.class, query, parameters).map(CassandraExecutorBase.boolean_mapper);
    }

    /**
     * Asynchronously executes the supplied CQL query and returns the first column of the first
     * row as a {@code char} value.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: read a char from a raw single-column query.
     * char grade = async.queryForChar("SELECT grade FROM users WHERE id = ?", 1L).get().orElse('-');
     *
     * // Typical: check presence first.
     * OptionalChar oc = async.queryForChar("SELECT grade FROM users WHERE id = ?", 1L).get();
     * if (oc.isPresent()) { useGrade(oc.getAsChar()); }
     *
     * // Edge: no matching row (or a null value) -> the future completes with an empty OptionalChar.
     * boolean empty = async.queryForChar("SELECT grade FROM users WHERE id = ?", -1L).get().isEmpty(); // returns true
     *
     * // Edge: a malformed query makes the future complete exceptionally; get() rethrows it wrapped.
     * try {
     *     async.queryForChar("SELECT FROM").get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver/parse failure
     * }
     * }</pre>
     *
     * @param query the parameterized CQL SELECT statement
     * @param parameters the parameter values to bind
     * @return a future whose payload is an {@link OptionalChar} holding the value, or empty if
     *         the query returned no row or the value is {@code null}
     */
    public final ContinuableFuture<OptionalChar> queryForChar(final String query, final Object... parameters) {
        return queryForSingleValue(Character.class, query, parameters).map(CassandraExecutorBase.char_mapper);
    }

    /**
     * Asynchronously executes the supplied CQL query and returns the first column of the first
     * row as a {@code byte} value.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: read a byte from a raw single-column query.
     * byte level = async.queryForByte("SELECT level FROM users WHERE id = ?", 1L).get().orElse((byte) 0);
     *
     * // Typical: check presence first.
     * OptionalByte ob = async.queryForByte("SELECT level FROM users WHERE id = ?", 1L).get();
     * if (ob.isPresent()) { useLevel(ob.getAsByte()); }
     *
     * // Edge: no matching row (or a null value) -> the future completes with an empty OptionalByte.
     * boolean empty = async.queryForByte("SELECT level FROM users WHERE id = ?", -1L).get().isEmpty(); // returns true
     *
     * // Edge: a malformed query makes the future complete exceptionally; get() rethrows it wrapped.
     * try {
     *     async.queryForByte("SELECT FROM").get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver/parse failure
     * }
     * }</pre>
     *
     * @param query the parameterized CQL SELECT statement
     * @param parameters the parameter values to bind
     * @return a future whose payload is an {@link OptionalByte} holding the value, or empty if
     *         the query returned no row or the value is {@code null}
     */
    public final ContinuableFuture<OptionalByte> queryForByte(final String query, final Object... parameters) {
        return queryForSingleValue(Byte.class, query, parameters).map(CassandraExecutorBase.byte_mapper);
    }

    /**
     * Asynchronously executes the supplied CQL query and returns the first column of the first
     * row as a {@code short} value.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: read a short from a raw single-column query.
     * short cnt = async.queryForShort("SELECT count FROM users WHERE id = ?", 1L).get().orElse((short) 0);
     *
     * // Typical: check presence first.
     * OptionalShort os = async.queryForShort("SELECT count FROM users WHERE id = ?", 1L).get();
     * if (os.isPresent()) { useCount(os.getAsShort()); }
     *
     * // Edge: no matching row (or a null value) -> the future completes with an empty OptionalShort.
     * boolean empty = async.queryForShort("SELECT count FROM users WHERE id = ?", -1L).get().isEmpty(); // returns true
     *
     * // Edge: a malformed query makes the future complete exceptionally; get() rethrows it wrapped.
     * try {
     *     async.queryForShort("SELECT FROM").get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver/parse failure
     * }
     * }</pre>
     *
     * @param query the parameterized CQL SELECT statement
     * @param parameters the parameter values to bind
     * @return a future whose payload is an {@link OptionalShort} holding the value, or empty if
     *         the query returned no row or the value is {@code null}
     */
    public final ContinuableFuture<OptionalShort> queryForShort(final String query, final Object... parameters) {
        return queryForSingleValue(Short.class, query, parameters).map(CassandraExecutorBase.short_mapper);
    }

    /**
     * Asynchronously executes the supplied CQL query and returns the first column of the first
     * row as an {@code int} value.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: read an int from a raw single-column query.
     * int age = async.queryForInt("SELECT age FROM users WHERE id = ?", 1L).get().orElse(0);
     *
     * // Typical: a COUNT(*) read as an int.
     * int total = async.queryForInt("SELECT COUNT(*) FROM users").get().orElse(0);
     *
     * // Edge: no matching row (or a null value) -> the future completes with an empty OptionalInt.
     * boolean empty = async.queryForInt("SELECT age FROM users WHERE id = ?", -1L).get().isEmpty(); // returns true
     *
     * // Edge: a malformed query makes the future complete exceptionally; get() rethrows it wrapped.
     * try {
     *     async.queryForInt("SELECT FROM").get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver/parse failure
     * }
     * }</pre>
     *
     * @param query the parameterized CQL SELECT statement
     * @param parameters the parameter values to bind
     * @return a future whose payload is an {@link OptionalInt} holding the value, or empty if the
     *         query returned no row or the value is {@code null}
     */
    public final ContinuableFuture<OptionalInt> queryForInt(final String query, final Object... parameters) {
        return queryForSingleValue(Integer.class, query, parameters).map(CassandraExecutorBase.int_mapper);
    }

    /**
     * Asynchronously executes the supplied CQL query and returns the first column of the first
     * row as a {@code long} value.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: a row-count query read as a long (preferred over the deprecated count method).
     * long total = async.queryForLong("SELECT COUNT(*) FROM users WHERE status = ?", "active").get().orElse(0L);
     *
     * // Typical: read a long id column.
     * long id = async.queryForLong("SELECT id FROM users WHERE email = ?", "a@b.com").get().orElse(0L);
     *
     * // Edge: no matching row (or a null value) -> the future completes with an empty OptionalLong.
     * boolean empty = async.queryForLong("SELECT id FROM users WHERE id = ?", -1L).get().isEmpty(); // returns true
     *
     * // Edge: a malformed query makes the future complete exceptionally; get() rethrows it wrapped.
     * try {
     *     async.queryForLong("SELECT FROM").get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver/parse failure
     * }
     * }</pre>
     *
     * @param query the parameterized CQL SELECT statement
     * @param parameters the parameter values to bind
     * @return a future whose payload is an {@link OptionalLong} holding the value, or empty if
     *         the query returned no row or the value is {@code null}
     */
    public final ContinuableFuture<OptionalLong> queryForLong(final String query, final Object... parameters) {
        return queryForSingleValue(Long.class, query, parameters).map(CassandraExecutorBase.long_mapper);
    }

    /**
     * Asynchronously executes the supplied CQL query and returns the first column of the first
     * row as a {@code float} value.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: read a float from a raw single-column query.
     * float score = async.queryForFloat("SELECT score FROM users WHERE id = ?", 1L).get().orElse(0f);
     *
     * // Typical: check presence first.
     * OptionalFloat of = async.queryForFloat("SELECT score FROM users WHERE id = ?", 1L).get();
     * if (of.isPresent()) { useScore(of.getAsFloat()); }
     *
     * // Edge: no matching row (or a null value) -> the future completes with an empty OptionalFloat.
     * boolean empty = async.queryForFloat("SELECT score FROM users WHERE id = ?", -1L).get().isEmpty(); // returns true
     *
     * // Edge: a malformed query makes the future complete exceptionally; get() rethrows it wrapped.
     * try {
     *     async.queryForFloat("SELECT FROM").get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver/parse failure
     * }
     * }</pre>
     *
     * @param query the parameterized CQL SELECT statement
     * @param parameters the parameter values to bind
     * @return a future whose payload is an {@link OptionalFloat} holding the value, or empty if
     *         the query returned no row or the value is {@code null}
     */
    public final ContinuableFuture<OptionalFloat> queryForFloat(final String query, final Object... parameters) {
        return queryForSingleValue(Float.class, query, parameters).map(CassandraExecutorBase.float_mapper);
    }

    /**
     * Asynchronously executes the supplied CQL query and returns the first column of the first
     * row as a {@code double} value.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: read a double from a raw single-column query.
     * double price = async.queryForDouble("SELECT price FROM users WHERE id = ?", 1L).get().orElse(0d);
     *
     * // Typical: check presence first.
     * OptionalDouble od = async.queryForDouble("SELECT price FROM users WHERE id = ?", 1L).get();
     * if (od.isPresent()) { usePrice(od.getAsDouble()); }
     *
     * // Edge: no matching row (or a null value) -> the future completes with an empty OptionalDouble.
     * boolean empty = async.queryForDouble("SELECT price FROM users WHERE id = ?", -1L).get().isEmpty(); // returns true
     *
     * // Edge: a malformed query makes the future complete exceptionally; get() rethrows it wrapped.
     * try {
     *     async.queryForDouble("SELECT FROM").get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver/parse failure
     * }
     * }</pre>
     *
     * @param query the parameterized CQL SELECT statement
     * @param parameters the parameter values to bind
     * @return a future whose payload is an {@link OptionalDouble} holding the value, or empty if
     *         the query returned no row or the value is {@code null}
     */
    public final ContinuableFuture<OptionalDouble> queryForDouble(final String query, final Object... parameters) {
        return queryForSingleValue(Double.class, query, parameters).map(CassandraExecutorBase.double_mapper);
    }

    /**
     * Asynchronously executes the supplied CQL query and returns the first column of the first
     * row as a {@link String} value.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: read a string from a raw single-column query.
     * String name = async.queryForString("SELECT name FROM users WHERE id = ?", 1L).get().orElse("");
     *
     * // Typical: distinguish "no row" from "SQL NULL value".
     * Nullable<String> nv = async.queryForString("SELECT name FROM users WHERE id = ?", 1L).get();
     * boolean rowFound = nv.isPresent();
     * boolean isSqlNull = nv.isPresent() && nv.isNull();
     *
     * // Edge: no matching row -> the future completes with an empty Nullable.
     * boolean empty = async.queryForString("SELECT name FROM users WHERE id = ?", -1L).get().isEmpty(); // returns true
     *
     * // Edge: a malformed query makes the future complete exceptionally; get() rethrows it wrapped.
     * try {
     *     async.queryForString("SELECT FROM").get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver/parse failure
     * }
     * }</pre>
     *
     * @param query the parameterized CQL SELECT statement
     * @param parameters the parameter values to bind
     * @return a future whose payload is a {@link Nullable} holding the {@code String} value, or
     *         empty if the query returned no row; the value is {@code null} when the column is
     *         SQL NULL
     */
    public final ContinuableFuture<Nullable<String>> queryForString(final String query, final Object... parameters) {
        return queryForSingleValue(String.class, query, parameters);
    }

    /**
     * Asynchronously executes the supplied CQL query and returns the first column of the first
     * row coerced to {@code valueClass}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: read a single column coerced to a chosen type.
     * Nullable<String> name = async.queryForSingleValue(String.class, "SELECT name FROM users WHERE id = ?", 1L).get();
     * String n = name.orElse("");
     *
     * // Typical: numeric coercion.
     * Long id = async.queryForSingleValue(Long.class, "SELECT id FROM users WHERE email = ?", "a@b.com").get().orElse(0L);
     *
     * // Edge: no matching row -> the future completes with an empty Nullable; a present-but-null value is allowed.
     * boolean empty = async.queryForSingleValue(String.class, "SELECT name FROM users WHERE id = ?", -1L).get().isEmpty(); // returns true
     *
     * // Edge: a malformed query makes the future complete exceptionally; get() rethrows it wrapped.
     * try {
     *     async.queryForSingleValue(String.class, "SELECT FROM").get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver/parse failure
     * }
     * }</pre>
     *
     * @param <T> the column-value type
     * @param valueClass the type to convert the column value to
     * @param query the parameterized CQL SELECT statement
     * @param parameters the parameter values to bind
     * @return a future whose payload is a {@link Nullable} holding the value, or empty if the
     *         query returned no row; the value is {@code null} when the column is SQL NULL
     */
    public <T> ContinuableFuture<Nullable<T>> queryForSingleValue(final Class<T> valueClass, final String query, final Object... parameters) {
        return execute(query, parameters).map(resultSet -> {
            final java.util.Iterator<RW> iter = resultSet.iterator();

            return iter.hasNext() ? Nullable.of(cassandraExecutor.createRowMapper(valueClass).apply(iter.next())) : Nullable.empty();
        });
    }

    /**
     * Asynchronously executes the supplied CQL query and returns the first column of the first
     * row coerced to {@code valueClass}, requiring a non-null value when a row is present.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: read a column that is non-null when a row is present.
     * Optional<String> name = async.queryForSingleNonNull(String.class, "SELECT name FROM users WHERE id = ?", 1L).get();
     * String n = name.orElse("");
     *
     * // Typical: numeric coercion of a required column.
     * Optional<Long> id = async.queryForSingleNonNull(Long.class, "SELECT id FROM users WHERE email = ?", "a@b.com").get();
     *
     * // Edge: no matching row -> the future completes with an empty Optional (no exception).
     * boolean empty = async.queryForSingleNonNull(String.class, "SELECT name FROM users WHERE id = ?", -1L).get().isEmpty(); // returns true
     *
     * // Edge: a row whose value is SQL NULL cannot be held by Optional; get() throws
     * // NullPointerException directly (the completed future evaluates the mapping inline).
     * try {
     *     async.queryForSingleNonNull(String.class, "SELECT null_col FROM users WHERE id = ?", 1L).get(); // throws NullPointerException
     * } catch (NullPointerException ex) {
     *     // the selected column value was null
     * }
     * }</pre>
     *
     * @param <T> the column-value type
     * @param valueClass the type to convert the column value to
     * @param query the parameterized CQL SELECT statement
     * @param parameters the parameter values to bind
     * @return a future whose payload is an {@link Optional} holding the value, or empty if the
     *         query returned no row; the future completes exceptionally with a
     *         {@link NullPointerException} if a row was returned but the column value is
     *         {@code null} (since {@link Optional} cannot hold {@code null})
     */
    public <T> ContinuableFuture<Optional<T>> queryForSingleNonNull(final Class<T> valueClass, final String query, final Object... parameters) {
        return execute(query, parameters).map(resultSet -> {
            final java.util.Iterator<RW> iter = resultSet.iterator();

            return iter.hasNext() ? Optional.of(cassandraExecutor.createRowMapper(valueClass).apply(iter.next())) : Optional.empty();
        });
    }

    /**
     * Asynchronously executes a CQL statement without parameters.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: run a parameterless statement and block for the result set.
     * RS rs = async.execute("SELECT * FROM users").get();
     *
     * // Typical: run a DDL/maintenance statement asynchronously.
     * async.execute("TRUNCATE users").thenRun(() -> log.info("truncated")); // runs after the statement completes
     *
     * // Edge: a malformed statement makes the future complete exceptionally; get() rethrows it wrapped.
     * try {
     *     async.execute("SELECT FROM").get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver/parse failure
     * }
     *
     * // Edge: a statement against a missing table likewise completes the future exceptionally.
     * try {
     *     async.execute("SELECT * FROM no_such_table").get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param query the CQL statement to execute
     * @return a future that completes with the result set from the query execution
     */
    public abstract ContinuableFuture<RS> execute(final String query);

    /**
     * Asynchronously executes a parameterized CQL statement.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: bind positional parameters and block for the result set.
     * RS rs = async.execute("SELECT * FROM users WHERE id = ?", 1L).get();
     *
     * // Typical: run a parameterized write.
     * async.execute("UPDATE users SET name = ? WHERE id = ?", "Alice", 1L).get();
     *
     * // Edge: a statement affecting no rows still completes normally.
     * async.execute("UPDATE users SET name = ? WHERE id = ?", "x", -1L).get(); // returns a result set; no row changed
     *
     * // Edge: a malformed statement makes the future complete exceptionally; get() rethrows it wrapped.
     * try {
     *     async.execute("SELECT FROM", 1L).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver/parse failure
     * }
     * }</pre>
     *
     * @param query the parameterized CQL statement
     * @param parameters the parameter values (can be individual values, arrays, collections, maps, or entities)
     * @return a future that completes with the result set from the query execution
     */
    public abstract ContinuableFuture<RS> execute(final String query, final Object... parameters);

    /**
     * Asynchronously executes a CQL statement with named parameters provided as a Map.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: bind named parameters from a Map.
     * Map<String, Object> params = new HashMap<>();
     * params.put("id", 1L);
     * RS rs = async.execute("SELECT * FROM users WHERE id = :id", params).get();
     *
     * // Typical: an empty parameter map for a statement that has no placeholders.
     * async.execute("SELECT * FROM users", new HashMap<String, Object>()).get(); // returns the result set
     *
     * // Edge: a missing named parameter makes the future complete exceptionally; get() rethrows it wrapped.
     * try {
     *     async.execute("SELECT * FROM users WHERE id = :id", new HashMap<String, Object>()).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the binding/driver failure
     * }
     *
     * // Edge: a malformed statement likewise completes the future exceptionally.
     * try {
     *     async.execute("SELECT FROM", params).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver/parse failure
     * }
     * }</pre>
     *
     * @param query the parameterized CQL statement with named parameters
     * @param parameters a Map containing parameter names as keys and parameter values as values
     * @return a future that completes with the result set from the query execution
     */
    public abstract ContinuableFuture<RS> execute(String query, Map<String, Object> parameters);

    /**
     * Asynchronously executes a pre-configured CQL statement.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async();
     *
     * // Typical: run a fully-built driver statement.
     * Statement<?> stmt = SimpleStatement.newInstance("SELECT * FROM users WHERE id = ?", 1L);
     * RS rs = async.execute(stmt).get();
     *
     * // Typical: apply per-statement options (e.g. page size) before executing.
     * Statement<?> paged = SimpleStatement.newInstance("SELECT * FROM users").setPageSize(500);
     * async.execute(paged).thenAccept(result -> process(result)); // processes the result set when ready
     *
     * // Edge: a null statement makes the future complete exceptionally; get() rethrows it wrapped.
     * try {
     *     async.execute((Statement<?>) null).get(); // throws ExecutionException (cause NullPointerException)
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is a NullPointerException
     * }
     *
     * // Edge: a statement against a missing table likewise completes the future exceptionally.
     * try {
     *     async.execute(SimpleStatement.newInstance("SELECT * FROM no_such_table")).get(); // throws ExecutionException
     * } catch (ExecutionException ex) {
     *     // ex.getCause() is the driver failure
     * }
     * }</pre>
     *
     * @param statement the configured CQL statement to execute
     * @return a future that completes with the result set from the statement execution
     */
    public abstract ContinuableFuture<RS> execute(final ST statement);

    /**
     * Asynchronously executes a prepared query with its parameters.
     *
     * @param cp the prepared query with parameters
     * @return a future that completes with the result set from the query execution
     */
    protected ContinuableFuture<RS> execute(final SP cp) {
        return execute(cp.query(), cp.parameters().toArray());
    }
}
