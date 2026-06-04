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
     * @param targetClass the entity class identifying the target table and column mappings
     * @param props the column-name to column-value map to insert
     * @return a future whose payload is the driver result set produced by the INSERT
     */
    public ContinuableFuture<RS> insert(final Class<?> targetClass, final Map<String, Object> props) {
        return execute(cassandraExecutor.prepareInsert(targetClass, props));
    }

    /**
     * Asynchronously inserts a collection of entities as a single CQL batch.
     *
     * @param entities the entities to insert; must be non-empty
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
     * @param query the CQL statement to execute
     * @return a future that completes with the result set from the query execution
     */
    public abstract ContinuableFuture<RS> execute(final String query);

    /**
     * Asynchronously executes a parameterized CQL statement.
     *
     * @param query the parameterized CQL statement
     * @param parameters the parameter values (can be individual values, arrays, collections, maps, or entities)
     * @return a future that completes with the result set from the query execution
     */
    public abstract ContinuableFuture<RS> execute(final String query, final Object... parameters);

    /**
     * Asynchronously executes a CQL statement with named parameters provided as a Map.
     *
     * @param query the parameterized CQL statement with named parameters
     * @param parameters a Map containing parameter names as keys and parameter values as values
     * @return a future that completes with the result set from the query execution
     */
    public abstract ContinuableFuture<RS> execute(String query, Map<String, Object> parameters);

    /**
     * Asynchronously executes a pre-configured CQL statement.
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
