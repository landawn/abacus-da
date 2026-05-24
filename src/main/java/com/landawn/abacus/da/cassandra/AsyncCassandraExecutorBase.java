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

    protected final CassandraExecutorBase<RW, RS, ST, PS, BT> cassandraExecutor;

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

    public final <T> ContinuableFuture<Optional<T>> get(final Class<T> targetClass, final Object... ids) {
        return get(targetClass, null, ids);
    }

    public final <T> ContinuableFuture<Optional<T>> get(final Class<T> targetClass, final Collection<String> selectPropNames, final Object... ids) {
        return get(targetClass, selectPropNames, CassandraExecutorBase.idsToCondition(targetClass, ids));
    }

    public <T> ContinuableFuture<Optional<T>> get(final Class<T> targetClass, final Condition whereClause) {
        return get(targetClass, null, whereClause);
    }

    public <T> ContinuableFuture<Optional<T>> get(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP cp = cassandraExecutor.prepareQuery(targetClass, selectPropNames, whereClause, 2);

        return execute(cp).map(resultSet -> Optional.ofNullable(cassandraExecutor.fetchOnlyOne(targetClass, resultSet)));
    }

    public final <T> ContinuableFuture<T> gett(final Class<T> targetClass, final Object... ids) {
        return gett(targetClass, null, ids);
    }

    public final <T> ContinuableFuture<T> gett(final Class<T> targetClass, final Collection<String> selectPropNames, final Object... ids) {
        return gett(targetClass, selectPropNames, CassandraExecutorBase.idsToCondition(targetClass, ids));
    }

    public <T> ContinuableFuture<T> gett(final Class<T> targetClass, final Condition whereClause) {
        return gett(targetClass, null, whereClause);
    }

    public <T> ContinuableFuture<T> gett(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP cp = cassandraExecutor.prepareQuery(targetClass, selectPropNames, whereClause, 2);

        return execute(cp).map(resultSet -> cassandraExecutor.fetchOnlyOne(targetClass, resultSet));
    }

    public ContinuableFuture<RS> insert(final Object entity) {
        return execute(cassandraExecutor.prepareInsert(entity));
    }

    public ContinuableFuture<RS> insert(final Class<?> targetClass, final Map<String, Object> props) {
        return execute(cassandraExecutor.prepareInsert(targetClass, props));
    }

    public ContinuableFuture<RS> batchInsert(final Collection<?> entities, final BT type) {
        return execute(cassandraExecutor.prepareBatchInsertStatement(entities, type));
    }

    public ContinuableFuture<RS> batchInsert(final Class<?> targetClass, final Collection<? extends Map<String, Object>> propsList, final BT type) {
        return execute(cassandraExecutor.prepareBatchInsertStatement(targetClass, propsList, type));
    }

    public ContinuableFuture<RS> update(final Object entity) {
        final Class<?> entityClass = entity.getClass();
        final Set<String> keyNameSet = getKeyNameSet(entityClass);
        final Collection<String> updatePropNames = QueryUtil.getUpdatePropNames(entityClass, keyNameSet);

        return update(entity, updatePropNames);
    }

    public ContinuableFuture<RS> update(final Object entity, final Collection<String> propNamesToUpdate) {
        N.checkArgument(N.notEmpty(propNamesToUpdate), "'propNamesToUpdate' can't be null or empty");

        return execute(cassandraExecutor.prepareUpdate(entity, propNamesToUpdate));
    }

    public ContinuableFuture<RS> update(final Class<?> targetClass, final Map<String, Object> props, final Condition whereClause) {
        return execute(cassandraExecutor.prepareUpdate(targetClass, props, whereClause));
    }

    public ContinuableFuture<RS> update(final String query, final Object... parameters) {
        return execute(query, parameters);
    }

    public ContinuableFuture<RS> batchUpdate(final Collection<?> entities, final BT type) {
        N.checkArgument(N.notEmpty(entities), "'entities' can't be null or empty.");

        final Object firstEntity = N.firstOrNullIfEmpty(entities);
        N.checkArgNotNull(firstEntity, "The first entity in the collection can't be null.");
        final Class<?> entityClass = firstEntity.getClass();
        final Set<String> keyNameSet = getKeyNameSet(entityClass);
        final Collection<String> updatePropNames = QueryUtil.getUpdatePropNames(entityClass, keyNameSet);

        return batchUpdate(entities, updatePropNames, type);
    }

    public ContinuableFuture<RS> batchUpdate(final Collection<?> entities, final Collection<String> propNamesToUpdate, final BT type) {
        N.checkArgument(N.notEmpty(propNamesToUpdate), "'propNamesToUpdate' can't be null or empty");

        return execute(cassandraExecutor.prepareBatchUpdateStatement(entities, propNamesToUpdate, type));
    }

    public ContinuableFuture<RS> batchUpdate(final Class<?> targetClass, final Collection<? extends Map<String, Object>> propsList, final BT type) {
        return execute(cassandraExecutor.prepareBatchUpdateStatement(targetClass, propsList, type));
    }

    public ContinuableFuture<RS> batchUpdate(final String query, final Collection<?> parametersList, final BT type) {
        return execute(cassandraExecutor.prepareBatchUpdateStatement(query, parametersList, type));
    }

    public ContinuableFuture<RS> delete(final Object entity) {
        return delete(entity, null);
    }

    public ContinuableFuture<RS> delete(final Object entity, final Collection<String> propNamesToDelete) {
        N.checkArgument(propNamesToDelete == null || N.notEmpty(propNamesToDelete), "'propNamesToDelete' can't be null or empty");

        return delete(entity.getClass(), propNamesToDelete, CassandraExecutorBase.entityToCondition(entity));
    }

    public final ContinuableFuture<RS> delete(final Class<?> targetClass, final Object... ids) {
        return delete(targetClass, null, ids);
    }

    public final ContinuableFuture<RS> delete(final Class<?> targetClass, final Collection<String> propNamesToDelete, final Object... ids) {
        N.checkArgument(propNamesToDelete == null || N.notEmpty(propNamesToDelete), "'propNamesToDelete' can't be null or empty");

        return delete(targetClass, propNamesToDelete, CassandraExecutorBase.idsToCondition(targetClass, ids));
    }

    public ContinuableFuture<RS> delete(final Class<?> targetClass, final Condition whereClause) {
        return delete(targetClass, null, whereClause);
    }

    public ContinuableFuture<RS> delete(final Class<?> targetClass, final Collection<String> propNamesToDelete, final Condition whereClause) {
        N.checkArgument(propNamesToDelete == null || N.notEmpty(propNamesToDelete), "'propNamesToDelete' can't be null or empty");

        return execute(cassandraExecutor.prepareDelete(targetClass, propNamesToDelete, whereClause));
    }

    public ContinuableFuture<RS> batchDelete(final Collection<?> entities) {
        N.checkArgument(N.notEmpty(entities), "'entities' can't be null or empty.");

        final Object firstEntity = N.firstOrNullIfEmpty(entities);
        N.checkArgNotNull(firstEntity, "The first entity in the collection can't be null.");
        final Class<?> entityClass = firstEntity.getClass();
        final Condition cond = CassandraExecutorBase.entityToCondition(entityClass, entities);

        return delete(entityClass, cond);
    }

    public ContinuableFuture<RS> batchDelete(final Collection<?> entities, final Collection<String> propNamesToDelete) {
        N.checkArgument(N.notEmpty(entities), "'entities' can't be null or empty.");
        N.checkArgument(propNamesToDelete == null || N.notEmpty(propNamesToDelete), "'propNamesToDelete' can't be null or empty");

        final Object firstEntity = N.firstOrNullIfEmpty(entities);
        N.checkArgNotNull(firstEntity, "The first entity in the collection can't be null.");
        final Class<?> entityClass = firstEntity.getClass();
        final Condition cond = CassandraExecutorBase.entityToCondition(entityClass, entities);

        return delete(entityClass, propNamesToDelete, cond);
    }

    public final ContinuableFuture<Boolean> exists(final Class<?> targetClass, final Object... ids) {
        return exists(targetClass, CassandraExecutorBase.idsToCondition(targetClass, ids));
    }

    public ContinuableFuture<Boolean> exists(final Class<?> targetClass, final Condition whereClause) {
        final SP cp = cassandraExecutor.prepareQuery(targetClass, getKeyNames(targetClass), whereClause, 1);

        return exists(cp.query(), cp.parameters().toArray());
    }

    public final ContinuableFuture<Boolean> exists(final String query, final Object... parameters) {
        return execute(query, parameters).map(CassandraExecutorBase.exists_mapper);
    }

    @SuppressWarnings("deprecation")
    public ContinuableFuture<Long> count(final Class<?> targetClass, final Condition whereClause) {
        final SP cp = cassandraExecutor.prepareQuery(targetClass, CassandraExecutorBase.COUNT_SELECT_PROP_NAMES, whereClause, 0);

        return count(cp.query(), cp.parameters().toArray());
    }

    /**
     * Asynchronously counts the number of records returned by the given query.
     *
     * @param query the CQL query to execute (should return a count)
     * @param parameters the query parameters
     * @return a future that completes with the count of matching records, or 0 if none found
     * @deprecated Use {@link #queryForLong(String, Object...)} with {@code COUNT(*)} in the query instead.
     */
    @Deprecated
    public final ContinuableFuture<Long> count(final String query, final Object... parameters) {
        return queryForSingleValue(Long.class, query, parameters).map(CassandraExecutorBase.long_secondMapper);
    }

    public <T> ContinuableFuture<List<T>> list(final Class<T> targetClass, final Condition whereClause) {
        return list(targetClass, null, whereClause);
    }

    public <T> ContinuableFuture<List<T>> list(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP cp = cassandraExecutor.prepareQuery(targetClass, selectPropNames, whereClause);

        return list(targetClass, cp.query(), cp.parameters().toArray());
    }

    public final ContinuableFuture<List<Map<String, Object>>> list(final String query, final Object... parameters) {
        return list(Clazz.PROPS_MAP, query, parameters);
    }

    public final <T> ContinuableFuture<List<T>> list(final Class<T> targetClass, final String query, final Object... parameters) {
        return execute(query, parameters).map(resultSet -> cassandraExecutor.toList(targetClass, resultSet));
    }

    public <T> ContinuableFuture<Dataset> query(final Class<T> targetClass, final Condition whereClause) {
        return query(targetClass, null, whereClause);
    }

    public <T> ContinuableFuture<Dataset> query(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP cp = cassandraExecutor.prepareQuery(targetClass, selectPropNames, whereClause);

        return query(targetClass, cp.query(), cp.parameters().toArray());
    }

    public final ContinuableFuture<Dataset> query(final String query, final Object... parameters) {
        return query(Map.class, query, parameters);
    }

    public final ContinuableFuture<Dataset> query(final Class<?> targetClass, final String query, final Object... parameters) {
        return execute(query, parameters).map(resultSet -> cassandraExecutor.extractData(targetClass, resultSet));
    }

    public <T> ContinuableFuture<Stream<T>> stream(final Class<T> targetClass, final Condition whereClause) {
        return stream(targetClass, null, whereClause);
    }

    public <T> ContinuableFuture<Stream<T>> stream(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP cp = cassandraExecutor.prepareQuery(targetClass, selectPropNames, whereClause);

        return stream(targetClass, cp.query(), cp.parameters().toArray());
    }

    public ContinuableFuture<Stream<Object[]>> stream(final String query, final Object... parameters) {
        return stream(Object[].class, query, parameters);
    }

    public final <T> ContinuableFuture<Stream<T>> stream(final Class<T> targetClass, final String query, final Object... parameters) {
        return execute(query, parameters).map(resultSet -> Stream.of(resultSet.iterator()).map(cassandraExecutor.createRowMapper(targetClass)));
    }

    public <T> ContinuableFuture<Stream<T>> stream(final Class<T> targetClass, final ST statement) {
        return execute(statement).map(resultSet -> Stream.of(resultSet.iterator()).map(cassandraExecutor.createRowMapper(targetClass)));
    }

    public <T> ContinuableFuture<Optional<T>> findFirst(final Class<T> targetClass, final Condition whereClause) {
        return findFirst(targetClass, null, whereClause);
    }

    public <T> ContinuableFuture<Optional<T>> findFirst(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP cp = cassandraExecutor.prepareQuery(targetClass, selectPropNames, whereClause, 1);

        return findFirst(targetClass, cp.query(), cp.parameters().toArray());
    }

    public final ContinuableFuture<Optional<Map<String, Object>>> findFirst(final String query, final Object... parameters) {
        return findFirst(Clazz.PROPS_MAP, query, parameters);
    }

    public <T> ContinuableFuture<Optional<T>> findFirst(final Class<T> targetClass, final String query, final Object... parameters) {
        return execute(query, parameters).map(resultSet -> {
            final java.util.Iterator<RW> iter = resultSet.iterator();

            return iter.hasNext() ? Optional.of(cassandraExecutor.createRowMapper(targetClass).apply(iter.next())) : Optional.empty();
        });
    }

    public <T> ContinuableFuture<OptionalBoolean> queryForBoolean(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, Boolean.class, propName, whereClause).map(CassandraExecutorBase.boolean_mapper);
    }

    public <T> ContinuableFuture<OptionalChar> queryForChar(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, Character.class, propName, whereClause).map(CassandraExecutorBase.char_mapper);
    }

    public <T> ContinuableFuture<OptionalByte> queryForByte(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, Byte.class, propName, whereClause).map(CassandraExecutorBase.byte_mapper);
    }

    public <T> ContinuableFuture<OptionalShort> queryForShort(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, Short.class, propName, whereClause).map(CassandraExecutorBase.short_mapper);
    }

    public <T> ContinuableFuture<OptionalInt> queryForInt(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, Integer.class, propName, whereClause).map(CassandraExecutorBase.int_mapper);
    }

    public <T> ContinuableFuture<OptionalLong> queryForLong(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, Long.class, propName, whereClause).map(CassandraExecutorBase.long_mapper);
    }

    public <T> ContinuableFuture<OptionalFloat> queryForFloat(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, Float.class, propName, whereClause).map(CassandraExecutorBase.float_mapper);
    }

    public <T> ContinuableFuture<OptionalDouble> queryForDouble(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, Double.class, propName, whereClause).map(CassandraExecutorBase.double_mapper);
    }

    public <T> ContinuableFuture<Nullable<String>> queryForString(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, String.class, propName, whereClause);
    }

    public <T> ContinuableFuture<Nullable<Date>> queryForDate(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, Date.class, propName, whereClause);
    }

    public <T, E extends Date> ContinuableFuture<Nullable<E>> queryForDate(final Class<T> targetClass, final Class<E> valueClass, final String propName,
            final Condition whereClause) {
        return queryForSingleValue(targetClass, valueClass, propName, whereClause);
    }

    public <T, V> ContinuableFuture<Nullable<V>> queryForSingleValue(final Class<T> targetClass, final Class<V> valueClass, final String propName,
            final Condition whereClause) {
        final SP cp = cassandraExecutor.prepareQuery(targetClass, List.of(propName), whereClause, 1);

        return queryForSingleValue(valueClass, cp.query(), cp.parameters().toArray());
    }

    public <T, V> ContinuableFuture<Optional<V>> queryForSingleNonNull(final Class<T> targetClass, final Class<V> valueClass, final String propName,
            final Condition whereClause) {
        final SP cp = cassandraExecutor.prepareQuery(targetClass, List.of(propName), whereClause, 1);

        return queryForSingleNonNull(valueClass, cp.query(), cp.parameters().toArray());
    }

    public final ContinuableFuture<OptionalBoolean> queryForBoolean(final String query, final Object... parameters) {
        return queryForSingleValue(Boolean.class, query, parameters).map(CassandraExecutorBase.boolean_mapper);
    }

    public final ContinuableFuture<OptionalChar> queryForChar(final String query, final Object... parameters) {
        return queryForSingleValue(Character.class, query, parameters).map(CassandraExecutorBase.char_mapper);
    }

    public final ContinuableFuture<OptionalByte> queryForByte(final String query, final Object... parameters) {
        return queryForSingleValue(Byte.class, query, parameters).map(CassandraExecutorBase.byte_mapper);
    }

    public final ContinuableFuture<OptionalShort> queryForShort(final String query, final Object... parameters) {
        return queryForSingleValue(Short.class, query, parameters).map(CassandraExecutorBase.short_mapper);
    }

    public final ContinuableFuture<OptionalInt> queryForInt(final String query, final Object... parameters) {
        return queryForSingleValue(Integer.class, query, parameters).map(CassandraExecutorBase.int_mapper);
    }

    public final ContinuableFuture<OptionalLong> queryForLong(final String query, final Object... parameters) {
        return queryForSingleValue(Long.class, query, parameters).map(CassandraExecutorBase.long_mapper);
    }

    public final ContinuableFuture<OptionalFloat> queryForFloat(final String query, final Object... parameters) {
        return queryForSingleValue(Float.class, query, parameters).map(CassandraExecutorBase.float_mapper);
    }

    public final ContinuableFuture<OptionalDouble> queryForDouble(final String query, final Object... parameters) {
        return queryForSingleValue(Double.class, query, parameters).map(CassandraExecutorBase.double_mapper);
    }

    public final ContinuableFuture<Nullable<String>> queryForString(final String query, final Object... parameters) {
        return queryForSingleValue(String.class, query, parameters);
    }

    public <T> ContinuableFuture<Nullable<T>> queryForSingleValue(final Class<T> valueClass, final String query, final Object... parameters) {
        return execute(query, parameters).map(resultSet -> {
            final java.util.Iterator<RW> iter = resultSet.iterator();

            return iter.hasNext() ? Nullable.of(cassandraExecutor.createRowMapper(valueClass).apply(iter.next())) : Nullable.empty();
        });
    }

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
