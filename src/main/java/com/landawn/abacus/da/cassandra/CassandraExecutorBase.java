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
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import com.landawn.abacus.annotation.Beta;
import com.landawn.abacus.da.cassandra.CqlBuilder.NAC;
import com.landawn.abacus.da.cassandra.CqlBuilder.NLC;
import com.landawn.abacus.da.cassandra.CqlBuilder.NSC;
import com.landawn.abacus.exception.DuplicateResultException;
import com.landawn.abacus.parser.ParserUtil;
import com.landawn.abacus.parser.ParserUtil.BeanInfo;
import com.landawn.abacus.query.AbstractQueryBuilder.SP;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.QueryUtil;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.util.Beans;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.Clazz;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.ImmutableList;
import com.landawn.abacus.util.ImmutableSet;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.Strings;
import com.landawn.abacus.util.Throwables;
import com.landawn.abacus.util.Tuple;
import com.landawn.abacus.util.Tuple.Tuple2;
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
import com.landawn.abacus.util.function.ToBooleanFunction;
import com.landawn.abacus.util.function.ToByteFunction;
import com.landawn.abacus.util.function.ToCharFunction;
import com.landawn.abacus.util.function.ToDoubleFunction;
import com.landawn.abacus.util.function.ToFloatFunction;
import com.landawn.abacus.util.function.ToIntFunction;
import com.landawn.abacus.util.function.ToLongFunction;
import com.landawn.abacus.util.function.ToShortFunction;
import com.landawn.abacus.util.stream.Stream;

/**
 * Abstract base class providing common functionality for Cassandra database executors.
 * 
 * <p>This abstract class serves as the foundation for both modern and legacy Cassandra executors,
 * implementing shared functionality such as entity operations, query building, result processing,
 * and parameter binding. It provides a comprehensive set of database operations while remaining
 * agnostic to the specific Cassandra driver version.</p>
 *
 * <h2>Core Responsibilities</h2>
 * <h3>Primary Functions</h3>
 * <ul>
 * <li><strong>Entity Operations:</strong>
 *     <ul>
 *     <li>CRUD operations (Create, Read, Update, Delete) for entity classes</li>
 *     <li>Batch operations for multiple entities</li>
 *     <li>Conditional operations with IF EXISTS/IF NOT EXISTS</li>
 *     <li>TTL and timestamp support for data lifecycle management</li>
 *     </ul>
 * </li>
 * <li><strong>Query Building and Execution:</strong>
 *     <ul>
 *     <li>Dynamic CQL generation using {@link CqlBuilder} integration</li>
 *     <li>Parameter binding support for multiple formats (positional, named, entity-based)</li>
 *     <li>Prepared statement management and caching</li>
 *     <li>Result set processing and mapping</li>
 *     </ul>
 * </li>
 * <li><strong>Type System Integration:</strong>
 *     <ul>
 *     <li>Automatic type conversion and validation</li>
 *     <li>Support for primitive types, collections, and custom objects</li>
 *     <li>Configurable naming policies for property-to-column mapping</li>
 *     <li>Bean introspection and reflection-based operations</li>
 *     </ul>
 * </li>
 * <li><strong>Advanced Features:</strong>
 *     <ul>
 *     <li>Asynchronous operation support with {@link ContinuableFuture}</li>
 *     <li>Stream-based result processing for large datasets</li>
 *     <li>Custom row mappers and result transformations</li>
 *     <li>Integration with condition factories for dynamic WHERE clauses</li>
 *     </ul>
 * </li>
 * </ul>
 * 
 * <h3>Generic Type Parameters</h3>
 * <p>This class uses generic types to abstract away driver-specific implementations:</p>
 * <ul>
 * <li><strong>RW:</strong> Row type (e.g., {@code com.datastax.oss.driver.api.core.cql.Row})</li>
 * <li><strong>RS:</strong> ResultSet type (e.g., {@code com.datastax.oss.driver.api.core.cql.ResultSet})</li>
 * <li><strong>ST:</strong> Statement type (e.g., {@code com.datastax.oss.driver.api.core.cql.Statement})</li>
 * <li><strong>PS:</strong> PreparedStatement type (e.g., {@code com.datastax.oss.driver.api.core.cql.PreparedStatement})</li>
 * <li><strong>BT:</strong> BatchType enum (e.g., {@code com.datastax.oss.driver.api.core.cql.BatchType})</li>
 * </ul>
 * 
 * <h3>Parameter Binding Support</h3>
 * <p>The base class supports multiple parameter binding approaches:</p>
 * <ul>
 * <li><strong>Positional parameters:</strong> {@code SELECT * FROM users WHERE id = ?}</li>
 * <li><strong>Named parameters:</strong> {@code SELECT * FROM users WHERE id = :userId}</li>
 * <li><strong>Entity binding:</strong> Automatic extraction from POJO properties</li>
 * <li><strong>Map binding:</strong> Key-value pairs for named parameters</li>
 * <li><strong>Array/Collection binding:</strong> Multiple values for batch operations</li>
 * </ul>
 * 
 * <h3>Entity Operation Patterns</h3>
 * <p>The base class implements common entity operation patterns:</p>
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Basic CRUD operations
 * executor.insert(user);   // INSERT based on entity
 * Optional<User> found = executor.get(User.class, id);   // SELECT by primary key
 * executor.update(user);   // UPDATE based on entity
 * executor.delete(User.class, id);   // DELETE by primary key
 *
 * // Get entity (returns Optional)
 * User user = executor.get(User.class, id).orElse(null);
 * // Or with gett() for the nullable variant
 * // (returns null if not found, throws only if multiple rows match)
 * User user2 = executor.gett(User.class, id);
 * 
 * // Batch operations
 * List<User> users = Arrays.asList(user1, user2, user3);
 * executor.batchInsert(users);
 * executor.batchUpdate(users, Arrays.asList("name", "email"));
 * 
 * // Query operations
 * List<User> activeUsers = executor.list(User.class, 
 *     "SELECT * FROM users WHERE status = ?", "active");
 * 
 * Optional<User> user = executor.findFirst(User.class,
 *     "SELECT * FROM users WHERE email = ?", email);
 * }</pre>
 * 
 * <h3>Naming Policy Integration</h3>
 * <p>The class supports configurable naming policies for mapping Java property names
 * to Cassandra column names:</p>
 * <ul>
 * <li><strong>SNAKE_CASE:</strong> {@code firstName} → {@code first_name}</li>
 * <li><strong>SCREAMING_SNAKE_CASE:</strong> {@code firstName} → {@code FIRST_NAME}</li>
 * <li><strong>CAMEL_CASE:</strong> {@code firstName} → {@code firstName}</li>
 * </ul>
 * 
 * <h3>Thread Safety</h3>
 * <p>Implementations of this base class are expected to be thread-safe. The base class
 * provides thread-safe caching mechanisms and concurrent access patterns, but concrete
 * implementations must ensure thread safety of driver-specific operations.</p>
 * 
 * <h3>Extension Points</h3>
 * <p>Concrete implementations must provide the following abstract methods:</p>
 * <ul>
 * <li>{@code execute()}: Core statement execution</li>
 * <li>{@code prepare()}: Prepared statement creation</li>
 * <li>{@code bind()}: Parameter binding</li>
 * <li>{@code toList()}: Result set to list conversion</li>
 * <li>{@code extractData()}: Result set to Dataset conversion</li>
 * <li>And others as defined by the abstract contract</li>
 * </ul>
 * 
 * @param <RW> the row type for the specific Cassandra driver version
 * @param <RS> the result set type for the specific Cassandra driver version
 * @param <ST> the statement type for the specific Cassandra driver version
 * @param <PS> the prepared statement type for the specific Cassandra driver version
 * @param <BT> the batch type enum for the specific Cassandra driver version
 * 
 * @see CassandraExecutor
 * @see com.landawn.abacus.da.cassandra.v3.CassandraExecutor
 * @see CqlBuilder
 * @see CqlMapper
 * @see ParsedCql
 * @see com.landawn.abacus.query.Filters
 */
@SuppressWarnings("java:S1192")
public abstract class CassandraExecutorBase<RW, RS extends Iterable<RW>, ST, PS, BT> implements AutoCloseable {

    protected static final String NULL_STR = "NULL";

    protected static final ImmutableList<String> EXISTS_SELECT_PROP_NAMES = ImmutableList.of("1");
    protected static final ImmutableList<String> COUNT_SELECT_PROP_NAMES = ImmutableList.of(NSC.COUNT_ALL);

    protected static final int POOLABLE_LENGTH = 1024;

    protected static final Throwables.Function<Nullable<Boolean>, OptionalBoolean, RuntimeException> boolean_mapper = t -> t
            .mapToBoolean(ToBooleanFunction.UNBOX);

    protected static final Throwables.Function<Nullable<Character>, OptionalChar, RuntimeException> char_mapper = t -> t.mapToChar(ToCharFunction.UNBOX);

    protected static final Throwables.Function<Nullable<Byte>, OptionalByte, RuntimeException> byte_mapper = t -> t.mapToByte(ToByteFunction.UNBOX);

    protected static final Throwables.Function<Nullable<Short>, OptionalShort, RuntimeException> short_mapper = t -> t.mapToShort(ToShortFunction.UNBOX);

    protected static final Throwables.Function<Nullable<Integer>, OptionalInt, RuntimeException> int_mapper = t -> t.mapToInt(ToIntFunction.UNBOX);

    protected static final Throwables.Function<Nullable<Long>, OptionalLong, RuntimeException> long_mapper = t -> t.mapToLong(ToLongFunction.UNBOX);

    protected static final Throwables.Function<Nullable<Long>, Long, RuntimeException> long_secondMapper = t -> t.mapToLong(ToLongFunction.UNBOX).orElse(0);

    protected static final Throwables.Function<Nullable<Float>, OptionalFloat, RuntimeException> float_mapper = t -> t.mapToFloat(ToFloatFunction.UNBOX);

    protected static final Throwables.Function<Nullable<Double>, OptionalDouble, RuntimeException> double_mapper = t -> t.mapToDouble(ToDoubleFunction.UNBOX);

    protected static final Throwables.Function<Iterable<?>, Boolean, RuntimeException> exists_mapper = resultSet -> resultSet.iterator().hasNext();

    protected static final Map<Class<?>, Tuple2<ImmutableList<String>, ImmutableSet<String>>> entityKeyNamesMap = new ConcurrentHashMap<>();

    protected final CqlMapper cqlMapper;

    protected final NamingPolicy namingPolicy;

    protected CassandraExecutorBase(final CqlMapper cqlMapper, final NamingPolicy namingPolicy) {
        this.cqlMapper = cqlMapper;
        this.namingPolicy = namingPolicy == null ? NamingPolicy.SNAKE_CASE : namingPolicy;
    }

    /**
     * Registers primary key field names for the specified entity class.
     * 
     * <p>This method allows manual registration of key fields for entity classes that do not
     * use {@code @Id} annotations. The registered key names are used by CRUD operations to
     * identify primary key columns for WHERE clauses in SELECT, UPDATE, and DELETE operations.</p>
     * 
     * <p><b>Important:</b> This method is deprecated. New code should use {@code @Id} annotations
     * on entity fields or {@code javax.persistence.Id} annotations instead of manual registration.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Legacy approach (deprecated)
     * CassandraExecutorBase.registerKeys(User.class, Arrays.asList("userId", "tenantId"));
     * 
     * // Preferred approach
     * public class User {
     *     @Id
     *     private String userId;
     *     
     *     @Id  // Composite key
     *     private String tenantId;
     *     
     *     // other fields...
     * }
     * }</pre>
     * 
     * @param entityClass the entity class
     * @param keyNames collection of property names that comprise the primary key
     * @throws IllegalArgumentException if keyNames is null or empty
     * @see com.landawn.abacus.annotation.Id
     * @deprecated Define or annotate the key/id field with {@code @Id} instead.
     */
    @Deprecated
    public static void registerKeys(final Class<?> entityClass, final Collection<String> keyNames) {
        N.checkArgument(N.notEmpty(keyNames), "'keyNames' can't be null or empty");

        final Set<String> keyNameSet = N.newLinkedHashSet(keyNames.size());

        for (final String keyName : keyNames) {
            keyNameSet.add(Beans.getPropNameByMethod(Beans.getPropGetter(entityClass, keyName)));
        }

        entityKeyNamesMap.put(entityClass, Tuple.of(ImmutableList.copyOf(keyNameSet), ImmutableSet.wrap(keyNameSet)));
    }

    /**
     * Retrieves the primary key field names for the specified entity class.
     * 
     * <p>This method returns the list of property names that comprise the primary key for
     * the given entity class. It first checks for manually registered keys, then falls back
     * to introspecting the class for {@code @Id} annotations.</p>
     * 
     * <p>The returned key names are used internally by CRUD operations to construct
     * appropriate WHERE clauses for database operations.</p>
     * 
     * @param entityClass the entity class
     * @return an immutable list of property names that form the primary key
     * @throws IllegalArgumentException if entityClass is null
     */
    protected static ImmutableList<String> getKeyNames(final Class<?> entityClass) {
        Tuple2<ImmutableList<String>, ImmutableSet<String>> tp = entityKeyNamesMap.get(entityClass);

        if (tp == null) {
            @SuppressWarnings("deprecation")
            final List<String> idPropNames = QueryUtil.getIdPropNames(entityClass);
            tp = Tuple.of(ImmutableList.copyOf(idPropNames), ImmutableSet.copyOf(idPropNames));
            final Tuple2<ImmutableList<String>, ImmutableSet<String>> existing = entityKeyNamesMap.putIfAbsent(entityClass, tp);
            if (existing != null) {
                tp = existing;
            }
        }

        return tp._1;
    }

    /**
     * Retrieves the primary key field names as a Set for the specified entity class.
     * 
     * <p>This method returns the same key names as {@link #getKeyNames(Class)} but as a Set
     * for efficient lookup operations. This is particularly useful when checking if a
     * property name is part of the primary key.</p>
     * 
     * @param entityClass the entity class
     * @return an immutable set of property names that form the primary key
     * @throws IllegalArgumentException if entityClass is null
     * @see #getKeyNames(Class)
     */
    protected static Set<String> getKeyNameSet(final Class<?> entityClass) {
        Tuple2<ImmutableList<String>, ImmutableSet<String>> tp = entityKeyNamesMap.get(entityClass);

        if (tp == null) {
            @SuppressWarnings("deprecation")
            final List<String> idPropNames = QueryUtil.getIdPropNames(entityClass);
            tp = Tuple.of(ImmutableList.copyOf(idPropNames), ImmutableSet.copyOf(idPropNames));
            final Tuple2<ImmutableList<String>, ImmutableSet<String>> existing = entityKeyNamesMap.putIfAbsent(entityClass, tp);
            if (existing != null) {
                tp = existing;
            }
        }

        return tp._2;
    }

    /**
     * Converts primary key ID values into a WHERE condition for database operations.
     * 
     * <p>This method creates a {@link Condition} object from the provided ID values,
     * matching them against the primary key fields of the target entity class. For
     * single-key entities, it creates a simple equality condition. For composite keys,
     * it creates an AND condition with all key components.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Single key entity
     * Condition cond = idsToCondition(User.class, "user123");
     * // Result: WHERE user_id = ?
     * 
     * // Composite key entity
     * Condition cond = idsToCondition(UserSession.class, "user123", "session456");
     * // Result: WHERE user_id = ? AND session_id = ?
     * }</pre>
     * 
     * @param targetClass the entity class
     * @param ids the ID values in the same order as the key fields
     * @return a Condition representing the primary key equality check
     * @throws IllegalArgumentException if ids array is empty or doesn't match key count
     */
    protected static Condition idsToCondition(final Class<?> targetClass, final Object... ids) {
        N.checkArgNotEmpty(ids, "ids");

        final ImmutableList<String> keyNames = getKeyNames(targetClass);

        if (keyNames.size() == 1 && ids.length == 1) {
            return Filters.eq(keyNames.get(0), ids[0]);
        } else if (ids.length == keyNames.size()) {
            final Iterator<String> iter = keyNames.iterator();
            final List<Condition> conds = new ArrayList<>();

            for (final Object id : ids) {
                conds.add(Filters.eq(iter.next(), id));
            }

            return Filters.and(conds);
        } else {
            throw new IllegalArgumentException("The number: " + ids.length + " of input ids doesn't match the (registered) key names: "
                    + (N.isEmpty(keyNames) ? "[id]" : N.toString(keyNames)) + " in class: " + ClassUtil.getCanonicalClassName(targetClass));
        }
    }

    /**
     * Extracts primary key values from an entity and converts them into a WHERE condition.
     * 
     * <p>This method inspects the provided entity object, extracts the values from its
     * primary key properties, and creates an appropriate {@link Condition} for database
     * operations. This is commonly used in UPDATE and DELETE operations where the entity
     * object contains the key values needed to identify the target row.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = new User();
     * user.setUserId("user123");
     * user.setTenantId("tenant456");
     * 
     * Condition cond = entityToCondition(user);
     * // Result: WHERE user_id = ? AND tenant_id = ?
     * // with values "user123" and "tenant456"
     * }</pre>
     * 
     * @param entity the entity object containing primary key values
     * @return a Condition representing the primary key equality check based on entity values
     * @throws IllegalArgumentException if entity is null or has null key values
     */
    protected static Condition entityToCondition(final Object entity) {
        final Class<?> targetClass = entity.getClass();
        final ImmutableList<String> keyNames = getKeyNames(targetClass);

        if (keyNames.isEmpty()) {
            throw new IllegalArgumentException("No key names defined for entity class: " + targetClass.getSimpleName());
        }

        if (keyNames.size() == 1) {
            final String keyName = keyNames.get(0);
            final Object propVal = Beans.getPropValue(entity, keyName);

            if (propVal == null || (propVal instanceof CharSequence && Strings.isEmpty((CharSequence) propVal))) {
                throw new IllegalArgumentException("No property value specified in entity for key name: " + keyName);
            }

            return Filters.eq(keyName, propVal);
        } else {
            final List<Condition> conds = new ArrayList<>(keyNames.size());
            Object propVal = null;
            String missingKeyName = null;

            for (final String keyName : keyNames) {
                propVal = Beans.getPropValue(entity, keyName);

                if (propVal == null || (propVal instanceof CharSequence) && Strings.isEmpty(((CharSequence) propVal))) {
                    missingKeyName = keyName;
                    continue;
                }

                conds.add(Filters.eq(keyName, propVal));
            }

            if (missingKeyName != null || conds.size() != keyNames.size()) {
                throw new IllegalArgumentException("No property value specified in entity for key name: " + missingKeyName);
            }

            return Filters.and(conds);
        }
    }

    /**
     * Creates a WHERE condition to match any entity in the provided collection.
     * 
     * <p>This method generates a condition that can match any of the entities in the collection
     * by their primary key values. For single-key entities, it creates an IN condition. For
     * composite-key entities, it creates an OR condition with AND clauses for each entity's
     * key components.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Single key entities
     * List<User> users = Arrays.asList(user1, user2, user3);
     * Condition cond = entityToCondition(User.class, users);
     * // Result: WHERE user_id IN (?, ?, ?)
     * 
     * // Composite key entities
     * List<UserSession> sessions = Arrays.asList(session1, session2);
     * Condition cond = entityToCondition(UserSession.class, sessions);
     * // Result: WHERE (user_id = ? AND session_id = ?) OR (user_id = ? AND session_id = ?)
     * }</pre>
     * 
     * @param entityClass the entity class
     * @param entities the collection of entities whose keys should be matched
     * @return a Condition that matches any entity in the collection by primary key
     * @throws IllegalArgumentException if entityClass or entities is null or empty
     */
    protected static Condition entityToCondition(final Class<?> entityClass, final Collection<?> entities) {
        N.checkArgNotNull(entityClass, "entityClass");
        N.checkArgument(N.notEmpty(entities), "'entities' can't be null or empty.");

        final Set<String> keyNameSet = getKeyNameSet(entityClass);

        if (N.isEmpty(keyNameSet)) {
            throw new IllegalArgumentException("No key names defined for entity class: " + entityClass.getSimpleName());
        }

        Condition cond = null;

        if (keyNameSet.size() == 1) {
            final String keyName = keyNameSet.iterator().next();
            final List<Object> keys = Stream.of(entities)
                    .peek(it -> N.checkArgNotNull(it, "Entity in collection can't be null."))
                    .map(it -> Beans.getPropValue(it, keyName))
                    .toList();
            cond = Filters.in(keyName, keys);
        } else {
            cond = Filters.or(entities.stream()
                    .peek(it -> N.checkArgNotNull(it, "Entity in collection can't be null."))
                    .map(it -> Filters.and(keyNameSet.stream().map(keyName -> Filters.eq(keyName, Beans.getPropValue(it, keyName))).toList()))
                    .toList());
        }

        return cond;
    }

    /**
     * Retrieves an entity by its primary key values.
     *
     * <p>This method performs a SELECT query based on the provided primary key values
     * and returns the matching entity wrapped in an Optional. Returns Optional.empty()
     * if no entity is found.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Optional<User> user = executor.get(User.class, "user123");
     * user.ifPresent(u -> System.out.println("Found: " + u.getName()));
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param ids the primary key values
     * @return an Optional containing the entity if found, otherwise empty
     * @throws IllegalArgumentException if ids is null or empty
     * @throws DuplicateResultException if more than one entity is found
     */
    public final <T> Optional<T> get(final Class<T> targetClass, final Object... ids) throws DuplicateResultException {
        return get(targetClass, null, ids);
    }

    /**
     * Retrieves specific properties of an entity by its primary key values.
     *
     * <p>This method performs a SELECT query for only the specified properties
     * based on the provided primary key values. This is useful for optimizing
     * queries when only certain fields are needed.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Optional<User> user = executor.get(User.class,
     *     Arrays.asList("name", "email"), "user123");
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param ids the primary key values
     * @return an Optional containing the entity if found, otherwise empty
     * @throws IllegalArgumentException if ids is null or empty
     * @throws DuplicateResultException if more than one entity is found
     */
    public final <T> Optional<T> get(final Class<T> targetClass, final Collection<String> selectPropNames, final Object... ids)
            throws DuplicateResultException {
        return get(targetClass, selectPropNames, idsToCondition(targetClass, ids));
    }

    /**
     * Retrieves an entity based on a custom WHERE condition.
     * 
     * <p>This method performs a SELECT query using the provided condition
     * and returns the matching entity wrapped in an Optional.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Condition cond = Filters.eq("email", "user@example.com");
     * Optional<User> user = executor.get(User.class, cond);
     * }</pre>
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param whereClause the WHERE condition
     * @return an Optional containing the entity if found, otherwise empty
     * @throws DuplicateResultException if more than one entity is found
     */
    public <T> Optional<T> get(final Class<T> targetClass, final Condition whereClause) throws DuplicateResultException {
        return get(targetClass, null, whereClause);
    }

    /**
     * Retrieves specific properties of an entity based on a custom WHERE condition.
     * 
     * <p>This method performs a SELECT query for only the specified properties
     * using the provided condition. This is useful for optimizing queries when
     * only certain fields are needed.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Condition cond = Filters.and(Filters.eq("status", "active"), Filters.gte("age", 18));
     * Optional<User> user = executor.get(User.class, 
     *     Arrays.asList("name", "email"), cond);
     * }</pre>
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param whereClause the WHERE condition
     * @return an Optional containing the entity if found, otherwise empty
     * @throws DuplicateResultException if more than one entity is found
     */
    public <T> Optional<T> get(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause)
            throws DuplicateResultException {
        return Optional.ofNullable(gett(targetClass, selectPropNames, whereClause));
    }

    /**
     * Retrieves an entity by its primary key values through the nullable {@code gett} variant.
     *
     * <p>This method is the nullable counterpart to {@link #get(Class, Object...)}.
     * It returns the entity directly or {@code null} when no row matches, while still
     * throwing {@link DuplicateResultException} if multiple rows are returned.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = executor.gett(User.class, "user123");
     * if (user != null) {
     *     System.out.println("Found: " + user.getName());
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param ids the primary key values
     * @return the entity if found, otherwise null
     * @throws IllegalArgumentException if ids is null or empty
     * @throws DuplicateResultException if more than one entity is found
     */
    public final <T> T gett(final Class<T> targetClass, final Object... ids) throws DuplicateResultException {
        return gett(targetClass, null, ids);
    }

    /**
     * Retrieves specific properties of an entity by its primary key values through the nullable {@code gett} variant.
     *
     * <p>This method performs a SELECT query for only the specified properties
     * based on the provided primary key values. It mirrors {@link #get(Class, Collection, Object...)}
     * but returns the mapped entity directly or {@code null} instead of {@link Optional}.</p>
     *
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param ids the primary key values
     * @return the entity if found, otherwise null
     * @throws IllegalArgumentException if ids is null or empty
     * @throws DuplicateResultException if more than one entity is found
     */
    public final <T> T gett(final Class<T> targetClass, final Collection<String> selectPropNames, final Object... ids) throws DuplicateResultException {
        return gett(targetClass, selectPropNames, idsToCondition(targetClass, ids));
    }

    /**
     * Retrieves an entity based on a custom WHERE condition through the nullable {@code gett} variant.
     * 
     * <p>This method performs a SELECT query using the provided condition
     * and returns the entity directly or {@code null} instead of {@link Optional}.</p>
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param whereClause the WHERE condition
     * @return the entity if found, otherwise null
     * @throws DuplicateResultException if more than one entity is found
     */
    public <T> T gett(final Class<T> targetClass, final Condition whereClause) throws DuplicateResultException {
        return gett(targetClass, null, whereClause);
    }

    /**
     * Retrieves specific properties of an entity based on a custom WHERE condition through the nullable {@code gett} variant.
     * 
     * <p>This abstract method must be implemented by concrete subclasses to perform
     * the actual database query and entity mapping.</p>
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param whereClause the WHERE condition
     * @return the entity if found, otherwise null
     * @throws DuplicateResultException if more than one entity is found
     */
    public abstract <T> T gett(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause)
            throws DuplicateResultException;

    /**
     * Inserts an entity into the database.
     *
     * <p>This method generates an INSERT statement based on the entity's properties
     * and executes it. All non-null properties of the entity will be included in
     * the INSERT statement.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = new User();
     * user.setUserId("user123");
     * user.setName("John Doe");
     * user.setEmail("john@example.com");
     *
     * ResultSet result = executor.insert(user);
     * }</pre>
     *
     * @param entity the entity to insert
     * @return the result set from the INSERT operation
     * @throws IllegalArgumentException if entity is null
     */
    public RS insert(final Object entity) {
        final SP cp = prepareInsert(entity);

        return execute(cp);
    }

    /**
     * Inserts a new record with the specified properties.
     *
     * <p>This method generates an INSERT statement using the provided property map
     * for the specified entity class. This is useful when you don't have a full
     * entity object but just the properties to insert.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, Object> props = new HashMap<>();
     * props.put("userId", "user123");
     * props.put("name", "John Doe");
     * props.put("email", "john@example.com");
     *
     * ResultSet result = executor.insert(User.class, props);
     * }</pre>
     *
     * @param targetClass the entity class
     * @param props the properties to insert
     * @return the result set from the INSERT operation
     * @throws IllegalArgumentException if targetClass is null or props is null or empty
     */
    public RS insert(final Class<?> targetClass, final Map<String, Object> props) {
        final SP cp = prepareInsert(targetClass, props);

        return execute(cp);
    }

    /**
     * Performs a batch insert of multiple entities.
     *
     * <p>This method creates a batch INSERT statement for all provided entities
     * and executes them together for better performance. All entities must be
     * of the same type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<User> users = Arrays.asList(user1, user2, user3);
     * ResultSet result = executor.batchInsert(users, BatchType.LOGGED);
     * }</pre>
     *
     * @param entities the collection of entities to insert
     * @param type the batch type
     * @return the result set from the batch INSERT operation
     * @throws IllegalArgumentException if entities is null or empty
     */
    public RS batchInsert(final Collection<?> entities, final BT type) {
        final ST stmt = prepareBatchInsertStatement(entities, type);

        return execute(stmt);
    }

    /**
     * Performs a batch insert of multiple records using property maps.
     *
     * <p>This method creates a batch INSERT statement for all provided property maps
     * and executes them together for better performance.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Map<String, Object>> propsList = new ArrayList<>();
     * propsList.add(Map.of("userId", "user1", "name", "Alice"));
     * propsList.add(Map.of("userId", "user2", "name", "Bob"));
     *
     * ResultSet result = executor.batchInsert(User.class, propsList, BatchType.LOGGED);
     * }</pre>
     *
     * @param targetClass the entity class
     * @param propsList the collection of property maps to insert
     * @param type the batch type
     * @return the result set from the batch INSERT operation
     * @throws IllegalArgumentException if targetClass is null or propsList is null or empty
     */
    public RS batchInsert(final Class<?> targetClass, final Collection<? extends Map<String, Object>> propsList, final BT type) {
        final ST stmt = prepareBatchInsertStatement(targetClass, propsList, type);

        return execute(stmt);
    }

    /**
     * Updates an entity in the database.
     *
     * <p>This method generates an UPDATE statement based on the entity's non-key properties
     * and executes it. The primary key fields are used in the WHERE clause to identify
     * the record to update. By default, all non-key properties are updated.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = executor.gett(User.class, "user123");
     * user.setEmail("newemail@example.com");
     * user.setLastModified(new Date());
     *
     * ResultSet result = executor.update(user);
     * }</pre>
     *
     * @param entity the entity to update
     * @return the result set from the UPDATE operation
     * @throws IllegalArgumentException if entity is null
     */
    @SuppressWarnings("deprecation")
    public RS update(final Object entity) {
        final Class<?> entityClass = entity.getClass();
        final Set<String> keyNameSet = getKeyNameSet(entityClass);
        final Collection<String> updatePropNames = QueryUtil.getUpdatePropNames(entityClass, keyNameSet);

        return update(entity, updatePropNames);
    }

    /**
     * Updates specific properties of an entity in the database.
     * 
     * <p>This method generates an UPDATE statement for only the specified properties
     * and executes it. The primary key fields are used in the WHERE clause to identify
     * the record to update.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = new User();
     * user.setUserId("user123");
     * user.setEmail("newemail@example.com");
     * user.setLastModified(new Date());
     * 
     * ResultSet result = executor.update(user, 
     *     Arrays.asList("email", "lastModified"));
     * }</pre>
     * 
     * @param entity the entity containing the values to update
     * @param propNamesToUpdate the property names to update
     * @return the result set from the UPDATE operation
     * @throws IllegalArgumentException if propNamesToUpdate is null or empty
     */
    public RS update(final Object entity, final Collection<String> propNamesToUpdate) {
        N.checkArgument(N.notEmpty(propNamesToUpdate), "'propNamesToUpdate' can't be null or empty");

        final SP cp = prepareUpdate(entity, propNamesToUpdate);

        return execute(cp);
    }

    /**
     * Updates records based on a custom WHERE condition.
     *
     * <p>This method generates an UPDATE statement with the specified properties
     * and WHERE condition, then executes it. This is useful for updating multiple
     * records that match certain criteria.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, Object> props = new HashMap<>();
     * props.put("status", "inactive");
     * props.put("lastModified", new Date());
     *
     * Condition where = Filters.lt("lastLogin", thirtyDaysAgo);
     * ResultSet result = executor.update(User.class, props, where);
     * }</pre>
     *
     * @param targetClass the entity class
     * @param props the properties to update
     * @param whereClause the WHERE condition
     * @return the result set from the UPDATE operation
     * @throws IllegalArgumentException if targetClass is null or props is null or empty
     */
    public RS update(final Class<?> targetClass, final Map<String, Object> props, final Condition whereClause) {
        N.checkArgument(N.notEmpty(props), "'props' can't be null or empty.");

        final SP cp = prepareUpdate(targetClass, props, whereClause);

        return execute(cp);
    }

    /**
     * Executes a custom UPDATE query with parameters.
     * 
     * <p>This method executes a user-provided UPDATE query with the specified parameters.
     * Parameters are bound positionally to the query's placeholders.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String query = "UPDATE users SET status = ? WHERE last_login < ?";
     * ResultSet result = executor.update(query, "inactive", thirtyDaysAgo);
     * }</pre>
     * 
     * @param query the UPDATE query to execute
     * @param parameters the query parameters
     * @return the result set from the UPDATE operation
     */
    public RS update(final String query, final Object... parameters) {
        return execute(query, parameters);
    }

    /**
     * Performs a batch update of multiple entities.
     * 
     * <p>This method creates a batch UPDATE statement for all provided entities
     * and executes them together for better performance. By default, all non-key
     * properties are updated.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<User> users = Arrays.asList(user1, user2, user3);
     * ResultSet result = executor.batchUpdate(users, BatchType.LOGGED);
     * }</pre>
     * 
     * @param entities the collection of entities to update
     * @param type the batch type
     * @return the result set from the batch UPDATE operation
     * @throws IllegalArgumentException if entities is null or empty
     */
    @SuppressWarnings("deprecation")
    public RS batchUpdate(final Collection<?> entities, final BT type) {
        N.checkArgument(N.notEmpty(entities), "'entities' can't be null or empty.");

        final Object firstEntity = N.firstOrNullIfEmpty(entities);
        N.checkArgNotNull(firstEntity, "The first entity in the collection can't be null.");
        final Class<?> entityClass = firstEntity.getClass();
        final Set<String> keyNameSet = getKeyNameSet(entityClass);
        final Collection<String> updatePropNames = QueryUtil.getUpdatePropNames(entityClass, keyNameSet);

        return batchUpdate(entities, updatePropNames, type);
    }

    /**
     * Performs a batch update of specific properties for multiple entities.
     * 
     * <p>This method creates a batch UPDATE statement for all provided entities,
     * updating only the specified properties, and executes them together for better performance.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<User> users = Arrays.asList(user1, user2, user3);
     * ResultSet result = executor.batchUpdate(users, 
     *     Arrays.asList("status", "lastModified"), BatchType.LOGGED);
     * }</pre>
     * 
     * @param entities the collection of entities to update
     * @param propNamesToUpdate the property names to update
     * @param type the batch type
     * @return the result set from the batch UPDATE operation
     * @throws IllegalArgumentException if entities or propNamesToUpdate is null or empty
     */
    public RS batchUpdate(final Collection<?> entities, final Collection<String> propNamesToUpdate, final BT type) {
        N.checkArgument(N.notEmpty(entities), "'entities' can't be null or empty.");
        N.checkArgument(N.notEmpty(propNamesToUpdate), "'propNamesToUpdate' can't be null or empty");

        final ST stmt = prepareBatchUpdateStatement(entities, propNamesToUpdate, type);

        return execute(stmt);
    }

    /**
     * Performs a batch update using property maps.
     *
     * <p>This method creates a batch UPDATE statement for all provided property maps
     * and executes them together for better performance. Each map must contain the
     * key fields to identify the record and the fields to update.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Map<String, Object>> propsList = new ArrayList<>();
     * propsList.add(Map.of("userId", "user1", "status", "active"));
     * propsList.add(Map.of("userId", "user2", "status", "inactive"));
     *
     * ResultSet result = executor.batchUpdate(User.class, propsList, BatchType.LOGGED);
     * }</pre>
     *
     * @param targetClass the entity class
     * @param propsList the collection of property maps to update
     * @param type the batch type
     * @return the result set from the batch UPDATE operation
     * @throws IllegalArgumentException if targetClass is null or propsList is null or empty
     */
    public RS batchUpdate(final Class<?> targetClass, final Collection<? extends Map<String, Object>> propsList, final BT type) {
        final ST stmt = prepareBatchUpdateStatement(targetClass, propsList, type);

        return execute(stmt);
    }

    /**
     * Performs a batch update with a custom query and multiple parameter sets.
     *
     * <p>This method executes the same UPDATE query multiple times with different
     * parameter sets in a single batch for better performance.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String query = "UPDATE users SET status = ? WHERE user_id = ?";
     * List<Object[]> params = Arrays.asList(
     *     new Object[] {"active", "user1"},
     *     new Object[] {"inactive", "user2"}
     * );
     *
     * ResultSet result = executor.batchUpdate(query, params, BatchType.LOGGED);
     * }</pre>
     *
     * @param query the UPDATE query to execute
     * @param parametersList the collection of parameter arrays
     * @param type the batch type
     * @return the result set from the batch UPDATE operation
     * @throws IllegalArgumentException if query is null or parametersList is null or empty
     */
    public RS batchUpdate(final String query, final Collection<?> parametersList, final BT type) {
        final ST stmt = prepareBatchUpdateStatement(query, parametersList, type);

        return execute(stmt);
    }

    /**
     * Deletes an entity from the database.
     *
     * <p>This method generates a DELETE statement based on the entity's primary key
     * values and executes it. The entire row will be deleted.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = new User();
     * user.setUserId("user123");
     *
     * ResultSet result = executor.delete(user);
     * }</pre>
     *
     * @param entity the entity to delete (must have primary key values set)
     * @return the result set from the DELETE operation
     * @throws IllegalArgumentException if entity is null
     */
    public RS delete(final Object entity) {
        return delete(entity, null);
    }

    /**
     * Deletes specific properties of an entity from the database.
     *
     * <p>In Cassandra, this method can be used to delete specific column values
     * (setting them to null) rather than deleting the entire row. If propNamesToDelete
     * is null or empty, the entire row is deleted.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = new User();
     * user.setUserId("user123");
     *
     * // Delete only specific columns
     * ResultSet result = executor.delete(user, Arrays.asList("email", "phone"));
     * }</pre>
     *
     * @param entity the entity identifying the row to delete from
     * @param propNamesToDelete the property names to delete (null for entire row)
     * @return the result set from the DELETE operation
     * @throws IllegalArgumentException if entity is null or propNamesToDelete is empty (but not null)
     */
    public RS delete(final Object entity, final Collection<String> propNamesToDelete) {
        N.checkArgument(propNamesToDelete == null || N.notEmpty(propNamesToDelete), "'propNamesToDelete' can't be null or empty");

        return delete(entity.getClass(), propNamesToDelete, entityToCondition(entity));
    }

    /**
     * Deletes a record by its primary key values.
     *
     * <p>This method generates a DELETE statement for the specified primary key
     * values and executes it. The entire row will be deleted.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ResultSet result = executor.delete(User.class, "user123");
     * }</pre>
     *
     * @param targetClass the entity class
     * @param ids the primary key values
     * @return the result set from the DELETE operation
     * @throws IllegalArgumentException if targetClass is null or ids is null or empty
     */
    public final RS delete(final Class<?> targetClass, final Object... ids) {
        return delete(targetClass, null, ids);
    }

    /**
     * Deletes specific properties or entire record by primary key values.
     *
     * <p>If propNamesToDelete is specified, only those column values are deleted (set to null).
     * If propNamesToDelete is null or empty, the entire row is deleted.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Delete specific columns
     * ResultSet result = executor.delete(User.class,
     *     Arrays.asList("email", "phone"), "user123");
     *
     * // Delete entire row
     * ResultSet result = executor.delete(User.class, null, "user123");
     * }</pre>
     *
     * @param targetClass the entity class
     * @param propNamesToDelete the property names to delete (null for entire row)
     * @param ids the primary key values
     * @return the result set from the DELETE operation
     * @throws IllegalArgumentException if targetClass is null, ids is null or empty, or propNamesToDelete is empty (but not null)
     */
    public final RS delete(final Class<?> targetClass, final Collection<String> propNamesToDelete, final Object... ids) {
        N.checkArgument(propNamesToDelete == null || N.notEmpty(propNamesToDelete), "'propNamesToDelete' can't be null or empty");

        return delete(targetClass, propNamesToDelete, idsToCondition(targetClass, ids));
    }

    /**
     * Deletes records based on a custom WHERE condition.
     *
     * <p>This method generates a DELETE statement with the specified WHERE condition
     * and executes it. All matching rows will be deleted.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Condition where = Filters.eq("status", "deleted");
     * ResultSet result = executor.delete(User.class, where);
     * }</pre>
     *
     * @param targetClass the entity class
     * @param whereClause the WHERE condition
     * @return the result set from the DELETE operation
     * @throws IllegalArgumentException if targetClass is null
     */
    public RS delete(final Class<?> targetClass, final Condition whereClause) {
        return delete(targetClass, null, whereClause);
    }

    /**
     * Deletes specific properties or entire records based on a WHERE condition.
     *
     * <p>If propNamesToDelete is specified, only those column values are deleted (set to null)
     * in the matching rows. If propNamesToDelete is null or empty, the entire matching rows are deleted.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Condition where = Filters.and(Filters.eq("tenant", "tenant1"), Filters.eq("status", "inactive"));
     *
     * // Delete specific columns in matching rows
     * ResultSet result = executor.delete(User.class,
     *     Arrays.asList("lastLogin", "sessionToken"), where);
     * }</pre>
     *
     * @param targetClass the entity class
     * @param propNamesToDelete the property names to delete (null for entire rows)
     * @param whereClause the WHERE condition
     * @return the result set from the DELETE operation
     * @throws IllegalArgumentException if targetClass is null or propNamesToDelete is empty (but not null)
     */
    public RS delete(final Class<?> targetClass, final Collection<String> propNamesToDelete, final Condition whereClause) {
        N.checkArgument(propNamesToDelete == null || N.notEmpty(propNamesToDelete), "'propNamesToDelete' can't be null or empty");

        final SP cp = prepareDelete(targetClass, propNamesToDelete, whereClause);

        return execute(cp);
    }

    /**
     * Performs a batch delete of multiple entities.
     * 
     * <p>This method deletes all provided entities in a single batch operation
     * for better performance. Each entity must have its primary key values set.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<User> usersToDelete = Arrays.asList(user1, user2, user3);
     * ResultSet result = executor.batchDelete(usersToDelete);
     * }</pre>
     * 
     * @param entities the collection of entities to delete
     * @return the result set from the batch DELETE operation
     * @throws IllegalArgumentException if entities is null or empty
     */
    public RS batchDelete(final Collection<?> entities) {
        N.checkArgument(N.notEmpty(entities), "'entities' can't be null or empty.");

        final Object firstEntity = N.firstOrNullIfEmpty(entities);
        N.checkArgNotNull(firstEntity, "The first entity in the collection can't be null.");
        final Class<?> entityClass = firstEntity.getClass();
        final Condition cond = entityToCondition(entityClass, entities);

        return delete(entityClass, cond);
    }

    /**
     * Performs a batch delete of specific properties from multiple entities.
     * 
     * <p>This method deletes specific column values from all provided entities
     * in a single batch operation. If propNamesToDelete is null, entire rows are deleted.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<User> users = Arrays.asList(user1, user2, user3);
     * ResultSet result = executor.batchDelete(users, Arrays.asList("email", "phone"));
     * }</pre>
     * 
     * @param entities the collection of entities to delete from
     * @param propNamesToDelete the property names to delete (null for entire rows)
     * @return the result set from the batch DELETE operation
     * @throws IllegalArgumentException if entities is empty or propNamesToDelete is empty (but not null)
     */
    public RS batchDelete(final Collection<?> entities, final Collection<String> propNamesToDelete) {
        N.checkArgument(N.notEmpty(entities), "'entities' can't be null or empty.");
        N.checkArgument(propNamesToDelete == null || N.notEmpty(propNamesToDelete), "'propNamesToDelete' can't be null or empty");

        final Object firstEntity = N.firstOrNullIfEmpty(entities);
        N.checkArgNotNull(firstEntity, "The first entity in the collection can't be null.");
        final Class<?> entityClass = firstEntity.getClass();
        final Condition cond = entityToCondition(entityClass, entities);

        return delete(entityClass, propNamesToDelete, cond);
    }

    /**
     * Checks if a record exists by its primary key values.
     *
     * <p>This method performs an efficient existence check using the provided
     * primary key values. It's optimized to return only a minimal result set.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * boolean exists = executor.exists(User.class, "user123");
     * if (exists) {
     *     System.out.println("User exists");
     * }
     * }</pre>
     *
     * @param targetClass the entity class
     * @param ids the primary key values
     * @return {@code true} if a record exists with the given primary key, {@code false} otherwise
     * @throws IllegalArgumentException if targetClass is null or ids is null or empty
     */
    public final boolean exists(final Class<?> targetClass, final Object... ids) {
        return exists(targetClass, idsToCondition(targetClass, ids));
    }

    /**
     * Checks if any records exist matching the specified condition.
     *
     * <p>This method performs an efficient existence check using the provided
     * WHERE condition. It's optimized to return only a minimal result set.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Condition where = Filters.eq("email", "user@example.com");
     * boolean exists = executor.exists(User.class, where);
     * }</pre>
     *
     * @param targetClass the entity class
     * @param whereClause the WHERE condition
     * @return {@code true} if at least one record matches the condition, {@code false} otherwise
     * @throws IllegalArgumentException if targetClass is null
     */
    public boolean exists(final Class<?> targetClass, final Condition whereClause) {
        final ImmutableList<String> keyNames = getKeyNames(targetClass);
        final SP cp = prepareQuery(targetClass, keyNames, whereClause, 1);
        final RS resultSet = execute(cp);

        return resultSet.iterator().hasNext();
    }

    /**
     * Counts the number of records matching the specified condition.
     *
     * <p>This method executes a COUNT query with the provided WHERE condition
     * and returns the count of matching records.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Condition where = Filters.eq("status", "active");
     * long activeUsers = executor.count(User.class, where);
     * System.out.println("Active users: " + activeUsers);
     * }</pre>
     *
     * @param targetClass the entity class
     * @param whereClause the WHERE condition
     * @return the count of records matching the condition
     * @throws IllegalArgumentException if targetClass is null
     */
    public long count(final Class<?> targetClass, final Condition whereClause) {
        final SP cp = prepareQuery(targetClass, N.asList(CqlBuilder.COUNT_ALL), whereClause, 1);

        return queryForSingleResult(long.class, cp.query(), cp.parameters().toArray()).orElse(0L);
    }

    /**
     * Finds the first entity matching the specified condition.
     *
     * <p>This method executes a query with LIMIT 1 and returns the first matching
     * entity wrapped in an Optional. Returns Optional.empty() if no match is found.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Condition where = Filters.eq("email", "user@example.com");
     * Optional<User> user = executor.findFirst(User.class, where);
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param whereClause the WHERE condition
     * @return an Optional containing the first matching entity, or empty if none found
     * @throws IllegalArgumentException if targetClass is null
     */
    public <T> Optional<T> findFirst(final Class<T> targetClass, final Condition whereClause) {
        return findFirst(targetClass, null, whereClause);
    }

    /**
     * Finds the first entity with specific properties matching the condition.
     *
     * <p>This method executes a query selecting only specified properties with LIMIT 1
     * and returns the first matching entity wrapped in an Optional.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Condition where = Filters.eq("status", "active");
     * Optional<User> user = executor.findFirst(User.class,
     *     Arrays.asList("userId", "name"), where);
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param whereClause the WHERE condition
     * @return an Optional containing the first matching entity, or empty if none found
     * @throws IllegalArgumentException if targetClass is null
     */
    public <T> Optional<T> findFirst(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP cp = prepareQuery(targetClass, selectPropNames, whereClause, 1);

        return findFirst(targetClass, cp.query(), cp.parameters().toArray());
    }

    /**
     * Lists all entities matching the specified condition.
     *
     * <p>This method executes a query and returns all matching entities as a List.
     * Be careful with large result sets as all results are loaded into memory.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Condition where = Filters.eq("status", "active");
     * List<User> activeUsers = executor.list(User.class, where);
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param whereClause the WHERE condition
     * @return a List of all matching entities
     * @throws IllegalArgumentException if targetClass is null
     */
    public <T> List<T> list(final Class<T> targetClass, final Condition whereClause) {
        return list(targetClass, null, whereClause);
    }

    /**
     * Lists entities with specific properties matching the condition.
     *
     * <p>This method executes a query selecting only specified properties
     * and returns all matching entities as a List.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Condition where = Filters.gte("age", 18);
     * List<User> adults = executor.list(User.class,
     *     Arrays.asList("userId", "name", "age"), where);
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param whereClause the WHERE condition
     * @return a List of all matching entities
     * @throws IllegalArgumentException if targetClass is null
     */
    public <T> List<T> list(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP cp = prepareQuery(targetClass, selectPropNames, whereClause);

        return list(targetClass, cp.query(), cp.parameters().toArray());
    }

    /**
     * Queries for a Dataset of entities matching the specified condition.
     *
     * <p>This method executes a query and returns results as a Dataset, which provides
     * additional data manipulation capabilities compared to a simple List.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Condition where = Filters.between("age", 25, 35);
     * Dataset dataset = executor.query(User.class, where);
     * dataset.groupBy("department").aggregate("salary", Collectors.averagingDouble());
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param whereClause the WHERE condition
     * @return a Dataset containing all matching entities
     * @throws IllegalArgumentException if targetClass is null
     */
    public <T> Dataset query(final Class<T> targetClass, final Condition whereClause) {
        return query(targetClass, null, whereClause);
    }

    /**
     * Queries for a Dataset of entities with specific properties matching the specified condition.
     *
     * <p>This method allows selecting only specific properties from the entity, which can improve
     * performance by reducing data transfer.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Dataset dataset = executor.query(User.class, Arrays.asList("id", "name", "email"),
     *                                  Filters.eq("status", "active"));
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param whereClause the WHERE condition
     * @return a Dataset containing the selected properties of matching entities
     * @throws IllegalArgumentException if targetClass is null
     */
    public <T> Dataset query(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP cp = prepareQuery(targetClass, selectPropNames, whereClause);

        return query(targetClass, cp.query(), cp.parameters().toArray());
    }

    /**
     * Queries for a single boolean value from the specified property.
     *
     * <p>This method is marked as Beta and may change in future versions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalBoolean isActive = executor.queryForBoolean(User.class, "is_active",
     *                                                     Filters.eq("id", userId));
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param propName the property name to retrieve
     * @param whereClause the WHERE condition
     * @return an OptionalBoolean containing the result, or empty if no result found
     * @throws IllegalArgumentException if targetClass is null or propName is null or empty
     */
    @Beta
    public <T> OptionalBoolean queryForBoolean(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleResult(targetClass, Boolean.class, propName, whereClause).mapToBoolean(ToBooleanFunction.UNBOX);
    }

    /**
     * Queries for a single character value from the specified property.
     *
     * <p>This method is marked as Beta and may change in future versions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalChar grade = executor.queryForChar(Student.class, "grade",
     *                                            Filters.eq("student_id", studentId));
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param propName the property name to retrieve
     * @param whereClause the WHERE condition
     * @return an OptionalChar containing the result, or empty if no result found
     * @throws IllegalArgumentException if targetClass is null or propName is null or empty
     */
    @Beta
    public <T> OptionalChar queryForChar(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleResult(targetClass, Character.class, propName, whereClause).mapToChar(ToCharFunction.UNBOX);
    }

    /**
     * Queries for a single byte value from the specified property.
     *
     * <p>This method is marked as Beta and may change in future versions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalByte statusCode = executor.queryForByte(Request.class, "status_code",
     *                                                 Filters.eq("request_id", requestId));
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param propName the property name to retrieve
     * @param whereClause the WHERE condition
     * @return an OptionalByte containing the result, or empty if no result found
     * @throws IllegalArgumentException if targetClass is null or propName is null or empty
     */
    @Beta
    public <T> OptionalByte queryForByte(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleResult(targetClass, Byte.class, propName, whereClause).mapToByte(ToByteFunction.UNBOX);
    }

    /**
     * Queries for a single short value from the specified property.
     *
     * <p>This method is marked as Beta and may change in future versions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalShort port = executor.queryForShort(Server.class, "port",
     *                                             Filters.eq("server_name", "web-01"));
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param propName the property name to retrieve
     * @param whereClause the WHERE condition
     * @return an OptionalShort containing the result, or empty if no result found
     * @throws IllegalArgumentException if targetClass is null or propName is null or empty
     */
    @Beta
    public <T> OptionalShort queryForShort(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleResult(targetClass, Short.class, propName, whereClause).mapToShort(ToShortFunction.UNBOX);
    }

    /**
     * Queries for a single integer value from the specified property.
     *
     * <p>This method is marked as Beta and may change in future versions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalInt count = executor.queryForInt(Product.class, "quantity",
     *                                          Filters.eq("product_id", productId));
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param propName the property name to retrieve
     * @param whereClause the WHERE condition
     * @return an OptionalInt containing the result, or empty if no result found
     * @throws IllegalArgumentException if targetClass is null or propName is null or empty
     */
    @Beta
    public <T> OptionalInt queryForInt(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleResult(targetClass, Integer.class, propName, whereClause).mapToInt(ToIntFunction.UNBOX);
    }

    /**
     * Queries for a single long value from the specified property.
     *
     * <p>This method is marked as Beta and may change in future versions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalLong timestamp = executor.queryForLong(Event.class, "timestamp",
     *                                                Filters.eq("event_id", eventId));
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param propName the property name to retrieve
     * @param whereClause the WHERE condition
     * @return an OptionalLong containing the result, or empty if no result found
     * @throws IllegalArgumentException if targetClass is null or propName is null or empty
     */
    @Beta
    public <T> OptionalLong queryForLong(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleResult(targetClass, Long.class, propName, whereClause).mapToLong(ToLongFunction.UNBOX);
    }

    /**
     * Queries for a single float value from the specified property.
     *
     * <p>This method is marked as Beta and may change in future versions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalFloat rating = executor.queryForFloat(Product.class, "rating",
     *                                               Filters.eq("product_id", productId));
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param propName the property name to retrieve
     * @param whereClause the WHERE condition
     * @return an OptionalFloat containing the result, or empty if no result found
     * @throws IllegalArgumentException if targetClass is null or propName is null or empty
     */
    @Beta
    public <T> OptionalFloat queryForFloat(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleResult(targetClass, Float.class, propName, whereClause).mapToFloat(ToFloatFunction.UNBOX);
    }

    /**
     * Queries for a single double value from the specified property.
     *
     * <p>This method is marked as Beta and may change in future versions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalDouble price = executor.queryForDouble(Product.class, "price",
     *                                                Filters.eq("product_id", productId));
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param propName the property name to retrieve
     * @param whereClause the WHERE condition
     * @return an OptionalDouble containing the result, or empty if no result found
     * @throws IllegalArgumentException if targetClass is null or propName is null or empty
     */
    @Beta
    public <T> OptionalDouble queryForDouble(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleResult(targetClass, Double.class, propName, whereClause).mapToDouble(ToDoubleFunction.UNBOX);
    }

    /**
     * Queries for a single String value from the specified property.
     *
     * <p>This method is marked as Beta and may change in future versions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Nullable<String> email = executor.queryForString(User.class, "email",
     *                                                  Filters.eq("username", "john"));
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param propName the property name to retrieve
     * @param whereClause the WHERE condition
     * @return a Nullable containing the result, which may be null
     * @throws IllegalArgumentException if targetClass is null or propName is null or empty
     */
    @Beta
    public <T> Nullable<String> queryForString(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return this.queryForSingleResult(targetClass, String.class, propName, whereClause);
    }

    /**
     * Queries for a single Date value from the specified property.
     *
     * <p>This method is marked as Beta and may change in future versions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Nullable<Date> created = executor.queryForDate(User.class, "created_at",
     *                                                Filters.eq("user_id", userId));
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param propName the property name to retrieve
     * @param whereClause the WHERE condition
     * @return a Nullable containing the result, which may be null
     * @throws IllegalArgumentException if targetClass is null or propName is null or empty
     */
    @Beta
    public <T> Nullable<Date> queryForDate(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return this.queryForSingleResult(targetClass, Date.class, propName, whereClause);
    }

    /**
     * Queries for a single Date value of a specific type from the specified property.
     *
     * <p>This method is marked as Beta and may change in future versions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Nullable<java.sql.Timestamp> lastLogin = executor.queryForDate(User.class,
     *                                                                java.sql.Timestamp.class,
     *                                                                "last_login",
     *                                                                Filters.eq("user_id", userId));
     * }</pre>
     *
     * @param <T> the entity type
     * @param <E> the specific Date type (e.g., java.sql.Timestamp)
     * @param targetClass the entity class
     * @param valueClass the specific Date class to return
     * @param propName the property name to retrieve
     * @param whereClause the WHERE condition
     * @return a Nullable containing the result, which may be null
     * @throws IllegalArgumentException if targetClass is null or valueClass is null or propName is null or empty
     */
    @Beta
    public <T, E extends Date> Nullable<E> queryForDate(final Class<T> targetClass, final Class<E> valueClass, final String propName,
            final Condition whereClause) {
        return this.queryForSingleResult(targetClass, valueClass, propName, whereClause);
    }

    /**
     * Queries for a single result value of the specified type from a property.
     *
     * <p>This is a generic method for retrieving a single value of any type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Nullable<BigDecimal> price = executor.queryForSingleResult(Product.class,
     *                                                            BigDecimal.class,
     *                                                            "price",
     *                                                            Filters.eq("id", productId));
     * }</pre>
     *
     * @param <T> the entity type
     * @param <V> the value type to return
     * @param targetClass the entity class
     * @param valueClass the class of the value to return
     * @param propName the property name to retrieve
     * @param whereClause the WHERE condition
     * @return a Nullable containing the result, which may be null
     * @throws IllegalArgumentException if targetClass is null or valueClass is null or propName is null or empty
     */
    public <T, V> Nullable<V> queryForSingleResult(final Class<T> targetClass, final Class<V> valueClass, final String propName, final Condition whereClause) {
        final SP cp = prepareQuery(targetClass, List.of(propName), whereClause, 1);

        return queryForSingleResult(valueClass, cp.query(), cp.parameters().toArray());
    }

    /**
     * Queries for a single non-null result value of the specified type from a property.
     *
     * <p>Unlike queryForSingleResult, this method returns an Optional that will be empty
     * if the result is null, providing a cleaner API for non-nullable values.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Optional<String> email = executor.queryForSingleNonNull(User.class,
     *                                                         String.class,
     *                                                         "email",
     *                                                         Filters.eq("username", "john"));
     * }</pre>
     *
     * @param <T> the entity type
     * @param <V> the value type to return
     * @param targetClass the entity class
     * @param valueClass the class of the value to return
     * @param propName the property name to retrieve
     * @param whereClause the WHERE condition
     * @return an Optional containing the result, or empty if no result or result is null
     * @throws IllegalArgumentException if targetClass is null or valueClass is null or propName is null or empty
     */
    public <T, V> Optional<V> queryForSingleNonNull(final Class<T> targetClass, final Class<V> valueClass, final String propName, final Condition whereClause) {
        final SP cp = prepareQuery(targetClass, List.of(propName), whereClause, 1);

        return queryForSingleNonNull(valueClass, cp.query(), cp.parameters().toArray());
    }

    /**
     * Creates a Stream of entities matching the specified condition.
     *
     * <p>Streams provide lazy evaluation and can be more memory-efficient for large result sets.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long count = executor.stream(User.class, Filters.gt("age", 18))
     *                      .filter(u -> u.isActive())
     *                      .count();
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param whereClause the WHERE condition
     * @return a Stream of matching entities
     * @throws IllegalArgumentException if targetClass is null
     */
    public <T> Stream<T> stream(final Class<T> targetClass, final Condition whereClause) {
        return stream(targetClass, null, whereClause);
    }

    /**
     * Creates a Stream of entities with specific properties matching the specified condition.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Stream<User> userStream = executor.stream(User.class,
     *                                           Arrays.asList("id", "name", "email"),
     *                                           Filters.eq("status", "active"));
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param whereClause the WHERE condition
     * @return a Stream of matching entities with selected properties
     * @throws IllegalArgumentException if targetClass is null
     */
    public <T> Stream<T> stream(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP cp = prepareQuery(targetClass, selectPropNames, whereClause);

        return stream(targetClass, cp.query(), cp.parameters().toArray());
    }

    /**
     * Checks if any records exist matching the given query.
     * 
     * <p>Always remember to set "LIMIT 1" in the CQL statement for better performance,
     * as this method only needs to check for the existence of at least one record.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * boolean exists = executor.exists("SELECT * FROM users WHERE email = ? LIMIT 1", 
     *                                  "user@example.com");
     * }</pre>
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return {@code true} if at least one record exists, {@code false} otherwise
     */
    public final boolean exists(final String query, final Object... parameters) {
        final RS resultSet = execute(query, parameters);

        return resultSet.iterator().hasNext();
    }

    /**
     * Counts the number of records matching the given query.
     *
     * @deprecated Use {@link #queryForLong(String, Object...)} with COUNT(*) in the query instead. Note: queryForLong returns OptionalLong; use {@code .orElse(0L)} for equivalent behavior.
     *             This method will be removed in a future version.
     *
     * @param query the CQL query to execute (should return a count)
     * @param parameters the query parameters
     * @return the count of matching records, or 0 if none found
     * @see #queryForLong(String, Object...)
     * @see #count(Class, Condition)
     */
    @Deprecated
    public final long count(final String query, final Object... parameters) {
        return queryForSingleResult(long.class, query, parameters).orElse(0L);
    }

    /**
     * Queries for a single boolean value from the query result.
     * 
     * <p>This method is marked as Beta and may change in future versions.</p>
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return an OptionalBoolean containing the result, or empty if no result found
     */
    @Beta
    public final OptionalBoolean queryForBoolean(final String query, final Object... parameters) {
        return this.queryForSingleResult(Boolean.class, query, parameters).mapToBoolean(ToBooleanFunction.UNBOX);
    }

    /**
     * Queries for a single character value from the query result.
     * 
     * <p>This method is marked as Beta and may change in future versions.</p>
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return an OptionalChar containing the result, or empty if no result found
     */
    @Beta
    public final OptionalChar queryForChar(final String query, final Object... parameters) {
        return this.queryForSingleResult(Character.class, query, parameters).mapToChar(ToCharFunction.UNBOX);
    }

    /**
     * Queries for a single byte value from the query result.
     * 
     * <p>This method is marked as Beta and may change in future versions.</p>
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return an OptionalByte containing the result, or empty if no result found
     */
    @Beta
    public final OptionalByte queryForByte(final String query, final Object... parameters) {
        return this.queryForSingleResult(Byte.class, query, parameters).mapToByte(ToByteFunction.UNBOX);
    }

    /**
     * Queries for a single short value from the query result.
     * 
     * <p>This method is marked as Beta and may change in future versions.</p>
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return an OptionalShort containing the result, or empty if no result found
     */
    @Beta
    public final OptionalShort queryForShort(final String query, final Object... parameters) {
        return this.queryForSingleResult(Short.class, query, parameters).mapToShort(ToShortFunction.UNBOX);
    }

    /**
     * Queries for a single integer value from the query result.
     * 
     * <p>This method is marked as Beta and may change in future versions.</p>
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return an OptionalInt containing the result, or empty if no result found
     */
    @Beta
    public final OptionalInt queryForInt(final String query, final Object... parameters) {
        return this.queryForSingleResult(Integer.class, query, parameters).mapToInt(ToIntFunction.UNBOX);
    }

    /**
     * Queries for a single long value from the query result.
     * 
     * <p>This method is marked as Beta and may change in future versions.</p>
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return an OptionalLong containing the result, or empty if no result found
     */
    @Beta
    public final OptionalLong queryForLong(final String query, final Object... parameters) {
        return this.queryForSingleResult(Long.class, query, parameters).mapToLong(ToLongFunction.UNBOX);
    }

    /**
     * Queries for a single float value from the query result.
     * 
     * <p>This method is marked as Beta and may change in future versions.</p>
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return an OptionalFloat containing the result, or empty if no result found
     */
    @Beta
    public final OptionalFloat queryForFloat(final String query, final Object... parameters) {
        return this.queryForSingleResult(Float.class, query, parameters).mapToFloat(ToFloatFunction.UNBOX);
    }

    /**
     * Queries for a single double value from the query result.
     * 
     * <p>This method is marked as Beta and may change in future versions.</p>
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return an OptionalDouble containing the result, or empty if no result found
     */
    @Beta
    public final OptionalDouble queryForDouble(final String query, final Object... parameters) {
        return this.queryForSingleResult(Double.class, query, parameters).mapToDouble(ToDoubleFunction.UNBOX);
    }

    /**
     * Queries for a single String value from the query result.
     * 
     * <p>This method is marked as Beta and may change in future versions.</p>
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a Nullable containing the result, which may be null
     */
    @Beta
    public final Nullable<String> queryForString(final String query, final Object... parameters) {
        return this.queryForSingleResult(String.class, query, parameters);
    }

    /**
     * Executes a CQL query and returns a single result value.
     *
     * <p>This method is designed for queries that return a single column and single row,
     * such as aggregate functions (COUNT, SUM, MAX, etc.) or lookup queries that are
     * expected to return at most one value. The result is automatically converted to
     * the specified value class.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Count total users
     * Nullable<Long> userCount = executor.queryForSingleResult(
     *     Long.class, "SELECT COUNT(*) FROM users");
     *
     * // Get user name by ID
     * Nullable<String> userName = executor.queryForSingleResult(
     *     String.class, "SELECT name FROM users WHERE id = ?", userId);
     *
     * // Get maximum timestamp
     * Nullable<Instant> maxTimestamp = executor.queryForSingleResult(
     *     Instant.class, "SELECT MAX(created_at) FROM events WHERE date = ?", today);
     * }</pre>
     *
     * @param <E> the type of the expected result value
     * @param valueClass the Java class to convert the result to
     * @param query the CQL query string
     * @param parameters the query parameters
     * @return a Nullable containing the result value, or empty if no result was found
     * @throws IllegalArgumentException if valueClass or query is null
     */
    public abstract <E> Nullable<E> queryForSingleResult(final Class<E> valueClass, final String query, final Object... parameters);

    /**
     * Executes a CQL query and returns a single non-null result value.
     *
     * <p>Similar to {@link #queryForSingleResult}, but returns an {@link Optional}
     * that will be empty if the result is null. This method is useful when you need
     * to distinguish between "no result found" and "result found but value is null".</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get active user count (never null for COUNT queries)
     * Optional<Long> activeUsers = executor.queryForSingleNonNull(
     *     Long.class, "SELECT COUNT(*) FROM users WHERE status = 'active'");
     *
     * // Get user email (could be null)
     * Optional<String> email = executor.queryForSingleNonNull(
     *     String.class, "SELECT email FROM users WHERE id = ?", userId);
     * }</pre>
     *
     * @param <E> the type of the expected result value
     * @param valueClass the Java class to convert the result to
     * @param query the CQL query string
     * @param parameters the query parameters
     * @return an Optional containing the non-null result value, or empty if no result or null value
     * @throws IllegalArgumentException if valueClass or query is null
     */
    public abstract <E> Optional<E> queryForSingleNonNull(final Class<E> valueClass, final String query, final Object... parameters);

    /**
     * Finds the first row from the query result as a Map.
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Optional<Map<String, Object>> user = executor.findFirst(
     *     "SELECT * FROM users WHERE status = ?", "active");
     * }</pre>
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return an Optional containing the first row as a Map, or empty if no results
     */
    public final Optional<Map<String, Object>> findFirst(final String query, final Object... parameters) {
        return findFirst(Clazz.PROPS_MAP, query, parameters);
    }

    /**
     * Finds the first row from a CQL query result and maps it to the specified type.
     *
     * <p>This method executes the query and returns the first row as an entity of the
     * specified target class. If no rows are found, an empty Optional is returned.
     * This is particularly useful for lookup operations where you expect at most one result.</p>
     *
     * <p>The target class can be:</p>
     * <ul>
     * <li><strong>Entity class:</strong> A POJO with getter/setter methods matching column names</li>
     * <li><strong>Map.class:</strong> Results mapped to a {@code Map<String, Object>}</li>
     * <li><strong>Collection class:</strong> Results mapped to List, Set, etc.</li>
     * <li><strong>Array class:</strong> Results mapped to Object[] or typed arrays</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Find user by email
     * Optional<User> user = executor.findFirst(User.class,
     *     "SELECT * FROM users WHERE email = ? LIMIT 1", email);
     *
     * // Get as map
     * Optional<Map<String, Object>> userData = executor.findFirst(Map.class,
     *     "SELECT name, email FROM users WHERE id = ?", userId);
     *
     * // Get as array
     * Optional<Object[]> row = executor.findFirst(Object[].class,
     *     "SELECT COUNT(*), MAX(created_at) FROM events");
     * }</pre>
     *
     * @param <T> the type to map the result row to
     * @param targetClass an entity class with getter/setter methods or Map.class
     * @param query the CQL query string
     * @param parameters the query parameters
     * @return an Optional containing the first result row mapped to the target type, or empty if no results
     * @throws IllegalArgumentException if targetClass or query is null
     */
    public abstract <T> Optional<T> findFirst(final Class<T> targetClass, final String query, final Object... parameters);

    /**
     * Returns all query results as a List of Maps.
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a List of Maps representing all result rows
     */
    public final List<Map<String, Object>> list(final String query, final Object... parameters) {
        return list(Clazz.PROPS_MAP, query, parameters);
    }

    /**
     * Returns all query results as a List of the specified type.
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<User> activeUsers = executor.list(User.class, 
     *     "SELECT * FROM users WHERE status = ?", "active");
     * }</pre>
     * 
     * @param <T> the target type
     * @param targetClass an entity class with getter/setter methods, Map.class, or basic single value type
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a List of results mapped to the target type
     */
    public final <T> List<T> list(final Class<T> targetClass, final String query, final Object... parameters) {
        return toList(targetClass, execute(query, parameters));
    }

    /**
     * Executes a query and returns results as a Dataset with Map rows.
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a Dataset containing all result rows
     */
    public final Dataset query(final String query, final Object... parameters) {
        return query(Map.class, query, parameters);
    }

    /**
     * Executes a query and returns results as a Dataset with rows mapped to the specified type.
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Dataset userDataset = executor.query(User.class, 
     *     "SELECT * FROM users WHERE age > ?", 18);
     * }</pre>
     * 
     * @param targetClass an entity class with getter/setter methods or Map.class
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a Dataset containing all result rows mapped to the target type
     */
    public final Dataset query(final Class<?> targetClass, final String query, final Object... parameters) {
        return extractData(targetClass, execute(query, parameters));
    }

    /**
     * Creates a Stream of Object arrays from the query results.
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a Stream of Object arrays, each representing a result row
     */
    public final Stream<Object[]> stream(final String query, final Object... parameters) {
        return stream(Object[].class, query, parameters);
    }

    /**
     * Creates a Stream of results mapped to the specified type.
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * executor.stream(User.class, "SELECT * FROM users")
     *         .filter(u -> u.getAge() > 21)
     *         .forEach(System.out::println);
     * }</pre>
     * 
     * @param <T> the target type
     * @param targetClass an entity class with getter/setter methods or Map.class
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a Stream of results mapped to the target type
     */
    public final <T> Stream<T> stream(final Class<T> targetClass, final String query, final Object... parameters) {
        return Stream.of(execute(query, parameters).iterator()).map(createRowMapper(targetClass));
    }

    /**
     * Creates a Stream of results from a prepared statement.
     *
     * @param <T> the target type
     * @param targetClass an entity class with getter/setter methods or Map.class
     * @param statement the prepared statement to execute
     * @return a Stream of results mapped to the target type
     * @throws IllegalArgumentException if targetClass is null or statement is null
     */
    public <T> Stream<T> stream(final Class<T> targetClass, final ST statement) {
        return Stream.of(execute(statement).iterator()).map(createRowMapper(targetClass));
    }

    /**
     * Executes a CQL statement without parameters and returns the result set.
     *
     * <p>This method executes the provided CQL statement and returns the result set.
     * This is useful for DDL operations, simple queries without parameters,
     * or when you need direct access to the result set for custom processing.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create table
     * executor.execute("CREATE TABLE IF NOT EXISTS users (id UUID PRIMARY KEY, name TEXT)");
     *
     * // Simple query
     * ResultSet results = executor.execute("SELECT * FROM users LIMIT 10");
     *
     * // Process results manually
     * for (Row row : results) {
     *     UUID id = row.getUuid("id");
     *     String name = row.getString("name");
     *     // Process row...
     * }
     * }</pre>
     *
     * @param query the CQL statement to execute
     * @return the result set from the query execution
     * @throws IllegalArgumentException if query is null or empty
     */
    public abstract RS execute(final String query);

    /**
     * Executes a parameterized CQL statement and returns the result set.
     *
     * <p>This method supports multiple parameter binding styles and automatically prepares
     * and caches statements for optimal performance. The parameters can be provided in
     * various formats to match different use cases.</p>
     *
     * <p>Supported parameter formats:</p>
     * <ul>
     * <li><strong>Individual values:</strong> {@code execute("SELECT * FROM users WHERE id = ?", userId)}</li>
     * <li><strong>Array:</strong> {@code execute(query, new Object[] {val1, val2})}</li>
     * <li><strong>Collection:</strong> {@code execute(query, Arrays.asList(val1, val2))}</li>
     * <li><strong>Map:</strong> {@code execute("SELECT * FROM users WHERE name = :name", N.asMap("name", "John"))}</li>
     * <li><strong>Entity:</strong> {@code execute(query, userEntity)} (uses bean properties)</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Positional parameters
     * ResultSet users = executor.execute(
     *     "SELECT * FROM users WHERE status = ? AND created_at > ?",
     *     "active", yesterday);
     *
     * // Named parameters with map
     * Map<String, Object> params = new HashMap<>();
     * params.put("email", "john@example.com");
     * params.put("status", "active");
     * ResultSet result = executor.execute(
     *     "SELECT * FROM users WHERE email = :email AND status = :status", params);
     *
     * // Entity parameters
     * User searchCriteria = new User();
     * searchCriteria.setStatus("active");
     * searchCriteria.setDepartment("engineering");
     * ResultSet engineeringUsers = executor.execute(
     *     "SELECT * FROM users WHERE status = :status AND department = :department",
     *     searchCriteria);
     * }</pre>
     *
     * @param query the parameterized CQL statement
     * @param parameters the parameter values (can be individual values, arrays, collections, maps, or entities)
     * @return the result set from the query execution
     * @throws IllegalArgumentException if query is null or if parameter count/names don't match
     */
    public abstract RS execute(final String query, final Object... parameters);

    /**
     * Executes a CQL statement with named parameters provided as a Map.
     *
     * <p>This method allows you to execute parameterized CQL statements using named
     * parameters. The parameter names in the query should match the keys in the
     * provided Map. This approach is more readable and less error-prone than positional
     * parameters, especially for queries with many parameters.</p>
     *
     * <p>Supports both Cassandra native named parameters ({@code :paramName}) and
     * custom parameter placeholders. The method handles parameter binding automatically
     * and performs necessary type conversions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Query with named parameters
     * String query = "SELECT * FROM users WHERE department = :dept AND status = :status AND created_at > :since";
     *
     * Map<String, Object> params = new HashMap<>();
     * params.put("dept", "engineering");
     * params.put("status", "active");
     * params.put("since", Instant.now().minus(Duration.ofDays(30)));
     *
     * ResultSet results = executor.execute(query, params);
     *
     * // Alternative using N.asMap utility
     * ResultSet results2 = executor.execute(
     *     "SELECT * FROM orders WHERE user_id = :userId AND status = :status",
     *     N.asMap("userId", UUID.fromString("123e4567-e89b-12d3-a456-426614174000"),
     *             "status", "pending"));
     * }</pre>
     *
     * @param query the parameterized CQL statement with named parameters
     * @param parameters a Map containing parameter names as keys and parameter values as values
     * @return the result set from the query execution
     * @throws IllegalArgumentException if query is null or if required parameters are missing
     */
    public abstract RS execute(String query, Map<String, Object> parameters);

    /**
     * Executes a pre-configured CQL Statement and returns the result set.
     *
     * <p>This method executes any type of Cassandra Statement object, including
     * BoundStatement, BatchStatement, or SimpleStatement. This provides maximum
     * flexibility when you need fine-grained control over statement configuration
     * such as consistency levels, timeouts, or retry policies.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Execute a prepared statement with custom settings
     * PreparedStatement preparedStatement = session.prepare(
     *     "INSERT INTO users (id, name, email) VALUES (?, ?, ?)");
     *
     * BoundStatement boundStatement = preparedStatement
     *     .bind(UUID.randomUUID(), "John Doe", "john@example.com")
     *     .setConsistencyLevel(ConsistencyLevel.QUORUM)
     *     .setTimeout(Duration.ofSeconds(10));
     *
     * ResultSet result = executor.execute(boundStatement);
     *
     * // Execute a batch statement
     * BatchStatement batch = BatchStatement.newInstance(BatchType.LOGGED)
     *     .add(statement1)
     *     .add(statement2)
     *     .add(statement3);
     *
     * ResultSet batchResult = executor.execute(batch);
     *
     * // Execute a simple statement with custom settings
     * SimpleStatement simpleStatement = SimpleStatement.builder(
     *         "SELECT * FROM users WHERE token(id) > token(?)", lastId)
     *     .setPageSize(1000)
     *     .build();
     *
     * ResultSet pagedResults = executor.execute(simpleStatement);
     * }</pre>
     *
     * @param statement the configured CQL Statement to execute
     * @return the result set from the statement execution
     * @throws IllegalArgumentException if statement is null
     */
    public abstract RS execute(final ST statement);

    /**
     * Asynchronously retrieves an entity by its primary key(s).
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ContinuableFuture<Optional<User>> future = executor.asyncGet(User.class, userId);
     * future.thenRunAsync(opt -> opt.ifPresent(System.out::println));
     * }</pre>
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param ids the primary key value(s)
     * @return a future containing an Optional with the entity, or empty if not found
     */
    public final <T> ContinuableFuture<Optional<T>> asyncGet(final Class<T> targetClass, final Object... ids) {
        return asyncGet(targetClass, null, ids);
    }

    /**
     * Asynchronously retrieves specific properties of an entity by its primary key(s).
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param ids the primary key value(s)
     * @return a future containing an Optional with the entity, or empty if not found
     * @throws DuplicateResultException if multiple results are found
     */
    public final <T> ContinuableFuture<Optional<T>> asyncGet(final Class<T> targetClass, final Collection<String> selectPropNames, final Object... ids)
            throws DuplicateResultException {
        return asyncGet(targetClass, selectPropNames, idsToCondition(targetClass, ids));
    }

    /**
     * Asynchronously retrieves an entity matching the specified condition.
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param whereClause the WHERE condition
     * @return a future containing an Optional with the entity, or empty if not found
     */
    public <T> ContinuableFuture<Optional<T>> asyncGet(final Class<T> targetClass, final Condition whereClause) {
        return asyncGet(targetClass, null, whereClause);
    }

    /**
     * Asynchronously retrieves specific properties of an entity matching the specified condition.
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param whereClause the WHERE condition
     * @return a future containing an Optional with the entity, or empty if not found
     */
    public abstract <T> ContinuableFuture<Optional<T>> asyncGet(final Class<T> targetClass, final Collection<String> selectPropNames,
            final Condition whereClause);

    /**
     * Asynchronously retrieves a single entity by its primary key(s) through the nullable {@code asyncGett} variant.
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param ids the primary key value(s)
     * @return a future that completes with the entity, or {@code null} if no row matches
     */
    public final <T> ContinuableFuture<T> asyncGett(final Class<T> targetClass, final Object... ids) {
        return asyncGett(targetClass, null, ids);
    }

    /**
     * Asynchronously retrieves specific properties of a single entity by its primary key(s) through the nullable {@code asyncGett} variant.
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param ids the primary key value(s)
     * @return a future that completes with the entity, or {@code null} if no row matches
     * @throws DuplicateResultException if multiple results are found
     */
    public final <T> ContinuableFuture<T> asyncGett(final Class<T> targetClass, final Collection<String> selectPropNames, final Object... ids)
            throws DuplicateResultException {
        return asyncGett(targetClass, selectPropNames, idsToCondition(targetClass, ids));
    }

    /**
     * Asynchronously retrieves a single entity matching the specified condition through the nullable {@code asyncGett} variant.
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param whereClause the WHERE condition
     * @return a future that completes with the entity, or {@code null} if no row matches
     */
    public <T> ContinuableFuture<T> asyncGett(final Class<T> targetClass, final Condition whereClause) {
        return asyncGett(targetClass, null, whereClause);
    }

    /**
     * Asynchronously retrieves specific properties of a single entity matching the specified condition through the nullable {@code asyncGett} variant.
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param whereClause the WHERE condition
     * @return a future that completes with the entity, or {@code null} if no row matches
     */
    public abstract <T> ContinuableFuture<T> asyncGett(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause);

    /**
     * Asynchronously inserts an entity into the database.
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User newUser = new User("John", "john@example.com");
     * executor.asyncInsert(newUser).thenRunAsync(rs -> System.out.println("User inserted"));
     * }</pre>
     * 
     * @param entity the entity to insert
     * @return a future containing the result set from the insert operation
     */
    public ContinuableFuture<RS> asyncInsert(final Object entity) {
        final SP cp = prepareInsert(entity);

        return asyncExecute(cp);
    }

    /**
     * Asynchronously inserts a record with the specified properties.
     * 
     * @param targetClass the entity class
     * @param props map of property names to values to insert
     * @return a future containing the result set from the insert operation
     */
    public ContinuableFuture<RS> asyncInsert(final Class<?> targetClass, final Map<String, Object> props) {
        final SP cp = prepareInsert(targetClass, props);

        return asyncExecute(cp);
    }

    /**
     * Asynchronously performs a batch insert of multiple entities.
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<User> users = Arrays.asList(new User("John"), new User("Jane"));
     * executor.asyncBatchInsert(users, BatchType.LOGGED);
     * }</pre>
     * 
     * @param entities collection of entities to insert
     * @param type the batch type
     * @return a future containing the result set from the batch insert operation
     */
    public ContinuableFuture<RS> asyncBatchInsert(final Collection<?> entities, final BT type) {
        final ST stmt = prepareBatchInsertStatement(entities, type);

        return asyncExecute(stmt);
    }

    /**
     * Asynchronously performs a batch insert of records with specified properties.
     * 
     * @param targetClass the entity class
     * @param propsList collection of property maps to insert
     * @param type the batch type
     * @return a future containing the result set from the batch insert operation
     */
    public ContinuableFuture<RS> asyncBatchInsert(final Class<?> targetClass, final Collection<? extends Map<String, Object>> propsList, final BT type) {
        final ST stmt = prepareBatchInsertStatement(targetClass, propsList, type);

        return asyncExecute(stmt);
    }

    /**
     * Asynchronously updates an entity in the database using its primary key(s).
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * user.setEmail("newemail@example.com");
     * executor.asyncUpdate(user);
     * }</pre>
     * 
     * @param entity the entity to update
     * @return a future containing the result set from the update operation
     */
    @SuppressWarnings("deprecation")
    public ContinuableFuture<RS> asyncUpdate(final Object entity) {
        final Class<?> entityClass = entity.getClass();
        final Set<String> keyNameSet = getKeyNameSet(entityClass);
        final Collection<String> updatePropNames = QueryUtil.getUpdatePropNames(entityClass, keyNameSet);

        return asyncUpdate(entity, updatePropNames);
    }

    /**
     * Asynchronously updates specific properties of an entity.
     * 
     * @param entity the entity containing updated values
     * @param propNamesToUpdate collection of property names to update
     * @return a future containing the result set from the update operation
     */
    public ContinuableFuture<RS> asyncUpdate(final Object entity, final Collection<String> propNamesToUpdate) {
        N.checkArgument(N.notEmpty(propNamesToUpdate), "'propNamesToUpdate' can't be null or empty");

        final SP cp = prepareUpdate(entity, propNamesToUpdate);

        return asyncExecute(cp);
    }

    /**
     * Asynchronously updates records matching the specified condition.
     * 
     * @param targetClass the entity class
     * @param props map of property names to new values
     * @param whereClause the WHERE condition
     * @return a future containing the result set from the update operation
     */
    public ContinuableFuture<RS> asyncUpdate(final Class<?> targetClass, final Map<String, Object> props, final Condition whereClause) {
        final SP cp = prepareUpdate(targetClass, props, whereClause);

        return asyncExecute(cp);
    }

    /**
     * Asynchronously executes an update query with parameters.
     * 
     * @param query the update query to execute
     * @param parameters the query parameters
     * @return a future containing the result set from the update operation
     */
    public ContinuableFuture<RS> asyncUpdate(final String query, final Object... parameters) {
        return asyncExecute(query, parameters);
    }

    /**
     * Asynchronously performs a batch update of multiple entities using their primary keys.
     * 
     * @param entities collection of entities to update
     * @param type the batch type
     * @return a future containing the result set from the batch update operation
     */
    @SuppressWarnings("deprecation")
    public ContinuableFuture<RS> asyncBatchUpdate(final Collection<?> entities, final BT type) {
        N.checkArgument(N.notEmpty(entities), "'entities' can't be null or empty.");

        final Object firstEntity = N.firstOrNullIfEmpty(entities);
        N.checkArgNotNull(firstEntity, "The first entity in the collection can't be null.");
        final Class<?> entityClass = firstEntity.getClass();
        final Set<String> keyNameSet = getKeyNameSet(entityClass);
        final Collection<String> updatePropNames = QueryUtil.getUpdatePropNames(entityClass, keyNameSet);

        return asyncBatchUpdate(entities, updatePropNames, type);
    }

    /**
     * Asynchronously performs a batch update of specific properties for multiple entities.
     * 
     * @param entities collection of entities to update
     * @param propNamesToUpdate collection of property names to update
     * @param type the batch type
     * @return a future containing the result set from the batch update operation
     */
    public ContinuableFuture<RS> asyncBatchUpdate(final Collection<?> entities, final Collection<String> propNamesToUpdate, final BT type) {
        N.checkArgument(N.notEmpty(propNamesToUpdate), "'propNamesToUpdate' can't be null or empty");

        final ST stmt = prepareBatchUpdateStatement(entities, propNamesToUpdate, type);

        return asyncExecute(stmt);

    }

    /**
     * Asynchronously performs a batch update using property maps.
     * 
     * @param targetClass the entity class
     * @param propsList collection of property maps for updates
     * @param type the batch type
     * @return a future containing the result set from the batch update operation
     */
    public ContinuableFuture<RS> asyncBatchUpdate(final Class<?> targetClass, final Collection<? extends Map<String, Object>> propsList, final BT type) {
        final ST stmt = prepareBatchUpdateStatement(targetClass, propsList, type);

        return asyncExecute(stmt);
    }

    /**
     * Asynchronously performs a batch update with parameterized queries.
     * 
     * @param query the update query template
     * @param parametersList collection of parameter arrays for each update
     * @param type the batch type
     * @return a future containing the result set from the batch update operation
     */
    public ContinuableFuture<RS> asyncBatchUpdate(final String query, final Collection<?> parametersList, final BT type) {
        final ST stmt = prepareBatchUpdateStatement(query, parametersList, type);

        return asyncExecute(stmt);
    }

    /**
     * Asynchronously deletes an entire entity from the database.
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * executor.asyncDelete(userToDelete);
     * }</pre>
     * 
     * @param entity the entity to delete
     * @return a future containing the result set from the delete operation
     */
    public ContinuableFuture<RS> asyncDelete(final Object entity) {
        return asyncDelete(entity, null);
    }

    /**
     * Asynchronously deletes specific properties of an entity.
     * 
     * @param entity the entity containing the primary key values
     * @param propNamesToDelete collection of property names to delete, or null to delete entire record
     * @return a future containing the result set from the delete operation
     */
    public ContinuableFuture<RS> asyncDelete(final Object entity, final Collection<String> propNamesToDelete) {
        N.checkArgument(propNamesToDelete == null || N.notEmpty(propNamesToDelete), "'propNamesToDelete' can't be null or empty");

        return asyncDelete(entity.getClass(), propNamesToDelete, entityToCondition(entity));
    }

    /**
     * Asynchronously deletes a record by its primary key(s).
     * 
     * @param targetClass the entity class
     * @param ids the primary key value(s)
     * @return a future containing the result set from the delete operation
     */
    public final ContinuableFuture<RS> asyncDelete(final Class<?> targetClass, final Object... ids) {
        return asyncDelete(targetClass, null, ids);
    }

    /**
     * Asynchronously deletes specific properties of a record by its primary key(s).
     * 
     * <p>Delete the specified properties if propNamesToDelete is not null or empty, 
     * otherwise, delete the whole record.</p>
     * 
     * @param targetClass the entity class
     * @param propNamesToDelete collection of property names to delete, or null to delete entire record
     * @param ids the primary key value(s)
     * @return a future containing the result set from the delete operation
     */
    public final ContinuableFuture<RS> asyncDelete(final Class<?> targetClass, final Collection<String> propNamesToDelete, final Object... ids) {
        N.checkArgument(propNamesToDelete == null || N.notEmpty(propNamesToDelete), "'propNamesToDelete' can't be null or empty");

        return asyncDelete(targetClass, propNamesToDelete, idsToCondition(targetClass, ids));
    }

    /**
     * Asynchronously deletes records matching the specified condition.
     * 
     * @param targetClass the entity class
     * @param whereClause the WHERE condition
     * @return a future containing the result set from the delete operation
     */
    public ContinuableFuture<RS> asyncDelete(final Class<?> targetClass, final Condition whereClause) {
        return asyncDelete(targetClass, null, whereClause);
    }

    /**
     * Asynchronously deletes specific properties of records matching the specified condition.
     * 
     * <p>Delete the specified properties if propNamesToDelete is not null or empty, 
     * otherwise, delete the whole record.</p>
     * 
     * @param targetClass the entity class
     * @param propNamesToDelete collection of property names to delete, or null to delete entire records
     * @param whereClause the WHERE condition
     * @return a future containing the result set from the delete operation
     */
    public ContinuableFuture<RS> asyncDelete(final Class<?> targetClass, final Collection<String> propNamesToDelete, final Condition whereClause) {
        N.checkArgument(propNamesToDelete == null || N.notEmpty(propNamesToDelete), "'propNamesToDelete' can't be null or empty");

        final SP cp = prepareDelete(targetClass, propNamesToDelete, whereClause);

        return asyncExecute(cp);
    }

    /**
     * Asynchronously performs a batch delete of multiple entities.
     * 
     * @param entities collection of entities to delete
     * @return a future containing the result set from the batch delete operation
     */
    public ContinuableFuture<RS> asyncBatchDelete(final Collection<?> entities) {
        N.checkArgument(N.notEmpty(entities), "'entities' can't be null or empty.");

        final Object firstEntity = N.firstOrNullIfEmpty(entities);
        N.checkArgNotNull(firstEntity, "The first entity in the collection can't be null.");
        final Class<?> entityClass = firstEntity.getClass();
        final Condition cond = entityToCondition(entityClass, entities);

        return asyncDelete(entityClass, cond);
    }

    /**
     * Asynchronously performs a batch delete of specific properties for multiple entities.
     * 
     * @param entities collection of entities containing primary key values
     * @param propNamesToDelete collection of property names to delete, or null to delete entire records
     * @return a future containing the result set from the batch delete operation
     */
    public ContinuableFuture<RS> asyncBatchDelete(final Collection<?> entities, final Collection<String> propNamesToDelete) {
        N.checkArgument(N.notEmpty(entities), "'entities' can't be null or empty.");
        N.checkArgument(propNamesToDelete == null || N.notEmpty(propNamesToDelete), "'propNamesToDelete' can't be null or empty");

        final Object firstEntity = N.firstOrNullIfEmpty(entities);
        N.checkArgNotNull(firstEntity, "The first entity in the collection can't be null.");
        final Class<?> entityClass = firstEntity.getClass();
        final Condition cond = entityToCondition(entityClass, entities);

        return asyncDelete(entityClass, propNamesToDelete, cond);
    }

    /**
     * Asynchronously checks if a record exists by its primary key(s).
     * 
     * @param targetClass the entity class
     * @param ids the primary key value(s)
     * @return a future containing true if the record exists, {@code false} otherwise
     */
    public final ContinuableFuture<Boolean> asyncExists(final Class<?> targetClass, final Object... ids) {
        return asyncExists(targetClass, idsToCondition(targetClass, ids));
    }

    /**
     * Asynchronously checks if any records exist matching the specified condition.
     * 
     * @param targetClass the entity class
     * @param whereClause the WHERE condition
     * @return a future containing true if at least one record exists, {@code false} otherwise
     */
    public ContinuableFuture<Boolean> asyncExists(final Class<?> targetClass, final Condition whereClause) {
        final ImmutableList<String> keyNames = getKeyNames(targetClass);
        final SP cp = prepareQuery(targetClass, keyNames, whereClause, 1);

        return asyncExists(cp.query(), cp.parameters().toArray());
    }

    /**
     * Asynchronously counts records matching the specified condition.
     * 
     * @param targetClass the entity class
     * @param whereClause the WHERE condition
     * @return a future containing the count of matching records
     */
    @SuppressWarnings("deprecation")
    public ContinuableFuture<Long> asyncCount(final Class<?> targetClass, final Condition whereClause) {
        final SP cp = prepareQuery(targetClass, N.asList(NSC.COUNT_ALL), whereClause, 1);

        return asyncCount(cp.query(), cp.parameters().toArray());
    }

    /**
     * Asynchronously retrieves a list of entities matching the specified condition.
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param whereClause the WHERE condition
     * @return a future containing a list of matching entities
     */
    public <T> ContinuableFuture<List<T>> asyncList(final Class<T> targetClass, final Condition whereClause) {
        return asyncList(targetClass, null, whereClause);
    }

    /**
     * Asynchronously retrieves a list of entities with specific properties matching the specified condition.
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param whereClause the WHERE condition
     * @return a future containing a list of matching entities
     */
    public <T> ContinuableFuture<List<T>> asyncList(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP cp = prepareQuery(targetClass, selectPropNames, whereClause);

        return asyncList(targetClass, cp.query(), cp.parameters().toArray());
    }

    /**
     * Asynchronously queries for a Dataset of entities matching the specified condition.
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param whereClause the WHERE condition
     * @return a future containing a Dataset of matching entities
     */
    public <T> ContinuableFuture<Dataset> asyncQuery(final Class<T> targetClass, final Condition whereClause) {
        return asyncQuery(targetClass, null, whereClause);
    }

    /**
     * Asynchronously queries for a Dataset with specific properties matching the specified condition.
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param whereClause the WHERE condition
     * @return a future containing a Dataset of matching entities
     */
    public <T> ContinuableFuture<Dataset> asyncQuery(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP cp = prepareQuery(targetClass, selectPropNames, whereClause);

        return asyncQuery(targetClass, cp.query(), cp.parameters().toArray());
    }

    /**
     * Asynchronously queries for a single boolean value from the specified property.
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param propName the property name to retrieve
     * @param whereClause the WHERE condition
     * @return a future containing an OptionalBoolean with the result
     */
    public <T> ContinuableFuture<OptionalBoolean> asyncQueryForBoolean(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return asyncQueryForSingleResult(targetClass, Boolean.class, propName, whereClause).map(boolean_mapper);
    }

    /**
     * Asynchronously queries for a single character value from the specified property.
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param propName the property name to retrieve
     * @param whereClause the WHERE condition
     * @return a future containing an OptionalChar with the result
     */
    public <T> ContinuableFuture<OptionalChar> asyncQueryForChar(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return asyncQueryForSingleResult(targetClass, Character.class, propName, whereClause).map(char_mapper);
    }

    /**
     * Asynchronously queries for a single byte value from the specified property.
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param propName the property name to retrieve
     * @param whereClause the WHERE condition
     * @return a future containing an OptionalByte with the result
     */
    public <T> ContinuableFuture<OptionalByte> asyncQueryForByte(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return asyncQueryForSingleResult(targetClass, Byte.class, propName, whereClause).map(byte_mapper);
    }

    /**
     * Asynchronously queries for a single short value from the specified property.
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param propName the property name to retrieve
     * @param whereClause the WHERE condition
     * @return a future containing an OptionalShort with the result
     */
    public <T> ContinuableFuture<OptionalShort> asyncQueryForShort(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return asyncQueryForSingleResult(targetClass, Short.class, propName, whereClause).map(short_mapper);
    }

    /**
     * Asynchronously queries for a single integer value from the specified property.
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param propName the property name to retrieve
     * @param whereClause the WHERE condition
     * @return a future containing an OptionalInt with the result
     */
    public <T> ContinuableFuture<OptionalInt> asyncQueryForInt(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return asyncQueryForSingleResult(targetClass, Integer.class, propName, whereClause).map(int_mapper);
    }

    /**
     * Asynchronously queries for a single long value from the specified property.
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param propName the property name to retrieve
     * @param whereClause the WHERE condition
     * @return a future containing an OptionalLong with the result
     */
    public <T> ContinuableFuture<OptionalLong> asyncQueryForLong(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return asyncQueryForSingleResult(targetClass, Long.class, propName, whereClause).map(long_mapper);
    }

    /**
     * Asynchronously queries for a single float value from the specified property.
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param propName the property name to retrieve
     * @param whereClause the WHERE condition
     * @return a future containing an OptionalFloat with the result
     */
    public <T> ContinuableFuture<OptionalFloat> asyncQueryForFloat(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return asyncQueryForSingleResult(targetClass, Float.class, propName, whereClause).map(float_mapper);
    }

    /**
     * Asynchronously queries for a single double value from the specified property.
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param propName the property name to retrieve
     * @param whereClause the WHERE condition
     * @return a future containing an OptionalDouble with the result
     */
    public <T> ContinuableFuture<OptionalDouble> asyncQueryForDouble(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return asyncQueryForSingleResult(targetClass, Double.class, propName, whereClause).map(double_mapper);
    }

    /**
     * Asynchronously queries for a single String value from the specified property.
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param propName the property name to retrieve
     * @param whereClause the WHERE condition
     * @return a future containing a Nullable with the result
     */
    public <T> ContinuableFuture<Nullable<String>> asyncQueryForString(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return asyncQueryForSingleResult(targetClass, String.class, propName, whereClause);
    }

    /**
     * Asynchronously queries for a single Date value from the specified property.
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param propName the property name to retrieve
     * @param whereClause the WHERE condition
     * @return a future containing a Nullable with the result
     */
    public <T> ContinuableFuture<Nullable<Date>> asyncQueryForDate(final Class<T> targetClass, final String propName, final Condition whereClause) {
        return asyncQueryForSingleResult(targetClass, Date.class, propName, whereClause);
    }

    /**
     * Asynchronously queries for a single Date value of a specific type from the specified property.
     * 
     * @param <T> the entity type
     * @param <E> the specific Date type (e.g., java.sql.Timestamp)
     * @param targetClass the entity class
     * @param valueClass the specific Date class to return
     * @param propName the property name to retrieve
     * @param whereClause the WHERE condition
     * @return a future containing a Nullable with the result
     */
    public <T, E extends Date> ContinuableFuture<Nullable<E>> asyncQueryForDate(final Class<T> targetClass, final Class<E> valueClass, final String propName,
            final Condition whereClause) {
        return asyncQueryForSingleResult(targetClass, valueClass, propName, whereClause);
    }

    /**
     * Asynchronously queries for a single result value of the specified type from a property.
     * 
     * @param <T> the entity type
     * @param <V> the value type to return
     * @param targetClass the entity class
     * @param valueClass the class of the value to return
     * @param propName the property name to retrieve
     * @param whereClause the WHERE condition
     * @return a future containing a Nullable with the result
     */
    public <T, V> ContinuableFuture<Nullable<V>> asyncQueryForSingleResult(final Class<T> targetClass, final Class<V> valueClass, final String propName,
            final Condition whereClause) {
        final SP cp = prepareQuery(targetClass, List.of(propName), whereClause, 1);

        return asyncQueryForSingleResult(valueClass, cp.query(), cp.parameters().toArray());
    }

    /**
     * Asynchronously queries for a single non-null result value of the specified type from a property.
     * 
     * @param <T> the entity type
     * @param <V> the value type to return
     * @param targetClass the entity class
     * @param valueClass the class of the value to return
     * @param propName the property name to retrieve
     * @param whereClause the WHERE condition
     * @return a future containing an Optional with the result
     */
    public <T, V> ContinuableFuture<Optional<V>> asyncQueryForSingleNonNull(final Class<T> targetClass, final Class<V> valueClass, final String propName,
            final Condition whereClause) {
        final SP cp = prepareQuery(targetClass, List.of(propName), whereClause, 1);

        return asyncQueryForSingleNonNull(valueClass, cp.query(), cp.parameters().toArray());
    }

    /**
     * Asynchronously finds the first entity matching the specified condition.
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param whereClause the WHERE condition
     * @return a future containing an Optional with the first entity, or empty if none found
     */
    public <T> ContinuableFuture<Optional<T>> asyncFindFirst(final Class<T> targetClass, final Condition whereClause) {
        return asyncFindFirst(targetClass, null, whereClause);
    }

    /**
     * Asynchronously finds the first entity with specific properties matching the specified condition.
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param whereClause the WHERE condition
     * @return a future containing an Optional with the first entity, or empty if none found
     */
    public <T> ContinuableFuture<Optional<T>> asyncFindFirst(final Class<T> targetClass, final Collection<String> selectPropNames,
            final Condition whereClause) {
        final SP cp = prepareQuery(targetClass, selectPropNames, whereClause, 1);

        return asyncFindFirst(targetClass, cp.query(), cp.parameters().toArray());
    }

    /**
     * Asynchronously creates a Stream of entities matching the specified condition.
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param whereClause the WHERE condition
     * @return a future containing a Stream of matching entities
     */
    public <T> ContinuableFuture<Stream<T>> asyncStream(final Class<T> targetClass, final Condition whereClause) {
        return asyncStream(targetClass, null, whereClause);
    }

    /**
     * Asynchronously creates a Stream of entities with specific properties matching the specified condition.
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param whereClause the WHERE condition
     * @return a future containing a Stream of matching entities
     */
    public <T> ContinuableFuture<Stream<T>> asyncStream(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP cp = prepareQuery(targetClass, selectPropNames, whereClause);

        return asyncStream(targetClass, cp.query(), cp.parameters().toArray());
    }

    /**
     * Asynchronously checks if any records exist matching the given query.
     * 
     * <p>Always remember to set "LIMIT 1" in the CQL statement for better performance.</p>
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a future containing true if at least one record exists, {@code false} otherwise
     */
    public final ContinuableFuture<Boolean> asyncExists(final String query, final Object... parameters) {
        return asyncExecute(query, parameters).map(exists_mapper);
    }

    /**
     * Asynchronously counts the number of records matching the given query.
     * 
     * @deprecated Use {@link #asyncQueryForLong(String, Object...)} with {@code COUNT(*)} in the query instead.
     * 
     * @param query the CQL query to execute (should return a count)
     * @param parameters the query parameters
     * @return a future containing the count of matching records
     */
    @Deprecated
    public final ContinuableFuture<Long> asyncCount(final String query, final Object... parameters) {
        return asyncQueryForSingleResult(Long.class, query, parameters).map(long_secondMapper);
    }

    /**
     * Asynchronously queries for a single boolean value from the query result.
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a future containing an OptionalBoolean with the result
     */
    public final ContinuableFuture<OptionalBoolean> asyncQueryForBoolean(final String query, final Object... parameters) {
        return asyncQueryForSingleResult(Boolean.class, query, parameters).map(boolean_mapper);
    }

    /**
     * Asynchronously queries for a single character value from the query result.
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a future containing an OptionalChar with the result
     */
    public final ContinuableFuture<OptionalChar> asyncQueryForChar(final String query, final Object... parameters) {
        return asyncQueryForSingleResult(Character.class, query, parameters).map(char_mapper);
    }

    /**
     * Asynchronously queries for a single byte value from the query result.
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a future containing an OptionalByte with the result
     */
    public final ContinuableFuture<OptionalByte> asyncQueryForByte(final String query, final Object... parameters) {
        return asyncQueryForSingleResult(Byte.class, query, parameters).map(byte_mapper);
    }

    /**
     * Asynchronously queries for a single short value from the query result.
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a future containing an OptionalShort with the result
     */
    public final ContinuableFuture<OptionalShort> asyncQueryForShort(final String query, final Object... parameters) {
        return asyncQueryForSingleResult(Short.class, query, parameters).map(short_mapper);
    }

    /**
     * Asynchronously queries for a single integer value from the query result.
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a future containing an OptionalInt with the result
     */
    public final ContinuableFuture<OptionalInt> asyncQueryForInt(final String query, final Object... parameters) {
        return asyncQueryForSingleResult(Integer.class, query, parameters).map(int_mapper);
    }

    /**
     * Asynchronously queries for a single long value from the query result.
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a future containing an OptionalLong with the result
     */
    public final ContinuableFuture<OptionalLong> asyncQueryForLong(final String query, final Object... parameters) {
        return asyncQueryForSingleResult(Long.class, query, parameters).map(long_mapper);
    }

    /**
     * Asynchronously queries for a single float value from the query result.
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a future containing an OptionalFloat with the result
     */
    public final ContinuableFuture<OptionalFloat> asyncQueryForFloat(final String query, final Object... parameters) {
        return asyncQueryForSingleResult(Float.class, query, parameters).map(float_mapper);
    }

    /**
     * Asynchronously queries for a single double value from the query result.
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a future containing an OptionalDouble with the result
     */
    public final ContinuableFuture<OptionalDouble> asyncQueryForDouble(final String query, final Object... parameters) {
        return asyncQueryForSingleResult(Double.class, query, parameters).map(double_mapper);
    }

    /**
     * Asynchronously queries for a single String value from the query result.
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a future containing a Nullable with the result
     */
    public final ContinuableFuture<Nullable<String>> asyncQueryForString(final String query, final Object... parameters) {
        return asyncQueryForSingleResult(String.class, query, parameters);
    }

    /**
     * Asynchronously queries for a single result value of the specified type.
     * 
     * @param <T> the value type to return
     * @param valueClass the class of the value to return
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a future containing a Nullable with the result
     */
    public abstract <T> ContinuableFuture<Nullable<T>> asyncQueryForSingleResult(final Class<T> valueClass, final String query, final Object... parameters);

    /**
     * Asynchronously queries for a single non-null result value of the specified type.
     * 
     * @param <T> the value type to return
     * @param valueClass the class of the value to return
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a future containing an Optional with the result
     */
    public abstract <T> ContinuableFuture<Optional<T>> asyncQueryForSingleNonNull(final Class<T> valueClass, final String query, final Object... parameters);

    /**
     * Asynchronously finds the first row from the query result as a Map.
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a future containing an Optional with the first row as a Map
     */
    public final ContinuableFuture<Optional<Map<String, Object>>> asyncFindFirst(final String query, final Object... parameters) {
        return asyncFindFirst(Clazz.PROPS_MAP, query, parameters);
    }

    /**
     * Asynchronously finds the first row from the query result and maps it to the specified type.
     * 
     * @param <T> the target type
     * @param targetClass an entity class with getter/setter methods or Map.class
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a future containing an Optional with the first result
     */
    public abstract <T> ContinuableFuture<Optional<T>> asyncFindFirst(final Class<T> targetClass, final String query, final Object... parameters);

    /**
     * Asynchronously returns all query results as a List of Maps.
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a future containing a List of Maps representing all result rows
     */
    public final ContinuableFuture<List<Map<String, Object>>> asyncList(final String query, final Object... parameters) {
        return asyncList(Clazz.PROPS_MAP, query, parameters);
    }

    /**
     * Asynchronously returns all query results as a List of the specified type.
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * executor.asyncList(User.class, "SELECT * FROM users WHERE status = ?", "active")
     *         .thenRunAsync(users -> users.forEach(System.out::println));
     * }</pre>
     * 
     * @param <T> the target type
     * @param targetClass an entity class with getter/setter methods, Map.class, or basic single value type
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a future containing a List of results mapped to the target type
     */
    public final <T> ContinuableFuture<List<T>> asyncList(final Class<T> targetClass, final String query, final Object... parameters) {
        return asyncExecute(query, parameters).map(resultSet -> toList(targetClass, resultSet));
    }

    /**
     * Asynchronously executes a query and returns results as a Dataset with Map rows.
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a future containing a Dataset with all result rows
     */
    public final ContinuableFuture<Dataset> asyncQuery(final String query, final Object... parameters) {
        return asyncQuery(Map.class, query, parameters);
    }

    /**
     * Asynchronously executes a query and returns results as a Dataset with rows mapped to the specified type.
     * 
     * @param targetClass an entity class with getter/setter methods or Map.class
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a future containing a Dataset with all result rows
     */
    public final ContinuableFuture<Dataset> asyncQuery(final Class<?> targetClass, final String query, final Object... parameters) {
        return asyncExecute(query, parameters).map(resultSet -> extractData(targetClass, resultSet));
    }

    /**
     * Asynchronously creates a Stream of Object arrays from the query results.
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a future containing a Stream of Object arrays
     */
    public abstract ContinuableFuture<Stream<Object[]>> asyncStream(final String query, final Object... parameters);

    /**
     * Asynchronously creates a Stream of results mapped to the specified type.
     * 
     * @param <T> the target type
     * @param targetClass an entity class with getter/setter methods or Map.class
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a future containing a Stream of results
     */
    public final <T> ContinuableFuture<Stream<T>> asyncStream(final Class<T> targetClass, final String query, final Object... parameters) {
        return asyncExecute(query, parameters).map(resultSet -> Stream.of(resultSet.iterator()).map(createRowMapper(targetClass)));
    }

    /**
     * Asynchronously creates a Stream of results from a prepared statement.
     * 
     * @param <T> the target type
     * @param targetClass an entity class with getter/setter methods or Map.class
     * @param statement the prepared statement to execute
     * @return a future containing a Stream of results
     */
    public <T> ContinuableFuture<Stream<T>> asyncStream(final Class<T> targetClass, final ST statement) {
        return asyncExecute(statement).map(resultSet -> Stream.of(resultSet.iterator()).map(createRowMapper(targetClass)));
    }

    /**
     * Asynchronously executes a CQL query without parameters.
     * 
     * @param query the CQL query to execute
     * @return a future containing the result set
     */
    public abstract ContinuableFuture<RS> asyncExecute(final String query);

    /**
     * Asynchronously executes a CQL query with positional parameters.
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a future containing the result set
     */
    public abstract ContinuableFuture<RS> asyncExecute(final String query, final Object... parameters);

    /**
     * Asynchronously executes a CQL query with named parameters.
     * 
     * @param query the CQL query with named parameters
     * @param parameters the query parameters
     * @return a future containing the result set
     */
    public abstract ContinuableFuture<RS> asyncExecute(String query, Map<String, Object> parameters);

    /**
     * Asynchronously executes a prepared statement.
     * 
     * @param statement the prepared statement to execute
     * @return a future containing the result set
     */
    public abstract ContinuableFuture<RS> asyncExecute(final ST statement);

    /**
     * Asynchronously executes a prepared query with parameters.
     * 
     * @param cp the prepared query with parameters
     * @return a future containing the result set
     */
    protected ContinuableFuture<RS> asyncExecute(final SP cp) {
        return asyncExecute(cp.query(), cp.parameters().toArray());
    }

    /**
     * Executes a prepared query with parameters.
     * 
     * @param cp the prepared query with parameters
     * @return the result set
     */
    protected RS execute(final SP cp) {
        return execute(cp.query(), cp.parameters().toArray());
    }

    /**
     * Prepares an INSERT statement for the given entity.
     * 
     * @param entity the entity to insert
     * @return the prepared statement with parameters
     */
    protected SP prepareInsert(final Object entity) {
        final Class<?> targetClass = entity.getClass();

        switch (namingPolicy) {
            case SNAKE_CASE:
                return NSC.insert(entity).into(targetClass).build();

            case SCREAMING_SNAKE_CASE:
                return NAC.insert(entity).into(targetClass).build();

            case CAMEL_CASE:
                return NLC.insert(entity).into(targetClass).build();

            default:
                throw new RuntimeException("Unsupported naming policy: " + namingPolicy);
        }
    }

    /**
     * Prepares an INSERT statement for the given properties.
     * 
     * @param targetClass the entity class
     * @param props map of property names to values
     * @return the prepared statement with parameters
     */
    protected SP prepareInsert(final Class<?> targetClass, final Map<String, Object> props) {
        switch (namingPolicy) {
            case SNAKE_CASE:
                return NSC.insert(props).into(targetClass).build();

            case SCREAMING_SNAKE_CASE:
                return NAC.insert(props).into(targetClass).build();

            case CAMEL_CASE:
                return NLC.insert(props).into(targetClass).build();

            default:
                throw new RuntimeException("Unsupported naming policy: " + namingPolicy);
        }
    }

    /**
     * Prepares a batch statement of the specified type.
     * 
     * @param type the batch type
     * @return the prepared batch statement
     */
    protected abstract ST prepareBatchStatement(final BT type);

    /**
     * Prepares a batch INSERT statement for multiple entities.
     * 
     * @param entities collection of entities to insert
     * @param type the batch type
     * @return the prepared batch statement
     */
    protected abstract ST prepareBatchInsertStatement(final Collection<?> entities, final BT type);

    /**
     * Prepares a batch INSERT statement for multiple property maps.
     * 
     * @param targetClass the entity class
     * @param propsList collection of property maps
     * @param type the batch type
     * @return the prepared batch statement
     */
    protected abstract ST prepareBatchInsertStatement(final Class<?> targetClass, final Collection<? extends Map<String, Object>> propsList, final BT type);

    /**
     * Prepares an UPDATE statement for the given entity.
     * 
     * @param entity the entity containing updated values
     * @param propNamesToUpdate collection of property names to update
     * @return the prepared statement with parameters
     */
    protected SP prepareUpdate(final Object entity, final Collection<String> propNamesToUpdate) {
        N.checkArgument(N.notEmpty(propNamesToUpdate), "'propNamesToUpdate' can't be null or empty.");

        final Class<?> targetClass = entity.getClass();
        final BeanInfo entityInfo = ParserUtil.getBeanInfo(targetClass);

        final Set<String> primaryKeyNames = getKeyNameSet(entity.getClass());
        final List<Condition> conds = new ArrayList<>(primaryKeyNames.size());

        for (final String keyName : primaryKeyNames) {
            conds.add(Filters.eq(keyName, entityInfo.getPropValue(entity, keyName)));
        }

        switch (namingPolicy) {
            case SNAKE_CASE:
                return NSC.update(targetClass).set(Beans.beanToMap(entity, propNamesToUpdate)).where(Filters.and(conds)).build();

            case SCREAMING_SNAKE_CASE:
                return NAC.update(targetClass).set(Beans.beanToMap(entity, propNamesToUpdate)).where(Filters.and(conds)).build();

            case CAMEL_CASE:
                return NLC.update(targetClass).set(Beans.beanToMap(entity, propNamesToUpdate)).where(Filters.and(conds)).build();

            default:
                throw new RuntimeException("Unsupported naming policy: " + namingPolicy);
        }
    }

    /**
     * Prepares an UPDATE statement with a WHERE clause.
     * 
     * @param targetClass the entity class
     * @param props map of property names to new values
     * @param whereClause the WHERE condition
     * @return the prepared statement with parameters
     */
    protected SP prepareUpdate(final Class<?> targetClass, final Map<String, Object> props, final Condition whereClause) {
        final boolean isNonNullCond = whereClause != null;

        switch (namingPolicy) {
            case SNAKE_CASE:
                return NSC.update(targetClass).set(props).appendIf(isNonNullCond, whereClause).build();

            case SCREAMING_SNAKE_CASE:
                return NAC.update(targetClass).set(props).appendIf(isNonNullCond, whereClause).build();

            case CAMEL_CASE:
                return NLC.update(targetClass).set(props).appendIf(isNonNullCond, whereClause).build();

            default:
                throw new RuntimeException("Unsupported naming policy: " + namingPolicy);
        }
    }

    /**
     * Prepares a batch UPDATE statement for multiple entities.
     * 
     * @param entities collection of entities to update
     * @param propNamesToUpdate collection of property names to update
     * @param type the batch type
     * @return the prepared batch statement
     */
    protected abstract ST prepareBatchUpdateStatement(final Collection<?> entities, final Collection<String> propNamesToUpdate, final BT type);

    /**
     * Prepares a batch UPDATE statement for multiple property maps.
     * 
     * @param targetClass the entity class
     * @param propsList collection of property maps
     * @param type the batch type
     * @return the prepared batch statement
     */
    protected abstract ST prepareBatchUpdateStatement(final Class<?> targetClass, final Collection<? extends Map<String, Object>> propsList, final BT type);

    /**
     * Prepares a batch UPDATE statement with parameterized queries.
     * 
     * @param query the update query template
     * @param parametersList collection of parameter arrays
     * @param type the batch type
     * @return the prepared batch statement
     */
    protected abstract ST prepareBatchUpdateStatement(final String query, final Collection<?> parametersList, final BT type);

    /**
     * Prepares a DELETE statement with a WHERE clause.
     * 
     * @param targetClass the entity class
     * @param propNamesToDelete collection of property names to delete, or null to delete entire records
     * @param whereClause the WHERE condition
     * @return the prepared statement with parameters
     */
    protected SP prepareDelete(final Class<?> targetClass, final Collection<String> propNamesToDelete, final Condition whereClause) {
        final boolean isNonNullCond = whereClause != null;

        switch (namingPolicy) {
            case SNAKE_CASE:
                if (N.isEmpty(propNamesToDelete)) {
                    return NSC.deleteFrom(targetClass).appendIf(isNonNullCond, whereClause).build();
                } else {
                    return NSC.delete(propNamesToDelete).from(targetClass).appendIf(isNonNullCond, whereClause).build();
                }

            case SCREAMING_SNAKE_CASE:
                if (N.isEmpty(propNamesToDelete)) {
                    return NAC.deleteFrom(targetClass).appendIf(isNonNullCond, whereClause).build();
                } else {
                    return NAC.delete(propNamesToDelete).from(targetClass).appendIf(isNonNullCond, whereClause).build();
                }

            case CAMEL_CASE:
                if (N.isEmpty(propNamesToDelete)) {
                    return NLC.deleteFrom(targetClass).appendIf(isNonNullCond, whereClause).build();
                } else {
                    return NLC.delete(propNamesToDelete).from(targetClass).appendIf(isNonNullCond, whereClause).build();
                }

            default:
                throw new RuntimeException("Unsupported naming policy: " + namingPolicy);
        }
    }

    /**
     * Prepares a SELECT query for the specified entity class and conditions.
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param whereClause the WHERE condition
     * @return the prepared statement with parameters
     */
    protected <T> SP prepareQuery(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        return prepareQuery(targetClass, selectPropNames, whereClause, 0);
    }

    /**
     * Prepares a SELECT query statement with the specified parameters.
     * 
     * <p>This method constructs a CQL SELECT query based on the target entity class,
     * optional property selection, WHERE condition, and result limit. The query is
     * built according to the configured naming policy for column name transformation.</p>
     * 
     * <p>The method supports different naming policies:</p>
     * <ul>
     * <li><strong>SNAKE_CASE:</strong> camelCase → snake_case</li>
     * <li><strong>SCREAMING_SNAKE_CASE:</strong> camelCase → SCREAMING_SNAKE_CASE</li>
     * <li><strong>CAMEL_CASE:</strong> preserves original camelCase</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Select specific properties with condition and limit
     * SP statement = prepareQuery(User.class, 
     *     Arrays.asList("name", "email"), 
     *     Filters.eq("status", "active"), 
     *     10);
     * 
     * // Select all properties without condition
     * SP statement = prepareQuery(User.class, null, null, 0);
     * }</pre>
     * 
     * @param <T> the target entity type
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param whereClause the WHERE condition
     * @param count the maximum number of results to return (0 for no limit)
     * @return an SP (Statement/Parameters) pair ready for execution
     */
    protected <T> SP prepareQuery(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause, final int count) {
        final boolean isNonNullCond = whereClause != null;
        CqlBuilder cqlBuilder = null;

        switch (namingPolicy) {
            case SNAKE_CASE:
                if (N.isEmpty(selectPropNames)) {
                    cqlBuilder = NSC.selectFrom(targetClass).appendIf(isNonNullCond, whereClause);
                } else {
                    cqlBuilder = NSC.select(selectPropNames).from(targetClass).appendIf(isNonNullCond, whereClause);
                }

                break;

            case SCREAMING_SNAKE_CASE:
                if (N.isEmpty(selectPropNames)) {
                    cqlBuilder = NAC.selectFrom(targetClass).appendIf(isNonNullCond, whereClause);
                } else {
                    cqlBuilder = NAC.select(selectPropNames).from(targetClass).appendIf(isNonNullCond, whereClause);
                }

                break;

            case CAMEL_CASE:
                if (N.isEmpty(selectPropNames)) {
                    cqlBuilder = NLC.selectFrom(targetClass).appendIf(isNonNullCond, whereClause);
                } else {
                    cqlBuilder = NLC.select(selectPropNames).from(targetClass).appendIf(isNonNullCond, whereClause);
                }

                break;

            default:
                throw new RuntimeException("Unsupported naming policy: " + namingPolicy);
        }

        if (count > 0) {
            cqlBuilder.limit(count);
        }

        return cqlBuilder.build();
    }

    /**
     * Prepares a statement from a CQL query string.
     * 
     * @param query the CQL query
     * @return the prepared statement
     */
    protected abstract ST prepareStatement(final String query);

    /**
     * Prepares a statement from a CQL query with parameters.
     * 
     * @param query the CQL query
     * @param parameters the query parameters
     * @return the prepared statement
     */
    protected abstract ST prepareStatement(final String query, final Object... parameters);

    /**
     * Prepares a reusable prepared statement from a CQL query.
     * 
     * @param query the CQL query
     * @return the prepared statement
     */
    protected abstract PS prepare(final String query);

    /**
     * Binds parameters to a prepared statement.
     * 
     * @param preStmt the prepared statement
     * @param parameters the query parameters
     * @return the bound statement ready for execution
     */
    protected abstract ST bind(final PS preStmt, final Object... parameters);

    /**
     * Parses a CQL query string into a ParsedCql object with caching support.
     * 
     * <p>This method first attempts to retrieve a pre-configured CQL statement from
     * the CQL mapper (if available), falling back to parsing the raw CQL string if
     * not found. This provides a two-tier approach to CQL management:</p>
     * <ol>
     * <li><strong>Mapped CQL:</strong> Pre-configured statements with metadata</li>
     * <li><strong>Ad-hoc CQL:</strong> Dynamic parsing of arbitrary CQL strings</li>
     * </ol>
     * 
     * <p>The parsing process includes:</p>
     * <ul>
     * <li>Parameter detection and normalization (positional, named, MyBatis-style)</li>
     * <li>Query optimization and validation</li>
     * <li>Caching for improved performance on repeated queries</li>
     * <li>Metadata extraction for statement configuration</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Parse mapped CQL (retrieved from CqlMapper if available)
     * ParsedCql parsed1 = parseCql("getUserById");
     * 
     * // Parse ad-hoc CQL string
     * ParsedCql parsed2 = parseCql("SELECT * FROM users WHERE status = ?");
     * 
     * // Access parsing results
     * String parameterizedCql = parsed2.getParameterizedCql();
     * int paramCount = parsed2.parameterCount();
     * Map<String, String> attributes = parsed2.getAttributes();
     * }</pre>
     * 
     * @param cql the CQL query string to parse (may be a mapper key or raw CQL)
     * @return a ParsedCql object containing the parsed query and metadata
     * @see CqlMapper#get(String)
     * @see ParsedCql#parse(String, Map)
     */
    protected ParsedCql parseCql(final String cql) {
        ParsedCql parsedCql = null;

        if (cqlMapper != null) {
            parsedCql = cqlMapper.get(cql);
        }

        if (parsedCql == null) {
            parsedCql = ParsedCql.parse(cql, null);
        }

        return parsedCql;
    }

    /**
     * Converts a result set to a List of the specified type.
     * 
     * @param <T> the target type
     * @param targetClass the entity class
     * @param execute the result set to convert
     * @return a List of mapped objects
     */
    protected abstract <T> List<T> toList(Class<T> targetClass, RS execute);

    /**
     * Extracts data from a result set into a Dataset.
     * 
     * @param targetClass the entity class
     * @param execute the result set to extract from
     * @return a Dataset containing the extracted data
     */
    protected abstract Dataset extractData(Class<?> targetClass, RS execute);

    /**
     * Creates a row mapper function for the specified target class.
     * 
     * @param <T> the target type
     * @param targetClass the entity class
     * @return a function that maps result rows to the target type
     */
    protected abstract <T> Function<RW, T> createRowMapper(Class<T> targetClass);
}
