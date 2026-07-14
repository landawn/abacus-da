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

import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.NAC;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.NLC;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.NSC;

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
import com.landawn.abacus.exception.DuplicateResultException;
import com.landawn.abacus.query.AbstractQueryBuilder.SP;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.QueryUtil;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.util.Beans;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.Clazz;
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
 * Driver-agnostic base class shared by the synchronous {@link CassandraExecutor} (DataStax 4.x driver)
 * and the legacy {@code com.landawn.abacus.da.cassandra.v3.CassandraExecutor} (DataStax 3.x driver),
 * as well as their {@code Async*} counterparts.
 *
 * <p>The base implements the parts of the contract that do not depend on a particular driver release:
 * entity-to-CQL mapping, {@link CqlBuilder} integration, primary-key extraction, {@link Condition}
 * translation into {@code WHERE} clauses, and the common {@code queryForXxx}/{@code findFirst}/
 * {@code list}/{@code stream}/{@code count}/{@code exists}/{@code insert}/{@code update}/{@code delete}
 * façades. The concrete subclasses are responsible for the driver-specific pieces — opening sessions,
 * executing/preparing/binding statements, registering codecs, and shaping the result set.</p>
 *
 * <h2>Contract Honored By Subclasses</h2>
 * <ul>
 * <li><strong>Preferred statement cache:</strong> subclasses are expected to cache the
 *     {@code PreparedStatement} returned by their {@link #prepare(String)} implementation so that
 *     repeated calls with the same CQL string reuse a single prepared statement.</li>
 * <li><strong>ParsedCql cache:</strong> {@link #parseCql(String)} consults the optional
 *     {@link CqlMapper} first (allowing pre-registered, named CQL fragments) and falls back to
 *     {@link ParsedCql#parse(String)} for ad-hoc CQL. Subclasses should reuse the
 *     resulting {@link ParsedCql} where possible to avoid re-parsing.</li>
 * <li><strong>Codec registry:</strong> subclasses install the driver's codec registry so that
 *     entity properties and bound parameters round-trip through the configured codecs (including any
 *     custom user codecs).</li>
 * <li><strong>Entity-to-CQL mapping:</strong> entity properties are mapped to columns through the
 *     configured {@link NamingPolicy} (see below). Primary-key columns are discovered through
 *     {@code @Id} annotations or {@link #registerKeys(Class, Collection)}.</li>
 * </ul>
 *
 * <h2>Generic Type Parameters</h2>
 * <ul>
 * <li><strong>{@code RW}:</strong> driver row type (for example
 *     {@code com.datastax.oss.driver.api.core.cql.Row}).</li>
 * <li><strong>{@code RS}:</strong> driver result-set type, required to be {@code Iterable<RW>}
 *     (for example {@code com.datastax.oss.driver.api.core.cql.ResultSet}).</li>
 * <li><strong>{@code ST}:</strong> driver statement type (for example
 *     {@code com.datastax.oss.driver.api.core.cql.Statement}).</li>
 * <li><strong>{@code PS}:</strong> driver prepared-statement type.</li>
 * <li><strong>{@code BT}:</strong> driver batch-type enum (for example
 *     {@code com.datastax.oss.driver.api.core.cql.BatchType}).</li>
 * </ul>
 *
 * <h2>Parameter Binding</h2>
 * <ul>
 * <li><strong>Positional parameters:</strong> {@code SELECT * FROM users WHERE id = ?}</li>
 * <li><strong>Named parameters:</strong> {@code SELECT * FROM users WHERE id = :userId}</li>
 * <li><strong>Entity binding:</strong> a single bean argument supplies values for named placeholders
 *     by property name.</li>
 * <li><strong>Map binding:</strong> a single {@code Map<String, Object>} supplies values for named
 *     placeholders by key.</li>
 * <li><strong>Array/Collection binding:</strong> a single array or collection is unpacked
 *     positionally.</li>
 * </ul>
 *
 * <h2>Naming Policy</h2>
 * <p>{@link NamingPolicy} controls the property-to-column mapping used by the embedded
 * {@link CqlBuilder} variants (NSC for snake-case, NAC for screaming-snake-case, NLC for camel-case).
 * The default applied by the constructor when {@code null} is passed in is snake-case:</p>
 * <ul>
 * <li><strong>SNAKE_CASE:</strong> {@code firstName} maps to {@code first_name}</li>
 * <li><strong>SCREAMING_SNAKE_CASE:</strong> {@code firstName} maps to {@code FIRST_NAME}</li>
 * <li><strong>CAMEL_CASE:</strong> {@code firstName} is kept as {@code firstName}</li>
 * </ul>
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * // Basic CRUD operations
 * executor.insert(user);                                  // INSERT based on entity
 * Optional<User> found = executor.get(User.class, id);    // SELECT by primary key
 * executor.update(user);                                  // UPDATE based on entity
 * executor.delete(User.class, id);                        // DELETE by primary key
 *
 * // gett() is the nullable variant — returns null when not found,
 * // throws DuplicateResultException only if multiple rows match.
 * User user = executor.gett(User.class, id);
 *
 * // Batch operations
 * executor.batchInsert(Arrays.asList(user1, user2, user3), BatchType.LOGGED);
 *
 * // Query operations
 * List<User> activeUsers = executor.list(User.class,
 *     "SELECT * FROM users WHERE status = ?", "active");
 * }</pre>
 *
 * <h2>Lifecycle</h2>
 * <p>The concrete subclass exposes a public {@code close()} to release its underlying
 * driver session; the base itself holds no closeable resources.</p>
 *
 * <h2>Thread Safety</h2>
 * <p>Implementations are expected to be thread-safe. The static caches maintained by the base
 * (e.g. {@link #entityKeyNamesMap}) are concurrent. Mutability of {@link Condition} or entity
 * arguments passed in by callers is the caller's responsibility.</p>
 *
 * @param <RW> driver row type
 * @param <RS> driver result-set type ({@code Iterable<RW>})
 * @param <ST> driver statement type
 * @param <PS> driver prepared-statement type
 * @param <BT> driver batch-type enum
 *
 * @see CassandraExecutor
 * @see com.landawn.abacus.da.cassandra.v3.CassandraExecutor
 * @see CqlBuilder
 * @see CqlMapper
 * @see ParsedCql
 * @see com.landawn.abacus.query.Filters
 */
public abstract class CassandraExecutorBase<RW, RS extends Iterable<RW>, ST, PS, BT> {

    protected static final String NULL_STR = "NULL";

    protected static final ImmutableList<String> COUNT_SELECT_PROP_NAMES = ImmutableList.of(CqlBuilder.COUNT_ALL);

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

    /**
     * Constructs the base with an optional named-CQL mapper and a property-to-column naming policy.
     *
     * <p>Subclasses should invoke this constructor from their own constructors with the
     * caller-supplied {@link CqlMapper} (or {@code null}) and {@link NamingPolicy} (or {@code null}
     * to accept the snake-case default).</p>
     *
     * @param cqlMapper optional registry of named CQL fragments consulted first by
     *        {@link #parseCql(String)}; pass {@code null} to disable named-CQL lookup
     * @param namingPolicy the policy applied when mapping entity property names to CQL column names;
     *        one of {@link NamingPolicy#SNAKE_CASE}, {@link NamingPolicy#SCREAMING_SNAKE_CASE} or
     *        {@link NamingPolicy#CAMEL_CASE}; when {@code null}, snake-case is used
     * @throws IllegalArgumentException if {@code namingPolicy} is non-null but not one of the three
     *         supported policies
     */
    protected CassandraExecutorBase(final CqlMapper cqlMapper, final NamingPolicy namingPolicy) {
        // Fail fast: the prepare* methods support only these three policies; without this check an
        // unsupported policy would surface as an IllegalStateException on the first Condition-based operation.
        if (namingPolicy != null && namingPolicy != NamingPolicy.SNAKE_CASE && namingPolicy != NamingPolicy.SCREAMING_SNAKE_CASE
                && namingPolicy != NamingPolicy.CAMEL_CASE) {
            throw new IllegalArgumentException(
                    "Unsupported naming policy: " + namingPolicy + ". Only SNAKE_CASE, SCREAMING_SNAKE_CASE and CAMEL_CASE are supported");
        }

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
     * // post-state: getKeyNames(User.class) now returns ["userId", "tenantId"]
     *
     * CassandraExecutorBase.registerKeys(User.class, Arrays.asList("userId"));
     * // post-state: a single-column key; getKeyNames(User.class) returns ["userId"]
     *
     * CassandraExecutorBase.registerKeys(User.class, null);              // throws IllegalArgumentException
     * CassandraExecutorBase.registerKeys(User.class, new ArrayList<>()); // throws IllegalArgumentException (empty)
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
     * @return an immutable list of property names that form the primary key (empty if neither
     *         registration nor {@code @Id} annotations are present)
     */
    protected static ImmutableList<String> getKeyNames(final Class<?> entityClass) {
        Tuple2<ImmutableList<String>, ImmutableSet<String>> tp = entityKeyNamesMap.get(entityClass);

        if (tp == null) {
            final List<String> idPropNames = QueryUtil.idPropNames(entityClass);
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
     * @return an immutable set of property names that form the primary key (empty if neither
     *         registration nor {@code @Id} annotations are present)
     * @see #getKeyNames(Class)
     */
    protected static Set<String> getKeyNameSet(final Class<?> entityClass) {
        Tuple2<ImmutableList<String>, ImmutableSet<String>> tp = entityKeyNamesMap.get(entityClass);

        if (tp == null) {
            final List<String> idPropNames = QueryUtil.idPropNames(entityClass);
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
     * @param targetClass the entity class whose primary-key column names are used
     * @param ids the ID values in the same order as the key fields
     * @return a Condition representing the primary key equality check
     * @throws IllegalArgumentException if {@code ids} is null or empty, or if its length does not
     *         match the number of registered/annotated key columns on {@code targetClass}
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
     * @throws IllegalArgumentException if no key names are defined for the entity class, or a key property value is null or empty
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
     * <p>This method generates a condition that matches any of the entities in the collection
     * by their primary key values. Only single-key entities are supported: it builds an IN
     * condition over the key values. Composite-key entities are rejected with an
     * {@link IllegalArgumentException}, because a batch of composite keys cannot be expressed
     * in a single Cassandra {@code WHERE} clause.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Single key entities
     * List<User> users = Arrays.asList(user1, user2, user3);
     * Condition cond = entityToCondition(User.class, users);
     * // Result: WHERE user_id IN (?, ?, ?)
     *
     * // Composite key entities are not supported
     * List<UserSession> sessions = Arrays.asList(session1, session2);
     * entityToCondition(UserSession.class, sessions); // throws IllegalArgumentException
     * }</pre>
     *
     * @param entityClass the entity class
     * @param entities the collection of entities whose keys should be matched
     * @return a Condition that matches any entity in the collection by primary key
     * @throws IllegalArgumentException if {@code entityClass} is null, if {@code entities} is null or empty,
     *         if the entity declares no key names, if it has a composite (multi-column) primary key, if any
     *         entity in the collection is null, or if any entity has no value for the key property
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

            for (final Object key : keys) {
                if (key == null || (key instanceof CharSequence && Strings.isEmpty((CharSequence) key))) {
                    throw new IllegalArgumentException("No property value specified in entity for key name: " + keyName);
                }
            }

            cond = Filters.in(keyName, keys);
        } else {
            throw new IllegalArgumentException("Batch operations with composite primary keys are not supported by a single Cassandra WHERE clause");
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
     * <p>For the nullable counterpart that returns the entity directly (or {@code null} when no row
     * matches) instead of an {@link Optional}, use {@link #gett(Class, Object...)}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // single-key entity
     * Optional<User> user = executor.get(User.class, "user123"); // returns present Optional<User> when one row matches
     * user.ifPresent(u -> System.out.println("Found: " + u.getName()));
     *
     * Optional<User> missing = executor.get(User.class, "nope");  // returns Optional.empty() when no row matches
     *
     * executor.get(User.class);              // throws IllegalArgumentException (ids is empty)
     * executor.get(User.class, "a", "b");    // throws IllegalArgumentException (id count != single key column)
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param ids the primary key values
     * @return an Optional containing the entity if found, otherwise empty
     * @throws IllegalArgumentException if ids is null or empty
     * @throws DuplicateResultException if more than one entity is found
     * @see #gett(Class, Object...)
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
     *     Arrays.asList("name", "email"), "user123"); // returns present Optional with only name/email populated
     *
     * Optional<User> all = executor.get(User.class,
     *     (Collection<String>) null, "user123");      // returns present Optional with all properties populated
     *
     * executor.get(User.class, Arrays.asList("name"));            // throws IllegalArgumentException (ids is empty)
     * executor.get(User.class, Arrays.asList("name"), "a", "b");  // throws IllegalArgumentException (id count mismatch)
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param ids the primary key values
     * @return an Optional containing the entity if found, otherwise empty
     * @throws IllegalArgumentException if ids is null or empty
     * @throws DuplicateResultException if more than one entity is found
     * @see #gett(Class, Collection, Object...)
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
     * Optional<User> user = executor.get(User.class, cond);            // returns present Optional when a row matches
     *
     * Optional<User> none = executor.get(User.class,
     *     Filters.eq("email", "missing@example.com"));                 // returns Optional.empty() when no row matches
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param whereClause the WHERE condition
     * @return an Optional containing the entity if found, otherwise empty
     * @throws DuplicateResultException if more than one entity is found
     * @see #gett(Class, Condition)
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
     *     Arrays.asList("name", "email"), cond);                       // returns present Optional (name/email only) when a row matches
     *
     * Optional<User> none = executor.get(User.class,
     *     Arrays.asList("name", "email"), Filters.eq("status", "x"));  // returns Optional.empty() when no row matches
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param whereClause the WHERE condition
     * @return an Optional containing the entity if found, otherwise empty
     * @throws DuplicateResultException if more than one entity is found
     * @see #gett(Class, Collection, Condition)
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
     * User user = executor.gett(User.class, "user123"); // returns the mapped User, or null when no row matches
     * if (user != null) {
     *     System.out.println("Found: " + user.getName());
     * }
     *
     * executor.gett(User.class);            // throws IllegalArgumentException (ids is empty)
     * executor.gett(User.class, "a", "b");  // throws IllegalArgumentException (id count != single key column)
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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = executor.gett(User.class,
     *     Arrays.asList("name", "email"), "user123"); // returns User with only name/email set, or null when no row matches
     *
     * User all = executor.gett(User.class,
     *     (Collection<String>) null, "user123");       // returns User with all properties, or null when no row matches
     *
     * executor.gett(User.class, Arrays.asList("name"));           // throws IllegalArgumentException (ids is empty)
     * executor.gett(User.class, Arrays.asList("name"), "a", "b"); // throws IllegalArgumentException (id count mismatch)
     * }</pre>
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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = executor.gett(User.class,
     *     Filters.eq("email", "user@example.com")); // returns the mapped User, or null when no row matches
     *
     * User none = executor.gett(User.class,
     *     Filters.eq("email", "missing@example.com")); // returns null when no row matches
     * }</pre>
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
     * the actual database query and entity mapping. It is the terminal {@code gett} overload all
     * other {@code gett}/{@code get} façades delegate to.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = executor.gett(User.class,
     *     Arrays.asList("name", "email"),
     *     Filters.eq("user_id", "user123")); // returns User with name/email populated, or null when no row matches
     *
     * User all = executor.gett(User.class, null,
     *     Filters.eq("user_id", "user123"));  // returns User with all properties, or null when no row matches
     * }</pre>
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
     * ResultSet result = executor.insert(user); // returns the driver ResultSet for the INSERT
     *
     * executor.insert((Object) null);           // throws IllegalArgumentException (entity is null)
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
     * ResultSet result = executor.insert(User.class, props); // returns the driver ResultSet for the INSERT
     *
     * executor.insert(User.class, new HashMap<>());          // throws IllegalArgumentException (props is empty)
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
     * of the same type. The base delegates to the subclass's batch-statement builder, and the
     * shipped executors validate the input there: a {@code null} or empty collection is rejected
     * with an {@code IllegalArgumentException}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<User> users = Arrays.asList(user1, user2, user3);
     * ResultSet result = executor.batchInsert(users, BatchType.LOGGED); // returns the driver ResultSet for the batch
     *
     * executor.batchInsert(new ArrayList<>(), BatchType.LOGGED); // throws IllegalArgumentException (entities is empty)
     * }</pre>
     *
     * @param entities the collection of entities to insert
     * @param type the batch type
     * @return the result set from the batch INSERT operation
     * @throws IllegalArgumentException if {@code entities} is {@code null} or empty (enforced by the
     *         shipped executors' batch-statement builders)
     */
    public RS batchInsert(final Collection<?> entities, final BT type) {
        final ST stmt = prepareBatchInsertStatement(entities, type);

        return execute(stmt);
    }

    /**
     * Performs a batch insert of multiple records using property maps.
     *
     * <p>This method creates a batch INSERT statement for all provided property maps
     * and executes them together for better performance. As with {@link #batchInsert(Collection, Object)},
     * the base delegates to the subclass's batch-statement builder, and the shipped executors
     * reject a {@code null} or empty {@code propsList} with an {@code IllegalArgumentException}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Map<String, Object>> propsList = new ArrayList<>();
     * propsList.add(Map.of("userId", "user1", "name", "Alice"));
     * propsList.add(Map.of("userId", "user2", "name", "Bob"));
     *
     * ResultSet result = executor.batchInsert(User.class, propsList, BatchType.LOGGED); // returns the driver ResultSet for the batch
     *
     * executor.batchInsert(User.class, new ArrayList<>(), BatchType.LOGGED); // throws IllegalArgumentException (propsList is empty)
     * }</pre>
     *
     * @param targetClass the entity class
     * @param propsList the collection of property maps to insert
     * @param type the batch type
     * @return the result set from the batch INSERT operation
     * @throws IllegalArgumentException if {@code propsList} is {@code null} or empty (enforced by the
     *         shipped executors' batch-statement builders)
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
     * ResultSet result = executor.update(user); // returns the driver ResultSet for the UPDATE (all non-key props)
     *
     * executor.update((Object) null);           // throws IllegalArgumentException (entity is null)
     * }</pre>
     *
     * @param entity the entity to update
     * @return the result set from the UPDATE operation
     * @throws IllegalArgumentException if entity is null
     */
    public RS update(final Object entity) {
        N.checkArgNotNull(entity, "entity");

        final Class<?> entityClass = entity.getClass();
        final Set<String> keyNameSet = getKeyNameSet(entityClass);
        final Collection<String> updatePropNames = QueryUtil.updatePropNames(entityClass, keyNameSet);

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
     *     Arrays.asList("email", "lastModified")); // returns the driver ResultSet (only email/lastModified updated)
     *
     * executor.update(user, new ArrayList<>());    // throws IllegalArgumentException (propNamesToUpdate is empty)
     * }</pre>
     *
     * @param entity the entity containing the values to update
     * @param propNamesToUpdate the property names to update
     * @return the result set from the UPDATE operation
     * @throws IllegalArgumentException if entity is null, or if propNamesToUpdate is null or empty
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
     * ResultSet result = executor.update(User.class, props, where); // returns the driver ResultSet for the UPDATE
     *
     * executor.update(User.class, new HashMap<>(), where);          // throws IllegalArgumentException (props is empty)
     * }</pre>
     *
     * @param targetClass the entity class
     * @param props the properties to update
     * @param whereClause the WHERE condition
     * @return the result set from the UPDATE operation
     * @throws IllegalArgumentException if targetClass is null, if props is null or empty, or if whereClause is null
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
     * ResultSet result = executor.update(query, "inactive", thirtyDaysAgo); // returns the driver ResultSet for the UPDATE
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
     * ResultSet result = executor.batchUpdate(users, BatchType.LOGGED); // returns the driver ResultSet for the batch
     *
     * executor.batchUpdate(new ArrayList<>(), BatchType.LOGGED);        // throws IllegalArgumentException (entities is empty)
     * }</pre>
     *
     * @param entities the collection of entities to update
     * @param type the batch type
     * @return the result set from the batch UPDATE operation
     * @throws IllegalArgumentException if entities is null or empty, or if its first element is null
     */
    public RS batchUpdate(final Collection<?> entities, final BT type) {
        N.checkArgument(N.notEmpty(entities), "'entities' can't be null or empty.");

        final Object firstEntity = N.firstOrNullIfEmpty(entities);
        N.checkArgNotNull(firstEntity, "The first entity in the collection can't be null.");
        final Class<?> entityClass = firstEntity.getClass();
        final Set<String> keyNameSet = getKeyNameSet(entityClass);
        final Collection<String> updatePropNames = QueryUtil.updatePropNames(entityClass, keyNameSet);

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
     *     Arrays.asList("status", "lastModified"), BatchType.LOGGED); // returns the driver ResultSet for the batch
     *
     * executor.batchUpdate(new ArrayList<>(),
     *     Arrays.asList("status"), BatchType.LOGGED);                   // throws IllegalArgumentException (entities is empty)
     * executor.batchUpdate(users, new ArrayList<>(), BatchType.LOGGED); // throws IllegalArgumentException (propNamesToUpdate is empty)
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
     * key fields to identify the record and the fields to update. Like the other
     * {@code (Class, propsList, BT)} batch overloads, the base delegates to the subclass's
     * batch-statement builder, and the shipped executors reject a {@code null} or empty
     * {@code propsList} with an {@code IllegalArgumentException}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Map<String, Object>> propsList = new ArrayList<>();
     * propsList.add(Map.of("userId", "user1", "status", "active"));
     * propsList.add(Map.of("userId", "user2", "status", "inactive"));
     *
     * ResultSet result = executor.batchUpdate(User.class, propsList, BatchType.LOGGED); // returns the driver ResultSet for the batch
     *
     * executor.batchUpdate(User.class, new ArrayList<>(), BatchType.LOGGED); // throws IllegalArgumentException (propsList is empty)
     * }</pre>
     *
     * @param targetClass the entity class
     * @param propsList the collection of property maps to update
     * @param type the batch type
     * @return the result set from the batch UPDATE operation
     * @throws IllegalArgumentException if {@code propsList} is {@code null} or empty (enforced by the
     *         shipped executors' batch-statement builders)
     */
    public RS batchUpdate(final Class<?> targetClass, final Collection<? extends Map<String, Object>> propsList, final BT type) {
        final ST stmt = prepareBatchUpdateStatement(targetClass, propsList, type);

        return execute(stmt);
    }

    /**
     * Performs a batch update with a custom query and multiple parameter sets.
     *
     * <p>This method executes the same UPDATE query multiple times with different
     * parameter sets in a single batch for better performance. The base delegates to the
     * subclass's batch-statement builder, and the shipped executors reject a {@code null} or
     * empty {@code parametersList} with an {@code IllegalArgumentException}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String query = "UPDATE users SET status = ? WHERE user_id = ?";
     * List<Object[]> params = Arrays.asList(
     *     new Object[] {"active", "user1"},
     *     new Object[] {"inactive", "user2"}
     * );
     *
     * ResultSet result = executor.batchUpdate(query, params, BatchType.LOGGED); // returns the driver ResultSet for the batch
     *
     * executor.batchUpdate(query, new ArrayList<>(), BatchType.LOGGED); // throws IllegalArgumentException (parametersList is empty)
     * }</pre>
     *
     * @param query the UPDATE query to execute
     * @param parametersList the collection of parameter arrays
     * @param type the batch type
     * @return the result set from the batch UPDATE operation
     * @throws IllegalArgumentException if {@code parametersList} is {@code null} or empty (enforced
     *         by the shipped executors' batch-statement builders)
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
     * ResultSet result = executor.delete(user); // returns the driver ResultSet for the DELETE (entire row)
     *
     * executor.delete((Object) null);           // throws IllegalArgumentException (entity is null)
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
     * ResultSet result = executor.delete(user, Arrays.asList("email", "phone")); // returns the driver ResultSet (clears email/phone)
     *
     * // Delete the entire row (null prop names)
     * ResultSet whole = executor.delete(user, (Collection<String>) null);        // returns the driver ResultSet (entire row removed)
     *
     * executor.delete(user, new ArrayList<>()); // throws IllegalArgumentException (propNamesToDelete is empty, not null)
     * }</pre>
     *
     * @param entity the entity identifying the row to delete from
     * @param propNamesToDelete the property names to delete (null for entire row)
     * @return the result set from the DELETE operation
     * @throws IllegalArgumentException if entity is null, or if propNamesToDelete is empty (but not null)
     */
    public RS delete(final Object entity, final Collection<String> propNamesToDelete) {
        N.checkArgNotNull(entity, "entity");
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
     * ResultSet result = executor.delete(User.class, "user123"); // returns the driver ResultSet for the DELETE
     *
     * executor.delete(User.class, new Object[0]); // throws IllegalArgumentException (ids is empty)
     * executor.delete(User.class, "a", "b");      // throws IllegalArgumentException (id count != single key column)
     * }</pre>
     *
     * @param targetClass the entity class
     * @param ids the primary key values, in the order of the declared key columns
     * @return the result set from the DELETE operation
     * @throws IllegalArgumentException if {@code ids} is null or empty, or if its length does not
     *         match the registered/annotated key columns of {@code targetClass}
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
     *     Arrays.asList("email", "phone"), "user123"); // returns the driver ResultSet (clears email/phone)
     *
     * // Delete entire row
     * ResultSet whole = executor.delete(User.class, null, "user123"); // returns the driver ResultSet (entire row removed)
     *
     * executor.delete(User.class, new ArrayList<>(), "user123"); // throws IllegalArgumentException (propNamesToDelete empty, not null)
     * executor.delete(User.class, Arrays.asList("email"), new Object[0]); // throws IllegalArgumentException (ids is empty)
     * }</pre>
     *
     * @param targetClass the entity class
     * @param propNamesToDelete the property names to delete; pass {@code null} to delete the
     *        entire row
     * @param ids the primary key values, in the order of the declared key columns
     * @return the result set from the DELETE operation
     * @throws IllegalArgumentException if {@code ids} is null or empty, if its length does not
     *         match the registered/annotated key columns of {@code targetClass}, or if
     *         {@code propNamesToDelete} is non-null but empty
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
     * ResultSet result = executor.delete(User.class, where); // returns the driver ResultSet for the DELETE
     * }</pre>
     *
     * @param targetClass the entity class
     * @param whereClause the WHERE condition
     * @return the result set from the DELETE operation
     * @throws IllegalArgumentException if targetClass is null or whereClause is null
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
     *     Arrays.asList("lastLogin", "sessionToken"), where);     // returns the driver ResultSet (clears those columns)
     *
     * // Delete the entire matching rows (null prop names)
     * ResultSet whole = executor.delete(User.class, null, where); // returns the driver ResultSet (entire rows removed)
     *
     * executor.delete(User.class, new ArrayList<>(), where); // throws IllegalArgumentException (propNamesToDelete empty, not null)
     * }</pre>
     *
     * @param targetClass the entity class
     * @param propNamesToDelete the property names to delete (null for entire rows)
     * @param whereClause the WHERE condition
     * @return the result set from the DELETE operation
     * @throws IllegalArgumentException if targetClass is null, if propNamesToDelete is empty (but not
     *         null), or if whereClause is null
     */
    public RS delete(final Class<?> targetClass, final Collection<String> propNamesToDelete, final Condition whereClause) {
        N.checkArgument(propNamesToDelete == null || N.notEmpty(propNamesToDelete), "'propNamesToDelete' can't be null or empty");

        final SP cp = prepareDelete(targetClass, propNamesToDelete, whereClause);

        return execute(cp);
    }

    /**
     * Performs a batch delete of multiple entities.
     *
     * <p>This method deletes all provided entities in a single {@code DELETE} whose {@code WHERE}
     * clause matches every entity's primary key (an {@code IN} over the key). Each entity must have
     * its primary key values set.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<User> usersToDelete = Arrays.asList(user1, user2, user3);
     * ResultSet result = executor.batchDelete(usersToDelete); // returns the driver ResultSet for the batch DELETE
     *
     * executor.batchDelete(new ArrayList<>());                // throws IllegalArgumentException (entities is empty)
     * }</pre>
     *
     * @param entities the collection of entities to delete
     * @return the result set from the batch DELETE operation
     * @throws IllegalArgumentException if entities is null or empty, or if its first element is null
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
     * ResultSet result = executor.batchDelete(users, Arrays.asList("email", "phone")); // returns the driver ResultSet (clears email/phone)
     *
     * // null prop names -> entire rows removed
     * ResultSet whole = executor.batchDelete(users, (Collection<String>) null);        // returns the driver ResultSet (entire rows removed)
     *
     * executor.batchDelete(new ArrayList<>(), Arrays.asList("email")); // throws IllegalArgumentException (entities is empty)
     * executor.batchDelete(users, new ArrayList<>());                  // throws IllegalArgumentException (propNamesToDelete empty, not null)
     * }</pre>
     *
     * @param entities the collection of entities to delete from
     * @param propNamesToDelete the property names to delete (null for entire rows)
     * @return the result set from the batch DELETE operation
     * @throws IllegalArgumentException if entities is empty or its first element is null, or if
     *         propNamesToDelete is empty (but not null)
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
     * boolean exists = executor.exists(User.class, "user123"); // returns true if a row with that key exists, else false
     * if (exists) {
     *     System.out.println("User exists");
     * }
     *
     * executor.exists(User.class);            // throws IllegalArgumentException (ids is empty)
     * executor.exists(User.class, "a", "b");  // throws IllegalArgumentException (id count != single key column)
     * }</pre>
     *
     * @param targetClass the entity class
     * @param ids the primary key values, in the order of the declared key columns
     * @return {@code true} if a record exists with the given primary key, {@code false} otherwise
     * @throws IllegalArgumentException if {@code ids} is null or empty, or if its length does not
     *         match the registered/annotated key columns of {@code targetClass}
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
     * boolean exists = executor.exists(User.class, where);     // returns true if at least one row matches, else false
     *
     * boolean any = executor.exists(User.class, (Condition) null); // returns true if the table has at least one row
     * }</pre>
     *
     * @param targetClass the entity class whose table is queried
     * @param whereClause the WHERE condition; pass {@code null} to check whether the table has at
     *        least one row
     * @return {@code true} if at least one record matches the condition, {@code false} otherwise
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
     * long activeUsers = executor.count(User.class, where);     // returns the matching row count (0 when none match)
     * System.out.println("Active users: " + activeUsers);
     *
     * long total = executor.count(User.class, (Condition) null); // returns the total row count of the table
     * }</pre>
     *
     * @param targetClass the entity class whose table is queried
     * @param whereClause the WHERE condition; pass {@code null} to count all rows in the table
     * @return the count of records matching the condition, or {@code 0} if no rows match
     */
    public long count(final Class<?> targetClass, final Condition whereClause) {
        // Note: LIMIT must NOT be applied to a COUNT(*) query. In Cassandra, "SELECT count(*) ... LIMIT 1"
        // caps the aggregated count itself (a table with 10 matching rows would return count = 1).
        final SP cp = prepareQuery(targetClass, COUNT_SELECT_PROP_NAMES, whereClause, 0);

        return queryForSingleValue(long.class, cp.query(), cp.parameters().toArray()).orElse(0L);
    }

    /**
     * Builds a {@code SELECT ... LIMIT 1} CQL query from the given condition and returns the first
     * matching row mapped to an instance of {@code targetClass}.
     *
     * <p>All properties of the entity class are selected. The generated CQL uses {@code ?} placeholders
     * bound from the {@link Condition}; only the first row of the {@code ResultSet} is consumed.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code Optional.empty()} is returned <i>only</i> when the
     * query produces no rows. When a row is found, the mapped entity is returned as a <i>present</i>
     * {@code Optional}. Because {@code Optional} cannot carry a {@code null} payload, the mapped entity
     * itself must be non-null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Optional<User> user = executor.findFirst(User.class,
     *     CF.eq("email", "user@example.com")); // returns present Optional<User> for the first match
     *
     * Optional<User> none = executor.findFirst(User.class,
     *     CF.eq("email", "missing@example.com")); // returns Optional.empty() when no row matches
     * }</pre>
     *
     * @param <T> the entity type to map the result row to
     * @param targetClass the entity class with getter/setter methods matching column names
     * @param whereClause the WHERE condition used to build the CQL query (may be {@code null})
     * @return a <i>present</i> {@code Optional<T>} holding the first mapped row when at least one row is
     *         returned; {@code Optional.empty()} when the query returns no rows
     * @see #findFirst(Class, Collection, Condition)
     * @see #findFirst(Class, String, Object...)
     */
    public <T> Optional<T> findFirst(final Class<T> targetClass, final Condition whereClause) {
        return findFirst(targetClass, null, whereClause);
    }

    /**
     * Builds a {@code SELECT <selectPropNames> ... LIMIT 1} CQL query from the given condition and
     * returns the first matching row mapped to an instance of {@code targetClass}.
     *
     * <p>The generated CQL projects only the requested properties (or all properties when
     * {@code selectPropNames} is {@code null}). Only the first row of the {@code ResultSet} is consumed.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code Optional.empty()} is returned <i>only</i> when the
     * query produces no rows. When a row is found, the mapped entity is returned as a <i>present</i>
     * {@code Optional}. Because {@code Optional} cannot carry a {@code null} payload, the mapped entity
     * itself must be non-null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Optional<User> user = executor.findFirst(User.class,
     *     Arrays.asList("userId", "name"),
     *     CF.eq("status", "active")); // returns present Optional<User> (only userId/name set) for the first match
     *
     * Optional<User> all = executor.findFirst(User.class, null,
     *     CF.eq("status", "active")); // returns present Optional with all properties, or empty when no row matches
     * }</pre>
     *
     * @param <T> the entity type to map the result row to
     * @param targetClass the entity class with getter/setter methods matching column names
     * @param selectPropNames the property names to select, or {@code null} for all properties
     * @param whereClause the WHERE condition used to build the CQL query (may be {@code null})
     * @return a <i>present</i> {@code Optional<T>} holding the first mapped row when at least one row is
     *         returned; {@code Optional.empty()} when the query returns no rows
     * @see #findFirst(Class, Condition)
     * @see #findFirst(Class, String, Object...)
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
     * List<User> activeUsers = executor.list(User.class, where); // returns a (possibly empty) mutable List of matches
     *
     * List<User> all = executor.list(User.class, (Condition) null); // returns every row in the table as a List
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class with getter/setter methods matching column names
     * @param whereClause the WHERE condition (may be {@code null} to select every row)
     * @return a list of all matching entities; empty if no row matches
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
     *     Arrays.asList("userId", "name", "age"), where); // returns a (possibly empty) List with only those 3 props set
     *
     * List<User> allProps = executor.list(User.class, null, where); // returns matches with all properties populated
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class with getter/setter methods matching column names
     * @param selectPropNames the property names to select; pass {@code null} for all properties
     * @param whereClause the WHERE condition (may be {@code null} to select every row)
     * @return a list of all matching entities; empty if no row matches
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
     * Dataset dataset = executor.query(User.class, where); // returns a (possibly empty) Dataset of matching rows
     * dataset.groupBy("department").aggregate("salary", Collectors.averagingDouble());
     *
     * Dataset all = executor.query(User.class, (Condition) null); // returns a Dataset of every row in the table
     * }</pre>
     *
     * @param targetClass the entity class with getter/setter methods matching column names
     * @param whereClause the WHERE condition (may be {@code null} to select every row)
     * @return a Dataset containing all matching entities; empty Dataset if no row matches
     */
    public Dataset query(final Class<?> targetClass, final Condition whereClause) {
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
     *                                  Filters.eq("status", "active")); // returns a Dataset with only those 3 columns
     *
     * Dataset all = executor.query(User.class, null,
     *                              Filters.eq("status", "active"));     // returns a Dataset with all columns
     * }</pre>
     *
     * @param targetClass the entity class with getter/setter methods matching column names
     * @param selectPropNames the property names to select; pass {@code null} for all properties
     * @param whereClause the WHERE condition (may be {@code null} to select every row)
     * @return a Dataset containing the selected properties of matching entities; empty Dataset
     *         if no row matches
     */
    public Dataset query(final Class<?> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP cp = prepareQuery(targetClass, selectPropNames, whereClause);

        return query(targetClass, cp.query(), cp.parameters().toArray());
    }

    /**
     * Builds a {@code SELECT <propName> ... LIMIT 1} CQL query from the given condition and returns the
     * value of {@code propName} from the first row as a boolean.
     *
     * <p>Only the first row of the {@code ResultSet} is read; remaining rows are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code OptionalBoolean.empty()} is returned <i>only</i> when
     * the query produces no rows. If a row exists but the column is {@code NULL}, the returned
     * {@code OptionalBoolean} is <i>present</i> and holds the primitive default {@code false}. Use
     * {@link #queryForSingleValue(Class, Class, String, Condition)} with {@code Boolean.class} if you need
     * to distinguish {@code NULL} from a real {@code false}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalBoolean isActive = executor.queryForBoolean(User.class, "is_active",
     *     CF.eq("id", userId)); // returns present OptionalBoolean (false when the column is NULL); empty when no row matches
     * }</pre>
     *
     * @param targetClass the entity class used to derive the table/column mapping
     * @param propName the property name (column) whose value is returned
     * @param whereClause the WHERE condition used to build the CQL query
     * @return a <i>present</i> {@code OptionalBoolean} holding the column value (or {@code false} for
     *         {@code NULL}) when at least one row is returned; {@code OptionalBoolean.empty()} when the
     *         query returns no rows
     * @throws IllegalArgumentException if {@code targetClass} is {@code null} or {@code propName} is
     *         {@code null} or empty
     * @see #queryForBoolean(String, Object...)
     * @see #queryForSingleValue(Class, Class, String, Condition)
     */
    @Beta
    public OptionalBoolean queryForBoolean(final Class<?> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, Boolean.class, propName, whereClause).mapToBoolean(ToBooleanFunction.UNBOX);
    }

    /**
     * Builds a {@code SELECT <propName> ... LIMIT 1} CQL query from the given condition and returns the
     * value of {@code propName} from the first row as a char.
     *
     * <p>Only the first row of the {@code ResultSet} is read; remaining rows are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code OptionalChar.empty()} is returned <i>only</i> when
     * the query produces no rows. If a row exists but the column is {@code NULL}, the returned
     * {@code OptionalChar} is <i>present</i> and holds the primitive default {@code (char) 0}. Use
     * {@link #queryForSingleValue(Class, Class, String, Condition)} with {@code Character.class} if you
     * need to distinguish {@code NULL} from a real {@code (char) 0}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalChar grade = executor.queryForChar(Student.class, "grade",
     *     CF.eq("student_id", studentId)); // returns present OptionalChar ((char) 0 when NULL); empty when no row matches
     * }</pre>
     *
     * @param targetClass the entity class used to derive the table/column mapping
     * @param propName the property name (column) whose value is returned
     * @param whereClause the WHERE condition used to build the CQL query
     * @return a <i>present</i> {@code OptionalChar} holding the column value (or the default {@code char}
     *         for {@code NULL}) when at least one row is returned; {@code OptionalChar.empty()} when the
     *         query returns no rows
     * @throws IllegalArgumentException if {@code targetClass} is {@code null} or {@code propName} is
     *         {@code null} or empty
     * @see #queryForSingleValue(Class, Class, String, Condition)
     * @see #queryForChar(String, Object...)
     */
    @Beta
    public OptionalChar queryForChar(final Class<?> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, Character.class, propName, whereClause).mapToChar(ToCharFunction.UNBOX);
    }

    /**
     * Builds a {@code SELECT <propName> ... LIMIT 1} CQL query from the given condition and returns the
     * value of {@code propName} from the first row as a byte.
     *
     * <p>Only the first row of the {@code ResultSet} is read; remaining rows are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code OptionalByte.empty()} is returned <i>only</i> when
     * the query produces no rows. If a row exists but the column is {@code NULL}, the returned
     * {@code OptionalByte} is <i>present</i> and holds the primitive default {@code (byte) 0}. Use
     * {@link #queryForSingleValue(Class, Class, String, Condition)} with {@code Byte.class} if you need
     * to distinguish {@code NULL} from a real {@code 0}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalByte statusCode = executor.queryForByte(Request.class, "status_code",
     *     CF.eq("request_id", requestId)); // returns present OptionalByte (0 when NULL); empty when no row matches
     * }</pre>
     *
     * @param targetClass the entity class used to derive the table/column mapping
     * @param propName the property name (column) whose value is returned
     * @param whereClause the WHERE condition used to build the CQL query
     * @return a <i>present</i> {@code OptionalByte} holding the column value (or {@code 0} for
     *         {@code NULL}) when at least one row is returned; {@code OptionalByte.empty()} when the
     *         query returns no rows
     * @throws IllegalArgumentException if {@code targetClass} is {@code null} or {@code propName} is
     *         {@code null} or empty
     * @see #queryForByte(String, Object...)
     * @see #queryForSingleValue(Class, Class, String, Condition)
     */
    @Beta
    public OptionalByte queryForByte(final Class<?> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, Byte.class, propName, whereClause).mapToByte(ToByteFunction.UNBOX);
    }

    /**
     * Builds a {@code SELECT <propName> ... LIMIT 1} CQL query from the given condition and returns the
     * value of {@code propName} from the first row as a short.
     *
     * <p>Only the first row of the {@code ResultSet} is read; remaining rows are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code OptionalShort.empty()} is returned <i>only</i> when
     * the query produces no rows. If a row exists but the column is {@code NULL}, the returned
     * {@code OptionalShort} is <i>present</i> and holds the primitive default {@code (short) 0}. Use
     * {@link #queryForSingleValue(Class, Class, String, Condition)} with {@code Short.class} if you need
     * to distinguish {@code NULL} from a real {@code 0}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalShort port = executor.queryForShort(Server.class, "port",
     *     CF.eq("server_name", "web-01")); // returns present OptionalShort (0 when NULL); empty when no row matches
     * }</pre>
     *
     * @param targetClass the entity class used to derive the table/column mapping
     * @param propName the property name (column) whose value is returned
     * @param whereClause the WHERE condition used to build the CQL query
     * @return a <i>present</i> {@code OptionalShort} holding the column value (or {@code 0} for
     *         {@code NULL}) when at least one row is returned; {@code OptionalShort.empty()} when the
     *         query returns no rows
     * @throws IllegalArgumentException if {@code targetClass} is {@code null} or {@code propName} is
     *         {@code null} or empty
     * @see #queryForShort(String, Object...)
     * @see #queryForSingleValue(Class, Class, String, Condition)
     */
    @Beta
    public OptionalShort queryForShort(final Class<?> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, Short.class, propName, whereClause).mapToShort(ToShortFunction.UNBOX);
    }

    /**
     * Builds a {@code SELECT <propName> ... LIMIT 1} CQL query from the given condition and returns the
     * value of {@code propName} from the first row as an int.
     *
     * <p>Only the first row of the {@code ResultSet} is read; remaining rows are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code OptionalInt.empty()} is returned <i>only</i> when the
     * query produces no rows. If a row exists but the column is {@code NULL}, the returned
     * {@code OptionalInt} is <i>present</i> and holds the primitive default {@code 0}. Use
     * {@link #queryForSingleValue(Class, Class, String, Condition)} with {@code Integer.class} if you
     * need to distinguish {@code NULL} from a real {@code 0}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalInt count = executor.queryForInt(Product.class, "quantity",
     *     CF.eq("product_id", productId)); // returns present OptionalInt (0 when NULL); empty when no row matches
     * }</pre>
     *
     * @param targetClass the entity class used to derive the table/column mapping
     * @param propName the property name (column) whose value is returned
     * @param whereClause the WHERE condition used to build the CQL query
     * @return a <i>present</i> {@code OptionalInt} holding the column value (or {@code 0} for
     *         {@code NULL}) when at least one row is returned; {@code OptionalInt.empty()} when the
     *         query returns no rows
     * @throws IllegalArgumentException if {@code targetClass} is {@code null} or {@code propName} is
     *         {@code null} or empty
     * @see #queryForInt(String, Object...)
     * @see #queryForSingleValue(Class, Class, String, Condition)
     */
    @Beta
    public OptionalInt queryForInt(final Class<?> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, Integer.class, propName, whereClause).mapToInt(ToIntFunction.UNBOX);
    }

    /**
     * Builds a {@code SELECT <propName> ... LIMIT 1} CQL query from the given condition and returns the
     * value of {@code propName} from the first row as a long.
     *
     * <p>Only the first row of the {@code ResultSet} is read; remaining rows are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code OptionalLong.empty()} is returned <i>only</i> when
     * the query produces no rows. If a row exists but the column is {@code NULL}, the returned
     * {@code OptionalLong} is <i>present</i> and holds the primitive default {@code 0L}. Use
     * {@link #queryForSingleValue(Class, Class, String, Condition)} with {@code Long.class} if you need
     * to distinguish {@code NULL} from a real {@code 0L}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalLong timestamp = executor.queryForLong(Event.class, "timestamp",
     *     CF.eq("event_id", eventId)); // returns present OptionalLong (0L when NULL); empty when no row matches
     * }</pre>
     *
     * @param targetClass the entity class used to derive the table/column mapping
     * @param propName the property name (column) whose value is returned
     * @param whereClause the WHERE condition used to build the CQL query
     * @return a <i>present</i> {@code OptionalLong} holding the column value (or {@code 0L} for
     *         {@code NULL}) when at least one row is returned; {@code OptionalLong.empty()} when the
     *         query returns no rows
     * @throws IllegalArgumentException if {@code targetClass} is {@code null} or {@code propName} is
     *         {@code null} or empty
     * @see #queryForLong(String, Object...)
     * @see #queryForSingleValue(Class, Class, String, Condition)
     */
    @Beta
    public OptionalLong queryForLong(final Class<?> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, Long.class, propName, whereClause).mapToLong(ToLongFunction.UNBOX);
    }

    /**
     * Builds a {@code SELECT <propName> ... LIMIT 1} CQL query from the given condition and returns the
     * value of {@code propName} from the first row as a float.
     *
     * <p>Only the first row of the {@code ResultSet} is read; remaining rows are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code OptionalFloat.empty()} is returned <i>only</i> when
     * the query produces no rows. If a row exists but the column is {@code NULL}, the returned
     * {@code OptionalFloat} is <i>present</i> and holds the primitive default {@code 0.0f}. Use
     * {@link #queryForSingleValue(Class, Class, String, Condition)} with {@code Float.class} if you need
     * to distinguish {@code NULL} from a real {@code 0.0f}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalFloat rating = executor.queryForFloat(Product.class, "rating",
     *     CF.eq("product_id", productId)); // returns present OptionalFloat (0.0f when NULL); empty when no row matches
     * }</pre>
     *
     * @param targetClass the entity class used to derive the table/column mapping
     * @param propName the property name (column) whose value is returned
     * @param whereClause the WHERE condition used to build the CQL query
     * @return a <i>present</i> {@code OptionalFloat} holding the column value (or {@code 0.0f} for
     *         {@code NULL}) when at least one row is returned; {@code OptionalFloat.empty()} when the
     *         query returns no rows
     * @throws IllegalArgumentException if {@code targetClass} is {@code null} or {@code propName} is
     *         {@code null} or empty
     * @see #queryForFloat(String, Object...)
     * @see #queryForSingleValue(Class, Class, String, Condition)
     */
    @Beta
    public OptionalFloat queryForFloat(final Class<?> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, Float.class, propName, whereClause).mapToFloat(ToFloatFunction.UNBOX);
    }

    /**
     * Builds a {@code SELECT <propName> ... LIMIT 1} CQL query from the given condition and returns the
     * value of {@code propName} from the first row as a double.
     *
     * <p>Only the first row of the {@code ResultSet} is read; remaining rows are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code OptionalDouble.empty()} is returned <i>only</i> when
     * the query produces no rows. If a row exists but the column is {@code NULL}, the returned
     * {@code OptionalDouble} is <i>present</i> and holds the primitive default {@code 0.0d}. Use
     * {@link #queryForSingleValue(Class, Class, String, Condition)} with {@code Double.class} if you need
     * to distinguish {@code NULL} from a real {@code 0.0d}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalDouble price = executor.queryForDouble(Product.class, "price",
     *     CF.eq("product_id", productId)); // returns present OptionalDouble (0.0d when NULL); empty when no row matches
     * }</pre>
     *
     * @param targetClass the entity class used to derive the table/column mapping
     * @param propName the property name (column) whose value is returned
     * @param whereClause the WHERE condition used to build the CQL query
     * @return a <i>present</i> {@code OptionalDouble} holding the column value (or {@code 0.0d} for
     *         {@code NULL}) when at least one row is returned; {@code OptionalDouble.empty()} when the
     *         query returns no rows
     * @throws IllegalArgumentException if {@code targetClass} is {@code null} or {@code propName} is
     *         {@code null} or empty
     * @see #queryForDouble(String, Object...)
     * @see #queryForSingleValue(Class, Class, String, Condition)
     */
    @Beta
    public OptionalDouble queryForDouble(final Class<?> targetClass, final String propName, final Condition whereClause) {
        return queryForSingleValue(targetClass, Double.class, propName, whereClause).mapToDouble(ToDoubleFunction.UNBOX);
    }

    /**
     * Builds a {@code SELECT <propName> ... LIMIT 1} CQL query from the given condition and returns the
     * value of {@code propName} from the first row as a String.
     *
     * <p>Only the first row of the {@code ResultSet} is read; remaining rows are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code Nullable.empty()} is returned <i>only</i> when the
     * query produces no rows. If a row exists but the column is {@code NULL}, the returned
     * {@code Nullable} is <i>present-but-null</i> ({@code Nullable.of(null)}), preserving the distinction
     * between "no row matched" and "row matched but value is null".</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Nullable<String> email = executor.queryForString(User.class, "email",
     *     CF.eq("username", "john")); // returns present Nullable (may hold null when the column is NULL); empty when no row matches
     * }</pre>
     *
     * @param targetClass the entity class used to derive the table/column mapping
     * @param propName the property name (column) whose value is returned
     * @param whereClause the WHERE condition used to build the CQL query
     * @return a <i>present</i> {@code Nullable<String>} holding the column value (possibly {@code null}
     *         for {@code NULL}) when at least one row is returned; {@code Nullable.empty()} when the
     *         query returns no rows
     * @throws IllegalArgumentException if {@code targetClass} is {@code null} or {@code propName} is
     *         {@code null} or empty
     * @see #queryForString(String, Object...)
     * @see #queryForSingleValue(Class, Class, String, Condition)
     */
    @Beta
    public Nullable<String> queryForString(final Class<?> targetClass, final String propName, final Condition whereClause) {
        return this.queryForSingleValue(targetClass, String.class, propName, whereClause);
    }

    /**
     * Builds a {@code SELECT <propName> ... LIMIT 1} CQL query from the given condition and returns the
     * value of {@code propName} from the first row as a {@link java.util.Date}.
     *
     * <p>Only the first row of the {@code ResultSet} is read; remaining rows are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code Nullable.empty()} is returned <i>only</i> when the
     * query produces no rows. If a row exists but the column is {@code NULL}, the returned
     * {@code Nullable} is <i>present-but-null</i> ({@code Nullable.of(null)}), preserving the distinction
     * between "no row matched" and "row matched but value is null".</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Nullable<Date> created = executor.queryForDate(User.class, "created_at",
     *     CF.eq("user_id", userId)); // returns present Nullable (may hold null when the column is NULL); empty when no row matches
     * }</pre>
     *
     * @param targetClass the entity class used to derive the table/column mapping
     * @param propName the property name (column) whose value is returned
     * @param whereClause the WHERE condition used to build the CQL query
     * @return a <i>present</i> {@code Nullable<Date>} holding the column value (possibly {@code null} for
     *         {@code NULL}) when at least one row is returned; {@code Nullable.empty()} when the query
     *         returns no rows
     * @throws IllegalArgumentException if {@code targetClass} is {@code null} or {@code propName} is
     *         {@code null} or empty
     * @see #queryForDate(Class, Class, String, Condition)
     * @see #queryForSingleValue(Class, Class, String, Condition)
     */
    @Beta
    public Nullable<Date> queryForDate(final Class<?> targetClass, final String propName, final Condition whereClause) {
        return this.queryForSingleValue(targetClass, Date.class, propName, whereClause);
    }

    /**
     * Builds a {@code SELECT <propName> ... LIMIT 1} CQL query from the given condition and returns the
     * value of {@code propName} from the first row as the specified {@link Date} subclass.
     *
     * <p>Only the first row of the {@code ResultSet} is read; remaining rows are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code Nullable.empty()} is returned <i>only</i> when the
     * query produces no rows. If a row exists but the column is {@code NULL}, the returned
     * {@code Nullable} is <i>present-but-null</i> ({@code Nullable.of(null)}), preserving the distinction
     * between "no row matched" and "row matched but value is null".</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Nullable<java.sql.Timestamp> lastLogin = executor.queryForDate(
     *     User.class, java.sql.Timestamp.class, "last_login",
     *     CF.eq("user_id", userId)); // returns present Nullable (may hold null when the column is NULL); empty when no row matches
     * }</pre>
     *
     * @param <E> the specific {@link Date} subtype to return (for example, {@link java.sql.Timestamp})
     * @param targetClass the entity class used to derive the table/column mapping
     * @param valueClass the specific {@link Date} class the column value is converted to
     * @param propName the property name (column) whose value is returned
     * @param whereClause the WHERE condition used to build the CQL query
     * @return a <i>present</i> {@code Nullable<E>} holding the column value (possibly {@code null} for
     *         {@code NULL}) when at least one row is returned; {@code Nullable.empty()} when the query
     *         returns no rows
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}, {@code valueClass} is
     *         {@code null}, or {@code propName} is {@code null} or empty
     * @see #queryForDate(Class, String, Condition)
     * @see #queryForSingleValue(Class, Class, String, Condition)
     */
    @Beta
    public <E extends Date> Nullable<E> queryForDate(final Class<?> targetClass, final Class<E> valueClass, final String propName,
            final Condition whereClause) {
        return this.queryForSingleValue(targetClass, valueClass, propName, whereClause);
    }

    /**
     * Builds a {@code SELECT <propName> ... LIMIT 1} CQL query from the given condition and returns the
     * value of {@code propName} from the first row converted to {@code valueClass}.
     *
     * <p>Only the first row of the {@code ResultSet} is read; remaining rows are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code Nullable.empty()} is returned <i>only</i> when the
     * query produces no rows. If a row exists but the column is {@code NULL}, the returned
     * {@code Nullable} is <i>present-but-null</i> ({@code Nullable.of(null)}), preserving the distinction
     * between "no row matched" and "row matched but value is null". Unlike the primitive
     * {@code queryForXxx} variants (which collapse {@code NULL} into a primitive default), this overload
     * always conveys {@code NULL} precisely as Java {@code null} inside the {@code Nullable}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Nullable<BigDecimal> price = executor.queryForSingleValue(
     *     Product.class, BigDecimal.class, "price",
     *     CF.eq("id", productId)); // returns present Nullable (may hold null when the column is NULL); empty when no row matches
     * }</pre>
     *
     * @param <V> the value type to return
     * @param targetClass the entity class used to derive the table/column mapping
     * @param valueClass the Java class the column value is converted to
     * @param propName the property name (column) whose value is returned
     * @param whereClause the WHERE condition used to build the CQL query
     * @return a <i>present</i> {@code Nullable<V>} holding the column value (possibly {@code null} for
     *         {@code NULL}) when at least one row is returned; {@code Nullable.empty()} when the query
     *         returns no rows
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}, {@code valueClass} is
     *         {@code null}, or {@code propName} is {@code null} or empty
     * @see #queryForSingleNonNull(Class, Class, String, Condition)
     * @see #queryForSingleValue(Class, String, Object...)
     */
    public <V> Nullable<V> queryForSingleValue(final Class<?> targetClass, final Class<V> valueClass, final String propName, final Condition whereClause) {
        N.checkArgNotEmpty(propName, "propName");

        final SP cp = prepareQuery(targetClass, List.of(propName), whereClause, 1);

        return queryForSingleValue(valueClass, cp.query(), cp.parameters().toArray());
    }

    /**
     * Builds a {@code SELECT <propName> ... LIMIT 1} CQL query from the given condition and returns the
     * value of {@code propName} from the first row converted to {@code valueClass}, wrapped in an
     * {@link Optional} that is guaranteed to be non-null when present.
     *
     * <p>Only the first row of the {@code ResultSet} is read; remaining rows are ignored.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code Optional.empty()} is returned <i>only</i> when the
     * query returns no rows. When a row is returned, the column value is wrapped in the
     * {@code Optional} via {@link Optional#of(Object)}, which does not accept a null payload — so if
     * the column value is {@code NULL} (or the conversion to {@code valueClass} yields {@code null}),
     * this method throws {@link NullPointerException} rather than returning {@code Optional.empty()}.
     * Use {@link #queryForSingleValue(Class, Class, String, Condition)} (which returns
     * {@link Nullable}) when the column may legitimately be {@code NULL}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Optional<String> email = executor.queryForSingleNonNull(
     *     User.class, String.class, "email",
     *     CF.eq("username", "john")); // returns present Optional with the value; empty when no row matches; throws NullPointerException if the matched value is NULL
     * }</pre>
     *
     * @param <V> the value type to return
     * @param targetClass the entity class used to derive the table/column mapping
     * @param valueClass the Java class the column value is converted to
     * @param propName the property name (column) whose value is returned
     * @param whereClause the WHERE condition used to build the CQL query
     * @return a <i>present</i> {@code Optional<V>} holding the (non-null) column value when at least
     *         one row is returned with a non-null value; {@code Optional.empty()} when the query
     *         returns no rows
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}, {@code valueClass} is
     *         {@code null}, or {@code propName} is {@code null} or empty
     * @throws NullPointerException if a row is returned but the column value (or its conversion) is
     *         {@code null}, because {@link Optional#of(Object)} rejects a null payload
     * @see #queryForSingleValue(Class, Class, String, Condition)
     * @see #queryForSingleNonNull(Class, String, Object...)
     */
    public <V> Optional<V> queryForSingleNonNull(final Class<?> targetClass, final Class<V> valueClass, final String propName, final Condition whereClause) {
        N.checkArgNotEmpty(propName, "propName");

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
     * long count = executor.stream(User.class, Filters.gt("age", 18)) // returns a lazy Stream<User> of matches
     *                      .filter(u -> u.isActive())
     *                      .count(); // count is the number of active users older than 18
     *
     * Stream<User> all = executor.stream(User.class, (Condition) null); // returns a Stream over every row in the table
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class with getter/setter methods matching column names
     * @param whereClause the WHERE condition (may be {@code null} to stream every row)
     * @return a stream of matching entities (lazily produced; empty if no row matches)
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
     *                                           Filters.eq("status", "active")); // returns a lazy Stream<User> with only those 3 props set
     *
     * Stream<User> allProps = executor.stream(User.class, null,
     *                                         Filters.eq("status", "active"));   // returns a Stream with all properties populated
     * }</pre>
     *
     * @param <T> the entity type
     * @param targetClass the entity class with getter/setter methods matching column names
     * @param selectPropNames the property names to select; pass {@code null} for all properties
     * @param whereClause the WHERE condition (may be {@code null} to stream every row)
     * @return a stream of matching entities with selected properties (lazily produced; empty if
     *         no row matches)
     */
    public <T> Stream<T> stream(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP cp = prepareQuery(targetClass, selectPropNames, whereClause);

        return stream(targetClass, cp.query(), cp.parameters().toArray());
    }

    /**
     * Executes the given CQL query and returns whether it produced at least one row.
     *
     * <p>This method only consults {@code resultSet.iterator().hasNext()}, so adding
     * {@code LIMIT 1} to the CQL is strongly recommended to avoid the server materializing a
     * larger result set than necessary. Parameters are bound positionally to {@code ?}
     * placeholders.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * boolean exists = executor.exists(
     *     "SELECT email FROM users WHERE email = ? LIMIT 1",
     *     "user@example.com"); // returns true if the query yields at least one row, else false
     * }</pre>
     *
     * @param query the CQL query string with {@code ?} placeholders for parameters
     * @param parameters the values to bind, in declaration order
     * @return {@code true} if the query returned at least one row, {@code false} otherwise
     */
    public final boolean exists(final String query, final Object... parameters) {
        final RS resultSet = execute(query, parameters);

        return resultSet.iterator().hasNext();
    }

    /**
     * Executes the given CQL query (assumed to be a {@code SELECT count(*)} / {@code SELECT sum(...)}
     * or similar single-value aggregate) and returns the first column of the first row as a
     * {@code long}.
     *
     * <p>Parameters are bound positionally to {@code ?} placeholders. If the query produces no rows
     * (or the aggregate returns {@code NULL}) this method returns {@code 0L}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long active = executor.count(
     *     "SELECT count(*) FROM users WHERE status = ?", "active"); // returns the COUNT(*) value, or 0L if no row/NULL
     * }</pre>
     *
     * @param query the CQL query to execute (typically a {@code COUNT(*)} aggregate)
     * @param parameters the values to bind, in declaration order
     * @return the aggregate value, or {@code 0L} if no row is returned
     * @see #queryForLong(String, Object...)
     * @see #count(Class, Condition)
     * @deprecated prefer {@link #queryForLong(String, Object...)} with a {@code COUNT(*)} query and
     *             {@code .orElse(0L)} for the same behavior. Slated for removal in a future
     *             release.
     */
    @Deprecated
    public final long count(final String query, final Object... parameters) {
        return queryForSingleValue(long.class, query, parameters).orElse(0L);
    }

    /**
     * Executes the given CQL query and returns the first column of the first row as a boolean.
     *
     * <p>Only the first column of the first row of the {@code ResultSet} is read; remaining rows and
     * columns are ignored. Parameters are bound positionally to {@code ?} placeholders.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code OptionalBoolean.empty()} is returned <i>only</i> when
     * the query produces no rows. If a row exists but the column is {@code NULL}, the returned
     * {@code OptionalBoolean} is <i>present</i> and holds the primitive default {@code false}. Use
     * {@link #queryForSingleValue(Class, String, Object...)} with {@code Boolean.class} if you need to
     * distinguish {@code NULL} from a real {@code false}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalBoolean isActive = executor.queryForBoolean(
     *     "SELECT is_active FROM users WHERE id = ?", userId); // returns present OptionalBoolean (false when NULL); empty when no row matches
     * }</pre>
     *
     * @param query the CQL query string with {@code ?} placeholders for parameters
     * @param parameters the values to bind, in declaration order
     * @return a <i>present</i> {@code OptionalBoolean} holding the column value (or {@code false} for
     *         {@code NULL}) when at least one row is returned; {@code OptionalBoolean.empty()} when the
     *         query returns no rows
     * @see #queryForBoolean(Class, String, Condition)
     * @see #queryForSingleValue(Class, String, Object...)
     */
    @Beta
    public final OptionalBoolean queryForBoolean(final String query, final Object... parameters) {
        return this.queryForSingleValue(Boolean.class, query, parameters).mapToBoolean(ToBooleanFunction.UNBOX);
    }

    /**
     * Executes the given CQL query and returns the first column of the first row as a char.
     *
     * <p>Only the first column of the first row of the {@code ResultSet} is read; remaining rows and
     * columns are ignored. Parameters are bound positionally to {@code ?} placeholders.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code OptionalChar.empty()} is returned <i>only</i> when
     * the query produces no rows. If a row exists but the column is {@code NULL}, the returned
     * {@code OptionalChar} is <i>present</i> and holds the primitive default {@code (char) 0}. Use
     * {@link #queryForSingleValue(Class, String, Object...)} with {@code Character.class} if you need to
     * distinguish {@code NULL} from a real {@code (char) 0}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalChar grade = executor.queryForChar(
     *     "SELECT grade FROM students WHERE id = ?", studentId); // returns present OptionalChar ((char) 0 when NULL); empty when no row matches
     * }</pre>
     *
     * @param query the CQL query string with {@code ?} placeholders for parameters
     * @param parameters the values to bind, in declaration order
     * @return a <i>present</i> {@code OptionalChar} holding the column value (or the default {@code char}
     *         for {@code NULL}) when at least one row is returned; {@code OptionalChar.empty()} when the
     *         query returns no rows
     * @see #queryForSingleValue(Class, String, Object...)
     */
    @Beta
    public final OptionalChar queryForChar(final String query, final Object... parameters) {
        return this.queryForSingleValue(Character.class, query, parameters).mapToChar(ToCharFunction.UNBOX);
    }

    /**
     * Executes the given CQL query and returns the first column of the first row as a byte.
     *
     * <p>Only the first column of the first row of the {@code ResultSet} is read; remaining rows and
     * columns are ignored. Parameters are bound positionally to {@code ?} placeholders.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code OptionalByte.empty()} is returned <i>only</i> when
     * the query produces no rows. If a row exists but the column is {@code NULL}, the returned
     * {@code OptionalByte} is <i>present</i> and holds the primitive default {@code (byte) 0}. Use
     * {@link #queryForSingleValue(Class, String, Object...)} with {@code Byte.class} if you need to
     * distinguish {@code NULL} from a real {@code 0}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalByte status = executor.queryForByte(
     *     "SELECT status_code FROM records WHERE id = ?", recordId); // returns present OptionalByte (0 when NULL); empty when no row matches
     * }</pre>
     *
     * @param query the CQL query string with {@code ?} placeholders for parameters
     * @param parameters the values to bind, in declaration order
     * @return a <i>present</i> {@code OptionalByte} holding the column value (or {@code 0} for
     *         {@code NULL}) when at least one row is returned; {@code OptionalByte.empty()} when the
     *         query returns no rows
     * @see #queryForByte(Class, String, Condition)
     * @see #queryForSingleValue(Class, String, Object...)
     */
    @Beta
    public final OptionalByte queryForByte(final String query, final Object... parameters) {
        return this.queryForSingleValue(Byte.class, query, parameters).mapToByte(ToByteFunction.UNBOX);
    }

    /**
     * Executes the given CQL query and returns the first column of the first row as a short.
     *
     * <p>Only the first column of the first row of the {@code ResultSet} is read; remaining rows and
     * columns are ignored. Parameters are bound positionally to {@code ?} placeholders.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code OptionalShort.empty()} is returned <i>only</i> when
     * the query produces no rows. If a row exists but the column is {@code NULL}, the returned
     * {@code OptionalShort} is <i>present</i> and holds the primitive default {@code (short) 0}. Use
     * {@link #queryForSingleValue(Class, String, Object...)} with {@code Short.class} if you need to
     * distinguish {@code NULL} from a real {@code 0}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalShort year = executor.queryForShort(
     *     "SELECT release_year FROM movies WHERE id = ?", movieId); // returns present OptionalShort (0 when NULL); empty when no row matches
     * }</pre>
     *
     * @param query the CQL query string with {@code ?} placeholders for parameters
     * @param parameters the values to bind, in declaration order
     * @return a <i>present</i> {@code OptionalShort} holding the column value (or {@code 0} for
     *         {@code NULL}) when at least one row is returned; {@code OptionalShort.empty()} when the
     *         query returns no rows
     * @see #queryForShort(Class, String, Condition)
     * @see #queryForSingleValue(Class, String, Object...)
     */
    @Beta
    public final OptionalShort queryForShort(final String query, final Object... parameters) {
        return this.queryForSingleValue(Short.class, query, parameters).mapToShort(ToShortFunction.UNBOX);
    }

    /**
     * Executes the given CQL query and returns the first column of the first row as an int.
     *
     * <p>Only the first column of the first row of the {@code ResultSet} is read; remaining rows and
     * columns are ignored. Parameters are bound positionally to {@code ?} placeholders. Commonly used
     * for {@code COUNT(*)} queries.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code OptionalInt.empty()} is returned <i>only</i> when the
     * query produces no rows. If a row exists but the column is {@code NULL}, the returned
     * {@code OptionalInt} is <i>present</i> and holds the primitive default {@code 0}. Use
     * {@link #queryForSingleValue(Class, String, Object...)} with {@code Integer.class} if you need to
     * distinguish {@code NULL} from a real {@code 0}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalInt count = executor.queryForInt(
     *     "SELECT count(*) FROM users WHERE status = ?", "active"); // returns present OptionalInt with the count (0 when NULL); empty when no row matches
     * }</pre>
     *
     * @param query the CQL query string with {@code ?} placeholders for parameters
     * @param parameters the values to bind, in declaration order
     * @return a <i>present</i> {@code OptionalInt} holding the column value (or {@code 0} for
     *         {@code NULL}) when at least one row is returned; {@code OptionalInt.empty()} when the
     *         query returns no rows
     * @see #queryForInt(Class, String, Condition)
     * @see #queryForSingleValue(Class, String, Object...)
     */
    @Beta
    public final OptionalInt queryForInt(final String query, final Object... parameters) {
        return this.queryForSingleValue(Integer.class, query, parameters).mapToInt(ToIntFunction.UNBOX);
    }

    /**
     * Executes the given CQL query and returns the first column of the first row as a long.
     *
     * <p>Only the first column of the first row of the {@code ResultSet} is read; remaining rows and
     * columns are ignored. Parameters are bound positionally to {@code ?} placeholders.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code OptionalLong.empty()} is returned <i>only</i> when
     * the query produces no rows. If a row exists but the column is {@code NULL}, the returned
     * {@code OptionalLong} is <i>present</i> and holds the primitive default {@code 0L}. Use
     * {@link #queryForSingleValue(Class, String, Object...)} with {@code Long.class} if you need to
     * distinguish {@code NULL} from a real {@code 0L}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalLong total = executor.queryForLong(
     *     "SELECT sum(amount) FROM sales WHERE year = ?", year); // returns present OptionalLong (0L when NULL); empty when no row matches
     * }</pre>
     *
     * @param query the CQL query string with {@code ?} placeholders for parameters
     * @param parameters the values to bind, in declaration order
     * @return a <i>present</i> {@code OptionalLong} holding the column value (or {@code 0L} for
     *         {@code NULL}) when at least one row is returned; {@code OptionalLong.empty()} when the
     *         query returns no rows
     * @see #queryForLong(Class, String, Condition)
     * @see #queryForSingleValue(Class, String, Object...)
     */
    @Beta
    public final OptionalLong queryForLong(final String query, final Object... parameters) {
        return this.queryForSingleValue(Long.class, query, parameters).mapToLong(ToLongFunction.UNBOX);
    }

    /**
     * Executes the given CQL query and returns the first column of the first row as a float.
     *
     * <p>Only the first column of the first row of the {@code ResultSet} is read; remaining rows and
     * columns are ignored. Parameters are bound positionally to {@code ?} placeholders.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code OptionalFloat.empty()} is returned <i>only</i> when
     * the query produces no rows. If a row exists but the column is {@code NULL}, the returned
     * {@code OptionalFloat} is <i>present</i> and holds the primitive default {@code 0.0f}. Use
     * {@link #queryForSingleValue(Class, String, Object...)} with {@code Float.class} if you need to
     * distinguish {@code NULL} from a real {@code 0.0f}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalFloat rating = executor.queryForFloat(
     *     "SELECT avg_rating FROM products WHERE id = ?", productId); // returns present OptionalFloat (0.0f when NULL); empty when no row matches
     * }</pre>
     *
     * @param query the CQL query string with {@code ?} placeholders for parameters
     * @param parameters the values to bind, in declaration order
     * @return a <i>present</i> {@code OptionalFloat} holding the column value (or {@code 0.0f} for
     *         {@code NULL}) when at least one row is returned; {@code OptionalFloat.empty()} when the
     *         query returns no rows
     * @see #queryForFloat(Class, String, Condition)
     * @see #queryForSingleValue(Class, String, Object...)
     */
    @Beta
    public final OptionalFloat queryForFloat(final String query, final Object... parameters) {
        return this.queryForSingleValue(Float.class, query, parameters).mapToFloat(ToFloatFunction.UNBOX);
    }

    /**
     * Executes the given CQL query and returns the first column of the first row as a double.
     *
     * <p>Only the first column of the first row of the {@code ResultSet} is read; remaining rows and
     * columns are ignored. Parameters are bound positionally to {@code ?} placeholders.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code OptionalDouble.empty()} is returned <i>only</i> when
     * the query produces no rows. If a row exists but the column is {@code NULL}, the returned
     * {@code OptionalDouble} is <i>present</i> and holds the primitive default {@code 0.0d}. Use
     * {@link #queryForSingleValue(Class, String, Object...)} with {@code Double.class} if you need to
     * distinguish {@code NULL} from a real {@code 0.0d}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OptionalDouble avgSalary = executor.queryForDouble(
     *     "SELECT avg(salary) FROM employees WHERE dept = ?", "Engineering"); // returns present OptionalDouble (0.0d when NULL); empty when no row matches
     * }</pre>
     *
     * @param query the CQL query string with {@code ?} placeholders for parameters
     * @param parameters the values to bind, in declaration order
     * @return a <i>present</i> {@code OptionalDouble} holding the column value (or {@code 0.0d} for
     *         {@code NULL}) when at least one row is returned; {@code OptionalDouble.empty()} when the
     *         query returns no rows
     * @see #queryForDouble(Class, String, Condition)
     * @see #queryForSingleValue(Class, String, Object...)
     */
    @Beta
    public final OptionalDouble queryForDouble(final String query, final Object... parameters) {
        return this.queryForSingleValue(Double.class, query, parameters).mapToDouble(ToDoubleFunction.UNBOX);
    }

    /**
     * Executes the given CQL query and returns the first column of the first row as a String.
     *
     * <p>Only the first column of the first row of the {@code ResultSet} is read; remaining rows and
     * columns are ignored. Parameters are bound positionally to {@code ?} placeholders.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code Nullable.empty()} is returned <i>only</i> when the
     * query produces no rows. If a row exists but the column is {@code NULL}, the returned
     * {@code Nullable} is <i>present-but-null</i> ({@code Nullable.of(null)}), preserving the distinction
     * between "no row matched" and "row matched but value is null".</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Nullable<String> name = executor.queryForString(
     *     "SELECT name FROM users WHERE id = ?", userId); // returns present Nullable (may hold null when the column is NULL); empty when no row matches
     * }</pre>
     *
     * @param query the CQL query string with {@code ?} placeholders for parameters
     * @param parameters the values to bind, in declaration order
     * @return a <i>present</i> {@code Nullable<String>} holding the column value (possibly {@code null}
     *         for {@code NULL}) when at least one row is returned; {@code Nullable.empty()} when the
     *         query returns no rows
     * @see #queryForString(Class, String, Condition)
     * @see #queryForSingleValue(Class, String, Object...)
     */
    @Beta
    public final Nullable<String> queryForString(final String query, final Object... parameters) {
        return this.queryForSingleValue(String.class, query, parameters);
    }

    /**
     * Executes the given CQL query and returns the first column of the first row as a {@link Date}.
     *
     * <p>Only the first column of the first row of the {@code ResultSet} is read; remaining rows and
     * columns are ignored. Parameters are bound positionally to {@code ?} placeholders.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code Nullable.empty()} is returned <i>only</i> when the
     * query produces no rows. If a row exists but the column is {@code NULL}, the returned
     * {@code Nullable} is <i>present-but-null</i> ({@code Nullable.of(null)}), preserving the distinction
     * between "no row matched" and "row matched but value is null".</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Nullable<Date> createdAt = executor.queryForDate(
     *     "SELECT created_at FROM users WHERE id = ?", userId); // returns present Nullable (may hold null when the column is NULL); empty when no row matches
     * }</pre>
     *
     * @param query the CQL query string with {@code ?} placeholders for parameters
     * @param parameters the values to bind, in declaration order
     * @return a <i>present</i> {@code Nullable<Date>} holding the column value (possibly {@code null}
     *         for {@code NULL}) when at least one row is returned; {@code Nullable.empty()} when the
     *         query returns no rows
     * @see #queryForDate(Class, String, Object...)
     * @see #queryForDate(Class, String, Condition)
     * @see #queryForSingleValue(Class, String, Object...)
     */
    @Beta
    public final Nullable<Date> queryForDate(final String query, final Object... parameters) {
        return this.queryForSingleValue(Date.class, query, parameters);
    }

    /**
     * Executes the given CQL query and returns the first column of the first row converted to the
     * specified {@link Date} subclass (e.g. {@code java.sql.Timestamp}).
     *
     * <p>Only the first column of the first row of the {@code ResultSet} is read; remaining rows and
     * columns are ignored. Parameters are bound positionally to {@code ?} placeholders.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code Nullable.empty()} is returned <i>only</i> when the
     * query produces no rows. If a row exists but the column is {@code NULL}, the returned
     * {@code Nullable} is <i>present-but-null</i> ({@code Nullable.of(null)}), preserving the distinction
     * between "no row matched" and "row matched but value is null".</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Nullable<Timestamp> lastLogin = executor.queryForDate(
     *     Timestamp.class, "SELECT last_login FROM users WHERE id = ?", userId); // returns present Nullable (may hold null when the column is NULL); empty when no row matches
     * }</pre>
     *
     * @param <E> the concrete {@code Date} subtype to be returned
     * @param valueClass the {@code Date} subclass the column value is converted to
     * @param query the CQL query string with {@code ?} placeholders for parameters
     * @param parameters the values to bind, in declaration order
     * @return a <i>present</i> {@code Nullable<E>} holding the column value (possibly {@code null}
     *         for {@code NULL}) when at least one row is returned; {@code Nullable.empty()} when the
     *         query returns no rows
     * @see #queryForDate(String, Object...)
     * @see #queryForDate(Class, Class, String, Condition)
     * @see #queryForSingleValue(Class, String, Object...)
     */
    @Beta
    public final <E extends Date> Nullable<E> queryForDate(final Class<E> valueClass, final String query, final Object... parameters) {
        return this.queryForSingleValue(valueClass, query, parameters);
    }

    /**
     * Executes the given CQL query and returns the first column of the first row converted to
     * {@code valueClass}.
     *
     * <p>This method is designed for queries that return a single column and a single row, such as
     * aggregate functions ({@code COUNT}, {@code SUM}, {@code MAX}, ...) or lookup queries that are
     * expected to return at most one value. Parameters are bound positionally to {@code ?} placeholders;
     * only the first column of the first row of the {@code ResultSet} is consumed.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code Nullable.empty()} is returned <i>only</i> when the
     * query produces no rows. If a row exists but the column is {@code NULL}, the returned
     * {@code Nullable} is <i>present-but-null</i> ({@code Nullable.of(null)}), preserving the distinction
     * between "no row matched" and "row matched but value is null".</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Nullable<Long> userCount = executor.queryForSingleValue(
     *     Long.class, "SELECT count(*) FROM users"); // returns present Nullable holding the count; empty only if there are zero result rows
     *
     * Nullable<String> userName = executor.queryForSingleValue(
     *     String.class, "SELECT name FROM users WHERE id = ?", userId); // returns present Nullable (may hold null when the column is NULL); empty when no row matches
     *
     * Nullable<Instant> maxTimestamp = executor.queryForSingleValue(
     *     Instant.class, "SELECT max(created_at) FROM events WHERE date = ?", today); // returns present Nullable (may hold null when no events); empty when no row matches
     * }</pre>
     *
     * @param <V> the type of the single result value to be returned
     * @param valueClass the Java class the column value is converted to
     * @param query the CQL query string with {@code ?} placeholders for parameters
     * @param parameters the values to bind, in declaration order
     * @return a <i>present</i> {@code Nullable<V>} holding the column value (possibly {@code null} for
     *         {@code NULL}) when at least one row is returned; {@code Nullable.empty()} when the query
     *         returns no rows
     * @throws IllegalArgumentException if {@code valueClass} or {@code query} is {@code null}
     * @see #queryForSingleNonNull(Class, String, Object...)
     */
    public abstract <V> Nullable<V> queryForSingleValue(final Class<V> valueClass, final String query, final Object... parameters);

    /**
     * Executes the given CQL query and returns the first column of the first row converted to
     * {@code valueClass}, wrapped in an {@link Optional} that is guaranteed to be non-null when present.
     *
     * <p>Only the first column of the first row of the {@code ResultSet} is read; remaining rows and
     * columns are ignored. Parameters are bound positionally to {@code ?} placeholders.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code Optional.empty()} is returned <i>only</i> when the
     * query produces no rows. When a row is returned, the column value is wrapped in the
     * {@code Optional} via {@link Optional#of(Object)}, which does not accept a null payload — so if
     * the column value is {@code NULL} (or the conversion to {@code valueClass} yields {@code null}),
     * this method throws {@link NullPointerException} rather than returning {@code Optional.empty()}.
     * Use {@link #queryForSingleValue(Class, String, Object...)} (which returns {@link Nullable})
     * when the column may legitimately be {@code NULL}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Optional<Long> activeUsers = executor.queryForSingleNonNull(
     *     Long.class, "SELECT count(*) FROM users WHERE status = 'active'"); // returns present Optional with the count; empty only if zero result rows
     *
     * Optional<String> email = executor.queryForSingleNonNull(
     *     String.class, "SELECT email FROM users WHERE id = ?", userId); // returns present Optional with the email; empty when no row matches; throws NullPointerException if the matched value is NULL
     * }</pre>
     *
     * @param <V> the type of the single result value to be returned
     * @param valueClass the Java class the column value is converted to
     * @param query the CQL query string with {@code ?} placeholders for parameters
     * @param parameters the values to bind, in declaration order
     * @return a <i>present</i> {@code Optional<V>} holding the (non-null) column value when at least
     *         one row is returned with a non-null value; {@code Optional.empty()} when the query
     *         returns no rows
     * @throws IllegalArgumentException if {@code valueClass} or {@code query} is {@code null}
     * @throws NullPointerException if a row is returned but the column value (or its conversion) is
     *         {@code null}, because {@link Optional#of(Object)} rejects a null payload
     * @see #queryForSingleValue(Class, String, Object...)
     */
    public abstract <V> Optional<V> queryForSingleNonNull(final Class<V> valueClass, final String query, final Object... parameters);

    /**
     * Executes the given CQL query and returns the first row as a {@code Map<String, Object>} keyed by
     * column name.
     *
     * <p>Only the first row of the {@code ResultSet} is read; remaining rows are ignored. Parameters are
     * bound positionally to {@code ?} placeholders.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code Optional.empty()} is returned <i>only</i> when the
     * query produces no rows. When a row is found, the map is returned as a <i>present</i>
     * {@code Optional} — individual column values within the map may themselves be {@code null}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Optional<Map<String, Object>> user = executor.findFirst(
     *     "SELECT * FROM users WHERE status = ? LIMIT 1", "active"); // returns present Optional holding the first row as a Map; empty when no row matches
     * }</pre>
     *
     * @param query the CQL query string with {@code ?} placeholders for parameters
     * @param parameters the values to bind, in declaration order
     * @return a <i>present</i> {@code Optional} holding the first row as a {@code Map<String, Object>}
     *         when at least one row is returned; {@code Optional.empty()} when the query returns no rows
     * @see #findFirst(Class, String, Object...)
     */
    public final Optional<Map<String, Object>> findFirst(final String query, final Object... parameters) {
        return findFirst(Clazz.PROPS_MAP, query, parameters);
    }

    /**
     * Executes the given CQL query and returns the first row mapped to an instance of {@code targetClass}.
     *
     * <p>Only the first row of the {@code ResultSet} is read; remaining rows are ignored. Parameters are
     * bound positionally to {@code ?} placeholders.</p>
     *
     * <p>{@code targetClass} can be:</p>
     * <ul>
     * <li><strong>Entity class:</strong> A POJO with getter/setter methods matching column names</li>
     * <li><strong>{@code Map.class}:</strong> Results mapped to a {@code Map<String, Object>}</li>
     * <li><strong>Collection class:</strong> Results mapped to {@code List}, {@code Set}, ...</li>
     * <li><strong>Array class:</strong> Results mapped to {@code Object[]} or typed arrays</li>
     * </ul>
     *
     * <p><b>Empty vs. present semantics:</b> {@code Optional.empty()} is returned <i>only</i> when the
     * query produces no rows. When a row is found, the mapped value is returned as a <i>present</i>
     * {@code Optional}. Because {@code Optional} cannot carry a {@code null} payload, the mapped value
     * itself must be non-null.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Optional<User> user = executor.findFirst(User.class,
     *     "SELECT * FROM users WHERE email = ? LIMIT 1", email); // returns present Optional<User>; empty when no row matches
     *
     * Optional<Map> userData = executor.findFirst(Map.class,
     *     "SELECT name, email FROM users WHERE id = ?", userId); // returns present Optional holding a Map; empty when no row matches
     *
     * Optional<Object[]> row = executor.findFirst(Object[].class,
     *     "SELECT count(*), max(created_at) FROM events"); // returns present Optional holding an Object[] of the two aggregates
     * }</pre>
     *
     * @param <T> the type to map the result row to
     * @param targetClass an entity class with getter/setter methods, {@code Map.class}, a collection, or
     *        an array class
     * @param query the CQL query string with {@code ?} placeholders for parameters
     * @param parameters the values to bind, in declaration order
     * @return a <i>present</i> {@code Optional<T>} holding the first mapped row when at least one row is
     *         returned; {@code Optional.empty()} when the query returns no rows
     * @throws IllegalArgumentException if {@code targetClass} or {@code query} is {@code null}
     * @see #findFirst(String, Object...)
     * @see #findFirst(Class, Condition)
     */
    public abstract <T> Optional<T> findFirst(final Class<T> targetClass, final String query, final Object... parameters);

    /**
     * Executes the given CQL query and returns every row as a {@code Map<String, Object>} keyed by
     * column name.
     *
     * <p>Parameters are bound positionally to {@code ?} placeholders. The returned list is mutable
     * and never {@code null}; an empty list signals "no rows".</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Map<String, Object>> rows = executor.list(
     *     "SELECT name, email FROM users WHERE status = ?", "active"); // returns a (possibly empty) mutable List of row maps
     * }</pre>
     *
     * @param query the CQL query string with {@code ?} placeholders for parameters
     * @param parameters the values to bind, in declaration order
     * @return a list of result rows, each as a {@code Map<String, Object>} keyed by column name
     * @see #list(Class, String, Object...)
     */
    public final List<Map<String, Object>> list(final String query, final Object... parameters) {
        return list(Clazz.PROPS_MAP, query, parameters);
    }

    /**
     * Executes the given CQL query and returns every row mapped to an instance of {@code targetClass}.
     *
     * <p>Parameters are bound positionally to {@code ?} placeholders. The returned list is mutable
     * and never {@code null}; an empty list signals "no rows".</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<User> activeUsers = executor.list(User.class,
     *     "SELECT * FROM users WHERE status = ?", "active"); // returns a (possibly empty) mutable List<User>
     * }</pre>
     *
     * @param <T> the target type
     * @param targetClass an entity class with getter/setter methods matching column names,
     *        {@code Map.class}, or a basic single-value type
     * @param query the CQL query string with {@code ?} placeholders for parameters
     * @param parameters the values to bind, in declaration order
     * @return a list of result rows mapped to {@code targetClass}
     */
    public final <T> List<T> list(final Class<T> targetClass, final String query, final Object... parameters) {
        return toList(targetClass, execute(query, parameters));
    }

    /**
     * Executes the given CQL query and returns the result rows as a {@link Dataset}.
     *
     * <p>Each row is converted to a {@code Map<String, Object>} entry. Parameters are bound
     * positionally to {@code ?} placeholders. The returned Dataset is never {@code null};
     * an empty Dataset signals "no rows".</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Dataset ds = executor.query(
     *     "SELECT * FROM users WHERE status = ?", "active"); // returns a non-null Dataset (empty when no rows)
     * }</pre>
     *
     * @param query the CQL query string with {@code ?} placeholders for parameters
     * @param parameters the values to bind, in declaration order
     * @return a Dataset containing all result rows
     * @see #query(Class, String, Object...)
     */
    public final Dataset query(final String query, final Object... parameters) {
        return query(Map.class, query, parameters);
    }

    /**
     * Executes the given CQL query and returns the result rows as a {@link Dataset} with rows
     * shaped according to {@code targetClass}.
     *
     * <p>Parameters are bound positionally to {@code ?} placeholders. The returned Dataset is
     * never {@code null}; an empty Dataset signals "no rows".</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Dataset userDataset = executor.query(User.class,
     *     "SELECT * FROM users WHERE age > ?", 18); // returns a non-null Dataset shaped by User (empty when no rows)
     * }</pre>
     *
     * @param targetClass an entity class with getter/setter methods matching column names,
     *        or {@code Map.class}
     * @param query the CQL query string with {@code ?} placeholders for parameters
     * @param parameters the values to bind, in declaration order
     * @return a Dataset containing all result rows shaped according to {@code targetClass}
     */
    public final Dataset query(final Class<?> targetClass, final String query, final Object... parameters) {
        return extractData(targetClass, execute(query, parameters));
    }

    /**
     * Executes the given CQL query and returns the result rows as a lazy stream of {@code Object[]}.
     *
     * <p>Each emitted array has one element per selected column, ordered by the column index in the
     * result set. Parameters are bound positionally to {@code ?} placeholders.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Stream<Object[]> rows = executor.stream(
     *     "SELECT id, name FROM users WHERE status = ?", "active"); // returns a lazy Stream where each element is {id, name}
     * }</pre>
     *
     * @param query the CQL query string with {@code ?} placeholders for parameters
     * @param parameters the values to bind, in declaration order
     * @return a stream of result rows, each as an {@code Object[]} of column values
     * @see #stream(Class, String, Object...)
     */
    public final Stream<Object[]> stream(final String query, final Object... parameters) {
        return stream(Object[].class, query, parameters);
    }

    /**
     * Executes the given CQL query and returns the result rows as a lazy stream of
     * {@code targetClass} instances.
     *
     * <p>Rows are mapped on demand by the row mapper returned from
     * {@link #createRowMapper(Class)}. Parameters are bound positionally to {@code ?} placeholders.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * executor.stream(User.class, "SELECT * FROM users") // returns a lazy Stream<User>
     *         .filter(u -> u.getAge() > 21)
     *         .forEach(System.out::println);
     * }</pre>
     *
     * @param <T> the target type
     * @param targetClass an entity class with getter/setter methods matching column names,
     *        or {@code Map.class}
     * @param query the CQL query string with {@code ?} placeholders for parameters
     * @param parameters the values to bind, in declaration order
     * @return a stream of result rows mapped to {@code targetClass}
     */
    public final <T> Stream<T> stream(final Class<T> targetClass, final String query, final Object... parameters) {
        return Stream.of(execute(query, parameters).iterator()).map(createRowMapper(targetClass));
    }

    /**
     * Executes the supplied driver statement and returns the result rows as a lazy stream of
     * {@code targetClass} instances.
     *
     * <p>Rows are mapped on demand by the row mapper returned from
     * {@link #createRowMapper(Class)}. The statement is run via {@link #execute(Object)}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // 'boundStatement' is a driver statement (e.g. a BoundStatement) of type ST
     * Stream<User> users = executor.stream(User.class, boundStatement); // returns a lazy Stream<User> over the statement's rows
     * }</pre>
     *
     * @param <T> the target type
     * @param targetClass an entity class with getter/setter methods matching column names,
     *        or {@code Map.class}
     * @param statement the driver statement to execute (for example a bound or batch statement)
     * @return a stream of result rows mapped to {@code targetClass}
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
     * executor.execute("CREATE TABLE IF NOT EXISTS users (id UUID PRIMARY KEY, name TEXT)"); // returns the driver ResultSet (empty for DDL)
     *
     * // Simple query
     * ResultSet results = executor.execute("SELECT * FROM users LIMIT 10"); // returns the driver ResultSet of matching rows
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
     * @throws NullPointerException if query is null
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
     *     "active", yesterday); // returns the driver ResultSet of matching rows
     *
     * // Named parameters with map
     * Map<String, Object> params = new HashMap<>();
     * params.put("email", "john@example.com");
     * params.put("status", "active");
     * ResultSet result = executor.execute(
     *     "SELECT * FROM users WHERE email = :email AND status = :status", params); // returns the driver ResultSet of matching rows
     *
     * // Entity parameters
     * User searchCriteria = new User();
     * searchCriteria.setStatus("active");
     * searchCriteria.setDepartment("engineering");
     * ResultSet engineeringUsers = executor.execute(
     *     "SELECT * FROM users WHERE status = :status AND department = :department",
     *     searchCriteria); // returns the driver ResultSet of matching rows (bean property binding)
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
     * @throws RuntimeException if statement is null (the v4 driver rejects it with
     *         IllegalArgumentException, the v3 driver with NullPointerException)
     */
    public abstract RS execute(final ST statement);

    /**
     * Executes the CQL string and bound positional parameters carried by the supplied {@link SP}
     * pair and returns the driver result set.
     *
     * <p>This is a convenience for the common case where {@link CqlBuilder} produces an
     * {@code SP} (statement + parameters) pair; it dispatches to
     * {@link #execute(String, Object...)}.</p>
     *
     * @param cp the parameterized CQL statement to execute
     * @return the driver result set
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
        N.checkArgNotNull(entity, "entity");

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
        N.checkArgNotNull(entity, "entity");
        N.checkArgument(N.notEmpty(propNamesToUpdate), "'propNamesToUpdate' can't be null or empty.");

        final Class<?> targetClass = entity.getClass();
        final Condition cond = entityToCondition(entity);

        switch (namingPolicy) {
            case SNAKE_CASE:
                return NSC.update(targetClass).set(Beans.beanToMap(entity, propNamesToUpdate)).where(cond).build();

            case SCREAMING_SNAKE_CASE:
                return NAC.update(targetClass).set(Beans.beanToMap(entity, propNamesToUpdate)).where(cond).build();

            case CAMEL_CASE:
                return NLC.update(targetClass).set(Beans.beanToMap(entity, propNamesToUpdate)).where(cond).build();

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
        N.checkArgNotNull(whereClause, "whereClause");

        switch (namingPolicy) {
            case SNAKE_CASE:
                return NSC.update(targetClass).set(props).where(whereClause).build();

            case SCREAMING_SNAKE_CASE:
                return NAC.update(targetClass).set(props).where(whereClause).build();

            case CAMEL_CASE:
                return NLC.update(targetClass).set(props).where(whereClause).build();

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
        N.checkArgNotNull(whereClause, "whereClause");

        switch (namingPolicy) {
            case SNAKE_CASE:
                if (N.isEmpty(propNamesToDelete)) {
                    return NSC.deleteFrom(targetClass).where(whereClause).build();
                } else {
                    return NSC.delete(propNamesToDelete).from(targetClass).where(whereClause).build();
                }

            case SCREAMING_SNAKE_CASE:
                if (N.isEmpty(propNamesToDelete)) {
                    return NAC.deleteFrom(targetClass).where(whereClause).build();
                } else {
                    return NAC.delete(propNamesToDelete).from(targetClass).where(whereClause).build();
                }

            case CAMEL_CASE:
                if (N.isEmpty(propNamesToDelete)) {
                    return NLC.deleteFrom(targetClass).where(whereClause).build();
                } else {
                    return NLC.delete(propNamesToDelete).from(targetClass).where(whereClause).build();
                }

            default:
                throw new RuntimeException("Unsupported naming policy: " + namingPolicy);
        }
    }

    /**
     * Prepares a SELECT query for the specified entity class and conditions.
     *
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param whereClause the WHERE condition
     * @return the prepared statement with parameters
     */
    protected SP prepareQuery(final Class<?> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
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
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param whereClause the WHERE condition
     * @param count the maximum number of results to return (0 for no limit)
     * @return an SP (Statement/Parameters) pair ready for execution
     */
    protected SP prepareQuery(final Class<?> targetClass, final Collection<String> selectPropNames, final Condition whereClause, final int count) {
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
     * String parameterizedCql = parsed2.parameterizedCql();
     * int paramCount = parsed2.parameterCount();
     * }</pre>
     *
     * @param cql the CQL query string to parse (may be a mapper key or raw CQL)
     * @return a ParsedCql object containing the parsed query and metadata
     * @see CqlMapper#get(String)
     * @see ParsedCql#parse(String)
     */
    protected ParsedCql parseCql(final String cql) {
        ParsedCql parsedCql = null;

        if (cqlMapper != null) {
            parsedCql = cqlMapper.get(cql);
        }

        if (parsedCql == null) {
            parsedCql = ParsedCql.parse(cql);
        }

        return parsedCql;
    }

    /**
     * Converts a result set to a List of the specified type.
     *
     * @param <T> the target type
     * @param targetClass the entity class
     * @param resultSet the result set to convert
     * @return a List of mapped objects
     */
    protected abstract <T> List<T> toList(Class<T> targetClass, RS resultSet);

    /**
     * Extracts data from a result set into a Dataset.
     *
     * @param targetClass the entity class
     * @param resultSet the result set to extract from
     * @return a Dataset containing the extracted data
     */
    protected abstract Dataset extractData(Class<?> targetClass, RS resultSet);

    /**
     * Fetches exactly one row from a result set.
     *
     * @param <T> the target type
     * @param targetClass the entity class
     * @param resultSet the result set to read from
     * @return the mapped row, or null if no row is present
     * @throws DuplicateResultException if more than one row is present
     */
    protected abstract <T> T fetchOnlyOne(Class<T> targetClass, RS resultSet);

    /**
     * Creates a row mapper function for the specified target class.
     *
     * @param <T> the target type
     * @param targetClass the entity class
     * @return a function that maps result rows to the target type
     */
    protected abstract <T> Function<RW, T> createRowMapper(Class<T> targetClass);

    /**
     * Reads the first column of the given row and converts it to the target type.
     *
     * <p>Used by the single-value query paths (e.g. {@code queryForSingleValue}) to extract
     * a scalar from the first column of the first row.</p>
     *
     * @param <T> the target type
     * @param row the driver row to read from
     * @param targetClass the type to convert the first column value to
     * @return the converted first-column value (may be null)
     */
    protected abstract <T> T readFirstColumn(RW row, Class<T> targetClass);
}
