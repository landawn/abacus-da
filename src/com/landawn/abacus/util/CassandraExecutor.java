/*
 * Copyright (C) 2015 HaiYang Li
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.landawn.abacus.util;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.landawn.abacus.DataSet;
import com.landawn.abacus.DirtyMarker;
import com.landawn.abacus.annotation.Beta;
import com.landawn.abacus.condition.And;
import com.landawn.abacus.condition.Condition;
import com.landawn.abacus.condition.ConditionFactory.CF;
import com.landawn.abacus.core.RowDataSet;
import com.landawn.abacus.exception.DuplicatedResultException;
import com.landawn.abacus.pool.KeyedObjectPool;
import com.landawn.abacus.pool.PoolFactory;
import com.landawn.abacus.pool.PoolableWrapper;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.CQLBuilder.CP;
import com.landawn.abacus.util.CQLBuilder.NAC;
import com.landawn.abacus.util.CQLBuilder.NLC;
import com.landawn.abacus.util.CQLBuilder.NSC;
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
import com.landawn.abacus.util.function.BiFunction;
import com.landawn.abacus.util.function.Function;
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
 * It's a simple wrapper of Cassandra Java client.
 * Raw/parameterized cql are supported. The parameters can be array/list/map/entity:
 * <li> row parameterized cql with question mark: <code>SELECT * FROM account WHERE id = ?</li>
 * <li> Parameterized cql with named parameter: <code>SELECT * FROM account WHERE id = :id</li>
 * 
 * <br />
 * <code>CQLBuilder</code> is designed to build CQL.
 * 
 * @since 0.8
 * 
 * @author Haiyang Li
 * 
 * @see CQLBuilder
 * @see {@link com.datastax.driver.core.Cluster}
 * @see {@link com.datastax.driver.core.Session}
 */
public final class CassandraExecutor implements Closeable {
    static final List<String> EXISTS_SELECT_PROP_NAMES = ImmutableList.of("1");
    static final List<String> COUNT_SELECT_PROP_NAMES = ImmutableList.of(NSC.COUNT_ALL);

    static final int POOLABLE_LENGTH = 1024;

    private static final Map<String, Class<?>> namedDataType = new HashMap<String, Class<?>>();

    static {
        namedDataType.put("BOOLEAN", Boolean.class);
        namedDataType.put("CHAR", Character.class);
        namedDataType.put("Character", Character.class);
        namedDataType.put("TINYINT", Byte.class);
        namedDataType.put("SMALLINT", Short.class);
        namedDataType.put("INT", Integer.class);
        namedDataType.put("BIGINT", Long.class);
        namedDataType.put("FLOAT", Float.class);
        namedDataType.put("DOUBLE", Double.class);
        namedDataType.put("BIGINT", Long.class);
        namedDataType.put("VARINT", BigInteger.class);
        namedDataType.put("DECIMAL", BigDecimal.class);
        namedDataType.put("TEXT", String.class);
        namedDataType.put("ASCII", String.class);
        namedDataType.put("INET", InetAddress.class);
        namedDataType.put("TIME", Long.class);

        try {
            namedDataType.put("DATE", ClassUtil.forClass("com.datastax.driver.core.LocalDate"));
        } catch (Exception e) {
            // ignore
        }

        namedDataType.put("TIMESTAMP", Date.class);
        namedDataType.put("VARCHAR", String.class);
        namedDataType.put("BLOB", ByteBuffer.class);
        namedDataType.put("COUNTER", Long.class);
        namedDataType.put("UUID", UUID.class);
        namedDataType.put("TIMEUUID", UUID.class);
        namedDataType.put("LIST", List.class);
        namedDataType.put("SET", Set.class);
        namedDataType.put("MAP", Map.class);
        namedDataType.put("UDT", UDTValue.class);
        namedDataType.put("TUPLE", TupleValue.class);
        namedDataType.put("CUSTOM", ByteBuffer.class);
    }

    private final KeyedObjectPool<String, PoolableWrapper<Statement>> stmtPool = PoolFactory.createKeyedObjectPool(1024, 3000);
    private final KeyedObjectPool<String, PoolableWrapper<PreparedStatement>> preStmtPool = PoolFactory.createKeyedObjectPool(1024, 3000);

    private final CQLMapper cqlMapper;

    private final Cluster cluster;
    private final Session session;
    private final CodecRegistry codecRegistry;
    private final MappingManager mappingManager;
    private final StatementSettings settings;
    private final NamingPolicy namingPolicy;
    private final AsyncExecutor asyncExecutor;

    public CassandraExecutor(final Session session) {
        this(session, null);
    }

    public CassandraExecutor(final Session session, final StatementSettings settings) {
        this(session, settings, (AsyncExecutor) null);
    }

    /**
     * 
     * @param session
     * @param settings
     * @param asyncExecutor
     * @see {@link com.datastax.driver.core.Session#executeAsync}
     * @deprecated {@code asyncExecutor} is not used anymore. {@code Session.executeAsync} is called.
     */
    @Deprecated
    public CassandraExecutor(final Session session, final StatementSettings settings, final AsyncExecutor asyncExecutor) {
        this(session, settings, null, null, asyncExecutor);
    }

    public CassandraExecutor(final Session session, final StatementSettings settings, final CQLMapper cqlMapper) {
        this(session, settings, cqlMapper, null);
    }

    public CassandraExecutor(final Session session, final StatementSettings settings, final CQLMapper cqlMapper, final NamingPolicy namingPolicy) {
        this(session, settings, cqlMapper, namingPolicy, null);
    }

    /**
     * 
     * @param session
     * @param settings
     * @param cqlMapper
     * @param namingPolicy
     * @param asyncExecutor
     * @see {@link com.datastax.driver.core.Session#executeAsync}
     * @deprecated {@code asyncExecutor} is not used anymore. {@code Session.executeAsync} is called.
     */
    @Deprecated
    public CassandraExecutor(final Session session, final StatementSettings settings, final CQLMapper cqlMapper, final NamingPolicy namingPolicy,
            final AsyncExecutor asyncExecutor) {
        this.cluster = session.getCluster();
        this.session = session;
        this.codecRegistry = cluster.getConfiguration().getCodecRegistry();
        this.mappingManager = new MappingManager(session);

        if (settings == null) {
            this.settings = null;
        } else {
            this.settings = settings.copy();
        }

        this.cqlMapper = cqlMapper;
        this.namingPolicy = namingPolicy == null ? NamingPolicy.LOWER_CASE_WITH_UNDERSCORE : namingPolicy;
        this.asyncExecutor = asyncExecutor == null ? new AsyncExecutor(64, 300, TimeUnit.SECONDS) : asyncExecutor;
    }

    AsyncExecutor asyncExecutor() {
        return asyncExecutor;
    }

    public Cluster cluster() {
        return cluster;
    }

    public Session session() {
        return session;
    }

    public <T> Mapper<T> mapper(Class<T> targetClass) {
        return mappingManager.mapper(targetClass);
    }

    private static final Map<Class<?>, Tuple2<List<String>, Set<String>>> entityKeyNamesMap = new ConcurrentHashMap<>();

    public static void registerKeys(Class<?> entityClass, Collection<String> keyNames) {
        N.checkArgument(N.notNullOrEmpty(keyNames), "'keyNames' can't be null or empty");

        final Set<String> keyNameSet = new LinkedHashSet<>(N.initHashCapacity(keyNames.size()));

        for (String keyName : keyNames) {
            keyNameSet.add(ClassUtil.getPropNameByMethod(ClassUtil.getPropGetMethod(entityClass, keyName)));
        }

        entityKeyNamesMap.put(entityClass, Tuple.<List<String>, Set<String>> of(ImmutableList.copyOf(keyNameSet), ImmutableSet.of(keyNameSet)));
    }

    private static List<String> getKeyNames(final Class<?> entityClass) {
        Tuple2<List<String>, Set<String>> tp = entityKeyNamesMap.get(entityClass);

        if (tp == null) {
            List<String> idPropNames = ClassUtil.getIdFieldNames(entityClass);
            tp = Tuple.<List<String>, Set<String>> of(ImmutableList.copyOf(idPropNames), ImmutableSet.copyOf(idPropNames));
            entityKeyNamesMap.put(entityClass, tp);
        }

        return tp._1;
    }

    private static Set<String> getKeyNameSet(final Class<?> entityClass) {
        Tuple2<List<String>, Set<String>> tp = entityKeyNamesMap.get(entityClass);

        if (tp == null) {
            List<String> idPropNames = ClassUtil.getIdFieldNames(entityClass);
            tp = Tuple.<List<String>, Set<String>> of(ImmutableList.copyOf(idPropNames), ImmutableSet.copyOf(idPropNames));
            entityKeyNamesMap.put(entityClass, tp);
        }

        return tp._2;
    }

    public static DataSet extractData(final ResultSet resultSet) {
        return extractData(null, resultSet);
    }

    /**
     * 
     * @param targetClass an entity class with getter/setter method or <code>Map.class</code>
     * @param resultSet
     * @return
     */
    public static DataSet extractData(final Class<?> targetClass, final ResultSet resultSet) {
        final boolean isEntity = targetClass != null && ClassUtil.isEntity(targetClass);
        final boolean isMap = targetClass != null && Map.class.isAssignableFrom(targetClass);
        final ColumnDefinitions columnDefinitions = resultSet.getColumnDefinitions();
        final int columnCount = columnDefinitions.size();
        final List<Row> rowList = resultSet.all();
        final int rowCount = N.isNullOrEmpty(rowList) ? 0 : rowList.size();

        final List<String> columnNameList = new ArrayList<>(columnCount);
        final List<List<Object>> columnList = new ArrayList<>(columnCount);
        final Class<?>[] columnClasses = new Class<?>[columnCount];

        for (int i = 0; i < columnCount; i++) {
            columnNameList.add(columnDefinitions.getName(i));
            columnList.add(new ArrayList<>(rowCount));
            columnClasses[i] = isEntity ? ClassUtil.getPropGetMethod(targetClass, columnNameList.get(i)).getReturnType() : (isMap ? Map.class : Object[].class);
        }

        Object propValue = null;

        for (Row row : rowList) {
            for (int i = 0; i < columnCount; i++) {
                propValue = row.getObject(i);

                if (propValue instanceof Row && (columnClasses[i] == null || !columnClasses[i].isAssignableFrom(Row.class))) {
                    columnList.get(i).add(readRow(columnClasses[i], (Row) propValue));
                } else if (propValue == null || targetClass == null || isMap || columnClasses[i] == null
                        || columnClasses[i].isAssignableFrom(propValue.getClass())) {
                    columnList.get(i).add(propValue);
                } else {
                    columnList.get(i).add(N.convert(propValue, columnClasses[i]));
                }
            }
        }

        return new RowDataSet(columnNameList, columnList);
    }

    private static Object readRow(final Class<?> rowClass, final Row row) {
        final Type<?> rowType = rowClass == null ? null : N.typeOf(rowClass);
        final ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
        final int columnCount = columnDefinitions.size();
        Object res = null;
        Object propValue = null;

        if (rowType == null || rowType.isObjectArray()) {
            final Object[] a = new Object[columnCount];

            for (int i = 0; i < columnCount; i++) {
                propValue = row.getObject(i);

                if (propValue instanceof Row) {
                    a[i] = readRow(Object[].class, (Row) propValue);
                } else {
                    a[i] = propValue;
                }
            }

            res = a;
        } else if (rowType.isCollection()) {
            final Collection<Object> c = (Collection<Object>) N.newInstance(rowClass);

            for (int i = 0; i < columnCount; i++) {
                propValue = row.getObject(i);

                if (propValue instanceof Row) {
                    c.add(readRow(List.class, (Row) propValue));
                } else {
                    c.add(propValue);
                }
            }

            res = c;
        } else if (rowType.isMap()) {
            final Map<String, Object> m = (Map<String, Object>) N.newInstance(rowClass);

            for (int i = 0; i < columnCount; i++) {
                propValue = row.getObject(i);

                if (propValue instanceof Row) {
                    m.put(columnDefinitions.getName(i), readRow(Map.class, (Row) propValue));
                } else {
                    m.put(columnDefinitions.getName(i), propValue);
                }
            }

            res = m;
        } else if (rowType.isEntity()) {
            res = toEntity(rowClass, row);
        } else {
            throw new IllegalArgumentException("Unsupported row/column type: " + ClassUtil.getCanonicalClassName(rowClass));
        }

        return res;
    }

    /**
     * 
     * @param targetClass an entity class with getter/setter method, <code>Map.class</code> or basic single value type(Primitive/String/Date...)
     * @param resultSet
     * @return
     */
    public static <T> List<T> toList(final Class<T> targetClass, final ResultSet resultSet) {
        if (targetClass.isAssignableFrom(Row.class)) {
            return (List<T>) resultSet.all();
        }

        final Type<T> type = N.typeOf(targetClass);
        final ColumnDefinitions columnDefinitions = resultSet.getColumnDefinitions();
        final List<Row> rowList = resultSet.all();
        final List<Object> resultList = new ArrayList<>();

        if (type.isEntity() || type.isMap()) {
            for (Row row : rowList) {
                resultList.add(toEntity(targetClass, row, columnDefinitions));
            }
        } else if (columnDefinitions.size() == 1) {
            if (rowList.size() > 0) {
                if (rowList.get(0).getObject(0) != null && targetClass.isAssignableFrom(rowList.get(0).getObject(0).getClass())) {
                    for (Row row : rowList) {
                        resultList.add(row.getObject(0));
                    }
                } else {
                    for (Row row : rowList) {
                        resultList.add(N.convert(row.getObject(0), targetClass));
                    }
                }
            }
        } else {
            throw new IllegalArgumentException(
                    "Can't covert result with columns: " + columnDefinitions.toString() + " to class: " + ClassUtil.getCanonicalClassName(targetClass));
        }

        return (List<T>) resultList;
    }

    /**
     * 
     * @param targetClass an entity class with getter/setter method or <code>Map.class</code>
     * @param row
     * @return
     */
    public static <T> T toEntity(final Class<T> targetClass, final Row row) {
        checkTargetClass(targetClass);

        if (row == null) {
            return null;
        }

        return toEntity(targetClass, row, row.getColumnDefinitions());
    }

    @SuppressWarnings("deprecation")
    static <T> T toEntity(final Class<T> targetClass, final Row row, final ColumnDefinitions columnDefinitions) {
        final int columnCount = columnDefinitions.size();

        if (Map.class.isAssignableFrom(targetClass)) {
            final Map<String, Object> map = (Map<String, Object>) N.newInstance(targetClass);

            String propName = null;
            Object propValue = null;

            for (int i = 0; i < columnCount; i++) {
                propName = columnDefinitions.getName(i);
                propValue = row.getObject(i);

                if (propValue instanceof Row) {
                    map.put(propName, toEntity(Map.class, (Row) propValue));
                } else {
                    map.put(propName, propValue);
                }
            }

            return (T) map;
        } else if (ClassUtil.isEntity(targetClass)) {
            final T entity = N.newInstance(targetClass);

            String propName = null;
            Object propValue = null;
            Method propSetMethod = null;
            Class<?> parameterType = null;

            for (int i = 0; i < columnCount; i++) {
                propName = columnDefinitions.getName(i);
                propSetMethod = ClassUtil.getPropSetMethod(targetClass, propName);

                if (propSetMethod == null) {
                    if (propName.indexOf(WD._PERIOD) > 0) {
                        ClassUtil.setPropValue(entity, propName, row.getObject(i), true);
                    }

                    continue;
                }

                parameterType = propSetMethod.getParameterTypes()[0];
                propValue = row.getObject(i);

                if (propValue == null || parameterType.isAssignableFrom(propValue.getClass())) {
                    ClassUtil.setPropValue(entity, propSetMethod, propValue);
                } else {
                    if (propValue instanceof Row) {
                        if (Map.class.isAssignableFrom(parameterType) || ClassUtil.isEntity(parameterType)) {
                            ClassUtil.setPropValue(entity, propSetMethod, toEntity(parameterType, (Row) propValue));
                        } else {
                            ClassUtil.setPropValue(entity, propSetMethod, N.valueOf(parameterType, N.stringOf(toEntity(Map.class, (Row) propValue))));
                        }
                    } else {
                        ClassUtil.setPropValue(entity, propSetMethod, propValue);
                    }
                }
            }

            if (ClassUtil.isDirtyMarker(entity.getClass())) {
                ((DirtyMarker) entity).markDirty(false);
            }

            return entity;
        } else if (columnDefinitions.size() == 1) {
            return N.convert(row.getObject(0), targetClass);
        } else {
            throw new IllegalArgumentException("Unsupported target type: " + targetClass);
        }
    }

    static Condition ids2Cond(final Class<?> targetClass, final Object... ids) {
        N.checkArgNotNullOrEmpty(ids, "ids");

        final List<String> keyNames = getKeyNames(targetClass);

        if (keyNames.size() == 1 && ids.length == 1) {
            return CF.eq(keyNames.get(0), ids[0]);
        } else if (ids.length <= keyNames.size()) {
            final Iterator<String> iter = keyNames.iterator();
            final And and = new And();

            for (Object id : ids) {
                and.add(CF.eq(iter.next(), id));
            }

            return and;
        } else {
            throw new IllegalArgumentException("The number: " + ids.length + " of input ids doesn't match the (registered) key names: "
                    + (keyNames == null ? "[id]" : N.toString(keyNames)) + " in class: " + ClassUtil.getCanonicalClassName(targetClass));
        }
    }

    static Condition entity2Cond(final Object entity) {
        final Class<?> targetClass = entity.getClass();
        final List<String> keyNames = getKeyNames(targetClass);

        if (keyNames.size() == 1) {
            return CF.eq(keyNames.get(0), ClassUtil.getPropValue(entity, keyNames.get(0)));
        } else {
            final And and = new And();
            Object propVal = null;

            for (String keyName : keyNames) {
                propVal = ClassUtil.getPropValue(entity, keyName);

                if (propVal == null || (propVal instanceof CharSequence) && N.isNullOrEmpty(((CharSequence) propVal))) {
                    break;
                }

                and.add(CF.eq(keyName, ClassUtil.getPropValue(entity, keyName)));
            }

            if (N.isNullOrEmpty(and.getConditions())) {
                throw new IllegalArgumentException("No property value specified in entity for key names: " + keyNames);
            }

            return and;
        }
    }

    @SafeVarargs
    public final <T> Optional<T> get(final Class<T> targetClass, final Object... ids) throws DuplicatedResultException {
        return get(targetClass, null, ids);
    }

    @SafeVarargs
    public final <T> Optional<T> get(final Class<T> targetClass, final Collection<String> selectPropNames, final Object... ids)
            throws DuplicatedResultException {
        return get(targetClass, selectPropNames, ids2Cond(targetClass, ids));
    }

    public <T> Optional<T> get(final Class<T> targetClass, final Condition whereCause) throws DuplicatedResultException {
        return get(targetClass, null, whereCause);
    }

    /**
     * 
     * @param targetClass
     * @param selectPropNames
     * @param whereCause
     * @return
     * @throws DuplicatedResultException if more than one record found.
     */
    public <T> Optional<T> get(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereCause)
            throws DuplicatedResultException {
        return Optional.ofNullable(gett(targetClass, selectPropNames, whereCause));
    }

    @SafeVarargs
    public final <T> T gett(final Class<T> targetClass, final Object... ids) throws DuplicatedResultException {
        return gett(targetClass, null, ids);
    }

    @SafeVarargs
    public final <T> T gett(final Class<T> targetClass, final Collection<String> selectPropNames, final Object... ids) throws DuplicatedResultException {
        return gett(targetClass, selectPropNames, ids2Cond(targetClass, ids));
    }

    public <T> T gett(final Class<T> targetClass, final Condition whereCause) throws DuplicatedResultException {
        return gett(targetClass, null, whereCause);
    }

    /**
     * 
     * @param targetClass
     * @param selectPropNames
     * @param whereCause
     * @return
     * @throws DuplicatedResultException if more than one record found.
     */
    public <T> T gett(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereCause) throws DuplicatedResultException {
        final CP cp = prepareQuery(targetClass, selectPropNames, whereCause, 2);
        final ResultSet resultSet = execute(cp);
        final Row row = resultSet.one();

        if (row == null) {
            return null;
        } else if (resultSet.isExhausted()) {
            return toEntity(targetClass, row);
        } else {
            throw new DuplicatedResultException();
        }
    }

    public ResultSet insert(final Object entity) {
        final CP cp = prepareInsert(entity);

        return execute(cp);
    }

    private CP prepareInsert(final Object entity) {
        final Class<?> targetClass = entity.getClass();

        switch (namingPolicy) {
            case LOWER_CASE_WITH_UNDERSCORE:
                return NSC.insert(entity).into(targetClass).pair();

            case UPPER_CASE_WITH_UNDERSCORE:
                return NAC.insert(entity).into(targetClass).pair();

            case LOWER_CAMEL_CASE:
                return NLC.insert(entity).into(targetClass).pair();

            default:
                throw new RuntimeException("Unsupported naming policy: " + namingPolicy);
        }
    }

    public ResultSet insert(final Class<?> targetClass, final Map<String, Object> props) {
        final CP cp = prepareInsert(targetClass, props);

        return execute(cp);
    }

    private CP prepareInsert(final Class<?> targetClass, final Map<String, Object> props) {
        switch (namingPolicy) {
            case LOWER_CASE_WITH_UNDERSCORE:
                return NSC.insert(props).into(targetClass).pair();

            case UPPER_CASE_WITH_UNDERSCORE:
                return NAC.insert(props).into(targetClass).pair();

            case LOWER_CAMEL_CASE:
                return NLC.insert(props).into(targetClass).pair();

            default:
                throw new RuntimeException("Unsupported naming policy: " + namingPolicy);
        }
    }

    public ResultSet batchInsert(final Collection<?> entities, final BatchStatement.Type type) {
        final BatchStatement batchStatement = prepareBatchInsertStatement(entities, type);

        return execute(batchStatement);
    }

    private BatchStatement prepareBatchInsertStatement(final Collection<?> entities, final BatchStatement.Type type) {
        N.checkArgument(N.notNullOrEmpty(entities), "'entities' can't be null or empty.");

        final BatchStatement batchStatement = prepareBatchStatement(type);
        CP cp = null;

        for (Object entity : entities) {
            cp = prepareInsert(entity);
            batchStatement.add(prepareStatement(cp.cql, cp.parameters.toArray()));
        }

        return batchStatement;
    }

    private BatchStatement prepareBatchStatement(final BatchStatement.Type type) {
        final BatchStatement batchStatement = new BatchStatement(type == null ? BatchStatement.Type.LOGGED : type);

        if (settings != null) {
            batchStatement.setConsistencyLevel(settings.getConsistency());
            batchStatement.setSerialConsistencyLevel(settings.getSerialConsistency());
            batchStatement.setRetryPolicy(settings.getRetryPolicy());

            if (settings.traceQuery) {
                batchStatement.enableTracing();
            } else {
                batchStatement.disableTracing();
            }
        }

        return batchStatement;
    }

    public ResultSet batchInsert(final Class<?> targetClass, final Collection<? extends Map<String, Object>> propsList, final BatchStatement.Type type) {
        final BatchStatement batchStatement = prepareBatchInsertStatement(targetClass, propsList, type);

        return execute(batchStatement);
    }

    private BatchStatement prepareBatchInsertStatement(final Class<?> targetClass, final Collection<? extends Map<String, Object>> propsList,
            final BatchStatement.Type type) {
        N.checkArgument(N.notNullOrEmpty(propsList), "'propsList' can't be null or empty.");

        final BatchStatement batchStatement = prepareBatchStatement(type);
        CP cp = null;

        for (Map<String, Object> props : propsList) {
            cp = prepareInsert(targetClass, props);
            batchStatement.add(prepareStatement(cp.cql, cp.parameters.toArray()));
        }

        return batchStatement;
    }

    public ResultSet update(final Object entity) {
        return update(entity, getKeyNameSet(entity.getClass()));
    }

    public ResultSet update(final Object entity, final Set<String> primaryKeyNames) {
        final CP cp = prepareUpdate(entity, primaryKeyNames);

        return execute(cp);
    }

    private CP prepareUpdate(final Object entity, final Set<String> primaryKeyNames) {
        N.checkArgument(N.notNullOrEmpty(primaryKeyNames), "'primaryKeyNames' can't be null or empty.");

        final Class<?> targetClass = entity.getClass();
        final And and = new And();

        for (String keyName : primaryKeyNames) {
            and.add(CF.eq(keyName, ClassUtil.getPropValue(entity, keyName)));
        }

        switch (namingPolicy) {
            case LOWER_CASE_WITH_UNDERSCORE:
                return NSC.update(targetClass).set(entity, primaryKeyNames).where(and).pair();

            case UPPER_CASE_WITH_UNDERSCORE:
                return NAC.update(targetClass).set(entity, primaryKeyNames).where(and).pair();

            case LOWER_CAMEL_CASE:
                return NLC.update(targetClass).set(entity, primaryKeyNames).where(and).pair();

            default:
                throw new RuntimeException("Unsupported naming policy: " + namingPolicy);
        }
    }

    public ResultSet update(final Class<?> targetClass, final Map<String, Object> props, final Condition whereCause) {
        N.checkArgument(N.notNullOrEmpty(props), "'props' can't be null or empty.");

        final CP cp = prepareUpdate(targetClass, props, whereCause);

        return execute(cp);
    }

    private CP prepareUpdate(final Class<?> targetClass, final Map<String, Object> props, final Condition whereCause) {
        switch (namingPolicy) {
            case LOWER_CASE_WITH_UNDERSCORE:
                return NSC.update(targetClass).set(props).where(whereCause).pair();

            case UPPER_CASE_WITH_UNDERSCORE:
                return NAC.update(targetClass).set(props).where(whereCause).pair();

            case LOWER_CAMEL_CASE:
                return NLC.update(targetClass).set(props).where(whereCause).pair();

            default:
                throw new RuntimeException("Unsupported naming policy: " + namingPolicy);
        }
    }

    public ResultSet batchUpdate(final Collection<?> entities, final BatchStatement.Type type) {
        N.checkArgument(N.notNullOrEmpty(entities), "'entities' can't be null or empty.");

        return batchUpdate(entities, getKeyNameSet(N.firstOrNullIfEmpty(entities).getClass()), type);
    }

    public ResultSet batchUpdate(final Collection<?> entities, final Set<String> primaryKeyNames, final BatchStatement.Type type) {
        N.checkArgument(N.notNullOrEmpty(entities), "'entities' can't be null or empty.");

        final BatchStatement batchStatement = prepareBatchUpdateStatement(entities, primaryKeyNames, type);

        return execute(batchStatement);
    }

    private BatchStatement prepareBatchUpdateStatement(final Collection<?> entities, final Set<String> primaryKeyNames, final BatchStatement.Type type) {
        N.checkArgument(N.notNullOrEmpty(entities), "'entities' can't be null or empty.");
        N.checkArgument(N.notNullOrEmpty(primaryKeyNames), "'primaryKeyNames' can't be null or empty");

        final BatchStatement batchStatement = prepareBatchStatement(type);

        for (Object entity : entities) {
            final CP cp = prepareUpdate(entity, primaryKeyNames);
            batchStatement.add(prepareStatement(cp.cql, cp.parameters.toArray()));
        }

        return batchStatement;
    }

    public ResultSet batchUpdate(final Class<?> targetClass, final Collection<? extends Map<String, Object>> propsList, final Set<String> primaryKeyNames,
            final BatchStatement.Type type) {
        final BatchStatement batchStatement = prepareBatchUpdateStatement(targetClass, propsList, primaryKeyNames, type);

        return execute(batchStatement);
    }

    private BatchStatement prepareBatchUpdateStatement(final Class<?> targetClass, final Collection<? extends Map<String, Object>> propsList,
            final Set<String> primaryKeyNames, final BatchStatement.Type type) {
        N.checkArgument(N.notNullOrEmpty(propsList), "'propsList' can't be null or empty.");
        N.checkArgument(N.notNullOrEmpty(primaryKeyNames), "'primaryKeyNames' can't be null or empty.");

        final BatchStatement batchStatement = prepareBatchStatement(type);

        for (Map<String, Object> props : propsList) {
            final Map<String, Object> tmp = new HashMap<>(props);
            final And and = new And();

            for (String keyName : primaryKeyNames) {
                and.add(CF.eq(keyName, tmp.remove(keyName)));
            }

            final CP cp = prepareUpdate(targetClass, tmp, and);
            batchStatement.add(prepareStatement(cp.cql, cp.parameters.toArray()));
        }

        return batchStatement;
    }

    public ResultSet delete(final Object entity) {
        return delete(entity, null);
    }

    public ResultSet delete(final Object entity, final Collection<String> deletingPropNames) {
        return delete(entity.getClass(), deletingPropNames, entity2Cond(entity));
    }

    @SafeVarargs
    public final ResultSet delete(final Class<?> targetClass, final Object... ids) {
        return delete(targetClass, null, ids);
    }

    /**
     * Delete the specified properties if <code>propNames</code> is not null or empty, otherwise, delete the whole record.
     * 
     * @param targetClass
     * @param deletingPropNames
     * @param id
     */
    @SafeVarargs
    public final ResultSet delete(final Class<?> targetClass, final Collection<String> deletingPropNames, final Object... ids) {
        return delete(targetClass, deletingPropNames, ids2Cond(targetClass, ids));
    }

    public ResultSet delete(final Class<?> targetClass, final Condition whereCause) {
        return delete(targetClass, null, whereCause);
    }

    /**
     * Delete the specified properties if <code>propNames</code> is not null or empty, otherwise, delete the whole record.
     * 
     * @param targetClass
     * @param deletingPropNames
     * @param whereCause
     */
    public ResultSet delete(final Class<?> targetClass, final Collection<String> deletingPropNames, final Condition whereCause) {
        final CP cp = prepareDelete(targetClass, deletingPropNames, whereCause);

        return execute(cp);
    }

    private CP prepareDelete(final Class<?> targetClass, final Collection<String> deletingPropNames, final Condition whereCause) {
        switch (namingPolicy) {
            case LOWER_CASE_WITH_UNDERSCORE:
                if (N.isNullOrEmpty(deletingPropNames)) {
                    return NSC.deleteFrom(targetClass).where(whereCause).pair();
                } else {
                    return NSC.delete(deletingPropNames).from(targetClass).where(whereCause).pair();
                }

            case UPPER_CASE_WITH_UNDERSCORE:
                if (N.isNullOrEmpty(deletingPropNames)) {
                    return NAC.deleteFrom(targetClass).where(whereCause).pair();
                } else {
                    return NAC.delete(deletingPropNames).from(targetClass).where(whereCause).pair();
                }

            case LOWER_CAMEL_CASE:
                if (N.isNullOrEmpty(deletingPropNames)) {
                    return NLC.deleteFrom(targetClass).where(whereCause).pair();
                } else {
                    return NLC.delete(deletingPropNames).from(targetClass).where(whereCause).pair();
                }

            default:
                throw new RuntimeException("Unsupported naming policy: " + namingPolicy);
        }
    }

    @SafeVarargs
    public final boolean exists(final Class<?> targetClass, final Object... ids) {
        return exists(targetClass, ids2Cond(targetClass, ids));
    }

    public boolean exists(final Class<?> targetClass, final Condition whereCause) {
        final List<String> keyNames = getKeyNames(targetClass);
        final CP cp = prepareQuery(targetClass, keyNames, whereCause, 1);
        final ResultSet resultSet = execute(cp);

        return resultSet.iterator().hasNext();
    }

    public long count(final Class<?> targetClass, final Condition whereCause) {
        final CP cp = prepareQuery(targetClass, N.asList(CQLBuilder.COUNT_ALL), whereCause, 1);

        return count(cp.cql, cp.parameters.toArray());
    }

    public <T> Optional<T> findFirst(final Class<T> targetClass, final Condition whereCause) {
        return findFirst(targetClass, null, whereCause);
    }

    public <T> Optional<T> findFirst(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereCause) {
        final CP cp = prepareQuery(targetClass, selectPropNames, whereCause, 1);

        return findFirst(targetClass, cp.cql, cp.parameters.toArray());
    }

    public <T> List<T> list(final Class<T> targetClass, final Condition whereCause) {
        return list(targetClass, null, whereCause);
    }

    public <T> List<T> list(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereCause) {
        final CP cp = prepareQuery(targetClass, selectPropNames, whereCause);

        return list(targetClass, cp.cql, cp.parameters.toArray());
    }

    public <T> DataSet query(final Class<T> targetClass, final Condition whereCause) {
        return query(targetClass, null, whereCause);
    }

    public <T> DataSet query(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereCause) {
        final CP cp = prepareQuery(targetClass, selectPropNames, whereCause);

        return query(targetClass, cp.cql, cp.parameters.toArray());
    }

    @Beta
    public <T> OptionalBoolean queryForBoolean(final Class<T> targetClass, final String propName, final Condition whereCause) {
        return queryForSingleResult(targetClass, Boolean.class, propName, whereCause).mapToBoolean(ToBooleanFunction.UNBOX);
    }

    @Beta
    public <T> OptionalChar queryForChar(final Class<T> targetClass, final String propName, final Condition whereCause) {
        return queryForSingleResult(targetClass, Character.class, propName, whereCause).mapToChar(ToCharFunction.UNBOX);
    }

    @Beta
    public <T> OptionalByte queryForByte(final Class<T> targetClass, final String propName, final Condition whereCause) {
        return queryForSingleResult(targetClass, Byte.class, propName, whereCause).mapToByte(ToByteFunction.UNBOX);
    }

    @Beta
    public <T> OptionalShort queryForShort(final Class<T> targetClass, final String propName, final Condition whereCause) {
        return queryForSingleResult(targetClass, Short.class, propName, whereCause).mapToShort(ToShortFunction.UNBOX);
    }

    @Beta
    public <T> OptionalInt queryForInt(final Class<T> targetClass, final String propName, final Condition whereCause) {
        return queryForSingleResult(targetClass, Integer.class, propName, whereCause).mapToInt(ToIntFunction.UNBOX);
    }

    @Beta
    public <T> OptionalLong queryForLong(final Class<T> targetClass, final String propName, final Condition whereCause) {
        return queryForSingleResult(targetClass, Long.class, propName, whereCause).mapToLong(ToLongFunction.UNBOX);
    }

    @Beta
    public <T> OptionalFloat queryForFloat(final Class<T> targetClass, final String propName, final Condition whereCause) {
        return queryForSingleResult(targetClass, Float.class, propName, whereCause).mapToFloat(ToFloatFunction.UNBOX);
    }

    @Beta
    public <T> OptionalDouble queryForDouble(final Class<T> targetClass, final String propName, final Condition whereCause) {
        return queryForSingleResult(targetClass, Double.class, propName, whereCause).mapToDouble(ToDoubleFunction.UNBOX);
    }

    @Beta
    public <T> Nullable<String> queryForString(final Class<T> targetClass, final String propName, final Condition whereCause) {
        return this.queryForSingleResult(targetClass, String.class, propName, whereCause);
    }

    @Beta
    public <T> Nullable<Date> queryForDate(final Class<T> targetClass, final String propName, final Condition whereCause) {
        return this.queryForSingleResult(targetClass, Date.class, propName, whereCause);
    }

    @Beta
    public <T, E extends Date> Nullable<E> queryForDate(final Class<T> targetClass, final Class<E> valueClass, final String propName,
            final Condition whereCause) {
        return this.queryForSingleResult(targetClass, valueClass, propName, whereCause);
    }

    public <T, V> Nullable<V> queryForSingleResult(final Class<T> targetClass, final Class<V> valueClass, final String propName, final Condition whereCause) {
        final CP cp = prepareQuery(targetClass, Arrays.asList(propName), whereCause, 1);

        return queryForSingleResult(valueClass, cp.cql, cp.parameters.toArray());
    }

    public <T> Stream<T> stream(final Class<T> targetClass, final Condition whereCause) {
        return stream(targetClass, null, whereCause);
    }

    public <T> Stream<T> stream(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereCause) {
        final CP cp = prepareQuery(targetClass, selectPropNames, whereCause);

        return stream(targetClass, cp.cql, cp.parameters.toArray());
    }

    /**
     * Always remember to set "<code>LIMIT 1</code>" in the cql statement for better performance.
     *
     * @param query
     * @param parameters
     * @return
     */
    @SafeVarargs
    public final boolean exists(final String query, final Object... parameters) {
        final ResultSet resultSet = execute(query, parameters);

        return resultSet.iterator().hasNext();
    }

    /**
     * 
     * @param query
     * @param parameters
     * @return
     * @deprecated may be misused and it's inefficient.
     */
    @Deprecated
    @SafeVarargs
    public final long count(final String query, final Object... parameters) {
        return queryForSingleResult(long.class, query, parameters).orElse(0L);
    }

    @Beta
    @SafeVarargs
    public final OptionalBoolean queryForBoolean(final String query, final Object... parameters) {
        return this.queryForSingleResult(Boolean.class, query, parameters).mapToBoolean(ToBooleanFunction.UNBOX);
    }

    @Beta
    @SafeVarargs
    public final OptionalChar queryForChar(final String query, final Object... parameters) {
        return this.queryForSingleResult(Character.class, query, parameters).mapToChar(ToCharFunction.UNBOX);
    }

    @Beta
    @SafeVarargs
    public final OptionalByte queryForByte(final String query, final Object... parameters) {
        return this.queryForSingleResult(Byte.class, query, parameters).mapToByte(ToByteFunction.UNBOX);
    }

    @Beta
    @SafeVarargs
    public final OptionalShort queryForShort(final String query, final Object... parameters) {
        return this.queryForSingleResult(Short.class, query, parameters).mapToShort(ToShortFunction.UNBOX);
    }

    @Beta
    @SafeVarargs
    public final OptionalInt queryForInt(final String query, final Object... parameters) {
        return this.queryForSingleResult(Integer.class, query, parameters).mapToInt(ToIntFunction.UNBOX);
    }

    @Beta
    @SafeVarargs
    public final OptionalLong queryForLong(final String query, final Object... parameters) {
        return this.queryForSingleResult(Long.class, query, parameters).mapToLong(ToLongFunction.UNBOX);
    }

    @Beta
    @SafeVarargs
    public final OptionalFloat queryForFloat(final String query, final Object... parameters) {
        return this.queryForSingleResult(Float.class, query, parameters).mapToFloat(ToFloatFunction.UNBOX);
    }

    @Beta
    @SafeVarargs
    public final OptionalDouble queryForDouble(final String query, final Object... parameters) {
        return this.queryForSingleResult(Double.class, query, parameters).mapToDouble(ToDoubleFunction.UNBOX);
    }

    @Beta
    @SafeVarargs
    public final Nullable<String> queryForString(final String query, final Object... parameters) {
        return this.queryForSingleResult(String.class, query, parameters);
    }

    @SafeVarargs
    public final <E> Nullable<E> queryForSingleResult(final Class<E> valueClass, final String query, final Object... parameters) {
        final ResultSet resultSet = execute(query, parameters);
        final Row row = resultSet.one();

        return row == null ? (Nullable<E>) Nullable.empty() : Nullable.of(N.convert(row.getObject(0), valueClass));
    }

    /**
     * 
     * @param query
     * @param parameters
     * @return
     */
    @SafeVarargs
    public final Optional<Map<String, Object>> findFirst(final String query, final Object... parameters) {
        return findFirst(Clazz.PROPS_MAP, query, parameters);
    }

    /**
     * 
     * @param targetClass an entity class with getter/setter method or <code>Map.class</code>
     * @param query
     * @param parameters
     * @return
     */
    @SafeVarargs
    public final <T> Optional<T> findFirst(final Class<T> targetClass, final String query, final Object... parameters) {
        final ResultSet resultSet = execute(query, parameters);
        final Row row = resultSet.one();

        return row == null ? (Optional<T>) Optional.empty() : Optional.of(toEntity(targetClass, row));
    }

    /**
     * 
     * @param query
     * @param parameters
     * @return
     */
    @SafeVarargs
    public final List<Map<String, Object>> list(final String query, final Object... parameters) {
        return list(Clazz.PROPS_MAP, query, parameters);
    }

    /**
     * 
     * @param targetClass an entity class with getter/setter method, <code>Map.class</code> or basic single value type(Primitive/String/Date...)
     * @param query
     * @param parameters
     * @return
     */
    @SafeVarargs
    public final <T> List<T> list(final Class<T> targetClass, final String query, final Object... parameters) {
        return toList(targetClass, execute(query, parameters));
    }

    @SafeVarargs
    public final DataSet query(final String query, final Object... parameters) {
        return query(Map.class, query, parameters);
    }

    /**
     * 
     * @param targetClass an entity class with getter/setter method or <code>Map.class</code>
     * @param query
     * @param parameters
     * @return
     */
    @SafeVarargs
    public final DataSet query(final Class<?> targetClass, final String query, final Object... parameters) {
        return extractData(targetClass, execute(query, parameters));
    }

    @SafeVarargs
    public final Stream<Object[]> stream(final String query, final Object... parameters) {
        final MutableInt columnCount = MutableInt.of(0);

        return Stream.of(execute(query, parameters).iterator()).map(new Function<Row, Object[]>() {
            @Override
            public Object[] apply(Row row) {
                if (columnCount.value() == 0) {
                    final ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
                    columnCount.setAndGet(columnDefinitions.size());
                }

                final Object[] a = new Object[columnCount.value()];
                Object propValue = null;

                for (int i = 0, len = a.length; i < len; i++) {
                    propValue = row.getObject(i);

                    if (propValue instanceof Row) {
                        a[i] = readRow(Object[].class, (Row) propValue);
                    } else {
                        a[i] = propValue;
                    }
                }

                return a;
            }
        });
    }

    /**
     * 
     * @param targetClass an entity class with getter/setter method or <code>Map.class</code>
     * @param query
     * @param parameters
     * @return
     */
    @SafeVarargs
    public final <T> Stream<T> stream(final Class<T> targetClass, final String query, final Object... parameters) {
        return Stream.of(execute(query, parameters).iterator()).map(new Function<Row, T>() {
            @Override
            public T apply(Row row) {
                return toEntity(targetClass, row);
            }
        });
    }

    @SafeVarargs
    public final <T> Stream<T> stream(final String query, final BiFunction<ColumnDefinitions, Row, T> rowMapper, final Object... parameters) {
        N.checkArgNotNull(rowMapper, "rowMapper");

        return Stream.of(execute(query, parameters).iterator()).map(new Function<Row, T>() {
            private volatile ColumnDefinitions cds = null;

            @Override
            public T apply(Row row) {
                if (cds == null) {
                    cds = row.getColumnDefinitions();
                }

                return rowMapper.apply(cds, row);
            }
        });
    }

    private <T> CP prepareQuery(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereCause) {
        return prepareQuery(targetClass, selectPropNames, whereCause, 0);
    }

    private <T> CP prepareQuery(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereCause, final int count) {
        CQLBuilder cqlBuilder = null;

        switch (namingPolicy) {
            case LOWER_CASE_WITH_UNDERSCORE:
                if (N.isNullOrEmpty(selectPropNames)) {
                    cqlBuilder = NSC.selectFrom(targetClass).where(whereCause);
                } else {
                    cqlBuilder = NSC.select(selectPropNames).from(targetClass).where(whereCause);
                }

                break;

            case UPPER_CASE_WITH_UNDERSCORE:
                if (N.isNullOrEmpty(selectPropNames)) {
                    cqlBuilder = NAC.selectFrom(targetClass).where(whereCause);
                } else {
                    cqlBuilder = NAC.select(selectPropNames).from(targetClass).where(whereCause);
                }

                break;

            case LOWER_CAMEL_CASE:
                if (N.isNullOrEmpty(selectPropNames)) {
                    cqlBuilder = NLC.selectFrom(targetClass).where(whereCause);
                } else {
                    cqlBuilder = NLC.select(selectPropNames).from(targetClass).where(whereCause);
                }

                break;

            default:
                throw new RuntimeException("Unsupported naming policy: " + namingPolicy);
        }

        if (count > 0) {
            cqlBuilder.limit(count);
        }

        return cqlBuilder.pair();
    }

    private ResultSet execute(CP cp) {
        return execute(cp.cql, cp.parameters.toArray());
    }

    public ResultSet execute(final String query) {
        return session.execute(prepareStatement(query));
    }

    @SafeVarargs
    public final ResultSet execute(final String query, final Object... parameters) {
        return session.execute(prepareStatement(query, parameters));
    }

    public ResultSet execute(final Statement statement) {
        return session.execute(statement);
    }

    @SafeVarargs
    public final <T> ContinuableFuture<Optional<T>> asyncGet(final Class<T> targetClass, final Object... ids) {
        return asyncGet(targetClass, null, ids);
    }

    @SafeVarargs
    public final <T> ContinuableFuture<Optional<T>> asyncGet(final Class<T> targetClass, final Collection<String> selectPropNames, final Object... ids)
            throws DuplicatedResultException {
        return asyncGet(targetClass, selectPropNames, ids2Cond(targetClass, ids));
    }

    public <T> ContinuableFuture<Optional<T>> asyncGet(final Class<T> targetClass, final Condition whereCause) {
        return asyncGet(targetClass, null, whereCause);
    }

    /**
     * 
     * @param targetClass
     * @param selectPropNames
     * @param idNameVal
     * @return
     */
    public <T> ContinuableFuture<Optional<T>> asyncGet(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereCause) {
        final CP cp = prepareQuery(targetClass, selectPropNames, whereCause, 2);

        return asyncExecute(cp).map(new Try.Function<ResultSet, Optional<T>, RuntimeException>() {
            @Override
            public Optional<T> apply(final ResultSet resultSet) throws RuntimeException {
                final Row row = resultSet.one();

                if (row == null) {
                    return null;
                } else if (resultSet.isExhausted()) {
                    return Optional.ofNullable(toEntity(targetClass, row));
                } else {
                    throw new DuplicatedResultException();
                }
            }
        });
    }

    @SafeVarargs
    public final <T> ContinuableFuture<T> asyncGett(final Class<T> targetClass, final Object... ids) {
        return asyncGett(targetClass, null, ids);
    }

    @SafeVarargs
    public final <T> ContinuableFuture<T> asyncGett(final Class<T> targetClass, final Collection<String> selectPropNames, final Object... ids)
            throws DuplicatedResultException {
        return asyncGett(targetClass, selectPropNames, ids2Cond(targetClass, ids));
    }

    public <T> ContinuableFuture<T> asyncGett(final Class<T> targetClass, final Condition whereCause) {
        return asyncGett(targetClass, null, whereCause);
    }

    /**
     * 
     * @param targetClass
     * @param selectPropNames
     * @param idNameVal
     * @return
     */
    public <T> ContinuableFuture<T> asyncGett(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereCause) {
        final CP cp = prepareQuery(targetClass, selectPropNames, whereCause, 2);

        return asyncExecute(cp).map(new Try.Function<ResultSet, T, RuntimeException>() {
            @Override
            public T apply(final ResultSet resultSet) throws RuntimeException {
                final Row row = resultSet.one();

                if (row == null) {
                    return null;
                } else if (resultSet.isExhausted()) {
                    return toEntity(targetClass, row);
                } else {
                    throw new DuplicatedResultException();
                }
            }
        });
    }

    public ContinuableFuture<ResultSet> asyncInsert(final Object entity) {
        final CP cp = prepareInsert(entity);

        return asyncExecute(cp);
    }

    public ContinuableFuture<ResultSet> asyncInsert(final Class<?> targetClass, final Map<String, Object> props) {
        final CP cp = prepareInsert(targetClass, props);

        return asyncExecute(cp);
    }

    public ContinuableFuture<ResultSet> asyncBatchInsert(final Collection<?> entities, final BatchStatement.Type type) {
        final BatchStatement batchStatement = prepareBatchInsertStatement(entities, type);

        return asyncExecute(batchStatement);
    }

    public ContinuableFuture<ResultSet> asyncBatchInsert(final Class<?> targetClass, final Collection<? extends Map<String, Object>> propsList,
            final BatchStatement.Type type) {
        final BatchStatement batchStatement = prepareBatchInsertStatement(targetClass, propsList, type);

        return asyncExecute(batchStatement);
    }

    public ContinuableFuture<ResultSet> asyncUpdate(final Object entity) {
        return asyncUpdate(entity, getKeyNameSet(entity.getClass()));
    }

    public ContinuableFuture<ResultSet> asyncUpdate(final Object entity, final Set<String> primaryKeyNames) {
        final CP cp = prepareUpdate(entity, primaryKeyNames);

        return asyncExecute(cp);
    }

    public ContinuableFuture<ResultSet> asyncUpdate(final Class<?> targetClass, final Map<String, Object> props, final Condition whereCause) {
        final CP cp = prepareUpdate(targetClass, props, whereCause);

        return asyncExecute(cp);
    }

    public ContinuableFuture<ResultSet> asyncBatchUpdate(final Collection<?> entities, final BatchStatement.Type type) {
        N.checkArgument(N.notNullOrEmpty(entities), "'entities' can't be null or empty.");

        return asyncBatchUpdate(entities, getKeyNameSet(N.firstOrNullIfEmpty(entities).getClass()), type);
    }

    public ContinuableFuture<ResultSet> asyncBatchUpdate(final Collection<?> entities, final Set<String> primaryKeyNames, final BatchStatement.Type type) {
        final BatchStatement batchStatement = prepareBatchUpdateStatement(entities, primaryKeyNames, type);

        return asyncExecute(batchStatement);

    }

    public ContinuableFuture<ResultSet> asyncBatchUpdate(final Class<?> targetClass, final Collection<? extends Map<String, Object>> propsList,
            final Set<String> primaryKeyNames, final BatchStatement.Type type) {
        final BatchStatement batchStatement = prepareBatchUpdateStatement(targetClass, propsList, primaryKeyNames, type);

        return asyncExecute(batchStatement);
    }

    public ContinuableFuture<ResultSet> asyncDelete(final Object entity) {
        return asyncDelete(entity, null);
    }

    public ContinuableFuture<ResultSet> asyncDelete(final Object entity, final Collection<String> deletingPropNames) {
        return asyncDelete(entity.getClass(), deletingPropNames, entity2Cond(entity));
    }

    @SafeVarargs
    public final ContinuableFuture<ResultSet> asyncDelete(final Class<?> targetClass, final Object... ids) {
        return asyncDelete(targetClass, null, ids);
    }

    /**
     * Delete the specified properties if <code>propNames</code> is not null or empty, otherwise, delete the whole record.
     * 
     * @param targetClass
     * @param deletingPropNames
     * @param id
     * @return
     */
    @SafeVarargs
    public final ContinuableFuture<ResultSet> asyncDelete(final Class<?> targetClass, final Collection<String> deletingPropNames, final Object... ids) {
        return asyncDelete(targetClass, deletingPropNames, ids2Cond(targetClass, ids));
    }

    public ContinuableFuture<ResultSet> asyncDelete(final Class<?> targetClass, final Condition whereCause) {
        return asyncDelete(targetClass, null, whereCause);
    }

    /**
     * Delete the specified properties if <code>propNames</code> is not null or empty, otherwise, delete the whole record.
     * 
     * @param targetClass
     * @param deletingPropNames
     * @param whereCause
     * @return
     */
    public ContinuableFuture<ResultSet> asyncDelete(final Class<?> targetClass, final Collection<String> deletingPropNames, final Condition whereCause) {
        final CP cp = prepareDelete(targetClass, deletingPropNames, whereCause);

        return asyncExecute(cp);
    }

    @SafeVarargs
    public final ContinuableFuture<Boolean> asyncExists(final Class<?> targetClass, final Object... ids) {
        return asyncExists(targetClass, ids2Cond(targetClass, ids));
    }

    public ContinuableFuture<Boolean> asyncExists(final Class<?> targetClass, final Condition whereCause) {
        final List<String> keyNames = getKeyNames(targetClass);
        final CP cp = prepareQuery(targetClass, keyNames, whereCause, 1);

        return asyncExists(cp.cql, cp.parameters.toArray());
    }

    public ContinuableFuture<Long> asyncCount(final Class<?> targetClass, final Condition whereCause) {
        final CP cp = prepareQuery(targetClass, N.asList(NSC.COUNT_ALL), whereCause, 1);

        return asyncCount(cp.cql, cp.parameters.toArray());
    }

    public <T> ContinuableFuture<List<T>> asyncList(final Class<T> targetClass, final Condition whereCause) {
        return asyncList(targetClass, null, whereCause);
    }

    public <T> ContinuableFuture<List<T>> asyncList(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereCause) {
        final CP cp = prepareQuery(targetClass, selectPropNames, whereCause);

        return asyncList(targetClass, cp.cql, cp.parameters.toArray());
    }

    public <T> ContinuableFuture<DataSet> asyncQuery(final Class<T> targetClass, final Condition whereCause) {
        return asyncQuery(targetClass, null, whereCause);
    }

    public <T> ContinuableFuture<DataSet> asyncQuery(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereCause) {
        final CP cp = prepareQuery(targetClass, selectPropNames, whereCause);

        return asyncQuery(targetClass, cp.cql, cp.parameters.toArray());
    }

    private static final Try.Function<Nullable<Boolean>, OptionalBoolean, RuntimeException> boolean_mapper = new Try.Function<Nullable<Boolean>, OptionalBoolean, RuntimeException>() {
        @Override
        public OptionalBoolean apply(Nullable<Boolean> t) throws RuntimeException {
            return t.mapToBoolean(ToBooleanFunction.UNBOX);
        }
    };

    public <T> ContinuableFuture<OptionalBoolean> asyncQueryForBoolean(final Class<T> targetClass, final String propName, final Condition whereCause) {
        return asyncQueryForSingleResult(targetClass, Boolean.class, propName, whereCause).map(boolean_mapper);
    }

    private static final Try.Function<Nullable<Character>, OptionalChar, RuntimeException> char_mapper = new Try.Function<Nullable<Character>, OptionalChar, RuntimeException>() {
        @Override
        public OptionalChar apply(Nullable<Character> t) throws RuntimeException {
            return t.mapToChar(ToCharFunction.UNBOX);
        }
    };

    public <T> ContinuableFuture<OptionalChar> asyncQueryForChar(final Class<T> targetClass, final String propName, final Condition whereCause) {
        return asyncQueryForSingleResult(targetClass, Character.class, propName, whereCause).map(char_mapper);
    }

    private static final Try.Function<Nullable<Byte>, OptionalByte, RuntimeException> byte_mapper = new Try.Function<Nullable<Byte>, OptionalByte, RuntimeException>() {
        @Override
        public OptionalByte apply(Nullable<Byte> t) throws RuntimeException {
            return t.mapToByte(ToByteFunction.UNBOX);
        }
    };

    public <T> ContinuableFuture<OptionalByte> asyncQueryForByte(final Class<T> targetClass, final String propName, final Condition whereCause) {
        return asyncQueryForSingleResult(targetClass, Byte.class, propName, whereCause).map(byte_mapper);
    }

    private static final Try.Function<Nullable<Short>, OptionalShort, RuntimeException> short_mapper = new Try.Function<Nullable<Short>, OptionalShort, RuntimeException>() {
        @Override
        public OptionalShort apply(Nullable<Short> t) throws RuntimeException {
            return t.mapToShort(ToShortFunction.UNBOX);
        }
    };

    public <T> ContinuableFuture<OptionalShort> asyncQueryForShort(final Class<T> targetClass, final String propName, final Condition whereCause) {
        return asyncQueryForSingleResult(targetClass, Short.class, propName, whereCause).map(short_mapper);
    }

    private static final Try.Function<Nullable<Integer>, OptionalInt, RuntimeException> int_mapper = new Try.Function<Nullable<Integer>, OptionalInt, RuntimeException>() {
        @Override
        public OptionalInt apply(Nullable<Integer> t) throws RuntimeException {
            return t.mapToInt(ToIntFunction.UNBOX);
        }
    };

    public <T> ContinuableFuture<OptionalInt> asyncQueryForInt(final Class<T> targetClass, final String propName, final Condition whereCause) {
        return asyncQueryForSingleResult(targetClass, Integer.class, propName, whereCause).map(int_mapper);
    }

    private static final Try.Function<Nullable<Long>, OptionalLong, RuntimeException> long_mapper = new Try.Function<Nullable<Long>, OptionalLong, RuntimeException>() {
        @Override
        public OptionalLong apply(Nullable<Long> t) throws RuntimeException {
            return t.mapToLong(ToLongFunction.UNBOX);
        }
    };

    public <T> ContinuableFuture<OptionalLong> asyncQueryForLong(final Class<T> targetClass, final String propName, final Condition whereCause) {
        return asyncQueryForSingleResult(targetClass, Long.class, propName, whereCause).map(long_mapper);
    }

    private static final Try.Function<Nullable<Float>, OptionalFloat, RuntimeException> float_mapper = new Try.Function<Nullable<Float>, OptionalFloat, RuntimeException>() {
        @Override
        public OptionalFloat apply(Nullable<Float> t) throws RuntimeException {
            return t.mapToFloat(ToFloatFunction.UNBOX);
        }
    };

    public <T> ContinuableFuture<OptionalFloat> asyncQueryForFloat(final Class<T> targetClass, final String propName, final Condition whereCause) {
        return asyncQueryForSingleResult(targetClass, Float.class, propName, whereCause).map(float_mapper);
    }

    private static final Try.Function<Nullable<Double>, OptionalDouble, RuntimeException> double_mapper = new Try.Function<Nullable<Double>, OptionalDouble, RuntimeException>() {
        @Override
        public OptionalDouble apply(Nullable<Double> t) throws RuntimeException {
            return t.mapToDouble(ToDoubleFunction.UNBOX);
        }
    };

    public <T> ContinuableFuture<OptionalDouble> asyncQueryForDouble(final Class<T> targetClass, final String propName, final Condition whereCause) {
        return asyncQueryForSingleResult(targetClass, Double.class, propName, whereCause).map(double_mapper);
    }

    public <T> ContinuableFuture<Nullable<String>> asyncQueryForString(final Class<T> targetClass, final String propName, final Condition whereCause) {
        return asyncQueryForSingleResult(targetClass, String.class, propName, whereCause);
    }

    public <T> ContinuableFuture<Nullable<Date>> asyncQueryForDate(final Class<T> targetClass, final String propName, final Condition whereCause) {
        return asyncQueryForSingleResult(targetClass, Date.class, propName, whereCause);
    }

    public <T, E extends Date> ContinuableFuture<Nullable<E>> asyncQueryForDate(final Class<T> targetClass, final Class<E> valueClass, final String propName,
            final Condition whereCause) {
        return asyncQueryForSingleResult(targetClass, valueClass, propName, whereCause);
    }

    public <T, V> ContinuableFuture<Nullable<V>> asyncQueryForSingleResult(final Class<T> targetClass, final Class<V> valueClass, final String propName,
            final Condition whereCause) {
        final CP cp = prepareQuery(targetClass, Arrays.asList(propName), whereCause, 1);

        return asyncQueryForSingleResult(valueClass, cp.cql, cp.parameters.toArray());
    }

    public <T> ContinuableFuture<Optional<T>> asyncFindFirst(final Class<T> targetClass, final Condition whereCause) {
        return asyncFindFirst(targetClass, null, whereCause);
    }

    public <T> ContinuableFuture<Optional<T>> asyncFindFirst(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereCause) {
        final CP cp = prepareQuery(targetClass, selectPropNames, whereCause, 1);

        return asyncFindFirst(targetClass, cp.cql, cp.parameters.toArray());
    }

    public <T> ContinuableFuture<Stream<T>> asyncStream(final Class<T> targetClass, final Condition whereCause) {
        return asyncStream(targetClass, null, whereCause);
    }

    public <T> ContinuableFuture<Stream<T>> asyncStream(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereCause) {
        final CP cp = prepareQuery(targetClass, selectPropNames, whereCause);

        return asyncStream(targetClass, cp.cql, cp.parameters.toArray());
    }

    private static final Try.Function<ResultSet, Boolean, RuntimeException> exists_mapper = new Try.Function<ResultSet, Boolean, RuntimeException>() {
        @Override
        public Boolean apply(ResultSet resultSet) throws RuntimeException {
            return resultSet.iterator().hasNext();
        }
    };

    /**
     * Always remember to set "<code>LIMIT 1</code>" in the cql statement for better performance.
     *
     * @param query
     * @param parameters
     * @return
     */
    @SafeVarargs
    public final ContinuableFuture<Boolean> asyncExists(final String query, final Object... parameters) {
        return asyncExecute(query, parameters).map(exists_mapper);
    }

    private static final Try.Function<Nullable<Long>, Long, RuntimeException> long_mapper2 = new Try.Function<Nullable<Long>, Long, RuntimeException>() {
        @Override
        public Long apply(Nullable<Long> t) throws RuntimeException {
            return t.mapToLong(ToLongFunction.UNBOX).orElse(0);
        }
    };

    /**
     * 
     * @param query
     * @param parameters
     * @return
     * @deprecated may be misused and it's inefficient.
     */
    @Deprecated
    @SafeVarargs
    public final ContinuableFuture<Long> asyncCount(final String query, final Object... parameters) {
        return asyncQueryForSingleResult(Long.class, query, parameters).map(long_mapper2);
    }

    @SafeVarargs
    public final <T> ContinuableFuture<OptionalBoolean> asyncQueryForBoolean(final String query, final Object... parameters) {
        return asyncQueryForSingleResult(Boolean.class, query, parameters).map(boolean_mapper);
    }

    @SafeVarargs
    public final <T> ContinuableFuture<OptionalChar> asyncQueryForChar(final String query, final Object... parameters) {
        return asyncQueryForSingleResult(Character.class, query, parameters).map(char_mapper);
    }

    @SafeVarargs
    public final <T> ContinuableFuture<OptionalByte> asyncQueryForByte(final String query, final Object... parameters) {
        return asyncQueryForSingleResult(Byte.class, query, parameters).map(byte_mapper);
    }

    @SafeVarargs
    public final <T> ContinuableFuture<OptionalShort> asyncQueryForShort(final String query, final Object... parameters) {
        return asyncQueryForSingleResult(Short.class, query, parameters).map(short_mapper);
    }

    @SafeVarargs
    public final <T> ContinuableFuture<OptionalInt> asyncQueryForInt(final String query, final Object... parameters) {
        return asyncQueryForSingleResult(Integer.class, query, parameters).map(int_mapper);
    }

    @SafeVarargs
    public final <T> ContinuableFuture<OptionalLong> asyncQueryForLong(final String query, final Object... parameters) {
        return asyncQueryForSingleResult(Long.class, query, parameters).map(long_mapper);
    }

    @SafeVarargs
    public final <T> ContinuableFuture<OptionalFloat> asyncQueryForFloat(final String query, final Object... parameters) {
        return asyncQueryForSingleResult(Float.class, query, parameters).map(float_mapper);
    }

    @SafeVarargs
    public final <T> ContinuableFuture<OptionalDouble> asyncQueryForDouble(final String query, final Object... parameters) {
        return asyncQueryForSingleResult(Double.class, query, parameters).map(double_mapper);
    }

    @SafeVarargs
    public final <T> ContinuableFuture<Nullable<String>> asyncQueryForString(final String query, final Object... parameters) {
        return asyncQueryForSingleResult(String.class, query, parameters);
    }

    @SafeVarargs
    public final <T> ContinuableFuture<Nullable<T>> asyncQueryForSingleResult(final Class<T> valueClass, final String query, final Object... parameters) {
        return asyncExecute(query, parameters).map(new Try.Function<ResultSet, Nullable<T>, RuntimeException>() {
            @Override
            public Nullable<T> apply(final ResultSet resultSet) throws RuntimeException {
                final Row row = resultSet.one();

                return row == null ? (Nullable<T>) Nullable.empty() : Nullable.of(N.convert(row.getObject(0), valueClass));
            }
        });
    }

    @SafeVarargs
    public final ContinuableFuture<Optional<Map<String, Object>>> asyncFindFirst(final String query, final Object... parameters) {
        return asyncFindFirst(Clazz.PROPS_MAP, query, parameters);
    }

    @SafeVarargs
    public final <T> ContinuableFuture<Optional<T>> asyncFindFirst(final Class<T> targetClass, final String query, final Object... parameters) {
        return asyncExecute(query, parameters).map(new Try.Function<ResultSet, Optional<T>, RuntimeException>() {
            @Override
            public Optional<T> apply(final ResultSet resultSet) throws RuntimeException {
                final Row row = resultSet.one();

                return row == null ? (Optional<T>) Optional.empty() : Optional.of(toEntity(targetClass, row));
            }
        });
    }

    @SafeVarargs
    public final ContinuableFuture<List<Map<String, Object>>> asyncList(final String query, final Object... parameters) {
        return asyncList(Clazz.PROPS_MAP, query, parameters);
    }

    @SafeVarargs
    public final <T> ContinuableFuture<List<T>> asyncList(final Class<T> targetClass, final String query, final Object... parameters) {
        return asyncExecute(query, parameters).map(new Try.Function<ResultSet, List<T>, RuntimeException>() {
            @Override
            public List<T> apply(final ResultSet resultSet) throws RuntimeException {
                return toList(targetClass, resultSet);
            }
        });
    }

    @SafeVarargs
    public final ContinuableFuture<DataSet> asyncQuery(final String query, final Object... parameters) {
        return asyncQuery(Map.class, query, parameters);
    }

    @SafeVarargs
    public final ContinuableFuture<DataSet> asyncQuery(final Class<?> targetClass, final String query, final Object... parameters) {
        return asyncExecute(query, parameters).map(new Try.Function<ResultSet, DataSet, RuntimeException>() {
            @Override
            public DataSet apply(final ResultSet resultSet) throws RuntimeException {
                return extractData(targetClass, resultSet);
            }
        });
    }

    @SafeVarargs
    public final ContinuableFuture<Stream<Object[]>> asyncStream(final String query, final Object... parameters) {
        return asyncExecute(query, parameters).map(new Try.Function<ResultSet, Stream<Object[]>, RuntimeException>() {
            @Override
            public Stream<Object[]> apply(final ResultSet resultSet) throws RuntimeException {
                final MutableInt columnCount = MutableInt.of(0);

                return Stream.of(resultSet.iterator()).map(new Function<Row, Object[]>() {
                    @Override
                    public Object[] apply(Row row) {
                        if (columnCount.value() == 0) {
                            final ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
                            columnCount.setAndGet(columnDefinitions.size());
                        }

                        final Object[] a = new Object[columnCount.value()];
                        Object propValue = null;

                        for (int i = 0, len = a.length; i < len; i++) {
                            propValue = row.getObject(i);

                            if (propValue instanceof Row) {
                                a[i] = readRow(Object[].class, (Row) propValue);
                            } else {
                                a[i] = propValue;
                            }
                        }

                        return a;
                    }
                });
            }
        });
    }

    /**
     * 
     * @param targetClass an entity class with getter/setter method or <code>Map.class</code>
     * @param query
     * @param parameters
     * @return
     */
    @SafeVarargs
    public final <T> ContinuableFuture<Stream<T>> asyncStream(final Class<T> targetClass, final String query, final Object... parameters) {
        return asyncExecute(query, parameters).map(new Try.Function<ResultSet, Stream<T>, RuntimeException>() {
            @Override
            public Stream<T> apply(final ResultSet resultSet) throws RuntimeException {
                return Stream.of(resultSet.iterator()).map(new Function<Row, T>() {
                    @Override
                    public T apply(Row row) {
                        return toEntity(targetClass, row);
                    }
                });
            }
        });
    }

    @SafeVarargs
    public final <T> ContinuableFuture<Stream<T>> asyncStream(final String query, final BiFunction<ColumnDefinitions, Row, T> rowMapper,
            final Object... parameters) {
        return asyncExecute(query, parameters).map(new Try.Function<ResultSet, Stream<T>, RuntimeException>() {
            @Override
            public Stream<T> apply(final ResultSet resultSet) throws RuntimeException {
                return Stream.of(resultSet.iterator()).map(new Function<Row, T>() {
                    private volatile ColumnDefinitions cds = null;

                    @Override
                    public T apply(Row row) {
                        if (cds == null) {
                            cds = row.getColumnDefinitions();
                        }

                        return rowMapper.apply(cds, row);
                    }
                });
            }
        });
    }

    private ContinuableFuture<ResultSet> asyncExecute(final CP cp) {
        return asyncExecute(cp.cql, cp.parameters.toArray());
    }

    public ContinuableFuture<ResultSet> asyncExecute(final String query) {
        return ContinuableFuture.wrap(session.executeAsync(query));
    }

    @SafeVarargs
    public final ContinuableFuture<ResultSet> asyncExecute(final String query, final Object... parameters) {
        return ContinuableFuture.wrap(session.executeAsync(query, parameters));
    }

    public final ContinuableFuture<ResultSet> asyncExecute(final Statement statement) {
        return ContinuableFuture.wrap(session.executeAsync(statement));
    }

    @Override
    public void close() throws IOException {
        try {
            if (session.isClosed() == false) {
                session.close();
            }
        } finally {
            if (cluster.isClosed() == false) {
                cluster.close();
            }
        }
    }

    private static <T> void checkTargetClass(final Class<T> targetClass) {
        if (!(ClassUtil.isEntity(targetClass) || Map.class.isAssignableFrom(targetClass))) {
            throw new IllegalArgumentException("The target class must be an entity class with getter/setter methods or Map.class. But it is: "
                    + ClassUtil.getCanonicalClassName(targetClass));
        }
    }

    private Statement prepareStatement(final String query) {
        Statement stmt = null;

        if (query.length() <= POOLABLE_LENGTH) {
            PoolableWrapper<Statement> wrapper = stmtPool.get(query);

            if (wrapper != null) {
                stmt = wrapper.value();
            }
        }

        if (stmt == null) {
            final NamedCQL namedCQL = getNamedCQL(query);
            final String cql = namedCQL.getPureCQL();
            stmt = bind(prepare(cql));

            if (query.length() <= POOLABLE_LENGTH) {
                stmtPool.put(query, PoolableWrapper.of(stmt));
            }
        }

        return stmt;
    }

    private Statement prepareStatement(String query, Object... parameters) {
        if (N.isNullOrEmpty(parameters)) {
            return prepareStatement(query);
        }

        final NamedCQL namedCQL = getNamedCQL(query);
        final String cql = namedCQL.getPureCQL();
        PreparedStatement preStmt = null;

        if (query.length() <= POOLABLE_LENGTH) {
            PoolableWrapper<PreparedStatement> wrapper = preStmtPool.get(query);
            if (wrapper != null && wrapper.value() != null) {
                preStmt = wrapper.value();
            }
        }

        if (preStmt == null) {
            preStmt = prepare(cql);

            if (query.length() <= POOLABLE_LENGTH) {
                preStmtPool.put(query, PoolableWrapper.of(preStmt));
            }
        }

        final ColumnDefinitions columnDefinitions = preStmt.getVariables();
        final int parameterCount = columnDefinitions.size();
        DataType colType = null;
        Class<?> javaClass = null;

        if (parameterCount == 0) {
            return preStmt.bind();
        } else if (N.isNullOrEmpty(parameters)) {
            throw new IllegalArgumentException("Null or empty parameters for parameterized query: " + query);
        }

        if (parameterCount == 1 && parameters.length == 1) {
            colType = columnDefinitions.getType(0);
            javaClass = namedDataType.get(colType.getName().name());

            if (parameters[0] == null || (javaClass.isAssignableFrom(parameters[0].getClass())
                    || (colType instanceof UserType && codecRegistry.codecFor(colType).accepts(parameters[0])))) {
                return bind(preStmt, parameters);
            } else if (parameters[0] instanceof List && ((List<Object>) parameters[0]).size() == 1) {
                final Object tmp = ((List<Object>) parameters[0]).get(0);

                if (tmp == null
                        || (javaClass.isAssignableFrom(tmp.getClass()) || (colType instanceof UserType && codecRegistry.codecFor(colType).accepts(tmp)))) {
                    return bind(preStmt, tmp);
                }
            }
        }

        Object[] values = parameters;

        if (parameters.length == 1 && (parameters[0] instanceof Map || ClassUtil.isEntity(parameters[0].getClass()))) {
            values = new Object[parameterCount];
            final Object parameter_0 = parameters[0];
            final Map<Integer, String> namedParameters = namedCQL.getNamedParameters();
            final boolean isCassandraNamedParameters = N.isNullOrEmpty(namedParameters);
            String parameterName = null;
            if (parameter_0 instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> m = (Map<String, Object>) parameter_0;

                for (int i = 0; i < parameterCount; i++) {
                    parameterName = isCassandraNamedParameters ? columnDefinitions.getName(i) : namedParameters.get(i);
                    values[i] = m.get(parameterName);

                    if ((values[i] == null) && !m.containsKey(parameterName)) {
                        throw new IllegalArgumentException("Parameter for property '" + parameterName + "' is missed");
                    }
                }
            } else {
                Object entity = parameter_0;
                Class<?> clazz = entity.getClass();
                Method propGetMethod = null;

                for (int i = 0; i < parameterCount; i++) {
                    parameterName = isCassandraNamedParameters ? columnDefinitions.getName(i) : namedParameters.get(i);
                    propGetMethod = ClassUtil.getPropGetMethod(clazz, parameterName);

                    if (propGetMethod == null) {
                        throw new IllegalArgumentException("Parameter for property '" + parameterName + "' is missed");
                    }

                    values[i] = ClassUtil.invokeMethod(entity, propGetMethod);
                }
            }
        } else if ((parameters.length == 1) && (parameters[0] != null)) {
            if (parameters[0] instanceof Object[] && ((((Object[]) parameters[0]).length) >= namedCQL.getParameterCount())) {
                values = (Object[]) parameters[0];
            } else if (parameters[0] instanceof List && (((List<?>) parameters[0]).size() >= namedCQL.getParameterCount())) {
                final Collection<?> c = (Collection<?>) parameters[0];
                values = c.toArray(new Object[c.size()]);
            }
        }

        for (int i = 0; i < parameterCount; i++) {
            colType = columnDefinitions.getType(i);
            javaClass = namedDataType.get(colType.getName().name());

            if (values[i] == null) {
                values[i] = N.defaultValueOf(javaClass);
            } else if (javaClass.isAssignableFrom(values[i].getClass())
                    || (colType instanceof UserType && codecRegistry.codecFor(colType).accepts(values[i]))) {
                // continue;
            } else {
                try {
                    values[i] = N.convert(values[i], javaClass);
                } catch (Exception e) {
                    // ignore.
                }
            }
        }

        return bind(preStmt, values.length == parameterCount ? values : N.copyOfRange(values, 0, parameterCount));
    }

    private PreparedStatement prepare(final String query) {
        PreparedStatement preStat = session.prepare(query);

        if (settings != null) {
            if (settings.getConsistency() != null) {
                preStat.setConsistencyLevel(settings.getConsistency());
            }

            if (settings.getSerialConsistency() != null) {
                preStat.setSerialConsistencyLevel(settings.getSerialConsistency());
            }

            if (settings.getRetryPolicy() != null) {
                preStat.setRetryPolicy(settings.getRetryPolicy());
            }

            if (settings.isTraceQuery()) {
                preStat.enableTracing();
            } else {
                preStat.disableTracing();
            }
        }

        return preStat;
    }

    private BoundStatement bind(PreparedStatement preStmt) {
        BoundStatement stmt = preStmt.bind();

        if (settings != null && settings.getFetchSize() > 0) {
            stmt.setFetchSize(settings.getFetchSize());
        }

        return stmt;
    }

    private BoundStatement bind(PreparedStatement preStmt, Object... parameters) {
        BoundStatement stmt = preStmt.bind(parameters);

        if (settings != null && settings.getFetchSize() > 0) {
            stmt.setFetchSize(settings.getFetchSize());
        }

        return stmt;
    }

    private NamedCQL getNamedCQL(String cql) {
        NamedCQL namedCQL = null;

        if (cqlMapper != null) {
            namedCQL = cqlMapper.get(cql);
        }

        if (namedCQL == null) {
            namedCQL = NamedCQL.parse(cql, null);
        }

        return namedCQL;
    }

    /**
     * <pre>
     * <code>   static final CassandraExecutor cassandraExecutor;
    
    static {
        final CodecRegistry codecRegistry = new CodecRegistry();
        final Cluster cluster = Cluster.builder().withCodecRegistry(codecRegistry).addContactPoint("127.0.0.1").build();
    
        codecRegistry.register(new UDTCodec&lt;Address&gt;(cluster, "simplex", "address", Address.class) {
            protected Address deserialize(UDTValue value) {
                if (value == null) {
                    return null;
                }
                Address address = new Address();
                address.setStreet(value.getString("street"));
                address.setCity(value.getString("city"));
                address.setZipCode(value.getInt("zipCode"));
                return address;
            }
    
            protected UDTValue serialize(Address value) {
                return value == null ? null
                        : newUDTValue().setString("street", value.getStreet()).setInt("zipcode", value.getZipCode());
            }
        });
    
    
        cassandraExecutor = new CassandraExecutor(cluster);
    }
     * </code>
     * </pre>
     * 
     * @author haiyangl
     *
     * @param <T>
     */
    public abstract static class UDTCodec<T> extends TypeCodec<T> {
        private final TypeCodec<UDTValue> innerCodec;
        private final UserType userType;
        private final Class<T> javaType;

        public UDTCodec(TypeCodec<UDTValue> innerCodec, Class<T> javaType) {
            super(innerCodec.getCqlType(), javaType);
            this.innerCodec = innerCodec;
            this.userType = (UserType) innerCodec.getCqlType();
            this.javaType = javaType;
        }

        public UDTCodec(final Cluster cluster, final String keySpace, final String userType, Class<T> javaType) {
            this(TypeCodec.userType(cluster.getMetadata().getKeyspace(keySpace).getUserType(userType)), javaType);
        }

        @Override
        public ByteBuffer serialize(T value, ProtocolVersion protocolVersion) throws InvalidTypeException {
            return innerCodec.serialize(serialize(value), protocolVersion);
        }

        @Override
        public T deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
            return deserialize(innerCodec.deserialize(bytes, protocolVersion));
        }

        @Override
        public T parse(String value) throws InvalidTypeException {
            return N.isNullOrEmpty(value) ? null : N.fromJSON(javaType, value);
        }

        @Override
        public String format(T value) throws InvalidTypeException {
            return value == null ? null : N.toJSON(value);
        }

        protected UDTValue newUDTValue() {
            return userType.newValue();
        }

        protected abstract UDTValue serialize(T value);

        protected abstract T deserialize(UDTValue value);
    }

    public static final class StatementSettings {
        private ConsistencyLevel consistency;
        private ConsistencyLevel serialConsistency;
        private boolean traceQuery;
        private RetryPolicy retryPolicy;
        private int fetchSize;

        public StatementSettings() {
        }

        public StatementSettings(ConsistencyLevel consistency, ConsistencyLevel serialConsistency, boolean traceQuery, RetryPolicy retryPolicy, int fetchSize) {
            this.consistency = consistency;
            this.serialConsistency = serialConsistency;
            this.traceQuery = traceQuery;
            this.retryPolicy = retryPolicy;
            this.fetchSize = fetchSize;
        }

        public static StatementSettings create() {
            return new StatementSettings();
        }

        public ConsistencyLevel getConsistency() {
            return consistency;
        }

        public StatementSettings setConsistency(ConsistencyLevel consistency) {
            this.consistency = consistency;

            return this;
        }

        public ConsistencyLevel getSerialConsistency() {
            return serialConsistency;
        }

        public StatementSettings setSerialConsistency(ConsistencyLevel serialConsistency) {
            this.serialConsistency = serialConsistency;

            return this;
        }

        public boolean isTraceQuery() {
            return traceQuery;
        }

        public StatementSettings setTraceQuery(boolean traceQuery) {
            this.traceQuery = traceQuery;

            return this;
        }

        public RetryPolicy getRetryPolicy() {
            return retryPolicy;
        }

        public StatementSettings setRetryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = retryPolicy;

            return this;
        }

        public int getFetchSize() {
            return fetchSize;
        }

        public StatementSettings setFetchSize(int fetchSize) {
            this.fetchSize = fetchSize;

            return this;
        }

        public StatementSettings copy() {
            StatementSettings copy = new StatementSettings();

            copy.consistency = this.consistency;
            copy.serialConsistency = this.serialConsistency;
            copy.traceQuery = this.traceQuery;
            copy.retryPolicy = this.retryPolicy;
            copy.fetchSize = this.fetchSize;

            return copy;
        }

        @Override
        public int hashCode() {
            int h = 17;
            h = 31 * h + N.hashCode(consistency);
            h = 31 * h + N.hashCode(serialConsistency);
            h = 31 * h + N.hashCode(traceQuery);
            h = 31 * h + N.hashCode(retryPolicy);
            h = 31 * h + N.hashCode(fetchSize);

            return h;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj instanceof StatementSettings) {
                StatementSettings other = (StatementSettings) obj;

                if (N.equals(consistency, other.consistency) && N.equals(serialConsistency, other.serialConsistency) && N.equals(traceQuery, other.traceQuery)
                        && N.equals(retryPolicy, other.retryPolicy) && N.equals(fetchSize, other.fetchSize)) {

                    return true;
                }
            }

            return false;
        }

        @Override
        public String toString() {
            return "{" + "consistency=" + N.toString(consistency) + ", " + "serialConsistency=" + N.toString(serialConsistency) + ", " + "traceQuery="
                    + N.toString(traceQuery) + ", " + "retryPolicy=" + N.toString(retryPolicy) + ", " + "fetchSize=" + N.toString(fetchSize) + "}";
        }
    }
}
