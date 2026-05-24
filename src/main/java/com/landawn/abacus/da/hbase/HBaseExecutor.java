/*
 * Copyright (C) 2015 HaiYang Li
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

package com.landawn.abacus.da.hbase;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.landawn.abacus.da.hbase.annotation.ColumnFamily;
import com.landawn.abacus.exception.UncheckedIOException;
import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.parser.ParserUtil;
import com.landawn.abacus.parser.ParserUtil.BeanInfo;
import com.landawn.abacus.parser.ParserUtil.PropInfo;
import com.landawn.abacus.query.QueryUtil;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.Beans;
import com.landawn.abacus.util.BooleanList;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.ExceptionUtil;
import com.landawn.abacus.util.HBaseColumn;
import com.landawn.abacus.util.IOUtil;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.Strings;
import com.landawn.abacus.util.Tuple;
import com.landawn.abacus.util.Tuple.Tuple2;
import com.landawn.abacus.util.Tuple.Tuple3;
import com.landawn.abacus.util.function.Function;
import com.landawn.abacus.util.function.Supplier;
import com.landawn.abacus.util.stream.ObjIteratorEx;
import com.landawn.abacus.util.stream.Stream;

/**
 * High-level facade over Apache HBase that adds entity mapping, automatic byte-array
 * conversion, batch helpers, and a fluent {@code AnyXxx} builder API on top of the
 * native HBase client {@link Connection}/{@link Table}/{@link Admin} interfaces.
 *
 * <p>The executor owns an HBase {@link Admin} and reuses the supplied {@link Connection}.
 * Most operations acquire a short-lived {@link Table} from the connection, run the call,
 * and close the table in a {@code finally} block; the connection itself is left open
 * until {@link #close()} is invoked.</p>
 *
 * <h2>Byte-array conversion</h2>
 * <p>Convenience overloads that accept {@code Object} row keys or {@link String} column
 * families and qualifiers convert their arguments to {@code byte[]} via the package-private
 * helpers {@code toRowKeyBytes}, {@code toFamilyQualifierBytes}, and {@code toValueBytes}.
 * Overloads that take raw {@code byte[]} pass the bytes through unchanged. Strings are
 * encoded with {@link Bytes#toBytes(String)}; other objects are converted to their
 * {@code N.stringOf} form first; {@link ByteBuffer}s are read into a fresh array;
 * existing {@code byte[]} values are returned as-is.</p>
 *
 * <h2>Entity mapping</h2>
 *
 * <h3>Default mapping</h3>
 * <p>Without annotations, each field name is interpreted as the HBase Column Family name
 * and the column qualifier defaults to the empty string for scalar fields. Nested bean
 * fields are flattened so that each nested property becomes a qualifier under the parent
 * field's family.</p>
 *
 * <p><b>Example (no annotations):</b></p>
 * <pre>{@code
 * public static class Account {
 *     @Id
 *     private String id;            // HBase: "id:"          (columnFamily=id,  qualifier="")
 *     private String gui;           // HBase: "gui:"         (columnFamily=gui, qualifier="")
 *     private Name name;            // HBase: "name:firstName" and "name:lastName"
 *     private String emailAddress;  // HBase: "emailAddress:"
 * }
 *
 * public static class Name {
 *     private String firstName;  // HBase: "name:firstName"
 *     private String lastName;   // HBase: "name:lastName"
 * }
 * }</pre>
 *
 * <h3>Annotated mapping</h3>
 * <p>A class-level {@link ColumnFamily} sets the default family for every field, and a
 * field-level {@link ColumnFamily} overrides it. {@code @Column} customizes the qualifier.</p>
 *
 * <p><b>Example (annotated):</b></p>
 * <pre>{@code
 * @ColumnFamily("cf")
 * public static class Account {
 *     @Id
 *     private String id;            // HBase: "cf:id"
 *     @Column("guid")
 *     private String gui;           // HBase: "cf:guid"
 *     @ColumnFamily("name")
 *     private Name name;            // HBase: "name:givenName" and "name:lastName"
 * }
 * }</pre>
 *
 * <h2>Asynchronous access</h2>
 * <p>{@link #async()} exposes an {@link AsyncHBaseExecutor} that wraps the same operations
 * in {@link com.landawn.abacus.util.ContinuableFuture} return values, submitting them to
 * the {@link AsyncExecutor} supplied at construction (or the shared
 * {@link #DEFAULT_ASYNC_EXECUTOR}).</p>
 *
 * <h2>Scan streams</h2>
 * <p>{@code scan(...)} returns a lazy {@link Stream} that opens the underlying
 * {@link Table} and {@link ResultScanner} only when iteration begins; both are closed
 * when the stream is closed. Always consume scan streams inside a try-with-resources
 * block to avoid leaking the table or scanner.</p>
 *
 * <h2>Basic usage</h2>
 * <pre>{@code
 * try (HBaseExecutor executor = new HBaseExecutor(connection)) {
 *     boolean exists = executor.exists("users", AnyGet.of("user123"));
 *     Result result  = executor.get("users", AnyGet.of("user123"));
 *     executor.put("users", AnyPut.of("user123").addColumn("info", "name", "John"));
 *
 *     HBaseMapper<User, String> mapper = executor.mapper(User.class);
 *     User user = mapper.get("user123");
 *     mapper.put(user);
 * }
 * }</pre>
 *
 * @see com.landawn.abacus.util.HBaseColumn
 * @see com.landawn.abacus.da.hbase.annotation.ColumnFamily
 * @see AsyncHBaseExecutor
 * @see <a href="http://hbase.apache.org/devapidocs/index.html">Apache HBase Java API</a>
 */
public final class HBaseExecutor implements AutoCloseable {

    static {
        final BiFunction<Result, Class<?>, Object> converter = HBaseExecutor::toValue;

        N.registerConverter(Result.class, converter);
    }

    private static final Logger logger = LoggerFactory.getLogger(HBaseExecutor.class);

    static final String EMPTY_QUALIFIER = Strings.EMPTY;

    static final AsyncExecutor DEFAULT_ASYNC_EXECUTOR = new AsyncExecutor(//
            N.max(64, IOUtil.CPU_CORES * 8), // coreThreadPoolSize
            N.max(128, IOUtil.CPU_CORES * 16), // maxThreadPoolSize
            180L, TimeUnit.SECONDS);

    private static final Map<String, byte[]> familyQualifierBytesPool = new ConcurrentHashMap<>();

    private static final Map<Class<?>, Method> classRowKeySetMethodPool = new ConcurrentHashMap<>();

    private static final Map<Class<?>, Map<NamingPolicy, Map<String, Tuple3<String, String, Boolean>>>> classFamilyColumnNamePool = new ConcurrentHashMap<>();
    private static final Map<Class<?>, Tuple2<Map<String, Map<String, Tuple2<String, Boolean>>>, Map<String, String>>> classFamilyColumnFieldNamePool = new ConcurrentHashMap<>();

    private final Admin admin;

    private final Connection conn;

    private final AsyncHBaseExecutor asyncHBaseExecutor;

    /**
     * Constructs an {@code HBaseExecutor} bound to the given HBase {@link Connection},
     * using the shared {@link #DEFAULT_ASYNC_EXECUTOR} for async operations.
     *
     * <p>The default async executor is statically configured with:</p>
     * <ul>
     *   <li>Core thread pool size: {@code max(64, IOUtil.CPU_CORES * 8)}</li>
     *   <li>Max thread pool size: {@code max(128, IOUtil.CPU_CORES * 16)}</li>
     *   <li>Keep-alive time: 180 seconds</li>
     * </ul>
     *
     * <p>The supplied {@code conn} is not copied; closing this executor closes the
     * connection it wraps (see {@link #close()}).</p>
     *
     * @param conn the HBase connection to use for database operations
     * @throws UncheckedIOException if obtaining the {@link Admin} interface from the
     *         connection fails with an {@link IOException}
     */
    public HBaseExecutor(final Connection conn) {
        this(conn, DEFAULT_ASYNC_EXECUTOR);
    }

    /**
     * Constructs an {@code HBaseExecutor} bound to the given HBase {@link Connection}
     * and a caller-supplied {@link AsyncExecutor} for asynchronous operations.
     *
     * <p>Use this constructor when you want to share or constrain the thread pool that
     * drives the {@link AsyncHBaseExecutor} returned from {@link #async()}.</p>
     *
     * <p>If {@link Connection#getAdmin()} throws, the partially constructed admin is closed
     * quietly and the underlying {@link IOException} is rethrown wrapped in
     * {@link UncheckedIOException}.</p>
     *
     * @param conn the HBase connection to use for database operations; not copied — closing
     *        this executor closes the connection
     * @param asyncExecutor the executor that drives the {@link AsyncHBaseExecutor}
     * @throws UncheckedIOException if obtaining the {@link Admin} interface from the
     *         connection fails with an {@link IOException}
     */
    public HBaseExecutor(final Connection conn, final AsyncExecutor asyncExecutor) {
        Admin tmpAdmin = null;
        boolean noException = false;
        try {
            tmpAdmin = conn.getAdmin();
            this.admin = tmpAdmin;
            this.conn = conn;
            this.asyncHBaseExecutor = new AsyncHBaseExecutor(this, asyncExecutor);
            noException = true;
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            if (!noException && tmpAdmin != null) {
                IOUtil.closeQuietly(tmpAdmin);
            }
        }
    }

    /**
     * Returns the HBase {@link Admin} owned by this executor.
     *
     * <p>The admin interface exposes table-management and cluster-administration calls
     * (create/drop tables, manage regions, etc.). The returned instance is owned by this
     * executor and is closed by {@link #close()}; do not close it independently.</p>
     *
     * @return the HBase admin interface bound to this executor
     * @see Admin
     */
    public Admin admin() {
        return admin;
    }

    /**
     * Returns the {@link Connection} this executor wraps.
     *
     * <p>The connection is shared by every operation performed through this executor
     * and is closed by {@link #close()}. Exposed for advanced scenarios that need to
     * issue calls directly against the native HBase API.</p>
     *
     * @return the wrapped HBase {@link Connection}
     * @see Connection
     */
    public Connection connection() {
        return conn;
    }

    /**
     * Returns the asynchronous facade for non-blocking HBase operations.
     *
     * <p>The returned {@link AsyncHBaseExecutor} exposes the same operations as this
     * executor but submits them to the {@link AsyncExecutor} supplied at construction
     * (or {@link #DEFAULT_ASYNC_EXECUTOR}) and returns
     * {@link com.landawn.abacus.util.ContinuableFuture} results.</p>
     *
     * <p>The returned instance is owned by this executor and its lifetime is bound to
     * this {@code HBaseExecutor}; do not close it independently.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ContinuableFuture<Boolean> existsFuture = executor.async().exists("users", AnyGet.of("user123"));
     * ContinuableFuture<User>    userFuture   = executor.async().get("users", AnyGet.of("user123"), User.class);
     * }</pre>
     *
     * @return the asynchronous HBase executor bound to this executor
     * @see AsyncHBaseExecutor
     */
    public AsyncHBaseExecutor async() {
        return asyncHBaseExecutor;
    }

    /**
     * Manually registers a property of {@code cls} as the HBase row-key for the entity class.
     *
     * <p>The setter of the registered property is recorded and used by the result-to-entity
     * conversion path to populate the row-key value when reading rows. Calling this method
     * also clears any previously cached column-family/qualifier name maps for {@code cls}
     * so they are recomputed from the new row-key configuration.</p>
     *
     * <p><strong>Deprecated:</strong> prefer annotating the row-key field with {@code @Id}
     * from one of the following:</p>
     * <ul>
     *   <li>{@code com.landawn.abacus.annotation.Id}</li>
     *   <li>{@code javax.persistence.Id}</li>
     *   <li>{@code jakarta.persistence.Id}</li>
     * </ul>
     *
     * @param cls the entity class (must be a JavaBean class) on which to register the row-key property
     * @param rowKeyPropertyName the name of the property to use as the row key
     * @throws IllegalArgumentException if {@code cls} has no getter or setter for
     *         {@code rowKeyPropertyName}, or if the property's declared type is
     *         {@link HBaseColumn} (directly, as the single type-arg of a generic such as
     *         {@code Collection<HBaseColumn>}, or as the value type of a two-arg generic
     *         such as {@code Map<?, HBaseColumn>})
     * @see com.landawn.abacus.annotation.Id
     * @deprecated Annotate the row-key field with {@code @Id} instead.
     */
    @Deprecated
    public static void registerRowKeyProperty(final Class<?> cls, final String rowKeyPropertyName) {
        if (Beans.getPropGetter(cls, rowKeyPropertyName) == null || Beans.getPropSetter(cls, rowKeyPropertyName) == null) {
            throw new IllegalArgumentException("The specified class: " + ClassUtil.getCanonicalClassName(cls)
                    + " doesn't have getter or setter method for the specified row key property: " + rowKeyPropertyName);
        }

        final BeanInfo entityInfo = ParserUtil.getBeanInfo(cls);
        final PropInfo rowKeyPropInfo = entityInfo.getPropInfo(rowKeyPropertyName);
        final Method setMethod = rowKeyPropInfo.setMethod;

        if (HBaseColumn.class.equals(rowKeyPropInfo.clazz)
                || (rowKeyPropInfo.type.parameterTypes().size() == 1 && rowKeyPropInfo.type.parameterTypes().get(0).javaType().equals(HBaseColumn.class))
                || (rowKeyPropInfo.type.parameterTypes().size() == 2 && rowKeyPropInfo.type.parameterTypes().get(1).javaType().equals(HBaseColumn.class))) {
            throw new IllegalArgumentException(
                    "Unsupported row key property type: " + setMethod.toGenericString() + ". Row key property type cannot be HBaseColumn");
        }

        classRowKeySetMethodPool.put(cls, setMethod);

        classFamilyColumnNamePool.remove(cls);
        classFamilyColumnFieldNamePool.remove(cls);
    }

    @SuppressWarnings("deprecation")
    static <T> Method getRowKeySetMethod(final Class<T> targetClass) {
        Method rowKeySetMethod = classRowKeySetMethodPool.get(targetClass);

        if (rowKeySetMethod == null) {
            final List<String> ids = QueryUtil.getIdPropNames(targetClass);

            if (ids.size() > 1) {
                throw new IllegalArgumentException("Multiple ids: " + ids + " defined/annotated in class: " + ClassUtil.getCanonicalClassName(targetClass));
            } else if (ids.size() == 1) {
                registerRowKeyProperty(targetClass, ids.get(0));
                rowKeySetMethod = classRowKeySetMethodPool.get(targetClass);
            }

            if (rowKeySetMethod == null) {
                rowKeySetMethod = ClassUtil.SENTINEL_METHOD;
                classRowKeySetMethodPool.put(targetClass, rowKeySetMethod);
            }
        }

        return rowKeySetMethod == ClassUtil.SENTINEL_METHOD ? null : rowKeySetMethod;
    }

    static Map<String, Tuple3<String, String, Boolean>> getClassFamilyColumnNameMap(final Class<?> entityClass, final NamingPolicy namingPolicy) {
        final Map<NamingPolicy, Map<String, Tuple3<String, String, Boolean>>> namingPolicyFamilyColumnNameMap = classFamilyColumnNamePool
                .computeIfAbsent(entityClass, k -> new ConcurrentHashMap<>());

        return namingPolicyFamilyColumnNameMap.computeIfAbsent(namingPolicy, k -> {
            final Map<String, Tuple3<String, String, Boolean>> classFamilyColumnNameMap = new HashMap<>();

            final BeanInfo entityInfo = ParserUtil.getBeanInfo(entityClass);
            final ColumnFamily defaultColumnFamilyAnno = entityInfo.getAnnotation(ColumnFamily.class);
            final String defaultColumnFamilyName = defaultColumnFamilyAnno == null ? null : getAnnotatedColumnFamily(defaultColumnFamilyAnno);

            for (final PropInfo propInfo : entityInfo.propInfoList) {
                String columnFamilyName = null;
                String columnName = null;
                boolean hasColumnAnnotation = false;

                if (propInfo.isAnnotationPresent(ColumnFamily.class)) {
                    columnFamilyName = getAnnotatedColumnFamily(propInfo.getAnnotation(ColumnFamily.class));
                } else if (Strings.isNotEmpty(defaultColumnFamilyName)) {
                    columnFamilyName = defaultColumnFamilyName;
                } else {
                    columnFamilyName = formatName(propInfo.name, namingPolicy);
                }

                if (propInfo.columnName.isPresent()) {
                    columnName = propInfo.columnName.get();
                    hasColumnAnnotation = true;
                } else {
                    columnName = formatName(propInfo.name, namingPolicy);
                }

                classFamilyColumnNameMap.put(propInfo.name, Tuple.of(columnFamilyName, columnName, hasColumnAnnotation));
            }

            return classFamilyColumnNameMap;
        });
    }

    private static Tuple2<Map<String, Map<String, Tuple2<String, Boolean>>>, Map<String, String>> getFamilyColumnFieldNameMap(final Class<?> entityClass) {
        Tuple2<Map<String, Map<String, Tuple2<String, Boolean>>>, Map<String, String>> familyColumnFieldNameMapTP = classFamilyColumnFieldNamePool
                .get(entityClass);

        if (familyColumnFieldNameMapTP == null) {
            final BeanInfo entityInfo = ParserUtil.getBeanInfo(entityClass);
            final ColumnFamily defaultColumnFamily = entityInfo.getAnnotation(ColumnFamily.class);
            final String defaultColumnFamilyName = defaultColumnFamily == null ? null : getAnnotatedColumnFamily(defaultColumnFamily);

            familyColumnFieldNameMapTP = Tuple.of(new HashMap<>(), new HashMap<>(entityInfo.propInfoList.size()));

            final Tuple2<Map<String, Map<String, Tuple2<String, Boolean>>>, Map<String, String>> finalFamilyColumnFieldNameMapTP = familyColumnFieldNameMapTP;

            for (final PropInfo propInfo : entityInfo.propInfoList) {
                List<String> columnFamilyNames = null;
                List<String> columnNames = null;
                boolean hasColumnAnnotation = false;

                if (propInfo.isAnnotationPresent(ColumnFamily.class)) {
                    columnFamilyNames = N.asList(getAnnotatedColumnFamily(propInfo.getAnnotation(ColumnFamily.class)));
                } else if (Strings.isNotEmpty(defaultColumnFamilyName)) {
                    columnFamilyNames = N.asList(defaultColumnFamilyName);
                } else {
                    columnFamilyNames = Stream.of(NamingPolicy.values())
                            .map(it -> formatName(propInfo.name, it))
                            .filter(it -> !finalFamilyColumnFieldNameMapTP._1.containsKey(it))
                            .toList();
                }

                if (propInfo.columnName.isPresent()) {
                    columnNames = N.asList(propInfo.columnName.get());
                    hasColumnAnnotation = true;
                } else {
                    columnNames = Stream.of(NamingPolicy.values())
                            .map(it -> formatName(propInfo.name, it))
                            .filter(it -> !finalFamilyColumnFieldNameMapTP._2.containsKey(it))
                            .transform(s -> propInfo.type.isBean()
                                    || (Strings.isEmpty(defaultColumnFamilyName) && !propInfo.isAnnotationPresent(ColumnFamily.class))
                                            ? s.append(EMPTY_QUALIFIER)
                                            : s)
                            .toList();
                }

                for (final String columnFamilyName : columnFamilyNames) {
                    Map<String, Tuple2<String, Boolean>> columnFieldMap = familyColumnFieldNameMapTP._1.get(columnFamilyName);

                    if (columnFieldMap == null) {
                        columnFieldMap = new HashMap<>(columnNames.size());
                        familyColumnFieldNameMapTP._1.put(columnFamilyName, columnFieldMap);
                    }

                    for (final String columnName : columnNames) {
                        columnFieldMap.put(columnName, Tuple.of(propInfo.name, hasColumnAnnotation));
                    }
                }

                for (final String columnName : columnNames) {
                    familyColumnFieldNameMapTP._2.put(columnName, propInfo.name);
                }
            }

            classFamilyColumnFieldNamePool.put(entityClass, familyColumnFieldNameMapTP);
        }

        return familyColumnFieldNameMapTP;
    }

    private static String getAnnotatedColumnFamily(final ColumnFamily defaultColumnFamilyAnno) {
        return N.checkArgNotEmpty(defaultColumnFamilyAnno.value(), "Column Family can't be null or empty");
    }

    /**
     * Reads every {@link Result} from {@code resultScanner}, converts each into
     * {@code targetClass}, and returns them as a {@link List}.
     *
     * <p>Equivalent to {@code toList(resultScanner, 0, Integer.MAX_VALUE, targetClass)}.
     * The scanner is always closed (via {@link IOUtil#closeQuietly(java.io.Closeable)})
     * before this method returns, even on exception.</p>
     *
     * @param <T> the target type for conversion
     * @param resultScanner the HBase result scanner to drain
     * @param targetClass the target class — a JavaBean class or a single-value type
     *                    (e.g. {@code String}, {@code Integer}, {@code Date})
     * @return a list of converted objects (skipping no rows)
     * @throws UncheckedIOException if reading from {@code resultScanner} fails with an {@link IOException}
     * @see #toList(ResultScanner, int, int, Class)
     */
    public static <T> List<T> toList(final ResultScanner resultScanner, final Class<T> targetClass) {
        return toList(resultScanner, 0, Integer.MAX_VALUE, targetClass);
    }

    /**
     * Reads a windowed range of results from {@code resultScanner}, converts each into
     * {@code targetClass}, and returns them as a {@link List}.
     *
     * <p>The first {@code offset} results are read and discarded; up to {@code count}
     * subsequent results are converted via the per-row mapping for {@code targetClass}.
     * The scanner is always closed (via {@link IOUtil#closeQuietly(java.io.Closeable)})
     * before this method returns, even on exception.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Skip the first 50 results, then take up to 100
     * List<User> users = HBaseExecutor.toList(scanner, 50, 100, User.class);
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param resultScanner the HBase result scanner to process
     * @param offset the number of results to skip from the beginning; must be non-negative
     * @param count the maximum number of results to convert; must be non-negative
     * @param targetClass the target class — a JavaBean class or a single-value type
     *                    (e.g. {@code String}, {@code Integer}, {@code Date})
     * @return a list of converted objects from the requested window
     * @throws IllegalArgumentException if {@code offset} or {@code count} is negative
     * @throws UncheckedIOException if reading from {@code resultScanner} fails with an {@link IOException}
     */
    public static <T> List<T> toList(final ResultScanner resultScanner, int offset, int count, final Class<T> targetClass) {
        if (offset < 0 || count < 0) {
            throw new IllegalArgumentException("Offset and count can't be negative");
        }

        final Type<T> targetType = N.typeOf(targetClass);

        final BeanInfo entityInfo = targetType.isBean() ? ParserUtil.getBeanInfo(targetClass) : null;
        final Method rowKeySetMethod = targetType.isBean() ? getRowKeySetMethod(targetClass) : null;
        final Type<?> rowKeyType = rowKeySetMethod == null ? null : N.typeOf(rowKeySetMethod.getParameterTypes()[0]);
        final Map<String, Map<String, Tuple2<String, Boolean>>> familyFieldNameMap = targetType.isBean() ? getFamilyColumnFieldNameMap(targetClass)._1 : null;

        final List<T> resultList = new ArrayList<>();

        try {
            while (offset-- > 0 && resultScanner.next() != null) { //NOSONAR
            }

            Result result = null;

            while (count-- > 0 && (result = resultScanner.next()) != null) {
                resultList.add(toValue(targetType, entityInfo, rowKeySetMethod, rowKeyType, familyFieldNameMap, result));
            }

        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            IOUtil.closeQuietly(resultScanner);
        }

        return resultList;
    }

    /**
     * Converts an in-memory list of HBase {@link Result}s into entities of {@code targetClass}.
     *
     * <p>Each element of {@code results} is converted using the same per-row mapping as
     * {@link #toEntity(Result, Class)}. Elements for which {@link Result#isEmpty()} is
     * {@code true} are skipped, so the returned list may be shorter than {@code results}
     * and is not necessarily index-aligned with it.</p>
     *
     * @param <T> the target type for conversion
     * @param results the HBase results to convert
     * @param targetClass the target class — a JavaBean class or a single-value type
     *                    (e.g. {@code String}, {@code Integer}, {@code Date})
     * @return a list of converted entities; empty results are skipped
     * @throws UncheckedIOException if reading cells from any result fails with an {@link IOException}
     */
    static <T> List<T> toList(final List<Result> results, final Class<T> targetClass) {
        final Type<T> targetType = N.typeOf(targetClass);

        final BeanInfo entityInfo = targetType.isBean() ? ParserUtil.getBeanInfo(targetClass) : null;
        final Method rowKeySetMethod = targetType.isBean() ? getRowKeySetMethod(targetClass) : null;
        final Type<?> rowKeyType = rowKeySetMethod == null ? null : N.typeOf(rowKeySetMethod.getParameterTypes()[0]);
        final Map<String, Map<String, Tuple2<String, Boolean>>> familyFieldNameMap = targetType.isBean() ? getFamilyColumnFieldNameMap(targetClass)._1 : null;

        final List<T> resultList = new ArrayList<>(results.size());

        try {
            for (final Result result : results) {
                if (result.isEmpty()) {
                    continue;
                }

                resultList.add(toValue(targetType, entityInfo, rowKeySetMethod, rowKeyType, familyFieldNameMap, result));
            }
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }

        return resultList;
    }

    /**
     * Converts a single HBase {@link Result} into an instance of {@code targetClass}.
     *
     * <p>For JavaBean target classes this performs full object-relational mapping: the
     * row-key cell (if a row-key setter is registered or discovered via {@code @Id})
     * populates the row-key property, and each subsequent cell is matched to a bean
     * property using the entity's column-family/qualifier mapping. Cells whose family,
     * qualifier, or field name cannot be resolved are silently ignored. Bean properties
     * declared as {@link HBaseColumn}, {@code Collection<HBaseColumn>}, or
     * {@code Map<?, HBaseColumn>} are populated cell-by-cell with versioned values.</p>
     *
     * <p>For single-value target types the {@link Result} must contain exactly one cell,
     * whose value is decoded via {@link Type#valueOf(String)} into {@code T}; a result
     * with more cells triggers {@link IllegalArgumentException}. Map target types are
     * explicitly rejected.</p>
     *
     * <p>Entity-class requirements:</p>
     * <ul>
     *   <li>JavaBean conventions (getters/setters)</li>
     *   <li>{@code @ColumnFamily} / {@code @Column} for custom mapping; without them,
     *       field names become column-family names with empty qualifiers (or are flattened
     *       for nested bean fields)</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Result result = table.get(new Get(Bytes.toBytes("user123")));
     * User user = HBaseExecutor.toEntity(result, User.class);
     *
     * // Single-cell single-value extraction
     * String name = HBaseExecutor.toEntity(result, String.class);
     * }</pre>
     *
     * @param <T> the target type
     * @param result the HBase Result to convert
     * @param targetClass the target class — a JavaBean class with getter/setter methods or a
     *                    single-value type (e.g. {@code String}, {@code Integer}, {@code Date}).
     *                    {@link Map} types are not supported.
     * @return the converted entity, or the type's default value if the result is empty
     * @throws IllegalArgumentException if {@code targetClass} is a {@link Map} type, or if the
     *         result has more than one cell when {@code targetClass} is a single-value type
     * @throws UncheckedIOException if reading cells from {@code result} fails with an {@link IOException}
     * @see Result
     */
    public static <T> T toEntity(final Result result, final Class<T> targetClass) {
        return toValue(result, targetClass);
    }

    //    public static Map<String, Object> toMap(final Result result) {
    //        return toEntityOrMap(Clazz.PROPS_MAP, result);
    //    }
    //
    //    public static Map<String, Object> toMap(final Result result, final IntFunction<Map<String, Object>> mapSupplier) {
    //        return toEntityOrMap(Clazz.PROPS_MAP, result, mapSupplier);
    //    }

    /**
     * Package-private entry point that converts a single HBase {@link Result} to
     * {@code targetClass}, returning the {@link Type#defaultValue()} for empty results.
     *
     * <p>Used internally by {@link #toEntity(Result, Class)} and the per-table {@code get}
     * overloads. Wraps any {@link IOException} from the result's cell scanner in
     * {@link UncheckedIOException}.</p>
     *
     * @param <T> the target type for conversion
     * @param result the HBase Result to convert
     * @param targetClass the target class — a JavaBean class or a single-value type
     *                    (e.g. {@code String}, {@code Integer}, {@code Date})
     * @return the converted entity, or the type's default value if the result is empty
     * @throws IllegalArgumentException if {@code targetClass} is a {@link Map} type
     * @throws UncheckedIOException if reading cells from {@code result} fails with an {@link IOException}
     */
    static <T> T toValue(final Result result, final Class<T> targetClass) {
        final Type<T> targetType = N.typeOf(targetClass);

        try {
            return toValue(result, targetClass, targetType);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static <T> T toValue(final Result result, final Class<T> targetType, final Type<T> type) throws IOException {
        if (type.isMap()) {
            throw new IllegalArgumentException("Map type is not supported for HBase result conversion");
        }

        if (result.isEmpty() || !result.advance()) {
            return type.defaultValue();
        }

        if (type.isBean()) {
            final BeanInfo entityInfo = ParserUtil.getBeanInfo(targetType);
            final Method rowKeySetMethod = getRowKeySetMethod(targetType);
            final Type<?> rowKeyType = rowKeySetMethod == null ? null : N.typeOf(rowKeySetMethod.getParameterTypes()[0]);
            final Map<String, Map<String, Tuple2<String, Boolean>>> familyFieldNameMap = getFamilyColumnFieldNameMap(targetType)._1;

            return toValue(type, entityInfo, rowKeySetMethod, rowKeyType, familyFieldNameMap, result);
        } else {
            final CellScanner cellScanner = result.cellScanner();

            if (!cellScanner.advance()) {
                return type.defaultValue();
            }

            final Cell cell = cellScanner.current();

            final T value = type.valueOf(getValueString(cell));

            if (cellScanner.advance()) {
                throw new IllegalArgumentException("Cannot convert result with columns: " + getFamilyString(cell) + ":" + getQualifierString(cell)
                        + " to class: " + ClassUtil.getCanonicalClassName(type.javaType()));
            }

            return value;
        }
    }

    @SuppressWarnings({ "null", "rawtypes" })
    private static <T> T toValue(final Type<T> type, final BeanInfo entityInfo, final Method rowKeySetMethod, final Type<?> rowKeyType,
            final Map<String, Map<String, Tuple2<String, Boolean>>> familyFieldNameMap, final Result result) throws IOException {
        if (type.isMap()) {
            throw new IllegalArgumentException("Map type is not supported for HBase result conversion");
        }

        // Don't call result.advance() here as the empty check: a non-empty Result always has at least one cell,
        // and this method may be entered after the caller (3-arg toValue) already advanced the Result's internal
        // cell cursor. A second advance() would skip/consume the only cell of a single-cell Result and make it
        // look empty. Both branches below call result.cellScanner() which resets the cursor before reading.
        if (result.isEmpty()) {
            return type.defaultValue();
        }

        if (type.isBean()) {
            final Object entity = entityInfo.createBeanResult();
            final CellScanner cellScanner = result.cellScanner();

            Map<String, Map<String, Type<?>>> familyColumnValueTypeMap = null;
            Map<String, Map<String, Collection<HBaseColumn<?>>>> familyColumnCollectionMap = null;
            Map<String, Map<String, Map<Long, HBaseColumn<?>>>> familyColumnMapMap = null;

            Object rowKey = null;
            String family = null;
            String qualifier = null;
            String fieldName = null;
            PropInfo familyPropInfo = null;
            PropInfo columnPropInfo = null;
            Type<?> columnValueType = null;
            Map<String, Tuple2<String, Boolean>> familyTPMap = null;
            Tuple2<String, Boolean> familyTP = null;

            Map<String, Type<?>> columnValueTypeMap = null;
            Collection<HBaseColumn<?>> columnColl = null;
            Map<String, Collection<HBaseColumn<?>>> columnCollectionMap = null;
            Map<Long, HBaseColumn<?>> columnMap = null;
            Map<String, Map<Long, HBaseColumn<?>>> columnMapMap = null;
            HBaseColumn<?> column = null;

            while (cellScanner.advance()) {
                final Cell cell = cellScanner.current();

                if (rowKeyType != null && rowKey == null) {
                    rowKey = rowKeyType.valueOf(getRowKeyString(cell));
                    Beans.setPropValue(entity, rowKeySetMethod, rowKey);
                }

                family = getFamilyString(cell);
                qualifier = getQualifierString(cell);

                // .....................................................................................
                columnMapMap = familyColumnMapMap == null ? null : familyColumnMapMap.get(family);

                if (N.notEmpty(columnMapMap)) {
                    columnMap = columnMapMap.get(qualifier);

                    if (N.notEmpty(columnMap)) {
                        final Map<String, Type<?>> familyTypeMap = familyColumnValueTypeMap.get(family);
                        if (familyTypeMap != null) {
                            columnValueType = familyTypeMap.get(qualifier);
                            column = HBaseColumn.valueOf(columnValueType.valueOf(getValueString(cell)), cell.getTimestamp()); //NOSONAR
                            columnMap.put(column.version(), column);

                            continue;
                        }
                    }
                }

                // .....................................................................................
                columnCollectionMap = familyColumnCollectionMap == null ? null : familyColumnCollectionMap.get(family);

                if (N.notEmpty(columnCollectionMap)) {
                    columnColl = columnCollectionMap.get(qualifier);

                    if (N.notEmpty(columnColl)) {
                        final Map<String, Type<?>> familyTypeMap = familyColumnValueTypeMap.get(family);
                        if (familyTypeMap != null) {
                            columnValueType = familyTypeMap.get(qualifier);
                            column = HBaseColumn.valueOf(columnValueType.valueOf(getValueString(cell)), cell.getTimestamp());
                            columnColl.add(column);

                            continue;
                        }
                    }
                }

                // .....................................................................................
                familyTPMap = familyFieldNameMap.get(family);

                // ignore unknown column family.
                if (familyTPMap == null) {
                    continue;
                }

                familyTP = familyTPMap.get(qualifier);

                if (familyTP == null) {
                    familyTP = familyTPMap.get(EMPTY_QUALIFIER);
                }

                // ignore the unknown column:
                if (familyTP == null) {
                    continue;
                }

                fieldName = familyTP._1;
                familyPropInfo = entityInfo.getPropInfo(fieldName);

                // ignore the unknown field/property:
                if (familyPropInfo == null) {
                    continue;
                }

                if (familyPropInfo.jsonXmlType.isBean() && !familyTP._2) {
                    final Class<?> propEntityClass = familyPropInfo.jsonXmlType.javaType();
                    final Map<String, String> propEntityColumnFieldNameMap = getFamilyColumnFieldNameMap(propEntityClass)._2;
                    final BeanInfo propBeanInfo = ParserUtil.getBeanInfo(propEntityClass);
                    Object propEntity = familyPropInfo.getPropValue(entity);

                    if (propEntity == null) {
                        propEntity = N.newInstance(propEntityClass);

                        familyPropInfo.setPropValue(entity, propEntity);
                    }

                    columnPropInfo = propBeanInfo.getPropInfo(propEntityColumnFieldNameMap.getOrDefault(qualifier, qualifier));

                    // ignore the unknown property.
                    if (columnPropInfo == null) {
                        continue;
                    }

                    if (columnPropInfo.jsonXmlType.isMap() && columnPropInfo.jsonXmlType.parameterTypes().get(1).javaType().equals(HBaseColumn.class)) {
                        columnValueType = columnPropInfo.jsonXmlType.parameterTypes().get(1).elementType();

                        if (familyColumnValueTypeMap == null) {
                            familyColumnValueTypeMap = new HashMap<>();
                        } else {
                            columnValueTypeMap = familyColumnValueTypeMap.get(family);
                        }

                        if (columnValueTypeMap == null) {
                            columnValueTypeMap = new HashMap<>();
                            familyColumnValueTypeMap.put(family, columnValueTypeMap);
                        }

                        columnValueTypeMap.put(qualifier, columnValueType);
                        columnMap = N.<Long, HBaseColumn<?>> newMap((Class) columnPropInfo.jsonXmlType.javaType());
                        columnPropInfo.setPropValue(propEntity, columnMap);

                        if (columnMapMap == null) {
                            if (familyColumnMapMap == null) {
                                familyColumnMapMap = new HashMap<>();
                            }

                            columnMapMap = new HashMap<>();
                            familyColumnMapMap.put(family, columnMapMap);
                        }

                        columnMapMap.put(qualifier, columnMap);

                        column = HBaseColumn.valueOf(columnValueType.valueOf(getValueString(cell)), cell.getTimestamp());
                        columnMap.put(column.version(), column);
                    } else if (columnPropInfo.jsonXmlType.isCollection()
                            && columnPropInfo.jsonXmlType.parameterTypes().get(0).javaType().equals(HBaseColumn.class)) {
                        columnValueType = columnPropInfo.jsonXmlType.parameterTypes().get(0).elementType();

                        if (familyColumnValueTypeMap == null) {
                            familyColumnValueTypeMap = new HashMap<>();
                        } else {
                            columnValueTypeMap = familyColumnValueTypeMap.get(family);
                        }

                        if (columnValueTypeMap == null) {
                            columnValueTypeMap = new HashMap<>();
                            familyColumnValueTypeMap.put(family, columnValueTypeMap);
                        }

                        columnValueTypeMap.put(qualifier, columnValueType);
                        columnColl = N.newCollection((Class) columnPropInfo.jsonXmlType.javaType());
                        columnPropInfo.setPropValue(propEntity, columnColl);

                        if (columnCollectionMap == null) {
                            if (familyColumnCollectionMap == null) {
                                familyColumnCollectionMap = new HashMap<>();
                            }

                            columnCollectionMap = new HashMap<>();
                            familyColumnCollectionMap.put(family, columnCollectionMap);
                        }

                        columnCollectionMap.put(qualifier, columnColl);

                        column = HBaseColumn.valueOf(columnValueType.valueOf(getValueString(cell)), cell.getTimestamp());
                        columnColl.add(column);
                    } else if (columnPropInfo.jsonXmlType.javaType().equals(HBaseColumn.class)) {
                        if (familyColumnValueTypeMap == null) {
                            familyColumnValueTypeMap = new HashMap<>();
                        } else {
                            columnValueTypeMap = familyColumnValueTypeMap.get(family);
                        }

                        if (columnValueTypeMap == null) {
                            columnValueTypeMap = new HashMap<>();
                            familyColumnValueTypeMap.put(family, columnValueTypeMap);
                        }

                        columnValueType = columnValueTypeMap.get(qualifier);

                        if (columnValueType == null) {
                            columnValueType = columnPropInfo.jsonXmlType.parameterTypes().get(0);
                            columnValueTypeMap.put(qualifier, columnValueType);
                        }

                        column = HBaseColumn.valueOf(columnValueType.valueOf(getValueString(cell)), cell.getTimestamp());

                        columnPropInfo.setPropValue(propEntity, column);
                    } else {
                        columnPropInfo.setPropValue(propEntity, columnPropInfo.jsonXmlType.valueOf(getValueString(cell)));
                    }

                } else if (familyPropInfo.jsonXmlType.isMap() && familyPropInfo.jsonXmlType.parameterTypes().get(1).javaType().equals(HBaseColumn.class)) {
                    columnValueType = familyPropInfo.jsonXmlType.parameterTypes().get(1).elementType();

                    if (familyColumnValueTypeMap == null) {
                        familyColumnValueTypeMap = new HashMap<>();
                    } else {
                        columnValueTypeMap = familyColumnValueTypeMap.get(family);
                    }

                    if (columnValueTypeMap == null) {
                        columnValueTypeMap = new HashMap<>();
                        familyColumnValueTypeMap.put(family, columnValueTypeMap);
                    }

                    columnValueTypeMap.put(qualifier, columnValueType);
                    columnMap = N.<Long, HBaseColumn<?>> newMap((Class) familyPropInfo.jsonXmlType.javaType());
                    familyPropInfo.setPropValue(entity, columnMap);

                    if (columnMapMap == null) {
                        if (familyColumnMapMap == null) {
                            familyColumnMapMap = new HashMap<>();
                        }

                        columnMapMap = new HashMap<>();
                        familyColumnMapMap.put(family, columnMapMap);
                    }

                    columnMapMap.put(qualifier, columnMap);

                    column = HBaseColumn.valueOf(columnValueType.valueOf(getValueString(cell)), cell.getTimestamp());
                    columnMap.put(column.version(), column);
                } else if (familyPropInfo.jsonXmlType.isCollection()
                        && familyPropInfo.jsonXmlType.parameterTypes().get(0).javaType().equals(HBaseColumn.class)) {
                    columnValueType = familyPropInfo.jsonXmlType.parameterTypes().get(0).elementType();

                    if (familyColumnValueTypeMap == null) {
                        familyColumnValueTypeMap = new HashMap<>();
                    } else {
                        columnValueTypeMap = familyColumnValueTypeMap.get(family);
                    }

                    if (columnValueTypeMap == null) {
                        columnValueTypeMap = new HashMap<>();
                        familyColumnValueTypeMap.put(family, columnValueTypeMap);
                    }

                    columnValueTypeMap.put(qualifier, columnValueType);
                    columnColl = N.newCollection((Class) familyPropInfo.jsonXmlType.javaType());
                    familyPropInfo.setPropValue(entity, columnColl);

                    if (columnCollectionMap == null) {
                        if (familyColumnCollectionMap == null) {
                            familyColumnCollectionMap = new HashMap<>();
                        }

                        columnCollectionMap = new HashMap<>();
                        familyColumnCollectionMap.put(family, columnCollectionMap);
                    }

                    columnCollectionMap.put(qualifier, columnColl);

                    column = HBaseColumn.valueOf(columnValueType.valueOf(getValueString(cell)), cell.getTimestamp());
                    columnColl.add(column);
                } else if (familyPropInfo.jsonXmlType.javaType().equals(HBaseColumn.class)) {
                    if (familyColumnValueTypeMap == null) {
                        familyColumnValueTypeMap = new HashMap<>();
                    } else {
                        columnValueTypeMap = familyColumnValueTypeMap.get(family);
                    }

                    if (columnValueTypeMap == null) {
                        columnValueTypeMap = new HashMap<>();
                        familyColumnValueTypeMap.put(family, columnValueTypeMap);
                    }

                    columnValueType = columnValueTypeMap.get(qualifier);

                    if (columnValueType == null) {
                        columnValueType = familyPropInfo.jsonXmlType.parameterTypes().get(0);
                        columnValueTypeMap.put(qualifier, columnValueType);
                    }

                    column = HBaseColumn.valueOf(columnValueType.valueOf(getValueString(cell)), cell.getTimestamp());

                    familyPropInfo.setPropValue(entity, column);
                } else {
                    familyPropInfo.setPropValue(entity, familyPropInfo.jsonXmlType.valueOf(getValueString(cell)));
                }
            }

            return entityInfo.finishBeanResult(entity);
        } else {
            final CellScanner cellScanner = result.cellScanner();

            if (!cellScanner.advance()) {
                return type.defaultValue();
            }

            final Cell cell = cellScanner.current();

            final T value = type.valueOf(getValueString(cell));

            if (cellScanner.advance()) {
                throw new IllegalArgumentException("Cannot convert result with columns: " + getFamilyString(cell) + ":" + getQualifierString(cell)
                        + " to class: " + ClassUtil.getCanonicalClassName(type.javaType()));
            }

            return value;
        }
    }

    private static <T> Function<Result, T> createRowMapper(final Class<T> targetType) {
        return t -> toValue(t, targetType);
    }

    static <T> void checkEntityClass(final Class<T> targetType) {
        if (!Beans.isBeanClass(targetType)) {
            throw new IllegalArgumentException("Unsupported type: " + ClassUtil.getCanonicalClassName(targetType)
                    + ". Only Entity class generated by CodeGenerator with getter/setter methods are supported");
        }
    }

    private static String formatName(final String name, final NamingPolicy namingPolicy) {
        return namingPolicy == NamingPolicy.CAMEL_CASE ? name : namingPolicy.convert(name);
    }

    /**
     * Opens a fresh {@link Table} handle for {@code tableName} from this executor's
     * {@link Connection}.
     *
     * <p>Useful for direct access to HBase APIs that are not surfaced by this executor.
     * Note that {@link Connection#getTable(TableName)} returns a new lightweight wrapper
     * on each call — it does not validate that the table exists.</p>
     *
     * <p><strong>Resource ownership:</strong> the caller owns the returned {@link Table}
     * and must close it (typically via try-with-resources) to release any associated
     * region locator/thread-local resources.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * try (Table table = executor.getTable("users")) {
     *     Result result = table.get(new Get(Bytes.toBytes("user123")));
     *     // process result
     * }
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @return a new {@link Table} handle for {@code tableName}
     * @throws UncheckedIOException if the underlying call fails with an {@link IOException}
     * @see Table
     * @see org.apache.hadoop.hbase.TableName
     */
    public Table getTable(final String tableName) throws UncheckedIOException {
        if (logger.isDebugEnabled()) {
            logger.debug("Acquiring HBase table: {}", tableName);
        }

        try {
            return conn.getTable(TableName.valueOf(tableName));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @SuppressWarnings("rawtypes")
    private final Map<Class<?>, HBaseMapper> mapperPool = new ConcurrentHashMap<>();

    /**
     * Returns the cached typed {@link HBaseMapper} for {@code targetEntityClass}, resolving
     * the table name from the class's {@code @Table} annotation.
     *
     * <p>The returned mapper is cached per entity class on this executor and uses
     * {@link NamingPolicy#CAMEL_CASE}. The entity class requirements are:</p>
     * <ul>
     *   <li>Annotated with {@code @Table} (from
     *       {@code com.landawn.abacus.annotation}, {@code javax.persistence}, or
     *       {@code jakarta.persistence}) to supply the HBase table name</li>
     *   <li>Exactly one property marked with {@code @Id} (used as the row key)</li>
     *   <li>JavaBean conventions (getters/setters)</li>
     * </ul>
     *
     * <p>If no {@code @Table} annotation is present, use
     * {@link #mapper(Class, String, NamingPolicy)} to supply the table name explicitly.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * @Table("users")
     * public class User {
     *     @Id
     *     private String id;
     *     private String name;
     *     // getters and setters
     * }
     *
     * HBaseMapper<User, String> userMapper = executor.mapper(User.class);
     * User user = userMapper.get("user123");
     * userMapper.put(user);
     * }</pre>
     *
     * @param <T> the entity type
     * @param <K> the row key type
     * @param targetEntityClass an entity class carrying a {@code @Table} annotation
     * @return a cached typed mapper for the specified entity class
     * @throws IllegalArgumentException if {@code targetEntityClass} has no {@code @Table}
     *         annotation, has no {@code @Id} property, or has more than one {@code @Id} property
     * @see HBaseMapper
     * @see #mapper(Class, String, NamingPolicy)
     */
    public <T, K> HBaseMapper<T, K> mapper(final Class<T> targetEntityClass) {
        @SuppressWarnings("rawtypes")
        HBaseMapper mapper = mapperPool.get(targetEntityClass);

        if (mapper == null) {
            final BeanInfo entityInfo = ParserUtil.getBeanInfo(targetEntityClass);

            if (entityInfo.tableName.isEmpty()) {
                throw new IllegalArgumentException("Entity class " + targetEntityClass
                        + " must be annotated with @Table (com.landawn.abacus.annotation, javax.persistence, or jakarta.persistence). Alternatively, use HBaseExecutor.mapper(String tableName, Class<T> entityClass)");
            }

            mapper = mapper(targetEntityClass, entityInfo.tableName.get(), NamingPolicy.CAMEL_CASE);

            mapperPool.put(targetEntityClass, mapper);
        }

        return mapper;
    }

    /**
     * Builds a fresh {@link HBaseMapper} with an explicit table name and naming policy.
     *
     * <p>Use this overload when the entity class has no {@code @Table} annotation or when
     * you need a non-default naming policy. The returned mapper is <em>not</em> cached on
     * this executor (in contrast to {@link #mapper(Class)}).</p>
     *
     * <p>The naming policy controls how Java property names map to HBase column-family /
     * qualifier strings (e.g. {@link NamingPolicy#CAMEL_CASE} preserves names,
     * {@link NamingPolicy#SNAKE_CASE} converts {@code fieldName} to {@code field_name}).
     * A {@code null} {@code namingPolicy} is treated as {@link NamingPolicy#CAMEL_CASE}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * HBaseMapper<User, String> mapper = executor.mapper(
     *     User.class,
     *     "user_table",
     *     NamingPolicy.SNAKE_CASE
     * );
     * }</pre>
     *
     * @param <T> the entity type
     * @param <K> the row key type
     * @param targetEntityClass the entity class to map (must be a JavaBean class with one {@code @Id} property)
     * @param tableName the HBase table name to bind the mapper to; must not be empty
     * @param namingPolicy the naming policy for column name conversion; {@code null} maps to {@link NamingPolicy#CAMEL_CASE}
     * @return a configured (non-cached) typed mapper for the specified entity class and table
     * @throws IllegalArgumentException if {@code targetEntityClass} is not a bean class, has no
     *         {@code @Id} property, or has more than one {@code @Id} property; or if {@code tableName} is empty
     * @see NamingPolicy
     * @see HBaseMapper
     */
    public <T, K> HBaseMapper<T, K> mapper(final Class<T> targetEntityClass, final String tableName, final NamingPolicy namingPolicy) {
        return new HBaseMapper<>(targetEntityClass, this, tableName, namingPolicy);
    }

    private static void closeQuietly(final Table table) {
        IOUtil.closeQuietly(table);
    }

    // There is no too much benefit to add method for "Object rowKey"
    // And it may cause error because the "Object" is ambiguous to any type.
    boolean exists(final String tableName, final Object rowKey) throws UncheckedIOException {
        return exists(tableName, AnyGet.of(rowKey));
    }

    /**
     * Tests whether the specified Get operation would return any results.
     *
     * <p>This method performs a server-side existence check without transferring any data
     * to the client, making it more efficient than retrieving the full result when you only
     * need to know if data exists.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Get get = new Get(Bytes.toBytes("user123"));
     * get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
     * boolean exists = executor.exists("users", get);
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param get the Get operation to test for existence
     * @return {@code true} if the Get operation would return results, {@code false} otherwise
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @see Get
     */
    public boolean exists(final String tableName, final Get get) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            return table.exists(get);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Tests whether the specified Get operations would return any results.
     *
     * <p>This method performs batch server-side existence checks for multiple Get operations
     * without transferring any data to the client, making it efficient for checking existence
     * of multiple rows or cells.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Get> gets = Arrays.asList(
     *     new Get(Bytes.toBytes("user123")),
     *     new Get(Bytes.toBytes("user456"))
     * );
     * List<Boolean> results = executor.exists("users", gets);
     * // results.get(0) == true if user123 exists
     * // results.get(1) == true if user456 exists
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param gets the list of Get operations to test for existence
     * @return a list of Boolean values in the same order as {@code gets}, where the i-th entry
     *         is {@code true} if the i-th Get would match one or more cells, {@code false} otherwise
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @see Get
     */
    public List<Boolean> exists(final String tableName, final List<Get> gets) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            return BooleanList.of(table.exists(gets)).boxed();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    //    /**
    //     * Test for the existence of columns in the table, as specified by the Gets.
    //     * This will return an array of booleans. Each value will be true if the related Get matches
    //     * one or more keys, {@code false} if not.
    //     * This is a server-side call so it prevents any data from being transferred to
    //     * the client.
    //     *
    //     * @param tableName
    //     * @param gets
    //     * @return Array of boolean.  True if the specified Get matches one or more keys, {@code false} if not.
    //     * @throws UncheckedIOException the unchecked IO exception
    //     * @deprecated since 2.0 version and will be removed in 3.0 version.
    //     *             use {@code exists(List)}
    //     */
    //    @Deprecated
    //    public List<Boolean> existsAll(final String tableName, final List<Get> gets) throws UncheckedIOException {
    //        final Table table = getTable(tableName);
    //
    //        try {
    //            return BooleanList.of(table.existsAll(gets)).toList();
    //        } catch (IOException e) {
    //            throw new UncheckedIOException(e);
    //        } finally {
    //            closeQuietly(table);
    //        }
    //    }

    /**
     * Tests whether the specified AnyGet operation would return any results.
     *
     * <p>This is a convenience wrapper around {@link #exists(String, Get)} that accepts
     * an AnyGet instance, which provides a fluent API for building Get operations.</p>
     *
     * @param tableName the name of the HBase table
     * @param anyGet the AnyGet operation to test for existence
     * @return {@code true} if the Get operation would return results, {@code false} otherwise
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @see AnyGet
     * @see #exists(String, Get)
     */
    public boolean exists(final String tableName, final AnyGet anyGet) throws UncheckedIOException {
        return exists(tableName, anyGet.val());
    }

    /**
     * Tests whether the specified AnyGet operations would return any results.
     *
     * <p>This is a convenience wrapper around {@link #exists(String, List)} that accepts
     * a collection of AnyGet instances for batch existence checking.</p>
     *
     * @param tableName the name of the HBase table
     * @param anyGets the collection of AnyGet operations to test for existence
     * @return a list of Boolean values corresponding to each AnyGet operation
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @see AnyGet
     * @see #exists(String, List)
     */
    public List<Boolean> exists(final String tableName, final Collection<AnyGet> anyGets) throws UncheckedIOException {
        return exists(tableName, AnyGet.toGet(anyGets));
    }

    //    @Deprecated
    //    public List<Boolean> existsAll(final String tableName, final Collection<AnyGet> anyGets) throws UncheckedIOException {
    //        return existsAll(tableName, AnyGet.toGet(anyGets));
    //    }

    // There is no too much benefit to add method for "Object rowKey"
    // And it may cause error because the "Object" is ambiguous to any type.
    Result get(final String tableName, final Object rowKey) throws UncheckedIOException {
        return get(tableName, AnyGet.of(rowKey));
    }

    /**
     * Retrieves data from HBase using the specified Get operation.
     *
     * <p>This method executes the Get operation against the specified table and returns
     * the raw HBase Result. The Result can then be processed manually or converted to
     * an entity using the {@link #toEntity(Result, Class)} method.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Get get = new Get(Bytes.toBytes("user123"));
     * get.addFamily(Bytes.toBytes("info"));
     * Result result = executor.get("users", get);
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param get the Get operation specifying what data to retrieve
     * @return the HBase Result containing the retrieved data, or empty Result if no data found
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @see Get
     * @see Result
     */
    public Result get(final String tableName, final Get get) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            return table.get(get);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Retrieves multiple rows of data from HBase using a batch of Get operations.
     *
     * <p>This method executes multiple Get operations in a single batch call, which is more
     * efficient than executing them individually. The results are returned in the same order
     * as the input Get operations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Get> gets = Arrays.asList(
     *     new Get(Bytes.toBytes("user123")),
     *     new Get(Bytes.toBytes("user456"))
     * );
     * List<Result> results = executor.get("users", gets);
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param gets the list of Get operations to execute
     * @return a list of Results in the same order as {@code gets}; entries for rows that
     *         do not exist are present in the list but are {@linkplain Result#isEmpty() empty}
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @see Get
     * @see Result
     */
    public List<Result> get(final String tableName, final List<Get> gets) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            return N.toList(table.get(gets));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Retrieves data from HBase using an AnyGet operation.
     *
     * <p>This is a convenience wrapper around {@link #get(String, Get)} that accepts
     * an AnyGet instance, which provides a fluent API for building Get operations.</p>
     *
     * @param tableName the name of the HBase table
     * @param anyGet the AnyGet operation specifying what data to retrieve
     * @return the HBase Result containing the retrieved data
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @see AnyGet
     * @see #get(String, Get)
     */
    public Result get(final String tableName, final AnyGet anyGet) throws UncheckedIOException {
        return get(tableName, anyGet.val());
    }

    /**
     * Retrieves multiple rows of data from HBase using a collection of AnyGet operations.
     *
     * <p>This is a convenience wrapper around {@link #get(String, List)} that accepts
     * a collection of AnyGet instances for batch retrieval.</p>
     *
     * @param tableName the name of the HBase table
     * @param anyGets the collection of AnyGet operations to execute
     * @return a list of Results corresponding to each AnyGet operation
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @see AnyGet
     * @see #get(String, List)
     */
    public List<Result> get(final String tableName, final Collection<AnyGet> anyGets) throws UncheckedIOException {
        return get(tableName, AnyGet.toGet(anyGets));
    }

    // There is no too much benefit to add method for "Object rowKey"
    // And it may cause error because the "Object" is ambiguous to any type.
    <T> T get(final String tableName, final Object rowKey, final Class<T> targetType) throws UncheckedIOException {
        return get(tableName, AnyGet.of(rowKey), targetType);
    }

    /**
     * Retrieves and converts HBase data to the specified target type.
     *
     * <p>This method combines data retrieval and type conversion in a single operation.
     * It executes the Get operation and automatically converts the result to the specified
     * target type using the configured mapping strategy.</p>
     *
     * <p>Supports conversion to:</p>
     * <ul>
     * <li>Entity classes with getter/setter methods</li>
     * <li>Basic value types (String, Integer, Long, Date, etc.)</li>
     * <li>Custom types with appropriate type converters</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Get get = new Get(Bytes.toBytes("user123"));
     * User user = executor.get("users", get, User.class);
     * String userName = executor.get("users", get, String.class);   // single cell value
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table
     * @param get the Get operation specifying what data to retrieve
     * @param targetType the class to convert the result to
     * @return the converted object of the specified type, or null/default if no data found
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @see #get(String, Get)
     * @see #toEntity(Result, Class)
     */
    public <T> T get(final String tableName, final Get get, final Class<T> targetType) throws UncheckedIOException {
        return toValue(get(tableName, get), targetType);
    }

    /**
     * Retrieves multiple rows and converts them to the specified target type.
     *
     * <p>This method executes multiple Get operations in batch and converts each result
     * to the specified target type using the configured mapping strategy.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Get> gets = Arrays.asList(
     *     new Get(Bytes.toBytes("user123")),
     *     new Get(Bytes.toBytes("user456"))
     * );
     * List<User> users = executor.get("users", gets, User.class);
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table
     * @param gets the list of Get operations to execute
     * @param targetType the class to convert each result to
     * @return a list of converted objects of the specified type. Empty results (rows that
     *         did not exist) are skipped, so the returned list may contain fewer elements
     *         than {@code gets} and does not necessarily correspond positionally to it.
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @see #get(String, List)
     * @see #toEntity(Result, Class)
     */
    public <T> List<T> get(final String tableName, final List<Get> gets, final Class<T> targetType) throws UncheckedIOException {
        return toList(get(tableName, gets), targetType);
    }

    /**
     * Retrieves and converts HBase data to the specified target type using an AnyGet operation.
     *
     * <p>This is a convenience wrapper that combines {@link #get(String, AnyGet)} with
     * automatic type conversion.</p>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table
     * @param anyGet the AnyGet operation specifying what data to retrieve
     * @param targetType the class to convert the result to
     * @return the converted object of the specified type
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @see AnyGet
     * @see #get(String, Get, Class)
     */
    public <T> T get(final String tableName, final AnyGet anyGet, final Class<T> targetType) throws UncheckedIOException {
        return toValue(get(tableName, anyGet), targetType);
    }

    /**
     * Retrieves multiple rows using AnyGet operations and converts them to the specified target type.
     *
     * <p>This is a convenience wrapper that combines {@link #get(String, Collection)} with
     * automatic batch type conversion.</p>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table
     * @param anyGets the collection of AnyGet operations to execute
     * @param targetType the class to convert each result to
     * @return a list of converted objects of the specified type. Empty results (rows that
     *         did not exist) are skipped, so the returned list may contain fewer elements
     *         than {@code anyGets} and does not necessarily correspond positionally to it.
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @see AnyGet
     * @see #get(String, List, Class)
     */
    public <T> List<T> get(final String tableName, final Collection<AnyGet> anyGets, final Class<T> targetType) throws UncheckedIOException {
        return toList(get(tableName, anyGets), targetType);
    }

    /**
     * Performs a scan operation on the specified column family and returns a stream of Results.
     *
     * <p>This convenience method creates a Scan operation that retrieves all columns from
     * the specified column family across all rows in the table.</p>
     *
     * @param tableName the name of the HBase table to scan
     * @param family the name of the column family to scan
     * @return a lazy stream of HBase Results from the scan operation
     * @see #scan(String, Scan)
     */
    public Stream<Result> scan(final String tableName, final String family) {
        return scan(tableName, AnyScan.create().addFamily(family));
    }

    /**
     * Performs a scan operation on a specific column and returns a stream of Results.
     *
     * <p>This convenience method creates a Scan operation that retrieves only the specified
     * column (family:qualifier) across all rows in the table.</p>
     *
     * @param tableName the name of the HBase table to scan
     * @param family the name of the column family
     * @param qualifier the column qualifier
     * @return a lazy stream of HBase Results from the scan operation
     * @see #scan(String, Scan)
     */
    public Stream<Result> scan(final String tableName, final String family, final String qualifier) {
        return scan(tableName, AnyScan.create().addColumn(family, qualifier));
    }

    /**
     * Performs a scan operation on the specified column family (byte array) and returns a stream of Results.
     *
     * <p>This is the byte array version of {@link #scan(String, String)}.</p>
     *
     * @param tableName the name of the HBase table to scan
     * @param family the column family as a byte array
     * @return a lazy stream of HBase Results from the scan operation
     * @see #scan(String, Scan)
     */
    public Stream<Result> scan(final String tableName, final byte[] family) {
        return scan(tableName, AnyScan.create().addFamily(family));
    }

    /**
     * Performs a scan operation on a specific column (byte arrays) and returns a stream of Results.
     *
     * <p>This is the byte array version of {@link #scan(String, String, String)}.</p>
     *
     * @param tableName the name of the HBase table to scan
     * @param family the column family as a byte array
     * @param qualifier the column qualifier as a byte array
     * @return a lazy stream of HBase Results from the scan operation
     * @see #scan(String, Scan)
     */
    public Stream<Result> scan(final String tableName, final byte[] family, final byte[] qualifier) {
        return scan(tableName, AnyScan.create().addColumn(family, qualifier));
    }

    /**
     * Performs a scan operation using an AnyScan specification and returns a stream of Results.
     *
     * <p>This is a convenience wrapper around {@link #scan(String, Scan)} that accepts
     * an AnyScan instance, which provides a fluent API for building Scan operations.</p>
     *
     * @param tableName the name of the HBase table to scan
     * @param anyScan the AnyScan operation defining the scan parameters
     * @return a lazy stream of HBase Results from the scan operation
     * @see AnyScan
     * @see #scan(String, Scan)
     */
    public Stream<Result> scan(final String tableName, final AnyScan anyScan) {
        return scan(tableName, anyScan.val());
    }

    /**
     * Performs a scan against the specified HBase table and returns a lazy {@link Stream}
     * of {@link Result}s.
     *
     * <p>The returned stream is deferred: the underlying {@link Table} and
     * {@link ResultScanner} are opened only when iteration begins, and both are closed
     * when the stream is closed (whether by reaching the end of iteration, calling
     * {@link Stream#close()}, or exiting a try-with-resources block).</p>
     *
     * <p><strong>Resource ownership:</strong> the stream owns the table and scanner it
     * opens. Always consume scan streams inside a try-with-resources block, or call
     * {@link Stream#close()} explicitly, to avoid leaking the table connection.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Scan scan = new Scan();
     * scan.addFamily(Bytes.toBytes("info"));
     * scan.setStartRow(Bytes.toBytes("user_"));
     * scan.setStopRow(Bytes.toBytes("user_zzz"));
     *
     * try (Stream<Result> stream = executor.scan("users", scan)) {
     *     stream.limit(100)
     *           .filter(result -> !result.isEmpty())
     *           .forEach(this::processResult);
     * }
     * }</pre>
     *
     * @param tableName the name of the HBase table to scan
     * @param scan the Scan operation defining the scan parameters
     * @return a lazy, closable {@link Stream} of HBase {@link Result}s
     * @throws IllegalArgumentException if {@code tableName} or {@code scan} is {@code null}
     * @throws UncheckedIOException if opening the table or scanner fails with an {@link IOException}
     * @see Scan
     * @see Result
     * @see Stream
     */
    public Stream<Result> scan(final String tableName, final Scan scan) {
        N.checkArgNotNull(tableName, "tableName");
        N.checkArgNotNull(scan, "scan");

        final ObjIteratorEx<Result> lazyIter = ObjIteratorEx.defer(new Supplier<ObjIteratorEx<Result>>() {
            private ObjIteratorEx<Result> internalIter = null;

            @Override
            public ObjIteratorEx<Result> get() {
                if (internalIter == null) {
                    final Table table = getTable(tableName);

                    try {
                        final ResultScanner resultScanner = table.getScanner(scan);
                        final Iterator<Result> iter = resultScanner.iterator();

                        internalIter = new ObjIteratorEx<>() {
                            @Override
                            public boolean hasNext() {
                                return iter.hasNext();
                            }

                            @Override
                            public Result next() {
                                return iter.next();
                            }

                            @Override
                            public void close() {
                                try {
                                    IOUtil.closeQuietly(resultScanner);
                                } finally {
                                    IOUtil.closeQuietly(table);
                                }
                            }
                        };
                    } catch (final IOException e) {
                        throw new UncheckedIOException(e);
                    } finally {
                        if (internalIter == null) {
                            IOUtil.closeQuietly(table);
                        }
                    }
                }

                return internalIter;
            }
        });

        return Stream.of(lazyIter).onClose(lazyIter::close);
    }

    /**
     * Scans a column family and converts results to the specified target type.
     *
     * <p>This method combines scanning with automatic type conversion, returning a stream
     * of typed objects instead of raw Results.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * try (Stream<User> users = executor.scan("users", "info", User.class)) {
     *     users.forEach(System.out::println);
     * }
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to scan
     * @param family the name of the column family to scan
     * @param targetType the class to convert each result to
     * @return a lazy stream of converted objects
     * @see #scan(String, String)
     */
    public <T> Stream<T> scan(final String tableName, final String family, final Class<T> targetType) {
        //noinspection resource
        return scan(tableName, family).map(createRowMapper(targetType));
    }

    /**
     * Scans a specific column and converts results to the specified target type.
     *
     * <p>This method scans only the specified column (family:qualifier) and converts
     * each result to the target type.</p>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to scan
     * @param family the name of the column family
     * @param qualifier the column qualifier
     * @param targetType the class to convert each result to
     * @return a lazy stream of converted objects
     * @see #scan(String, String, String)
     */
    public <T> Stream<T> scan(final String tableName, final String family, final String qualifier, final Class<T> targetType) {
        //noinspection resource
        return scan(tableName, family, qualifier).map(createRowMapper(targetType));
    }

    /**
     * Scans a column family (byte array) and converts results to the specified target type.
     *
     * <p>This is the byte array version of {@link #scan(String, String, Class)}.</p>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to scan
     * @param family the column family as a byte array
     * @param targetType the class to convert each result to
     * @return a lazy stream of converted objects
     * @see #scan(String, byte[])
     */
    public <T> Stream<T> scan(final String tableName, final byte[] family, final Class<T> targetType) {
        //noinspection resource
        return scan(tableName, family).map(createRowMapper(targetType));
    }

    /**
     * Scans a specific column (byte arrays) and converts results to the specified target type.
     *
     * <p>This is the byte array version of {@link #scan(String, String, String, Class)}.</p>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to scan
     * @param family the column family as a byte array
     * @param qualifier the column qualifier as a byte array
     * @param targetType the class to convert each result to
     * @return a lazy stream of converted objects
     * @see #scan(String, byte[], byte[])
     */
    public <T> Stream<T> scan(final String tableName, final byte[] family, final byte[] qualifier, final Class<T> targetType) {
        //noinspection resource
        return scan(tableName, family, qualifier).map(createRowMapper(targetType));
    }

    /**
     * Scans using AnyScan specification and converts results to the specified target type.
     *
     * <p>This method combines the fluent AnyScan API with automatic type conversion.</p>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to scan
     * @param anyScan the AnyScan operation defining the scan parameters
     * @param targetType the class to convert each result to
     * @return a lazy stream of converted objects
     * @see AnyScan
     * @see #scan(String, AnyScan)
     */
    public <T> Stream<T> scan(final String tableName, final AnyScan anyScan, final Class<T> targetType) {
        //noinspection resource
        return scan(tableName, anyScan).map(createRowMapper(targetType));
    }

    /**
     * Scans using a Scan operation and converts results to the specified target type.
     *
     * <p>This method combines scanning with automatic type conversion, providing a stream
     * of typed objects that can be processed using stream operations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Scan scan = new Scan();
     * scan.setStartRow(Bytes.toBytes("user_a"));
     * scan.setStopRow(Bytes.toBytes("user_z"));
     *
     * try (Stream<User> users = executor.scan("users", scan, User.class)) {
     *     List<User> activeUsers = users
     *         .filter(user -> user.isActive())
     *         .limit(100)
     *         .collect(Collectors.toList());
     * }
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to scan
     * @param scan the Scan operation defining the scan parameters
     * @param targetType the class to convert each result to
     * @return a lazy stream of converted objects
     * @see Scan
     * @see #scan(String, Scan)
     */
    public <T> Stream<T> scan(final String tableName, final Scan scan, final Class<T> targetType) {
        //noinspection resource
        return scan(tableName, scan).map(createRowMapper(targetType));
    }

    /**
     * Stores data in HBase using the specified Put operation.
     *
     * <p>This method executes a single Put operation to store data in the specified table.
     * The Put operation can contain multiple column families and qualifiers with their values.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Put put = new Put(Bytes.toBytes("user123"));
     * put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("John Doe"));
     * put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("email"), Bytes.toBytes("john@example.com"));
     * executor.put("users", put);
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param put the Put operation containing the data to store
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @see Put
     */
    public void put(final String tableName, final Put put) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            table.put(put);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Stores multiple rows of data in HBase using a batch of Put operations.
     *
     * <p>This method executes multiple Put operations in a single batch call, which is more
     * efficient than executing them individually. All puts are sent to the server in one request.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Put> puts = new ArrayList<>();
     * puts.add(new Put(Bytes.toBytes("user123"))
     *     .addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("John")));
     * puts.add(new Put(Bytes.toBytes("user456"))
     *     .addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("Jane")));
     * executor.put("users", puts);
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param puts the list of Put operations to execute
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @see Put
     */
    public void put(final String tableName, final List<Put> puts) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            table.put(puts);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Stores data in HBase using an AnyPut operation.
     *
     * <p>This is a convenience wrapper around {@link #put(String, Put)} that accepts
     * an AnyPut instance, which provides a fluent API for building Put operations.</p>
     *
     * @param tableName the name of the HBase table
     * @param anyPut the AnyPut operation containing the data to store
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @see AnyPut
     * @see #put(String, Put)
     */
    public void put(final String tableName, final AnyPut anyPut) throws UncheckedIOException {
        put(tableName, anyPut.val());
    }

    /**
     * Stores multiple rows of data in HBase using a collection of AnyPut operations.
     *
     * <p>This is a convenience wrapper around {@link #put(String, List)} that accepts
     * a collection of AnyPut instances for batch storage.</p>
     *
     * @param tableName the name of the HBase table
     * @param anyPuts the collection of AnyPut operations to execute
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @see AnyPut
     * @see #put(String, List)
     */
    public void put(final String tableName, final Collection<AnyPut> anyPuts) throws UncheckedIOException {
        put(tableName, AnyPut.toPut(anyPuts));
    }

    // There is no too much benefit to add method for "Object rowKey"
    // And it may cause error because the "Object" is ambiguous to any type.
    void delete(final String tableName, final Object rowKey) throws UncheckedIOException {
        delete(tableName, AnyDelete.of(rowKey));
    }

    /**
     * Deletes data from HBase using the specified Delete operation.
     *
     * <p>This method executes a single Delete operation to remove data from the specified table.
     * The Delete operation can target entire rows, specific column families, or individual columns.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Delete entire row
     * Delete delete = new Delete(Bytes.toBytes("user123"));
     * executor.delete("users", delete);
     *
     * // Delete specific column
     * Delete deleteColumn = new Delete(Bytes.toBytes("user123"));
     * deleteColumn.addColumn(Bytes.toBytes("info"), Bytes.toBytes("email"));
     * executor.delete("users", deleteColumn);
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param delete the Delete operation specifying what data to remove
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @see Delete
     */
    public void delete(final String tableName, final Delete delete) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            table.delete(delete);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Deletes multiple rows or cells from HBase using a batch of Delete operations.
     *
     * <p>This method executes multiple Delete operations in a single batch call, which is more
     * efficient than executing them individually. All deletes are sent to the server in one request.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Delete> deletes = Arrays.asList(
     *     new Delete(Bytes.toBytes("user123")),
     *     new Delete(Bytes.toBytes("user456"))
     * );
     * executor.delete("users", deletes);
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param deletes the list of Delete operations to execute
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @see Delete
     */
    public void delete(final String tableName, final List<Delete> deletes) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            table.delete(deletes);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Deletes data from HBase using an AnyDelete operation.
     *
     * <p>This is a convenience wrapper around {@link #delete(String, Delete)} that accepts
     * an AnyDelete instance, which provides a fluent API for building Delete operations.</p>
     *
     * @param tableName the name of the HBase table
     * @param anyDelete the AnyDelete operation specifying what data to remove
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @see AnyDelete
     * @see #delete(String, Delete)
     */
    public void delete(final String tableName, final AnyDelete anyDelete) throws UncheckedIOException {
        delete(tableName, anyDelete.val());
    }

    /**
     * Deletes multiple rows or cells from HBase using a collection of AnyDelete operations.
     *
     * <p>This is a convenience wrapper around {@link #delete(String, List)} that accepts
     * a collection of AnyDelete instances for batch deletion.</p>
     *
     * @param tableName the name of the HBase table
     * @param anyDeletes the collection of AnyDelete operations to execute
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @see AnyDelete
     * @see #delete(String, List)
     */
    public void delete(final String tableName, final Collection<AnyDelete> anyDeletes) throws UncheckedIOException {
        delete(tableName, AnyDelete.toDelete(anyDeletes));
    }

    /**
     * Performs multiple mutations atomically on a single row using an AnyRowMutations operation.
     *
     * <p>This is a convenience wrapper around {@link #mutateRow(String, RowMutations)}.</p>
     *
     * @param tableName the name of the HBase table
     * @param rm the AnyRowMutations containing the atomic mutations to perform
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @see AnyRowMutations
     * @see #mutateRow(String, RowMutations)
     */
    public void mutateRow(final String tableName, final AnyRowMutations rm) throws UncheckedIOException {
        mutateRow(tableName, rm.val());
    }

    /**
     * Performs multiple mutations atomically on a single row.
     *
     * <p>This method allows combining multiple Put and Delete operations on the same row
     * into a single atomic operation. All mutations either succeed together or fail together.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * RowMutations mutations = new RowMutations(Bytes.toBytes("user123"));
     * mutations.add(new Put(Bytes.toBytes("user123"))
     *     .addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("John")));
     * mutations.add(new Delete(Bytes.toBytes("user123"))
     *     .addColumn(Bytes.toBytes("temp"), Bytes.toBytes("old_data")));
     * executor.mutateRow("users", mutations);
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param rm the RowMutations containing the atomic mutations to perform
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @see RowMutations
     */
    public void mutateRow(final String tableName, final RowMutations rm) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            table.mutateRow(rm);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Appends values to one or more columns within a single row using an AnyAppend operation.
     *
     * <p>This is a convenience wrapper around {@link #append(String, Append)}.</p>
     *
     * @param tableName the name of the HBase table
     * @param append the AnyAppend operation specifying the values to append
     * @return the Result containing the new values after the append operation
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @see AnyAppend
     * @see #append(String, Append)
     */
    public Result append(final String tableName, final AnyAppend append) throws UncheckedIOException {
        return append(tableName, append.val());
    }

    /**
     * Appends values to one or more columns within a single row.
     *
     * <p>This operation atomically appends data to existing cell values. If the cell doesn't
     * exist, it's treated as an empty value. This is particularly useful for maintaining
     * lists or sequences in HBase cells.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Append append = new Append(Bytes.toBytes("user123"));
     * append.addColumn(Bytes.toBytes("logs"), Bytes.toBytes("access"),
     *     Bytes.toBytes(",2023-10-15"));
     * Result result = executor.append("users", append);
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param append the Append operation specifying the values to append
     * @return the Result containing the new values after the append operation
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @see Append
     */
    public Result append(final String tableName, final Append append) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            return table.append(append);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Increments one or more column values within a single row using an AnyIncrement operation.
     *
     * <p>This is a convenience wrapper around {@link #increment(String, Increment)}.</p>
     *
     * @param tableName the name of the HBase table
     * @param increment the AnyIncrement operation specifying the values to increment
     * @return the Result containing the new values after the increment operation
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @see AnyIncrement
     * @see #increment(String, Increment)
     */
    public Result increment(final String tableName, final AnyIncrement increment) throws UncheckedIOException {
        return increment(tableName, increment.val());
    }

    /**
     * Increments (or decrements) one or more column values within a single row.
     *
     * <p>This operation atomically increments numeric cell values. If the cell doesn't exist,
     * it's treated as zero before incrementing. Negative amounts can be used for decrementing.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Increment increment = new Increment(Bytes.toBytes("user123"));
     * increment.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("login_count"), 1);
     * increment.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("points"), 100);
     * Result result = executor.increment("users", increment);
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param increment the Increment operation specifying the values to increment
     * @return the Result containing the new values after the increment operation
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @see Increment
     */
    public Result increment(final String tableName, final Increment increment) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            return table.increment(increment);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Atomically increments a single column's numeric value and returns the post-increment value.
     *
     * <p>{@code rowKey} is converted to bytes via {@link #toRowKeyBytes(Object)} (so it may be
     * any of the types accepted by that helper); {@code family} and {@code qualifier} are
     * encoded via the cached {@link #toFamilyQualifierBytes(String)}. If the cell does not
     * exist it is treated as zero before incrementing. {@code amount} may be negative to
     * decrement.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long newValue = executor.incrementColumnValue(
     *     "users", "user123", "stats", "login_count", 1);
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param rowKey the row key (converted to bytes via {@link #toRowKeyBytes(Object)})
     * @param family the column family name (converted to bytes via {@link #toFamilyQualifierBytes(String)})
     * @param qualifier the column qualifier name (converted to bytes via {@link #toFamilyQualifierBytes(String)})
     * @param amount the amount to add (negative values decrement)
     * @return the value of the column after the increment
     * @throws UncheckedIOException if the HBase call fails with an {@link IOException}
     */
    public long incrementColumnValue(final String tableName, final Object rowKey, final String family, final String qualifier, final long amount)
            throws UncheckedIOException {
        return incrementColumnValue(tableName, rowKey, toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), amount);
    }

    /**
     * Atomically increments a single column's numeric value with the specified
     * {@link Durability} guarantee.
     *
     * <p>{@code rowKey}, {@code family}, and {@code qualifier} are converted to bytes as
     * documented on {@link #incrementColumnValue(String, Object, String, String, long)}.</p>
     *
     * @param tableName the name of the HBase table
     * @param rowKey the row key (converted via {@link #toRowKeyBytes(Object)})
     * @param family the column family name (converted via {@link #toFamilyQualifierBytes(String)})
     * @param qualifier the column qualifier name (converted via {@link #toFamilyQualifierBytes(String)})
     * @param amount the amount to add (negative values decrement)
     * @param durability the durability level to use for the WAL write
     * @return the value of the column after the increment
     * @throws UncheckedIOException if the HBase call fails with an {@link IOException}
     * @see Durability
     */
    public long incrementColumnValue(final String tableName, final Object rowKey, final String family, final String qualifier, final long amount,
            final Durability durability) throws UncheckedIOException {
        return incrementColumnValue(tableName, rowKey, toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), amount, durability);
    }

    /**
     * Byte-array variant of {@link #incrementColumnValue(String, Object, String, String, long)}.
     *
     * <p>{@code family} and {@code qualifier} are passed through to HBase unchanged (no
     * encoding cache is consulted); {@code rowKey} is still converted via
     * {@link #toRowKeyBytes(Object)}.</p>
     *
     * @param tableName the name of the HBase table
     * @param rowKey the row key (converted via {@link #toRowKeyBytes(Object)})
     * @param family the column family bytes (used as-is)
     * @param qualifier the column qualifier bytes (used as-is)
     * @param amount the amount to add (negative values decrement)
     * @return the value of the column after the increment
     * @throws UncheckedIOException if the HBase call fails with an {@link IOException}
     */
    public long incrementColumnValue(final String tableName, final Object rowKey, final byte[] family, final byte[] qualifier, final long amount)
            throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            return table.incrementColumnValue(toRowKeyBytes(rowKey), family, qualifier, amount);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Byte-array variant of
     * {@link #incrementColumnValue(String, Object, String, String, long, Durability)}.
     *
     * <p>{@code family} and {@code qualifier} are passed through to HBase unchanged;
     * {@code rowKey} is still converted via {@link #toRowKeyBytes(Object)}.</p>
     *
     * @param tableName the name of the HBase table
     * @param rowKey the row key (converted via {@link #toRowKeyBytes(Object)})
     * @param family the column family bytes (used as-is)
     * @param qualifier the column qualifier bytes (used as-is)
     * @param amount the amount to add (negative values decrement)
     * @param durability the durability level to use for the WAL write
     * @return the value of the column after the increment
     * @throws UncheckedIOException if the HBase call fails with an {@link IOException}
     * @see Durability
     */
    public long incrementColumnValue(final String tableName, final Object rowKey, final byte[] family, final byte[] qualifier, final long amount,
            final Durability durability) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            return table.incrementColumnValue(toRowKeyBytes(rowKey), family, qualifier, amount, durability);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Returns a {@link CoprocessorRpcChannel} for the region hosting {@code rowKey}.
     *
     * <p>The channel is created by a short-lived {@link Table} that is closed before this
     * method returns; the returned channel itself drives subsequent RPCs through the
     * shared {@link Connection}, so the closed table does not affect channel usage.</p>
     *
     * <p>{@code rowKey} is converted to bytes via {@link #toRowKeyBytes(Object)}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CoprocessorRpcChannel channel = executor.coprocessorService("users", "user123");
     * MyService.BlockingInterface service = MyService.newBlockingStub(channel);
     * MyResponse response = service.myMethod(null, request);
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param rowKey the row key identifying the target region (converted via {@link #toRowKeyBytes(Object)})
     * @return a CoprocessorRpcChannel pointed at the region hosting {@code rowKey}
     * @throws UncheckedIOException if obtaining the {@link Table} fails with an {@link IOException}
     * @see CoprocessorRpcChannel
     */
    public CoprocessorRpcChannel coprocessorService(final String tableName, final Object rowKey) {
        // The returned CoprocessorRpcChannel uses the underlying Connection (not the Table)
        // for region lookup and RPC, so the Table can be safely closed before returning the channel.
        // Without this close, every call leaks one Table (and its associated thread-local state).
        final Table table = getTable(tableName);

        try {
            return table.coprocessorService(toRowKeyBytes(rowKey));
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Invokes a coprocessor {@link Service} on every region whose row range overlaps
     * {@code [startRowKey, endRowKey)} and returns the per-region results.
     *
     * <p>{@code startRowKey} and {@code endRowKey} are converted to bytes via
     * {@link #toRowKeyBytes(Object)}. The underlying {@link Table} is closed in a
     * {@code finally} block.</p>
     *
     * @param <T> the service type
     * @param <R> the result type produced by {@code callable}
     * @param tableName the name of the HBase table
     * @param service the service interface class
     * @param startRowKey the start row key (inclusive; {@code null} for unbounded start)
     * @param endRowKey the end row key (exclusive; {@code null} for unbounded end)
     * @param callable the per-region callable to execute
     * @return a map from region name bytes to the result returned by {@code callable} for that region
     * @throws UncheckedIOException if the call fails with an {@link IOException}
     * @throws RuntimeException if the coprocessor invocation throws a non-{@link IOException}
     *         {@link Throwable} (wrapped via {@code ExceptionUtil.toRuntimeException})
     * @see Service
     * @see Batch.Call
     */
    public <T extends Service, R> Map<byte[], R> coprocessorService(final String tableName, final Class<T> service, final Object startRowKey,
            final Object endRowKey, final Batch.Call<T, R> callable) throws UncheckedIOException {
        final Table table = getTable(tableName);

        try {
            return table.coprocessorService(service, toRowKeyBytes(startRowKey), toRowKeyBytes(endRowKey), callable);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } catch (final Throwable e) {
            throw ExceptionUtil.toRuntimeException(e, true);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Invokes a coprocessor {@link Service} on every region whose row range overlaps
     * {@code [startRowKey, endRowKey)}, delivering each per-region result to
     * {@code callback} as it becomes available.
     *
     * <p>{@code startRowKey} and {@code endRowKey} are converted to bytes via
     * {@link #toRowKeyBytes(Object)}. The underlying {@link Table} is closed in a
     * {@code finally} block.</p>
     *
     * @param <T> the service type
     * @param <R> the result type produced by {@code callable}
     * @param tableName the name of the HBase table
     * @param service the service interface class
     * @param startRowKey the start row key (inclusive; {@code null} for unbounded start)
     * @param endRowKey the end row key (exclusive; {@code null} for unbounded end)
     * @param callable the per-region callable to execute
     * @param callback the callback that receives each region's result
     * @throws UncheckedIOException if the call fails with an {@link IOException}
     * @throws Exception if the coprocessor invocation throws a non-{@link IOException}
     *         {@link Throwable}; the original cause is wrapped in a new {@link Exception}
     * @see Service
     * @see Batch.Call
     * @see Batch.Callback
     */
    public <T extends Service, R> void coprocessorService(final String tableName, final Class<T> service, final Object startRowKey, final Object endRowKey,
            final Batch.Call<T, R> callable, final Batch.Callback<R> callback) throws UncheckedIOException, Exception {
        final Table table = getTable(tableName);

        try {
            table.coprocessorService(service, toRowKeyBytes(startRowKey), toRowKeyBytes(endRowKey), callable, callback);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } catch (final Throwable e) {
            throw new Exception(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Executes a batch coprocessor service call against a range of rows and returns results.
     *
     * <p>This method invokes a coprocessor method on all regions that span the specified
     * row range using Protocol Buffers for serialization. Results from each region are
     * collected and returned.</p>
     *
     * @param <R> the response message type
     * @param tableName the name of the HBase table
     * @param methodDescriptor the Protocol Buffers method descriptor
     * @param request the Protocol Buffers request message
     * @param startRowKey the start row key (inclusive)
     * @param endRowKey the end row key (exclusive)
     * @param responsePrototype the prototype for the response message
     * @return a map of region names (byte arrays) to their corresponding response messages
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @throws Exception if the coprocessor execution throws a checked exception other than {@link IOException}
     *         (the original {@link Throwable} from the coprocessor is wrapped in a new {@link Exception})
     * @see Message
     * @see Descriptors.MethodDescriptor
     */
    public <R extends Message> Map<byte[], R> batchCoprocessorService(final String tableName, final Descriptors.MethodDescriptor methodDescriptor,
            final Message request, final Object startRowKey, final Object endRowKey, final R responsePrototype) throws UncheckedIOException, Exception {
        final Table table = getTable(tableName);

        try {
            return table.batchCoprocessorService(methodDescriptor, request, toRowKeyBytes(startRowKey), toRowKeyBytes(endRowKey), responsePrototype);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } catch (final Throwable e) {
            throw new Exception(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Executes a batch coprocessor service call against a range of rows with a callback for results.
     *
     * <p>This method invokes a coprocessor method on all regions that span the specified
     * row range using Protocol Buffers, invoking the callback with each region's result
     * as it becomes available.</p>
     *
     * @param <R> the response message type
     * @param tableName the name of the HBase table
     * @param methodDescriptor the Protocol Buffers method descriptor
     * @param request the Protocol Buffers request message
     * @param startRowKey the start row key (inclusive)
     * @param endRowKey the end row key (exclusive)
     * @param responsePrototype the prototype for the response message
     * @param callback the callback to receive response messages from each region
     * @throws UncheckedIOException if an I/O error occurs during the operation
     * @throws Exception if the coprocessor execution throws a checked exception other than {@link IOException}
     *         (the original {@link Throwable} from the coprocessor is wrapped in a new {@link Exception})
     * @see Message
     * @see Descriptors.MethodDescriptor
     * @see Batch.Callback
     */
    public <R extends Message> void batchCoprocessorService(final String tableName, final Descriptors.MethodDescriptor methodDescriptor, final Message request,
            final Object startRowKey, final Object endRowKey, final R responsePrototype, final Batch.Callback<R> callback)
            throws UncheckedIOException, Exception {
        final Table table = getTable(tableName);

        try {
            table.batchCoprocessorService(methodDescriptor, request, toRowKeyBytes(startRowKey), toRowKeyBytes(endRowKey), responsePrototype, callback);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } catch (final Throwable e) {
            throw new Exception(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Converts a column-family or qualifier string to its UTF-8 byte representation,
     * caching the result in a shared {@link ConcurrentHashMap} keyed by the input string.
     *
     * <p>The cache (an unbounded {@link ConcurrentHashMap}) avoids re-encoding hot family
     * and qualifier names on every call. Returns {@code null} for a {@code null} input.
     * The returned array is the shared cached instance and must not be mutated by callers.</p>
     *
     * @param str the family or qualifier name to encode; may be {@code null}
     * @return the UTF-8 bytes for {@code str}, or {@code null} if {@code str} is {@code null}
     */
    static byte[] toFamilyQualifierBytes(final String str) {
        if (str == null) {
            return null; // NOSONAR
        }

        byte[] bytes = familyQualifierBytesPool.get(str);

        if (bytes == null) {
            bytes = Bytes.toBytes(str);

            familyQualifierBytesPool.put(str, bytes);
        }

        return bytes;
    }

    /**
     * Converts an arbitrary row-key value to its HBase byte representation.
     *
     * <p>Equivalent to {@link #toValueBytes(Object)}; defined as a separate helper to keep
     * row-key call sites self-describing at the call site.</p>
     *
     * @param rowKey the row key value; may be {@code null}
     * @return the bytes for the row key, or {@code null} if {@code rowKey} is {@code null}
     * @see #toValueBytes(Object)
     */
    static byte[] toRowKeyBytes(final Object rowKey) {
        return toValueBytes(rowKey);
    }

    /**
     * Converts an arbitrary {@code row} value to its HBase byte representation.
     *
     * <p>Equivalent to {@link #toValueBytes(Object)}; used at call sites that operate on
     * an HBase {@code row} parameter (as distinct from a row key).</p>
     *
     * @param row the row value; may be {@code null}
     * @return the bytes for {@code row}, or {@code null} if {@code row} is {@code null}
     * @see #toValueBytes(Object)
     */
    static byte[] toRowBytes(final Object row) {
        return toValueBytes(row);
    }

    /**
     * Converts an arbitrary value to its HBase byte representation.
     *
     * <p>The conversion is type-directed:</p>
     * <ul>
     *   <li>{@code null} &rarr; {@code null}</li>
     *   <li>{@code byte[]} &rarr; returned as-is (no defensive copy)</li>
     *   <li>{@link ByteBuffer} &rarr; a fresh byte array containing the buffer's remaining
     *       bytes (the source buffer's position is not mutated)</li>
     *   <li>{@link String} &rarr; UTF-8 bytes via {@link Bytes#toBytes(String)}</li>
     *   <li>Any other type &rarr; converted to a {@link String} via {@code N.stringOf(value)}
     *       and then to UTF-8 bytes</li>
     * </ul>
     *
     * @param value the value to convert; may be {@code null}
     * @return the byte representation, or {@code null} if {@code value} is {@code null}
     */
    static byte[] toValueBytes(final Object value) {
        if (value == null) {
            return null; // NOSONAR
        } else if (value instanceof byte[]) {
            return (byte[]) value;
        } else if (value instanceof ByteBuffer) {
            final ByteBuffer buffer = (ByteBuffer) value;
            final byte[] bytes = new byte[buffer.remaining()];
            buffer.duplicate().get(bytes);
            return bytes;
        } else if (value instanceof String) {
            return Bytes.toBytes((String) value);
        } else {
            return Bytes.toBytes(N.stringOf(value));
        }
    }

    //
    //    static byte[] toBytes(final String str) {
    //        return str == null ? null : Bytes.toBytes(str);
    //    }
    //
    //    static byte[] toBytes(final Object obj) {
    //        return obj == null ? null : (obj instanceof byte[] ? (byte[]) obj : toBytes(N.stringOf(obj)));
    //    }

    static String toRowKeyString(final byte[] bytes, final int offset, final int len) {
        return Bytes.toString(bytes, offset, len);
    }

    static String toFamilyQualifierString(final byte[] bytes, final int offset, final int len) {
        return Bytes.toString(bytes, offset, len);
    }

    static String toValueString(final byte[] bytes, final int offset, final int len) {
        return Bytes.toString(bytes, offset, len);
    }

    static String getRowKeyString(final Cell cell) {
        return toRowKeyString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
    }

    static String getFamilyString(final Cell cell) {
        return toFamilyQualifierString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
    }

    static String getQualifierString(final Cell cell) {
        return toFamilyQualifierString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
    }

    static String getValueString(final Cell cell) {
        return toValueString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    }

    /**
     * Closes this executor, releasing the underlying {@link Admin} and {@link Connection}.
     *
     * <p>This method closes the {@link Admin} first and then the wrapped {@link Connection}
     * (if it has not already been closed). After this call, the executor must not be used
     * for further operations. Because the connection supplied at construction is shared with
     * this executor and closed here, do not pass in a connection that other components
     * still need.</p>
     *
     * <p><strong>Note:</strong> the {@link AsyncExecutor} passed at construction is
     * <em>not</em> shut down by this method; its lifecycle is the caller's responsibility.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * try (HBaseExecutor executor = new HBaseExecutor(connection)) {
     *     // perform HBase operations
     * }
     * }</pre>
     *
     * @throws IOException if closing {@link Admin} or {@link Connection} fails
     * @see AutoCloseable
     */
    @Override
    public void close() throws IOException {
        try {
            if (admin != null) {
                admin.close();
            }
        } finally {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
    }

    /**
     * A type-safe mapper that provides simplified CRUD operations for a specific entity type.
     *
     * <p>This mapper encapsulates HBase operations for a particular entity class, providing
     * a higher-level, object-oriented interface that handles the conversion between Java
     * objects and HBase data automatically.</p>
     *
     * <p>Key features:</p>
     * <ul>
     * <li>Type-safe operations with compile-time checking</li>
     * <li>Automatic object-to-HBase mapping and vice versa</li>
     * <li>Support for batch operations</li>
     * <li>Integration with scanning operations</li>
     * <li>Configurable naming policies</li>
     * </ul>
     *
     * <p>This class is typically obtained through {@link HBaseExecutor#mapper(Class)} or
     * {@link HBaseExecutor#mapper(Class, String, NamingPolicy)} methods.</p>
     *
     * @param <T> the entity type that this mapper handles
     * @param <K> the row key type for the entity
     * @since 1.0
     * @see HBaseExecutor#mapper(Class)
     * @see HBaseExecutor#mapper(Class, String, NamingPolicy)
     */
    public static class HBaseMapper<T, K> {
        private final HBaseExecutor hbaseExecutor;
        private final String tableName;
        private final Class<T> targetEntityClass;
        private final String rowKeyPropName;
        private final NamingPolicy namingPolicy;

        HBaseMapper(final Class<T> targetEntityClass, final HBaseExecutor hbaseExecutor, final String tableName, final NamingPolicy namingPolicy) {
            N.checkArgNotNull(targetEntityClass, "targetEntityClass");
            N.checkArgNotNull(hbaseExecutor, "hbaseExecutor");
            N.checkArgNotEmpty(tableName, "tableName");

            N.checkArgument(Beans.isBeanClass(targetEntityClass), "{} is not an entity class with getter/setter method", targetEntityClass);

            @SuppressWarnings("deprecation")
            final List<String> idPropNames = QueryUtil.getIdPropNames(targetEntityClass);

            if (idPropNames.size() != 1) {
                throw new IllegalArgumentException(
                        "No or multiple ids: " + idPropNames + " defined/annotated in class: " + ClassUtil.getCanonicalClassName(targetEntityClass));
            }

            this.hbaseExecutor = hbaseExecutor;
            this.tableName = tableName;
            this.targetEntityClass = targetEntityClass;
            rowKeyPropName = idPropNames.get(0);

            this.namingPolicy = namingPolicy == null ? NamingPolicy.CAMEL_CASE : namingPolicy;
        }

        /**
         * Checks if an entity with the specified row key exists in the table.
         *
         * @param rowKey the row key to check
         * @return {@code true} if an entity with the given row key exists, {@code false} otherwise
         * @throws UncheckedIOException if an I/O error occurs during the operation
         */
        public boolean exists(final K rowKey) throws UncheckedIOException {
            return hbaseExecutor.exists(tableName, AnyGet.of(rowKey));
        }

        /**
         * Checks if entities with the specified row keys exist in the table.
         *
         * @param rowKeys the collection of row keys to check
         * @return a list of Boolean values corresponding to each row key, where {@code true}
         *         indicates the entity exists
         * @throws UncheckedIOException if an I/O error occurs during the operation
         */
        public List<Boolean> exists(final Collection<? extends K> rowKeys) throws UncheckedIOException {
            final List<AnyGet> anyGets = N.map(rowKeys, AnyGet::of);

            return hbaseExecutor.exists(tableName, anyGets);
        }

        /**
         * Retrieves an entity by its row key.
         *
         * @param rowKey the row key of the entity to retrieve
         * @return the entity object, or the type's default value (typically {@code null} for bean classes)
         *         when no row matches the given key
         * @throws UncheckedIOException if an I/O error occurs during the operation
         */
        public T get(final K rowKey) throws UncheckedIOException {
            return hbaseExecutor.get(tableName, AnyGet.of(rowKey), targetEntityClass);
        }

        /**
         * Retrieves multiple entities by their row keys.
         *
         * <p>Row keys that do not exist in the table are skipped — the returned list
         * may contain fewer elements than {@code rowKeys} and the order does not
         * necessarily correspond positionally to the input collection.</p>
         *
         * @param rowKeys the collection of row keys to retrieve
         * @return a list of entity objects for the row keys that were found
         * @throws UncheckedIOException if an I/O error occurs during the operation
         */
        public List<T> get(final Collection<? extends K> rowKeys) throws UncheckedIOException {
            final List<AnyGet> anyGets = N.map(rowKeys, AnyGet::of);

            return hbaseExecutor.get(tableName, anyGets, targetEntityClass);
        }

        /**
         * Stores an entity in the table.
         *
         * <p>The row key is extracted from the entity's {@code @Id} annotated field,
         * and the entity's properties are mapped to HBase columns according to the
         * configured naming policy.</p>
         *
         * @param entityToPut the entity to store
         * @throws UncheckedIOException if an I/O error occurs during the operation
         */
        public void put(final T entityToPut) throws UncheckedIOException {
            hbaseExecutor.put(tableName, AnyPut.create(entityToPut, namingPolicy));
        }

        /**
         * Stores multiple entities in the table in a batch operation.
         *
         * @param entitiesToPut the collection of entities to store
         * @throws UncheckedIOException if an I/O error occurs during the operation
         */
        public void put(final Collection<? extends T> entitiesToPut) throws UncheckedIOException {
            hbaseExecutor.put(tableName, AnyPut.create(entitiesToPut, namingPolicy));
        }

        /**
         * Deletes an entity from the table.
         *
         * <p>The row key is extracted from the entity's {@code @Id} annotated field.</p>
         *
         * @param entityToDelete the entity to delete
         * @throws UncheckedIOException if an I/O error occurs during the operation
         */
        @SuppressWarnings("unchecked")
        public void delete(final T entityToDelete) throws UncheckedIOException {
            deleteByRowKey((K) Beans.getPropValue(entityToDelete, rowKeyPropName));
        }

        /**
         * Deletes multiple entities from the table in a batch operation.
         *
         * @param entitiesToDelete the collection of entities to delete
         * @throws UncheckedIOException if an I/O error occurs during the operation
         */
        @SuppressWarnings("unchecked")
        public void delete(final Collection<? extends T> entitiesToDelete) throws UncheckedIOException {
            deleteByRowKey(N.map(entitiesToDelete, entity -> (K) Beans.getPropValue(entity, rowKeyPropName)));
        }

        /**
         * Deletes an entity by its row key.
         *
         * @param rowKey the row key of the entity to delete
         * @throws UncheckedIOException if an I/O error occurs during the operation
         */
        public void deleteByRowKey(final K rowKey) throws UncheckedIOException {
            hbaseExecutor.delete(tableName, AnyDelete.of(rowKey));
        }

        /**
         * Deletes multiple entities by their row keys in a batch operation.
         *
         * @param rowKeys the collection of row keys to delete
         * @throws UncheckedIOException if an I/O error occurs during the operation
         */
        public void deleteByRowKey(final Collection<? extends K> rowKeys) throws UncheckedIOException {
            final List<AnyDelete> anyDeletes = N.map(rowKeys, AnyDelete::of);

            hbaseExecutor.delete(tableName, anyDeletes);
        }

        /**
         * Checks if data exists for the specified AnyGet operation.
         *
         * @param anyGet the AnyGet operation specifying what to check
         * @return {@code true} if the data exists, {@code false} otherwise
         * @throws UncheckedIOException if an I/O error occurs during the operation
         * @see AnyGet
         */
        public boolean exists(final AnyGet anyGet) throws UncheckedIOException {
            return hbaseExecutor.exists(tableName, anyGet.val());
        }

        /**
         * Checks if data exists for multiple AnyGet operations.
         *
         * @param anyGets the list of AnyGet operations to check
         * @return a list of Boolean values corresponding to each AnyGet operation
         * @throws UncheckedIOException if an I/O error occurs during the operation
         * @see AnyGet
         */
        public List<Boolean> exists(final List<AnyGet> anyGets) throws UncheckedIOException {
            return hbaseExecutor.exists(tableName, AnyGet.toGet(anyGets));
        }

        /**
         * Retrieves an entity using an AnyGet operation.
         *
         * @param anyGet the AnyGet operation specifying what to retrieve
         * @return the entity object, or null if not found
         * @throws UncheckedIOException if an I/O error occurs during the operation
         * @see AnyGet
         */
        public T get(final AnyGet anyGet) throws UncheckedIOException {
            return hbaseExecutor.get(tableName, anyGet, targetEntityClass);
        }

        /**
         * Retrieves multiple entities using AnyGet operations.
         *
         * @param anyGets the list of AnyGet operations to execute
         * @return a list of entity objects
         * @throws UncheckedIOException if an I/O error occurs during the operation
         * @see AnyGet
         */
        public List<T> get(final List<AnyGet> anyGets) throws UncheckedIOException {
            return hbaseExecutor.get(tableName, anyGets, targetEntityClass);
        }

        /**
         * Scans a column family and returns a stream of entities.
         *
         * @param family the name of the column family to scan
         * @return a lazy stream of entity objects
         * @see Stream
         */
        public Stream<T> scan(final String family) {
            return hbaseExecutor.scan(tableName, family, targetEntityClass);
        }

        /**
         * Scans a specific column and returns a stream of entities.
         *
         * @param family the name of the column family
         * @param qualifier the column qualifier
         * @return a lazy stream of entity objects
         * @see Stream
         */
        public Stream<T> scan(final String family, final String qualifier) {
            return hbaseExecutor.scan(tableName, family, qualifier, targetEntityClass);
        }

        /**
         * Scans a column family (byte array) and returns a stream of entities.
         *
         * @param family the column family as a byte array
         * @return a lazy stream of entity objects
         * @see Stream
         */
        public Stream<T> scan(final byte[] family) {
            return hbaseExecutor.scan(tableName, family, targetEntityClass);
        }

        /**
         * Scans a specific column (byte arrays) and returns a stream of entities.
         *
         * @param family the column family as a byte array
         * @param qualifier the column qualifier as a byte array
         * @return a lazy stream of entity objects
         * @see Stream
         */
        public Stream<T> scan(final byte[] family, final byte[] qualifier) {
            return hbaseExecutor.scan(tableName, family, qualifier, targetEntityClass);
        }

        /**
         * Scans using an AnyScan specification and returns a stream of entities.
         *
         * @param anyScan the AnyScan operation defining the scan parameters
         * @return a lazy stream of entity objects
         * @see AnyScan
         * @see Stream
         */
        public Stream<T> scan(final AnyScan anyScan) {
            return hbaseExecutor.scan(tableName, anyScan, targetEntityClass);
        }

        /**
         * Stores data using an AnyPut operation.
         *
         * @param anyPut the AnyPut operation containing the data to store
         * @throws UncheckedIOException if an I/O error occurs during the operation
         * @see AnyPut
         */
        public void put(final AnyPut anyPut) throws UncheckedIOException {
            hbaseExecutor.put(tableName, anyPut);
        }

        /**
         * Stores data using multiple AnyPut operations in a batch.
         *
         * @param anyPuts the list of AnyPut operations to execute
         * @throws UncheckedIOException if an I/O error occurs during the operation
         * @see AnyPut
         */
        public void put(final List<AnyPut> anyPuts) throws UncheckedIOException {
            hbaseExecutor.put(tableName, anyPuts);
        }

        /**
         * Deletes data using an AnyDelete operation.
         *
         * @param anyDelete the AnyDelete operation specifying what to delete
         * @throws UncheckedIOException if an I/O error occurs during the operation
         * @see AnyDelete
         */
        public void delete(final AnyDelete anyDelete) throws UncheckedIOException {
            hbaseExecutor.delete(tableName, anyDelete);
        }

        /**
         * Deletes data using multiple AnyDelete operations in a batch.
         *
         * @param anyDeletes the list of AnyDelete operations to execute
         * @throws UncheckedIOException if an I/O error occurs during the operation
         * @see AnyDelete
         */
        public void delete(final List<AnyDelete> anyDeletes) throws UncheckedIOException {
            hbaseExecutor.delete(tableName, anyDeletes);
        }

        /**
         * Performs multiple atomic mutations on a single row.
         *
         * @param rm the AnyRowMutations containing the mutations to perform
         * @throws UncheckedIOException if an I/O error occurs during the operation
         * @see AnyRowMutations
         */
        public void mutateRow(final AnyRowMutations rm) throws UncheckedIOException {
            hbaseExecutor.mutateRow(tableName, rm);
        }

        /**
         * Appends values to columns.
         *
         * @param append the AnyAppend operation specifying the values to append
         * @return the Result containing the new values after the append
         * @throws UncheckedIOException if an I/O error occurs during the operation
         * @see AnyAppend
         */
        public Result append(final AnyAppend append) throws UncheckedIOException {
            return hbaseExecutor.append(tableName, append);
        }

        /**
         * Increments column values.
         *
         * @param increment the AnyIncrement operation specifying the values to increment
         * @return the Result containing the new values after the increment
         * @throws UncheckedIOException if an I/O error occurs during the operation
         * @see AnyIncrement
         */
        public Result increment(final AnyIncrement increment) throws UncheckedIOException {
            return hbaseExecutor.increment(tableName, increment);
        }

        /**
         * Atomically increments a single column's numeric value on this mapper's table.
         *
         * <p>Delegates to
         * {@link HBaseExecutor#incrementColumnValue(String, Object, String, String, long)}
         * with the mapper's bound table name.</p>
         *
         * @param rowKey the row key (converted via {@link HBaseExecutor#toRowKeyBytes(Object)})
         * @param family the column family name (converted via {@link HBaseExecutor#toFamilyQualifierBytes(String)})
         * @param qualifier the column qualifier name (converted via {@link HBaseExecutor#toFamilyQualifierBytes(String)})
         * @param amount the amount to add (negative values decrement)
         * @return the value of the column after the increment
         * @throws UncheckedIOException if the HBase call fails with an {@link IOException}
         */
        public long incrementColumnValue(final Object rowKey, final String family, final String qualifier, final long amount) throws UncheckedIOException {
            return hbaseExecutor.incrementColumnValue(tableName, rowKey, family, qualifier, amount);
        }

        /**
         * Atomically increments a single column value with specified durability.
         *
         * @param rowKey the row key
         * @param family the column family name
         * @param qualifier the column qualifier name
         * @param amount the amount to increment
         * @param durability the durability level for this operation
         * @return the new value after the increment
         * @throws UncheckedIOException if an I/O error occurs during the operation
         * @see Durability
         */
        public long incrementColumnValue(final Object rowKey, final String family, final String qualifier, final long amount, final Durability durability)
                throws UncheckedIOException {
            return hbaseExecutor.incrementColumnValue(tableName, rowKey, family, qualifier, amount, durability);
        }

        /**
         * Atomically increments a single column value using byte arrays.
         *
         * @param rowKey the row key
         * @param family the column family as a byte array
         * @param qualifier the column qualifier as a byte array
         * @param amount the amount to increment
         * @return the new value after the increment
         * @throws UncheckedIOException if an I/O error occurs during the operation
         */
        public long incrementColumnValue(final Object rowKey, final byte[] family, final byte[] qualifier, final long amount) throws UncheckedIOException {
            return hbaseExecutor.incrementColumnValue(tableName, rowKey, family, qualifier, amount);
        }

        /**
         * Atomically increments a single column value using byte arrays with specified durability.
         *
         * @param rowKey the row key
         * @param family the column family as a byte array
         * @param qualifier the column qualifier as a byte array
         * @param amount the amount to increment
         * @param durability the durability level for this operation
         * @return the new value after the increment
         * @throws UncheckedIOException if an I/O error occurs during the operation
         * @see Durability
         */
        public long incrementColumnValue(final Object rowKey, final byte[] family, final byte[] qualifier, final long amount, final Durability durability)
                throws UncheckedIOException {
            return hbaseExecutor.incrementColumnValue(tableName, rowKey, family, qualifier, amount, durability);
        }

        /**
         * Gets a CoprocessorRpcChannel for communicating with a coprocessor.
         *
         * @param rowKey the row key to identify which region server to connect to
         * @return the CoprocessorRpcChannel for the specified row's region
         * @throws UncheckedIOException if an I/O error occurs while obtaining the underlying {@link Table}
         * @see CoprocessorRpcChannel
         */
        public CoprocessorRpcChannel coprocessorService(final Object rowKey) {
            return hbaseExecutor.coprocessorService(tableName, rowKey);
        }

        /**
         * Executes a coprocessor call against a range of rows and returns results.
         *
         * @param <S> the service type
         * @param <R> the result type
         * @param service the service interface class
         * @param startRowKey the start row key (inclusive)
         * @param endRowKey the end row key (exclusive)
         * @param callable the callable to execute on each region
         * @return a map of region names to their corresponding results
         * @throws UncheckedIOException if an I/O error occurs during the operation
         * @see Service
         * @see Batch.Call
         */
        public <S extends Service, R> Map<byte[], R> coprocessorService(final Class<S> service, final Object startRowKey, final Object endRowKey,
                final Batch.Call<S, R> callable) throws UncheckedIOException {
            return hbaseExecutor.coprocessorService(tableName, service, startRowKey, endRowKey, callable);
        }

        /**
         * Executes a coprocessor call against a range of rows with a callback for results.
         *
         * @param <S> the service type
         * @param <R> the result type
         * @param service the service interface class
         * @param startRowKey the start row key (inclusive)
         * @param endRowKey the end row key (exclusive)
         * @param callable the callable to execute on each region
         * @param callback the callback to receive results from each region
         * @throws UncheckedIOException if an I/O error occurs during the operation
         * @throws Exception if the coprocessor execution throws an exception
         * @see Service
         * @see Batch.Call
         * @see Batch.Callback
         */
        public <S extends Service, R> void coprocessorService(final Class<S> service, final Object startRowKey, final Object endRowKey,
                final Batch.Call<S, R> callable, final Batch.Callback<R> callback) throws UncheckedIOException, Exception {
            hbaseExecutor.coprocessorService(tableName, service, startRowKey, endRowKey, callable, callback);
        }

        /**
         * Executes a batch coprocessor service call against a range of rows and returns results.
         *
         * @param <R> the response message type
         * @param methodDescriptor the Protocol Buffers method descriptor
         * @param request the Protocol Buffers request message
         * @param startRowKey the start row key (inclusive)
         * @param endRowKey the end row key (exclusive)
         * @param responsePrototype the prototype for the response message
         * @return a map of region names to their corresponding response messages
         * @throws UncheckedIOException if an I/O error occurs during the operation
         * @throws Exception if the coprocessor execution throws an exception
         * @see Message
         * @see Descriptors.MethodDescriptor
         */
        public <R extends Message> Map<byte[], R> batchCoprocessorService(final Descriptors.MethodDescriptor methodDescriptor, final Message request,
                final Object startRowKey, final Object endRowKey, final R responsePrototype) throws UncheckedIOException, Exception {
            return hbaseExecutor.batchCoprocessorService(tableName, methodDescriptor, request, startRowKey, endRowKey, responsePrototype);
        }

        /**
         * Executes a batch coprocessor service call against a range of rows with a callback for results.
         *
         * @param <R> the response message type
         * @param methodDescriptor the Protocol Buffers method descriptor
         * @param request the Protocol Buffers request message
         * @param startRowKey the start row key (inclusive)
         * @param endRowKey the end row key (exclusive)
         * @param responsePrototype the prototype for the response message
         * @param callback the callback to receive response messages from each region
         * @throws UncheckedIOException if an I/O error occurs during the operation
         * @throws Exception if the coprocessor execution throws an exception
         * @see Message
         * @see Descriptors.MethodDescriptor
         * @see Batch.Callback
         */
        public <R extends Message> void batchCoprocessorService(final Descriptors.MethodDescriptor methodDescriptor, final Message request,
                final Object startRowKey, final Object endRowKey, final R responsePrototype, final Batch.Callback<R> callback)
                throws UncheckedIOException, Exception {
            hbaseExecutor.batchCoprocessorService(tableName, methodDescriptor, request, startRowKey, endRowKey, responsePrototype, callback);
        }
    }
}
