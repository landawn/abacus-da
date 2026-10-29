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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.hadoop.hbase.client.Operation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.landawn.abacus.da.cs;
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
import com.landawn.abacus.util.Holder;
import com.landawn.abacus.util.IOUtil;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.Strings;
import com.landawn.abacus.util.Tuple;
import com.landawn.abacus.util.Tuple.Tuple2;
import com.landawn.abacus.util.Tuple.Tuple3;
import com.landawn.abacus.util.u;
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
 * {@code N.stringOf} form first; the remaining bytes of {@link ByteBuffer}s are copied
 * into a fresh array without changing the buffer position;
 * existing {@code byte[]} values are returned as-is. An HBase operation or {@code Any*} wrapper
 * passed as a row key is rejected with {@link IllegalArgumentException}: it is not a row-key value.</p>
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
 *     private String id;            // row key (not stored as a cell)
 *     private String guid;          // HBase: "guid:"        (columnFamily=guid, qualifier="")
 *     private Name name;            // HBase: "name:firstName" and "name:lastName"
 *     private String emailAddress;  // HBase: "emailAddress:"
 *     // JavaBean getters and setters omitted
 * }
 *
 * public static class Name {
 *     private String firstName;  // HBase: "name:firstName"
 *     private String lastName;   // HBase: "name:lastName"
 *     // JavaBean getters and setters omitted
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
 *     private String id;            // row key (not stored as a cell)
 *     @Column("guid")
 *     private String guid;          // HBase: "cf:guid"
 *     @ColumnFamily("name")
 *     private Name name;            // HBase: "name:firstName" and "name:lastName"
 *     // JavaBean getters and setters omitted
 * }
 * }</pre>
 *
 * <p><b>Boolean properties:</b> a cell mapped to a {@code Boolean}/{@code boolean} property is read as text and follows
 * abacus-common's strict rule: only {@code 1} (true), {@code 0} or {@code -1} (both false), {@code y}, {@code Y}, {@code n},
 * {@code N}, {@code true} or {@code false} (any case) convert, so the {@code true}/{@code false} text this executor writes reads
 * back unchanged. Leading and trailing whitespace is ignored ({@code " TRUE "} reads as {@code true}), and an empty or
 * whitespace-only cell yields the property's default: {@code false} for {@code boolean}, {@code null} for {@code Boolean}.
 * Any other cell, such as {@code yes} or {@code 2}, or a binary {@code Bytes.toBytes(boolean)} or
 * {@code Bytes.toBytes(int)} value, throws {@link IllegalArgumentException}.</p>
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
 * HBaseExecutor executor = new HBaseExecutor(connection);
 * try {
 *     boolean exists = executor.exists("users", AnyGet.of("user123"));
 *     Result result  = executor.get("users", AnyGet.of("user123"));
 *     executor.put("users", AnyPut.of("user123").addColumn("info", "name", "John"));
 *
 *     // User is a JavaBean annotated with @Table("users") and one @Id property.
 *     HBaseMapper<User, String> mapper = executor.mapper(User.class);
 *     User user = mapper.get("user123");
 *     if (user != null) {
 *         mapper.put(user);
 *     }
 * } finally {
 *     executor.close();
 * }
 * }</pre>
 *
 * <p><b>Naming convention:</b> this executor deliberately mirrors the HBase client's own method
 * vocabulary ({@code get}, {@code scan}, {@code put}, {@code delete}, {@code append}, {@code increment},
 * {@code mutateRow}, {@code exists}) so it reads naturally to HBase users. It does <i>not</i> adopt the
 * abacus "house" CRUD vocabulary ({@code findFirst}/{@code list}/{@code insert}/{@code update}) used by
 * the {@code Condition}-based executors such as Cassandra and BigQuery.</p>
 *
 * @see com.landawn.abacus.util.HBaseColumn
 * @see com.landawn.abacus.da.hbase.annotation.ColumnFamily
 * @see AsyncHBaseExecutor
 * @see <a href="https://hbase.apache.org/devapidocs/index.html">Apache HBase Java API</a>
 */
public final class HBaseExecutor {

    static {
        final BiFunction<Result, Class<?>, Object> converter = HBaseExecutor::toValue;

        N.registerConverter(Result.class, converter);
    }

    private static final Logger logger = LoggerFactory.getLogger(HBaseExecutor.class);

    /** The empty-string column qualifier used for properties stored directly under a column family without a qualifier. */
    static final String EMPTY_QUALIFIER = Strings.EMPTY;

    /**
     * The shared {@link AsyncExecutor} that backs {@link #async()} when no executor is supplied
     * at construction. Its core pool is {@code max(64, IOUtil.CPU_CORES * 8)} threads, created as tasks arrive and kept
     * when idle. The maximum pool size {@code max(128, IOUtil.CPU_CORES * 16)} and the 180-second keep-alive are passed as
     * well, but {@link AsyncExecutor} queues tasks in an unbounded queue, so at most the core number of tasks run at once
     * and further ones wait in that queue. It is never shut down by {@link #close()}.
     */
    static final AsyncExecutor DEFAULT_ASYNC_EXECUTOR = new AsyncExecutor(//
            N.max(64, IOUtil.CPU_CORES * 8), // coreThreadPoolSize
            N.max(128, IOUtil.CPU_CORES * 16), // maxThreadPoolSize
            180L, TimeUnit.SECONDS);

    private static final Map<Class<?>, Method> classRowKeySetMethodPool = new ConcurrentHashMap<>();

    private static final Map<Class<?>, Map<NamingPolicy, Map<String, Tuple3<String, String, Boolean>>>> classFamilyColumnNamePool = new ConcurrentHashMap<>();
    // (family -> qualifier -> property, flattened-nested-bean qualifier -> property, family -> qualifier -> sole writing property
    // or CONTESTED_CELL_TP); see getFamilyColumnFieldNameMap.
    private static final Map<Class<?>, Tuple3<Map<String, Map<String, Tuple2<String, Boolean>>>, Map<String, String>, Map<String, Map<String, Tuple2<String, Boolean>>>>> classFamilyColumnFieldNamePool = new ConcurrentHashMap<>();
    // Marks (by identity) a cell in _3 that several properties write, or whose one writer would not read it back.
    private static final Tuple2<String, Boolean> CONTESTED_CELL_TP = Tuple.of("", Boolean.FALSE);

    private final Admin admin;

    private final Connection conn;

    private final AsyncHBaseExecutor asyncHBaseExecutor;

    /**
     * Constructs an {@code HBaseExecutor} bound to the given HBase {@link Connection},
     * using the shared {@link #DEFAULT_ASYNC_EXECUTOR} for async operations.
     *
     * <p>The default async executor is statically configured with:</p>
     * <ul>
     *   <li>Core thread pool size: {@code max(64, IOUtil.CPU_CORES * 8)}, which is also the most tasks that run at once:
     *       further tasks wait in an unbounded queue</li>
     *   <li>Max thread pool size: {@code max(128, IOUtil.CPU_CORES * 16)} (never reached, because of that queue)</li>
     *   <li>Keep-alive time: 180 seconds (core threads are kept when idle)</li>
     * </ul>
     *
     * <p>The supplied {@code conn} is not copied; closing this executor closes the
     * connection it wraps (see {@link #close()}).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Configuration config = HBaseConfiguration.create();
     * Connection conn = ConnectionFactory.createConnection(config);
     *
     * HBaseExecutor executor = new HBaseExecutor(conn);  // throws UncheckedIOException if conn.getAdmin() fails
     * try {
     *     // ... perform HBase operations; closing executor closes conn too
     * } finally {
     *     executor.close();
     * }
     * }</pre>
     *
     * @param conn the HBase connection to use for database operations
     * @throws IllegalArgumentException if {@code conn} is {@code null}
     * @throws UncheckedIOException if obtaining the {@link Admin} interface from the
     *         connection fails with an {@link IOException}
     */
    public HBaseExecutor(final Connection conn) throws IllegalArgumentException, UncheckedIOException {
        this(conn, DEFAULT_ASYNC_EXECUTOR);
    }

    /**
     * Constructs an {@code HBaseExecutor} bound to the given HBase {@link Connection}
     * and a caller-supplied {@link AsyncExecutor} for asynchronous operations.
     *
     * <p>Use this constructor when you want to share or constrain the thread pool that
     * drives the {@link AsyncHBaseExecutor} returned from {@link #async()}.</p>
     *
     * <p>An {@link IOException} from {@link Connection#getAdmin()} is rethrown wrapped in
     * {@link UncheckedIOException}; if a later construction step fails, the already-obtained
     * {@link Admin} is closed quietly.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncExecutor sharedPool = new AsyncExecutor(16, 32, 60L, TimeUnit.SECONDS);
     * Connection conn = ConnectionFactory.createConnection(HBaseConfiguration.create());
     *
     * HBaseExecutor executor = new HBaseExecutor(conn, sharedPool);  // throws UncheckedIOException if conn.getAdmin() fails
     * try {
     *     ContinuableFuture<Result> future = executor.async().get("users", AnyGet.of("user123"));
     * } finally {
     *     executor.close();
     * }
     * // sharedPool is NOT shut down by closing the executor — the caller owns it
     * }</pre>
     *
     * @param conn the HBase connection to use for database operations; not copied — closing
     *        this executor closes the connection
     * @param asyncExecutor the executor that drives the {@link AsyncHBaseExecutor}
     * @throws IllegalArgumentException if {@code conn} or {@code asyncExecutor} is {@code null}
     * @throws UncheckedIOException if obtaining the {@link Admin} interface from the
     *         connection fails with an {@link IOException}
     */
    public HBaseExecutor(final Connection conn, final AsyncExecutor asyncExecutor) throws IllegalArgumentException, UncheckedIOException {
        N.checkArgNotNull(conn, cs.conn);
        N.checkArgNotNull(asyncExecutor, cs.asyncExecutor);

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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Admin admin = executor.admin();
     * boolean tableExists = admin.tableExists(TableName.valueOf("users"));  // throws IOException
     * }</pre>
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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Connection conn = executor.connection();
     * try (Table table = conn.getTable(TableName.valueOf("users"))) {  // throws IOException
     *     Result result = table.get(new Get(Bytes.toBytes("user123")));
     * }
     * }</pre>
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
     * also defensively clears any previously cached column-family/qualifier name maps for
     * {@code cls}; they are lazily rebuilt on next use.</p>
     *
     * <p><strong>Deprecated:</strong> prefer annotating the row-key field with {@code @Id}
     * from one of the following:</p>
     * <ul>
     *   <li>{@code com.landawn.abacus.annotation.Id}</li>
     *   <li>{@code javax.persistence.Id}</li>
     *   <li>{@code jakarta.persistence.Id}</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Register "id" as the row key for the Account entity
     * HBaseExecutor.registerRowKeyProperty(Account.class, "id");   // void; no return value
     *
     * // Unknown property -> IllegalArgumentException
     * HBaseExecutor.registerRowKeyProperty(Account.class, "noSuchProp");   // throws IllegalArgumentException
     *
     * // A property declared as HBaseColumn (or as a Collection/Map of HBaseColumn) cannot be the row key.
     * // For an entity that declares one, e.g. "private HBaseColumn<String> hc1;":
     * HBaseExecutor.registerRowKeyProperty(AccountWithVersionedColumn.class, "hc1");   // throws IllegalArgumentException
     * }</pre>
     *
     * @param cls the entity class (must be a JavaBean class) on which to register the row-key property; must not be {@code null}
     * @param rowKeyPropertyName the name of the property to use as the row key; must not be {@code null}
     * @throws IllegalArgumentException if {@code cls} or {@code rowKeyPropertyName} is {@code null}, if {@code cls} has no getter or setter for
     *         {@code rowKeyPropertyName}, or if the property's declared type is {@link HBaseColumn} (directly, as the single type-arg of a generic
     *         such as {@code Collection<HBaseColumn>}, or as the value type of a two-arg generic such as {@code Map<?, HBaseColumn>})
     * @see com.landawn.abacus.annotation.Id
     * @deprecated Annotate the row-key field with {@code @Id} instead.
     */
    @Deprecated
    public static void registerRowKeyProperty(final Class<?> cls, final String rowKeyPropertyName) throws IllegalArgumentException {
        N.checkArgNotNull(cls, cs.cls);
        N.checkArgNotNull(rowKeyPropertyName, cs.rowKeyPropertyName);

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

    /**
     * Resolves and caches the row-key property setter for {@code targetType}: either a property
     * explicitly registered via {@link #registerRowKeyProperty(Class, String)} or the single ID
     * property discovered on the class ({@code @Id}/{@code @ReadOnlyId}-annotated or, when no property
     * is annotated, a conventionally typed property named {@code id}; see {@link QueryUtil#idPropNames(Class)}).
     *
     * @param targetType the entity class whose row-key setter is requested
     * @return the setter {@link Method} of the row-key property, or {@code null} if
     *         {@code targetType} has no row-key property
     * @throws IllegalArgumentException if {@code targetType} is {@code null}, declares multiple row-key properties, a row-key property without
     *         both a getter and setter, or a row-key type containing {@code HBaseColumn}
     */
    @SuppressWarnings("deprecation")
    static Method getRowKeySetMethod(final Class<?> targetType) throws IllegalArgumentException {
        N.checkArgNotNull(targetType, cs.targetType);

        Method rowKeySetMethod = classRowKeySetMethodPool.get(targetType);

        if (rowKeySetMethod == null) {
            final List<String> ids = QueryUtil.idPropNames(targetType);

            if (ids.size() > 1) {
                throw new IllegalArgumentException("Multiple ids: " + ids + " defined/annotated in class: " + ClassUtil.getCanonicalClassName(targetType));
            } else if (ids.size() == 1) {
                registerRowKeyProperty(targetType, ids.get(0));
                rowKeySetMethod = classRowKeySetMethodPool.get(targetType);
            }

            if (rowKeySetMethod == null) {
                rowKeySetMethod = ClassUtil.SENTINEL_METHOD;
                classRowKeySetMethodPool.put(targetType, rowKeySetMethod);
            }
        }

        return rowKeySetMethod == ClassUtil.SENTINEL_METHOD ? null : rowKeySetMethod;
    }

    /**
     * Returns the cached property-to-column mapping for {@code entityClass} under
     * {@code namingPolicy}, building it from the class's {@code @ColumnFamily}/{@code @Column}
     * annotations (and the given naming policy) on first use.
     *
     * @param entityClass the entity class to map
     * @param namingPolicy the naming policy applied to property names without annotations
     * @return a map from property name to a {@link Tuple3} of (column family name, column
     *         qualifier name, whether an explicit {@code @Column} annotation is present)
     * @throws IllegalArgumentException if {@code entityClass} or {@code namingPolicy} is {@code null}
     */
    static Map<String, Tuple3<String, String, Boolean>> getClassFamilyColumnNameMap(final Class<?> entityClass, final NamingPolicy namingPolicy)
            throws IllegalArgumentException {
        N.checkArgNotNull(entityClass, cs.entityClass);
        N.checkArgNotNull(namingPolicy, cs.namingPolicy);

        final Map<NamingPolicy, Map<String, Tuple3<String, String, Boolean>>> namingPolicyFamilyColumnNameMap = classFamilyColumnNamePool
                .computeIfAbsent(entityClass, k -> new ConcurrentHashMap<>());

        return namingPolicyFamilyColumnNameMap.computeIfAbsent(namingPolicy, k -> {
            final Map<String, Tuple3<String, String, Boolean>> classFamilyColumnNameMap = new HashMap<>();

            final BeanInfo entityInfo = ParserUtil.getBeanInfo(entityClass);
            final ColumnFamily defaultColumnFamilyAnno = entityInfo.getAnnotation(ColumnFamily.class);
            final String defaultColumnFamilyName = hasColumnFamilyValue(defaultColumnFamilyAnno) ? getAnnotatedColumnFamily(defaultColumnFamilyAnno) : null;

            for (final PropInfo propInfo : entityInfo.propInfoList) {
                String columnFamilyName = null;
                String columnName = null;
                boolean hasColumnAnnotation = false;

                final ColumnFamily propColumnFamilyAnno = propInfo.getAnnotation(ColumnFamily.class);

                if (hasColumnFamilyValue(propColumnFamilyAnno)) {
                    columnFamilyName = getAnnotatedColumnFamily(propColumnFamilyAnno);
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

    // _1: family -> qualifier -> (property, has @Column); _2: qualifier -> property, for this class flattened as a nested bean;
    // _3: family -> qualifier -> the only property that writes that cell (see the end of the method). toValue consults _3 only
    // for a non-empty qualifier without an entry in _1, and never counts a _3 entry as a member of the family when it searches
    // the family's nested beans, so a _3 entry cannot change how any other cell is routed.
    @SuppressWarnings("deprecation")
    private static Tuple3<Map<String, Map<String, Tuple2<String, Boolean>>>, Map<String, String>, Map<String, Map<String, Tuple2<String, Boolean>>>> getFamilyColumnFieldNameMap(
            final Class<?> entityClass) {
        Tuple3<Map<String, Map<String, Tuple2<String, Boolean>>>, Map<String, String>, Map<String, Map<String, Tuple2<String, Boolean>>>> familyColumnFieldNameMapTP = classFamilyColumnFieldNamePool
                .get(entityClass);

        if (familyColumnFieldNameMapTP == null) {
            final BeanInfo entityInfo = ParserUtil.getBeanInfo(entityClass);
            final ColumnFamily defaultColumnFamily = entityInfo.getAnnotation(ColumnFamily.class);
            final String defaultColumnFamilyName = hasColumnFamilyValue(defaultColumnFamily) ? getAnnotatedColumnFamily(defaultColumnFamily) : null;

            familyColumnFieldNameMapTP = Tuple.of(new HashMap<>(), new HashMap<>(entityInfo.propInfoList.size()), new HashMap<>());

            final Tuple3<Map<String, Map<String, Tuple2<String, Boolean>>>, Map<String, String>, Map<String, Map<String, Tuple2<String, Boolean>>>> finalFamilyColumnFieldNameMapTP = familyColumnFieldNameMapTP;
            final List<Tuple2<String, Tuple2<String, Boolean>>> familyVariantClaims = new ArrayList<>();
            final Map<String, Tuple2<String, Boolean>> propColumnFieldTPs = new HashMap<>(entityInfo.propInfoList.size());

            for (final PropInfo propInfo : entityInfo.propInfoList) {
                List<String> columnFamilyNames = null;
                List<String> claimedFamilyNameVariants = N.emptyList();
                List<String> columnNames = null;
                boolean hasColumnAnnotation = false;

                final ColumnFamily propColumnFamilyAnno = propInfo.getAnnotation(ColumnFamily.class);
                final boolean hasFixedColumnFamily = hasColumnFamilyValue(propColumnFamilyAnno) || Strings.isNotEmpty(defaultColumnFamilyName);

                if (hasColumnFamilyValue(propColumnFamilyAnno)) {
                    columnFamilyNames = N.asList(getAnnotatedColumnFamily(propColumnFamilyAnno));
                } else if (Strings.isNotEmpty(defaultColumnFamilyName)) {
                    columnFamilyNames = N.asList(defaultColumnFamilyName);
                } else {
                    // A naming-policy variant must not hijack a name an earlier property already registered, but the
                    // property's own (CAMEL_CASE) name is always kept: dropping it would silently lose this property's
                    // cells whenever an earlier property claimed the same name (e.g. an explicit @ColumnFamily/@Column).
                    final List<String> familyNameVariants = Stream.of(NamingPolicy.values()).map(it -> formatName(propInfo.name, it)).toList();
                    final List<String> unclaimedFamilyNames = Stream.of(familyNameVariants)
                            .filter(it -> it.equals(propInfo.name) || !finalFamilyColumnFieldNameMapTP._1.containsKey(it))
                            .toList();

                    columnFamilyNames = unclaimedFamilyNames;
                    claimedFamilyNameVariants = Stream.of(familyNameVariants).filter(it -> !unclaimedFamilyNames.contains(it)).toList();
                }

                if (propInfo.columnName.isPresent()) {
                    columnNames = N.asList(propInfo.columnName.get());
                    hasColumnAnnotation = true;
                } else {
                    // A qualifier variant is a real write coordinate only in a fixed (annotated or class-default) family: there
                    // AnyPut writes formatName(name, policy) as the qualifier; elsewhere it writes the empty qualifier. So in a
                    // fixed family the variant is checked against that family below, not dropped because another family uses
                    // the same name: dropping it made a SNAKE_CASE mapper lose a cell it wrote itself.
                    columnNames = Stream.of(NamingPolicy.values())
                            .map(it -> formatName(propInfo.name, it))
                            .filter(it -> hasFixedColumnFamily || it.equals(propInfo.name) || !finalFamilyColumnFieldNameMapTP._2.containsKey(it))
                            .transform(s -> propInfo.type.isBean() || (Strings.isEmpty(defaultColumnFamilyName) && !hasColumnFamilyValue(propColumnFamilyAnno))
                                    ? s.append(EMPTY_QUALIFIER)
                                    : s)
                            .toList();
                }

                final Tuple2<String, Boolean> columnFieldTP = Tuple.of(propInfo.name, hasColumnAnnotation);
                propColumnFieldTPs.put(propInfo.name, columnFieldTP);

                for (final String columnFamilyName : columnFamilyNames) {
                    Map<String, Tuple2<String, Boolean>> columnFieldMap = familyColumnFieldNameMapTP._1.get(columnFamilyName);

                    if (columnFieldMap == null) {
                        columnFieldMap = new HashMap<>(columnNames.size());
                        familyColumnFieldNameMapTP._1.put(columnFamilyName, columnFieldMap);
                    }

                    for (final String columnName : columnNames) {
                        if (columnName.isEmpty() && propInfo.type.isBean() && !hasColumnAnnotation) {
                            // A nested bean's empty qualifier is only a fallback; it must not displace the real owner.
                            columnFieldMap.putIfAbsent(columnName, columnFieldTP);
                        } else if (hasFixedColumnFamily && !hasColumnAnnotation && !columnName.equals(propInfo.name)
                                && (columnFieldMap.containsKey(columnName) || (finalFamilyColumnFieldNameMapTP._2.containsKey(columnName)
                                        && isNestedBeanQualifier(entityInfo, defaultColumnFamilyName, columnFamilyName, columnName)))) {
                            // A qualifier variant never displaces a qualifier another property registered in this family. A variant
                            // that only another family uses (formerly always dropped) is kept, unless a flattened nested bean of this
                            // family may be written with the same qualifier: such a cell is still routed to the nested bean, as before.
                            continue;
                        } else {
                            columnFieldMap.put(columnName, columnFieldTP);
                        }
                    }
                }

                // An unannotated scalar is written to (formatName(name, policy), empty qualifier). When an earlier property
                // already registered that family name, the variant family was dropped as a whole and a mapper using that
                // policy lost the cell it wrote itself. Claim just that coordinate (after all properties are registered, below).
                if (!hasColumnAnnotation && !propInfo.type.isBean()) {
                    for (final String columnFamilyName : claimedFamilyNameVariants) {
                        familyVariantClaims.add(Tuple.of(columnFamilyName, columnFieldTP));
                    }
                }

                // This map resolves the qualifiers of a FLATTENED nested bean, whose properties all share one family on write,
                // so here a qualifier variant still must not hijack a name registered by ANY earlier property.
                for (final String columnName : columnNames) {
                    if (!hasFixedColumnFamily || hasColumnAnnotation || columnName.isEmpty() || columnName.equals(propInfo.name)
                            || !familyColumnFieldNameMapTP._2.containsKey(columnName)) {
                        familyColumnFieldNameMapTP._2.put(columnName, propInfo.name);
                    }
                }
            }

            // The claimed coordinate is taken when it is free, or held by a nested bean's empty-qualifier fallback (which skips
            // an empty qualifier anyway) only if that bean is the family's sole nested bean and keeps another entry there: once
            // the fallback is a scalar, toValue finds a nested bean only through its other entries (findNestedBeanOwner), so taking it
            // otherwise lost the whole bean or routed its cells to another nested bean of the family. Done after the loop so
            // a later property cannot change the entries this decision is based on.
            for (final Tuple2<String, Tuple2<String, Boolean>> claim : familyVariantClaims) {
                final Map<String, Tuple2<String, Boolean>> columnFieldMap = familyColumnFieldNameMapTP._1.get(claim._1);
                final Tuple2<String, Boolean> existing = columnFieldMap.get(EMPTY_QUALIFIER);

                if (existing == null || isSoleRoutableNestedBeanFallback(entityInfo, columnFieldMap, existing)) {
                    columnFieldMap.put(EMPTY_QUALIFIER, claim._2);
                }
            }

            // WHY: a @Column property and a flattened nested bean without a fixed family are written to (formatName(name, policy),
            // the @Column name or each nested qualifier); under a policy whose family variant an earlier property had registered,
            // the variant was dropped as a whole and the property lost its own cells, as the unannotated scalar above did. More
            // generally, a cell with no entry of its own reached a nested bean of its family only through the family's fallback,
            // which could pick a bean that never writes it. So every non-empty qualifier that has no entry in its family and is
            // written there by exactly one property, under any naming policy (as AnyPut.create writes), is routed to that
            // property (_3); a nested bean only when the qualifier reads back into the nested property that writes it. Any
            // other written cell (two or more properties write it, under the same or different policies, or its one writer
            // would not read it back) is marked CONTESTED_CELL_TP and keeps the routing it had (see toValue). Done after every
            // other registration.
            // WHY the row key is left out of the writers: AnyPut.create never writes the row-key property, which getCellWriterMap
            // lists like any other, so a cell it shared with one other property was marked contested, and that property could not
            // read back a cell only it writes. The row key resolved so far is used (registerRowKeyProperty clears this cache, so a
            // new row key rebuilds the map); a cell then written by no property gets no _3 entry and toValue routes it as unwritten.
            final Method resolvedRowKeySetMethod = classRowKeySetMethodPool.get(entityClass);
            final Method rowKeySetMethod = resolvedRowKeySetMethod == ClassUtil.SENTINEL_METHOD ? null : resolvedRowKeySetMethod;

            for (final Map.Entry<String, Map<String, List<PropInfo>>> familyEntry : getCellWriterMap(entityInfo, defaultColumnFamilyName).entrySet()) {
                final String columnFamilyName = familyEntry.getKey();
                final Map<String, Tuple2<String, Boolean>> columnFieldMap = familyColumnFieldNameMapTP._1.get(columnFamilyName);

                for (final Map.Entry<String, List<PropInfo>> qualifierEntry : familyEntry.getValue().entrySet()) {
                    final String qualifier = qualifierEntry.getKey();

                    if (qualifier.isEmpty() || columnFieldMap == null || columnFieldMap.containsKey(qualifier)) {
                        continue;
                    }

                    final List<PropInfo> writers = new ArrayList<>(qualifierEntry.getValue().size());

                    for (final PropInfo cellWriter : qualifierEntry.getValue()) {
                        if (rowKeySetMethod == null || !rowKeySetMethod.equals(cellWriter.setMethod)) {
                            writers.add(cellWriter);
                        }
                    }

                    if (writers.isEmpty()) {
                        continue;
                    }

                    final PropInfo writer = writers.size() == 1 ? writers.get(0) : null;
                    final boolean isSoleWriter = writer != null
                            && (!isFlattenedNestedBean(writer) || readsBackIntoWriter(writer, defaultColumnFamilyName, columnFamilyName, qualifier));

                    familyColumnFieldNameMapTP._3.computeIfAbsent(columnFamilyName, k -> new HashMap<>())
                            .put(qualifier, isSoleWriter ? propColumnFieldTPs.get(writer.name) : CONTESTED_CELL_TP);
                }
            }

            classFamilyColumnFieldNamePool.put(entityClass, familyColumnFieldNameMapTP);
        }

        return familyColumnFieldNameMapTP;
    }

    /**
     * Returns whether the given {@link ColumnFamily} annotation carries a usable family name.
     *
     * @param columnFamily the annotation to inspect; may be {@code null}
     * @return {@code true} if {@code columnFamily} is not {@code null} and its {@code value}
     *         is not empty, {@code false} otherwise
     */
    static boolean hasColumnFamilyValue(final ColumnFamily columnFamily) {
        return columnFamily != null && Strings.isNotEmpty(columnFamily.value());
    }

    private static boolean isNestedBeanFallback(final BeanInfo entityInfo, final Tuple2<String, Boolean> columnFieldTP) {
        final PropInfo propInfo = columnFieldTP._2 ? null : entityInfo.getPropInfo(columnFieldTP._1);

        return propInfo != null && propInfo.type.isBean();
    }

    // Whether fallbackTP is a nested bean's empty-qualifier fallback that toValue still reaches through another entry of the
    // family (columnFieldMap), with no other nested bean in the family competing for its qualifiers in toValue's search.
    private static boolean isSoleRoutableNestedBeanFallback(final BeanInfo entityInfo, final Map<String, Tuple2<String, Boolean>> columnFieldMap,
            final Tuple2<String, Boolean> fallbackTP) {
        if (!isNestedBeanFallback(entityInfo, fallbackTP)) {
            return false;
        }

        boolean hasOtherEntry = false;

        for (final Map.Entry<String, Tuple2<String, Boolean>> entry : columnFieldMap.entrySet()) {
            final Tuple2<String, Boolean> columnFieldTP = entry.getValue();

            if (entry.getKey().isEmpty()) {
                continue;
            } else if (columnFieldTP._1.equals(fallbackTP._1)) {
                hasOtherEntry = true;
            } else if (!columnFieldTP._2) {
                final PropInfo propInfo = entityInfo.getPropInfo(columnFieldTP._1);

                if (propInfo != null && propInfo.jsonXmlType.isBean()) {
                    return false;
                }
            }
        }

        return hasOtherEntry;
    }

    // Whether a nested bean of entityInfo that is flattened into columnFamilyName may write qualifier under some naming
    // policy. Reads only the bean metadata (never getFamilyColumnFieldNameMap), so a self-referencing bean cannot recurse.
    private static boolean isNestedBeanQualifier(final BeanInfo entityInfo, final String defaultColumnFamilyName, final String columnFamilyName,
            final String qualifier) {
        for (final PropInfo propInfo : entityInfo.propInfoList) {
            if (!propInfo.jsonXmlType.isBean() || propInfo.columnName.isPresent()) {
                continue;
            }

            final ColumnFamily propColumnFamilyAnno = propInfo.getAnnotation(ColumnFamily.class);
            boolean inFamily = false;

            if (hasColumnFamilyValue(propColumnFamilyAnno)) {
                inFamily = propColumnFamilyAnno.value().equals(columnFamilyName);
            } else if (Strings.isNotEmpty(defaultColumnFamilyName)) {
                inFamily = defaultColumnFamilyName.equals(columnFamilyName);
            } else {
                for (final NamingPolicy namingPolicy : NamingPolicy.values()) {
                    inFamily = inFamily || formatName(propInfo.name, namingPolicy).equals(columnFamilyName);
                }
            }

            if (inFamily) {
                for (final PropInfo nestedPropInfo : ParserUtil.getBeanInfo(propInfo.jsonXmlType.javaType()).propInfoList) {
                    if (nestedPropInfo.columnName.isPresent()) {
                        if (nestedPropInfo.columnName.get().equals(qualifier)) {
                            return true;
                        }
                    } else {
                        for (final NamingPolicy namingPolicy : NamingPolicy.values()) {
                            if (formatName(nestedPropInfo.name, namingPolicy).equals(qualifier)) {
                                return true;
                            }
                        }
                    }
                }
            }
        }

        return false;
    }

    // Whether AnyPut.create flattens propInfo: a bean-typed property without @Column, written as one cell per nested property.
    private static boolean isFlattenedNestedBean(final PropInfo propInfo) {
        return propInfo.jsonXmlType.isBean() && !propInfo.columnName.isPresent();
    }

    // The column family AnyPut.create writes propInfo to under namingPolicy.
    private static String getWrittenColumnFamily(final PropInfo propInfo, final String defaultColumnFamilyName, final NamingPolicy namingPolicy) {
        final ColumnFamily propColumnFamilyAnno = propInfo.getAnnotation(ColumnFamily.class);

        return hasColumnFamilyValue(propColumnFamilyAnno) ? propColumnFamilyAnno.value()
                : Strings.isNotEmpty(defaultColumnFamilyName) ? defaultColumnFamilyName : formatName(propInfo.name, namingPolicy);
    }

    // family -> qualifier -> the properties of entityInfo that AnyPut.create writes that cell for under some naming policy (each
    // once, in declaration order). Mirrors AnyPut.create's choice of family and qualifier; reads only the bean metadata. The
    // row-key property, which AnyPut.create skips, is listed too (it may change through registerRowKeyProperty):
    // getFamilyColumnFieldNameMap leaves the resolved row key out of each cell's writers, and toValue treats a cell recorded for
    // the row key alone (in a map built concurrently with the row key's registration) as written by no property.
    private static Map<String, Map<String, List<PropInfo>>> getCellWriterMap(final BeanInfo entityInfo, final String defaultColumnFamilyName) {
        final Map<String, Map<String, List<PropInfo>>> cellWriterMap = new HashMap<>();

        for (final PropInfo propInfo : entityInfo.propInfoList) {
            final ColumnFamily propColumnFamilyAnno = propInfo.getAnnotation(ColumnFamily.class);
            final boolean hasFixedColumnFamily = hasColumnFamilyValue(propColumnFamilyAnno) || Strings.isNotEmpty(defaultColumnFamilyName);
            final List<PropInfo> nestedPropInfos = isFlattenedNestedBean(propInfo) ? ParserUtil.getBeanInfo(propInfo.jsonXmlType.javaType()).propInfoList
                    : null;

            for (final NamingPolicy namingPolicy : NamingPolicy.values()) {
                final Map<String, List<PropInfo>> qualifierWriterMap = cellWriterMap
                        .computeIfAbsent(getWrittenColumnFamily(propInfo, defaultColumnFamilyName, namingPolicy), k -> new HashMap<>());

                if (nestedPropInfos != null) {
                    for (final PropInfo nestedPropInfo : nestedPropInfos) {
                        addCellWriter(qualifierWriterMap,
                                nestedPropInfo.columnName.isPresent() ? nestedPropInfo.columnName.get() : formatName(nestedPropInfo.name, namingPolicy),
                                propInfo);
                    }
                } else {
                    addCellWriter(qualifierWriterMap, propInfo.columnName.isPresent() ? propInfo.columnName.get()
                            : hasFixedColumnFamily ? formatName(propInfo.name, namingPolicy) : EMPTY_QUALIFIER, propInfo);
                }
            }
        }

        return cellWriterMap;
    }

    private static void addCellWriter(final Map<String, List<PropInfo>> qualifierWriterMap, final String qualifier, final PropInfo propInfo) {
        final List<PropInfo> writers = qualifierWriterMap.computeIfAbsent(qualifier, k -> new ArrayList<>(1));

        if (!writers.contains(propInfo)) {
            writers.add(propInfo);
        }
    }

    // Whether a cell (columnFamilyName, qualifier) that the flattened nested bean propInfo writes reads back into the nested
    // property that writes it: exactly one nested property writes that qualifier under the policies that put propInfo in
    // that family, and the nested bean's qualifier map resolves the qualifier to it, as toValue resolves it.
    private static boolean readsBackIntoWriter(final PropInfo propInfo, final String defaultColumnFamilyName, final String columnFamilyName,
            final String qualifier) {
        final BeanInfo nestedBeanInfo = ParserUtil.getBeanInfo(propInfo.jsonXmlType.javaType());
        PropInfo nestedWriter = null;

        for (final NamingPolicy namingPolicy : NamingPolicy.values()) {
            if (getWrittenColumnFamily(propInfo, defaultColumnFamilyName, namingPolicy).equals(columnFamilyName)) {
                for (final PropInfo nestedPropInfo : nestedBeanInfo.propInfoList) {
                    if (qualifier
                            .equals(nestedPropInfo.columnName.isPresent() ? nestedPropInfo.columnName.get() : formatName(nestedPropInfo.name, namingPolicy))) {
                        if (nestedWriter != null && !nestedWriter.name.equals(nestedPropInfo.name)) {
                            return false;
                        }

                        nestedWriter = nestedPropInfo;
                    }
                }
            }
        }

        final PropInfo readPropInfo = nestedWriter == null ? null
                : nestedBeanInfo.getPropInfo(getNestedColumnFieldNameMap(nestedBeanInfo).getOrDefault(qualifier, qualifier));

        return readPropInfo != null && readPropInfo.name.equals(nestedWriter.name);
    }

    // The qualifier -> property map (_2) that getFamilyColumnFieldNameMap builds for nestedBeanInfo, computed the same way
    // from the bean metadata alone, so a self-referencing nested bean cannot recurse into getFamilyColumnFieldNameMap.
    private static Map<String, String> getNestedColumnFieldNameMap(final BeanInfo nestedBeanInfo) {
        final ColumnFamily defaultColumnFamily = nestedBeanInfo.getAnnotation(ColumnFamily.class);
        final String defaultColumnFamilyName = hasColumnFamilyValue(defaultColumnFamily) ? getAnnotatedColumnFamily(defaultColumnFamily) : null;
        final Map<String, String> columnFieldNameMap = new HashMap<>(nestedBeanInfo.propInfoList.size());

        for (final PropInfo propInfo : nestedBeanInfo.propInfoList) {
            final ColumnFamily propColumnFamilyAnno = propInfo.getAnnotation(ColumnFamily.class);
            final boolean hasFixedColumnFamily = hasColumnFamilyValue(propColumnFamilyAnno) || Strings.isNotEmpty(defaultColumnFamilyName);
            final boolean hasColumnAnnotation = propInfo.columnName.isPresent();
            final List<String> columnNames = new ArrayList<>();

            if (hasColumnAnnotation) {
                columnNames.add(propInfo.columnName.get());
            } else {
                for (final NamingPolicy namingPolicy : NamingPolicy.values()) {
                    final String columnName = formatName(propInfo.name, namingPolicy);

                    if (hasFixedColumnFamily || columnName.equals(propInfo.name) || !columnFieldNameMap.containsKey(columnName)) {
                        columnNames.add(columnName);
                    }
                }

                if (propInfo.type.isBean() || (Strings.isEmpty(defaultColumnFamilyName) && !hasColumnFamilyValue(propColumnFamilyAnno))) {
                    columnNames.add(EMPTY_QUALIFIER);
                }
            }

            for (final String columnName : columnNames) {
                if (!hasFixedColumnFamily || hasColumnAnnotation || columnName.isEmpty() || columnName.equals(propInfo.name)
                        || !columnFieldNameMap.containsKey(columnName)) {
                    columnFieldNameMap.put(columnName, propInfo.name);
                }
            }
        }

        return columnFieldNameMap;
    }

    // The flattened nested-bean properties that hold an entry (other than a @Column one) in a family's map, in declaration order.
    private static List<Tuple2<String, Boolean>> getNestedBeanTPs(final BeanInfo entityInfo, final Map<String, Tuple2<String, Boolean>> columnFieldMap) {
        final Map<String, Tuple2<String, Boolean>> propNameTPMap = new HashMap<>();

        for (final Tuple2<String, Boolean> columnFieldTP : columnFieldMap.values()) {
            if (!columnFieldTP._2) {
                propNameTPMap.putIfAbsent(columnFieldTP._1, columnFieldTP);
            }
        }

        final List<Tuple2<String, Boolean>> nestedBeanTPs = new ArrayList<>();

        for (final PropInfo propInfo : entityInfo.propInfoList) {
            final Tuple2<String, Boolean> columnFieldTP = propNameTPMap.get(propInfo.name);

            if (columnFieldTP != null && propInfo.jsonXmlType.isBean()) {
                nestedBeanTPs.add(columnFieldTP);
            }
        }

        return nestedBeanTPs;
    }

    // The nested bean of a family that receives a qualifier that has no entry of its own and that no property writes: the first
    // one (in declaration order) with a nested property of exactly that name or @Column name, else the first one whose qualifier
    // map knows it as a naming-policy variant, else null. (A written cell is routed by _3 or, when contested, as it always was.)
    private static Tuple2<String, Boolean> findNestedBeanOwner(final BeanInfo entityInfo, final List<Tuple2<String, Boolean>> nestedBeanTPs,
            final String qualifier) {
        for (final Tuple2<String, Boolean> nestedBeanTP : nestedBeanTPs) {
            for (final PropInfo nestedPropInfo : ParserUtil.getBeanInfo(entityInfo.getPropInfo(nestedBeanTP._1).jsonXmlType.javaType()).propInfoList) {
                if (qualifier.equals(nestedPropInfo.columnName.isPresent() ? nestedPropInfo.columnName.get() : nestedPropInfo.name)) {
                    return nestedBeanTP;
                }
            }
        }

        for (final Tuple2<String, Boolean> nestedBeanTP : nestedBeanTPs) {
            if (getFamilyColumnFieldNameMap(entityInfo.getPropInfo(nestedBeanTP._1).jsonXmlType.javaType())._2.containsKey(qualifier)) {
                return nestedBeanTP;
            }
        }

        return null;
    }

    private static String getAnnotatedColumnFamily(final ColumnFamily defaultColumnFamilyAnno) {
        return N.checkArgNotEmpty(defaultColumnFamilyAnno.value(), "Column Family can't be null or empty");
    }

    /**
     * Reads every {@link Result} from {@code resultScanner}, converts each into
     * {@code targetType}, and returns them as a {@link List}.
     *
     * <p>Equivalent to {@code toList(resultScanner, 0, Integer.MAX_VALUE, targetType)}.
     * The scanner is always closed (via {@link IOUtil#closeQuietly(AutoCloseable)})
     * before this method returns, even on exception.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * try (Table table = executor.getTable("users")) {              // throws UncheckedIOException
     *     ResultScanner scanner = table.getScanner(new Scan());     // throws IOException
     *     List<User> users = HBaseExecutor.toList(scanner, User.class);  // scanner is closed inside toList
     * }
     *
     * // A null scanner is rejected before conversion metadata is prepared
     * HBaseExecutor.toList((ResultScanner) null, User.class);       // throws IllegalArgumentException
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param resultScanner the HBase result scanner to drain; must not be {@code null}
     * @param targetType the target class — a JavaBean class or a single-value type
     *                    (e.g. {@code String}, {@code Integer}, {@code Date}); must not be {@code null}
     * @return a list of converted objects; empty results, including cursor-only progress
     *         notifications, are skipped
     * @throws IllegalArgumentException if {@code resultScanner} or {@code targetType} is {@code null}, a nonempty row is requested as a
     *         {@link Map}, a scalar row contains more than one cell, or the target bean has multiple row-key properties, an unsupported row-key
     *         type, or a row-key property without both a getter and setter
     * @throws UncheckedIOException if reading from {@code resultScanner} fails with an {@link IOException}
     * @throws RuntimeException if converting a cell or row key, creating a target bean, or reading or assigning one of its properties fails
     * @see #toList(ResultScanner, int, int, Class)
     */
    public static <T> List<T> toList(final ResultScanner resultScanner, final Class<T> targetType)
            throws IllegalArgumentException, UncheckedIOException, RuntimeException {
        return toList(resultScanner, 0, Integer.MAX_VALUE, targetType);
    }

    /**
     * Reads a windowed range of results from {@code resultScanner}, converts each into
     * {@code targetType}, and returns them as a {@link List}.
     *
     * <p>The first {@code offset} data-bearing results are read and discarded; up to
     * {@code count} subsequent data-bearing results are converted via the per-row mapping for
     * {@code targetType}. Empty results are ignored. In particular, cursor-only progress
     * notifications produced by a scan with cursor results enabled neither consume the offset
     * nor appear in the returned list.
     * The scanner is always closed (via {@link IOUtil#closeQuietly(AutoCloseable)})
     * before this method returns, even on exception.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Skip the first 50 results, then take up to 100
     * List<User> users = HBaseExecutor.toList(scanner, 50, 100, User.class);  // scanner closed inside toList
     *
     * // count == 0 reads (and discards) the first 50 rows, then returns an empty list
     * List<User> none = HBaseExecutor.toList(scanner2, 50, 0, User.class);    // returns [] (empty list)
     *
     * // A negative offset or count is rejected up front
     * HBaseExecutor.toList(scanner3, -1, 100, User.class);   // throws IllegalArgumentException
     * HBaseExecutor.toList(scanner4, 0, -1, User.class);     // throws IllegalArgumentException
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param resultScanner the HBase result scanner to process; must not be {@code null} unless both
     *                    {@code offset} and {@code count} are zero
     * @param offset the number of data-bearing results to skip from the beginning; must be non-negative
     * @param count the maximum number of data-bearing results to convert; must be non-negative
     * @param targetType the target class — a JavaBean class or a single-value type
     *                    (e.g. {@code String}, {@code Integer}, {@code Date}); must not be {@code null}
     * @return a list of converted objects from the requested window
     * @throws IllegalArgumentException if {@code offset} or {@code count} is negative; if {@code resultScanner} is {@code null} and either
     *         {@code offset} or {@code count} is positive; if {@code targetType} is {@code null}; or if a nonempty row is
     *         requested as a {@link Map}, a scalar row contains more than one cell, or the target bean has multiple row-key properties, an
     *         unsupported row-key type, or a row-key property without both a getter and setter
     * @throws UncheckedIOException if reading from {@code resultScanner} fails with an {@link IOException}
     * @throws RuntimeException if converting a cell or row key, creating a target bean, or reading or assigning one of its properties fails
     */
    public static <T> List<T> toList(final ResultScanner resultScanner, int offset, int count, final Class<T> targetType)
            throws IllegalArgumentException, UncheckedIOException, RuntimeException {
        try {
            N.checkArgument(offset >= 0 && count >= 0, "Offset and count can't be negative");

            if (offset > 0 || count > 0) {
                N.checkArgNotNull(resultScanner, cs.resultScanner);
            }
            N.checkArgNotNull(targetType, cs.targetType);

            final Type<T> type = N.typeOf(targetType);

            final BeanInfo entityInfo = type.isBean() ? ParserUtil.getBeanInfo(targetType) : null;
            final Method rowKeySetMethod = type.isBean() ? getRowKeySetMethod(targetType) : null;
            final Type<?> rowKeyType = rowKeySetMethod == null ? null : N.typeOf(rowKeySetMethod.getParameterTypes()[0]);
            final Map<String, Map<String, Tuple2<String, Boolean>>> familyFieldNameMap = type.isBean() ? getFamilyColumnFieldNameMap(targetType)._1 : null;

            final List<T> resultList = new ArrayList<>();

            Result result = null;

            while (offset > 0 && (result = resultScanner.next()) != null) {
                if (!result.isEmpty()) {
                    offset--;
                }
            }

            while (count > 0 && (result = resultScanner.next()) != null) {
                if (!result.isEmpty()) {
                    resultList.add(toValue(type, entityInfo, rowKeySetMethod, rowKeyType, familyFieldNameMap, result));
                    count--;
                }
            }

            return resultList;

        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            IOUtil.closeQuietly(resultScanner);
        }
    }

    /**
     * Converts an in-memory list of HBase {@link Result}s into entities of {@code targetType}.
     *
     * <p>Each element of {@code results} is converted using the same per-row mapping as
     * {@link #toEntity(Result, Class)}. Elements for which {@link Result#isEmpty()} is
     * {@code true} are skipped, so the returned list may be shorter than {@code results}
     * and is not necessarily index-aligned with it.</p>
     *
     * @param <T> the target type for conversion
     * @param results the HBase results to convert; must not be {@code null}
     * @param targetType the target class — a JavaBean class or a single-value type
     *                    (e.g. {@code String}, {@code Integer}, {@code Date})
     * @return a list of converted entities; empty results are skipped
     * @throws IllegalArgumentException if {@code results} or {@code targetType} is {@code null}; also if the target bean declares multiple
     *         row-key properties, an unsupported row-key type, or a row-key property without a getter and setter; also if a nonempty result is
     *         requested as a {@link Map} or a scalar result contains more than one cell
     * @throws NullPointerException if {@code results} contains a {@code null} element
     * @throws UncheckedIOException if reading cells from any result fails with an {@link IOException}
     * @throws RuntimeException if converting a cell or row key, creating a target bean, or reading or assigning one of its properties fails
     */
    static <T> List<T> toList(final List<Result> results, final Class<T> targetType)
            throws IllegalArgumentException, NullPointerException, UncheckedIOException, RuntimeException {
        N.checkArgNotNull(results, cs.results);
        N.checkArgNotNull(targetType, cs.targetType);

        final Type<T> type = N.typeOf(targetType);

        final BeanInfo entityInfo = type.isBean() ? ParserUtil.getBeanInfo(targetType) : null;
        final Method rowKeySetMethod = type.isBean() ? getRowKeySetMethod(targetType) : null;
        final Type<?> rowKeyType = rowKeySetMethod == null ? null : N.typeOf(rowKeySetMethod.getParameterTypes()[0]);
        final Map<String, Map<String, Tuple2<String, Boolean>>> familyFieldNameMap = type.isBean() ? getFamilyColumnFieldNameMap(targetType)._1 : null;

        final List<T> resultList = new ArrayList<>(results.size());

        try {
            for (final Result result : results) {
                if (result.isEmpty()) {
                    continue;
                }

                resultList.add(toValue(type, entityInfo, rowKeySetMethod, rowKeyType, familyFieldNameMap, result));
            }
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }

        return resultList;
    }

    /**
     * Converts a single HBase {@link Result} into an instance of {@code targetType}.
     *
     * <p>For JavaBean target classes this performs full object-relational mapping: the
     * row-key cell (if a row-key setter is registered or discovered via {@code @Id})
     * populates the row-key property, and each subsequent cell is matched to a bean
     * property using the entity's column-family/qualifier mapping. Cells whose family,
     * qualifier, or field name cannot be resolved are silently ignored without creating nested
     * bean instances, and so are cells of read-only properties - no setter and no writable field, such as a
     * getter-only computed property or a field-backed getter-only property under {@code @AccessFieldByMethod} - whose
     * values {@link AnyPut#create(Object)} writes but which cannot be assigned. Only that write path decides: a
     * {@code @JsonXmlField(direction = SERIALIZE_ONLY)} property that has a setter is still written on read (as in
     * DynamoDB item mapping), and a read-only property is still skipped when its field declares
     * {@code DESERIALIZE_ONLY}. The one exception is a read-only mutable nested bean that is flattened into its own column
     * family (the family is the nested bean property, the qualifiers are its properties) and held by a field: its
     * cells are written into the instance that field already holds, which needs no setter; when that field holds
     * {@code null} the whole family is skipped and the rest of the row is still read (a read-only immutable nested bean
     * cannot be filled in place, so its family is always skipped). Otherwise a flattened mutable nested bean that the
     * property of a mutable entity already holds is filled in place, and any other flattened nested bean is created for
     * the row and assigned to its property once, after the last cell of the row, so a setter or builder method that
     * copies its argument receives it complete. Such a nested bean may be immutable (a record, or a bean built through
     * its constructor or builder): it is built from its cells, and a nested property without a cell gets its default
     * value or, only when the owning entity is mutable, the value it has in the instance the property already holds (the
     * nested value that the builder or constructor of an immutable entity starts with is not consulted). A read-only
     * nested bean mapped as a single {@code @Column} cell is skipped like any other read-only property. A cell whose column maps to the
     * row-key property is ignored too: that property always receives the key of the row. A property without a cell, such
     * as one that was {@code null} or an empty value wrapper (empty {@code Optional}/{@code Nullable}/{@code OptionalInt}
     * and the like, a {@code Holder} of {@code null}) when {@link AnyPut#create(Object)} wrote the row, is not assigned and
     * keeps the value a new instance has. An empty qualifier maps a
     * scalar property stored directly in its family; it does not select a property inside a
     * flattened nested bean. Bean properties declared as {@link HBaseColumn}, {@code Collection<HBaseColumn>}, or
     * {@code Map<?, HBaseColumn>} are populated cell-by-cell with versioned values (a single {@code HBaseColumn} gets the
     * newest cell; a map is keyed by each cell's timestamp, as a {@code Long}, whatever key type it declares).</p>
     *
     * <p>A cell whose qualifier has no mapping of its own in its family (such as a flattened nested property's) is read into
     * the only property that {@link AnyPut#create(Object, NamingPolicy)} writes that cell for under any naming policy. A cell
     * that several properties can write (under the same or different naming policies) is ambiguous: the value it holds belongs
     * to whichever of them was written to it, so it is routed as in earlier versions, to the family's first nested bean when
     * that bean knows the qualifier, else to another nested bean of the family that knows it (in no specified order), and
     * only one of the writers can read it back. A cell that no property writes goes to the first declared nested bean with
     * a nested property of exactly that name or {@code @Column} name, else to the first declared one that has the qualifier
     * as a naming-policy variant of a nested property.</p>
     *
     * <p>For single-value target types the {@link Result} must contain exactly one cell,
     * whose value is decoded into {@code T} — via {@link Type#valueOf(String)} for text-decoded
     * types, while {@code byte[]} and {@link ByteBuffer} targets receive a copy of the raw cell
     * bytes; a result with more cells triggers {@link IllegalArgumentException}. Map target types
     * are explicitly rejected.</p>
     *
     * <p>Conversion reads the cells through {@code result.cellScanner()}, which resets and then
     * advances the {@link Result}'s single internal cell cursor; the same {@code Result} instance
     * can be converted repeatedly, but not concurrently from several threads.</p>
     *
     * <p>Entity-class requirements:</p>
     * <ul>
     *   <li>JavaBean conventions (getters/setters); an immutable bean (a record, or a bean built through its constructor
     *       or builder) is supported as a target without a row-key property, whose properties are passed to its
     *       constructor or builder</li>
     *   <li>{@code @ColumnFamily} / {@code @Column} for custom mapping; without them,
     *       field names become column-family names with empty qualifiers (or are flattened
     *       for nested bean fields)</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Result result = table.get(new Get(Bytes.toBytes("user123")));
     * User user = HBaseExecutor.toEntity(result, User.class);   // mapped entity, or null if result is empty
     *
     * // Single-cell single-value extraction (the Result must hold exactly one cell)
     * Result nameCell = table.get(new Get(Bytes.toBytes("user123")).addColumn(Bytes.toBytes("info"), Bytes.toBytes("name")));
     * String name = HBaseExecutor.toEntity(nameCell, String.class);
     *
     * // An empty Result yields the target type's default value (null for a bean type)
     * User none = HBaseExecutor.toEntity(Result.EMPTY_RESULT, User.class);   // returns null
     *
     * // Map target types are rejected (the check runs even for an empty Result)
     * HBaseExecutor.toEntity(Result.EMPTY_RESULT, Map.class);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param <T> the target type
     * @param result the HBase Result to convert; must not be {@code null}
     * @param targetType the target class — a JavaBean class with getter/setter methods or a
     *                    single-value type (e.g. {@code String}, {@code Integer}, {@code Date}).
     *                    {@link Map} types are not supported. Must not be {@code null}.
     * @return the converted entity, or the type's default value if the result is empty
     * @throws IllegalArgumentException if {@code result} or {@code targetType} is {@code null}, {@code targetType} is a {@link Map}, a scalar
     *         result contains more than one cell, or the target bean declares multiple row-key properties, an unsupported row-key type, or a
     *         row-key property without both a getter and setter
     * @throws UncheckedIOException if reading cells from {@code result} fails with an {@link IOException}
     * @throws RuntimeException if converting a cell or row key, creating a target bean, or reading or assigning one of its properties fails
     * @see Result
     */
    public static <T> T toEntity(final Result result, final Class<T> targetType) throws IllegalArgumentException, UncheckedIOException, RuntimeException {
        N.checkArgNotNull(result, cs.result);

        return toValue(result, targetType);
    }

    /**
     * Package-private entry point that converts a single HBase {@link Result} to
     * {@code targetType}, returning the {@link Type#defaultValue()} for empty results.
     *
     * <p>Used internally by {@link #toEntity(Result, Class)} and the per-table {@code get}
     * overloads. Wraps any {@link IOException} from the result's cell scanner in
     * {@link UncheckedIOException}.</p>
     *
     * @param <T> the target type for conversion
     * @param result the HBase Result to convert; must not be {@code null}
     * @param targetType the target class — a JavaBean class or a single-value type
     *                    (e.g. {@code String}, {@code Integer}, {@code Date}); must not be {@code null}
     * @return the converted entity, or the type's default value if the result is empty
     * @throws IllegalArgumentException if {@code result} or {@code targetType} is {@code null}, {@code targetType} is a {@link Map}, a scalar
     *         result contains more than one cell, or the target bean declares multiple row-key properties, an unsupported row-key type, or a
     *         row-key property without both a getter and setter
     * @throws UncheckedIOException if reading cells from {@code result} fails with an {@link IOException}
     * @throws RuntimeException if converting a cell or row key, creating a target bean, or reading or assigning one of its properties fails
     */
    static <T> T toValue(final Result result, final Class<T> targetType) throws IllegalArgumentException, UncheckedIOException, RuntimeException {
        N.checkArgNotNull(result, cs.result);
        N.checkArgNotNull(targetType, cs.targetType);

        final Type<T> type = N.typeOf(targetType);

        try {
            return toValue(result, targetType, type);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Converts one result using its resolved target type, preparing bean metadata when needed.
     *
     * @param <T> the converted value type
     * @param result the result whose cells are read
     * @param targetType the target class used to resolve bean metadata
     * @param type the resolved target type
     * @return the converted value, or the type's default value for an empty result
     * @throws NullPointerException if {@code type} or {@code result} is {@code null}
     * @throws IllegalArgumentException if the target is a map, a scalar result contains more than one cell,
     *         or the bean declares multiple row-key properties, a row-key type containing {@code HBaseColumn},
     *         or a row-key property without both a getter and setter
     * @throws IOException if advancing the result's cell scanner fails
     * @throws RuntimeException if converting a cell or row key, creating a target bean, or reading or assigning one of its properties fails
     */
    private static <T> T toValue(final Result result, final Class<T> targetType, final Type<T> type)
            throws NullPointerException, IllegalArgumentException, IOException, RuntimeException {
        if (type.isMap()) {
            throw new IllegalArgumentException("Map type is not supported for HBase result conversion");
        }

        // Don't probe with result.advance() here: Result implements CellScanner with a single internal
        // cursor, and a probing advance() would leave the cursor consumed. After a full conversion the
        // cursor already sits at end-of-cells, so a second conversion of the same Result would throw
        // NoSuchElementException from Result.advance(). A non-empty Result always has at least one cell,
        // and both branches below reset the cursor via result.cellScanner() before reading.
        if (result.isEmpty()) {
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

            final T value = (T) getCellValue(cell, type);

            if (cellScanner.advance()) {
                final Cell extraCell = cellScanner.current();
                throw new IllegalArgumentException(
                        "Cannot convert a result with more than one cell to class: " + ClassUtil.getCanonicalClassName(type.javaType())
                                + ". Found unexpected extra column: " + getFamilyString(extraCell) + ":" + getQualifierString(extraCell));
            }

            return value;
        }
    }

    /**
     * Converts one result using previously resolved bean and column metadata.
     *
     * @param <T> the converted value type
     * @param type the resolved target type
     * @param entityInfo the bean metadata, required for a nonempty bean result
     * @param rowKeySetMethod the row-key setter, or {@code null} when no row-key property is mapped
     * @param rowKeyType the row-key value type, or {@code null} when no row-key property is mapped
     * @param familyFieldNameMap the family and qualifier mappings, required for a nonempty bean result
     * @param result the result whose cells are read
     * @return the converted value, or the type's default value for an empty result
     * @throws NullPointerException if {@code type} or {@code result} is {@code null}, or required bean metadata is absent
     * @throws IllegalArgumentException if the target is a map or a scalar result contains more than one cell
     * @throws IOException if advancing the result's cell scanner fails
     * @throws RuntimeException if converting a cell or row key, creating a target bean, or reading or assigning one of its properties fails
     */
    @SuppressWarnings({ "null", "rawtypes" })
    private static <T> T toValue(final Type<T> type, final BeanInfo entityInfo, final Method rowKeySetMethod, final Type<?> rowKeyType,
            final Map<String, Map<String, Tuple2<String, Boolean>>> familyFieldNameMap, final Result result)
            throws NullPointerException, IllegalArgumentException, IOException, RuntimeException {
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
            Set<String> assignedSingleVersionColumns = null;
            Map<String, Map<String, Tuple2<String, Boolean>>> familySoleWriterTPMap = null;
            Map<String, List<Tuple2<String, Boolean>>> familyNestedBeanTPs = null;
            List<Tuple3<PropInfo, BeanInfo, Object>> nestedBeanResults = null;
            List<Tuple3<Object, PropInfo, Object>> versionedContainers = null;
            boolean isContestedCell = false;

            Map<String, Type<?>> columnValueTypeMap = null;
            Collection<HBaseColumn<?>> columnColl = null;
            Map<String, Collection<HBaseColumn<?>>> columnCollectionMap = null;
            Map<Long, HBaseColumn<?>> columnMap = null;
            Map<String, Map<Long, HBaseColumn<?>>> columnMapMap = null;
            HBaseColumn<?> column = null;

            while (cellScanner.advance()) {
                final Cell cell = cellScanner.current();

                if (rowKeyType != null && rowKey == null) {
                    rowKey = getRowKeyValue(cell, rowKeyType);
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
                            column = HBaseColumn.valueOf(getCellValue(cell, columnValueType), cell.getTimestamp()); //NOSONAR
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
                            column = HBaseColumn.valueOf(getCellValue(cell, columnValueType), cell.getTimestamp());
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
                isContestedCell = false;

                if (familyTP == null) {
                    // A cell that exactly one property writes (see getFamilyColumnFieldNameMap): it goes to that property.
                    if (familySoleWriterTPMap == null) {
                        familySoleWriterTPMap = getFamilyColumnFieldNameMap(type.javaType())._3;
                    }

                    final Map<String, Tuple2<String, Boolean>> soleWriterTPMap = familySoleWriterTPMap.get(family);

                    familyTP = soleWriterTPMap == null ? null : soleWriterTPMap.get(qualifier);

                    if (familyTP == CONTESTED_CELL_TP) {
                        isContestedCell = true;
                        familyTP = null;
                    } else if (familyTP != null && rowKeySetMethod != null && rowKeySetMethod.equals(entityInfo.getPropInfo(familyTP._1).setMethod)) {
                        // WHY: getCellWriterMap lists the row-key property like any other, but AnyPut.create never writes it (and
                        // the row key ignores mapped cells below), so a cell listed only for the row key is written by no property:
                        // route it as such instead of dropping it. (getFamilyColumnFieldNameMap already leaves out the row key it
                        // knows; this covers a map built concurrently with the row key's registration.)
                        familyTP = null;
                    }
                }

                if (familyTP == null) {
                    familyTP = familyTPMap.get(EMPTY_QUALIFIER);

                    // The family map keeps a single EMPTY_QUALIFIER entry: a real owner takes precedence over
                    // nested-bean fallbacks, and the first fallback is retained when no real owner exists.
                    // When that entry doesn't own this qualifier, resolve it against each candidate bean property
                    // of the family so cells of the other nested beans are not dropped or misrouted.
                    if (familyTP != null) {
                        final PropInfo fallbackPropInfo = entityInfo.getPropInfo(familyTP._1);
                        final boolean isBeanFallback = fallbackPropInfo != null && fallbackPropInfo.jsonXmlType.isBean();

                        // This branch is only reached with a non-empty, unknown qualifier (the empty qualifier
                        // resolves directly above). The fallback entry exists for bean-typed properties, whose
                        // nested field names are the qualifiers; routing a foreign qualifier into a scalar or
                        // versioned property would overwrite its real (empty-qualifier) cell value, so never use a
                        // non-bean fallback. The fallback may also be held by a scalar property sharing this family,
                        // so search the family's nested-bean properties for the owner of the qualifier.
                        // WHY: in a contested cell (several properties write it, possibly under different naming policies)
                        // the stored value belongs to whichever property was written to it, which depends on the policy and
                        // the write order, not on the qualifier; any rule that moved such a cell to another property traded one
                        // correct read for another. So a contested cell keeps exactly the routing it always had (the bean
                        // fallback when it knows the qualifier, else the first bean found in the family map's iteration order).
                        // Only a cell that no property writes uses the deterministic search (findNestedBeanOwner).
                        if (isContestedCell) {
                            if (!isBeanFallback || !getFamilyColumnFieldNameMap(fallbackPropInfo.jsonXmlType.javaType())._2.containsKey(qualifier)) {
                                final Tuple2<String, Boolean> fallbackTP = familyTP;

                                if (!isBeanFallback) {
                                    familyTP = null;
                                }

                                for (final Tuple2<String, Boolean> candidateTP : familyTPMap.values()) {
                                    final PropInfo candidatePropInfo = candidateTP == fallbackTP || candidateTP._2 ? null
                                            : entityInfo.getPropInfo(candidateTP._1);

                                    if (candidatePropInfo != null && candidatePropInfo.jsonXmlType.isBean()
                                            && getFamilyColumnFieldNameMap(candidatePropInfo.jsonXmlType.javaType())._2.containsKey(qualifier)) {
                                        familyTP = candidateTP;
                                        break;
                                    }
                                }
                            }
                        } else {
                            if (familyNestedBeanTPs == null) {
                                familyNestedBeanTPs = new HashMap<>();
                            }

                            List<Tuple2<String, Boolean>> nestedBeanTPs = familyNestedBeanTPs.get(family);

                            if (nestedBeanTPs == null) {
                                nestedBeanTPs = getNestedBeanTPs(entityInfo, familyTPMap);
                                familyNestedBeanTPs.put(family, nestedBeanTPs);
                            }

                            final Tuple2<String, Boolean> ownerTP = findNestedBeanOwner(entityInfo, nestedBeanTPs, qualifier);

                            if (ownerTP != null) {
                                familyTP = ownerTP;
                            } else if (!isBeanFallback) {
                                familyTP = null;
                            }
                        }
                    }
                }

                // ignore the unknown column:
                if (familyTP == null) {
                    continue;
                }

                fieldName = familyTP._1;
                familyPropInfo = entityInfo.getPropInfo(fieldName);

                // ignore the unknown field/property, and a read-only one (see isReadOnlyProperty), except a field-backed
                // read-only nested bean: its cells are written into the instance its field already holds (handled below),
                // which never needs the setter, as before abacus-common 8.2 made such a property read-only.
                if (familyPropInfo == null
                        || (isReadOnlyProperty(familyPropInfo) && !(familyPropInfo.field != null && familyPropInfo.jsonXmlType.isBean() && !familyTP._2))) {
                    continue;
                }

                // The row key decoded from the cell's row is authoritative; a mapped cell must not change the entity's identity.
                if (rowKeySetMethod != null && rowKeySetMethod.equals(familyPropInfo.setMethod)) {
                    continue;
                }

                if (familyPropInfo.jsonXmlType.isBean() && !familyTP._2) {
                    // EMPTY_QUALIFIER is a family-level fallback, not a nested-property name.
                    // Flattened nested properties are written with their own qualifiers.
                    if (qualifier.isEmpty()) {
                        continue;
                    }

                    final Class<?> propEntityClass = familyPropInfo.jsonXmlType.javaType();
                    final Map<String, String> propEntityColumnFieldNameMap = getFamilyColumnFieldNameMap(propEntityClass)._2;
                    final BeanInfo propBeanInfo = ParserUtil.getBeanInfo(propEntityClass);
                    columnPropInfo = propBeanInfo.getPropInfo(propEntityColumnFieldNameMap.getOrDefault(qualifier, qualifier));

                    // ignore the unknown property, and a read-only one.
                    if (columnPropInfo == null || isReadOnlyProperty(columnPropInfo)) {
                        continue;
                    }

                    // WHY: a nested bean created for this row is assigned to its property once, after the last cell (below), not
                    // on creation: a setter or builder method that copies or normalizes its argument kept a snapshot of the still
                    // empty bean, and the cells written into the created instance were lost (all of them for a builder). Later
                    // cells of the property therefore find it in nestedBeanResults, never by reading the property back. Only a
                    // mutable entity's property is read (an immutable entity's intermediate result may be a builder, which the
                    // property cannot read: ClassCastException), for a mutable nested bean it already holds, which is filled in
                    // place. AnyPut.create also flattens an immutable nested bean (a record, or a bean built through its
                    // constructor or builder), which has no no-args constructor and no setters: its cells fill its intermediate
                    // result (createBeanResult).
                    Object propEntity = findNestedBeanResult(nestedBeanResults, familyPropInfo);

                    if (propEntity == null && !entityInfo.isImmutable && !propBeanInfo.isImmutable) {
                        propEntity = familyPropInfo.getPropValue(entity);
                    }

                    if (propEntity == null) {
                        if (isReadOnlyProperty(familyPropInfo)) {
                            // no instance to fill (an immutable nested bean cannot be filled in place), and the read-only property
                            // cannot be assigned one
                            continue;
                        }

                        if (propBeanInfo.isImmutable) {
                            propEntity = propBeanInfo.createBeanResult();

                            // Only a mutable entity's property can be read (see above): the nested value an immutable entity's
                            // builder or constructor starts with is not consulted.
                            final Object existingPropEntity = entityInfo.isImmutable ? null : familyPropInfo.getPropValue(entity);

                            if (existingPropEntity != null) {
                                // A nested property without a cell keeps the value of the instance the property already holds,
                                // as it does when a mutable nested bean is filled in place.
                                for (final PropInfo nestedPropInfo : propBeanInfo.propInfoList) {
                                    nestedPropInfo.setPropValue(propEntity, nestedPropInfo.getPropValue(existingPropEntity));
                                }
                            }
                        } else {
                            propEntity = N.newInstance(propEntityClass);
                        }

                        if (nestedBeanResults == null) {
                            nestedBeanResults = new ArrayList<>(4);
                        }

                        nestedBeanResults.add(Tuple.of(familyPropInfo, propBeanInfo, propEntity));
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
                        versionedContainers = deferVersionedContainer(versionedContainers, propEntity, columnPropInfo, columnMap);

                        if (columnMapMap == null) {
                            if (familyColumnMapMap == null) {
                                familyColumnMapMap = new HashMap<>();
                            }

                            columnMapMap = new HashMap<>();
                            familyColumnMapMap.put(family, columnMapMap);
                        }

                        columnMapMap.put(qualifier, columnMap);

                        column = HBaseColumn.valueOf(getCellValue(cell, columnValueType), cell.getTimestamp());
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
                        versionedContainers = deferVersionedContainer(versionedContainers, propEntity, columnPropInfo, columnColl);

                        if (columnCollectionMap == null) {
                            if (familyColumnCollectionMap == null) {
                                familyColumnCollectionMap = new HashMap<>();
                            }

                            columnCollectionMap = new HashMap<>();
                            familyColumnCollectionMap.put(family, columnCollectionMap);
                        }

                        columnCollectionMap.put(qualifier, columnColl);

                        column = HBaseColumn.valueOf(getCellValue(cell, columnValueType), cell.getTimestamp());
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

                        if (assignedSingleVersionColumns == null) {
                            assignedSingleVersionColumns = new HashSet<>();
                        }

                        if (assignedSingleVersionColumns.add(family + '\0' + qualifier) == false) {
                            continue;
                        }

                        column = HBaseColumn.valueOf(getCellValue(cell, columnValueType), cell.getTimestamp());

                        columnPropInfo.setPropValue(propEntity, column);
                    } else {
                        if (assignedSingleVersionColumns == null) {
                            assignedSingleVersionColumns = new HashSet<>();
                        }

                        if (assignedSingleVersionColumns.add(family + '\0' + qualifier) == false) {
                            continue;
                        }

                        columnPropInfo.setPropValue(propEntity, getCellValue(cell, columnPropInfo.jsonXmlType));
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
                    versionedContainers = deferVersionedContainer(versionedContainers, entity, familyPropInfo, columnMap);

                    if (columnMapMap == null) {
                        if (familyColumnMapMap == null) {
                            familyColumnMapMap = new HashMap<>();
                        }

                        columnMapMap = new HashMap<>();
                        familyColumnMapMap.put(family, columnMapMap);
                    }

                    columnMapMap.put(qualifier, columnMap);

                    column = HBaseColumn.valueOf(getCellValue(cell, columnValueType), cell.getTimestamp());
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
                    versionedContainers = deferVersionedContainer(versionedContainers, entity, familyPropInfo, columnColl);

                    if (columnCollectionMap == null) {
                        if (familyColumnCollectionMap == null) {
                            familyColumnCollectionMap = new HashMap<>();
                        }

                        columnCollectionMap = new HashMap<>();
                        familyColumnCollectionMap.put(family, columnCollectionMap);
                    }

                    columnCollectionMap.put(qualifier, columnColl);

                    column = HBaseColumn.valueOf(getCellValue(cell, columnValueType), cell.getTimestamp());
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

                    if (assignedSingleVersionColumns == null) {
                        assignedSingleVersionColumns = new HashSet<>();
                    }

                    if (assignedSingleVersionColumns.add(family + '\0' + qualifier) == false) {
                        continue;
                    }

                    column = HBaseColumn.valueOf(getCellValue(cell, columnValueType), cell.getTimestamp());

                    familyPropInfo.setPropValue(entity, column);
                } else {
                    if (assignedSingleVersionColumns == null) {
                        assignedSingleVersionColumns = new HashSet<>();
                    }

                    if (assignedSingleVersionColumns.add(family + '\0' + qualifier) == false) {
                        continue;
                    }

                    familyPropInfo.setPropValue(entity, getCellValue(cell, familyPropInfo.jsonXmlType));
                }
            }

            if (versionedContainers != null) {
                // WHY: a versioned Collection/Map property receives its later versions (cells) after its container is created,
                // but setPropValue stores a CONVERTED copy when the container is not an instance of the declared type (an
                // ImmutableList/ImmutableSet/ImmutableMap property gets the ArrayList/HashSet/HashMap N.newCollection/newMap
                // build, also as a component of an immutable nested bean): the copy was taken while still empty, so every
                // version was lost. Each container is therefore assigned once, after the last cell, in creation order (a
                // later container for the same property still replaces an earlier one, as before), and before the nested beans
                // holding some of them are built or handed to a setter that may copy them below.
                for (final Tuple3<Object, PropInfo, Object> versionedContainer : versionedContainers) {
                    versionedContainer._2.setPropValue(versionedContainer._1, versionedContainer._3);
                }
            }

            if (nestedBeanResults != null) {
                // Assign each nested bean created for this row, now complete (see above), in creation order; an immutable one is
                // first built from the intermediate result its cells filled (finishBeanResult returns a mutable one as is).
                for (int i = 0, size = nestedBeanResults.size(); i < size; i++) {
                    final Tuple3<PropInfo, BeanInfo, Object> nestedBeanResult = nestedBeanResults.get(i);

                    nestedBeanResult._1.setPropValue(entity, nestedBeanResult._2.finishBeanResult(nestedBeanResult._3));
                }
            }

            return entityInfo.finishBeanResult(entity);
        } else {
            final CellScanner cellScanner = result.cellScanner();

            if (!cellScanner.advance()) {
                return type.defaultValue();
            }

            final Cell cell = cellScanner.current();

            final T value = (T) getCellValue(cell, type);

            if (cellScanner.advance()) {
                final Cell extraCell = cellScanner.current();
                throw new IllegalArgumentException(
                        "Cannot convert a result with more than one cell to class: " + ClassUtil.getCanonicalClassName(type.javaType())
                                + ". Found unexpected extra column: " + getFamilyString(extraCell) + ":" + getQualifierString(extraCell));
            }

            return value;
        }
    }

    /**
     * Creates a mapper whose validation and conversion failures are raised when it is applied to a result.
     *
     * @param targetType the class to convert each result to
     * @param <T> the target type
     * @return the deferred result converter
     */
    private static <T> Function<Result, T> createRowMapper(final Class<T> targetType) {
        return t -> toValue(t, targetType);
    }

    // Guard here rather than in each scan(..., Class) overload: the returned Stream is lazy, so an
    // unchecked targetType would only fail when the first row is pulled, far from the call site.
    /**
     * Adds lazy conversion to a newly created result stream after validating the requested type.
     *
     * @param results the newly created result stream
     * @param targetType the class to convert each result to
     * @param <T> the target type
     * @return the lazily mapped stream
     * @throws IllegalArgumentException if {@code targetType} is {@code null}
     */
    private static <T> Stream<T> mapResults(final Stream<Result> results, final Class<T> targetType) throws IllegalArgumentException {
        N.checkArgNotNull(targetType, cs.targetType);

        return results.filter(result -> !result.isEmpty()).map(createRowMapper(targetType));
    }

    // ByteBuffer is special-cased to mirror the write side (toValueBytes stores a ByteBuffer's raw remaining
    // bytes): reading it back through Type.valueOf(String) would base64-decode the UTF-8 string of those raw
    // bytes and silently corrupt the value.
    private static Object getRowKeyValue(final Cell cell, final Type<?> targetType) {
        if (byte[].class.equals(targetType.javaType())) {
            return copyOf(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
        } else if (ByteBuffer.class.equals(targetType.javaType())) {
            return ByteBuffer.wrap(copyOf(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
        } else {
            return targetType.valueOf(getRowKeyString(cell));
        }
    }

    private static Object getCellValue(final Cell cell, final Type<?> targetType) {
        if (byte[].class.equals(targetType.javaType())) {
            return copyOf(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
        } else if (ByteBuffer.class.equals(targetType.javaType())) {
            return ByteBuffer.wrap(copyOf(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        } else {
            return targetType.valueOf(getValueString(cell));
        }
    }

    // A getter-only (computed) property, e.g. on an @Entity class or, since abacus-common 8.1.0, inherited from an
    // @Entity superclass, is in propInfoList, so AnyPut.create writes its value as a cell; but PropInfo.setPropValue
    // throws UnsupportedOperationException for it. Its cells are skipped on read, as the JSON parser does. Use the
    // (public since abacus-common 8.2) PropInfo flag itself: since 8.2 a field-backed @AccessFieldByMethod property
    // without a setter is read-only too, which the former "no field and SERIALIZE_ONLY" approximation missed.
    // Deliberately NOT jsonXmlExpose: the JSON/XML direction is a serialization hint, not a write path. A declared
    // SERIALIZE_ONLY property with a setter is written on read (as DynamoDB item mapping does), and a read-only
    // property is skipped even when its field declares DESERIALIZE_ONLY (setPropValue would still throw).
    private static boolean isReadOnlyProperty(final PropInfo propInfo) {
        return propInfo.isReadOnlyProperty;
    }

    // The nested bean (or immutable nested bean's intermediate result) that toValue created for propInfo in the current row, or
    // null. A row has few nested beans, so a list scanned by PropInfo identity (one instance per property of the entity's
    // BeanInfo, whatever name variant reached it) is cheaper per row and per cell than a map.
    private static Object findNestedBeanResult(final List<Tuple3<PropInfo, BeanInfo, Object>> nestedBeanResults, final PropInfo propInfo) {
        if (nestedBeanResults != null) {
            for (int i = 0, size = nestedBeanResults.size(); i < size; i++) {
                final Tuple3<PropInfo, BeanInfo, Object> nestedBeanResult = nestedBeanResults.get(i);

                if (nestedBeanResult._1 == propInfo) {
                    return nestedBeanResult._3;
                }
            }
        }

        return null;
    }

    // Records a versioned Collection/Map container to be assigned to propInfo of owner after the last cell (see toValue).
    private static List<Tuple3<Object, PropInfo, Object>> deferVersionedContainer(final List<Tuple3<Object, PropInfo, Object>> versionedContainers,
            final Object owner, final PropInfo propInfo, final Object container) {
        final List<Tuple3<Object, PropInfo, Object>> result = versionedContainers == null ? new ArrayList<>() : versionedContainers;

        result.add(Tuple.of(owner, propInfo, container));

        return result;
    }

    private static byte[] copyOf(final byte[] bytes, final int offset, final int len) {
        final byte[] result = new byte[len];
        System.arraycopy(bytes, offset, result, 0, len);
        return result;
    }

    /**
     * Verifies that {@code targetType} is a JavaBean entity class with getter/setter methods.
     *
     * @param targetType the class to validate
     * @throws IllegalArgumentException if {@code targetType} is not a bean class
     */
    static void checkEntityClass(final Class<?> targetType) throws IllegalArgumentException {
        if (!Beans.isBeanClass(targetType)) {
            throw new IllegalArgumentException(
                    "Unsupported type: " + ClassUtil.getCanonicalClassName(targetType) + ". Only bean classes with getter/setter methods are supported");
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
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules
     * @throws UncheckedIOException if obtaining the table from the connection fails with an {@link IOException}
     * @see Table
     * @see org.apache.hadoop.hbase.TableName
     */
    public Table getTable(final String tableName) throws IllegalArgumentException, UncheckedIOException {
        N.checkArgNotNull(tableName, cs.tableName);

        final TableName hbaseTableName = TableName.valueOf(tableName);

        if (logger.isDebugEnabled()) {
            logger.debug("Acquiring HBase table: {}", tableName);
        }

        try {
            return conn.getTable(hbaseTableName);
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
     *   <li>Exactly one ID property (used as the row key): marked with {@code @Id}, or, when no
     *       property is annotated, a conventionally typed property named {@code id}</li>
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
     * @param targetEntityClass an entity class carrying a {@code @Table} annotation; must not be {@code null}
     * @return a cached typed mapper for the specified entity class
     * @throws IllegalArgumentException if {@code targetEntityClass} is {@code null}, is not a bean class (a {@link Map}, even an
     *         {@code @Entity}-annotated {@code Map} subclass, is not one), lacks a nonempty {@code @Table} name, or
     *         declares zero or multiple ID properties
     * @see HBaseMapper
     * @see #mapper(Class, String, NamingPolicy)
     */
    public <T, K> HBaseMapper<T, K> mapper(final Class<T> targetEntityClass) throws IllegalArgumentException {
        N.checkArgNotNull(targetEntityClass, cs.targetEntityClass);

        @SuppressWarnings("rawtypes")
        HBaseMapper mapper = mapperPool.get(targetEntityClass);

        if (mapper == null) {
            final BeanInfo entityInfo = ParserUtil.getBeanInfo(targetEntityClass);

            if (entityInfo.tableName.isEmpty()) {
                throw new IllegalArgumentException("Entity class " + ClassUtil.getCanonicalClassName(targetEntityClass)
                        + " must be annotated with @Table (com.landawn.abacus.annotation, javax.persistence, or jakarta.persistence). Alternatively, use HBaseExecutor.mapper(Class<T> targetEntityClass, String tableName, NamingPolicy namingPolicy)");
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
     * A {@code null} {@code namingPolicy} is treated as {@link NamingPolicy#CAMEL_CASE}.
     * The policy is applied when entities are written ({@link HBaseMapper#put(Object)} and
     * {@link HBaseMapper#put(Collection)}); when reading, cells named under any supported
     * naming policy are recognized, so a mapper can read rows written with a different policy. A name that
     * several properties produce under different policies is ambiguous: a property's own name or explicit
     * {@code @ColumnFamily}/{@code @Column} name wins over another property's policy variant of it. A cell that only one
     * property writes, under any policy, is read back into that property unless its name is also mapped to another property
     * of that family; a cell that several properties can write reads back into only one of them (see
     * {@link HBaseExecutor#toEntity(Result, Class)}).</p>
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
     * @param targetEntityClass the entity class to map (must be a JavaBean class with exactly one ID property: an {@code @Id} property or,
     *        when no property is annotated, a conventionally typed property named {@code id}); must not be {@code null}
     * @param tableName the HBase table name to bind the mapper to; must not be {@code null} or empty
     * @param namingPolicy the naming policy for column name conversion; {@code null} maps to {@link NamingPolicy#CAMEL_CASE}
     * @return a configured (non-cached) typed mapper for the specified entity class and table
     * @throws IllegalArgumentException if {@code targetEntityClass} is {@code null} or is not a bean
     *         class (a {@link Map}, even an {@code @Entity}-annotated {@code Map} subclass, is not one), has no ID property, or has more
     *         than one ID property; or if
     *         {@code tableName} is {@code null} or empty
     * @see NamingPolicy
     * @see HBaseMapper
     */
    public <T, K> HBaseMapper<T, K> mapper(final Class<T> targetEntityClass, final String tableName, final NamingPolicy namingPolicy)
            throws IllegalArgumentException {
        return new HBaseMapper<>(targetEntityClass, this, tableName, namingPolicy);
    }

    private static void closeQuietly(final Table table) {
        IOUtil.closeQuietly(table);
    }

    // There is no too much benefit to add method for "Object rowKey"
    // And it may cause error because the "Object" is ambiguous to any type.
    /**
     * Tests whether the row identified by {@code rowKey} has any cells.
     *
     * <p>Package-private convenience overload delegating to {@link #exists(String, AnyGet)};
     * {@code rowKey} is converted to bytes via {@link #toRowKeyBytes(Object)}.</p>
     *
     * @param tableName the name of the HBase table
     * @param rowKey the row key to check
     * @return {@code true} if the row has one or more cells, {@code false} otherwise
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code rowKey} is {@code null}, an HBase operation or {@code Any*} wrapper (see {@link #toRowKeyBytes(Object)}) or an
     *         empty value wrapper such as {@code Optional.empty()} (which has no byte representation), or its byte representation is empty
     *         or exceeds 32,767 bytes
     * @throws RuntimeException if converting {@code rowKey} to bytes invokes a failing string conversion
     * @throws UncheckedIOException if acquiring the table or checking row existence fails with an {@link IOException}
     */
    boolean exists(final String tableName, final Object rowKey) throws IllegalArgumentException, RuntimeException, UncheckedIOException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

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
     * @param get the Get operation to test for existence; must not be {@code null}
     * @return {@code true} if the Get operation would return results, {@code false} otherwise
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code get} is {@code null}
     * @throws UncheckedIOException if acquiring the table or checking row existence fails with an {@link IOException}
     * @see Get
     */
    public boolean exists(final String tableName, final Get get) throws IllegalArgumentException, UncheckedIOException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);
        N.checkArgNotNull(get, cs.get);

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
     * without transferring cell values to the client, making it efficient for checking existence
     * of multiple rows or cells. An empty list returns immediately without acquiring a table
     * handle.</p>
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
     * @param gets the list of Get operations to test for existence; must not be {@code null}
     * @return a list of Boolean values in the same order as {@code gets}, where the i-th entry
     *         is {@code true} if the i-th Get would match one or more cells, {@code false}
     *         otherwise; an empty input produces an empty list
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code gets} is {@code null}
     * @throws UncheckedIOException if acquiring the table or checking row existence fails with an {@link IOException}
     * @throws NullPointerException if the HBase client dereferences a null operation in {@code gets}
     * @see Get
     */
    public List<Boolean> exists(final String tableName, final List<Get> gets) throws IllegalArgumentException, UncheckedIOException, NullPointerException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);
        N.checkArgNotNull(gets, cs.gets);

        if (gets.isEmpty()) {
            return new ArrayList<>();
        }

        final Table table = getTable(tableName);

        try {
            return BooleanList.of(table.exists(gets)).boxed();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Tests whether the specified AnyGet operation would return any results.
     *
     * <p>This is a convenience wrapper around {@link #exists(String, Get)} that accepts
     * an AnyGet instance, which provides a fluent API for building Get operations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * boolean exists = executor.exists("users", AnyGet.of("user123"));   // true if user123 has any cell
     *
     * // Narrow the existence check to a single column
     * boolean hasEmail = executor.exists("users", AnyGet.of("user123").addColumn("info", "email"));
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param anyGet the AnyGet operation to test for existence; must not be {@code null}
     * @return {@code true} if the Get operation would return results, {@code false} otherwise
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code anyGet} is {@code null}
     * @throws UncheckedIOException if acquiring the table or checking row existence fails with an {@link IOException}
     * @see AnyGet
     * @see #exists(String, Get)
     */
    public boolean exists(final String tableName, final AnyGet anyGet) throws IllegalArgumentException, UncheckedIOException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

        N.checkArgNotNull(anyGet, cs.anyGet);

        return exists(tableName, anyGet.val());
    }

    /**
     * Tests whether the specified AnyGet operations would return any results.
     *
     * <p>This is a convenience wrapper around {@link #exists(String, List)} that accepts
     * a collection of AnyGet instances for batch existence checking.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Boolean> results = executor.exists("users",
     *         N.asList(AnyGet.of("user123"), AnyGet.of("user456")));
     * // results.get(0) == true if user123 exists; results.get(1) == true if user456 exists
     *
     * // An empty collection yields an empty result list (no server round trip needed)
     * List<Boolean> empty = executor.exists("users", N.<AnyGet> emptyList());   // returns [] (empty list)
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param anyGets the collection of AnyGet operations to test for existence; must not be
     *        {@code null} and must not contain {@code null} elements
     * @return a list of Boolean values corresponding to each AnyGet operation
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code anyGets} is {@code null} or contains a {@code null} element
     * @throws UncheckedIOException if acquiring the table or checking row existence fails with an {@link IOException}
     * @see AnyGet
     * @see #exists(String, List)
     */
    public List<Boolean> exists(final String tableName, final Collection<AnyGet> anyGets) throws IllegalArgumentException, UncheckedIOException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

        return exists(tableName, AnyGet.toGet(anyGets));
    }

    // There is no too much benefit to add method for "Object rowKey"
    // And it may cause error because the "Object" is ambiguous to any type.
    /**
     * Retrieves the row identified by {@code rowKey}.
     *
     * <p>Package-private convenience overload delegating to {@link #get(String, AnyGet)};
     * {@code rowKey} is converted to bytes via {@link #toRowKeyBytes(Object)}.</p>
     *
     * @param tableName the name of the HBase table
     * @param rowKey the row key of the row to retrieve
     * @return the HBase Result containing the retrieved data, or a present-but-{@linkplain Result#isEmpty() empty} Result if the row does not exist
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code rowKey} is {@code null}, an HBase operation or {@code Any*} wrapper (see {@link #toRowKeyBytes(Object)}) or an
     *         empty value wrapper such as {@code Optional.empty()} (which has no byte representation), or its byte representation is empty
     *         or exceeds 32,767 bytes
     * @throws RuntimeException if converting {@code rowKey} to bytes invokes a failing string conversion
     * @throws UncheckedIOException if acquiring the table, fetching rows, or reading result cells fails with an {@link IOException}
     */
    Result get(final String tableName, final Object rowKey) throws IllegalArgumentException, RuntimeException, UncheckedIOException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

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
     * @param get the Get operation specifying what data to retrieve; must not be {@code null}
     * @return the HBase Result containing the retrieved data, or empty Result if no data found
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code get} is {@code null}
     * @throws UncheckedIOException if acquiring the table, fetching rows, or reading result cells fails with an {@link IOException}
     * @see Get
     * @see Result
     */
    public Result get(final String tableName, final Get get) throws IllegalArgumentException, UncheckedIOException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);
        N.checkArgNotNull(get, cs.get);

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
     * as the input Get operations. An empty list returns immediately without acquiring a table
     * handle.</p>
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
     * @param gets the list of Get operations to execute; must not be {@code null}
     * @return a list of Results in the same order as {@code gets}; entries for rows that
     *         do not exist are present in the list but are {@linkplain Result#isEmpty() empty};
     *         an empty input produces an empty list
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code gets} is {@code null}
     * @throws UncheckedIOException if acquiring the table, fetching rows, or reading result cells fails with an {@link IOException}
     * @throws NullPointerException if the HBase client dereferences a null operation in {@code gets}
     * @see Get
     * @see Result
     */
    public List<Result> get(final String tableName, final List<Get> gets) throws IllegalArgumentException, UncheckedIOException, NullPointerException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);
        N.checkArgNotNull(gets, cs.gets);

        if (gets.isEmpty()) {
            return new ArrayList<>();
        }

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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Result result = executor.get("users", AnyGet.of("user123"));
     * if (!result.isEmpty()) {
     *     // process the cells of the row
     * }
     *
     * // A non-existent row yields a present-but-empty Result (never null)
     * Result missing = executor.get("users", AnyGet.of("no_such_row"));   // missing.isEmpty() == true
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param anyGet the AnyGet operation specifying what data to retrieve; must not be {@code null}
     * @return the HBase Result containing the retrieved data, or a present-but-{@linkplain Result#isEmpty() empty} Result if the row does not exist
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code anyGet} is {@code null}
     * @throws UncheckedIOException if acquiring the table, fetching rows, or reading result cells fails with an {@link IOException}
     * @see AnyGet
     * @see #get(String, Get)
     */
    public Result get(final String tableName, final AnyGet anyGet) throws IllegalArgumentException, UncheckedIOException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

        N.checkArgNotNull(anyGet, cs.anyGet);

        return get(tableName, anyGet.val());
    }

    /**
     * Retrieves multiple rows of data from HBase using a collection of AnyGet operations.
     *
     * <p>This is a convenience wrapper around {@link #get(String, List)} that accepts
     * a collection of AnyGet instances for batch retrieval.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Result> results = executor.get("users",
     *         N.asList(AnyGet.of("user123"), AnyGet.of("user456")));
     * // results is positionally aligned with the input; entries for missing rows are empty Results
     *
     * // An empty collection yields an empty result list
     * List<Result> empty = executor.get("users", N.<AnyGet> emptyList());   // returns [] (empty list)
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param anyGets the collection of AnyGet operations to execute; must not be {@code null} and
     *        must not contain {@code null} elements
     * @return a list of Results positionally aligned with {@code anyGets}; entries for rows that
     *         do not exist are present in the list but are {@linkplain Result#isEmpty() empty}
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code anyGets} is {@code null} or contains a {@code null} element
     * @throws UncheckedIOException if acquiring the table, fetching rows, or reading result cells fails with an {@link IOException}
     * @see AnyGet
     * @see #get(String, List)
     */
    public List<Result> get(final String tableName, final Collection<AnyGet> anyGets) throws IllegalArgumentException, UncheckedIOException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

        return get(tableName, AnyGet.toGet(anyGets));
    }

    // There is no too much benefit to add method for "Object rowKey"
    // And it may cause error because the "Object" is ambiguous to any type.
    /**
     * Retrieves the row identified by {@code rowKey} and converts it to {@code targetType}.
     *
     * <p>Package-private convenience overload delegating to
     * {@link #get(String, AnyGet, Class)}; {@code rowKey} is converted to bytes via
     * {@link #toRowKeyBytes(Object)}.</p>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table
     * @param rowKey the row key of the row to retrieve
     * @param targetType the class to convert the result to
     * @return the converted object of the specified type, or the type's default value if the row does not exist
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code rowKey} is {@code null}, an HBase operation or {@code Any*} wrapper (see {@link #toRowKeyBytes(Object)}) or an
     *         empty value wrapper such as {@code Optional.empty()} (which has no byte representation), or its byte representation is empty
     *         or exceeds 32,767 bytes; also if {@code targetType} is
     *         {@code null}, a result is requested as a {@link Map}, a scalar result contains more than one cell, or the target bean has invalid
     *         row-key metadata
     * @throws RuntimeException if converting a cell or row key, creating a target bean, or reading or assigning one of its properties fails
     * @throws UncheckedIOException if acquiring the table, fetching rows, or reading result cells fails with an {@link IOException}
     */
    <T> T get(final String tableName, final Object rowKey, final Class<T> targetType) throws IllegalArgumentException, RuntimeException, UncheckedIOException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

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
     *
     * // A single-value target needs a Result with at most one cell, so select exactly one column
     * Get nameGet = new Get(Bytes.toBytes("user123")).addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
     * String userName = executor.get("users", nameGet, String.class);   // the info:name cell value
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table
     * @param get the Get operation specifying what data to retrieve; must not be {@code null}
     * @param targetType the class to convert the result to
     * @return the converted object of the specified type, or null/default if no data found
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code get} or {@code targetType} is {@code null}, a result is requested as a {@link Map}, a scalar result contains more
     *         than one cell, or the target bean has invalid row-key metadata
     * @throws UncheckedIOException if acquiring the table, fetching rows, or reading result cells fails with an {@link IOException}
     * @throws RuntimeException if converting a cell or row key, creating a target bean, or reading or assigning one of its properties fails
     * @see #get(String, Get)
     * @see #toEntity(Result, Class)
     */
    public <T> T get(final String tableName, final Get get, final Class<T> targetType) throws IllegalArgumentException, UncheckedIOException, RuntimeException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

        N.checkArgNotNull(get, cs.get);
        N.checkArgNotNull(targetType, cs.targetType);

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
     * @param gets the list of Get operations to execute; must not be {@code null}
     * @param targetType the class to convert each result to
     * @return a list of converted objects of the specified type. Empty results (rows that
     *         did not exist) are skipped, so the returned list may contain fewer elements
     *         than {@code gets} and does not necessarily correspond positionally to it.
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code gets} or {@code targetType} is {@code null}, a nonempty result is requested as a {@link Map}, a scalar result
     *         contains more than one cell, or the target bean has invalid row-key metadata
     * @throws UncheckedIOException if acquiring the table, fetching rows, or reading result cells fails with an {@link IOException}
     * @throws NullPointerException if the HBase client dereferences a null operation in {@code gets}
     * @throws RuntimeException if converting a cell or row key, creating a target bean, or reading or assigning one of its properties fails
     * @see #get(String, List)
     * @see #toEntity(Result, Class)
     */
    public <T> List<T> get(final String tableName, final List<Get> gets, final Class<T> targetType)
            throws IllegalArgumentException, UncheckedIOException, NullPointerException, RuntimeException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

        N.checkArgNotNull(gets, cs.gets);
        N.checkArgNotNull(targetType, cs.targetType);

        return toList(get(tableName, gets), targetType);
    }

    /**
     * Retrieves and converts HBase data to the specified target type using an AnyGet operation.
     *
     * <p>This is a convenience wrapper that combines {@link #get(String, AnyGet)} with
     * automatic type conversion.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = executor.get("users", AnyGet.of("user123"), User.class);   // null if the row is missing
     *
     * // A non-existent row converts to the target type's default value (null for a bean)
     * User missing = executor.get("users", AnyGet.of("no_such_row"), User.class);   // returns null
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table
     * @param anyGet the AnyGet operation specifying what data to retrieve; must not be {@code null}
     * @param targetType the class to convert the result to; must not be {@code null}
     * @return the converted object of the specified type
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code anyGet} is {@code null}; also if {@code targetType} is {@code null}, a result is requested as a {@link Map}, a scalar
     *         result contains more than one cell, or the target bean has invalid row-key metadata
     * @throws UncheckedIOException if acquiring the table, fetching rows, or reading result cells fails with an {@link IOException}
     * @throws RuntimeException if converting a cell or row key, creating a target bean, or reading or assigning one of its properties fails
     * @see AnyGet
     * @see #get(String, Get, Class)
     */
    public <T> T get(final String tableName, final AnyGet anyGet, final Class<T> targetType)
            throws IllegalArgumentException, UncheckedIOException, RuntimeException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

        N.checkArgNotNull(anyGet, cs.anyGet);
        N.checkArgNotNull(targetType, cs.targetType);

        return toValue(get(tableName, anyGet), targetType);
    }

    /**
     * Retrieves multiple rows using AnyGet operations and converts them to the specified target type.
     *
     * <p>This is a convenience wrapper that combines {@link #get(String, Collection)} with
     * automatic batch type conversion.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<User> users = executor.get("users",
     *         N.asList(AnyGet.of("user123"), AnyGet.of("user456")), User.class);
     * // rows that do not exist are skipped, so users may be shorter than the input
     *
     * // An empty collection yields an empty result list
     * List<User> empty = executor.get("users", N.<AnyGet> emptyList(), User.class);   // returns [] (empty list)
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table
     * @param anyGets the collection of AnyGet operations to execute; must not be {@code null} and
     *        must not contain {@code null} elements
     * @param targetType the class to convert each result to; must not be {@code null}
     * @return a list of converted objects of the specified type. Empty results (rows that
     *         did not exist) are skipped, so the returned list may contain fewer elements
     *         than {@code anyGets} and does not necessarily correspond positionally to it.
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code anyGets} is {@code null} or contains a {@code null} element; also if {@code targetType} is {@code null}, a nonempty
     *         result is requested as a {@link Map}, a scalar result contains more than one cell, or the target bean has invalid row-key metadata
     * @throws UncheckedIOException if acquiring the table, fetching rows, or reading result cells fails with an {@link IOException}
     * @throws RuntimeException if converting a cell or row key, creating a target bean, or reading or assigning one of its properties fails
     * @see AnyGet
     * @see #get(String, List, Class)
     */
    public <T> List<T> get(final String tableName, final Collection<AnyGet> anyGets, final Class<T> targetType)
            throws IllegalArgumentException, UncheckedIOException, RuntimeException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

        N.checkArgNotNull(anyGets, cs.anyGets);
        for (final AnyGet anyGet : anyGets) {
            N.checkArgNotNull(anyGet, cs.anyGet);
        }
        N.checkArgNotNull(targetType, cs.targetType);

        return toList(get(tableName, anyGets), targetType);
    }

    /**
     * Performs a scan operation on the specified column family and returns a stream of Results.
     *
     * <p>This convenience method creates a Scan operation that retrieves all columns from
     * the specified column family across all rows in the table.</p>
     *
     * <p>The returned stream is lazy and owns the underlying table/scanner; always close it
     * (a try-with-resources block) to avoid leaking the table connection.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * try (Stream<Result> stream = executor.scan("users", "info")) {
     *     long count = stream.count();   // number of rows that have at least one cell in family "info"
     * }
     * }</pre>
     *
     * @param tableName the name of the HBase table to scan
     * @param family the name of the column family to scan; a {@code null} family is accepted here, but HBase cannot serialize it, so consuming the stream fails
     * @return a lazy stream of HBase Results from the scan operation
     * @throws IllegalArgumentException if {@code tableName} is {@code null}
     * @see #scan(String, Scan)
     */
    public Stream<Result> scan(final String tableName, final String family) throws IllegalArgumentException {
        N.checkArgNotNull(tableName, cs.tableName);

        return scan(tableName, AnyScan.create().addFamily(family));
    }

    /**
     * Performs a scan operation on a specific column and returns a stream of Results.
     *
     * <p>This convenience method creates a Scan operation that retrieves only the specified
     * column (family:qualifier) across all rows in the table.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Each emitted Result carries only the info:email column
     * try (Stream<Result> stream = executor.scan("users", "info", "email")) {
     *     List<Result> rows = stream.toList();
     * }
     * }</pre>
     *
     * @param tableName the name of the HBase table to scan
     * @param family the name of the column family; a {@code null} family is accepted here, but HBase cannot serialize it, so consuming the stream fails
     * @param qualifier the column qualifier
     * @return a lazy stream of HBase Results from the scan operation
     * @throws IllegalArgumentException if {@code tableName} is {@code null}
     * @see #scan(String, Scan)
     */
    public Stream<Result> scan(final String tableName, final String family, final String qualifier) throws IllegalArgumentException {
        N.checkArgNotNull(tableName, cs.tableName);

        return scan(tableName, AnyScan.create().addColumn(family, qualifier));
    }

    /**
     * Performs a scan operation on the specified column family (byte array) and returns a stream of Results.
     *
     * <p>This is the byte array version of {@link #scan(String, String)}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * try (Stream<Result> stream = executor.scan("users", Bytes.toBytes("info"))) {
     *     long count = stream.count();
     * }
     * }</pre>
     *
     * @param tableName the name of the HBase table to scan
     * @param family the column family as a byte array; a {@code null} family is accepted here, but HBase cannot serialize it, so consuming the stream fails
     * @return a lazy stream of HBase Results from the scan operation
     * @throws IllegalArgumentException if {@code tableName} is {@code null}
     * @see #scan(String, Scan)
     */
    public Stream<Result> scan(final String tableName, final byte[] family) throws IllegalArgumentException {
        N.checkArgNotNull(tableName, cs.tableName);

        return scan(tableName, AnyScan.create().addFamily(family));
    }

    /**
     * Performs a scan operation on a specific column (byte arrays) and returns a stream of Results.
     *
     * <p>This is the byte array version of {@link #scan(String, String, String)}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * try (Stream<Result> stream = executor.scan("users",
     *         Bytes.toBytes("info"), Bytes.toBytes("email"))) {
     *     List<Result> rows = stream.toList();
     * }
     * }</pre>
     *
     * @param tableName the name of the HBase table to scan
     * @param family the column family as a byte array; a {@code null} family is accepted here, but HBase cannot serialize it, so consuming the stream fails
     * @param qualifier the column qualifier as a byte array
     * @return a lazy stream of HBase Results from the scan operation
     * @throws IllegalArgumentException if {@code tableName} is {@code null}
     * @see #scan(String, Scan)
     */
    public Stream<Result> scan(final String tableName, final byte[] family, final byte[] qualifier) throws IllegalArgumentException {
        N.checkArgNotNull(tableName, cs.tableName);

        return scan(tableName, AnyScan.create().addColumn(family, qualifier));
    }

    /**
     * Performs a scan operation using an AnyScan specification and returns a stream of Results.
     *
     * <p>This is a convenience wrapper around {@link #scan(String, Scan)} that accepts
     * an AnyScan instance, which provides a fluent API for building Scan operations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan anyScan = AnyScan.create()
     *         .addFamily("info")
     *         .withStartRow("user_")
     *         .withStopRow("user_zzz");
     *
     * try (Stream<Result> stream = executor.scan("users", anyScan)) {
     *     stream.limit(100).forEach(System.out::println);
     * }
     * }</pre>
     *
     * @param tableName the name of the HBase table to scan
     * @param anyScan the AnyScan operation defining the scan parameters; must not be {@code null}
     * @return a lazy stream of HBase Results from the scan operation
     * @throws IllegalArgumentException if {@code tableName} or {@code anyScan} is {@code null}
     * @see AnyScan
     * @see #scan(String, Scan)
     */
    public Stream<Result> scan(final String tableName, final AnyScan anyScan) throws IllegalArgumentException {
        N.checkArgNotNull(tableName, cs.tableName);

        N.checkArgNotNull(anyScan, cs.anyScan);

        return scan(tableName, anyScan.val());
    }

    /**
     * Performs a scan against the specified HBase table and returns a lazy {@link Stream}
     * of {@link Result}s.
     *
     * <p>The returned stream is deferred: the underlying {@link Table} and
     * {@link ResultScanner} are opened only when iteration begins, and both are closed
     * when the stream is closed (automatically once a terminal operation such as {@code toList()}
     * or {@code forEach(...)} completes, or by calling {@link Stream#close()} / exiting a
     * try-with-resources block; exhausting the stream's {@code iterator()} does not close it).</p>
     *
     * <p><strong>Resource ownership:</strong> the stream owns the table and scanner it
     * opens. Always consume scan streams inside a try-with-resources block, or call
     * {@link Stream#close()} explicitly, to avoid leaking the table connection.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Scan scan = new Scan();
     * scan.addFamily(Bytes.toBytes("info"));
     * scan.withStartRow(Bytes.toBytes("user_"));
     * scan.withStopRow(Bytes.toBytes("user_zzz"));
     *
     * try (Stream<Result> stream = executor.scan("users", scan)) {
     *     stream.limit(100)
     *           .filter(result -> !result.isEmpty())
     *           .forEach(System.out::println);
     * }
     * }</pre>
     *
     * <p>Table and scanner acquisition is deferred until the stream is consumed, so {@code tableName}
     * is not validated eagerly: an empty or malformed table name surfaces as
     * {@link IllegalArgumentException} from the first terminal operation. Failures opening
     * either resource then propagate as {@link UncheckedIOException}; scanner iteration may
     * propagate HBase's runtime wrappers for read failures.</p>
     *
     * @param tableName the name of the HBase table to scan
     * @param scan the Scan operation defining the scan parameters
     * @return a lazy, closable {@link Stream} of HBase {@link Result}s
     * @throws IllegalArgumentException if {@code tableName} or {@code scan} is {@code null}
     * @see Scan
     * @see Result
     * @see Stream
     */
    public Stream<Result> scan(final String tableName, final Scan scan) throws IllegalArgumentException {
        N.checkArgNotNull(tableName, cs.tableName);
        N.checkArgNotNull(scan, cs.scan);

        final ObjIteratorEx<Result> lazyIter = ObjIteratorEx.defer(new Supplier<ObjIteratorEx<Result>>() {
            private ObjIteratorEx<Result> internalIter = null;

            /**
             * Acquires the table and scanner on first traversal.
             *
             * @return the scanner-backed iterator
             * @throws IllegalArgumentException if the deferred table name violates HBase's naming rules
             * @throws UncheckedIOException if acquiring the table or opening its scanner fails with an {@link IOException}
             */
            @Override
            public ObjIteratorEx<Result> get() throws IllegalArgumentException, UncheckedIOException {
                if (internalIter == null) {
                    final Table table = getTable(tableName);
                    ResultScanner scannerToClose = null;

                    try {
                        final ResultScanner resultScanner = table.getScanner(scan);
                        scannerToClose = resultScanner;
                        final Iterator<Result> iter = resultScanner.iterator();

                        internalIter = new ObjIteratorEx<>() {
                            /**
                             * Fetches a row if the scanner has no prefetched row.
                             *
                             * @return whether a row is available
                             * @throws RuntimeException if the HBase iterator wraps an {@link IOException} while fetching a row
                             */
                            @Override
                            public boolean hasNext() throws RuntimeException {
                                return iter.hasNext();
                            }

                            /**
                             * Retrieves the next row using the scanner iterator's exhaustion behavior.
                             *
                             * @return the next result, or the scanner iterator's exhaustion value
                             * @throws RuntimeException if the HBase iterator wraps an {@link IOException} while fetching a row
                             */
                            @Override
                            public Result next() throws RuntimeException {
                                return iter.next();
                            }

                            @Override
                            public void closeResource() {
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
                            try {
                                IOUtil.closeQuietly(scannerToClose);
                            } finally {
                                IOUtil.closeQuietly(table);
                            }
                        }
                    }
                }

                return internalIter;
            }
        });

        return Stream.of(lazyIter).onClose(lazyIter::closeResource);
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
     * @param family the name of the column family to scan; a {@code null} family is accepted here, but HBase cannot serialize it, so consuming the stream fails
     * @param targetType the class to convert each result to; must not be {@code null}
     * @return a lazy stream of converted objects
     * @throws IllegalArgumentException if {@code tableName} or {@code targetType} is {@code null}
     * @see #scan(String, String)
     */
    public <T> Stream<T> scan(final String tableName, final String family, final Class<T> targetType) throws IllegalArgumentException {
        N.checkArgNotNull(tableName, cs.tableName);
        N.checkArgNotNull(targetType, cs.targetType);

        //noinspection resource
        return mapResults(scan(tableName, family), targetType);
    }

    /**
     * Scans a specific column and converts results to the specified target type.
     *
     * <p>This method scans only the specified column (family:qualifier) and converts
     * each result to the target type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * try (Stream<String> emails = executor.scan("users", "info", "email", String.class)) {
     *     List<String> all = emails.toList();
     * }
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to scan
     * @param family the name of the column family; a {@code null} family is accepted here, but HBase cannot serialize it, so consuming the stream fails
     * @param qualifier the column qualifier
     * @param targetType the class to convert each result to; must not be {@code null}
     * @return a lazy stream of converted objects
     * @throws IllegalArgumentException if {@code tableName} or {@code targetType} is {@code null}
     * @see #scan(String, String, String)
     */
    public <T> Stream<T> scan(final String tableName, final String family, final String qualifier, final Class<T> targetType) throws IllegalArgumentException {
        N.checkArgNotNull(tableName, cs.tableName);
        N.checkArgNotNull(targetType, cs.targetType);

        //noinspection resource
        return mapResults(scan(tableName, family, qualifier), targetType);
    }

    /**
     * Scans a column family (byte array) and converts results to the specified target type.
     *
     * <p>This is the byte array version of {@link #scan(String, String, Class)}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * try (Stream<User> users = executor.scan("users", Bytes.toBytes("info"), User.class)) {
     *     long count = users.count();
     * }
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to scan
     * @param family the column family as a byte array; a {@code null} family is accepted here, but HBase cannot serialize it, so consuming the stream fails
     * @param targetType the class to convert each result to; must not be {@code null}
     * @return a lazy stream of converted objects
     * @throws IllegalArgumentException if {@code tableName} or {@code targetType} is {@code null}
     * @see #scan(String, byte[])
     */
    public <T> Stream<T> scan(final String tableName, final byte[] family, final Class<T> targetType) throws IllegalArgumentException {
        N.checkArgNotNull(tableName, cs.tableName);
        N.checkArgNotNull(targetType, cs.targetType);

        //noinspection resource
        return mapResults(scan(tableName, family), targetType);
    }

    /**
     * Scans a specific column (byte arrays) and converts results to the specified target type.
     *
     * <p>This is the byte array version of {@link #scan(String, String, String, Class)}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * try (Stream<String> emails = executor.scan("users",
     *         Bytes.toBytes("info"), Bytes.toBytes("email"), String.class)) {
     *     List<String> all = emails.toList();
     * }
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to scan
     * @param family the column family as a byte array; a {@code null} family is accepted here, but HBase cannot serialize it, so consuming the stream fails
     * @param qualifier the column qualifier as a byte array
     * @param targetType the class to convert each result to; must not be {@code null}
     * @return a lazy stream of converted objects
     * @throws IllegalArgumentException if {@code tableName} or {@code targetType} is {@code null}
     * @see #scan(String, byte[], byte[])
     */
    public <T> Stream<T> scan(final String tableName, final byte[] family, final byte[] qualifier, final Class<T> targetType) throws IllegalArgumentException {
        N.checkArgNotNull(tableName, cs.tableName);
        N.checkArgNotNull(targetType, cs.targetType);

        //noinspection resource
        return mapResults(scan(tableName, family, qualifier), targetType);
    }

    /**
     * Scans using AnyScan specification and converts results to the specified target type.
     *
     * <p>This method combines the fluent AnyScan API with automatic type conversion. Empty
     * results, including cursor-only progress notifications, are omitted. If partial results or
     * batching are enabled on the wrapped scan, each partial {@link Result} is converted
     * independently; use {@link #scan(String, AnyScan)} and assemble complete results first when
     * one complete entity per row is required.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyScan anyScan = AnyScan.create().withStartRow("user_a").withStopRow("user_z");
     *
     * try (Stream<User> users = executor.scan("users", anyScan, User.class)) {
     *     List<User> page = users.limit(100).toList();
     * }
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to scan
     * @param anyScan the AnyScan operation defining the scan parameters; must not be {@code null}
     * @param targetType the class to convert each result to; must not be {@code null}
     * @return a lazy stream of converted objects
     * @throws IllegalArgumentException if {@code tableName}, {@code anyScan} or {@code targetType} is {@code null}
     * @see AnyScan
     * @see #scan(String, AnyScan)
     */
    public <T> Stream<T> scan(final String tableName, final AnyScan anyScan, final Class<T> targetType) throws IllegalArgumentException {
        N.checkArgNotNull(tableName, cs.tableName);
        N.checkArgNotNull(anyScan, cs.anyScan);
        N.checkArgNotNull(targetType, cs.targetType);

        //noinspection resource
        return mapResults(scan(tableName, anyScan), targetType);
    }

    /**
     * Scans using a Scan operation and converts results to the specified target type.
     *
     * <p>This method combines scanning with automatic type conversion, providing a stream
     * of typed objects that can be processed using stream operations. Empty results, including
     * cursor-only progress notifications requested through
     * {@link Scan#setNeedCursorResult(boolean)}, are omitted; use the raw
     * {@link #scan(String, Scan)} overload when cursor updates are needed. Partial results and
     * per-row batches are converted independently rather than combined, so callers requiring one
     * complete entity per row should assemble complete results before conversion.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Scan scan = new Scan();
     * scan.withStartRow(Bytes.toBytes("user_a"));
     * scan.withStopRow(Bytes.toBytes("user_z"));
     *
     * try (Stream<User> users = executor.scan("users", scan, User.class)) {
     *     List<User> page = users.limit(100).toList();
     * }
     * }</pre>
     *
     * @param <T> the target type for conversion
     * @param tableName the name of the HBase table to scan
     * @param scan the Scan operation defining the scan parameters; must not be {@code null}
     * @param targetType the class to convert each result to; must not be {@code null}
     * @return a lazy stream of converted objects
     * @throws IllegalArgumentException if {@code tableName}, {@code scan} or {@code targetType} is {@code null}
     * @see Scan
     * @see #scan(String, Scan)
     */
    public <T> Stream<T> scan(final String tableName, final Scan scan, final Class<T> targetType) throws IllegalArgumentException {
        N.checkArgNotNull(tableName, cs.tableName);
        N.checkArgNotNull(scan, cs.scan);
        N.checkArgNotNull(targetType, cs.targetType);

        //noinspection resource
        return mapResults(scan(tableName, scan), targetType);
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
     * @param put the Put operation containing the data to store; must not be {@code null}
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code put} is {@code null}; also if HBase rejects an empty mutation or a cell exceeds the configured maximum KeyValue size
     * @throws UncheckedIOException if acquiring the table or writing the supplied mutations fails with an {@link IOException}
     * @see Put
     */
    public void put(final String tableName, final Put put) throws IllegalArgumentException, UncheckedIOException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);
        N.checkArgNotNull(put, cs.put);

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
     * <p>This method submits multiple Put operations in one client batch call, which is more
     * efficient than invoking this executor once per put. The HBase client may partition that
     * batch into multiple server requests based on region placement. An empty list is a no-op and
     * does not acquire a table handle.</p>
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
     * @param puts the list of Put operations to execute; must not be {@code null}
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code puts} is {@code null}; also if HBase rejects an empty mutation or a cell exceeds the configured maximum KeyValue size
     * @throws UncheckedIOException if acquiring the table or writing the supplied mutations fails with an {@link IOException}
     * @throws NullPointerException if the HBase client dereferences a null operation in {@code puts}
     * @see Put
     */
    public void put(final String tableName, final List<Put> puts) throws IllegalArgumentException, UncheckedIOException, NullPointerException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);
        N.checkArgNotNull(puts, cs.puts);

        if (puts.isEmpty()) {
            return;
        }

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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * executor.put("users", AnyPut.of("user123")
     *         .addColumn("info", "name", "John Doe")
     *         .addColumn("info", "email", "john@example.com"));   // void; row stored after the call
     *
     * // An AnyPut created from an entity maps its @Id field to the row key
     * executor.put("users", AnyPut.create(user));   // void
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param anyPut the AnyPut operation containing the data to store; must not be {@code null}
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code anyPut} is {@code null}; also if HBase rejects a mutation with no cells or a cell exceeds the configured maximum KeyValue
     *         size
     * @throws UncheckedIOException if acquiring the table or writing the supplied mutations fails with an {@link IOException}
     * @see AnyPut
     * @see #put(String, Put)
     */
    public void put(final String tableName, final AnyPut anyPut) throws IllegalArgumentException, UncheckedIOException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

        N.checkArgNotNull(anyPut, cs.anyPut);

        put(tableName, anyPut.val());
    }

    /**
     * Stores multiple rows of data in HBase using a collection of AnyPut operations.
     *
     * <p>This is a convenience wrapper around {@link #put(String, List)} that accepts
     * a collection of AnyPut instances for batch storage.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * executor.put("users", N.asList(
     *         AnyPut.of("user123").addColumn("info", "name", "John"),
     *         AnyPut.of("user456").addColumn("info", "name", "Jane")));   // void; both rows stored
     *
     * // An empty collection is a no-op
     * executor.put("users", N.<AnyPut> emptyList());   // void; nothing written
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param anyPuts the collection of AnyPut operations to execute; must not be {@code null} and
     *        must not contain {@code null} elements
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code anyPuts} is {@code null} or contains a {@code null} element; also if HBase rejects a mutation with no cells or a cell
     *         exceeds the configured maximum KeyValue size
     * @throws UncheckedIOException if acquiring the table or writing the supplied mutations fails with an {@link IOException}
     * @see AnyPut
     * @see #put(String, List)
     */
    public void put(final String tableName, final Collection<AnyPut> anyPuts) throws IllegalArgumentException, UncheckedIOException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

        N.checkArgNotNull(anyPuts, cs.anyPuts);

        put(tableName, AnyPut.toPut(anyPuts));
    }

    // There is no too much benefit to add method for "Object rowKey"
    // And it may cause error because the "Object" is ambiguous to any type.
    /**
     * Deletes the row identified by {@code rowKey}.
     *
     * <p>Package-private convenience overload delegating to {@link #delete(String, AnyDelete)};
     * {@code rowKey} is converted to bytes via {@link #toRowKeyBytes(Object)}.</p>
     *
     * @param tableName the name of the HBase table
     * @param rowKey the row key of the row to delete
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code rowKey} is {@code null}, an HBase operation or {@code Any*} wrapper (see {@link #toRowKeyBytes(Object)}) or an
     *         empty value wrapper such as {@code Optional.empty()} (which has no byte representation), or its byte representation is empty
     *         or exceeds 32,767 bytes
     * @throws RuntimeException if converting {@code rowKey} to bytes invokes a failing string conversion
     * @throws UncheckedIOException if acquiring the table or applying the delete mutations fails with an {@link IOException}
     */
    void delete(final String tableName, final Object rowKey) throws IllegalArgumentException, RuntimeException, UncheckedIOException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

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
     * @param delete the Delete operation specifying what data to remove; must not be {@code null}
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code delete} is {@code null}
     * @throws UncheckedIOException if acquiring the table or applying the delete mutations fails with an {@link IOException}
     * @see Delete
     */
    public void delete(final String tableName, final Delete delete) throws IllegalArgumentException, UncheckedIOException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);
        N.checkArgNotNull(delete, cs.delete);

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
     * <p>This method submits multiple Delete operations in one client batch call, which is more
     * efficient than invoking this executor once per delete. The HBase client may partition that
     * batch into multiple server requests based on region placement. An empty list is a no-op and
     * does not acquire a table handle.</p>
     *
     * <p>{@code deletes} is passed to {@link Table#delete(List)} as-is, and the HBase client
     * <em>modifies</em> it: every successfully applied delete is removed, so afterwards (also when
     * this method fails) the list holds only the deletes that were not applied. Pass a modifiable
     * list such as an {@link ArrayList}; with an unmodifiable list ({@code Arrays.asList(...)},
     * {@code List.of(...)}, {@code N.asList(...)}) the deletes are applied and then
     * {@link UnsupportedOperationException} is thrown.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Delete> deletes = new ArrayList<>();
     * deletes.add(new Delete(Bytes.toBytes("user123")));
     * deletes.add(new Delete(Bytes.toBytes("user456")));
     * executor.delete("users", deletes);   // void; on return, deletes is empty (every delete was applied)
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param deletes the modifiable list of Delete operations to execute; must not be {@code null}. Successfully
     *        applied deletes are removed from it by the HBase client
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code deletes} is {@code null}
     * @throws UncheckedIOException if acquiring the table or applying the delete mutations fails with an {@link IOException}
     * @throws NullPointerException if the HBase client dereferences a null operation in {@code deletes}
     * @throws UnsupportedOperationException if HBase tries to remove successful deletions from a list that does not support removal
     * @see Delete
     */
    public void delete(final String tableName, final List<Delete> deletes)
            throws IllegalArgumentException, UncheckedIOException, NullPointerException, UnsupportedOperationException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);
        N.checkArgNotNull(deletes, cs.deletes);

        if (deletes.isEmpty()) {
            return;
        }

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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Delete the entire row
     * executor.delete("users", AnyDelete.of("user123"));   // void
     *
     * // Delete a single column only
     * executor.delete("users", AnyDelete.of("user123").addColumn("info", "email"));   // void
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param anyDelete the AnyDelete operation specifying what data to remove; must not be {@code null}
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code anyDelete} is {@code null}
     * @throws UncheckedIOException if acquiring the table or applying the delete mutations fails with an {@link IOException}
     * @see AnyDelete
     * @see #delete(String, Delete)
     */
    public void delete(final String tableName, final AnyDelete anyDelete) throws IllegalArgumentException, UncheckedIOException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

        N.checkArgNotNull(anyDelete, cs.anyDelete);

        delete(tableName, anyDelete.val());
    }

    /**
     * Deletes multiple rows or cells from HBase using a collection of AnyDelete operations.
     *
     * <p>This is a convenience wrapper around {@link #delete(String, List)} that accepts
     * a collection of AnyDelete instances for batch deletion.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * executor.delete("users", N.asList(
     *         AnyDelete.of("user123"), AnyDelete.of("user456")));   // void; both rows deleted
     *
     * // An empty collection is a no-op
     * executor.delete("users", N.<AnyDelete> emptyList());   // void; nothing deleted
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param anyDeletes the collection of AnyDelete operations to execute; must not be {@code null}
     *        and must not contain {@code null} elements
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code anyDeletes} is {@code null} or contains a {@code null} element
     * @throws UncheckedIOException if acquiring the table or applying the delete mutations fails with an {@link IOException}
     * @see AnyDelete
     * @see #delete(String, List)
     */
    public void delete(final String tableName, final Collection<AnyDelete> anyDeletes) throws IllegalArgumentException, UncheckedIOException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

        delete(tableName, AnyDelete.toDelete(anyDeletes));
    }

    /**
     * Performs multiple mutations atomically on a single row using an AnyRowMutations operation.
     *
     * <p>This is a convenience wrapper around {@link #mutateRow(String, RowMutations)}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AnyRowMutations rm = AnyRowMutations.of("user123")
     *         .add(new Put(Bytes.toBytes("user123"))
     *                 .addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("John")))
     *         .add(new Delete(Bytes.toBytes("user123"))
     *                 .addColumns(Bytes.toBytes("temp"), Bytes.toBytes("old")));   // add(..) throws IOException
     *
     * executor.mutateRow("users", rm);   // void; all mutations applied atomically
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param rm the AnyRowMutations containing the atomic mutations to perform; must not be {@code null}
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code rm} is {@code null}
     * @throws UncheckedIOException if acquiring the table or applying the row mutations fails with an {@link IOException}
     * @see AnyRowMutations
     * @see #mutateRow(String, RowMutations)
     */
    public void mutateRow(final String tableName, final AnyRowMutations rm) throws IllegalArgumentException, UncheckedIOException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

        N.checkArgNotNull(rm, cs.rm);

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
     * @param rm the RowMutations containing the atomic mutations to perform; must not be {@code null}
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code rm} is {@code null}
     * @throws UncheckedIOException if acquiring the table or applying the row mutations fails with an {@link IOException}
     * @see RowMutations
     */
    public void mutateRow(final String tableName, final RowMutations rm) throws IllegalArgumentException, UncheckedIOException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);
        N.checkArgNotNull(rm, cs.rm);

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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Result result = executor.append("users",
     *         AnyAppend.of("user123").addColumn("logs", "access", ",2023-10-15"));
     * // With the default return-results setting, result holds the post-append cell values.
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param append the AnyAppend operation specifying the values to append; must not be {@code null}
     * @return the post-append values when return-results is enabled; HBase may return
     *         {@code null} when {@link Append#setReturnResults(boolean)} is set to {@code false}
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code append} is {@code null}; also if HBase rejects a mutation with no cells
     * @throws UncheckedIOException if acquiring the table or appending the cell values fails with an {@link IOException}
     * @see AnyAppend
     * @see #append(String, Append)
     */
    public Result append(final String tableName, final AnyAppend append) throws IllegalArgumentException, UncheckedIOException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

        N.checkArgNotNull(append, cs.append);

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
     * @param append the Append operation specifying the values to append; must not be {@code null}
     * @return the post-append values when return-results is enabled; HBase may return
     *         {@code null} when {@link Append#setReturnResults(boolean)} is set to {@code false}
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code append} is {@code null}; also if HBase rejects an empty mutation
     * @throws UncheckedIOException if acquiring the table or appending the cell values fails with an {@link IOException}
     * @see Append
     */
    public Result append(final String tableName, final Append append) throws IllegalArgumentException, UncheckedIOException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);
        N.checkArgNotNull(append, cs.append);

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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Result result = executor.increment("users",
     *         AnyIncrement.of("user123")
     *                 .addColumn("stats", "login_count", 1)
     *                 .addColumn("stats", "points", -5));   // negative amount decrements
     * // With the default return-results setting, result holds each post-increment value.
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param increment the AnyIncrement operation specifying the values to increment; must not be {@code null}
     * @return the post-increment values when return-results is enabled; callers that set
     *         {@link Increment#setReturnResults(boolean)} to {@code false} must not rely on returned values
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code increment} is {@code null}; also if HBase rejects a mutation with no cells
     * @throws UncheckedIOException if acquiring the table or incrementing the cell values fails with an {@link IOException}
     * @see AnyIncrement
     * @see #increment(String, Increment)
     */
    public Result increment(final String tableName, final AnyIncrement increment) throws IllegalArgumentException, UncheckedIOException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

        N.checkArgNotNull(increment, cs.increment);

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
     * @param increment the Increment operation specifying the values to increment; must not be {@code null}
     * @return the post-increment values when return-results is enabled; callers that set
     *         {@link Increment#setReturnResults(boolean)} to {@code false} must not rely on returned values
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code increment} is {@code null}; also if HBase rejects an empty mutation
     * @throws UncheckedIOException if acquiring the table or incrementing the cell values fails with an {@link IOException}
     * @see Increment
     */
    public Result increment(final String tableName, final Increment increment) throws IllegalArgumentException, UncheckedIOException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);
        N.checkArgNotNull(increment, cs.increment);

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
     * encoded via {@link #toFamilyQualifierBytes(String)}. If the cell does not
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
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code rowKey} is an HBase operation or {@code Any*} wrapper (see {@link #toRowKeyBytes(Object)})
     * @throws UncheckedIOException if acquiring the table or incrementing the column fails with an {@link IOException}, including when HBase
     *         rejects a null row key or family
     * @throws RuntimeException if converting {@code rowKey} to bytes invokes a failing string conversion
     */
    public long incrementColumnValue(final String tableName, final Object rowKey, final String family, final String qualifier, final long amount)
            throws IllegalArgumentException, UncheckedIOException, RuntimeException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

        return incrementColumnValue(tableName, rowKey, toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), amount);
    }

    /**
     * Atomically increments a single column's numeric value with the specified
     * {@link Durability} guarantee.
     *
     * <p>{@code rowKey}, {@code family}, and {@code qualifier} are converted to bytes as
     * documented on {@link #incrementColumnValue(String, Object, String, String, long)}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long newValue = executor.incrementColumnValue(
     *         "users", "user123", "stats", "login_count", 1, Durability.SKIP_WAL);
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param rowKey the row key (converted via {@link #toRowKeyBytes(Object)})
     * @param family the column family name (converted via {@link #toFamilyQualifierBytes(String)})
     * @param qualifier the column qualifier name (converted via {@link #toFamilyQualifierBytes(String)})
     * @param amount the amount to add (negative values decrement)
     * @param durability the durability level to use for the WAL write
     * @return the value of the column after the increment
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code rowKey} is an HBase operation or {@code Any*} wrapper (see {@link #toRowKeyBytes(Object)})
     * @throws UncheckedIOException if acquiring the table or incrementing the column fails with an {@link IOException}, including when HBase
     *         rejects a null row key or family
     * @throws RuntimeException if converting {@code rowKey} to bytes invokes a failing string conversion
     * @see Durability
     */
    public long incrementColumnValue(final String tableName, final Object rowKey, final String family, final String qualifier, final long amount,
            final Durability durability) throws IllegalArgumentException, UncheckedIOException, RuntimeException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

        return incrementColumnValue(tableName, rowKey, toFamilyQualifierBytes(family), toFamilyQualifierBytes(qualifier), amount, durability);
    }

    /**
     * Byte-array variant of {@link #incrementColumnValue(String, Object, String, String, long)}.
     *
     * <p>{@code family} and {@code qualifier} are passed through to HBase unchanged;
     * {@code rowKey} is still converted via {@link #toRowKeyBytes(Object)}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long newValue = executor.incrementColumnValue(
     *         "users", "user123", Bytes.toBytes("stats"), Bytes.toBytes("login_count"), 1);
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param rowKey the row key (converted via {@link #toRowKeyBytes(Object)})
     * @param family the column family bytes (used as-is)
     * @param qualifier the column qualifier bytes (used as-is)
     * @param amount the amount to add (negative values decrement)
     * @return the value of the column after the increment
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code rowKey} is an HBase operation or {@code Any*} wrapper (see {@link #toRowKeyBytes(Object)})
     * @throws UncheckedIOException if acquiring the table or incrementing the column fails with an {@link IOException}, including when HBase
     *         rejects a null row key or family
     * @throws RuntimeException if converting {@code rowKey} to bytes invokes a failing string conversion
     */
    public long incrementColumnValue(final String tableName, final Object rowKey, final byte[] family, final byte[] qualifier, final long amount)
            throws IllegalArgumentException, UncheckedIOException, RuntimeException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long newValue = executor.incrementColumnValue(
     *         "users", "user123", Bytes.toBytes("stats"), Bytes.toBytes("login_count"),
     *         1, Durability.SKIP_WAL);
     * }</pre>
     *
     * @param tableName the name of the HBase table
     * @param rowKey the row key (converted via {@link #toRowKeyBytes(Object)})
     * @param family the column family bytes (used as-is)
     * @param qualifier the column qualifier bytes (used as-is)
     * @param amount the amount to add (negative values decrement)
     * @param durability the durability level to use for the WAL write
     * @return the value of the column after the increment
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code rowKey} is an HBase operation or {@code Any*} wrapper (see {@link #toRowKeyBytes(Object)})
     * @throws UncheckedIOException if acquiring the table or incrementing the column fails with an {@link IOException}, including when HBase
     *         rejects a null row key or family
     * @throws RuntimeException if converting {@code rowKey} to bytes invokes a failing string conversion
     * @see Durability
     */
    public long incrementColumnValue(final String tableName, final Object rowKey, final byte[] family, final byte[] qualifier, final long amount,
            final Durability durability) throws IllegalArgumentException, UncheckedIOException, RuntimeException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

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
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code rowKey} is an HBase operation or {@code Any*} wrapper (see {@link #toRowKeyBytes(Object)})
     * @throws UncheckedIOException if acquiring the table fails with an {@link IOException}
     * @throws RuntimeException if converting {@code rowKey} to bytes invokes a failing string conversion
     * @see CoprocessorRpcChannel
     */
    public CoprocessorRpcChannel coprocessorService(final String tableName, final Object rowKey)
            throws IllegalArgumentException, UncheckedIOException, RuntimeException {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

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
     * Invokes a coprocessor {@link Service} on every region from the region containing
     * {@code startRowKey} through the region containing {@code endRowKey}, inclusive, and
     * returns the per-region results.
     *
     * <p>{@code startRowKey} and {@code endRowKey} are converted to bytes via
     * {@link #toRowKeyBytes(Object)}. The underlying {@link Table} is closed in a
     * {@code finally} block.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<byte[], Long> rowCounts = executor.coprocessorService(
     *         "users", MyService.class, "user_a", "user_z",
     *         instance -> instance.countRows());
     * // one entry per selected region from user_a through user_z, inclusive; key = region name bytes
     * }</pre>
     *
     * @param <T> the service type
     * @param <R> the result type produced by {@code callable}
     * @param tableName the name of the HBase table
     * @param service the service interface class
     * @param startRowKey the start row key (inclusive; {@code null} for unbounded start)
     * @param endRowKey the end row key (inclusive; {@code null} for unbounded end)
     * @param callable the per-region callable to execute
     * @return a map from region name bytes to the result returned by {@code callable} for that region
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code callable} is {@code null}, or {@code startRowKey} or {@code endRowKey} is an HBase operation or {@code Any*}
     *         wrapper (see {@link #toRowKeyBytes(Object)})
     * @throws UncheckedIOException if acquiring the table or invoking the coprocessor fails with an {@link IOException}
     * @throws RuntimeException if converting a row-range boundary to bytes fails, or the coprocessor invocation throws a
     *         non-{@link IOException}, non-{@link Error} throwable (wrapped via
     *         {@code ExceptionUtil.toRuntimeException})
     * @throws Error if converting a row-range boundary or invoking the coprocessor throws an {@link Error}, which is propagated unchanged
     * @see Service
     * @see Batch.Call
     */
    public <T extends Service, R> Map<byte[], R> coprocessorService(final String tableName, final Class<T> service, final Object startRowKey,
            final Object endRowKey, final Batch.Call<T, R> callable) throws IllegalArgumentException, UncheckedIOException, RuntimeException, Error {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

        N.checkArgNotNull(callable, cs.callable);

        final Table table = getTable(tableName);

        try {
            return table.coprocessorService(service, toRowKeyBytes(startRowKey), toRowKeyBytes(endRowKey), callable);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } catch (final Throwable e) {
            // Don't mask JVM errors: rethrow an Error unchanged rather than wrapping it in a RuntimeException.
            if (e instanceof Error error) {
                throw error;
            }

            throw ExceptionUtil.toRuntimeException(e, true);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Invokes a coprocessor {@link Service} on every region from the region containing
     * {@code startRowKey} through the region containing {@code endRowKey}, inclusive,
     * delivering each per-region result to {@code callback} as it becomes available.
     *
     * <p>{@code startRowKey} and {@code endRowKey} are converted to bytes via
     * {@link #toRowKeyBytes(Object)}. The underlying {@link Table} is closed in a
     * {@code finally} block.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * executor.coprocessorService(
     *         "users", MyService.class, "user_a", "user_z",
     *         instance -> instance.countRows(),
     *         (region, row, count) -> total.addAndGet(count));   // callback runs per region as results arrive
     * }</pre>
     *
     * @param <T> the service type
     * @param <R> the result type produced by {@code callable}
     * @param tableName the name of the HBase table
     * @param service the service interface class
     * @param startRowKey the start row key (inclusive; {@code null} for unbounded start)
     * @param endRowKey the end row key (inclusive; {@code null} for unbounded end)
     * @param callable the per-region callable to execute
     * @param callback the callback that receives each region's result
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code callable} or {@code callback} is {@code null}, or {@code startRowKey} or {@code endRowKey} is an HBase operation
     *         or {@code Any*} wrapper (see {@link #toRowKeyBytes(Object)})
     * @throws UncheckedIOException if acquiring the table or invoking the coprocessor fails with an {@link IOException}
     * @throws Exception if converting a row-range boundary to bytes or invoking the coprocessor throws a non-{@link IOException},
     *         non-{@link Error} throwable: it is rethrown as-is if it is already an
     *         {@link Exception}, otherwise wrapped in a new {@link Exception} (an {@link Error} is always rethrown unchanged)
     * @throws Error if converting a row-range boundary or invoking the coprocessor throws an {@link Error}, which is propagated unchanged
     * @see Service
     * @see Batch.Call
     * @see Batch.Callback
     */
    public <T extends Service, R> void coprocessorService(final String tableName, final Class<T> service, final Object startRowKey, final Object endRowKey,
            final Batch.Call<T, R> callable, final Batch.Callback<R> callback) throws IllegalArgumentException, UncheckedIOException, Exception, Error {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

        N.checkArgNotNull(callable, cs.callable);
        N.checkArgNotNull(callback, cs.callback);

        final Table table = getTable(tableName);

        try {
            table.coprocessorService(service, toRowKeyBytes(startRowKey), toRowKeyBytes(endRowKey), callable, callback);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } catch (final Throwable e) {
            // Don't mask JVM errors or downgrade unchecked exceptions by wrapping them in a checked Exception.
            if (e instanceof Error error) {
                throw error;
            }

            throw e instanceof Exception exception ? exception : new Exception(e);
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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<byte[], MyResponse> responses = executor.batchCoprocessorService(
     *         "users",
     *         MyService.getDescriptor().findMethodByName("count"),
     *         MyRequest.getDefaultInstance(),
     *         "user_a", "user_z",
     *         MyResponse.getDefaultInstance());
     * // one entry per selected region from user_a through user_z, inclusive; key = region name bytes
     * }</pre>
     *
     * @param <R> the response message type
     * @param tableName the name of the HBase table
     * @param methodDescriptor the Protocol Buffers method descriptor
     * @param request the Protocol Buffers request message
     * @param startRowKey the start row key (inclusive; {@code null} for unbounded start)
     * @param endRowKey the end row key (inclusive; {@code null} for unbounded end)
     * @param responsePrototype the prototype for the response message
     * @return a map of region names (byte arrays) to their corresponding response messages
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code startRowKey} or {@code endRowKey} is an HBase operation or {@code Any*} wrapper (see {@link #toRowKeyBytes(Object)})
     * @throws UncheckedIOException if acquiring the table or invoking the coprocessor fails with an {@link IOException}
     * @throws Exception if converting a row-range boundary to bytes or invoking the coprocessor throws a throwable other than
     *         {@link IOException} or {@link Error}: it is rethrown as-is if it is
     *         already an {@link Exception}, otherwise wrapped in a new {@link Exception} (an {@link Error} is always rethrown unchanged)
     * @throws Error if converting a row-range boundary or invoking the coprocessor throws an {@link Error}, which is propagated unchanged
     * @see Message
     * @see Descriptors.MethodDescriptor
     */
    public <R extends Message> Map<byte[], R> batchCoprocessorService(final String tableName, final Descriptors.MethodDescriptor methodDescriptor,
            final Message request, final Object startRowKey, final Object endRowKey, final R responsePrototype)
            throws IllegalArgumentException, UncheckedIOException, Exception, Error {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

        final Table table = getTable(tableName);

        try {
            return table.batchCoprocessorService(methodDescriptor, request, toRowKeyBytes(startRowKey), toRowKeyBytes(endRowKey), responsePrototype);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } catch (final Throwable e) {
            // Don't mask JVM errors or downgrade unchecked exceptions by wrapping them in a checked Exception.
            if (e instanceof Error error) {
                throw error;
            }

            throw e instanceof Exception exception ? exception : new Exception(e);
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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * executor.batchCoprocessorService(
     *         "users",
     *         MyService.getDescriptor().findMethodByName("count"),
     *         MyRequest.getDefaultInstance(),
     *         "user_a", "user_z",
     *         MyResponse.getDefaultInstance(),
     *         (region, row, response) -> total.addAndGet(response.getCount()));   // callback runs per region
     * }</pre>
     *
     * @param <R> the response message type
     * @param tableName the name of the HBase table
     * @param methodDescriptor the Protocol Buffers method descriptor
     * @param request the Protocol Buffers request message
     * @param startRowKey the start row key (inclusive; {@code null} for unbounded start)
     * @param endRowKey the end row key (inclusive; {@code null} for unbounded end)
     * @param responsePrototype the prototype for the response message
     * @param callback the callback to receive response messages from each region
     * @throws IllegalArgumentException if {@code tableName} is {@code null}, empty or violates HBase's namespace or table-qualifier naming rules;
     *         also if {@code callback} is {@code null}, or {@code startRowKey} or {@code endRowKey} is an HBase operation or {@code Any*}
     *         wrapper (see {@link #toRowKeyBytes(Object)})
     * @throws UncheckedIOException if acquiring the table or invoking the coprocessor fails with an {@link IOException}
     * @throws Exception if converting a row-range boundary to bytes or invoking the coprocessor throws a throwable other than
     *         {@link IOException} or {@link Error}: it is rethrown as-is if it is
     *         already an {@link Exception}, otherwise wrapped in a new {@link Exception} (an {@link Error} is always rethrown unchanged)
     * @throws Error if converting a row-range boundary or invoking the coprocessor throws an {@link Error}, which is propagated unchanged
     * @see Message
     * @see Descriptors.MethodDescriptor
     * @see Batch.Callback
     */
    public <R extends Message> void batchCoprocessorService(final String tableName, final Descriptors.MethodDescriptor methodDescriptor, final Message request,
            final Object startRowKey, final Object endRowKey, final R responsePrototype, final Batch.Callback<R> callback)
            throws IllegalArgumentException, UncheckedIOException, Exception, Error {
        N.checkArgNotNull(tableName, cs.tableName);
        TableName.valueOf(tableName);

        N.checkArgNotNull(callback, cs.callback);

        final Table table = getTable(tableName);

        try {
            table.batchCoprocessorService(methodDescriptor, request, toRowKeyBytes(startRowKey), toRowKeyBytes(endRowKey), responsePrototype, callback);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } catch (final Throwable e) {
            // Don't mask JVM errors or downgrade unchecked exceptions by wrapping them in a checked Exception.
            if (e instanceof Error error) {
                throw error;
            }

            throw e instanceof Exception exception ? exception : new Exception(e);
        } finally {
            closeQuietly(table);
        }
    }

    /**
     * Converts a column-family or qualifier string to a fresh UTF-8 byte array.
     *
     * <p>A new array is returned on each invocation because HBase operations retain the supplied
     * family and qualifier arrays and expose them through live maps. Sharing a cached array would
     * allow mutation of one operation's family map to corrupt unrelated operations created later.
     * Returns {@code null} for a {@code null} input.</p>
     *
     * @param str the family or qualifier name to encode; may be {@code null}
     * @return the UTF-8 bytes for {@code str}, or {@code null} if {@code str} is {@code null}
     */
    static byte[] toFamilyQualifierBytes(final String str) {
        return str == null ? null : Bytes.toBytes(str);
    }

    /**
     * Converts an arbitrary row-key value to its HBase byte representation.
     *
     * <p>Converts like {@link #toValueBytes(Object)}, except that an HBase operation ({@link Operation} or {@link Row},
     * such as a {@link Get}, {@link Put}, {@link Delete}, {@link Scan} or {@link RowMutations}) or an {@code Any*} wrapper
     * ({@code AnyGet}, {@code AnyPut}, {@code AnyScan}, {@code AnyRowMutations} and the like) is rejected: it is not a
     * row-key value. So is one that a present value wrapper ({@code java.util.Optional}, {@code u.Optional},
     * {@code u.Nullable} or {@link Holder}, also nested) holds, since a wrapper converts as the value it holds. Every
     * {@code Object} row key of this package goes through this helper (the {@code Any*.of(Object...)} factories, the
     * {@code AnyScan} row bounds and prefixes, and this executor's {@code incrementColumnValue} and coprocessor row
     * arguments).</p>
     *
     * @param rowKey the row key value; may be {@code null}
     * @return the bytes for the row key, or {@code null} if {@code rowKey} is {@code null} or an empty value wrapper
     *         (see {@link #isEmptyValueWrapper(Object)})
     * @throws IllegalArgumentException if {@code rowKey} is, or is a present value wrapper holding, an HBase operation or an
     *         {@code Any*} wrapper
     * @throws RuntimeException if converting {@code rowKey} to bytes invokes a failing string conversion
     * @see #toValueBytes(Object)
     */
    static byte[] toRowKeyBytes(final Object rowKey) throws IllegalArgumentException, RuntimeException {
        // WHY: an operation passed where a row key is expected (AnyGet.of((Object) anyGet), AnyDelete.of(anyDelete) as an
        // attempted copy, a Get typed as Object) fell through to N.stringOf, whose bean JSON ({"priority": -1, ...}) does not
        // even contain the operation's row: the call silently read, wrote or deleted that garbage row. Rejected as
        // AnyPut.create rejects an operation passed as an entity. N.stringOf converts a present Optional/Nullable/Holder as the
        // value it holds, so Optional.of(anyGet) made the same garbage row: an operation inside one is rejected too.
        final Object operation = heldOperation(rowKey);

        if (operation != null) {
            throw new IllegalArgumentException("Cannot use " + ClassUtil.getCanonicalClassName(operation.getClass())
                    + (operation == rowKey ? "" : " held by " + ClassUtil.getCanonicalClassName(rowKey.getClass()))
                    + " as a row key: it is an HBase operation, not a row-key value. Pass its row (for example getRow()) instead");
        }

        return toValueBytes(rowKey);
    }

    /**
     * Returns {@code value} if it is an HBase operation ({@link Operation}, {@link Row}) or an {@code Any*} wrapper, else the
     * one a present {@code java.util.Optional}, {@code u.Optional}, {@code u.Nullable} or {@link Holder} holds (nested ones
     * included), else {@code null}.
     */
    private static Object heldOperation(final Object value) {
        if (value instanceof Operation || value instanceof Row || value instanceof AnyOperation) {
            return value;
        } else if (value instanceof java.util.Optional<?> optional) {
            return optional.isPresent() ? heldOperation(optional.get()) : null;
        } else if (value instanceof u.Optional<?> optional) {
            return optional.isPresent() ? heldOperation(optional.get()) : null;
        } else if (value instanceof u.Nullable<?> nullable) {
            return nullable.isNotNull() ? heldOperation(nullable.get()) : null;
        } else if (value instanceof Holder<?> holder) {
            return holder.isNotNull() ? heldOperation(holder.value()) : null;
        }

        return null;
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
     *       and then to UTF-8 bytes; a value whose string form is {@code null}, such as an empty
     *       wrapper (see {@link #isEmptyValueWrapper(Object)}), &rarr; {@code null}, like a {@code null} value</li>
     * </ul>
     *
     * @param value the value to convert; may be {@code null}
     * @return the byte representation, or {@code null} if {@code value} is {@code null} or its string form is {@code null}
     * @throws RuntimeException if converting {@code value} to bytes invokes a failing string conversion
     */
    static byte[] toValueBytes(final Object value) throws RuntimeException {
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
            // N.stringOf returns null for an empty wrapper (empty Optional/Nullable/OptionalInt..., Holder of null), and
            // Bytes.toBytes(null) threw NullPointerException: such a value is stored like a null value instead.
            final String str = N.stringOf(value);

            return str == null ? null : Bytes.toBytes(str); // NOSONAR
        }
    }

    /**
     * Tests whether {@code value} is a value wrapper that holds no value: an empty JDK {@link java.util.Optional},
     * {@link java.util.OptionalInt}, {@link java.util.OptionalLong} or {@link java.util.OptionalDouble}; an empty abacus
     * {@code u.Optional} or primitive {@code u.OptionalXxx}; a {@code u.Nullable} that is empty or holds {@code null}; or a
     * {@link com.landawn.abacus.util.Holder} holding {@code null}. Their string form ({@code N.stringOf}) is {@code null},
     * so {@link #toValueBytes(Object)} converts them to {@code null}, and entity-to-{@link Put} conversion skips a property
     * holding one, as it skips a {@code null} property: an empty cell would read back as a present empty string.
     *
     * @param value the value to test; may be {@code null}
     * @return {@code true} if {@code value} is one of the wrappers above and holds no value; {@code false} otherwise,
     *         including for {@code null}
     */
    static boolean isEmptyValueWrapper(final Object value) {
        if (value == null) {
            return false;
        } else if (value instanceof java.util.Optional<?> optional) {
            return optional.isEmpty();
        } else if (value instanceof java.util.OptionalInt optional) {
            return optional.isEmpty();
        } else if (value instanceof java.util.OptionalLong optional) {
            return optional.isEmpty();
        } else if (value instanceof java.util.OptionalDouble optional) {
            return optional.isEmpty();
        } else if (value instanceof u.Optional<?> optional) {
            return optional.isEmpty();
        } else if (value instanceof u.Nullable<?> nullable) {
            return nullable.isNull();
        } else if (value instanceof u.OptionalInt optional) {
            return optional.isEmpty();
        } else if (value instanceof u.OptionalLong optional) {
            return optional.isEmpty();
        } else if (value instanceof u.OptionalDouble optional) {
            return optional.isEmpty();
        } else if (value instanceof u.OptionalBoolean optional) {
            return optional.isEmpty();
        } else if (value instanceof u.OptionalChar optional) {
            return optional.isEmpty();
        } else if (value instanceof u.OptionalByte optional) {
            return optional.isEmpty();
        } else if (value instanceof u.OptionalShort optional) {
            return optional.isEmpty();
        } else if (value instanceof u.OptionalFloat optional) {
            return optional.isEmpty();
        } else if (value instanceof Holder<?> holder) {
            return holder.isNull();
        }

        return false;
    }

    /**
     * Decodes {@code len} bytes of a row key starting at {@code offset} as a UTF-8 string.
     *
     * @param bytes the backing byte array
     * @param offset the offset of the first byte to decode
     * @param len the number of bytes to decode
     * @return the decoded row-key string
     */
    static String toRowKeyString(final byte[] bytes, final int offset, final int len) {
        return Bytes.toString(bytes, offset, len);
    }

    /**
     * Decodes {@code len} bytes of a column family or qualifier starting at {@code offset}
     * as a UTF-8 string.
     *
     * @param bytes the backing byte array
     * @param offset the offset of the first byte to decode
     * @param len the number of bytes to decode
     * @return the decoded family/qualifier string
     */
    static String toFamilyQualifierString(final byte[] bytes, final int offset, final int len) {
        return Bytes.toString(bytes, offset, len);
    }

    /**
     * Decodes {@code len} bytes of a cell value starting at {@code offset} as a UTF-8 string.
     *
     * @param bytes the backing byte array
     * @param offset the offset of the first byte to decode
     * @param len the number of bytes to decode
     * @return the decoded value string
     */
    static String toValueString(final byte[] bytes, final int offset, final int len) {
        return Bytes.toString(bytes, offset, len);
    }

    /**
     * Returns the row key of {@code cell} decoded as a UTF-8 string.
     *
     * @param cell the cell whose row key is read; must not be {@code null}
     * @return the row-key string of {@code cell}
     * @throws IllegalArgumentException if {@code cell} is {@code null}
     */
    static String getRowKeyString(final Cell cell) throws IllegalArgumentException {
        N.checkArgNotNull(cell, cs.cell);

        return toRowKeyString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
    }

    /**
     * Returns the column family of {@code cell} decoded as a UTF-8 string.
     *
     * @param cell the cell whose column family is read; must not be {@code null}
     * @return the column-family string of {@code cell}
     * @throws IllegalArgumentException if {@code cell} is {@code null}
     */
    static String getFamilyString(final Cell cell) throws IllegalArgumentException {
        N.checkArgNotNull(cell, cs.cell);

        return toFamilyQualifierString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
    }

    /**
     * Returns the column qualifier of {@code cell} decoded as a UTF-8 string.
     *
     * @param cell the cell whose column qualifier is read; must not be {@code null}
     * @return the column-qualifier string of {@code cell}
     * @throws IllegalArgumentException if {@code cell} is {@code null}
     */
    static String getQualifierString(final Cell cell) throws IllegalArgumentException {
        N.checkArgNotNull(cell, cs.cell);

        return toFamilyQualifierString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
    }

    /**
     * Returns the value of {@code cell} decoded as a UTF-8 string.
     *
     * @param cell the cell whose value is read; must not be {@code null}
     * @return the value string of {@code cell}
     * @throws IllegalArgumentException if {@code cell} is {@code null}
     */
    static String getValueString(final Cell cell) throws IllegalArgumentException {
        N.checkArgNotNull(cell, cs.cell);

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
     * HBaseExecutor executor = new HBaseExecutor(connection);
     * try {
     *     // perform HBase operations
     * } finally {
     *     executor.close();
     * }
     * }</pre>
     *
     * @throws IOException if closing {@link Admin} or {@link Connection} fails. If both fail with distinct
     *         exceptions, the admin failure is thrown and the connection failure is attached as suppressed.
     * @throws RuntimeException if either resource throws a runtime exception while closing
     * @throws Error if either resource throws an error while closing
     */
    public void close() throws IOException, RuntimeException, Error {
        Throwable failure = null;

        try {
            if (admin != null) {
                admin.close();
            }
        } catch (final Throwable e) { // close the connection even when Admin.close fails
            failure = e;
        }

        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        } catch (final Throwable e) {
            if (failure == null) {
                failure = e;
            } else if (failure != e) {
                failure.addSuppressed(e);
            }
        }

        if (failure instanceof IOException e) {
            throw e;
        } else if (failure instanceof RuntimeException e) {
            throw e;
        } else if (failure instanceof Error e) {
            throw e;
        } else if (failure != null) {
            throw new IOException(failure);
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

        /**
         * Constructs an {@code HBaseMapper} for {@code targetEntityClass} bound to
         * {@code tableName} on the given executor.
         *
         * <p>Instances are normally obtained via {@link HBaseExecutor#mapper(Class)} or
         * {@link HBaseExecutor#mapper(Class, String, NamingPolicy)} rather than constructed
         * directly. The entity class must be a JavaBean class with exactly one ID property
         * ({@code @Id}-annotated or, when no property is annotated, a conventionally typed
         * property named {@code id}), which is used as the row key unless another property was registered
         * through the deprecated {@link HBaseExecutor#registerRowKeyProperty(Class, String)}.</p>
         *
         * @param targetEntityClass the entity class this mapper handles
         * @param hbaseExecutor the executor that performs the underlying HBase operations
         * @param tableName the HBase table name to bind this mapper to; must not be {@code null} or empty
         * @param namingPolicy the naming policy for column name conversion; {@code null} maps to {@link NamingPolicy#CAMEL_CASE}
         * @throws IllegalArgumentException if {@code targetEntityClass} is {@code null}, is not a bean class (a {@link Map}, even an
         *         {@code @Entity}-annotated {@code Map} subclass, is not one), or has no or more than one
         *         ID property; if {@code hbaseExecutor} is {@code null}; or if {@code tableName} is {@code null} or empty
         */
        HBaseMapper(final Class<T> targetEntityClass, final HBaseExecutor hbaseExecutor, final String tableName, final NamingPolicy namingPolicy)
                throws IllegalArgumentException {
            N.checkArgNotNull(targetEntityClass, cs.targetEntityClass);
            N.checkArgument(Beans.isBeanClass(targetEntityClass), "{} is not an entity class with getter/setter method",
                    ClassUtil.getCanonicalClassName(targetEntityClass));

            // WHY: Beans.isBeanClass accepts an @Entity-annotated Map subclass, whose bean properties are its Map getters
            // (keySet, values, entrySet, size, empty). Such a mapper could never read a row (toEntity rejects Map types), and
            // its put mapped those getters instead of the entries, so reject a Map up front, as a plain Map already is.
            N.checkArgument(!Map.class.isAssignableFrom(targetEntityClass), "{} is a Map, not an entity class with getter/setter method",
                    ClassUtil.getCanonicalClassName(targetEntityClass));

            final List<String> idPropNames = QueryUtil.idPropNames(targetEntityClass);

            if (idPropNames.size() != 1) {
                throw new IllegalArgumentException(
                        "No or multiple ids: " + idPropNames + " defined/annotated in class: " + ClassUtil.getCanonicalClassName(targetEntityClass));
            }

            N.checkArgNotNull(hbaseExecutor, cs.hbaseExecutor);
            N.checkArgNotEmpty(tableName, cs.tableName);

            this.hbaseExecutor = hbaseExecutor;
            this.tableName = tableName;
            this.targetEntityClass = targetEntityClass;
            rowKeyPropName = idPropNames.get(0);

            this.namingPolicy = namingPolicy == null ? NamingPolicy.CAMEL_CASE : namingPolicy;
        }

        /**
         * Checks if an entity with the specified row key exists in the table.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * HBaseMapper<User, String> mapper = executor.mapper(User.class);
         * boolean present = mapper.exists("user123");   // true if a row keyed "user123" exists
         * }</pre>
         *
         * @param rowKey the row key to check
         * @return {@code true} if an entity with the given row key exists, {@code false} otherwise
         * @throws IllegalArgumentException if a row key is an HBase operation or {@code Any*} wrapper (see
         *         {@link HBaseExecutor#toRowKeyBytes(Object)}) or its byte representation is {@code null}, empty or exceeds 32,767 bytes; also
         *         if this mapper's table name violates HBase's namespace or table-qualifier naming rules
         * @throws RuntimeException if converting a row key to bytes invokes a failing string conversion
         * @throws UncheckedIOException if acquiring the table or checking row existence fails with an {@link IOException}
         */
        public boolean exists(final K rowKey) throws IllegalArgumentException, RuntimeException, UncheckedIOException {
            return hbaseExecutor.exists(tableName, AnyGet.of(rowKey));
        }

        /**
         * Checks if entities with the specified row keys exist in the table.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<Boolean> flags = mapper.exists(N.asList("user123", "user456"));
         * // flags is positionally aligned with the input keys
         *
         * List<Boolean> empty = mapper.exists(N.<String> emptyList());   // returns [] (empty list)
         * }</pre>
         *
         * @param rowKeys the collection of row keys to check; {@code null} is treated as an empty
         *        collection and yields an empty list
         * @return a list of Boolean values corresponding to each row key, where {@code true}
         *         indicates the entity exists
         * @throws IllegalArgumentException if a row key is an HBase operation or {@code Any*} wrapper (see
         *         {@link HBaseExecutor#toRowKeyBytes(Object)}) or its byte representation is {@code null}, empty or exceeds 32,767 bytes; also
         *         if this mapper's table name violates HBase's namespace or table-qualifier naming rules
         * @throws RuntimeException if converting a row key to bytes invokes a failing string conversion
         * @throws UncheckedIOException if acquiring the table or checking row existence fails with an {@link IOException}
         */
        public List<Boolean> exists(final Collection<? extends K> rowKeys) throws IllegalArgumentException, RuntimeException, UncheckedIOException {
            final List<AnyGet> anyGets = N.map(rowKeys, AnyGet::of);

            return hbaseExecutor.exists(tableName, anyGets);
        }

        /**
         * Retrieves an entity by its row key.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User user = mapper.get("user123");      // mapped entity
         * User missing = mapper.get("no_such");   // returns null (no matching row)
         * }</pre>
         *
         * @param rowKey the row key of the entity to retrieve
         * @return the entity object, or the type's default value (typically {@code null} for bean classes)
         *         when no row matches the given key
         * @throws IllegalArgumentException if a row key is an HBase operation or {@code Any*} wrapper (see
         *         {@link HBaseExecutor#toRowKeyBytes(Object)}) or its byte representation is {@code null}, empty or exceeds 32,767 bytes; also
         *         if this mapper's table name violates HBase's namespace or table-qualifier naming rules; or if preparing the target entity's
         *         row-key metadata finds an unsupported property type or a missing getter or setter
         * @throws RuntimeException if converting a cell or row key, creating a target bean, or reading or assigning one of its properties fails
         * @throws UncheckedIOException if acquiring the table, fetching rows, or reading result cells fails with an {@link IOException}
         */
        public T get(final K rowKey) throws IllegalArgumentException, RuntimeException, UncheckedIOException {
            return hbaseExecutor.get(tableName, AnyGet.of(rowKey), targetEntityClass);
        }

        /**
         * Retrieves multiple entities by their row keys.
         *
         * <p>Row keys that do not exist in the table are skipped — the returned list
         * may contain fewer elements than {@code rowKeys} and the order does not
         * necessarily correspond positionally to the input collection.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<User> users = mapper.get(N.asList("user123", "user456"));
         * // users contains only the rows that were found
         *
         * List<User> empty = mapper.get(N.<String> emptyList());   // returns [] (empty list)
         * }</pre>
         *
         * @param rowKeys the collection of row keys to retrieve; {@code null} is treated as an empty
         *        collection and yields an empty list
         * @return a list of entity objects for the row keys that were found
         * @throws IllegalArgumentException if a row key is an HBase operation or {@code Any*} wrapper (see
         *         {@link HBaseExecutor#toRowKeyBytes(Object)}) or its byte representation is {@code null}, empty or exceeds 32,767 bytes; also
         *         if this mapper's table name violates HBase's namespace or table-qualifier naming rules; or if preparing the target entity's
         *         row-key metadata finds an unsupported property type or a missing getter or setter
         * @throws RuntimeException if converting a cell or row key, creating a target bean, or reading or assigning one of its properties fails
         * @throws UncheckedIOException if acquiring the table, fetching rows, or reading result cells fails with an {@link IOException}
         */
        public List<T> get(final Collection<? extends K> rowKeys) throws IllegalArgumentException, RuntimeException, UncheckedIOException {
            final List<AnyGet> anyGets = N.map(rowKeys, AnyGet::of);

            return hbaseExecutor.get(tableName, anyGets, targetEntityClass);
        }

        /**
         * Stores an entity in the table.
         *
         * <p>The row key is extracted from the entity's ID property ({@code @Id}-annotated or, when no
         * property is annotated, a conventionally typed property named {@code id}), or from the property registered for the
         * entity's class through the deprecated {@link HBaseExecutor#registerRowKeyProperty(Class, String)}, which takes
         * precedence; the entity's other properties are mapped to HBase columns according to the configured naming policy.
         * A property whose value is {@code null} or an empty value wrapper (an empty {@code Optional}, {@code OptionalInt},
         * {@code Nullable} and the like, or a {@code Holder} of {@code null}) is not written, so reading the row back leaves it
         * at the value a new instance has (see {@link AnyPut#create(Object, NamingPolicy)}).</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User user = new User();
         * user.setId("user123");
         * user.setName("John");
         * mapper.put(user);   // void; row keyed by user.getId() is stored
         * }</pre>
         *
         * @param entityToPut the entity to store; must not be {@code null}
         * @throws IllegalArgumentException if {@code entityToPut} is {@code null}, is not a bean or has missing, multiple or unsupported row-key
         *         properties, its row-key value is {@code null} or an empty value wrapper (no byte representation), empty or exceeds 32,767 bytes,
         *         a column version is negative, or an encoded column
         *         family exceeds 127 bytes; also if HBase rejects a mutation with no cells or a cell exceeds the configured maximum KeyValue size; also
         *         if this mapper's table name violates HBase's namespace or table-qualifier naming rules
         * @throws NullPointerException if a versioned column collection or map contains a {@code null}
         *         {@code HBaseColumn}
         * @throws RuntimeException if reading an entity property or converting its row key or column values to bytes fails
         * @throws UncheckedIOException if acquiring the table or writing the supplied mutations fails with an {@link IOException}
         */
        public void put(final T entityToPut) throws IllegalArgumentException, NullPointerException, RuntimeException, UncheckedIOException {
            N.checkArgNotNull(entityToPut, cs.entityToPut);

            hbaseExecutor.put(tableName, AnyPut.create(entityToPut, namingPolicy));
        }

        /**
         * Stores multiple entities in the table in a batch operation.
         *
         * <p>Each entity is mapped as by {@link #put(Object)}: a property whose value is {@code null} or an empty value
         * wrapper is not written.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User user = new User();
         * user.setId("user123");
         * user.setName("John");
         * mapper.put(N.asList(user));   // void; the row is stored
         *
         * mapper.put(N.<User> emptyList());   // void; no-op
         * }</pre>
         *
         * @param entitiesToPut the collection of entities to store; must not be {@code null} and
         *        must not contain {@code null} elements
         * @throws IllegalArgumentException if {@code entitiesToPut} is {@code null} or contains a {@code null} element, an entity is not a bean or
         *         has missing, multiple or unsupported row-key properties, its row-key value is {@code null} or an empty value wrapper (no byte
         *         representation), empty or exceeds 32,767 bytes, a column version is negative, or an encoded column family exceeds 127 bytes;
         *         also if HBase rejects a mutation with no cells or a cell exceeds the configured maximum KeyValue size; also
         *         if this mapper's table name violates HBase's namespace or table-qualifier naming rules
         * @throws NullPointerException if a versioned column collection or map contains a {@code null}
         *         {@code HBaseColumn}
         * @throws RuntimeException if reading an entity property or converting its row key or column values to bytes fails
         * @throws UncheckedIOException if acquiring the table or writing the supplied mutations fails with an {@link IOException}
         */
        public void put(final Collection<? extends T> entitiesToPut)
                throws IllegalArgumentException, NullPointerException, RuntimeException, UncheckedIOException {
            N.checkArgNotNull(entitiesToPut, cs.entitiesToPut);

            hbaseExecutor.put(tableName, AnyPut.create(entitiesToPut, namingPolicy));
        }

        /**
         * Deletes an entity from the table.
         *
         * <p>The row key is read from the same property {@link #put(Object)} writes it from: the entity's ID property
         * ({@code @Id}-annotated or, when no property is annotated, a conventionally typed property named {@code id}), or the
         * property registered for the entity's class through the deprecated
         * {@link HBaseExecutor#registerRowKeyProperty(Class, String)}, which takes precedence.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User user = mapper.get("user123");
         * if (user != null) {
         *     mapper.delete(user);   // void; row keyed by user.getId() is removed
         * }
         * }</pre>
         *
         * @param entityToDelete the entity to delete; must not be {@code null}
         * @throws IllegalArgumentException if {@code entityToDelete} is {@code null}, its class has multiple or unsupported row-key properties
         *         (as for {@link #put(Object)}), its row-key value is {@code null} or an empty value wrapper (no byte representation), or the
         *         byte representation of its row-key value is empty or exceeds 32,767 bytes; also
         *         if this mapper's table name violates HBase's namespace or table-qualifier naming rules
         * @throws RuntimeException if reading an entity's row-key property or converting its value to bytes fails
         * @throws UncheckedIOException if acquiring the table or applying the delete mutations fails with an {@link IOException}
         */
        public void delete(final T entityToDelete) throws IllegalArgumentException, RuntimeException, UncheckedIOException {
            N.checkArgNotNull(entityToDelete, cs.entityToDelete);

            deleteByRowKey(getRowKey(entityToDelete));
        }

        // Reads the row key from the property that put(entity) writes it from (AnyPut.create): a property registered through
        // registerRowKeyProperty takes precedence over the ID property there, so it must here too, or delete(entity) would
        // remove a different row than the one put(entity) wrote.
        // WHY getRowKeySetMethod rather than a cache lookup: AnyPut.create resolves (and caches) the row-key property of the
        // entity's own class. A subclass declaring its own @Id that had not been put or read yet had no cache entry, so it was
        // deleted by the mapped class's ID property (another row), and by its own @Id only after a put or get of that class.
        @SuppressWarnings("unchecked")
        private K getRowKey(final T entity) throws IllegalArgumentException {
            N.checkArgNotNull(entity, cs.entity);

            final Method rowKeySetMethod = getRowKeySetMethod(entity.getClass());
            final String propName = rowKeySetMethod == null ? rowKeyPropName : Beans.getPropNameByMethod(rowKeySetMethod);

            return (K) Beans.getPropValue(entity, propName);
        }

        /**
         * Deletes multiple entities from the table in a batch operation.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<User> users = mapper.get(N.asList("user123", "user456"));
         * mapper.delete(users);   // void; the row of every entity is removed
         * }</pre>
         *
         * @param entitiesToDelete the collection of entities to delete; {@code null} is treated as an
         *        empty collection and is a no-op
         * @throws IllegalArgumentException if any element of {@code entitiesToDelete} is {@code null}, an element's class has multiple or
         *         unsupported row-key properties, an element's row-key value is
         *         {@code null} or an empty value wrapper (no byte representation), or the byte representation of an entity's row-key value is
         *         empty or exceeds 32,767 bytes; also
         *         if this mapper's table name violates HBase's namespace or table-qualifier naming rules
         * @throws RuntimeException if reading an entity's row-key property or converting its value to bytes fails
         * @throws UncheckedIOException if acquiring the table or applying the delete mutations fails with an {@link IOException}
         */
        public void delete(final Collection<? extends T> entitiesToDelete) throws IllegalArgumentException, RuntimeException, UncheckedIOException {
            deleteByRowKey(N.map(entitiesToDelete, entity -> getRowKey(entity)));
        }

        /**
         * Deletes an entity by its row key.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * mapper.deleteByRowKey("user123");   // void; row "user123" removed (no-op if absent)
         * }</pre>
         *
         * @param rowKey the row key of the entity to delete; must not be {@code null}
         * @throws IllegalArgumentException if {@code rowKey} is {@code null}, an HBase operation or {@code Any*} wrapper (see
         *         {@link HBaseExecutor#toRowKeyBytes(Object)}) or an empty value wrapper such as {@code Optional.empty()} (which
         *         has no byte representation), or its byte representation is empty or exceeds 32,767 bytes; also
         *         if this mapper's table name violates HBase's namespace or table-qualifier naming rules
         * @throws RuntimeException if converting a row key to bytes invokes a failing string conversion
         * @throws UncheckedIOException if acquiring the table or applying the delete mutations fails with an {@link IOException}
         */
        public void deleteByRowKey(final K rowKey) throws IllegalArgumentException, RuntimeException, UncheckedIOException {
            hbaseExecutor.delete(tableName, AnyDelete.of(rowKey));
        }

        /**
         * Deletes multiple entities by their row keys in a batch operation.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * mapper.deleteByRowKey(N.asList("user123", "user456"));   // void; both rows removed
         *
         * mapper.deleteByRowKey(N.<String> emptyList());   // void; no-op
         * }</pre>
         *
         * @param rowKeys the collection of row keys to delete; {@code null} is treated as an empty
         *        collection and is a no-op
         * @throws IllegalArgumentException if a row key is {@code null}, an HBase operation or {@code Any*} wrapper (see
         *         {@link HBaseExecutor#toRowKeyBytes(Object)}) or an empty value wrapper such as {@code Optional.empty()} (which has
         *         no byte representation), or its byte representation is empty or exceeds 32,767 bytes; also
         *         if this mapper's table name violates HBase's namespace or table-qualifier naming rules
         * @throws RuntimeException if converting a row key to bytes invokes a failing string conversion
         * @throws UncheckedIOException if acquiring the table or applying the delete mutations fails with an {@link IOException}
         */
        public void deleteByRowKey(final Collection<? extends K> rowKeys) throws IllegalArgumentException, RuntimeException, UncheckedIOException {
            final List<AnyDelete> anyDeletes = N.map(rowKeys, AnyDelete::of);

            hbaseExecutor.delete(tableName, anyDeletes);
        }

        /**
         * Tests whether the specified AnyGet operation would return any results on this mapper's table.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * boolean hasEmail = mapper.exists(AnyGet.of("user123").addColumn("info", "email"));
         * }</pre>
         *
         * @param anyGet the AnyGet operation specifying what to check; must not be {@code null}
         * @return {@code true} if the data exists, {@code false} otherwise
         * @throws IllegalArgumentException if {@code anyGet} is {@code null}; also
         *         if this mapper's table name violates HBase's namespace or table-qualifier naming rules
         * @throws UncheckedIOException if acquiring the table or checking row existence fails with an {@link IOException}
         * @see AnyGet
         */
        public boolean exists(final AnyGet anyGet) throws IllegalArgumentException, UncheckedIOException {
            N.checkArgNotNull(anyGet, cs.anyGet);

            return hbaseExecutor.exists(tableName, anyGet.val());
        }

        /**
         * Tests whether the specified AnyGet operations would return any results on this mapper's table.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<Boolean> flags = mapper.exists(N.asList(
         *         AnyGet.of("user123"), AnyGet.of("user456")));
         * // flags is positionally aligned with the input list
         * }</pre>
         *
         * @param anyGets the list of AnyGet operations to check; must not be {@code null} and must
         *        not contain {@code null} elements
         * @return a list of Boolean values corresponding to each AnyGet operation
         * @throws IllegalArgumentException if {@code anyGets} is {@code null} or contains a {@code null} element; also
         *         if this mapper's table name violates HBase's namespace or table-qualifier naming rules
         * @throws UncheckedIOException if acquiring the table or checking row existence fails with an {@link IOException}
         * @see AnyGet
         */
        public List<Boolean> exists(final List<AnyGet> anyGets) throws IllegalArgumentException, UncheckedIOException {
            return hbaseExecutor.exists(tableName, AnyGet.toGet(anyGets));
        }

        /**
         * Retrieves an entity using an AnyGet operation.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Read up to 1000 versions of each cell for this row
         * User user = mapper.get(AnyGet.of("user123").readVersions(1000));   // null if the row is missing
         * }</pre>
         *
         * @param anyGet the AnyGet operation specifying what to retrieve; must not be {@code null}
         * @return the entity object, or the type's default value (typically {@code null} for bean classes)
         *         when no row matches
         * @throws IllegalArgumentException if {@code anyGet} is {@code null}; also
         *         if this mapper's table name violates HBase's namespace or table-qualifier naming rules; or if preparing the target entity's
         *         row-key metadata finds an unsupported property type or a missing getter or setter
         * @throws UncheckedIOException if acquiring the table, fetching rows, or reading result cells fails with an {@link IOException}
         * @throws RuntimeException if converting a cell or row key, creating a target bean, or reading or assigning one of its properties fails
         * @see AnyGet
         */
        public T get(final AnyGet anyGet) throws IllegalArgumentException, UncheckedIOException, RuntimeException {
            return hbaseExecutor.get(tableName, anyGet, targetEntityClass);
        }

        /**
         * Retrieves multiple entities using AnyGet operations.
         *
         * <p>AnyGet operations whose rows do not exist in the table are skipped — the returned list
         * may contain fewer elements than {@code anyGets} and the order does not
         * necessarily correspond positionally to the input list.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<User> users = mapper.get(N.asList(
         *         AnyGet.of("user123"), AnyGet.of("user456")));
         * // users contains only the rows that were found
         * }</pre>
         *
         * @param anyGets the list of AnyGet operations to execute; must not be {@code null} and must
         *        not contain {@code null} elements
         * @return a list of entity objects for the operations whose rows were found
         * @throws IllegalArgumentException if {@code anyGets} is {@code null} or contains a {@code null} element; also
         *         if this mapper's table name violates HBase's namespace or table-qualifier naming rules; or if preparing the target entity's
         *         row-key metadata finds an unsupported property type or a missing getter or setter
         * @throws UncheckedIOException if acquiring the table, fetching rows, or reading result cells fails with an {@link IOException}
         * @throws RuntimeException if converting a cell or row key, creating a target bean, or reading or assigning one of its properties fails
         * @see AnyGet
         */
        public List<T> get(final List<AnyGet> anyGets) throws IllegalArgumentException, UncheckedIOException, RuntimeException {
            return hbaseExecutor.get(tableName, anyGets, targetEntityClass);
        }

        /**
         * Scans a column family and returns a stream of entities.
         *
         * <p>The stream is lazy and owns the underlying table/scanner; always close it
         * (a try-with-resources block) to avoid leaking the table connection.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * try (Stream<User> users = mapper.scan("info")) {
         *     List<User> all = users.toList();
         * }
         * }</pre>
         *
         * @param family the name of the column family to scan; a {@code null} family is accepted here, but HBase cannot serialize it, so consuming the stream fails
         * @return a lazy stream of entity objects
         * @see Stream
         */
        public Stream<T> scan(final String family) {
            return hbaseExecutor.scan(tableName, family, targetEntityClass);
        }

        /**
         * Scans a specific column and returns a stream of entities.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * try (Stream<User> users = mapper.scan("info", "email")) {
         *     users.forEach(System.out::println);
         * }
         * }</pre>
         *
         * @param family the name of the column family; a {@code null} family is accepted here, but HBase cannot serialize it, so consuming the stream fails
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
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * try (Stream<User> users = mapper.scan(Bytes.toBytes("info"))) {
         *     long count = users.count();
         * }
         * }</pre>
         *
         * @param family the column family as a byte array; a {@code null} family is accepted here, but HBase cannot serialize it, so consuming the stream fails
         * @return a lazy stream of entity objects
         * @see Stream
         */
        public Stream<T> scan(final byte[] family) {
            return hbaseExecutor.scan(tableName, family, targetEntityClass);
        }

        /**
         * Scans a specific column (byte arrays) and returns a stream of entities.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * try (Stream<User> users = mapper.scan(Bytes.toBytes("info"), Bytes.toBytes("email"))) {
         *     List<User> all = users.toList();
         * }
         * }</pre>
         *
         * @param family the column family as a byte array; a {@code null} family is accepted here, but HBase cannot serialize it, so consuming the stream fails
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
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * try (Stream<User> users = mapper.scan(AnyScan.create())) {
         *     long total = users.count();   // every row in the mapper's table
         * }
         * }</pre>
         *
         * @param anyScan the AnyScan operation defining the scan parameters; must not be {@code null}
         * @return a lazy stream of entity objects
         * @throws IllegalArgumentException if {@code anyScan} is {@code null}
         * @see AnyScan
         * @see Stream
         */
        public Stream<T> scan(final AnyScan anyScan) throws IllegalArgumentException {
            return hbaseExecutor.scan(tableName, anyScan, targetEntityClass);
        }

        /**
         * Stores data in HBase on this mapper's table using an AnyPut operation.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * mapper.put(AnyPut.of("user123").addColumn("info", "name", "John"));   // void
         * }</pre>
         *
         * @param anyPut the AnyPut operation containing the data to store; must not be {@code null}
         * @throws IllegalArgumentException if {@code anyPut} is {@code null}; also if HBase rejects a mutation with no cells or a cell exceeds the
         *         configured maximum KeyValue size; also
         *         if this mapper's table name violates HBase's namespace or table-qualifier naming rules
         * @throws UncheckedIOException if acquiring the table or writing the supplied mutations fails with an {@link IOException}
         * @see AnyPut
         */
        public void put(final AnyPut anyPut) throws IllegalArgumentException, UncheckedIOException {
            hbaseExecutor.put(tableName, anyPut);
        }

        /**
         * Stores multiple rows of data in HBase on this mapper's table using a list of AnyPut operations.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * mapper.put(N.asList(
         *         AnyPut.of("user123").addColumn("info", "name", "John"),
         *         AnyPut.of("user456").addColumn("info", "name", "Jane")));   // void
         * }</pre>
         *
         * @param anyPuts the list of AnyPut operations to execute; must not be {@code null} and must
         *        not contain {@code null} elements
         * @throws IllegalArgumentException if {@code anyPuts} is {@code null} or contains a {@code null} element; also if HBase rejects a mutation
         *         with no cells or a cell exceeds the configured maximum KeyValue size; also
         *         if this mapper's table name violates HBase's namespace or table-qualifier naming rules
         * @throws UncheckedIOException if acquiring the table or writing the supplied mutations fails with an {@link IOException}
         * @see AnyPut
         */
        public void put(final List<AnyPut> anyPuts) throws IllegalArgumentException, UncheckedIOException {
            hbaseExecutor.put(tableName, anyPuts);
        }

        /**
         * Deletes data from HBase on this mapper's table using an AnyDelete operation.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Delete a single column rather than the whole row
         * mapper.delete(AnyDelete.of("user123").addColumn("info", "email"));   // void
         * }</pre>
         *
         * @param anyDelete the AnyDelete operation specifying what to delete; must not be {@code null}
         * @throws IllegalArgumentException if {@code anyDelete} is {@code null}; also
         *         if this mapper's table name violates HBase's namespace or table-qualifier naming rules
         * @throws UncheckedIOException if acquiring the table or applying the delete mutations fails with an {@link IOException}
         * @see AnyDelete
         */
        public void delete(final AnyDelete anyDelete) throws IllegalArgumentException, UncheckedIOException {
            hbaseExecutor.delete(tableName, anyDelete);
        }

        /**
         * Deletes multiple rows or cells from HBase on this mapper's table using a list of AnyDelete operations.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * mapper.delete(N.asList(
         *         AnyDelete.of("user123"), AnyDelete.of("user456")));   // void; both rows removed
         * }</pre>
         *
         * @param anyDeletes the list of AnyDelete operations to execute; must not be {@code null} and
         *        must not contain {@code null} elements
         * @throws IllegalArgumentException if {@code anyDeletes} is {@code null} or contains a {@code null} element; also
         *         if this mapper's table name violates HBase's namespace or table-qualifier naming rules
         * @throws UncheckedIOException if acquiring the table or applying the delete mutations fails with an {@link IOException}
         * @see AnyDelete
         */
        public void delete(final List<AnyDelete> anyDeletes) throws IllegalArgumentException, UncheckedIOException {
            hbaseExecutor.delete(tableName, anyDeletes);
        }

        /**
         * Performs multiple mutations atomically on a single row on this mapper's table
         * using an AnyRowMutations operation.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * AnyRowMutations rm = AnyRowMutations.of("user123")
         *         .add(new Put(Bytes.toBytes("user123"))
         *                 .addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("John")));   // add(..) throws IOException
         * mapper.mutateRow(rm);                                                                               // void; mutations applied atomically
         * }</pre>
         *
         * @param rm the AnyRowMutations containing the mutations to perform; must not be {@code null}
         * @throws IllegalArgumentException if {@code rm} is {@code null}; also
         *         if this mapper's table name violates HBase's namespace or table-qualifier naming rules
         * @throws UncheckedIOException if acquiring the table or applying the row mutations fails with an {@link IOException}
         * @see AnyRowMutations
         */
        public void mutateRow(final AnyRowMutations rm) throws IllegalArgumentException, UncheckedIOException {
            hbaseExecutor.mutateRow(tableName, rm);
        }

        /**
         * Appends values to one or more columns within a single row on this mapper's table
         * using an AnyAppend operation.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Result result = mapper.append(
         *         AnyAppend.of("user123").addColumn("logs", "access", ",2023-10-15"));
         * // With the default return-results setting, result holds the post-append values.
         * }</pre>
         *
         * @param append the AnyAppend operation specifying the values to append; must not be {@code null}
         * @return the post-append values when return-results is enabled; HBase may return
         *         {@code null} when {@link Append#setReturnResults(boolean)} is set to {@code false}
         * @throws IllegalArgumentException if {@code append} is {@code null}; also if HBase rejects a mutation with no cells; also
         *         if this mapper's table name violates HBase's namespace or table-qualifier naming rules
         * @throws UncheckedIOException if acquiring the table or appending the cell values fails with an {@link IOException}
         * @see AnyAppend
         */
        public Result append(final AnyAppend append) throws IllegalArgumentException, UncheckedIOException {
            return hbaseExecutor.append(tableName, append);
        }

        /**
         * Increments one or more column values within a single row on this mapper's table
         * using an AnyIncrement operation.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Result result = mapper.increment(
         *         AnyIncrement.of("user123").addColumn("stats", "login_count", 1));
         * // With the default return-results setting, result holds the post-increment values.
         * }</pre>
         *
         * @param increment the AnyIncrement operation specifying the values to increment; must not be {@code null}
         * @return the post-increment values when return-results is enabled; callers that set
         *         {@link Increment#setReturnResults(boolean)} to {@code false} must not rely on returned values
         * @throws IllegalArgumentException if {@code increment} is {@code null}; also if HBase rejects a mutation with no cells; also
         *         if this mapper's table name violates HBase's namespace or table-qualifier naming rules
         * @throws UncheckedIOException if acquiring the table or incrementing the cell values fails with an {@link IOException}
         * @see AnyIncrement
         */
        public Result increment(final AnyIncrement increment) throws IllegalArgumentException, UncheckedIOException {
            return hbaseExecutor.increment(tableName, increment);
        }

        /**
         * Atomically increments a single column's numeric value on this mapper's table.
         *
         * <p>Delegates to
         * {@link HBaseExecutor#incrementColumnValue(String, Object, String, String, long)}
         * with the mapper's bound table name.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * long newValue = mapper.incrementColumnValue("user123", "stats", "login_count", 1);
         * }</pre>
         *
         * @param rowKey the row key (converted via {@link HBaseExecutor#toRowKeyBytes(Object)})
         * @param family the column family name (converted via {@link HBaseExecutor#toFamilyQualifierBytes(String)})
         * @param qualifier the column qualifier name (converted via {@link HBaseExecutor#toFamilyQualifierBytes(String)})
         * @param amount the amount to add (negative values decrement)
         * @return the value of the column after the increment
         * @throws IllegalArgumentException if {@code rowKey} is an HBase operation or {@code Any*} wrapper (see
         *         {@link HBaseExecutor#toRowKeyBytes(Object)}), or this mapper's table name violates HBase's namespace or table-qualifier
         *         naming rules
         * @throws UncheckedIOException if acquiring the table or incrementing the column fails with an {@link IOException}, including when HBase
         *         rejects a null row key or family
         * @throws RuntimeException if converting {@code rowKey} to bytes invokes a failing string conversion
         */
        public long incrementColumnValue(final Object rowKey, final String family, final String qualifier, final long amount)
                throws IllegalArgumentException, UncheckedIOException, RuntimeException {
            return hbaseExecutor.incrementColumnValue(tableName, rowKey, family, qualifier, amount);
        }

        /**
         * Atomically increments a single column's numeric value on this mapper's table with the
         * specified {@link Durability} guarantee.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * long newValue = mapper.incrementColumnValue(
         *         "user123", "stats", "login_count", 1, Durability.SKIP_WAL);
         * }</pre>
         *
         * @param rowKey the row key (converted via {@link HBaseExecutor#toRowKeyBytes(Object)})
         * @param family the column family name (converted via {@link HBaseExecutor#toFamilyQualifierBytes(String)})
         * @param qualifier the column qualifier name (converted via {@link HBaseExecutor#toFamilyQualifierBytes(String)})
         * @param amount the amount to add (negative values decrement)
         * @param durability the durability level to use for the WAL write
         * @return the value of the column after the increment
         * @throws IllegalArgumentException if {@code rowKey} is an HBase operation or {@code Any*} wrapper (see
         *         {@link HBaseExecutor#toRowKeyBytes(Object)}), or this mapper's table name violates HBase's namespace or table-qualifier
         *         naming rules
         * @throws UncheckedIOException if acquiring the table or incrementing the column fails with an {@link IOException}, including when HBase
         *         rejects a null row key or family
         * @throws RuntimeException if converting {@code rowKey} to bytes invokes a failing string conversion
         * @see Durability
         */
        public long incrementColumnValue(final Object rowKey, final String family, final String qualifier, final long amount, final Durability durability)
                throws IllegalArgumentException, UncheckedIOException, RuntimeException {
            return hbaseExecutor.incrementColumnValue(tableName, rowKey, family, qualifier, amount, durability);
        }

        /**
         * Byte-array variant of {@link #incrementColumnValue(Object, String, String, long)} on
         * this mapper's table.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * long newValue = mapper.incrementColumnValue(
         *         "user123", Bytes.toBytes("stats"), Bytes.toBytes("login_count"), 1);
         * }</pre>
         *
         * @param rowKey the row key (converted via {@link HBaseExecutor#toRowKeyBytes(Object)})
         * @param family the column family bytes (used as-is)
         * @param qualifier the column qualifier bytes (used as-is)
         * @param amount the amount to add (negative values decrement)
         * @return the value of the column after the increment
         * @throws IllegalArgumentException if {@code rowKey} is an HBase operation or {@code Any*} wrapper (see
         *         {@link HBaseExecutor#toRowKeyBytes(Object)}), or this mapper's table name violates HBase's namespace or table-qualifier
         *         naming rules
         * @throws UncheckedIOException if acquiring the table or incrementing the column fails with an {@link IOException}, including when HBase
         *         rejects a null row key or family
         * @throws RuntimeException if converting {@code rowKey} to bytes invokes a failing string conversion
         */
        public long incrementColumnValue(final Object rowKey, final byte[] family, final byte[] qualifier, final long amount)
                throws IllegalArgumentException, UncheckedIOException, RuntimeException {
            return hbaseExecutor.incrementColumnValue(tableName, rowKey, family, qualifier, amount);
        }

        /**
         * Byte-array variant of {@link #incrementColumnValue(Object, String, String, long, Durability)}
         * on this mapper's table.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * long newValue = mapper.incrementColumnValue(
         *         "user123", Bytes.toBytes("stats"), Bytes.toBytes("login_count"),
         *         1, Durability.SKIP_WAL);
         * }</pre>
         *
         * @param rowKey the row key (converted via {@link HBaseExecutor#toRowKeyBytes(Object)})
         * @param family the column family bytes (used as-is)
         * @param qualifier the column qualifier bytes (used as-is)
         * @param amount the amount to add (negative values decrement)
         * @param durability the durability level to use for the WAL write
         * @return the value of the column after the increment
         * @throws IllegalArgumentException if {@code rowKey} is an HBase operation or {@code Any*} wrapper (see
         *         {@link HBaseExecutor#toRowKeyBytes(Object)}), or this mapper's table name violates HBase's namespace or table-qualifier
         *         naming rules
         * @throws UncheckedIOException if acquiring the table or incrementing the column fails with an {@link IOException}, including when HBase
         *         rejects a null row key or family
         * @throws RuntimeException if converting {@code rowKey} to bytes invokes a failing string conversion
         * @see Durability
         */
        public long incrementColumnValue(final Object rowKey, final byte[] family, final byte[] qualifier, final long amount, final Durability durability)
                throws IllegalArgumentException, UncheckedIOException, RuntimeException {
            return hbaseExecutor.incrementColumnValue(tableName, rowKey, family, qualifier, amount, durability);
        }

        /**
         * Returns a {@link CoprocessorRpcChannel} for the region hosting {@code rowKey} on this
         * mapper's table.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * CoprocessorRpcChannel channel = mapper.coprocessorService("user123");
         * MyService.BlockingInterface stub = MyService.newBlockingStub(channel);
         * MyResponse response = stub.myMethod(null, request);
         * }</pre>
         *
         * @param rowKey the row key identifying the target region (converted via {@link HBaseExecutor#toRowKeyBytes(Object)})
         * @return a CoprocessorRpcChannel pointed at the region hosting {@code rowKey}
         * @throws IllegalArgumentException if {@code rowKey} is an HBase operation or {@code Any*} wrapper (see
         *         {@link HBaseExecutor#toRowKeyBytes(Object)}), or this mapper's table name violates HBase's namespace or table-qualifier
         *         naming rules
         * @throws UncheckedIOException if acquiring the table fails with an {@link IOException}
         * @throws RuntimeException if converting {@code rowKey} to bytes invokes a failing string conversion
         * @see CoprocessorRpcChannel
         */
        public CoprocessorRpcChannel coprocessorService(final Object rowKey) throws IllegalArgumentException, UncheckedIOException, RuntimeException {
            return hbaseExecutor.coprocessorService(tableName, rowKey);
        }

        /**
         * Invokes a coprocessor {@link Service} on every region from the region containing
         * {@code startRowKey} through the region containing {@code endRowKey}, inclusive, and
         * returns the per-region results.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<byte[], Long> counts = mapper.coprocessorService(
         *         MyService.class, "user_a", "user_z",
         *         instance -> instance.countRows());
         * // one entry per selected region from user_a through user_z, inclusive
         * }</pre>
         *
         * @param <S> the service type
         * @param <R> the result type
         * @param service the service interface class
         * @param startRowKey the start row key (inclusive; {@code null} for unbounded start)
         * @param endRowKey the end row key (inclusive; {@code null} for unbounded end)
         * @param callable the callable to execute on each region
         * @return a map from region name bytes to the result returned by {@code callable} for that region
         * @throws IllegalArgumentException if {@code callable} is {@code null}, or {@code startRowKey} or {@code endRowKey} is an HBase operation or
         *         {@code Any*} wrapper (see {@link HBaseExecutor#toRowKeyBytes(Object)}); also
         *         if this mapper's table name violates HBase's namespace or table-qualifier naming rules
         * @throws UncheckedIOException if acquiring the table or invoking the coprocessor fails with an {@link IOException}
         * @throws RuntimeException if converting a row-range boundary to bytes fails, or the coprocessor invocation throws a
         *         non-{@link IOException}, non-{@link Error} throwable (wrapped via
         *         {@code ExceptionUtil.toRuntimeException})
         * @throws Error if converting a row-range boundary or invoking the coprocessor throws an {@link Error}, which is propagated unchanged
         * @see Service
         * @see Batch.Call
         */
        public <S extends Service, R> Map<byte[], R> coprocessorService(final Class<S> service, final Object startRowKey, final Object endRowKey,
                final Batch.Call<S, R> callable) throws IllegalArgumentException, UncheckedIOException, RuntimeException, Error {
            return hbaseExecutor.coprocessorService(tableName, service, startRowKey, endRowKey, callable);
        }

        /**
         * Invokes a coprocessor {@link Service} on every region from the region containing
         * {@code startRowKey} through the region containing {@code endRowKey}, inclusive,
         * delivering each per-region result to {@code callback} as it becomes available.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * mapper.coprocessorService(
         *         MyService.class, "user_a", "user_z",
         *         instance -> instance.countRows(),
         *         (region, row, count) -> total.addAndGet(count));   // callback runs per region
         * }</pre>
         *
         * @param <S> the service type
         * @param <R> the result type
         * @param service the service interface class
         * @param startRowKey the start row key (inclusive; {@code null} for unbounded start)
         * @param endRowKey the end row key (inclusive; {@code null} for unbounded end)
         * @param callable the callable to execute on each region
         * @param callback the callback to receive results from each region
         * @throws IllegalArgumentException if {@code callable} or {@code callback} is {@code null}, or {@code startRowKey} or {@code endRowKey} is
         *         an HBase operation or {@code Any*} wrapper (see {@link HBaseExecutor#toRowKeyBytes(Object)}); also
         *         if this mapper's table name violates HBase's namespace or table-qualifier naming rules
         * @throws UncheckedIOException if acquiring the table or invoking the coprocessor fails with an {@link IOException}
         * @throws Exception if converting a row-range boundary to bytes or invoking the coprocessor throws a non-{@link IOException},
         *         non-{@link Error} throwable: it is rethrown as-is if it is
         *         already an {@link Exception}, otherwise wrapped in a new {@link Exception} (an {@link Error} is always rethrown unchanged)
         * @throws Error if converting a row-range boundary or invoking the coprocessor throws an {@link Error}, which is propagated unchanged
         * @see Service
         * @see Batch.Call
         * @see Batch.Callback
         */
        public <S extends Service, R> void coprocessorService(final Class<S> service, final Object startRowKey, final Object endRowKey,
                final Batch.Call<S, R> callable, final Batch.Callback<R> callback) throws IllegalArgumentException, UncheckedIOException, Exception, Error {
            hbaseExecutor.coprocessorService(tableName, service, startRowKey, endRowKey, callable, callback);
        }

        /**
         * Executes a batch coprocessor service call against a range of rows and returns results.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<byte[], MyResponse> responses = mapper.batchCoprocessorService(
         *         MyService.getDescriptor().findMethodByName("count"),
         *         MyRequest.getDefaultInstance(),
         *         "user_a", "user_z",
         *         MyResponse.getDefaultInstance());
         * // one entry per selected region from user_a through user_z, inclusive
         * }</pre>
         *
         * @param <R> the response message type
         * @param methodDescriptor the Protocol Buffers method descriptor
         * @param request the Protocol Buffers request message
         * @param startRowKey the start row key (inclusive; {@code null} for unbounded start)
         * @param endRowKey the end row key (inclusive; {@code null} for unbounded end)
         * @param responsePrototype the prototype for the response message
         * @return a map of region names to their corresponding response messages
         * @throws IllegalArgumentException if {@code startRowKey} or {@code endRowKey} is an HBase operation or {@code Any*} wrapper (see
         *         {@link HBaseExecutor#toRowKeyBytes(Object)}), or this mapper's table name violates HBase's namespace or table-qualifier
         *         naming rules
         * @throws UncheckedIOException if acquiring the table or invoking the coprocessor fails with an {@link IOException}
         * @throws Exception if converting a row-range boundary to bytes or invoking the coprocessor throws a throwable other than
         *         {@link IOException} or {@link Error}: it is rethrown as-is if it is
         *         already an {@link Exception}, otherwise wrapped in a new {@link Exception} (an {@link Error} is always rethrown unchanged)
         * @throws Error if converting a row-range boundary or invoking the coprocessor throws an {@link Error}, which is propagated unchanged
         * @see Message
         * @see Descriptors.MethodDescriptor
         */
        public <R extends Message> Map<byte[], R> batchCoprocessorService(final Descriptors.MethodDescriptor methodDescriptor, final Message request,
                final Object startRowKey, final Object endRowKey, final R responsePrototype)
                throws IllegalArgumentException, UncheckedIOException, Exception, Error {
            return hbaseExecutor.batchCoprocessorService(tableName, methodDescriptor, request, startRowKey, endRowKey, responsePrototype);
        }

        /**
         * Executes a batch coprocessor service call against a range of rows with a callback for results.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * mapper.batchCoprocessorService(
         *         MyService.getDescriptor().findMethodByName("count"),
         *         MyRequest.getDefaultInstance(),
         *         "user_a", "user_z",
         *         MyResponse.getDefaultInstance(),
         *         (region, row, response) -> total.addAndGet(response.getCount()));   // callback runs per region
         * }</pre>
         *
         * @param <R> the response message type
         * @param methodDescriptor the Protocol Buffers method descriptor
         * @param request the Protocol Buffers request message
         * @param startRowKey the start row key (inclusive; {@code null} for unbounded start)
         * @param endRowKey the end row key (inclusive; {@code null} for unbounded end)
         * @param responsePrototype the prototype for the response message
         * @param callback the callback to receive response messages from each region
         * @throws IllegalArgumentException if {@code callback} is {@code null}, or {@code startRowKey} or {@code endRowKey} is an HBase operation or
         *         {@code Any*} wrapper (see {@link HBaseExecutor#toRowKeyBytes(Object)}); also
         *         if this mapper's table name violates HBase's namespace or table-qualifier naming rules
         * @throws UncheckedIOException if acquiring the table or invoking the coprocessor fails with an {@link IOException}
         * @throws Exception if converting a row-range boundary to bytes or invoking the coprocessor throws a throwable other than
         *         {@link IOException} or {@link Error}: it is rethrown as-is if it is
         *         already an {@link Exception}, otherwise wrapped in a new {@link Exception} (an {@link Error} is always rethrown unchanged)
         * @throws Error if converting a row-range boundary or invoking the coprocessor throws an {@link Error}, which is propagated unchanged
         * @see Message
         * @see Descriptors.MethodDescriptor
         * @see Batch.Callback
         */
        public <R extends Message> void batchCoprocessorService(final Descriptors.MethodDescriptor methodDescriptor, final Message request,
                final Object startRowKey, final Object endRowKey, final R responsePrototype, final Batch.Callback<R> callback)
                throws IllegalArgumentException, UncheckedIOException, Exception, Error {
            hbaseExecutor.batchCoprocessorService(tableName, methodDescriptor, request, startRowKey, endRowKey, responsePrototype, callback);
        }
    }
}
