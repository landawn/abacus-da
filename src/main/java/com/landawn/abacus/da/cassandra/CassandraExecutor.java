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

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.IntFunction;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.landawn.abacus.exception.DuplicateResultException;
import com.landawn.abacus.parser.ParserUtil;
import com.landawn.abacus.parser.ParserUtil.BeanInfo;
import com.landawn.abacus.parser.ParserUtil.PropInfo;
import com.landawn.abacus.pool.KeyedObjectPool;
import com.landawn.abacus.pool.PoolFactory;
import com.landawn.abacus.pool.Poolable;
import com.landawn.abacus.pool.PoolableAdapter;
import com.landawn.abacus.query.AbstractQueryBuilder.SP;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.QueryUtil;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.Beans;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.IntFunctions;
import com.landawn.abacus.util.MutableInt;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.RowDataset;
import com.landawn.abacus.util.SK;
import com.landawn.abacus.util.Strings;
import com.landawn.abacus.util.u.Nullable;
import com.landawn.abacus.util.u.Optional;
import com.landawn.abacus.util.function.Function;
import com.landawn.abacus.util.stream.Stream;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * Primary Cassandra database executor providing high-level CQL operations and result mapping.
 *
 * <p>The CassandraExecutor serves as a sophisticated wrapper around the Cassandra Java Driver,
 * offering simplified database operations while maintaining full control over Cassandra-specific
 * features. It provides both synchronous and asynchronous execution modes, comprehensive parameter
 * binding support, and automatic result set mapping to Java objects.</p>
 *
 * <h2>Key Features</h2>
 * <h3>Multiple Capabilities</h3>
 * <ul>
 * <li><strong>Multiple Parameter Binding Styles:</strong>
 *     <ul>
 *     <li>Positional parameters: {@code SELECT * FROM users WHERE id = ?}</li>
 *     <li>Named parameters: {@code SELECT * FROM users WHERE id = :userId}</li>
 *     <li>Entity binding: Automatically maps entity properties to parameters</li>
 *     <li>Map binding: Uses map keys as parameter names</li>
 *     </ul>
 * </li>
 * <li><strong>Automatic Result Mapping:</strong>
 *     <ul>
 *     <li>Entity mapping: Maps result rows to POJOs</li>
 *     <li>Collection mapping: Supports List, Set, and Map result types</li>
 *     <li>Primitive type extraction: Direct extraction of single values</li>
 *     <li>Custom row mappers: Flexible result transformation</li>
 *     </ul>
 * </li>
 * <li><strong>Cassandra-Specific Operations:</strong>
 *     <ul>
 *     <li>Prepared statement caching with connection pooling</li>
 *     <li>Batch operations with configurable consistency levels</li>
 *     <li>TTL and timestamp support for data expiration</li>
 *     <li>Conditional operations (IF EXISTS, IF NOT EXISTS)</li>
 *     <li>Paging and streaming for large result sets</li>
 *     </ul>
 * </li>
 * <li><strong>Performance Optimizations:</strong>
 *     <ul>
 *     <li>Statement pooling for frequently used queries</li>
 *     <li>Connection reuse and session management</li>
 *     <li>Asynchronous execution with CompletableFuture integration</li>
 *     <li>Efficient type conversion and codec registry support</li>
 *     </ul>
 * </li>
 * </ul>
 *
 * <h3>Basic Usage Examples</h3>
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Initialize executor with session
 * CqlSession session = CqlSession.builder()
 *     .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
 *     .withLocalDatacenter("datacenter1")
 *     .build();
 *
 * CassandraExecutor executor = new CassandraExecutor(session);
 *
 * // Simple query execution
 * List<User> users = executor.list(User.class, "SELECT * FROM users WHERE status = ?", "active");
 *
 * // Entity-based operations
 * User user = new User("john", "john@example.com", "active");
 * executor.insert(user);
 *
 * // Named parameter binding
 * Optional<User> found = executor.findFirst(User.class,
 *     "SELECT * FROM users WHERE email = :email", 
 *     N.asMap("email", "john@example.com"));
 *
 * // Asynchronous operations
 * ContinuableFuture<List<User>> futureUsers = executor.asyncList(User.class, 
 *     "SELECT * FROM users WHERE created_at > ?", yesterday);
 * }</pre>
 *
 * <h3>Advanced Features</h3>
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Custom statement settings
 * StatementSettings settings = StatementSettings.builder()
 *     .consistency(ConsistencyLevel.QUORUM)
 *     .timeout(Duration.ofSeconds(30))
 *     .fetchSize(1000)
 *     .build();
 *
 * CassandraExecutor executor = new CassandraExecutor(session, settings);
 *
 * // Batch operations
 * List<User> users = Arrays.asList(user1, user2, user3);
 * executor.batchInsert(users, BatchType.LOGGED);
 *
 * // Custom type codec registration
 * executor.registerTypeCodec(CustomType.class);
 *
 * // Streaming large result sets
 * try (Stream<User> userStream = executor.stream(User.class, "SELECT * FROM users")) {
 *     userStream.filter(user -> user.isActive())
 *              .forEach(this::processUser);
 * }
 * }</pre>
 *
 * <h3>CQL Builder Integration</h3>
 * <p>This executor integrates seamlessly with the CqlBuilder for dynamic query construction:</p>
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Using CqlBuilder for dynamic queries
 * String cql = NSC.select("id", "name", "email")
 *                 .from("users")
 *                 .where(Filters.eq("status", "active"))
 *                 .and(Filters.gt("created_at", lastWeek))
 *                 .orderBy("created_at")
 *                 .limit(100)
 *                 .cql();
 *
 * List<User> recentUsers = executor.list(User.class, cql);
 * }</pre>
 *
 * <h3>Thread Safety</h3>
 * <p>This class is thread-safe and designed for concurrent use. The underlying Cassandra session
 * and prepared statement pools are managed safely across multiple threads. However, applications
 * should properly manage the session lifecycle and call {@link #close()} when the executor
 * is no longer needed.</p>
 *
 * <h3>Resource Management</h3>
 * <p>The executor implements {@link AutoCloseable} and should be closed properly to release
 * underlying resources:</p>
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * try (CassandraExecutor executor = new CassandraExecutor(session)) {
 *     // Perform database operations
 * }
 * // Executor and session are automatically closed
 * }</pre>
 *
 * @see CqlBuilder
 * @see CqlMapper
 * @see CassandraExecutorBase
 * @see ParsedCql
 * @see com.datastax.oss.driver.api.core.CqlSession
 * @see com.landawn.abacus.query.Filters
 */
@SuppressWarnings("java:S1192")
public final class CassandraExecutor extends CassandraExecutorBase<Row, ResultSet, Statement<?>, PreparedStatement, BatchType> {

    static {
        final BiFunction<Row, Class<?>, Object> converter = (row, rowClass) -> readRow(rowClass, row);

        N.registerConverter(Row.class, converter);
    }

    private static final Map<Integer, Class<?>> protocolCodeDataType = new HashMap<>();

    static {
        protocolCodeDataType.put(ProtocolConstants.DataType.BOOLEAN, Boolean.class);
        protocolCodeDataType.put(ProtocolConstants.DataType.TINYINT, Byte.class);
        protocolCodeDataType.put(ProtocolConstants.DataType.SMALLINT, Short.class);
        protocolCodeDataType.put(ProtocolConstants.DataType.INT, Integer.class);
        protocolCodeDataType.put(ProtocolConstants.DataType.COUNTER, Long.class);
        protocolCodeDataType.put(ProtocolConstants.DataType.BIGINT, Long.class);
        protocolCodeDataType.put(ProtocolConstants.DataType.FLOAT, Float.class);
        protocolCodeDataType.put(ProtocolConstants.DataType.DOUBLE, Double.class);
        protocolCodeDataType.put(ProtocolConstants.DataType.VARINT, BigInteger.class);
        protocolCodeDataType.put(ProtocolConstants.DataType.DECIMAL, BigDecimal.class);

        protocolCodeDataType.put(ProtocolConstants.DataType.DATE, LocalDate.class);
        protocolCodeDataType.put(ProtocolConstants.DataType.TIME, LocalTime.class);
        protocolCodeDataType.put(ProtocolConstants.DataType.TIMESTAMP, Instant.class);

        protocolCodeDataType.put(ProtocolConstants.DataType.ASCII, String.class);
        protocolCodeDataType.put(ProtocolConstants.DataType.VARCHAR, String.class);

        protocolCodeDataType.put(ProtocolConstants.DataType.INET, InetAddress.class);
        protocolCodeDataType.put(ProtocolConstants.DataType.UUID, UUID.class);
        protocolCodeDataType.put(ProtocolConstants.DataType.TIMEUUID, UUID.class);
        protocolCodeDataType.put(ProtocolConstants.DataType.LIST, List.class);
        protocolCodeDataType.put(ProtocolConstants.DataType.SET, Set.class);
        protocolCodeDataType.put(ProtocolConstants.DataType.MAP, Map.class);
        protocolCodeDataType.put(ProtocolConstants.DataType.TUPLE, TupleValue.class);
        protocolCodeDataType.put(ProtocolConstants.DataType.DURATION, CqlDuration.class);
        protocolCodeDataType.put(ProtocolConstants.DataType.UDT, UdtValue.class);
        protocolCodeDataType.put(ProtocolConstants.DataType.BLOB, ByteBuffer.class);
        protocolCodeDataType.put(ProtocolConstants.DataType.CUSTOM, ByteBuffer.class);
    }

    private final KeyedObjectPool<String, PoolableAdapter<BoundStatement>> statementPool = PoolFactory.createKeyedObjectPool(1024, 3000);

    private final KeyedObjectPool<String, PoolableAdapter<PreparedStatement>> preparedStatementPool = PoolFactory.createKeyedObjectPool(1024, 3000);

    private final CqlSession session;

    private final MutableCodecRegistry codecRegistry;

    private final StatementSettings settings;

    /**
     * Creates a new CassandraExecutor with the specified Cassandra session.
     *
     * <p>This constructor initializes the executor with default settings and no CQL mapper.
     * The session should be properly configured with contact points, keyspace, and other
     * connection parameters before being passed to this constructor.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlSession session = CqlSession.builder()
     *     .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
     *     .withLocalDatacenter("datacenter1")
     *     .withKeyspace("my_keyspace")
     *     .build();
     * CassandraExecutor executor = new CassandraExecutor(session);
     * }</pre>
     *
     * @param session the Cassandra session to use for database operations
     * @see CqlSession
     */
    public CassandraExecutor(final CqlSession session) {
        this(session, null);
    }

    /**
     * Creates a new CassandraExecutor with the specified session and statement settings.
     *
     * <p>The statement settings allow you to configure default behavior for all statements
     * executed by this executor, such as consistency levels, timeouts, and fetch sizes.
     * These settings can be overridden on a per-statement basis when needed.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * StatementSettings settings = StatementSettings.builder()
     *     .consistency(ConsistencyLevel.QUORUM)
     *     .timeout(Duration.ofSeconds(30))
     *     .fetchSize(1000)
     *     .build();
     * CassandraExecutor executor = new CassandraExecutor(session, settings);
     * }</pre>
     *
     * @param session the Cassandra session to use for database operations
     * @param settings default statement settings to apply to all operations, or null for defaults
     * @see StatementSettings
     */
    public CassandraExecutor(final CqlSession session, final StatementSettings settings) {
        this(session, settings, null);
    }

    /**
     * Creates a new CassandraExecutor with session, settings, and CQL mapper.
     *
     * <p>The CQL mapper provides access to pre-configured CQL statements stored in external
     * configuration files. This is useful for externalizing complex queries and maintaining
     * them separately from application code.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper cqlMapper = new CqlMapper("queries/user-queries.cql");
     * CassandraExecutor executor = new CassandraExecutor(session, settings, cqlMapper);
     * // Now you can use named queries: executor.execute(cqlMapper.get("findActiveUsers"))
     * }</pre>
     *
     * @param session the Cassandra session to use for database operations
     * @param settings default statement settings, or null for defaults
     * @param cqlMapper CQL mapper containing pre-configured statements, or null if not needed
     * @see CqlMapper
     */
    public CassandraExecutor(final CqlSession session, final StatementSettings settings, final CqlMapper cqlMapper) {
        this(session, settings, cqlMapper, null);
    }

    /**
     * Creates a new CassandraExecutor with full configuration options.
     *
     * <p>This is the most comprehensive constructor, allowing full customization of the executor
     * behavior. The naming policy controls how Java property names are mapped to Cassandra
     * column names, which is essential for entity-based operations.</p>
     *
     * <h4>Naming Policy Examples:</h4>
     * <ul>
     * <li>{@code SNAKE_CASE}: {@code firstName} → {@code first_name}</li>
     * <li>{@code SCREAMING_SNAKE_CASE}: {@code firstName} → {@code FIRST_NAME}</li>
     * <li>{@code CAMEL_CASE}: {@code firstName} → {@code firstName}</li>
     * </ul>
     *
     * @param session the Cassandra session to use for database operations
     * @param settings default statement settings, or null for defaults
     * @param cqlMapper CQL mapper containing pre-configured statements, or null if not needed
     * @param namingPolicy policy for mapping Java property names to column names,
     *                     or null for {@code SNAKE_CASE}
     * @see NamingPolicy
     */
    public CassandraExecutor(final CqlSession session, final StatementSettings settings, final CqlMapper cqlMapper, final NamingPolicy namingPolicy) {
        super(cqlMapper, namingPolicy);
        this.session = session;
        codecRegistry = (MutableCodecRegistry) session.getContext().getCodecRegistry();

        if (settings == null) {
            this.settings = null;
        } else {
            this.settings = new StatementSettings().consistency(settings.consistency())
                    .serialConsistency(settings.serialConsistency())
                    .fetchSize(settings.fetchSize())
                    .timeout(settings.timeout())
                    .traceQuery(settings.traceQuery());
        }

    }

    /**
     * Returns the underlying Cassandra session used by this executor.
     *
     * <p>This provides access to the raw Cassandra session for operations that are not
     * directly supported by this executor. Use with caution, as direct session usage
     * bypasses the executor's caching and optimization features.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlSession session = executor.session();
     * Metadata metadata = session.getMetadata();
     * metadata.getKeyspaces().forEach((name, ks) -> System.out.println("Keyspace: " + name));
     * }</pre>
     *
     * @return the Cassandra session instance
     */
    public CqlSession session() {
        return session;
    }

    /**
     * Registers a custom type codec for the specified Java class.
     *
     * <p>This method registers a string-based codec that can serialize/deserialize
     * objects of the specified class to/from JSON strings. This is useful for storing
     * complex Java objects as text columns in Cassandra.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Register codec for custom class
     * executor.registerTypeCodec(Address.class);
     *
     * // Now Address objects can be used directly in CQL operations
     * User user = new User();
     * user.setAddress(new Address("123 Main St", "City", "12345"));
     * executor.insert(user);   // Address will be automatically serialized
     * }</pre>
     *
     * @param javaClazz the Java class for which to register a type codec
     * @throws NullPointerException if javaClazz is null
     * @see #registerTypeCodec(MutableCodecRegistry, Class)
     */
    public void registerTypeCodec(final Class<?> javaClazz) {
        registerTypeCodec(codecRegistry, javaClazz);
    }

    /**
     * Registers a custom type codec for the specified Java class in the given codec registry.
     *
     * <p>This static utility method allows registration of type codecs in external codec
     * registries. The codec will handle automatic JSON serialization/deserialization for
     * the specified class type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MutableCodecRegistry registry = (MutableCodecRegistry) session.getContext().getCodecRegistry();
     * CassandraExecutor.registerTypeCodec(registry, CustomAddress.class);
     * }</pre>
     *
     * @param codecRegistry the mutable codec registry to register the codec in
     * @param javaClazz the Java class for which to register a type codec
     * @throws NullPointerException if either parameter is null
     */
    public static void registerTypeCodec(final MutableCodecRegistry codecRegistry, final Class<?> javaClazz) {
        codecRegistry.register(new StringCodec<>(javaClazz));
    }

    /**
     * Extracts all data from a Cassandra ResultSet into a Dataset.
     *
     * <p>This method converts a Cassandra ResultSet into an in-memory Dataset structure
     * that provides column-oriented access to the result data. This is useful for
     * analytical operations or when you need to work with result data in a tabular format.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ResultSet resultSet = session.execute("SELECT id, name, age FROM users");
     * Dataset dataset = CassandraExecutor.extractData(resultSet);
     * dataset.println();   // Print dataset in tabular format
     * }</pre>
     *
     * @param resultSet the Cassandra ResultSet to extract data from
     * @return a Dataset containing all rows and columns from the result set
     * @throws NullPointerException if resultSet is null
     * @see Dataset
     */
    public static Dataset extractData(final ResultSet resultSet) {
        return extractData(resultSet, null);
    }

    /**
     * Extracts data from a Cassandra ResultSet with type-aware column mapping.
     *
     * <p>This method extracts data from the ResultSet and attempts to perform type
     * conversion based on the target class. When a target class is provided, the method
     * will inspect the class properties to determine appropriate column types and perform
     * necessary conversions.</p>
     *
     * @param resultSet the Cassandra ResultSet to extract data from
     * @param targetClass the entity class
     *                   or null for no type conversion
     * @return a Dataset with type-converted data based on the target class
     * @throws NullPointerException if resultSet is null
     */
    public static Dataset extractData(final ResultSet resultSet, final Class<?> targetClass) {
        final boolean isEntity = Beans.isBeanClass(targetClass);
        final boolean isMap = targetClass != null && Map.class.isAssignableFrom(targetClass);
        final ColumnDefinitions columnDefinitions = resultSet.getColumnDefinitions();
        final int columnCount = columnDefinitions.size();
        final List<Row> rowList = resultSet.all();
        final int rowCount = N.isEmpty(rowList) ? 0 : rowList.size();

        final List<String> columnNameList = new ArrayList<>(columnCount);
        final List<List<Object>> columnList = new ArrayList<>(columnCount);
        final Class<?>[] columnClasses = new Class<?>[columnCount];

        for (int i = 0; i < columnCount; i++) {
            columnNameList.add(columnDefinitions.get(i).getName().asInternal());
            columnList.add(new ArrayList<>(rowCount));
            if (isEntity) {
                final Method method = Beans.getPropGetter(targetClass, columnNameList.get(i));
                columnClasses[i] = method != null ? method.getReturnType() : null;
            } else {
                columnClasses[i] = isMap ? Map.class : Object[].class;
            }
        }

        Object propValue = null;

        for (final Row row : rowList) {
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

        return new RowDataset(columnNameList, columnList);
    }

    /**
     * Converts a Cassandra ResultSet to a typed List of objects.
     *
     * <p>This method processes all rows in the ResultSet and converts each row to an instance
     * of the specified target class. The conversion supports various target types including
     * entity classes, Map.class, arrays, collections, and basic types.</p>
     *
     * <p>Supported target class types:</p>
     * <ul>
     * <li><strong>Entity classes:</strong> POJOs with getter/setter methods matching column names</li>
     * <li><strong>Map.class:</strong> Each row becomes a {@code Map<String, Object>}</li>
     * <li><strong>Row.class:</strong> Returns raw Cassandra Row objects</li>
     * <li><strong>Array classes:</strong> Each row becomes an Object[] or typed array</li>
     * <li><strong>Collection classes:</strong> Each row becomes a List, Set, etc.</li>
     * <li><strong>Basic types:</strong> For single-column results (String, Integer, etc.)</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ResultSet resultSet = session.execute("SELECT id, name, email FROM users");
     *
     * // Convert to entity list
     * List<User> users = CassandraExecutor.toList(resultSet, User.class);
     *
     * // Convert to map list
     * List<Map<String, Object>> userMaps = CassandraExecutor.toList(resultSet, Map.class);
     *
     * // Convert to array list (for SELECT COUNT(*) queries)
     * ResultSet countResult = session.execute("SELECT COUNT(*) FROM users");
     * List<Object[]> counts = CassandraExecutor.toList(countResult, Object[].class);
     * }</pre>
     *
     * @param <T> the type of objects in the returned list
     * @param resultSet the Cassandra ResultSet to convert
     * @param targetClass the entity class
     * @return a List containing all rows converted to the specified type
     * @throws NullPointerException if resultSet or targetClass is null
     */
    public static <T> List<T> toList(final ResultSet resultSet, final Class<T> targetClass) {
        if (targetClass.isAssignableFrom(Row.class)) {
            return (List<T>) resultSet.all();
        }

        final ColumnDefinitions columnDefinitions = resultSet.getColumnDefinitions();
        final List<Row> rowList = resultSet.all();
        final List<T> resultList = new ArrayList<>(rowList.size());
        final Function<? super Row, ? extends T> mapper = createRowMapper(targetClass, columnDefinitions);

        for (final Row row : rowList) {
            resultList.add(mapper.apply(row));
        }

        return resultList;
    }

    /**
     * Converts a single Cassandra Row to an entity object.
     *
     * <p>This method maps column values from a Cassandra Row to properties of the specified
     * entity class. The mapping is performed by matching column names to entity property names,
     * with support for nested property paths and automatic type conversion.</p>
     *
     * <p>The method handles:</p>
     * <ul>
     * <li><strong>Direct mapping:</strong> Column names matching property names exactly</li>
     * <li><strong>Case conversion:</strong> Automatic conversion between naming conventions</li>
     * <li><strong>Nested properties:</strong> Support for dot-notation property paths</li>
     * <li><strong>Type conversion:</strong> Automatic conversion between compatible types</li>
     * <li><strong>Null values:</strong> Proper handling of null column values</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ResultSet resultSet = session.execute("SELECT id, name, email FROM users WHERE id = ?", userId);
     * Row row = resultSet.one();
     *
     * if (row != null) {
     *     User user = CassandraExecutor.toEntity(row, User.class);
     *     System.out.println("User: " + user.getName());
     * }
     *
     * // For nested properties (if supported by entity structure)
     * // Column 'address.street' maps to user.getAddress().setStreet(value)
     * }</pre>
     *
     * @param <T> the type of the entity to create
     * @param row the Cassandra Row containing the data
     * @param entityClass the target entity class with getter/setter methods
     * @return a new instance of the entity class populated with row data
     * @throws NullPointerException if row or entityClass is null
     */
    public static <T> T toEntity(final Row row, final Class<T> entityClass) {
        final ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
        final int columnCount = columnDefinitions.size();

        final Map<String, String> column2FieldNameMap = QueryUtil.getColumn2PropNameMap(entityClass);
        final BeanInfo entityInfo = ParserUtil.getBeanInfo(entityClass);
        final Object entity = entityInfo.createBeanResult();
        PropInfo propInfo = null;
        String propName = null;
        Object propValue = null;
        Class<?> parameterType = null;
        String fieldName = null;

        for (int i = 0; i < columnCount; i++) {
            propName = columnDefinitions.get(i).getName().asInternal();
            propValue = row.getObject(i);

            propInfo = entityInfo.getPropInfo(propName);

            if (propInfo == null && (fieldName = column2FieldNameMap.get(propName)) != null) {
                propName = fieldName;
                propInfo = entityInfo.getPropInfo(propName);
            }

            if (propInfo == null) {
                if (propName.indexOf(SK._PERIOD) > 0) { //NOSONAR
                    entityInfo.setPropValue(entity, propName, propValue, true);
                }

                continue;
            }

            parameterType = propInfo.clazz;

            if ((propValue == null || parameterType.isAssignableFrom(propValue.getClass())) || !(propValue instanceof Row)) {
                propInfo.setPropValue(entity, propValue);
            } else {
                propInfo.setPropValue(entity, readRow(parameterType, (Row) propValue));
            }
        }

        return entityInfo.finishBeanResult(entity);
    }

    /**
     * Converts a Cassandra Row to a Map with column names as keys.
     *
     * <p>This method extracts all column values from a Cassandra Row and creates
     * a Map where the keys are column names and the values are the corresponding
     * column values. Nested Row objects are recursively converted to Maps.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ResultSet resultSet = session.execute("SELECT id, name, email, address FROM users WHERE id = ?", userId);
     * Row row = resultSet.one();
     *
     * if (row != null) {
     *     Map<String, Object> userMap = CassandraExecutor.toMap(row);
     *     System.out.println("User ID: " + userMap.get("id"));
     *     System.out.println("Name: " + userMap.get("name"));
     *
     *     // If 'address' is a UDT, it will be converted to a nested Map
     *     Map<String, Object> address = (Map<String, Object>) userMap.get("address");
     * }
     * }</pre>
     *
     * @param row the Cassandra Row to convert
     * @return a Map containing all column names and values from the row
     * @throws NullPointerException if row is null
     */
    public static Map<String, Object> toMap(final Row row) {
        return toMap(row, IntFunctions.ofMap());
    }

    /**
     * Converts a Cassandra Row to a Map using a custom Map supplier.
     *
     * <p>This method is similar to {@link #toMap(Row)} but allows you to specify
     * the type of Map implementation to use. This is useful when you need specific
     * Map characteristics like ordering (LinkedHashMap) or concurrent access (ConcurrentHashMap).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ResultSet resultSet = session.execute("SELECT id, name, email FROM users WHERE id = ?", userId);
     * Row row = resultSet.one();
     *
     * if (row != null) {
     *     // Use LinkedHashMap to preserve column order
     *     Map<String, Object> orderedMap = CassandraExecutor.toMap(row, LinkedHashMap::new);
     *
     *     // Use TreeMap for sorted keys
     *     Map<String, Object> sortedMap = CassandraExecutor.toMap(row, size -> new TreeMap<>());
     *
     *     // Use specific initial capacity
     *     Map<String, Object> sizedMap = CassandraExecutor.toMap(row, HashMap::new);
     * }
     * }</pre>
     *
     * @param row the Cassandra Row to convert
     * @param supplier a function that creates a new Map instance with the specified initial capacity
     * @return a Map of the specified type containing all column names and values from the row
     * @throws NullPointerException if row or supplier is null
     */
    public static Map<String, Object> toMap(final Row row, final IntFunction<? extends Map<String, Object>> supplier) {
        final ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
        final int columnCount = columnDefinitions.size();
        final Map<String, Object> map = supplier.apply(columnCount);

        String propName = null;
        Object propValue = null;

        for (int i = 0; i < columnCount; i++) {
            propName = columnDefinitions.get(i).getName().asInternal();
            propValue = row.getObject(i);

            if (propValue instanceof Row) {
                map.put(propName, toMap((Row) propValue, supplier));
            } else {
                map.put(propName, propValue);
            }
        }

        return map;
    }

    @SuppressWarnings("rawtypes")
    private static <T> T readRow(final Class<T> rowClass, final Row row) {
        if (row == null) {
            return rowClass == null ? null : N.defaultValueOf(rowClass);
        }

        final Type<?> rowType = rowClass == null ? null : N.typeOf(rowClass);
        final ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
        final int columnCount = columnDefinitions.size();
        Object res = null;
        Object value = null;

        if (rowType == null || rowType.isObjectArray()) {
            final Object[] a = rowClass == null ? new Object[columnCount] : N.newArray(rowClass.getComponentType(), columnCount);

            for (int i = 0; i < columnCount; i++) {
                value = row.getObject(i);

                if (value instanceof Row) {
                    a[i] = readRow(Object[].class, (Row) value);
                } else {
                    a[i] = value;
                }
            }

            res = a;
        } else if (rowType.isCollection()) {
            final Collection<Object> c = N.newCollection((Class<Collection>) rowClass);

            for (int i = 0; i < columnCount; i++) {
                value = row.getObject(i);

                if (value instanceof Row) {
                    c.add(readRow(List.class, (Row) value));
                } else {
                    c.add(value);
                }
            }

            res = c;
        } else if (rowType.isMap()) {
            res = toMap(row, IntFunctions.ofMap((Class<Map>) rowClass));
        } else if (rowType.isBean()) {
            res = toEntity(row, rowClass);
        } else if (columnCount == 1) {
            value = row.getObject(0);

            if (value == null || rowClass.isAssignableFrom(value.getClass())) {
                res = value;
            } else {
                res = N.convert(value, rowClass);
            }

        } else {
            throw new IllegalArgumentException("Unsupported row/column type: " + ClassUtil.getCanonicalClassName(rowClass));
        }

        return (T) res;
    }

    @SuppressWarnings("rawtypes")
    private static <T> Function<Row, T> createRowMapper(final Class<T> rowClass, final ColumnDefinitions columnDefinitions) {
        final Type<?> rowType = rowClass == null ? null : N.typeOf(rowClass);
        final int columnCount = columnDefinitions.size();

        Function<Row, T> mapper = null;

        if (rowType == null || rowType.isObjectArray()) {
            mapper = row -> {
                final Object[] a = rowClass == null ? new Object[columnCount] : N.newArray(rowClass.getComponentType(), columnCount);
                Object value = null;

                for (int i = 0; i < columnCount; i++) {
                    value = row.getObject(i);

                    if (value instanceof Row) {
                        a[i] = readRow(Object[].class, (Row) value);
                    } else {
                        a[i] = value;
                    }
                }

                return (T) a;
            };

        } else if (rowType.isCollection()) {
            mapper = row -> {
                final Collection<Object> c = N.newCollection((Class<Collection>) rowClass);
                Object value = null;

                for (int i = 0; i < columnCount; i++) {
                    value = row.getObject(i);

                    if (value instanceof Row) {
                        c.add(readRow(List.class, (Row) value));
                    } else {
                        c.add(value);
                    }
                }

                return (T) c;
            };
        } else if (rowType.isMap()) {
            //noinspection rawtypes
            mapper = row -> (T) toMap(row, IntFunctions.ofMap((Class<Map>) rowClass));
        } else if (rowType.isBean()) {
            mapper = row -> toEntity(row, rowClass);
        } else if (columnCount == 1) {
            mapper = new Function<>() {
                private boolean isAssignable = false;
                private Class<?> valueClass = null;

                @Override
                public T apply(final Row row) {
                    if (isAssignable) {
                        return (T) row.getObject(0);
                    }

                    final Object value = row.getObject(0);

                    if (valueClass == null && value != null) {
                        valueClass = value.getClass();
                        isAssignable = rowClass.isAssignableFrom(valueClass);

                        if (isAssignable) {
                            return (T) value;
                        } else {
                            return N.convert(value, rowClass);
                        }
                    } else {
                        return N.convert(value, rowClass);
                    }
                }
            };
        } else {
            throw new IllegalArgumentException("Unsupported row/column type: " + ClassUtil.getCanonicalClassName(rowClass));
        }

        return mapper;
    }

    /**
     * Retrieves a single entity of the specified type from the database through the nullable {@code gett} contract.
     *
     * <p>This method executes a query that is expected to return at most one row,
     * and maps the result to an instance of the specified target class. If no row
     * matches, it returns {@code null}. If more than one row is returned, a
     * {@link DuplicateResultException} is thrown.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Find user by email
     * User user = executor.gett(User.class, null, Filters.eq("email", email));
     * }</pre>
     *
     * @param <T> the type of the entity to retrieve
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param whereClause the WHERE condition
     * @return an instance of the target class populated with data from the first row, or null if no result is found
     * @throws DuplicateResultException if more than one row is returned
     */
    @Override
    public <T> T gett(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) throws DuplicateResultException {
        final SP cp = prepareQuery(targetClass, selectPropNames, whereClause, 2);
        final ResultSet resultSet = execute(cp);
        return fetchOnlyOne(targetClass, resultSet);
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
    @Override
    public <E> Nullable<E> queryForSingleResult(final Class<E> valueClass, final String query, final Object... parameters) {
        final ResultSet resultSet = execute(query, parameters);
        final Row row = resultSet.one();

        return row == null ? (Nullable<E>) Nullable.empty() : Nullable.of(N.convert(row.getObject(0), valueClass));
    }

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
    @Override
    public <E> Optional<E> queryForSingleNonNull(final Class<E> valueClass, final String query, final Object... parameters) {
        final ResultSet resultSet = execute(query, parameters);
        final Row row = resultSet.one();

        return row == null ? (Optional<E>) Optional.empty() : Optional.of(N.convert(row.getObject(0), valueClass));
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
     * @param targetClass the entity class
     * @param query the CQL query string
     * @param parameters the query parameters
     * @return an Optional containing the first result row mapped to the target type, or empty if no results
     * @throws IllegalArgumentException if targetClass or query is null
     */
    @Override
    public <T> Optional<T> findFirst(final Class<T> targetClass, final String query, final Object... parameters) {
        final ResultSet resultSet = execute(query, parameters);
        final Row row = resultSet.one();

        return row == null ? (Optional<T>) Optional.empty() : Optional.of(readRow(targetClass, row));
    }

    /**
     * Executes a CQL query and returns a Stream with a custom row mapper.
     *
     * <p>This method provides fine-grained control over row processing by allowing
     * you to specify a custom mapping function. The mapper receives both the column
     * definitions and each row, enabling sophisticated row processing logic.</p>
     *
     * <p>The returned Stream is lazy and processes rows on-demand, making it memory-efficient
     * for large result sets. However, ensure the Stream is properly closed to release resources.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Custom mapping with column inspection
     * BiFunction<ColumnDefinitions, Row, UserSummary> mapper = (columns, row) -> {
     *     UserSummary summary = new UserSummary();
     *
     *     for (int i = 0; i < columns.size(); i++) {
     *         String columnName = columns.get(i).getName().asInternal();
     *         Object value = row.getObject(i);
     *
     *         // Custom logic based on column names and values
     *         switch (columnName) {
     *             case "full_name" -> summary.setDisplayName((String) value);
     *             case "created_at" -> summary.setAge(calculateAge((Instant) value));
     *             case "status" -> summary.setActive("active".equals(value));
     *         }
     *     }
     *     return summary;
     * };
     *
     * try (Stream<UserSummary> summaries = executor.stream(
     *         "SELECT full_name, created_at, status FROM users WHERE department = ?", 
     *         mapper, "engineering")) {
     *
     *     List<UserSummary> activeSummaries = summaries
     *         .filter(UserSummary::isActive)
     *         .collect(Collectors.toList());
     * }
     * }</pre>
     *
     * @param <T> the type of objects in the returned stream
     * @param query the CQL query string
     * @param rowMapper a function that maps column definitions and rows to result objects
     * @param parameters the query parameters
     * @return a Stream of mapped objects
     * @throws IllegalArgumentException if query or rowMapper is null
     * @see #stream(Class, String, Object...)
     */
    public <T> Stream<T> stream(final String query, final BiFunction<ColumnDefinitions, Row, T> rowMapper, final Object... parameters) {
        N.checkArgNotNull(rowMapper, "rowMapper");

        return Stream.of(execute(query, parameters).iterator()).map(createRowMapper(rowMapper));
    }

    /**
     * Executes a pre-configured CQL Statement and returns a Stream with a custom row mapper.
     *
     * <p>This method is similar to {@link #stream(String, BiFunction, Object...)} but accepts
     * a pre-configured Statement object instead of a query string. This is useful when you need
     * fine-grained control over statement configuration (consistency levels, timeouts, etc.)
     * or when working with complex prepared statements.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create a custom statement with specific settings
     * PreparedStatement preparedStatement = session.prepare(
     *     "SELECT id, name, last_login FROM users WHERE status = ?");
     *
     * BoundStatement statement = preparedStatement.bind("active")
     *     .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
     *     .setTimeout(Duration.ofSeconds(30));
     *
     * // Custom mapper for user activity analysis
     * BiFunction<ColumnDefinitions, Row, UserActivity> mapper = (columns, row) -> {
     *     UUID id = row.getUuid("id");
     *     String name = row.getString("name");
     *     Instant lastLogin = row.getInstant("last_login");
     *
     *     return new UserActivity(id, name, 
     *         lastLogin != null ? Duration.between(lastLogin, Instant.now()) : null);
     * };
     *
     * try (Stream<UserActivity> activities = executor.stream(statement, mapper)) {
     *     activities.filter(activity -> activity.getDaysSinceLogin() > 30)
     *              .forEach(this::sendReactivationEmail);
     * }
     * }</pre>
     *
     * @param <T> the type of objects in the returned stream
     * @param statement the configured CQL Statement to execute
     * @param rowMapper a function that maps column definitions and rows to result objects
     * @return a Stream of mapped objects
     * @throws IllegalArgumentException if statement or rowMapper is null
     */
    public <T> Stream<T> stream(final Statement<?> statement, final BiFunction<ColumnDefinitions, Row, T> rowMapper) {
        N.checkArgNotNull(rowMapper, "rowMapper");

        return Stream.of(execute(statement).iterator()).map(createRowMapper(rowMapper));
    }

    /**
     * Executes a CQL statement without parameters and returns the raw ResultSet.
     *
     * <p>This method executes the provided CQL statement and returns the raw Cassandra
     * ResultSet. This is useful for DDL operations, simple queries without parameters,
     * or when you need direct access to the ResultSet for custom processing.</p>
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
     * @return the raw ResultSet from Cassandra
     * @throws IllegalArgumentException if query is null or empty
     * @throws com.datastax.oss.driver.api.core.AllNodesFailedException if all contact points are unreachable
     * @throws com.datastax.oss.driver.api.core.servererrors.QueryValidationException if the query is invalid
     */
    @Override
    public ResultSet execute(final String query) {
        return session.execute(prepareStatement(query));
    }

    /**
     * Executes a parameterized CQL statement and returns the raw ResultSet.
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
     * @return the raw ResultSet from Cassandra
     * @throws IllegalArgumentException if query is null or if parameter count/names don't match
     * @throws com.datastax.oss.driver.api.core.AllNodesFailedException if all contact points are unreachable
     */
    @Override
    public ResultSet execute(final String query, final Object... parameters) {
        return session.execute(prepareStatement(query, parameters));
    }

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
     * @param query the parameterized CQL statement
     * @param parameters the query parameters
     * @return the raw ResultSet from Cassandra
     * @throws IllegalArgumentException if query is null or if required parameters are missing
     * @throws com.datastax.oss.driver.api.core.AllNodesFailedException if all contact points are unreachable
     */
    @Override
    public ResultSet execute(final String query, final Map<String, Object> parameters) {
        return session.execute(prepareStatement(query, parameters));
    }

    /**
     * Executes a pre-configured CQL Statement and returns the raw ResultSet.
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
     * @return the raw ResultSet from Cassandra
     * @throws IllegalArgumentException if statement is null
     * @throws com.datastax.oss.driver.api.core.AllNodesFailedException if all contact points are unreachable
     */
    @Override
    public ResultSet execute(final Statement<?> statement) {
        return session.execute(statement);
    }

    /**
     * Asynchronously retrieves a single entity of the specified type from the database through the nullable {@code asyncGett} contract.
     *
     * <p>This method executes a query that is expected to return at most one row,
     * and maps the result to an instance of the specified target class. If no row
     * matches, the returned future completes with {@code null}. If more than one row
     * is returned, the future completes exceptionally with
     * {@link DuplicateResultException}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Async find user by email
     * ContinuableFuture<Optional<User>> futureUser = executor.asyncGet(User.class, null, Filters.eq("email", email));
     * }</pre>
     *
     * @param <T> the type of the entity to retrieve
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param whereClause the WHERE condition
     * @return a ContinuableFuture that will complete with an Optional containing the entity instance, or an empty Optional if no result is found
     */
    @Override
    public <T> ContinuableFuture<Optional<T>> asyncGet(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP cp = prepareQuery(targetClass, selectPropNames, whereClause, 2);

        return asyncExecute(cp).map(resultSet -> Optional.ofNullable(fetchOnlyOne(targetClass, resultSet)));
    }

    /**
     * Asynchronously retrieves a single entity of the specified type from the database.
     *
     * <p>This method executes a query that is expected to return at most one row,
     * and maps the result to an instance of the specified target class. If more than
     * one row is returned, a {@link DuplicateResultException} is thrown.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Async find user by email
     * ContinuableFuture<User> futureUser = executor.asyncGett(User.class, null, Filters.eq("email", email));
     * }</pre>
     *
     * @param <T> the type of the entity to retrieve
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param whereClause the WHERE condition
     * @return a ContinuableFuture that completes with an instance of the target class populated with data from the first row,
     *         or {@code null} if no result is found
     */
    @Override
    public <T> ContinuableFuture<T> asyncGett(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP cp = prepareQuery(targetClass, selectPropNames, whereClause, 2);

        return asyncExecute(cp).map(resultSet -> fetchOnlyOne(targetClass, resultSet));
    }

    /**
     * Asynchronously executes a CQL query and returns a single result value.
     *
     * <p>This method executes the query asynchronously and returns a {@link ContinuableFuture}
     * that will complete with the result. This is ideal for non-blocking operations and can
     * improve application performance when dealing with high-latency queries or when you need
     * to execute multiple queries concurrently.</p>
     *
     * <p>The returned {@link ContinuableFuture} provides enhanced functionality over standard
     * {@link java.util.concurrent.CompletableFuture}, including additional composition methods
     * and better exception handling.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Async count query
     * ContinuableFuture<Nullable<Long>> futureCount = executor.asyncQueryForSingleResult(
     *     Long.class, "SELECT COUNT(*) FROM users WHERE status = ?", "active");
     *
     * // Chain operations
     * ContinuableFuture<String> result = futureCount
     *     .map(count -> count.orElse(0L))
     *     .map(count -> "Total active users: " + count);
     *
     * // Handle completion
     * futureCount.thenRunAsync(count -> {
     *     if (count.isPresent()) {
     *         System.out.println("User count: " + count.get());
     *     }
     * });
     *
     * // Multiple concurrent queries
     * ContinuableFuture<Nullable<Long>> activeUsers = executor.asyncQueryForSingleResult(
     *     Long.class, "SELECT COUNT(*) FROM users WHERE status = 'active'");
     * ContinuableFuture<Nullable<Long>> inactiveUsers = executor.asyncQueryForSingleResult(
     *     Long.class, "SELECT COUNT(*) FROM users WHERE status = 'inactive'");
     *
     * ContinuableFuture<String> summary = ContinuableFuture.allOf(activeUsers, inactiveUsers)
     *     .thenCallAsync(v -> String.format("Active: %d, Inactive: %d",
     *                      activeUsers.join().orElse(0L),
     *                      inactiveUsers.join().orElse(0L)));
     * }</pre>
     *
     * @param <T> the type of the expected result value
     * @param valueClass the Java class to convert the result to
     * @param query the CQL query string
     * @param parameters the query parameters
     * @return a ContinuableFuture that will complete with a Nullable containing the result
     * @throws IllegalArgumentException if valueClass or query is null
     * @see ContinuableFuture
     */
    @Override
    public <T> ContinuableFuture<Nullable<T>> asyncQueryForSingleResult(final Class<T> valueClass, final String query, final Object... parameters) {
        return asyncExecute(query, parameters).map(resultSet -> {
            final Row row = resultSet.one();

            return row == null ? Nullable.empty() : Nullable.of(N.convert(row.getObject(0), valueClass));
        });
    }

    /**
     * Asynchronously executes a CQL query and returns a single non-null result value.
     *
     * <p>This method is similar to {@link #asyncQueryForSingleResult}, but it returns an
     * {@link Optional} that will be empty if the result is null. This is useful when you need
     * to distinguish between "no result found" and "result found but value is null".</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Async get active user count (never null for COUNT queries)
     * ContinuableFuture<Optional<Long>> futureActiveUsers = executor.asyncQueryForSingleNonNull(
     *     Long.class, "SELECT COUNT(*) FROM users WHERE status = 'active'");
     *
     * // Async get user email (could be null)
     * ContinuableFuture<Optional<String>> futureEmail = executor.asyncQueryForSingleNonNull(
     *     String.class, "SELECT email FROM users WHERE id = ?", userId);
     * }</pre>
     *
     * @param <T> the type of the expected result value
     * @param valueClass the Java class to convert the result to
     * @param query the CQL query string
     * @param parameters the query parameters
     * @return a ContinuableFuture that will complete with an Optional containing the non-null result value, or empty if no result or null value
     */
    @Override
    public <T> ContinuableFuture<Optional<T>> asyncQueryForSingleNonNull(final Class<T> valueClass, final String query, final Object... parameters) {
        return asyncExecute(query, parameters).map(resultSet -> {
            final Row row = resultSet.one();

            return row == null ? Optional.empty() : Optional.of(N.convert(row.getObject(0), valueClass));
        });
    }

    /**
     * Asynchronously finds the first row from a CQL query result and maps it to the specified type.
     *
     * <p>This method executes the query asynchronously and returns a ContinuableFuture that will
     * complete with the first result row mapped to the target type. If no rows are found, an empty
     * Optional is returned.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Async find user by email
     * ContinuableFuture<Optional<User>> futureUser = executor.asyncFindFirst(User.class,
     *     "SELECT * FROM users WHERE email = ? LIMIT 1", email);
     * }</pre>
     *
     * @param <T> the type to map the result row to
     * @param targetClass the entity class
     * @param query the CQL query string
     * @param parameters the query parameters
     * @return a ContinuableFuture that will complete with an Optional containing the first result row mapped to the target type, or empty if no results
     * @throws IllegalArgumentException if targetClass or query is null
     */
    @Override
    public <T> ContinuableFuture<Optional<T>> asyncFindFirst(final Class<T> targetClass, final String query, final Object... parameters) {
        return asyncExecute(query, parameters).map(resultSet -> {
            final Row row = resultSet.one();

            return row == null ? Optional.empty() : Optional.of(readRow(targetClass, row));
        });
    }

    /**
     * Asynchronously executes a CQL query and returns a Stream of Object arrays.
     *
     * <p>This method executes the query asynchronously and returns a {@link ContinuableFuture}
     * that will complete with a {@link Stream} of rows, where each row is represented as an
     * Object array. This is useful for processing large result sets in a memory-efficient
     * manner.</p>
     *
     * <p>The returned Stream is lazy and processes rows on-demand. Ensure the Stream is
     * properly closed to release resources.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ContinuableFuture<Stream<Object[]>> futureStream = executor.asyncStream(
     *     "SELECT id, name, email FROM users WHERE status = ?", "active");
     *
     * futureStream.thenRunAsync(stream -> {
     *     try (Stream<Object[]> rows = stream) {
     *         rows.forEach(row -> {
     *             UUID id = (UUID) row[0];
     *             String name = (String) row[1];
     *             String email = (String) row[2];
     *             // Process row...
     *         });
     *     }
     * });
     * }</pre>
     *
     * @param query the CQL query string to execute
     * @param parameters the query parameters
     * @return a {@link ContinuableFuture} that will complete with a {@link Stream} of Object arrays
     * @throws IllegalArgumentException if the query is null or empty
     * @see ContinuableFuture
     * @see Stream
     */
    @Override
    public ContinuableFuture<Stream<Object[]>> asyncStream(final String query, final Object... parameters) {
        return asyncExecute(query, parameters).map(resultSet -> {
            final MutableInt columnCount = MutableInt.of(0);

            return Stream.of(resultSet.iterator()).map(row -> {
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
            });
        });
    }

    /**
     * Asynchronously executes a CQL query and returns a ContinuableFuture of Stream with custom row mapping.
     *
     * <p>This method combines asynchronous execution with custom row mapping, allowing you to
     * process query results with sophisticated mapping logic while maintaining non-blocking
     * operation. The returned ContinuableFuture will complete with a Stream that can be
     * processed when the query execution finishes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Custom mapper for data transformation
     * BiFunction<ColumnDefinitions, Row, UserStats> mapper = (columns, row) -> {
     *     return new UserStats(
     *         row.getUuid("id"),
     *         row.getString("name"),
     *         row.getLong("login_count"),
     *         row.getInstant("last_active")
     *     );
     * };
     *
     * ContinuableFuture<Stream<UserStats>> futureStream = executor.asyncStream(
     *     "SELECT id, name, login_count, last_active FROM user_stats WHERE department = ?",
     *     mapper,
     *     "engineering"
     * );
     *
     * // Process results when available
     * ContinuableFuture<List<UserStats>> futureActiveUsers = futureStream
     *     .map(stream -> {
     *         try (stream) {
     *             return stream
     *                 .filter(stats -> stats.getLastActive().isAfter(Instant.now().minus(Duration.ofDays(7))))
     *                 .collect(Collectors.toList());
     *         }
     *     });
     *
     * futureActiveUsers.thenRunAsync(activeUsers -> {
     *     System.out.println("Found " + activeUsers.size() + " recently active users");
     * });
     * }</pre>
     *
     * @param <T> the type of objects in the returned stream
     * @param query the CQL query string
     * @param rowMapper a function that maps column definitions and rows to result objects
     * @param parameters the query parameters
     * @return a ContinuableFuture that will complete with a Stream of mapped objects
     * @throws IllegalArgumentException if query or rowMapper is null
     */
    public <T> ContinuableFuture<Stream<T>> asyncStream(final String query, final BiFunction<ColumnDefinitions, Row, T> rowMapper, final Object... parameters) {
        return asyncExecute(query, parameters).map(resultSet -> Stream.of(resultSet.iterator()).map(createRowMapper(rowMapper)));
    }

    /**
     * Asynchronously executes a pre-configured Statement and returns a ContinuableFuture of Stream with custom row mapping.
     *
     * <p>This method provides asynchronous execution of pre-configured Statement objects with
     * custom row mapping capabilities. It's useful when you need both fine-grained control
     * over statement configuration and sophisticated row processing logic, while maintaining
     * non-blocking operation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Configure statement with specific settings
     * PreparedStatement preparedStatement = session.prepare(
     *     "SELECT user_id, action, timestamp, metadata FROM activity_log WHERE date_bucket = ?");
     *
     * BoundStatement statement = preparedStatement.bind(LocalDate.now())
     *     .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE)
     *     .setPageSize(5000);
     *
     * // Custom mapper for activity analysis
     * BiFunction<ColumnDefinitions, Row, ActivityEvent> mapper = (columns, row) -> {
     *     UUID userId = row.getUuid("user_id");
     *     String action = row.getString("action");
     *     Instant timestamp = row.getInstant("timestamp");
     *     String metadata = row.getString("metadata");
     *
     *     return new ActivityEvent(userId, action, timestamp, 
     *         metadata != null ? parseMetadata(metadata) : Collections.emptyMap());
     * };
     *
     * ContinuableFuture<Stream<ActivityEvent>> futureStream = 
     *     executor.asyncStream(statement, mapper);
     *
     * // Chain processing operations
     * ContinuableFuture<Map<String, Long>> actionCounts = futureStream
     *     .map(stream -> {
     *         try (stream) {
     *             return stream
     *                 .filter(event -> event.getTimestamp().isAfter(Instant.now().minus(Duration.ofHours(1))))
     *                 .collect(Collectors.groupingBy(
     *                     ActivityEvent::getAction,
     *                     Collectors.counting()));
     *         }
     *     });
     * }</pre>
     *
     * @param <T> the type of objects in the returned stream
     * @param statement the configured CQL Statement to execute
     * @param rowMapper a function that maps column definitions and rows to result objects
     * @return a ContinuableFuture that will complete with a Stream of mapped objects
     * @throws IllegalArgumentException if statement or rowMapper is null
     */
    public <T> ContinuableFuture<Stream<T>> asyncStream(final Statement<?> statement, final BiFunction<ColumnDefinitions, Row, T> rowMapper) {
        return asyncExecute(statement).map(resultSet -> Stream.of(resultSet.iterator()).map(createRowMapper(rowMapper)));
    }

    /**
     * Asynchronously executes a CQL query and returns a ContinuableFuture of ResultSet.
     *
     * <p>This method executes the provided CQL query asynchronously and returns a
     * ContinuableFuture that will complete with the ResultSet. This is useful for
     * non-blocking operations and can improve application performance when dealing
     * with high-latency queries or when you need to execute multiple queries concurrently.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Async execute query
     * ContinuableFuture<ResultSet> futureResults = executor.asyncExecute(
     *     "SELECT id, name, email FROM users WHERE status = ?", "active");
     *
     * // Handle completion
     * futureResults.thenRunAsync(results -> {
     *     for (Row row : results) {
     *         UUID id = row.getUuid("id");
     *         String name = row.getString("name");
     *         String email = row.getString("email");
     *         // Process row...
     *     }
     * });
     * }</pre>
     *
     * @param query the CQL query string to execute
     * @return a ContinuableFuture that will complete with the ResultSet from Cassandra
     */
    @Override
    public ContinuableFuture<ResultSet> asyncExecute(final String query) {
        return ContinuableFuture.wrap(session.executeAsync(prepareStatement(query)).toCompletableFuture()).map(ResultSets::wrap);
    }

    /**
     * Asynchronously executes a CQL query with the provided parameters and returns a ContinuableFuture of ResultSet.
     *
     * <p>This method executes the given query asynchronously, allowing non-blocking operations.
     * It supports parameterized queries with variable arguments, which are automatically bound
     * to the query. The returned {@link ContinuableFuture} can be used to handle the result
     * once the query execution is complete.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ContinuableFuture<ResultSet> futureResults = executor.asyncExecute(
     *     "SELECT * FROM users WHERE status = ?", "active");
     *
     * futureResults.thenRunAsync(results -> {
     *     for (Row row : results) {
     *         UUID id = row.getUuid("id");
     *         String name = row.getString("name");
     *         // Process row...
     *     }
     * });
     * }</pre>
     *
     * @param query the CQL query string to execute. Must not be null or empty.
     * @param parameters the query parameters
     * @return a {@link ContinuableFuture} that will complete with the {@link ResultSet} from Cassandra.
     * @throws IllegalArgumentException if the query is null or empty.
     * @see ContinuableFuture
     * @see ResultSet
     */
    @Override
    public ContinuableFuture<ResultSet> asyncExecute(final String query, final Object... parameters) {
        return ContinuableFuture.wrap(session.executeAsync(prepareStatement(query, parameters)).toCompletableFuture()).map(ResultSets::wrap);
    }

    /**
     * Asynchronously executes a CQL query with the provided named parameters and returns a ContinuableFuture of ResultSet.
     *
     * <p>This method allows executing parameterized CQL queries using a Map of named parameters.
     * The parameter names in the query should match the keys in the provided Map. This approach
     * improves readability and reduces errors compared to positional parameters.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, Object> params = new HashMap<>();
     * params.put("status", "active");
     * params.put("department", "engineering");
     *
     * ContinuableFuture<ResultSet> futureResults = executor.asyncExecute(
     *     "SELECT * FROM users WHERE status = :status AND department = :department", params);
     *
     * futureResults.thenRunAsync(results -> {
     *     for (Row row : results) {
     *         UUID id = row.getUuid("id");
     *         String name = row.getString("name");
     *         // Process row...
     *     }
     * });
     * }</pre>
     *
     * @param query the CQL query string with named parameters. Must not be null or empty.
     * @param parameters the query parameters
     *                   Must not be null or empty.
     * @return a {@link ContinuableFuture} that will complete with the {@link ResultSet} from Cassandra.
     * @throws IllegalArgumentException if the query is null, empty, or if required parameters are missing.
     * @see ContinuableFuture
     * @see ResultSet
     */
    @Override
    public ContinuableFuture<ResultSet> asyncExecute(final String query, final Map<String, Object> parameters) {
        return ContinuableFuture.wrap(session.executeAsync(prepareStatement(query, parameters)).toCompletableFuture()).map(ResultSets::wrap);
    }

    /**
     * Asynchronously executes a pre-configured CQL Statement and returns a ContinuableFuture of ResultSet.
     *
     * <p>This method executes the provided Statement asynchronously, allowing non-blocking operations.
     * It supports any type of Cassandra Statement object, including BoundStatement, BatchStatement,
     * or SimpleStatement. The returned ContinuableFuture can be used to handle the result once the
     * query execution is complete.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Prepare a statement
     * PreparedStatement preparedStatement = session.prepare("SELECT * FROM users WHERE status = ?");
     * BoundStatement boundStatement = preparedStatement.bind("active");
     *
     * ContinuableFuture<ResultSet> futureResults = executor.asyncExecute(boundStatement);
     *
     * futureResults.thenRunAsync(results -> {
     *     for (Row row : results) {
     *         UUID id = row.getUuid("id");
     *         String name = row.getString("name");
     *         // Process row...
     *     }
     * });
     * }</pre>
     *
     * @param statement the configured CQL Statement to execute. Must not be null.
     * @return a {@link ContinuableFuture} that will complete with the {@link ResultSet} from Cassandra.
     */
    @Override
    public ContinuableFuture<ResultSet> asyncExecute(final Statement<?> statement) {
        return ContinuableFuture.wrap(session.executeAsync(statement).toCompletableFuture()).map(ResultSets::wrap);
    }

    /**
     * Closes this executor and releases all associated resources.
     *
     * <p>This method closes the underlying Cassandra session and releases all cached
     * prepared statements and bound statements. After calling this method, the executor
     * should not be used for further database operations.</p>
     *
     * <p>This method is idempotent - calling it multiple times has the same effect as
     * calling it once. It's recommended to use try-with-resources or explicit close()
     * calls to ensure proper resource cleanup.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Using try-with-resources (recommended)
     * try (CassandraExecutor executor = new CassandraExecutor(session)) {
     *     // Perform database operations
     * } // Automatically closed
     *
     * // Manual close
     * CassandraExecutor executor = new CassandraExecutor(session);
     * try {
     *     // Perform database operations
     * } finally {
     *     executor.close();
     * }
     * }</pre>
     *
     * @see AutoCloseable
     */
    @Override
    public void close() {
        try {
            if (!session.isClosed()) {
                session.close();
            }
        } finally {
            try {
                statementPool.close();
            } finally {
                preparedStatementPool.close();
            }
        }
    }

    @Override
    protected BatchStatement prepareBatchInsertStatement(final Collection<?> entities, final BatchType type) {
        N.checkArgument(N.notEmpty(entities), "'entities' can't be null or empty.");
        N.checkElementNotNull(entities);

        BatchStatement stmt = prepareBatchStatement(type);
        SP cp = null;

        for (final Object entity : entities) {
            cp = prepareInsert(entity);
            stmt = stmt.add(prepareStatement(cp.query(), cp.parameters().toArray()));
        }

        return stmt;
    }

    @Override
    protected BatchStatement prepareBatchInsertStatement(final Class<?> targetClass, final Collection<? extends Map<String, Object>> propsList,
            final BatchType type) {
        N.checkArgument(N.notEmpty(propsList), "'propsList' can't be null or empty.");
        N.checkElementNotNull(propsList);

        BatchStatement stmt = prepareBatchStatement(type);
        SP cp = null;

        for (final Map<String, Object> props : propsList) {
            cp = prepareInsert(targetClass, props);
            stmt = stmt.add(prepareStatement(cp.query(), cp.parameters().toArray()));
        }

        return stmt;
    }

    @Override
    protected BatchStatement prepareBatchUpdateStatement(final Collection<?> entities, final Collection<String> propNamesToUpdate, final BatchType type) {
        N.checkArgument(N.notEmpty(entities), "'entities' can't be null or empty.");
        N.checkArgument(N.notEmpty(propNamesToUpdate), "'propNamesToUpdate' can't be null or empty");
        N.checkElementNotNull(entities);

        BatchStatement stmt = prepareBatchStatement(type);

        for (final Object entity : entities) {
            final SP cp = prepareUpdate(entity, propNamesToUpdate);
            stmt = stmt.add(prepareStatement(cp.query(), cp.parameters().toArray()));
        }

        return stmt;
    }

    @Override
    protected BatchStatement prepareBatchUpdateStatement(final Class<?> targetClass, final Collection<? extends Map<String, Object>> propsList,
            final BatchType type) {
        N.checkArgument(N.notEmpty(propsList), "'propsList' can't be null or empty.");
        N.checkElementNotNull(propsList);

        final Set<String> primaryKeyNames = getKeyNameSet(targetClass);
        BatchStatement stmt = prepareBatchStatement(type);

        for (final Map<String, Object> props : propsList) {
            final Map<String, Object> tmp = new HashMap<>(props);
            final List<Condition> conds = new ArrayList<>(primaryKeyNames.size());

            for (final String keyName : primaryKeyNames) {
                conds.add(Filters.eq(keyName, tmp.remove(keyName)));
            }

            final SP cp = prepareUpdate(targetClass, tmp, Filters.and(conds));
            stmt = stmt.add(prepareStatement(cp.query(), cp.parameters().toArray()));
        }

        return stmt;
    }

    @Override
    protected Statement<?> prepareBatchUpdateStatement(final String query, final Collection<?> parametersList, final BatchType type) {
        N.checkArgument(N.notEmpty(parametersList), "'propsList' can't be null or empty.");
        N.checkElementNotNull(parametersList);

        BatchStatement stmt = prepareBatchStatement(type);

        for (final Object params : parametersList) {
            stmt = stmt.add(prepareStatement(query, params));
        }

        return stmt;
    }

    @Override
    protected BatchStatement prepareBatchStatement(final BatchType type) {
        final BatchStatement stmt = BatchStatement.newInstance(type == null ? BatchType.LOGGED : type);

        return configStatement(stmt);
    }

    @Override
    protected BoundStatement prepareStatement(final String query) {
        BoundStatement stmt = null;

        if (query.length() <= POOLABLE_LENGTH) {
            final PoolableAdapter<BoundStatement> wrapper = statementPool.get(query);

            if (wrapper != null) {
                stmt = wrapper.value();
            }
        }

        if (stmt == null) {
            final ParsedCql parseCql = parseCql(query);
            final String cql = parseCql.getParameterizedCql();
            stmt = bind(prepare(cql));

            if (query.length() <= POOLABLE_LENGTH) {
                statementPool.put(query, Poolable.wrap(stmt));
            }
        }

        return stmt;
    }

    @Override
    protected BoundStatement prepareStatement(final String query, final Object... parameters) {
        if (N.isEmpty(parameters)) {
            return prepareStatement(query);
        }

        final ParsedCql parseCql = parseCql(query);
        final String cql = parseCql.getParameterizedCql();

        PreparedStatement preStmt = null;

        if (query.length() <= POOLABLE_LENGTH) {
            final PoolableAdapter<PreparedStatement> wrapper = preparedStatementPool.get(query);
            if (wrapper != null && wrapper.value() != null) {
                preStmt = wrapper.value();
            }
        }

        if (preStmt == null) {
            preStmt = prepare(cql);

            if (query.length() <= POOLABLE_LENGTH) {
                preparedStatementPool.put(query, Poolable.wrap(preStmt));
            }
        }

        final ColumnDefinitions columnDefinitions = preStmt.getVariableDefinitions();
        final int parameterCount = columnDefinitions.size();
        DataType colType = null;
        Class<?> javaClass = null;

        if (parameterCount == 0) {
            return preStmt.bind();
        } else if (N.isEmpty(parameters)) {
            throw new IllegalArgumentException("Null or empty parameters for parameterized query: " + query);
        }

        if (parameterCount == 1 && parameters.length == 1) {
            colType = columnDefinitions.get(0).getType();
            javaClass = protocolCodeDataType.get(colType.getProtocolCode());

            if (parameters[0] == null || (javaClass.isAssignableFrom(parameters[0].getClass()) || codecRegistry.codecFor(colType).accepts(parameters[0]))) {
                return bind(preStmt, parameters);
            } else if (parameters[0] instanceof List && ((List<Object>) parameters[0]).size() == 1) {
                final Object tmp = ((List<Object>) parameters[0]).get(0);

                if (tmp == null || (javaClass.isAssignableFrom(tmp.getClass()) || codecRegistry.codecFor(colType).accepts(tmp))) {
                    return bind(preStmt, tmp);
                }
            }
        }

        Object[] values = parameters;

        if (parameters.length == 1 && (parameters[0] instanceof Map || Beans.isBeanClass(parameters[0].getClass()))) {
            values = new Object[parameterCount];
            final Object parameter_0 = parameters[0];
            final Map<Integer, String> namedParameters = parseCql.namedParameters();
            final boolean isCassandraNamedParameters = N.isEmpty(namedParameters);
            String parameterName = null;
            if (parameter_0 instanceof Map) {
                @SuppressWarnings("unchecked")
                final Map<String, Object> m = (Map<String, Object>) parameter_0;

                for (int i = 0; i < parameterCount; i++) {
                    parameterName = isCassandraNamedParameters ? columnDefinitions.get(i).getName().asInternal() : namedParameters.get(i);

                    if (parameterName == null) {
                        throw new IllegalArgumentException("Parameter name at index " + i + " is null");
                    }

                    values[i] = m.get(parameterName);

                    if ((values[i] == null) && !m.containsKey(parameterName)) {
                        throw new IllegalArgumentException("Missing required parameter: '" + parameterName + "'");
                    }
                }
            } else {
                //noinspection UnnecessaryLocalVariable
                final Object entity = parameter_0;
                final Class<?> clazz = entity.getClass();
                Method propGetMethod = null;

                for (int i = 0; i < parameterCount; i++) {
                    parameterName = isCassandraNamedParameters ? columnDefinitions.get(i).getName().asInternal() : namedParameters.get(i);

                    if (parameterName == null) {
                        throw new IllegalArgumentException("Parameter name at index " + i + " is null");
                    }

                    propGetMethod = Beans.getPropGetter(clazz, parameterName);

                    if (propGetMethod == null) {
                        throw new IllegalArgumentException("Missing required parameter: '" + parameterName + "'");
                    }

                    values[i] = ClassUtil.invokeMethod(entity, propGetMethod);
                }
            }
        } else if ((parameters.length == 1) && (parameters[0] != null)) {
            if (parameters[0] instanceof Object[] && ((((Object[]) parameters[0]).length) >= parseCql.parameterCount())) {
                values = (Object[]) parameters[0];
            } else if (parameters[0] instanceof final Collection<?> c && (c.size() >= parseCql.parameterCount())) {
                values = c.toArray(new Object[0]);
            }
        }

        for (int i = 0; i < parameterCount; i++) {
            colType = columnDefinitions.get(i).getType();
            javaClass = protocolCodeDataType.get(colType.getProtocolCode());

            if (values[i] == null) {
                values[i] = N.defaultValueOf(javaClass);
            } else if (javaClass.isAssignableFrom(values[i].getClass()) || codecRegistry.codecFor(colType).accepts(values[i])) {
                // continue;
            } else {
                try {
                    values[i] = N.convert(values[i], javaClass);
                } catch (final ClassCastException | IllegalArgumentException e) {
                    // Type conversion failed, keep original value
                }
            }
        }

        return bind(preStmt, values.length == parameterCount ? values : N.copyOfRange(values, 0, parameterCount));
    }

    @Override
    protected PreparedStatement prepare(final String query) {
        return session.prepare(query);
    }

    @Override
    protected BoundStatement bind(final PreparedStatement preStmt, final Object... parameters) {
        final BoundStatement stmt = preStmt.bind(parameters);

        return configStatement(stmt);
    }

    protected <T extends Statement<T>> T configStatement(T stmt) {
        if (settings != null) {
            if (settings.consistency() != null) {
                stmt = stmt.setConsistencyLevel(settings.consistency());
            }

            if (settings.serialConsistency() != null) {
                stmt = stmt.setSerialConsistencyLevel(settings.serialConsistency());
            }

            //    if (settings.retryPolicy() != null) {
            //        stmt = stmt.setRetryPolicy(settings.retryPolicy());
            //    }

            if (settings.fetchSize() != null) {
                stmt = stmt.setPageSize(settings.fetchSize());
            }

            if (settings.timeout() != null) {
                stmt = stmt.setTimeout(settings.timeout());
            }

            if (settings.traceQuery() != null) {
                stmt = stmt.setTracing(settings.traceQuery());
            }
        }

        return stmt;
    }

    @Override
    protected <T> List<T> toList(final Class<T> targetClass, final ResultSet rs) {
        return toList(rs, targetClass);
    }

    @Override
    protected Dataset extractData(final Class<?> targetClass, final ResultSet rs) {
        return extractData(rs, targetClass);
    }

    @Override
    protected <T> Function<Row, T> createRowMapper(final Class<T> rowClass) {
        return new Function<>() {
            private Function<Row, T> mapper;

            @Override
            public T apply(final Row row) {
                if (mapper == null) {
                    mapper = createRowMapper(rowClass, row.getColumnDefinitions());
                }

                return mapper.apply(row);
            }
        };
    }

    protected <T> Function<Row, T> createRowMapper(final BiFunction<ColumnDefinitions, Row, T> rowMapper) {
        return new Function<>() {
            private ColumnDefinitions cds = null;

            @Override
            public T apply(final Row row) {
                if (cds == null) {
                    cds = row.getColumnDefinitions();
                }

                return rowMapper.apply(cds, row);
            }
        };
    }

    protected <T> T fetchOnlyOne(final Class<T> targetClass, final ResultSet resultSet) {
        final Iterator<Row> iter = resultSet.iterator();

        if (!iter.hasNext()) {
            return null;
        }

        final Row row = iter.next();

        if (iter.hasNext()) {
            throw new DuplicateResultException();
        }

        return readRow(targetClass, row);
    }

    /**
     * Abstract base class for creating custom User Defined Type (UDT) codecs.
     *
     * <p>This class provides a framework for encoding and decoding Cassandra UDTs to/from Java objects.
     * Subclasses must implement the conversion logic between UdtValue and the target Java type.</p>
     *
     * @param <T> the Java type to encode/decode
     */
    public abstract static class UDTCodec<T> implements TypeCodec<T> {

        private final UserDefinedType cqlType;
        private final GenericType<T> javaType;
        private final Class<T> javaClazz;
        private final TypeCodec<UdtValue> udtValueTypeCodec;

        protected UDTCodec(final UserDefinedType cqlType, final Class<T> javaClazz) {
            this.cqlType = cqlType;
            javaType = GenericType.of(javaClazz);
            this.javaClazz = javaClazz;
            udtValueTypeCodec = TypeCodecs.udtOf(cqlType);
        }

        /**
         * Creates a new instance of a UDTCodec for the specified User Defined Type (UDT) and Java class.
         *
         * <p>This method generates a custom codec for mapping a Cassandra UDT to a Java class.
         * The codec handles serialization and deserialization between the UDT and the Java object.
         * It is particularly useful for working with complex UDTs in Cassandra.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * UserDefinedType userType = session.getMetadata()
         *     .getKeyspace("my_keyspace")
         *     .flatMap(ks -> ks.getUserDefinedType("my_udt"))
         *     .orElseThrow(() -> new IllegalArgumentException("UDT not found"));
         *
         * UDTCodec<MyClass> codec = UDTCodec.create(userType, MyClass.class);
         * }</pre>
         *
         * @param <T> the type of the Java class to map the UDT to
         * @param userType the Cassandra User Defined Type to map
         * @param javaClazz the Java class to map the UDT to
         * @return a new instance of {@link UDTCodec} for the specified UDT and Java class
         */
        public static <T> UDTCodec<T> create(final UserDefinedType userType, final Class<T> javaClazz) {
            return new UDTCodec<>(userType, javaClazz) {
                @Override
                protected T deserialize(final UdtValue udtValue) {
                    if (udtValue == null) {
                        return null;
                    }

                    final int size = userType.getFieldNames().size();

                    if (Collection.class.isAssignableFrom(javaClazz)) {
                        final Collection<Object> coll = N.newCollection((Class<Collection<Object>>) javaClazz, size);

                        for (int i = 0; i < size; i++) {
                            coll.add(udtValue.getObject(i));
                        }

                        return (T) coll;
                    } else if (Map.class.isAssignableFrom(javaClazz)) {
                        final Map<String, Object> map = N.newMap((Class<Map<String, Object>>) javaClazz);
                        final Collection<String> fieldNames = userType.getFieldNames().stream().map(CqlIdentifier::asInternal).toList();

                        for (final String fieldName : fieldNames) {
                            map.put(fieldName, udtValue.getObject(fieldName));
                        }

                        return (T) map;
                    } else if (Beans.isBeanClass(javaClazz)) {
                        final BeanInfo beanInfo = ParserUtil.getBeanInfo(javaClazz);
                        final Collection<String> fieldNames = userType.getFieldNames().stream().map(CqlIdentifier::asInternal).toList();
                        Object targetBean = beanInfo.createBeanResult();
                        PropInfo propInfo = null;

                        for (final String fieldName : fieldNames) {
                            propInfo = beanInfo.getPropInfo(fieldName);

                            if (propInfo != null) {
                                propInfo.setPropValue(targetBean, udtValue.getObject(fieldName));
                            }
                        }

                        targetBean = beanInfo.finishBeanResult(targetBean);

                        return (T) targetBean;
                    } else {
                        throw new IllegalArgumentException("Invalid Java class type: " + javaClazz + ". Expected: Collection, Map, or Bean class");
                    }
                }

                @Override
                protected UdtValue serialize(final T value) {
                    if (value == null) {
                        return null;
                    }

                    final UdtValue udtValue = newUDTValue();

                    if (value instanceof Collection) {
                        final Collection<Object> coll = (Collection<Object>) value;
                        int idx = 0;

                        for (final Object val : coll) {
                            if (val == null) {
                                //noinspection ResultOfMethodCallIgnored
                                udtValue.setToNull(idx++);
                            } else {
                                //noinspection ResultOfMethodCallIgnored
                                udtValue.set(idx++, val, (Class<Object>) val.getClass());
                            }
                        }
                    } else if (value instanceof Map) {
                        final Map<String, Object> map = (Map<String, Object>) value;
                        final Collection<String> fieldNames = userType.getFieldNames().stream().map(CqlIdentifier::asInternal).toList();
                        Object propValue;

                        for (final String fieldName : fieldNames) {
                            propValue = map.get(fieldName);

                            if (propValue == null) {
                                //noinspection ResultOfMethodCallIgnored
                                udtValue.setToNull(fieldName);
                            } else {
                                //noinspection ResultOfMethodCallIgnored
                                udtValue.set(fieldName, propValue, (Class<Object>) propValue.getClass());
                            }
                        }
                    } else if (Beans.isBeanClass(javaClazz)) {
                        final BeanInfo beanInfo = ParserUtil.getBeanInfo(javaClazz);
                        //noinspection UnnecessaryLocalVariable
                        final Object bean = value;
                        final Collection<String> fieldNames = userType.getFieldNames().stream().map(CqlIdentifier::asInternal).toList();
                        PropInfo propInfo = null;
                        Object propValue = null;

                        for (final String fieldName : fieldNames) {
                            propInfo = beanInfo.getPropInfo(fieldName);

                            if (propInfo != null) {
                                propValue = propInfo.getPropValue(bean);

                                if (propValue == null) {
                                    //noinspection ResultOfMethodCallIgnored
                                    udtValue.setToNull(fieldName);
                                } else {
                                    //noinspection ResultOfMethodCallIgnored
                                    udtValue.set(fieldName, propValue, (Class<Object>) propValue.getClass());
                                }
                            }
                        }
                    } else {
                        throw new IllegalArgumentException("Invalid Java class type: " + javaClazz + ". Expected: Collection, Map, or Bean class");
                    }

                    return udtValue;
                }
            };
        }

        /**
         * Creates a new instance of a UDTCodec for the specified User Defined Type (UDT) and Java class.
         *
         * <p>This method generates a custom codec for mapping a Cassandra UDT to a Java class.
         * The codec handles serialization and deserialization between the UDT and the Java object.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * UDTCodec<MyClass> codec = UDTCodec.create(session, "my_keyspace", "my_udt", MyClass.class);
         * }</pre>
         *
         * @param <T> the type of the Java class to map the UDT to
         * @param session the Cassandra Session to use for metadata retrieval
         * @param keySpace the keyspace containing the UDT
         * @param userType the name of the User Defined Type (UDT)
         * @param javaClazz the Java class to map the UDT to
         * @return a new instance of {@link UDTCodec} for the specified UDT and Java class
         */
        public static <T> UDTCodec<T> create(final Session session, final String keySpace, final String userType, final Class<T> javaClazz) {
            return create(session.getMetadata().getKeyspace(keySpace).orElseThrow().getUserDefinedType(userType).orElseThrow(), javaClazz);
        }

        /**
         * Serializes the given value to a ByteBuffer using the UDT codec.
         *
         * <p>This method is deprecated. Use {@link #encode(Object, ProtocolVersion)} instead.</p>
         *
         * @param value the value to serialize
         * @param protocolVersion the protocol version to use for serialization
         * @return a ByteBuffer containing the serialized value
         * @deprecated Use {@link #encode(Object, ProtocolVersion)} instead.
         */
        @Deprecated
        public ByteBuffer serialize(final T value, final ProtocolVersion protocolVersion) {
            return encode(value, protocolVersion);
        }

        /**
         * Deserializes the given ByteBuffer to a value using the UDT codec.
         *
         * <p>This method is deprecated. Use {@link #decode(ByteBuffer, ProtocolVersion)} instead.</p>
         *
         * @param bytes the ByteBuffer containing the serialized value
         * @param protocolVersion the protocol version to use for deserialization
         * @return the deserialized value
         * @deprecated Use {@link #decode(ByteBuffer, ProtocolVersion)} instead.
         */
        @Deprecated
        public T deserialize(final ByteBuffer bytes, final ProtocolVersion protocolVersion) {
            return decode(bytes, protocolVersion);
        }

        /**
         * Encodes the given value to a ByteBuffer using the UDT codec.
         *
         * <p>This method encodes the value into a ByteBuffer suitable for storage in Cassandra.</p>
         *
         * @param value the value to encode
         * @param protocolVersion the protocol version to use for encoding
         * @return a ByteBuffer containing the encoded value
         */
        @Override
        public ByteBuffer encode(final T value, final ProtocolVersion protocolVersion) {
            return udtValueTypeCodec.encode(serialize(value), protocolVersion);
        }

        /**
         * Decodes the given ByteBuffer to a value using the UDT codec.
         *
         * <p>This method decodes a ByteBuffer containing a serialized value into the corresponding Java object.</p>
         *
         * @param bytes the ByteBuffer containing the serialized value
         * @param protocolVersion the protocol version to use for decoding
         * @return the deserialized value
         */
        @Override
        public T decode(final ByteBuffer bytes, final ProtocolVersion protocolVersion) {
            return deserialize(udtValueTypeCodec.decode(bytes, protocolVersion));
        }

        /**
         * Parses the given string value into a Java object of type T.
         *
         * <p>This method converts a JSON string representation of the object into an instance of type T.
         * If the string is empty or equals to {@link #NULL_STR}, it returns null.</p>
         *
         * @param value the JSON string to parse
         * @return an instance of type T, or null if the input is empty or equals to {@link #NULL_STR}
         */
        @Override
        public T parse(final String value) {
            return Strings.isEmpty(value) || NULL_STR.equals(value) ? null : N.fromJson(value, javaClazz);
        }

        /**
         * Formats the given value into a JSON string representation.
         *
         * <p>This method converts the object of type T into its JSON string representation.
         * If the value is null, it returns {@link #NULL_STR}.</p>
         *
         * @param value the value to format
         * @return a JSON string representation of the value, or {@link #NULL_STR} if the value is null
         */
        @Override
        public String format(final T value) {
            return value == null ? NULL_STR : N.toJson(value);
        }

        /**
         * Returns the Java type of this codec.
         *
         * <p>This method returns the generic type of the Java class associated with this codec.</p>
         *
         * @return the Java type of this codec
         */
        @Override
        public GenericType<T> getJavaType() {
            return javaType;
        }

        /**
         * Returns the CQL type of this codec.
         *
         * <p>This method returns the Cassandra User Defined Type (UDT) associated with this codec.</p>
         *
         * @return the CQL type of this codec
         */
        @Override
        public DataType getCqlType() {
            return cqlType;
        }

        protected UdtValue newUDTValue() {
            return cqlType.newValue();
        }

        protected abstract UdtValue serialize(T value);

        protected abstract T deserialize(UdtValue value);
    }

    /**
     * A codec for serializing and deserializing Java objects to/from Cassandra TEXT type.
     *
     * <p>This codec converts Java objects to JSON strings for storage in Cassandra TEXT columns.
     * It supports serialization and deserialization of any Java class that can be represented as JSON.</p>
     *
     * @param <T> the type of the Java class to map
     */
    static class StringCodec<T> implements TypeCodec<T> {
        private static final TypeCodec<String> stringTypeCodec = TypeCodecs.TEXT;
        private final Class<T> javaClazz;
        private final GenericType<T> javaType;

        protected StringCodec(final Class<T> javaClazz) {
            this.javaClazz = javaClazz;
            javaType = GenericType.of(javaClazz);
        }

        /**
         * Serializes the given value to a ByteBuffer using the String/TEXT codec.
         *
         * <p>This method is deprecated. Use {@link #encode(Object, ProtocolVersion)} instead.</p>
         *
         * @param value the value to serialize
         * @param protocolVersion the protocol version to use for serialization
         * @return a ByteBuffer containing the serialized value
         * @deprecated Use {@link #encode(Object, ProtocolVersion)} instead.
         */
        @Deprecated
        public ByteBuffer serialize(final T value, final ProtocolVersion protocolVersion) {
            return stringTypeCodec.encode(serialize(value), protocolVersion);
        }

        /**
         * Deserializes the given ByteBuffer to a value using the String codec.
         *
         * <p>This method is deprecated. Use {@link #decode(ByteBuffer, ProtocolVersion)} instead.</p>
         *
         * @param bytes the ByteBuffer containing the serialized value
         * @param protocolVersion the protocol version to use for deserialization
         * @return the deserialized value
         * @deprecated Use {@link #decode(ByteBuffer, ProtocolVersion)} instead.
         */
        @Deprecated
        public T deserialize(final ByteBuffer bytes, final ProtocolVersion protocolVersion) {
            return deserialize(stringTypeCodec.decode(bytes, protocolVersion));
        }

        /**
         * Encodes the given value to a ByteBuffer using the String codec.
         *
         * <p>This method encodes the value into a ByteBuffer suitable for storage in Cassandra TEXT columns.</p>
         *
         * @param value the value to encode
         * @param protocolVersion the protocol version to use for encoding
         * @return a ByteBuffer containing the encoded value
         */
        @Override
        public ByteBuffer encode(final T value, final ProtocolVersion protocolVersion) {
            return stringTypeCodec.encode(serialize(value), protocolVersion);
        }

        /**
         * Decodes the given ByteBuffer to a value using the String codec.
         *
         * <p>This method decodes a ByteBuffer containing a serialized value into the corresponding Java object.</p>
         *
         * @param bytes the ByteBuffer containing the serialized value
         * @param protocolVersion the protocol version to use for decoding
         * @return the deserialized value
         */
        @Override
        public T decode(final ByteBuffer bytes, final ProtocolVersion protocolVersion) {
            return deserialize(stringTypeCodec.decode(bytes, protocolVersion));
        }

        /**
         * Parses the given string value into a Java object of type T.
         *
         * <p>This method converts a JSON string representation of the object into an instance of type T.
         * If the string is empty or equals to {@link #NULL_STR}, it returns null.</p>
         *
         * @param value the JSON string to parse
         * @return an instance of type T, or null if the input is empty or equals to {@link #NULL_STR}
         */
        @Override
        public T parse(final String value) {
            return Strings.isEmpty(value) || NULL_STR.equals(value) ? null : N.fromJson(value, javaClazz);
        }

        /**
         * Formats the given value into a JSON string representation.
         *
         * <p>This method converts the object of type T into its JSON string representation.
         * If the value is null, it returns {@link #NULL_STR}.</p>
         *
         * @param value the value to format
         * @return a JSON string representation of the value, or {@link #NULL_STR} if the value is null
         */
        @Override
        public String format(final T value) {
            return value == null ? NULL_STR : N.toJson(value);
        }

        /**
         * Returns the Java type of this codec.
         *
         * <p>This method returns the generic type of the Java class associated with this codec.</p>
         *
         * @return the Java type of this codec
         */
        @Override
        public GenericType<T> getJavaType() {
            return javaType;
        }

        /**
         * Returns the CQL type of this codec.
         *
         * <p>This method returns the Cassandra TEXT type associated with this codec.</p>
         *
         * @return the CQL type of this codec
         */
        @Override
        public DataType getCqlType() {
            return DataTypes.TEXT;
        }

        protected String serialize(final T value) {
            return N.toJson(value);
        }

        protected T deserialize(final String value) {
            return N.fromJson(value, javaClazz);
        }
    }

    /**
     * Configuration settings for Cassandra statements.
     *
     * <p>This class encapsulates various execution settings that can be applied to CQL statements,
     * such as consistency levels, timeouts, fetch sizes, and query tracing.</p>
     *
     * <p>This class uses Lombok annotations to provide builder pattern, getters/setters,
     * and both no-argument and all-arguments constructors for convenient instantiation.</p>
     */
    @Builder
    @Data
    @Accessors(fluent = true)
    public static final class StatementSettings {

        private ConsistencyLevel consistency;
        private ConsistencyLevel serialConsistency;
        private Integer fetchSize;
        private Duration timeout;
        private Boolean traceQuery;

        /**
         * Default constructor.
         * Creates a new StatementSettings instance with default values.
         */
        public StatementSettings() {
        }

        /**
         * All-arguments constructor.
         * Creates a new StatementSettings instance with all specified values.
         *
         * @param consistency the consistency level
         * @param serialConsistency the serial consistency level
         * @param fetchSize the fetch size
         * @param timeout the timeout duration
         * @param traceQuery whether to enable query tracing
         */
        public StatementSettings(final ConsistencyLevel consistency, final ConsistencyLevel serialConsistency, final Integer fetchSize, final Duration timeout,
                final Boolean traceQuery) {
            this.consistency = consistency;
            this.serialConsistency = serialConsistency;
            this.fetchSize = fetchSize;
            this.timeout = timeout;
            this.traceQuery = traceQuery;
        }

    }
}
