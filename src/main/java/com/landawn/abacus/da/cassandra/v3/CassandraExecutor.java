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

package com.landawn.abacus.da.cassandra.v3;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.IntFunction;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Duration;
import com.datastax.driver.core.LocalDate;
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
import com.landawn.abacus.da.cassandra.CqlBuilder;
import com.landawn.abacus.da.cassandra.CqlBuilder.NSC;
import com.landawn.abacus.da.cassandra.CqlMapper;
import com.landawn.abacus.da.cassandra.CassandraExecutorBase;
import com.landawn.abacus.da.cassandra.ParsedCql;
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
import com.landawn.abacus.util.ImmutableList;
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
 * Legacy Cassandra database executor for Cassandra Java Driver 3.x compatibility.
 * 
 * <p><strong>IMPORTANT:</strong> This is a legacy implementation designed for compatibility
 * with Cassandra Java Driver 3.x. For new applications, use the modern
 * {@link com.landawn.abacus.da.cassandra.CassandraExecutor} which supports
 * the latest Cassandra Java Driver 4.x with improved performance, better async support,
 * and enhanced features.</p>
 * 
 * <p>This executor provides comprehensive database operations for Cassandra using the
 * DataStax Java Driver 3.x API. It offers the same rich feature set as the modern
 * executor but uses the older driver architecture and API interfaces.</p>
 *
 * <h2>Key Features</h2>
 * <ul>
 * <li><strong>Driver 3.x Support:</strong> Compatible with com.datastax.driver.core.* packages</li>
 * <li><strong>Multiple Parameter Binding:</strong>
 *     <ul>
 *     <li>Positional parameters: {@code SELECT * FROM users WHERE id = ?}</li>
 *     <li>Named parameters: {@code SELECT * FROM users WHERE id = :userId}</li>
 *     <li>Entity parameter binding using reflection</li>
 *     <li>Map-based parameter binding</li>
 *     </ul>
 * </li>
 * <li><strong>Comprehensive Result Mapping:</strong>
 *     <ul>
 *     <li>Automatic POJO mapping with configurable naming policies</li>
 *     <li>Collection and Map result types</li>
 *     <li>Primitive value extraction</li>
 *     <li>Custom row mappers and type codecs</li>
 *     </ul>
 * </li>
 * <li><strong>Cassandra-Specific Features:</strong>
 *     <ul>
 *     <li>Prepared statement caching and pooling</li>
 *     <li>Batch operations with different batch types</li>
 *     <li>TTL and timestamp support</li>
 *     <li>Conditional operations (IF EXISTS, IF NOT EXISTS)</li>
 *     <li>Async operations with Guava ListenableFuture</li>
 *     </ul>
 * </li>
 * <li><strong>Performance Optimizations:</strong>
 *     <ul>
 *     <li>Statement and prepared statement pooling</li>
 *     <li>Type codec registration and caching</li>
 *     <li>Connection reuse through session management</li>
 *     <li>Configurable consistency levels and retry policies</li>
 *     </ul>
 * </li>
 * </ul>
 * 
 * <h3>Migration Path</h3>
 * <p>Applications using this legacy executor should consider migrating to the modern
 * {@link com.landawn.abacus.da.cassandra.CassandraExecutor} for:</p>
 * <ul>
 * <li>Better performance and reduced memory usage</li>
 * <li>Improved async programming model with CompletableFuture</li>
 * <li>Enhanced query builder integration</li>
 * <li>Active development and bug fixes</li>
 * <li>Support for newer Cassandra features</li>
 * </ul>
 * 
 * <h3>Basic Usage Examples</h3>
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Initialize with Cluster and Session (Driver 3.x)
 * Cluster cluster = Cluster.builder()
 *     .addContactPoint("127.0.0.1")
 *     .build();
 * Session session = cluster.connect("mykeyspace");
 * 
 * // Create executor with custom settings
 * StatementSettings settings = StatementSettings.builder()
 *     .consistency(ConsistencyLevel.QUORUM)
 *     .fetchSize(1000)
 *     .readTimeoutMillis(30000)
 *     .build();
 * 
 * CassandraExecutor executor = new CassandraExecutor(session, settings);
 * 
 * // Basic operations
 * List<User> users = executor.list(User.class, "SELECT * FROM users WHERE status = ?", "active");
 * 
 * // Entity operations
 * User user = new User("john", "john@example.com", "active");
 * executor.insert(user);
 * 
 * // Async operations (returns Guava ListenableFuture)
 * ContinuableFuture<List<User>> futureUsers = executor.asyncList(User.class, 
 *     "SELECT * FROM users WHERE created_at > ?", yesterday);
 * }</pre>
 * 
 * <h3>Advanced Features</h3>
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Custom type codec registration
 * executor.registerTypeCodec(CustomAddress.class);
 * 
 * // Batch operations
 * List<User> users = Arrays.asList(user1, user2, user3);
 * executor.batchInsert(users, BatchStatement.Type.LOGGED);
 * 
 * // UDT (User Defined Type) support
 * UserType addressType = cluster.getMetadata()
 *     .getKeyspace("mykeyspace")
 *     .getUserType("address");
 * 
 * UDTCodec<Address> addressCodec = UDTCodec.create(addressType, Address.class);
 * cluster.getConfiguration().getCodecRegistry().register(addressCodec);
 * 
 * // Custom row mapping
 * List<String> userNames = executor.list(String.class, 
 *     "SELECT name FROM users WHERE department = ?", "engineering");
 * }</pre>
 * 
 * <h3>Thread Safety</h3>
 * <p>This class is thread-safe and designed for concurrent use. The underlying Cassandra
 * session and prepared statement pools are managed safely across multiple threads.
 * However, applications should properly manage the session and cluster lifecycle.</p>
 * 
 * <h3>Resource Management</h3>
 * <p>Always properly close resources to avoid connection leaks:</p>
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * try (CassandraExecutor executor = new CassandraExecutor(session)) {
 *     // Perform database operations
 * } finally {
 *     session.close();
 *     cluster.close();
 * }
 * }</pre>
 * 
 * @deprecated Use {@link com.landawn.abacus.da.cassandra.CassandraExecutor}
 *             for new applications (requires Cassandra Java Driver 4.x).
 * @see com.landawn.abacus.da.cassandra.CassandraExecutor
 * @see CqlBuilder
 * @see CqlMapper
 * @see com.datastax.driver.core.Cluster
 * @see com.datastax.driver.core.Session
 * @see com.landawn.abacus.query.Filters
 */
@SuppressWarnings("java:S1192")
public final class CassandraExecutor extends CassandraExecutorBase<Row, ResultSet, Statement, PreparedStatement, BatchStatement.Type> {

    static {
        final BiFunction<Row, Class<?>, Object> converter = CassandraExecutor::readRow;

        N.registerConverter(Row.class, converter);
    }

    //    static final AsyncExecutor DEFAULT_ASYNC_EXECUTOR = new AsyncExecutor(//
    //            N.max(64, IOUtil.CPU_CORES * 8), // coreThreadPoolSize
    //            N.max(128, IOUtil.CPU_CORES * 16), // maxThreadPoolSize
    //            180L, TimeUnit.SECONDS);

    static final ImmutableList<String> EXISTS_SELECT_PROP_NAMES = ImmutableList.of("1");

    static final ImmutableList<String> COUNT_SELECT_PROP_NAMES = ImmutableList.of(NSC.COUNT_ALL);

    static final int POOLABLE_LENGTH = 1024;

    private static final Map<String, Class<?>> namedDataType = new HashMap<>();

    static {
        namedDataType.put(DataType.Name.BOOLEAN.name(), Boolean.class);
        namedDataType.put(DataType.Name.TINYINT.name(), Byte.class);
        namedDataType.put(DataType.Name.SMALLINT.name(), Short.class);
        namedDataType.put(DataType.Name.INT.name(), Integer.class);
        namedDataType.put(DataType.Name.COUNTER.name(), Long.class);
        namedDataType.put(DataType.Name.BIGINT.name(), Long.class);
        namedDataType.put(DataType.Name.FLOAT.name(), Float.class);
        namedDataType.put(DataType.Name.DOUBLE.name(), Double.class);
        namedDataType.put(DataType.Name.VARINT.name(), BigInteger.class);
        namedDataType.put(DataType.Name.DECIMAL.name(), BigDecimal.class);

        namedDataType.put(DataType.Name.TIME.name(), Long.class);
        namedDataType.put(DataType.Name.DATE.name(), LocalDate.class);
        namedDataType.put(DataType.Name.TIMESTAMP.name(), Date.class);

        namedDataType.put(DataType.Name.ASCII.name(), String.class);
        namedDataType.put(DataType.Name.TEXT.name(), String.class);
        namedDataType.put(DataType.Name.VARCHAR.name(), String.class);

        namedDataType.put(DataType.Name.INET.name(), InetAddress.class);
        namedDataType.put(DataType.Name.UUID.name(), UUID.class);
        namedDataType.put(DataType.Name.TIMEUUID.name(), UUID.class);
        namedDataType.put(DataType.Name.LIST.name(), List.class);
        namedDataType.put(DataType.Name.SET.name(), Set.class);
        namedDataType.put(DataType.Name.MAP.name(), Map.class);
        namedDataType.put(DataType.Name.TUPLE.name(), TupleValue.class);
        namedDataType.put(DataType.Name.DURATION.name(), Duration.class);
        namedDataType.put(DataType.Name.UDT.name(), UDTValue.class);
        namedDataType.put(DataType.Name.BLOB.name(), ByteBuffer.class);
        namedDataType.put(DataType.Name.CUSTOM.name(), ByteBuffer.class);
    }

    private final KeyedObjectPool<String, PoolableAdapter<Statement>> statementPool = PoolFactory.createKeyedObjectPool(1024, 3000);

    private final KeyedObjectPool<String, PoolableAdapter<PreparedStatement>> preparedStatementPool = PoolFactory.createKeyedObjectPool(1024, 3000);

    private final Cluster cluster;

    private final Session session;

    private final CodecRegistry codecRegistry;

    private final MappingManager mappingManager;

    private final StatementSettings settings;

    /**
     * Creates a new CassandraExecutor with the specified Cassandra session (Driver 3.x).
     * 
     * <p>This constructor initializes the executor with default settings and no CQL mapper.
     * The session should be obtained from a properly configured Cluster instance with
     * appropriate contact points and keyspace settings.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
     * Session session = cluster.connect("mykeyspace");
     * CassandraExecutor executor = new CassandraExecutor(session);
     * }</pre>
     * 
     * @param session the Cassandra session from Driver 3.x (com.datastax.driver.core.Session)
     * @throws IllegalArgumentException if session is null
     */
    public CassandraExecutor(final Session session) {
        this(session, null);
    }

    /**
     * Creates a new CassandraExecutor with specified session and statement settings.
     * 
     * <p>This constructor allows configuration of default statement execution behavior
     * while using automatic discovery for other settings. The statement settings will
     * be applied to all operations unless overridden on a per-operation basis.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * StatementSettings settings = StatementSettings.builder()
     *     .consistency(ConsistencyLevel.QUORUM)
     *     .fetchSize(1000)
     *     .build();
     * CassandraExecutor executor = new CassandraExecutor(session, settings);
     * }</pre>
     * 
     * @param session the Cassandra session from Driver 3.x
     * @param settings default statement configuration, or null for driver defaults
     * @throws IllegalArgumentException if session is null
     */
    public CassandraExecutor(final Session session, final StatementSettings settings) {
        this(session, settings, null);
    }

    /**
     * Creates a new CassandraExecutor with session, settings, and CQL mapper.
     * 
     * <p>This constructor enables use of pre-configured CQL statements stored in external
     * configuration files through the CQL mapper, while also allowing custom statement
     * execution settings. This is useful for applications that externalize complex queries.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper cqlMapper = CqlMapper.fromFile("queries.xml");
     * CassandraExecutor executor = new CassandraExecutor(session, settings, cqlMapper);
     * // Now you can use named queries: executor.execute(cqlMapper.get("findUserById"), userId);
     * }</pre>
     * 
     * @param session the Cassandra session from Driver 3.x
     * @param settings default statement configuration, or null for driver defaults
     * @param cqlMapper mapper containing pre-configured CQL statements, or null if not needed
     * @throws IllegalArgumentException if session is null
     * @see CqlMapper
     */
    public CassandraExecutor(final Session session, final StatementSettings settings, final CqlMapper cqlMapper) {
        this(session, settings, cqlMapper, null);
    }

    /**
     * Creates a new CassandraExecutor with full configuration options for Driver 3.x.
     * 
     * <p>This constructor provides complete control over executor behavior, including
     * statement execution settings, pre-configured CQL mappings, and property name
     * mapping policies for entity operations.</p>
     * 
     * <h4>StatementSettings Configuration:</h4>
     * <ul>
     * <li><strong>consistency:</strong> Default consistency level (ONE, QUORUM, ALL, etc.)</li>
     * <li><strong>serialConsistency:</strong> Serial consistency for conditional operations</li>
     * <li><strong>retryPolicy:</strong> Retry behavior for failed operations</li>
     * <li><strong>fetchSize:</strong> Number of rows to fetch per page</li>
     * <li><strong>readTimeoutMillis:</strong> Query timeout in milliseconds</li>
     * <li><strong>traceQuery:</strong> Enable query tracing for debugging</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * StatementSettings settings = StatementSettings.builder()
     *     .consistency(ConsistencyLevel.QUORUM)
     *     .serialConsistency(ConsistencyLevel.SERIAL)
     *     .fetchSize(1000)
     *     .readTimeoutMillis(30000)
     *     .traceQuery(false)
     *     .build();
     * 
     * CassandraExecutor executor = new CassandraExecutor(
     *     session, settings, cqlMapper, NamingPolicy.SNAKE_CASE);
     * }</pre>
     * 
     * @param session the Cassandra session from Driver 3.x
     * @param settings default statement configuration, or null for driver defaults
     * @param cqlMapper mapper containing pre-configured CQL statements, or null
     * @param namingPolicy policy for mapping Java property names to Cassandra column names, 
     *                     or null for SNAKE_CASE
     * @throws IllegalArgumentException if session is null
     * @see com.datastax.driver.core.Session#executeAsync
     * @see StatementSettings
     * @see NamingPolicy
     */
    public CassandraExecutor(final Session session, final StatementSettings settings, final CqlMapper cqlMapper, final NamingPolicy namingPolicy) {
        super(cqlMapper, namingPolicy);
        cluster = session.getCluster();
        this.session = session;
        codecRegistry = cluster.getConfiguration().getCodecRegistry();
        mappingManager = new MappingManager(session);

        if (settings == null) {
            this.settings = null;
        } else {
            this.settings = new StatementSettings().consistency(settings.consistency())
                    .serialConsistency(settings.serialConsistency())
                    .retryPolicy(settings.retryPolicy())
                    .fetchSize(settings.fetchSize())
                    .readTimeoutMillis(settings.readTimeoutMillis())
                    .traceQuery(settings.traceQuery());
        }
    }

    /**
     * Returns the underlying Cassandra cluster instance from Driver 3.x.
     * 
     * <p>This provides access to the cluster configuration, metadata, and connection
     * pooling settings. Use with caution as direct cluster manipulation can affect
     * the executor's behavior and connection management.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cluster cluster = executor.cluster();
     * // Access metadata
     * Metadata metadata = cluster.getMetadata();
     * // Get keyspace information
     * KeyspaceMetadata keyspace = metadata.getKeyspace("mykeyspace");
     * }</pre>
     * 
     * @return the Cassandra cluster instance from Driver 3.x
     */
    public Cluster cluster() {
        return cluster;
    }

    /**
     * Returns the underlying Cassandra session from Driver 3.x.
     * 
     * <p>This provides direct access to the session for operations not covered by
     * the executor API. Use with caution as direct session manipulation bypasses
     * the executor's statement management and pooling features.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Session session = executor.session();
     * // Direct statement execution
     * ResultSet rs = session.execute("SELECT * FROM users");
     * }</pre>
     * 
     * @return the Cassandra session instance
     */
    public Session session() {
        return session;
    }

    /**
     * Returns a DataStax object mapper for the specified entity class.
     * 
     * <p>The mapper provides automatic CRUD operations for entities using
     * DataStax's object mapping annotations. This is useful for simple entity
     * operations without writing CQL statements.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * @Table(name = "users")
     * public class User {
     *     @PartitionKey
     *     private UUID id;
     *     private String name;
     *     // getters and setters
     * }
     * 
     * Mapper<User> mapper = executor.mapper(User.class);
     * mapper.save(user);
     * User found = mapper.get(userId);
     * }</pre>
     * 
     * @param <T> the type of the entity
     * @param targetClass the entity class
     * @return a DataStax mapper for the entity class
     */
    public <T> Mapper<T> mapper(final Class<T> targetClass) {
        return mappingManager.mapper(targetClass);
    }

    /**
     * Registers a custom type codec for the specified Java class.
     *
     * <p>This method registers a string-based codec that can serialize/deserialize
     * objects of the specified class to/from JSON strings. This is useful for storing
     * complex Java objects as text columns in Cassandra without having to define
     * User Defined Types (UDTs).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Register codec for custom class
     * executor.registerTypeCodec(Address.class);
     *
     * // Now Address objects can be used directly in CQL operations
     * Address address = new Address("123 Main St", "Springfield", "12345");
     * executor.execute("INSERT INTO users (id, name, address) VALUES (?, ?, ?)",
     *     UUID.randomUUID(), "John Doe", address);
     *
     * // Retrieve and deserialize automatically
     * User user = executor.findFirst(User.class,
     *     "SELECT * FROM users WHERE id = ?", userId).orElse(null);
     * // user.getAddress() returns the Address object automatically
     *
     * // Register multiple custom types
     * executor.registerTypeCodec(ContactInfo.class);
     * executor.registerTypeCodec(PaymentDetails.class);
     * executor.registerTypeCodec(Preferences.class);
     * }</pre>
     *
     * @param javaClazz the Java class for which to register a type codec
     * @throws IllegalArgumentException if javaClazz is null
     * @see #registerTypeCodec(CodecRegistry, Class)
     */
    public void registerTypeCodec(final Class<?> javaClazz) {
        registerTypeCodec(codecRegistry, javaClazz);
    }

    /**
     * Registers a custom type codec for the specified Java class in the given codec registry.
     * 
     * <p>This static method allows registration of type codecs in any codec registry,
     * not just the one associated with this executor. The codec will serialize objects
     * to/from JSON strings for storage in Cassandra text columns.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CodecRegistry registry = cluster.getConfiguration().getCodecRegistry();
     * CassandraExecutor.registerTypeCodec(registry, CustomData.class);
     * }</pre>
     * 
     * @param codecRegistry the codec registry to register the codec in
     * @param javaClazz the Java class to register a codec for
     */
    public static void registerTypeCodec(final CodecRegistry codecRegistry, final Class<?> javaClazz) {
        codecRegistry.register(new StringCodec<>(javaClazz));
    }

    /**
     * Extracts data from a ResultSet into a Dataset with automatic type detection.
     * 
     * <p>This method converts all rows in the ResultSet into a Dataset structure,
     * preserving column names and types. Nested Row objects are automatically
     * converted to appropriate structures.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ResultSet rs = executor.execute("SELECT * FROM users");
     * Dataset dataset = CassandraExecutor.extractData(rs);
     * // Access data by column name
     * List<String> names = dataset.getColumn("name");
     * }</pre>
     * 
     * @param resultSet the Cassandra ResultSet to extract data from
     * @return a Dataset containing all data from the ResultSet
     */
    public static Dataset extractData(final ResultSet resultSet) {
        return extractData(resultSet, null);
    }

    /**
     * Extracts data from a ResultSet into a Dataset with specified target type.
     * 
     * <p>This method converts ResultSet rows into a Dataset, using the target class
     * to determine appropriate type conversions. If targetClass is an entity class,
     * column values will be converted to match the entity's property types.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ResultSet rs = executor.execute("SELECT * FROM users");
     * Dataset dataset = CassandraExecutor.extractData(rs, User.class);
     * // Column types match User property types
     * List<User> users = dataset.toList(User.class);
     * }</pre>
     * 
     * @param resultSet the Cassandra ResultSet to extract data from
     * @param targetClass an entity class with getter/setter methods or Map.class
     * @return a Dataset containing all data from the ResultSet with appropriate type conversions
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
            columnNameList.add(columnDefinitions.getName(i));
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
                    columnList.get(i).add(readRow((Row) propValue, columnClasses[i]));
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
     * Converts a ResultSet to a list of objects of the specified type.
     * 
     * <p>This method supports various target types including entity classes, Maps,
     * Collections, arrays, and single-value types. For entity classes, column names
     * are automatically mapped to property names.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Convert to entity list
     * ResultSet rs = executor.execute("SELECT * FROM users");
     * List<User> users = CassandraExecutor.toList(rs, User.class);
     * 
     * // Convert to Map list
     * List<Map> maps = CassandraExecutor.toList(rs, Map.class);
     * 
     * // Convert single column to value list
     * ResultSet names = executor.execute("SELECT name FROM users");
     * List<String> nameList = CassandraExecutor.toList(names, String.class);
     * }</pre>
     * 
     * @param <T> the target type
     * @param resultSet the Cassandra ResultSet to convert
     * @param targetClass an entity class with getter/setter methods or Map.class
     * @return a list of objects converted from the ResultSet rows
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
     * <li><strong>Case conversion:</strong> Automatic conversion between naming conventions (e.g., snake_case to camelCase)</li>
     * <li><strong>Nested properties:</strong> Support for dot-notation property paths</li>
     * <li><strong>Type conversion:</strong> Automatic conversion between compatible types</li>
     * <li><strong>Null values:</strong> Proper handling of null column values</li>
     * <li><strong>UDT support:</strong> Automatic conversion of User Defined Types</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ResultSet resultSet = session.execute("SELECT id, name, email FROM users WHERE id = ?", userId);
     * Row row = resultSet.one();
     *
     * if (row != null) {
     *     User user = CassandraExecutor.toEntity(row, User.class);
     *     System.out.println("User: " + user.getName() + " (" + user.getEmail() + ")");
     * }
     *
     * // With UDT (User Defined Type)
     * Row addressRow = session.execute("SELECT id, name, address FROM users WHERE id = ?", userId).one();
     * User userWithAddress = CassandraExecutor.toEntity(addressRow, User.class);
     * // address UDT is automatically mapped to Address object
     *
     * // Batch conversion
     * ResultSet allUsers = session.execute("SELECT * FROM users");
     * for (Row userRow : allUsers) {
     *     User user = CassandraExecutor.toEntity(userRow, User.class);
     *     processUser(user);
     * }
     * }</pre>
     *
     * @param <T> the entity type
     * @param row the Cassandra Row to convert
     * @param entityClass the target entity class with getter/setter methods
     * @return an instance of the entity class populated with row data
     * @throws IllegalArgumentException if row or entityClass is null
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
            propName = columnDefinitions.getName(i);
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
                propInfo.setPropValue(entity, readRow((Row) propValue, parameterType));
            }
        }

        return entityInfo.finishBeanResult(entity);
    }

    /**
     * Converts a Cassandra Row to a Map with default map implementation.
     * 
     * <p>This method creates a HashMap containing all column name-value pairs
     * from the Row. Nested Row objects are recursively converted to Maps.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Row row = resultSet.one();
     * Map<String, Object> map = CassandraExecutor.toMap(row);
     * Object value = map.get("column_name");
     * }</pre>
     * 
     * @param row the Cassandra Row to convert
     * @return a Map containing all column name-value pairs from the row
     */
    public static Map<String, Object> toMap(final Row row) {
        return toMap(row, IntFunctions.ofMap());
    }

    /**
     * Converts a Cassandra Row to a Map using a custom map supplier.
     * 
     * <p>This method allows specification of the Map implementation to use.
     * The supplier receives the expected map size as a parameter for optimal
     * initialization. Nested Row objects are recursively converted to Maps.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Row row = resultSet.one();
     * // Use LinkedHashMap to preserve column order
     * Map<String, Object> map = CassandraExecutor.toMap(row, LinkedHashMap::new);
     * 
     * // Use TreeMap for sorted keys
     * Map<String, Object> sortedMap = CassandraExecutor.toMap(row, size -> new TreeMap<>());
     * }</pre>
     * 
     * @param row the Cassandra Row to convert
     * @param supplier function to create the Map instance with expected size
     * @return a Map containing all column name-value pairs from the row
     */
    public static Map<String, Object> toMap(final Row row, final IntFunction<? extends Map<String, Object>> supplier) {
        final ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
        final int columnCount = columnDefinitions.size();
        final Map<String, Object> map = supplier.apply(columnCount);

        String propName = null;
        Object propValue = null;

        for (int i = 0; i < columnCount; i++) {
            propName = columnDefinitions.getName(i);
            propValue = row.getObject(i);

            if (propValue instanceof Row) {
                map.put(propName, toMap((Row) propValue, supplier));
            } else {
                map.put(propName, propValue);
            }
        }

        return map;
    }

    /**
     * Reads a Cassandra Row and converts it to the specified row class type.
     * 
     * <p>This internal method handles conversion of Row objects to various Java types
     * including arrays, collections, maps, entity beans, and single values. It performs
     * recursive conversion for nested Row objects.</p>
     * 
     * @param <T> the target type
     * @param row the Cassandra Row to read, or null
     * @param rowClass the target class type, or null for Object[]
     * @return the converted object, or default value if row is null
     */
    @SuppressWarnings("rawtypes")
    private static <T> T readRow(final Row row, final Class<T> rowClass) {
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
                    a[i] = readRow((Row) value, Object[].class);
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
                    c.add(readRow((Row) value, List.class));
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

    /**
     * Creates a row mapper function for converting Rows to the specified class type.
     * 
     * <p>This internal method creates optimized mapper functions based on the target
     * class type. The mapper handles arrays, collections, maps, entity beans, and
     * single-value conversions.</p>
     * 
     * @param <T> the target type
     * @param rowClass the target class for row conversion
     * @param columnDefinitions the column definitions from the ResultSet
     * @return a function that maps Row objects to the target type
     */
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
                        a[i] = readRow((Row) value, Object[].class);
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
                        c.add(readRow((Row) value, List.class));
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
     * Gets a single entity by condition through the nullable {@code gett} contract.
     * 
     * <p>This method retrieves exactly one entity matching the specified condition.
     * If no entity is found, it returns {@code null}. If multiple entities match,
     * it throws {@link DuplicateResultException}.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = executor.gett(User.class, 
     *     Arrays.asList("id", "name", "email"),
     *     Filters.eq("id", userId));
     * }</pre>
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param whereClause the WHERE condition
     * @return the single matching entity, or null if not found
     * @throws DuplicateResultException if multiple entities match the condition
     */
    @Override
    public <T> T gett(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) throws DuplicateResultException {
        final SP cp = prepareQuery(targetClass, selectPropNames, whereClause, 2);
        final ResultSet resultSet = execute(cp);
        return fetchOnlyOne(targetClass, resultSet);
    }

    /**
     * Queries for a single result value with nullable return.
     *
     * <p>This method executes a query expected to return a single row with a single
     * column value. The result is wrapped in a Nullable to handle the case where
     * no row is returned or when the value itself is null.</p>
     *
     * <p>This is particularly useful for aggregate queries (COUNT, MAX, MIN, AVG, SUM)
     * or queries that return a single scalar value. The method automatically extracts
     * the first column from the first row and converts it to the specified type.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Get count of active users
     * Nullable<Long> count = executor.queryForSingleResult(Long.class,
     *     "SELECT COUNT(*) FROM users WHERE status = ?", "active");
     *
     * if (count.isPresent()) {
     *     System.out.println("Active users: " + count.get());
     * }
     *
     * // Get maximum salary (could be null if table is empty)
     * Nullable<BigDecimal> maxSalary = executor.queryForSingleResult(
     *     BigDecimal.class, "SELECT MAX(salary) FROM employees WHERE department = ?", "engineering");
     *
     * // Get user email (may be null if user not found or email is null)
     * Nullable<String> email = executor.queryForSingleResult(
     *     String.class, "SELECT email FROM users WHERE id = ?", userId);
     *
     * // Handle nullable result with chaining
     * email.ifPresent(e -> sendNotification(e))
     *      .ifEmpty(() -> log.warn("No email found for user: " + userId));
     * }</pre>
     *
     * @param <E> the value type
     * @param valueClass the class of the expected value
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a Nullable containing the result value, or empty if no result
     * @throws IllegalArgumentException if valueClass or query is null
     */
    @Override
    public <E> Nullable<E> queryForSingleResult(final Class<E> valueClass, final String query, final Object... parameters) {
        final ResultSet resultSet = execute(query, parameters);
        final Row row = resultSet.one();

        return row == null ? (Nullable<E>) Nullable.empty() : Nullable.of(N.convert(row.getObject(0), valueClass));
    }

    /**
     * Queries for a single non-null result value.
     * 
     * <p>This method executes a query expected to return a single row with a single
     * non-null column value. The result is wrapped in an Optional. This is useful
     * when you expect the query to return either one result or no results.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Optional<String> email = executor.queryForSingleNonNull(String.class,
     *     "SELECT email FROM users WHERE id = ?", userId);
     * 
     * email.ifPresent(e -> sendEmail(e));
     * }</pre>
     * 
     * @param <E> the value type
     * @param valueClass the class of the expected value
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return an Optional containing the result value, or empty if no result
     */
    @Override
    public <E> Optional<E> queryForSingleNonNull(final Class<E> valueClass, final String query, final Object... parameters) {
        final ResultSet resultSet = execute(query, parameters);
        final Row row = resultSet.one();

        return row == null ? (Optional<E>) Optional.empty() : Optional.of(N.convert(row.getObject(0), valueClass));
    }

    /**
     * Finds the first row matching the query and converts it to the target class.
     *
     * <p>This method executes a query and returns the first row as an object of
     * the specified type. It's useful for queries that may return multiple rows
     * but you only need the first one. This is particularly useful for lookup
     * operations where you expect at most one result.</p>
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
     * user.ifPresent(u -> System.out.println("Found: " + u.getName()));
     *
     * // Get latest user
     * Optional<User> latestUser = executor.findFirst(User.class,
     *     "SELECT * FROM users ORDER BY created_at DESC LIMIT 1");
     *
     * latestUser.ifPresent(user -> {
     *     System.out.println("Latest user: " + user.getName());
     * });
     *
     * // Get as map
     * Optional<Map<String, Object>> userData = executor.findFirst(Map.class,
     *     "SELECT name, email FROM users WHERE id = ?", userId);
     *
     * // Get as array (for aggregate queries)
     * Optional<Object[]> stats = executor.findFirst(Object[].class,
     *     "SELECT COUNT(*), MAX(created_at) FROM events WHERE type = ?", "login");
     * }</pre>
     *
     * @param <T> the target type
     * @param targetClass an entity class with getter/setter methods or Map.class
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return an Optional containing the first result, or empty if no results
     * @throws IllegalArgumentException if targetClass or query is null
     */
    @Override
    public <T> Optional<T> findFirst(final Class<T> targetClass, final String query, final Object... parameters) {
        final ResultSet resultSet = execute(query, parameters);
        final Row row = resultSet.one();

        return row == null ? (Optional<T>) Optional.empty() : Optional.of(readRow(row, targetClass));
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
     * // Custom DTO mapping
     * try (Stream<UserDTO> stream = executor.stream(
     *     "SELECT * FROM users WHERE department = ?",
     *     (columns, row) -> new UserDTO(
     *         row.getUUID("id"),
     *         row.getString("name"),
     *         row.getString("email")
     *     ),
     *     "engineering")) {
     *
     *     stream.filter(user -> user.getEmail().endsWith("@company.com"))
     *           .forEach(user -> processUser(user));
     * }
     *
     * // Dynamic column mapping based on column definitions
     * try (Stream<Map<String, Object>> stream = executor.stream(
     *     "SELECT * FROM dynamic_table WHERE type = ?",
     *     (columns, row) -> {
     *         Map<String, Object> map = new HashMap<>();
     *         for (int i = 0; i < columns.size(); i++) {
     *             String colName = columns.getName(i);
     *             map.put(colName, row.getObject(i));
     *         }
     *         return map;
     *     },
     *     "config")) {
     *
     *     long count = stream.filter(map -> map.containsKey("enabled")).count();
     *     System.out.println("Enabled configs: " + count);
     * }
     *
     * // Complex transformation with null handling
     * try (Stream<UserSummary> stream = executor.stream(
     *     "SELECT id, first_name, last_name, email, created_at FROM users",
     *     (columns, row) -> {
     *         String firstName = row.getString("first_name");
     *         String lastName = row.getString("last_name");
     *         String fullName = firstName + " " + (lastName != null ? lastName : "");
     *         return new UserSummary(row.getUUID("id"), fullName.trim(), row.getString("email"));
     *     })) {
     *
     *     List<UserSummary> summaries = stream.collect(Collectors.toList());
     * }
     * }</pre>
     *
     * @param <T> the result type
     * @param query the CQL query to execute
     * @param rowMapper function to convert column definitions and rows to result objects
     * @param parameters the query parameters
     * @return a Stream of mapped results
     * @throws IllegalArgumentException if query or rowMapper is null
     */
    public <T> Stream<T> stream(final String query, final BiFunction<ColumnDefinitions, Row, T> rowMapper, final Object... parameters) {
        N.checkArgNotNull(rowMapper, "rowMapper");

        return Stream.of(execute(query, parameters).iterator()).map(createRowMapper(rowMapper));
    }

    /**
     * Creates a stream of results from a statement with custom row mapping.
     * 
     * <p>This method returns a Stream that lazily fetches and maps rows using
     * the provided row mapper function. The statement can be pre-configured with
     * specific settings like consistency level and fetch size.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Statement stmt = new SimpleStatement("SELECT * FROM large_table")
     *     .setFetchSize(5000)
     *     .setConsistencyLevel(ConsistencyLevel.ONE);
     * 
     * try (Stream<Record> stream = executor.stream(stmt,
     *     (columns, row) -> mapToRecord(row))) {
     *     
     *     long count = stream.filter(record -> record.isValid()).count();
     *     System.out.println("Valid records: " + count);
     * }
     * }</pre>
     * 
     * @param <T> the result type
     * @param statement the prepared statement to execute
     * @param rowMapper function to convert column definitions and rows to result objects
     * @return a Stream of mapped results
     * @throws IllegalArgumentException if statement or rowMapper is null
     */
    public <T> Stream<T> stream(final Statement statement, final BiFunction<ColumnDefinitions, Row, T> rowMapper) {
        N.checkArgNotNull(rowMapper, "rowMapper");

        return Stream.of(execute(statement).iterator()).map(createRowMapper(rowMapper));
    }

    /**
     * Executes a CQL query without parameters and returns the raw ResultSet.
     *
     * <p>This method prepares and executes a CQL statement, applying any configured
     * statement settings. The query is automatically prepared and cached for optimal
     * performance on repeated executions.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Simple SELECT query
     * ResultSet rs = executor.execute("SELECT * FROM users");
     * for (Row row : rs) {
     *     System.out.println(row.getString("name"));
     * }
     *
     * // DDL statement
     * executor.execute("CREATE TABLE IF NOT EXISTS logs (id UUID PRIMARY KEY, message TEXT)");
     *
     * // Simple INSERT
     * executor.execute("INSERT INTO audit_log (id, timestamp) VALUES (uuid(), toTimestamp(now()))");
     * }</pre>
     *
     * @param query the CQL query to execute
     * @return the ResultSet containing query results
     * @throws IllegalArgumentException if query is null or empty
     * @throws com.datastax.driver.core.exceptions.NoHostAvailableException if no Cassandra nodes are available
     */
    @Override
    public ResultSet execute(final String query) {
        return session.execute(prepareStatement(query));
    }

    /**
     * Executes a parameterized CQL query with positional parameters and returns the raw ResultSet.
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
     * ResultSet rs = executor.execute(
     *     "SELECT * FROM users WHERE status = ? AND age > ?",
     *     "active", 18);
     *
     * // Array parameters
     * Object[] params = {"active", "engineering", 5};
     * ResultSet users = executor.execute(
     *     "SELECT * FROM users WHERE status = ? AND department = ? AND experience_years > ?",
     *     params);
     *
     * // Entity as parameter source
     * User searchParams = new User();
     * searchParams.setStatus("active");
     * searchParams.setDepartment("engineering");
     * ResultSet rs2 = executor.execute(
     *     "SELECT * FROM users WHERE status = ? AND department = ?",
     *     searchParams);
     *
     * // Collection parameters
     * List<Object> paramList = Arrays.asList(userId, "active");
     * ResultSet result = executor.execute(
     *     "SELECT * FROM user_sessions WHERE user_id = ? AND status = ?",
     *     paramList);
     * }</pre>
     *
     * @param query the CQL query with parameter placeholders
     * @param parameters the query parameters
     * @return the ResultSet containing query results
     * @throws IllegalArgumentException if query is null or if parameter count doesn't match placeholders
     * @throws com.datastax.driver.core.exceptions.NoHostAvailableException if all contact points are unreachable
     */
    @Override
    public ResultSet execute(final String query, final Object... parameters) {
        return session.execute(prepareStatement(query, parameters));
    }

    /**
     * Executes a CQL query with named parameters provided as a Map and returns the raw ResultSet.
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
     * String query = "SELECT * FROM users WHERE department = :dept AND status = :status AND age > :minAge";
     *
     * Map<String, Object> params = new HashMap<>();
     * params.put("dept", "engineering");
     * params.put("status", "active");
     * params.put("minAge", 18);
     *
     * ResultSet results = executor.execute(query, params);
     *
     * // Alternative using N.asMap utility
     * ResultSet results2 = executor.execute(
     *     "SELECT * FROM orders WHERE user_id = :userId AND status = :status",
     *     N.asMap("userId", UUID.fromString("123e4567-e89b-12d3-a456-426614174000"),
     *             "status", "pending"));
     *
     * // Complex query with multiple named parameters
     * Map<String, Object> searchParams = new HashMap<>();
     * searchParams.put("startDate", LocalDate.now().minusDays(30));
     * searchParams.put("endDate", LocalDate.now());
     * searchParams.put("status", "completed");
     *
     * ResultSet recentOrders = executor.execute(
     *     "SELECT * FROM orders WHERE created_date >= :startDate AND created_date <= :endDate AND status = :status",
     *     searchParams);
     * }</pre>
     *
     * @param query the CQL query with named parameter placeholders
     * @param parameters the query parameters
     * @return the ResultSet containing query results
     * @throws IllegalArgumentException if query is null or if required parameters are missing
     * @throws com.datastax.driver.core.exceptions.NoHostAvailableException if all contact points are unreachable
     */
    @Override
    public ResultSet execute(final String query, final Map<String, Object> parameters) {
        return session.execute(prepareStatement(query, parameters));
    }

    /**
     * Executes a pre-configured CQL Statement and returns the raw ResultSet.
     *
     * <p>This method executes any type of Cassandra Statement object from Driver 3.x, including
     * SimpleStatement, BoundStatement, or BatchStatement. This provides maximum
     * flexibility when you need fine-grained control over statement configuration
     * such as consistency levels, timeouts, or retry policies.</p>
     *
     * <p>The configured statement settings for this executor are applied to the
     * statement before execution if not already set on the statement itself.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Execute a simple statement with custom settings
     * Statement simpleStmt = new SimpleStatement("SELECT * FROM users WHERE token(id) > token(?)", lastId)
     *     .setConsistencyLevel(ConsistencyLevel.QUORUM)
     *     .setFetchSize(5000);
     * ResultSet rs = executor.execute(simpleStmt);
     *
     * // Execute a prepared statement with custom settings
     * PreparedStatement preparedStatement = session.prepare(
     *     "INSERT INTO users (id, name, email) VALUES (?, ?, ?)");
     *
     * BoundStatement boundStatement = preparedStatement
     *     .bind(UUID.randomUUID(), "John Doe", "john@example.com")
     *     .setConsistencyLevel(ConsistencyLevel.QUORUM)
     *     .setReadTimeoutMillis(10000);
     *
     * ResultSet result = executor.execute(boundStatement);
     *
     * // Execute a batch statement
     * BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED);
     * batch.add(new SimpleStatement("INSERT INTO users (id, name) VALUES (?, ?)", uuid1, "Alice"));
     * batch.add(new SimpleStatement("INSERT INTO users (id, name) VALUES (?, ?)", uuid2, "Bob"));
     * batch.add(new SimpleStatement("UPDATE user_count SET total = total + 2"));
     * executor.execute(batch);
     *
     * // Execute with retry policy
     * Statement stmtWithRetry = new SimpleStatement("SELECT * FROM critical_data WHERE id = ?", dataId)
     *     .setRetryPolicy(DefaultRetryPolicy.INSTANCE);
     * ResultSet criticalData = executor.execute(stmtWithRetry);
     * }</pre>
     *
     * @param statement the configured CQL Statement to execute
     * @return the ResultSet containing execution results
     * @throws IllegalArgumentException if statement is null
     * @throws com.datastax.driver.core.exceptions.NoHostAvailableException if all contact points are unreachable
     */
    @Override
    public ResultSet execute(final Statement statement) {
        return session.execute(statement);
    }

    /**
     * Asynchronously gets an entity by condition.
     * 
     * <p>This method asynchronously retrieves a single entity matching the specified
     * condition. Returns a future that completes with an Optional containing the
     * entity if found, or empty if not found.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ContinuableFuture<Optional<User>> future = executor.asyncGet(User.class,
     *     Arrays.asList("id", "name", "email"),
     *     Filters.eq("id", userId));
     * 
     * future.thenRunAsync(optUser -> {
     *     optUser.ifPresent(user -> System.out.println("Found: " + user.getName()));
     * });
     * }</pre>
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param whereClause the WHERE condition
     * @return a future containing an Optional with the entity
     */
    @Override
    public <T> ContinuableFuture<Optional<T>> asyncGet(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP cp = prepareQuery(targetClass, selectPropNames, whereClause, 2);

        return asyncExecute(cp).map(resultSet -> Optional.ofNullable(fetchOnlyOne(targetClass, resultSet)));
    }

    /**
     * Asynchronously gets a single entity by condition, throwing exception if multiple results.
     * 
     * <p>This method asynchronously retrieves exactly one entity matching the condition.
     * If no entity is found, the returned future completes with {@code null}. If multiple
     * entities match, the future completes exceptionally with
     * {@link DuplicateResultException}.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ContinuableFuture<User> future = executor.asyncGett(User.class,
     *     null,  // select all columns
     *     Filters.eq("email", userEmail));
     * 
     * future.handle((user, error) -> {
     *     if (error != null) {
     *         if (error instanceof DuplicateResultException) {
     *             System.err.println("Multiple users with same email!");
     *         }
     *     } else if (user != null) {
     *         System.out.println("User: " + user.getName());
     *     }
     *     return null;
     * });
     * }</pre>
     * 
     * @param <T> the entity type
     * @param targetClass the entity class
     * @param selectPropNames the property names to select (null for all properties)
     * @param whereClause the WHERE condition
     * @return a future containing the single matching entity, or {@code null} if no entity matches
     * @throws DuplicateResultException if multiple entities match (thrown when future completes)
     */
    @Override
    public <T> ContinuableFuture<T> asyncGett(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause) {
        final SP cp = prepareQuery(targetClass, selectPropNames, whereClause, 2);

        return asyncExecute(cp).map(resultSet -> fetchOnlyOne(targetClass, resultSet));
    }

    /**
     * Asynchronously queries for a single result value with nullable return.
     * 
     * <p>This method asynchronously executes a query expected to return a single
     * value. The future completes with a Nullable containing the result.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ContinuableFuture<Nullable<Long>> future = executor.asyncQueryForSingleResult(
     *     Long.class,
     *     "SELECT COUNT(*) FROM users WHERE created_at > ?",
     *     yesterday);
     * 
     * future.thenRunAsync(count -> {
     *     System.out.println("New users: " + count.orElse(0L));
     * });
     * }</pre>
     * 
     * @param <T> the value type
     * @param valueClass the class of the expected value
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a future containing a Nullable with the result
     */
    @Override
    public <T> ContinuableFuture<Nullable<T>> asyncQueryForSingleResult(final Class<T> valueClass, final String query, final Object... parameters) {
        return asyncExecute(query, parameters).map(resultSet -> {
            final Row row = resultSet.one();

            return row == null ? Nullable.empty() : Nullable.of(N.convert(row.getObject(0), valueClass));
        });
    }

    /**
     * Asynchronously queries for a single non-null result value.
     * 
     * <p>This method asynchronously executes a query expected to return a single
     * non-null value. The future completes with an Optional containing the result.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ContinuableFuture<Optional<UUID>> future = executor.asyncQueryForSingleNonNull(
     *     UUID.class,
     *     "SELECT id FROM users WHERE email = ? ALLOW FILTERING",
     *     userEmail);
     * 
     * future.thenCompose(optId -> {
     *     if (optId.isPresent()) {
     *         return loadUserDetails(optId.get());
     *     } else {
     *         return CompletableFuture.completedFuture(null);
     *     }
     * });
     * }</pre>
     * 
     * @param <T> the value type
     * @param valueClass the class of the expected value
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a future containing an Optional with the result
     */
    @Override
    public <T> ContinuableFuture<Optional<T>> asyncQueryForSingleNonNull(final Class<T> valueClass, final String query, final Object... parameters) {
        return asyncExecute(query, parameters).map(resultSet -> {
            final Row row = resultSet.one();

            return row == null ? Optional.empty() : Optional.of(N.convert(row.getObject(0), valueClass));
        });
    }

    /**
     * Asynchronously finds the first result of a query mapped to the specified target class.
     * 
     * <p>This method executes a query asynchronously and maps the first row to an instance
     * of the target class. The result is wrapped in an Optional that will be empty if
     * no rows are returned.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ContinuableFuture<Optional<User>> future = executor.asyncFindFirst(
     *     User.class,
     *     "SELECT * FROM users WHERE status = ? LIMIT 1",
     *     "active");
     * 
     * future.thenRunAsync(optUser -> {
     *     optUser.ifPresent(user -> System.out.println("Found user: " + user.getName()));
     * });
     * }</pre>
     * 
     * @param <T> the target type
     * @param targetClass the entity class
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a future containing an Optional with the mapped result
     * @throws IllegalArgumentException if targetClass is null
     * @throws IllegalStateException if the session is closed
     */
    @Override
    public <T> ContinuableFuture<Optional<T>> asyncFindFirst(final Class<T> targetClass, final String query, final Object... parameters) {
        return asyncExecute(query, parameters).map(resultSet -> {
            final Row row = resultSet.one();

            return row == null ? Optional.empty() : Optional.of(readRow(row, targetClass));
        });
    }

    /**
     * Asynchronously executes a query and returns a stream of Object arrays.
     * 
     * <p>Each Object array represents a row from the result set, with array elements
     * corresponding to column values in the order they appear in the query result.
     * This method is useful for queries where the column structure is dynamic or unknown.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ContinuableFuture<Stream<Object[]>> future = executor.asyncStream(
     *     "SELECT id, name, email FROM users WHERE department = ?",
     *     "engineering");
     * 
     * future.thenRunAsync(stream -> {
     *     stream.forEach(row -> {
     *         UUID id = (UUID) row[0];
     *         String name = (String) row[1];
     *         String email = (String) row[2];
     *         System.out.println(name + " (" + email + ")");
     *     });
     * });
     * }</pre>
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a future containing a Stream of Object arrays
     * @throws IllegalArgumentException if query is null or empty
     * @throws IllegalStateException if the session is closed
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
                        a[i] = readRow((Row) propValue, Object[].class);
                    } else {
                        a[i] = propValue;
                    }
                }

                return a;
            });
        });
    }

    /**
     * Asynchronously executes a query and returns a stream with custom row mapping.
     * 
     * <p>This method allows you to provide a custom mapper function to transform each row
     * into a specific type. The mapper receives both the column definitions and the row data,
     * allowing for flexible mapping strategies.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ContinuableFuture<Stream<UserSummary>> future = executor.asyncStream(
     *     "SELECT id, name, email, created_at FROM users",
     *     (columns, row) -> new UserSummary(
     *         row.getUUID("id"),
     *         row.getString("name"),
     *         row.getString("email"),
     *         row.getTimestamp("created_at")
     *     )
     * );
     * }</pre>
     * 
     * @param <T> the target type for each row
     * @param query the CQL query to execute
     * @param rowMapper the function to map each row to type T
     * @param parameters the query parameters
     * @return a future containing a Stream of mapped objects
     * @throws IllegalArgumentException if query or rowMapper is null
     * @throws IllegalStateException if the session is closed
     */
    public <T> ContinuableFuture<Stream<T>> asyncStream(final String query, final BiFunction<ColumnDefinitions, Row, T> rowMapper, final Object... parameters) {
        return asyncExecute(query, parameters).map(resultSet -> Stream.of(resultSet.iterator()).map(createRowMapper(rowMapper)));
    }

    /**
     * Asynchronously executes a statement and returns a stream with custom row mapping.
     * 
     * <p>This method is similar to {@link #asyncStream(String, BiFunction, Object...)} but accepts
     * a pre-built Statement object instead of a query string. This is useful when you need
     * fine-grained control over statement configuration.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Statement stmt = new SimpleStatement("SELECT * FROM users")
     *     .setConsistencyLevel(ConsistencyLevel.QUORUM)
     *     .setFetchSize(5000);
     * 
     * ContinuableFuture<Stream<User>> future = executor.asyncStream(
     *     stmt,
     *     (columns, row) -> mapToUser(row)
     * );
     * }</pre>
     * 
     * @param <T> the target type for each row
     * @param statement the statement to execute
     * @param rowMapper the function to map each row to type T
     * @return a future containing a Stream of mapped objects
     * @throws IllegalArgumentException if statement or rowMapper is null
     * @throws IllegalStateException if the session is closed
     */
    public <T> ContinuableFuture<Stream<T>> asyncStream(final Statement statement, final BiFunction<ColumnDefinitions, Row, T> rowMapper) {
        return asyncExecute(statement).map(resultSet -> Stream.of(resultSet.iterator()).map(createRowMapper(rowMapper)));
    }

    /**
     * Asynchronously executes a CQL query without parameters.
     * 
     * <p>This method executes a simple query that doesn't require any parameters.
     * The returned ResultSet can be used to iterate over the results or extract specific values.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ContinuableFuture<ResultSet> future = executor.asyncExecute(
     *     "SELECT * FROM system.local"
     * );
     * 
     * future.thenRunAsync(resultSet -> {
     *     Row row = resultSet.one();
     *     System.out.println("Cluster name: " + row.getString("cluster_name"));
     * });
     * }</pre>
     * 
     * @param query the CQL query to execute
     * @return a future containing the ResultSet
     * @throws IllegalArgumentException if query is null or empty
     * @throws IllegalStateException if the session is closed
     */
    @Override
    public ContinuableFuture<ResultSet> asyncExecute(final String query) {
        return ContinuableFuture.wrap(session.executeAsync(prepareStatement(query)));
    }

    /**
     * Asynchronously executes a parameterized CQL query.
     * 
     * <p>This method executes a query with positional parameters. Parameters are bound
     * in the order they appear in the query. The method supports various parameter formats
     * including individual values, arrays, collections, maps, and entity objects.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Simple parameters
     * ContinuableFuture<ResultSet> future = executor.asyncExecute(
     *     "SELECT * FROM users WHERE department = ? AND status = ?",
     *     "engineering", "active"
     * );
     * 
     * // Entity as parameter
     * User user = new User();
     * user.setDepartment("engineering");
     * user.setStatus("active");
     * ContinuableFuture<ResultSet> future2 = executor.asyncExecute(
     *     "SELECT * FROM users WHERE department = :department AND status = :status",
     *     user
     * );
     * }</pre>
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a future containing the ResultSet
     * @throws IllegalArgumentException if query is null or parameter count doesn't match
     * @throws IllegalStateException if the session is closed
     */
    @Override
    public ContinuableFuture<ResultSet> asyncExecute(final String query, final Object... parameters) {
        return ContinuableFuture.wrap(session.executeAsync(prepareStatement(query, parameters)));
    }

    /**
     * Asynchronously executes a CQL query with named parameters from a map.
     * 
     * <p>This method is specifically designed for queries with named parameters where
     * the parameter values are provided as a map. The map keys should match the
     * parameter names in the query (without the colon prefix).</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, Object> params = new HashMap<>();
     * params.put("dept", "engineering");
     * params.put("status", "active");
     * 
     * ContinuableFuture<ResultSet> future = executor.asyncExecute(
     *     "SELECT * FROM users WHERE department = :dept AND status = :status",
     *     params
     * );
     * }</pre>
     * 
     * @param query the CQL query to execute
     * @param parameters the query parameters
     * @return a future containing the ResultSet
     * @throws IllegalArgumentException if query is null or required parameters are missing
     * @throws IllegalStateException if the session is closed
     */
    @Override
    public ContinuableFuture<ResultSet> asyncExecute(final String query, final Map<String, Object> parameters) {
        return ContinuableFuture.wrap(session.executeAsync(prepareStatement(query, parameters)));
    }

    /**
     * Asynchronously executes a pre-built statement.
     * 
     * <p>This method executes a Statement object that has been pre-configured with
     * all necessary settings. This is useful when you need direct control over
     * statement configuration such as consistency level, retry policy, or fetch size.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BoundStatement stmt = preparedStatement.bind("engineering", "active");
     * stmt.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
     * stmt.setFetchSize(1000);
     * 
     * ContinuableFuture<ResultSet> future = executor.asyncExecute(stmt);
     * }</pre>
     * 
     * @param statement the statement to execute
     * @return a future containing the ResultSet
     * @throws IllegalArgumentException if statement is null
     * @throws IllegalStateException if the session is closed
     */
    @Override
    public ContinuableFuture<ResultSet> asyncExecute(final Statement statement) {
        return ContinuableFuture.wrap(session.executeAsync(statement));
    }

    /**
     * Closes the executor and releases all resources.
     * 
     * <p>This method closes the underlying Cassandra session and cluster connections.
     * After calling this method, the executor instance cannot be used for any further operations.
     * This method is idempotent - calling it multiple times has no additional effect.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CassandraExecutor executor = new CassandraExecutor(session);
     * try {
     *     // Perform database operations
     * } finally {
     *     executor.close();   // Ensures resources are released
     * }
     * }</pre>
     */
    @Override
    public void close() {
        try {
            if (!session.isClosed()) {
                session.close();
            }
        } finally {
            try {
                if (!cluster.isClosed()) {
                    cluster.close();
                }
            } finally {
                try {
                    statementPool.close();
                } finally {
                    preparedStatementPool.close();
                }
            }
        }
    }

    /**
     * Prepares a batch insert statement for a collection of entities.
     * 
     * <p>This method creates a batch statement that inserts multiple entities in a single
     * round trip to the database. The batch type determines the atomicity and performance
     * characteristics of the operation.</p>
     * 
     * @param entities the collection of entities to insert
     * @param type the batch type
     * @return a prepared BatchStatement ready for execution
     * @throws IllegalArgumentException if entities is null or empty
     */
    @Override
    protected BatchStatement prepareBatchInsertStatement(final Collection<?> entities, final BatchStatement.Type type) {
        N.checkArgument(N.notEmpty(entities), "'entities' can't be null or empty.");
        N.checkElementNotNull(entities);

        final BatchStatement stmt = prepareBatchStatement(type);
        SP cp = null;

        for (final Object entity : entities) {
            cp = prepareInsert(entity);
            stmt.add(prepareStatement(cp.query(), cp.parameters().toArray()));
        }

        return stmt;
    }

    /**
     * Prepares a batch insert statement for a collection of property maps.
     * 
     * <p>This method creates a batch statement that inserts multiple rows based on
     * property maps rather than entity objects. This is useful when working with
     * dynamic data structures.</p>
     * 
     * @param targetClass the entity class
     * @param propsList collection of property maps to insert
     * @param type the batch type
     * @return a prepared BatchStatement ready for execution
     * @throws IllegalArgumentException if propsList is null or empty
     */
    @Override
    protected BatchStatement prepareBatchInsertStatement(final Class<?> targetClass, final Collection<? extends Map<String, Object>> propsList,
            final BatchStatement.Type type) {
        N.checkArgument(N.notEmpty(propsList), "'propsList' can't be null or empty.");
        N.checkElementNotNull(propsList);

        final BatchStatement stmt = prepareBatchStatement(type);
        SP cp = null;

        for (final Map<String, Object> props : propsList) {
            cp = prepareInsert(targetClass, props);
            stmt.add(prepareStatement(cp.query(), cp.parameters().toArray()));
        }

        return stmt;
    }

    /**
     * Prepares a batch update statement for a collection of entities.
     * 
     * <p>This method creates a batch statement that updates multiple entities,
     * updating only the specified properties for each entity.</p>
     * 
     * @param entities the collection of entities to update
     * @param propNamesToUpdate the property names to update
     * @param type the batch type
     * @return a prepared BatchStatement ready for execution
     * @throws IllegalArgumentException if entities or propNamesToUpdate is null or empty
     */
    @Override
    protected BatchStatement prepareBatchUpdateStatement(final Collection<?> entities, final Collection<String> propNamesToUpdate,
            final BatchStatement.Type type) {
        N.checkArgument(N.notEmpty(entities), "'entities' can't be null or empty.");
        N.checkArgument(N.notEmpty(propNamesToUpdate), "'propNamesToUpdate' can't be null or empty");
        N.checkElementNotNull(entities);

        final BatchStatement stmt = prepareBatchStatement(type);

        for (final Object entity : entities) {
            final SP cp = prepareUpdate(entity, propNamesToUpdate);
            stmt.add(prepareStatement(cp.query(), cp.parameters().toArray()));
        }

        return stmt;
    }

    /**
     * Prepares a batch update statement for a collection of property maps.
     * 
     * <p>This method creates a batch statement that updates multiple rows based on
     * property maps. The primary key values must be included in each property map.</p>
     * 
     * @param targetClass the entity class
     * @param propsList collection of property maps to update
     * @param type the batch type
     * @return a prepared BatchStatement ready for execution
     * @throws IllegalArgumentException if propsList is null or empty
     */
    @Override
    protected BatchStatement prepareBatchUpdateStatement(final Class<?> targetClass, final Collection<? extends Map<String, Object>> propsList,
            final BatchStatement.Type type) {
        N.checkArgument(N.notEmpty(propsList), "'propsList' can't be null or empty.");
        N.checkElementNotNull(propsList);

        final Set<String> primaryKeyNames = getKeyNameSet(targetClass);
        final BatchStatement stmt = prepareBatchStatement(type);

        for (final Map<String, Object> props : propsList) {
            final Map<String, Object> tmp = new HashMap<>(props);
            final List<Condition> conds = new ArrayList<>(primaryKeyNames.size());

            for (final String keyName : primaryKeyNames) {
                conds.add(Filters.eq(keyName, tmp.remove(keyName)));
            }

            final SP cp = prepareUpdate(targetClass, tmp, Filters.and(conds));
            stmt.add(prepareStatement(cp.query(), cp.parameters().toArray()));
        }

        return stmt;
    }

    /**
     * Prepares a batch update statement with custom queries and parameters.
     * 
     * <p>This method creates a batch statement that executes multiple update queries
     * with their respective parameters. Each element in parametersList corresponds
     * to one execution of the query.</p>
     * 
     * @param query the CQL update query to execute
     * @param parametersList collection of parameter sets for each query execution
     * @param type the batch type
     * @return a prepared BatchStatement ready for execution
     * @throws IllegalArgumentException if parametersList is null or empty
     */
    @Override
    protected BatchStatement prepareBatchUpdateStatement(final String query, final Collection<?> parametersList, final BatchStatement.Type type) {
        N.checkArgument(N.notEmpty(parametersList), "'propsList' can't be null or empty.");
        N.checkElementNotNull(parametersList);

        final BatchStatement stmt = prepareBatchStatement(type);

        for (final Object params : parametersList) {
            stmt.add(prepareStatement(query, params));
        }

        return stmt;
    }

    /**
     * Creates and configures a new batch statement.
     * 
     * <p>This method creates a batch statement of the specified type and applies
     * any configured statement settings such as consistency level and retry policy.</p>
     * 
     * @param type the batch type
     * @return a configured BatchStatement
     */
    @Override
    protected BatchStatement prepareBatchStatement(final BatchStatement.Type type) {
        final BatchStatement stmt = new BatchStatement(type);

        configStatement(stmt);

        return stmt;
    }

    /**
     * Prepares a statement from a CQL query without parameters.
     * 
     * <p>This method prepares a statement from the given query string, utilizing
     * statement pooling for frequently used queries to improve performance.</p>
     * 
     * @param query the CQL query to prepare
     * @return a prepared Statement ready for execution
     * @throws IllegalArgumentException if query is null or empty
     */
    @Override
    protected Statement prepareStatement(final String query) {
        Statement stmt = null;

        if (query.length() <= POOLABLE_LENGTH) {
            final PoolableAdapter<Statement> wrapper = statementPool.get(query);

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

    /**
     * Prepares a parameterized statement from a CQL query.
     * 
     * <p>This method prepares a statement with the given parameters, supporting various
     * parameter formats including positional, named, entity-based, and map-based parameters.
     * The method utilizes prepared statement pooling for improved performance.</p>
     * 
     * @param query the CQL query to prepare
     * @param parameters the query parameters
     * @return a prepared Statement ready for execution
     * @throws IllegalArgumentException if parameters don't match the query requirements
     */
    @Override
    protected Statement prepareStatement(final String query, final Object... parameters) {
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

        final ColumnDefinitions columnDefinitions = preStmt.getVariables();
        final int parameterCount = columnDefinitions.size();
        DataType colType = null;
        Class<?> javaClazz = null;

        if (parameterCount == 0) {
            return preStmt.bind();
        } else if (N.isEmpty(parameters)) {
            throw new IllegalArgumentException("Null or empty parameters for parameterized query: " + query);
        }

        if (parameterCount == 1 && parameters.length == 1) {
            colType = columnDefinitions.getType(0);
            javaClazz = namedDataType.get(colType.getName().name());

            if (parameters[0] == null || (javaClazz.isAssignableFrom(parameters[0].getClass()) || codecRegistry.codecFor(colType).accepts(parameters[0]))) {
                return bind(preStmt, parameters);
            } else if (parameters[0] instanceof List && ((List<Object>) parameters[0]).size() == 1) {
                final Object tmp = ((List<Object>) parameters[0]).get(0);

                if (tmp == null || (javaClazz.isAssignableFrom(tmp.getClass()) || codecRegistry.codecFor(colType).accepts(tmp))) {
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
                    parameterName = isCassandraNamedParameters ? columnDefinitions.getName(i) : namedParameters.get(i);

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
                    parameterName = isCassandraNamedParameters ? columnDefinitions.getName(i) : namedParameters.get(i);

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
            colType = columnDefinitions.getType(i);
            javaClazz = namedDataType.get(colType.getName().name());

            if (values[i] == null) {
                values[i] = N.defaultValueOf(javaClazz);
            } else if (javaClazz.isAssignableFrom(values[i].getClass()) || codecRegistry.codecFor(colType).accepts(values[i])) {
                // continue;
            } else {
                try {
                    values[i] = N.convert(values[i], javaClazz);
                } catch (final ClassCastException | IllegalArgumentException e) {
                    // Type conversion failed, keep original value
                }
            }
        }

        return bind(preStmt, values.length == parameterCount ? values : N.copyOfRange(values, 0, parameterCount));
    }

    /**
     * Prepares a CQL statement for execution.
     * 
     * <p>This method creates a PreparedStatement from the given query and applies
     * any configured settings such as consistency level, retry policy, and tracing options.</p>
     * 
     * @param query the CQL query to prepare
     * @return a PreparedStatement ready for binding
     * @throws IllegalArgumentException if query is invalid
     */
    @Override
    protected PreparedStatement prepare(final String query) {
        final PreparedStatement preStat = session.prepare(query);

        if (settings != null) {
            if (settings.consistency() != null) {
                preStat.setConsistencyLevel(settings.consistency());
            }

            if (settings.serialConsistency() != null) {
                preStat.setSerialConsistencyLevel(settings.serialConsistency());
            }

            if (settings.retryPolicy() != null) {
                preStat.setRetryPolicy(settings.retryPolicy());
            }

            if (settings.traceQuery() != null) {
                if (settings.traceQuery()) {
                    preStat.enableTracing();
                } else {
                    preStat.disableTracing();
                }
            }
        }

        return preStat;
    }

    /**
     * Binds parameters to a prepared statement.
     * 
     * <p>This method creates a BoundStatement by binding the provided parameters
     * to the prepared statement and applies any configured statement settings.</p>
     * 
     * @param preStmt the prepared statement to bind
     * @param parameters the query parameters
     * @return a BoundStatement ready for execution
     * @throws IllegalArgumentException if parameter count doesn't match
     */
    @Override
    protected BoundStatement bind(final PreparedStatement preStmt, final Object... parameters) {
        final BoundStatement stmt = preStmt.bind(parameters);

        configStatement(stmt);

        return stmt;
    }

    /**
     * Configures a statement with the executor's settings.
     * 
     * <p>This method applies all configured settings (consistency level, retry policy,
     * timeouts, fetch size, tracing) to the given statement.</p>
     * 
     * @param stmt the statement to configure
     */
    protected void configStatement(final Statement stmt) {
        if (settings != null) {
            if (settings.consistency() != null) {
                stmt.setConsistencyLevel(settings.consistency());
            }

            if (settings.serialConsistency() != null) {
                stmt.setSerialConsistencyLevel(settings.serialConsistency());
            }

            if (settings.retryPolicy() != null) {
                stmt.setRetryPolicy(settings.retryPolicy());
            }

            if (settings.readTimeoutMillis() != null) {
                stmt.setReadTimeoutMillis(settings.readTimeoutMillis());
            }

            if (settings.fetchSize() != null) {
                stmt.setFetchSize(settings.fetchSize());
            }

            if (settings.traceQuery() != null) {
                if (settings.traceQuery()) {
                    stmt.enableTracing();
                } else {
                    stmt.disableTracing();
                }
            }
        }
    }

    /**
     * Converts a ResultSet to a list of the specified target class.
     * 
     * <p>This method iterates through all rows in the ResultSet and maps each row
     * to an instance of the target class.</p>
     * 
     * @param <T> the target type
     * @param targetClass the entity class
     * @param rs the ResultSet to convert
     * @return a list of mapped objects
     */
    @Override
    protected <T> List<T> toList(final Class<T> targetClass, final ResultSet rs) {
        return toList(rs, targetClass);
    }

    /**
     * Extracts data from a ResultSet into a Dataset.
     * 
     * <p>This method converts the ResultSet into a Dataset structure, which provides
     * additional functionality for data manipulation and analysis.</p>
     * 
     * @param targetClass the entity class
     * @param rs the ResultSet to extract data from
     * @return a Dataset containing the extracted data
     */
    @Override
    protected Dataset extractData(final Class<?> targetClass, final ResultSet rs) {
        return extractData(rs, targetClass);
    }

    /**
     * Creates a row mapper function for the specified class.
     * 
     * <p>This method returns a function that maps Row objects to instances of the
     * specified class. The mapper is lazily initialized on first use based on the
     * actual column definitions.</p>
     * 
     * @param <T> the target type
     * @param rowClass the class to map rows to
     * @return a Function that maps Row to T
     */
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

    /**
     * Creates a row mapper function from a custom BiFunction.
     * 
     * <p>This method wraps a BiFunction that takes ColumnDefinitions and Row
     * into a simpler Function that only takes Row, caching the ColumnDefinitions
     * for efficiency.</p>
     * 
     * @param <T> the target type
     * @param rowMapper the BiFunction to wrap
     * @return a Function that maps Row to T
     */
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

    /**
     * Fetches exactly one result from a ResultSet.
     * 
     * <p>This method ensures that the ResultSet contains exactly one row.
     * If no rows or multiple rows are found, appropriate exceptions are thrown.</p>
     * 
     * @param <T> the target type
     * @param targetClass the entity class
     * @param resultSet the ResultSet to fetch from
     * @return the single mapped result, or null if no rows
     * @throws DuplicateResultException if more than one row is found
     */
    protected <T> T fetchOnlyOne(final Class<T> targetClass, final ResultSet resultSet) {
        final Row row = resultSet.one();

        if (row == null) {
            return null;
        } else if (resultSet.isExhausted()) {
            return readRow(row, targetClass);
        } else {
            throw new DuplicateResultException();
        }
    }

    /**
     * Abstract base class for User Defined Type (UDT) codecs.
     * 
     * <p>This class provides a framework for creating custom type codecs that convert between
     * Cassandra UDT values and Java objects. It supports mapping to Collections, Maps, and
     * Java Bean classes.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Define a custom Address class
     * public class Address {
     *     private String street;
     *     private String city;
     *     private String zipCode;
     *     // getters and setters...
     * }
     * 
     * // Create a codec for the Address UDT
     * UserType addressType = cluster.getMetadata()
     *     .getKeyspace("mykeyspace")
     *     .getUserType("address");
     * 
     * UDTCodec<Address> addressCodec = UDTCodec.create(addressType, Address.class);
     * cluster.getConfiguration().getCodecRegistry().register(addressCodec);
     * }</pre>
     * 
     * @param <T> the Java type this codec handles
     */
    public abstract static class UDTCodec<T> extends TypeCodec<T> {

        private final UserType userType;
        private final Class<T> javaClazz;
        private final TypeCodec<UDTValue> udtValueTypeCodec;

        /**
         * Constructs a new UDT codec.
         * 
         * @param userType the Cassandra user type definition
         * @param javaClazz the Java class to map to
         */
        protected UDTCodec(final UserType userType, final Class<T> javaClazz) {
            super(userType, javaClazz);
            this.userType = userType;
            this.javaClazz = javaClazz;
            udtValueTypeCodec = TypeCodec.userType(userType);
        }

        /**
         * Creates a UDT codec for the specified user type and Java class.
         * 
         * <p>This factory method creates a codec that can convert between Cassandra UDT values
         * and Java objects. The Java class can be a Collection, Map, or Bean class.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * UserType addressType = session.getCluster().getMetadata()
         *     .getKeyspace("mykeyspace")
         *     .getUserType("address");
         * 
         * UDTCodec<Address> codec = UDTCodec.create(addressType, Address.class);
         * }</pre>
         * 
         * @param <T> the Java type
         * @param userType the Cassandra user type
         * @param javaClazz the Java class to map to
         * @return a new UDT codec instance
         * @throws IllegalArgumentException if javaClazz is not a Collection, Map, or Bean class
         */
        public static <T> UDTCodec<T> create(final UserType userType, final Class<T> javaClazz) {
            return new UDTCodec<>(userType, javaClazz) {
                @Override
                protected T deserialize(final UDTValue udtValue) {
                    if (udtValue == null) {
                        return null;
                    }

                    final int size = userType.size();

                    if (Collection.class.isAssignableFrom(javaClazz)) {
                        final Collection<Object> coll = N.newCollection((Class<Collection<Object>>) javaClazz, size);

                        for (int i = 0; i < size; i++) {
                            coll.add(udtValue.getObject(i));
                        }

                        return (T) coll;
                    } else if (Map.class.isAssignableFrom(javaClazz)) {
                        final Map<String, Object> map = N.newMap((Class<Map<String, Object>>) javaClazz);
                        final Collection<String> fieldNames = userType.getFieldNames();

                        for (final String fieldName : fieldNames) {
                            map.put(fieldName, udtValue.getObject(fieldName));
                        }

                        return (T) map;
                    } else if (Beans.isBeanClass(javaClazz)) {
                        final BeanInfo beanInfo = ParserUtil.getBeanInfo(javaClazz);
                        final Collection<String> fieldNames = userType.getFieldNames();
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
                protected UDTValue serialize(final T value) {
                    if (value == null) {
                        return null;
                    }

                    final UDTValue udtValue = newUDTValue();

                    if (value instanceof Collection) {
                        final Collection<Object> coll = (Collection<Object>) value;
                        int idx = 0;

                        for (final Object val : coll) {
                            if (val == null) {
                                udtValue.setToNull(idx++);
                            } else {
                                udtValue.set(idx++, val, (Class<Object>) val.getClass());
                            }
                        }
                    } else if (value instanceof Map) {
                        final Map<String, Object> map = (Map<String, Object>) value;
                        final Collection<String> fieldNames = userType.getFieldNames();
                        Object propValue;

                        for (final String fieldName : fieldNames) {
                            propValue = map.get(fieldName);

                            if (propValue == null) {
                                udtValue.setToNull(fieldName);
                            } else {
                                udtValue.set(fieldName, propValue, (Class<Object>) propValue.getClass());
                            }
                        }
                    } else if (Beans.isBeanClass(javaClazz)) {
                        final BeanInfo beanInfo = ParserUtil.getBeanInfo(javaClazz);
                        //noinspection UnnecessaryLocalVariable
                        final Object bean = value;
                        final Collection<String> fieldNames = userType.getFieldNames();
                        PropInfo propInfo = null;
                        Object propValue = null;

                        for (final String fieldName : fieldNames) {
                            propInfo = beanInfo.getPropInfo(fieldName);

                            if (propInfo != null) {
                                propValue = propInfo.getPropValue(bean);

                                if (propValue == null) {
                                    udtValue.setToNull(fieldName);
                                } else {
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
         * Creates a UDT codec using cluster metadata.
         * 
         * <p>This convenience method looks up the user type from the cluster metadata
         * and creates a codec for it.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * UDTCodec<Address> codec = UDTCodec.create(
         *     cluster, "mykeyspace", "address", Address.class
         * );
         * }</pre>
         * 
         * @param <T> the Java type
         * @param cluster the Cassandra cluster
         * @param keySpace the keyspace containing the UDT
         * @param userType the name of the user type
         * @param javaClazz the Java class to map to
         * @return a new UDT codec instance
         * @throws IllegalArgumentException if the user type doesn't exist
         */
        public static <T> UDTCodec<T> create(final Cluster cluster, final String keySpace, final String userType, final Class<T> javaClazz) {
            return create(cluster.getMetadata().getKeyspace(keySpace).getUserType(userType), javaClazz);
        }

        /**
         * Serializes a Java object to a ByteBuffer.
         * 
         * @param value the value to serialize
         * @param protocolVersion the protocol version to use
         * @return the serialized ByteBuffer
         * @throws InvalidTypeException if serialization fails
         */
        @Override
        public ByteBuffer serialize(final T value, final ProtocolVersion protocolVersion) throws InvalidTypeException {
            return udtValueTypeCodec.serialize(serialize(value), protocolVersion);
        }

        /**
         * Deserializes a ByteBuffer to a Java object.
         * 
         * @param bytes the ByteBuffer to deserialize
         * @param protocolVersion the protocol version to use
         * @return the deserialized object
         * @throws InvalidTypeException if deserialization fails
         */
        @Override
        public T deserialize(final ByteBuffer bytes, final ProtocolVersion protocolVersion) throws InvalidTypeException {
            return deserialize(udtValueTypeCodec.deserialize(bytes, protocolVersion));
        }

        /**
         * Parses a string representation to a Java object.
         * 
         * @param value the string to parse
         * @return the parsed object
         * @throws InvalidTypeException if parsing fails
         */
        @Override
        public T parse(final String value) throws InvalidTypeException {
            return Strings.isEmpty(value) || NULL_STR.equals(value) ? null : N.fromJson(value, javaClazz);
        }

        /**
         * Formats a Java object to its string representation.
         * 
         * @param value the value to format
         * @return the formatted string
         * @throws InvalidTypeException if formatting fails
         */
        @Override
        public String format(final T value) throws InvalidTypeException {
            return value == null ? NULL_STR : N.toJson(value);
        }

        /**
         * Creates a new UDT value instance.
         * 
         * @return a new UDTValue
         */
        protected UDTValue newUDTValue() {
            return userType.newValue();
        }

        /**
         * Serializes a Java object to a UDT value.
         * 
         * @param value the value to serialize
         * @return the serialized UDTValue
         */
        protected abstract UDTValue serialize(T value);

        /**
         * Deserializes a UDT value to a Java object.
         * 
         * @param udtValue the UDT value to deserialize
         * @return the deserialized object
         */
        protected abstract T deserialize(UDTValue udtValue);
    }

    /**
     * Type codec for storing Java objects as JSON strings in VARCHAR columns.
     * 
     * <p>This codec serializes Java objects to JSON strings for storage in Cassandra
     * VARCHAR columns, and deserializes them back to Java objects when reading.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Register a codec for storing complex objects as JSON
     * StringCodec<UserPreferences> prefsCodec = new StringCodec<>(UserPreferences.class);
     * cluster.getConfiguration().getCodecRegistry().register(prefsCodec);
     * 
     * // Now UserPreferences objects can be stored in VARCHAR columns
     * UserPreferences prefs = new UserPreferences();
     * executor.execute("INSERT INTO users (id, preferences) VALUES (?, ?)", 
     *     userId, prefs);
     * }</pre>
     * 
     * @param <T> the Java type this codec handles
     */
    static class StringCodec<T> extends TypeCodec<T> {
        private static final TypeCodec<String> stringTypeCodec = TypeCodec.varchar();
        private final Class<T> javaClazz;

        /**
         * Constructs a new string codec for the specified Java class.
         * 
         * @param javaClazz the Java class to handle
         */
        protected StringCodec(final Class<T> javaClazz) {
            super(DataType.varchar(), javaClazz);
            this.javaClazz = javaClazz;
        }

        /**
         * Serializes a Java object to a ByteBuffer.
         * 
         * @param value the value to serialize
         * @param protocolVersion the protocol version to use
         * @return the serialized ByteBuffer
         * @throws InvalidTypeException if serialization fails
         */
        @Override
        public ByteBuffer serialize(final T value, final ProtocolVersion protocolVersion) throws InvalidTypeException {
            return stringTypeCodec.serialize(serialize(value), protocolVersion);
        }

        /**
         * Deserializes a ByteBuffer to a Java object.
         * 
         * @param bytes the ByteBuffer to deserialize
         * @param protocolVersion the protocol version to use
         * @return the deserialized object
         * @throws InvalidTypeException if deserialization fails
         */
        @Override
        public T deserialize(final ByteBuffer bytes, final ProtocolVersion protocolVersion) throws InvalidTypeException {
            return deserialize(stringTypeCodec.deserialize(bytes, protocolVersion));
        }

        /**
         * Parses a string representation to a Java object.
         * 
         * @param value the string to parse
         * @return the parsed object
         * @throws InvalidTypeException if parsing fails
         */
        @Override
        public T parse(final String value) throws InvalidTypeException {
            return Strings.isEmpty(value) || NULL_STR.equals(value) ? null : N.fromJson(value, javaClazz);
        }

        /**
         * Formats a Java object to its string representation.
         * 
         * @param value the value to format
         * @return the formatted string
         * @throws InvalidTypeException if formatting fails
         */
        @Override
        public String format(final T value) throws InvalidTypeException {
            return value == null ? NULL_STR : N.toJson(value);
        }

        /**
         * Serializes a Java object to a JSON string.
         * 
         * @param value the value to serialize
         * @return the JSON string
         */
        protected String serialize(final T value) {
            return N.toJson(value);
        }

        /**
         * Deserializes a JSON string to a Java object.
         * 
         * @param value the JSON string to deserialize
         * @return the deserialized object
         */
        protected T deserialize(final String value) {
            return N.fromJson(value, javaClazz);
        }
    }

    /**
     * Configuration settings for Cassandra statements.
     * 
     * <p>This class encapsulates all configurable options for statement execution,
     * including consistency levels, retry policies, timeouts, and tracing options.
     * It uses a fluent builder pattern for easy configuration.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * StatementSettings settings = StatementSettings.builder()
     *     .consistency(ConsistencyLevel.LOCAL_QUORUM)
     *     .serialConsistency(ConsistencyLevel.SERIAL)
     *     .retryPolicy(DefaultRetryPolicy.INSTANCE)
     *     .fetchSize(5000)
     *     .readTimeoutMillis(30000)
     *     .traceQuery(true)
     *     .build();
     * 
     * CassandraExecutor executor = new CassandraExecutor(session, settings);
     * }</pre>
     */
    @Builder
    @Data
    @Accessors(fluent = true)
    public static final class StatementSettings {
        private ConsistencyLevel consistency;
        private ConsistencyLevel serialConsistency;
        private RetryPolicy retryPolicy;
        private Integer fetchSize;
        private Integer readTimeoutMillis;
        private Boolean traceQuery;

        /**
         * Creates a new StatementSettings instance with default null values for all settings.
         *
         * <p>All statement configuration options are initialized to {@code null}, which means
         * the driver's default values will be used when statements are executed. This constructor
         * is primarily used when you want to selectively configure only specific settings using
         * the fluent setter methods.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * StatementSettings settings = new StatementSettings();
         * settings.consistency(ConsistencyLevel.QUORUM)
         *         .fetchSize(1000);
         * CassandraExecutor executor = new CassandraExecutor(session, settings);
         * }</pre>
         */
        public StatementSettings() {
        }

        /**
         * Creates a new StatementSettings instance with all specified configuration values.
         *
         * <p>This constructor allows you to set all statement execution parameters at once.
         * Any parameter set to {@code null} will use the driver's default value. This is useful
         * when you want to create a fully configured settings object in a single call without
         * using the builder pattern or fluent setters.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * StatementSettings settings = new StatementSettings(
         *     ConsistencyLevel.LOCAL_QUORUM,
         *     ConsistencyLevel.SERIAL,
         *     DefaultRetryPolicy.INSTANCE,
         *     5000,
         *     30000,
         *     false
         * );
         * CassandraExecutor executor = new CassandraExecutor(session, settings);
         * }</pre>
         *
         * @param consistency the consistency level for regular reads/writes, or {@code null} for driver default
         * @param serialConsistency the serial consistency level for conditional operations, or {@code null} for driver default
         * @param retryPolicy the retry policy for failed operations, or {@code null} for driver default
         * @param fetchSize the number of rows to fetch per page, or {@code null} for driver default
         * @param readTimeoutMillis the read timeout in milliseconds, or {@code null} for driver default
         * @param traceQuery whether to enable query tracing ({@code true}/{@code false}), or {@code null} for driver default
         */
        public StatementSettings(final ConsistencyLevel consistency, final ConsistencyLevel serialConsistency, final RetryPolicy retryPolicy,
                final Integer fetchSize, final Integer readTimeoutMillis, final Boolean traceQuery) {
            this.consistency = consistency;
            this.serialConsistency = serialConsistency;
            this.retryPolicy = retryPolicy;
            this.fetchSize = fetchSize;
            this.readTimeoutMillis = readTimeoutMillis;
            this.traceQuery = traceQuery;
        }
    }
}
