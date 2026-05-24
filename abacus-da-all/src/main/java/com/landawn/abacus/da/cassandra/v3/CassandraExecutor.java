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
import com.landawn.abacus.da.cassandra.CassandraExecutorBase;
import com.landawn.abacus.da.cassandra.CqlBuilder;
import com.landawn.abacus.da.cassandra.CqlBuilder.NSC;
import com.landawn.abacus.da.cassandra.CqlMapper;
import com.landawn.abacus.da.cassandra.ParsedCql;
import com.landawn.abacus.exception.DuplicateResultException;
import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;
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
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.ImmutableList;
import com.landawn.abacus.util.IntFunctions;
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
 * // Async operations (returned by AsyncCassandraExecutor)
 * ContinuableFuture<List<User>> futureUsers = executor.async().list(User.class,
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

    private static final Logger logger = LoggerFactory.getLogger(CassandraExecutor.class);

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

    private final AsyncCassandraExecutor asyncCassandraExecutor;

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
     * @throws NullPointerException if session is null
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
     * @throws NullPointerException if session is null
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
     * CqlMapper cqlMapper = new CqlMapper("queries.xml");
     * CassandraExecutor executor = new CassandraExecutor(session, settings, cqlMapper);
     * // Now you can use named queries: executor.execute(cqlMapper.get("findUserById"), userId);
     * }</pre>
     * 
     * @param session the Cassandra session from Driver 3.x
     * @param settings default statement configuration, or null for driver defaults
     * @param cqlMapper mapper containing pre-configured CQL statements, or null if not needed
     * @throws NullPointerException if session is null
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
     * @throws NullPointerException if session is null
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

        asyncCassandraExecutor = new AsyncCassandraExecutor(this);
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
     * Returns an asynchronous facade backed by this executor.
     *
     * <p>The returned {@link AsyncCassandraExecutor} exposes the same Cassandra operations
     * but submits them to an internal executor service and returns
     * {@link com.landawn.abacus.util.ContinuableFuture} results, allowing non-blocking
     * composition of multiple Cassandra calls.</p>
     *
     * <p>The returned instance is owned by this executor; do not close it independently.
     * Its lifetime is bound to this {@code CassandraExecutor}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ContinuableFuture<List<User>> future = executor.async().list(User.class,
     *     "SELECT * FROM users WHERE status = ?", "active");
     * }</pre>
     *
     * @return the {@link AsyncCassandraExecutor} bound to this executor
     */
    public AsyncCassandraExecutor async() {
        return asyncCassandraExecutor;
    }

    /**
     * Returns the DataStax Driver 3.x object {@link Mapper} for the given annotated entity class.
     *
     * <p>This is the driver's built-in object mapper (resolved via the internal
     * {@link MappingManager}); the entity class must be annotated with the driver's mapping
     * annotations (e.g. {@code @Table}, {@code @PartitionKey}, {@code @Column}). The
     * mapper exposes CRUD-style methods ({@code save}, {@code get}, {@code delete}) and is
     * independent of the higher-level CQL helpers on this executor.</p>
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
     * @param targetClass the entity class (must carry driver 3.x mapping annotations)
     * @return the DataStax {@link Mapper} for the entity class
     */
    public <T> Mapper<T> mapper(final Class<T> targetClass) {
        return mappingManager.mapper(targetClass);
    }

    /**
     * Registers a JSON-based string codec for the specified Java class on this executor's
     * {@link CodecRegistry}.
     *
     * <p>The registered codec is a {@link StringCodec} that serializes objects of
     * {@code javaClazz} to JSON via {@code N.toJson(value)} and stores them in a CQL
     * {@code varchar} (text) column. On read, the JSON text is parsed back into
     * {@code javaClazz} via {@code N.fromJson(...)}. This avoids defining a Cassandra
     * User Defined Type (UDT) for arbitrary application objects.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Register codec for custom class
     * executor.registerTypeCodec(Address.class);
     *
     * // Now Address objects can be used directly in CQL operations against a varchar column
     * Address address = new Address("123 Main St", "Springfield", "12345");
     * executor.execute("INSERT INTO users (id, name, address) VALUES (?, ?, ?)",
     *     UUID.randomUUID(), "John Doe", address);
     *
     * // Retrieve and deserialize automatically
     * User user = executor.findFirst(User.class,
     *     "SELECT * FROM users WHERE id = ?", userId).orElse(null);
     * // user.getAddress() returns the Address object automatically
     * }</pre>
     *
     * @param javaClazz the Java class for which to register a string-based codec
     * @throws NullPointerException if {@code javaClazz} is {@code null}
     * @see #registerTypeCodec(CodecRegistry, Class)
     * @see StringCodec
     */
    public void registerTypeCodec(final Class<?> javaClazz) {
        registerTypeCodec(codecRegistry, javaClazz);
    }

    /**
     * Registers a JSON-based string codec for the specified Java class in the given
     * {@link CodecRegistry}.
     *
     * <p>This static helper registers a {@link StringCodec} (JSON-over-{@code varchar})
     * in any codec registry, not just the one associated with this executor.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CodecRegistry registry = cluster.getConfiguration().getCodecRegistry();
     * CassandraExecutor.registerTypeCodec(registry, CustomData.class);
     * }</pre>
     *
     * @param codecRegistry the codec registry to register the codec in
     * @param javaClazz the Java class to register a codec for
     * @throws NullPointerException if {@code codecRegistry} or {@code javaClazz} is {@code null}
     * @see StringCodec
     */
    public static void registerTypeCodec(final CodecRegistry codecRegistry, final Class<?> javaClazz) {
        codecRegistry.register(new StringCodec<>(javaClazz));
    }

    /**
     * Extracts data from a ResultSet into a {@link Dataset} without per-column type hints.
     *
     * <p>This method consumes all rows in {@code resultSet} (via {@link ResultSet#all()})
     * and returns a {@link Dataset} that preserves column names and the raw values produced
     * by {@link Row#getObject(int)}. Nested {@link Row} values are recursively flattened
     * to {@code Object[]}.</p>
     *
     * <p>Because no target class is provided, no per-column type conversion is performed —
     * values are stored as returned by the driver.</p>
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
     * @return a {@link Dataset} containing all rows from the ResultSet
     * @see #extractData(ResultSet, Class)
     */
    public static Dataset extractData(final ResultSet resultSet) {
        return extractData(resultSet, null);
    }

    /**
     * Extracts data from a ResultSet into a {@link Dataset}, using {@code targetClass} as a
     * per-column type hint.
     *
     * <p>This method consumes all rows in {@code resultSet} (via {@link ResultSet#all()})
     * and returns a {@link Dataset} preserving column names. When {@code targetClass} is a
     * bean class, each column value is converted (via {@code N.convert}) to the type of the
     * matching bean property where one exists. Nested {@link Row} values are recursively
     * converted to the matching property type.</p>
     *
     * <p>When {@code targetClass} is {@code null} or a {@link Map} class, values are stored
     * as returned by the driver with no per-column conversion.</p>
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
     * @param targetClass an entity class with getter/setter methods, a {@link Map} class,
     *                    or {@code null} for no type hint
     * @return a {@link Dataset} containing all rows from the ResultSet, with values converted
     *         to match {@code targetClass} property types when applicable
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
     * Converts every row of {@code resultSet} into an instance of {@code targetClass}
     * and returns them as a {@link List}.
     *
     * <p>Supported {@code targetClass} values are:</p>
     * <ul>
     *   <li>{@link Row} — rows are returned unchanged (the existing list from
     *       {@link ResultSet#all()} is returned)</li>
     *   <li>An array class — each row becomes an array of column values, recursively
     *       flattening nested {@link Row}s</li>
     *   <li>A {@link Collection} class — each row becomes a new collection of column values</li>
     *   <li>A {@link Map} class — each row becomes a new map keyed by column name</li>
     *   <li>A JavaBean class — each row is mapped via {@link #toEntity(Row, Class)}</li>
     *   <li>Any single-value type — used only when the result set has exactly one column;
     *       the column value is converted to {@code T}</li>
     * </ul>
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
     * @param targetClass the per-row target type (see supported types above)
     * @return a list of converted rows
     * @throws IllegalArgumentException if {@code targetClass} is a single-value type but
     *         the result set has more than one column
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
     * <li><strong>Naming-policy mapping:</strong> Column-to-property name resolution via
     *     {@link QueryUtil#getColumn2PropNameMap(Class)} (e.g., snake_case columns to camelCase properties)</li>
     * <li><strong>Nested properties:</strong> Dot-notation property paths (e.g., {@code "address.city"})
     *     when no direct property is found</li>
     * <li><strong>Null values:</strong> {@code null} column values are written through as {@code null}</li>
     * <li><strong>Nested {@link Row} values:</strong> Recursively converted via {@link #readRow(Row, Class)}
     *     to the property type</li>
     * </ul>
     *
     * <p>Note: this method does <em>not</em> perform UDT-to-bean conversion itself; UDT
     * columns require a registered {@link UDTCodec} or are surfaced as {@link UDTValue}.</p>
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
     * Converts a Cassandra Row to a {@link HashMap} of column name to value.
     *
     * <p>This convenience overload delegates to {@link #toMap(Row, IntFunction)} using
     * {@link IntFunctions#ofMap()}, which returns a {@link HashMap}. The returned map
     * therefore does <em>not</em> preserve column order. Nested {@link Row} values are
     * recursively converted to maps via the same supplier.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Row row = resultSet.one();
     * Map<String, Object> map = CassandraExecutor.toMap(row);
     * Object value = map.get("column_name");
     * }</pre>
     *
     * @param row the Cassandra Row to convert
     * @return a {@link HashMap} containing all column name-value pairs from the row
     * @see #toMap(Row, IntFunction)
     */
    public static Map<String, Object> toMap(final Row row) {
        return toMap(row, IntFunctions.ofMap());
    }

    /**
     * Converts a Cassandra Row to a Map using a caller-supplied map factory.
     *
     * <p>The {@code supplier} receives the expected column count and must return a fresh,
     * mutable {@link Map} instance. Use this overload when you need a specific map
     * implementation (e.g., {@link java.util.LinkedHashMap LinkedHashMap} to preserve
     * column order, or {@link java.util.TreeMap TreeMap} for sorted keys). Nested
     * {@link Row} values are recursively converted using the same supplier.</p>
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
     * @param supplier factory invoked with the expected column count to create the target map
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
     * Reads a single {@link Row} and converts it to {@code rowClass}.
     *
     * <p>Internal dispatcher used by the public conversion helpers. Behavior by
     * {@code rowClass}:</p>
     * <ul>
     *   <li>{@code null} or an object-array class: each column becomes an array element;
     *       nested {@link Row}s are recursively flattened to {@code Object[]}</li>
     *   <li>{@link Collection} class: a fresh collection of column values</li>
     *   <li>{@link Map} class: a fresh map keyed by column name</li>
     *   <li>Bean class: delegates to {@link #toEntity(Row, Class)}</li>
     *   <li>Single-value type: only legal when the row has exactly one column; the value
     *       is converted to {@code T}</li>
     * </ul>
     *
     * @param <T> the target type
     * @param row the Cassandra Row to read, or {@code null}
     * @param rowClass the target class, or {@code null} to default to {@code Object[]}
     * @return the converted object, or {@link N#defaultValueOf(Class)} when {@code row} is {@code null}
     *         and {@code rowClass} is non-null
     * @throws IllegalArgumentException if {@code rowClass} is a single-value type but the row has
     *         more than one column
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
     * Builds a specialized {@code Row -> T} mapper for the given target class and column metadata.
     *
     * <p>The returned function chooses one of several strategies up front (array, collection,
     * map, bean, or single-value) and uses that strategy for every row. For single-value
     * conversions the mapper caches whether the runtime column type is already assignable
     * to {@code T} to skip {@code N.convert} on later rows.</p>
     *
     * @param <T> the target type
     * @param rowClass the target class for row conversion
     * @param columnDefinitions the column definitions of the result set
     * @return a function that maps {@link Row} to {@code T}
     * @throws IllegalArgumentException if {@code rowClass} is a single-value type but the
     *         result set has more than one column
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
     * Gets a single entity matching the specified condition, or {@code null} if none is found.
     *
     * <p>This method retrieves at most one entity matching the specified condition.
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
     * Nullable<Long> count = executor.queryForSingleValue(Long.class,
     *     "SELECT count(*) FROM users WHERE status = ?", "active");
     *
     * Nullable<BigDecimal> maxSalary = executor.queryForSingleValue(
     *     BigDecimal.class, "SELECT max(salary) FROM employees WHERE department = ?", "engineering");
     *
     * Nullable<String> email = executor.queryForSingleValue(
     *     String.class, "SELECT email FROM users WHERE id = ?", userId);
     * }</pre>
     *
     * @param <E> the type of the single result value to be returned
     * @param valueClass the Java class the column value is converted to
     * @param query the CQL query string with {@code ?} placeholders for parameters
     * @param parameters the values to bind, in declaration order
     * @return a <i>present</i> {@code Nullable<E>} holding the column value (possibly {@code null} for
     *         {@code NULL}) when at least one row is returned; {@code Nullable.empty()} when the query
     *         returns no rows
     * @throws IllegalArgumentException if {@code valueClass} or {@code query} is {@code null}
     * @see #queryForSingleNonNull(Class, String, Object...)
     */
    @Override
    public <E> Nullable<E> queryForSingleValue(final Class<E> valueClass, final String query, final Object... parameters) {
        final ResultSet resultSet = execute(query, parameters);
        final Row row = resultSet.one();

        return row == null ? (Nullable<E>) Nullable.empty() : Nullable.of(N.convert(row.getObject(0), valueClass));
    }

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
     * Optional<String> email = executor.queryForSingleNonNull(String.class,
     *     "SELECT email FROM users WHERE id = ?", userId);
     * }</pre>
     *
     * @param <E> the type of the single result value to be returned
     * @param valueClass the Java class the column value is converted to
     * @param query the CQL query string with {@code ?} placeholders for parameters
     * @param parameters the values to bind, in declaration order
     * @return a <i>present</i> {@code Optional<E>} holding the (non-null) column value when at least
     *         one row is returned with a non-null value; {@code Optional.empty()} when the query
     *         returns no rows
     * @throws NullPointerException if a row is returned but the column value (or its conversion) is
     *         {@code null}, because {@link Optional#of(Object)} rejects a null payload
     * @see #queryForSingleValue(Class, String, Object...)
     */
    @Override
    public <E> Optional<E> queryForSingleNonNull(final Class<E> valueClass, final String query, final Object... parameters) {
        final ResultSet resultSet = execute(query, parameters);
        final Row row = resultSet.one();

        return row == null ? (Optional<E>) Optional.empty() : Optional.of(N.convert(row.getObject(0), valueClass));
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
     *     "SELECT * FROM users WHERE email = ? LIMIT 1", email);
     *
     * Optional<User> latestUser = executor.findFirst(User.class,
     *     "SELECT * FROM users ORDER BY created_at DESC LIMIT 1");
     *
     * Optional<Map<String, Object>> userData = executor.findFirst(Map.class,
     *     "SELECT name, email FROM users WHERE id = ?", userId);
     *
     * Optional<Object[]> stats = executor.findFirst(Object[].class,
     *     "SELECT count(*), max(created_at) FROM events WHERE type = ?", "login");
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
     * @param statement the statement to execute
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
     * @throws NullPointerException if query is null
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
     * Closes the executor and releases all associated resources.
     *
     * <p>This method closes, in order: the underlying Cassandra {@link Session}, the
     * {@link Cluster}, and the internal statement and prepared-statement pools. Sessions
     * and clusters already closed are skipped. After calling this method, the executor
     * instance must not be used for any further operations.</p>
     *
     * <p><strong>Note:</strong> closing the executor closes the {@code Session} and
     * {@code Cluster} that were supplied at construction time, so do not share those
     * resources with other components if you intend to call this method.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CassandraExecutor executor = new CassandraExecutor(session);
     * try {
     *     // Perform database operations
     * } finally {
     *     executor.close();   // Closes session, cluster, and internal pools
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
     * Builds a {@link BatchStatement} containing one {@code INSERT} per entity.
     *
     * <p>Each element of {@code entities} is converted to a parameterized INSERT via
     * {@code prepareInsert(entity)} and added to a new batch of the given {@code type}.
     * The batch is not executed by this method.</p>
     *
     * @param entities the entities to insert; must be non-null, non-empty, and contain no {@code null}
     * @param type the batch type ({@link BatchStatement.Type#LOGGED}, {@code UNLOGGED}, or {@code COUNTER});
     *        {@code null} is treated as {@code LOGGED} by {@link #prepareBatchStatement(BatchStatement.Type)}
     * @return a populated {@link BatchStatement} ready for execution
     * @throws IllegalArgumentException if {@code entities} is null, empty, or contains a {@code null} element
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
     * Builds a {@link BatchStatement} containing one {@code INSERT} per property map.
     *
     * <p>Each element of {@code propsList} (a property-name &rarr; value map) is converted
     * to a parameterized INSERT for {@code targetClass} via {@code prepareInsert(targetClass, props)}
     * and added to a new batch of the given {@code type}. This overload is intended for
     * dynamic data where entity instances are not available.</p>
     *
     * @param targetClass the entity class whose table/columns are targeted
     * @param propsList property-name &rarr; value maps, one per INSERT;
     *        must be non-null, non-empty, and contain no {@code null} elements
     * @param type the batch type; {@code null} is treated as {@link BatchStatement.Type#LOGGED}
     * @return a populated {@link BatchStatement} ready for execution
     * @throws IllegalArgumentException if {@code propsList} is null, empty, or contains a {@code null} element
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
     * Builds a {@link BatchStatement} containing one {@code UPDATE} per entity, restricted
     * to the columns named by {@code propNamesToUpdate}.
     *
     * <p>For each entity, an UPDATE is prepared via {@code prepareUpdate(entity, propNamesToUpdate)}
     * and added to a new batch of the given {@code type}. The WHERE clause is derived from
     * the entity's primary-key properties.</p>
     *
     * @param entities the entities to update; must be non-null, non-empty, and contain no {@code null}
     * @param propNamesToUpdate the property names to include in each UPDATE; must be non-null and non-empty
     * @param type the batch type; {@code null} is treated as {@link BatchStatement.Type#LOGGED}
     * @return a populated {@link BatchStatement} ready for execution
     * @throws IllegalArgumentException if {@code entities} or {@code propNamesToUpdate} is null or empty,
     *         or if {@code entities} contains a {@code null} element
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
     * Builds a {@link BatchStatement} containing one {@code UPDATE} per property map.
     *
     * <p>For each map in {@code propsList}, the primary-key entries (as determined by
     * {@code getKeyNameSet(targetClass)}) are extracted to form an equality WHERE clause,
     * and the remaining entries become the SET assignments. Each resulting UPDATE is added
     * to a new batch of the given {@code type}. Every map must therefore contain values for
     * all primary-key columns of {@code targetClass}.</p>
     *
     * @param targetClass the entity class whose table/columns are targeted
     * @param propsList property-name &rarr; value maps, one per UPDATE; each map must include
     *        all primary-key values for {@code targetClass}. Must be non-null, non-empty,
     *        and contain no {@code null} elements
     * @param type the batch type; {@code null} is treated as {@link BatchStatement.Type#LOGGED}
     * @return a populated {@link BatchStatement} ready for execution
     * @throws IllegalArgumentException if {@code propsList} is null, empty, or contains a {@code null} element
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
     * Builds a {@link BatchStatement} that runs the same CQL {@code query} once per element
     * of {@code parametersList}, each with its own parameter binding.
     *
     * <p>Each element of {@code parametersList} is forwarded as the {@code parameters} argument
     * to {@link #prepareStatement(String, Object...)}, so it may itself be an {@code Object[]},
     * a {@link Collection}, a {@link Map} (for named parameters), or a bean.</p>
     *
     * @param query the CQL statement to execute; must be non-null
     * @param parametersList parameter bundles, one per execution; must be non-null, non-empty,
     *        and contain no {@code null} elements
     * @param type the batch type; {@code null} is treated as {@link BatchStatement.Type#LOGGED}
     * @return a populated {@link BatchStatement} ready for execution
     * @throws IllegalArgumentException if {@code parametersList} is null, empty, or contains a {@code null} element
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
     * Creates a new, empty {@link BatchStatement} of the given type and applies this
     * executor's {@link StatementSettings} via {@link #configStatement(Statement)}.
     *
     * @param type the batch type ({@link BatchStatement.Type#LOGGED}, {@code UNLOGGED}, or {@code COUNTER});
     *        {@code null} is treated as {@code LOGGED}
     * @return a configured, empty {@link BatchStatement}
     */
    @Override
    protected BatchStatement prepareBatchStatement(final BatchStatement.Type type) {
        final BatchStatement stmt = new BatchStatement(type == null ? BatchStatement.Type.LOGGED : type);

        configStatement(stmt);

        return stmt;
    }

    /**
     * Returns an executable {@link Statement} for a parameterless CQL query.
     *
     * <p>For queries no longer than {@link #POOLABLE_LENGTH} characters, the resulting
     * {@link BoundStatement} is cached in an internal pool keyed by the query string so
     * subsequent calls with the same query reuse the prepared/bound form. For longer
     * queries the statement is always built fresh.</p>
     *
     * @param query the CQL query to prepare; must be non-null
     * @return an executable {@link Statement}
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
     * Returns an executable {@link Statement} for the given CQL query with parameters bound.
     *
     * <p>Parameters can be supplied in several forms:</p>
     * <ul>
     *   <li>Positional values matching {@code ?} placeholders</li>
     *   <li>A single {@code Object[]} or {@link Collection} containing all positional values</li>
     *   <li>A single {@link Map} (named parameters: keys match {@code :name} placeholders or
     *       the column names of the prepared statement variables)</li>
     *   <li>A single bean instance (named parameters resolved via bean property getters)</li>
     * </ul>
     *
     * <p>Values are coerced to the column's declared Java type using the executor's
     * {@link CodecRegistry} and {@code N.convert(...)} where applicable. If
     * {@code parameters} is empty the call is delegated to {@link #prepareStatement(String)}.
     * For queries no longer than {@link #POOLABLE_LENGTH} characters the underlying
     * {@link PreparedStatement} is cached and reused.</p>
     *
     * @param query the CQL query to prepare; must be non-null
     * @param parameters the parameters to bind, in one of the supported forms
     * @return an executable {@link Statement} with parameters bound
     * @throws IllegalArgumentException if a required parameter is missing, if a named
     *         parameter cannot be resolved, or if fewer values are supplied than the
     *         prepared statement requires
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

            if (parameters[0] == null
                    || (javaClazz == null || javaClazz.isAssignableFrom(parameters[0].getClass()) || codecRegistry.codecFor(colType).accepts(parameters[0]))) {
                return bind(preStmt, parameters);
            } else if (parameters[0] instanceof List && ((List<Object>) parameters[0]).size() == 1) {
                final Object tmp = ((List<Object>) parameters[0]).get(0);

                if (tmp == null || (javaClazz.isAssignableFrom(tmp.getClass()) || codecRegistry.codecFor(colType).accepts(tmp))) {
                    return bind(preStmt, tmp);
                }
            }
        }

        Object[] values = parameters;

        if (parameters.length == 1 && parameters[0] != null && (parameters[0] instanceof Map || Beans.isBeanClass(parameters[0].getClass()))) {
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

        if (values.length < parameterCount) {
            throw new IllegalArgumentException(
                    "Not enough parameters for parameterized query: expected " + parameterCount + " but got " + values.length + " for query: " + query);
        }

        for (int i = 0; i < parameterCount; i++) {
            colType = columnDefinitions.getType(i);
            javaClazz = namedDataType.get(colType.getName().name());

            if (values[i] == null) {
                values[i] = javaClazz == null ? null : N.defaultValueOf(javaClazz);
            } else if (javaClazz == null || javaClazz.isAssignableFrom(values[i].getClass()) || codecRegistry.codecFor(colType).accepts(values[i])) {
                // continue;
            } else {
                try {
                    values[i] = N.convert(values[i], javaClazz);
                } catch (final ClassCastException | IllegalArgumentException e) {
                    // Type conversion failed, keep original value
                    if (logger.isDebugEnabled()) {
                        logger.debug("Failed to convert parameter at index {} from {} to {}; keeping original value", i,
                                values[i] == null ? null : values[i].getClass().getName(), javaClazz.getName(), e);
                    }
                }
            }
        }

        return bind(preStmt, values.length == parameterCount ? values : N.copyOfRange(values, 0, parameterCount));
    }

    /**
     * Calls {@link Session#prepare(String)} on the underlying session and applies the
     * subset of this executor's {@link StatementSettings} that can be set on a
     * {@link PreparedStatement} (consistency, serial consistency, retry policy, tracing).
     *
     * <p>Read-timeout and fetch size are not propagated here; they are applied when the
     * resulting bound statement is configured via {@link #configStatement(Statement)}.</p>
     *
     * @param query the CQL query to prepare; must be non-null
     * @return a {@link PreparedStatement} bound to this executor's settings, ready for
     *         {@link #bind(PreparedStatement, Object...)}
     */
    @Override
    protected PreparedStatement prepare(final String query) {
        if (logger.isDebugEnabled()) {
            logger.debug("Preparing CQL: {}", query);
        }

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
     * Binds the supplied positional parameters to {@code preStmt} and applies this
     * executor's {@link StatementSettings} to the resulting {@link BoundStatement}.
     *
     * @param preStmt the prepared statement to bind
     * @param parameters the positional parameter values, already coerced to the column types
     * @return a {@link BoundStatement} ready for execution
     * @throws com.datastax.driver.core.exceptions.InvalidTypeException if a parameter value
     *         cannot be assigned to its declared column type
     */
    @Override
    protected BoundStatement bind(final PreparedStatement preStmt, final Object... parameters) {
        final BoundStatement stmt = preStmt.bind(parameters);

        configStatement(stmt);

        return stmt;
    }

    /**
     * Applies this executor's {@link StatementSettings} to the given {@link Statement}.
     *
     * <p>For each setting on {@link #settings} that is non-{@code null}, the corresponding
     * property is set on {@code stmt}: consistency level, serial consistency level, retry
     * policy, read-timeout, fetch size, and tracing (enabled/disabled). If this executor
     * was built without a {@code StatementSettings}, this method is a no-op.</p>
     *
     * @param stmt the statement to configure; must not be {@code null}
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
     * Template-method bridge that delegates to the static
     * {@link #toList(ResultSet, Class)} with arguments reordered.
     *
     * @param <T> the target type
     * @param targetClass the per-row target type (see {@link #toList(ResultSet, Class)} for supported kinds)
     * @param rs the ResultSet to convert
     * @return a list of converted rows
     */
    @Override
    protected <T> List<T> toList(final Class<T> targetClass, final ResultSet rs) {
        return toList(rs, targetClass);
    }

    /**
     * Template-method bridge that delegates to the static
     * {@link #extractData(ResultSet, Class)} with arguments reordered.
     *
     * @param targetClass an entity class used as a per-column type hint, or {@code null}
     * @param rs the ResultSet to extract data from
     * @return a {@link Dataset} containing all rows from the ResultSet
     */
    @Override
    protected Dataset extractData(final Class<?> targetClass, final ResultSet rs) {
        return extractData(rs, targetClass);
    }

    /**
     * Returns a lazy {@link Function} that maps {@link Row} instances to {@code T}.
     *
     * <p>The returned function defers building the actual per-column mapper until it is
     * invoked for the first row, at which point it reads the {@link ColumnDefinitions}
     * from that row and caches the resulting mapper for subsequent rows. This is the
     * template-method hook used by streaming/iterating helpers in the base executor.</p>
     *
     * @param <T> the target type
     * @param rowClass the class to map rows to (entity, {@link Map}, {@link Collection},
     *        array, or a single-value type when the result has one column)
     * @return a {@link Function} that maps {@link Row} to {@code T}, lazily initialized
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
     * Adapts a user-supplied {@code (ColumnDefinitions, Row) -> T} mapper into a
     * single-argument {@link Function Function&lt;Row, T&gt;}.
     *
     * <p>The {@link ColumnDefinitions} are captured from the first row passed to the
     * returned function and reused for every subsequent row, avoiding repeated metadata
     * lookups.</p>
     *
     * @param <T> the target type
     * @param rowMapper the user mapper that accepts column metadata and a row
     * @return a {@link Function} that maps {@link Row} to {@code T}
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
     * Returns the single row of {@code resultSet} mapped to {@code targetClass}, or
     * {@code null} if the result set is empty.
     *
     * <p>If the result set contains exactly one row, that row is converted via
     * {@link #readRow(Row, Class)} and returned. If it contains no rows, {@code null}
     * is returned. If it contains more than one row, {@link DuplicateResultException}
     * is thrown.</p>
     *
     * @param <T> the target type
     * @param targetClass the entity class to map the row to
     * @param resultSet the ResultSet to fetch from
     * @return the single mapped row, or {@code null} if the result set is empty
     * @throws DuplicateResultException if more than one row is present
     */
    @Override
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
     * Abstract base class for codecs that translate between Cassandra User Defined Type
     * (UDT) values and Java objects (Driver 3.x).
     *
     * <p>This class extends the DataStax {@link TypeCodec} so the codec can be registered
     * with the driver's {@link CodecRegistry} and used transparently in CQL statements.
     * Subclasses (or the instance returned by {@link #create(UserType, Class)}) implement
     * {@link #serialize(Object)} and {@link #deserialize(UDTValue)} to map between the
     * UDT field layout and the Java type {@code T}. Supported Java types are
     * {@link Collection}, {@link Map}, and JavaBean classes.</p>
     *
     * <p>String-form serialization (used by {@link #format(Object)}/{@link #parse(String)})
     * goes through {@code N.toJson(...)}/{@code N.fromJson(...)}.</p>
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
         * Constructs a new UDT codec bound to the specified Cassandra user type and Java class.
         *
         * @param userType the Cassandra user type definition the codec encodes/decodes
         * @param javaClazz the Java class the codec maps to (a {@link Collection},
         *                  {@link Map}, or bean class)
         */
        protected UDTCodec(final UserType userType, final Class<T> javaClazz) {
            super(userType, javaClazz);
            this.userType = userType;
            this.javaClazz = javaClazz;
            udtValueTypeCodec = TypeCodec.userType(userType);
        }

        /**
         * Creates a ready-to-use UDT codec for the given user type and Java class.
         *
         * <p>The returned codec encodes/decodes UDT values as follows:</p>
         * <ul>
         *   <li>{@link Collection} subclasses: encoded as the UDT fields in declaration
         *       order; decoded into a new collection of the same type</li>
         *   <li>{@link Map} subclasses: encoded by reading each UDT field by name from the map;
         *       decoded into a new map keyed by UDT field name</li>
         *   <li>JavaBean classes: encoded and decoded by reading/writing the bean property
         *       whose name matches each UDT field name</li>
         * </ul>
         * <p>The {@link IllegalArgumentException} for unsupported types is thrown lazily at
         * encode/decode time, not by this factory.</p>
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
         * @param javaClazz the Java class to map to (a {@link Collection}, {@link Map}, or bean class)
         * @return a new UDT codec instance
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
         * Creates a UDT codec by looking up the user type from the cluster's metadata.
         *
         * <p>Delegates to {@link #create(UserType, Class)} after resolving the UDT named
         * {@code userType} in keyspace {@code keySpace} from {@code cluster}'s metadata.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * UDTCodec<Address> codec = UDTCodec.create(
         *     cluster, "mykeyspace", "address", Address.class
         * );
         * }</pre>
         *
         * @param <T> the Java type
         * @param cluster the Cassandra cluster whose metadata is queried
         * @param keySpace the keyspace containing the UDT
         * @param userType the name of the user type within {@code keySpace}
         * @param javaClazz the Java class to map to (a {@link Collection}, {@link Map}, or bean class)
         * @return a new UDT codec instance
         * @throws NullPointerException if the keyspace or user type cannot be found in the metadata
         */
        public static <T> UDTCodec<T> create(final Cluster cluster, final String keySpace, final String userType, final Class<T> javaClazz) {
            return create(cluster.getMetadata().getKeyspace(keySpace).getUserType(userType), javaClazz);
        }

        /**
         * Serializes a Java value into the binary representation of its UDT, delegating
         * to {@link #serialize(Object)} and then to the underlying {@link UDTValue} codec.
         *
         * @param value the value to serialize, or {@code null}
         * @param protocolVersion the native protocol version negotiated with the cluster
         * @return the serialized {@link ByteBuffer}, or {@code null} for a {@code null} input
         * @throws InvalidTypeException if the value cannot be serialized to this UDT
         */
        @Override
        public ByteBuffer serialize(final T value, final ProtocolVersion protocolVersion) throws InvalidTypeException {
            return udtValueTypeCodec.serialize(serialize(value), protocolVersion);
        }

        /**
         * Deserializes the binary representation of a UDT into the target Java type, delegating
         * to the underlying {@link UDTValue} codec and then to {@link #deserialize(UDTValue)}.
         *
         * @param bytes the binary UDT payload, or {@code null}
         * @param protocolVersion the native protocol version negotiated with the cluster
         * @return the deserialized object, or {@code null} for a {@code null} input
         * @throws InvalidTypeException if the bytes cannot be decoded as this UDT
         */
        @Override
        public T deserialize(final ByteBuffer bytes, final ProtocolVersion protocolVersion) throws InvalidTypeException {
            return deserialize(udtValueTypeCodec.deserialize(bytes, protocolVersion));
        }

        /**
         * Parses a JSON string into a Java object of type {@code T}.
         *
         * <p>An empty string or the literal {@code "NULL"} ({@code TypeCodec#NULL_STR}) is
         * decoded as {@code null}.</p>
         *
         * @param value the string to parse
         * @return the parsed object, or {@code null} for empty/{@code "NULL"} input
         * @throws InvalidTypeException if the string is not valid JSON for {@code T}
         */
        @Override
        public T parse(final String value) throws InvalidTypeException {
            return Strings.isEmpty(value) || NULL_STR.equals(value) ? null : N.fromJson(value, javaClazz);
        }

        /**
         * Formats a Java object as a JSON string.
         *
         * <p>A {@code null} value is rendered as the literal {@code "NULL"}
         * ({@code TypeCodec#NULL_STR}).</p>
         *
         * @param value the value to format
         * @return the JSON string (or {@code "NULL"} if {@code value} is {@code null})
         */
        @Override
        public String format(final T value) throws InvalidTypeException {
            return value == null ? NULL_STR : N.toJson(value);
        }

        /**
         * Returns a new, empty {@link UDTValue} for this codec's user type.
         *
         * @return a freshly allocated UDT value
         */
        protected UDTValue newUDTValue() {
            return userType.newValue();
        }

        /**
         * Converts a Java value into a {@link UDTValue} following this codec's field layout.
         *
         * @param value the value to serialize; implementations must accept {@code null}
         * @return the populated {@link UDTValue}, or {@code null} for a {@code null} input
         */
        protected abstract UDTValue serialize(T value);

        /**
         * Converts a {@link UDTValue} into the Java type handled by this codec.
         *
         * @param udtValue the UDT value to read; implementations must accept {@code null}
         * @return the deserialized Java object, or {@code null} for a {@code null} input
         */
        protected abstract T deserialize(UDTValue udtValue);
    }

    /**
     * Type codec (Driver 3.x) that stores arbitrary Java objects as JSON strings in
     * Cassandra {@code varchar} columns.
     *
     * <p>On encode, the value is converted to JSON via {@code N.toJson(value)}; on
     * decode, the column text is parsed back into {@code T} via {@code N.fromJson(...)}.
     * A {@code null} reference round-trips as the literal {@code "NULL"}
     * ({@code TypeCodec#NULL_STR}) for the string forms. This is the codec implementation
     * used by {@link CassandraExecutor#registerTypeCodec(Class)}.</p>
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
         * Constructs a new JSON-over-{@code varchar} codec for the specified Java class.
         *
         * @param javaClazz the Java class this codec serializes/deserializes
         */
        protected StringCodec(final Class<T> javaClazz) {
            super(DataType.varchar(), javaClazz);
            this.javaClazz = javaClazz;
        }

        /**
         * Serializes a Java value into the binary representation of a Cassandra
         * {@code varchar}, by first encoding it to JSON via {@link #serialize(Object)}.
         *
         * @param value the value to serialize, or {@code null}
         * @param protocolVersion the native protocol version negotiated with the cluster
         * @return the serialized {@link ByteBuffer}
         * @throws InvalidTypeException if the value cannot be encoded as JSON
         */
        @Override
        public ByteBuffer serialize(final T value, final ProtocolVersion protocolVersion) throws InvalidTypeException {
            return stringTypeCodec.serialize(serialize(value), protocolVersion);
        }

        /**
         * Deserializes a Cassandra {@code varchar} payload into the target Java type, by
         * first decoding it to a string and then parsing the JSON via {@link #deserialize(String)}.
         *
         * @param bytes the binary {@code varchar} payload, or {@code null}
         * @param protocolVersion the native protocol version negotiated with the cluster
         * @return the deserialized object, or {@code null} for a {@code null} payload
         * @throws InvalidTypeException if the bytes cannot be decoded as JSON of {@code T}
         */
        @Override
        public T deserialize(final ByteBuffer bytes, final ProtocolVersion protocolVersion) throws InvalidTypeException {
            return deserialize(stringTypeCodec.deserialize(bytes, protocolVersion));
        }

        /**
         * Parses a JSON string into a Java object of type {@code T}.
         *
         * <p>An empty string or the literal {@code "NULL"} ({@code TypeCodec#NULL_STR}) is
         * decoded as {@code null}.</p>
         *
         * @param value the string to parse
         * @return the parsed object, or {@code null} for empty/{@code "NULL"} input
         * @throws InvalidTypeException if the string is not valid JSON for {@code T}
         */
        @Override
        public T parse(final String value) throws InvalidTypeException {
            return Strings.isEmpty(value) || NULL_STR.equals(value) ? null : N.fromJson(value, javaClazz);
        }

        /**
         * Formats a Java object as a JSON string.
         *
         * <p>A {@code null} value is rendered as the literal {@code "NULL"}
         * ({@code TypeCodec#NULL_STR}).</p>
         *
         * @param value the value to format
         * @return the JSON string (or {@code "NULL"} if {@code value} is {@code null})
         */
        @Override
        public String format(final T value) throws InvalidTypeException {
            return value == null ? NULL_STR : N.toJson(value);
        }

        /**
         * Encodes a Java object to its JSON string form. Equivalent to
         * {@code N.toJson(value)}; does not apply the {@code "NULL"} sentinel
         * used by {@link #format(Object)}.
         *
         * @param value the value to serialize
         * @return the JSON string representation
         */
        protected String serialize(final T value) {
            return N.toJson(value);
        }

        /**
         * Decodes a JSON string back into a Java object. Equivalent to
         * {@code N.fromJson(value, javaClazz)}; does not handle the {@code "NULL"}
         * sentinel used by {@link #parse(String)}.
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
