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
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.IntFunction;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
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
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.landawn.abacus.da.cs;
import com.landawn.abacus.exception.DuplicateResultException;
import com.landawn.abacus.exception.ParsingException;
import com.landawn.abacus.exception.UncheckedIOException;
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
import com.landawn.abacus.query.QueryUtil;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.Beans;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.Dataset;
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
 * Primary Cassandra database executor providing high-level CQL operations and result mapping.
 *
 * <p>The CassandraExecutor wraps the DataStax Cassandra Java Driver, offering simplified database
 * operations while preserving full access to Cassandra-specific features. It provides synchronous
 * (this class) and asynchronous (via {@link #async()}) execution modes, comprehensive parameter
 * binding support, and automatic result set mapping to Java objects.</p>
 *
 * <p><b>Naming convention:</b> this executor uses the abacus "house" CRUD vocabulary shared by the
 * {@code Condition}/{@code SqlBuilder}-based executors (Cassandra, BigQuery): {@code get}/{@code gett},
 * {@code findFirst}, {@code list}, {@code query} (returns a {@code Dataset}), {@code stream},
 * {@code exists}, {@code count}, and {@code insert}/{@code update}/{@code delete} (plus the
 * {@code batchInsert}/{@code batchUpdate}/{@code batchDelete} helpers). Driver-native method names are
 * intentionally not exposed; contrast the driver-passthrough executors (HBase, DynamoDB, Cosmos, Neo4j),
 * which keep their underlying driver's vocabulary.</p>
 *
 * <h2>Prepared Statement Caching</h2>
 * <p>Parameterized queries are parsed once via {@link ParsedCql} (which also extracts {@code :name}
 * named-parameter positions) and prepared on first use. The {@link PreparedStatement} is pooled
 * (keyed by the resolved CQL text) up to {@code POOLABLE_LENGTH} characters of query text, so
 * subsequent identical CQL strings reuse the cached server-side preparation; a fresh
 * {@link BoundStatement} is created from the cached preparation on every call. Bind values are
 * positionally bound after parameter-name resolution and type conversion against the
 * prepared statement's metadata (see <i>Number and Blob Conversion</i> below for the conversions that are rejected).
 * For a {@code timestamp} column (driver type {@link java.time.Instant}) a
 * {@link java.time.LocalDateTime} is bound as the instant of its wall-clock time in the JVM default time zone (as the
 * driver 3 executor and a raw {@link CqlBuilder} literal store it), a {@link java.time.LocalDate} as the start of that day in
 * the JVM default time zone (as the driver 3 executor stores it), and a {@link java.time.ZonedDateTime} or
 * {@link java.time.OffsetDateTime} as the instant it denotes; a {@code timestamp} read into a {@code LocalDateTime},
 * {@code ZonedDateTime} or {@code OffsetDateTime} target or bean property is that instant expressed in the JVM default
 * time zone (use {@code withZoneSameInstant(ZoneOffset.UTC)}, or {@code withOffsetSameInstant} for an {@code OffsetDateTime}, for
 * UTC), as the driver 3 executor reads it, and one read into a {@code LocalDate}, {@code LocalTime} or {@code OffsetTime}
 * target or property is that instant's date or time of day in the JVM default time zone (the driver 3 executor reads a
 * {@code LocalDate} or {@code LocalTime} the same way). A {@code date} column read into a {@code LocalDateTime} target or
 * property is the start of that day, and one read into an {@code Instant}, {@code ZonedDateTime} or {@code OffsetDateTime}
 * the start of that day in the JVM default time zone (the instant a {@code Date} target receives). An {@code inet} column read
 * into a {@code String} target or property is the address text (for example {@code "127.0.0.1"}, not the
 * {@code "/127.0.0.1"} of {@code InetAddress.toString()}), as is an {@link java.net.InetAddress} bound to a {@code text}
 * column.</p>
 *
 * <h2>Boolean Conversion</h2>
 * <p>Column reads into {@code Boolean}/{@code boolean} targets and bind values for {@code boolean} columns follow the strict
 * rule described under <i>Boolean Conversion</i> in {@link CassandraExecutorBase}: only an {@code int}, {@code smallint} or
 * {@code tinyint} value {@code 1} (true), {@code 0} or {@code -1} (both false), and the text {@code 1}, {@code 0}, {@code -1},
 * {@code y}, {@code Y}, {@code n}, {@code N}, {@code true} or {@code false} (any case) convert. Text is read without its
 * leading and trailing padding ({@code " Y "} converts), and empty or whitespace-only text converts to {@code false} for a
 * {@code boolean} and a {@code Boolean} target alike. Any other value read, including
 * every {@code bigint}, {@code double} or {@code decimal} value and a {@code count(*)} result, throws
 * {@link IllegalArgumentException}; a bind value the rule rejects (a {@code Long 1L}, the text {@code "yes"}) is bound
 * unchanged and fails with a {@link CodecNotFoundException}.</p>
 *
 * <h2>Number and Blob Conversion</h2>
 * <p>A bind value converted to its column's type must keep its number: a number with a fractional part (for example a
 * {@code Double}, {@code Float} or {@code BigDecimal} {@code 1.5}, or an abacus {@code Fraction} {@code 3/2}) bound to an integer
 * column ({@code tinyint}, {@code smallint}, {@code int}, {@code bigint}, {@code varint} or {@code counter}), and a finite number
 * beyond the range of a {@code float} or {@code double} column (for example {@code 1e300} for a {@code float}), are rejected with
 * an {@link IllegalArgumentException} naming the parameter, the value and the column's CQL type, instead of being bound truncated
 * or as {@code Infinity}. An exact conversion is bound ({@code 2.0} to an {@code int} column binds {@code 2}), as is a number
 * rounded to the nearest {@code float} or {@code double} ({@code 0.1} to a {@code float} column binds {@code 0.1f}), and a value
 * out of the integer column's range, or a {@code NaN} or infinite value for it, fails with {@link ArithmeticException}. This
 * check applies to binding only: a column value read into a narrower target (a {@code double} column into an {@code int}
 * property) is converted as {@code N.convert} converts it.
 * A {@code String} bound to a {@code blob} column (or to a custom-type column, which the driver also binds from a
 * {@link ByteBuffer}, or written to a {@code blob} UDT field) is base64 text: it is decoded into the
 * blob's bytes, and text that is not valid base64 is rejected with an {@link IllegalArgumentException}. A {@code blob} read into a
 * {@code String} target or bean property (including a nested dotted column and a UDT field read into a bean) is the base64 text
 * of the buffer's remaining bytes; the driver's buffer is not consumed. {@code byte[]} and {@link ByteBuffer} targets receive the
 * blob's bytes, and a {@code ByteBuffer} bound to a {@code text} column is the base64 text of its remaining bytes.</p>
 *
 * <h2>Row Conversion with {@code N.convert}</h2>
 * <p>Loading this class registers a converter with abacus-common for driver rows, so {@code N.convert(row, targetClass)} maps a
 * {@link Row} as {@link #toList(ResultSet, Class)} maps each row: to a bean, a {@code Map}, a collection, an object array, or (for
 * a one-column row) a single value. abacus-common looks converters up by class and superclass but not by interface, so the
 * converter is registered for {@code Row.class} and for the driver's row implementation class
 * ({@code com.datastax.oss.driver.internal.core.cql.DefaultRow}). A target the row already is an instance of (such as
 * {@code Row} or {@code Object}) returns the row itself.</p>
 *
 * <h2>Batch Semantics</h2>
 * <p>Batch helpers accept a {@link BatchType}: {@link BatchType#LOGGED LOGGED} (the default when
 * {@code type} is {@code null}) uses Cassandra's batch log for
 * atomicity across partitions at the cost of extra coordination; {@link BatchType#UNLOGGED UNLOGGED}
 * skips the batch log (recommended only for single-partition batches); {@link BatchType#COUNTER
 * COUNTER} is required for counter mutations. Batches do <i>not</i> provide ACID transactions.</p>
 *
 * <h2>Consistency Levels &amp; Statement Settings</h2>
 * <p>Default consistency, serial consistency, page size, per-statement timeout, and query tracing from
 * {@link StatementSettings} are applied to every prepared/bound statement this executor builds
 * (via {@code prepareStatement}/{@code bind}/{@code prepareBatch*}). Caller-supplied {@link Statement}
 * instances passed to {@code execute}/{@code stream} are used as-is (executor settings are not applied).
 * Driver 4.19.3 ignores a batch statement's own timeout (its {@code getTimeout()} returns {@code null}), so
 * a {@code prepareBatch*} statement runs with its execution profile's request timeout; its other settings apply.</p>
 *
 * <h2>Key Features</h2>
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
 *     <li>PreparedStatement caching (keyed by resolved CQL text up to {@code POOLABLE_LENGTH}); a fresh BoundStatement per call</li>
 *     <li>Connection reuse and session management</li>
 *     <li>Asynchronous execution exposed via {@link #async()} returning ContinuableFuture-typed results</li>
 *     <li>Efficient type conversion and codec registry support</li>
 *     </ul>
 * </li>
 * </ul>
 *
 * <h3>Basic Usage Examples</h3>
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
 * ContinuableFuture<List<User>> futureUsers = executor.async().list(User.class,
 *     "SELECT * FROM users WHERE created_at > ?", yesterday);
 * }</pre>
 *
 * <h3>Advanced Features</h3>
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
 *              .forEach(System.out::println);
 * }
 * }</pre>
 *
 * <h3>CQL Builder Integration</h3>
 * <p>This executor integrates with the CqlBuilder for dynamic query construction:</p>
 * <pre>{@code
 * // Using CqlBuilder for dynamic queries
 * String cql = PSC.select("id", "name", "email")
 *                 .from("users")
 *                 .where(Filters.and(Filters.eq("status", "active"), Filters.gt("created_at", lastWeek)))
 *                 .orderBy("created_at")
 *                 .limit(100)
 *                 .build().query();
 *
 * List<User> recentUsers = executor.list(User.class, cql, "active", lastWeek);
 * }</pre>
 *
 * <h3>Thread Safety</h3>
 * <p>This class is thread-safe and designed for concurrent use. The underlying Cassandra session
 * and prepared statement pools are managed safely across multiple threads. However, applications
 * should properly manage the session lifecycle and call {@link #close()} when the executor
 * is no longer needed.</p>
 *
 * <h3>Resource Management</h3>
 * <p>The executor should be closed properly via {@link #close()} to release
 * underlying resources:</p>
 * <pre>{@code
 * CassandraExecutor executor = new CassandraExecutor(session);
 * try {
 *     // Perform database operations
 * } finally {
 *     executor.close();   // closes the session and releases cached statements
 * }
 * }</pre>
 *
 * @see CqlBuilder
 * @see CqlMapper
 * @see CassandraExecutorBase
 * @see ParsedCql
 * @see com.datastax.oss.driver.api.core.CqlSession
 * @see com.landawn.abacus.query.Filters
 */
public final class CassandraExecutor extends CassandraExecutorBase<Row, ResultSet, Statement<?>, PreparedStatement, BatchType> {

    static {
        final BiFunction<Row, Class<?>, Object> converter = (row, rowClass) -> readRow(rowClass, row);

        N.registerConverter(Row.class, converter);
        // WHY: abacus-common looks a converter up by the value's exact class and then its superclasses, never its interfaces,
        // so the Row.class registration alone never fires: every row the driver returns is a DefaultRow, and N.convert(row,
        // Bean.class) or N.convert(row, Map.class) threw ParsingException while N.convert(row, Object[].class) gave [row].
        // Registering the driver's concrete row class too makes N.convert map a row as toList/findFirst do.
        registerRowConverter("com.datastax.oss.driver.internal.core.cql.DefaultRow", converter);
    }

    /**
     * Registers {@code converter} for the driver's row class {@code className}. The class is internal to the driver, so it is
     * resolved by name; a driver version without it (or without it as a {@link Row}) is skipped, keeping only the
     * {@code Row.class} registration. A class that already has a converter keeps it (abacus registration is first-wins).
     */
    private static void registerRowConverter(final String className, final BiFunction<Row, Class<?>, Object> converter) {
        final Class<?> rowClass;

        try {
            rowClass = Class.forName(className, false, Row.class.getClassLoader());
        } catch (final ClassNotFoundException | LinkageError e) {
            return;
        }

        if (Row.class.isAssignableFrom(rowClass)) {
            N.registerConverter(rowClass.asSubclass(Row.class), converter);
        }
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

    private static final Logger logger = LoggerFactory.getLogger(CassandraExecutor.class);

    private final KeyedObjectPool<String, PoolableAdapter<PreparedStatement>> preparedStatementPool;

    private final CqlSession session;

    private final CodecRegistry codecRegistry;

    private final StatementSettings settings;

    private final AsyncCassandraExecutor asyncCassandraExecutor;

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
     * CassandraExecutor executor = new CassandraExecutor(session); // ready to use; null session throws IllegalArgumentException
     * }</pre>
     *
     * @param session the Cassandra session to use for database operations
     * @throws IllegalArgumentException if {@code session} is {@code null}
     * @see CqlSession
     */
    public CassandraExecutor(final CqlSession session) throws IllegalArgumentException {
        this(session, null);
    }

    /**
     * Creates a new CassandraExecutor with the specified session and statement settings.
     *
     * <p>The statement settings configure defaults (consistency, timeout, page size, tracing)
     * applied to every prepared/bound statement this executor builds. Caller-supplied
     * {@link Statement} instances passed to {@code execute}/{@code stream} are not modified.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * StatementSettings settings = StatementSettings.builder()
     *     .consistency(ConsistencyLevel.QUORUM)
     *     .timeout(Duration.ofSeconds(30))
     *     .fetchSize(1000)
     *     .build();
     * CassandraExecutor executor = new CassandraExecutor(session, settings); // settings may be null (driver defaults are used)
     * }</pre>
     *
     * @param session the Cassandra session to use for database operations
     * @param settings default statement settings for statements this executor prepares/binds/batches, or null for defaults
     * @throws IllegalArgumentException if {@code session} is {@code null}
     * @see StatementSettings
     */
    public CassandraExecutor(final CqlSession session, final StatementSettings settings) throws IllegalArgumentException {
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
     * CqlMapper cqlMapper = CqlMapper.loadFrom("queries/user-queries.cql");
     * CassandraExecutor executor = new CassandraExecutor(session, settings, cqlMapper); // settings and cqlMapper may both be null
     * // Pass the mapper KEY (id) as the query; the executor resolves it via the CqlMapper:
     * ResultSet rs = executor.execute("findActiveUsers"); // "findActiveUsers" is looked up in cqlMapper
     * }</pre>
     *
     * @param session the Cassandra session to use for database operations
     * @param settings default statement settings, or null for defaults
     * @param cqlMapper CQL mapper containing pre-configured statements, or null if not needed
     * @throws IllegalArgumentException if {@code session} is {@code null}
     * @see CqlMapper
     */
    public CassandraExecutor(final CqlSession session, final StatementSettings settings, final CqlMapper cqlMapper) throws IllegalArgumentException {
        this(session, settings, cqlMapper, null);
    }

    /**
     * Creates a new CassandraExecutor with full configuration options.
     *
     * <p>This is the most comprehensive constructor, allowing full customization of executor
     * behavior. The naming policy controls how Java property names are mapped to Cassandra
     * column names, which is essential for entity-based operations.</p>
     *
     * <p>When {@code settings} is non-null it is defensively copied (so later mutations to the caller's
     * instance do not affect this executor). The {@link AsyncCassandraExecutor} returned by
     * {@link #async()} is created eagerly here and shares this executor's session, statement caches,
     * and codec registry.</p>
     *
     * <h4>Naming Policy Examples:</h4>
     * <ul>
     * <li>{@link NamingPolicy#SNAKE_CASE}: {@code firstName} &rarr; {@code first_name}</li>
     * <li>{@link NamingPolicy#SCREAMING_SNAKE_CASE}: {@code firstName} &rarr; {@code FIRST_NAME}</li>
     * <li>{@link NamingPolicy#CAMEL_CASE}: {@code firstName} &rarr; {@code firstName}</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * StatementSettings settings = StatementSettings.builder().consistency(ConsistencyLevel.QUORUM).build();
     * CqlMapper cqlMapper = CqlMapper.loadFrom("queries/user-queries.cql");
     *
     * CassandraExecutor executor = new CassandraExecutor(
     *     session, settings, cqlMapper, NamingPolicy.SNAKE_CASE); // entity prop firstName maps to column first_name
     *
     * // All four arguments except session are optional:
     * CassandraExecutor plain = new CassandraExecutor(
     *     session, null, null, null);                             // no settings/mapper; null namingPolicy defaults to SNAKE_CASE
     * }</pre>
     *
     * @param session the Cassandra session to use for database operations
     * @param settings default statement settings to apply to every prepared/bound statement built by
     *                 this executor, or {@code null} to apply no defaults (copied defensively)
     * @param cqlMapper CQL mapper containing pre-configured statements, or {@code null} if not needed
     * @param namingPolicy policy for mapping Java property names to column names; when {@code null},
     *                     {@link NamingPolicy#SNAKE_CASE} is used
     * @throws IllegalArgumentException if {@code namingPolicy} is not {@code SNAKE_CASE},
     *         {@code SCREAMING_SNAKE_CASE} or {@code CAMEL_CASE} (checked before {@code session}), or
     *         {@code session} is {@code null}
     * @see NamingPolicy
     */
    public CassandraExecutor(final CqlSession session, final StatementSettings settings, final CqlMapper cqlMapper, final NamingPolicy namingPolicy)
            throws IllegalArgumentException {
        super(cqlMapper, namingPolicy);
        this.session = N.checkArgNotNull(session, cs.session);
        codecRegistry = this.session.getContext().getCodecRegistry();

        if (settings == null) {
            this.settings = null;
        } else {
            this.settings = new StatementSettings().consistency(settings.consistency())
                    .serialConsistency(settings.serialConsistency())
                    .fetchSize(settings.fetchSize())
                    .timeout(settings.timeout())
                    .traceQuery(settings.traceQuery());
        }

        asyncCassandraExecutor = new AsyncCassandraExecutor(this);

        // The pool registers eviction and shutdown tasks. Allocate it last so failed session initialization
        // cannot leave an unreachable pool whose tasks keep it alive.
        preparedStatementPool = PoolFactory.createKeyedObjectPool(1024, 3000);
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
     * CqlSession session = executor.session(); // returns the underlying CqlSession (never null)
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
     * Returns an asynchronous facade for this executor.
     *
     * <p>The returned {@link AsyncCassandraExecutor} shares the same {@link CqlSession}, prepared
     * statement cache, codec registry, and {@link StatementSettings} as this synchronous executor;
     * it merely exposes non-blocking variants of the same operations that return
     * {@code ContinuableFuture}-typed results. The async facade is created eagerly at construction and
     * is safe to call repeatedly.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AsyncCassandraExecutor async = executor.async(); // returns the same facade on every call (executor.async() == executor.async())
     *
     * ContinuableFuture<List<User>> future = async.list(User.class,
     *     "SELECT * FROM users WHERE status = ?", "active"); // non-blocking; resolve with future.get()
     * }</pre>
     *
     * @return the {@link AsyncCassandraExecutor} bound to this executor's session
     */
    public AsyncCassandraExecutor async() {
        return asyncCassandraExecutor;
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
     * executor.registerTypeCodec(Address.class); // Address now (de)serializes as a JSON TEXT column
     *
     * // Now Address objects can be used directly in CQL operations
     * User user = new User();
     * user.setAddress(new Address("123 Main St", "City", "12345"));
     * executor.insert(user);   // Address will be automatically serialized
     *
     * executor.registerTypeCodec(null); // throws IllegalArgumentException (null class)
     * }</pre>
     *
     * @param javaClazz the Java class for which to register a type codec
     * @throws IllegalStateException if the session exposes a read-only codec registry (checked before
     *         {@code javaClazz})
     * @throws IllegalArgumentException if {@code javaClazz} is {@code null}
     * @see #registerTypeCodec(MutableCodecRegistry, Class)
     */
    public void registerTypeCodec(final Class<?> javaClazz) throws IllegalStateException, IllegalArgumentException {
        if (!(codecRegistry instanceof final MutableCodecRegistry mutableCodecRegistry)) {
            throw new IllegalStateException("The session codec registry is not mutable; register the codec while building the driver context");
        }

        N.checkArgNotNull(javaClazz, cs.javaClazz);

        registerTypeCodec(mutableCodecRegistry, javaClazz);
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
     * CassandraExecutor.registerTypeCodec(registry, CustomAddress.class); // CustomAddress now (de)serializes as a JSON TEXT column
     *
     * CassandraExecutor.registerTypeCodec(registry, null);                                   // throws IllegalArgumentException (null class)
     * CassandraExecutor.registerTypeCodec((MutableCodecRegistry) null, CustomAddress.class); // throws IllegalArgumentException (null registry)
     * }</pre>
     *
     * @param codecRegistry the mutable codec registry to register the codec in
     * @param javaClazz the Java class for which to register a type codec
     * @throws IllegalArgumentException if {@code codecRegistry} or {@code javaClazz} is {@code null}
     */
    public static void registerTypeCodec(final MutableCodecRegistry codecRegistry, final Class<?> javaClazz) throws IllegalArgumentException {
        N.checkArgNotNull(codecRegistry, cs.codecRegistry);
        N.checkArgNotNull(javaClazz, cs.javaClazz);

        codecRegistry.register(new StringCodec<>(javaClazz));
    }

    /**
     * Extracts all data from a Cassandra ResultSet into a Dataset.
     *
     * <p>This method converts a Cassandra ResultSet into an in-memory Dataset structure
     * that provides column-oriented access to the result data. This is useful for
     * analytical operations or when you need to work with result data in a tabular format.</p>
     *
     * <p>Column values retain their driver types, except nested {@link Row} values, which are
     * recursively flattened to {@code Object[]}.</p>
     *
     * <p>Remaining rows are decoded as pages are consumed, without first collecting the driver rows.
     * If decoding a row fails, that row has been consumed, but later rows are not drained.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ResultSet resultSet = session.execute("SELECT id, name, age FROM users");
     * Dataset dataset = CassandraExecutor.extractData(resultSet); // returns a Dataset with raw driver values, no type conversion
     * dataset.println();                                          // Print dataset in tabular format
     *
     * CassandraExecutor.extractData((ResultSet) null); // throws IllegalArgumentException (null result set)
     * }</pre>
     *
     * @param resultSet the Cassandra ResultSet to extract data from
     * @return a Dataset containing all rows and columns from the result set
     * @throws IllegalArgumentException if {@code resultSet} is {@code null}
     * @throws RuntimeException if fetching a remaining result page or decoding a column value fails
     * @see Dataset
     */
    public static Dataset extractData(final ResultSet resultSet) throws IllegalArgumentException, RuntimeException {
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
     * <p>When {@code targetClass} is {@code null}, {@code Map.class} (or any {@code Map} subtype), or
     * a non-bean type, scalar values retain their driver types. Nested {@link Row} values become maps
     * for a map target, or {@code Object[]} when no bean-property type is available.</p>
     *
     * <p>Remaining rows are mapped as pages are consumed, without first collecting the driver rows.
     * If mapping a row fails, that row has been consumed, but later rows are not drained.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ResultSet resultSet = session.execute("SELECT id, name, age FROM users");
     * Dataset dataset = CassandraExecutor.extractData(resultSet, User.class); // column values converted to User property types
     *
     * // With Map.class (or null) no per-column conversion is performed:
     * ResultSet rs2 = session.execute("SELECT id, name, age FROM users");
     * Dataset raw = CassandraExecutor.extractData(rs2, Map.class);            // raw driver values, no conversion
     *
     * // Edge case: a null result set throws IllegalArgumentException
     * CassandraExecutor.extractData((ResultSet) null, User.class); // throws IllegalArgumentException
     * }</pre>
     *
     * @param resultSet the Cassandra ResultSet to extract data from
     * @param targetClass the entity class used to determine target column types; a map class, other non-bean
     *                    class, or {@code null} keeps scalar driver values unchanged
     * @return a Dataset with type-converted data based on the target class
     * @throws IllegalArgumentException if {@code resultSet} is {@code null}
     * @throws RuntimeException if the driver fails to fetch a remaining result page, decoding a column value fails, or a column value
     *         cannot be converted to the type of the matching {@code targetClass} property
     */
    public static Dataset extractData(final ResultSet resultSet, final Class<?> targetClass) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(resultSet, cs.resultSet);

        final boolean isMap = targetClass != null && Map.class.isAssignableFrom(targetClass);
        // WHY: Beans.isBeanClass also accepts an @Entity-annotated Map subclass. Such a target is a map target, as for readRow and
        // toList: typing its columns by its Map getters (isEmpty() -> "empty") turned a nested row into an Object[] (or a single value
        // of the getter's type, failing strict Boolean conversion) instead of a map. Scalar values stayed raw either way (isMap).
        final boolean isEntity = !isMap && Beans.isBeanClass(targetClass);
        final ColumnDefinitions columnDefinitions = resultSet.getColumnDefinitions();
        final int columnCount = columnDefinitions.size();
        // Only a sizing hint: a faulty ResultSet reporting a negative count must not fail the extraction.
        final int initialRowCapacity = Math.max(0, resultSet.getAvailableWithoutFetching());

        final List<String> columnNameList = new ArrayList<>(columnCount);
        final List<List<Object>> columnList = new ArrayList<>(columnCount);
        final Class<?>[] columnClasses = new Class<?>[columnCount];
        final Map<String, String> columnToPropNameMap = isEntity ? QueryUtil.columnToPropNameMap(targetClass) : null;

        for (int i = 0; i < columnCount; i++) {
            columnNameList.add(columnDefinitions.get(i).getName().asInternal());
            columnList.add(new ArrayList<>(initialRowCapacity));
            if (isEntity) {
                final String columnName = columnNameList.get(i);
                Method method = Beans.getPropGetter(targetClass, columnName);

                if (method == null) {
                    final String propName = columnToPropNameMap.get(columnName);
                    method = propName == null ? null : Beans.getPropGetter(targetClass, propName);
                }

                columnClasses[i] = method != null ? method.getReturnType() : null;
            } else {
                // null = no per-column conversion (raw driver values), per the documented contract for
                // non-bean targets; nested Row values still flatten through readRow below. Object[].class
                // here would force every scalar through N.convert(value, Object[].class) and wrap it.
                columnClasses[i] = isMap ? Map.class : null;
            }
        }

        Object propValue = null;

        // Convert as pages are consumed so encoded driver rows are not retained alongside the entire Dataset.
        for (final Row row : resultSet) {
            for (int i = 0; i < columnCount; i++) {
                propValue = row.getObject(i);

                if (propValue instanceof Row && (columnClasses[i] == null || !columnClasses[i].isAssignableFrom(Row.class))) {
                    columnList.get(i).add(readRow(columnClasses[i], (Row) propValue));
                } else if (propValue == null || targetClass == null || isMap || columnClasses[i] == null
                        || columnClasses[i].isAssignableFrom(propValue.getClass())) {
                    columnList.get(i).add(propValue);
                } else {
                    columnList.get(i).add(decodeValue(propValue, columnClasses[i]));
                }
            }
        }

        // The public constructor copies every column (abacus-common 8.1.0+), holding the whole result twice. These lists are
        // built here, one per column, and never exposed or changed afterwards, so the Dataset can adopt them.
        return RowDataset.fromOwnedColumns(columnNameList, columnList);
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
     * <li><strong>Row.class, Object.class, or another supertype of Row:</strong> Returns raw Cassandra Row objects</li>
     * <li><strong>Object array classes:</strong> Each row becomes an Object[] or a typed object array (e.g. {@code String[]}),
     *     one element per column</li>
     * <li><strong>Collection classes:</strong> Each row becomes a List, Set, etc.</li>
     * <li><strong>Basic types:</strong> For single-column results (String, Integer, etc.); a primitive array class such as
     *     {@code long[]} is one of these: the single column (for example a {@code list<bigint>}) is converted to the array</li>
     * </ul>
     *
     * <p>For single-value targets, a {@code NULL} column maps to the default value of {@code targetClass}: the primitive
     * default for a primitive class token (for example, {@code 0} for {@code int.class}), an empty value for
     * {@code Optional}, {@code Nullable}, {@code OptionalInt} and similar types, and {@code null} for wrappers and other
     * reference types; use {@code Integer.class} instead of {@code int.class} to distinguish NULL from a stored zero.
     * An empty result set produces an empty list for either target type.</p>
     *
     * <p>For raw-row targets, the list returned by {@link ResultSet#all()} is returned unchanged.
     * Other targets are mapped as pages are consumed, without first collecting the driver rows.
     * If mapping a row fails, that row has been consumed, but later rows are not drained.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ResultSet resultSet = session.execute("SELECT id, name, email FROM users");
     *
     * // Convert to entity list
     * List<User> users = CassandraExecutor.toList(resultSet, User.class); // one User per row
     *
     * // Convert to map list (a ResultSet is single-pass, so re-execute rather than reuse 'resultSet')
     * ResultSet resultSet2 = session.execute("SELECT id, name, email FROM users");
     * List<Map> userMaps = CassandraExecutor.toList(resultSet2, Map.class); // one Map<String,Object> per row
     *
     * // Convert single column to value list
     * ResultSet names = session.execute("SELECT name FROM users");
     * List<String> nameList = CassandraExecutor.toList(names, String.class); // one String per row
     *
     * // Single-value type but multiple columns selected -> error
     * ResultSet multi = session.execute("SELECT id, name FROM users");
     * CassandraExecutor.toList(multi, String.class); // throws IllegalArgumentException (more than one column)
     *
     * // Edge case: a null result set throws IllegalArgumentException
     * CassandraExecutor.toList((ResultSet) null, User.class); // throws IllegalArgumentException
     * }</pre>
     *
     * @param <T> the type of objects in the returned list
     * @param resultSet the Cassandra ResultSet to convert
     * @param targetClass the target type each row is converted to (entity class, {@code Map.class},
     *        {@code Row.class}, an object array class, a collection class, or a basic single-value type, including a primitive
     *        array class); must not be {@code null}
     * @return a List containing all rows converted to the specified type
     * @throws IllegalArgumentException if {@code resultSet} or {@code targetClass} is {@code null}, or if {@code targetClass} is a single-value
     *         type but the result set does not have exactly one column
     * @throws RuntimeException if the driver fails to fetch a remaining result page, a column value cannot be converted
     *         to the requested Java type, or a bean property cannot be read or written
     */
    public static <T> List<T> toList(final ResultSet resultSet, final Class<T> targetClass) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(resultSet, cs.resultSet);
        N.checkArgNotNull(targetClass, cs.targetClass);

        if (targetClass.isAssignableFrom(Row.class)) {
            return (List<T>) resultSet.all();
        }

        final ColumnDefinitions columnDefinitions = resultSet.getColumnDefinitions();
        final Function<? super Row, ? extends T> mapper = createRowMapper(targetClass, columnDefinitions);
        // Only a sizing hint: a faulty ResultSet reporting a negative count must not fail the conversion.
        final List<T> resultList = new ArrayList<>(Math.max(0, resultSet.getAvailableWithoutFetching()));

        // Avoid an intermediate all-rows list, which retains every page while the mapped result grows.
        for (final Row row : resultSet) {
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
     * <li><strong>Name variants:</strong> a column name also matches a property case-insensitively or in its snake_case
     *     form (e.g., {@code first_name} to {@code firstName}), as the bean metadata resolves it, or by the property's
     *     declared {@code @Column} name (see {@link QueryUtil#columnToPropNameMap(Class)}); no executor naming policy is
     *     involved, since this method is static</li>
     * <li><strong>Nested properties:</strong> Dot-notation property paths (e.g., {@code "address.city"}) when no direct
     *     property is found; the value is converted for the path's last property as a top-level column value is, and a
     *     path that does not resolve, or whose last property is read-only, is ignored</li>
     * <li><strong>Type conversion:</strong> Automatic conversion between compatible types (see the class documentation for
     *     the {@code timestamp}, {@code date}, {@code inet} and {@code blob} rules; a {@code blob} read into a {@code String}
     *     property is the base64 text of its bytes), for top-level and nested properties alike. A collection or map column read
     *     into a property declared as a parameterized collection or map (for example {@code List<byte[]>} or
     *     {@code Map<String, String>}) or as an object array has each element, key and value converted by the same rules for the
     *     declared element type, nested containers included (a {@code list<timestamp>} into a {@code List<LocalDateTime>}, a
     *     {@code set<inet>} into a {@code Set<String>}); the driver's collection is set unchanged when every element already has
     *     its declared type, and otherwise a new one of the declared kind holding the converted elements in the driver's order.
     *     A UDT or tuple element is kept as the driver decodes it; an element that cannot be converted fails the row as a
     *     top-level value would</li>
     * <li><strong>Null values:</strong> a {@code NULL} column sets the property to the default value of its type:
     *     {@code null} for a wrapper or other reference type, the primitive default for a primitive property (for example,
     *     {@code 0} for an {@code int}), and an empty value for an {@code Optional}-like property</li>
     * <li><strong>Read-only properties:</strong> a column matching a property that cannot be written (for example a
     *     getter-only property) is skipped, whatever its {@code @JsonXmlField} direction</li>
     * </ul>
     *
     * <p>A UDT column is set as the driver decodes it: a {@link UdtValue}, or the Java value of a registered
     * {@link UDTCodec}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ResultSet resultSet = session.execute("SELECT id, name, email FROM users WHERE id = ?", userId);
     * Row row = resultSet.one();
     *
     * if (row != null) {
     *     User user = CassandraExecutor.toEntity(row, User.class); // returns a populated User (never null for a non-null row)
     *     System.out.println("User: " + user.getName());
     * }
     *
     * // For nested properties (if supported by entity structure)
     * // Column 'address.street' maps to user.getAddress().setStreet(value)
     *
     * // Edge case: a null row throws IllegalArgumentException
     * CassandraExecutor.toEntity((Row) null, User.class); // throws IllegalArgumentException
     *
     * // Edge case: a null entity class is rejected as an illegal argument
     * CassandraExecutor.toEntity(row, (Class<User>) null); // throws IllegalArgumentException ('entityClass' cannot be null)
     * }</pre>
     *
     * @param <T> the type of the entity to create
     * @param row the Cassandra Row containing the data
     * @param entityClass the target entity class with getter/setter methods (must not be {@code null})
     * @return a new instance of the entity class populated with row data
     * @throws IllegalArgumentException if {@code row} or {@code entityClass} is {@code null}, or {@code entityClass} is not a bean class (it has no property
     *         getter/setter methods or public fields)
     * @throws RuntimeException if a column value cannot be converted to the requested Java type or a bean property cannot be read or written
     */
    public static <T> T toEntity(final Row row, final Class<T> entityClass) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(row, cs.row);
        N.checkArgNotNull(entityClass, cs.entityClass);

        final ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
        final int columnCount = columnDefinitions.size();

        final Map<String, String> column2FieldNameMap = QueryUtil.columnToPropNameMap(entityClass);
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
                    entityInfo.setPropValue(entity, propName, toNestedPropValue(entityInfo, propName, propValue), true);
                }

                continue;
            }

            if (isReadOnlyProperty(propInfo)) {
                continue;
            }

            parameterType = propInfo.clazz;

            if ((propValue == null || parameterType.isAssignableFrom(propValue.getClass())) || !(propValue instanceof Row)) {
                propInfo.setPropValue(entity, toPropValue(propInfo, propValue));
            } else {
                propInfo.setPropValue(entity, readRow(parameterType, (Row) propValue));
            }
        }

        return entityInfo.finishBeanResult(entity);
    }

    /**
     * Converts the value of a dotted (nested-path) column, which {@link BeanInfo#setPropValue(Object, String, Object, boolean)}
     * stores, as {@link #toPropValue(PropInfo, Object)} converts a top-level column value, for the (element) type of the path's last
     * property. A path that does not resolve, or whose last property is read-only, keeps the value unchanged; the setter then
     * ignores it, as before.
     * @throws RuntimeException if {@code value} cannot be converted to the last property's class
     */
    private static Object toNestedPropValue(final BeanInfo entityInfo, final String path, final Object value) throws RuntimeException {
        // WHY: BeanInfo.setPropValue(path) stores the value through the last property's PropInfo.setPropValue, whose N.convert
        // conversion is the one toPropValue corrects for a top-level property, so a nested property got none of those
        // conversions: an inet column became "/127.0.0.1", a timestamp into a LocalDate or a date into a LocalDateTime threw
        // DateTimeParseException, and a BLOB into a byte[] became null. The read-only check is the setter's own (it skips such a
        // property without converting), so a value it ignores is not converted, and cannot fail, here either.
        final List<PropInfo> chain = entityInfo.getPropInfoChain(path);

        if (chain.isEmpty()) {
            return value;
        }

        final PropInfo leaf = chain.get(chain.size() - 1);

        return isReadOnlyProperty(leaf) ? value : toPropValue(leaf, value);
    }

    // A getter-only (computed) property, e.g. one inherited from an @Entity superclass (abacus-common 8.1.0), is in
    // propInfoList, so generated INSERT/SELECT statements (and UDT serialization) include its column; but
    // PropInfo.setPropValue throws UnsupportedOperationException for it, which failed the whole row on read. Its value is
    // skipped on read, as the JSON parser does. Since abacus-common 8.2.1 a read-only property may also have a backing field
    // (an @AccessFieldByMethod field without a setter), so the public isReadOnlyProperty flag is tested rather than
    // field == null. WHY no direction test: an explicit DESERIALIZE_ONLY (declarable only on an @AccessFieldByMethod field
    // without a setter) cannot make the property writable; setPropValue still throws, which failed the whole row. A read-only
    // property is skipped whatever its direction, as by the HBase, DynamoDB and MongoDB executors.
    private static boolean isReadOnlyProperty(final PropInfo propInfo) {
        return propInfo.isReadOnlyProperty;
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
     *     Map<String, Object> userMap = CassandraExecutor.toMap(row); // returns a HashMap of column name -> value (column order NOT preserved)
     *     System.out.println("User ID: " + userMap.get("id"));
     *     System.out.println("Name: " + userMap.get("name"));
     *
     *     // Only nested Row values are recursively converted to nested Maps
     *     // (a UDT column surfaces as a UdtValue and is kept as-is).
     * }
     *
     * CassandraExecutor.toMap((Row) null); // throws IllegalArgumentException (null row)
     * }</pre>
     *
     * @param row the Cassandra Row to convert
     * @return a Map containing all column names and values from the row
     * @throws IllegalArgumentException if {@code row} is {@code null}
     * @throws RuntimeException if reading the row metadata or decoding a column value fails
     */
    public static Map<String, Object> toMap(final Row row) throws IllegalArgumentException, RuntimeException {
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
     *     Map<String, Object> orderedMap = CassandraExecutor.toMap(row, size -> new LinkedHashMap<>()); // column order preserved
     *
     *     // Use TreeMap for sorted keys
     *     Map<String, Object> sortedMap = CassandraExecutor.toMap(row, size -> new TreeMap<>());        // keys sorted
     *
     *     // Use specific initial capacity
     *     Map<String, Object> sizedMap = CassandraExecutor.toMap(row, HashMap::new);                    // HashMap sized to column count
     * }
     *
     * CassandraExecutor.toMap((Row) null, size -> new HashMap<>()); // throws IllegalArgumentException (null row)
     * }</pre>
     *
     * @param row the Cassandra Row to convert
     * @param supplier a function that creates a new Map instance with the specified initial capacity
     * @return a Map of the specified type containing all column names and values from the row
     * @throws IllegalArgumentException if {@code row} or {@code supplier} is {@code null}
     * @throws RuntimeException if reading the row metadata, creating a map with {@code supplier}, decoding a column value,
     *         or adding a column entry to the supplied map fails
     * @throws NullPointerException if {@code supplier} returns {@code null} for a nonempty row
     */
    public static Map<String, Object> toMap(final Row row, final IntFunction<? extends Map<String, Object>> supplier)
            throws IllegalArgumentException, RuntimeException, NullPointerException {
        N.checkArgNotNull(row, cs.row);
        N.checkArgNotNull(supplier, cs.supplier);

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

    /**
     * Converts a driver-decoded (or caller-supplied bind) value to {@code targetClass}. The BLOB mapping between
     * {@link ByteBuffer} and {@code byte[]} is handled here because {@code N.convert} does not support it: it
     * converts a {@code ByteBuffer} to {@code null} for {@code byte[]}, and a {@code byte[]} to a
     * {@code ByteBuffer} whose position already equals its limit (so the driver would encode an empty blob).
     * A {@code ByteBuffer} converted to {@code String} is the base64 text of its remaining bytes, its position unchanged
     * ({@code N.convert} encodes only the bytes before the position, so a blob read into a {@code String} became {@code ""}).
     * A {@link LocalTime} is converted to {@code java.sql.Time} here as well (keeping milliseconds), because
     * {@code N.convert} rejects one with a fractional second or on a whole minute. The {@code timestamp} mappings that
     * {@code N.convert} rejects are handled here too: a {@link java.time.LocalDateTime} bound to a {@code timestamp} column
     * (driver type {@link Instant}) is the instant of its wall-clock time in the JVM default time zone (the instant the driver 3
     * executor stores for the same value, and the one {@link CqlBuilder} inlines for a raw {@code LocalDateTime} literal), a
     * {@link java.time.ZonedDateTime} (any {@link java.time.chrono.ChronoZonedDateTime}) is its instant, a {@link LocalDate}
     * (bound to a {@code timestamp} column, or a {@code date} column read into an {@code Instant} target) is the start of that day in
     * the JVM default time zone (as the driver 3 executor stores it), and an {@code Instant}
     * read into a {@code LocalDateTime}, {@code ZonedDateTime} or {@link java.time.OffsetDateTime} target is that instant
     * expressed in the JVM default time zone ({@code N.convert} gave the zoned targets offset UTC, the accidental result of parsing
     * {@code Instant.toString()}; the driver 3 executor and abacus-common's own {@code Date} conversions use the default zone).
     * An {@link InetAddress} converted to {@code String} is its {@link InetAddress#getHostAddress() host address} ({@code N.convert}
     * renders {@code InetAddress.toString()}, for example {@code "/127.0.0.1"}).
     * Every other conversion is delegated to {@code N.convert}.
     * @throws RuntimeException if {@code value} cannot be converted to {@code targetClass}
     */
    @SuppressWarnings("unchecked")
    static <T> T convertValue(final Object value, final Class<T> targetClass) throws RuntimeException {
        if (targetClass == byte[].class && value instanceof final ByteBuffer byteBuffer) {
            return (T) remainingBytes(byteBuffer);
        } else if (targetClass == ByteBuffer.class && value instanceof final byte[] bytes) {
            return (T) ByteBuffer.wrap(bytes);
        } else if (targetClass == String.class && value instanceof final ByteBuffer byteBuffer) {
            // WHY: N.convert(ByteBuffer, String.class) base64-encodes only the bytes before the position, and a driver-decoded blob
            // is positioned at 0, so a blob read into a String target or property became "". Encode the remaining bytes (the blob's
            // content, as for a byte[] target above) through a duplicate, leaving the caller's buffer untouched; base64 is the text
            // form a String bound to a blob column is decoded from (see toBindValue).
            return (T) Strings.base64Encode(remainingBytes(byteBuffer));
        } else if (targetClass == java.sql.Time.class && value instanceof final LocalTime localTime) {
            return (T) fromTimeOfDay(localTime, targetClass);
        } else if (targetClass == Instant.class && value instanceof final java.time.LocalDateTime localDateTime) {
            // WHY: N.convert(LocalDateTime, Instant.class) parses the zone-less toString() as an Instant and throws
            // DateTimeParseException, which the bind loop does not catch, so a LocalDateTime bound to a timestamp column failed.
            // Read in the JVM default zone: the instant the driver 3 executor stores for it (N.convert(localDateTime, Date.class))
            // and the one CqlBuilder inlines for a raw LocalDateTime literal.
            return (T) localDateTime.atZone(java.time.ZoneId.systemDefault()).toInstant();
        } else if (targetClass == Instant.class && value instanceof final java.time.chrono.ChronoZonedDateTime<?> zonedDateTime) {
            // WHY: N.convert(ZonedDateTime, Instant.class) parses the "[region]"-suffixed toString() and throws as well; the value
            // already denotes an instant. (An OffsetDateTime converts through N.convert.)
            return (T) zonedDateTime.toInstant();
        } else if (targetClass == Instant.class && value instanceof final LocalDate localDate) {
            // WHY: N.convert(LocalDate, Instant.class) parses the date-only toString() as an Instant and throws DateTimeParseException,
            // which the bind loop does not catch, so a LocalDate bound to a timestamp column failed (and a date column could not be
            // read into an Instant). The start of that day in the JVM default zone: the instant the driver 3 executor stores for it
            // (N.convert(localDate, Date.class)), a LocalDateTime at midnight binds, and a Date target of a date column receives.
            return (T) localDate.atStartOfDay(java.time.ZoneId.systemDefault()).toInstant();
        } else if (targetClass == String.class && value instanceof final InetAddress inetAddress) {
            // WHY: N.convert renders InetAddress.toString(), "hostname/address" ("/127.0.0.1" for an inet value the driver decodes),
            // which is not an address literal (Cassandra rejects it for an inet column). The host address is the value's text form,
            // the one CqlBuilder renders for an inet literal.
            return (T) inetAddress.getHostAddress();
        } else if (targetClass == java.time.LocalDateTime.class && value instanceof final Instant instant) {
            // WHY: N.convert(Instant, LocalDateTime.class) rejects the trailing 'Z' (DateTimeParseException), so a timestamp column
            // could not be read into a LocalDateTime target or bean property. The driver 3 executor reads a timestamp (a Date) into
            // a LocalDateTime in the JVM default zone; do the same.
            return (T) java.time.LocalDateTime.ofInstant(instant, java.time.ZoneId.systemDefault());
        } else if (targetClass == java.time.ZonedDateTime.class && value instanceof final Instant instant) {
            // WHY: N.convert(Instant, ZonedDateTime.class) parses Instant.toString() and so yielded the accidental offset UTC ("Z").
            // The instant is preserved either way; one rule for every java.time target: zone-less and zoned targets are expressed in
            // the JVM default zone, consistent with the LocalDateTime read above, with the driver 3 executor (N.convert(Date,
            // ZonedDateTime.class) uses the default zone) and with abacus-common's own Date conversions. Callers wanting UTC use
            // withZoneSameInstant(ZoneOffset.UTC).
            return (T) instant.atZone(java.time.ZoneId.systemDefault());
        } else if (targetClass == java.time.OffsetDateTime.class && value instanceof final Instant instant) {
            // WHY: same as the ZonedDateTime branch; N.convert gave offset UTC rather than the JVM default zone's offset.
            return (T) java.time.OffsetDateTime.ofInstant(instant, java.time.ZoneId.systemDefault());
        }

        return N.convert(value, targetClass);
    }

    // A copy of the buffer's remaining bytes (a blob's content); the buffer's position, limit and mark are unchanged.
    private static byte[] remainingBytes(final ByteBuffer byteBuffer) {
        final ByteBuffer duplicate = byteBuffer.duplicate();
        final byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);

        return bytes;
    }

    /**
     * Converts a bind value that the column's codec does not accept to the column's Java type {@code javaClass}, as
     * {@link #prepareStatement(String, Object...)} binds it. Text for a {@code blob} (or custom-type) column is base64-decoded into
     * a buffer positioned at 0. Any other value is converted by {@link #convertValue(Object, Class)}; a value that conversion rejects
     * with {@link ClassCastException} or {@link IllegalArgumentException} is returned unchanged, so the driver reports it (a
     * {@link CodecNotFoundException}). A conversion that would change a number is rejected instead of bound: a number with a
     * fractional part (see {@link #isLossyNumberConversion(Object, Object)}) for an integer column ({@code tinyint},
     * {@code smallint}, {@code int}, {@code bigint}, {@code varint} or {@code counter}), and a finite number beyond the range of a
     * {@code float} or {@code double} column.
     *
     * @throws IllegalArgumentException if the conversion would change a number as described above, or text bound to a {@code blob}
     *         column is not valid base64; the message names the parameter, the value or problem, and the column's CQL type
     * @throws ArithmeticException if a value is out of the range of its integer column, or is {@code NaN} or infinite for it
     * @throws RuntimeException if the conversion fails otherwise
     */
    private static Object toBindValue(final Object value, final Class<?> javaClass, final DataType colType, final ColumnDefinitions variables,
            final Map<Integer, String> namedParameters, final int index) throws IllegalArgumentException, ArithmeticException, RuntimeException {
        if (javaClass == ByteBuffer.class && value instanceof final String text) {
            // WHY: N.convert(String, ByteBuffer.class) base64-decodes the text but returns the buffer positioned at its limit, so the
            // driver bound an EMPTY blob; and text that is not base64 failed inside that conversion, was bound unchanged and surfaced
            // only as the driver's CodecNotFoundException. Decode into a buffer positioned at 0 (the text form a blob read into a
            // String has, see convertValue), and reject invalid text here, outside the keep-the-original fallback below.
            try {
                return ByteBuffer.wrap(Strings.base64Decode(text));
            } catch (final IllegalArgumentException e) {
                throw new IllegalArgumentException("Cannot bind text to " + describeParameter(variables, namedParameters, index) + " of CQL type "
                        + colType.asCql(false, true) + ": the text is not valid base64 (" + e.getMessage() + ")", e);
            }
        }

        final Object converted;

        try {
            converted = convertValue(value, javaClass);
        } catch (final ClassCastException | IllegalArgumentException e) {
            // Type conversion failed, keep original value
            if (logger.isDebugEnabled()) {
                logger.debug("Failed to convert parameter at index {} from {} to {}; keeping original value", index, value.getClass().getName(),
                        javaClass.getName(), e);
            }

            return value;
        }

        // WHY: N.convert truncates a fractional number for an integer type (1.5 -> 1) and turns a finite number beyond the float or
        // double range into Infinity, so a value other than the one bound was silently stored (the driver's own codecs reject the
        // original value). Thrown here, outside the catch above, so the bind fails with this message rather than the original value
        // being bound and rejected as a CodecNotFoundException.
        if (isLossyNumberConversion(value, converted)) {
            throw new IllegalArgumentException("Cannot bind " + ClassUtil.getSimpleClassName(value.getClass()) + " value " + value + " to "
                    + describeParameter(variables, namedParameters, index) + " of CQL type " + colType.asCql(false, true)
                    + " without changing it: it would be bound as " + converted);
        }

        return converted;
    }

    /**
     * Returns whether converting the bind value {@code source} produced a different number {@code converted}: a number with a
     * fractional part (a {@code Double}, {@code Float}, {@code BigDecimal}, or another {@link Number} such as an abacus
     * {@code Fraction} or {@code MutableDouble}) converted to an integer type, or a finite number converted to an infinite
     * {@code Float} or {@code Double}. (A non-finite value, or an integral value out of the integer type's range, already makes
     * {@code N.convert} throw {@link ArithmeticException}; rounding to the nearest {@code float}/{@code double} is not a change.)
     */
    private static boolean isLossyNumberConversion(final Object source, final Object converted) {
        if (!(source instanceof final Number number)) {
            return false;
        }

        // WHY (r2): the first version tested only Double/Float/BigDecimal sources. N.convert truncates any Number through
        // intValue()/longValue(), so a Fraction 3/2 or MutableDouble 1.5 was still bound as 1; and it took every other Number for
        // finite, so an infinite MutableDouble/MutableFloat/DoubleAdder bound to a float or double column (which stays Infinity)
        // was rejected "without changing it: it would be bound as Infinity". Only BigDecimal and BigInteger values are finite
        // whatever their double value (which overflows to Infinity for a huge value); any other number is as finite as its
        // double value, and an integral one (Long, AtomicLong, MutableInt, ...) always has an integral double value.
        if (converted instanceof Float || converted instanceof Double) {
            return Double.isInfinite(((Number) converted).doubleValue())
                    && (number instanceof BigDecimal || number instanceof BigInteger || Double.isFinite(number.doubleValue()));
        } else if (converted instanceof Byte || converted instanceof Short || converted instanceof Integer || converted instanceof Long
                || converted instanceof BigInteger) {
            if (number instanceof final BigDecimal decimal) {
                return decimal.signum() != 0 && decimal.stripTrailingZeros().scale() > 0;
            }

            // A Float widens to double exactly; a huge BigInteger gives Infinity, which rint keeps.
            final double d = number.doubleValue();

            return d != Math.rint(d);
        }

        return false;
    }

    // "parameter 'name' (index i)", or "parameter at index i" when no name is known, for bind error messages. A ParsedCql marker name
    // (":name" or "#{name}") is preferred; otherwise the prepared statement's variable name (the bound column) is used.
    private static String describeParameter(final ColumnDefinitions variables, final Map<Integer, String> namedParameters, final int index) {
        String name = namedParameters == null ? null : namedParameters.get(index);

        if (Strings.isEmpty(name)) {
            final ColumnDefinition variable = variables.get(index);
            final CqlIdentifier id = variable == null ? null : variable.getName();
            name = id == null ? null : id.asInternal();
        }

        return Strings.isEmpty(name) ? "parameter at index " + index : "parameter '" + name + "' (index " + index + ")";
    }

    /**
     * Converts a driver-decoded column or field value to {@code targetClass}. A CQL time, which the driver decodes as a
     * {@link LocalTime}, becomes a value of each supported date/time type (see {@link #fromTimeOfDay(LocalTime, Class)});
     * {@code N.convert} parses the LocalTime's text and rejects it for those types.
     * A CQL timestamp, decoded as an {@link Instant}, read into a {@link LocalDate}, {@link LocalTime} or
     * {@link java.time.OffsetTime} target is that instant's date or time of day in the JVM default time zone, and a CQL date,
     * decoded as a {@link LocalDate}, read into a {@link java.time.LocalDateTime}, {@link java.time.ZonedDateTime} or
     * {@link java.time.OffsetDateTime} target is the start of that day (in the JVM default time zone for the zoned targets);
     * {@code N.convert} parses the value's text and rejects every one of these pairs.
     * Every other value is converted by {@link #convertValue(Object, Class)}. A null value becomes the default value of
     * {@code targetClass}: the primitive default, an empty value for Optional/Nullable/OptionalInt-like types, and null
     * for wrappers and other reference types.
     * @throws RuntimeException if {@code value} cannot be converted to {@code targetClass}
     */
    @SuppressWarnings("unchecked")
    private static <T> T decodeValue(final Object value, final Class<T> targetClass) throws RuntimeException {
        if (value instanceof final LocalTime localTime && isTimeOfDayTarget(targetClass)) {
            return (T) fromTimeOfDay(localTime, targetClass);
        } else if (value instanceof final Instant instant
                && (targetClass == LocalDate.class || targetClass == LocalTime.class || targetClass == java.time.OffsetTime.class)) {
            // WHY: N.convert parses Instant.toString() for these targets and throws (DateTimeParseException for LocalDate and LocalTime,
            // IllegalArgumentException for OffsetTime), so a timestamp column could not be read into them although the driver 3 executor
            // reads a timestamp (a Date) into a LocalDate or LocalTime in the JVM default zone. Same zone as the LocalDateTime read
            // (convertValue), so each target agrees with LocalDateTime.ofInstant(instant, systemDefault()).
            final java.time.ZonedDateTime zoned = instant.atZone(java.time.ZoneId.systemDefault());

            return (T) (targetClass == LocalDate.class ? zoned.toLocalDate()
                    : targetClass == LocalTime.class ? zoned.toLocalTime() : zoned.toOffsetDateTime().toOffsetTime());
        } else if (value instanceof final LocalDate localDate && (targetClass == java.time.LocalDateTime.class || targetClass == java.time.ZonedDateTime.class
                || targetClass == java.time.OffsetDateTime.class)) {
            // WHY: N.convert parses the date-only toString() as a date-time and throws DateTimeParseException, so a date column could
            // not be read into these targets, while a Date, Timestamp or Calendar target receives the start of that day in the JVM
            // default zone (N.convert's LocalDate conversion); the zoned targets get that same instant (as an Instant target does).
            if (targetClass == java.time.LocalDateTime.class) {
                return (T) localDate.atStartOfDay();
            }

            final java.time.ZonedDateTime startOfDay = localDate.atStartOfDay(java.time.ZoneId.systemDefault());

            return (T) (targetClass == java.time.ZonedDateTime.class ? startOfDay : startOfDay.toOffsetDateTime());
        }

        // Keep null on the conversion path: it supplies the target's default (primitive default, empty Optional-like, or null).
        return convertValue(value, targetClass);
    }

    // The date/time types a time column or UDT field is read into. A LocalTime, Date, or Calendar bound to time reads back
    // its time of day; the java.time date-time types, which this executor does not bind to time, receive it on 1970-01-01.
    private static boolean isTimeOfDayTarget(final Class<?> targetClass) {
        return targetClass == LocalTime.class || Date.class.isAssignableFrom(targetClass) || Calendar.class.isAssignableFrom(targetClass)
                || targetClass == Instant.class || targetClass == java.time.LocalDateTime.class || targetClass == java.time.OffsetDateTime.class
                || targetClass == java.time.ZonedDateTime.class || targetClass == java.time.OffsetTime.class;
    }

    /**
     * Converts a CQL time of day to a date/time target: the time itself for {@code LocalTime}, and otherwise that time of day on
     * 1970-01-01 in the current JVM default zone, the date a {@code java.sql.Time} carries. A {@code Date} or {@code Calendar}
     * bound to a time column or field therefore reads back with the same local time of day while the JVM default zone is
     * unchanged (a Date is bound in the zone captured when Abacus initialized); the date part is not stored. A
     * {@code java.sql.Time} keeps milliseconds and a {@code Timestamp} nanoseconds.
     */
    private static Object fromTimeOfDay(final LocalTime localTime, final Class<?> targetClass) {
        if (targetClass == LocalTime.class) {
            return localTime;
        } else if (targetClass == java.sql.Time.class) {
            // N.convert parses the LocalTime's text and rejects both a fractional second ("01:02:03.456", usual for a CQL time,
            // which has nanosecond precision) and a whole minute ("01:02", e.g. midnight "00:00"). Same local time of day on
            // 1970-01-01 as the standard conversion gives for whole seconds, plus the milliseconds.
            final java.sql.Time time = java.sql.Time.valueOf(localTime);
            time.setTime(time.getTime() + localTime.getNano() / 1_000_000);

            return time;
        }

        final java.time.LocalDateTime dateTime = LocalDate.EPOCH.atTime(localTime);

        if (targetClass == java.time.LocalDateTime.class) {
            return dateTime;
        } else if (targetClass == java.time.OffsetTime.class) {
            return localTime.atOffset(java.time.ZoneId.systemDefault().getRules().getOffset(dateTime));
        }

        final java.time.ZonedDateTime zonedDateTime = dateTime.atZone(java.time.ZoneId.systemDefault());

        if (targetClass == java.time.ZonedDateTime.class) {
            return zonedDateTime;
        } else if (targetClass == java.time.OffsetDateTime.class) {
            return zonedDateTime.toOffsetDateTime();
        } else if (targetClass == Instant.class) {
            return zonedDateTime.toInstant();
        }

        // Date (including java.sql.Timestamp) and Calendar: N.convert reads a LocalDateTime in the current default zone.
        return N.convert(dateTime, targetClass);
    }

    /**
     * Converts a driver-decoded value for {@link PropInfo#setPropValue(Object, Object)} where that method's own conversion
     * fails: it turns a BLOB {@link ByteBuffer} into {@code null} for a {@code byte[]} property, rejects a {@link LocalTime}
     * for a {@code java.sql.Time} or other date/time property (see {@link #fromTimeOfDay(LocalTime, Class)}), rejects a
     * {@code timestamp} {@link Instant} for a {@link java.time.LocalDateTime}, {@link LocalDate}, {@link LocalTime} or
     * {@link java.time.OffsetTime} property and would express it at offset UTC for a
     * {@link java.time.ZonedDateTime}/{@link java.time.OffsetDateTime} property, rejects a {@code date} {@link LocalDate} for an
     * {@code Instant}, {@code LocalDateTime}, {@code ZonedDateTime} or {@code OffsetDateTime} property (see
     * {@link #decodeValue(Object, Class)} and {@link #convertValue(Object, Class)}: the JVM default zone is used), renders an
     * {@code inet} {@link InetAddress} as {@code "/address"} for a {@code String} property, and turns a BLOB {@code ByteBuffer} into
     * {@code ""} for a {@code String} property (here it becomes the base64 text of the buffer's remaining bytes). Any other value is
     * returned unchanged.
     * @throws RuntimeException if {@code value} cannot be converted to {@code propClass}
     */
    private static Object toPropValue(final Class<?> propClass, final Object value) throws RuntimeException {
        // WHY: PropInfo.setPropValue converts through N.convert, which throws DateTimeParseException for Instant -> LocalDateTime,
        // LocalDate or LocalTime and for LocalDate -> Instant or a java.time date-time, yields offset UTC for Instant ->
        // ZonedDateTime/OffsetDateTime, renders InetAddress.toString() for a String, and encodes only the bytes before a ByteBuffer's
        // position ("" for a blob) for a String; route those pairs through decodeValue like the other driver values it cannot map.
        return ((propClass == byte[].class || propClass == String.class) && value instanceof ByteBuffer)
                || (value instanceof LocalTime && propClass != LocalTime.class && isTimeOfDayTarget(propClass))
                || (value instanceof Instant && (propClass == java.time.LocalDateTime.class || propClass == java.time.ZonedDateTime.class
                        || propClass == java.time.OffsetDateTime.class || propClass == LocalDate.class || propClass == LocalTime.class
                        || propClass == java.time.OffsetTime.class))
                || (value instanceof LocalDate && (propClass == Instant.class || propClass == java.time.LocalDateTime.class
                        || propClass == java.time.ZonedDateTime.class || propClass == java.time.OffsetDateTime.class))
                || (value instanceof InetAddress && propClass == String.class) ? decodeValue(value, propClass) : value;
    }

    /**
     * Converts a driver-decoded value for {@code propInfo}, as {@link PropInfo#setPropValue(Object, Object)} then stores it. A
     * collection or map for a property declared as a parameterized collection or map, or as an object array, has its elements
     * (keys and values) converted to the declared element types (see {@link #toContainerValue(Type, Object)}); any other value is
     * converted as {@link #toPropValue(Class, Object)} converts it.
     * @throws RuntimeException if {@code value}, or one of its elements, cannot be converted to the property's (element) type
     */
    private static Object toPropValue(final PropInfo propInfo, final Object value) throws RuntimeException {
        if (value instanceof Collection || value instanceof Map) {
            // WHY: PropInfo.setPropValue stores a driver collection or map as is when the property's raw class accepts it (a List
            // property, an ArrayList), so a List<byte[]> or List<String> property of a list<blob> column held ByteBuffers, a
            // List<LocalDateTime> of a list<timestamp> held Instants and a Set<String> of a set<inet> held InetAddresses: heap
            // pollution that failed with ClassCastException where the elements were used. The elements now get the conversions a
            // top-level value of the element type gets. A container whose elements all fit is stored as before (no copy).
            final Object converted = toContainerValue(propInfo.type, value);

            if (converted != value) {
                return propInfo.clazz.isInstance(converted) ? converted : N.convert(converted, propInfo.type);
            }

            return value;
        }

        return toPropValue(propInfo.clazz, value);
    }

    /**
     * Converts the elements of a driver-decoded collection (for a collection or object-array {@code type}) or the keys and values of
     * a map (for a map {@code type}) to the element types {@code type} declares, by {@link #toElementValue(Type, Object)}. Returns
     * {@code value} itself when every element already fits (or {@code type} declares no element types); otherwise a new
     * {@link ArrayList} (a {@link LinkedHashSet} for a set type, or a {@code Set} value of a type that a {@code LinkedHashSet}
     * satisfies) or {@link LinkedHashMap} of the converted elements, in the driver's iteration order, which the caller converts to
     * the declared container class if it is not one already.
     * @throws RuntimeException if an element cannot be converted to its declared type
     */
    private static Object toContainerValue(final Type<?> type, final Object value) throws RuntimeException {
        if (value instanceof final Collection<?> collection && (type.isCollection() || type.isObjectArray())) {
            final Type<?> elementType = type.elementType();

            if (elementType == null) {
                return value;
            }

            List<Object> converted = null;
            int index = 0;

            for (final Object element : collection) {
                final Object elementValue = toElementValue(elementType, element);

                if (converted != null) {
                    converted.add(elementValue);
                } else if (elementValue != element) {
                    // The first element that changes: copy the elements before it, which fit as they are.
                    converted = new ArrayList<>(collection.size());
                    final Iterator<?> iter = collection.iterator();

                    for (int i = 0; i < index; i++) {
                        converted.add(iter.next());
                    }

                    converted.add(elementValue);
                }

                index++;
            }

            if (converted == null) {
                return value;
            }

            return type.isSet() || (collection instanceof Set && type.javaType().isAssignableFrom(LinkedHashSet.class)) ? new LinkedHashSet<>(converted)
                    : converted;
        } else if (value instanceof final Map<?, ?> map && type.isMap()) {
            final List<Type<?>> parameterTypes = type.parameterTypes();

            if (parameterTypes.size() != 2) {
                return value;
            }

            final Type<?> keyType = parameterTypes.get(0);
            final Type<?> valueType = parameterTypes.get(1);
            Map<Object, Object> converted = null;
            int index = 0;

            for (final Map.Entry<?, ?> entry : map.entrySet()) {
                final Object key = toElementValue(keyType, entry.getKey());
                final Object entryValue = toElementValue(valueType, entry.getValue());

                if (converted != null) {
                    converted.put(key, entryValue);
                } else if (key != entry.getKey() || entryValue != entry.getValue()) {
                    converted = new LinkedHashMap<>();
                    final Iterator<? extends Map.Entry<?, ?>> iter = map.entrySet().iterator();

                    for (int i = 0; i < index; i++) {
                        final Map.Entry<?, ?> previous = iter.next();
                        converted.put(previous.getKey(), previous.getValue());
                    }

                    converted.put(key, entryValue);
                }

                index++;
            }

            return converted == null ? value : converted;
        }

        return value;
    }

    /**
     * Converts one element (or map key or value) of a driver-decoded collection or map to {@code type}, as a top-level column value
     * of that type is converted ({@link #decodeValue(Object, Class)}): for example a {@code blob} to {@code byte[]} or base64 text, a
     * {@code timestamp} to a {@code LocalDateTime} in the JVM default zone, an {@code inet} to its address text. A nested
     * collection or map is converted element by element ({@link #toContainerValue(Type, Object)}) and then to {@code type}'s
     * container class (for example a {@code String[]}) if it is not one already; a single value for a collection or map
     * {@code type} is converted by {@code N.convert} to that type, element type included. Returns {@code value} itself when it
     * already is a {@code type} (a nested container: when its elements also fit), and keeps a {@code null}, a UDT value and a tuple
     * value as the driver decodes them, as a UDT or tuple column is set.
     * @throws RuntimeException if {@code value} cannot be converted to {@code type}
     */
    private static Object toElementValue(final Type<?> type, final Object value) throws RuntimeException {
        if (value == null) {
            return null;
        }

        final Class<?> cls = type.javaType();

        if (value instanceof Collection || value instanceof Map) {
            final Object converted = toContainerValue(type, value);

            return cls.isInstance(converted) ? converted : N.convert(converted, type);
        }

        if (cls.isInstance(value) || value instanceof UdtValue || value instanceof TupleValue) {
            return value;
        }

        // WHY (r2 verify): a scalar for a parameterized collection or map element type (a set<int> column into a List<List<Long>>
        // property) is converted to that full type, as PropInfo.setPropValue's N.convert of the whole container did before; the raw
        // class (N.convert(3, List.class)) dropped the element type and stored a List<Integer> in it.
        return type.isCollection() || type.isMap() ? N.convert(value, type) : decodeValue(value, cls);
    }

    /**
     * Converts a column value for storage in an array row whose component type is {@code componentType}. A typed array
     * (e.g. {@code String[]}) otherwise rejects a driver value of another type with {@link ArrayStoreException}.
     * @throws RuntimeException if {@code value} cannot be converted to {@code componentType}
     */
    private static Object toArrayElement(final Object value, final Class<?> componentType) throws RuntimeException {
        return value == null || componentType.isInstance(value) ? value : decodeValue(value, componentType);
    }

    private static Object toArrayElement(final Row row, final Class<?> componentType) throws RuntimeException {
        if (componentType != Object.class && componentType.isInstance(row)) {
            return row;
        }

        // Object[] retains the established nested-row flattening; typed arrays require their own component type.
        if (componentType == Object.class) {
            return readRow(Object[].class, row);
        }

        return readRow(componentType, row);
    }

    /**
     * @throws IllegalArgumentException if {@code rowClass} is a single-value type and the row does not have exactly one column
     * @throws RuntimeException if reading or converting a column, creating the target object, or assigning a bean property fails
     */
    @SuppressWarnings({ "rawtypes", "null" })
    private static <T> T readRow(final Class<T> rowClass, final Row row) throws IllegalArgumentException, RuntimeException {
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
                    a[i] = toArrayElement((Row) value, a.getClass().getComponentType());
                } else {
                    a[i] = toArrayElement(value, a.getClass().getComponentType());
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

            // A NULL column goes through decodeValue like createRowMapper's: a primitive class token yields its default.
            if (value != null && rowClass.isAssignableFrom(value.getClass())) {
                res = value;
            } else {
                res = decodeValue(value, rowClass);
            }

        } else {
            throw new IllegalArgumentException("Column count must be 1 to map a row to the single-value type: " + ClassUtil.getCanonicalClassName(rowClass)
                    + ", but the row has " + columnCount + " columns");
        }

        return (T) res;
    }

    /**
     * @throws IllegalArgumentException if {@code rowClass} is a single-value type and the result does not have exactly one column
     */
    @SuppressWarnings("rawtypes")
    private static <T> Function<Row, T> createRowMapper(final Class<T> rowClass, final ColumnDefinitions columnDefinitions) throws IllegalArgumentException {
        final Type<?> rowType = rowClass == null ? null : N.typeOf(rowClass);
        final int columnCount = columnDefinitions.size();

        Function<Row, T> mapper = null;

        if (rowType == null || rowType.isObjectArray()) {
            final Class<?> componentType = rowClass == null ? Object.class : rowClass.getComponentType();

            mapper = row -> {
                final Object[] a = N.newArray(componentType, columnCount);
                Object value = null;

                for (int i = 0; i < columnCount; i++) {
                    value = row.getObject(i);

                    if (value instanceof Row) {
                        a[i] = toArrayElement((Row) value, componentType);
                    } else {
                        a[i] = toArrayElement(value, componentType);
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

                /**
                 * @throws RuntimeException if the row does not match the target type, a column cannot be read or converted, or a mapped bean
                 *         cannot be populated
                 */
                @SuppressWarnings("null")
                @Override
                public T apply(final Row row) throws RuntimeException {
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
                            return decodeValue(value, rowClass);
                        }
                    } else {
                        return decodeValue(value, rowClass);
                    }
                }
            };
        } else {
            throw new IllegalArgumentException("Column count must be 1 to map a row to the single-value type: " + ClassUtil.getCanonicalClassName(rowClass)
                    + ", but the row has " + columnCount + " columns");
        }

        return mapper;
    }

    /**
     * Retrieves at most one entity matching the given WHERE condition (the "get-typed" contract).
     *
     * <p>Builds a {@code SELECT} that requests the matching row with {@code LIMIT 2} (so a duplicate
     * match can be detected) and maps the result to an instance of {@code targetClass}. If no row
     * matches, returns {@code null}. If two or more rows match, a {@link DuplicateResultException}
     * is thrown.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Find user by email (selecting all properties)
     * User user = executor.gett(User.class, null, Filters.eq("email", email)); // returns the single matching User, or null if none
     *
     * // Select only specific properties
     * User partial = executor.gett(User.class, Arrays.asList("id", "name"), Filters.eq("id", userId)); // only id and name populated
     *
     * // If more than one row matches the condition:
     * executor.gett(User.class, null, Filters.eq("status", "active")); // throws DuplicateResultException when >1 match
     * }</pre>
     *
     * @param <T> the type of the entity to retrieve
     * @param targetClass the entity class to map the result row to
     * @param selectPropNames the property names to select, or {@code null} to select all entity properties
     * @param whereClause the WHERE condition
     * @return an instance of {@code targetClass} populated with data from the single matching row,
     *         or {@code null} when no row matches
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}, {@code whereClause} contains a relation unsupported by CQL,
     *         or a condition value is rejected when converted to its column's type (see <i>Number and Blob Conversion</i> in the class
     *         documentation)
     * @throws RuntimeException if preparing, binding, executing, or fetching the CQL fails; or a returned column cannot
     *         be converted to the requested Java type or assigned to a mapped bean property
     * @throws DuplicateResultException if more than one row matches the WHERE condition
     */
    @Override
    public <T> T gett(final Class<T> targetClass, final Collection<String> selectPropNames, final Condition whereClause)
            throws IllegalArgumentException, RuntimeException, DuplicateResultException {
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
     * query produces no rows. If a row exists but the column is {@code NULL}, a wrapper or other reference
     * {@code valueClass} yields a <i>present-but-null</i> {@code Nullable} ({@code Nullable.of(null)}).
     * A primitive class token yields the primitive default (for example, {@code int.class} yields
     * {@code Nullable.of(0)}), and a type whose default is not {@code null}, such as {@code Optional},
     * {@code Nullable} or {@code OptionalInt}, yields its empty value (for example, {@code Optional.class} yields
     * {@code Nullable.of(Optional.empty())}). Use a wrapper class such as {@code Integer.class} to preserve {@code NULL}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Nullable<Long> userCount = executor.queryForSingleValue(
     *     Long.class, "SELECT count(*) FROM users"); // present Nullable holding the count
     *
     * // No rows matched -> Nullable.empty()
     * Nullable<String> userName = executor.queryForSingleValue(
     *     String.class, "SELECT name FROM users WHERE id = ?", unknownId); // returns Nullable.empty() when no row matches
     *
     * // Row matched but column is NULL -> present-but-null (NOT empty)
     * Nullable<String> nick = executor.queryForSingleValue(
     *     String.class, "SELECT nickname FROM users WHERE id = ?", userId); // Nullable.of(null) when the row exists but column is NULL
     * }</pre>
     *
     * @param <V> the type of the single result value to be returned
     * @param valueClass the Java class the column value is converted to (must not be {@code null})
     * @param query the CQL query string with {@code ?} placeholders for parameters
     * @param parameters the values to bind, in declaration order
     * @return a <i>present</i> {@code Nullable<V>} holding the converted column value (for a {@code NULL} column, the default
     *         value of {@code valueClass}: the primitive default, the empty value of an {@code Optional}-like type, or {@code null})
     *         when at least one row is returned; {@code Nullable.empty()} when the query returns no rows
     * @throws IllegalArgumentException if {@code valueClass} or {@code query} is {@code null}, the CQL contains malformed or mixed
     *         parameter markers, the supplied parameter count or names do not match the prepared statement, or a parameter value is
     *         rejected when converted to its column's type (see <i>Number and Blob Conversion</i> in the class documentation)
     * @throws RuntimeException if preparing, binding, executing, or fetching the CQL fails; or a returned column cannot
     *         be converted to the requested Java type or assigned to a mapped bean property
     * @see #queryForSingleNonNull(Class, String, Object...)
     */
    @Override
    public <V> Nullable<V> queryForSingleValue(final Class<V> valueClass, final String query, final Object... parameters)
            throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(valueClass, cs.valueClass);

        final ResultSet resultSet = execute(query, parameters);
        final Row row = resultSet.one();

        return row == null ? (Nullable<V>) Nullable.empty() : Nullable.of(decodeValue(row.getObject(0), valueClass));
    }

    /**
     * Executes the given CQL query and returns the first column of the first row converted to
     * {@code valueClass}, wrapped in an {@link Optional} that is guaranteed to be non-null when present.
     *
     * <p>Only the first column of the first row of the {@code ResultSet} is read; remaining rows and
     * columns are ignored. Parameters are bound positionally to {@code ?} placeholders.</p>
     *
     * <p><b>Empty vs. present semantics:</b> {@code Optional.empty()} is returned <i>only</i> when the
     * query produces no rows. When a row is returned, the converted column value is wrapped in the
     * {@code Optional} via {@link Optional#of(Object)}, which does not accept a null payload. If
     * conversion to {@code valueClass} yields {@code null},
     * this method throws {@link NullPointerException} rather than returning {@code Optional.empty()}.
     * A {@code NULL} column maps to the default value of {@code valueClass}: the primitive default for a primitive
     * class token (for example, {@code int.class} yields {@code Optional.of(0)}), an empty value for a type whose
     * default is not {@code null} such as {@code Optional}, {@code Nullable} or {@code OptionalInt} (for example,
     * {@code Optional.class} yields {@code Optional.of(Optional.empty())}), and {@code null} for a wrapper or other
     * reference type, which causes the exception.
     * Use {@link #queryForSingleValue(Class, String, Object...)} (which returns {@link Nullable})
     * with a wrapper or other reference target whose default is {@code null} (not an {@code Optional}-like type) to preserve {@code NULL}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Optional<Long> activeUsers = executor.queryForSingleNonNull(
     *     Long.class, "SELECT count(*) FROM users WHERE status = 'active'"); // present Optional holding the count
     *
     * Optional<String> email = executor.queryForSingleNonNull(
     *     String.class, "SELECT email FROM users WHERE id = ?", userId); // present Optional holding the email
     *
     * // No rows matched -> Optional.empty()
     * Optional<String> none = executor.queryForSingleNonNull(
     *     String.class, "SELECT email FROM users WHERE id = ?", unknownId); // returns Optional.empty() when no row matches
     *
     * // Row matched but column is NULL -> throws (Optional cannot hold null)
     * executor.queryForSingleNonNull(String.class,
     *     "SELECT nickname FROM users WHERE id = ?", userId); // throws NullPointerException when the matched value is NULL
     * }</pre>
     *
     * @param <V> the type of the single result value to be returned
     * @param valueClass the Java class the column value is converted to (must not be {@code null})
     * @param query the CQL query string with {@code ?} placeholders for parameters
     * @param parameters the values to bind, in declaration order
     * @return a <i>present</i> {@code Optional<V>} holding the converted non-null value when at least
     *         one row is returned; {@code Optional.empty()} when the query
     *         returns no rows
     * @throws IllegalArgumentException if {@code valueClass} or {@code query} is {@code null}, the CQL contains malformed or mixed
     *         parameter markers, the supplied parameter count or names do not match the prepared statement, or a parameter value is
     *         rejected when converted to its column's type (see <i>Number and Blob Conversion</i> in the class documentation)
     * @throws RuntimeException if preparing, binding, executing, or fetching the CQL fails; or a returned column cannot
     *         be converted to the requested Java type or assigned to a mapped bean property
     * @throws NullPointerException if a row is returned but the converted column value is
     *         {@code null}, because {@link Optional#of(Object)} rejects a null payload
     * @see #queryForSingleValue(Class, String, Object...)
     */
    @Override
    public <V> Optional<V> queryForSingleNonNull(final Class<V> valueClass, final String query, final Object... parameters)
            throws IllegalArgumentException, RuntimeException, NullPointerException {
        N.checkArgNotNull(valueClass, cs.valueClass);

        final ResultSet resultSet = execute(query, parameters);
        final Row row = resultSet.one();

        return row == null ? (Optional<V>) Optional.empty() : Optional.of(decodeValue(row.getObject(0), valueClass));
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
     * <li><strong>Object array class:</strong> Results mapped to {@code Object[]} or typed object arrays, one element per
     *     column (a primitive array class such as {@code long[]} is a single-value type read from one column)</li>
     * </ul>
     *
     * <p><b>Empty vs. present semantics:</b> {@code Optional.empty()} is returned <i>only</i> when the
     * query produces no rows. When a row is found, the mapped value is returned as a <i>present</i>
     * {@code Optional}. Because {@code Optional} cannot carry a {@code null} payload, the mapped value
     * itself must be non-null. For a single-value {@code targetClass}, a {@code NULL} column maps to the default
     * value of that type: the primitive default for a primitive class token (for example, {@code int.class} yields
     * {@code Optional.of(0)}), an empty value for a type whose default is not {@code null} such as {@code Optional},
     * {@code Nullable} or {@code OptionalInt} (for example, {@code Optional.class} yields {@code Optional.of(Optional.empty())}),
     * and {@code null} for a wrapper or other reference type, which causes the {@link NullPointerException}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Optional<User> user = executor.findFirst(User.class,
     *     "SELECT * FROM users WHERE email = ? LIMIT 1", email); // present Optional with the first matching User
     *
     * // No rows matched -> Optional.empty()
     * Optional<User> none = executor.findFirst(User.class,
     *     "SELECT * FROM users WHERE email = ?", unknownEmail); // returns Optional.empty() when no row matches
     *
     * Optional<Map> userData = executor.findFirst(Map.class,
     *     "SELECT name, email FROM users WHERE id = ?", userId); // present Optional holding a Map of the row
     *
     * Optional<Object[]> row = executor.findFirst(Object[].class,
     *     "SELECT count(*), max(created_at) FROM events"); // present Optional holding an Object[] of the columns
     * }</pre>
     *
     * @param <T> the type to map the result row to
     * @param targetClass an entity class with getter/setter methods, {@code Map.class}, a collection, an object
     *        array class, or a single-value type (must not be {@code null})
     * @param query the CQL query string with {@code ?} placeholders for parameters
     * @param parameters the values to bind, in declaration order
     * @return a <i>present</i> {@code Optional<T>} holding the first mapped row when at least one row is
     *         returned; {@code Optional.empty()} when the query returns no rows
     * @throws IllegalArgumentException if {@code targetClass} or {@code query} is {@code null}, the CQL contains malformed or mixed parameter
     *         markers, the supplied parameter count or names do not match the prepared statement, a parameter value is rejected when
     *         converted to its column's type (see <i>Number and Blob Conversion</i> in the class documentation), or {@code targetClass} is a
     *         single-value type but the first row does not have exactly one column
     * @throws RuntimeException if preparing, binding, executing, or fetching the CQL fails; or a returned column cannot
     *         be converted to the requested Java type or assigned to a mapped bean property
     * @throws NullPointerException if the first row maps to a null value, which {@link Optional#of(Object)} cannot contain
     * @see #findFirst(String, Object...)
     * @see #findFirst(Class, Condition)
     */
    @Override
    public <T> Optional<T> findFirst(final Class<T> targetClass, final String query, final Object... parameters)
            throws IllegalArgumentException, RuntimeException, NullPointerException {
        N.checkArgNotNull(targetClass, cs.targetClass);

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
     *         .toList();
     * }
     *
     * // A null rowMapper is rejected eagerly
     * executor.stream("SELECT * FROM users", (BiFunction<ColumnDefinitions, Row, User>) null); // throws IllegalArgumentException
     * }</pre>
     *
     * @param <T> the type of objects in the returned stream
     * @param query the CQL query string
     * @param rowMapper a function that maps column definitions and rows to result objects
     * @param parameters the query parameters
     * @return a Stream of mapped objects
     * @throws IllegalArgumentException if {@code query} or {@code rowMapper} is {@code null}, the CQL contains malformed or mixed
     *         parameter markers, the supplied parameter count or names do not match the prepared statement, or a parameter value is
     *         rejected when converted to its column's type (see <i>Number and Blob Conversion</i> in the class documentation)
     * @throws RuntimeException if the driver rejects CQL or a bound value, the session is closed, or preparing,
     *         executing, or fetching the query fails
     * @see #stream(Class, String, Object...)
     */
    public <T> Stream<T> stream(final String query, final BiFunction<ColumnDefinitions, Row, T> rowMapper, final Object... parameters)
            throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(query, cs.query);
        N.checkArgNotNull(rowMapper, cs.rowMapper);

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
     *              .forEach(System.out::println);
     * }
     *
     * // A null rowMapper is rejected eagerly
     * executor.stream(statement, (BiFunction<ColumnDefinitions, Row, UserActivity>) null); // throws IllegalArgumentException
     * }</pre>
     *
     * @param <T> the type of objects in the returned stream
     * @param statement the configured CQL Statement to execute
     * @param rowMapper a function that maps column definitions and rows to result objects
     * @return a Stream of mapped objects
     * @throws IllegalArgumentException if {@code statement} or {@code rowMapper} is {@code null}
     * @throws RuntimeException if the session is closed or the driver rejects request submission or statement execution
     */
    public <T> Stream<T> stream(final Statement<?> statement, final BiFunction<ColumnDefinitions, Row, T> rowMapper)
            throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(statement, cs.statement);
        N.checkArgNotNull(rowMapper, cs.rowMapper);

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
     * @throws IllegalArgumentException if {@code query} is {@code null}, the CQL contains malformed or mixed
     *         parameter markers, or the supplied parameter count or names do not match the prepared statement
     * @throws AllNodesFailedException if every node tried by the driver fails to execute the request
     * @throws RuntimeException if the driver rejects CQL or a bound value, the session is closed, or preparing,
     *         executing, or fetching the query fails
     */
    @Override
    public ResultSet execute(final String query) throws IllegalArgumentException, AllNodesFailedException, RuntimeException {
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
     * @throws IllegalArgumentException if {@code query} is {@code null}, the CQL contains malformed or mixed
     *         parameter markers, the supplied parameter count or names do not match the prepared statement, or a parameter value is
     *         rejected when converted to its column's type (see <i>Number and Blob Conversion</i> in the class documentation)
     * @throws AllNodesFailedException if every node tried by the driver fails to execute the request
     * @throws RuntimeException if the driver rejects CQL or a bound value, the session is closed, or preparing,
     *         executing, or fetching the query fails
     */
    @Override
    public ResultSet execute(final String query, final Object... parameters) throws IllegalArgumentException, AllNodesFailedException, RuntimeException {
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
     * @throws IllegalArgumentException if {@code query} is {@code null}, the CQL contains malformed or mixed
     *         parameter markers, the supplied parameter count or names do not match the prepared statement, or a parameter value is
     *         rejected when converted to its column's type (see <i>Number and Blob Conversion</i> in the class documentation)
     * @throws AllNodesFailedException if every node tried by the driver fails to execute the request
     * @throws RuntimeException if the driver rejects CQL or a bound value, the session is closed, or preparing,
     *         executing, or fetching the query fails
     */
    @Override
    public ResultSet execute(final String query, final Map<String, Object> parameters)
            throws IllegalArgumentException, AllNodesFailedException, RuntimeException {
        return session.execute(prepareStatement(query, parameters));
    }

    /**
     * Executes a pre-configured CQL Statement and returns the raw ResultSet.
     *
     * <p>This method executes any type of Cassandra Statement object, including
     * BoundStatement, BatchStatement, or SimpleStatement. This provides maximum
     * flexibility when you need fine-grained control over statement configuration
     * such as consistency levels, timeouts, or the execution profile. (In driver 4.x the retry
     * policy is configured per execution profile, not on the statement.)</p>
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
     *         "SELECT * FROM users WHERE token(id) > token(?)")
     *     .addPositionalValue(lastId)
     *     .setPageSize(1000)
     *     .build();
     *
     * ResultSet pagedResults = executor.execute(simpleStatement);
     * }</pre>
     *
     * @param statement the configured CQL Statement to execute
     * @return the raw ResultSet from Cassandra
     * @throws IllegalArgumentException if {@code statement} is {@code null}
     * @throws AllNodesFailedException if every node tried by the driver fails to execute the request
     * @throws RuntimeException if the session is closed or the driver rejects request submission or statement execution
     */
    @Override
    public ResultSet execute(final Statement<?> statement) throws IllegalArgumentException, AllNodesFailedException, RuntimeException {
        N.checkArgNotNull(statement, cs.statement);

        return session.execute(statement);
    }

    /**
     * Closes this executor and releases all associated resources.
     *
     * <p>This method closes the underlying Cassandra session and releases all cached
     * prepared statements. Bound statements are created fresh and are not cached. After calling this method, the executor
     * should not be used for further database operations.</p>
     *
     * <p>This method is idempotent - calling it multiple times has the same effect as
     * calling it once. It's recommended to call {@link #close()} from a finally block
     * to ensure proper resource cleanup.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CassandraExecutor executor = new CassandraExecutor(session);
     * try {
     *     // Perform database operations
     * } finally {
     *     executor.close();
     * }
     * }</pre>
     *
     * @throws RuntimeException if the driver rejects closing the session (for example, when called on a driver I/O
     *         thread) or closing it fails; the prepared-statement cache is released in either case
     */
    public void close() throws RuntimeException {
        try {
            if (!session.isClosed()) {
                session.close();
            }
        } finally {
            preparedStatementPool.close();
        }
    }

    // All five batch INSERT/UPDATE methods below accumulate statements in a builder: BatchStatement.add
    // copies all prior entries on each call, while the builder accumulates them in linear time.
    // Apply configStatement after build(); initializing a builder from a configured template copies settings
    // through driver getters, and getTimeout is broken in driver 4.19.3.

    /**
     * Builds a {@link BatchStatement} containing one {@code INSERT} per entity.
     *
     * <p>Each element of {@code entities} is converted to a parameterized INSERT via
     * {@code prepareInsert(entity)} and added to a new batch of the given {@code type}.
     * The batch is not executed by this method.</p>
     *
     * @param entities the entities to insert; must be non-null, non-empty, and contain no {@code null}
     * @param type the batch type ({@link BatchType#LOGGED}, {@code UNLOGGED}, or {@code COUNTER});
     *        {@code null} is treated as {@code LOGGED}
     * @return a populated {@link BatchStatement} in input iteration order, with this executor's {@link StatementSettings}
     *         (the driver ignores the timeout of a batch; see the class documentation)
     * @throws IllegalArgumentException if {@code entities} is null, empty, or contains a {@code null} element, or an
     *         entity is not a supported bean or exposes no insertable properties
     * @throws RuntimeException if the driver fails to prepare a generated statement or rejects a bound value
     * @throws IllegalStateException if more than 65,535 statements are added, the driver's batch-size limit
     */
    @Override
    protected BatchStatement prepareBatchInsertStatement(final Collection<?> entities, final BatchType type)
            throws IllegalArgumentException, RuntimeException, IllegalStateException {
        N.checkArgument(N.notEmpty(entities), "'entities' can't be null or empty.");
        N.checkElementNotNull(entities);

        final List<SP> statements = new ArrayList<>(entities.size());

        for (final Object entity : entities) {
            statements.add(prepareInsert(entity));
        }

        final BatchStatementBuilder stmt = BatchStatement.builder(type == null ? BatchType.LOGGED : type);

        for (final SP cp : statements) {
            stmt.addStatement(prepareStatement(cp.query(), cp.parameters().toArray()));
        }

        return configStatement(stmt.build());
    }

    /**
     * Builds a {@link BatchStatement} containing one {@code INSERT} per property map.
     *
     * <p>Each element of {@code propsList} (a property-name &rarr; value map) is converted
     * to a parameterized INSERT for {@code targetClass} via {@code prepareInsert(targetClass, props)}
     * and added to a new batch of the given {@code type}.</p>
     *
     * @param targetClass the entity class whose table/columns are targeted
     * @param propsList property-name &rarr; value maps, one per INSERT;
     *        must be non-null, non-empty, and contain no {@code null} elements
     * @param type the batch type; {@code null} is treated as {@link BatchType#LOGGED}
     * @return a populated {@link BatchStatement} in input iteration order, with this executor's {@link StatementSettings}
     *         (the driver ignores the timeout of a batch; see the class documentation)
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}, {@code propsList} is null, empty, or
     *         contains a {@code null} element, or a map in {@code propsList} is empty
     * @throws RuntimeException if the driver fails to prepare a generated statement or rejects a bound value
     * @throws IllegalStateException if more than 65,535 statements are added, the driver's batch-size limit
     */
    @Override
    protected BatchStatement prepareBatchInsertStatement(final Class<?> targetClass, final Collection<? extends Map<String, Object>> propsList,
            final BatchType type) throws IllegalArgumentException, RuntimeException, IllegalStateException {
        N.checkArgNotNull(targetClass, cs.targetClass);
        N.checkArgument(N.notEmpty(propsList), "'propsList' can't be null or empty.");
        N.checkElementNotNull(propsList);

        final List<SP> statements = new ArrayList<>(propsList.size());

        for (final Map<String, Object> props : propsList) {
            statements.add(prepareInsert(targetClass, props));
        }

        final BatchStatementBuilder stmt = BatchStatement.builder(type == null ? BatchType.LOGGED : type);

        for (final SP cp : statements) {
            stmt.addStatement(prepareStatement(cp.query(), cp.parameters().toArray()));
        }

        return configStatement(stmt.build());
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
     * @param type the batch type; {@code null} is treated as {@link BatchType#LOGGED}
     * @return a populated {@link BatchStatement} in input iteration order, with this executor's {@link StatementSettings}
     *         (the driver ignores the timeout of a batch; see the class documentation)
     * @throws IllegalArgumentException if {@code entities} is null or empty or contains a {@code null} element,
     *         if {@code propNamesToUpdate} is null or empty, if an entity's class is not a bean class, if
     *         {@code propNamesToUpdate} contains a key property, if an entity has no declared key or a null/empty
     *         key value, or if a name in {@code propNamesToUpdate} is not a property of an entity's class
     * @throws RuntimeException if the driver fails to prepare a generated statement or rejects a bound value
     * @throws IllegalStateException if more than 65,535 statements are added, the driver's batch-size limit
     */
    @Override
    protected BatchStatement prepareBatchUpdateStatement(final Collection<?> entities, final Collection<String> propNamesToUpdate, final BatchType type)
            throws IllegalArgumentException, RuntimeException, IllegalStateException {
        N.checkArgument(N.notEmpty(entities), "'entities' can't be null or empty.");
        N.checkElementNotNull(entities);
        N.checkArgument(N.notEmpty(propNamesToUpdate), "'propNamesToUpdate' can't be null or empty");

        final List<SP> statements = new ArrayList<>(entities.size());

        for (final Object entity : entities) {
            statements.add(prepareUpdate(entity, propNamesToUpdate));
        }

        final BatchStatementBuilder stmt = BatchStatement.builder(type == null ? BatchType.LOGGED : type);

        for (final SP cp : statements) {
            stmt.addStatement(prepareStatement(cp.query(), cp.parameters().toArray()));
        }

        return configStatement(stmt.build());
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
     * @param type the batch type; {@code null} is treated as {@link BatchType#LOGGED}
     * @return a populated {@link BatchStatement} in input iteration order, with this executor's {@link StatementSettings}
     *         (the driver ignores the timeout of a batch; see the class documentation)
     * @throws IllegalArgumentException if {@code targetClass} is {@code null}; {@code propsList} is null, empty, or
     *         contains a {@code null} element; {@code targetClass} declares no key; or a map lacks a key value, has a
     *         null/empty key value, or has no non-key property
     * @throws RuntimeException if the driver fails to prepare a generated statement or rejects a bound value
     * @throws IllegalStateException if more than 65,535 statements are added, the driver's batch-size limit
     */
    @Override
    protected BatchStatement prepareBatchUpdateStatement(final Class<?> targetClass, final Collection<? extends Map<String, Object>> propsList,
            final BatchType type) throws IllegalArgumentException, RuntimeException, IllegalStateException {
        N.checkArgNotNull(targetClass, cs.targetClass);
        N.checkArgument(N.notEmpty(propsList), "'propsList' can't be null or empty.");
        N.checkElementNotNull(propsList);

        final List<SP> statements = new ArrayList<>(propsList.size());

        for (final Map<String, Object> props : propsList) {
            statements.add(prepareBatchMapUpdate(targetClass, props));
        }

        final BatchStatementBuilder stmt = BatchStatement.builder(type == null ? BatchType.LOGGED : type);

        for (final SP cp : statements) {
            stmt.addStatement(prepareStatement(cp.query(), cp.parameters().toArray()));
        }

        return configStatement(stmt.build());
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
     * @param type the batch type; {@code null} is treated as {@link BatchType#LOGGED}
     * @return a populated {@link BatchStatement} in input iteration order, with this executor's {@link StatementSettings}
     *         (the driver ignores the timeout of a batch; see the class documentation)
     * @throws IllegalArgumentException if {@code query} is {@code null}; {@code parametersList} is null, empty, or
     *         contains a {@code null} element; the CQL contains malformed or mixed parameter markers; or an element's
     *         values do not match the prepared statement's parameter count or names
     * @throws RuntimeException if the driver fails to prepare a generated statement or rejects a bound value
     * @throws IllegalStateException if more than 65,535 statements are added, the driver's batch-size limit
     */
    @Override
    protected Statement<?> prepareBatchUpdateStatement(final String query, final Collection<?> parametersList, final BatchType type)
            throws IllegalArgumentException, RuntimeException, IllegalStateException {
        N.checkArgNotNull(query, cs.query);
        N.checkArgument(N.notEmpty(parametersList), "'parametersList' can't be null or empty.");
        N.checkElementNotNull(parametersList);

        final BatchStatementBuilder stmt = BatchStatement.builder(type == null ? BatchType.LOGGED : type);

        for (final Object params : parametersList) {
            stmt.addStatement(prepareStatement(query, params));
        }

        return configStatement(stmt.build());
    }

    /**
     * Creates a new, empty {@link BatchStatement} of the given type and applies this
     * executor's {@link StatementSettings} via {@link #configStatement(Statement)}.
     *
     * <p>The batch helpers above do not use this method: they accumulate statements with
     * {@link BatchStatement#builder(BatchType)} and configure the built batch. It is kept to fulfill the
     * {@code CassandraExecutorBase} contract.</p>
     *
     * @param type the batch type ({@link BatchType#LOGGED}, {@code UNLOGGED}, or {@code COUNTER});
     *        {@code null} is treated as {@code LOGGED}
     * @return a configured, empty {@link BatchStatement}
     */
    @Override
    protected BatchStatement prepareBatchStatement(final BatchType type) {
        final BatchStatement stmt = BatchStatement.newInstance(type == null ? BatchType.LOGGED : type);

        return configStatement(stmt);
    }

    /**
     * Creates a fresh bound statement for a parameterless query. Only the immutable prepared form is cached,
     * keyed by the resolved CQL text rather than a possible {@link CqlMapper} identifier.
     *
     * @param query the CQL text or mapper identifier
     * @return a newly bound, configured statement
     * @throws IllegalArgumentException if {@code query} is {@code null}, an unmapped query contains malformed or mixed
     *         parameter markers, or the resolved query contains bind markers
     * @throws RuntimeException if the driver fails to prepare the resolved CQL or the session is closed
     */
    @Override
    protected BoundStatement prepareStatement(final String query) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(query, cs.query);

        final ParsedCql parseCql = parseCql(query);
        final String cql = parseCql.parameterizedCql();
        final int parameterCount = parseCql.parameterCount();

        if (parameterCount > 0) {
            throw new IllegalArgumentException("No parameters supplied for parameterized query: expected " + parameterCount + " for query: " + query);
        }

        return bind(getOrPrepareStatement(cql));
    }

    /**
     * Resolves, prepares, and binds a parameterized query. Prepared statements are cached by resolved CQL text;
     * positional arrays and collections must contain exactly one value per driver bind marker.
     *
     * <p>A codec registered for a scalar or bean value takes precedence over expanding bean properties or
     * coercing it to the column's default Java type. Positional collections and named maps retain their
     * usual parameter-container behavior; in particular, a Map keyed by the name of a query's single named
     * marker is always read as the named-parameter container, even when that marker's column is itself a map
     * type. A bean's properties are matched to parameter names by property name (naming-policy variants
     * included) or by the column name declared with {@code @Column}. A single value of a type that abacus handles as a
     * value although it has getters and setters (e.g. a {@code Calendar} or {@code ByteBuffer}) is one positional value,
     * not a bean. A single empty map, collection, or array supplies no values, so it is accepted for a query without
     * bind markers.</p>
     *
     * <p>A value the column's codec does not accept is converted to the column's Java type; one the conversion rejects is bound
     * unchanged, and the driver then fails with a {@link CodecNotFoundException}. A conversion that would change a number is
     * rejected (see <i>Number and Blob Conversion</i> in the class documentation): a number with a fractional part (such as a
     * {@code Double}, {@code Float}, {@code BigDecimal} or {@code Fraction}) for an integer column, and a finite number beyond the
     * range of a {@code float} or {@code double} column. A {@code String} for a {@code blob} column is decoded as base64.</p>
     *
     * @param query the CQL text or mapper identifier
     * @param parameters positional values, a single positional array/collection, or a named map/bean
     * @return a newly bound, configured statement
     * @throws IllegalArgumentException if {@code query} is {@code null}, an unmapped query contains malformed or mixed
     *         parameter markers, named values cannot be resolved, the positional value count does not match, a value's
     *         conversion to its column's type would change the number, or text for a {@code blob} column is not valid base64
     * @throws ArithmeticException if a value is out of the range of its integer column, or is {@code NaN} or infinite for it
     * @throws RuntimeException if the driver fails to prepare the resolved CQL or the session is closed, a bean property
     *         getter fails, or the driver rejects a bound value
     */
    @Override
    protected BoundStatement prepareStatement(final String query, final Object... parameters) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(query, cs.query);

        if (N.isEmpty(parameters)) {
            return prepareStatement(query);
        }

        final ParsedCql parseCql = parseCql(query);
        final String cql = parseCql.parameterizedCql();

        final PreparedStatement preStmt = getOrPrepareStatement(cql);

        final ColumnDefinitions columnDefinitions = preStmt.getVariableDefinitions();
        final int parameterCount = columnDefinitions.size();
        DataType colType = null;
        Class<?> javaClass = null;

        if (parameterCount == 0) {
            // A single empty Map/Collection/array supplies zero parameters (e.g. execute(query, emptyMap)): it must not be
            // counted as one value.
            if (parameters.length == 1 && (parameters[0] instanceof final Map<?, ?> m && m.isEmpty()
                    || parameters[0] instanceof final Collection<?> c && c.isEmpty() || parameters[0] instanceof final Object[] a && a.length == 0)) {
                return bind(preStmt);
            }

            throw new IllegalArgumentException("Too many parameters for parameterless query: expected 0 but got " + parameters.length + " for query: " + query);
        } else if (N.isEmpty(parameters)) {
            throw new IllegalArgumentException("Null or empty parameters for parameterized query: " + query);
        }

        final Map<Integer, String> namedParameters = parseCql.namedParameters();

        // A Map keyed by the name of the query's single named marker (":name"/"#{name}") is the named-parameter container,
        // not the value: skip the single-value shortcut, which would otherwise bind the whole Map when the marker's column
        // is a map type (or has no mapped Java type) because a Map is then assignable to / accepted for that column.
        if (parameterCount == 1 && parameters.length == 1
                && !(parameters[0] instanceof final Map<?, ?> m && N.notEmpty(namedParameters) && containsKeySafely(m, namedParameters.get(0)))) {
            colType = columnDefinitions.get(0).getType();
            javaClass = protocolCodeDataType.get(colType.getProtocolCode());

            if (parameters[0] == null
                    || (javaClass == null || javaClass.isAssignableFrom(parameters[0].getClass()) || acceptsParameter(colType, parameters[0]))) {
                return bind(preStmt, parameters);
            } else if (parameters[0] instanceof List && ((List<Object>) parameters[0]).size() == 1) {
                final Object tmp = ((List<Object>) parameters[0]).get(0);

                if (tmp == null || (javaClass.isAssignableFrom(tmp.getClass()) || acceptsParameter(colType, tmp))) {
                    return bind(preStmt, tmp);
                }
            }
        }

        Object[] values = parameters;

        if (parameters.length == 1 && parameters[0] != null && (parameters[0] instanceof Map || isBeanParameter(parameters[0]))) {
            values = new Object[parameterCount];
            final Object parameter_0 = parameters[0];
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
                Map<String, String> columnToPropNameMap = null;
                Method propGetMethod = null;

                for (int i = 0; i < parameterCount; i++) {
                    parameterName = isCassandraNamedParameters ? columnDefinitions.get(i).getName().asInternal() : namedParameters.get(i);

                    if (parameterName == null) {
                        throw new IllegalArgumentException("Parameter name at index " + i + " is null");
                    }

                    propGetMethod = Beans.getPropGetter(clazz, parameterName);

                    if (propGetMethod == null) {
                        if (columnToPropNameMap == null) {
                            columnToPropNameMap = QueryUtil.columnToPropNameMap(clazz);
                        }

                        final String propName = columnToPropNameMap.get(parameterName);
                        propGetMethod = propName == null ? null : Beans.getPropGetter(clazz, propName);
                    }

                    if (propGetMethod == null) {
                        throw new IllegalArgumentException("Missing required parameter: '" + parameterName + "'");
                    }

                    values[i] = ClassUtil.invokeMethod(entity, propGetMethod);
                }
            }
        } else if ((parameters.length == 1) && (parameters[0] != null)) {
            if (parameters[0] instanceof Object[]) {
                values = (Object[]) parameters[0];
            } else if (parameters[0] instanceof final Collection<?> c) {
                values = c.toArray(new Object[0]);
            }
        }

        if (values.length != parameterCount) {
            throw new IllegalArgumentException((values.length < parameterCount ? "Not enough" : "Too many") + " parameters for parameterized query: expected "
                    + parameterCount + " but got " + values.length + " for query: " + query);
        }

        // Defensive copy: 'values' may alias the caller's own array (the varargs array itself, or an
        // Object[] passed as the single parameter); the conversion loop below must not mutate caller data.
        if (values == parameters || (parameters.length == 1 && parameters[0] == values)) {
            values = values.clone();
        }

        for (int i = 0; i < parameterCount; i++) {
            colType = columnDefinitions.get(i).getType();
            javaClass = protocolCodeDataType.get(colType.getProtocolCode());

            if (values[i] == null) {
                // Keep explicit nulls as null. The driver will bind or reject them according to the column type.
            } else if (javaClass == null || javaClass.isAssignableFrom(values[i].getClass()) || acceptsParameter(colType, values[i])) {
                // continue;
            } else {
                // A value the conversion rejects is kept (the driver reports it); a lossy number conversion or invalid base64 text
                // for a blob column throws IllegalArgumentException naming the parameter.
                values[i] = toBindValue(values[i], javaClass, colType, columnDefinitions, namedParameters, i);
            }
        }

        return bind(preStmt, values);
    }

    /**
     * Returns whether a single parameter is a bean whose properties supply named values, rather than one positional value.
     */
    private static boolean isBeanParameter(final Object parameter) {
        final Class<?> cls = parameter.getClass();

        // Beans.isBeanClass is also true for value types that merely have getters and setters, e.g. GregorianCalendar
        // (Calendar.getInstance()) or ByteBuffer; abacus's Type for those is not a bean type, and they must be bound
        // (converted) as a value, like they are among two or more positional parameters.
        return Beans.isBeanClass(cls) && N.typeOf(cls).isBean();
    }

    /**
     * Returns whether {@code map} contains the String key {@code name}, treating a map that cannot hold String keys as
     * not containing it.
     */
    private static boolean containsKeySafely(final Map<?, ?> map, final String name) {
        try {
            return map.containsKey(name);
        } catch (final ClassCastException | NullPointerException e) {
            // Map.containsKey may throw for a key of an inappropriate type: a sorted map with non-String keys (for example
            // a TreeMap<Integer, String> bound as the value of a map<int, text> column) compares the String name against
            // its own keys. Such a map cannot be a name -> value container, so it is the column value itself.
            return false;
        }
    }

    /**
     * Returns whether the default codec, or a registered scalar/bean codec, accepts a non-null parameter without conversion.
     *
     * @throws RuntimeException if the default codec cannot be resolved, or codec lookup or an acceptance callback fails
     *         other than because no value-specific codec is registered
     */
    private boolean acceptsParameter(final DataType colType, final Object value) throws RuntimeException {
        if (codecRegistry.codecFor(colType).accepts(value)) {
            return true;
        }

        // Containers can represent positional or named parameter bundles; do not infer a scalar codec for them.
        if (value instanceof Collection || value instanceof Map || value.getClass().isArray()) {
            return false;
        }

        try {
            return codecRegistry.codecFor(colType, value) != null;
        } catch (final CodecNotFoundException e) {
            return false;
        }
    }

    /**
     * Returns the cached prepared statement for {@code cql}, preparing (and caching) it on a miss. A closed cache is
     * treated as a miss: the statement is prepared without being cached. Any other {@link IllegalStateException} raised by
     * the cache propagates.
     *
     * @throws RuntimeException if the driver cannot prepare the CQL or the session is closed
     * @throws IllegalStateException if the statement cache fails for a reason other than being closed
     */
    private PreparedStatement getOrPrepareStatement(final String cql) throws RuntimeException {
        // The cache is only an optimization. The pool also closes itself from its own JVM shutdown hook, after which get/put
        // throw IllegalStateException; CQL executed by an application's shutdown hook must still run (uncached) then.
        // WHY: only a CLOSED cache is a miss. Swallowing every IllegalStateException hid any other cache failure (the driver 3
        // executor's getCachedStatement/cacheStatement rethrow unless the pool is closed); the two executors now agree.
        final boolean cacheable = cql.length() <= POOLABLE_LENGTH;
        PreparedStatement preStmt = null;

        if (cacheable && !preparedStatementPool.isClosed()) {
            try {
                final PoolableAdapter<PreparedStatement> wrapper = preparedStatementPool.get(cql);

                if (wrapper != null) {
                    preStmt = wrapper.value();
                }
            } catch (final IllegalStateException e) {
                if (!preparedStatementPool.isClosed()) {
                    throw e;
                }

                // Closed concurrently: a cache miss.
            }
        }

        if (preStmt == null) {
            preStmt = prepare(cql);

            if (cacheable && !preparedStatementPool.isClosed()) {
                try {
                    preparedStatementPool.put(cql, Poolable.wrap(preStmt));
                } catch (final IllegalStateException e) {
                    if (!preparedStatementPool.isClosed()) {
                        throw e;
                    }

                    // Closed concurrently: use the statement uncached.
                }
            }
        }

        return preStmt;
    }

    /**
     * @throws IllegalArgumentException if {@code query} is null
     * @throws RuntimeException if the session is closed or the driver cannot prepare {@code query}
     */
    @Override
    protected PreparedStatement prepare(final String query) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(query, cs.query);

        if (logger.isDebugEnabled()) {
            logger.debug("Preparing CQL: {}", query);
        }

        return session.prepare(query);
    }

    /**
     * @throws IllegalArgumentException if {@code preStmt} is null
     * @throws RuntimeException if a value cannot be encoded for its CQL type or configured statement settings are rejected
     */
    @Override
    protected BoundStatement bind(final PreparedStatement preStmt, final Object... parameters) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(preStmt, cs.preStmt);

        final BoundStatement stmt = preStmt.bind(parameters);

        return configStatement(stmt);
    }

    /**
     * Applies this executor's {@code StatementSettings} to the supplied statement and returns the
     * configured statement.
     *
     * <p>Only settings that were explicitly supplied are applied; each unset setting leaves the
     * driver default in place. Because driver 4.x statements are immutable, every {@code setXxx}
     * returns a new instance — hence the result must be used in place of the argument rather than
     * relying on mutation.</p>
     *
     * @param <T> the concrete driver statement type
     * @param stmt the statement to configure; may be null when no setting is applied
     * @return the configured statement, which may be a different instance than {@code stmt}
     * @throws IllegalArgumentException if {@code stmt} is null and at least one setting is configured
     */
    protected <T extends Statement<T>> T configStatement(T stmt) throws IllegalArgumentException {
        if (settings != null) {
            if (settings.consistency() != null || settings.serialConsistency() != null || settings.fetchSize() != null || settings.timeout() != null
                    || settings.traceQuery() != null) {
                N.checkArgNotNull(stmt, cs.stmt);
            }

            if (settings.consistency() != null) {
                stmt = stmt.setConsistencyLevel(settings.consistency());
            }

            if (settings.serialConsistency() != null) {
                stmt = stmt.setSerialConsistencyLevel(settings.serialConsistency());
            }

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

    /**
     * @throws IllegalArgumentException if {@code targetClass} or {@code rs} is null, or a single-value target does not match the result column
     *         count
     * @throws RuntimeException if fetching rows, converting columns, or assigning bean properties fails
     */
    @Override
    protected <T> List<T> toList(final Class<T> targetClass, final ResultSet rs) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(targetClass, cs.targetClass);

        return toList(rs, targetClass);
    }

    /**
     * @throws IllegalArgumentException if {@code rs} is null
     * @throws RuntimeException if fetching rows or converting a column to a target property type fails
     */
    @Override
    protected Dataset extractData(final Class<?> targetClass, final ResultSet rs) throws IllegalArgumentException, RuntimeException {
        return extractData(rs, targetClass);
    }

    @Override
    protected <T> Function<Row, T> createRowMapper(final Class<T> rowClass) {
        return new Function<>() {
            private Function<Row, T> mapper;

            /**
             * @throws RuntimeException if the row does not match the target type, a column cannot be read or converted, or a mapped bean cannot
             *         be populated
             */
            @Override
            public T apply(final Row row) throws RuntimeException {
                if (mapper == null) {
                    mapper = createRowMapper(rowClass, row.getColumnDefinitions());
                }

                return mapper.apply(row);
            }
        };
    }

    /**
     * Adapts a {@code (ColumnDefinitions, Row)} mapper to a plain {@code Function<Row, T>} by
     * caching the column definitions from the first row it sees and reusing them for every
     * subsequent row.
     *
     * <p>The returned function is stateful and therefore <b>not</b> thread-safe; create one per
     * result-set traversal. It is safe to reuse across the rows of a single result set because all
     * rows of one result set share the same column definitions.</p>
     *
     * @param <T> the mapped result type
     * @param rowMapper the mapper invoked with the result set's column definitions and each row
     * @return a row-only function that supplies the column definitions on the mapper's behalf
     */
    protected <T> Function<Row, T> createRowMapper(final BiFunction<ColumnDefinitions, Row, T> rowMapper) {
        return new Function<>() {
            private ColumnDefinitions cds = null;

            /**
             * @throws RuntimeException if reading the row metadata or invoking the supplied row mapper fails
             */
            @Override
            public T apply(final Row row) throws RuntimeException {
                if (cds == null) {
                    cds = row.getColumnDefinitions();
                }

                return rowMapper.apply(cds, row);
            }
        };
    }

    /**
     * @return the converted first column; a {@code NULL} column becomes the default value of {@code targetClass}: the
     *         primitive default, an empty value for Optional/Nullable/OptionalInt-like types, or {@code null} for a
     *         wrapper or other reference type
     * @throws IllegalArgumentException if {@code row} or {@code targetClass} is null
     * @throws RuntimeException if the first column cannot be read or converted to {@code targetClass}
     */
    @Override
    protected <T> T readFirstColumn(final Row row, final Class<T> targetClass) throws IllegalArgumentException, RuntimeException {
        N.checkArgNotNull(row, cs.row);
        N.checkArgNotNull(targetClass, cs.targetClass);

        return decodeValue(row.getObject(0), targetClass);
    }

    /**
     * Reads the only row and maps it to the requested Java type.
     *
     * @param targetClass the Java type for the row
     * @param resultSet the rows to consume
     * @return the mapped row, or {@code null} when there are no rows; for a single-value {@code targetClass}, a
     *         {@code NULL} column yields the default value of {@code targetClass} (the primitive default for a primitive class
     *         token, an empty value for Optional/Nullable/OptionalInt-like types, or {@code null} for a wrapper or other
     *         reference type)
     * @throws IllegalArgumentException if {@code resultSet} is null
     * @throws RuntimeException if fetching a row fails or its columns cannot be mapped to {@code targetClass}
     * @throws DuplicateResultException if {@code resultSet} contains more than one row
     */
    @Override
    protected <T> T fetchOnlyOne(final Class<T> targetClass, final ResultSet resultSet)
            throws IllegalArgumentException, RuntimeException, DuplicateResultException {
        N.checkArgNotNull(resultSet, cs.resultSet);

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
     * Sets one UDT field, with the same fallbacks as ordinary binding when no registered codec accepts the value: a {@code byte[]}
     * or base64 {@code String} for a BLOB field, a {@link Date}/{@link Calendar} for a TIME field, and for a TIMESTAMP field any value that
     * {@link #convertValue(Object, Class)} turns into an {@link Instant}: a {@link java.time.LocalDateTime} (in the JVM default
     * time zone), a {@link LocalDate} (the start of that day in the JVM default time zone), a {@link java.time.ZonedDateTime} or
     * {@link java.time.OffsetDateTime}, and also a {@code Date},
     * {@code java.sql.Timestamp}, {@code Calendar} or {@code Long} (epoch milliseconds), as the same value bound to a
     * {@code timestamp} column is converted. A value the conversion rejects fails with the driver's original
     * {@link CodecNotFoundException}.
     *
     * @throws IllegalArgumentException if a {@code String} for a BLOB field is not valid base64
     * @throws RuntimeException if no codec supports the field value, the value cannot be encoded, or the field index is invalid
     */
    private static void setUdtField(final UserDefinedType cqlType, final UdtValue udtValue, final int index, final Object value)
            throws IllegalArgumentException, RuntimeException {
        if (value == null) {
            udtValue.setToNull(index);
        } else {
            final TypeCodec<Object> fieldCodec;

            try {
                fieldCodec = cqlType.getAttachmentPoint().getCodecRegistry().codecFor(cqlType.getFieldTypes().get(index), value);
            } catch (final CodecNotFoundException e) {
                if (value instanceof final byte[] bytes && cqlType.getFieldTypes().get(index).getProtocolCode() == ProtocolConstants.DataType.BLOB) {
                    // Preserve any registered byte[] codec; otherwise use the driver's native BLOB representation.
                    udtValue.setByteBuffer(index, ByteBuffer.wrap(bytes));
                    return;
                } else if (value instanceof final String text && cqlType.getFieldTypes().get(index).getProtocolCode() == ProtocolConstants.DataType.BLOB) {
                    // WHY: the same rule as a String bound to a blob column (toBindValue): the text is base64, decoded into a buffer
                    // positioned at 0, and text that is not base64 is rejected with a message naming the field (this failed with
                    // CodecNotFoundException, while the column path bound an empty blob).
                    final byte[] bytes;

                    try {
                        bytes = Strings.base64Decode(text);
                    } catch (final IllegalArgumentException decodeFailure) {
                        throw new IllegalArgumentException("Cannot set text to UDT field '" + cqlType.getFieldNames().get(index).asInternal() + "' (index "
                                + index + ") of CQL type blob: the text is not valid base64 (" + decodeFailure.getMessage() + ")", decodeFailure);
                    }

                    udtValue.setByteBuffer(index, ByteBuffer.wrap(bytes));
                    return;
                } else if ((value instanceof Date || value instanceof Calendar)
                        && cqlType.getFieldTypes().get(index).getProtocolCode() == ProtocolConstants.DataType.TIME) {
                    // Match ordinary TIME binding for legacy temporal values after registered codec lookup fails.
                    // Serialization stays outside this catch so a registered codec's failure is never replaced by a fallback.
                    udtValue.setLocalTime(index, convertValue(value, LocalTime.class));
                    return;
                } else if (cqlType.getFieldTypes().get(index).getProtocolCode() == ProtocolConstants.DataType.TIMESTAMP) {
                    // WHY: match ordinary TIMESTAMP binding (convertValue) for every value the driver has no codec for: the java.time
                    // date-time types, and also Date, java.sql.Timestamp, Calendar and Long (epoch milliseconds), which bound to a
                    // timestamp column convert but as a UDT field failed with CodecNotFoundException. A value the conversion rejects
                    // keeps the original lookup failure (the conversion failure is suppressed) rather than a parse error.
                    final Instant instant;

                    try {
                        instant = convertValue(value, Instant.class);
                    } catch (final ClassCastException | IllegalArgumentException | java.time.DateTimeException conversionFailure) {
                        e.addSuppressed(conversionFailure);
                        throw e;
                    }

                    udtValue.setInstant(index, instant);
                    return;
                }

                throw e;
            }

            udtValue.set(index, value, fieldCodec);
        }
    }

    /**
     * Abstract base class for creating custom User Defined Type (UDT) codecs.
     *
     * <p>This class provides a framework for encoding and decoding Cassandra UDTs to/from Java objects.
     * Subclasses must implement the abstract conversion methods between {@link UdtValue} and the
     * target Java type {@code T}: {@link #serialize(Object)} (Java &rarr; UDT) and
     * {@link #deserialize(UdtValue)} (UDT &rarr; Java). The driver-facing {@link #encode} and
     * {@link #decode} methods delegate to these abstract methods via the underlying
     * {@link TypeCodecs#udtOf udt-of} codec.</p>
     *
     * <p>Note that the positional collection mapping described on {@link #create(UserDefinedType, Class)}
     * is a property of the codec that factory returns; a hand-written subclass supplies its own
     * {@code serialize} and is not bound by it. A raw {@link UdtValue} passed to {@link #encode} or {@link #format} is handed
     * to the driver's own UDT codec unchanged, which bypasses a subclass's {@code serialize(T)} when {@code T} is a supertype of
     * the driver value (for example a {@code UDTCodec<Object>}).</p>
     *
     * @param <T> the Java type to encode/decode
     */
    public abstract static class UDTCodec<T> implements TypeCodec<T> {

        private final UserDefinedType cqlType;
        private final GenericType<T> javaType;
        private final TypeCodec<UdtValue> udtValueTypeCodec;

        // The property bound to each UDT field (null where none), resolved against one BeanInfo instance.
        private record UdtBeanFields(BeanInfo beanInfo, PropInfo[] propInfos) {
        }

        /**
         * Constructs a {@code UDTCodec} bound to the given UDT definition and target Java class.
         *
         * @param cqlType the Cassandra User Defined Type this codec serializes against
         * @param javaClazz the Java class this codec marshals to/from {@code cqlType}
         * @throws IllegalArgumentException if {@code javaClazz} is {@code null}
         */
        protected UDTCodec(final UserDefinedType cqlType, final Class<T> javaClazz) throws IllegalArgumentException {
            N.checkArgNotNull(javaClazz, cs.javaClazz);

            this.cqlType = cqlType;
            javaType = GenericType.of(javaClazz);
            udtValueTypeCodec = TypeCodecs.udtOf(cqlType);
        }

        /**
         * Creates a new instance of a UDTCodec for the specified User Defined Type (UDT) and Java class.
         *
         * <p>This method generates a custom codec for mapping a Cassandra UDT to a Java class.
         * The codec handles serialization and deserialization between the UDT and the Java object.
         * It is particularly useful for working with complex UDTs in Cassandra.</p>
         *
         * <p>When {@code javaClazz} is a {@link Collection} type, values are mapped positionally in UDT
         * field order. A collection may omit trailing fields, which remain {@code null}; a collection with
         * more elements than the UDT has fields is rejected.</p>
         *
         * <p>When {@code javaClazz} is a {@link Map} type, a value is written from the entries keyed by the exact (internal)
         * field names, a missing field being written as {@code null} and other entries ignored, and decodes to a map keyed
         * by the field names. When it is a bean class, each field is bound to the property of that name (matched
         * case-insensitively or in its snake_case form, as {@link CassandraExecutor#toEntity(Row, Class)} matches a column)
         * or to the property declaring it as its {@code @Column} name. A field without a matching property is written as
         * {@code null} and ignored when decoding; a read-only (for example getter-only) property is written from its getter
         * but ignored when decoding. A field value decoded into a bean property is converted as
         * {@link CassandraExecutor#toEntity(Row, Class)} converts a column value, including the elements of a collection or map
         * field read into a parameterized collection, map or object-array property.</p>
         *
         * <p>A {@code byte[]} field value is accepted for a CQL {@code blob}. A registered codec for that
         * Java type takes precedence; otherwise the bytes are wrapped in a {@link ByteBuffer} for the
         * driver's standard BLOB codec. A {@code String} field value for a {@code blob} is likewise accepted as base64 text,
         * as a {@code String} bound to a {@code blob} column is: it is decoded into the field's bytes, and text that is not valid
         * base64 is rejected with an {@link IllegalArgumentException}. Blob fields decoded into {@code byte[]} bean properties are
         * copied from the buffer's remaining bytes, and those decoded into {@code String} bean properties are the base64 text of
         * those bytes.</p>
         *
         * <p>A CQL {@code time} field accepts the driver's native {@link LocalTime}, as well as {@link Date}
         * (including {@code java.sql.Time} and {@code java.sql.Timestamp}) and {@link Calendar} values.
         * Registered codecs take precedence, and failures from their serializers propagate unchanged. If no
         * registered codec accepts a {@code Date} or {@code Calendar} value, it is converted to {@code LocalTime}
         * the same way ordinary parameter binding converts it. Unlike ordinary binding, ISO time strings and
         * epoch-millisecond numbers are rejected unless a registered codec accepts them. Dates use the JVM default
         * zone captured when Abacus's {@link com.landawn.abacus.type.AbstractTemporalType} is initialized;
         * calendars use their own zone. Milliseconds are preserved, and {@code Timestamp} values retain
         * nanoseconds. Maps and collections decode TIME fields to {@code LocalTime}. Bean properties of {@code Date}
         * (including {@code java.sql.Time} and {@code java.sql.Timestamp}), {@code Calendar}, and the zoned or local
         * {@code java.time} date-time types receive that time of day on 1970-01-01 in the current JVM default zone. A
         * {@code Date} or {@code Calendar} written from such a property therefore reads back with the same local time of day
         * while the JVM default zone is unchanged; the date part is not stored. The {@code java.time} date-time types are not
         * accepted when writing a TIME field unless a registered codec accepts them. {@code java.sql.Time} properties retain
         * millisecond precision and {@code Timestamp} properties nanoseconds.</p>
         *
         * <p>A CQL {@code timestamp} field accepts the driver's native {@link Instant}. If no registered codec accepts another
         * value, it is converted to an {@code Instant} as a value bound to a {@code timestamp} column is: a
         * {@code LocalDateTime} or {@code LocalDate} in the JVM default zone (a {@code LocalDate} as the start of that day), a
         * {@code ZonedDateTime} or {@code OffsetDateTime} as the instant it denotes, a {@link Date} (including
         * {@code java.sql.Timestamp}) or {@link Calendar} as its instant, a {@code Long} as epoch milliseconds, and ISO-8601
         * instant text; a value that conversion rejects fails with the driver's {@link CodecNotFoundException}. Maps and
         * collections decode TIMESTAMP fields to {@code Instant}; bean properties receive the instant converted as a
         * {@code timestamp} column is read (a {@code java.time} property in the JVM default zone).</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * UserDefinedType userType = session.getMetadata()
         *     .getKeyspace("my_keyspace")
         *     .flatMap(ks -> ks.getUserDefinedType("my_udt"))
         *     .orElseThrow(() -> new IllegalArgumentException("UDT not found"));
         *
         * UDTCodec<MyClass> codec = UDTCodec.create(userType, MyClass.class); // returns a codec marshaling MyClass to/from the UDT
         * }</pre>
         *
         * <p>The returned codec rejects unsupported Java types and collections with more elements than UDT
         * fields when encoding or decoding a non-null value.</p>
         *
         * @param <T> the type of the Java class to map the UDT to
         * @param userType the Cassandra User Defined Type to map
         * @param javaClazz the Java class to map the UDT to
         * @return a new instance of {@link UDTCodec} for the specified UDT and Java class
         * @throws IllegalArgumentException if {@code javaClazz} is {@code null}
         */
        public static <T> UDTCodec<T> create(final UserDefinedType userType, final Class<T> javaClazz) throws IllegalArgumentException {
            return new UDTCodec<>(userType, javaClazz) {
                // The UDT definition and Java class are fixed for this codec, so the field names and, for a bean, the
                // property bound to each field are resolved once instead of for every encoded or decoded value. Both are
                // published through immutable objects (a benign race only repeats the identical resolution); the bean
                // resolution is redone if ParserUtil hands out a different (refreshed) BeanInfo.
                private List<String> fieldNames;
                private UdtBeanFields beanFields;

                private List<String> fieldNames() {
                    List<String> result = fieldNames;

                    if (result == null) {
                        result = userType.getFieldNames().stream().map(CqlIdentifier::asInternal).toList();
                        fieldNames = result;
                    }

                    return result;
                }

                private PropInfo[] fieldPropInfos(final BeanInfo beanInfo) {
                    UdtBeanFields result = beanFields;

                    if (result == null || result.beanInfo() != beanInfo) {
                        final List<String> names = fieldNames();
                        final Map<String, String> columnToPropNameMap = QueryUtil.columnToPropNameMap(javaClazz);
                        final PropInfo[] propInfos = new PropInfo[names.size()];
                        PropInfo propInfo = null;
                        String fieldName = null;

                        for (int i = 0; i < propInfos.length; i++) {
                            fieldName = names.get(i);
                            propInfo = beanInfo.getPropInfo(fieldName);

                            if (propInfo == null) {
                                final String propName = columnToPropNameMap.get(fieldName);
                                propInfo = propName == null ? null : beanInfo.getPropInfo(propName);
                            }

                            propInfos[i] = propInfo;
                        }

                        result = new UdtBeanFields(beanInfo, propInfos);
                        beanFields = result;
                    }

                    return result.propInfos();
                }

                /**
                 * @throws IllegalArgumentException if the UDT value is non-null and the configured Java type is neither a collection, map, nor bean
                 * @throws RuntimeException if reading a UDT field, creating the target object, or assigning a bean property fails
                 */
                @Override
                protected T deserialize(final UdtValue udtValue) throws IllegalArgumentException, RuntimeException {
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
                        final List<String> fieldNames = fieldNames();

                        // Fields are read by position: the driver's getObject(String) treats the name as CQL (case-insensitive
                        // unless quoted), so of two fields differing only by case both would read the first one's value.
                        for (int i = 0; i < size; i++) {
                            map.put(fieldNames.get(i), udtValue.getObject(i));
                        }

                        return (T) map;
                    } else if (Beans.isBeanClass(javaClazz)) {
                        final BeanInfo beanInfo = ParserUtil.getBeanInfo(javaClazz);
                        final PropInfo[] propInfos = fieldPropInfos(beanInfo);
                        Object targetBean = beanInfo.createBeanResult();
                        PropInfo propInfo = null;

                        for (int i = 0; i < size; i++) {
                            propInfo = propInfos[i];

                            if (propInfo != null && !isReadOnlyProperty(propInfo)) {
                                // Read by position for the same reason as the Map branch above.
                                propInfo.setPropValue(targetBean, toPropValue(propInfo, udtValue.getObject(i)));
                            }
                        }

                        targetBean = beanInfo.finishBeanResult(targetBean);

                        return (T) targetBean;
                    } else {
                        throw new IllegalArgumentException(
                                "Invalid Java class type: " + ClassUtil.getCanonicalClassName(javaClazz) + ". Expected: Collection, Map, or Bean class");
                    }
                }

                /**
                 * @throws IllegalArgumentException if the non-null value has an unsupported Java type, a collection has more elements than UDT
                 *         fields, or a {@code String} for a {@code blob} field is not valid base64
                 * @throws RuntimeException if reading a bean property, resolving a field codec, or encoding a field value fails
                 */
                @Override
                protected UdtValue serialize(final T value) throws IllegalArgumentException, RuntimeException {
                    if (value == null) {
                        return null;
                    }

                    final UdtValue udtValue = newUDTValue();

                    if (value instanceof Collection) {
                        final Collection<Object> coll = (Collection<Object>) value;

                        if (coll.size() > userType.getFieldNames().size()) {
                            throw new IllegalArgumentException("Collection size cannot exceed UDT field count: expected at most "
                                    + userType.getFieldNames().size() + " but got " + coll.size());
                        }

                        int idx = 0;

                        for (final Object val : coll) {
                            setUdtField(userType, udtValue, idx++, val);
                        }
                    } else if (value instanceof Map) {
                        final Map<String, Object> map = (Map<String, Object>) value;
                        final List<String> fieldNames = fieldNames();
                        int idx = 0;

                        for (final String fieldName : fieldNames) {
                            setUdtField(userType, udtValue, idx++, map.get(fieldName));
                        }
                    } else if (Beans.isBeanClass(javaClazz)) {
                        final BeanInfo beanInfo = ParserUtil.getBeanInfo(javaClazz);
                        //noinspection UnnecessaryLocalVariable
                        final Object bean = value;
                        final PropInfo[] propInfos = fieldPropInfos(beanInfo);
                        PropInfo propInfo = null;

                        for (int idx = 0; idx < propInfos.length; idx++) {
                            propInfo = propInfos[idx];

                            setUdtField(userType, udtValue, idx, propInfo == null ? null : propInfo.getPropValue(bean));
                        }
                    } else {
                        throw new IllegalArgumentException(
                                "Invalid Java class type: " + ClassUtil.getCanonicalClassName(javaClazz) + ". Expected: Collection, Map, or Bean class");
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
         * UDTCodec<MyClass> codec = UDTCodec.create(session, "my_keyspace", "my_udt", MyClass.class); // resolves the UDT via session metadata
         * // throws NoSuchElementException if the keyspace or the UDT does not exist (orElseThrow)
         * }</pre>
         *
         * @param <T> the type of the Java class to map the UDT to
         * @param session the Cassandra Session to use for metadata retrieval
         * @param keySpace the keyspace containing the UDT
         * @param userTypeName the name of the User Defined Type (UDT)
         * @param javaClazz the Java class to map the UDT to
         * @return a new instance of {@link UDTCodec} for the specified UDT and Java class
         * @throws IllegalArgumentException if {@code session}, {@code keySpace}, {@code userTypeName}, or
         *         {@code javaClazz} is {@code null}, or {@code keySpace} or {@code userTypeName} is an unquoted CQL
         *         identifier that requires double quotes
         * @throws NoSuchElementException if {@code keySpace} or the named user type cannot be
         *         found in the session metadata
         */
        public static <T> UDTCodec<T> create(final Session session, final String keySpace, final String userTypeName, final Class<T> javaClazz)
                throws IllegalArgumentException, NoSuchElementException {
            N.checkArgNotNull(session, cs.session);
            N.checkArgNotNull(keySpace, cs.keySpace);
            N.checkArgNotNull(userTypeName, cs.userTypeName);
            N.checkArgNotNull(javaClazz, cs.javaClazz);

            return create(session.getMetadata().getKeyspace(keySpace).orElseThrow().getUserDefinedType(userTypeName).orElseThrow(), javaClazz);
        }

        /**
         * Serializes the given value to a ByteBuffer using the UDT codec.
         *
         * <p>This method is deprecated. Use {@link #encode(Object, ProtocolVersion)} instead; this
         * overload simply forwards to {@code encode}.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * UDTCodec<Address> codec = UDTCodec.create(addressUserType, Address.class);
         *
         * ByteBuffer bytes = codec.serialize(new Address("123 Main St"), ProtocolVersion.V4); // delegates to encode(...)
         *
         * ByteBuffer none = codec.serialize(null, ProtocolVersion.V4);                        // returns null for a null value
         * }</pre>
         *
         * @param value the value to serialize
         * @param protocolVersion the protocol version to use for serialization
         * @return a ByteBuffer containing the serialized value, or {@code null} for a {@code null} value
         * @deprecated Use {@link #encode(Object, ProtocolVersion)} instead.
         * @throws IllegalArgumentException if the non-null value cannot be mapped to this UDT, including an unsupported Java type,
         *         a collection with more elements than UDT fields, or a {@code String} for a {@code blob} field that is not valid base64
         * @throws CodecNotFoundException if neither a registered codec nor a supported BLOB, TIME or TIMESTAMP conversion can map a non-null
         *         field value to its CQL type, or a field codec reports a missing codec while serializing
         * @throws RuntimeException if mapping the non-null value invokes a failing bean accessor, cannot create the target Java object, or a
         *         field codec rejects a value
         */
        @Deprecated
        public ByteBuffer serialize(final T value, final ProtocolVersion protocolVersion)
                throws IllegalArgumentException, CodecNotFoundException, RuntimeException {
            return encode(value, protocolVersion);
        }

        /**
         * Deserializes the given ByteBuffer to a value using the UDT codec.
         *
         * <p>This method is deprecated. Use {@link #decode(ByteBuffer, ProtocolVersion)} instead; this
         * overload simply forwards to {@code decode}.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * UDTCodec<Address> codec = UDTCodec.create(addressUserType, Address.class);
         *
         * ByteBuffer bytes = codec.encode(new Address("123 Main St"), ProtocolVersion.V4);
         * Address address = codec.deserialize(bytes, ProtocolVersion.V4); // delegates to decode(...); reconstructs the Address
         *
         * Address none = codec.deserialize(null, ProtocolVersion.V4);     // returns null for a null payload
         * }</pre>
         *
         * @param bytes the ByteBuffer containing the serialized value
         * @param protocolVersion the protocol version to use for deserialization
         * @return the deserialized value, or {@code null} for a {@code null} payload
         * @deprecated Use {@link #decode(ByteBuffer, ProtocolVersion)} instead.
         * @throws IllegalArgumentException if the input cannot be decoded as this UDT or the non-null UDT cannot be mapped to the configured Java type
         * @throws RuntimeException if mapping the non-null value invokes a failing bean accessor, cannot create the target Java object, or a
         *         field codec rejects a value
         */
        @Deprecated
        public T deserialize(final ByteBuffer bytes, final ProtocolVersion protocolVersion) throws IllegalArgumentException, RuntimeException {
            return decode(bytes, protocolVersion);
        }

        /**
         * Encodes the given value to a ByteBuffer using the UDT codec.
         *
         * <p>This method encodes the value into a ByteBuffer suitable for storage in Cassandra. The Java
         * value is first converted to a {@link UdtValue} via the subclass {@code serialize(T)} and then
         * encoded by the underlying UDT codec. A raw driver {@link UdtValue} is passed to the underlying UDT codec
         * unchanged, without being mapped as a {@code T}. The driver itself never hands one to this codec (a row's field is
         * decoded to {@code T} through this codec, and {@code setUdtValue} resolves a codec by the {@code UdtValue} Java type),
         * but caller code that resolves {@code CodecRegistry.codecFor(DataType)} for this user type itself, which returns
         * this registered codec, and passes it a value read from a row reaches this method with one.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * UDTCodec<Address> codec = UDTCodec.create(addressUserType, Address.class);
         *
         * ByteBuffer bytes = codec.encode(new Address("123 Main St"), ProtocolVersion.V4); // returns the UDT binary form
         *
         * ByteBuffer none = codec.encode(null, ProtocolVersion.V4);                        // returns null for a null value
         * }</pre>
         *
         * @param value the value to encode: a {@code T}, or a raw {@link UdtValue} of this codec's user type passed by caller code
         *        that resolved this codec by CQL type itself
         * @param protocolVersion the protocol version to use for encoding
         * @return a ByteBuffer containing the encoded value, or {@code null} for a {@code null} value
         * @throws IllegalArgumentException if the non-null value cannot be mapped to this UDT, including an unsupported Java type,
         *         a collection with more elements than UDT fields, or a {@code String} for a {@code blob} field that is not valid base64
         * @throws CodecNotFoundException if neither a registered codec nor a supported BLOB, TIME or TIMESTAMP conversion can map a non-null
         *         field value to its CQL type, or a field codec reports a missing codec while serializing
         * @throws RuntimeException if mapping the non-null value invokes a failing bean accessor, cannot create the target Java object, or a
         *         field codec rejects a value
         */
        @Override
        public ByteBuffer encode(final T value, final ProtocolVersion protocolVersion)
                throws IllegalArgumentException, CodecNotFoundException, RuntimeException {
            // WHY: as in format(T), the registry's codecFor(DataType) returns this registered codec for its user type, so caller code
            // that resolves the codec by CQL type itself can hand it a raw UdtValue read from a row (the driver never does: getObject
            // decodes a field to T through this codec, and setUdtValue/getFormattedContents look a codec up by the UdtValue Java type,
            // skipping this one); serialize(value) cast it to T and threw ClassCastException. A driver value already has the UDT
            // shape: the driver's UDT codec encodes it.
            if (value instanceof final UdtValue udtValue) {
                return udtValueTypeCodec.encode(udtValue, protocolVersion);
            }

            return udtValueTypeCodec.encode(serialize(value), protocolVersion);
        }

        /**
         * Decodes the given ByteBuffer to a value using the UDT codec.
         *
         * <p>This method decodes a ByteBuffer containing a serialized value into the corresponding Java
         * object. The bytes are first decoded by the underlying UDT codec into a {@link UdtValue} and then
         * converted to {@code T} via the subclass {@code deserialize(UdtValue)}.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * UDTCodec<Address> codec = UDTCodec.create(addressUserType, Address.class);
         *
         * ByteBuffer bytes = codec.encode(new Address("123 Main St"), ProtocolVersion.V4);
         * Address address = codec.decode(bytes, ProtocolVersion.V4); // returns the reconstructed Address
         *
         * Address none = codec.decode(null, ProtocolVersion.V4);     // returns null for a null payload
         * }</pre>
         *
         * @param bytes the ByteBuffer containing the serialized value
         * @param protocolVersion the protocol version to use for decoding
         * @return the deserialized value, or {@code null} for a {@code null} payload
         * @throws IllegalArgumentException if the input cannot be decoded as this UDT or the non-null UDT cannot be mapped to the configured Java type
         * @throws RuntimeException if mapping the non-null value invokes a failing bean accessor, cannot create the target Java object, or a
         *         field codec rejects a value
         */
        @Override
        public T decode(final ByteBuffer bytes, final ProtocolVersion protocolVersion) throws IllegalArgumentException, RuntimeException {
            return deserialize(udtValueTypeCodec.decode(bytes, protocolVersion));
        }

        /**
         * Parses a CQL UDT literal into a Java object of type {@code T}.
         *
         * <p>The driver UDT codec validates and decodes the literal first; the resulting
         * {@link UdtValue} is then mapped to the configured Java type. Empty or {@code NULL} input
         * returns {@code null}. The accepted representation is a CQL literal, not JSON.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * UDTCodec<Address> codec = UDTCodec.create(addressUserType, Address.class);
         *
         * Address a = codec.parse("{street:'123 Main St'}"); // returns the parsed Address
         *
         * Address empty = codec.parse("");     // returns null (empty input)
         * Address nullS = codec.parse("NULL"); // returns null (the "NULL" sentinel)
         * Address nullV = codec.parse(null);   // returns null (null input)
         * }</pre>
         *
         * @param value the CQL UDT literal to parse
         * @return an instance of type {@code T}, or {@code null} if the input is empty or equal
         *         (case-insensitively) to {@link CassandraExecutorBase#NULL_STR}
         * @throws IllegalArgumentException if the input cannot be decoded as this UDT or the non-null UDT cannot be mapped to the configured Java type
         * @throws RuntimeException if mapping the non-null value invokes a failing bean accessor, cannot create the target Java object, or a
         *         field codec rejects a value
         */
        @Override
        public T parse(final String value) throws IllegalArgumentException, RuntimeException {
            return Strings.isEmpty(value) || NULL_STR.equalsIgnoreCase(value) ? null : deserialize(udtValueTypeCodec.parse(value));
        }

        /**
         * Formats the given value as a CQL UDT literal.
         *
         * <p>The Java value is mapped to a {@link UdtValue}, then formatted by the driver's UDT
         * codec. If the value is null, this method returns {@link CassandraExecutorBase#NULL_STR}.
         * The returned representation is a CQL literal, not JSON. A driver {@link UdtValue}, which reaches this codec
         * when it is looked up by CQL type only ({@code CodecRegistry.codecFor(DataType)}), is formatted as is by the
         * driver's UDT codec.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * UDTCodec<Address> codec = UDTCodec.create(addressUserType, Address.class);
         *
         * String literal = codec.format(new Address("123 Main St")); // e.g. {street:'123 Main St'}
         *
         * String nullStr = codec.format(null);                    // returns "NULL"
         * }</pre>
         *
         * @param value the value to format
         * @return a CQL UDT literal, or {@link CassandraExecutorBase#NULL_STR} if the value is null
         * @throws IllegalArgumentException if the non-null value cannot be mapped to this UDT, including an unsupported Java type,
         *         a collection with more elements than UDT fields, or a {@code String} for a {@code blob} field that is not valid base64
         * @throws CodecNotFoundException if neither a registered codec nor a supported BLOB, TIME or TIMESTAMP conversion can map a non-null
         *         field value to its CQL type, or a field codec reports a missing codec while serializing
         * @throws RuntimeException if mapping the non-null value invokes a failing bean accessor, cannot create the target Java object, or a
         *         field codec rejects a value
         */
        @Override
        public String format(final T value) throws IllegalArgumentException, CodecNotFoundException, RuntimeException {
            if (value == null) {
                return NULL_STR;
            }

            // The registry's codecFor(DataType) returns this registered codec for its user type, so code formatting a value read
            // from a row through that codec passes the raw UdtValue; mapping it as a T failed with ClassCastException (twin of the
            // driver 3 UDTValue.toString() failure). A driver value already has the UDT shape: the driver's UDT codec formats it.
            if (value instanceof final UdtValue udtValue) {
                return udtValueTypeCodec.format(udtValue);
            }

            return udtValueTypeCodec.format(serialize(value));
        }

        /**
         * Returns the Java type of this codec.
         *
         * <p>This method returns the generic type of the Java class associated with this codec.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * UDTCodec<Address> codec = UDTCodec.create(addressUserType, Address.class);
         *
         * GenericType<Address> type = codec.getJavaType();      // GenericType wrapping Address.class
         * Class<?> raw = type.getRawType();                     // returns Address.class
         * }</pre>
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
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * UDTCodec<Address> codec = UDTCodec.create(addressUserType, Address.class);
         *
         * DataType cqlType = codec.getCqlType(); // returns the UserDefinedType this codec was created against
         * }</pre>
         *
         * @return the CQL type of this codec
         */
        @Override
        public DataType getCqlType() {
            return cqlType;
        }

        /**
         * Creates an empty {@link UdtValue} for this codec's user-defined type, with every field
         * unset. Subclasses call this from {@link #serialize(Object)} to obtain the container they
         * populate.
         *
         * @return a new, empty {@code UdtValue} of this codec's CQL type
         * @throws NullPointerException if this codec was constructed with a null CQL type
         */
        protected UdtValue newUDTValue() throws NullPointerException {
            return cqlType.newValue();
        }

        /**
         * Converts a Java value into its Cassandra UDT representation. Implementations typically
         * start from {@link #newUDTValue()} and set one field per Java property.
         *
         * @param value the Java value to convert; implementations must accept {@code null}
         * @return the equivalent {@code UdtValue}, or {@code null} for a {@code null} input
         * @throws RuntimeException if the value cannot be mapped between the configured Java type and the UDT, a field codec rejects a value, or
         *         a bean accessor fails
         */
        protected abstract UdtValue serialize(T value) throws RuntimeException;

        /**
         * Converts a Cassandra UDT value back into its Java representation — the inverse of
         * {@link #serialize(Object)}.
         *
         * @param value the {@code UdtValue} read from Cassandra; implementations must accept
         *        {@code null}
         * @return the equivalent Java value, or {@code null} for a {@code null} input
         * @throws RuntimeException if the value cannot be mapped between the configured Java type and the UDT, a field codec rejects a value, or
         *         a bean accessor fails
         */
        protected abstract T deserialize(UdtValue value) throws RuntimeException;
    }

    /**
     * Default codec used by {@link CassandraExecutor#registerTypeCodec(Class)} for arbitrary Java
     * classes, mapping them to/from the Cassandra {@code TEXT} type by JSON serialization.
     *
     * <p>Values are serialized via {@link N#toJson(Object)} and deserialized via
     * {@link N#fromJson(String, Class)}. The driver-facing {@link #format(Object)} and
     * {@link #parse(String)} methods use the underlying text codec, so their values follow CQL
     * literal quoting and escaping rules. A {@code null} reference is represented by the unquoted
     * {@code NULL} literal in string form and by a {@code null} buffer in binary form.</p>
     *
     * @param <T> the Java class encoded/decoded by this codec
     */
    static class StringCodec<T> implements TypeCodec<T> {
        private static final TypeCodec<String> stringTypeCodec = TypeCodecs.TEXT;
        private final Class<T> javaClazz;
        private final GenericType<T> javaType;

        /**
         * Creates a {@code StringCodec} that JSON-marshals values of {@code javaClazz} to and from
         * Cassandra {@code TEXT}.
         *
         * @param javaClazz the Java class this codec encodes/decodes
         * @throws IllegalArgumentException if {@code javaClazz} is {@code null}
         */
        protected StringCodec(final Class<T> javaClazz) throws IllegalArgumentException {
            N.checkArgNotNull(javaClazz, cs.javaClazz);

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
         * @throws ParsingException if JSON serialization rejects the value, including unsupported or cyclic content
         * @throws UncheckedIOException if a value serializer cannot read an underlying stream or reader
         * @throws IllegalArgumentException if the serialized JSON cannot be encoded as UTF-8 (for example, it contains
         *         an unpaired surrogate character)
         */
        @Deprecated
        public ByteBuffer serialize(final T value, final ProtocolVersion protocolVersion)
                throws ParsingException, UncheckedIOException, IllegalArgumentException {
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
         * @throws IllegalArgumentException if {@code bytes} is not valid UTF-8
         * @throws ParsingException if the decoded JSON syntax or structure cannot be read as {@code T}
         * @throws UncheckedIOException if a JSON value reader or converter reports an I/O failure while materializing {@code T}
         */
        @Deprecated
        public T deserialize(final ByteBuffer bytes, final ProtocolVersion protocolVersion)
                throws IllegalArgumentException, ParsingException, UncheckedIOException {
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
         * @throws ParsingException if JSON serialization rejects the value, including unsupported or cyclic content
         * @throws UncheckedIOException if a value serializer cannot read an underlying stream or reader
         * @throws IllegalArgumentException if the serialized JSON cannot be encoded as UTF-8 (for example, it contains
         *         an unpaired surrogate character)
         */
        @Override
        public ByteBuffer encode(final T value, final ProtocolVersion protocolVersion) throws ParsingException, UncheckedIOException, IllegalArgumentException {
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
         * @throws IllegalArgumentException if {@code bytes} is not valid UTF-8
         * @throws ParsingException if the decoded JSON syntax or structure cannot be read as {@code T}
         * @throws UncheckedIOException if a JSON value reader or converter reports an I/O failure while materializing {@code T}
         */
        @Override
        public T decode(final ByteBuffer bytes, final ProtocolVersion protocolVersion) throws IllegalArgumentException, ParsingException, UncheckedIOException {
            return deserialize(stringTypeCodec.decode(bytes, protocolVersion));
        }

        /**
         * Parses a CQL {@code text} literal into a Java object of type {@code T}.
         *
         * <p>The underlying text codec first removes CQL quoting and unescapes the JSON payload.
         * An empty input or the unquoted {@link CassandraExecutorBase#NULL_STR NULL} literal is
         * decoded as {@code null}.</p>
         *
         * @param value the CQL text literal to parse
         * @return an instance of {@code T}, or {@code null} for empty/{@code NULL} input
         * @throws IllegalArgumentException if the nonempty, non-null-literal {@code value} is not a quoted CQL text literal
         * @throws ParsingException if the decoded JSON syntax or structure cannot be read as {@code T}
         * @throws UncheckedIOException if a JSON value reader or converter reports an I/O failure while materializing {@code T}
         */
        @Override
        public T parse(final String value) throws IllegalArgumentException, ParsingException, UncheckedIOException {
            return Strings.isEmpty(value) || NULL_STR.equals(value) ? null : deserialize(stringTypeCodec.parse(value));
        }

        /**
         * Formats the given value as a CQL {@code text} literal containing JSON.
         *
         * <p>The underlying text codec quotes and escapes the serialized JSON. A {@code null}
         * value is rendered as the unquoted {@link CassandraExecutorBase#NULL_STR NULL} literal.</p>
         *
         * @param value the value to format
         * @return a quoted CQL text literal, or {@link CassandraExecutorBase#NULL_STR NULL}
         *         if the value is {@code null}
         * @throws ParsingException if JSON serialization rejects the value, including unsupported or cyclic content
         * @throws UncheckedIOException if a value serializer cannot read an underlying stream or reader
         */
        @Override
        public String format(final T value) throws ParsingException, UncheckedIOException {
            return value == null ? NULL_STR : stringTypeCodec.format(serialize(value));
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

        /**
         * Converts a Java value to its JSON payload without applying CQL literal quoting.
         *
         * @param value the value to serialize
         * @return the JSON payload, or {@code null} for a {@code null} value
         * @throws ParsingException if JSON serialization rejects the value, including unsupported or cyclic content
         * @throws UncheckedIOException if a value serializer cannot read an underlying stream or reader
         */
        protected String serialize(final T value) throws ParsingException, UncheckedIOException {
            return value == null ? null : N.toJson(value);
        }

        /**
         * Converts a decoded JSON payload to the configured Java type.
         *
         * @param value the decoded JSON payload
         * @return the deserialized value, or {@code null} for a {@code null} payload
         * @throws ParsingException if the decoded JSON syntax or structure cannot be read as {@code T}
         * @throws UncheckedIOException if a JSON value reader or converter reports an I/O failure while materializing {@code T}
         */
        protected T deserialize(final String value) throws ParsingException, UncheckedIOException {
            return value == null ? null : N.fromJson(value, javaClazz);
        }
    }

    /**
     * Configuration settings applied to every CQL statement built by a {@link CassandraExecutor}.
     *
     * <p>Encapsulates the driver settings that are pushed onto each {@link Statement} via
     * {@code configStatement(...)}:</p>
     * <ul>
     *   <li>{@link ConsistencyLevel consistency} &mdash; replication-level read/write consistency
     *       (for example {@link ConsistencyLevel#QUORUM QUORUM},
     *       {@link ConsistencyLevel#LOCAL_QUORUM LOCAL_QUORUM}).</li>
     *   <li>{@code serialConsistency} &mdash; serial consistency for lightweight transactions
     *       ({@code IF EXISTS} / {@code IF NOT EXISTS}); typically
     *       {@link ConsistencyLevel#SERIAL SERIAL} or {@link ConsistencyLevel#LOCAL_SERIAL
     *       LOCAL_SERIAL}.</li>
     *   <li>{@code fetchSize} &mdash; driver page size (rows per fetched page) when streaming
     *       results.</li>
     *   <li>{@code timeout} &mdash; per-statement timeout.</li>
     *   <li>{@code traceQuery} &mdash; enables server-side query tracing.</li>
     * </ul>
     *
     * <p>Any {@code null} field is left at the driver's default and not applied to the statement.
     * This class uses Lombok annotations ({@code @Builder}, {@code @Data},
     * {@code @Accessors(fluent = true)}) to provide a fluent builder and fluent accessors; the
     * default and all-arguments constructors are declared below.</p>
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
         * Creates an empty {@code StatementSettings} with every field {@code null} (i.e. no driver
         * defaults overridden). Fields can be populated via the fluent setters.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * StatementSettings settings = new StatementSettings(); // every field is null (driver defaults)
         * settings.consistency(ConsistencyLevel.QUORUM)         // fluent setters return the same instance
         *         .fetchSize(1000);
         * CassandraExecutor executor = new CassandraExecutor(session, settings);
         * }</pre>
         *
         */
        public StatementSettings() {
        }

        /**
         * Creates a {@code StatementSettings} with all fields populated. {@code null} fields are
         * treated as "do not override the driver default."
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * StatementSettings settings = new StatementSettings(
         *     ConsistencyLevel.QUORUM, ConsistencyLevel.SERIAL, 1000, Duration.ofSeconds(30), Boolean.TRUE);
         * // settings.consistency() == QUORUM, settings.fetchSize() == 1000, settings.traceQuery() == true
         *
         * // Leave individual settings at the driver default by passing null:
         * StatementSettings minimal = new StatementSettings(
         *     ConsistencyLevel.QUORUM, null, null, null, null); // only consistency overridden
         * }</pre>
         *
         * @param consistency replication-level consistency, or {@code null} for the driver default
         * @param serialConsistency serial consistency for LWT operations, or {@code null} for the
         *        driver default
         * @param fetchSize driver page size, or {@code null} for the driver default
         * @param timeout per-statement timeout, or {@code null} for the driver default; driver 4.19.3 ignores it for batch statements
         * @param traceQuery {@code true} to enable server-side query tracing, {@code false} to
         *        disable, or {@code null} to leave unset
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
