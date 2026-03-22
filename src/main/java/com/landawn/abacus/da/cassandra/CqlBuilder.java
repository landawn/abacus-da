/*
 * Copyright (c) 2016, Haiyang Li.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.landawn.abacus.da.cassandra;

import static com.landawn.abacus.util.SK._PARENTHESIS_L;
import static com.landawn.abacus.util.SK._PARENTHESIS_R;
import static com.landawn.abacus.util.SK._SPACE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.landawn.abacus.annotation.Beta;
import com.landawn.abacus.annotation.NonUpdatable;
import com.landawn.abacus.annotation.ReadOnly;
import com.landawn.abacus.annotation.ReadOnlyId;
import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.query.AbstractQueryBuilder;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.QueryUtil;
import com.landawn.abacus.query.condition.Between;
import com.landawn.abacus.query.condition.Binary;
import com.landawn.abacus.query.condition.Cell;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.query.condition.Expression;
import com.landawn.abacus.query.condition.Having;
import com.landawn.abacus.query.condition.In;
import com.landawn.abacus.query.condition.InSubQuery;
import com.landawn.abacus.query.condition.Junction;
import com.landawn.abacus.query.condition.NotBetween;
import com.landawn.abacus.query.condition.NotIn;
import com.landawn.abacus.query.condition.NotInSubQuery;
import com.landawn.abacus.query.condition.SubQuery;
import com.landawn.abacus.query.condition.Where;
import com.landawn.abacus.util.Array;
import com.landawn.abacus.util.Beans;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.OperationType;
import com.landawn.abacus.util.SK;
import com.landawn.abacus.util.Strings;
import com.landawn.abacus.util.u.Optional;

/**
 * A comprehensive fluent API builder for constructing Apache Cassandra CQL (Cassandra Query Language) statements.
 * 
 * <p>This abstract builder provides a type-safe, fluent interface for creating CQL queries, inserts, updates, 
 * and deletes. It handles proper parameterization, column name formatting according to naming policies, and
 * supports advanced Cassandra features like TTL, timestamps, lightweight transactions, and filtering.</p>
 *
 * <h2>Key Features</h2>
 * <h3>Main Capabilities</h3>
 * <ul>
 *   <li><b>Fluent API Design:</b> Method chaining for readable query construction</li>
 *   <li><b>Type Safety:</b> Compile-time checking with parameterized queries</li>
 *   <li><b>Cassandra-Specific Features:</b> TTL, TIMESTAMP, IF conditions, ALLOW FILTERING</li>
 *   <li><b>Naming Policy Support:</b> Automatic column name transformation (camelCase ↔ snake_case)</li>
 *   <li><b>Performance Optimized:</b> Efficient string building with resource management</li>
 *   <li><b>Lightweight Transactions:</b> IF, IF EXISTS, IF NOT EXISTS support</li>
 * </ul>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // SELECT with WHERE clause
 * String selectCql = PSC.select("id", "name", "email")
 *                       .from("users")
 *                       .where(Filters.eq("status", "active"))
 *                       .build().query();
 * // Result: SELECT id, name, email FROM users WHERE status = ?
 *
 * // INSERT with TTL
 * String insertCql = PSC.insert("id", "sessionToken", "createdAt")
 *                       .into("user_sessions")
 *                       .values(userId, token, new Date())
 *                       .usingTTL(3600)  // 1 hour TTL
 *                       .build().query();
 * // Result: INSERT INTO user_sessions (id, session_token, created_at) VALUES (?, ?, ?) USING TTL 3600
 *
 * // UPDATE with lightweight transaction
 * String updateCql = PSC.update("users")
 *                       .set("lastLogin")
 *                       .where(Filters.eq("id", userId))
 *                       .iF(Filters.eq("status", "active"))
 *                       .build().query();
 * // Result: UPDATE users SET last_login = ? WHERE id = ? IF status = ?
 *
 * // DELETE with conditional existence check
 * String deleteCql = PSC.deleteFrom("expired_sessions")
 *                       .where(Filters.lt("expiresAt", new Date()))
 *                       .ifExists()
 *                       .build().query();
 * // Result: DELETE FROM expired_sessions WHERE expires_at < ? IF EXISTS
 * }</pre>
 *
 * <h3>Cassandra-Specific Capabilities:</h3>
 * <ul>
 *   <li><b>TTL (Time To Live):</b> {@code usingTTL(seconds)} for automatic data expiration</li>
 *   <li><b>Timestamps:</b> {@code usingTimestamp(timestamp)} for precise operation timing</li>
 *   <li><b>Lightweight Transactions:</b> {@code iF()}, {@code ifExists()}, {@code ifNotExists()} for consistency</li>
 *   <li><b>Allow Filtering:</b> {@code allowFiltering()} for server-side filtering (use with caution)</li>
 *   <li><b>Clustering Columns:</b> Support for range queries and ordering</li>
 *   <li><b>Partition Keys:</b> Efficient query construction respecting Cassandra's data model</li>
 * </ul>
 *
 * <h3>Thread Safety:</h3>
 * <p>This builder is <b>NOT thread-safe</b>. Each thread should use its own builder instance. 
 * The builder maintains internal state during query construction and must not be shared across threads.</p>
 *
 * <h3>Resource Management:</h3>
 * <p>The builder uses internal resources that are automatically released when {@code cql()} or {@code pair()} 
 * is called. After calling these methods, the builder cannot be reused. Always call one of these methods 
 * to properly finalize the query and release resources.</p>
 *
 * <h3>Naming Policy:</h3>
 * <p>Column names are automatically transformed according to the configured naming policy. By default, 
 * Java camelCase property names are converted to Cassandra snake_case column names. Table names are 
 * NOT transformed and are used as-is.</p>
 *
 * <p><b>Supported Operations:</b></p>
 * <ul>
 *   <li>{@code select(...).from(tableName).where(...)} - SELECT queries with filtering</li>
 *   <li>{@code insert(...).into(tableName).values(...)} - INSERT operations with optional TTL/timestamp</li>
 *   <li>{@code update(tableName).set(...).where(...)} - UPDATE operations with conditional logic</li>
 *   <li>{@code deleteFrom(tableName).where(...)} - DELETE operations with optional conditions</li>
 * </ul>
 *
 * @see com.landawn.abacus.query.Filters
 * @see CassandraExecutor
 * @see ParsedCql
 */
@SuppressWarnings("java:S1192")
public abstract class CqlBuilder extends AbstractQueryBuilder<CqlBuilder> { // NOSONAR

    // TODO performance goal: 80% cases (or maybe CQL.length < 1024?) can be composed in 0.1 millisecond. 0.01 millisecond will be fantastic if possible.

    protected static final Logger logger = LoggerFactory.getLogger(CqlBuilder.class);

    static final char[] _SPACE_USING_TIMESTAMP_SPACE = " USING TIMESTAMP ".toCharArray();

    static final char[] _SPACE_USING_TTL_SPACE = " USING TTL ".toCharArray();

    static final char[] _SPACE_IF_SPACE = " IF ".toCharArray();

    static final char[] _SPACE_IF_EXISTS = " IF EXISTS".toCharArray();

    static final char[] _SPACE_IF_NOT_EXISTS = " IF NOT EXISTS".toCharArray();

    static final char[] _SPACE_ALLOW_FILTERING = " ALLOW FILTERING".toCharArray();

    /**
     * Constructs a new CqlBuilder with the specified naming policy and CQL policy.
     * 
     * @param namingPolicy the naming policy for column names, defaults to SNAKE_CASE if null
     * @param sqlPolicy the CQL generation policy, defaults to CQL if null
     */
    protected CqlBuilder(final NamingPolicy namingPolicy, final SQLPolicy sqlPolicy) {
        super(namingPolicy, sqlPolicy);
    }

    private static final Map<Integer, String> QM_CACHE = new HashMap<>();

    static {
        for (int i = 0; i <= 30; i++) {
            QM_CACHE.put(i, Strings.repeat("?", i, ", "));
        }

        QM_CACHE.put(100, Strings.repeat("?", 100, ", "));
        QM_CACHE.put(200, Strings.repeat("?", 200, ", "));
        QM_CACHE.put(300, Strings.repeat("?", 300, ", "));
        QM_CACHE.put(500, Strings.repeat("?", 500, ", "));
        QM_CACHE.put(1000, Strings.repeat("?", 1000, ", "));
    }

    /**
     * Repeat question mark({@code ?}) {@code n} times with delimiter {@code ", "}.
     * 
     * <p>This utility method generates a string of parameterized placeholders suitable for 
     * batch CQL operations, particularly useful for IN clauses or VALUES lists with dynamic
     * parameter counts.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Generate placeholders for batch insert
     * String placeholders = CqlBuilder.repeatPlaceholders(3);
     * // Result: "?, ?, ?"
     * 
     * // Use in CQL construction
     * String cql = "SELECT * FROM users WHERE id IN (" + CqlBuilder.repeatPlaceholders(5) + ")";
     * // Result: "SELECT * FROM users WHERE id IN (?, ?, ?, ?, ?)"
     * 
     * // For VALUES clause
     * String insertCql = "INSERT INTO users (id, name, email) VALUES (" + CqlBuilder.repeatPlaceholders(3) + ")";
     * // Result: "INSERT INTO users (id, name, email) VALUES (?, ?, ?)"
     * }</pre>
     *
     * @param count the number of question marks to repeat (must be non-negative)
     * @return a string containing {@code count} question marks separated by {@code ", "}
     * @throws IllegalArgumentException if count is negative
     */
    @Beta
    public static String repeatPlaceholders(final int count) {
        N.checkArgNotNegative(count, "count");

        String result = QM_CACHE.get(count);

        if (result == null) {
            result = Strings.repeat("?", count, ", ");
        }

        return result;
    }

    /**
     * Generates a string of comma-separated question mark placeholders.
     *
     * @param count the number of placeholders to generate
     * @return a comma-separated string of question mark placeholders
     * @deprecated Use {@link #repeatPlaceholders(int)} instead.
     */
    @Beta
    @Deprecated
    public static String repeatQM(final int count) {
        return repeatPlaceholders(count);
    }

    /**
     * Adds a USING TTL clause to the CQL statement with the specified TTL value.
     *
     * <p>The TTL (Time To Live) clause specifies how long the data should be stored
     * before it expires and is automatically deleted by Cassandra. This is particularly
     * useful for INSERT and UPDATE operations where you want to set an expiration time
     * for the data.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Insert with TTL of 3600 seconds (1 hour)
     * String cql = PSC.insert("name", "email")
     *                 .into("users")
     *                 .values("John", "john@example.com")
     *                 .usingTTL(3600)
     *                 .build().query();
     * // Output: INSERT INTO users (name, email) VALUES (?, ?) USING TTL 3600
     *
     * // Update with TTL
     * String cql = PSC.update("users")
     *                 .set("status")
     *                 .where(Filters.eq("id", 123))
     *                 .usingTTL(7200)
     *                 .build().query();
     * // Output: UPDATE users SET status = ? WHERE id = ? USING TTL 7200
     * }</pre>
     *
     * @param ttl the TTL value in seconds
     * @return this CqlBuilder instance for method chaining
     */
    public CqlBuilder usingTTL(final long ttl) {
        return usingTTL(String.valueOf(ttl));
    }

    /**
     * Adds a USING TTL clause to the CQL statement with the specified TTL value as a string.
     *
     * <p>The TTL (Time To Live) clause specifies how long the data should be stored
     * before it expires and is automatically deleted by Cassandra. This overload accepts
     * a string parameter, which can be useful for dynamic TTL values or when integrating
     * with external configuration systems.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Insert with TTL from configuration
     * String ttlConfig = "7200";  // 2 hours
     * String cql = PSC.insert("name", "email")
     *                 .into("users")
     *                 .values("John", "john@example.com")
     *                 .usingTTL(ttlConfig)
     *                 .build().query();
     * // Output: INSERT INTO users (name, email) VALUES (?, ?) USING TTL 7200
     *
     * // Update with dynamic TTL
     * String dynamicTTL = String.valueOf(System.currentTimeMillis() / 1000 + 3600);
     * String cql = PSC.update("users")
     *                 .set("status")
     *                 .where(Filters.eq("id", 123))
     *                 .usingTTL(dynamicTTL)
     *                 .build().query();
     * }</pre>
     *
     * @param ttl the TTL value as a string (should represent seconds)
     * @return this CqlBuilder instance for method chaining
     * @see #usingTTL(long)
     */
    public CqlBuilder usingTTL(final String ttl) {
        init(false);

        _sb.append(_SPACE_USING_TTL_SPACE);
        _sb.append(ttl);

        return this;
    }

    /**
     * Adds a USING TIMESTAMP clause to the CQL statement with the specified timestamp from a Date object.
     *
     * <p>The TIMESTAMP clause allows you to specify the exact timestamp for the operation in microseconds
     * since the Unix epoch. This is particularly useful for controlling the order of operations or
     * when importing historical data into Cassandra.</p>
     *
     * <p>This method converts the Date to milliseconds and delegates to {@link #usingTimestamp(long)}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Date specificTime = new Date(System.currentTimeMillis() - 86400000);   // 24 hours ago
     * String cql = PSC.insert("id", "data")
     *                 .into("events")
     *                 .values(123, "historical data")
     *                 .usingTimestamp(specificTime)
     *                 .build().query();
     * // Output: INSERT INTO events (id, data) VALUES (?, ?) USING TIMESTAMP 1234567890123000
     * }</pre>
     *
     * @param timestamp the timestamp as a Date object
     * @return this CqlBuilder instance for method chaining
     * @see #usingTimestamp(long)
     * @see #usingTimestamp(String)
     */
    public CqlBuilder usingTimestamp(final Date timestamp) {
        return usingTimestamp(timestamp.getTime());
    }

    /**
     * Adds a USING TIMESTAMP clause to the CQL statement with the specified timestamp in milliseconds.
     *
     * <p>The TIMESTAMP clause allows you to specify the exact timestamp for the operation in microseconds
     * since the Unix epoch. This method accepts milliseconds and converts them to the appropriate format
     * for Cassandra by delegating to {@link #usingTimestamp(String)}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long specificTime = System.currentTimeMillis() - 3600000;  // 1 hour ago
     * String cql = PSC.update("users")
     *                 .set("lastLogin")
     *                 .where(Filters.eq("id", 123))
     *                 .usingTimestamp(specificTime)
     *                 .build().query();
     * // Output: UPDATE users SET last_login = ? WHERE id = ? USING TIMESTAMP 1234567890123000
     * }</pre>
     *
     * @param timestamp the timestamp in milliseconds since Unix epoch
     * @return this CqlBuilder instance for method chaining
     * @see #usingTimestamp(Date)
     * @see #usingTimestamp(String)
     */
    public CqlBuilder usingTimestamp(final long timestamp) {
        return usingTimestamp(String.valueOf(Math.multiplyExact(timestamp, 1000L)));
    }

    /**
     * Adds a USING TIMESTAMP clause to the CQL statement with the specified timestamp as a string.
     *
     * <p>The TIMESTAMP clause allows you to specify the exact timestamp for the operation in microseconds
     * since the Unix epoch. This is useful for controlling the order of operations, importing historical
     * data, or ensuring consistent timestamps across multiple operations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String timestamp = String.valueOf(System.currentTimeMillis());
     * String cql = PSC.insert("id", "name", "createdAt")
     *                 .into("users")
     *                 .values(123, "John", new Date())
     *                 .usingTimestamp(timestamp)
     *                 .build().query();
     * // Output: INSERT INTO users (id, name, created_at) VALUES (?, ?, ?) USING TIMESTAMP 1234567890123000
     * }</pre>
     *
     * @param timestamp the timestamp as a string (should represent microseconds since Unix epoch)
     * @return this CqlBuilder instance for method chaining
     * @see #usingTimestamp(Date)
     * @see #usingTimestamp(long)
     */
    public CqlBuilder usingTimestamp(final String timestamp) {
        init(false);

        _sb.append(_SPACE_USING_TIMESTAMP_SPACE);
        _sb.append(timestamp);

        return this;
    }

    /**
     * Adds a conditional IF clause to the CQL statement with the specified expression.
     *
     * <p>The IF clause enables lightweight transactions (LWT) in Cassandra by adding conditions
     * that must be met for the operation to succeed. This is particularly useful for ensuring
     * data consistency in concurrent environments.</p>
     *
     * <p><b>Note:</b> The method name is spelled {@code iF} because {@code if} is a Java keyword.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // INSERT with condition
     * String cql = PSC.insert("id", "name")
     *                 .into("users")
     *                 .values(123, "John")
     *                 .iF("name = NULL")
     *                 .build().query();
     * // Output: INSERT INTO users (id, name) VALUES (?, ?) IF name = NULL
     *
     * // UPDATE with condition
     * String cql = PSC.update("users")
     *                 .set("status")
     *                 .where(Filters.eq("id", 123))
     *                 .iF("status = 'inactive'")
     *                 .build().query();
     * // Output: UPDATE users SET status = ? WHERE id = ? IF status = 'inactive'
     * }</pre>
     *
     * @param expr the conditional expression as a string
     * @return this CqlBuilder instance for method chaining
     * @see #iF(Condition)
     */
    public CqlBuilder iF(final String expr) {
        init(true);

        _sb.append(_SPACE_IF_SPACE);

        appendStringExpr(expr, false);

        return this;
    }

    /**
     * Adds a conditional IF clause to the CQL statement with the specified condition.
     *
     * <p>The IF clause enables lightweight transactions (LWT) in Cassandra by adding conditions
     * that must be met for the operation to succeed. This overload accepts a {@link Condition}
     * object, providing type-safe condition building with proper parameter binding.</p>
     *
     * <p><b>Note:</b> The method name is spelled {@code iF} because {@code if} is a Java keyword.</p>
     *
     * <p><b>Note:</b> Any literal written in Expression condition won't be formalized according
     * to the naming policy.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // INSERT with condition using Filters
     * String cql = PSC.insert("id", "name")
     *                 .into("users")
     *                 .values(123, "John")
     *                 .iF(Filters.isNull("name"))
     *                 .build().query();
     * // Output: INSERT INTO users (id, name) VALUES (?, ?) IF name IS NULL
     *
     * // UPDATE with complex condition
     * String cql = PSC.update("users")
     *                 .set("status")
     *                 .where(Filters.eq("id", 123))
     *                 .iF(Filters.and(Filters.eq("status", "inactive"), Filters.lt("retryCount", 3)))
     *                 .build().query();
     * // Output: UPDATE users SET status = ? WHERE id = ? IF status = ? AND retry_count < ?
     * }</pre>
     *
     * @param cond the condition object for the IF clause
     * @return this CqlBuilder instance for method chaining
     * @see #iF(String)
     * @see com.landawn.abacus.query.Filters
     */
    public CqlBuilder iF(final Condition cond) {
        init(true);

        _sb.append(_SPACE_IF_SPACE);

        appendCondition(cond);

        return this;
    }

    /**
     * Adds an IF EXISTS clause to the CQL statement.
     *
     * <p>The IF EXISTS clause is used with UPDATE and DELETE operations to ensure the operation
     * only executes if the target row exists. This provides a lightweight transaction that
     * prevents unnecessary operations and can help with error handling.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // UPDATE only if row exists
     * String cql = PSC.update("users")
     *                 .set("lastLogin", new Date())
     *                 .where(Filters.eq("id", 123))
     *                 .ifExists()
     *                 .build().query();
     * // Output: UPDATE users SET last_login = ? WHERE id = ? IF EXISTS
     *
     * // DELETE only if row exists
     * String cql = PSC.deleteFrom("users")
     *                 .where(Filters.eq("id", 123))
     *                 .ifExists()
     *                 .build().query();
     * // Output: DELETE FROM users WHERE id = ? IF EXISTS
     * }</pre>
     *
     * @return this CqlBuilder instance for method chaining
     * @see #ifNotExists()
     * @see #iF(String)
     * @see #iF(Condition)
     */
    public CqlBuilder ifExists() {
        init(true);

        _sb.append(_SPACE_IF_EXISTS);

        return this;
    }

    /**
     * Adds an IF NOT EXISTS clause to the CQL statement.
     *
     * <p>The IF NOT EXISTS clause is typically used with INSERT operations to ensure the operation
     * only executes if the target row does not already exist. This provides a lightweight transaction
     * that prevents duplicate insertions and can be useful for ensuring data uniqueness.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // INSERT only if row doesn't exist
     * String cql = PSC.insert("id", "name", "email")
     *                 .into("users")
     *                 .values(123, "John", "john@example.com")
     *                 .ifNotExists()
     *                 .build().query();
     * // Output: INSERT INTO users (id, name, email) VALUES (?, ?, ?) IF NOT EXISTS
     *
     * // Can also be used with UPDATE operations
     * String cql = PSC.update("users")
     *                 .set("status", "new")
     *                 .where(Filters.eq("id", 123))
     *                 .ifNotExists()
     *                 .build().query();
     * // Output: UPDATE users SET status = ? WHERE id = ? IF NOT EXISTS
     * }</pre>
     *
     * @return this CqlBuilder instance for method chaining
     * @see #ifExists()
     * @see #iF(String)
     * @see #iF(Condition)
     */
    public CqlBuilder ifNotExists() {
        init(true);

        _sb.append(_SPACE_IF_NOT_EXISTS);

        return this;
    }

    /**
     * Adds an ALLOW FILTERING clause to the CQL statement.
     *
     * <p>The ALLOW FILTERING clause permits queries that require server-side filtering,
     * which can be expensive and slow. This should be used with caution and only when
     * necessary, as it can significantly impact performance on large datasets.</p>
     *
     * <p>Typical use cases include:</p>
     * <ul>
     *   <li>Filtering on non-indexed columns</li>
     *   <li>Range queries on clustering columns without all preceding columns</li>
     *   <li>Secondary index queries with additional filtering</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Query with filtering on non-indexed column
     * String cql = PSC.select("id", "name", "age")
     *                 .from("users")
     *                 .where(Filters.gt("age", 18))
     *                 .allowFiltering()
     *                 .build().query();
     * // Output: SELECT id, name, age FROM users WHERE age > ? ALLOW FILTERING
     *
     * // Complex query requiring filtering
     * String cql = PSC.select("*")
     *                 .from("events")
     *                 .where(Filters.and(Filters.eq("type", "login"), Filters.gte("timestamp", yesterday)))
     *                 .allowFiltering()
     *                 .build().query();
     * // Output: SELECT * FROM events WHERE type = ? AND timestamp >= ? ALLOW FILTERING
     * }</pre>
     *
     * <p><b>Performance Warning:</b> Use this clause sparingly as it can cause full table scans
     * and significantly impact query performance.</p>
     *
     * @return this CqlBuilder instance for method chaining
     */
    public CqlBuilder allowFiltering() {
        init(true);

        _sb.append(_SPACE_ALLOW_FILTERING);

        return this;
    }

    @Override
    protected void appendOperationBeforeFrom(final String tableName) {
        if (_op != OperationType.QUERY && _op != OperationType.DELETE) {
            throw new RuntimeException("Invalid operation: " + _op);
        }

        if (_op == OperationType.QUERY && N.isEmpty(_propOrColumnNames) && N.isEmpty(_propOrColumnNameAliases) && N.isEmpty(_multiSelects)) {
            throw new RuntimeException("No columns selected. Call select() to specify columns before building query");
        }

        final int idx = tableName.indexOf(' ');

        if (idx > 0) {
            _tableName = tableName.substring(0, idx).trim();
            _tableAlias = tableName.substring(idx + 1).trim();
        } else {
            _tableName = tableName.trim();
        }

        if (_entityClass != null && Strings.isNotEmpty(_tableAlias)) {
            addPropColumnMapForAlias(_entityClass, _tableAlias);
        }

        if (_op == OperationType.DELETE) {
            _sb.append(_DELETE);
        } else {
            _sb.append(_SELECT);
        }

        if (Strings.isNotEmpty(_selectModifier)) {
            _sb.append(_SPACE);
            appendStringExpr(_selectModifier, false);
        }

        if (N.notEmpty(_propOrColumnNames) || N.notEmpty(_propOrColumnNameAliases) || N.notEmpty(_multiSelects)) {
            _sb.append(_SPACE);
        }
    }

    @Override
    protected void appendCondition(final Condition cond) {
        //    if (sb.charAt(sb.length() - 1) != _SPACE) {
        //        sb.append(_SPACE);
        //    }

        if (cond instanceof final Binary binary) {
            final String propName = binary.getPropName();

            appendColumnName(propName);

            _sb.append(_SPACE);
            _sb.append(binary.operator().toString());
            _sb.append(_SPACE);

            final Object propValue = binary.getPropValue();
            setParameter(propName, propValue);
        } else if (cond instanceof final Between bt) {
            final String propName = bt.getPropName();

            appendColumnName(propName);

            _sb.append(_SPACE);
            _sb.append(bt.operator().toString());
            _sb.append(_SPACE);

            final Object minValue = bt.getMinValue();
            if (_sqlPolicy == SQLPolicy.NAMED_SQL || _sqlPolicy == SQLPolicy.IBATIS_SQL) {
                setParameter("min" + Strings.capitalize(propName), minValue);
            } else {
                setParameter(propName, minValue);
            }

            _sb.append(_SPACE);
            _sb.append(SK.AND);
            _sb.append(_SPACE);

            final Object maxValue = bt.getMaxValue();
            if (_sqlPolicy == SQLPolicy.NAMED_SQL || _sqlPolicy == SQLPolicy.IBATIS_SQL) {
                setParameter("max" + Strings.capitalize(propName), maxValue);
            } else {
                setParameter(propName, maxValue);
            }
        } else if (cond instanceof final NotBetween nbt) {
            final String propName = nbt.getPropName();

            appendColumnName(propName);

            _sb.append(_SPACE);
            _sb.append(nbt.operator().toString());
            _sb.append(_SPACE);

            final Object minValue = nbt.getMinValue();
            if (_sqlPolicy == SQLPolicy.NAMED_SQL || _sqlPolicy == SQLPolicy.IBATIS_SQL) {
                setParameter("min" + Strings.capitalize(propName), minValue);
            } else {
                setParameter(propName, minValue);
            }

            _sb.append(_SPACE);
            _sb.append(SK.AND);
            _sb.append(_SPACE);

            final Object maxValue = nbt.getMaxValue();
            if (_sqlPolicy == SQLPolicy.NAMED_SQL || _sqlPolicy == SQLPolicy.IBATIS_SQL) {
                setParameter("max" + Strings.capitalize(propName), maxValue);
            } else {
                setParameter(propName, maxValue);
            }
        } else if (cond instanceof final In in) {
            final String propName = in.getPropName();
            final List<Object> params = in.getParameters();

            appendColumnName(propName);

            _sb.append(_SPACE);
            _sb.append(in.operator().toString());
            _sb.append(SK.SPACE_PARENTHESIS_L);

            for (int i = 0, len = params.size(); i < len; i++) {
                if (i > 0) {
                    _sb.append(SK.COMMA_SPACE);
                }

                if (_sqlPolicy == SQLPolicy.NAMED_SQL || _sqlPolicy == SQLPolicy.IBATIS_SQL) {
                    setParameter(propName + (i + 1), params.get(i));
                } else {
                    setParameter(propName, params.get(i));
                }
            }

            _sb.append(SK._PARENTHESIS_R);
        } else if (cond instanceof final InSubQuery inSubQuery) {
            final Collection<String> propNames = inSubQuery.getPropNames();

            if (propNames.size() == 1) {
                appendColumnName(propNames.iterator().next());
            } else {
                _sb.append(SK._PARENTHESIS_L);

                int idx = 0;

                for (final String e : propNames) {
                    if (idx++ > 0) {
                        _sb.append(_COMMA_SPACE);
                    }

                    appendColumnName(e);
                }

                _sb.append(SK._PARENTHESIS_R);
            }

            _sb.append(_SPACE);
            _sb.append(inSubQuery.operator().toString());

            _sb.append(SK.SPACE_PARENTHESIS_L);

            appendCondition(inSubQuery.getSubQuery());

            _sb.append(SK._PARENTHESIS_R);
        } else if (cond instanceof final NotIn notIn) {
            final String propName = notIn.getPropName();
            final List<Object> params = notIn.getParameters();

            appendColumnName(propName);

            _sb.append(_SPACE);
            _sb.append(notIn.operator().toString());
            _sb.append(SK.SPACE_PARENTHESIS_L);

            for (int i = 0, len = params.size(); i < len; i++) {
                if (i > 0) {
                    _sb.append(SK.COMMA_SPACE);
                }

                if (_sqlPolicy == SQLPolicy.NAMED_SQL || _sqlPolicy == SQLPolicy.IBATIS_SQL) {
                    setParameter(propName + (i + 1), params.get(i));
                } else {
                    setParameter(propName, params.get(i));
                }
            }

            _sb.append(SK._PARENTHESIS_R);
        } else if (cond instanceof final NotInSubQuery notInSubQuery) {
            final Collection<String> propNames = notInSubQuery.getPropNames();

            if (propNames.size() == 1) {
                appendColumnName(propNames.iterator().next());
            } else {
                _sb.append(SK._PARENTHESIS_L);

                int idx = 0;

                for (final String e : propNames) {
                    if (idx++ > 0) {
                        _sb.append(_COMMA_SPACE);
                    }

                    appendColumnName(e);
                }

                _sb.append(SK._PARENTHESIS_R);
            }

            _sb.append(_SPACE);
            _sb.append(notInSubQuery.operator().toString());
            _sb.append(SK.SPACE_PARENTHESIS_L);

            appendCondition(notInSubQuery.getSubQuery());

            _sb.append(SK._PARENTHESIS_R);
        } else if (cond instanceof Where || cond instanceof Having) {
            final Cell cell = (Cell) cond;

            _sb.append(_SPACE);
            _sb.append(cell.operator().toString());
            _sb.append(_SPACE);

            appendCondition(cell.getCondition());
        } else if (cond instanceof final Cell cell) {
            _sb.append(_SPACE);
            _sb.append(cell.operator().toString());
            _sb.append(_SPACE);

            _sb.append(_PARENTHESIS_L);
            appendCondition(cell.getCondition());
            _sb.append(_PARENTHESIS_R);
        } else if (cond instanceof final Junction junction) {
            final List<Condition> conditionList = junction.getConditions();

            if (N.isEmpty(conditionList)) {
                throw new IllegalArgumentException("The junction condition(" + junction.operator().toString() + ") doesn't include any element.");
            }

            if (conditionList.size() == 1) {
                appendCondition(conditionList.get(0));
            } else {
                // TODO ((id = :id) AND (gui = :gui)) is not support in Cassandra.
                // only (id = :id) AND (gui = :gui) works.
                // sb.append(_PARENTHESIS_L);

                for (int i = 0, size = conditionList.size(); i < size; i++) {
                    if (i > 0) {
                        _sb.append(_SPACE);
                        _sb.append(junction.operator().toString());
                        _sb.append(_SPACE);
                    }

                    _sb.append(_PARENTHESIS_L);

                    appendCondition(conditionList.get(i));

                    _sb.append(_PARENTHESIS_R);
                }

                // sb.append(_PARENTHESIS_R);
            }
        } else if (cond instanceof final SubQuery subQuery) {
            final Condition subCond = subQuery.getCondition();

            if (Strings.isNotEmpty(subQuery.sql())) {
                _sb.append(subQuery.sql());
            } else if (subQuery.getEntityClass() != null) {
                if (this instanceof SCCB) {
                    _sb.append(SCCB.select(subQuery.getSelectPropNames()).from(subQuery.getEntityClass()).append(subCond).build().query());
                } else if (this instanceof PSC) {
                    _sb.append(PSC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityClass()).append(subCond).build().query());
                } else if (this instanceof NSC) {
                    _sb.append(NSC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityClass()).append(subCond).build().query());
                } else if (this instanceof ACCB) {
                    _sb.append(ACCB.select(subQuery.getSelectPropNames()).from(subQuery.getEntityClass()).append(subCond).build().query());
                } else if (this instanceof PAC) {
                    _sb.append(PAC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityClass()).append(subCond).build().query());
                } else if (this instanceof NAC) {
                    _sb.append(NAC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityClass()).append(subCond).build().query());
                } else if (this instanceof LCCB) {
                    _sb.append(LCCB.select(subQuery.getSelectPropNames()).from(subQuery.getEntityClass()).append(subCond).build().query());
                } else if (this instanceof PLC) {
                    _sb.append(PLC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityClass()).append(subCond).build().query());
                } else if (this instanceof NLC) {
                    _sb.append(NLC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityClass()).append(subCond).build().query());
                } else if (this instanceof PSB) {
                    _sb.append(PSB.select(subQuery.getSelectPropNames()).from(subQuery.getEntityClass()).append(subCond).build().query());
                } else if (this instanceof NSB) {
                    _sb.append(NSB.select(subQuery.getSelectPropNames()).from(subQuery.getEntityClass()).append(subCond).build().query());
                } else {
                    throw new RuntimeException("Unsupported subQuery condition: " + cond);
                }
            } else if (this instanceof SCCB) {
                _sb.append(SCCB.select(subQuery.getSelectPropNames()).from(subQuery.getEntityName()).append(subCond).build().query());
            } else if (this instanceof PSC) {
                _sb.append(PSC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityName()).append(subCond).build().query());
            } else if (this instanceof NSC) {
                _sb.append(NSC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityName()).append(subCond).build().query());
            } else if (this instanceof ACCB) {
                _sb.append(ACCB.select(subQuery.getSelectPropNames()).from(subQuery.getEntityName()).append(subCond).build().query());
            } else if (this instanceof PAC) {
                _sb.append(PAC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityName()).append(subCond).build().query());
            } else if (this instanceof NAC) {
                _sb.append(NAC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityName()).append(subCond).build().query());
            } else if (this instanceof LCCB) {
                _sb.append(LCCB.select(subQuery.getSelectPropNames()).from(subQuery.getEntityName()).append(subCond).build().query());
            } else if (this instanceof PLC) {
                _sb.append(PLC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityName()).append(subCond).build().query());
            } else if (this instanceof NLC) {
                _sb.append(NLC.select(subQuery.getSelectPropNames()).from(subQuery.getEntityName()).append(subCond).build().query());
            } else if (this instanceof PSB) {
                _sb.append(PSB.select(subQuery.getSelectPropNames()).from(subQuery.getEntityName()).append(subCond).build().query());
            } else if (this instanceof NSB) {
                _sb.append(NSB.select(subQuery.getSelectPropNames()).from(subQuery.getEntityName()).append(subCond).build().query());
            } else {
                throw new RuntimeException("Unsupported subQuery condition: " + cond);
            }
        } else if (cond instanceof Expression) {
            // ==== version 1
            // sb.append(cond.toString());

            // ==== version 2
            //    final List<String> words = CQLParser.parse(((Expression) cond).getLiteral());
            //    final Map<String, String> propColumnNameMap = getPropColumnNameMap(entityClass, namingPolicy);
            //
            //    String word = null;
            //
            //    for (int i = 0, size = words.size(); i < size; i++) {
            //        word = words.get(i);
            //
            //        if ((i > 2) && SK.AS.equalsIgnoreCase(words.get(i - 2))) {
            //            sb.append(word);
            //        } else if ((i > 1) && SK.SPACE.equalsIgnoreCase(words.get(i - 1))
            //                && (propColumnNameMap.containsKey(words.get(i - 2)) || propColumnNameMap.containsValue(words.get(i - 2)))) {
            //            sb.append(word);
            //        } else {
            //            sb.append(formalizeColumnName(propColumnNameMap, word));
            //        }
            //    }

            // ==== version 3
            appendStringExpr(((Expression) cond).getLiteral(), false);
        } else {
            throw new IllegalArgumentException("Unsupported condition: " + cond.toString());
        }
    }

    private static Collection<String> getDeletePropNamesByClass(final Class<?> entityClass, final Set<String> excludedPropNames) {
        final Collection<String>[] val = loadPropNamesByClass(entityClass);
        final Collection<String> propNames = val[0];

        if (N.isEmpty(excludedPropNames)) {
            return propNames;
        } else {
            final List<String> tmp = new ArrayList<>(propNames);
            tmp.removeAll(excludedPropNames);
            return tmp;
        }
    }

    /**
     * Un-parameterized CQL builder with snake case (lower case with underscore) field/column naming strategy.
     * 
     * <p>This builder generates CQL with actual values embedded directly in the CQL string. Property names
     * are automatically converted from camelCase to snake_case for database column names.</p>
     * 
     * <p>Features:</p>
     * <ul>
     *   <li>Converts "firstName" to "first_name"</li>
     *   <li>Generates non-parameterized CQL (values embedded directly)</li>
     *   <li>Suitable for debugging or read-only queries</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SCCB.select("firstName", "lastName").from("account").where(Filters.eq("id", 1)).build().query();
     * // Output: SELECT first_name AS "firstName", last_name AS "lastName" FROM account WHERE id = 1
     * }</pre>
     *
     * @deprecated {@code PSC or NSC} is preferred for better security and performance. 
     *             Un-parameterized CQL is vulnerable to CQL injection attacks.
     */
    @Deprecated
    public static class SCCB extends CqlBuilder {

        /**
         * Constructs a new SCCB instance with snake_case naming policy and non-parameterized CQL policy.
         *
         * <p>This constructor is package-private and should not be called directly. Use the static
         * factory methods like {@link #select(String...)}, {@link #insert(String...)}, etc. instead.</p>
         */
        SCCB() {
            super(NamingPolicy.SNAKE_CASE, SQLPolicy.RAW_SQL);
        }

        /**
         * Creates a new instance of SCCB.
         *
         * <p>This factory method is used internally by the static methods to create new builder instances.
         * Each CQL building operation starts with a fresh instance to ensure thread safety.</p>
         *
         * @return a new SCCB instance
         */
        protected static SCCB createInstance() {
            return new SCCB();
        }

        /**
         * Creates an INSERT CQL builder for a single column.
         * 
         * <p>This method initializes an INSERT statement for one column. The column name will be
         * converted according to the snake_case naming policy.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = SCCB.insert("firstName")
         *                  .into("account")
         *                  .values("John")
         *                  .build().query();
         * // Output: INSERT INTO account (first_name) VALUES ('John')
         * }</pre>
         *
         * @param expr the column name or expression
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if expr is null or empty
         */
        public static CqlBuilder insert(final String expr) {
            N.checkArgNotEmpty(expr, INSERTION_PART_MSG);

            return insert(N.asArray(expr));
        }

        /**
         * Creates an INSERT CQL builder for multiple columns.
         * 
         * <p>This method initializes an INSERT statement for multiple columns. All column names
         * will be converted according to the snake_case naming policy.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = SCCB.insert("firstName", "lastName", "email")
         *                  .into("account")
         *                  .values("John", "Doe", "john@example.com")
         *                  .build().query();
         * // Output: INSERT INTO account (first_name, last_name, email) VALUES ('John', 'Doe', 'john@example.com')
         * }</pre>
         *
         * @param propOrColumnNames the property or column names to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder insert(final String... propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._propOrColumnNames = Array.asList(propOrColumnNames);

            return instance;
        }

        /**
         * Creates an INSERT CQL builder for a collection of columns.
         * 
         * <p>This method is useful when column names are determined dynamically. The collection
         * can contain property names that will be converted to column names.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("firstName", "lastName", "email");
         * String cql = SCCB.insert(columns)
         *                  .into("account")
         *                  .values("John", "Doe", "john@example.com")
         *                  .build().query();
         * // Output: INSERT INTO account (first_name, last_name, email) VALUES ('John', 'Doe', 'john@example.com')
         * }</pre>
         *
         * @param propOrColumnNames the collection of property or column names to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder insert(final Collection<String> propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._propOrColumnNames = propOrColumnNames;

            return instance;
        }

        /**
         * Creates an INSERT CQL builder with column-value mappings.
         * 
         * <p>This method allows specifying both column names and their values together. The map keys
         * represent column names (which will be converted to snake_case) and values are the data
         * to insert.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Object> props = N.asMap("firstName", "John", "age", 25);
         * String cql = SCCB.insert(props).into("account").build().query();
         * // Output: INSERT INTO account (first_name, age) VALUES ('John', 25)
         * }</pre>
         *
         * @param props map of property names to values
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if props is null or empty
         */
        public static CqlBuilder insert(final Map<String, Object> props) {
            N.checkArgNotEmpty(props, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._props = props;

            return instance;
        }

        /**
         * Creates an INSERT CQL builder from an entity object.
         * 
         * <p>This method extracts values from the entity's properties and creates an INSERT statement.
         * Properties marked with @Transient, @ReadOnly, or similar annotations are automatically excluded.
         * Property names are converted to snake_case column names.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Account account = new Account("John", "john@email.com");
         * String cql = SCCB.insert(account).into("account").build().query();
         * // Output: INSERT INTO account (first_name, email) VALUES ('John', 'john@email.com')
         * }</pre>
         *
         * @param entity the entity object to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entity is null
         */
        public static CqlBuilder insert(final Object entity) {
            return insert(entity, null);
        }

        /**
         * Creates an INSERT CQL builder from an entity object, excluding specified properties.
         * 
         * <p>This method allows selective insertion of entity properties. In addition to properties
         * automatically excluded by annotations, you can specify additional properties to exclude.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Account account = new Account("John", "john@email.com");
         * Set<String> excluded = N.asSet("createdDate");
         * String cql = SCCB.insert(account, excluded).into("account").build().query();
         * // Output: INSERT INTO account (first_name, email) VALUES ('John', 'john@email.com')
         * }</pre>
         *
         * @param entity the entity object to insert
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entity is null
         */
        public static CqlBuilder insert(final Object entity, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entity, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance.setEntityClass(entity.getClass());

            parseInsertEntity(instance, entity, excludedPropNames);

            return instance;
        }

        /**
         * Creates an INSERT CQL builder from an entity class.
         * 
         * <p>This method generates an INSERT template for all insertable properties of the class.
         * Properties marked with @ReadOnly, @ReadOnlyId, or @Transient are automatically excluded.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = SCCB.insert(Account.class).into("account").build().query();
         * // Output: INSERT INTO account (first_name, last_name, email, status)
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insert(final Class<?> entityClass) {
            return insert(entityClass, null);
        }

        /**
         * Creates an INSERT CQL builder from an entity class, excluding specified properties.
         * 
         * <p>This method generates an INSERT template excluding both annotation-based exclusions
         * and the specified properties. Useful for creating templates where certain fields
         * are populated by database defaults or triggers.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("id", "createdDate");
         * String cql = SCCB.insert(Account.class, excluded).into("account").build().query();
         * // Output: INSERT INTO account (first_name, last_name, email)
         * }</pre>
         *
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insert(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = QueryUtil.getInsertPropNames(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates an INSERT INTO CQL builder for an entity class.
         * 
         * <p>This convenience method combines insert() and into() operations. The table name
         * is derived from the entity class using the naming policy.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = SCCB.insertInto(Account.class)
         *                  .values("John", "Doe", "john@example.com")
         *                  .build().query();
         * // Output: INSERT INTO account (first_name, last_name, email) VALUES ('John', 'Doe', 'john@example.com')
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insertInto(final Class<?> entityClass) {
            return insertInto(entityClass, null);
        }

        /**
         * Creates an INSERT INTO CQL builder for an entity class, excluding specified properties.
         * 
         * <p>This convenience method combines insert() and into() operations with property exclusion.
         * The table name is derived from the entity class.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("id");
         * String cql = SCCB.insertInto(Account.class, excluded)
         *                  .values("John", "Doe", "john@example.com")
         *                  .build().query();
         * // Output: INSERT INTO account (first_name, last_name, email) VALUES ('John', 'Doe', 'john@example.com')
         * }</pre>
         *
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insertInto(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return insert(entityClass, excludedPropNames).into(entityClass);
        }

        /**
         * Creates a batch INSERT CQL builder for multiple entities or property maps.
         * 
         * <p>This method generates MyCQL-style batch insert CQL for efficient bulk inserts.
         * All entities or maps in the collection must have the same structure (same properties).</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<Account> accounts = Arrays.asList(
         *     new Account("John", "john@email.com"),
         *     new Account("Jane", "jane@email.com")
         * );
         * String cql = SCCB.batchInsert(accounts).into("account").build().query();
         * // Output: INSERT INTO account (first_name, email) VALUES ('John', 'john@email.com'), ('Jane', 'jane@email.com')
         * }</pre>
         *
         * @param propsList collection of entities or property maps to insert
         * @return a new CqlBuilder instance configured for batch INSERT operation
         * @throws IllegalArgumentException if propsList is null or empty
         * <p><b>Note:</b> This is a beta feature and may be subject to change</p>
         */
        @Beta
        public static CqlBuilder batchInsert(final Collection<?> propsList) {
            N.checkArgNotEmpty(propsList, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            final Optional<?> first = N.firstNonNull(propsList);

            if (first.isPresent() && Beans.isBeanClass(first.get().getClass())) {
                instance.setEntityClass(first.get().getClass());
            }

            instance._propsList = toInsertPropsList(propsList);

            return instance;
        }

        /**
         * Creates an UPDATE CQL builder for a table.
         * 
         * <p>This method starts building an UPDATE statement for the specified table.
         * The SET clause should be added using the set() method.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = SCCB.update("account")
         *                  .set("status", "'ACTIVE'")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: UPDATE account SET status = 'ACTIVE' WHERE id = 1
         * }</pre>
         *
         * @param tableName the name of the table to update
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder update(final String tableName) {
            N.checkArgNotEmpty(tableName, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;

            return instance;
        }

        /**
         * Creates an UPDATE CQL builder for a table with entity class context.
         * 
         * <p>This method provides entity class information for property-to-column name mapping
         * when building the UPDATE statement.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = SCCB.update("account", Account.class)
         *                  .set("firstName", "'Jane'")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: UPDATE account SET first_name = 'Jane' WHERE id = 1
         * }</pre>
         *
         * @param tableName the name of the table to update
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if tableName is null or empty, or entityClass is null
         */
        public static CqlBuilder update(final String tableName, final Class<?> entityClass) {
            N.checkArgNotEmpty(tableName, UPDATE_PART_MSG);
            N.checkArgNotNull(entityClass, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;
            instance.setEntityClass(entityClass);

            return instance;
        }

        /**
         * Creates an UPDATE CQL builder for an entity class.
         * 
         * <p>This method derives the table name from the entity class and includes all
         * updatable properties. Properties marked with @NonUpdatable or @ReadOnly are excluded.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = SCCB.update(Account.class)
         *                  .set("status", "'INACTIVE'")
         *                  .where(Filters.lt("lastLogin", "2023-01-01"))
         *                  .build().query();
         * // Output: UPDATE account SET status = 'INACTIVE' WHERE last_login < '2023-01-01'
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder update(final Class<?> entityClass) {
            return update(entityClass, null);
        }

        /**
         * Creates an UPDATE CQL builder for an entity class, excluding specified properties.
         * 
         * <p>This method allows additional property exclusions beyond those marked with
         * annotations. Useful for partial updates or when certain fields should not be modified.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("email", "createdDate");
         * String cql = SCCB.update(Account.class, excluded)
         *                  .set("status", "'ACTIVE'")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: UPDATE account SET status = 'ACTIVE' WHERE id = 1
         * }</pre>
         *
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the update
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder update(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance.setEntityClass(entityClass);
            instance._tableName = getTableName(entityClass, instance._namingPolicy);
            instance._propOrColumnNames = QueryUtil.getUpdatePropNames(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for a single column or expression.
         *
         * <p>This method initializes a DELETE statement for one column. The column name will be
         * converted according to the snake_case naming policy. This is useful for deleting
         * specific columns from a row rather than the entire row.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = SCCB.delete("firstName")
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE first_name FROM account WHERE id = 1
         * }</pre>
         *
         * @param expr the column name or expression to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if expr is null or empty
         */
        public static CqlBuilder delete(final String expr) {
            N.checkArgNotEmpty(expr, DELETION_PART_MSG);

            return delete(N.asArray(expr));
        }

        /**
         * Creates a DELETE CQL builder for multiple columns.
         *
         * <p>This method initializes a DELETE statement for multiple columns. All column names
         * will be converted according to the snake_case naming policy. This allows selective
         * deletion of specific columns from rows.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = SCCB.delete("firstName", "lastName", "email")
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE first_name, last_name, email FROM account WHERE id = 1
         * }</pre>
         *
         * @param columnNames the column names to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if columnNames is null or empty
         */
        public static CqlBuilder delete(final String... columnNames) {
            N.checkArgNotEmpty(columnNames, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._propOrColumnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for a collection of columns.
         *
         * <p>This method is useful when column names are determined dynamically. The collection
         * can contain property names that will be converted to column names according to the
         * snake_case naming policy.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("firstName", "lastName", "email");
         * String cql = SCCB.delete(columns)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE first_name, last_name, email FROM account WHERE id = 1
         * }</pre>
         *
         * @param columnNames the collection of column names to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if columnNames is null or empty
         */
        public static CqlBuilder delete(final Collection<String> columnNames) {
            N.checkArgNotEmpty(columnNames, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._propOrColumnNames = columnNames;

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for an entity class.
         *
         * <p>This method generates a DELETE statement for all deletable properties of the class.
         * Properties marked with @ReadOnly, @ReadOnlyId, or @Transient are automatically excluded.
         * This is useful for creating templates for partial row deletion.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = SCCB.delete(Account.class)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE first_name, last_name, email, status FROM account WHERE id = 1
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder delete(final Class<?> entityClass) {
            return delete(entityClass, null);
        }

        /**
         * Creates a DELETE CQL builder for an entity class, excluding specified properties.
         *
         * <p>This method generates a DELETE statement excluding both annotation-based exclusions
         * and the specified properties. Useful for selective column deletion where certain
         * fields should be preserved.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("email", "createdDate");
         * String cql = SCCB.delete(Account.class, excluded)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE first_name, last_name, status FROM account WHERE id = 1
         * }</pre>
         *
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder delete(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates a DELETE FROM CQL builder for a table.
         * 
         * <p>This method starts building a DELETE statement for the specified table.
         * A WHERE clause should typically be added to avoid deleting all rows.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = SCCB.deleteFrom("account")
         *                  .where(Filters.eq("status", "'DELETED'"))
         *                  .build().query();
         * // Output: DELETE FROM account WHERE status = 'DELETED'
         * }</pre>
         *
         * @param tableName the name of the table to delete from
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder deleteFrom(final String tableName) {
            N.checkArgNotEmpty(tableName, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;

            return instance;
        }

        /**
         * Creates a DELETE FROM CQL builder for a table with entity class context.
         * 
         * <p>This method provides entity class information for property-to-column name mapping
         * in the WHERE clause of the DELETE statement.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = SCCB.deleteFrom("account", Account.class)
         *                  .where(Filters.eq("status", "'INACTIVE'"))
         *                  .build().query();
         * // Output: DELETE FROM account WHERE status = 'INACTIVE'
         * }</pre>
         *
         * @param tableName the name of the table to delete from
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if tableName is null or empty, or entityClass is null
         */
        public static CqlBuilder deleteFrom(final String tableName, final Class<?> entityClass) {
            N.checkArgNotEmpty(tableName, DELETION_PART_MSG);
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;
            instance.setEntityClass(entityClass);

            return instance;
        }

        /**
         * Creates a DELETE FROM CQL builder for an entity class.
         * 
         * <p>This method derives the table name from the entity class using the naming policy.
         * Always add a WHERE clause to avoid accidentally deleting all rows.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = SCCB.deleteFrom(Account.class)
         *                  .where(Filters.and(
         *                      Filters.eq("status", "'INACTIVE'"),
         *                      Filters.lt("lastLogin", "2022-01-01")
         *                  ))
         *                  .build().query();
         * // Output: DELETE FROM account WHERE status = 'INACTIVE' AND last_login < '2022-01-01'
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder deleteFrom(final Class<?> entityClass) {
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance.setEntityClass(entityClass);
            instance._tableName = getTableName(entityClass, instance._namingPolicy);

            return instance;
        }

        /**
         * Creates a SELECT CQL builder with a custom select expression.
         * 
         * <p>This method allows complex SELECT expressions including aggregate functions,
         * calculated columns, or any valid CQL select expression.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = SCCB.select("COUNT(DISTINCT customer_id)")
         *                  .from("orders")
         *                  .where(Filters.between("order_date", "2023-01-01", "2023-12-31"))
         *                  .build().query();
         * // Output: SELECT COUNT(DISTINCT customer_id) FROM orders WHERE order_date BETWEEN '2023-01-01' AND '2023-12-31'
         * }</pre>
         *
         * @param selectPart the select expression
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if selectPart is null or empty
         */
        public static CqlBuilder select(final String selectPart) {
            N.checkArgNotEmpty(selectPart, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = Array.asList(selectPart);
            return instance;
        }

        /**
         * Creates a SELECT CQL builder for multiple columns.
         * 
         * <p>This method builds a SELECT statement with the specified columns. Property names
         * are converted to column names using the snake_case naming policy.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = SCCB.select("firstName", "lastName", "email")
         *                  .from("account")
         *                  .where(Filters.eq("status", "'ACTIVE'"))
         *                  .build().query();
         * // Output: SELECT first_name AS "firstName", last_name AS "lastName", email FROM account WHERE status = 'ACTIVE'
         * }</pre>
         *
         * @param propOrColumnNames the property or column names to select
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder select(final String... propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = Array.asList(propOrColumnNames);

            return instance;
        }

        /**
         * Creates a SELECT CQL builder for a collection of columns.
         * 
         * <p>This method is useful when column names are determined dynamically at runtime.
         * The collection can contain property names that will be converted to column names.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("firstName", "lastName", "email");
         * String cql = SCCB.select(columns)
         *                  .from("account")
         *                  .where(Filters.eq("status", "'ACTIVE'"))
         *                  .build().query();
         * // Output: SELECT first_name AS "firstName", last_name AS "lastName", email FROM account WHERE status = 'ACTIVE'
         * }</pre>
         *
         * @param propOrColumnNames the collection of property or column names to select
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder select(final Collection<String> propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = propOrColumnNames;

            return instance;
        }

        /**
         * Creates a SELECT CQL builder with column aliases.
         * 
         * <p>This method allows specifying custom aliases for each selected column. The map keys
         * are column names (converted to snake_case) and values are the aliases to use.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, String> aliases = N.asMap(
         *     "firstName", "fname",
         *     "lastName", "lname"
         * );
         * String cql = SCCB.select(aliases).from("account").build().query();
         * // Output: SELECT first_name AS fname, last_name AS lname FROM account
         * }</pre>
         *
         * @param propOrColumnNameAliases map of column names to their aliases
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if propOrColumnNameAliases is null or empty
         */
        public static CqlBuilder select(final Map<String, String> propOrColumnNameAliases) {
            N.checkArgNotEmpty(propOrColumnNameAliases, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNameAliases = propOrColumnNameAliases;

            return instance;
        }

        /**
         * Creates a SELECT CQL builder for all properties of an entity class.
         * 
         * <p>This method generates a SELECT statement including all properties of the entity class,
         * excluding those marked with @Transient annotation.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = SCCB.select(Account.class)
         *                  .from("account")
         *                  .build().query();
         * // Output: SELECT id, first_name AS "firstName", last_name AS "lastName", email FROM account
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass) {
            return select(entityClass, false);
        }

        /**
         * Creates a SELECT CQL builder for an entity class with optional sub-entity properties.
         * 
         * <p>When includeSubEntityProperties is true, properties from related entities (marked with
         * appropriate annotations) will also be included in the SELECT statement.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = SCCB.select(Order.class, true)
         *                  .from("orders")
         *                  .build().query();
         * // Output includes both Order properties and related Customer properties
         * }</pre>
         *
         * @param entityClass the entity class
         * @param includeSubEntityProperties whether to include properties from related entities
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass, final boolean includeSubEntityProperties) {
            return select(entityClass, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT CQL builder for an entity class, excluding specified properties.
         * 
         * <p>This method allows selective property selection by excluding certain properties
         * from the SELECT statement.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("password", "salt");
         * String cql = SCCB.select(Account.class, excluded)
         *                  .from("account")
         *                  .build().query();
         * // Output: SELECT id, first_name AS "firstName", last_name AS "lastName", email FROM account
         * }</pre>
         *
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return select(entityClass, false, excludedPropNames);
        }

        /**
         * Creates a SELECT CQL builder for an entity class with full control over property inclusion.
         * 
         * <p>This method provides complete control over which properties to include in the SELECT
         * statement, with options for sub-entity properties and property exclusion.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("internalData");
         * String cql = SCCB.select(Order.class, true, excluded)
         *                  .from("orders")
         *                  .build().query();
         * // Output includes Order and sub-entity properties, excluding internalData
         * }</pre>
         *
         * @param entityClass the entity class
         * @param includeSubEntityProperties whether to include properties from related entities
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass, final boolean includeSubEntityProperties, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = QueryUtil.getSelectPropNames(entityClass, includeSubEntityProperties, excludedPropNames);

            return instance;
        }

        /**
         * Creates a SELECT FROM CQL builder for an entity class.
         * 
         * <p>This convenience method combines select() and from() operations. The table name
         * is automatically derived from the entity class.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = SCCB.selectFrom(Account.class)
         *                  .where(Filters.eq("status", "'ACTIVE'"))
         *                  .build().query();
         * // Output: SELECT id, first_name AS "firstName", last_name AS "lastName", email FROM account WHERE status = 'ACTIVE'
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, false);
        }

        /**
         * Creates a SELECT FROM CQL builder for an entity class with table alias.
         * 
         * <p>This method allows specifying a table alias for use in complex queries with joins
         * or subqueries.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = SCCB.selectFrom(Account.class, "a")
         *                  .innerJoin("orders", "o").on("a.id = o.account_id")
         *                  .build().query();
         * // Output: SELECT a.id, a.first_name AS "firstName" ... FROM account a INNER JOIN orders o ON a.id = o.account_id
         * }</pre>
         *
         * @param entityClass the entity class
         * @param alias the table alias
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias) {
            return selectFrom(entityClass, alias, false);
        }

        /**
         * Creates a SELECT FROM CQL builder with sub-entity inclusion option.
         * 
         * <p>This convenience method combines select() and from() operations with the option
         * to include properties from related entities.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = SCCB.selectFrom(Order.class, true)
         *                  .where(Filters.gt("totalAmount", 100))
         *                  .build().query();
         * // Output includes automatic joins for sub-entities
         * }</pre>
         *
         * @param entityClass the entity class
         * @param includeSubEntityProperties whether to include properties from related entities
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final boolean includeSubEntityProperties) {
            return selectFrom(entityClass, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT FROM CQL builder with alias and sub-entity inclusion option.
         * 
         * <p>This method combines table aliasing with sub-entity property inclusion for
         * complex query construction.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = SCCB.selectFrom(Order.class, "o", true)
         *                  .where(Filters.eq("o.status", "'COMPLETED'"))
         *                  .build().query();
         * // Output includes aliased columns and joins for sub-entities
         * }</pre>
         *
         * @param entityClass the entity class
         * @param alias the table alias
         * @param includeSubEntityProperties whether to include properties from related entities
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final boolean includeSubEntityProperties) {
            return selectFrom(entityClass, alias, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT FROM CQL builder excluding specified properties.
         * 
         * <p>This convenience method combines select() and from() operations while excluding
         * certain properties from the selection.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("largeBlob", "metadata");
         * String cql = SCCB.selectFrom(Account.class, excluded)
         *                  .where(Filters.eq("active", true))
         *                  .build().query();
         * // Output: SELECT id, first_name AS "firstName", last_name AS "lastName", email FROM account WHERE active = true
         * }</pre>
         *
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, false, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM CQL builder with alias, excluding specified properties.
         * 
         * <p>This method provides aliasing capability while excluding specified properties
         * from the SELECT statement.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("internalCode");
         * String cql = SCCB.selectFrom(Account.class, "a", excluded)
         *                  .innerJoin("orders", "o").on("a.id = o.account_id")
         *                  .build().query();
         * // Output uses alias "a" and excludes internalCode property
         * }</pre>
         *
         * @param entityClass the entity class
         * @param alias the table alias
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, alias, false, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM CQL builder with sub-entity inclusion and property exclusion.
         * 
         * <p>This method provides a convenient way to create a complete SELECT FROM statement
         * with control over sub-entities and property exclusion.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("deletedFlag");
         * String cql = SCCB.selectFrom(Order.class, true, excluded)
         *                  .where(Filters.gt("createdDate", "2023-01-01"))
         *                  .build().query();
         * // Output includes Order with Customer sub-entity, excluding deletedFlag
         * }</pre>
         *
         * @param entityClass the entity class
         * @param includeSubEntityProperties whether to include properties from related entities
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final boolean includeSubEntityProperties, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, QueryUtil.getTableAlias(entityClass), includeSubEntityProperties, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM CQL builder with full control over all options.
         * 
         * <p>This method provides complete control over the SELECT FROM statement generation,
         * including table alias, sub-entity properties, and property exclusion. When sub-entities
         * are included, appropriate joins will be generated automatically.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("debugInfo");
         * String cql = SCCB.selectFrom(Order.class, "ord", true, excluded)
         *                  .where(Filters.gt("ord.totalAmount", 1000))
         *                  .build().query();
         * // Output: Complex SELECT with alias, sub-entities, and exclusions
         * }</pre>
         *
         * @param entityClass the entity class
         * @param alias the table alias
         * @param includeSubEntityProperties whether to include properties from related entities
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final boolean includeSubEntityProperties,
                final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            if (hasSubEntityToInclude(entityClass, includeSubEntityProperties)) {
                final List<String> selectTableNames = getSelectTableNames(entityClass, alias, excludedPropNames, NamingPolicy.SNAKE_CASE);
                //noinspection ConstantValue
                return select(entityClass, includeSubEntityProperties, excludedPropNames).from(entityClass, selectTableNames);
            }

            return select(entityClass, includeSubEntityProperties, excludedPropNames).from(entityClass, alias);
        }

        /**
         * Creates a COUNT(*) CQL builder for a table.
         * 
         * <p>This is a convenience method for creating COUNT queries to get the total number
         * of rows in a table.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = SCCB.count("account")
         *                  .where(Filters.eq("status", "'ACTIVE'"))
         *                  .build().query();
         * // Output: SELECT count(*) FROM account WHERE status = 'ACTIVE'
         * }</pre>
         *
         * @param tableName the name of the table to count rows from
         * @return a new CqlBuilder instance configured for COUNT operation
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder count(final String tableName) {
            N.checkArgNotEmpty(tableName, SELECTION_PART_MSG);

            return select(COUNT_ALL_LIST).from(tableName);
        }

        /**
         * Creates a COUNT(*) CQL builder for an entity class.
         * 
         * <p>This method derives the table name from the entity class and creates a COUNT query.
         * Useful for getting row counts with type-safe table name resolution.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = SCCB.count(Account.class)
         *                  .where(Filters.between("createdDate", "2023-01-01", "2023-12-31"))
         *                  .build().query();
         * // Output: SELECT count(*) FROM account WHERE created_date BETWEEN '2023-01-01' AND '2023-12-31'
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for COUNT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder count(final Class<?> entityClass) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            return select(COUNT_ALL_LIST).from(entityClass);
        }

        /**
         * Parses a condition into CQL with entity class context.
         * 
         * <p>This method is used to generate CQL fragments for conditions only, without
         * building a complete CQL statement. It's useful for debugging conditions or
         * building dynamic query parts that will be combined later.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Condition cond = Filters.and(
         *     Filters.eq("status", "'ACTIVE'"),
         *     Filters.gt("balance", 1000)
         * );
         * String cql = SCCB.parse(cond, Account.class).build().query();
         * // Output: status = 'ACTIVE' AND balance > 1000
         * }</pre>
         *
         * @param cond the condition to parse
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance containing the parsed condition
         * @throws IllegalArgumentException if cond is null
         */
        public static CqlBuilder parse(final Condition cond, final Class<?> entityClass) {
            N.checkArgNotNull(cond, "cond");

            final CqlBuilder instance = createInstance();

            instance.setEntityClass(entityClass);
            instance._op = OperationType.QUERY;
            instance._isForConditionOnly = true;
            instance.append(cond);

            return instance;
        }
    }

    /**
     * Un-parameterized CQL builder with all capital case (upper case with underscore) field/column naming strategy.
     * 
     * <p>This builder generates CQL with actual values embedded directly in the CQL string. Property names
     * are automatically converted from camelCase to SCREAMING_SNAKE_CASE for database column names.</p>
     * 
     * <p>Features:</p>
     * <ul>
     *   <li>Converts "firstName" to "FIRST_NAME"</li>
     *   <li>Generates non-parameterized CQL (values embedded directly)</li>
     *   <li>Suitable for databases that use uppercase column names</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ACSB.select("firstName", "lastName").from("account").where(Filters.eq("id", 1)).build().query();
     * // Output: SELECT FIRST_NAME AS "firstName", LAST_NAME AS "lastName" FROM ACCOUNT WHERE ID = 1
     * }</pre>
     *
     * @deprecated {@code PAC or NAC} is preferred for better security and performance. 
     *             Un-parameterized CQL is vulnerable to CQL injection attacks.
     */
    @Deprecated
    public static class ACCB extends CqlBuilder {

        /**
         * Constructs a new ACCB instance with SCREAMING_SNAKE_CASE naming policy and non-parameterized CQL policy.
         * 
         * <p>This constructor is package-private and should not be called directly. Use the static
         * factory methods like {@link #select(String...)}, {@link #insert(String...)}, etc. instead.</p>
         */
        ACCB() {
            super(NamingPolicy.SCREAMING_SNAKE_CASE, SQLPolicy.RAW_SQL);
        }

        /**
         * Creates a new instance of ACCB.
         *
         * <p>This factory method is used internally by the static methods to create new builder instances.
         * Each CQL building operation starts with a fresh instance to ensure thread safety.</p>
         *
         * @return a new ACCB instance
         */
        protected static ACCB createInstance() {
            return new ACCB();
        }

        /**
         * Creates an INSERT CQL builder for a single column.
         * 
         * <p>This method initializes an INSERT statement for one column. The column name will be
         * converted to SCREAMING_SNAKE_CASE according to the naming policy.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = ACSB.insert("firstName")
         *                  .into("users")
         *                  .values("John")
         *                  .build().query();
         * // Output: INSERT INTO USERS (FIRST_NAME) VALUES ('John')
         * }</pre>
         *
         * @param expr the column name or expression to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if expr is null or empty
         */
        public static CqlBuilder insert(final String expr) {
            N.checkArgNotEmpty(expr, INSERTION_PART_MSG);

            return insert(N.asArray(expr));
        }

        /**
         * Creates an INSERT CQL builder for multiple columns.
         * 
         * <p>This method initializes an INSERT statement for multiple columns. All column names
         * will be converted to SCREAMING_SNAKE_CASE according to the naming policy.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = ACSB.insert("firstName", "lastName", "email")
         *                  .into("users")
         *                  .values("John", "Doe", "john@example.com")
         *                  .build().query();
         * // Output: INSERT INTO USERS (FIRST_NAME, LAST_NAME, EMAIL) VALUES ('John', 'Doe', 'john@example.com')
         * }</pre>
         *
         * @param propOrColumnNames the property or column names to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder insert(final String... propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._propOrColumnNames = Array.asList(propOrColumnNames);

            return instance;
        }

        /**
         * Creates an INSERT CQL builder for a collection of columns.
         * 
         * <p>This method is useful when column names are dynamically determined. The collection
         * can contain property names that will be converted to uppercase column names.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("firstName", "lastName", "email");
         * String cql = ACSB.insert(columns)
         *                  .into("users")
         *                  .values("John", "Doe", "john@example.com")
         *                  .build().query();
         * // Output: INSERT INTO USERS (FIRST_NAME, LAST_NAME, EMAIL) VALUES ('John', 'Doe', 'john@example.com')
         * }</pre>
         *
         * @param propOrColumnNames the collection of property or column names to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder insert(final Collection<String> propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._propOrColumnNames = propOrColumnNames;

            return instance;
        }

        /**
         * Creates an INSERT CQL builder with column-value mappings.
         * 
         * <p>This method allows direct specification of column names and their corresponding
         * values as a Map. Column names will be converted to uppercase.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Object> data = new HashMap<>();
         * data.put("firstName", "John");
         * data.put("age", 30);
         * String cql = ACSB.insert(data).into("users").build().query();
         * // Output: INSERT INTO USERS (FIRST_NAME, AGE) VALUES ('John', 30)
         * }</pre>
         *
         * @param props map of property names to values
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if props is null or empty
         */
        public static CqlBuilder insert(final Map<String, Object> props) {
            N.checkArgNotEmpty(props, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._props = props;

            return instance;
        }

        /**
         * Creates an INSERT CQL builder from an entity object.
         * 
         * <p>This method extracts property values from the given entity object and creates
         * an INSERT statement. Properties marked with @Transient, @ReadOnly, or similar 
         * annotations are automatically excluded. Property names are converted to uppercase.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User user = new User("John", 30, "john@example.com");
         * String cql = ACSB.insert(user).into("users").build().query();
         * // Output: INSERT INTO USERS (FIRST_NAME, AGE, EMAIL) VALUES ('John', 30, 'john@example.com')
         * }</pre>
         *
         * @param entity the entity object to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entity is null
         */
        public static CqlBuilder insert(final Object entity) {
            return insert(entity, null);
        }

        /**
         * Creates an INSERT CQL builder from an entity object, excluding specified properties.
         * 
         * <p>This method allows selective insertion of entity properties. In addition to properties
         * automatically excluded by annotations, you can specify additional properties to exclude.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User user = new User("John", 30, "john@example.com");
         * Set<String> excluded = new HashSet<>(Arrays.asList("createdDate", "modifiedDate"));
         * String cql = ACSB.insert(user, excluded).into("users").build().query();
         * // Output: INSERT INTO USERS (FIRST_NAME, AGE, EMAIL) VALUES ('John', 30, 'john@example.com')
         * }</pre>
         *
         * @param entity the entity object to insert
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entity is null
         */
        public static CqlBuilder insert(final Object entity, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entity, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance.setEntityClass(entity.getClass());

            parseInsertEntity(instance, entity, excludedPropNames);

            return instance;
        }

        /**
         * Creates an INSERT CQL builder from an entity class.
         * 
         * <p>This method generates an INSERT template for the specified entity class,
         * including all insertable properties. Properties marked with @ReadOnly, @ReadOnlyId,
         * or @Transient are automatically excluded.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = ACSB.insert(User.class).into("users").build().query();
         * // Output: INSERT INTO USERS (FIRST_NAME, LAST_NAME, AGE, EMAIL)
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insert(final Class<?> entityClass) {
            return insert(entityClass, null);
        }

        /**
         * Creates an INSERT CQL builder from an entity class, excluding specified properties.
         * 
         * <p>This method generates an INSERT template excluding both annotation-based exclusions
         * and the specified properties. Useful for creating templates where certain fields
         * are populated by database defaults or triggers.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = new HashSet<>(Arrays.asList("id", "createdDate"));
         * String cql = ACSB.insert(User.class, excluded).into("users").build().query();
         * // Output: INSERT INTO USERS (FIRST_NAME, LAST_NAME, AGE, EMAIL)
         * }</pre>
         *
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insert(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = QueryUtil.getInsertPropNames(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates an INSERT INTO CQL builder for an entity class.
         * 
         * <p>This is a convenience method that combines insert() and into() operations.
         * The table name is derived from the entity class name and converted to uppercase.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = ACSB.insertInto(User.class)
         *                  .values("John", "Doe", 30, "john@example.com")
         *                  .build().query();
         * // Output: INSERT INTO USER (FIRST_NAME, LAST_NAME, AGE, EMAIL) VALUES ('John', 'Doe', 30, 'john@example.com')
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for INSERT INTO operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insertInto(final Class<?> entityClass) {
            return insertInto(entityClass, null);
        }

        /**
         * Creates an INSERT INTO CQL builder for an entity class, excluding specified properties.
         * 
         * <p>This convenience method combines insert() and into() operations with property exclusion.
         * The table name is derived from the entity class and converted to uppercase.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = new HashSet<>(Arrays.asList("id"));
         * String cql = ACSB.insertInto(User.class, excluded)
         *                  .values("John", "Doe", 30, "john@example.com")
         *                  .build().query();
         * // Output: INSERT INTO USER (FIRST_NAME, LAST_NAME, AGE, EMAIL) VALUES ('John', 'Doe', 30, 'john@example.com')
         * }</pre>
         *
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance configured for INSERT INTO operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insertInto(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return insert(entityClass, excludedPropNames).into(entityClass);
        }

        /**
         * Creates a batch INSERT CQL builder for multiple entities or property maps.
         * 
         * <p>This method generates MyCQL-style batch insert CQL for efficient bulk inserts.
         * All entities or maps in the collection must have the same structure (same properties).</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<User> users = Arrays.asList(
         *     new User("John", 30, "john@example.com"),
         *     new User("Jane", 25, "jane@example.com")
         * );
         * String cql = ACSB.batchInsert(users).into("users").build().query();
         * // Output: INSERT INTO USERS (FIRST_NAME, AGE, EMAIL) VALUES 
         * //         ('John', 30, 'john@example.com'), 
         * //         ('Jane', 25, 'jane@example.com')
         * }</pre>
         *
         * @param propsList collection of entities or property maps to insert
         * @return a new CqlBuilder instance configured for batch INSERT operation
         * @throws IllegalArgumentException if propsList is null or empty
         * <p><b>Note:</b> This is a beta feature and may be subject to change</p>
         */
        @Beta
        public static CqlBuilder batchInsert(final Collection<?> propsList) {
            N.checkArgNotEmpty(propsList, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            final Optional<?> first = N.firstNonNull(propsList);

            if (first.isPresent() && Beans.isBeanClass(first.get().getClass())) {
                instance.setEntityClass(first.get().getClass());
            }

            instance._propsList = toInsertPropsList(propsList);

            return instance;
        }

        /**
         * Creates an UPDATE CQL builder for the specified table.
         * 
         * <p>This method initializes a new CqlBuilder for UPDATE operations on the
         * specified table. The columns to update should be specified using the
         * set() method.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = ACSB.update("users")
         *                  .set("LAST_NAME", "'Smith'")
         *                  .where(Filters.eq("ID", 123))
         *                  .build().query();
         * // Output: UPDATE USERS SET LAST_NAME = 'Smith' WHERE ID = 123
         * }</pre>
         *
         * @param tableName the name of the table to update
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder update(final String tableName) {
            N.checkArgNotEmpty(tableName, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;

            return instance;
        }

        /**
         * Creates an UPDATE CQL builder for a table with entity class context.
         * 
         * <p>This method provides entity class information for property-to-column name mapping
         * when building the UPDATE statement. Property names will be converted to uppercase.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = ACSB.update("users", User.class)
         *                  .set("age", 31)  // "age" is mapped to "AGE" column
         *                  .where(Filters.eq("firstName", "'John'"))
         *                  .build().query();
         * // Output: UPDATE USERS SET AGE = 31 WHERE FIRST_NAME = 'John'
         * }</pre>
         *
         * @param tableName the name of the table to update
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if tableName is null or empty, or entityClass is null
         */
        public static CqlBuilder update(final String tableName, final Class<?> entityClass) {
            N.checkArgNotEmpty(tableName, UPDATE_PART_MSG);
            N.checkArgNotNull(entityClass, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;
            instance.setEntityClass(entityClass);

            return instance;
        }

        /**
         * Creates an UPDATE CQL builder for an entity class.
         * 
         * <p>This method derives the table name from the entity class and includes all
         * updatable properties. Properties marked with @NonUpdatable or @ReadOnly are excluded.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = ACSB.update(User.class)
         *                  .set("age", 31)
         *                  .where(Filters.eq("firstName", "'John'"))
         *                  .build().query();
         * // Output: UPDATE USER SET AGE = 31 WHERE FIRST_NAME = 'John'
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder update(final Class<?> entityClass) {
            return update(entityClass, null);
        }

        /**
         * Creates an UPDATE CQL builder for an entity class, excluding specified properties.
         * 
         * <p>This method allows additional property exclusions beyond those marked with
         * annotations. Useful for partial updates or when certain fields should not be modified.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = new HashSet<>(Arrays.asList("id", "createdDate"));
         * String cql = ACSB.update(User.class, excluded)
         *                  .set("age", 31)
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: UPDATE USER SET AGE = 31 WHERE ID = 1
         * }</pre>
         *
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the update
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder update(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance.setEntityClass(entityClass);
            instance._tableName = getTableName(entityClass, instance._namingPolicy);
            instance._propOrColumnNames = QueryUtil.getUpdatePropNames(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for a single column or expression.
         *
         * <p>This method initializes a DELETE statement for one column. The column name will be
         * converted according to the snake_case naming policy. This is useful for deleting
         * specific columns from a row rather than the entire row.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = ACCB.delete("firstName")
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE FIRST_NAME FROM account WHERE ID = 1
         * }</pre>
         *
         * @param expr the column name or expression to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if expr is null or empty
         */
        public static CqlBuilder delete(final String expr) {
            N.checkArgNotEmpty(expr, DELETION_PART_MSG);

            return delete(N.asArray(expr));
        }

        /**
         * Creates a DELETE CQL builder for multiple columns.
         *
         * <p>This method initializes a DELETE statement for multiple columns. All column names
         * will be converted according to the snake_case naming policy. This allows selective
         * deletion of specific columns from rows.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = ACCB.delete("firstName", "lastName", "email")
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE FIRST_NAME, LAST_NAME, email FROM account WHERE ID = 1
         * }</pre>
         *
         * @param columnNames the column names to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if columnNames is null or empty
         */
        public static CqlBuilder delete(final String... columnNames) {
            N.checkArgNotEmpty(columnNames, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._propOrColumnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for a collection of columns.
         *
         * <p>This method is useful when column names are determined dynamically. The collection
         * can contain property names that will be converted to column names according to the
         * snake_case naming policy.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("firstName", "lastName", "email");
         * String cql = ACCB.delete(columns)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE FIRST_NAME, LAST_NAME, EMAIL FROM account WHERE ID = 1
         * }</pre>
         *
         * @param columnNames the collection of column names to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if columnNames is null or empty
         */
        public static CqlBuilder delete(final Collection<String> columnNames) {
            N.checkArgNotEmpty(columnNames, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._propOrColumnNames = columnNames;

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for an entity class.
         *
         * <p>This method generates a DELETE statement for all deletable properties of the class.
         * Properties marked with @ReadOnly, @ReadOnlyId, or @Transient are automatically excluded.
         * This is useful for creating templates for partial row deletion.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = ACCB.delete(Account.class)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE FIRST_NAME, LAST_NAME, EMAIL, STATUS FROM account WHERE ID = 1
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder delete(final Class<?> entityClass) {
            return delete(entityClass, null);
        }

        /**
         * Creates a DELETE CQL builder for an entity class, excluding specified properties.
         *
         * <p>This method generates a DELETE statement excluding both annotation-based exclusions
         * and the specified properties. Useful for selective column deletion where certain
         * fields should be preserved.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("email", "createdDate");
         * String cql = ACCB.delete(Account.class, excluded)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE FIRST_NAME, LAST_NAME, STATUS FROM account WHERE ID = 1
         * }</pre>
         *
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder delete(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates a DELETE FROM CQL builder for the specified table.
         * 
         * <p>This method initializes a new CqlBuilder for DELETE operations on the
         * specified table. A WHERE clause should typically be added to avoid deleting all rows.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = ACSB.deleteFrom("users")
         *                  .where(Filters.lt("AGE", 18))
         *                  .build().query();
         * // Output: DELETE FROM USERS WHERE AGE < 18
         * }</pre>
         *
         * @param tableName the name of the table to delete from
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder deleteFrom(final String tableName) {
            N.checkArgNotEmpty(tableName, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;

            return instance;
        }

        /**
         * Creates a DELETE FROM CQL builder for a table with entity class context.
         * 
         * <p>This method provides entity class information for property-to-column name mapping
         * in the WHERE clause of the DELETE statement.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = ACSB.deleteFrom("users", User.class)
         *                  .where(Filters.eq("age", 18))  // "age" is mapped to "AGE" column
         *                  .build().query();
         * // Output: DELETE FROM USERS WHERE AGE = 18
         * }</pre>
         *
         * @param tableName the name of the table to delete from
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if tableName is null or empty, or entityClass is null
         */
        public static CqlBuilder deleteFrom(final String tableName, final Class<?> entityClass) {
            N.checkArgNotEmpty(tableName, DELETION_PART_MSG);
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;
            instance.setEntityClass(entityClass);

            return instance;
        }

        /**
         * Creates a DELETE FROM CQL builder for an entity class.
         * 
         * <p>This method derives the table name from the entity class name and converts it
         * to uppercase according to the naming policy.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = ACSB.deleteFrom(User.class)
         *                  .where(Filters.eq("ID", 1))
         *                  .build().query();
         * // Output: DELETE FROM USER WHERE ID = 1
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder deleteFrom(final Class<?> entityClass) {
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance.setEntityClass(entityClass);
            instance._tableName = getTableName(entityClass, instance._namingPolicy);

            return instance;
        }

        /**
         * Creates a SELECT CQL builder with a custom select expression.
         * 
         * <p>This method allows complex SELECT expressions including aggregate functions,
         * calculated columns, or any valid CQL select expression.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = ACSB.select("COUNT(*) as total, AVG(SALARY) as avgSalary")
         *                  .from("EMPLOYEES")
         *                  .build().query();
         * // Output: SELECT COUNT(*) as total, AVG(SALARY) as avgSalary FROM EMPLOYEES
         * }</pre>
         *
         * @param selectPart the select expression
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if selectPart is null or empty
         */
        public static CqlBuilder select(final String selectPart) {
            N.checkArgNotEmpty(selectPart, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = Array.asList(selectPart);
            return instance;
        }

        /**
         * Creates a SELECT CQL builder for the specified columns.
         * 
         * <p>This method builds a SELECT statement with the specified columns. Property names
         * are converted to uppercase column names using the SCREAMING_SNAKE_CASE naming policy.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = ACSB.select("firstName", "lastName", "age")
         *                  .from("users")
         *                  .where(Filters.gte("age", 18))
         *                  .build().query();
         * // Output: SELECT FIRST_NAME AS "firstName", LAST_NAME AS "lastName", AGE AS "age" 
         * //         FROM USERS WHERE AGE >= 18
         * }</pre>
         *
         * @param propOrColumnNames the property or column names to select
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder select(final String... propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = Array.asList(propOrColumnNames);

            return instance;
        }

        /**
         * Creates a SELECT CQL builder for a collection of columns.
         * 
         * <p>This method is useful when column names are determined dynamically at runtime.
         * The collection can contain property names that will be converted to uppercase column names.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = getRequiredColumns();   // returns ["firstName", "email"]
         * String cql = ACSB.select(columns)
         *                  .from("users")
         *                  .build().query();
         * // Output: SELECT FIRST_NAME AS "firstName", EMAIL AS "email" FROM USERS
         * }</pre>
         *
         * @param propOrColumnNames collection of property or column names to select
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder select(final Collection<String> propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = propOrColumnNames;

            return instance;
        }

        /**
         * Creates a SELECT CQL builder with column aliases.
         * 
         * <p>This method allows specifying custom aliases for each selected column. The map keys
         * are column names (converted to uppercase) and values are the aliases to use.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, String> aliases = new HashMap<>();
         * aliases.put("firstName", "fname");
         * aliases.put("lastName", "lname");
         * String cql = ACSB.select(aliases).from("users").build().query();
         * // Output: SELECT FIRST_NAME AS "fname", LAST_NAME AS "lname" FROM USERS
         * }</pre>
         *
         * @param propOrColumnNameAliases map of column names to their aliases
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if propOrColumnNameAliases is null or empty
         */
        public static CqlBuilder select(final Map<String, String> propOrColumnNameAliases) {
            N.checkArgNotEmpty(propOrColumnNameAliases, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNameAliases = propOrColumnNameAliases;

            return instance;
        }

        /**
         * Creates a SELECT CQL builder for all properties of an entity class.
         * 
         * <p>This method generates a SELECT statement including all properties of the entity class,
         * excluding those marked with @Transient annotation. Column names are converted to uppercase.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = ACSB.select(User.class)
         *                  .from("users")
         *                  .build().query();
         * // Output: SELECT ID AS "id", FIRST_NAME AS "firstName", LAST_NAME AS "lastName", AGE AS "age", EMAIL AS "email" FROM USERS
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass) {
            return select(entityClass, false);
        }

        /**
         * Creates a SELECT CQL builder for an entity class with sub-entity option.
         * 
         * <p>When includeSubEntityProperties is true, properties of nested entities are included.
         * This is useful for fetching related data in a single query.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = ACSB.select(Order.class, true)  // includes Customer sub-entity
         *                  .from("orders")
         *                  .build().query();
         * // Output includes both Order and nested Customer properties with uppercase column names
         * }</pre>
         *
         * @param entityClass the entity class
         * @param includeSubEntityProperties whether to include properties of sub-entities
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass, final boolean includeSubEntityProperties) {
            return select(entityClass, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT CQL builder for an entity class, excluding specified properties.
         * 
         * <p>This method allows selective property selection by excluding certain properties
         * from the SELECT statement.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = new HashSet<>(Arrays.asList("password", "secretKey"));
         * String cql = ACSB.select(User.class, excluded)
         *                  .from("users")
         *                  .build().query();
         * // Output: SELECT ID AS "id", FIRST_NAME AS "firstName", LAST_NAME AS "lastName", AGE AS "age", EMAIL AS "email" FROM USERS
         * }</pre>
         *
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return select(entityClass, false, excludedPropNames);
        }

        /**
         * Creates a SELECT CQL builder for an entity class with full control over property selection.
         * 
         * <p>This method combines sub-entity inclusion and property exclusion options, providing
         * complete control over which properties appear in the SELECT statement.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = new HashSet<>(Arrays.asList("internalNotes"));
         * String cql = ACSB.select(Order.class, true, excluded)
         *                  .from("orders")
         *                  .build().query();
         * // Output includes Order and Customer properties, excluding internalNotes
         * }</pre>
         *
         * @param entityClass the entity class
         * @param includeSubEntityProperties whether to include properties of sub-entities
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass, final boolean includeSubEntityProperties, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = QueryUtil.getSelectPropNames(entityClass, includeSubEntityProperties, excludedPropNames);

            return instance;
        }

        /**
         * Creates a complete SELECT FROM CQL builder for an entity class.
         * 
         * <p>This is a convenience method that combines select() and from() operations.
         * The table name is derived from the entity class and converted to uppercase.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = ACSB.selectFrom(User.class)
         *                  .where(Filters.gte("age", 18))
         *                  .build().query();
         * // Output: SELECT ID AS "id", FIRST_NAME AS "firstName", LAST_NAME AS "lastName", AGE AS "age", EMAIL AS "email" 
         * //         FROM USER WHERE AGE >= 18
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, false);
        }

        /**
         * Creates a SELECT FROM CQL builder with table alias for an entity class.
         * 
         * <p>The alias is used to qualify column names in complex queries with joins or subqueries.
         * The table name is derived from the entity class and converted to uppercase.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = ACSB.selectFrom(User.class, "u")
         *                  .where(Filters.gte("u.AGE", 18))
         *                  .build().query();
         * // Output: SELECT u.ID AS "id", u.FIRST_NAME AS "firstName", u.LAST_NAME AS "lastName", u.AGE AS "age", u.EMAIL AS "email" 
         * //         FROM USER u WHERE u.AGE >= 18
         * }</pre>
         *
         * @param entityClass the entity class
         * @param alias the table alias
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias) {
            return selectFrom(entityClass, alias, false);
        }

        /**
         * Creates a SELECT FROM CQL builder with sub-entity inclusion option.
         * 
         * <p>When includeSubEntityProperties is true, joins are added for sub-entities.
         * This provides a convenient way to fetch related data in a single query.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = ACSB.selectFrom(Order.class, true)
         *                  .where(Filters.eq("STATUS", "'ACTIVE'"))
         *                  .build().query();
         * // Output includes JOINs for sub-entities like Customer
         * }</pre>
         *
         * @param entityClass the entity class
         * @param includeSubEntityProperties whether to include and join sub-entities
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final boolean includeSubEntityProperties) {
            return selectFrom(entityClass, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT FROM CQL builder with alias and sub-entity options.
         * 
         * <p>This method combines table aliasing with sub-entity inclusion for building
         * complex queries with related data.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = ACSB.selectFrom(Order.class, "o", true)
         *                  .where(Filters.eq("o.STATUS", "'ACTIVE'"))
         *                  .build().query();
         * // Output includes aliased columns and JOINs for sub-entities
         * }</pre>
         *
         * @param entityClass the entity class
         * @param alias the table alias
         * @param includeSubEntityProperties whether to include and join sub-entities
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final boolean includeSubEntityProperties) {
            return selectFrom(entityClass, alias, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT FROM CQL builder with property exclusion.
         * 
         * <p>This method allows selective property selection with automatic FROM clause generation.
         * Properties in the excluded set will not appear in the SELECT statement.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = new HashSet<>(Arrays.asList("largeBlob", "metadata"));
         * String cql = ACSB.selectFrom(User.class, excluded)
         *                  .where(Filters.eq("ACTIVE", true))
         *                  .build().query();
         * // Output: SELECT ID AS "id", FIRST_NAME AS "firstName", LAST_NAME AS "lastName", AGE AS "age", EMAIL AS "email" 
         * //         FROM USER WHERE ACTIVE = true
         * }</pre>
         *
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, false, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM CQL builder with alias and property exclusion.
         * 
         * <p>This method combines aliasing with selective property selection for flexible
         * query construction.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = new HashSet<>(Arrays.asList("internalCode"));
         * String cql = ACSB.selectFrom(User.class, "u", excluded)
         *                  .innerJoin("ORDERS", "o").on("u.ID = o.USER_ID")
         *                  .build().query();
         * // Output uses alias "u" and excludes internalCode property
         * }</pre>
         *
         * @param entityClass the entity class
         * @param alias the table alias
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, alias, false, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM CQL builder with sub-entity and exclusion options.
         * 
         * <p>This method provides full control over entity selection including sub-entities
         * while allowing certain properties to be excluded.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = new HashSet<>(Arrays.asList("deletedFlag"));
         * String cql = ACSB.selectFrom(Order.class, true, excluded)
         *                  .where(Filters.gt("CREATED_DATE", "'2023-01-01'"))
         *                  .build().query();
         * // Output includes Order with Customer sub-entity, excluding deletedFlag
         * }</pre>
         *
         * @param entityClass the entity class
         * @param includeSubEntityProperties whether to include and join sub-entities
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final boolean includeSubEntityProperties, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, QueryUtil.getTableAlias(entityClass), includeSubEntityProperties, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM CQL builder with full control over all options.
         * 
         * <p>This is the most comprehensive selectFrom method providing complete control over
         * aliasing, sub-entity inclusion, and property exclusion. When sub-entities are included,
         * appropriate joins are generated automatically.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = new HashSet<>(Arrays.asList("debugInfo"));
         * String cql = ACSB.selectFrom(Order.class, "ord", true, excluded)
         *                  .where(Filters.gt("ord.TOTAL_AMOUNT", 1000))
         *                  .build().query();
         * // Output: Complex SELECT with alias, sub-entities, and exclusions
         * }</pre>
         *
         * @param entityClass the entity class
         * @param alias the table alias
         * @param includeSubEntityProperties whether to include and join sub-entities
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final boolean includeSubEntityProperties,
                final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            if (hasSubEntityToInclude(entityClass, includeSubEntityProperties)) {
                final List<String> selectTableNames = getSelectTableNames(entityClass, alias, excludedPropNames, NamingPolicy.SCREAMING_SNAKE_CASE);
                //noinspection ConstantValue
                return select(entityClass, includeSubEntityProperties, excludedPropNames).from(entityClass, selectTableNames);
            }

            return select(entityClass, includeSubEntityProperties, excludedPropNames).from(entityClass, alias);
        }

        /**
         * Creates a COUNT(*) CQL builder for a table.
         * 
         * <p>This is a convenience method for counting rows. The table name is
         * automatically converted to uppercase.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = ACSB.count("users")
         *                  .where(Filters.eq("ACTIVE", true))
         *                  .build().query();
         * // Output: SELECT count(*) FROM USERS WHERE ACTIVE = true
         * }</pre>
         *
         * @param tableName the name of the table to count rows from
         * @return a new CqlBuilder instance configured for COUNT operation
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder count(final String tableName) {
            N.checkArgNotEmpty(tableName, SELECTION_PART_MSG);

            return select(COUNT_ALL_LIST).from(tableName);
        }

        /**
         * Creates a COUNT(*) CQL builder for an entity class.
         * 
         * <p>The table name is derived from the entity class and converted to uppercase.
         * This provides type-safe row counting.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = ACSB.count(User.class)
         *                  .where(Filters.gte("AGE", 18))
         *                  .build().query();
         * // Output: SELECT count(*) FROM USER WHERE AGE >= 18
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for COUNT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder count(final Class<?> entityClass) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            return select(COUNT_ALL_LIST).from(entityClass);
        }

        /**
         * Parses a Condition object into CQL with entity class mapping.
         * 
         * <p>This method is used to generate CQL fragments from Condition objects.
         * Property names in the condition are converted to uppercase column names.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Condition cond = Filters.and(Filters.eq("firstName", "'John'"), Filters.gt("age", 18));
         * String cql = ACSB.parse(cond, User.class).build().query();
         * // Output: FIRST_NAME = 'John' AND AGE > 18
         * }</pre>
         *
         * @param cond the condition to parse
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance containing the parsed condition
         * @throws IllegalArgumentException if cond is null
         */
        public static CqlBuilder parse(final Condition cond, final Class<?> entityClass) {
            N.checkArgNotNull(cond, "cond");

            final CqlBuilder instance = createInstance();

            instance.setEntityClass(entityClass);
            instance._op = OperationType.QUERY;
            instance._isForConditionOnly = true;
            instance.append(cond);

            return instance;
        }
    }

    /**
     * CQL builder implementation with lower camel case naming policy.
     * 
     * <p>This builder generates CQL with actual values embedded directly in the CQL string.
     * Property names are preserved in camelCase format without conversion. This is useful
     * for databases that use camelCase column naming conventions.</p>
     * 
     * <p>Features:</p>
     * <ul>
     *   <li>Preserves "firstName" as "firstName" (no conversion)</li>
     *   <li>Generates non-parameterized CQL (values embedded directly)</li>
     *   <li>Suitable for databases with camelCase column names</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * LCSB.select("firstName", "lastName").from("userAccount").where(Filters.eq("userId", 1)).build().query();
     * // Output: SELECT firstName, lastName FROM userAccount WHERE userId = 1
     * }</pre>
     * 
     * @deprecated Use other naming policy implementations like {@link PSC}, {@link PAC}, {@link NSC} instead.
     *             Un-parameterized CQL is vulnerable to CQL injection attacks.
     */
    @Deprecated
    public static class LCCB extends CqlBuilder {

        /**
         * Constructs a new LCCB instance with CAMEL_CASE naming policy and non-parameterized CQL policy.
         * 
         * <p>This constructor is package-private and should not be called directly. Use the static
         * factory methods like {@link #select(String...)}, {@link #insert(String...)}, etc. instead.</p>
         */
        LCCB() {
            super(NamingPolicy.CAMEL_CASE, SQLPolicy.RAW_SQL);
        }

        /**
         * Creates a new instance of LCCB.
         *
         * <p>This factory method is used internally by the static methods to create new builder instances.
         * Each CQL building operation starts with a fresh instance to ensure thread safety.</p>
         *
         * @return a new LCCB instance
         */
        protected static LCCB createInstance() {
            return new LCCB();
        }

        /**
         * Creates an INSERT CQL builder for a single column expression.
         * 
         * <p>This method is a convenience wrapper that delegates to {@link #insert(String...)} 
         * with a single element array. Column names remain in camelCase format.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = LCSB.insert("userName")
         *                  .into("users")
         *                  .values("John")
         *                  .build().query();
         * // Output: INSERT INTO users (userName) VALUES ('John')
         * }</pre>
         * 
         * @param expr the column name or expression to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if expr is null or empty
         * 
         * @see #insert(String...)
         */
        public static CqlBuilder insert(final String expr) {
            N.checkArgNotEmpty(expr, INSERTION_PART_MSG);

            return insert(N.asArray(expr));
        }

        /**
         * Creates an INSERT CQL builder for the specified columns.
         * 
         * <p>This method initializes a new CqlBuilder for INSERT operations with the specified
         * column names. The actual values should be provided later using the VALUES clause.
         * Column names remain in camelCase format without conversion.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = LCSB.insert("firstName", "lastName", "email")
         *                  .into("users")
         *                  .values("John", "Doe", "john@example.com")
         *                  .build().query();
         * // Output: INSERT INTO users (firstName, lastName, email) VALUES ('John', 'Doe', 'john@example.com')
         * }</pre>
         * 
         * @param propOrColumnNames the property or column names to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder insert(final String... propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._propOrColumnNames = Array.asList(propOrColumnNames);

            return instance;
        }

        /**
         * Creates an INSERT CQL builder for the specified columns collection.
         * 
         * <p>This method is similar to {@link #insert(String...)} but accepts a Collection
         * of column names instead of varargs. Useful when column names are dynamically determined.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("firstName", "lastName", "email");
         * String cql = LCSB.insert(columns)
         *                  .into("users")
         *                  .values("John", "Doe", "john@example.com")
         *                  .build().query();
         * // Output: INSERT INTO users (firstName, lastName, email) VALUES ('John', 'Doe', 'john@example.com')
         * }</pre>
         * 
         * @param propOrColumnNames collection of property or column names to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder insert(final Collection<String> propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._propOrColumnNames = propOrColumnNames;

            return instance;
        }

        /**
         * Creates an INSERT CQL builder with property name-value pairs.
         * 
         * <p>This method allows direct specification of column names and their corresponding
         * values as a Map. Column names remain in camelCase format.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Object> props = new HashMap<>();
         * props.put("firstName", "John");
         * props.put("age", 30);
         * String cql = LCSB.insert(props).into("users").build().query();
         * // Output: INSERT INTO users (firstName, age) VALUES ('John', 30)
         * }</pre>
         * 
         * @param props map of property names to values
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if props is null or empty
         */
        public static CqlBuilder insert(final Map<String, Object> props) {
            N.checkArgNotEmpty(props, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._props = props;

            return instance;
        }

        /**
         * Creates an INSERT CQL builder from an entity object.
         * 
         * <p>This method extracts property values from the given entity object and creates
         * an INSERT statement. Properties marked with @Transient, @ReadOnly, or similar 
         * annotations are automatically excluded. Property names remain in camelCase.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User user = new User("John", "Doe", "john@example.com");
         * String cql = LCSB.insert(user).into("users").build().query();
         * // Output: INSERT INTO users (firstName, lastName, email) VALUES ('John', 'Doe', 'john@example.com')
         * }</pre>
         * 
         * @param entity the entity object to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entity is null
         * 
         * @see #insert(Object, Set)
         */
        public static CqlBuilder insert(final Object entity) {
            return insert(entity, null);
        }

        /**
         * Creates an INSERT CQL builder from an entity object with excluded properties.
         * 
         * <p>This method is similar to {@link #insert(Object)} but allows exclusion of
         * specific properties from the INSERT statement beyond those excluded by annotations.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User user = new User("John", "Doe", "john@example.com");
         * Set<String> excluded = new HashSet<>(Arrays.asList("createdDate", "modifiedDate"));
         * String cql = LCSB.insert(user, excluded).into("users").build().query();
         * // Output: INSERT INTO users (firstName, lastName, email) VALUES ('John', 'Doe', 'john@example.com')
         * }</pre>
         * 
         * @param entity the entity object to insert
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entity is null
         */
        public static CqlBuilder insert(final Object entity, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entity, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance.setEntityClass(entity.getClass());

            parseInsertEntity(instance, entity, excludedPropNames);

            return instance;
        }

        /**
         * Creates an INSERT CQL builder for an entity class.
         * 
         * <p>This method generates an INSERT template for the specified entity class,
         * including all insertable properties. Properties marked with @ReadOnly, @ReadOnlyId,
         * or @Transient are automatically excluded.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = LCSB.insert(User.class).into("users").build().query();
         * // Output: INSERT INTO users (firstName, lastName, email)
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entityClass is null
         * 
         * @see #insert(Class, Set)
         */
        public static CqlBuilder insert(final Class<?> entityClass) {
            return insert(entityClass, null);
        }

        /**
         * Creates an INSERT CQL builder for an entity class with excluded properties.
         * 
         * <p>This method generates an INSERT template for the specified entity class,
         * excluding the specified properties. Properties marked with {@link ReadOnly},
         * {@link ReadOnlyId}, or {@link com.landawn.abacus.annotation.Transient} annotations 
         * are automatically excluded.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = new HashSet<>(Arrays.asList("id", "createdDate"));
         * String cql = LCSB.insert(User.class, excluded).into("users").build().query();
         * // Output: INSERT INTO users (firstName, lastName, email)
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insert(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = QueryUtil.getInsertPropNames(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates an INSERT INTO CQL builder for an entity class.
         * 
         * <p>This is a convenience method that combines {@link #insert(Class)} and
         * {@link #into(Class)} operations. The table name is derived from the entity class.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = LCSB.insertInto(User.class)
         *                  .values("John", "Doe", "john@example.com")
         *                  .build().query();
         * // Output: INSERT INTO users (firstName, lastName, email) VALUES ('John', 'Doe', 'john@example.com')
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for INSERT INTO operation
         * @throws IllegalArgumentException if entityClass is null
         * 
         * @see #insertInto(Class, Set)
         */
        public static CqlBuilder insertInto(final Class<?> entityClass) {
            return insertInto(entityClass, null);
        }

        /**
         * Creates an INSERT INTO CQL builder for an entity class with excluded properties.
         * 
         * <p>This is a convenience method that combines {@link #insert(Class, Set)} and
         * {@link #into(Class)} operations. The table name is derived from the entity class.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = new HashSet<>(Arrays.asList("id"));
         * String cql = LCSB.insertInto(User.class, excluded)
         *                  .values("John", "Doe", "john@example.com")
         *                  .build().query();
         * // Output: INSERT INTO users (firstName, lastName, email) VALUES ('John', 'Doe', 'john@example.com')
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance configured for INSERT INTO operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insertInto(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return insert(entityClass, excludedPropNames).into(entityClass);
        }

        /**
         * Creates a batch INSERT CQL builder for multiple entities or property maps.
         * 
         * <p>This method generates MyCQL-style batch insert CQL for inserting multiple
         * rows in a single statement. The input collection can contain either entity
         * objects or Map instances. All items must have the same structure.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<User> users = Arrays.asList(
         *     new User("John", "Doe"),
         *     new User("Jane", "Smith")
         * );
         * String cql = LCSB.batchInsert(users).into("users").build().query();
         * // Output: INSERT INTO users (firstName, lastName) VALUES ('John', 'Doe'), ('Jane', 'Smith')
         * }</pre>
         * 
         * @param propsList collection of entities or property maps to insert
         * @return a new CqlBuilder instance configured for batch INSERT operation
         * @throws IllegalArgumentException if propsList is null or empty
         * <p><b>Note:</b> This is a beta feature and may be subject to change</p>
         */
        @Beta
        public static CqlBuilder batchInsert(final Collection<?> propsList) {
            N.checkArgNotEmpty(propsList, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            final Optional<?> first = N.firstNonNull(propsList);

            if (first.isPresent() && Beans.isBeanClass(first.get().getClass())) {
                instance.setEntityClass(first.get().getClass());
            }

            instance._propsList = toInsertPropsList(propsList);

            return instance;
        }

        /**
         * Creates an UPDATE CQL builder for the specified table.
         * 
         * <p>This method initializes a new CqlBuilder for UPDATE operations on the
         * specified table. The columns to update should be specified using the
         * {@code set()} method.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = LCSB.update("users")
         *                  .set("lastName", "'Smith'")
         *                  .where(Filters.eq("id", 123))
         *                  .build().query();
         * // Output: UPDATE users SET lastName = 'Smith' WHERE id = 123
         * }</pre>
         * 
         * @param tableName the name of the table to update
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder update(final String tableName) {
            N.checkArgNotEmpty(tableName, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;

            return instance;
        }

        /**
         * Creates an UPDATE CQL builder for the specified table with entity class context.
         * 
         * <p>This method is similar to {@link #update(String)} but also provides entity
         * class information for better type safety and property name mapping.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = LCSB.update("users", User.class)
         *                  .set("age", 31)
         *                  .where(Filters.eq("firstName", "'John'"))
         *                  .build().query();
         * // Output: UPDATE users SET age = 31 WHERE firstName = 'John'
         * }</pre>
         * 
         * @param tableName the name of the table to update
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if tableName is null or empty, or entityClass is null
         */
        public static CqlBuilder update(final String tableName, final Class<?> entityClass) {
            N.checkArgNotEmpty(tableName, UPDATE_PART_MSG);
            N.checkArgNotNull(entityClass, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;
            instance.setEntityClass(entityClass);

            return instance;
        }

        /**
         * Creates an UPDATE CQL builder for an entity class.
         * 
         * <p>This method derives the table name from the entity class and includes
         * all updatable properties. Properties marked with {@link NonUpdatable} or
         * {@link ReadOnly} are automatically excluded.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = LCSB.update(User.class)
         *                  .set("age", 31)
         *                  .where(Filters.eq("firstName", "'John'"))
         *                  .build().query();
         * // Output: UPDATE users SET age = 31 WHERE firstName = 'John'
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if entityClass is null
         * 
         * @see #update(Class, Set)
         */
        public static CqlBuilder update(final Class<?> entityClass) {
            return update(entityClass, null);
        }

        /**
         * Creates an UPDATE CQL builder for an entity class, excluding specified properties.
         * 
         * <p>This method generates an UPDATE template for the specified entity class,
         * excluding the specified properties. Properties marked with {@link ReadOnly},
         * {@link NonUpdatable}, or {@link com.landawn.abacus.annotation.Transient} 
         * annotations are automatically excluded.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = new HashSet<>(Arrays.asList("id", "createdDate"));
         * String cql = LCSB.update(User.class, excluded)
         *                  .set("firstName", "'John'")
         *                  .where(Filters.eq("id", 123))
         *                  .build().query();
         * // Output: UPDATE users SET firstName = 'John' WHERE id = 123
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the update
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder update(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance.setEntityClass(entityClass);
            instance._tableName = getTableName(entityClass, instance._namingPolicy);
            instance._propOrColumnNames = QueryUtil.getUpdatePropNames(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for a single column or expression.
         *
         * <p>This method initializes a DELETE statement for one column. The column name will be
         * converted according to the snake_case naming policy. This is useful for deleting
         * specific columns from a row rather than the entire row.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = LCCB.delete("firstName")
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE firstName FROM account WHERE id = 1
         * }</pre>
         *
         * @param expr the column name or expression to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if expr is null or empty
         */
        public static CqlBuilder delete(final String expr) {
            N.checkArgNotEmpty(expr, DELETION_PART_MSG);

            return delete(N.asArray(expr));
        }

        /**
         * Creates a DELETE CQL builder for multiple columns.
         *
         * <p>This method initializes a DELETE statement for multiple columns. All column names
         * will be converted according to the snake_case naming policy. This allows selective
         * deletion of specific columns from rows.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = LCCB.delete("firstName", "lastName", "email")
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE firstName, lastName, email FROM account WHERE id = 1
         * }</pre>
         *
         * @param columnNames the column names to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if columnNames is null or empty
         */
        public static CqlBuilder delete(final String... columnNames) {
            N.checkArgNotEmpty(columnNames, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._propOrColumnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for a collection of columns.
         *
         * <p>This method is useful when column names are determined dynamically. The collection
         * can contain property names that will be converted to column names according to the
         * snake_case naming policy.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("firstName", "lastName", "email");
         * String cql = LCCB.delete(columns)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE firstName, lastName, email FROM account WHERE id = 1
         * }</pre>
         *
         * @param columnNames the collection of column names to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if columnNames is null or empty
         */
        public static CqlBuilder delete(final Collection<String> columnNames) {
            N.checkArgNotEmpty(columnNames, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._propOrColumnNames = columnNames;

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for an entity class.
         *
         * <p>This method generates a DELETE statement for all deletable properties of the class.
         * Properties marked with @ReadOnly, @ReadOnlyId, or @Transient are automatically excluded.
         * This is useful for creating templates for partial row deletion.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = LCCB.delete(Account.class)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE firstName, lastName, email, status FROM account WHERE id = 1
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder delete(final Class<?> entityClass) {
            return delete(entityClass, null);
        }

        /**
         * Creates a DELETE CQL builder for an entity class, excluding specified properties.
         *
         * <p>This method generates a DELETE statement excluding both annotation-based exclusions
         * and the specified properties. Useful for selective column deletion where certain
         * fields should be preserved.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("email", "createdDate");
         * String cql = LCCB.delete(Account.class, excluded)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE firstName, lastName, status FROM account WHERE id = 1
         * }</pre>
         *
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder delete(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates a DELETE FROM CQL builder for the specified table.
         * 
         * <p>This method initializes a new CqlBuilder for DELETE operations on the
         * specified table. A WHERE clause should typically be added to avoid deleting all rows.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = LCSB.deleteFrom("users")
         *                  .where(Filters.eq("status", "'inactive'"))
         *                  .build().query();
         * // Output: DELETE FROM users WHERE status = 'inactive'
         * }</pre>
         * 
         * @param tableName the name of the table to delete from
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder deleteFrom(final String tableName) {
            N.checkArgNotEmpty(tableName, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;

            return instance;
        }

        /**
         * Creates a DELETE FROM CQL builder for the specified table with entity class context.
         * 
         * <p>This method is similar to {@link #deleteFrom(String)} but also provides entity
         * class information for better type safety in WHERE conditions.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = LCSB.deleteFrom("users", User.class)
         *                  .where(Filters.eq("age", 18))
         *                  .build().query();
         * // Output: DELETE FROM users WHERE age = 18
         * }</pre>
         * 
         * @param tableName the name of the table to delete from
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if tableName is null or empty, or entityClass is null
         */
        public static CqlBuilder deleteFrom(final String tableName, final Class<?> entityClass) {
            N.checkArgNotEmpty(tableName, DELETION_PART_MSG);
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;
            instance.setEntityClass(entityClass);

            return instance;
        }

        /**
         * Creates a DELETE FROM CQL builder for an entity class.
         * 
         * <p>This method derives the table name from the entity class using the
         * configured naming policy.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = LCSB.deleteFrom(User.class)
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE FROM users WHERE id = 1
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder deleteFrom(final Class<?> entityClass) {
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance.setEntityClass(entityClass);
            instance._tableName = getTableName(entityClass, instance._namingPolicy);

            return instance;
        }

        /**
         * Creates a SELECT CQL builder with a custom select expression.
         * 
         * <p>This method allows specification of complex SELECT expressions including
         * aggregate functions, calculated fields, etc.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = LCSB.select("COUNT(*) as total, AVG(salary) as avgSalary")
         *                  .from("employees")
         *                  .build().query();
         * // Output: SELECT COUNT(*) as total, AVG(salary) as avgSalary FROM employees
         * }</pre>
         * 
         * @param selectPart the select expression
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if selectPart is null or empty
         */
        public static CqlBuilder select(final String selectPart) {
            N.checkArgNotEmpty(selectPart, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = Array.asList(selectPart);
            return instance;
        }

        /**
         * Creates a SELECT CQL builder for the specified columns.
         * 
         * <p>This method initializes a SELECT query with the specified column names.
         * Column names remain in camelCase format without conversion.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = LCSB.select("firstName", "lastName", "email")
         *                  .from("users")
         *                  .where(Filters.eq("active", true))
         *                  .build().query();
         * // Output: SELECT firstName, lastName, email FROM users WHERE active = true
         * }</pre>
         * 
         * @param propOrColumnNames the property or column names to select
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder select(final String... propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = Array.asList(propOrColumnNames);

            return instance;
        }

        /**
         * Creates a SELECT CQL builder for the specified columns collection.
         * 
         * <p>This method is similar to {@link #select(String...)} but accepts a Collection
         * of column names instead of varargs.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = getRequiredColumns();
         * String cql = LCSB.select(columns)
         *                  .from("users")
         *                  .build().query();
         * // Output: SELECT firstName, lastName, email FROM users
         * }</pre>
         * 
         * @param propOrColumnNames collection of property or column names to select
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder select(final Collection<String> propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = propOrColumnNames;

            return instance;
        }

        /**
         * Creates a SELECT CQL builder with column aliases.
         * 
         * <p>This method allows specification of column names with their aliases for
         * the SELECT statement.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, String> aliases = new HashMap<>();
         * aliases.put("firstName", "fname");
         * aliases.put("lastName", "lname");
         * String cql = LCSB.select(aliases).from("users").build().query();
         * // Output: SELECT firstName AS fname, lastName AS lname FROM users
         * }</pre>
         * 
         * @param propOrColumnNameAliases map of column names to their aliases
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if propOrColumnNameAliases is null or empty
         */
        public static CqlBuilder select(final Map<String, String> propOrColumnNameAliases) {
            N.checkArgNotEmpty(propOrColumnNameAliases, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNameAliases = propOrColumnNameAliases;

            return instance;
        }

        /**
         * Creates a SELECT CQL builder for all properties of an entity class.
         * 
         * <p>This method generates a SELECT statement including all properties of the
         * specified entity class, excluding any transient fields.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = LCSB.select(User.class)
         *                  .from("users")
         *                  .build().query();
         * // Output: SELECT id, firstName, lastName, email FROM users
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         * 
         * @see #select(Class, boolean)
         */
        public static CqlBuilder select(final Class<?> entityClass) {
            return select(entityClass, false);
        }

        /**
         * Creates a SELECT CQL builder for an entity class with sub-entity option.
         * 
         * <p>When includeSubEntityProperties is true, properties of sub-entities
         * (nested objects) will also be included in the SELECT statement.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // If User has an Address sub-entity
         * String cql = LCSB.select(User.class, true)
         *                  .from("users")
         *                  .build().query();
         * // Output: SELECT firstName, lastName, address.street, address.city FROM users
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties whether to include properties of sub-entities
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass, final boolean includeSubEntityProperties) {
            return select(entityClass, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT CQL builder for an entity class, excluding specified properties.
         * 
         * <p>This method generates a SELECT statement for the entity class, excluding
         * the specified properties.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = new HashSet<>(Arrays.asList("password", "secretKey"));
         * String cql = LCSB.select(User.class, excluded)
         *                  .from("users")
         *                  .build().query();
         * // Output: SELECT id, firstName, lastName, email FROM users
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return select(entityClass, false, excludedPropNames);
        }

        /**
         * Creates a SELECT CQL builder for an entity class with sub-entity option and excluded properties.
         * 
         * <p>This method provides full control over which properties to include in the
         * SELECT statement, with options for sub-entities and property exclusion.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = new HashSet<>(Arrays.asList("internalNotes"));
         * String cql = LCSB.select(Order.class, true, excluded)
         *                  .from("orders")
         *                  .build().query();
         * // Output includes Order and Customer properties, excluding internalNotes
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties whether to include properties of sub-entities
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass, final boolean includeSubEntityProperties, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = QueryUtil.getSelectPropNames(entityClass, includeSubEntityProperties, excludedPropNames);

            return instance;
        }

        /**
         * Creates a SELECT FROM CQL builder for an entity class.
         * 
         * <p>This is a convenience method that combines SELECT and FROM operations.
         * The table name is derived from the entity class.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = LCSB.selectFrom(User.class)
         *                  .where(Filters.eq("active", true))
         *                  .build().query();
         * // Output: SELECT id, firstName, lastName, email FROM users WHERE active = true
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         * 
         * @see #selectFrom(Class, boolean)
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, false);
        }

        /**
         * Creates a SELECT FROM CQL builder for an entity class with table alias.
         * 
         * <p>This method allows specification of a table alias for use in complex queries
         * with joins or subqueries.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = LCSB.selectFrom(User.class, "u")
         *                  .where(Filters.eq("u.active", true))
         *                  .build().query();
         * // Output: SELECT u.id, u.firstName, u.lastName FROM users u WHERE u.active = true
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias) {
            return selectFrom(entityClass, alias, false);
        }

        /**
         * Creates a SELECT FROM CQL builder for an entity class with sub-entity option.
         * 
         * <p>This is a convenience method that combines SELECT and FROM operations
         * with the option to include sub-entity properties.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = LCSB.selectFrom(Order.class, true)
         *                  .where(Filters.gt("totalAmount", 100))
         *                  .build().query();
         * // Output includes JOINs for sub-entities
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties whether to include properties of sub-entities
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final boolean includeSubEntityProperties) {
            return selectFrom(entityClass, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT FROM CQL builder with table alias and sub-entity option.
         * 
         * <p>This method combines table aliasing with sub-entity property inclusion
         * for complex query construction.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = LCSB.selectFrom(Order.class, "o", true)
         *                  .where(Filters.eq("o.status", "'ACTIVE'"))
         *                  .build().query();
         * // Output includes aliased columns and JOINs for sub-entities
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @param includeSubEntityProperties whether to include properties of sub-entities
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final boolean includeSubEntityProperties) {
            return selectFrom(entityClass, alias, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT FROM CQL builder with property exclusion.
         * 
         * <p>This method allows selective property selection with automatic FROM clause.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = new HashSet<>(Arrays.asList("largeBlob", "metadata"));
         * String cql = LCSB.selectFrom(User.class, excluded)
         *                  .where(Filters.eq("active", true))
         *                  .build().query();
         * // Output: SELECT id, firstName, lastName, email FROM users WHERE active = true
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, false, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM CQL builder with table alias and property exclusion.
         * 
         * <p>This method provides aliasing capability while excluding specified properties
         * from the SELECT statement.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = new HashSet<>(Arrays.asList("internalCode"));
         * String cql = LCSB.selectFrom(User.class, "u", excluded)
         *                  .innerJoin("orders", "o").on("u.id = o.userId")
         *                  .build().query();
         * // Output uses alias "u" and excludes internalCode property
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, alias, false, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM CQL builder with sub-entity option and property exclusion.
         * 
         * <p>This method provides a convenient way to create a complete SELECT FROM
         * statement with control over sub-entities and property exclusion.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = new HashSet<>(Arrays.asList("deletedFlag"));
         * String cql = LCSB.selectFrom(Order.class, true, excluded)
         *                  .where(Filters.gt("createdDate", "'2023-01-01'"))
         *                  .build().query();
         * // Output includes Order with Customer sub-entity, excluding deletedFlag
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties whether to include properties of sub-entities
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final boolean includeSubEntityProperties, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, QueryUtil.getTableAlias(entityClass), includeSubEntityProperties, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM CQL builder with full control over all options.
         * 
         * <p>This method provides complete control over the SELECT FROM statement generation,
         * including table alias, sub-entity properties, and property exclusion. When
         * sub-entities are included, appropriate joins will be generated.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = new HashSet<>(Arrays.asList("debugInfo"));
         * String cql = LCSB.selectFrom(Order.class, "ord", true, excluded)
         *                  .where(Filters.gt("ord.totalAmount", 1000))
         *                  .build().query();
         * // Output: Complex SELECT with alias, sub-entities, and exclusions
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @param includeSubEntityProperties whether to include properties of sub-entities
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final boolean includeSubEntityProperties,
                final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            if (hasSubEntityToInclude(entityClass, includeSubEntityProperties)) {
                final List<String> selectTableNames = getSelectTableNames(entityClass, alias, excludedPropNames, NamingPolicy.CAMEL_CASE);
                //noinspection ConstantValue
                return select(entityClass, includeSubEntityProperties, excludedPropNames).from(entityClass, selectTableNames);
            }

            return select(entityClass, includeSubEntityProperties, excludedPropNames).from(entityClass, alias);
        }

        /**
         * Creates a COUNT(*) CQL builder for the specified table.
         * 
         * <p>This is a convenience method for creating COUNT queries.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = LCSB.count("users")
         *                  .where(Filters.eq("active", true))
         *                  .build().query();
         * // Output: SELECT count(*) FROM users WHERE active = true
         * }</pre>
         * 
         * @param tableName the name of the table to count rows from
         * @return a new CqlBuilder instance configured for COUNT operation
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder count(final String tableName) {
            N.checkArgNotEmpty(tableName, SELECTION_PART_MSG);

            return select(COUNT_ALL_LIST).from(tableName);
        }

        /**
         * Creates a COUNT(*) CQL builder for an entity class.
         * 
         * <p>This method derives the table name from the entity class and creates
         * a COUNT query.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = LCSB.count(User.class)
         *                  .where(Filters.between("age", 18, 65))
         *                  .build().query();
         * // Output: SELECT count(*) FROM users WHERE age BETWEEN 18 AND 65
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for COUNT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder count(final Class<?> entityClass) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            return select(COUNT_ALL_LIST).from(entityClass);
        }

        /**
         * Parses a condition into CQL with entity class context.
         * 
         * <p>This method is useful for generating just the CQL representation of a
         * condition without building a complete statement. It's typically used for
         * debugging or building dynamic query parts.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Condition cond = Filters.and(
         *     Filters.eq("active", true),
         *     Filters.gt("age", 18)
         * );
         * 
         * String cql = LCSB.parse(cond, User.class).build().query();
         * // Output: active = true AND age > 18
         * }</pre>
         * 
         * @param cond the condition to parse
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance containing the parsed condition
         * @throws IllegalArgumentException if cond is null
         * 
         * @see Filters
         */
        public static CqlBuilder parse(final Condition cond, final Class<?> entityClass) {
            N.checkArgNotNull(cond, "cond");

            final CqlBuilder instance = createInstance();

            instance.setEntityClass(entityClass);
            instance._op = OperationType.QUERY;
            instance._isForConditionOnly = true;
            instance.append(cond);

            return instance;
        }
    }

    /**
     * Parameterized CQL builder with no naming policy transformation.
     * 
     * <p>This builder generates parameterized CQL statements using '?' placeholders and preserves
     * the original casing of property and column names without any transformation.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Property names are preserved as-is
     * String cql = PSB.select("first_Name", "last_NaMe")
     *                 .from("account")
     *                 .where(Filters.eq("last_NaMe", 1))
     *                 .build().query();
     * // Output: SELECT first_Name, last_NaMe FROM account WHERE last_NaMe = ?
     * }</pre>
     */
    public static class PSB extends CqlBuilder {

        PSB() {
            super(NamingPolicy.NO_CHANGE, SQLPolicy.PARAMETERIZED_SQL);
        }

        protected static PSB createInstance() {
            return new PSB();
        }

        /**
         * Creates an INSERT statement builder for a single column expression.
         * 
         * <p>This method is a convenience wrapper that internally calls {@link #insert(String...)}
         * with a single-element array.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * CqlBuilder builder = PSB.insert("user_name").into("users");
         * }</pre>
         * 
         * @param expr the column name or expression to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if expr is null or empty
         */
        public static CqlBuilder insert(final String expr) {
            N.checkArgNotEmpty(expr, INSERTION_PART_MSG);

            return insert(N.asArray(expr));
        }

        /**
         * Creates an INSERT statement builder for the specified columns.
         * 
         * <p>The column names are used as-is without any naming transformation.
         * The actual values must be provided later using the {@code values()} method.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * CqlBuilder builder = PSB.insert("name", "email", "age")
         *                         .into("users")
         *                         .values("John", "john@example.com", 25);
         * }</pre>
         * 
         * @param propOrColumnNames the property or column names to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder insert(final String... propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._propOrColumnNames = Array.asList(propOrColumnNames);

            return instance;
        }

        /**
         * Creates an INSERT statement builder for the specified collection of columns.
         * 
         * <p>This method allows using any Collection implementation (List, Set, etc.) to specify
         * the columns for insertion.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("name", "email", "age");
         * CqlBuilder builder = PSB.insert(columns).into("users");
         * }</pre>
         * 
         * @param propOrColumnNames collection of property or column names to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder insert(final Collection<String> propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._propOrColumnNames = propOrColumnNames;

            return instance;
        }

        /**
         * Creates an INSERT statement builder using a map of property names to values.
         * 
         * <p>The map keys represent column names and the values are the corresponding values
         * to be inserted. This provides a convenient way to specify both columns and values
         * in a single call.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Object> data = new HashMap<>();
         * data.put("name", "John");
         * data.put("age", 25);
         * CqlBuilder builder = PSB.insert(data).into("users");
         * }</pre>
         * 
         * @param props map of property names to values
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if props is null or empty
         */
        public static CqlBuilder insert(final Map<String, Object> props) {
            N.checkArgNotEmpty(props, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._props = props;

            return instance;
        }

        /**
         * Creates an INSERT statement builder from an entity object.
         * 
         * <p>All non-null properties of the entity will be included in the INSERT statement,
         * except those marked with {@code @Transient}, {@code @ReadOnly}, or {@code @ReadOnlyId}
         * annotations.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User user = new User("John", "john@example.com", 25);
         * CqlBuilder builder = PSB.insert(user).into("users");
         * }</pre>
         * 
         * @param entity the entity object to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entity is null
         */
        public static CqlBuilder insert(final Object entity) {
            return insert(entity, null);
        }

        /**
         * Creates an INSERT statement builder from an entity object with excluded properties.
         * 
         * <p>Properties can be excluded from the INSERT statement by specifying their names
         * in the excludedPropNames set. This is useful when certain properties should not
         * be inserted even if they have values.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User user = new User();
         * Set<String> excluded = N.asSet("createdTime", "updatedTime");
         * CqlBuilder builder = PSB.insert(user, excluded).into("users");
         * }</pre>
         * 
         * @param entity the entity object to insert
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entity is null
         */
        public static CqlBuilder insert(final Object entity, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entity, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance.setEntityClass(entity.getClass());

            parseInsertEntity(instance, entity, excludedPropNames);

            return instance;
        }

        /**
         * Creates an INSERT statement builder for an entity class.
         * 
         * <p>This method generates an INSERT template for all insertable properties of the
         * specified entity class. Properties marked with {@code @Transient}, {@code @ReadOnly},
         * or {@code @ReadOnlyId} annotations are automatically excluded.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * CqlBuilder builder = PSB.insert(User.class)
         *                         .into("users")
         *                         .values("John", "john@example.com", 25);
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insert(final Class<?> entityClass) {
            return insert(entityClass, null);
        }

        /**
         * Creates an INSERT statement builder for an entity class with excluded properties.
         * 
         * <p>Generates an INSERT template excluding the specified properties in addition to
         * those automatically excluded by annotations.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("version", "lastModified");
         * CqlBuilder builder = PSB.insert(User.class, excluded).into("users");
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        @SuppressWarnings("deprecation")
        public static CqlBuilder insert(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = QueryUtil.getInsertPropNames(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates an INSERT INTO statement builder for an entity class.
         * 
         * <p>This is a convenience method that combines {@link #insert(Class)} and {@link #into(Class)}
         * in a single call. The table name is derived from the entity class name or its {@code @Table}
         * annotation.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * CqlBuilder builder = PSB.insertInto(User.class)
         *                         .values("John", "john@example.com", 25);
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for INSERT operation with table name set
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insertInto(final Class<?> entityClass) {
            return insertInto(entityClass, null);
        }

        /**
         * Creates an INSERT INTO statement builder for an entity class with excluded properties.
         * 
         * <p>Combines INSERT and INTO operations while excluding specified properties from
         * the generated statement.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("id", "version");
         * CqlBuilder builder = PSB.insertInto(User.class, excluded)
         *                         .values("John", "john@example.com");
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance configured for INSERT operation with table name set
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insertInto(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return insert(entityClass, excludedPropNames).into(entityClass);
        }

        /**
         * Creates a batch INSERT statement builder for multiple records.
         * 
         * <p>Generates MyCQL-style batch insert CQL that can insert multiple rows in a single
         * statement. The input collection can contain either entity objects or Map instances
         * representing the data to insert.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<User> users = Arrays.asList(
         *     new User("John", "john@example.com"),
         *     new User("Jane", "jane@example.com")
         * );
         * CqlBuilder builder = PSB.batchInsert(users).into("users");
         * // Generates: INSERT INTO users (name, email) VALUES (?, ?), (?, ?)
         * }</pre>
         * 
         * @param propsList collection of entities or property maps to insert
         * @return a new CqlBuilder instance configured for batch INSERT operation
         * @throws IllegalArgumentException if propsList is null or empty
         */
        @Beta
        public static CqlBuilder batchInsert(final Collection<?> propsList) {
            N.checkArgNotEmpty(propsList, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            final Optional<?> first = N.firstNonNull(propsList);

            if (first.isPresent() && Beans.isBeanClass(first.get().getClass())) {
                instance.setEntityClass(first.get().getClass());
            }

            instance._propsList = toInsertPropsList(propsList);

            return instance;
        }

        /**
         * Creates an UPDATE statement builder for the specified table.
         * 
         * <p>The table name is used as-is without any transformation. Columns to update
         * must be specified using the {@code set()} method.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * CqlBuilder builder = PSB.update("users")
         *                         .set("name", "email")
         *                         .where(Filters.eq("id", 1));
         * }</pre>
         * 
         * @param tableName the name of the table to update
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder update(final String tableName) {
            N.checkArgNotEmpty(tableName, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;

            return instance;
        }

        /**
         * Creates an UPDATE statement builder for a table with entity class mapping.
         * 
         * <p>Specifying the entity class enables property name transformation and validation
         * based on the entity's field definitions.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * CqlBuilder builder = PSB.update("users", User.class)
         *                         .set("name", "email")
         *                         .where(Filters.eq("id", 1));
         * }</pre>
         * 
         * @param tableName the name of the table to update
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if tableName or entityClass is null
         */
        public static CqlBuilder update(final String tableName, final Class<?> entityClass) {
            N.checkArgNotEmpty(tableName, UPDATE_PART_MSG);
            N.checkArgNotNull(entityClass, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;
            instance.setEntityClass(entityClass);

            return instance;
        }

        /**
         * Creates an UPDATE statement builder for an entity class.
         * 
         * <p>The table name is derived from the entity class name or its {@code @Table} annotation.
         * All updatable properties (excluding those marked with {@code @NonUpdatable}, {@code @ReadOnly},
         * or {@code @Transient}) are included in the SET clause.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * CqlBuilder builder = PSB.update(User.class)
         *                         .where(Filters.eq("id", 1));
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder update(final Class<?> entityClass) {
            return update(entityClass, null);
        }

        /**
         * Creates an UPDATE statement builder for an entity class with excluded properties.
         * 
         * <p>Generates an UPDATE statement excluding the specified properties in addition to
         * those automatically excluded by annotations.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("createdTime", "createdBy");
         * CqlBuilder builder = PSB.update(User.class, excluded)
         *                         .where(Filters.eq("id", 1));
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the update
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        @SuppressWarnings("deprecation")
        public static CqlBuilder update(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance.setEntityClass(entityClass);
            instance._tableName = getTableName(entityClass, instance._namingPolicy);
            instance._propOrColumnNames = QueryUtil.getUpdatePropNames(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for a single column or expression.
         *
         * <p>This method initializes a DELETE statement for one column. The column name will be
         * converted according to the SCREAMING_SNAKE_CASE naming policy. This is useful for deleting
         * specific columns from a row rather than the entire row.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PSB.delete("firstName")
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE firstName FROM account WHERE id = ?
         * }</pre>
         *
         * @param expr the column name or expression to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if expr is null or empty
         */
        public static CqlBuilder delete(final String expr) {
            N.checkArgNotEmpty(expr, DELETION_PART_MSG);

            return delete(N.asArray(expr));
        }

        /**
         * Creates a DELETE CQL builder for multiple columns.
         *
         * <p>This method initializes a DELETE statement for multiple columns. All column names
         * will be converted according to the SCREAMING_SNAKE_CASE naming policy. This allows selective
         * deletion of specific columns from rows.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PSB.delete("firstName", "lastName", "email")
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE firstName, lastName, email FROM account WHERE id = ?
         * }</pre>
         *
         * @param columnNames the column names to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if columnNames is null or empty
         */
        public static CqlBuilder delete(final String... columnNames) {
            N.checkArgNotEmpty(columnNames, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._propOrColumnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for a collection of columns.
         *
         * <p>This method is useful when column names are determined dynamically. The collection
         * can contain property names that will be converted to column names according to the
         * SCREAMING_SNAKE_CASE naming policy.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("firstName", "lastName", "email");
         * String cql = PSB.delete(columns)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE firstName, lastName, email FROM account WHERE id = ?
         * }</pre>
         *
         * @param columnNames the collection of column names to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if columnNames is null or empty
         */
        public static CqlBuilder delete(final Collection<String> columnNames) {
            N.checkArgNotEmpty(columnNames, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._propOrColumnNames = columnNames;

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for an entity class.
         *
         * <p>This method generates a DELETE statement for all deletable properties of the class.
         * Properties marked with @ReadOnly, @ReadOnlyId, or @Transient are automatically excluded.
         * This is useful for creating templates for partial row deletion.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PSB.delete(Account.class)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE firstName, lastName, email, status FROM account WHERE id = ?
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder delete(final Class<?> entityClass) {
            return delete(entityClass, null);
        }

        /**
         * Creates a DELETE CQL builder for an entity class, excluding specified properties.
         *
         * <p>This method generates a DELETE statement excluding both annotation-based exclusions
         * and the specified properties. Useful for selective column deletion where certain
         * fields should be preserved.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("email", "createdDate");
         * String cql = PSB.delete(Account.class, excluded)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE firstName, lastName, status FROM account WHERE id = ?
         * }</pre>
         *
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder delete(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates a DELETE FROM statement builder for the specified table.
         * 
         * <p>The table name is used as-is without any transformation. WHERE conditions
         * should be added to avoid deleting all records.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * CqlBuilder builder = PSB.deleteFrom("users")
         *                         .where(Filters.eq("status", "inactive"));
         * }</pre>
         * 
         * @param tableName the name of the table to delete from
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder deleteFrom(final String tableName) {
            N.checkArgNotEmpty(tableName, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;

            return instance;
        }

        /**
         * Creates a DELETE FROM statement builder for a table with entity class mapping.
         * 
         * <p>Specifying the entity class enables property name validation in WHERE conditions
         * based on the entity's field definitions.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * CqlBuilder builder = PSB.deleteFrom("users", User.class)
         *                         .where(Filters.eq("lastLogin", null));
         * }</pre>
         * 
         * @param tableName the name of the table to delete from
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if tableName or entityClass is null
         */
        public static CqlBuilder deleteFrom(final String tableName, final Class<?> entityClass) {
            N.checkArgNotEmpty(tableName, DELETION_PART_MSG);
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;
            instance.setEntityClass(entityClass);

            return instance;
        }

        /**
         * Creates a DELETE FROM statement builder for an entity class.
         * 
         * <p>The table name is derived from the entity class name or its {@code @Table} annotation.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * CqlBuilder builder = PSB.deleteFrom(User.class)
         *                         .where(Filters.eq("id", 1));
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder deleteFrom(final Class<?> entityClass) {
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance.setEntityClass(entityClass);
            instance._tableName = getTableName(entityClass, instance._namingPolicy);

            return instance;
        }

        /**
         * Creates a SELECT statement builder for a single column or expression.
         * 
         * <p>The selectPart can be a simple column name or a complex expression including
         * functions, aliases, etc.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * CqlBuilder builder = PSB.select("COUNT(*)").from("users");
         * CqlBuilder builder2 = PSB.select("name AS userName").from("users");
         * }</pre>
         * 
         * @param selectPart the select expression
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if selectPart is null or empty
         */
        public static CqlBuilder select(final String selectPart) {
            N.checkArgNotEmpty(selectPart, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = Array.asList(selectPart);
            return instance;
        }

        /**
         * Creates a SELECT statement builder for multiple columns.
         * 
         * <p>Column names are used as-is without any transformation. Each column can be
         * a simple name or include expressions and aliases.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * CqlBuilder builder = PSB.select("id", "name", "email")
         *                         .from("users")
         *                         .where(Filters.gt("age", 18));
         * }</pre>
         * 
         * @param propOrColumnNames the property or column names to select
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder select(final String... propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = Array.asList(propOrColumnNames);

            return instance;
        }

        /**
         * Creates a SELECT statement builder for a collection of columns.
         * 
         * <p>This method allows using any Collection implementation (List, Set, etc.) to specify
         * the columns to select.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("id", "name", "email");
         * CqlBuilder builder = PSB.select(columns).from("users");
         * }</pre>
         * 
         * @param propOrColumnNames collection of property or column names to select
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder select(final Collection<String> propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = propOrColumnNames;

            return instance;
        }

        /**
         * Creates a SELECT statement builder with column aliases.
         * 
         * <p>The map keys represent the column names or expressions to select, and the values
         * are their corresponding aliases in the result set.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, String> aliases = new LinkedHashMap<>();
         * aliases.put("u.name", "userName");
         * aliases.put("u.email", "userEmail");
         * CqlBuilder builder = PSB.select(aliases).from("users u");
         * // Generates: SELECT u.name AS userName, u.email AS userEmail FROM users u
         * }</pre>
         * 
         * @param propOrColumnNameAliases map where keys are column names and values are aliases
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if propOrColumnNameAliases is null or empty
         */
        public static CqlBuilder select(final Map<String, String> propOrColumnNameAliases) {
            N.checkArgNotEmpty(propOrColumnNameAliases, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNameAliases = propOrColumnNameAliases;

            return instance;
        }

        /**
         * Creates a SELECT statement builder for all properties of an entity class.
         * 
         * <p>Selects all properties of the entity class except those marked with
         * {@code @Transient} annotation. Sub-entity properties are not included by default.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * CqlBuilder builder = PSB.select(User.class)
         *                         .from("users")
         *                         .where(Filters.eq("active", true));
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass) {
            return select(entityClass, false);
        }

        /**
         * Creates a SELECT statement builder for an entity class with sub-entity option.
         * 
         * <p>When includeSubEntityProperties is true, properties of sub-entities (nested objects)
         * are also included in the selection with appropriate aliasing.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // If User has an Address sub-entity
         * CqlBuilder builder = PSB.select(User.class, true)
         *                         .from("users u")
         *                         .join("addresses a").on("u.address_id = a.id");
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties whether to include properties of sub-entities
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass, final boolean includeSubEntityProperties) {
            return select(entityClass, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT statement builder for an entity class with excluded properties.
         * 
         * <p>Generates a SELECT statement excluding the specified properties in addition to
         * those automatically excluded by {@code @Transient} annotation.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("password", "secretKey");
         * CqlBuilder builder = PSB.select(User.class, excluded)
         *                         .from("users");
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return select(entityClass, false, excludedPropNames);
        }

        /**
         * Creates a SELECT statement builder with full control over entity property selection.
         * 
         * <p>Provides complete control over which properties to include or exclude, including
         * sub-entity properties.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("internalNotes");
         * CqlBuilder builder = PSB.select(User.class, true, excluded)
         *                         .from("users u")
         *                         .join("addresses a").on("u.address_id = a.id");
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties whether to include properties of sub-entities
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        @SuppressWarnings("deprecation")
        public static CqlBuilder select(final Class<?> entityClass, final boolean includeSubEntityProperties, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = QueryUtil.getSelectPropNames(entityClass, includeSubEntityProperties, excludedPropNames);

            return instance;
        }

        /**
         * Creates a complete SELECT FROM statement builder for an entity class.
         * 
         * <p>This is a convenience method that combines SELECT and FROM operations.
         * The table name is derived from the entity class name or its {@code @Table} annotation.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * CqlBuilder builder = PSB.selectFrom(User.class)
         *                         .where(Filters.eq("status", "active"));
         * // Equivalent to: PSB.select(User.class).from(User.class)
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, false);
        }

        /**
         * Creates a SELECT FROM statement builder with a table alias.
         * 
         * <p>The alias is used to qualify column names in the generated CQL, which is useful
         * for joins and subqueries.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * CqlBuilder builder = PSB.selectFrom(User.class, "u")
         *                         .join("orders o").on("u.id = o.user_id");
         * // Generates: SELECT u.id, u.name, ... FROM users u JOIN orders o ON u.id = o.user_id
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias) {
            return selectFrom(entityClass, alias, false);
        }

        /**
         * Creates a SELECT FROM statement builder with sub-entity properties option.
         * 
         * <p>When includeSubEntityProperties is true, appropriate joins are automatically
         * generated for sub-entities.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * CqlBuilder builder = PSB.selectFrom(User.class, true)
         *                         .where(Filters.isNotNull("address.city"));
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties whether to include properties of sub-entities
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final boolean includeSubEntityProperties) {
            return selectFrom(entityClass, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT FROM statement builder with alias and sub-entity properties option.
         * 
         * <p>Combines table aliasing with sub-entity property inclusion for complex queries.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * CqlBuilder builder = PSB.selectFrom(User.class, "u", true)
         *                         .where(Filters.like("u.name", "John%"));
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @param includeSubEntityProperties whether to include properties of sub-entities
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final boolean includeSubEntityProperties) {
            return selectFrom(entityClass, alias, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT FROM statement builder with excluded properties.
         * 
         * <p>Convenience method for creating a complete SELECT FROM statement while excluding
         * specific properties.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("largeBlob", "internalData");
         * CqlBuilder builder = PSB.selectFrom(User.class, excluded)
         *                         .limit(10);
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, false, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM statement builder with alias and excluded properties.
         * 
         * <p>Provides aliasing capability while excluding specific properties from selection.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("password");
         * CqlBuilder builder = PSB.selectFrom(User.class, "u", excluded)
         *                         .join("roles r").on("u.role_id = r.id");
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, alias, false, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM statement builder with sub-entities and excluded properties.
         * 
         * <p>Allows including sub-entity properties while excluding specific properties
         * from the selection.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("user.password", "user.salt");
         * CqlBuilder builder = PSB.selectFrom(Order.class, true, excluded);
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties whether to include properties of sub-entities
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final boolean includeSubEntityProperties, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, QueryUtil.getTableAlias(entityClass), includeSubEntityProperties, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM statement builder with full control over all options.
         * 
         * <p>This method provides complete control over the SELECT FROM generation, including
         * aliasing, sub-entity properties, and property exclusion. When sub-entities are included,
         * appropriate JOIN clauses may be automatically generated.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("audit.createdBy", "audit.modifiedBy");
         * CqlBuilder builder = PSB.selectFrom(Product.class, "p", true, excluded)
         *                         .where(Filters.gt("p.price", 100))
         *                         .orderBy("p.name");
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @param includeSubEntityProperties whether to include properties of sub-entities
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final boolean includeSubEntityProperties,
                final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            if (hasSubEntityToInclude(entityClass, includeSubEntityProperties)) {
                final List<String> selectTableNames = getSelectTableNames(entityClass, alias, excludedPropNames, NamingPolicy.NO_CHANGE);
                return select(entityClass, includeSubEntityProperties, excludedPropNames).from(entityClass, selectTableNames);
            }

            return select(entityClass, includeSubEntityProperties, excludedPropNames).from(entityClass, alias);
        }

        /**
         * Creates a COUNT(*) query builder for the specified table.
         * 
         * <p>This is a convenience method for creating count queries without specifying
         * the COUNT(*) expression explicitly.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * CqlBuilder builder = PSB.count("users")
         *                         .where(Filters.eq("status", "active"));
         * // Generates: SELECT count(*) FROM users WHERE status = ?
         * }</pre>
         * 
         * @param tableName the name of the table to count rows from
         * @return a new CqlBuilder instance configured for COUNT operation
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder count(final String tableName) {
            N.checkArgNotEmpty(tableName, SELECTION_PART_MSG);

            return select(COUNT_ALL_LIST).from(tableName);
        }

        /**
         * Creates a COUNT(*) query builder for an entity class.
         * 
         * <p>The table name is derived from the entity class name or its {@code @Table} annotation.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * long count = PSB.count(User.class)
         *                 .where(Filters.like("email", "%@example.com"))
         *                 .queryForSingleResult(Long.class);
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for COUNT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder count(final Class<?> entityClass) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            return select(COUNT_ALL_LIST).from(entityClass);
        }

        /**
         * Parses a condition into CQL with entity class context.
         * 
         * <p>This method is useful for generating CQL fragments from Condition objects,
         * particularly for debugging or when building complex dynamic queries. The entity
         * class provides context for property name resolution.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Condition cond = Filters.and(
         *     Filters.eq("status", "active"),
         *     Filters.gt("age", 18)
         * );
         * String cql = PSB.parse(cond, User.class).build().query();
         * // Result: "status = ? AND age > ?"
         * }</pre>
         * 
         * @param cond the condition to parse
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance containing the parsed condition
         * @throws IllegalArgumentException if cond is null
         */
        public static CqlBuilder parse(final Condition cond, final Class<?> entityClass) {
            N.checkArgNotNull(cond, "cond");

            final CqlBuilder instance = createInstance();

            instance.setEntityClass(entityClass);
            instance._op = OperationType.QUERY;
            instance._isForConditionOnly = true;
            instance.append(cond);

            return instance;
        }
    }

    /**
     * Parameterized CQL builder with snake_case (lower case with underscore) field/column naming strategy.
     * 
     * <p>PSC (Parameterized Snake Case) generates CQL with placeholder parameters (?) and converts
     * property names from camelCase to snake_case. This is the most commonly used CQL builder
     * for applications using standard CQL databases with snake_case column naming conventions.</p>
     * 
     * <p><b>Naming Convention:</b></p>
     * <ul>
     *   <li>Property: firstName → Column: first_name</li>
     *   <li>Property: accountNumber → Column: account_number</li>
     *   <li>Property: isActive → Column: is_active</li>
     * </ul>
     * 
     * <p><b>Basic Usage Examples:</b></p>
     * <pre>{@code
     * // Simple SELECT
     * String cql = PSC.select("firstName", "lastName")
     *                 .from("account")
     *                 .where(Filters.eq("id", 1))
     *                 .build().query();
     * // Output: SELECT first_name AS "firstName", last_name AS "lastName" FROM account WHERE id = ?
     * 
     * // INSERT with entity
     * Account account = new Account();
     * account.setFirstName("John");
     * account.setLastName("Doe");
     * String cql = PSC.insert(account).into("account").build().query();
     * // Output: INSERT INTO account (first_name, last_name) VALUES (?, ?)
     * 
     * // UPDATE with specific fields
     * String cql = PSC.update("account")
     *                 .set("firstName", "John")
     *                 .set("lastName", "Smith")
     *                 .where(Filters.eq("id", 1))
     *                 .build().query();
     * // Output: UPDATE account SET first_name = ?, last_name = ? WHERE id = ?
     * }</pre>
     * 
     * <p><b>Advanced Examples:</b></p>
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // SELECT with entity class
     * String cql = PSC.selectFrom(Account.class)
     *                 .where(Filters.gt("createdDate", new Date()))
     *                 .orderBy("lastName ASC")
     *                 .limit(10)
     *                 .build().query();
     * 
     * // Batch INSERT
     * List<Account> accounts = Arrays.asList(account1, account2, account3);
     * SP cqlPair = PSC.batchInsert(accounts).into("account").pair();
     * // cqlPair.cql: INSERT INTO account (first_name, last_name) VALUES (?, ?), (?, ?), (?, ?)
     * // cqlPair.parameters: ["John", "Doe", "Jane", "Smith", "Bob", "Johnson"]
     * 
     * // Complex JOIN query
     * String cql = PSC.select("a.id", "a.firstName", "COUNT(o.id) AS orderCount")
     *                 .from("account a")
     *                 .leftJoin("orders o").on("a.id = o.account_id")
     *                 .groupBy("a.id", "a.firstName")
     *                 .having(Filters.gt("COUNT(o.id)", 5))
     *                 .build().query();
     * }</pre>
     * 
     * @see CqlBuilder
     * @see NSC
     */
    public static class PSC extends CqlBuilder {

        PSC() {
            super(NamingPolicy.SNAKE_CASE, SQLPolicy.PARAMETERIZED_SQL);
        }

        protected static PSC createInstance() {
            return new PSC();
        }

        /**
         * Creates an INSERT statement for a single column expression.
         * 
         * <p>This method creates an INSERT statement template with a single column. The actual value
         * will be provided as a parameter when executing the query.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PSC.insert("firstName").into("account").build().query();
         * // Output: INSERT INTO account (first_name) VALUES (?)
         * }</pre>
         * 
         * @param expr the column name or expression to insert
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if expr is null or empty
         */
        public static CqlBuilder insert(final String expr) {
            N.checkArgNotEmpty(expr, INSERTION_PART_MSG);

            return insert(N.asArray(expr));
        }

        /**
         * Creates an INSERT statement for multiple columns.
         * 
         * <p>This method creates an INSERT statement template with multiple columns. Property names
         * are automatically converted to snake_case format. Values will be provided as parameters
         * when executing the query.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PSC.insert("firstName", "lastName", "email")
         *                 .into("account")
         *                 .build().query();
         * // Output: INSERT INTO account (first_name, last_name, email) VALUES (?, ?, ?)
         * }</pre>
         * 
         * @param propOrColumnNames the property or column names to insert
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder insert(final String... propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._propOrColumnNames = Array.asList(propOrColumnNames);

            return instance;
        }

        /**
         * Creates an INSERT statement for a collection of columns.
         * 
         * <p>This method provides flexibility when column names are dynamically generated or come from
         * a collection. Property names are automatically converted to snake_case format.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("firstName", "lastName", "email");
         * String cql = PSC.insert(columns).into("account").build().query();
         * // Output: INSERT INTO account (first_name, last_name, email) VALUES (?, ?, ?)
         * }</pre>
         * 
         * @param propOrColumnNames collection of property or column names to insert
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder insert(final Collection<String> propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._propOrColumnNames = propOrColumnNames;

            return instance;
        }

        /**
         * Creates an INSERT statement from a map of property names and values.
         * 
         * <p>This method generates an INSERT statement where map keys represent property names
         * (converted to snake_case) and values are used to generate parameter placeholders.
         * The actual values can be retrieved using the {@code pair()} method.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Object> props = new HashMap<>();
         * props.put("firstName", "John");
         * props.put("lastName", "Doe");
         * SP cqlPair = PSC.insert(props).into("account").pair();
         * // cqlPair.cql: INSERT INTO account (first_name, last_name) VALUES (?, ?)
         * // cqlPair.parameters: ["John", "Doe"]
         * }</pre>
         * 
         * @param props map of property names to values
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if props is null or empty
         */
        public static CqlBuilder insert(final Map<String, Object> props) {
            N.checkArgNotEmpty(props, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._props = props;

            return instance;
        }

        /**
         * Creates an INSERT statement from an entity object.
         * 
         * <p>This method inspects the entity object and extracts all non-null properties that are
         * suitable for insertion. Properties marked with @Transient, @ReadOnly, or @ReadOnlyId
         * annotations are automatically excluded. Property names are converted to snake_case format.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Account account = new Account();
         * account.setFirstName("John");
         * account.setLastName("Doe");
         * account.setEmail("john.doe@example.com");
         * 
         * SP cqlPair = PSC.insert(account).into("account").pair();
         * // cqlPair.cql: INSERT INTO account (first_name, last_name, email) VALUES (?, ?, ?)
         * // cqlPair.parameters: ["John", "Doe", "john.doe@example.com"]
         * }</pre>
         * 
         * @param entity the entity object to insert
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entity is null
         */
        public static CqlBuilder insert(final Object entity) {
            return insert(entity, null);
        }

        /**
         * Creates an INSERT statement from an entity object with excluded properties.
         * 
         * <p>This method allows fine-grained control over which properties to include in the INSERT
         * statement. Properties in the exclusion set will not be included even if they have values
         * and are normally insertable.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Account account = new Account();
         * account.setFirstName("John");
         * account.setLastName("Doe");
         * account.setEmail("john.doe@example.com");
         * account.setCreatedDate(new Date());
         * 
         * Set<String> excluded = N.asSet("createdDate");
         * SP cqlPair = PSC.insert(account, excluded).into("account").pair();
         * // cqlPair.cql: INSERT INTO account (first_name, last_name, email) VALUES (?, ?, ?)
         * // cqlPair.parameters: ["John", "Doe", "john.doe@example.com"]
         * }</pre>
         * 
         * @param entity the entity object to insert
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entity is null
         */
        public static CqlBuilder insert(final Object entity, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entity, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance.setEntityClass(entity.getClass());

            parseInsertEntity(instance, entity, excludedPropNames);

            return instance;
        }

        /**
         * Creates an INSERT statement for an entity class.
         * 
         * <p>This method generates an INSERT statement template based on the entity class structure.
         * All properties suitable for insertion (excluding those marked with @Transient, @ReadOnly,
         * or @ReadOnlyId) are included. Property names are converted to snake_case format.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PSC.insert(Account.class).into("account").build().query();
         * // Output: INSERT INTO account (first_name, last_name, email, created_date) VALUES (?, ?, ?, ?)
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insert(final Class<?> entityClass) {
            return insert(entityClass, null);
        }

        /**
         * Creates an INSERT statement for an entity class with excluded properties.
         * 
         * <p>This method generates an INSERT statement template based on the entity class structure,
         * excluding specified properties. This is useful for creating reusable INSERT templates
         * that exclude certain fields like auto-generated IDs or timestamps.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("createdDate", "modifiedDate");
         * String cql = PSC.insert(Account.class, excluded).into("account").build().query();
         * // Output: INSERT INTO account (first_name, last_name, email) VALUES (?, ?, ?)
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        @SuppressWarnings("deprecation")
        public static CqlBuilder insert(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = QueryUtil.getInsertPropNames(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates an INSERT INTO statement for an entity class.
         * 
         * <p>This is a convenience method that combines insert() and into() operations.
         * The table name is automatically derived from the entity class name or @Table annotation.
         * Property names are converted to snake_case format.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PSC.insertInto(Account.class).build().query();
         * // Output: INSERT INTO account (first_name, last_name, email) VALUES (?, ?, ?)
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insertInto(final Class<?> entityClass) {
            return insertInto(entityClass, null);
        }

        /**
         * Creates an INSERT INTO statement for an entity class with excluded properties.
         * 
         * <p>This convenience method combines insert() and into() operations while allowing
         * property exclusion. The table name is derived from the entity class.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("id", "createdDate");
         * String cql = PSC.insertInto(Account.class, excluded).build().query();
         * // Output: INSERT INTO account (first_name, last_name, email) VALUES (?, ?, ?)
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insertInto(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return insert(entityClass, excludedPropNames).into(entityClass);
        }

        /**
         * Generates a MyCQL-style batch INSERT statement.
         * 
         * <p>This method creates an efficient batch insert statement with multiple value sets
         * in a single INSERT statement, which is particularly useful for MyCQL databases and
         * provides better performance than multiple individual INSERT statements.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<Account> accounts = Arrays.asList(
         *     new Account("John", "Doe"),
         *     new Account("Jane", "Smith"),
         *     new Account("Bob", "Johnson")
         * );
         * 
         * SP cqlPair = PSC.batchInsert(accounts).into("account").pair();
         * // cqlPair.cql: INSERT INTO account (first_name, last_name) VALUES (?, ?), (?, ?), (?, ?)
         * // cqlPair.parameters: ["John", "Doe", "Jane", "Smith", "Bob", "Johnson"]
         * }</pre>
         *
         * @param propsList collection of entities or property maps to insert
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if propsList is null or empty
         */
        @Beta
        public static CqlBuilder batchInsert(final Collection<?> propsList) {
            N.checkArgNotEmpty(propsList, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            final Optional<?> first = N.firstNonNull(propsList);

            if (first.isPresent() && Beans.isBeanClass(first.get().getClass())) {
                instance.setEntityClass(first.get().getClass());
            }

            instance._propsList = toInsertPropsList(propsList);

            return instance;
        }

        /**
         * Creates an UPDATE statement for a table.
         * 
         * <p>This method starts building an UPDATE statement. Use the {@code set()} method to specify
         * which columns to update and their values. Property names in subsequent operations will be
         * converted to snake_case format.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PSC.update("account")
         *                 .set("firstName", "John")
         *                 .set("lastName", "Smith")
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: UPDATE account SET first_name = ?, last_name = ? WHERE id = ?
         * }</pre>
         * 
         * @param tableName the name of the table to update
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder update(final String tableName) {
            N.checkArgNotEmpty(tableName, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;

            return instance;
        }

        /**
         * Creates an UPDATE statement for a table with entity class mapping.
         * 
         * <p>This method creates an UPDATE statement where the entity class provides property-to-column
         * name mapping information. This ensures proper snake_case conversion for all property names
         * used in the update operation.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PSC.update("account", Account.class)
         *                 .set("firstName", "John")
         *                 .set("lastModified", new Date())
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: UPDATE account SET first_name = ?, last_modified = ? WHERE id = ?
         * }</pre>
         * 
         * @param tableName the name of the table to update
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if tableName is null or empty, or entityClass is null
         */
        public static CqlBuilder update(final String tableName, final Class<?> entityClass) {
            N.checkArgNotEmpty(tableName, UPDATE_PART_MSG);
            N.checkArgNotNull(entityClass, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;
            instance.setEntityClass(entityClass);

            return instance;
        }

        /**
         * Creates an UPDATE statement for an entity class.
         * 
         * <p>This method creates an UPDATE statement where the table name is derived from the entity
         * class name or {@code @Table} annotation. All updatable properties (excluding those marked with
         * {@code @ReadOnly} or {@code @NonUpdatable}) are included by default.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PSC.update(Account.class)
         *                 .set("status", "active")
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: UPDATE account SET status = ? WHERE id = ?
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder update(final Class<?> entityClass) {
            return update(entityClass, null);
        }

        /**
         * Creates an UPDATE statement for an entity class with excluded properties.
         * 
         * <p>This method creates an UPDATE statement excluding specified properties in addition to
         * those automatically excluded by annotations (@ReadOnly, @NonUpdatable). This is useful
         * for partial updates or when certain fields should never be updated.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("createdDate", "createdBy");
         * String cql = PSC.update(Account.class, excluded)
         *                 .set(account)
         *                 .where(Filters.eq("id", account.getId()))
         *                 .build().query();
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the update
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        @SuppressWarnings("deprecation")
        public static CqlBuilder update(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance.setEntityClass(entityClass);
            instance._tableName = getTableName(entityClass, instance._namingPolicy);
            instance._propOrColumnNames = QueryUtil.getUpdatePropNames(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for a single column or expression.
         *
         * <p>This method initializes a DELETE statement for one column. The column name will be
         * converted according to the snake_case naming policy. This is useful for deleting
         * specific columns from a row rather than the entire row.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PSC.delete("firstName")
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE first_name FROM account WHERE id = ?
         * }</pre>
         *
         * @param expr the column name or expression to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if expr is null or empty
         */
        public static CqlBuilder delete(final String expr) {
            N.checkArgNotEmpty(expr, DELETION_PART_MSG);

            return delete(N.asArray(expr));
        }

        /**
         * Creates a DELETE CQL builder for multiple columns.
         *
         * <p>This method initializes a DELETE statement for multiple columns. All column names
         * will be converted according to the snake_case naming policy. This allows selective
         * deletion of specific columns from rows.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PSC.delete("firstName", "lastName", "email")
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE first_name, last_name, email FROM account WHERE id = ?
         * }</pre>
         *
         * @param columnNames the column names to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if columnNames is null or empty
         */
        public static CqlBuilder delete(final String... columnNames) {
            N.checkArgNotEmpty(columnNames, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._propOrColumnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for a collection of columns.
         *
         * <p>This method is useful when column names are determined dynamically. The collection
         * can contain property names that will be converted to column names according to the
         * snake_case naming policy.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("firstName", "lastName", "email");
         * String cql = PSC.delete(columns)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE first_name, last_name, email FROM account WHERE id = ?
         * }</pre>
         *
         * @param columnNames the collection of column names to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if columnNames is null or empty
         */
        public static CqlBuilder delete(final Collection<String> columnNames) {
            N.checkArgNotEmpty(columnNames, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._propOrColumnNames = columnNames;

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for an entity class.
         *
         * <p>This method generates a DELETE statement for all deletable properties of the class.
         * Properties marked with @ReadOnly, @ReadOnlyId, or @Transient are automatically excluded.
         * This is useful for creating templates for partial row deletion.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PSC.delete(Account.class)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE first_name, last_name, email, status FROM account WHERE id = ?
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder delete(final Class<?> entityClass) {
            return delete(entityClass, null);
        }

        /**
         * Creates a DELETE CQL builder for an entity class, excluding specified properties.
         *
         * <p>This method generates a DELETE statement excluding both annotation-based exclusions
         * and the specified properties. Useful for selective column deletion where certain
         * fields should be preserved.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("email", "createdDate");
         * String cql = PSC.delete(Account.class, excluded)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE first_name, last_name, status FROM account WHERE id = ?
         * }</pre>
         *
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder delete(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates a DELETE FROM statement for a table.
         * 
         * <p>This method starts building a DELETE statement. Typically followed by WHERE conditions
         * to specify which rows to delete. Property names in WHERE conditions will be converted
         * to snake_case format if an entity class is associated.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PSC.deleteFrom("account")
         *                 .where(Filters.eq("status", "inactive"))
         *                 .build().query();
         * // Output: DELETE FROM account WHERE status = ?
         * }</pre>
         * 
         * @param tableName the name of the table to delete from
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder deleteFrom(final String tableName) {
            N.checkArgNotEmpty(tableName, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;

            return instance;
        }

        /**
         * Creates a DELETE FROM statement for a table with entity class mapping.
         * 
         * <p>This method creates a DELETE statement where the entity class provides property-to-column
         * name mapping for WHERE conditions. This ensures proper snake_case conversion for property
         * names used in conditions.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PSC.deleteFrom("account", Account.class)
         *                 .where(Filters.lt("lastLoginDate", thirtyDaysAgo))
         *                 .build().query();
         * // Output: DELETE FROM account WHERE last_login_date < ?
         * }</pre>
         * 
         * @param tableName the name of the table to delete from
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if tableName is null or empty, or entityClass is null
         */
        public static CqlBuilder deleteFrom(final String tableName, final Class<?> entityClass) {
            N.checkArgNotEmpty(tableName, DELETION_PART_MSG);
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;
            instance.setEntityClass(entityClass);

            return instance;
        }

        /**
         * Creates a DELETE FROM statement for an entity class.
         * 
         * <p>This method creates a DELETE statement where the table name is derived from the entity
         * class name or @Table annotation. Property names in WHERE conditions will be automatically
         * converted to snake_case format.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PSC.deleteFrom(Account.class)
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: DELETE FROM account WHERE id = ?
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder deleteFrom(final Class<?> entityClass) {
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance.setEntityClass(entityClass);
            instance._tableName = getTableName(entityClass, instance._namingPolicy);

            return instance;
        }

        /**
         * Creates a SELECT statement with a single expression.
         * 
         * <p>This method is useful for complex select expressions, aggregate functions, or when
         * selecting computed values. The expression is used as-is without property name conversion.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PSC.select("COUNT(*)")
         *                 .from("account")
         *                 .where(Filters.eq("status", "active"))
         *                 .build().query();
         * // Output: SELECT count(*) FROM account WHERE status = ?
         * 
         * String cql2 = PSC.select("firstName || ' ' || lastName AS fullName")
         *                  .from("account")
         *                  .build().query();
         * // Output: SELECT firstName || ' ' || lastName AS fullName FROM account
         * }</pre>
         * 
         * @param selectPart the select expression
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if selectPart is null or empty
         */
        public static CqlBuilder select(final String selectPart) {
            N.checkArgNotEmpty(selectPart, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = Array.asList(selectPart);
            return instance;
        }

        /**
         * Creates a SELECT statement with multiple columns.
         * 
         * <p>This method creates a SELECT statement for multiple columns. Property names are
         * converted to snake_case format and aliased back to their original camelCase names
         * to maintain proper object mapping.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PSC.select("id", "firstName", "lastName", "email")
         *                 .from("account")
         *                 .where(Filters.eq("status", "active"))
         *                 .build().query();
         * // Output: SELECT id, first_name AS "firstName", last_name AS "lastName", email FROM account WHERE status = ?
         * }</pre>
         * 
         * @param propOrColumnNames the property or column names to select
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder select(final String... propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = Array.asList(propOrColumnNames);

            return instance;
        }

        /**
         * Creates a SELECT statement with a collection of columns.
         * 
         * <p>This method provides flexibility when column names are dynamically generated. Property
         * names are converted to snake_case format with appropriate aliases.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("id", "firstName", "lastName");
         * String cql = PSC.select(columns)
         *                 .from("account")
         *                 .build().query();
         * // Output: SELECT id, first_name AS "firstName", last_name AS "lastName" FROM account
         * }</pre>
         * 
         * @param propOrColumnNames collection of property or column names to select
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder select(final Collection<String> propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = propOrColumnNames;

            return instance;
        }

        /**
         * Creates a SELECT statement with column aliases.
         * 
         * <p>This method allows specifying custom aliases for selected columns. The map keys are
         * property names (converted to snake_case) and values are their desired aliases in the
         * result set.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, String> columnAliases = new HashMap<>();
         * columnAliases.put("firstName", "fname");
         * columnAliases.put("lastName", "lname");
         * columnAliases.put("emailAddress", "email");
         * 
         * String cql = PSC.select(columnAliases)
         *                 .from("account")
         *                 .build().query();
         * // Output: SELECT first_name AS "fname", last_name AS "lname", email_address AS "email" FROM account
         * }</pre>
         * 
         * @param propOrColumnNameAliases Map of property/column names to their aliases
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if propOrColumnNameAliases is null or empty
         */
        public static CqlBuilder select(final Map<String, String> propOrColumnNameAliases) {
            N.checkArgNotEmpty(propOrColumnNameAliases, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNameAliases = propOrColumnNameAliases;

            return instance;
        }

        /**
         * Creates a SELECT statement for all properties of an entity class.
         * 
         * <p>This method generates a SELECT statement including all properties from the entity class
         * that are not marked with @Transient. Property names are converted to snake_case with
         * appropriate aliases.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PSC.select(Account.class)
         *                 .from("account")
         *                 .build().query();
         * // Output: SELECT id, first_name AS "firstName", last_name AS "lastName", email, created_date AS "createdDate" FROM account
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass) {
            return select(entityClass, false);
        }

        /**
         * Creates a SELECT statement for an entity class with optional sub-entity properties.
         * 
         * <p>When includeSubEntityProperties is true, properties of nested entity objects are also
         * included in the selection with appropriate prefixes. This is useful for fetching related
         * entities in a single query.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Without sub-entities
         * String cql1 = PSC.select(Order.class, false)
         *                  .from("orders")
         *                  .build().query();
         * 
         * // With sub-entities (includes nested object properties)
         * String cql2 = PSC.select(Order.class, true)
         *                  .from("orders")
         *                  .build().query();
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties Whether to include properties of nested entity objects
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass, final boolean includeSubEntityProperties) {
            return select(entityClass, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT statement for an entity class with excluded properties.
         * 
         * <p>This method selects all properties from the entity class except those specified in
         * the exclusion set. This is useful for queries that need most but not all properties.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("password", "secretKey");
         * String cql = PSC.select(Account.class, excluded)
         *                 .from("account")
         *                 .build().query();
         * // Selects all Account properties except password and secretKey
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return select(entityClass, false, excludedPropNames);
        }

        /**
         * Creates a SELECT statement for an entity class with sub-entities and exclusions.
         * 
         * <p>This method provides full control over entity property selection, allowing both
         * inclusion of sub-entity properties and exclusion of specific properties.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("internalNotes", "auditLog");
         * String cql = PSC.select(Order.class, true, excluded)
         *                 .from("orders")
         *                 .build().query();
         * // Selects all Order properties including sub-entities, except excluded ones
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties Whether to include properties of nested entity objects
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        @SuppressWarnings("deprecation")
        public static CqlBuilder select(final Class<?> entityClass, final boolean includeSubEntityProperties, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = QueryUtil.getSelectPropNames(entityClass, includeSubEntityProperties, excludedPropNames);

            return instance;
        }

        /**
         * Creates a complete SELECT FROM statement for an entity class.
         * 
         * <p>This is a convenience method that combines select() and from() operations.
         * The table name is automatically derived from the entity class name or @Table annotation.
         * All property names are converted to snake_case with appropriate aliases.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PSC.selectFrom(Account.class)
         *                 .where(Filters.eq("status", "active"))
         *                 .build().query();
         * // Output: SELECT id, first_name AS "firstName", last_name AS "lastName", email FROM account WHERE status = ?
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, false);
        }

        /**
         * Creates a SELECT FROM statement for an entity class with table alias.
         * 
         * <p>This method creates a SELECT FROM statement where columns are prefixed with the table
         * alias. This is useful for joins and disambiguating column names in complex queries.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PSC.selectFrom(Account.class, "a")
         *                 .where(Filters.eq("a.status", "active"))
         *                 .build().query();
         * // Output: SELECT a.id, a.first_name AS "firstName", a.last_name AS "lastName", a.email FROM account a WHERE a.status = ?
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias) {
            return selectFrom(entityClass, alias, false);
        }

        /**
         * Creates a SELECT FROM statement with optional sub-entity properties.
         * 
         * <p>This convenience method combines SELECT and FROM operations with control over
         * sub-entity inclusion. When sub-entities are included, appropriate joins may be
         * generated automatically.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PSC.selectFrom(Order.class, true)
         *                 .where(Filters.gt("total", 100))
         *                 .build().query();
         * // Includes properties from nested entities like customer, items, etc.
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties Whether to include properties of nested entity objects
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final boolean includeSubEntityProperties) {
            return selectFrom(entityClass, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT FROM statement with alias and sub-entity option.
         * 
         * <p>This method combines table aliasing with sub-entity property inclusion for
         * complex queries involving related entities.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PSC.selectFrom(Order.class, "o", true)
         *                 .where(Filters.eq("o.status", "pending"))
         *                 .build().query();
         * // Selects from orders with alias 'o' including sub-entity properties
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @param includeSubEntityProperties Whether to include properties of nested entity objects
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final boolean includeSubEntityProperties) {
            return selectFrom(entityClass, alias, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT FROM statement with excluded properties.
         * 
         * <p>This convenience method creates a complete SELECT FROM statement while excluding
         * specific properties from the selection.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("password", "secretKey");
         * String cql = PSC.selectFrom(Account.class, excluded)
         *                 .where(Filters.eq("active", true))
         *                 .build().query();
         * // Selects all properties except password and secretKey
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, false, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM statement with alias and excluded properties.
         * 
         * <p>This method combines table aliasing with property exclusion for precise control
         * over the generated SELECT statement.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("password");
         * String cql = PSC.selectFrom(Account.class, "a", excluded)
         *                 .innerJoin("orders o").on("a.id = o.account_id")
         *                 .build().query();
         * // Selects from account with alias 'a', excluding password
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, alias, false, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM statement with sub-entities and exclusions.
         * 
         * <p>This method provides control over both sub-entity inclusion and property exclusion
         * while automatically determining the appropriate table alias.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("internalData");
         * String cql = PSC.selectFrom(Order.class, true, excluded)
         *                 .where(Filters.between("orderDate", startDate, endDate))
         *                 .build().query();
         * // Includes sub-entities but excludes internalData
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties Whether to include properties of nested entity objects
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final boolean includeSubEntityProperties, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, QueryUtil.getTableAlias(entityClass), includeSubEntityProperties, excludedPropNames);
        }

        /**
         * Creates a complete SELECT FROM statement with all options.
         * 
         * <p>This method provides maximum flexibility by allowing control over table alias,
         * sub-entity inclusion, and property exclusion. It handles complex scenarios including
         * automatic join generation for sub-entities.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("password", "internalNotes");
         * String cql = PSC.selectFrom(Account.class, "a", true, excluded)
         *                 .innerJoin("orders o").on("a.id = o.account_id")
         *                 .where(Filters.gt("o.total", 1000))
         *                 .build().query();
         * // Complex query with full control over selection
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @param includeSubEntityProperties Whether to include properties of nested entity objects
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final boolean includeSubEntityProperties,
                final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            if (hasSubEntityToInclude(entityClass, includeSubEntityProperties)) {
                final List<String> selectTableNames = getSelectTableNames(entityClass, alias, excludedPropNames, NamingPolicy.SNAKE_CASE);
                return select(entityClass, includeSubEntityProperties, excludedPropNames).from(entityClass, selectTableNames);
            }

            return select(entityClass, includeSubEntityProperties, excludedPropNames).from(entityClass, alias);
        }

        /**
         * Creates a COUNT(*) query for a table.
         * 
         * <p>Convenience method for generating count queries. This is equivalent to
         * {@code select("COUNT(*)").from(tableName)} but more expressive.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PSC.count("account")
         *                 .where(Filters.eq("status", "active"))
         *                 .build().query();
         * // Output: SELECT count(*) FROM account WHERE status = ?
         * }</pre>
         * 
         * @param tableName the name of the table to count rows from
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder count(final String tableName) {
            N.checkArgNotEmpty(tableName, SELECTION_PART_MSG);

            return select(COUNT_ALL_LIST).from(tableName);
        }

        /**
         * Creates a COUNT(*) query for an entity class.
         * 
         * <p>The table name is derived from the entity class name or @Table annotation.
         * This is a convenient way to count rows with entity class mapping for proper
         * property name conversion in WHERE conditions.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PSC.count(Account.class)
         *                 .where(Filters.isNotNull("email"))
         *                 .build().query();
         * // Output: SELECT count(*) FROM account WHERE email IS NOT NULL
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder count(final Class<?> entityClass) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            return select(COUNT_ALL_LIST).from(entityClass);
        }

        /**
         * Parses a condition into CQL with entity class mapping.
         * 
         * <p>This method is useful for generating just the WHERE clause portion of a query
         * with proper property-to-column name mapping.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Condition cond = Filters.and(
         *     Filters.eq("firstName", "John"),
         *     Filters.like("email", "%@example.com")
         * );
         * 
         * String cql = PSC.parse(cond, Account.class).build().query();
         * // Output: first_name = ? AND email LIKE ?
         * }</pre>
         * 
         * @param cond the condition to parse
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance containing the parsed condition
         * @throws IllegalArgumentException if cond is null
         */
        public static CqlBuilder parse(final Condition cond, final Class<?> entityClass) {
            N.checkArgNotNull(cond, "cond");

            final CqlBuilder instance = createInstance();

            instance.setEntityClass(entityClass);
            instance._op = OperationType.QUERY;
            instance._isForConditionOnly = true;
            instance.append(cond);

            return instance;
        }
    }

    /**
     * Parameterized CQL builder with SCREAMING_SNAKE_CASE naming policy.
     * 
     * <p>This builder generates parameterized CQL statements (using '?' placeholders) with column names 
     * converted to uppercase with underscores. This follows the traditional database naming convention.</p>
     * 
     * <p>Key features:</p>
     * <ul>
     *   <li>Converts camelCase property names to SCREAMING_SNAKE_CASE column names</li>
     *   <li>Uses '?' placeholders for parameter binding</li>
     *   <li>Maintains property name aliases in result sets for proper object mapping</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Property 'firstName' becomes column 'FIRST_NAME'
     * String cql = PAC.select("firstName", "lastName")
     *                 .from("account")
     *                 .where(Filters.eq("id", 1))
     *                 .build().query();
     * // Output: SELECT FIRST_NAME AS "firstName", LAST_NAME AS "lastName" FROM ACCOUNT WHERE ID = ?
     * }</pre>
     */
    public static class PAC extends CqlBuilder {

        PAC() {
            super(NamingPolicy.SCREAMING_SNAKE_CASE, SQLPolicy.PARAMETERIZED_SQL);
        }

        protected static PAC createInstance() {
            return new PAC();
        }

        /**
         * Creates an INSERT statement for a single expression or column.
         * 
         * <p>This method is a convenience wrapper that delegates to {@link #insert(String...)} 
         * with a single element array.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PAC.insert("name").into("users").build().query();
         * // Output: INSERT INTO USERS (NAME) VALUES (?)
         * }</pre>
         * 
         * @param expr the column name or expression to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if expr is null or empty
         */
        public static CqlBuilder insert(final String expr) {
            N.checkArgNotEmpty(expr, INSERTION_PART_MSG);

            return insert(N.asArray(expr));
        }

        /**
         * Creates an INSERT statement for specified columns.
         * 
         * <p>The column names will be converted according to the SCREAMING_SNAKE_CASE naming policy.
         * Use {@link #into(String)} to specify the target table.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PAC.insert("firstName", "lastName", "email")
         *                 .into("users")
         *                 .build().query();
         * // Output: INSERT INTO USERS (FIRST_NAME, LAST_NAME, EMAIL) VALUES (?, ?, ?)
         * }</pre>
         * 
         * @param propOrColumnNames the property or column names to include in the INSERT
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder insert(final String... propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._propOrColumnNames = Array.asList(propOrColumnNames);

            return instance;
        }

        /**
         * Creates an INSERT statement for specified columns from a collection.
         * 
         * <p>This method accepts a collection of column names, providing flexibility when 
         * the column list is dynamically generated.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("firstName", "lastName", "email");
         * String cql = PAC.insert(columns).into("users").build().query();
         * // Output: INSERT INTO USERS (FIRST_NAME, LAST_NAME, EMAIL) VALUES (?, ?, ?)
         * }</pre>
         * 
         * @param propOrColumnNames collection of property or column names to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder insert(final Collection<String> propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._propOrColumnNames = propOrColumnNames;

            return instance;
        }

        /**
         * Creates an INSERT statement from a map of property names to values.
         * 
         * <p>The map keys represent column names and will be converted according to the naming policy.
         * The values are used to determine the number of parameter placeholders needed.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Object> data = new HashMap<>();
         * data.put("firstName", "John");
         * data.put("lastName", "Doe");
         * String cql = PAC.insert(data).into("users").build().query();
         * // Output: INSERT INTO USERS (FIRST_NAME, LAST_NAME) VALUES (?, ?)
         * }</pre>
         * 
         * @param props map of property names to values
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if props is null or empty
         */
        public static CqlBuilder insert(final Map<String, Object> props) {
            N.checkArgNotEmpty(props, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._props = props;

            return instance;
        }

        /**
         * Creates an INSERT statement from an entity object.
         * 
         * <p>This method inspects the entity object and includes all properties that are not marked 
         * with exclusion annotations (@Transient, @ReadOnly, etc.). The table name is inferred 
         * from the entity class or @Table annotation.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User user = new User("John", "Doe", "john@example.com");
         * String cql = PAC.insert(user).into("users").build().query();
         * // Output: INSERT INTO USERS (FIRST_NAME, LAST_NAME, EMAIL) VALUES (?, ?, ?)
         * }</pre>
         * 
         * @param entity the entity object to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entity is null
         */
        public static CqlBuilder insert(final Object entity) {
            return insert(entity, null);
        }

        /**
         * Creates an INSERT statement from an entity object with excluded properties.
         * 
         * <p>This method allows fine-grained control over which properties to include in the INSERT.
         * Properties in the exclusion set will not be included even if they are normally insertable.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User user = new User("John", "Doe", "john@example.com");
         * Set<String> exclude = new HashSet<>(Arrays.asList("createdDate", "modifiedDate"));
         * String cql = PAC.insert(user, exclude).into("users").build().query();
         * // Output: INSERT INTO USERS (FIRST_NAME, LAST_NAME, EMAIL) VALUES (?, ?, ?)
         * }</pre>
         * 
         * @param entity the entity object to insert
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entity is null
         */
        public static CqlBuilder insert(final Object entity, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entity, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance.setEntityClass(entity.getClass());

            parseInsertEntity(instance, entity, excludedPropNames);

            return instance;
        }

        /**
         * Creates an INSERT statement template for an entity class.
         * 
         * <p>This method generates an INSERT statement based on the class structure without 
         * requiring an actual entity instance. All insertable properties are included.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PAC.insert(User.class).into("users").build().query();
         * // Output: INSERT INTO USERS (FIRST_NAME, LAST_NAME, EMAIL) VALUES (?, ?, ?)
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insert(final Class<?> entityClass) {
            return insert(entityClass, null);
        }

        /**
         * Creates an INSERT statement template for an entity class with excluded properties.
         * 
         * <p>This method generates an INSERT statement based on the class structure, excluding 
         * specified properties. Useful for creating reusable INSERT templates.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> exclude = new HashSet<>(Arrays.asList("id", "version"));
         * String cql = PAC.insert(User.class, exclude).into("users").build().query();
         * // Output: INSERT INTO USERS (FIRST_NAME, LAST_NAME, EMAIL) VALUES (?, ?, ?)
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        @SuppressWarnings("deprecation")
        public static CqlBuilder insert(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = QueryUtil.getInsertPropNames(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates an INSERT statement for an entity class with automatic table name resolution.
         * 
         * <p>This is a convenience method that combines {@link #insert(Class)} with {@link #into(Class)}.
         * The table name is determined from the @Table annotation or class name.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PAC.insertInto(User.class).build().query();
         * // Output: INSERT INTO USERS (FIRST_NAME, LAST_NAME, EMAIL) VALUES (?, ?, ?)
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insertInto(final Class<?> entityClass) {
            return insertInto(entityClass, null);
        }

        /**
         * Creates an INSERT statement for an entity class with excluded properties and automatic table name.
         * 
         * <p>Combines the functionality of specifying excluded properties with automatic table name resolution.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> exclude = new HashSet<>(Arrays.asList("id"));
         * String cql = PAC.insertInto(User.class, exclude).build().query();
         * // Output: INSERT INTO USERS (FIRST_NAME, LAST_NAME, EMAIL) VALUES (?, ?, ?)
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insertInto(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return insert(entityClass, excludedPropNames).into(entityClass);
        }

        /**
         * Creates a batch INSERT statement for multiple entities (MyCQL style).
         * 
         * <p>This method generates a single INSERT statement with multiple value sets, 
         * which is more efficient than multiple individual INSERTs. This is particularly 
         * useful for MyCQL and compatible databases.</p>
         * 
         * <p>Note: This is a beta feature and may change in future versions.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<User> users = Arrays.asList(
         *     new User("John", "Doe"),
         *     new User("Jane", "Smith")
         * );
         * String cql = PAC.batchInsert(users).into("users").build().query();
         * // Output: INSERT INTO USERS (FIRST_NAME, LAST_NAME) VALUES (?, ?), (?, ?)
         * }</pre>
         * 
         * @param propsList collection of entities or property maps to insert
         * @return a new CqlBuilder instance configured for batch INSERT operation
         * @throws IllegalArgumentException if propsList is null or empty
         */
        @Beta
        public static CqlBuilder batchInsert(final Collection<?> propsList) {
            N.checkArgNotEmpty(propsList, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            final Optional<?> first = N.firstNonNull(propsList);

            if (first.isPresent() && Beans.isBeanClass(first.get().getClass())) {
                instance.setEntityClass(first.get().getClass());
            }

            instance._propsList = toInsertPropsList(propsList);

            return instance;
        }

        /**
         * Creates an UPDATE statement for a specified table.
         * 
         * <p>This method starts building an UPDATE statement. Use {@link #set(String...)} 
         * to specify which columns to update.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PAC.update("users")
         *                 .set("firstName", "lastName")
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: UPDATE USERS SET FIRST_NAME = ?, LAST_NAME = ? WHERE ID = ?
         * }</pre>
         * 
         * @param tableName the name of the table to update
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder update(final String tableName) {
            N.checkArgNotEmpty(tableName, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;

            return instance;
        }

        /**
         * Creates an UPDATE statement for a table with entity class context.
         * 
         * <p>This method allows specifying both the table name and entity class, 
         * which enables proper property-to-column name mapping.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PAC.update("users", User.class)
         *                 .set("firstName", "lastName")
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: UPDATE USERS SET FIRST_NAME = ?, LAST_NAME = ? WHERE ID = ?
         * }</pre>
         * 
         * @param tableName the name of the table to update
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if tableName or entityClass is null
         */
        public static CqlBuilder update(final String tableName, final Class<?> entityClass) {
            N.checkArgNotEmpty(tableName, UPDATE_PART_MSG);
            N.checkArgNotNull(entityClass, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;
            instance.setEntityClass(entityClass);

            return instance;
        }

        /**
         * Creates an UPDATE statement for an entity class with automatic table name.
         * 
         * <p>The table name is determined from the @Table annotation or class name. 
         * All updatable properties (excluding @ReadOnly, @NonUpdatable) are included.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PAC.update(User.class)
         *                 .set("firstName", "lastName")
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: UPDATE USERS SET FIRST_NAME = ?, LAST_NAME = ? WHERE ID = ?
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder update(final Class<?> entityClass) {
            return update(entityClass, null);
        }

        /**
         * Creates an UPDATE statement for an entity class with excluded properties.
         * 
         * <p>This method generates an UPDATE statement excluding specified properties 
         * in addition to those marked with @ReadOnly or @NonUpdatable annotations.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> exclude = new HashSet<>(Arrays.asList("version", "modifiedDate"));
         * String cql = PAC.update(User.class, exclude)
         *                 .set("firstName", "lastName")
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: UPDATE USERS SET FIRST_NAME = ?, LAST_NAME = ? WHERE ID = ?
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the update
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        @SuppressWarnings("deprecation")
        public static CqlBuilder update(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance.setEntityClass(entityClass);
            instance._tableName = getTableName(entityClass, instance._namingPolicy);
            instance._propOrColumnNames = QueryUtil.getUpdatePropNames(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for a single column or expression.
         *
         * <p>This method initializes a DELETE statement for one column. The column name will be
         * converted according to the camelCase naming policy. This is useful for deleting
         * specific columns from a row rather than the entire row.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PAC.delete("firstName")
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE FIRST_NAME FROM account WHERE ID = ?
         * }</pre>
         *
         * @param expr the column name or expression to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if expr is null or empty
         */
        public static CqlBuilder delete(final String expr) {
            N.checkArgNotEmpty(expr, DELETION_PART_MSG);

            return delete(N.asArray(expr));
        }

        /**
         * Creates a DELETE CQL builder for multiple columns.
         *
         * <p>This method initializes a DELETE statement for multiple columns. All column names
         * will be converted according to the camelCase naming policy. This allows selective
         * deletion of specific columns from rows.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PAC.delete("firstName", "lastName", "email")
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE FIRST_NAME, LAST_NAME, EMAIL FROM account WHERE ID = ?
         * }</pre>
         *
         * @param columnNames the column names to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if columnNames is null or empty
         */
        public static CqlBuilder delete(final String... columnNames) {
            N.checkArgNotEmpty(columnNames, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._propOrColumnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for a collection of columns.
         *
         * <p>This method is useful when column names are determined dynamically. The collection
         * can contain property names that will be converted to column names according to the
         * camelCase naming policy.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("firstName", "lastName", "email");
         * String cql = PAC.delete(columns)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE FIRST_NAME, LAST_NAME, EMAIL FROM account WHERE ID = ?
         * }</pre>
         *
         * @param columnNames the collection of column names to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if columnNames is null or empty
         */
        public static CqlBuilder delete(final Collection<String> columnNames) {
            N.checkArgNotEmpty(columnNames, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._propOrColumnNames = columnNames;

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for an entity class.
         *
         * <p>This method generates a DELETE statement for all deletable properties of the class.
         * Properties marked with @ReadOnly, @ReadOnlyId, or @Transient are automatically excluded.
         * This is useful for creating templates for partial row deletion.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PAC.delete(Account.class)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE FIRST_NAME, LAST_NAME, EMAIL, STATUS FROM account WHERE ID = ?
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder delete(final Class<?> entityClass) {
            return delete(entityClass, null);
        }

        /**
         * Creates a DELETE CQL builder for an entity class, excluding specified properties.
         *
         * <p>This method generates a DELETE statement excluding both annotation-based exclusions
         * and the specified properties. Useful for selective column deletion where certain
         * fields should be preserved.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("email", "createdDate");
         * String cql = PAC.delete(Account.class, excluded)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE FIRST_NAME, LAST_NAME, STATUS FROM account WHERE ID = ?
         * }</pre>
         *
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder delete(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates a DELETE statement for a specified table.
         * 
         * <p>This method starts building a DELETE FROM statement. Typically followed 
         * by WHERE conditions to specify which rows to delete.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PAC.deleteFrom("users")
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: DELETE FROM USERS WHERE ID = ?
         * }</pre>
         * 
         * @param tableName the name of the table to delete from
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder deleteFrom(final String tableName) {
            N.checkArgNotEmpty(tableName, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;

            return instance;
        }

        /**
         * Creates a DELETE statement for a table with entity class context.
         * 
         * <p>This method allows specifying both the table name and entity class 
         * for proper property-to-column name mapping in WHERE conditions.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PAC.deleteFrom("users", User.class)
         *                 .where(Filters.eq("email", "john@example.com"))
         *                 .build().query();
         * // Output: DELETE FROM USERS WHERE EMAIL = ?
         * }</pre>
         * 
         * @param tableName the name of the table to delete from
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if tableName or entityClass is null
         */
        public static CqlBuilder deleteFrom(final String tableName, final Class<?> entityClass) {
            N.checkArgNotEmpty(tableName, DELETION_PART_MSG);
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;
            instance.setEntityClass(entityClass);

            return instance;
        }

        /**
         * Creates a DELETE statement for an entity class with automatic table name.
         * 
         * <p>The table name is determined from the @Table annotation or class name.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PAC.deleteFrom(User.class)
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: DELETE FROM USERS WHERE ID = ?
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder deleteFrom(final Class<?> entityClass) {
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance.setEntityClass(entityClass);
            instance._tableName = getTableName(entityClass, instance._namingPolicy);

            return instance;
        }

        /**
         * Creates a SELECT statement with a single expression or column.
         * 
         * <p>This method can accept complex expressions like aggregate functions, 
         * calculations, or simple column names.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PAC.select("COUNT(*)").from("users").build().query();
         * // Output: SELECT count(*) FROM USERS
         * 
         * String cql2 = PAC.select("MAX(age)").from("users").build().query();
         * // Output: SELECT MAX(AGE) FROM USERS
         * }</pre>
         * 
         * @param selectPart the select expression
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if selectPart is null or empty
         */
        public static CqlBuilder select(final String selectPart) {
            N.checkArgNotEmpty(selectPart, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = Array.asList(selectPart);
            return instance;
        }

        /**
         * Creates a SELECT statement with multiple columns.
         * 
         * <p>Column names will be converted according to the SCREAMING_SNAKE_CASE 
         * naming policy and aliased back to their original property names.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PAC.select("firstName", "lastName", "email")
         *                 .from("users")
         *                 .build().query();
         * // Output: SELECT FIRST_NAME AS "firstName", LAST_NAME AS "lastName", EMAIL AS "email" FROM USERS
         * }</pre>
         * 
         * @param propOrColumnNames the property or column names to select
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder select(final String... propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = Array.asList(propOrColumnNames);

            return instance;
        }

        /**
         * Creates a SELECT statement with columns from a collection.
         * 
         * <p>This method accepts a collection of column names, useful when the column 
         * list is dynamically generated.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("firstName", "lastName", "email");
         * String cql = PAC.select(columns).from("users").build().query();
         * // Output: SELECT FIRST_NAME AS "firstName", LAST_NAME AS "lastName", EMAIL AS "email" FROM USERS
         * }</pre>
         * 
         * @param propOrColumnNames collection of property or column names to select
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder select(final Collection<String> propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = propOrColumnNames;

            return instance;
        }

        /**
         * Creates a SELECT statement with column aliases.
         * 
         * <p>This method allows specifying custom aliases for selected columns. 
         * The map keys are column names and values are their aliases.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, String> aliases = new LinkedHashMap<>();
         * aliases.put("firstName", "fname");
         * aliases.put("lastName", "lname");
         * String cql = PAC.select(aliases).from("users").build().query();
         * // Output: SELECT FIRST_NAME AS "fname", LAST_NAME AS "lname" FROM USERS
         * }</pre>
         * 
         * @param propOrColumnNameAliases map of column names to their aliases
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if propOrColumnNameAliases is null or empty
         */
        public static CqlBuilder select(final Map<String, String> propOrColumnNameAliases) {
            N.checkArgNotEmpty(propOrColumnNameAliases, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNameAliases = propOrColumnNameAliases;

            return instance;
        }

        /**
         * Creates a SELECT statement for all properties of an entity class.
         * 
         * <p>This method selects all properties from the entity class that are not 
         * marked with @Transient. Sub-entity properties are not included by default.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PAC.select(User.class).from("users").build().query();
         * // Output: SELECT ID AS "id", FIRST_NAME AS "firstName", LAST_NAME AS "lastName", EMAIL AS "email" FROM USERS
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass) {
            return select(entityClass, false);
        }

        /**
         * Creates a SELECT statement for an entity class with sub-entity control.
         * 
         * <p>When includeSubEntityProperties is true, properties of nested entity types 
         * are also included in the selection with appropriate prefixes.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // If User has an Address sub-entity
         * String cql = PAC.select(User.class, true).from("users").build().query();
         * // Output includes address properties: ADDRESS_STREET AS "address.street", etc.
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties whether to include properties from sub-entities
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass, final boolean includeSubEntityProperties) {
            return select(entityClass, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT statement for an entity class with excluded properties.
         * 
         * <p>This method selects all properties except those specified in the exclusion set.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> exclude = new HashSet<>(Arrays.asList("password", "salt"));
         * String cql = PAC.select(User.class, exclude).from("users").build().query();
         * // Output excludes password and salt columns
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return select(entityClass, false, excludedPropNames);
        }

        /**
         * Creates a SELECT statement with full control over entity property selection.
         * 
         * <p>This method combines sub-entity inclusion control with property exclusion, 
         * providing maximum flexibility in column selection.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> exclude = new HashSet<>(Arrays.asList("password"));
         * String cql = PAC.select(User.class, true, exclude)
         *                 .from("users")
         *                 .build().query();
         * // Output includes sub-entity properties but excludes password
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties whether to include properties from sub-entities
         * @param excludedPropNames properties to exclude
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        @SuppressWarnings("deprecation")
        public static CqlBuilder select(final Class<?> entityClass, final boolean includeSubEntityProperties, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = QueryUtil.getSelectPropNames(entityClass, includeSubEntityProperties, excludedPropNames);

            return instance;
        }

        /**
         * Creates a complete SELECT FROM statement for an entity class.
         * 
         * <p>This convenience method combines SELECT and FROM operations. The table name 
         * is derived from the @Table annotation or entity class name.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PAC.selectFrom(User.class).where(Filters.eq("active", true)).build().query();
         * // Output: SELECT ID AS "id", FIRST_NAME AS "firstName", ... FROM USERS WHERE ACTIVE = ?
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, false);
        }

        /**
         * Creates a SELECT FROM statement with a table alias.
         * 
         * <p>The alias is used to qualify column names in the generated CQL, useful 
         * for self-joins or disambiguating columns in complex queries.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PAC.selectFrom(User.class, "u")
         *                 .where(Filters.eq("u.active", true))
         *                 .build().query();
         * // Output: SELECT u.ID AS "id", u.FIRST_NAME AS "firstName", ... FROM USERS u WHERE u.ACTIVE = ?
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias) {
            return selectFrom(entityClass, alias, false);
        }

        /**
         * Creates a SELECT FROM statement with sub-entity property control.
         * 
         * <p>When includeSubEntityProperties is true and the entity has sub-entities, 
         * appropriate joins may be generated automatically.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PAC.selectFrom(User.class, true)
         *                 .where(Filters.eq("active", true))
         *                 .build().query();
         * // Output includes joins for sub-entities if present
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties whether to include sub-entity properties
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final boolean includeSubEntityProperties) {
            return selectFrom(entityClass, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT FROM statement with table alias and sub-entity control.
         * 
         * <p>Combines table aliasing with sub-entity property inclusion for complex queries.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PAC.selectFrom(User.class, "u", true)
         *                 .where(Filters.eq("u.active", true))
         *                 .build().query();
         * // Output: SELECT u.ID AS "id", ... FROM USERS u WHERE u.ACTIVE = ?
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @param includeSubEntityProperties whether to include sub-entity properties
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final boolean includeSubEntityProperties) {
            return selectFrom(entityClass, alias, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT FROM statement with excluded properties.
         * 
         * <p>This method provides a convenient way to select from an entity while 
         * excluding specific properties.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> exclude = new HashSet<>(Arrays.asList("password"));
         * String cql = PAC.selectFrom(User.class, exclude).build().query();
         * // Output excludes the password column
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, false, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM statement with alias and excluded properties.
         * 
         * <p>Combines table aliasing with property exclusion for precise query control.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> exclude = new HashSet<>(Arrays.asList("password"));
         * String cql = PAC.selectFrom(User.class, "u", exclude).build().query();
         * // Output: SELECT u.ID AS "id", ... FROM USERS u (excluding password)
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @param excludedPropNames properties to exclude
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, alias, false, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM statement with sub-entity control and exclusions.
         * 
         * <p>Provides control over both sub-entity inclusion and property exclusion.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> exclude = new HashSet<>(Arrays.asList("password"));
         * String cql = PAC.selectFrom(User.class, true, exclude).build().query();
         * // Output includes sub-entities but excludes password
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties whether to include sub-entity properties
         * @param excludedPropNames properties to exclude
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final boolean includeSubEntityProperties, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, QueryUtil.getTableAlias(entityClass), includeSubEntityProperties, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM statement with full control over all options.
         * 
         * <p>This method provides maximum flexibility by allowing control over table alias, 
         * sub-entity inclusion, and property exclusion.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> exclude = new HashSet<>(Arrays.asList("password"));
         * String cql = PAC.selectFrom(User.class, "u", true, exclude)
         *                 .innerJoin("addresses", "a").on("u.id = a.user_id")
         *                 .build().query();
         * // Complex query with full control
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @param includeSubEntityProperties whether to include sub-entity properties
         * @param excludedPropNames properties to exclude
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final boolean includeSubEntityProperties,
                final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            if (hasSubEntityToInclude(entityClass, includeSubEntityProperties)) {
                final List<String> selectTableNames = getSelectTableNames(entityClass, alias, excludedPropNames, NamingPolicy.SCREAMING_SNAKE_CASE);
                //noinspection ConstantValue
                return select(entityClass, includeSubEntityProperties, excludedPropNames).from(entityClass, selectTableNames);
            }

            return select(entityClass, includeSubEntityProperties, excludedPropNames).from(entityClass, alias);
        }

        /**
         * Creates a COUNT(*) query for a table.
         * 
         * <p>Convenience method for generating count queries.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PAC.count("users").where(Filters.eq("active", true)).build().query();
         * // Output: SELECT count(*) FROM USERS WHERE ACTIVE = ?
         * }</pre>
         * 
         * @param tableName the name of the table to count rows from
         * @return a new CqlBuilder instance configured for COUNT operation
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder count(final String tableName) {
            N.checkArgNotEmpty(tableName, SELECTION_PART_MSG);

            return select(COUNT_ALL_LIST).from(tableName);
        }

        /**
         * Creates a COUNT(*) query for an entity class.
         * 
         * <p>Table name is derived from the entity class.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PAC.count(User.class)
         *                 .where(Filters.gt("age", 18))
         *                 .build().query();
         * // Output: SELECT count(*) FROM USERS WHERE AGE > ?
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for COUNT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder count(final Class<?> entityClass) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            return select(COUNT_ALL_LIST).from(entityClass);
        }

        /**
         * Parses a condition into CQL with entity property mapping.
         * 
         * <p>This method is useful for generating just the CQL representation of a condition, 
         * typically for use in complex queries or debugging.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Condition cond = Filters.and(Filters.eq("firstName", "John"), Filters.gt("age", 21));
         * String cql = PAC.parse(cond, User.class).build().query();
         * // Output: FIRST_NAME = ? AND AGE > ?
         * }</pre>
         * 
         * @param cond the condition to parse
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance containing the parsed condition
         * @throws IllegalArgumentException if cond is null
         */
        public static CqlBuilder parse(final Condition cond, final Class<?> entityClass) {
            N.checkArgNotNull(cond, "cond");

            final CqlBuilder instance = createInstance();

            instance.setEntityClass(entityClass);
            instance._op = OperationType.QUERY;
            instance._isForConditionOnly = true;
            instance.append(cond);

            return instance;
        }
    }

    /**
     * Parameterized CQL builder with camelCase field/column naming strategy.
     * 
     * <p>PLC (Parameterized Lower Camel Case) generates CQL with placeholder parameters (?) 
     * while maintaining camelCase naming for both properties and columns. This is useful when 
     * your database columns follow camelCase naming convention instead of the traditional 
     * snake_case.</p>
     * 
     * <p><b>Naming Convention:</b></p>
     * <ul>
     *   <li>Property: firstName → Column: firstName</li>
     *   <li>Property: accountNumber → Column: accountNumber</li>
     *   <li>Property: isActive → Column: isActive</li>
     * </ul>
     * 
     * <p><b>Basic Usage Examples:</b></p>
     * <pre>{@code
     * // Simple SELECT
     * String cql = PLC.select("firstName", "lastName")
     *                 .from("account")
     *                 .where(Filters.eq("id", 1))
     *                 .build().query();
     * // Output: SELECT firstName, lastName FROM account WHERE id = ?
     * 
     * // INSERT with entity
     * Account account = new Account();
     * account.setFirstName("John");
     * account.setLastName("Doe");
     * String cql = PLC.insert(account).into("account").build().query();
     * // Output: INSERT INTO account (firstName, lastName) VALUES (?, ?)
     * 
     * // UPDATE with specific fields
     * String cql = PLC.update("account")
     *                 .set("firstName", "John")
     *                 .set("lastName", "Smith")
     *                 .where(Filters.eq("id", 1))
     *                 .build().query();
     * // Output: UPDATE account SET firstName = ?, lastName = ? WHERE id = ?
     * }</pre>
     * 
     * <p><b>Advanced Examples:</b></p>
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Complex JOIN query with camelCase columns
     * String cql = PLC.select("a.id", "a.firstName", "COUNT(o.id) AS orderCount")
     *                 .from("account a")
     *                 .leftJoin("orders o").on("a.id = o.accountId")
     *                 .groupBy("a.id", "a.firstName")
     *                 .having(Filters.gt("COUNT(o.id)", 5))
     *                 .build().query();
     * 
     * // Using with MongoDB-style collections
     * String cql = PLC.selectFrom(UserProfile.class)
     *                 .where(Filters.and(
     *                     Filters.eq("isActive", true),
     *                     Filters.gte("lastLoginDate", lastWeek)
     *                 ))
     *                 .orderBy("lastLoginDate DESC")
     *                 .build().query();
     * }</pre>
     * 
     * @see CqlBuilder
     * @see PSC
     * @see NLC
     */
    public static class PLC extends CqlBuilder {

        PLC() {
            super(NamingPolicy.CAMEL_CASE, SQLPolicy.PARAMETERIZED_SQL);
        }

        protected static PLC createInstance() {
            return new PLC();
        }

        /**
         * Creates an INSERT statement for a single column expression.
         * 
         * <p>This method generates an INSERT statement with a single column using camelCase naming.
         * The generated CQL will use placeholder parameters (?) for the values.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PLC.insert("firstName").into("account").build().query();
         * // Output: INSERT INTO account (firstName) VALUES (?)
         * }</pre>
         * 
         * @param expr the column name or expression to insert
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if expr is null or empty
         */
        public static CqlBuilder insert(final String expr) {
            N.checkArgNotEmpty(expr, INSERTION_PART_MSG);

            return insert(N.asArray(expr));
        }

        /**
         * Creates an INSERT statement for multiple columns.
         * 
         * <p>This method generates an INSERT statement with multiple columns using camelCase naming.
         * The order of columns in the INSERT statement matches the order provided in the parameters.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PLC.insert("firstName", "lastName", "email")
         *                 .into("account")
         *                 .build().query();
         * // Output: INSERT INTO account (firstName, lastName, email) VALUES (?, ?, ?)
         * 
         * // With actual values using pair()
         * SP cqlPair = PLC.insert("firstName", "lastName", "email")
         *                 .into("account")
         *                 .values("John", "Doe", "john@example.com")
         *                 .pair();
         * // cqlPair.cql: INSERT INTO account (firstName, lastName, email) VALUES (?, ?, ?)
         * // cqlPair.parameters: ["John", "Doe", "john@example.com"]
         * }</pre>
         * 
         * @param propOrColumnNames the property or column names to insert
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder insert(final String... propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._propOrColumnNames = Array.asList(propOrColumnNames);

            return instance;
        }

        /**
         * Creates an INSERT statement for a collection of columns.
         * 
         * <p>This method is useful when the column list is dynamically generated or comes from
         * another source. The columns maintain their camelCase naming.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("firstName", "lastName", "email");
         * String cql = PLC.insert(columns).into("account").build().query();
         * // Output: INSERT INTO account (firstName, lastName, email) VALUES (?, ?, ?)
         * 
         * // Dynamic column selection
         * List<String> requiredFields = getRequiredFields();
         * String cql = PLC.insert(requiredFields).into("userProfile").build().query();
         * }</pre>
         * 
         * @param propOrColumnNames collection of property or column names to insert
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder insert(final Collection<String> propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._propOrColumnNames = propOrColumnNames;

            return instance;
        }

        /**
         * Creates an INSERT statement from a map of property names and values.
         * 
         * <p>This method is particularly useful when you have a dynamic set of fields to insert.
         * The map keys represent column names and values represent the data to insert.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Object> props = new HashMap<>();
         * props.put("firstName", "John");
         * props.put("lastName", "Doe");
         * props.put("emailAddress", "john.doe@example.com");
         * props.put("isActive", true);
         * 
         * SP cqlPair = PLC.insert(props).into("account").pair();
         * // cqlPair.cql: INSERT INTO account (firstName, lastName, emailAddress, isActive) VALUES (?, ?, ?, ?)
         * // cqlPair.parameters: ["John", "Doe", "john.doe@example.com", true]
         * 
         * // With null values (will be included)
         * props.put("middleName", null);
         * SP cqlPair2 = PLC.insert(props).into("account").pair();
         * // Includes middleName with NULL value
         * }</pre>
         * 
         * @param props map of property names to values
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if props is null or empty
         */
        public static CqlBuilder insert(final Map<String, Object> props) {
            N.checkArgNotEmpty(props, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._props = props;

            return instance;
        }

        /**
         * Creates an INSERT statement from an entity object.
         * 
         * <p>This method extracts all non-null properties from the entity object,
         * excluding those marked with @Transient, @ReadOnly, or @ReadOnlyId annotations.
         * Property names maintain their camelCase format. This is the most convenient way
         * to insert data when working with entity objects.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Account account = new Account();
         * account.setFirstName("John");
         * account.setLastName("Doe");
         * account.setEmailAddress("john.doe@example.com");
         * account.setCreatedDate(new Date());
         * 
         * SP cqlPair = PLC.insert(account).into("account").pair();
         * // cqlPair.cql: INSERT INTO account (firstName, lastName, emailAddress, createdDate) VALUES (?, ?, ?, ?)
         * // cqlPair.parameters: ["John", "Doe", "john.doe@example.com", Date object]
         * 
         * // Entity with @ReadOnly fields (will be excluded)
         * @Table("userProfile")
         * public class UserProfile {
         *     @ReadOnlyId
         *     private Long id;  // Excluded from INSERT
         *     private String userName;
         *     @ReadOnly
         *     private Date lastModified;  // Excluded from INSERT
         * }
         * }</pre>
         * 
         * @param entity the entity object to insert
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entity is null
         */
        public static CqlBuilder insert(final Object entity) {
            return insert(entity, null);
        }

        /**
         * Creates an INSERT statement from an entity object with excluded properties.
         * 
         * <p>This method allows fine-grained control over which properties to include in the INSERT.
         * Properties can be excluded explicitly in addition to those marked with annotations.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Account account = new Account();
         * account.setFirstName("John");
         * account.setLastName("Doe");
         * account.setEmailAddress("john@example.com");
         * account.setCreatedDate(new Date());
         * account.setInternalNotes("VIP customer");
         * 
         * Set<String> excluded = N.asSet("createdDate", "internalNotes");
         * SP cqlPair = PLC.insert(account, excluded).into("account").pair();
         * // cqlPair.cql: INSERT INTO account (firstName, lastName, emailAddress) VALUES (?, ?, ?)
         * // cqlPair.parameters: ["John", "Doe", "john@example.com"]
         * 
         * // Exclude all audit fields
         * Set<String> auditFields = N.asSet("createdBy", "createdDate", "modifiedBy", "modifiedDate");
         * SP cqlPair2 = PLC.insert(account, auditFields).into("account").pair();
         * }</pre>
         * 
         * @param entity the entity object to insert
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entity is null
         */
        public static CqlBuilder insert(final Object entity, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entity, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance.setEntityClass(entity.getClass());

            parseInsertEntity(instance, entity, excludedPropNames);

            return instance;
        }

        /**
         * Creates an INSERT statement for an entity class.
         * 
         * <p>This method includes all properties of the entity class that are suitable for insertion,
         * excluding those marked with @Transient, @ReadOnly, or @ReadOnlyId annotations. This is useful
         * when you want to generate the INSERT structure without having an actual entity instance.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PLC.insert(Account.class).into("account").build().query();
         * // Output: INSERT INTO account (firstName, lastName, email, createdDate) VALUES (?, ?, ?, ?)
         * 
         * // Can be used to prepare statements
         * String template = PLC.insert(UserProfile.class).into("userProfile").build().query();
         * PreparedStatement ps = connection.prepareStatement(template);
         * // Then set values: ps.setString(1, "John"); etc.
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insert(final Class<?> entityClass) {
            return insert(entityClass, null);
        }

        /**
         * Creates an INSERT statement for an entity class with excluded properties.
         * 
         * <p>This method provides control over which properties to include when generating
         * the INSERT statement from a class definition.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Exclude auto-generated fields
         * Set<String> excluded = N.asSet("id", "createdDate", "modifiedDate");
         * String cql = PLC.insert(Account.class, excluded).into("account").build().query();
         * // Output: INSERT INTO account (firstName, lastName, email) VALUES (?, ?, ?)
         * 
         * // Exclude computed fields
         * Set<String> computed = N.asSet("fullName", "age", "accountBalance");
         * String cql2 = PLC.insert(Customer.class, computed).into("customer").build().query();
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        @SuppressWarnings("deprecation")
        public static CqlBuilder insert(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = QueryUtil.getInsertPropNames(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates an INSERT INTO statement for an entity class.
         * 
         * <p>This is a convenience method that combines insert() and into() operations.
         * The table name is derived from the entity class name or @Table annotation.
         * This provides the most concise way to generate INSERT statements for entities.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Using class name as table name
         * String cql = PLC.insertInto(Account.class).build().query();
         * // Output: INSERT INTO account (firstName, lastName, email) VALUES (?, ?, ?)
         * 
         * // Using @Table annotation
         * @Table("user_accounts")
         * public class Account { ... }
         * 
         * String cql2 = PLC.insertInto(Account.class).build().query();
         * // Output: INSERT INTO user_accounts (firstName, lastName, email) VALUES (?, ?, ?)
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insertInto(final Class<?> entityClass) {
            return insertInto(entityClass, null);
        }

        /**
         * Creates an INSERT INTO statement for an entity class with excluded properties.
         * 
         * <p>This convenience method combines insert() and into() operations while allowing
         * property exclusions. The table name is automatically determined from the entity class.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("id", "createdDate", "version");
         * String cql = PLC.insertInto(Account.class, excluded).build().query();
         * // Output: INSERT INTO account (firstName, lastName, email) VALUES (?, ?, ?)
         * 
         * // For batch operations
         * String template = PLC.insertInto(Order.class, N.asSet("id", "orderNumber")).build().query();
         * // Use template for bulk inserts
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insertInto(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return insert(entityClass, excludedPropNames).into(entityClass);
        }

        /**
         * Generates a MyCQL-style batch INSERT statement.
         * 
         * <p>This method creates an efficient batch insert statement with multiple value sets
         * in a single INSERT statement. This is significantly more efficient than executing
         * multiple individual INSERT statements. Property names maintain their camelCase format.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<Account> accounts = Arrays.asList(
         *     new Account("John", "Doe", "john@example.com"),
         *     new Account("Jane", "Smith", "jane@example.com"),
         *     new Account("Bob", "Johnson", "bob@example.com")
         * );
         * 
         * SP cqlPair = PLC.batchInsert(accounts).into("account").pair();
         * // cqlPair.cql: INSERT INTO account (firstName, lastName, emailAddress) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)
         * // cqlPair.parameters: ["John", "Doe", "john@example.com", "Jane", "Smith", "jane@example.com", "Bob", "Johnson", "bob@example.com"]
         * 
         * // With maps
         * List<Map<String, Object>> data = Arrays.asList(
         *     N.asMap("firstName", "John", "lastName", "Doe"),
         *     N.asMap("firstName", "Jane", "lastName", "Smith")
         * );
         * SP cqlPair2 = PLC.batchInsert(data).into("account").pair();
         * }</pre>
         *
         * @param propsList collection of entities or property maps to insert
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if propsList is null or empty
         */
        @Beta
        public static CqlBuilder batchInsert(final Collection<?> propsList) {
            N.checkArgNotEmpty(propsList, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            final Optional<?> first = N.firstNonNull(propsList);

            if (first.isPresent() && Beans.isBeanClass(first.get().getClass())) {
                instance.setEntityClass(first.get().getClass());
            }

            instance._propsList = toInsertPropsList(propsList);

            return instance;
        }

        /**
         * Creates an UPDATE statement for a table.
         * 
         * <p>This method starts building an UPDATE statement. Column names maintain camelCase format.
         * The actual columns to update are specified using the set() method.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PLC.update("account")
         *                 .set("firstName", "John")
         *                 .set("lastName", "Smith")
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: UPDATE account SET firstName = ?, lastName = ? WHERE id = ?
         * 
         * // Update with expression
         * String cql2 = PLC.update("account")
         *                  .set("loginCount", "loginCount + 1")
         *                  .set("lastLoginDate", new Date())
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * }</pre>
         * 
         * @param tableName the name of the table to update
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder update(final String tableName) {
            N.checkArgNotEmpty(tableName, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;

            return instance;
        }

        /**
         * Creates an UPDATE statement for a table with entity class mapping.
         * 
         * <p>The entity class provides property-to-column name mapping information,
         * which is useful when using the set() method with entity objects.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Account account = new Account();
         * account.setFirstName("John");
         * account.setLastName("Smith");
         * account.setModifiedDate(new Date());
         * 
         * SP cqlPair = PLC.update("account", Account.class)
         *                 .set(account)
         *                 .where(Filters.eq("id", account.getId()))
         *                 .pair();
         * // cqlPair.cql: UPDATE account SET firstName = ?, lastName = ?, modifiedDate = ? WHERE id = ?
         * }</pre>
         * 
         * @param tableName the name of the table to update
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if tableName is null or empty, or entityClass is null
         */
        public static CqlBuilder update(final String tableName, final Class<?> entityClass) {
            N.checkArgNotEmpty(tableName, UPDATE_PART_MSG);
            N.checkArgNotNull(entityClass, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;
            instance.setEntityClass(entityClass);

            return instance;
        }

        /**
         * Creates an UPDATE statement for an entity class.
         * 
         * <p>The table name is derived from the entity class name or @Table annotation.
         * All updatable properties are included by default when using set() with an entity object.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Simple update
         * String cql = PLC.update(Account.class)
         *                 .set("status", "active")
         *                 .set("activatedDate", new Date())
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: UPDATE account SET status = ?, activatedDate = ? WHERE id = ?
         * 
         * // Update with entity
         * Account account = getAccount();
         * account.setStatus("inactive");
         * SP cqlPair = PLC.update(Account.class)
         *                 .set(account)
         *                 .where(Filters.eq("id", account.getId()))
         *                 .pair();
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder update(final Class<?> entityClass) {
            return update(entityClass, null);
        }

        /**
         * Creates an UPDATE statement for an entity class with excluded properties.
         * 
         * <p>Properties marked with @NonUpdatable or @ReadOnly are automatically excluded.
         * Additional properties can be excluded through the excludedPropNames parameter.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Exclude audit fields from update
         * Set<String> excluded = N.asSet("createdDate", "createdBy");
         * 
         * Account account = getAccount();
         * account.setFirstName("John");
         * account.setLastName("Doe");
         * account.setCreatedDate(new Date());   // This will be ignored
         * 
         * SP cqlPair = PLC.update(Account.class, excluded)
         *                 .set(account)
         *                 .where(Filters.eq("id", account.getId()))
         *                 .pair();
         * // createdDate excluded even though set in entity
         * 
         * // Exclude version control fields
         * Set<String> versionExcluded = N.asSet("version", "previousVersion");
         * String cql = PLC.update(Document.class, versionExcluded)
         *                 .set("content", newContent)
         *                 .where(Filters.eq("id", docId))
         *                 .build().query();
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the update
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        @SuppressWarnings("deprecation")
        public static CqlBuilder update(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance.setEntityClass(entityClass);
            instance._tableName = getTableName(entityClass, instance._namingPolicy);
            instance._propOrColumnNames = QueryUtil.getUpdatePropNames(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for a single column or expression.
         *
         * <p>This method initializes a DELETE statement for one column. The column name will be
         * converted according to the NO_CHANGE naming policy. This is useful for deleting
         * specific columns from a row rather than the entire row.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PLC.delete("firstName")
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE firstName FROM account WHERE id = ?
         * }</pre>
         *
         * @param expr the column name or expression to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if expr is null or empty
         */
        public static CqlBuilder delete(final String expr) {
            N.checkArgNotEmpty(expr, DELETION_PART_MSG);

            return delete(N.asArray(expr));
        }

        /**
         * Creates a DELETE CQL builder for multiple columns.
         *
         * <p>This method initializes a DELETE statement for multiple columns. All column names
         * will be converted according to the NO_CHANGE naming policy. This allows selective
         * deletion of specific columns from rows.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PLC.delete("firstName", "lastName", "email")
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE firstName, lastName, email FROM account WHERE id = ?
         * }</pre>
         *
         * @param columnNames the column names to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if columnNames is null or empty
         */
        public static CqlBuilder delete(final String... columnNames) {
            N.checkArgNotEmpty(columnNames, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._propOrColumnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for a collection of columns.
         *
         * <p>This method is useful when column names are determined dynamically. The collection
         * can contain property names that will be converted to column names according to the
         * NO_CHANGE naming policy.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("firstName", "lastName", "email");
         * String cql = PLC.delete(columns)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE firstName, lastName, email FROM account WHERE id = ?
         * }</pre>
         *
         * @param columnNames the collection of column names to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if columnNames is null or empty
         */
        public static CqlBuilder delete(final Collection<String> columnNames) {
            N.checkArgNotEmpty(columnNames, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._propOrColumnNames = columnNames;

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for an entity class.
         *
         * <p>This method generates a DELETE statement for all deletable properties of the class.
         * Properties marked with @ReadOnly, @ReadOnlyId, or @Transient are automatically excluded.
         * This is useful for creating templates for partial row deletion.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PLC.delete(Account.class)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE firstName, lastName, email, status FROM account WHERE id = ?
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder delete(final Class<?> entityClass) {
            return delete(entityClass, null);
        }

        /**
         * Creates a DELETE CQL builder for an entity class, excluding specified properties.
         *
         * <p>This method generates a DELETE statement excluding both annotation-based exclusions
         * and the specified properties. Useful for selective column deletion where certain
         * fields should be preserved.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("email", "createdDate");
         * String cql = PLC.delete(Account.class, excluded)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE firstName, lastName, status FROM account WHERE id = ?
         * }</pre>
         *
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder delete(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates a DELETE FROM statement for a table.
         * 
         * <p>This method creates a DELETE statement. Always use with a WHERE clause
         * to avoid deleting all rows in the table.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Delete specific records
         * String cql = PLC.deleteFrom("account")
         *                 .where(Filters.eq("status", "inactive"))
         *                 .build().query();
         * // Output: DELETE FROM account WHERE status = ?
         * 
         * // Delete with multiple conditions
         * String cql2 = PLC.deleteFrom("account")
         *                  .where(Filters.and(
         *                      Filters.eq("status", "inactive"),
         *                      Filters.lt("lastLoginDate", thirtyDaysAgo)
         *                  ))
         *                  .build().query();
         * 
         * // Delete with limit (database-specific)
         * String cql3 = PLC.deleteFrom("logs")
         *                  .where(Filters.lt("createdDate", oneYearAgo))
         *                  .limit(1000)
         *                  .build().query();
         * }</pre>
         * 
         * @param tableName the name of the table to delete from
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder deleteFrom(final String tableName) {
            N.checkArgNotEmpty(tableName, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;

            return instance;
        }

        /**
         * Creates a DELETE FROM statement for a table with entity class mapping.
         * 
         * <p>The entity class provides property-to-column name mapping for use in WHERE conditions.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Delete with entity property names
         * String cql = PLC.deleteFrom("account", Account.class)
         *                 .where(Filters.and(
         *                     Filters.eq("accountType", "trial"),
         *                     Filters.lt("createdDate", expirationDate)
         *                 ))
         *                 .build().query();
         * // Property names are used even though table name is specified
         * 
         * // Using with entity instance
         * Account account = getAccount();
         * String cql2 = PLC.deleteFrom("account", Account.class)
         *                  .where(Filters.eq("id", account.getId()))
         *                  .build().query();
         * }</pre>
         * 
         * @param tableName the name of the table to delete from
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if tableName is null or empty, or entityClass is null
         */
        public static CqlBuilder deleteFrom(final String tableName, final Class<?> entityClass) {
            N.checkArgNotEmpty(tableName, DELETION_PART_MSG);
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;
            instance.setEntityClass(entityClass);

            return instance;
        }

        /**
         * Creates a DELETE FROM statement for an entity class.
         * 
         * <p>The table name is derived from the entity class name or @Table annotation.
         * This provides a type-safe way to generate DELETE statements.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Delete by ID
         * String cql = PLC.deleteFrom(Account.class)
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: DELETE FROM account WHERE id = ?
         * 
         * // Bulk delete with conditions
         * String cql2 = PLC.deleteFrom(Order.class)
         *                  .where(Filters.and(
         *                      Filters.eq("status", "cancelled"),
         *                      Filters.lt("orderDate", oneYearAgo)
         *                  ))
         *                  .build().query();
         * 
         * // With @Table annotation
         * @Table("user_sessions")
         * public class Session { ... }
         * 
         * String cql3 = PLC.deleteFrom(Session.class)
         *                  .where(Filters.lt("expiryTime", now))
         *                  .build().query();
         * // Output: DELETE FROM user_sessions WHERE expiryTime < ?
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder deleteFrom(final Class<?> entityClass) {
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance.setEntityClass(entityClass);
            instance._tableName = getTableName(entityClass, instance._namingPolicy);

            return instance;
        }

        /**
         * Creates a SELECT statement with a single expression.
         * 
         * <p>This method is useful for complex select expressions, aggregate functions,
         * or when selecting computed values.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Count query
         * String cql = PLC.select("COUNT(*)")
         *                 .from("account")
         *                 .where(Filters.eq("status", "active"))
         *                 .build().query();
         * // Output: SELECT count(*) FROM account WHERE status = ?
         * 
         * // Complex expression
         * String cql2 = PLC.select("firstName || ' ' || lastName AS fullName")
         *                  .from("account")
         *                  .build().query();
         * // Output: SELECT firstName || ' ' || lastName AS fullName FROM account
         * 
         * // Aggregate with grouping
         * String cql3 = PLC.select("departmentId, COUNT(*) AS employeeCount")
         *                  .from("employee")
         *                  .groupBy("departmentId")
         *                  .having(Filters.gt("COUNT(*)", 10))
         *                  .build().query();
         * }</pre>
         * 
         * @param selectPart the select expression
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if selectPart is null or empty
         */
        public static CqlBuilder select(final String selectPart) {
            N.checkArgNotEmpty(selectPart, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = Array.asList(selectPart);
            return instance;
        }

        /**
         * Creates a SELECT statement with multiple columns.
         * 
         * <p>Column names maintain camelCase format. This is the most common way to
         * create SELECT statements when you know the specific columns needed.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Simple select
         * String cql = PLC.select("id", "firstName", "lastName", "email")
         *                 .from("account")
         *                 .where(Filters.eq("status", "active"))
         *                 .build().query();
         * // Output: SELECT id, firstName, lastName, email FROM account WHERE status = ?
         * 
         * // With table aliases
         * String cql2 = PLC.select("a.id", "a.firstName", "o.orderId", "o.totalAmount")
         *                  .from("account a")
         *                  .innerJoin("orders o").on("a.id = o.accountId")
         *                  .build().query();
         * 
         * // Mixed columns and expressions
         * String cql3 = PLC.select("id", "firstName", "lastName", 
         *                          "YEAR(CURRENT_DATE) - YEAR(birthDate) AS age")
         *                  .from("account")
         *                  .build().query();
         * }</pre>
         * 
         * @param propOrColumnNames the property or column names to select
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder select(final String... propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = Array.asList(propOrColumnNames);

            return instance;
        }

        /**
         * Creates a SELECT statement with a collection of columns.
         * 
         * <p>This method is useful when the column list is dynamically generated or
         * comes from another source.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Dynamic column selection
         * List<String> columns = getUserSelectedColumns();
         * String cql = PLC.select(columns)
         *                 .from("account")
         *                 .build().query();
         * 
         * // Programmatically built column list
         * List<String> cols = new ArrayList<>();
         * cols.add("id");
         * cols.add("firstName");
         * if (includeEmail) {
         *     cols.add("emailAddress");
         * }
         * String cql2 = PLC.select(cols).from("account").build().query();
         * 
         * // From entity metadata
         * List<String> entityColumns = getEntityColumns(Account.class);
         * String cql3 = PLC.select(entityColumns).from("account").build().query();
         * }</pre>
         * 
         * @param propOrColumnNames collection of property or column names to select
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder select(final Collection<String> propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = propOrColumnNames;

            return instance;
        }

        /**
         * Creates a SELECT statement with column aliases.
         * 
         * <p>This method allows you to specify custom aliases for each selected column,
         * which is useful for renaming columns in the result set.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, String> columnAliases = new HashMap<>();
         * columnAliases.put("firstName", "fname");
         * columnAliases.put("lastName", "lname");
         * columnAliases.put("emailAddress", "email");
         * 
         * String cql = PLC.select(columnAliases)
         *                 .from("account")
         *                 .build().query();
         * // Output: SELECT firstName AS fname, lastName AS lname, emailAddress AS email FROM account
         * 
         * // For JSON output formatting
         * Map<String, String> jsonAliases = new HashMap<>();
         * jsonAliases.put("id", "user_id");
         * jsonAliases.put("firstName", "first_name");
         * jsonAliases.put("lastName", "last_name");
         * String cql2 = PLC.select(jsonAliases).from("account").build().query();
         * 
         * // Complex aliases with expressions
         * Map<String, String> aliases = new HashMap<>();
         * aliases.put("firstName || ' ' || lastName", "full_name");
         * aliases.put("YEAR(CURRENT_DATE) - YEAR(birthDate)", "age");
         * String cql3 = PLC.select(aliases).from("account").build().query();
         * }</pre>
         * 
         * @param propOrColumnNameAliases Map of property/column names to their aliases
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if propOrColumnNameAliases is null or empty
         */
        public static CqlBuilder select(final Map<String, String> propOrColumnNameAliases) {
            N.checkArgNotEmpty(propOrColumnNameAliases, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNameAliases = propOrColumnNameAliases;

            return instance;
        }

        /**
         * Creates a SELECT statement for all properties of an entity class.
         * 
         * <p>This method selects all properties from an entity class, excluding those
         * marked with @Transient annotation. Properties maintain their camelCase naming.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Select all fields from Account entity
         * String cql = PLC.select(Account.class)
         *                 .from("account")
         *                 .build().query();
         * // Output: SELECT id, firstName, lastName, email, createdDate FROM account
         * 
         * // With WHERE clause
         * String cql2 = PLC.select(Account.class)
         *                  .from("account")
         *                  .where(Filters.eq("status", "active"))
         *                  .build().query();
         * 
         * // Entity with @Transient fields
         * public class User {
         *     private Long id;
         *     private String userName;
         *     @Transient
         *     private String tempPassword;  // Excluded from SELECT
         * }
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass) {
            return select(entityClass, false);
        }

        /**
         * Creates a SELECT statement for an entity class with optional sub-entity properties.
         * 
         * <p>When includeSubEntityProperties is true, properties of nested entity objects
         * are also included in the selection, which is useful for fetching related data.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Entity with nested object
         * public class Order {
         *     private Long id;
         *     private String orderNumber;
         *     private Customer customer;  // Nested entity
         * }
         * 
         * // Without sub-entities
         * String cql = PLC.select(Order.class, false)
         *                 .from("orders")
         *                 .build().query();
         * // Output: SELECT id, orderNumber FROM orders
         * 
         * // With sub-entities
         * String cql2 = PLC.select(Order.class, true)
         *                  .from("orders o")
         *                  .innerJoin("customers c").on("o.customerId = c.id")
         *                  .build().query();
         * // Includes customer properties as well
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties Whether to include properties of nested entity objects
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass, final boolean includeSubEntityProperties) {
            return select(entityClass, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT statement for an entity class with excluded properties.
         * 
         * <p>This method allows you to select most properties of an entity while
         * explicitly excluding certain ones.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Exclude sensitive fields
         * Set<String> excluded = N.asSet("password", "securityAnswer", "ssn");
         * String cql = PLC.select(Account.class, excluded)
         *                 .from("account")
         *                 .build().query();
         * // All fields except password, securityAnswer, and ssn
         * 
         * // Exclude large fields for list views
         * Set<String> listExcluded = N.asSet("biography", "profileImage", "attachments");
         * String cql2 = PLC.select(Author.class, listExcluded)
         *                  .from("authors")
         *                  .build().query();
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return select(entityClass, false, excludedPropNames);
        }

        /**
         * Creates a SELECT statement for an entity class with all options.
         * 
         * <p>This method provides full control over property selection, including
         * sub-entity properties and exclusions.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Complex entity with relationships
         * public class Invoice {
         *     private Long id;
         *     private String invoiceNumber;
         *     private Customer customer;
         *     private List<InvoiceItem> items;
         *     private byte[] pdfData;  // Large field
         * }
         * 
         * // Select with sub-entities but exclude large fields
         * Set<String> excluded = N.asSet("pdfData", "items");
         * String cql = PLC.select(Invoice.class, true, excluded)
         *                 .from("invoices i")
         *                 .innerJoin("customers c").on("i.customerId = c.id")
         *                 .build().query();
         * // Includes invoice and customer fields, but not pdfData or items
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties Whether to include properties of nested entity objects
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        @SuppressWarnings("deprecation")
        public static CqlBuilder select(final Class<?> entityClass, final boolean includeSubEntityProperties, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = QueryUtil.getSelectPropNames(entityClass, includeSubEntityProperties, excludedPropNames);

            return instance;
        }

        /**
         * Creates a complete SELECT FROM statement for an entity class.
         * 
         * <p>This is a convenience method that combines select() and from() operations.
         * The table name is derived from the entity class name or @Table annotation.
         * This is the most concise way to create entity-based queries.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Simple entity query
         * String cql = PLC.selectFrom(Account.class)
         *                 .where(Filters.eq("status", "active"))
         *                 .build().query();
         * // Output: SELECT id, firstName, lastName, email FROM account WHERE status = ?
         * 
         * // With @Table annotation
         * @Table("user_accounts")
         * public class Account { ... }
         * 
         * String cql2 = PLC.selectFrom(Account.class)
         *                  .orderBy("createdDate DESC")
         *                  .limit(10)
         *                  .build().query();
         * // Output: SELECT ... FROM user_accounts ORDER BY createdDate DESC LIMIT 10
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, false);
        }

        /**
         * Creates a SELECT FROM statement for an entity class with table alias.
         * 
         * <p>Table aliases are essential for joins and disambiguating column names
         * when multiple tables are involved in the query.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Simple alias usage
         * String cql = PLC.selectFrom(Account.class, "a")
         *                 .where(Filters.eq("a.status", "active"))
         *                 .build().query();
         * // Output: SELECT a.id, a.firstName, a.lastName, a.email FROM account a WHERE a.status = ?
         * 
         * // With joins
         * String cql2 = PLC.selectFrom(Order.class, "o")
         *                  .innerJoin("customers c").on("o.customerId = c.id")
         *                  .where(Filters.eq("c.country", "USA"))
         *                  .build().query();
         * 
         * // Self-join
         * String cql3 = PLC.selectFrom(Employee.class, "e1")
         *                  .leftJoin("employee e2").on("e1.managerId = e2.id")
         *                  .build().query();
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias) {
            return selectFrom(entityClass, alias, false);
        }

        /**
         * Creates a SELECT FROM statement with optional sub-entity properties.
         * 
         * <p>This method combines entity selection with the option to include
         * properties from related entities.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Entity with relationships
         * public class BlogPost {
         *     private Long id;
         *     private String title;
         *     private Author author;
         *     private List<Comment> comments;
         * }
         * 
         * // Without sub-entities (flat selection)
         * String cql = PLC.selectFrom(BlogPost.class, false)
         *                 .build().query();
         * // Output: SELECT id, title FROM blog_post
         * 
         * // With sub-entities (includes author fields)
         * String cql2 = PLC.selectFrom(BlogPost.class, true)
         *                  .build().query();
         * // Includes author properties in selection
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties Whether to include properties of nested entity objects
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final boolean includeSubEntityProperties) {
            return selectFrom(entityClass, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT FROM statement with alias and sub-entity option.
         * 
         * <p>This method provides alias support along with sub-entity property inclusion.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Query with related entities
         * String cql = PLC.selectFrom(Order.class, "o", true)
         *                 .innerJoin("customers c").on("o.customerId = c.id")
         *                 .innerJoin("addresses a").on("c.addressId = a.id")
         *                 .where(Filters.eq("a.country", "USA"))
         *                 .build().query();
         * // Includes order and related entity properties with proper aliases
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @param includeSubEntityProperties Whether to include properties of nested entity objects
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final boolean includeSubEntityProperties) {
            return selectFrom(entityClass, alias, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT FROM statement with excluded properties.
         * 
         * <p>This method creates a complete query while excluding specific properties
         * from the selection.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Exclude large or sensitive fields
         * Set<String> excluded = N.asSet("password", "biography", "photo");
         * String cql = PLC.selectFrom(UserProfile.class, excluded)
         *                 .where(Filters.eq("active", true))
         *                 .build().query();
         * 
         * // Exclude computed fields
         * Set<String> computed = N.asSet("age", "fullName", "totalSpent");
         * String cql2 = PLC.selectFrom(Customer.class, computed)
         *                  .build().query();
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, false, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM statement with alias and excluded properties.
         * 
         * <p>This method combines alias support with property exclusion for
         * precise control over the generated query.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Complex query with exclusions
         * Set<String> excluded = N.asSet("internalNotes", "debugInfo");
         * String cql = PLC.selectFrom(Order.class, "o", excluded)
         *                 .innerJoin("customers c").on("o.customerId = c.id")
         *                 .where(Filters.between("o.orderDate", startDate, endDate))
         *                 .build().query();
         * 
         * // Multiple table query
         * Set<String> sensitiveFields = N.asSet("ssn", "creditCard");
         * String cql2 = PLC.selectFrom(Customer.class, "c", sensitiveFields)
         *                  .leftJoin("orders o").on("c.id = o.customerId")
         *                  .groupBy("c.id")
         *                  .build().query();
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, alias, false, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM statement with sub-entities and exclusions.
         * 
         * <p>This method allows including sub-entity properties while excluding
         * specific properties from the main or sub-entities.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Include related entities but exclude sensitive fields
         * Set<String> excluded = N.asSet("password", "customer.creditCard");
         * String cql = PLC.selectFrom(Order.class, true, excluded)
         *                 .build().query();
         * // Includes order and customer fields except the excluded ones
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties Whether to include properties of nested entity objects
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final boolean includeSubEntityProperties, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, QueryUtil.getTableAlias(entityClass), includeSubEntityProperties, excludedPropNames);
        }

        /**
         * Creates a complete SELECT FROM statement with all options.
         * 
         * <p>This method provides maximum flexibility for creating SELECT statements
         * with entity mapping, allowing control over aliases, sub-entity inclusion,
         * and property exclusions.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Comprehensive query example
         * Set<String> excluded = N.asSet("password", "internalNotes", "customer.creditScore");
         * String cql = PLC.selectFrom(Order.class, "o", true, excluded)
         *                 .innerJoin("customers c").on("o.customerId = c.id")
         *                 .leftJoin("shipping_addresses sa").on("o.shippingAddressId = sa.id")
         *                 .where(Filters.and(
         *                     Filters.eq("o.status", "pending"),
         *                     Filters.gt("o.totalAmount", 100)
         *                 ))
         *                 .orderBy("o.createdDate DESC")
         *                 .build().query();
         * 
         * // Report query with specific field selection
         * Set<String> reportExcluded = N.asSet("id", "createdBy", "modifiedBy", "version");
         * String cql2 = PLC.selectFrom(SalesReport.class, "sr", false, reportExcluded)
         *                  .where(Filters.between("sr.reportDate", startDate, endDate))
         *                  .build().query();
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @param includeSubEntityProperties Whether to include properties of nested entity objects
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final boolean includeSubEntityProperties,
                final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            if (hasSubEntityToInclude(entityClass, includeSubEntityProperties)) {
                final List<String> selectTableNames = getSelectTableNames(entityClass, alias, excludedPropNames, NamingPolicy.CAMEL_CASE);
                return select(entityClass, includeSubEntityProperties, excludedPropNames).from(entityClass, selectTableNames);
            }

            return select(entityClass, includeSubEntityProperties, excludedPropNames).from(entityClass, alias);
        }

        /**
         * Creates a COUNT(*) query for a table.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PLC.count("account")
         *                 .where(Filters.eq("status", "active"))
         *                 .build().query();
         * // Output: SELECT count(*) FROM account WHERE status = ?
         * }</pre>
         * 
         * @param tableName the name of the table to count rows from
         * @return a new CqlBuilder instance for method chaining
         */
        public static CqlBuilder count(final String tableName) {
            N.checkArgNotEmpty(tableName, SELECTION_PART_MSG);

            return select(COUNT_ALL_LIST).from(tableName);
        }

        /**
         * Creates a COUNT(*) query for an entity class.
         * 
         * <p>The table name is derived from the entity class name or @Table annotation.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PLC.count(Account.class)
         *                 .where(Filters.isNotNull("email"))
         *                 .build().query();
         * // Output: SELECT count(*) FROM account WHERE email IS NOT NULL
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance for method chaining
         */
        public static CqlBuilder count(final Class<?> entityClass) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            return select(COUNT_ALL_LIST).from(entityClass);
        }

        /**
         * Parses a condition into CQL with entity class mapping.
         * 
         * <p>This method is useful for generating just the WHERE clause portion of a query
         * with proper property-to-column name mapping.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Condition cond = Filters.and(
         *     Filters.eq("firstName", "John"),
         *     Filters.like("emailAddress", "%@example.com")
         * );
         * 
         * String cql = PLC.parse(cond, Account.class).build().query();
         * // Output: firstName = ? AND emailAddress LIKE ?
         * }</pre>
         * 
         * @param cond the condition to parse
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance containing the parsed condition
         * @throws IllegalArgumentException if cond is null
         */
        public static CqlBuilder parse(final Condition cond, final Class<?> entityClass) {
            N.checkArgNotNull(cond, "cond");

            final CqlBuilder instance = createInstance();

            instance.setEntityClass(entityClass);
            instance._op = OperationType.QUERY;
            instance._isForConditionOnly = true;
            instance.append(cond);

            return instance;
        }
    }

    /**
     * Named CQL builder with {@code NamingPolicy.NO_CHANGE} field/column naming strategy.
     * 
     * <p>This builder generates CQL with named parameters (e.g., :paramName) and preserves the original
     * naming convention of fields and columns without any transformation. It's particularly useful when
     * working with databases where column names match exactly with your Java field names.</p>
     *
     * <p>For example:</p>
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * N.println(NSB.select("first_Name", "last_NaMe").from("account").where(Filters.eq("last_NaMe", 1)).build().query());
     * // SELECT first_Name, last_NaMe FROM account WHERE last_NaMe = :last_NaMe
     * }</pre>
     */
    public static class NSB extends CqlBuilder {

        /**
         * Constructs a new NSB instance with NO_CHANGE naming policy and named CQL policy.
         * This constructor is package-private and used internally by the builder pattern.
         */
        NSB() {
            super(NamingPolicy.NO_CHANGE, SQLPolicy.NAMED_SQL);
        }

        /**
         * Indicates whether this builder generates named CQL parameters.
         * 
         * <p>This implementation always returns {@code true} as NSB generates CQL with named parameters
         * (e.g., :paramName) instead of positional parameters (?).</p>
         * 
         * @return true, indicating this builder uses named CQL parameters
         */
        @Override
        protected boolean isNamedSql() {
            return true;
        }

        /**
         * Creates a new instance of NSB for internal use by the builder pattern.
         * 
         * <p>This factory method is used internally to create new builder instances
         * when starting a new CQL construction chain.</p>
         * 
         * @return a new NSB instance
         */
        protected static NSB createInstance() {
            return new NSB();
        }

        /**
         * Creates an INSERT CQL builder with a single column expression.
         * 
         * <p>This method is a convenience wrapper that internally calls {@link #insert(String...)} 
         * with a single-element array.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSB.insert("user_name").into("users").build().query();
         * // INSERT INTO users (user_name) VALUES (:user_name)
         * }</pre>
         *
         * @param expr the column name or expression to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if expr is null or empty
         */
        public static CqlBuilder insert(final String expr) {
            N.checkArgNotEmpty(expr, INSERTION_PART_MSG);

            return insert(N.asArray(expr));
        }

        /**
         * Creates an INSERT CQL builder with specified column names.
         * 
         * <p>The generated CQL will include placeholders for the specified columns using named parameters.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSB.insert("first_name", "last_name", "email")
         *                 .into("users")
         *                 .build().query();
         * // INSERT INTO users (first_name, last_name, email) VALUES (:first_name, :last_name, :email)
         * }</pre>
         *
         * @param propOrColumnNames the property or column names to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder insert(final String... propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._propOrColumnNames = Array.asList(propOrColumnNames);

            return instance;
        }

        /**
         * Creates an INSERT CQL builder with a collection of column names.
         * 
         * <p>This method allows using any Collection implementation (List, Set, etc.) to specify
         * the columns for the INSERT statement.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("id", "name", "created_date");
         * String cql = NSB.insert(columns).into("products").build().query();
         * // INSERT INTO products (id, name, created_date) VALUES (:id, :name, :created_date)
         * }</pre>
         *
         * @param propOrColumnNames collection of property or column names to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder insert(final Collection<String> propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._propOrColumnNames = propOrColumnNames;

            return instance;
        }

        /**
         * Creates an INSERT CQL builder from a map of column names to values.
         * 
         * <p>The map keys represent column names and the values are the corresponding values to insert.
         * This method is useful when you have dynamic column-value pairs.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Object> data = new HashMap<>();
         * data.put("username", "john_doe");
         * data.put("age", 25);
         * String cql = NSB.insert(data).into("users").build().query();
         * // INSERT INTO users (username, age) VALUES (:username, :age)
         * }</pre>
         *
         * @param props map of property names to values
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if props is null or empty
         */
        public static CqlBuilder insert(final Map<String, Object> props) {
            N.checkArgNotEmpty(props, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._props = props;

            return instance;
        }

        /**
         * Creates an INSERT CQL builder from an entity object.
         * 
         * <p>This method extracts all non-null properties from the entity object to create the INSERT statement.
         * Properties annotated with {@code @Transient}, {@code @ReadOnly}, or {@code @ReadOnlyId} are automatically excluded.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User user = new User();
         * user.setName("John");
         * user.setEmail("john@example.com");
         * String cql = NSB.insert(user).into("users").build().query();
         * // INSERT INTO users (name, email) VALUES (:name, :email)
         * }</pre>
         *
         * @param entity the entity object to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entity is null
         */
        public static CqlBuilder insert(final Object entity) {
            return insert(entity, null);
        }

        /**
         * Creates an INSERT CQL builder from an entity object with excluded properties.
         * 
         * <p>This method allows fine-grained control over which properties to exclude from the INSERT statement,
         * in addition to the automatically excluded annotated properties.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User user = new User();
         * user.setName("John");
         * user.setEmail("john@example.com");
         * user.setPassword("secret");
         * Set<String> exclude = N.asSet("password");
         * String cql = NSB.insert(user, exclude).into("users").build().query();
         * // INSERT INTO users (name, email) VALUES (:name, :email)
         * }</pre>
         *
         * @param entity the entity object to insert
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entity is null
         */
        public static CqlBuilder insert(final Object entity, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entity, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance.setEntityClass(entity.getClass());

            parseInsertEntity(instance, entity, excludedPropNames);

            return instance;
        }

        /**
         * Creates an INSERT CQL builder for a specific entity class.
         * 
         * <p>This method generates an INSERT template for all insertable properties of the entity class.
         * Properties are determined by the class structure and annotations.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSB.insert(User.class).into("users").build().query();
         * // INSERT INTO users (id, name, email, created_date) VALUES (:id, :name, :email, :created_date)
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insert(final Class<?> entityClass) {
            return insert(entityClass, null);
        }

        /**
         * Creates an INSERT CQL builder for a specific entity class with excluded properties.
         * 
         * <p>This method provides control over which properties to include in the INSERT statement
         * when generating CQL from a class definition.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> exclude = N.asSet("id", "createdDate");
         * String cql = NSB.insert(User.class, exclude).into("users").build().query();
         * // INSERT INTO users (name, email) VALUES (:name, :email)
         * }</pre>
         *
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        @SuppressWarnings("deprecation")
        public static CqlBuilder insert(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = QueryUtil.getInsertPropNames(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates an INSERT CQL builder with automatic table name detection.
         * 
         * <p>The table name is automatically determined from the entity class using the {@code @Table} annotation
         * or by converting the class name according to the naming policy.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * @Table("user_accounts")
         * class User { ... }
         * 
         * String cql = NSB.insertInto(User.class).build().query();
         * // INSERT INTO user_accounts (id, name, email) VALUES (:id, :name, :email)
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for INSERT operation with table name set
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insertInto(final Class<?> entityClass) {
            return insertInto(entityClass, null);
        }

        /**
         * Creates an INSERT CQL builder with automatic table name detection and excluded properties.
         * 
         * <p>Combines automatic table name detection with the ability to exclude specific properties
         * from the INSERT statement.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> exclude = N.asSet("version", "lastModified");
         * String cql = NSB.insertInto(User.class, exclude).build().query();
         * // INSERT INTO users (id, name, email) VALUES (:id, :name, :email)
         * }</pre>
         *
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude
         * @return a new CqlBuilder instance configured for INSERT operation with table name set
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insertInto(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return insert(entityClass, excludedPropNames).into(entityClass);
        }

        /**
         * Creates a batch INSERT CQL builder for multiple records (MyCQL style).
         * 
         * <p>This method generates a single INSERT statement with multiple value sets, which is more efficient
         * than multiple individual INSERT statements. The input can be a collection of entity objects or maps.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<User> users = Arrays.asList(
         *     new User("John", "john@email.com"),
         *     new User("Jane", "jane@email.com")
         * );
         * String cql = NSB.batchInsert(users).into("users").build().query();
         * // INSERT INTO users (name, email) VALUES (:name_0, :email_0), (:name_1, :email_1)
         * }</pre>
         *
         * @param propsList collection of entities or property maps to insert
         * @return a new CqlBuilder instance configured for batch INSERT operation
         * @throws IllegalArgumentException if propsList is null or empty
         * <p><b>Note:</b> This is a beta feature and may be subject to changes</p>
         */
        @Beta
        public static CqlBuilder batchInsert(final Collection<?> propsList) {
            N.checkArgNotEmpty(propsList, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            final Optional<?> first = N.firstNonNull(propsList);

            if (first.isPresent() && Beans.isBeanClass(first.get().getClass())) {
                instance.setEntityClass(first.get().getClass());
            }

            instance._propsList = toInsertPropsList(propsList);

            return instance;
        }

        /**
         * Creates an UPDATE CQL builder for the specified table.
         * 
         * <p>This method starts building an UPDATE statement for the given table. You must call
         * {@code set()} methods to specify which columns to update.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSB.update("users")
         *                 .set("last_login", "status")
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // UPDATE users SET last_login = :last_login, status = :status WHERE id = :id
         * }</pre>
         *
         * @param tableName the name of the table to update
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder update(final String tableName) {
            N.checkArgNotEmpty(tableName, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;

            return instance;
        }

        /**
         * Creates an UPDATE CQL builder for a table with entity class mapping.
         * 
         * <p>This method allows specifying both the table name and entity class, which enables
         * proper property-to-column name mapping based on the entity's annotations.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSB.update("user_accounts", User.class)
         *                 .set("lastLogin", "active")
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // UPDATE user_accounts SET last_login = :lastLogin, active = :active WHERE id = :id
         * }</pre>
         *
         * @param tableName the name of the table to update
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if tableName is null/empty or entityClass is null
         */
        public static CqlBuilder update(final String tableName, final Class<?> entityClass) {
            N.checkArgNotEmpty(tableName, UPDATE_PART_MSG);
            N.checkArgNotNull(entityClass, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;
            instance.setEntityClass(entityClass);

            return instance;
        }

        /**
         * Creates an UPDATE CQL builder for an entity class with automatic table name detection.
         * 
         * <p>The table name is derived from the entity class, and all updatable properties
         * (excluding those marked with {@code @NonUpdatable}, {@code @ReadOnly}, etc.) are included.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSB.update(User.class)
         *                 .set("name", "email")
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // UPDATE users SET name = :name, email = :email WHERE id = :id
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder update(final Class<?> entityClass) {
            return update(entityClass, null);
        }

        /**
         * Creates an UPDATE CQL builder for an entity class with excluded properties.
         * 
         * <p>This method automatically determines updatable properties from the entity class
         * while allowing additional properties to be excluded from the UPDATE.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> exclude = N.asSet("password", "createdDate");
         * String cql = NSB.update(User.class, exclude)
         *                 .set("name", "email")
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // UPDATE users SET name = :name, email = :email WHERE id = :id
         * }</pre>
         *
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the update
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        @SuppressWarnings("deprecation")
        public static CqlBuilder update(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance.setEntityClass(entityClass);
            instance._tableName = getTableName(entityClass, instance._namingPolicy);
            instance._propOrColumnNames = QueryUtil.getUpdatePropNames(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for a single column or expression.
         *
         * <p>This method initializes a DELETE statement for one column. The column name will be
         * converted according to the SCREAMING_SNAKE_CASE naming policy. This is useful for deleting
         * specific columns from a row rather than the entire row.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSB.delete("firstName")
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE firstName FROM account WHERE id = :id
         * }</pre>
         *
         * @param expr the column name or expression to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if expr is null or empty
         */
        public static CqlBuilder delete(final String expr) {
            N.checkArgNotEmpty(expr, DELETION_PART_MSG);

            return delete(N.asArray(expr));
        }

        /**
         * Creates a DELETE CQL builder for multiple columns.
         *
         * <p>This method initializes a DELETE statement for multiple columns. All column names
         * will be converted according to the SCREAMING_SNAKE_CASE naming policy. This allows selective
         * deletion of specific columns from rows.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSB.delete("firstName", "lastName", "email")
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE firstName, lastName, email FROM account WHERE id = :id
         * }</pre>
         *
         * @param columnNames the column names to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if columnNames is null or empty
         */
        public static CqlBuilder delete(final String... columnNames) {
            N.checkArgNotEmpty(columnNames, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._propOrColumnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for a collection of columns.
         *
         * <p>This method is useful when column names are determined dynamically. The collection
         * can contain property names that will be converted to column names according to the
         * SCREAMING_SNAKE_CASE naming policy.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("firstName", "lastName", "email");
         * String cql = NSB.delete(columns)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE firstName, lastName, email FROM account WHERE id = :id
         * }</pre>
         *
         * @param columnNames the collection of column names to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if columnNames is null or empty
         */
        public static CqlBuilder delete(final Collection<String> columnNames) {
            N.checkArgNotEmpty(columnNames, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._propOrColumnNames = columnNames;

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for an entity class.
         *
         * <p>This method generates a DELETE statement for all deletable properties of the class.
         * Properties marked with @ReadOnly, @ReadOnlyId, or @Transient are automatically excluded.
         * This is useful for creating templates for partial row deletion.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSB.delete(Account.class)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE firstName, lastName, email, status FROM account WHERE id = :id
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder delete(final Class<?> entityClass) {
            return delete(entityClass, null);
        }

        /**
         * Creates a DELETE CQL builder for an entity class, excluding specified properties.
         *
         * <p>This method generates a DELETE statement excluding both annotation-based exclusions
         * and the specified properties. Useful for selective column deletion where certain
         * fields should be preserved.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("email", "createdDate");
         * String cql = NSB.delete(Account.class, excluded)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE firstName, lastName, status FROM account WHERE id = :id
         * }</pre>
         *
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder delete(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for the specified table.
         * 
         * <p>This method initiates a DELETE statement. Typically, you'll want to add WHERE conditions
         * to avoid deleting all records in the table.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSB.deleteFrom("users")
         *                 .where(Filters.eq("status", "inactive"))
         *                 .build().query();
         * // DELETE FROM users WHERE status = :status
         * }</pre>
         *
         * @param tableName the name of the table to delete from
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder deleteFrom(final String tableName) {
            N.checkArgNotEmpty(tableName, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for a table with entity class mapping.
         * 
         * <p>This method enables proper property-to-column name mapping when building WHERE conditions
         * for the DELETE statement.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSB.deleteFrom("user_accounts", User.class)
         *                 .where(Filters.lt("lastLogin", someDate))
         *                 .build().query();
         * // DELETE FROM user_accounts WHERE last_login < :lastLogin
         * }</pre>
         *
         * @param tableName the name of the table to delete from
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if tableName is null/empty or entityClass is null
         */
        public static CqlBuilder deleteFrom(final String tableName, final Class<?> entityClass) {
            N.checkArgNotEmpty(tableName, DELETION_PART_MSG);
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;
            instance.setEntityClass(entityClass);

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for an entity class with automatic table name detection.
         * 
         * <p>The table name is derived from the entity class using {@code @Table} annotation
         * or naming policy conversion.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSB.deleteFrom(User.class)
         *                 .where(Filters.eq("id", 123))
         *                 .build().query();
         * // DELETE FROM users WHERE id = :id
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder deleteFrom(final Class<?> entityClass) {
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance.setEntityClass(entityClass);
            instance._tableName = getTableName(entityClass, instance._namingPolicy);

            return instance;
        }

        /**
         * Creates a SELECT CQL builder with a single column or expression.
         * 
         * <p>The selectPart parameter can be a simple column name or a complex CQL expression.
         * This method is useful for selecting computed values or using CQL functions.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSB.select("COUNT(*) AS total").from("users").build().query();
         * // SELECT COUNT(*) AS total FROM users
         * 
         * String cql2 = NSB.select("MAX(salary) - MIN(salary) AS salary_range")
         *                  .from("employees")
         *                  .build().query();
         * // SELECT MAX(salary) - MIN(salary) AS salary_range FROM employees
         * }</pre>
         *
         * @param selectPart the select expression
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if selectPart is null or empty
         */
        public static CqlBuilder select(final String selectPart) {
            N.checkArgNotEmpty(selectPart, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = Array.asList(selectPart);
            return instance;
        }

        /**
         * Creates a SELECT CQL builder with multiple columns.
         * 
         * <p>Each string in the array represents a column to select. The columns will be
         * included in the SELECT clause in the order specified.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSB.select("id", "name", "email", "created_date")
         *                 .from("users")
         *                 .where(Filters.eq("active", true))
         *                 .build().query();
         * // SELECT id, name, email, created_date FROM users WHERE active = :active
         * }</pre>
         *
         * @param propOrColumnNames the property or column names to select
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder select(final String... propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = Array.asList(propOrColumnNames);

            return instance;
        }

        /**
         * Creates a SELECT CQL builder with a collection of columns.
         * 
         * <p>This method allows using any Collection implementation to specify the columns
         * to select, providing flexibility in how column lists are constructed.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = getRequiredColumns();   // Dynamic column list
         * String cql = NSB.select(columns)
         *                 .from("products")
         *                 .where(Filters.gt("price", 100))
         *                 .build().query();
         * // SELECT column1, column2, ... FROM products WHERE price > :price
         * }</pre>
         *
         * @param propOrColumnNames collection of property or column names to select
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder select(final Collection<String> propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = propOrColumnNames;

            return instance;
        }

        /**
         * Creates a SELECT CQL builder with column aliases.
         * 
         * <p>The map keys represent the column names or expressions, and the values are their aliases.
         * This is useful for renaming columns in the result set.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, String> aliases = new LinkedHashMap<>();
         * aliases.put("u.first_name", "firstName");
         * aliases.put("u.last_name", "lastName");
         * aliases.put("COUNT(o.id)", "orderCount");
         * 
         * String cql = NSB.select(aliases)
         *                 .from("users u")
         *                 .leftJoin("orders o").on("u.id = o.user_id")
         *                 .groupBy("u.id")
         *                 .build().query();
         * // SELECT u.first_name AS firstName, u.last_name AS lastName, COUNT(o.id) AS orderCount
         * // FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id
         * }</pre>
         *
         * @param propOrColumnNameAliases map of column names/expressions to their aliases
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if propOrColumnNameAliases is null or empty
         */
        public static CqlBuilder select(final Map<String, String> propOrColumnNameAliases) {
            N.checkArgNotEmpty(propOrColumnNameAliases, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNameAliases = propOrColumnNameAliases;

            return instance;
        }

        /**
         * Creates a SELECT CQL builder for all properties of an entity class.
         * 
         * <p>This method selects all properties from the entity class that are not marked
         * as transient. Sub-entity properties are not included by default.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // If User class has properties: id, name, email, address
         * String cql = NSB.select(User.class).from("users").build().query();
         * // SELECT id, name, email, address FROM users
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass) {
            return select(entityClass, false);
        }

        /**
         * Creates a SELECT CQL builder for an entity class with optional sub-entity properties.
         * 
         * <p>When includeSubEntityProperties is true, properties of nested entity types are also included
         * in the selection, which is useful for fetching related data in a single query.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // If User has an Address sub-entity
         * String cql = NSB.select(User.class, true)
         *                 .from("users u")
         *                 .leftJoin("addresses a").on("u.address_id = a.id")
         *                 .build().query();
         * // SELECT u.id, u.name, u.email, a.street, a.city, a.zip FROM users u
         * // LEFT JOIN addresses a ON u.address_id = a.id
         * }</pre>
         *
         * @param entityClass the entity class
         * @param includeSubEntityProperties whether to include properties from nested entities
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass, final boolean includeSubEntityProperties) {
            return select(entityClass, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT CQL builder for an entity class with excluded properties.
         * 
         * <p>This method allows selecting most properties from an entity while excluding specific ones,
         * which is useful when you want to omit large fields like BLOBs or sensitive data.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> exclude = N.asSet("password", "profilePicture");
         * String cql = NSB.select(User.class, exclude).from("users").build().query();
         * // SELECT id, name, email, created_date FROM users
         * // (assuming User has id, name, email, created_date, password, and profilePicture)
         * }</pre>
         *
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return select(entityClass, false, excludedPropNames);
        }

        /**
         * Creates a SELECT CQL builder with full control over entity property selection.
         * 
         * <p>This method combines the ability to include sub-entity properties and exclude specific
         * properties, providing maximum flexibility in constructing SELECT statements.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> exclude = N.asSet("user.password", "address.coordinates");
         * String cql = NSB.select(User.class, true, exclude)
         *                 .from("users u")
         *                 .leftJoin("addresses a").on("u.address_id = a.id")
         *                 .build().query();
         * // Selects all User and Address properties except password and coordinates
         * }</pre>
         *
         * @param entityClass the entity class
         * @param includeSubEntityProperties whether to include properties from nested entities
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        @SuppressWarnings("deprecation")
        public static CqlBuilder select(final Class<?> entityClass, final boolean includeSubEntityProperties, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = QueryUtil.getSelectPropNames(entityClass, includeSubEntityProperties, excludedPropNames);

            return instance;
        }

        /**
         * Creates a complete SELECT...FROM CQL builder for an entity class.
         * 
         * <p>This is a convenience method that combines select() and from() operations.
         * The table name is automatically derived from the entity class.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSB.selectFrom(User.class).where(Filters.eq("active", true)).build().query();
         * // SELECT id, name, email FROM users WHERE active = :active
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, false);
        }

        /**
         * Creates a complete SELECT...FROM CQL builder with a table alias.
         * 
         * <p>This method allows specifying a table alias for use in joins and qualified column references.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSB.selectFrom(User.class, "u")
         *                 .leftJoin("orders o").on("u.id = o.user_id")
         *                 .where(Filters.isNotNull("o.id"))
         *                 .build().query();
         * // SELECT u.id, u.name, u.email FROM users u
         * // LEFT JOIN orders o ON u.id = o.user_id WHERE o.id IS NOT NULL
         * }</pre>
         *
         * @param entityClass the entity class
         * @param alias the table alias
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias) {
            return selectFrom(entityClass, alias, false);
        }

        /**
         * Creates a SELECT...FROM CQL builder with sub-entity property inclusion.
         * 
         * <p>When includeSubEntityProperties is true, the method automatically handles joining
         * related tables for nested entities.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Automatically includes joins for sub-entities
         * String cql = NSB.selectFrom(Order.class, true).build().query();
         * // May generate: SELECT o.*, c.*, p.* FROM orders o
         * // LEFT JOIN customers c ON o.customer_id = c.id
         * // LEFT JOIN products p ON o.product_id = p.id
         * }</pre>
         *
         * @param entityClass the entity class
         * @param includeSubEntityProperties whether to include nested entity properties
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final boolean includeSubEntityProperties) {
            return selectFrom(entityClass, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT...FROM CQL builder with alias and sub-entity control.
         * 
         * <p>Combines table aliasing with sub-entity property inclusion for complex queries.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSB.selectFrom(Order.class, "o", true)
         *                 .where(Filters.gt("o.total", 1000))
         *                 .build().query();
         * // Generates SELECT with proper aliases for main and sub-entities
         * }</pre>
         *
         * @param entityClass the entity class
         * @param alias the table alias
         * @param includeSubEntityProperties whether to include nested entity properties
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final boolean includeSubEntityProperties) {
            return selectFrom(entityClass, alias, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT...FROM CQL builder with property exclusion.
         * 
         * <p>This convenience method combines selecting specific properties and setting the FROM clause.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> exclude = N.asSet("largeData", "internalNotes");
         * String cql = NSB.selectFrom(User.class, exclude).build().query();
         * // SELECT id, name, email FROM users (excluding largeData and internalNotes)
         * }</pre>
         *
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, false, excludedPropNames);
        }

        /**
         * Creates a SELECT...FROM CQL builder with alias and property exclusion.
         * 
         * <p>Provides aliasing capability while excluding specific properties from selection.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> exclude = N.asSet("password");
         * String cql = NSB.selectFrom(User.class, "u", exclude)
         *                 .join("profiles p").on("u.id = p.user_id")
         *                 .build().query();
         * // SELECT u.id, u.name, u.email FROM users u JOIN profiles p ON u.id = p.user_id
         * }</pre>
         *
         * @param entityClass the entity class
         * @param alias the table alias
         * @param excludedPropNames properties to exclude
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, alias, false, excludedPropNames);
        }

        /**
         * Creates a SELECT...FROM CQL builder with sub-entities and exclusions.
         * 
         * <p>This method automatically handles complex FROM clauses when sub-entities are included.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> exclude = N.asSet("customer.creditCard");
         * String cql = NSB.selectFrom(Order.class, true, exclude).build().query();
         * // Selects Order with Customer sub-entity but excludes creditCard field
         * }</pre>
         *
         * @param entityClass the entity class
         * @param includeSubEntityProperties whether to include nested entity properties
         * @param excludedPropNames properties to exclude
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final boolean includeSubEntityProperties, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, QueryUtil.getTableAlias(entityClass), includeSubEntityProperties, excludedPropNames);
        }

        /**
         * Creates a fully-configured SELECT...FROM CQL builder.
         * 
         * <p>This is the most comprehensive selectFrom method, providing full control over
         * aliasing, sub-entity inclusion, and property exclusion.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> exclude = N.asSet("audit.details", "customer.internalNotes");
         * String cql = NSB.selectFrom(Order.class, "o", true, exclude)
         *                 .where(Filters.between("o.orderDate", startDate, endDate))
         *                 .orderBy("o.orderDate DESC")
         *                 .build().query();
         * // Complex SELECT with multiple tables, aliases, and exclusions
         * }</pre>
         *
         * @param entityClass the entity class
         * @param alias the table alias
         * @param includeSubEntityProperties whether to include nested entity properties
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final boolean includeSubEntityProperties,
                final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            if (hasSubEntityToInclude(entityClass, includeSubEntityProperties)) {
                final List<String> selectTableNames = getSelectTableNames(entityClass, alias, excludedPropNames, NamingPolicy.NO_CHANGE);
                return select(entityClass, includeSubEntityProperties, excludedPropNames).from(entityClass, selectTableNames);
            }

            return select(entityClass, includeSubEntityProperties, excludedPropNames).from(entityClass, alias);
        }

        /**
         * Creates a COUNT(*) query for the specified table.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSB.count("users")
         *                 .where(Filters.eq("status", "active"))
         *                 .build().query();
         * // SELECT count(*) FROM users WHERE status = :status
         * }</pre>
         *
         * @param tableName the name of the table to count rows from
         * @return a new CqlBuilder instance configured for COUNT operation
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder count(final String tableName) {
            N.checkArgNotEmpty(tableName, SELECTION_PART_MSG);

            return select(COUNT_ALL_LIST).from(tableName);
        }

        /**
         * Creates a COUNT(*) query for an entity class.
         *
         * <p>The table name is automatically derived from the entity class.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSB.count(User.class)
         *                 .where(Filters.eq("status", "active"))
         *                 .build().query();
         * // SELECT count(*) FROM users WHERE status = :status
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for COUNT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder count(final Class<?> entityClass) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            return select(COUNT_ALL_LIST).from(entityClass);
        }

        /**
         * Parses a condition into CQL with entity class mapping.
         * 
         * <p>This method is useful for generating just the CQL representation of a condition,
         * without the full query structure. It's primarily used for debugging or building
         * dynamic query parts.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Condition cond = Filters.and(
         *     Filters.eq("status", "active"),
         *     Filters.gt("age", 18)
         * );
         * String cql = NSB.parse(cond, User.class).build().query();
         * // status = :status AND age > :age
         * }</pre>
         *
         * @param cond the condition to parse
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance containing the parsed condition
         * @throws IllegalArgumentException if cond is null
         */
        public static CqlBuilder parse(final Condition cond, final Class<?> entityClass) {
            N.checkArgNotNull(cond, "cond");

            final CqlBuilder instance = createInstance();

            instance.setEntityClass(entityClass);
            instance._op = OperationType.QUERY;
            instance._isForConditionOnly = true;
            instance.append(cond);

            return instance;
        }
    }

    /**
     * Named CQL builder with snake_case (lower case with underscore) field/column naming strategy.
     * 
     * <p>This builder generates CQL statements using named parameters (e.g., :paramName) instead of 
     * positional parameters (?), and converts property names from camelCase to snake_case for column names.</p>
     * 
     * <p>Named parameters are useful for:</p>
     * <ul>
     *   <li>Better readability of generated CQL</li>
     *   <li>Reusing the same parameter multiple times in a query</li>
     *   <li>Integration with frameworks that support named parameters</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Simple SELECT with named parameters
     * String cql = NSC.select("firstName", "lastName")
     *                 .from("account")
     *                 .where(Filters.eq("id", 1))
     *                 .build().query();
     * // Output: SELECT first_name AS "firstName", last_name AS "lastName" FROM account WHERE id = :id
     * 
     * // INSERT with entity - generates named parameters
     * Account account = new Account();
     * account.setFirstName("John");
     * account.setLastName("Doe");
     * String cql = NSC.insert(account).into("account").build().query();
     * // Output: INSERT INTO account (first_name, last_name) VALUES (:firstName, :lastName)
     * }</pre>
     */
    public static class NSC extends CqlBuilder {

        /**
         * Creates a new instance of NSC builder with snake_case naming policy and named CQL generation.
         */
        NSC() {
            super(NamingPolicy.SNAKE_CASE, SQLPolicy.NAMED_SQL);
        }

        /**
         * Checks if this builder generates named CQL (with :paramName syntax).
         * 
         * @return always returns {@code true} for NSC builder
         */
        @Override
        protected boolean isNamedSql() {
            return true;
        }

        protected static NSC createInstance() {
            return new NSC();
        }

        /**
         * Creates an INSERT CQL builder for a single column expression with named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSC.insert("name").into("users").build().query();
         * // Output: INSERT INTO users (name) VALUES (:name)
         * }</pre>
         * 
         * @param expr the column name or expression to insert
         * @return a new CqlBuilder instance configured for INSERT operation with named parameters
         * @throws IllegalArgumentException if expr is null or empty
         */
        public static CqlBuilder insert(final String expr) {
            N.checkArgNotEmpty(expr, INSERTION_PART_MSG);

            return insert(N.asArray(expr));
        }

        /**
         * Creates an INSERT CQL builder for multiple columns with named parameters.
         * 
         * <p>Each column will have a corresponding named parameter in the VALUES clause.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSC.insert("firstName", "lastName", "email")
         *                 .into("users")
         *                 .build().query();
         * // Output: INSERT INTO users (first_name, last_name, email) VALUES (:firstName, :lastName, :email)
         * }</pre>
         * 
         * @param propOrColumnNames the property or column names to insert
         * @return a new CqlBuilder instance configured for INSERT operation with named parameters
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder insert(final String... propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._propOrColumnNames = Array.asList(propOrColumnNames);

            return instance;
        }

        /**
         * Creates an INSERT CQL builder for a collection of columns with named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("firstName", "lastName", "email");
         * String cql = NSC.insert(columns).into("users").build().query();
         * // Output: INSERT INTO users (first_name, last_name, email) VALUES (:firstName, :lastName, :email)
         * }</pre>
         * 
         * @param propOrColumnNames the collection of property or column names to insert
         * @return a new CqlBuilder instance configured for INSERT operation with named parameters
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder insert(final Collection<String> propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._propOrColumnNames = propOrColumnNames;

            return instance;
        }

        /**
         * Creates an INSERT CQL builder from a map of column-value pairs with named parameters.
         * 
         * <p>The map keys become both column names and parameter names.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Object> props = new HashMap<>();
         * props.put("firstName", "John");
         * props.put("lastName", "Doe");
         * String cql = NSC.insert(props).into("users").build().query();
         * // Output: INSERT INTO users (first_name, last_name) VALUES (:firstName, :lastName)
         * }</pre>
         * 
         * @param props map of property names to values
         * @return a new CqlBuilder instance configured for INSERT operation with named parameters
         * @throws IllegalArgumentException if props is null or empty
         */
        public static CqlBuilder insert(final Map<String, Object> props) {
            N.checkArgNotEmpty(props, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._props = props;

            return instance;
        }

        /**
         * Creates an INSERT CQL builder from an entity object with named parameters.
         * 
         * <p>Property names from the entity become named parameters in the CQL.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User user = new User();
         * user.setFirstName("John");
         * user.setLastName("Doe");
         * String cql = NSC.insert(user).into("users").build().query();
         * // Output: INSERT INTO users (first_name, last_name) VALUES (:firstName, :lastName)
         * }</pre>
         * 
         * @param entity the entity object to insert
         * @return a new CqlBuilder instance configured for INSERT operation with named parameters
         * @throws IllegalArgumentException if entity is null
         */
        public static CqlBuilder insert(final Object entity) {
            return insert(entity, null);
        }

        /**
         * Creates an INSERT CQL builder from an entity object with excluded properties and named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * User user = new User();
         * user.setFirstName("John");
         * user.setLastName("Doe");
         * user.setCreatedDate(new Date());
         * 
         * Set<String> excluded = Set.of("createdDate");
         * String cql = NSC.insert(user, excluded).into("users").build().query();
         * // Output: INSERT INTO users (first_name, last_name) VALUES (:firstName, :lastName)
         * }</pre>
         * 
         * @param entity the entity object to insert
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance configured for INSERT operation with named parameters
         * @throws IllegalArgumentException if entity is null
         */
        public static CqlBuilder insert(final Object entity, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entity, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance.setEntityClass(entity.getClass());

            parseInsertEntity(instance, entity, excludedPropNames);

            return instance;
        }

        /**
         * Creates an INSERT CQL builder for an entity class with named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSC.insert(User.class).into("users").build().query();
         * // Output: INSERT INTO users (id, first_name, last_name, email) VALUES (:id, :firstName, :lastName, :email)
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for INSERT operation with named parameters
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insert(final Class<?> entityClass) {
            return insert(entityClass, null);
        }

        /**
         * Creates an INSERT CQL builder for an entity class with excluded properties and named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = Set.of("id", "createdDate");
         * String cql = NSC.insert(User.class, excluded).into("users").build().query();
         * // Output: INSERT INTO users (first_name, last_name, email) VALUES (:firstName, :lastName, :email)
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance configured for INSERT operation with named parameters
         * @throws IllegalArgumentException if entityClass is null
         */
        @SuppressWarnings("deprecation")
        public static CqlBuilder insert(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = QueryUtil.getInsertPropNames(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates an INSERT INTO CQL builder for an entity class with named parameters.
         * 
         * <p>This is a convenience method that automatically determines the table name from the entity class.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * @Table("users")
         * public class User { ... }
         * 
         * String cql = NSC.insertInto(User.class).build().query();
         * // Output: INSERT INTO users (id, first_name, last_name, email) VALUES (:id, :firstName, :lastName, :email)
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for INSERT operation with table name set
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insertInto(final Class<?> entityClass) {
            return insertInto(entityClass, null);
        }

        /**
         * Creates an INSERT INTO CQL builder for an entity class with excluded properties and named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = Set.of("id");
         * String cql = NSC.insertInto(User.class, excluded).build().query();
         * // Output: INSERT INTO users (first_name, last_name, email) VALUES (:firstName, :lastName, :email)
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance configured for INSERT operation with table name set
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insertInto(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return insert(entityClass, excludedPropNames).into(entityClass);
        }

        /**
         * Creates a batch INSERT CQL builder with named parameters in MyCQL style.
         * 
         * <p>Note: Named parameters in batch inserts may have limited support depending on the database driver.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<User> users = Arrays.asList(
         *     new User("John", "Doe"),
         *     new User("Jane", "Smith")
         * );
         * String cql = NSC.batchInsert(users).into("users").build().query();
         * // Output format depends on the implementation
         * }</pre>
         * 
         * @param propsList collection of entities or property maps to insert
         * @return a new CqlBuilder instance configured for batch INSERT operation
         * @throws IllegalArgumentException if propsList is null or empty
         * <p><b>Note:</b> This API is in beta and may change in future versions</p>
         */
        @Beta
        public static CqlBuilder batchInsert(final Collection<?> propsList) {
            N.checkArgNotEmpty(propsList, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            final Optional<?> first = N.firstNonNull(propsList);

            if (first.isPresent() && Beans.isBeanClass(first.get().getClass())) {
                instance.setEntityClass(first.get().getClass());
            }

            instance._propsList = toInsertPropsList(propsList);

            return instance;
        }

        /**
         * Creates an UPDATE CQL builder for a table with named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSC.update("users")
         *                 .set("firstName", "John")
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: UPDATE users SET first_name = :firstName WHERE id = :id
         * }</pre>
         * 
         * @param tableName the name of the table to update
         * @return a new CqlBuilder instance configured for UPDATE operation with named parameters
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder update(final String tableName) {
            N.checkArgNotEmpty(tableName, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;

            return instance;
        }

        /**
         * Creates an UPDATE CQL builder for a table with entity class context and named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSC.update("users", User.class)
         *                 .set("firstName", "John")
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: UPDATE users SET first_name = :firstName WHERE id = :id
         * }</pre>
         * 
         * @param tableName the name of the table to update
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance configured for UPDATE operation with named parameters
         * @throws IllegalArgumentException if tableName is null or empty, or entityClass is null
         */
        public static CqlBuilder update(final String tableName, final Class<?> entityClass) {
            N.checkArgNotEmpty(tableName, UPDATE_PART_MSG);
            N.checkArgNotNull(entityClass, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;
            instance.setEntityClass(entityClass);

            return instance;
        }

        /**
         * Creates an UPDATE CQL builder for an entity class with named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSC.update(User.class)
         *                 .set("firstName", "John")
         *                 .set("lastName", "Doe")
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: UPDATE users SET first_name = :firstName, last_name = :lastName WHERE id = :id
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for UPDATE operation with named parameters
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder update(final Class<?> entityClass) {
            return update(entityClass, null);
        }

        /**
         * Creates an UPDATE CQL builder for an entity class with excluded properties and named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = Set.of("createdDate", "createdBy");
         * String cql = NSC.update(User.class, excluded)
         *                 .set("firstName", "John")
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: UPDATE users SET first_name = :firstName WHERE id = :id
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the update
         * @return a new CqlBuilder instance configured for UPDATE operation with named parameters
         * @throws IllegalArgumentException if entityClass is null
         */
        @SuppressWarnings("deprecation")
        public static CqlBuilder update(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance.setEntityClass(entityClass);
            instance._tableName = getTableName(entityClass, instance._namingPolicy);
            instance._propOrColumnNames = QueryUtil.getUpdatePropNames(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for a single column or expression.
         *
         * <p>This method initializes a DELETE statement for one column. The column name will be
         * converted according to the snake_case naming policy. This is useful for deleting
         * specific columns from a row rather than the entire row.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSC.delete("firstName")
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE first_name FROM account WHERE id = :id
         * }</pre>
         *
         * @param expr the column name or expression to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if expr is null or empty
         */
        public static CqlBuilder delete(final String expr) {
            N.checkArgNotEmpty(expr, DELETION_PART_MSG);

            return delete(N.asArray(expr));
        }

        /**
         * Creates a DELETE CQL builder for multiple columns.
         *
         * <p>This method initializes a DELETE statement for multiple columns. All column names
         * will be converted according to the snake_case naming policy. This allows selective
         * deletion of specific columns from rows.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSC.delete("firstName", "lastName", "email")
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE first_name, last_name, email FROM account WHERE id = :id
         * }</pre>
         *
         * @param columnNames the column names to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if columnNames is null or empty
         */
        public static CqlBuilder delete(final String... columnNames) {
            N.checkArgNotEmpty(columnNames, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._propOrColumnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for a collection of columns.
         *
         * <p>This method is useful when column names are determined dynamically. The collection
         * can contain property names that will be converted to column names according to the
         * snake_case naming policy.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("firstName", "lastName", "email");
         * String cql = NSC.delete(columns)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE first_name, last_name, email FROM account WHERE id = :id
         * }</pre>
         *
         * @param columnNames the collection of column names to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if columnNames is null or empty
         */
        public static CqlBuilder delete(final Collection<String> columnNames) {
            N.checkArgNotEmpty(columnNames, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._propOrColumnNames = columnNames;

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for an entity class.
         *
         * <p>This method generates a DELETE statement for all deletable properties of the class.
         * Properties marked with @ReadOnly, @ReadOnlyId, or @Transient are automatically excluded.
         * This is useful for creating templates for partial row deletion.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSC.delete(Account.class)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE first_name, last_name, email, status FROM account WHERE id = :id
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder delete(final Class<?> entityClass) {
            return delete(entityClass, null);
        }

        /**
         * Creates a DELETE CQL builder for an entity class, excluding specified properties.
         *
         * <p>This method generates a DELETE statement excluding both annotation-based exclusions
         * and the specified properties. Useful for selective column deletion where certain
         * fields should be preserved.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("email", "createdDate");
         * String cql = NSC.delete(Account.class, excluded)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE first_name, last_name, status FROM account WHERE id = :id
         * }</pre>
         *
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder delete(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates a DELETE FROM CQL builder for a table with named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSC.deleteFrom("users")
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: DELETE FROM users WHERE id = :id
         * }</pre>
         * 
         * @param tableName the name of the table to delete from
         * @return a new CqlBuilder instance configured for DELETE operation with named parameters
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder deleteFrom(final String tableName) {
            N.checkArgNotEmpty(tableName, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;

            return instance;
        }

        /**
         * Creates a DELETE FROM CQL builder for a table with entity class context and named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSC.deleteFrom("users", User.class)
         *                 .where(Filters.eq("firstName", "John"))
         *                 .build().query();
         * // Output: DELETE FROM users WHERE first_name = :firstName
         * }</pre>
         * 
         * @param tableName the name of the table to delete from
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance configured for DELETE operation with named parameters
         * @throws IllegalArgumentException if tableName is null or empty, or entityClass is null
         */
        public static CqlBuilder deleteFrom(final String tableName, final Class<?> entityClass) {
            N.checkArgNotEmpty(tableName, DELETION_PART_MSG);
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;
            instance.setEntityClass(entityClass);

            return instance;
        }

        /**
         * Creates a DELETE FROM CQL builder for an entity class with named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSC.deleteFrom(User.class)
         *                 .where(Filters.eq("firstName", "John"))
         *                 .build().query();
         * // Output: DELETE FROM users WHERE first_name = :firstName
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for DELETE operation with named parameters
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder deleteFrom(final Class<?> entityClass) {
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance.setEntityClass(entityClass);
            instance._tableName = getTableName(entityClass, instance._namingPolicy);

            return instance;
        }

        /**
         * Creates a SELECT CQL builder with a single column or expression using named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSC.select("COUNT(*)").from("users").where(Filters.eq("active", true)).build().query();
         * // Output: SELECT count(*) FROM users WHERE active = :active
         * }</pre>
         * 
         * @param selectPart the select expression
         * @return a new CqlBuilder instance configured for SELECT operation with named parameters
         * @throws IllegalArgumentException if selectPart is null or empty
         */
        public static CqlBuilder select(final String selectPart) {
            N.checkArgNotEmpty(selectPart, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = Array.asList(selectPart);
            return instance;
        }

        /**
         * Creates a SELECT CQL builder with multiple columns using named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSC.select("firstName", "lastName", "email")
         *                 .from("users")
         *                 .where(Filters.eq("active", true))
         *                 .build().query();
         * // Output: SELECT first_name AS "firstName", last_name AS "lastName", email FROM users WHERE active = :active
         * }</pre>
         * 
         * @param propOrColumnNames the property or column names to select
         * @return a new CqlBuilder instance configured for SELECT operation with named parameters
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder select(final String... propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = Array.asList(propOrColumnNames);

            return instance;
        }

        /**
         * Creates a SELECT CQL builder with a collection of columns using named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("firstName", "lastName", "email");
         * String cql = NSC.select(columns).from("users").build().query();
         * // Output: SELECT first_name AS "firstName", last_name AS "lastName", email FROM users
         * }</pre>
         * 
         * @param propOrColumnNames the collection of property or column names to select
         * @return a new CqlBuilder instance configured for SELECT operation with named parameters
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder select(final Collection<String> propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = propOrColumnNames;

            return instance;
        }

        /**
         * Creates a SELECT CQL builder with column aliases using named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, String> aliases = new HashMap<>();
         * aliases.put("firstName", "fname");
         * aliases.put("lastName", "lname");
         * String cql = NSC.select(aliases).from("users").build().query();
         * // Output: SELECT first_name AS fname, last_name AS lname FROM users
         * }</pre>
         * 
         * @param propOrColumnNameAliases map of column names to their aliases
         * @return a new CqlBuilder instance configured for SELECT operation with named parameters
         * @throws IllegalArgumentException if propOrColumnNameAliases is null or empty
         */
        public static CqlBuilder select(final Map<String, String> propOrColumnNameAliases) {
            N.checkArgNotEmpty(propOrColumnNameAliases, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNameAliases = propOrColumnNameAliases;

            return instance;
        }

        /**
         * Creates a SELECT CQL builder for all properties of an entity class with named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSC.select(User.class).from("users").where(Filters.eq("active", true)).build().query();
         * // Output: SELECT id, first_name AS "firstName", last_name AS "lastName", email FROM users WHERE active = :active
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for SELECT operation with named parameters
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass) {
            return select(entityClass, false);
        }

        /**
         * Creates a SELECT CQL builder for an entity class with sub-entity option and named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSC.select(User.class, true).from("users").build().query();
         * // Includes properties from User and any embedded entities
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties true to include properties of embedded entities
         * @return a new CqlBuilder instance configured for SELECT operation with named parameters
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass, final boolean includeSubEntityProperties) {
            return select(entityClass, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT CQL builder for an entity class with excluded properties and named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = Set.of("password", "secretKey");
         * String cql = NSC.select(User.class, excluded).from("users").build().query();
         * // Selects all User properties except password and secretKey
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT operation with named parameters
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return select(entityClass, false, excludedPropNames);
        }

        /**
         * Creates a SELECT CQL builder for an entity class with all options and named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = Set.of("password");
         * String cql = NSC.select(User.class, true, excluded)
         *                 .from("users")
         *                 .where(Filters.eq("active", true))
         *                 .build().query();
         * // Output uses named parameter :active
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties true to include properties of embedded entities
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT operation with named parameters
         * @throws IllegalArgumentException if entityClass is null
         */
        @SuppressWarnings("deprecation")
        public static CqlBuilder select(final Class<?> entityClass, final boolean includeSubEntityProperties, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = QueryUtil.getSelectPropNames(entityClass, includeSubEntityProperties, excludedPropNames);

            return instance;
        }

        /**
         * Creates a SELECT FROM CQL builder for an entity class with named parameters.
         * 
         * <p>This is a convenience method that combines SELECT and FROM operations.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSC.selectFrom(User.class).where(Filters.eq("id", 1)).build().query();
         * // Output: SELECT id, first_name AS "firstName", last_name AS "lastName" FROM users WHERE id = :id
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, false);
        }

        /**
         * Creates a SELECT FROM CQL builder for an entity class with table alias and named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSC.selectFrom(User.class, "u")
         *                 .where(Filters.eq("u.active", true))
         *                 .build().query();
         * // Output uses named parameter :active
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias) {
            return selectFrom(entityClass, alias, false);
        }

        /**
         * Creates a SELECT FROM CQL builder with sub-entity option and named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSC.selectFrom(User.class, true).build().query();
         * // Includes properties from User and any embedded entities with automatic joins
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties true to include properties of embedded entities
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final boolean includeSubEntityProperties) {
            return selectFrom(entityClass, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT FROM CQL builder with alias and sub-entity option using named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSC.selectFrom(User.class, "u", true).build().query();
         * // Includes properties from User and embedded entities with table alias
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @param includeSubEntityProperties true to include properties of embedded entities
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final boolean includeSubEntityProperties) {
            return selectFrom(entityClass, alias, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT FROM CQL builder with excluded properties and named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = Set.of("password");
         * String cql = NSC.selectFrom(User.class, excluded).where(Filters.eq("active", true)).build().query();
         * // Selects all properties except password, uses :active parameter
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, false, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM CQL builder with alias and excluded properties using named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = Set.of("password");
         * String cql = NSC.selectFrom(User.class, "u", excluded).build().query();
         * // Selects all properties except password with table alias
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, alias, false, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM CQL builder with sub-entity and exclusion options using named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = Set.of("password");
         * String cql = NSC.selectFrom(User.class, true, excluded).build().query();
         * // Selects all properties including sub-entities except password
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties true to include properties of embedded entities
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final boolean includeSubEntityProperties, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, QueryUtil.getTableAlias(entityClass), includeSubEntityProperties, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM CQL builder with all options and named parameters.
         * 
         * <p>This is the most flexible selectFrom method.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = Set.of("password");
         * String cql = NSC.selectFrom(User.class, "u", true, excluded)
         *                 .where(Filters.and(
         *                     Filters.eq("u.active", true),
         *                     Filters.like("u.email", "%@example.com")
         *                 ))
         *                 .build().query();
         * // Complex select with alias, sub-entities, exclusions, and named parameters
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @param includeSubEntityProperties true to include properties of embedded entities
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final boolean includeSubEntityProperties,
                final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            if (hasSubEntityToInclude(entityClass, includeSubEntityProperties)) {
                final List<String> selectTableNames = getSelectTableNames(entityClass, alias, excludedPropNames, NamingPolicy.SNAKE_CASE);
                return select(entityClass, includeSubEntityProperties, excludedPropNames).from(entityClass, selectTableNames);
            }

            return select(entityClass, includeSubEntityProperties, excludedPropNames).from(entityClass, alias);
        }

        /**
         * Creates a COUNT(*) CQL builder for a table with named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSC.count("users").where(Filters.eq("active", true)).build().query();
         * // Output: SELECT count(*) FROM users WHERE active = :active
         * }</pre>
         * 
         * @param tableName the name of the table to count rows from
         * @return a new CqlBuilder instance configured for COUNT operation
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder count(final String tableName) {
            N.checkArgNotEmpty(tableName, SELECTION_PART_MSG);

            return select(COUNT_ALL_LIST).from(tableName);
        }

        /**
         * Creates a COUNT(*) CQL builder for an entity class with named parameters.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NSC.count(User.class)
         *                 .where(Filters.and(
         *                     Filters.eq("firstName", "John"),
         *                     Filters.gt("age", 18)
         *                 ))
         *                 .build().query();
         * // Output: SELECT count(*) FROM users WHERE first_name = :firstName AND age = :age
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for COUNT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder count(final Class<?> entityClass) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            return select(COUNT_ALL_LIST).from(entityClass);
        }

        /**
         * Parses a condition into CQL with entity class context and named parameters.
         * 
         * <p>This method generates just the condition part of CQL with named parameters.</p>
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Condition cond = Filters.and(
         *     Filters.eq("firstName", "John"),
         *     Filters.gt("age", 18),
         *     Filters.like("email", "%@example.com")
         * );
         * String cql = NSC.parse(cond, User.class).build().query();
         * // Output: first_name = :firstName AND age = :age AND email LIKE :email
         * }</pre>
         * 
         * @param cond the condition to parse
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance containing the parsed condition
         * @throws IllegalArgumentException if cond is null
         */
        public static CqlBuilder parse(final Condition cond, final Class<?> entityClass) {
            N.checkArgNotNull(cond, "cond");

            final CqlBuilder instance = createInstance();

            instance.setEntityClass(entityClass);
            instance._op = OperationType.QUERY;
            instance._isForConditionOnly = true;
            instance.append(cond);

            return instance;
        }
    }

    /**
     * Named CQL builder with all capital case (upper case with underscore) field/column naming strategy.
     * This builder generates CQL with named parameters (e.g., :paramName) and converts property names
     * to SCREAMING_SNAKE_CASE format.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Simple SELECT with named parameters
     * N.println(NAC.select("firstName", "lastName").from("account").where(Filters.eq("id", 1)).build().query());
     * // Output: SELECT FIRST_NAME AS "firstName", LAST_NAME AS "lastName" FROM ACCOUNT WHERE ID = :id
     * 
     * // INSERT with entity
     * Account account = new Account();
     * account.setFirstName("John");
     * account.setLastName("Doe");
     * String cql = NAC.insert(account).into("ACCOUNT").build().query();
     * // Output: INSERT INTO ACCOUNT (FIRST_NAME, LAST_NAME) VALUES (:firstName, :lastName)
     * }</pre>
     */
    public static class NAC extends CqlBuilder {

        NAC() {
            super(NamingPolicy.SCREAMING_SNAKE_CASE, SQLPolicy.NAMED_SQL);
        }

        @Override
        protected boolean isNamedSql() {
            return true;
        }

        protected static NAC createInstance() {
            return new NAC();
        }

        /**
         * Creates an INSERT CQL builder for a single column expression.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.insert("FIRST_NAME").into("ACCOUNT").build().query();
         * // Output: INSERT INTO ACCOUNT (FIRST_NAME) VALUES (:FIRST_NAME)
         * }</pre>
         * 
         * @param expr the column name or expression to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if expr is null or empty
         */
        public static CqlBuilder insert(final String expr) {
            N.checkArgNotEmpty(expr, INSERTION_PART_MSG);

            return insert(N.asArray(expr));
        }

        /**
         * Creates an INSERT CQL builder for the specified property or column names.
         * Property names will be converted to SCREAMING_SNAKE_CASE format.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.insert("firstName", "lastName").into("ACCOUNT").build().query();
         * // Output: INSERT INTO ACCOUNT (FIRST_NAME, LAST_NAME) VALUES (:firstName, :lastName)
         * }</pre>
         * 
         * @param propOrColumnNames the property or column names to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder insert(final String... propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._propOrColumnNames = Array.asList(propOrColumnNames);

            return instance;
        }

        /**
         * Creates an INSERT CQL builder for the specified collection of property or column names.
         * Property names will be converted to SCREAMING_SNAKE_CASE format.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("firstName", "lastName", "email");
         * String cql = NAC.insert(columns).into("ACCOUNT").build().query();
         * // Output: INSERT INTO ACCOUNT (FIRST_NAME, LAST_NAME, EMAIL) VALUES (:firstName, :lastName, :email)
         * }</pre>
         * 
         * @param propOrColumnNames the collection of property or column names to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder insert(final Collection<String> propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._propOrColumnNames = propOrColumnNames;

            return instance;
        }

        /**
         * Creates an INSERT CQL builder for the specified property-value map.
         * Property names will be converted to SCREAMING_SNAKE_CASE format.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Object> props = Map.of("firstName", "John", "lastName", "Doe", "age", 30);
         * String cql = NAC.insert(props).into("ACCOUNT").build().query();
         * // Output: INSERT INTO ACCOUNT (FIRST_NAME, LAST_NAME, AGE) VALUES (:firstName, :lastName, :age)
         * }</pre>
         * 
         * @param props map of property names to values
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if props is null or empty
         */
        public static CqlBuilder insert(final Map<String, Object> props) {
            N.checkArgNotEmpty(props, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._props = props;

            return instance;
        }

        /**
         * Creates an INSERT CQL builder for the specified entity object.
         * The entity's properties will be extracted and used for the INSERT statement.
         * Properties marked with @ReadOnly, @ReadOnlyId, or @Transient will be excluded.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Account account = new Account();
         * account.setFirstName("John");
         * account.setLastName("Doe");
         * String cql = NAC.insert(account).into("ACCOUNT").build().query();
         * // Output: INSERT INTO ACCOUNT (FIRST_NAME, LAST_NAME) VALUES (:firstName, :lastName)
         * }</pre>
         * 
         * @param entity the entity object to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entity is null
         */
        public static CqlBuilder insert(final Object entity) {
            return insert(entity, null);
        }

        /**
         * Creates an INSERT CQL builder for the specified entity object with excluded properties.
         * The entity's properties will be extracted and used for the INSERT statement,
         * excluding the specified property names.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Account account = new Account();
         * account.setFirstName("John");
         * account.setLastName("Doe");
         * account.setCreatedTime(new Date());
         * String cql = NAC.insert(account, Set.of("createdTime")).into("ACCOUNT").build().query();
         * // Output: INSERT INTO ACCOUNT (FIRST_NAME, LAST_NAME) VALUES (:firstName, :lastName)
         * }</pre>
         * 
         * @param entity the entity object to insert
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entity is null
         */
        public static CqlBuilder insert(final Object entity, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entity, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance.setEntityClass(entity.getClass());

            parseInsertEntity(instance, entity, excludedPropNames);

            return instance;
        }

        /**
         * Creates an INSERT CQL builder for the specified entity class.
         * All insertable properties of the class will be included.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.insert(Account.class).into("ACCOUNT").build().query();
         * // Output: INSERT INTO ACCOUNT (ID, FIRST_NAME, LAST_NAME, EMAIL) VALUES (:id, :firstName, :lastName, :email)
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insert(final Class<?> entityClass) {
            return insert(entityClass, null);
        }

        /**
         * Creates an INSERT CQL builder for the specified entity class with excluded properties.
         * All insertable properties of the class will be included except those specified.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.insert(Account.class, Set.of("id", "createdTime")).into("ACCOUNT").build().query();
         * // Output: INSERT INTO ACCOUNT (FIRST_NAME, LAST_NAME, EMAIL) VALUES (:firstName, :lastName, :email)
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        @SuppressWarnings("deprecation")
        public static CqlBuilder insert(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = QueryUtil.getInsertPropNames(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates an INSERT INTO CQL builder for the specified entity class.
         * The table name will be derived from the entity class name or @Table annotation.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * @Table("USER_ACCOUNT")
         * class Account { ... }
         * 
         * String cql = NAC.insertInto(Account.class).build().query();
         * // Output: INSERT INTO USER_ACCOUNT (ID, FIRST_NAME, LAST_NAME) VALUES (:id, :firstName, :lastName)
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for INSERT operation with table name set
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insertInto(final Class<?> entityClass) {
            return insertInto(entityClass, null);
        }

        /**
         * Creates an INSERT INTO CQL builder for the specified entity class with excluded properties.
         * The table name will be derived from the entity class name or @Table annotation.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.insertInto(Account.class, Set.of("id")).build().query();
         * // Output: INSERT INTO ACCOUNT (FIRST_NAME, LAST_NAME, EMAIL) VALUES (:firstName, :lastName, :email)
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance configured for INSERT operation with table name set
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insertInto(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return insert(entityClass, excludedPropNames).into(entityClass);
        }

        /**
         * Creates a batch INSERT CQL builder for MyCQL-style batch inserts.
         * Generates a single INSERT statement with multiple value rows.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Account acc1 = new Account("John", "Doe");
         * Account acc2 = new Account("Jane", "Smith");
         * List<Account> accounts = Arrays.asList(acc1, acc2);
         * String cql = NAC.batchInsert(accounts).into("ACCOUNT").build().query();
         * // Output: INSERT INTO ACCOUNT (FIRST_NAME, LAST_NAME) VALUES
         * //         (:firstName_1, :lastName_1), (:firstName_2, :lastName_2)
         * }</pre>
         *
         * @param propsList collection of entities or property maps to insert
         * @return a new CqlBuilder instance configured for batch INSERT operation
         * @throws IllegalArgumentException if propsList is null or empty
         * <p><b>Note:</b> This is a beta feature and may change in future versions</p>
         */
        @Beta
        public static CqlBuilder batchInsert(final Collection<?> propsList) {
            N.checkArgNotEmpty(propsList, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            final Optional<?> first = N.firstNonNull(propsList);

            if (first.isPresent() && Beans.isBeanClass(first.get().getClass())) {
                instance.setEntityClass(first.get().getClass());
            }

            instance._propsList = toInsertPropsList(propsList);

            return instance;
        }

        /**
         * Creates an UPDATE CQL builder for the specified table.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.update("ACCOUNT")
         *                 .set("STATUS", "ACTIVE")
         *                 .where(Filters.eq("ID", 1))
         *                 .build().query();
         * // Output: UPDATE ACCOUNT SET STATUS = :STATUS WHERE ID = :id
         * }</pre>
         * 
         * @param tableName the name of the table to update
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder update(final String tableName) {
            N.checkArgNotEmpty(tableName, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;

            return instance;
        }

        /**
         * Creates an UPDATE CQL builder for the specified table with entity class mapping.
         * The entity class provides property-to-column mapping information.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.update("ACCOUNT", Account.class)
         *                 .set("status", "lastModified")
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: UPDATE ACCOUNT SET STATUS = :status, LAST_MODIFIED = :lastModified WHERE ID = :id
         * }</pre>
         * 
         * @param tableName the name of the table to update
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if tableName is null/empty or entityClass is null
         */
        public static CqlBuilder update(final String tableName, final Class<?> entityClass) {
            N.checkArgNotEmpty(tableName, UPDATE_PART_MSG);
            N.checkArgNotNull(entityClass, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;
            instance.setEntityClass(entityClass);

            return instance;
        }

        /**
         * Creates an UPDATE CQL builder for the specified entity class.
         * The table name will be derived from the entity class name or @Table annotation.
         * All updatable properties will be included.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.update(Account.class)
         *                 .set("status", "lastModified")
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: UPDATE ACCOUNT SET STATUS = :status, LAST_MODIFIED = :lastModified WHERE ID = :id
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder update(final Class<?> entityClass) {
            return update(entityClass, null);
        }

        /**
         * Creates an UPDATE CQL builder for the specified entity class with excluded properties.
         * The table name will be derived from the entity class name or @Table annotation.
         * Properties marked with @NonUpdatable or in the excluded set will be omitted.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.update(Account.class, Set.of("createdTime"))
         *                 .set("status", "lastModified")
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: UPDATE ACCOUNT SET STATUS = :status, LAST_MODIFIED = :lastModified WHERE ID = :id
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the update
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        @SuppressWarnings("deprecation")
        public static CqlBuilder update(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance.setEntityClass(entityClass);
            instance._tableName = getTableName(entityClass, instance._namingPolicy);
            instance._propOrColumnNames = QueryUtil.getUpdatePropNames(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for a single column or expression.
         *
         * <p>This method initializes a DELETE statement for one column. The column name will be
         * converted according to the camelCase naming policy. This is useful for deleting
         * specific columns from a row rather than the entire row.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.delete("firstName")
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE FIRST_NAME FROM account WHERE ID = :id
         * }</pre>
         *
         * @param expr the column name or expression to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if expr is null or empty
         */
        public static CqlBuilder delete(final String expr) {
            N.checkArgNotEmpty(expr, DELETION_PART_MSG);

            return delete(N.asArray(expr));
        }

        /**
         * Creates a DELETE CQL builder for multiple columns.
         *
         * <p>This method initializes a DELETE statement for multiple columns. All column names
         * will be converted according to the camelCase naming policy. This allows selective
         * deletion of specific columns from rows.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.delete("firstName", "lastName", "email")
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE FIRST_NAME, LAST_NAME, EMAIL FROM account WHERE ID = :id
         * }</pre>
         *
         * @param columnNames the column names to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if columnNames is null or empty
         */
        public static CqlBuilder delete(final String... columnNames) {
            N.checkArgNotEmpty(columnNames, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._propOrColumnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for a collection of columns.
         *
         * <p>This method is useful when column names are determined dynamically. The collection
         * can contain property names that will be converted to column names according to the
         * camelCase naming policy.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("firstName", "lastName", "email");
         * String cql = NAC.delete(columns)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE FIRST_NAME, LAST_NAME, EMAIL FROM account WHERE ID = :id
         * }</pre>
         *
         * @param columnNames the collection of column names to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if columnNames is null or empty
         */
        public static CqlBuilder delete(final Collection<String> columnNames) {
            N.checkArgNotEmpty(columnNames, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._propOrColumnNames = columnNames;

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for an entity class.
         *
         * <p>This method generates a DELETE statement for all deletable properties of the class.
         * Properties marked with @ReadOnly, @ReadOnlyId, or @Transient are automatically excluded.
         * This is useful for creating templates for partial row deletion.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.delete(Account.class)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE FIRST_NAME, LAST_NAME, EMAIL, STATUS FROM account WHERE ID = :id
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder delete(final Class<?> entityClass) {
            return delete(entityClass, null);
        }

        /**
         * Creates a DELETE CQL builder for an entity class, excluding specified properties.
         *
         * <p>This method generates a DELETE statement excluding both annotation-based exclusions
         * and the specified properties. Useful for selective column deletion where certain
         * fields should be preserved.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("email", "createdDate");
         * String cql = NAC.delete(Account.class, excluded)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE FIRST_NAME, LAST_NAME, STATUS FROM account WHERE ID = :id
         * }</pre>
         *
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder delete(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates a DELETE FROM CQL builder for the specified table.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.deleteFrom("ACCOUNT")
         *                 .where(Filters.eq("STATUS", "INACTIVE"))
         *                 .build().query();
         * // Output: DELETE FROM ACCOUNT WHERE STATUS = :STATUS
         * }</pre>
         * 
         * @param tableName the name of the table to delete from
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder deleteFrom(final String tableName) {
            N.checkArgNotEmpty(tableName, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;

            return instance;
        }

        /**
         * Creates a DELETE FROM CQL builder for the specified table with entity class mapping.
         * The entity class provides property-to-column mapping information for conditions.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.deleteFrom("ACCOUNT", Account.class)
         *                 .where(Filters.eq("status", "INACTIVE"))
         *                 .build().query();
         * // Output: DELETE FROM ACCOUNT WHERE STATUS = :status
         * }</pre>
         * 
         * @param tableName the name of the table to delete from
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if tableName is null/empty or entityClass is null
         */
        public static CqlBuilder deleteFrom(final String tableName, final Class<?> entityClass) {
            N.checkArgNotEmpty(tableName, DELETION_PART_MSG);
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;
            instance.setEntityClass(entityClass);

            return instance;
        }

        /**
         * Creates a DELETE FROM CQL builder for the specified entity class.
         * The table name will be derived from the entity class name or @Table annotation.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.deleteFrom(Account.class)
         *                 .where(Filters.and(Filters.eq("status", "INACTIVE"), Filters.lt("lastLogin", yesterday)))
         *                 .build().query();
         * // Output: DELETE FROM ACCOUNT WHERE STATUS = :status AND LAST_LOGIN < :lastLogin
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder deleteFrom(final Class<?> entityClass) {
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance.setEntityClass(entityClass);
            instance._tableName = getTableName(entityClass, instance._namingPolicy);

            return instance;
        }

        /**
         * Creates a SELECT CQL builder with a single select expression.
         * The expression can be a column name, function call, or any valid CQL expression.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.select("COUNT(*)").from("ACCOUNT").build().query();
         * // Output: SELECT count(*) FROM ACCOUNT
         * 
         * String cql2 = NAC.select("MAX(BALANCE)").from("ACCOUNT").where(Filters.eq("STATUS", "ACTIVE")).build().query();
         * // Output: SELECT MAX(BALANCE) FROM ACCOUNT WHERE STATUS = :STATUS
         * }</pre>
         * 
         * @param selectPart the select expression
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if selectPart is null or empty
         */
        public static CqlBuilder select(final String selectPart) {
            N.checkArgNotEmpty(selectPart, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = Array.asList(selectPart);
            return instance;
        }

        /**
         * Creates a SELECT CQL builder for the specified property or column names.
         * Property names will be converted to SCREAMING_SNAKE_CASE format.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.select("firstName", "lastName", "email")
         *                 .from("ACCOUNT")
         *                 .where(Filters.eq("STATUS", "ACTIVE"))
         *                 .build().query();
         * // Output: SELECT FIRST_NAME AS "firstName", LAST_NAME AS "lastName", EMAIL AS "email" 
         * //         FROM ACCOUNT WHERE STATUS = :STATUS
         * }</pre>
         * 
         * @param propOrColumnNames the property or column names to select
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder select(final String... propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = Array.asList(propOrColumnNames);

            return instance;
        }

        /**
         * Creates a SELECT CQL builder for the specified collection of property or column names.
         * Property names will be converted to SCREAMING_SNAKE_CASE format.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("firstName", "lastName", "accountBalance");
         * String cql = NAC.select(columns)
         *                 .from("ACCOUNT")
         *                 .orderBy("LAST_NAME")
         *                 .build().query();
         * // Output: SELECT FIRST_NAME AS "firstName", LAST_NAME AS "lastName", 
         * //         ACCOUNT_BALANCE AS "accountBalance" FROM ACCOUNT ORDER BY LAST_NAME
         * }</pre>
         * 
         * @param propOrColumnNames the collection of property or column names to select
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder select(final Collection<String> propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = propOrColumnNames;

            return instance;
        }

        /**
         * Creates a SELECT CQL builder with column aliases.
         * The map keys are property/column names and values are their aliases.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, String> aliases = Map.of(
         *     "firstName", "fname",
         *     "lastName", "lname",
         *     "accountBalance", "balance"
         * );
         * String cql = NAC.select(aliases).from("ACCOUNT").build().query();
         * // Output: SELECT FIRST_NAME AS fname, LAST_NAME AS lname, ACCOUNT_BALANCE AS balance FROM ACCOUNT
         * }</pre>
         * 
         * @param propOrColumnNameAliases map of property/column names to their aliases
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if propOrColumnNameAliases is null or empty
         */
        public static CqlBuilder select(final Map<String, String> propOrColumnNameAliases) {
            N.checkArgNotEmpty(propOrColumnNameAliases, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNameAliases = propOrColumnNameAliases;

            return instance;
        }

        /**
         * Creates a SELECT CQL builder for all properties of the specified entity class.
         * Properties marked with @Transient will be excluded.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.select(Account.class)
         *                 .from("ACCOUNT")
         *                 .where(Filters.gt("BALANCE", 1000))
         *                 .build().query();
         * // Output: SELECT ID AS "id", FIRST_NAME AS "firstName", LAST_NAME AS "lastName", 
         * //         EMAIL AS "email", BALANCE AS "balance" FROM ACCOUNT WHERE BALANCE > :BALANCE
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass) {
            return select(entityClass, false);
        }

        /**
         * Creates a SELECT CQL builder for properties of the specified entity class.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // With sub-entities included
         * String cql = NAC.select(Order.class, true)
         *                 .from("ORDER")
         *                 .build().query();
         * // Will include properties from Order and its related entities
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties if true, properties of sub-entities will be included
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass, final boolean includeSubEntityProperties) {
            return select(entityClass, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT CQL builder for properties of the specified entity class with exclusions.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.select(Account.class, Set.of("password", "securityQuestion"))
         *                 .from("ACCOUNT")
         *                 .build().query();
         * // Output: SELECT ID AS "id", FIRST_NAME AS "firstName", LAST_NAME AS "lastName", 
         * //         EMAIL AS "email" FROM ACCOUNT
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return select(entityClass, false, excludedPropNames);
        }

        /**
         * Creates a SELECT CQL builder for properties of the specified entity class with options.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.select(Order.class, true, Set.of("internalNotes"))
         *                 .from("ORDER o")
         *                 .join("CUSTOMER c", Filters.eq("o.CUSTOMER_ID", "c.ID"))
         *                 .build().query();
         * // Selects all Order properties except internalNotes, plus Customer properties
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties if true, properties of sub-entities will be included
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        @SuppressWarnings("deprecation")
        public static CqlBuilder select(final Class<?> entityClass, final boolean includeSubEntityProperties, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = QueryUtil.getSelectPropNames(entityClass, includeSubEntityProperties, excludedPropNames);

            return instance;
        }

        /**
         * Creates a SELECT FROM CQL builder for the specified entity class.
         * Combines SELECT and FROM operations in a single call.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.selectFrom(Account.class)
         *                 .where(Filters.eq("status", "ACTIVE"))
         *                 .orderBy("lastName")
         *                 .build().query();
         * // Output: SELECT ID AS "id", FIRST_NAME AS "firstName", LAST_NAME AS "lastName" 
         * //         FROM ACCOUNT WHERE STATUS = :status ORDER BY LAST_NAME
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, false);
        }

        /**
         * Creates a SELECT FROM CQL builder for the specified entity class with table alias.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.selectFrom(Account.class, "a")
         *                 .where(Filters.eq("a.status", "ACTIVE"))
         *                 .build().query();
         * // Output: SELECT a.ID AS "id", a.FIRST_NAME AS "firstName", a.LAST_NAME AS "lastName" 
         * //         FROM ACCOUNT a WHERE a.STATUS = :status
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias) {
            return selectFrom(entityClass, alias, false);
        }

        /**
         * Creates a SELECT FROM CQL builder for the specified entity class with sub-entity option.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.selectFrom(Order.class, true)
         *                 .where(Filters.gt("orderDate", yesterday))
         *                 .build().query();
         * // Will select from Order and its related entity tables with automatic joins
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties if true, properties of sub-entities will be included
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final boolean includeSubEntityProperties) {
            return selectFrom(entityClass, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT FROM CQL builder with table alias and sub-entity option.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.selectFrom(Order.class, "o", true)
         *                 .where(Filters.eq("o.status", "PENDING"))
         *                 .build().query();
         * // Selects from Order with alias 'o' and includes sub-entity properties
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @param includeSubEntityProperties if true, properties of sub-entities will be included
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final boolean includeSubEntityProperties) {
            return selectFrom(entityClass, alias, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT FROM CQL builder with excluded properties.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.selectFrom(Account.class, Set.of("password", "securityAnswer"))
         *                 .where(Filters.eq("email", "john@example.com"))
         *                 .build().query();
         * // Selects all Account properties except password and securityAnswer
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, false, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM CQL builder with table alias and excluded properties.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.selectFrom(Account.class, "acc", Set.of("password"))
         *                 .join("ORDER o", Filters.eq("acc.ID", "o.ACCOUNT_ID"))
         *                 .build().query();
         * // Selects Account properties with alias 'acc', excluding password
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, alias, false, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM CQL builder with sub-entity option and excluded properties.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.selectFrom(Order.class, true, Set.of("internalNotes", "auditLog"))
         *                 .where(Filters.between("orderDate", startDate, endDate))
         *                 .build().query();
         * // Selects Order and sub-entity properties, excluding internalNotes and auditLog
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties if true, properties of sub-entities will be included
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final boolean includeSubEntityProperties, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, QueryUtil.getTableAlias(entityClass), includeSubEntityProperties, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM CQL builder with all options.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.selectFrom(Order.class, "ord", true, Set.of("deletedFlag"))
         *                 .where(Filters.and(
         *                     Filters.eq("ord.status", "SHIPPED"),
         *                     Filters.gt("ord.amount", 100)
         *                 ))
         *                 .build().query();
         * // Comprehensive SELECT with alias, sub-entities, and exclusions
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @param includeSubEntityProperties if true, properties of sub-entities will be included
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final boolean includeSubEntityProperties,
                final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            if (hasSubEntityToInclude(entityClass, includeSubEntityProperties)) {
                final List<String> selectTableNames = getSelectTableNames(entityClass, alias, excludedPropNames, NamingPolicy.SCREAMING_SNAKE_CASE);
                //noinspection ConstantValue
                return select(entityClass, includeSubEntityProperties, excludedPropNames).from(entityClass, selectTableNames);
            }

            return select(entityClass, includeSubEntityProperties, excludedPropNames).from(entityClass, alias);
        }

        /**
         * Creates a COUNT(*) CQL builder for the specified table.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.count("ACCOUNT").where(Filters.eq("STATUS", "ACTIVE")).build().query();
         * // Output: SELECT count(*) FROM ACCOUNT WHERE STATUS = :STATUS
         * 
         * String cql2 = NAC.count("ORDER").where(Filters.gt("AMOUNT", 100)).build().query();
         * // Output: SELECT count(*) FROM ORDER WHERE AMOUNT > :AMOUNT
         * }</pre>
         * 
         * @param tableName the name of the table to count rows from
         * @return a new CqlBuilder instance configured for COUNT operation
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder count(final String tableName) {
            N.checkArgNotEmpty(tableName, SELECTION_PART_MSG);

            return select(COUNT_ALL_LIST).from(tableName);
        }

        /**
         * Creates a COUNT(*) CQL builder for the specified entity class.
         * The table name will be derived from the entity class name or @Table annotation.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NAC.count(Account.class)
         *                 .where(Filters.and(
         *                     Filters.eq("status", "ACTIVE"),
         *                     Filters.gt("balance", 0)
         *                 ))
         *                 .build().query();
         * // Output: SELECT count(*) FROM ACCOUNT WHERE STATUS = :status AND BALANCE > :balance
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for COUNT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder count(final Class<?> entityClass) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            return select(COUNT_ALL_LIST).from(entityClass);
        }

        /**
         * Parses a condition into CQL using the entity class for property mapping.
         * This method is useful for generating just the CQL fragment for a condition.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Condition cond = Filters.and(
         *     Filters.eq("status", "ACTIVE"),
         *     Filters.gt("balance", 1000),
         *     Filters.like("lastName", "Smith%")
         * );
         * String cql = NAC.parse(cond, Account.class).build().query();
         * // Output: STATUS = :status AND BALANCE > :balance AND LAST_NAME LIKE :lastName
         * }</pre>
         * 
         * @param cond the condition to parse
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance containing the parsed condition
         * @throws IllegalArgumentException if cond is null
         */
        public static CqlBuilder parse(final Condition cond, final Class<?> entityClass) {
            N.checkArgNotNull(cond, "cond");

            final CqlBuilder instance = createInstance();

            instance.setEntityClass(entityClass);
            instance._op = OperationType.QUERY;
            instance._isForConditionOnly = true;
            instance.append(cond);

            return instance;
        }
    }

    /**
     * Named CQL builder with lower camel case field/column naming strategy.
     * This builder generates CQL with named parameters (e.g., :paramName) and preserves
     * property names in camelCase format.
     * 
     * <p>The NLC builder is ideal for applications that use camelCase naming conventions
     * in their Java code and want to maintain this convention in their CQL queries.
     * Named parameters make the generated CQL more readable and easier to debug.</p>
     * 
     * <p>Key features:</p>
     * <ul>
     *   <li>Generates named parameters instead of positional parameters (?)</li>
     *   <li>Preserves camelCase property names without conversion</li>
     *   <li>Supports all standard CQL operations (SELECT, INSERT, UPDATE, DELETE)</li>
     *   <li>Integrates with entity classes using annotations</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Simple SELECT with named parameters
     * N.println(NLC.select("firstName", "lastName").from("account").where(Filters.eq("id", 1)).build().query());
     * // Output: SELECT firstName, lastName FROM account WHERE id = :id
     * 
     * // INSERT with entity
     * Account account = new Account();
     * account.setFirstName("John");
     * account.setLastName("Doe");
     * String cql = NLC.insert(account).into("account").build().query();
     * // Output: INSERT INTO account (firstName, lastName) VALUES (:firstName, :lastName)
     * }</pre>
     */
    public static class NLC extends CqlBuilder {

        NLC() {
            super(NamingPolicy.CAMEL_CASE, SQLPolicy.NAMED_SQL);
        }

        /**
         * Indicates whether this builder generates named CQL parameters.
         * Named parameters use the format :parameterName instead of positional ? placeholders.
         * 
         * @return always returns {@code true} for NLC builders
         */
        @Override
        protected boolean isNamedSql() {
            return true;
        }

        /**
         * Creates a new instance of NLC builder.
         * This factory method is used internally to create new builder instances.
         * 
         * @return a new NLC instance with default configuration
         */
        protected static NLC createInstance() {
            return new NLC();
        }

        /**
         * Creates an INSERT CQL builder for a single column expression.
         * This method is useful when inserting data into a single column or when using CQL expressions.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NLC.insert("firstName").into("account").build().query();
         * // Output: INSERT INTO account (firstName) VALUES (:firstName)
         * }</pre>
         * 
         * @param expr the column name or expression to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if expr is null or empty
         */
        public static CqlBuilder insert(final String expr) {
            N.checkArgNotEmpty(expr, INSERTION_PART_MSG);

            return insert(N.asArray(expr));
        }

        /**
         * Creates an INSERT CQL builder for the specified property or column names.
         * Property names will be preserved in camelCase format without any naming conversion.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NLC.insert("firstName", "lastName", "email")
         *                 .into("account")
         *                 .build().query();
         * // Output: INSERT INTO account (firstName, lastName, email) VALUES (:firstName, :lastName, :email)
         * }</pre>
         * 
         * @param propOrColumnNames the property or column names to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder insert(final String... propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._propOrColumnNames = Array.asList(propOrColumnNames);

            return instance;
        }

        /**
         * Creates an INSERT CQL builder for the specified collection of property or column names.
         * This method is useful when the column names are dynamically determined at runtime.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("firstName", "lastName", "email");
         * String cql = NLC.insert(columns).into("account").build().query();
         * // Output: INSERT INTO account (firstName, lastName, email) VALUES (:firstName, :lastName, :email)
         * }</pre>
         * 
         * @param propOrColumnNames the collection of property or column names to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder insert(final Collection<String> propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._propOrColumnNames = propOrColumnNames;

            return instance;
        }

        /**
         * Creates an INSERT CQL builder for the specified property-value map.
         * The map keys represent column names and values represent the data to insert.
         * This method allows direct specification of both columns and their values.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Object> data = new HashMap<>();
         * data.put("firstName", "John");
         * data.put("lastName", "Doe");
         * data.put("age", 30);
         * String cql = NLC.insert(data).into("account").build().query();
         * // Output: INSERT INTO account (firstName, lastName, age) VALUES (:firstName, :lastName, :age)
         * }</pre>
         * 
         * @param props map of property names to values
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if props is null or empty
         */
        public static CqlBuilder insert(final Map<String, Object> props) {
            N.checkArgNotEmpty(props, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance._props = props;

            return instance;
        }

        /**
         * Creates an INSERT CQL builder for the specified entity object.
         * The entity's properties will be extracted using reflection and used for the INSERT statement.
         * Properties marked with @ReadOnly, @ReadOnlyId, or @Transient annotations will be automatically excluded.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Account account = new Account();
         * account.setFirstName("John");
         * account.setLastName("Doe");
         * account.setEmail("john.doe@example.com");
         * String cql = NLC.insert(account).into("account").build().query();
         * // Output: INSERT INTO account (firstName, lastName, email) VALUES (:firstName, :lastName, :email)
         * }</pre>
         * 
         * @param entity the entity object to insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entity is null
         */
        public static CqlBuilder insert(final Object entity) {
            return insert(entity, null);
        }

        /**
         * Creates an INSERT CQL builder for the specified entity object with excluded properties.
         * This method allows fine-grained control over which properties are included in the INSERT statement.
         * Properties in the excluded set, as well as those marked with @ReadOnly, @ReadOnlyId, or @Transient, will be omitted.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Account account = new Account();
         * account.setFirstName("John");
         * account.setLastName("Doe");
         * account.setCreatedTime(new Date());
         * String cql = NLC.insert(account, Set.of("createdTime"))
         *                 .into("account")
         *                 .build().query();
         * // Output: INSERT INTO account (firstName, lastName) VALUES (:firstName, :lastName)
         * }</pre>
         * 
         * @param entity the entity object to insert
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entity is null
         */
        public static CqlBuilder insert(final Object entity, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entity, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance.setEntityClass(entity.getClass());

            parseInsertEntity(instance, entity, excludedPropNames);

            return instance;
        }

        /**
         * Creates an INSERT CQL builder for the specified entity class.
         * All insertable properties of the class will be included in the INSERT statement.
         * This method is useful when you want to prepare an INSERT template based on the entity structure.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NLC.insert(Account.class).into("account").build().query();
         * // Output: INSERT INTO account (firstName, lastName, email, age) VALUES (:firstName, :lastName, :email, :age)
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insert(final Class<?> entityClass) {
            return insert(entityClass, null);
        }

        /**
         * Creates an INSERT CQL builder for the specified entity class with excluded properties.
         * This method generates an INSERT template based on the entity class structure,
         * excluding specified properties and those marked with restrictive annotations.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NLC.insert(Account.class, Set.of("id", "createdTime"))
         *                 .into("account")
         *                 .build().query();
         * // Output: INSERT INTO account (firstName, lastName, email, age) VALUES (:firstName, :lastName, :email, :age)
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance configured for INSERT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        @SuppressWarnings("deprecation")
        public static CqlBuilder insert(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = QueryUtil.getInsertPropNames(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates an INSERT INTO CQL builder for the specified entity class.
         * This is a convenience method that combines insert() and into() operations.
         * The table name will be derived from the entity class name or @Table annotation.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NLC.insertInto(Account.class).build().query();
         * // Output: INSERT INTO account (firstName, lastName, email, age) VALUES (:firstName, :lastName, :email, :age)
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for INSERT operation with table name set
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insertInto(final Class<?> entityClass) {
            return insertInto(entityClass, null);
        }

        /**
         * Creates an INSERT INTO CQL builder for the specified entity class with excluded properties.
         * This convenience method combines insert() and into() operations while allowing property exclusion.
         * The table name will be derived from the entity class name or @Table annotation.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NLC.insertInto(Account.class, Set.of("id", "version"))
         *                 .build().query();
         * // Output: INSERT INTO account (firstName, lastName, email, age) VALUES (:firstName, :lastName, :email, :age)
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance configured for INSERT operation with table name set
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder insertInto(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return insert(entityClass, excludedPropNames).into(entityClass);
        }

        /**
         * Creates a batch INSERT CQL builder for MyCQL-style batch inserts.
         * This method generates a single INSERT statement with multiple value rows,
         * which is more efficient than executing multiple individual INSERT statements.
         * Each entity in the collection will have its own set of named parameters with numeric suffixes.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Account account1 = new Account("John", "Doe");
         * Account account2 = new Account("Jane", "Smith");
         * Account account3 = new Account("Bob", "Johnson");
         * List<Account> accounts = Arrays.asList(account1, account2, account3);
         * 
         * String cql = NLC.batchInsert(accounts).into("account").build().query();
         * // Output: INSERT INTO account (firstName, lastName) VALUES
         * //         (:firstName_1, :lastName_1),
         * //         (:firstName_2, :lastName_2),
         * //         (:firstName_3, :lastName_3)
         * }</pre>
         *
         * @param propsList collection of entities or property maps to insert
         * @return a new CqlBuilder instance configured for batch INSERT operation
         * @throws IllegalArgumentException if propsList is null or empty
         * <p><b>Note:</b> This is a beta feature and may change in future versions</p>
         */
        @Beta
        public static CqlBuilder batchInsert(final Collection<?> propsList) {
            N.checkArgNotEmpty(propsList, INSERTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.ADD;
            final Optional<?> first = N.firstNonNull(propsList);

            if (first.isPresent() && Beans.isBeanClass(first.get().getClass())) {
                instance.setEntityClass(first.get().getClass());
            }

            instance._propsList = toInsertPropsList(propsList);

            return instance;
        }

        /**
         * Creates an UPDATE CQL builder for the specified table.
         * This method starts building an UPDATE statement for the given table name.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NLC.update("account")
         *                 .set("status", "active")
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: UPDATE account SET status = :status WHERE id = :id
         * }</pre>
         * 
         * @param tableName the name of the table to update
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder update(final String tableName) {
            N.checkArgNotEmpty(tableName, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;

            return instance;
        }

        /**
         * Creates an UPDATE CQL builder for the specified table with entity class mapping.
         * The entity class provides property-to-column mapping information for the UPDATE statement.
         * This is useful when you want to update a table using entity property names.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NLC.update("account", Account.class)
         *                 .set("firstName", "John")
         *                 .set("lastName", "Doe")
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: UPDATE account SET firstName = :firstName, lastName = :lastName WHERE id = :id
         * }</pre>
         * 
         * @param tableName the name of the table to update
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if tableName is null/empty or entityClass is null
         */
        public static CqlBuilder update(final String tableName, final Class<?> entityClass) {
            N.checkArgNotEmpty(tableName, UPDATE_PART_MSG);
            N.checkArgNotNull(entityClass, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance._tableName = tableName;
            instance.setEntityClass(entityClass);

            return instance;
        }

        /**
         * Creates an UPDATE CQL builder for the specified entity class.
         * The table name will be derived from the entity class name or @Table annotation.
         * All updatable properties (not marked with @NonUpdatable) will be available for updating.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NLC.update(Account.class)
         *                 .set("status", "active")
         *                 .set("lastLoginTime", new Date())
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: UPDATE account SET status = :status, lastLoginTime = :lastLoginTime WHERE id = :id
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder update(final Class<?> entityClass) {
            return update(entityClass, null);
        }

        /**
         * Creates an UPDATE CQL builder for the specified entity class with excluded properties.
         * The table name will be derived from the entity class name or @Table annotation.
         * Properties marked with @NonUpdatable or in the excluded set will be omitted from updates.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NLC.update(Account.class, Set.of("createdTime", "createdBy"))
         *                 .set("status", "active")
         *                 .set("modifiedTime", new Date())
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: UPDATE account SET status = :status, modifiedTime = :modifiedTime WHERE id = :id
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the update
         * @return a new CqlBuilder instance configured for UPDATE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        @SuppressWarnings("deprecation")
        public static CqlBuilder update(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, UPDATE_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.UPDATE;
            instance.setEntityClass(entityClass);
            instance._tableName = getTableName(entityClass, instance._namingPolicy);
            instance._propOrColumnNames = QueryUtil.getUpdatePropNames(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for a single column or expression.
         *
         * <p>This method initializes a DELETE statement for one column. The column name will be
         * converted according to the NO_CHANGE naming policy. This is useful for deleting
         * specific columns from a row rather than the entire row.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NLC.delete("firstName")
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE firstName FROM account WHERE id = :id
         * }</pre>
         *
         * @param expr the column name or expression to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if expr is null or empty
         */
        public static CqlBuilder delete(final String expr) {
            N.checkArgNotEmpty(expr, DELETION_PART_MSG);

            return delete(N.asArray(expr));
        }

        /**
         * Creates a DELETE CQL builder for multiple columns.
         *
         * <p>This method initializes a DELETE statement for multiple columns. All column names
         * will be converted according to the NO_CHANGE naming policy. This allows selective
         * deletion of specific columns from rows.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NLC.delete("firstName", "lastName", "email")
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE firstName, lastName, email FROM account WHERE id = :id
         * }</pre>
         *
         * @param columnNames the column names to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if columnNames is null or empty
         */
        public static CqlBuilder delete(final String... columnNames) {
            N.checkArgNotEmpty(columnNames, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._propOrColumnNames = Array.asList(columnNames);

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for a collection of columns.
         *
         * <p>This method is useful when column names are determined dynamically. The collection
         * can contain property names that will be converted to column names according to the
         * NO_CHANGE naming policy.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("firstName", "lastName", "email");
         * String cql = NLC.delete(columns)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE firstName, lastName, email FROM account WHERE id = :id
         * }</pre>
         *
         * @param columnNames the collection of column names to delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if columnNames is null or empty
         */
        public static CqlBuilder delete(final Collection<String> columnNames) {
            N.checkArgNotEmpty(columnNames, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._propOrColumnNames = columnNames;

            return instance;
        }

        /**
         * Creates a DELETE CQL builder for an entity class.
         *
         * <p>This method generates a DELETE statement for all deletable properties of the class.
         * Properties marked with @ReadOnly, @ReadOnlyId, or @Transient are automatically excluded.
         * This is useful for creating templates for partial row deletion.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NLC.delete(Account.class)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE firstName, lastName, email, status FROM account WHERE id = :id
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder delete(final Class<?> entityClass) {
            return delete(entityClass, null);
        }

        /**
         * Creates a DELETE CQL builder for an entity class, excluding specified properties.
         *
         * <p>This method generates a DELETE statement excluding both annotation-based exclusions
         * and the specified properties. Useful for selective column deletion where certain
         * fields should be preserved.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("email", "createdDate");
         * String cql = NLC.delete(Account.class, excluded)
         *                  .from("account")
         *                  .where(Filters.eq("id", 1))
         *                  .build().query();
         * // Output: DELETE firstName, lastName, status FROM account WHERE id = :id
         * }</pre>
         *
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from the delete
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder delete(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = getDeletePropNamesByClass(entityClass, excludedPropNames);

            return instance;
        }

        /**
         * Creates a DELETE FROM CQL builder for the specified table.
         * This method starts building a DELETE statement for the given table name.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NLC.deleteFrom("account")
         *                 .where(Filters.eq("status", "inactive"))
         *                 .build().query();
         * // Output: DELETE FROM account WHERE status = :status
         * }</pre>
         * 
         * @param tableName the name of the table to delete from
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder deleteFrom(final String tableName) {
            N.checkArgNotEmpty(tableName, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;

            return instance;
        }

        /**
         * Creates a DELETE FROM CQL builder for the specified table with entity class mapping.
         * The entity class provides property-to-column mapping information for WHERE conditions.
         * This is useful when you want to use entity property names in the WHERE clause.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NLC.deleteFrom("account", Account.class)
         *                 .where(Filters.and(
         *                     Filters.eq("status", "inactive"),
         *                     Filters.lt("lastLoginTime", oneYearAgo)
         *                 ))
         *                 .build().query();
         * // Output: DELETE FROM account WHERE status = :status AND lastLoginTime < :lastLoginTime
         * }</pre>
         * 
         * @param tableName the name of the table to delete from
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if tableName is null/empty or entityClass is null
         */
        public static CqlBuilder deleteFrom(final String tableName, final Class<?> entityClass) {
            N.checkArgNotEmpty(tableName, DELETION_PART_MSG);
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance._tableName = tableName;
            instance.setEntityClass(entityClass);

            return instance;
        }

        /**
         * Creates a DELETE FROM CQL builder for the specified entity class.
         * The table name will be derived from the entity class name or @Table annotation.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NLC.deleteFrom(Account.class)
         *                 .where(Filters.eq("status", "inactive"))
         *                 .build().query();
         * // Output: DELETE FROM account WHERE status = :status
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for DELETE operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder deleteFrom(final Class<?> entityClass) {
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.DELETE;
            instance.setEntityClass(entityClass);
            instance._tableName = getTableName(entityClass, instance._namingPolicy);

            return instance;
        }

        /**
         * Creates a SELECT CQL builder with a single select expression.
         * The expression can be a column name, function call, or any valid CQL expression.
         * This method is useful for simple queries or when using CQL functions.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NLC.select("COUNT(*)").from("account").build().query();
         * // Output: SELECT count(*) FROM account
         * 
         * String cql2 = NLC.select("MAX(balance)").from("account").where(Filters.eq("status", "active")).build().query();
         * // Output: SELECT MAX(balance) FROM account WHERE status = :status
         * }</pre>
         * 
         * @param selectPart the select expression
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if selectPart is null or empty
         */
        public static CqlBuilder select(final String selectPart) {
            N.checkArgNotEmpty(selectPart, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = Array.asList(selectPart);
            return instance;
        }

        /**
         * Creates a SELECT CQL builder for the specified property or column names.
         * Property names will be preserved in camelCase format without any conversion.
         * This is the most common way to start building a SELECT query.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NLC.select("firstName", "lastName", "email")
         *                 .from("account")
         *                 .where(Filters.eq("status", "active"))
         *                 .build().query();
         * // Output: SELECT firstName, lastName, email FROM account WHERE status = :status
         * }</pre>
         * 
         * @param propOrColumnNames the property or column names to select
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder select(final String... propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = Array.asList(propOrColumnNames);

            return instance;
        }

        /**
         * Creates a SELECT CQL builder for the specified collection of property or column names.
         * This method is useful when the columns to select are determined dynamically at runtime.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<String> columns = Arrays.asList("firstName", "lastName", "email");
         * String cql = NLC.select(columns)
         *                 .from("account")
         *                 .orderBy("lastName")
         *                 .build().query();
         * // Output: SELECT firstName, lastName, email FROM account ORDER BY lastName
         * }</pre>
         * 
         * @param propOrColumnNames the collection of property or column names to select
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if propOrColumnNames is null or empty
         */
        public static CqlBuilder select(final Collection<String> propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = propOrColumnNames;

            return instance;
        }

        /**
         * Creates a SELECT CQL builder with column aliases.
         * The map keys are property/column names and values are their aliases.
         * This method allows you to rename columns in the result set.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, String> columnAliases = new HashMap<>();
         * columnAliases.put("firstName", "fname");
         * columnAliases.put("lastName", "lname");
         * columnAliases.put("emailAddress", "email");
         * 
         * String cql = NLC.select(columnAliases)
         *                 .from("account")
         *                 .build().query();
         * // Output: SELECT firstName AS fname, lastName AS lname, emailAddress AS email FROM account
         * }</pre>
         * 
         * @param propOrColumnNameAliases map of property/column names to their aliases
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if propOrColumnNameAliases is null or empty
         */
        public static CqlBuilder select(final Map<String, String> propOrColumnNameAliases) {
            N.checkArgNotEmpty(propOrColumnNameAliases, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNameAliases = propOrColumnNameAliases;

            return instance;
        }

        /**
         * Creates a SELECT CQL builder for all properties of the specified entity class.
         * This method selects all properties that are not marked with @Transient annotation.
         * Sub-entity properties are not included by default.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NLC.select(Account.class)
         *                 .from("account")
         *                 .where(Filters.eq("status", "active"))
         *                 .build().query();
         * // Output: SELECT id, firstName, lastName, email, status, balance FROM account WHERE status = :status
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass) {
            return select(entityClass, false);
        }

        /**
         * Creates a SELECT CQL builder for properties of the specified entity class.
         * This method allows control over whether properties from sub-entities should be included.
         * Sub-entities are typically used for one-to-one or many-to-one relationships.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Without sub-entity properties
         * String cql1 = NLC.select(Order.class, false).from("orders").build().query();
         * // Output: SELECT id, orderNumber, amount, status FROM orders
         * 
         * // With sub-entity properties (if Order has an Account sub-entity)
         * String cql2 = NLC.select(Order.class, true).from("orders").build().query();
         * // Output: SELECT id, orderNumber, amount, status, account.id, account.firstName, account.lastName FROM orders
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties if true, properties of sub-entities will be included
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass, final boolean includeSubEntityProperties) {
            return select(entityClass, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT CQL builder for properties of the specified entity class with exclusions.
         * This method selects all properties except those specified in the excluded set.
         * Properties marked with @Transient are always excluded.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = Set.of("password", "secretKey");
         * String cql = NLC.select(Account.class, excluded)
         *                 .from("account")
         *                 .build().query();
         * // Output: SELECT id, firstName, lastName, email, status, balance FROM account
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder select(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return select(entityClass, false, excludedPropNames);
        }

        /**
         * Creates a SELECT CQL builder for properties of the specified entity class with full control.
         * This method provides complete control over property selection, sub-entity inclusion, and exclusions.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = Set.of("password", "internalNotes");
         * String cql = NLC.select(Account.class, true, excluded)
         *                 .from("account")
         *                 .build().query();
         * // Output: SELECT id, firstName, lastName, email, status, balance, address.street, address.city FROM account
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties if true, properties of sub-entities will be included
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT operation
         * @throws IllegalArgumentException if entityClass is null
         */
        @SuppressWarnings("deprecation")
        public static CqlBuilder select(final Class<?> entityClass, final boolean includeSubEntityProperties, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            final CqlBuilder instance = createInstance();

            instance._op = OperationType.QUERY;
            instance.setEntityClass(entityClass);
            instance._propOrColumnNames = QueryUtil.getSelectPropNames(entityClass, includeSubEntityProperties, excludedPropNames);

            return instance;
        }

        /**
         * Creates a SELECT FROM CQL builder for the specified entity class.
         * This is a convenience method that combines SELECT and FROM operations.
         * The table name is derived from the entity class name or @Table annotation.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NLC.selectFrom(Account.class)
         *                 .where(Filters.eq("status", "active"))
         *                 .orderBy("lastName")
         *                 .build().query();
         * // Output: SELECT id, firstName, lastName, email, status, balance FROM account WHERE status = :status ORDER BY lastName
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, false);
        }

        /**
         * Creates a SELECT FROM CQL builder for the specified entity class with table alias.
         * The alias is used to qualify column names in complex queries with joins.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NLC.selectFrom(Account.class, "a")
         *                 .innerJoin("orders o", Filters.eq("a.id", "o.accountId"))
         *                 .where(Filters.eq("a.status", "active"))
         *                 .build().query();
         * // Output: SELECT a.id, a.firstName, a.lastName, a.email, a.status, a.balance 
         * //         FROM account a 
         * //         INNER JOIN orders o ON a.id = o.accountId 
         * //         WHERE a.status = :status
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias) {
            return selectFrom(entityClass, alias, false);
        }

        /**
         * Creates a SELECT FROM CQL builder for the specified entity class with sub-entity option.
         * When sub-entity properties are included, the appropriate JOIN clauses are automatically generated.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Assuming Order has an Account sub-entity
         * String cql = NLC.selectFrom(Order.class, true)
         *                 .where(Filters.gt("amount", 100))
         *                 .build().query();
         * // Output: SELECT o.id, o.orderNumber, o.amount, o.status, 
         * //                a.id AS "account.id", a.firstName AS "account.firstName", a.lastName AS "account.lastName"
         * //         FROM orders o 
         * //         LEFT JOIN account a ON o.accountId = a.id
         * //         WHERE o.amount > :amount
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties if true, properties of sub-entities will be included
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final boolean includeSubEntityProperties) {
            return selectFrom(entityClass, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT FROM CQL builder with table alias and sub-entity option.
         * This method provides control over both table aliasing and sub-entity inclusion.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NLC.selectFrom(Order.class, "ord", true)
         *                 .where(Filters.between("orderDate", startDate, endDate))
         *                 .build().query();
         * // Output: SELECT ord.id, ord.orderNumber, ord.amount, ord.status,
         * //                acc.id AS "account.id", acc.firstName AS "account.firstName"
         * //         FROM orders ord
         * //         LEFT JOIN account acc ON ord.accountId = acc.id
         * //         WHERE orderDate BETWEEN :minOrderDate AND :maxOrderDate
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @param includeSubEntityProperties if true, properties of sub-entities will be included
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final boolean includeSubEntityProperties) {
            return selectFrom(entityClass, alias, includeSubEntityProperties, null);
        }

        /**
         * Creates a SELECT FROM CQL builder with excluded properties.
         * This is a convenience method for common use cases where certain properties should be excluded.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> sensitiveFields = Set.of("password", "ssn", "creditCardNumber");
         * String cql = NLC.selectFrom(Account.class, sensitiveFields)
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: SELECT id, firstName, lastName, email, status, balance FROM account WHERE id = :id
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, false, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM CQL builder with table alias and excluded properties.
         * This method combines table aliasing with property exclusion for complex queries.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = Set.of("password", "securityAnswer");
         * String cql = NLC.selectFrom(Account.class, "acc", excluded)
         *                 .innerJoin("orders o", Filters.eq("acc.id", "o.accountId"))
         *                 .where(Filters.gt("o.amount", 1000))
         *                 .build().query();
         * // Output: SELECT acc.id, acc.firstName, acc.lastName, acc.email, acc.status, acc.balance
         * //         FROM account acc
         * //         INNER JOIN orders o ON acc.id = o.accountId
         * //         WHERE o.amount > :amount
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, alias, false, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM CQL builder with sub-entity option and excluded properties.
         * This method allows inclusion of sub-entity properties while excluding specific fields.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = Set.of("account.password", "internalNotes");
         * String cql = NLC.selectFrom(Order.class, true, excluded)
         *                 .where(Filters.eq("status", "COMPLETED"))
         *                 .build().query();
         * // Output: SELECT o.id, o.orderNumber, o.amount, o.status,
         * //                a.id AS "account.id", a.firstName AS "account.firstName", a.lastName AS "account.lastName"
         * //         FROM orders o
         * //         LEFT JOIN account a ON o.accountId = a.id
         * //         WHERE o.status = :status
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param includeSubEntityProperties if true, properties of sub-entities will be included
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final boolean includeSubEntityProperties, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, QueryUtil.getTableAlias(entityClass), includeSubEntityProperties, excludedPropNames);
        }

        /**
         * Creates a SELECT FROM CQL builder with full control over all options.
         * This is the most comprehensive method for creating SELECT queries with entity classes.
         * When includeSubEntityProperties is true, appropriate JOIN clauses are automatically generated.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = Set.of("password", "account.internalId");
         * String cql = NLC.selectFrom(Order.class, "o", true, excluded)
         *                 .where(Filters.and(
         *                     Filters.eq("o.status", "PENDING"),
         *                     Filters.gt("o.amount", 500)
         *                 ))
         *                 .orderBy("o.orderDate DESC")
         *                 .build().query();
         * // Output: SELECT o.id, o.orderNumber, o.amount, o.status, o.orderDate,
         * //                a.id AS "account.id", a.firstName AS "account.firstName", a.lastName AS "account.lastName"
         * //         FROM orders o
         * //         LEFT JOIN account a ON o.accountId = a.id
         * //         WHERE o.status = :status AND o.amount > :amount
         * //         ORDER BY o.orderDate DESC
         * }</pre>
         * 
         * @param entityClass the entity class
         * @param alias the table alias
         * @param includeSubEntityProperties if true, properties of sub-entities will be included
         * @param excludedPropNames properties to exclude from selection
         * @return a new CqlBuilder instance configured for SELECT FROM operation
         * @throws IllegalArgumentException if entityClass is null
         */
        public static CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final boolean includeSubEntityProperties,
                final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            if (hasSubEntityToInclude(entityClass, includeSubEntityProperties)) {
                final List<String> selectTableNames = getSelectTableNames(entityClass, alias, excludedPropNames, NamingPolicy.CAMEL_CASE);
                return select(entityClass, includeSubEntityProperties, excludedPropNames).from(entityClass, selectTableNames);
            }

            return select(entityClass, includeSubEntityProperties, excludedPropNames).from(entityClass, alias);
        }

        /**
         * Creates a COUNT(*) CQL builder for the specified table.
         * This is a convenience method for creating simple count queries.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NLC.count("account")
         *                 .where(Filters.eq("status", "active"))
         *                 .build().query();
         * // Output: SELECT count(*) FROM account WHERE status = :status
         * 
         * // Can also be used with joins
         * String cql2 = NLC.count("account a")
         *                  .innerJoin("orders o", Filters.eq("a.id", "o.accountId"))
         *                  .where(Filters.gt("o.amount", 1000))
         *                  .build().query();
         * // Output: SELECT count(*) FROM account a INNER JOIN orders o ON a.id = o.accountId WHERE o.amount > :amount
         * }</pre>
         * 
         * @param tableName the name of the table to count rows from
         * @return a new CqlBuilder instance configured for COUNT operation
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public static CqlBuilder count(final String tableName) {
            N.checkArgNotEmpty(tableName, SELECTION_PART_MSG);

            return select(COUNT_ALL_LIST).from(tableName);
        }

        /**
         * Creates a COUNT(*) CQL builder for the specified entity class.
         * The table name will be derived from the entity class name or @Table annotation.
         * This is a convenience method for counting rows in entity-mapped tables.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = NLC.count(Account.class)
         *                 .where(Filters.eq("status", "active"))
         *                 .build().query();
         * // Output: SELECT count(*) FROM account WHERE status = :status
         * 
         * // Can use entity properties in WHERE clause
         * String cql2 = NLC.count(Order.class)
         *                  .where(Filters.and(
         *                      Filters.gt("amount", 100),
         *                      Filters.between("orderDate", startDate, endDate)
         *                  ))
         *                  .build().query();
         * // Output: SELECT count(*) FROM orders WHERE amount > :amount AND orderDate BETWEEN :minOrderDate AND :maxOrderDate
         * }</pre>
         * 
         * @param entityClass the entity class
         * @return a new CqlBuilder instance configured for COUNT operation
         */
        public static CqlBuilder count(final Class<?> entityClass) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            return select(COUNT_ALL_LIST).from(entityClass);
        }

        /**
         * Parses a condition into CQL using the entity class for property mapping.
         * This method is useful for generating just the CQL fragment for a condition.
         * 
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Condition cond = Filters.and(
         *     Filters.eq("status", "ACTIVE"),
         *     Filters.gt("balance", 1000),
         *     Filters.like("lastName", "Smith%")
         * );
         * String cql = NLC.parse(cond, Account.class).build().query();
         * // Output: status = :status AND balance > :balance AND lastName LIKE :lastName
         * }</pre>
         * 
         * @param cond the condition to parse
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance containing the parsed condition
         * @throws IllegalArgumentException if cond is null
         */
        public static CqlBuilder parse(final Condition cond, final Class<?> entityClass) {
            N.checkArgNotNull(cond, "cond");

            final CqlBuilder instance = createInstance();

            instance.setEntityClass(entityClass);
            instance._op = OperationType.QUERY;
            instance._isForConditionOnly = true;
            instance.append(cond);

            return instance;
        }
    }
}
