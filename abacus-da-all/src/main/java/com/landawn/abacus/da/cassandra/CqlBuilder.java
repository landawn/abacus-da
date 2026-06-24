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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.landawn.abacus.annotation.Beta;
import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.query.AbstractQueryBuilder;
import com.landawn.abacus.query.QueryUtil;
import com.landawn.abacus.query.SqlDialect;
import com.landawn.abacus.query.SqlDialect.SQLPolicy;
import com.landawn.abacus.query.condition.Between;
import com.landawn.abacus.query.condition.Binary;
import com.landawn.abacus.query.condition.Cell;
import com.landawn.abacus.query.condition.ComposableCell;
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
 * <p>This builder provides a type-safe, fluent interface for creating CQL queries, inserts, updates,
 * and deletes. It handles proper parameterization, column name formatting according to naming policies, and
 * supports advanced Cassandra features like TTL, timestamps, lightweight transactions, and filtering.</p>
 *
 * <p>Instances are not thread-safe; create a new builder per thread or per query. Always call {@code build()}
 * to finalize construction and release internal resources.</p>
 *
 * <h2>Key Features</h2>
 * <ul>
 *   <li><b>Fluent API Design:</b> Method chaining for readable query construction</li>
 *   <li><b>Type Safety:</b> Compile-time checking with parameterized queries</li>
 *   <li><b>Cassandra-Specific Features:</b> TTL, TIMESTAMP, IF conditions, ALLOW FILTERING</li>
 *   <li><b>Naming Policy Support:</b> Automatic column name transformation (camelCase &lt;-&gt; snake_case)</li>
 *   <li><b>Performance Optimized:</b> Efficient string building with resource management</li>
 *   <li><b>Lightweight Transactions:</b> IF, IF EXISTS, IF NOT EXISTS support</li>
 * </ul>
 *
 * <h2>Builder Variants</h2>
 * <p>Use one of the predefined {@link Dsl} constants depending on the parameter style and naming policy you
 * want for the generated CQL. Each constant is a {@code Dsl} bound to a specific {@link SqlDialect}; call one
 * of its statement methods ({@code insert}, {@code select}, {@code update}, {@code deleteFrom}, etc.) to obtain
 * a fresh {@link CqlBuilder}.</p>
 * <ul>
 *   <li><b>Un-parameterized / raw CQL (values embedded directly):</b>
 *       {@link #SCCB} (snake_case columns), {@link #ACCB} (SCREAMING_SNAKE_CASE columns),
 *       {@link #LCCB} (camelCase columns). These are deprecated due to CQL-injection risk.</li>
 *   <li><b>Question-mark / positional parameters ({@code ?}):</b>
 *       {@link #PSB} (no name change), {@link #PSC} (snake_case columns),
 *       {@link #PAC} (SCREAMING_SNAKE_CASE columns), {@link #PLC} (camelCase columns).</li>
 *   <li><b>Named parameters ({@code :name}):</b>
 *       {@link #NSB} (no name change), {@link #NSC} (snake_case columns),
 *       {@link #NAC} (SCREAMING_SNAKE_CASE columns), {@link #NLC} (camelCase columns).</li>
 * </ul>
 *
 * <h2>Usage Examples</h2>
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
 *                       .usingTTL(3600)  // 1 hour TTL
 *                       .build().query();
 * // Result: INSERT INTO user_sessions (id, session_token, created_at) VALUES (?, ?, ?) USING TTL 3600
 *
 * // UPDATE with lightweight transaction
 * String updateCql = PSC.update("users")
 *                       .set("lastLogin")
 *                       .where(Filters.eq("id", userId))
 *                       .onlyIf(Filters.eq("status", "active"))
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
 * <h2>Cassandra-Specific Capabilities</h2>
 * <ul>
 *   <li><b>TTL (Time To Live):</b> {@code usingTTL(seconds)} for automatic data expiration</li>
 *   <li><b>Timestamps:</b> {@code usingTimestamp(timestamp)} for precise operation timing</li>
 *   <li><b>Lightweight Transactions:</b> {@code iF()}, {@code ifExists()}, {@code ifNotExists()} for consistency</li>
 *   <li><b>Allow Filtering:</b> {@code allowFiltering()} for server-side filtering (use with caution)</li>
 * </ul>
 *
 * <h2>Thread Safety</h2>
 * <p>This builder is <b>NOT thread-safe</b>. Each thread should use its own builder instance.
 * The builder maintains internal state during query construction and must not be shared across threads.
 * The {@link Dsl} constants, however, are immutable and thread-safe.</p>
 *
 * <h2>Resource Management</h2>
 * <p>The builder uses internal resources that are automatically released when {@code build()} (or {@code apply()}/{@code accept()})
 * is called. After calling any of these terminal methods, the builder cannot be reused. Always call one of them
 * to properly finalize the query and release resources.</p>
 *
 * <h2>Naming Policy</h2>
 * <p>Column names are automatically transformed according to the configured naming policy. By default,
 * Java camelCase property names are converted to Cassandra snake_case column names. Table names are
 * NOT transformed and are used as-is.</p>
 *
 * @see com.landawn.abacus.query.Filters
 * @see CassandraExecutor
 * @see ParsedCql
 */
public class CqlBuilder extends AbstractQueryBuilder<CqlBuilder> { // NOSONAR

    /**
     * Raw-CQL DSL with {@code snake_case} naming; values are inlined as CQL literals rather than parameterized.
     *
     * @deprecated {@link #PSC} or {@link #NSC} is preferred for better security and performance.
     *             Un-parameterized CQL is vulnerable to CQL injection attacks.
     */
    @Deprecated
    public static final CqlBuilder.Dsl SCCB = Dsl.forDialect(SqlDialect.builder().namingPolicy(NamingPolicy.SNAKE_CASE).sqlPolicy(SQLPolicy.RAW_SQL).build());

    /**
     * Raw-CQL DSL with {@code UPPER_CASE_WITH_UNDERSCORE} naming; values are inlined as CQL literals rather than parameterized.
     *
     * @deprecated {@link #PAC} or {@link #NAC} is preferred for better security and performance.
     *             Un-parameterized CQL is vulnerable to CQL injection attacks.
     */
    @Deprecated
    public static final CqlBuilder.Dsl ACCB = Dsl
            .forDialect(SqlDialect.builder().namingPolicy(NamingPolicy.SCREAMING_SNAKE_CASE).sqlPolicy(SQLPolicy.RAW_SQL).build());

    /**
     * Raw-CQL DSL with {@code lowerCamelCase} naming; values are inlined as CQL literals rather than parameterized.
     *
     * @deprecated {@link #PLC} or {@link #NLC} is preferred for better security and performance.
     *             Un-parameterized CQL is vulnerable to CQL injection attacks.
     */
    @Deprecated
    public static final CqlBuilder.Dsl LCCB = Dsl.forDialect(SqlDialect.builder().namingPolicy(NamingPolicy.CAMEL_CASE).sqlPolicy(SQLPolicy.RAW_SQL).build());

    /**
     * Parameterized-CQL DSL ({@code ?} placeholders) that leaves property/column names unchanged.
     */
    public static final CqlBuilder.Dsl PSB = Dsl
            .forDialect(SqlDialect.builder().namingPolicy(NamingPolicy.NO_CHANGE).sqlPolicy(SQLPolicy.PARAMETERIZED_SQL).build());

    /**
     * Parameterized-CQL DSL ({@code ?} placeholders) with {@code snake_case} naming.
     */
    public static final CqlBuilder.Dsl PSC = Dsl
            .forDialect(SqlDialect.builder().namingPolicy(NamingPolicy.SNAKE_CASE).sqlPolicy(SQLPolicy.PARAMETERIZED_SQL).build());

    /**
     * Parameterized-CQL DSL ({@code ?} placeholders) with {@code UPPER_CASE_WITH_UNDERSCORE} naming.
     */
    public static final CqlBuilder.Dsl PAC = Dsl
            .forDialect(SqlDialect.builder().namingPolicy(NamingPolicy.SCREAMING_SNAKE_CASE).sqlPolicy(SQLPolicy.PARAMETERIZED_SQL).build());

    /**
     * Parameterized-CQL DSL ({@code ?} placeholders) with {@code lowerCamelCase} naming.
     */
    public static final CqlBuilder.Dsl PLC = Dsl
            .forDialect(SqlDialect.builder().namingPolicy(NamingPolicy.CAMEL_CASE).sqlPolicy(SQLPolicy.PARAMETERIZED_SQL).build());

    /**
     * Named-CQL DSL ({@code :name} placeholders) that leaves property/column names unchanged.
     */
    public static final CqlBuilder.Dsl NSB = Dsl.forDialect(SqlDialect.builder().namingPolicy(NamingPolicy.NO_CHANGE).sqlPolicy(SQLPolicy.NAMED_SQL).build());

    /**
     * Named-CQL DSL ({@code :name} placeholders) with {@code snake_case} naming.
     */
    public static final CqlBuilder.Dsl NSC = Dsl.forDialect(SqlDialect.builder().namingPolicy(NamingPolicy.SNAKE_CASE).sqlPolicy(SQLPolicy.NAMED_SQL).build());

    /**
     * Named-CQL DSL ({@code :name} placeholders) with {@code UPPER_CASE_WITH_UNDERSCORE} naming.
     */
    public static final CqlBuilder.Dsl NAC = Dsl
            .forDialect(SqlDialect.builder().namingPolicy(NamingPolicy.SCREAMING_SNAKE_CASE).sqlPolicy(SQLPolicy.NAMED_SQL).build());

    /**
     * Named-CQL DSL ({@code :name} placeholders) with {@code lowerCamelCase} naming.
     */
    public static final CqlBuilder.Dsl NLC = Dsl.forDialect(SqlDialect.builder().namingPolicy(NamingPolicy.CAMEL_CASE).sqlPolicy(SQLPolicy.NAMED_SQL).build());

    // TODO performance goal: 80% cases (or maybe CQL.length < 1024?) can be composed in 0.1 millisecond. 0.01 millisecond will be fantastic if possible.

    protected static final Logger logger = LoggerFactory.getLogger(CqlBuilder.class);

    static final char[] _SPACE_USING_TIMESTAMP_SPACE = " USING TIMESTAMP ".toCharArray();

    static final char[] _SPACE_USING_TTL_SPACE = " USING TTL ".toCharArray();

    private static final String SPACE_USING = " USING ";

    private static final String SPACE_SET = " SET ";

    private static final String SPACE_WHERE = " WHERE ";

    private static final String SPACE_IF = " IF ";

    private static final String TTL = "TTL";

    private static final String TIMESTAMP = "TIMESTAMP";

    static final char[] _SPACE_IF_SPACE = " IF ".toCharArray();

    static final char[] _SPACE_IF_EXISTS = " IF EXISTS".toCharArray();

    static final char[] _SPACE_IF_NOT_EXISTS = " IF NOT EXISTS".toCharArray();

    static final char[] _SPACE_ALLOW_FILTERING = " ALLOW FILTERING".toCharArray();

    // True while iF(Condition) renders its condition: Cassandra's LWT IF grammar rejects parenthesized
    // members, so Junction members are rendered without parentheses in that context.
    private boolean _appendingIfClause; //NOSONAR

    /**
     * Constructs a new CqlBuilder with the specified dialect (naming policy + parameter style).
     *
     * @param sqlDialect the dialect for this builder
     */
    protected CqlBuilder(final SqlDialect sqlDialect) {
        super(sqlDialect);
    }

    /**
     * Returns a string of {@code count} question-mark ({@code ?}) placeholders separated by
     * {@code ", "}.
     *
     * <p>This utility is intended for IN clauses or VALUES lists whose parameter count is
     * known only at runtime; for {@code count == 0} an empty string is returned.</p>
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

        return QueryUtil.placeholders(count);
    }

    /**
     * Generates a string of comma-separated question mark placeholders.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * String placeholders = CqlBuilder.repeatQM(3);
     * // returns "?, ?, ?"
     *
     * String single = CqlBuilder.repeatQM(1);
     * // returns "?"
     *
     * // Edge case: zero placeholders yields an empty string
     * String none = CqlBuilder.repeatQM(0);
     * // returns ""
     *
     * // Negative count is rejected
     * CqlBuilder.repeatQM(-1);
     * // throws IllegalArgumentException
     * }</pre>
     *
     * @param count the number of placeholders to generate (must be non-negative)
     * @return a comma-separated string of {@code count} question mark placeholders
     * @throws IllegalArgumentException if {@code count} is negative
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
     * // Output: UPDATE users USING TTL 7200 SET status = ? WHERE id = ?
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
        appendUsingOption(TTL, ttl);

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
     *                 .usingTimestamp(specificTime)
     *                 .build().query();
     * // Output: INSERT INTO events (id, data) VALUES (?, ?) USING TIMESTAMP <microseconds>
     * }</pre>
     *
     * @param timestamp the timestamp as a Date object
     * @return this CqlBuilder instance for method chaining
     * @throws IllegalArgumentException if timestamp is null
     * @see #usingTimestamp(long)
     * @see #usingTimestamp(String)
     */
    public CqlBuilder usingTimestamp(final Date timestamp) {
        N.checkArgNotNull(timestamp, "timestamp");

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
     * // Output: UPDATE users USING TIMESTAMP <microseconds> SET last_login = ? WHERE id = ?
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
     * <p><b>Note:</b> The string is appended as-is into the generated CQL; no unit conversion is
     * performed here. The caller is responsible for supplying a value in microseconds (unlike the
     * {@link #usingTimestamp(long)} and {@link #usingTimestamp(Date)} overloads which convert
     * milliseconds to microseconds internally).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Caller must supply microseconds when using this overload:
     * String timestamp = String.valueOf(System.currentTimeMillis() * 1000L);
     * String cql = PSC.insert("id", "name", "createdAt")
     *                 .into("users")
     *                 .usingTimestamp(timestamp)
     *                 .build().query();
     * // Output: INSERT INTO users (id, name, created_at) VALUES (?, ?, ?) USING TIMESTAMP <microseconds>
     * }</pre>
     *
     * @param timestamp the timestamp as a string (should represent microseconds since Unix epoch)
     * @return this CqlBuilder instance for method chaining
     * @see #usingTimestamp(Date)
     * @see #usingTimestamp(long)
     */
    public CqlBuilder usingTimestamp(final String timestamp) {
        appendUsingOption(TIMESTAMP, timestamp);

        return this;
    }

    private void appendUsingOption(final String optionName, final String optionValue) {
        assertNotClosed();

        // init(true) (not init(false)): for update(entityClass) the one-shot init is what expands the
        // implicit SET clause; consuming it with false here silently dropped the SET assignments (and
        // their parameters) whenever USING was appended before where()/build().
        init(true);

        checkInsertClauseOrder();

        final String cql = _sb.toString();
        final int insertionIndex = findUsingInsertionIndex(cql);
        final int usingIndex = cql.indexOf(SPACE_USING);
        final String option = optionName + _SPACE + optionValue;

        if (usingIndex >= 0 && usingIndex < insertionIndex) {
            _sb.insert(insertionIndex, _SPACE_AND_SPACE);
            _sb.insert(insertionIndex + _SPACE_AND_SPACE.length, option);
        } else {
            _sb.insert(insertionIndex, SPACE_USING + option);
        }
    }

    private int findUsingInsertionIndex(final String cql) {
        if (_op == OperationType.UPDATE) {
            final int setIndex = cql.indexOf(SPACE_SET);

            if (setIndex >= 0) {
                return setIndex;
            }
        }

        int insertionIndex = cql.length();
        int clauseIndex = cql.indexOf(SPACE_WHERE);

        if (clauseIndex >= 0) {
            insertionIndex = Math.min(insertionIndex, clauseIndex);
        }

        // For INSERT the CQL grammar is "... VALUES (...) [IF NOT EXISTS] [USING ...]": USING goes
        // after the IF clause, so only reposition before " IF " for UPDATE/DELETE.
        if (_op != OperationType.ADD) {
            clauseIndex = cql.indexOf(SPACE_IF);

            if (clauseIndex >= 0) {
                insertionIndex = Math.min(insertionIndex, clauseIndex);
            }
        }

        return insertionIndex;
    }

    private void checkInsertClauseOrder() {
        // For an INSERT nothing has been emitted before into() is called; inserting a USING/IF clause
        // into the empty buffer would silently prepend it before the INSERT keyword.
        if (_op == OperationType.ADD && _sb.length() == 0) {
            throw new IllegalStateException("into() must be called before appending USING/IF clauses to an INSERT statement");
        }
    }

    private void assertNotClosed() {
        if (_sb == null) {
            throw new IllegalStateException("This CqlBuilder has been closed after build() was called. No further operation is supported");
        }
    }

    @Override
    protected void setParameterForRawSQL(final Object propValue) {
        // CQL escapes a single quote inside a string literal by doubling it ('O''Brien'); the SQL-style
        // backslash escaping produced by the parent implementation is a CQL syntax error.
        if (propValue instanceof final String str) {
            _sb.append('\'').append(Strings.replaceAll(str, "'", "''")).append('\'');
        } else {
            super.setParameterForRawSQL(propValue);
        }
    }

    private void appendIfClause(final char[] ifClause) {
        // For INSERT the CQL grammar is "... VALUES (...) [IF NOT EXISTS] [USING ...]": the IF clause
        // must precede any USING clause already emitted.
        if (_op == OperationType.ADD) {
            final int usingIndex = _sb.indexOf(SPACE_USING);

            if (usingIndex >= 0) {
                _sb.insert(usingIndex, new String(ifClause));
                return;
            }
        }

        _sb.append(ifClause);
    }

    /**
     * Adds a conditional IF clause to the CQL statement with the specified expression.
     *
     * <p>The IF clause enables lightweight transactions (LWT) in Cassandra by adding conditions
     * that must be met for the operation to succeed. This is particularly useful for ensuring
     * data consistency in concurrent environments.</p>
     *
     * <p>Prefer this method over {@link #iF(String)}. The {@code iF} method is kept for
     * backward compatibility because {@code if} is a Java keyword.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // INSERT with a raw expression (appended verbatim)
     * String cql = PSC.insert("id", "name")
     *                 .into("users")
     *                 .onlyIf("name = NULL")
     *                 .build().query();
     * // Output: INSERT INTO users (id, name) VALUES (?, ?) IF name = NULL
     *
     * // UPDATE with a raw expression
     * String cql = PSC.update("users")
     *                 .set("status")
     *                 .where(Filters.eq("id", 123))
     *                 .onlyIf("status = 'inactive'")
     *                 .build().query();
     * // Output: UPDATE users SET status = ? WHERE id = ? IF status = 'inactive'
     * }</pre>
     *
     * @param expr the conditional expression as a string
     * @return this CqlBuilder instance for method chaining
     * @see #onlyIf(Condition)
     * @see #iF(String)
     */
    public CqlBuilder onlyIf(final String expr) {
        return iF(expr);
    }

    /**
     * Adds a conditional IF clause to the CQL statement with the specified condition.
     *
     * <p>Prefer this method over {@link #iF(Condition)}. The {@code iF} method is kept for
     * backward compatibility because {@code if} is a Java keyword.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // INSERT with a single condition
     * String cql = PSC.insert("id", "name")
     *                 .into("users")
     *                 .onlyIf(Filters.isNull("name"))
     *                 .build().query();
     * // Output: INSERT INTO users (id, name) VALUES (?, ?) IF name IS NULL
     *
     * // UPDATE with a compound condition (operands are NOT parenthesized, per Cassandra's LWT grammar)
     * String cql = PSC.update("users")
     *                 .set("status")
     *                 .where(Filters.eq("id", 123))
     *                 .onlyIf(Filters.and(Filters.eq("status", "inactive"), Filters.lt("retryCount", 3)))
     *                 .build().query();
     * // Output: UPDATE users SET status = ? WHERE id = ? IF status = ? AND retry_count < ?
     * }</pre>
     *
     * @param cond the condition object for the IF clause
     * @return this CqlBuilder instance for method chaining
     * @see #onlyIf(String)
     * @see #iF(Condition)
     * @see com.landawn.abacus.query.Filters
     */
    public CqlBuilder onlyIf(final Condition cond) {
        return iF(cond);
    }

    /**
     * Adds a conditional IF clause to the CQL statement with the specified expression.
     *
     * <p>The IF clause enables lightweight transactions (LWT) in Cassandra by adding conditions
     * that must be met for the operation to succeed. This is particularly useful for ensuring
     * data consistency in concurrent environments.</p>
     *
     * <p><b>Note:</b> Prefer {@link #onlyIf(String)}. The method name is spelled {@code iF}
     * because {@code if} is a Java keyword.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // INSERT with condition
     * String cql = PSC.insert("id", "name")
     *                 .into("users")
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
     * @see #onlyIf(String)
     * @see #iF(Condition)
     */
    public CqlBuilder iF(final String expr) {
        assertNotClosed();
        init(true);

        checkInsertClauseOrder();

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
     * <p><b>Note:</b> Prefer {@link #onlyIf(Condition)}. The method name is spelled {@code iF}
     * because {@code if} is a Java keyword.</p>
     *
     * <p><b>Note:</b> Any literal written in Expression condition won't be formalized according
     * to the naming policy.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // INSERT with condition using Filters
     * String cql = PSC.insert("id", "name")
     *                 .into("users")
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
     * @see #onlyIf(Condition)
     * @see #iF(String)
     * @see com.landawn.abacus.query.Filters
     */
    public CqlBuilder iF(final Condition cond) {
        assertNotClosed();
        init(true);

        checkInsertClauseOrder();

        _sb.append(_SPACE_IF_SPACE);

        // Cassandra's LWT grammar is "IF columnCondition (AND columnCondition)*"; parenthesized members
        // are a syntax error there (unlike WHERE relations), so the Junction branch of appendCondition
        // renders without per-member parentheses while this flag is set.
        _appendingIfClause = true;

        try {
            appendCondition(cond);
        } finally {
            _appendingIfClause = false;
        }

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
     *                 .set("lastLogin")
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
        assertNotClosed();
        init(true);

        checkInsertClauseOrder();

        appendIfClause(_SPACE_IF_EXISTS);

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
     *                 .ifNotExists()
     *                 .build().query();
     * // Output: INSERT INTO users (id, name, email) VALUES (?, ?, ?) IF NOT EXISTS
     *
     * // Can also be used with UPDATE operations
     * String cql = PSC.update("users")
     *                 .set("status")
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
        assertNotClosed();
        init(true);

        checkInsertClauseOrder();

        appendIfClause(_SPACE_IF_NOT_EXISTS);

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
     * // Output: SELECT * FROM events WHERE (type = ?) AND (timestamp >= ?) ALLOW FILTERING
     * }</pre>
     *
     * <p><b>Performance Warning:</b> Use this clause sparingly as it can cause full table scans
     * and significantly impact query performance.</p>
     *
     * @return this CqlBuilder instance for method chaining
     */
    public CqlBuilder allowFiltering() {
        assertNotClosed();
        init(true);

        checkInsertClauseOrder();

        _sb.append(_SPACE_ALLOW_FILTERING);

        return this;
    }

    @Override
    protected void appendOperationBeforeFrom(final String tableName) {
        if (_op != OperationType.QUERY && _op != OperationType.DELETE) {
            throw new IllegalStateException("Invalid operation: " + _op);
        }

        if (_op == OperationType.QUERY && N.isEmpty(_propOrColumnNames) && N.isEmpty(_propOrColumnNameAliases) && N.isEmpty(_multiSelects)) {
            throw new IllegalStateException("No columns selected. Call select() to specify columns before building query");
        }

        final int idx = tableName.indexOf(' ');

        if (idx > 0) {
            _tableName = tableName.substring(0, idx).trim();

            String alias = tableName.substring(idx + 1).trim();

            // "account AS a" => alias "a", matching the parent builder's alias normalization.
            if (Strings.startsWithIgnoreCase(alias, "AS ")) {
                alias = alias.substring(3).trim();
            }

            _tableAlias = alias;
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
                setParameter("min" + Strings.capitalize(sanitizeNamedParameterName(propName)), minValue);
            } else {
                setParameter(propName, minValue);
            }

            _sb.append(_SPACE);
            _sb.append(SK.AND);
            _sb.append(_SPACE);

            final Object maxValue = bt.getMaxValue();
            if (_sqlPolicy == SQLPolicy.NAMED_SQL || _sqlPolicy == SQLPolicy.IBATIS_SQL) {
                setParameter("max" + Strings.capitalize(sanitizeNamedParameterName(propName)), maxValue);
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
                setParameter("min" + Strings.capitalize(sanitizeNamedParameterName(propName)), minValue);
            } else {
                setParameter(propName, minValue);
            }

            _sb.append(_SPACE);
            _sb.append(SK.AND);
            _sb.append(_SPACE);

            final Object maxValue = nbt.getMaxValue();
            if (_sqlPolicy == SQLPolicy.NAMED_SQL || _sqlPolicy == SQLPolicy.IBATIS_SQL) {
                setParameter("max" + Strings.capitalize(sanitizeNamedParameterName(propName)), maxValue);
            } else {
                setParameter(propName, maxValue);
            }
        } else if (cond instanceof final In in) {
            final String propName = in.getPropName();
            // getValues(), not getParameters(): getParameters() splices Condition-typed elements into
            // their flattened parameter values, silently changing the placeholder count of the IN list.
            final List<?> params = in.getValues();

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
            // getValues(), not getParameters(): see the In branch above.
            final List<?> params = notIn.getValues();

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
        } else if (cond instanceof final ComposableCell cell) {
            // Handles Not / Exists / NotExists / Any / All / Some — these extend ComposableCell, not Cell,
            // so without this branch they would fall through to the final `else` and throw
            // "Unsupported condition", breaking e.g. PSC.select(...).where(Filters.not(...)).
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

            if (SK.OR.equalsIgnoreCase(junction.operator().toString())) {
                throw new IllegalArgumentException("OR conditions are not supported by Cassandra CQL WHERE clauses");
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

                    // The LWT IF grammar is "IF columnCondition (AND columnCondition)*" — parenthesized
                    // members are rejected there, unlike WHERE relations.
                    if (_appendingIfClause) {
                        appendCondition(conditionList.get(i));
                    } else {
                        _sb.append(_PARENTHESIS_L);

                        appendCondition(conditionList.get(i));

                        _sb.append(_PARENTHESIS_R);
                    }
                }

                // sb.append(_PARENTHESIS_R);
            }
        } else if (cond instanceof final SubQuery subQuery) {
            final Condition subCond = subQuery.getCondition();

            if (Strings.isNotEmpty(subQuery.sql())) {
                _sb.append(subQuery.sql());
            } else {
                // Render through a sub-builder and keep its bind parameters: appending only the query text
                // dropped the subquery's parameters, leaving '?' placeholders with no bound values (and, for
                // named builders, colliding placeholder names between the outer and inner statements).
                final CqlBuilder subBuilder = newSubQueryBuilder(subQuery);
                seedNamedParameterOccurrences(subBuilder);

                // The SubQuery contract allows a null condition (select without a WHERE clause).
                if (subCond != null) {
                    subBuilder.append(subCond);
                }

                final SP subSP = subBuilder.build();
                adoptNamedParameterOccurrences(subBuilder);

                _sb.append(subSP.query());

                if (N.notEmpty(subSP.parameters())) {
                    _parameters.addAll(subSP.parameters());
                }
            }
        } else if (cond instanceof Expression) {
            // Note: Any literal written in Expression condition won't be formalized according to naming policy.
            appendStringExpr(((Expression) cond).getLiteral(), false);
        } else {
            throw new IllegalArgumentException("Unsupported condition: " + cond.toString());
        }
    }

    /**
     * Allocates a fresh sub-query builder for a {@link SubQuery} condition, carrying over this builder's
     * {@link SqlDialect} so the sub-query is rendered with the same naming and parameter policy as the
     * enclosing statement.
     *
     * @param subQuery the sub-query condition being rendered
     * @return a fresh sub-query builder bound to the same {@link SqlDialect} as {@code this}
     * @throws IllegalArgumentException if {@code subQuery} has no selected property/column names
     */
    private CqlBuilder newSubQueryBuilder(final SubQuery subQuery) {
        final Collection<String> selectPropNames = subQuery.getSelectPropNames();
        N.checkArgNotEmpty(selectPropNames, SELECTION_PART_MSG);

        final CqlBuilder subBuilder = new CqlBuilder(sqlDialect);
        subBuilder._op = OperationType.QUERY;
        subBuilder._propOrColumnNames = selectPropNames;

        if (subQuery.getEntityClass() != null) {
            return subBuilder.from(subQuery.getEntityClass());
        }

        return subBuilder.from(subQuery.getEntityName());
    }

    private static void validateColumnAliases(final Map<String, String> propOrColumnNameAliases) {
        for (final String alias : propOrColumnNameAliases.values()) {
            if (Strings.isBlank(alias) || alias.indexOf('"') >= 0 || alias.indexOf('`') >= 0 || alias.indexOf('\r') >= 0 || alias.indexOf('\n') >= 0
                    || alias.contains("--") || alias.contains("/*") || alias.contains("*/")) {
                throw new IllegalArgumentException("Column alias must not be null, blank, quoted, or contain CQL comment tokens");
            }
        }
    }

    private static Collection<String> getDeletePropNamesByClass(final Class<?> entityClass, final Set<String> excludedPropNames) {
        final Collection<String>[] val = loadPropNamesByClass(entityClass);
        // val[2] (writable props: @ReadOnly/@ReadOnlyId/@Transient removed, no dotted sub-entity props) —
        // the documented contract for delete(Class). Id props are removed as well because Cassandra
        // rejects DELETE on primary-key columns.
        final List<String> propNames = new ArrayList<>(val[2]);
        propNames.removeAll(QueryUtil.getIdPropNames(entityClass));

        if (N.notEmpty(excludedPropNames)) {
            propNames.removeAll(excludedPropNames);
        }

        return propNames;
    }

    /**
     * Entry point for building CQL statements with a fixed {@link SqlDialect} (naming policy + parameter style).
     *
     * <p>
     * DSL = a specialized language/API for expressing one kind of task clearly
     * </p>
     *
     * <p>Each predefined constant on {@link CqlBuilder} (e.g. {@link CqlBuilder#PSC}, {@link CqlBuilder#NSC},
     * {@link CqlBuilder#SCCB}) is a {@code Dsl} bound to a specific dialect. Call one of the statement methods
     * &mdash; {@code insert}, {@code select}, {@code update}, {@code delete}, {@code deleteFrom}, {@code count},
     * etc. &mdash; to obtain a fresh {@link CqlBuilder} configured for that operation. Dsl instances are
     * immutable and thread-safe; the {@link CqlBuilder} instances they produce are not.</p>
     */
    public static final class Dsl {
        private final SqlDialect sqlDialect;

        Dsl(final SqlDialect sqlDialect) {
            this.sqlDialect = sqlDialect;
        }

        /**
         * Creates a {@code Dsl} bound to the given {@link SqlDialect}, fixing the naming policy and
         * parameter style of every {@link CqlBuilder} it produces.
         *
         * <p>The common dialect combinations are already exposed as predefined constants on
         * {@link CqlBuilder} (for example {@link CqlBuilder#PSC} or {@link CqlBuilder#NSC}); use this
         * factory to obtain a DSL for a combination that is not predefined. The returned {@code Dsl} is
         * immutable and thread-safe, so it is typically stored in a {@code static final} field and reused.</p>
         *
         * @param sqlDialect the dialect (naming policy + parameter style) the new DSL is bound to
         * @return a new {@code Dsl} that produces {@link CqlBuilder} instances using the given dialect
         * @throws IllegalArgumentException if {@code sqlDialect} is {@code null}
         */
        public static Dsl forDialect(final SqlDialect sqlDialect) {
            N.checkArgNotNull(sqlDialect, "sqlDialect");

            return new Dsl(sqlDialect);
        }

        private CqlBuilder createCqlBuilderInstance() {
            return new CqlBuilder(sqlDialect);
        }

        private NamingPolicy namingPolicy() {
            return sqlDialect.namingPolicy() == null ? NamingPolicy.SNAKE_CASE : sqlDialect.namingPolicy();
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
        public CqlBuilder insert(final String expr) {
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
        public CqlBuilder insert(final String... propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, INSERTION_PART_MSG);

            final CqlBuilder instance = createCqlBuilderInstance();

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
        public CqlBuilder insert(final Collection<String> propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, INSERTION_PART_MSG);

            final CqlBuilder instance = createCqlBuilderInstance();

            instance._op = OperationType.ADD;
            instance._propOrColumnNames = new ArrayList<>(propOrColumnNames);

            return instance;
        }

        /**
         * Creates an INSERT statement from a map of property names and values.
         *
         * <p>This method generates an INSERT statement where map keys represent property names
         * (converted to snake_case) and values are used to generate parameter placeholders.
         * The CQL string and parameter list can both be retrieved from the {@code SP} returned
         * by {@link CqlBuilder#build()}.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Map<String, Object> props = new HashMap<>();
         * props.put("firstName", "John");
         * props.put("lastName", "Doe");
         * SP cqlPair = PSC.insert(props).into("account").build();
         * // cqlPair.query():      INSERT INTO account (first_name, last_name) VALUES (?, ?)
         * // cqlPair.parameters(): ["John", "Doe"]
         * }</pre>
         *
         * @param props map of property names to values
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if props is null or empty
         */
        public CqlBuilder insert(final Map<String, Object> props) {
            N.checkArgNotEmpty(props, INSERTION_PART_MSG);

            final CqlBuilder instance = createCqlBuilderInstance();

            instance._op = OperationType.ADD;
            instance._props = new LinkedHashMap<>(props);

            return instance;
        }

        /**
         * Creates an INSERT statement from an entity object.
         *
         * <p>This method inspects the entity object and extracts the properties that are
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
         * SP cqlPair = PSC.insert(account).into("account").build();
         * // cqlPair.query():      INSERT INTO account (first_name, last_name, email) VALUES (?, ?, ?)
         * // cqlPair.parameters(): ["John", "Doe", "john.doe@example.com"]
         * }</pre>
         *
         * @param entity the entity object to insert
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entity is null
         */
        public CqlBuilder insert(final Object entity) {
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
         * SP cqlPair = PSC.insert(account, excluded).into("account").build();
         * // cqlPair.query():      INSERT INTO account (first_name, last_name, email) VALUES (?, ?, ?)
         * // cqlPair.parameters(): ["John", "Doe", "john.doe@example.com"]
         * }</pre>
         *
         * @param entity the entity object to insert
         * @param excludedPropNames properties to exclude from the insert
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entity is null
         */
        public CqlBuilder insert(final Object entity, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entity, INSERTION_PART_MSG);

            final CqlBuilder instance = createCqlBuilderInstance();

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
        public CqlBuilder insert(final Class<?> entityClass) {
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
        public CqlBuilder insert(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, INSERTION_PART_MSG);

            final CqlBuilder instance = createCqlBuilderInstance();

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
        public CqlBuilder insertInto(final Class<?> entityClass) {
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
        public CqlBuilder insertInto(final Class<?> entityClass, final Set<String> excludedPropNames) {
            return insert(entityClass, excludedPropNames).into(entityClass);
        }

        /**
         * Generates a multi-row batch INSERT statement.
         *
         * <p>This method creates a single INSERT statement that contains multiple value sets,
         * which can be more efficient than issuing multiple individual INSERT statements.</p>
         *
         * <p><b>Note:</b> This is a Cassandra {@code CqlBuilder} but the generated SQL form
         * (a single INSERT with multiple VALUES tuples) is not part of standard CQL; this
         * method is provided for parity with the SQL builder and is marked {@code @Beta}.
         * Use Cassandra BATCH statements when batching multiple INSERTs against Cassandra.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * List<Account> accounts = Arrays.asList(
         *     new Account("John", "Doe"),
         *     new Account("Jane", "Smith"),
         *     new Account("Bob", "Johnson")
         * );
         *
         * SP cqlPair = PSC.batchInsert(accounts).into("account").build();
         * // cqlPair.query():      INSERT INTO account (first_name, last_name) VALUES (?, ?), (?, ?), (?, ?)
         * // cqlPair.parameters(): ["John", "Doe", "Jane", "Smith", "Bob", "Johnson"]
         * }</pre>
         *
         * @param propsList collection of entities or property maps to insert
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if propsList is null or empty
         */
        @Beta
        public CqlBuilder batchInsert(final Collection<?> propsList) {
            N.checkArgNotEmpty(propsList, INSERTION_PART_MSG);

            final CqlBuilder instance = createCqlBuilderInstance();

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
         *                 .set("firstName")
         *                 .set("lastName")
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: UPDATE account SET first_name = ?, last_name = ? WHERE id = ?
         * }</pre>
         *
         * @param tableName the name of the table to update
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if tableName is null or empty
         */
        public CqlBuilder update(final String tableName) {
            N.checkArgNotEmpty(tableName, UPDATE_PART_MSG);

            final CqlBuilder instance = createCqlBuilderInstance();

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
         *                 .set("firstName")
         *                 .set("lastModified")
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
        public CqlBuilder update(final String tableName, final Class<?> entityClass) {
            N.checkArgNotEmpty(tableName, UPDATE_PART_MSG);
            N.checkArgNotNull(entityClass, UPDATE_PART_MSG);

            final CqlBuilder instance = createCqlBuilderInstance();

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
         *                 .set("status")
         *                 .where(Filters.eq("id", 1))
         *                 .build().query();
         * // Output: UPDATE account SET status = ? WHERE id = ?
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public CqlBuilder update(final Class<?> entityClass) {
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
        public CqlBuilder update(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, UPDATE_PART_MSG);

            final CqlBuilder instance = createCqlBuilderInstance();

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
        public CqlBuilder delete(final String expr) {
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
        public CqlBuilder delete(final String... columnNames) {
            N.checkArgNotEmpty(columnNames, DELETION_PART_MSG);

            final CqlBuilder instance = createCqlBuilderInstance();

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
        public CqlBuilder delete(final Collection<String> columnNames) {
            N.checkArgNotEmpty(columnNames, DELETION_PART_MSG);

            final CqlBuilder instance = createCqlBuilderInstance();

            instance._op = OperationType.DELETE;
            instance._propOrColumnNames = new ArrayList<>(columnNames);

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
        public CqlBuilder delete(final Class<?> entityClass) {
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
        public CqlBuilder delete(final Class<?> entityClass, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createCqlBuilderInstance();

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
        public CqlBuilder deleteFrom(final String tableName) {
            N.checkArgNotEmpty(tableName, DELETION_PART_MSG);

            final CqlBuilder instance = createCqlBuilderInstance();

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
        public CqlBuilder deleteFrom(final String tableName, final Class<?> entityClass) {
            N.checkArgNotEmpty(tableName, DELETION_PART_MSG);
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createCqlBuilderInstance();

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
        public CqlBuilder deleteFrom(final Class<?> entityClass) {
            N.checkArgNotNull(entityClass, DELETION_PART_MSG);

            final CqlBuilder instance = createCqlBuilderInstance();

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
        public CqlBuilder select(final String selectPart) {
            N.checkArgNotEmpty(selectPart, SELECTION_PART_MSG);

            final CqlBuilder instance = createCqlBuilderInstance();

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
        public CqlBuilder select(final String... propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, SELECTION_PART_MSG);

            final CqlBuilder instance = createCqlBuilderInstance();

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
        public CqlBuilder select(final Collection<String> propOrColumnNames) {
            N.checkArgNotEmpty(propOrColumnNames, SELECTION_PART_MSG);

            final CqlBuilder instance = createCqlBuilderInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNames = new ArrayList<>(propOrColumnNames);

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
        public CqlBuilder select(final Map<String, String> propOrColumnNameAliases) {
            N.checkArgNotEmpty(propOrColumnNameAliases, SELECTION_PART_MSG);
            validateColumnAliases(propOrColumnNameAliases);

            final CqlBuilder instance = createCqlBuilderInstance();

            instance._op = OperationType.QUERY;
            instance._propOrColumnNameAliases = new LinkedHashMap<>(propOrColumnNameAliases);

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
         * // Output: SELECT id AS "id", first_name AS "firstName", last_name AS "lastName", email AS "email", created_date AS "createdDate" FROM account
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public CqlBuilder select(final Class<?> entityClass) {
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
        public CqlBuilder select(final Class<?> entityClass, final boolean includeSubEntityProperties) {
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
        public CqlBuilder select(final Class<?> entityClass, final Set<String> excludedPropNames) {
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
        public CqlBuilder select(final Class<?> entityClass, final boolean includeSubEntityProperties, final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            final CqlBuilder instance = createCqlBuilderInstance();

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
         * // Output: SELECT id AS "id", first_name AS "firstName", last_name AS "lastName", email AS "email" FROM account WHERE status = ?
         * }</pre>
         *
         * @param entityClass the entity class
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public CqlBuilder selectFrom(final Class<?> entityClass) {
            return selectFrom(entityClass, false);
        }

        /**
         * Creates a SELECT FROM statement for an entity class with table alias.
         *
         * <p>This method creates a SELECT FROM statement where columns are prefixed with the table
         * alias. Cassandra CQL does not support JOIN; the alias applies only to the single
         * FROM table and is used to qualify column references.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * String cql = PSC.selectFrom(Account.class, "a")
         *                 .where(Filters.eq("a.status", "active"))
         *                 .build().query();
         * // Output: SELECT a.id AS "id", a.first_name AS "firstName", a.last_name AS "lastName", a.email AS "email" FROM account a WHERE a.status = ?
         * }</pre>
         *
         * @param entityClass the entity class
         * @param alias the table alias
         * @return a new CqlBuilder instance for method chaining
         * @throws IllegalArgumentException if entityClass is null
         */
        public CqlBuilder selectFrom(final Class<?> entityClass, final String alias) {
            return selectFrom(entityClass, alias, false);
        }

        /**
         * Creates a SELECT FROM statement with optional sub-entity properties.
         *
         * <p>This convenience method combines SELECT and FROM operations with control over
         * sub-entity inclusion. Cassandra CQL does not support JOIN; sub-entity columns are
         * simply added to the projection of the single FROM table.</p>
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
        public CqlBuilder selectFrom(final Class<?> entityClass, final boolean includeSubEntityProperties) {
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
        public CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final boolean includeSubEntityProperties) {
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
        public CqlBuilder selectFrom(final Class<?> entityClass, final Set<String> excludedPropNames) {
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
         *                 .where(Filters.eq("a.id", 1))
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
        public CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final Set<String> excludedPropNames) {
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
        public CqlBuilder selectFrom(final Class<?> entityClass, final boolean includeSubEntityProperties, final Set<String> excludedPropNames) {
            return selectFrom(entityClass, QueryUtil.getTableAlias(entityClass), includeSubEntityProperties, excludedPropNames);
        }

        /**
         * Creates a complete SELECT FROM statement with all options.
         *
         * <p>This method provides maximum flexibility by allowing control over table alias,
         * sub-entity inclusion, and property exclusion.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Set<String> excluded = N.asSet("password", "internalNotes");
         * String cql = PSC.selectFrom(Account.class, "a", true, excluded)
         *                 .where(Filters.gt("a.balance", 1000))
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
        public CqlBuilder selectFrom(final Class<?> entityClass, final String alias, final boolean includeSubEntityProperties,
                final Set<String> excludedPropNames) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            if (hasSubEntityToInclude(entityClass, includeSubEntityProperties)) {
                final List<String> selectTableNames = getSelectTableNames(entityClass, alias, excludedPropNames, namingPolicy());
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
        public CqlBuilder count(final String tableName) {
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
        public CqlBuilder count(final Class<?> entityClass) {
            N.checkArgNotNull(entityClass, SELECTION_PART_MSG);

            return select(COUNT_ALL_LIST).from(entityClass);
        }

        /**
         * Parses a condition into CQL with entity class mapping.
         *
         * <p>This method is useful for generating just the WHERE clause portion of a query
         * with proper property-to-column name mapping. The resulting builder is condition-only:
         * no {@code SELECT}, {@code FROM}, or other clause keyword is emitted, only the rendered condition.</p>
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Condition cond = Filters.and(
         *     Filters.eq("firstName", "John"),
         *     Filters.like("email", "%@example.com")
         * );
         *
         * String cql = PSC.parse(cond, Account.class).build().query();
         * // Output: (first_name = ?) AND (email LIKE ?)
         * }</pre>
         *
         * @param cond the condition to parse
         * @param entityClass the entity class for property mapping
         * @return a new CqlBuilder instance containing the parsed condition
         * @throws IllegalArgumentException if cond is null
         */
        public CqlBuilder parse(final Condition cond, final Class<?> entityClass) {
            N.checkArgNotNull(cond, "cond");

            final CqlBuilder instance = createCqlBuilderInstance();

            instance.setEntityClass(entityClass);
            instance._op = OperationType.QUERY;
            instance._isForConditionOnly = true;
            instance.append(cond);

            return instance;
        }
    }
}
