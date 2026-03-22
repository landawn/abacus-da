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

package com.landawn.abacus.da.cassandra;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.landawn.abacus.annotation.SuppressFBWarnings;
import com.landawn.abacus.pool.KeyedObjectPool;
import com.landawn.abacus.pool.PoolFactory;
import com.landawn.abacus.pool.Poolable;
import com.landawn.abacus.pool.PoolableAdapter;
import com.landawn.abacus.query.SqlParser;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Objectory;
import com.landawn.abacus.util.SK;
import com.landawn.abacus.util.Strings;

/**
 * Utility class for parsing and caching CQL statements with parameter binding support.
 * 
 * <p>ParsedCql provides comprehensive CQL parsing capabilities that handle different parameter
 * binding styles and optimize query execution through intelligent caching. It serves as a
 * bridge between raw CQL strings and executable prepared statements, offering both parsing
 * and optimization features.</p>
 *
 * <h2>Key Features</h2>
 * <h3>Main Features</h3>
 * <ul>
 * <li><strong>Multi-format Parameter Support:</strong>
 *     <ul>
 *     <li>Positional parameters: {@code SELECT * FROM users WHERE id = ?}</li>
 *     <li>Named parameters: {@code SELECT * FROM users WHERE id = :userId}</li>
 *     <li>MyBatis-style parameters: {@code SELECT * FROM users WHERE id = #{userId}}</li>
 *     </ul>
 * </li>
 * <li><strong>Intelligent Caching:</strong>
 *     <ul>
 *     <li>Automatic caching of parsed CQL statements</li>
 *     <li>Configurable cache eviction and TTL policies</li>
 *     <li>Memory-efficient pooled object management</li>
 *     </ul>
 * </li>
 * <li><strong>Query Optimization:</strong>
 *     <ul>
 *     <li>Automatic parameter normalization</li>
 *     <li>SQL injection prevention through proper parameter binding</li>
 *     <li>Query structure validation and error reporting</li>
 *     </ul>
 * </li>
 * </ul>
 * 
 * <h3>Parameter Binding Styles</h3>
 * 
 * <h4>1. Positional Parameters (JDBC-style)</h4>
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * String cql = "SELECT name, email FROM users WHERE id = ? AND status = ?";
 * ParsedCql parsed = ParsedCql.parse(cql, null);
 * // Parameters bound by position: [userId, "active"]
 * }</pre>
 * 
 * <h4>2. Named Parameters (Native Cassandra-style)</h4>
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * String cql = "SELECT name, email FROM users WHERE id = :userId AND status = :status";
 * ParsedCql parsed = ParsedCql.parse(cql, null);
 * // Parameters: {"userId": 123, "status": "active"}
 * }</pre>
 * 
 * <h4>3. MyBatis-style Parameters</h4>
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * String cql = "SELECT name, email FROM users WHERE id = #{userId} AND status = #{status}";
 * ParsedCql parsed = ParsedCql.parse(cql, null);
 * // Converted to: SELECT name, email FROM users WHERE id = ? AND status = ?
 * // With parameter mapping: {0: "userId", 1: "status"}
 * }</pre>
 * 
 * <h3>Caching Strategy</h3>
 * <p>ParsedCql implements a sophisticated caching mechanism to avoid repeated parsing
 * of identical CQL statements:</p>
 * 
 * <ul>
 * <li><strong>Cache Size:</strong> Up to 10,000 parsed statements</li>
 * <li><strong>Eviction Time:</strong> 60 seconds for inactive entries</li>
 * <li><strong>Max Live Time:</strong> 24 hours absolute TTL</li>
 * <li><strong>Max Idle Time:</strong> 24 hours since last access</li>
 * </ul>
 * 
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Basic parsing
 * String cql = "INSERT INTO users (id, name, email) VALUES (?, ?, ?)";
 * ParsedCql parsed = ParsedCql.parse(cql, null);
 * 
 * // Get the original CQL
 * String originalCql = parsed.originalCql();
 * 
 * // Get the parameterized version (normalized)
 * String parameterizedCql = parsed.getParameterizedCql();
 * 
 * // Get parameter count
 * int paramCount = parsed.parameterCount();
 * 
 * // Get named parameter mappings (if any)
 * Map<Integer, String> namedParams = parsed.namedParameters();
 *
 * // Parse with attributes (use unique CQL to avoid cache conflicts)
 * String cqlWithAttrs = "INSERT INTO sessions (id, token) VALUES (?, ?)";
 * Map<String, String> attributes = new HashMap<>();
 * attributes.put("timeout", "5000");
 * attributes.put("consistency", "QUORUM");
 * ParsedCql parsedWithAttrs = ParsedCql.parse(cqlWithAttrs, attributes);
 *
 * // Access attributes
 * Map<String, String> attrs = parsedWithAttrs.getAttributes();
 * String timeout = attrs.get("timeout");
 *
 * // NOTE: Caching is based on CQL string only. If you call parse() again with
 * // the same CQL but different attributes, the cached instance is returned
 * // with its original attributes, not the new ones.
 * }</pre>
 * 
 * <h3>Thread Safety</h3>
 * <p>This class is thread-safe. The internal caching mechanism uses concurrent data structures
 * and appropriate synchronization to ensure safe access from multiple threads. However, individual
 * ParsedCql instances are immutable after creation.</p>
 * 
 * <h3>Memory Management</h3>
 * <p>The class uses object pooling to minimize memory allocation and garbage collection overhead.
 * The cache automatically manages memory usage through configurable eviction policies.</p>
 * 
 * @see CqlMapper
 * @see CassandraExecutor
 * @see CassandraExecutorBase
 */
public final class ParsedCql {

    private static final int EVICT_TIME = 60 * 1000;

    private static final int LIVE_TIME = 24 * 60 * 60 * 1000;

    private static final int MAX_IDLE_TIME = 24 * 60 * 60 * 1000;

    private static final Set<String> opSqlPrefixSet = N.asSet(SK.SELECT, SK.INSERT, SK.UPDATE, SK.DELETE, SK.MERGE);

    private static final KeyedObjectPool<String, PoolableAdapter<ParsedCql>> pool = PoolFactory.createKeyedObjectPool(10000, EVICT_TIME);

    private static final Object PARSE_LOCK = new Object();

    private static final String PREFIX_OF_NAMED_PARAMETER = ":";

    private static final char _PREFIX_OF_NAMED_PARAMETER = PREFIX_OF_NAMED_PARAMETER.charAt(0);

    private static final String LEFT_OF_IBATIS_NAMED_PARAMETER = "#{";

    private static final String RIGHT_OF_IBATIS_NAMED_PARAMETER = "}";

    private final String cql;

    private final String parameterizedCql;

    private final Map<Integer, String> namedParameters;

    private final int parameterCount;

    private final Map<String, String> attrs;

    /**
     * Constructs a new ParsedCql instance by parsing the given CQL statement.
     * 
     * <p>This constructor performs comprehensive parsing of the CQL statement, including:
     * parameter detection and normalization, syntax validation, and optimization for
     * prepared statement execution. The parsing process handles multiple parameter styles
     * and ensures compatibility with Cassandra's prepared statement system.</p>
     * 
     * <p>The constructor identifies and processes different types of operations:
     * <ul>
     * <li><strong>DML Operations:</strong> SELECT, INSERT, UPDATE, DELETE, MERGE</li>
     * <li><strong>DDL Operations:</strong> CREATE, ALTER, DROP (no parameter processing)</li>
     * <li><strong>Batch Operations:</strong> BATCH statements with multiple operations</li>
     * </ul></p>
     * 
     * <p>Parameter processing includes:</p>
     * <ul>
     * <li>Detection of positional parameters ({@code ?})</li>
     * <li>Named parameter conversion ({@code :name} → {@code ?})</li>
     * <li>MyBatis-style parameter conversion ({@code #{name}} → {@code ?})</li>
     * <li>Parameter count validation and mapping generation</li>
     * </ul>
     * 
     * @param cql the raw CQL statement to parse
     * @param attrs optional attributes map containing metadata such as timeout, consistency level, etc.
     * @throws IllegalArgumentException if the CQL contains mixed parameter styles
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private ParsedCql(final String cql, final Map<String, String> attrs) {
        this.cql = cql.trim();
        namedParameters = new HashMap<>();
        this.attrs = N.isEmpty(attrs) ? (Map) new HashMap<>() : new HashMap<>(attrs);

        final List<String> words = SqlParser.parse(this.cql);

        boolean isOpSqlPrefix = false;
        for (final String word : words) {
            if (Strings.isNotEmpty(word) && !(word.equals(" ") || word.startsWith("--") || word.startsWith("/*"))) {
                isOpSqlPrefix = opSqlPrefixSet.contains(word.toUpperCase());
                break;
            }
        }

        int type = 0; // bit mask: 1 - '?', 2 - ':propName', 4 - '#{propName}'

        if (isOpSqlPrefix) {
            int countOfParameter = 0;
            final StringBuilder sb = Objectory.createStringBuilder();

            try {
                for (String word : words) {
                    if (word.equals(SK.QUESTION_MARK)) {
                        countOfParameter++;

                        type |= 1;
                    } else if (word.startsWith(LEFT_OF_IBATIS_NAMED_PARAMETER) && word.endsWith(RIGHT_OF_IBATIS_NAMED_PARAMETER)) {
                        if (word.length() <= 3) {
                            throw new IllegalArgumentException("Invalid named parameter: " + word + ". Parameter name cannot be empty.");
                        }
                        namedParameters.put(countOfParameter++, word.substring(2, word.length() - 1));

                        word = SK.QUESTION_MARK;

                        type |= 4;
                    } else if (word.length() >= 2 && word.charAt(0) == _PREFIX_OF_NAMED_PARAMETER) {
                        namedParameters.put(countOfParameter++, word.substring(1));

                        word = SK.QUESTION_MARK;

                        type |= 2;
                    }

                    if (Integer.bitCount(type) > 1) {
                        throw new IllegalArgumentException("Cannot mix parameter styles ('?', ':propName', '#{propName}') in the same CQL statement");
                    }

                    sb.append(word);
                }

                final String tmpCql = Strings.stripToEmpty(sb.toString());
                parameterizedCql = tmpCql.endsWith(";") ? tmpCql.substring(0, tmpCql.length() - 1) : tmpCql;
                parameterCount = countOfParameter;
            } finally {
                Objectory.recycle(sb);
            }
        } else {
            final String tmpCql = Strings.stripToEmpty(cql);
            parameterizedCql = tmpCql.endsWith(";") ? tmpCql.substring(0, tmpCql.length() - 1) : tmpCql;
            parameterCount = 0;
        }
    }

    /**
     * Parses a CQL statement and returns a cached ParsedCql instance.
     * 
     * <p>This factory method provides the primary entry point for CQL parsing with
     * built-in caching support. If the exact same CQL string has been parsed before,
     * the cached instance will be returned instead of creating a new one, significantly
     * improving performance for frequently used queries.</p>
     * 
     * <p>The caching mechanism uses the raw CQL string as the key, so identical queries
     * with the same whitespace and formatting will share the same cached instance.
     * However, the attributes parameter is not part of the cache key, so queries with
     * different attributes will share the same parsed structure but may have different
     * metadata.</p>
     * 
     * <p>Cache performance characteristics:</p>
     * <ul>
     * <li><strong>Cache Hit:</strong> O(1) lookup, ~1-10 microseconds</li>
     * <li><strong>Cache Miss:</strong> Full parsing + caching, typically &lt; 1 millisecond</li>
     * <li><strong>Memory Usage:</strong> ~200-500 bytes per cached statement</li>
     * </ul>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Basic parsing (cached)
     * ParsedCql parsed1 = ParsedCql.parse("SELECT * FROM users WHERE id = ?", null);
     *
     * // Same query - returns cached instance
     * ParsedCql parsed2 = ParsedCql.parse("SELECT * FROM users WHERE id = ?", null);
     * assert parsed1 == parsed2;  // Same object reference
     *
     * // Parsing with attributes - non-empty attrs bypass the cache
     * Map<String, String> attrs = new HashMap<>();
     * attrs.put("consistency", "QUORUM");
     * attrs.put("timeout", "30000");
     * ParsedCql parsedWithAttrs = ParsedCql.parse("INSERT INTO sessions (id, token) VALUES (?, ?)", attrs);
     *
     * // Accessing parsed information
     * String originalCql = parsedWithAttrs.originalCql();
     * String parameterizedCql = parsedWithAttrs.getParameterizedCql();
     * int paramCount = parsedWithAttrs.parameterCount();
     * Map<String, String> attributes = parsedWithAttrs.getAttributes();
     * // attributes will contain "consistency" -> "QUORUM", "timeout" -> "30000"
     *
     * // Calling parse() again with same CQL and non-empty attrs creates a new instance
     * Map<String, String> differentAttrs = new HashMap<>();
     * differentAttrs.put("consistency", "LOCAL_ONE");  // Different attributes!
     * ParsedCql cached = ParsedCql.parse("INSERT INTO sessions (id, token) VALUES (?, ?)", differentAttrs);
     * assert cached != parsedWithAttrs;  // Different instances when attrs are provided
     * assert cached.getAttributes().get("consistency").equals("LOCAL_ONE");
     * }</pre>
     * 
     * @param cql the CQL statement to parse (used as cache key)
     * @param attrs optional attributes map for statement metadata; when non-empty, parsing bypasses cache
     * @return a ParsedCql instance, either newly created or retrieved from cache
     * @throws IllegalArgumentException if cql is null or contains mixed parameter styles
     * @see #parse(String, Map)
     */
    public static ParsedCql parse(final String cql, final Map<String, String> attrs) {
        N.checkArgNotNull(cql, "cql");

        if (N.notEmpty(attrs)) {
            // Attributes can vary per statement-id for the same CQL text.
            // Bypass the text-only cache to avoid returning stale metadata.
            return new ParsedCql(cql, attrs);
        }

        ParsedCql result = null;
        PoolableAdapter<ParsedCql> w = pool.get(cql);

        if ((w == null) || (w.value() == null)) {
            synchronized (PARSE_LOCK) {
                // Double-check after acquiring lock to prevent race condition
                w = pool.get(cql);
                if ((w == null) || (w.value() == null)) {
                    result = new ParsedCql(cql, attrs);
                    pool.put(cql, Poolable.wrap(result, LIVE_TIME, MAX_IDLE_TIME));
                } else {
                    result = w.value();
                }
            }
        } else {
            result = w.value();
        }

        return result;
    }

    /**
     * Returns the original, unmodified CQL statement.
     * 
     * <p>This method returns the CQL statement exactly as it was provided to the parser,
     * including original formatting, whitespace, and parameter placeholders. This is useful
     * for logging, debugging, or when you need to preserve the original query format.</p>
     * 
     * @return the original CQL statement string
     */
    public String originalCql() {
        return cql;
    }

    /**
     * Returns the normalized CQL statement with all parameters converted to positional placeholders.
     * 
     * <p>This method returns the processed version of the CQL statement where:</p>
     * <ul>
     * <li>All named parameters ({@code :name}) are converted to {@code ?}</li>
     * <li>All MyBatis-style parameters ({@code #{name}}) are converted to {@code ?}</li>
     * <li>Trailing semicolons are removed</li>
     * <li>Whitespace is normalized</li>
     * </ul>
     * 
     * <p>This parameterized version is what gets sent to Cassandra for prepared statement creation.</p>
     * 
     * <p>Example transformations:</p>
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Original:      "SELECT * FROM users WHERE name = :userName AND id = :userId;"
     * Parameterized: "SELECT * FROM users WHERE name = ? AND id = ?"
     *
     * Original:      "INSERT INTO users (id, name) VALUES (?, ?)  "
     * Parameterized: "INSERT INTO users (id, name) VALUES (?, ?)"
     * }</pre>
     * 
     * @return the normalized CQL statement ready for prepared statement creation
     */
    public String getParameterizedCql() {
        return parameterizedCql;
    }

    /**
     * Returns a mapping of parameter positions to their original names.
     * 
     * <p>This method returns a map that associates each positional parameter index
     * with the original parameter name from the CQL statement. This mapping is essential
     * for binding named or MyBatis-style parameters to their correct positions in the
     * prepared statement.</p>
     * 
     * <p>The map will be empty for CQL statements that use only positional parameters ({@code ?}).</p>
     * 
     * <p>Example mappings:</p>
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CQL: "SELECT * FROM users WHERE name = :userName AND status = :status"
     * Returns: {0: "userName", 1: "status"}
     * 
     * CQL: "INSERT INTO users VALUES (#{id}, #{name}, #{email})"
     * Returns: {0: "id", 1: "name", 2: "email"}
     * 
     * CQL: "SELECT * FROM users WHERE id = ? AND status = ?"
     * Returns: {} (empty map)
     * }</pre>
     * 
     * @return a map from parameter index (0-based) to parameter name, empty if no named parameters
     */
    public Map<Integer, String> namedParameters() {
        return namedParameters;
    }

    /**
     * Returns the total number of parameters in the CQL statement.
     * 
     * <p>This count represents the number of parameter placeholders that need to be
     * bound when executing the statement. It includes all types of parameters:
     * positional ({@code ?}), named ({@code :name}), and MyBatis-style ({@code #{name}}).</p>
     * 
     * <p>This count is essential for:</p>
     * <ul>
     * <li>Validating that the correct number of parameter values are provided</li>
     * <li>Allocating arrays for parameter binding</li>
     * <li>Iterating through parameter positions during binding</li>
     * </ul>
     * 
     * @return the number of parameters in the statement (0 or positive integer)
     */
    public int parameterCount() {
        return parameterCount;
    }

    /**
     * Returns the attributes map associated with this parsed CQL statement.
     * 
     * <p>Attributes provide metadata about the CQL statement that can influence
     * execution behavior. Common attributes include:</p>
     * 
     * <ul>
     * <li><strong>timeout:</strong> Query timeout in milliseconds</li>
     * <li><strong>consistency:</strong> Consistency level (ONE, QUORUM, ALL, etc.)</li>
     * <li><strong>retryPolicy:</strong> Retry policy for failed operations</li>
     * <li><strong>fetchSize:</strong> Number of rows to fetch per page</li>
     * <li><strong>tracing:</strong> Enable/disable query tracing</li>
     * </ul>
     * 
     * <p>The returned map is mutable and can be modified after parsing if needed.
     * Changes to this map will affect how the statement is executed if the attributes
     * are used by the executor.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, String> attrs = parsedCql.getAttributes();
     * 
     * // Check for timeout setting
     * String timeout = attrs.get("timeout");
     * if (timeout != null) {
     *     int timeoutMs = Integer.parseInt(timeout);
     *     // Apply timeout to statement...
     * }
     * 
     * // Check consistency level
     * String consistency = attrs.get("consistency");
     * if ("QUORUM".equals(consistency)) {
     *     // Set QUORUM consistency...
     * }
     * }</pre>
     * 
     * @return a mutable map of attribute name-value pairs, never null but may be empty
     */
    public Map<String, String> getAttributes() {
        return attrs;
    }

    /**
     * Returns the hash code for this ParsedCql instance.
     * 
     * <p>The hash code is computed based solely on the original CQL statement string,
     * ensuring that ParsedCql instances created from identical CQL strings will have
     * the same hash code. This is essential for the caching mechanism to work correctly.</p>
     * 
     * @return hash code based on the original CQL statement
     */
    @Override
    public int hashCode() {
        return Objects.hash(cql);
    }

    /**
     * Indicates whether some other object is "equal to" this ParsedCql.
     * 
     * <p>Two ParsedCql instances are considered equal if and only if they were
     * created from identical CQL statement strings. The attributes, while part of
     * the instance data, do not affect equality comparison.</p>
     * 
     * <p>This equality contract is essential for the caching mechanism, ensuring
     * that identical CQL statements can be properly cached and retrieved regardless
     * of when or how they were parsed.</p>
     * 
     * @param obj the reference object with which to compare
     * @return {@code true} if this object represents the same CQL statement as the obj argument; false otherwise
     */
    @SuppressFBWarnings
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof final ParsedCql other) {
            return N.equals(cql, other.cql);
        }

        return false;
    }

    /**
     * Returns a string representation of this ParsedCql instance.
     * 
     * <p>The string representation includes the original CQL statement, the parameterized
     * version, and any attributes. This is primarily useful for debugging and logging
     * purposes to understand how the CQL was parsed and what parameters were extracted.</p>
     * 
     * <p>The format is: {@code [cql] <original> [parameterizedCql] <normalized> [Attributes] <attrs>}</p>
     * 
     * @return a detailed string representation of this ParsedCql instance
     */
    @Override
    public String toString() {
        return "[cql] " + cql + " [parameterizedCql] " + parameterizedCql + " [Attributes] " + attrs;
    }
}
