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
 * <h2>Parameter Binding Styles</h2>
 *
 * <h3>1. Positional Parameters (JDBC-style)</h3>
 * <pre>{@code
 * String cql = "SELECT name, email FROM users WHERE id = ? AND status = ?";
 * ParsedCql parsed = ParsedCql.parse(cql, null);
 * // Parameters bound by position: [userId, "active"]
 * }</pre>
 *
 * <h3>2. Named Parameters (Native Cassandra-style)</h3>
 * <pre>{@code
 * String cql = "SELECT name, email FROM users WHERE id = :userId AND status = :status";
 * ParsedCql parsed = ParsedCql.parse(cql, null);
 * // Parameters: {"userId": 123, "status": "active"}
 * }</pre>
 *
 * <h3>3. MyBatis-style Parameters</h3>
 * <pre>{@code
 * String cql = "SELECT name, email FROM users WHERE id = #{userId} AND status = #{status}";
 * ParsedCql parsed = ParsedCql.parse(cql, null);
 * // Converted to: SELECT name, email FROM users WHERE id = ? AND status = ?
 * // With parameter mapping: {0: "userId", 1: "status"}
 * }</pre>
 *
 * <h2>Caching Strategy</h2>
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
 * <h2>Usage Examples</h2>
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
 * // NOTE: The text-only cache is consulted only when the supplied attribute
 * // map is empty (or null). When a non-empty attributes map is supplied, the
 * // cache is bypassed and a fresh ParsedCql instance is created, so each
 * // call sees the attributes it provided.
 * }</pre>
 * 
 * <h2>Thread Safety</h2>
 * <p>The internal cache used by {@link #parse(String, Map)} is thread-safe. The parsed fields of
 * an individual {@code ParsedCql} instance (original CQL, parameterized CQL, named-parameter
 * mapping, parameter count) are effectively immutable after construction. The attribute map
 * returned by {@link #getAttributes()} is a live, mutable {@link Map}; callers that share an
 * instance across threads and mutate the attribute map must provide their own synchronization.</p>
 *
 * <h2>Memory Management</h2>
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
     * <li><strong>DML Operations:</strong> SELECT, INSERT, UPDATE, DELETE, MERGE (parameter processing applied)</li>
     * <li><strong>Other statements:</strong> any statement not starting with one of the
     *     keywords above (for example DDL such as CREATE, ALTER, DROP, or BATCH) is stored
     *     as-is with no parameter detection or normalization</li>
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
     * @throws IllegalArgumentException if the CQL contains a named parameter with an empty name,
     *         or mixes different parameter styles ({@code ?}, {@code :name}, {@code #{name}}) in the same statement
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
                int curlyDepth = 0;

                for (String word : words) {
                    final int prevCurlyDepth = curlyDepth;
                    curlyDepth = updateCurlyDepth(curlyDepth, word);

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
                    } else if (word.length() >= 1 && word.charAt(0) == _PREFIX_OF_NAMED_PARAMETER) {
                        // A token starting with ':' is a named parameter ONLY when it is not inside a '{...}' map/UDT
                        // literal, where ':' is the key/value separator (e.g. {'a':1}). Use prevCurlyDepth (the brace
                        // depth before this token's own braces are applied) so a token like ":2}" that closes the map
                        // is still recognized as being inside the literal.
                        if (prevCurlyDepth == 0) {
                            if (word.length() == 1) {
                                throw new IllegalArgumentException("Invalid named parameter: " + word + ". Parameter name cannot be empty.");
                            } else {
                                namedParameters.put(countOfParameter++, word.substring(1));

                                word = SK.QUESTION_MARK;

                                type |= 2;
                            }
                        }
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

    private static int updateCurlyDepth(final int currentDepth, final String word) {
        int result = currentDepth;
        char quoteChar = 0; // 0 = outside any string/identifier literal; otherwise the opening quote character

        for (int i = 0, len = word.length(); i < len; i++) {
            final char ch = word.charAt(i);

            if (quoteChar != 0) {
                // Inside a quoted string/identifier literal: '{' and '}' are data, not map/UDT-literal delimiters.
                // SqlParser keeps a quoted literal as a single token (with its quotes), recognizing both doubled-quote
                // ('', "") and backslash escaping, so we mirror that here to find the real closing quote.
                if (ch == '\\') {
                    i++; // skip the backslash-escaped character
                } else if (ch == quoteChar) {
                    if (i + 1 < len && word.charAt(i + 1) == quoteChar) {
                        i++; // a doubled quote ('' or "") is an escaped quote; stay inside the literal
                    } else {
                        quoteChar = 0; // closing quote
                    }
                }
            } else if (ch == '\'' || ch == '"') {
                quoteChar = ch;
            } else if (ch == '{') {
                result++;
            } else if (ch == '}' && result > 0) {
                result--;
            }
        }

        return result;
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
     * Caching only applies when the attributes map is empty; if a non-empty attributes
     * map is supplied, the cache is bypassed and a new instance is always created so
     * that statement-specific metadata is not shared across calls.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Basic parsing (cached): same CQL + null attrs returns the same instance
     * ParsedCql parsed1 = ParsedCql.parse("SELECT * FROM users WHERE id = ?", null);
     * ParsedCql parsed2 = ParsedCql.parse("SELECT * FROM users WHERE id = ?", null);
     * boolean cachedHit = parsed1 == parsed2;          // returns true (same cached reference)
     * int count = parsed1.parameterCount();            // returns 1
     *
     * // Parsing with attributes - non-empty attrs bypass the cache (always a fresh instance)
     * Map<String, String> attrs = new HashMap<>();
     * attrs.put("consistency", "QUORUM");
     * attrs.put("timeout", "30000");
     * ParsedCql withAttrs = ParsedCql.parse("INSERT INTO sessions (id, token) VALUES (?, ?)", attrs);
     * withAttrs.getAttributes().get("consistency");    // returns "QUORUM"
     *
     * // Same CQL with different non-empty attrs => different instance
     * Map<String, String> other = new HashMap<>();
     * other.put("consistency", "LOCAL_ONE");
     * ParsedCql withOther = ParsedCql.parse("INSERT INTO sessions (id, token) VALUES (?, ?)", other);
     * boolean distinct = withOther != withAttrs;       // returns true (attrs bypass cache)
     *
     * ParsedCql.parse(null, null);                                          // throws IllegalArgumentException (cql is null)
     * ParsedCql.parse("SELECT * FROM u WHERE a = ? AND b = :x", null);      // throws IllegalArgumentException (mixed '?'/':name' styles)
     * ParsedCql.parse("SELECT * FROM u WHERE id = #{}", null);              // throws IllegalArgumentException (empty named parameter)
     * }</pre>
     *
     * @param cql the CQL statement to parse (used as cache key)
     * @param attrs optional attributes map for statement metadata; when non-empty, parsing bypasses cache
     * @return a ParsedCql instance, either newly created or retrieved from cache
     * @throws IllegalArgumentException if {@code cql} is null, or the CQL contains a named
     *         parameter with an empty name, or mixes different parameter styles
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
     * Returns the original CQL statement (with leading and trailing whitespace trimmed).
     *
     * <p>This method returns the CQL statement as it was provided to the parser, after
     * applying a {@code trim()} on the leading and trailing whitespace. Internal formatting,
     * whitespace, and the original parameter placeholders ({@code ?}, {@code :name},
     * {@code #{name}}) are preserved, as is a trailing semicolon (which is only stripped from
     * the parameterized form). This is useful for logging, debugging, or when you
     * need to preserve the original query format.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ParsedCql.parse("   SELECT * FROM u WHERE id = ?   ", null).originalCql(); // returns "SELECT * FROM u WHERE id = ?" (trimmed)
     * ParsedCql.parse("SELECT * FROM u WHERE id = :userId", null).originalCql(); // returns "SELECT * FROM u WHERE id = :userId" (placeholders kept)
     * ParsedCql.parse("SELECT id FROM u WHERE id = ?;", null).originalCql();     // returns "SELECT id FROM u WHERE id = ?;" (trailing ';' kept)
     * ParsedCql.parse(null, null).originalCql();                                 // throws IllegalArgumentException (from parse(), cql is null)
     * }</pre>
     *
     * @return the trimmed original CQL statement string
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
     * <li>Leading and trailing whitespace is stripped</li>
     * <li>A single trailing semicolon, if present, is removed</li>
     * </ul>
     * 
     * <p>This parameterized version is what gets sent to Cassandra for prepared statement creation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Named parameters converted to '?', trailing ';' removed
     * ParsedCql.parse("SELECT * FROM u WHERE name = :userName AND id = :userId;", null).getParameterizedCql();
     * // returns "SELECT * FROM u WHERE name = ? AND id = ?"
     *
     * // MyBatis-style #{name} also converted to '?'
     * ParsedCql.parse("INSERT INTO u VALUES (#{id}, #{name}, #{email})", null).getParameterizedCql();
     * // returns "INSERT INTO u VALUES (?, ?, ?)"
     *
     * // Positional parameters unchanged; trailing whitespace stripped
     * ParsedCql.parse("INSERT INTO u (id, name) VALUES (?, ?)  ", null).getParameterizedCql();
     * // returns "INSERT INTO u (id, name) VALUES (?, ?)"
     *
     * // No parameters: returned as-is (after strip/semicolon handling)
     * ParsedCql.parse("SELECT * FROM u", null).getParameterizedCql();
     * // returns "SELECT * FROM u"
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
     * <p>The returned map is the instance's internal map; it is populated during construction and
     * is not intended to be modified by callers. Treat it as read-only.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<Integer, String> m1 = ParsedCql.parse("SELECT * FROM u WHERE name = :userName AND status = :status", null).namedParameters();
     * m1.get(0);   // returns "userName"
     * m1.get(1);   // returns "status"
     *
     * Map<Integer, String> m2 = ParsedCql.parse("INSERT INTO u VALUES (#{id}, #{name}, #{email})", null).namedParameters();
     * m2.get(0);   // returns "id"
     * m2.get(2);   // returns "email"
     *
     * // Positional-only parameters => empty map
     * ParsedCql.parse("SELECT * FROM u WHERE id = ? AND status = ?", null).namedParameters().isEmpty(); // returns true
     *
     * // No parameters at all => empty map
     * ParsedCql.parse("SELECT * FROM u", null).namedParameters().isEmpty();                             // returns true
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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ParsedCql.parse("SELECT * FROM u WHERE id = ? AND status = ?", null).parameterCount();         // returns 2 (positional)
     * ParsedCql.parse("SELECT * FROM u WHERE id = :id AND status = :status", null).parameterCount(); // returns 2 (named)
     * ParsedCql.parse("SELECT * FROM u", null).parameterCount();                                     // returns 0 (no parameters)
     * ParsedCql.parse("CREATE TABLE t (id INT PRIMARY KEY)", null).parameterCount();                 // returns 0 (DDL: no parameter scanning)
     * }</pre>
     *
     * @return the number of parameters in the statement (0 or positive integer)
     */
    public int parameterCount() {
        return parameterCount;
    }

    /**
     * Returns the attributes map associated with this parsed CQL statement.
     * 
     * <p>Attributes are descriptive metadata carried along with the CQL statement
     * (typically loaded from the XML configuration). Common attributes include:</p>
     *
     * <ul>
     * <li><strong>timeout:</strong> Query timeout in milliseconds</li>
     * <li><strong>consistency:</strong> Consistency level (ONE, QUORUM, ALL, etc.)</li>
     * <li><strong>retryPolicy:</strong> Retry policy for failed operations</li>
     * <li><strong>fetchSize:</strong> Number of rows to fetch per page</li>
     * <li><strong>tracing:</strong> Enable/disable query tracing</li>
     * </ul>
     *
     * <p><b>Note:</b> attributes are retained for XML round-tripping and as metadata only;
     * they are <i>not</i> applied to statement execution by either executor, so changing them
     * does not affect how the statement runs. The returned map is the live internal map (not a
     * defensive copy), and mutating it is discouraged: instances parsed <i>without</i> attributes
     * are cached and shared, so a mutation through this map leaks into every other caller that
     * parses the same CQL text for the lifetime of the cache entry.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // No attributes supplied => empty (never null) map
     * ParsedCql.parse("SELECT 1 FROM t", null).getAttributes().isEmpty();   // returns true
     *
     * // Attributes supplied at parse time are preserved
     * Map<String, String> input = new HashMap<>();
     * input.put("timeout", "5000");
     * input.put("consistency", "QUORUM");
     * ParsedCql parsed = ParsedCql.parse("SELECT * FROM t WHERE id = ?", input);
     * parsed.getAttributes().get("timeout");        // returns "5000"
     * parsed.getAttributes().get("consistency");    // returns "QUORUM"
     *
     * // The map is the live internal map; mutations are visible on subsequent reads
     * // (and, for no-attrs parses, to other callers sharing the cached instance).
     * parsed.getAttributes().put("fetchSize", "100");
     * parsed.getAttributes().get("fetchSize");      // returns "100" (still NOT applied at execution)
     * parsed.getAttributes().get("missingKey");     // returns null (absent key)
     * }</pre>
     *
     * @return the live internal map of attribute name-value pairs, never null but may be empty;
     *         treat it as read-only
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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ParsedCql a = ParsedCql.parse("SELECT 1 FROM t WHERE id = ?", null);
     * ParsedCql b = ParsedCql.parse("SELECT 1 FROM t WHERE id = ?", null);
     * a.hashCode() == b.hashCode();      // returns true (same CQL text)
     *
     * // Attributes are not part of the hash; same text => same hash code
     * Map<String, String> attrs = new HashMap<>();
     * attrs.put("consistency", "ONE");
     * ParsedCql c = ParsedCql.parse("SELECT 2 FROM t WHERE id = ?", attrs);
     * ParsedCql d = ParsedCql.parse("SELECT 2 FROM t WHERE id = ?", null);
     * c.hashCode() == d.hashCode();      // returns true (attributes ignored)
     * }</pre>
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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ParsedCql a = ParsedCql.parse("SELECT 1 FROM t WHERE id = ?", null);
     * ParsedCql b = ParsedCql.parse("SELECT 1 FROM t WHERE id = ?", null);
     * ParsedCql c = ParsedCql.parse("SELECT 1 FROM other WHERE id = ?", null);
     * a.equals(a);                              // returns true (same instance)
     * a.equals(b);                              // returns true (identical CQL text)
     * a.equals(c);                              // returns false (different CQL text)
     * a.equals("SELECT 1 FROM t WHERE id = ?"); // returns false (not a ParsedCql)
     * a.equals(null);                           // returns false
     *
     * // Attributes do not affect equality; same text still equals
     * Map<String, String> attrs = new HashMap<>();
     * attrs.put("consistency", "ONE");
     * ParsedCql d = ParsedCql.parse("SELECT 1 FROM t WHERE id = ?", attrs);
     * a.equals(d);                  // returns true (attributes ignored)
     * }</pre>
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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ParsedCql.parse("SELECT id FROM t WHERE id = :uid", null).toString();
     * // returns "[cql] SELECT id FROM t WHERE id = :uid [parameterizedCql] SELECT id FROM t WHERE id = ? [Attributes] {}"
     *
     * ParsedCql.parse("SELECT id FROM t WHERE id = ?", null).toString();
     * // returns "[cql] SELECT id FROM t WHERE id = ? [parameterizedCql] SELECT id FROM t WHERE id = ? [Attributes] {}"
     * }</pre>
     *
     * @return a detailed string representation of this ParsedCql instance
     */
    @Override
    public String toString() {
        return "[cql] " + cql + " [parameterizedCql] " + parameterizedCql + " [Attributes] " + attrs;
    }
}
