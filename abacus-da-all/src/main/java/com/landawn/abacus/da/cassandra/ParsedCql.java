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
 * <p>A {@code ParsedCql} captures only the parsed form of a statement: the original CQL, the
 * parameterized CQL, the named-parameter mapping and the parameter count. Statement-level
 * metadata (such as {@code timeout} or {@code consistency} attributes loaded from XML) is
 * <em>not</em> held here; it is owned by {@link CqlMapper}, keyed by statement id. See
 * {@link CqlMapper#getAttributes(String)}.</p>
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
 * ParsedCql parsed = ParsedCql.parse(cql);
 * // Parameters bound by position: [userId, "active"]
 * }</pre>
 *
 * <h3>2. Named Parameters (Native Cassandra-style)</h3>
 * <pre>{@code
 * String cql = "SELECT name, email FROM users WHERE id = :userId AND status = :status";
 * ParsedCql parsed = ParsedCql.parse(cql);
 * // Parameters: {"userId": 123, "status": "active"}
 * }</pre>
 *
 * <h3>3. MyBatis-style Parameters</h3>
 * <pre>{@code
 * String cql = "SELECT name, email FROM users WHERE id = #{userId} AND status = #{status}";
 * ParsedCql parsed = ParsedCql.parse(cql);
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
 * ParsedCql parsed = ParsedCql.parse(cql);
 *
 * // Get the original CQL
 * String originalCql = parsed.originalCql();
 *
 * // Get the parameterized version (normalized)
 * String parameterizedCql = parsed.parameterizedCql();
 *
 * // Get parameter count
 * int paramCount = parsed.parameterCount();
 *
 * // Get named parameter mappings (if any)
 * Map<Integer, String> namedParams = parsed.namedParameters();
 * }</pre>
 *
 * <h2>Thread Safety</h2>
 * <p>The internal cache used by {@link #parse(String)} is thread-safe. The fields of an individual
 * {@code ParsedCql} instance (original CQL, parameterized CQL, named-parameter mapping, parameter
 * count) are immutable after construction, so an instance can be freely shared across threads.</p>
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

    /** Cached hash code; this instance is effectively immutable, so {@code Objects.hash(cql)} is computed once. */
    private final int hashCode;

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
     * @throws IllegalArgumentException if the CQL contains a named parameter with an empty name,
     *         mixes different parameter styles ({@code ?}, {@code :name}, {@code #{name}}) in the same statement,
     *         or contains a malformed iBatis/MyBatis parameter that is missing its closing brace
     */
    private ParsedCql(final String cql) {
        this.cql = cql.trim();
        namedParameters = new HashMap<>();
        hashCode = Objects.hash(this.cql);

        final List<String> words = SqlParser.parse(this.cql);
        final boolean isOpSqlPrefix = isOpSqlPrefix(words);

        int type = 0; // bit mask: 1 - '?', 2 - ':propName', 4 - '#{propName}'

        if (isOpSqlPrefix) {
            int countOfParameter = 0;
            final StringBuilder sb = Objectory.createStringBuilder();

            try {
                int curlyDepth = 0;

                for (int i = 0, size = words.size(); i < size; i++) {
                    String word = words.get(i);
                    final int prevCurlyDepth = curlyDepth;
                    curlyDepth = updateCurlyDepth(curlyDepth, word);

                    if (word.equals(SK.QUESTION_MARK)) {
                        countOfParameter++;

                        type |= 1;
                    } else if (word.startsWith(LEFT_OF_IBATIS_NAMED_PARAMETER)) {
                        // A single tokenized word may carry more than one '#{...}' marker (for example "#{a}#{b}"),
                        // and the closing '}' may even land in a later token, because none of '#', '{', '}' are
                        // guaranteed token separators. Extract every leading '#{...}' marker in turn, pulling in
                        // following tokens until the closing '}' is found, and keep any trailing text for
                        // re-processing. curlyDepth is advanced for every consumed token so the map/UDT literal
                        // tracking used by the ':' branch below stays accurate; a well-formed '#{...}' marker
                        // contributes a balanced '{'/'}' pair and therefore leaves the depth unchanged.
                        final StringBuilder rebuilt = new StringBuilder(word.length() + 4);
                        final StringBuilder ibatisTokenBuilder = new StringBuilder();

                        while (word.startsWith(LEFT_OF_IBATIS_NAMED_PARAMETER)) {
                            ibatisTokenBuilder.setLength(0);
                            ibatisTokenBuilder.append(word);

                            while (ibatisTokenBuilder.indexOf(RIGHT_OF_IBATIS_NAMED_PARAMETER) < 0 && i < size - 1) {
                                final String next = words.get(++i);
                                curlyDepth = updateCurlyDepth(curlyDepth, next);
                                ibatisTokenBuilder.append(next);
                            }

                            final String ibatisToken = ibatisTokenBuilder.toString();
                            final int rightBracketIndex = ibatisToken.indexOf(RIGHT_OF_IBATIS_NAMED_PARAMETER);

                            if (rightBracketIndex < 0) {
                                throw new IllegalArgumentException(
                                        "Malformed iBatis/MyBatis parameter: missing closing '}' for token starting with '#{' in CQL: " + cql);
                            }

                            final String namedParameter = extractIbatisNamedParameter(ibatisToken.substring(2, rightBracketIndex));

                            if (Strings.isEmpty(namedParameter)) {
                                throw new IllegalArgumentException(
                                        "Invalid named parameter: " + ibatisToken.substring(0, rightBracketIndex + 1) + ". Parameter name cannot be empty.");
                            }

                            namedParameters.put(countOfParameter++, namedParameter);
                            rebuilt.append(SK.QUESTION_MARK);
                            word = rightBracketIndex + 1 < ibatisToken.length() ? ibatisToken.substring(rightBracketIndex + 1) : Strings.EMPTY;

                            type |= 4;
                        }

                        rebuilt.append(word);
                        word = rebuilt.toString();
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

                parameterizedCql = stripTrailingSemicolons(Strings.stripToEmpty(sb.toString()));
                parameterCount = countOfParameter;
            } finally {
                Objectory.recycle(sb);
            }
        } else {
            parameterizedCql = stripTrailingSemicolons(Strings.stripToEmpty(this.cql));
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
     * Returns {@code true} if the first non-comment, non-whitespace token of the parsed statement is one of the
     * recognized data-operation keywords ({@code SELECT}, {@code INSERT}, {@code UPDATE}, {@code DELETE},
     * {@code MERGE}). Only such statements have their parameter placeholders detected and normalized; any other
     * statement (DDL such as {@code CREATE}/{@code ALTER}/{@code DROP}, {@code BATCH}, etc.) is stored as-is.
     */
    private static boolean isOpSqlPrefix(final List<String> words) {
        for (final String word : words) {
            if (!isCommentOrSpaceToken(word)) {
                return isOpSqlPrefixWord(word);
            }
        }

        return false;
    }

    /**
     * Equivalent to {@code opSqlPrefixSet.contains(word.toUpperCase())} but without allocating a temporary
     * upper-cased String. All entries of {@code opSqlPrefixSet} are uppercase ASCII keywords, so a
     * case-insensitive scan yields the identical result.
     */
    private static boolean isOpSqlPrefixWord(final String word) {
        for (final String prefix : opSqlPrefixSet) {
            if (prefix.equalsIgnoreCase(word)) {
                return true;
            }
        }

        return false;
    }

    private static boolean isCommentOrSpaceToken(final String word) {
        return Strings.isEmpty(word) || " ".equals(word) || word.startsWith("--") || word.startsWith("/*");
    }

    /**
     * Extracts the parameter name from the inside of an iBatis/MyBatis {@code #{...}} marker. Leading and trailing
     * whitespace is removed and, mirroring MyBatis, anything after the first comma (e.g. {@code #{id,jdbcType=INT}})
     * is dropped so only the property name remains. Returns {@link Strings#EMPTY} when the content is empty or blank.
     */
    private static String extractIbatisNamedParameter(final String content) {
        final String trimmed = Strings.stripToEmpty(content);

        if (Strings.isEmpty(trimmed)) {
            return Strings.EMPTY;
        }

        final int commaIndex = trimmed.indexOf(SK._COMMA);
        return (commaIndex >= 0 ? trimmed.substring(0, commaIndex) : trimmed).trim();
    }

    /**
     * Removes all trailing semicolons (and any whitespace between or around them) so that {@code "... ;"},
     * {@code "... ;;"} and {@code "... ; ;"} all produce the same parameterized form.
     */
    private static String stripTrailingSemicolons(final String cql) {
        int endIdx = cql.length();

        while (endIdx > 0) {
            final char ch = cql.charAt(endIdx - 1);

            if (ch == ';' || Character.isWhitespace(ch)) {
                endIdx--;
            } else {
                break;
            }
        }

        return endIdx == cql.length() ? cql : cql.substring(0, endIdx);
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
     * with the same whitespace and formatting will share the same cached instance.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Basic parsing (cached): the same CQL text returns the same instance
     * ParsedCql parsed1 = ParsedCql.parse("SELECT * FROM users WHERE id = ?");
     * ParsedCql parsed2 = ParsedCql.parse("SELECT * FROM users WHERE id = ?");
     * boolean cachedHit = parsed1 == parsed2;          // returns true (same cached reference)
     * int count = parsed1.parameterCount();            // returns 1
     *
     * ParsedCql.parse(null);                                          // throws IllegalArgumentException (cql is null)
     * ParsedCql.parse("SELECT * FROM u WHERE a = ? AND b = :x");      // throws IllegalArgumentException (mixed '?'/':name' styles)
     * ParsedCql.parse("SELECT * FROM u WHERE id = #{}");              // throws IllegalArgumentException (empty named parameter)
     * ParsedCql.parse("SELECT * FROM u WHERE id = #{abc");            // throws IllegalArgumentException (iBatis parameter missing '}')
     * }</pre>
     *
     * @param cql the CQL statement to parse (used as cache key)
     * @return a ParsedCql instance, either newly created or retrieved from cache
     * @throws IllegalArgumentException if {@code cql} is null, the CQL contains a named parameter
     *         with an empty name, mixes different parameter styles, or contains a malformed
     *         iBatis/MyBatis parameter that is missing its closing brace
     */
    public static ParsedCql parse(final String cql) {
        N.checkArgNotNull(cql, "cql");

        ParsedCql result = null;
        PoolableAdapter<ParsedCql> w = pool.get(cql);

        if ((w == null) || (w.value() == null)) {
            synchronized (PARSE_LOCK) {
                // Double-check after acquiring lock to prevent race condition
                w = pool.get(cql);
                if ((w == null) || (w.value() == null)) {
                    result = new ParsedCql(cql);
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
     * {@code #{name}}) are preserved, as are any trailing semicolons (which are only stripped from
     * the parameterized form). This is useful for logging, debugging, or when you
     * need to preserve the original query format.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ParsedCql.parse("   SELECT * FROM u WHERE id = ?   ").originalCql(); // returns "SELECT * FROM u WHERE id = ?" (trimmed)
     * ParsedCql.parse("SELECT * FROM u WHERE id = :userId").originalCql(); // returns "SELECT * FROM u WHERE id = :userId" (placeholders kept)
     * ParsedCql.parse("SELECT id FROM u WHERE id = ?;").originalCql();     // returns "SELECT id FROM u WHERE id = ?;" (trailing ';' kept)
     * ParsedCql.parse(null).originalCql();                                 // throws IllegalArgumentException (from parse(), cql is null)
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
     * <li>All trailing semicolons (and any whitespace between or around them) are removed</li>
     * </ul>
     *
     * <p>This parameterized version is what gets sent to Cassandra for prepared statement creation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Named parameters converted to '?', trailing ';' removed
     * ParsedCql.parse("SELECT * FROM u WHERE name = :userName AND id = :userId;").parameterizedCql();
     * // returns "SELECT * FROM u WHERE name = ? AND id = ?"
     *
     * // MyBatis-style #{name} also converted to '?'
     * ParsedCql.parse("INSERT INTO u VALUES (#{id}, #{name}, #{email})").parameterizedCql();
     * // returns "INSERT INTO u VALUES (?, ?, ?)"
     *
     * // Positional parameters unchanged; trailing whitespace stripped
     * ParsedCql.parse("INSERT INTO u (id, name) VALUES (?, ?)  ").parameterizedCql();
     * // returns "INSERT INTO u (id, name) VALUES (?, ?)"
     *
     * // No parameters: returned as-is (after strip/semicolon handling)
     * ParsedCql.parse("SELECT * FROM u").parameterizedCql();
     * // returns "SELECT * FROM u"
     * }</pre>
     *
     * @return the normalized CQL statement ready for prepared statement creation
     */
    public String parameterizedCql() {
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
     * Map<Integer, String> m1 = ParsedCql.parse("SELECT * FROM u WHERE name = :userName AND status = :status").namedParameters();
     * m1.get(0);   // returns "userName"
     * m1.get(1);   // returns "status"
     *
     * Map<Integer, String> m2 = ParsedCql.parse("INSERT INTO u VALUES (#{id}, #{name}, #{email})").namedParameters();
     * m2.get(0);   // returns "id"
     * m2.get(2);   // returns "email"
     *
     * // Positional-only parameters => empty map
     * ParsedCql.parse("SELECT * FROM u WHERE id = ? AND status = ?").namedParameters().isEmpty(); // returns true
     *
     * // No parameters at all => empty map
     * ParsedCql.parse("SELECT * FROM u").namedParameters().isEmpty();                             // returns true
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
     * ParsedCql.parse("SELECT * FROM u WHERE id = ? AND status = ?").parameterCount();         // returns 2 (positional)
     * ParsedCql.parse("SELECT * FROM u WHERE id = :id AND status = :status").parameterCount(); // returns 2 (named)
     * ParsedCql.parse("SELECT * FROM u").parameterCount();                                     // returns 0 (no parameters)
     * ParsedCql.parse("CREATE TABLE t (id INT PRIMARY KEY)").parameterCount();                 // returns 0 (DDL: no parameter scanning)
     * }</pre>
     *
     * @return the number of parameters in the statement (0 or positive integer)
     */
    public int parameterCount() {
        return parameterCount;
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
     * ParsedCql a = ParsedCql.parse("SELECT 1 FROM t WHERE id = ?");
     * ParsedCql b = ParsedCql.parse("SELECT 1 FROM t WHERE id = ?");
     * a.hashCode() == b.hashCode();      // returns true (same CQL text)
     * }</pre>
     *
     * @return hash code based on the original CQL statement
     */
    @Override
    public int hashCode() {
        return hashCode;
    }

    /**
     * Indicates whether some other object is "equal to" this ParsedCql.
     *
     * <p>Two ParsedCql instances are considered equal if and only if they were
     * created from identical CQL statement strings.</p>
     *
     * <p>This equality contract is essential for the caching mechanism, ensuring
     * that identical CQL statements can be properly cached and retrieved regardless
     * of when or how they were parsed.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ParsedCql a = ParsedCql.parse("SELECT 1 FROM t WHERE id = ?");
     * ParsedCql b = ParsedCql.parse("SELECT 1 FROM t WHERE id = ?");
     * ParsedCql c = ParsedCql.parse("SELECT 1 FROM other WHERE id = ?");
     * a.equals(a);                              // returns true (same instance)
     * a.equals(b);                              // returns true (identical CQL text)
     * a.equals(c);                              // returns false (different CQL text)
     * a.equals("SELECT 1 FROM t WHERE id = ?"); // returns false (not a ParsedCql)
     * a.equals(null);                           // returns false
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
     * <p>The string representation includes the original CQL statement and the parameterized
     * version. This is primarily useful for debugging and logging purposes to understand how
     * the CQL was parsed and what parameters were extracted.</p>
     *
     * <p>The format is: {@code [cql] <original> [parameterizedCql] <normalized>}</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ParsedCql.parse("SELECT id FROM t WHERE id = :uid").toString();
     * // returns "[cql] SELECT id FROM t WHERE id = :uid [parameterizedCql] SELECT id FROM t WHERE id = ?"
     *
     * ParsedCql.parse("SELECT id FROM t WHERE id = ?").toString();
     * // returns "[cql] SELECT id FROM t WHERE id = ? [parameterizedCql] SELECT id FROM t WHERE id = ?"
     * }</pre>
     *
     * @return a detailed string representation of this ParsedCql instance
     */
    @Override
    public String toString() {
        return "[cql] " + cql + " [parameterizedCql] " + parameterizedCql;
    }
}
