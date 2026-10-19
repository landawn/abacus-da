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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.landawn.abacus.annotation.SuppressFBWarnings;
import com.landawn.abacus.da.cs;
import com.landawn.abacus.pool.KeyedObjectPool;
import com.landawn.abacus.pool.PoolFactory;
import com.landawn.abacus.pool.Poolable;
import com.landawn.abacus.pool.PoolableAdapter;
import com.landawn.abacus.query.SqlParser;
import com.landawn.abacus.util.ImmutableMap;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Objectory;
import com.landawn.abacus.util.SK;
import com.landawn.abacus.util.Strings;

/**
 * Immutable parsed CQL text and parameter metadata, with caching through {@link #parse(String)}.
 *
 * <p>For statements whose first non-comment token is {@code SELECT}, {@code INSERT}, {@code UPDATE},
 * {@code DELETE}, {@code MERGE}, or {@code BEGIN} (case-insensitive), this class counts positional
 * placeholders and converts supported named placeholders to {@code ?}. Other statements receive
 * only surrounding-whitespace and trailing-semicolon normalization in their parameterized form.
 * Parsing collects binding metadata; it does not bind values, prepare statements, or validate
 * Cassandra CQL grammar. See {@link #parameterizedCql()} for normalization rules and marker limitations.</p>
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
 * <li><strong>Parsed-statement caching:</strong>
 *     <ul>
 *     <li>Reuse of cached results for identical raw CQL text</li>
 *     <li>Built-in cache eviction and TTL policies (see the Caching Strategy section below)</li>
 *     <li>Entry-count and input-length limits on cache retention</li>
 *     </ul>
 * </li>
 * <li><strong>Prepared-statement support:</strong>
 *     <ul>
 *     <li>Automatic parameter normalization</li>
 *     <li>Consistent positional binding for supported placeholder styles</li>
 *     <li>Validation of malformed or mixed placeholder styles</li>
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
 * // Parameterized CQL: SELECT name, email FROM users WHERE id = ? AND status = ?
 * // Parameter mapping: {0: "userId", 1: "status"}; values are supplied separately
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
 * <p>The cache is keyed by the complete input string, before trimming. Its retention policy is:</p>
 *
 * <ul>
 * <li><strong>Cache Size:</strong> Up to 1,000 parsed statements</li>
 * <li><strong>Admission:</strong> inputs whose {@link String#length()} exceeds {@value #MAX_CACHEABLE_CQL_LENGTH}
 *     UTF-16 code units are parsed without a cache lookup or insertion; surrounding whitespace counts toward this limit</li>
 * <li><strong>Eviction Sweep:</strong> an expiration sweep is scheduled every 60 seconds</li>
 * <li><strong>Max Live Time:</strong> 24 hours absolute TTL</li>
 * <li><strong>Max Idle Time:</strong> 24 hours since last access</li>
 * </ul>
 * <p>Capacity-based eviction may remove an entry before it expires. Concurrent misses for the same
 * text may parse independently and return distinct instances. Reference identity is not guaranteed;
 * use {@link #equals(Object)} to compare parsed statements by their trimmed original text.</p>
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
 * <p>The class keeps parsed instances in a bounded keyed pool to avoid repeated parsing and
 * allocation. The pool manages retained entries through its configured eviction policies.</p>
 *
 * @see CqlMapper
 * @see CassandraExecutor
 * @see CassandraExecutorBase
 */
public final class ParsedCql {

    private static final int EVICT_TIME = 60 * 1000;

    private static final int LIVE_TIME = 24 * 60 * 60 * 1000;

    private static final int MAX_IDLE_TIME = 24 * 60 * 60 * 1000;

    // Admission limit on the raw input length, measured before trimming (so surrounding whitespace counts). Longer
    // statements still parse normally; they only bypass the cache lookup and insertion, which keeps the retained key
    // text bounded at roughly capacity * MAX_CACHEABLE_CQL_LENGTH characters.
    private static final int MAX_CACHEABLE_CQL_LENGTH = 4096;

    private static final Set<String> opSqlPrefixSet = N.asSet(SK.SELECT, SK.INSERT, SK.UPDATE, SK.DELETE, SK.MERGE, "BEGIN");

    // Retention is bounded by entry count and by MAX_CACHEABLE_CQL_LENGTH; no byte-measure budget is installed.
    // get() and put() are individually thread-safe, but a miss is not published atomically: concurrent misses for the
    // same key may each parse and return their own instance (see parse(String)).
    private static final KeyedObjectPool<String, PoolableAdapter<ParsedCql>> pool = PoolFactory.createKeyedObjectPool(1000, EVICT_TIME);

    private static final String PREFIX_OF_NAMED_PARAMETER = ":";

    private static final char _PREFIX_OF_NAMED_PARAMETER = PREFIX_OF_NAMED_PARAMETER.charAt(0);

    private static final String LEFT_OF_IBATIS_NAMED_PARAMETER = "#{";

    // SqlParser reads '[' as the start of a bracket-quoted identifier or array subscript and keeps the whole group as
    // ONE token, which hides the bind markers inside CQL list literals and subscripts ("l + [?, ?]", "m[:k]") and, for
    // a group that does not start with a marker, may end it at a ']' inside a string literal and swallow the rest of
    // the statement. Outside quoted/dollar-quoted regions and comments, '[' and ']' are therefore replaced by a pair of
    // unused non-ASCII characters (private-use characters are tried first; see BracketMask.of), which the mask's
    // tokenizer treats as single-character separators, and restored on append. The pair must not occur anywhere in the
    // statement (not even inside a literal or comment), or a real character would be mistaken for a masked bracket.
    private static final char DEFAULT_MASKED_LEFT_BRACKET = (char) 0xE000; // Unicode private-use area

    private static final char DEFAULT_MASKED_RIGHT_BRACKET = (char) 0xE001;

    private static final char LAST_PRIVATE_USE_CHAR = (char) 0xF8FF;

    private static final SqlParser.Tokenizer CQL_TOKENIZER = SqlParser.tokenizer(SqlParser.TokenizerConfig.builder()
            .withSeparator("..")
            .withoutSeparator("?-")
            .withoutSeparator("?|")
            .withoutSeparator("?&")
            .withoutSeparator("?#")
            .withoutSeparator("?-|")
            .withoutSeparator("?||")
            .build());

    private static final BracketMask DEFAULT_BRACKET_MASK = new BracketMask(DEFAULT_MASKED_LEFT_BRACKET, DEFAULT_MASKED_RIGHT_BRACKET);

    private final String cql;

    private final String parameterizedCql;

    private final ImmutableMap<Integer, String> namedParameters;

    private final int parameterCount;

    /** Cached hash code, preserving the single-element {@code java.util.Objects.hash(cql)} value without a varargs array. */
    private final int hashCode;

    /**
     * Constructs a new ParsedCql instance by parsing the given CQL statement.
     *
     * <p>This constructor trims surrounding statement whitespace and, in the parameterized form,
     * removes trailing semicolons. For recognized data operations, it also detects and normalizes
     * parameter placeholders for prepared statement execution. It validates supported placeholder
     * forms and combinations, but does not validate Cassandra CQL grammar.</p>
     *
     * <p>The constructor identifies and processes different types of operations:</p>
     * <ul>
     * <li><strong>Data operations:</strong> SELECT, INSERT, UPDATE, DELETE, MERGE, and Cassandra
     *     {@code BEGIN ... BATCH} statements (parameter processing applied)</li>
     * <li><strong>Other statements:</strong> any statement whose first non-comment token does not match one of the
     *     keywords above, ignoring case (for example DDL such as CREATE, ALTER, or DROP), receives only
     *     statement-level normalization; placeholder scanning is skipped</li>
     * </ul>
     *
     * <p>Parameter processing includes:</p>
     * <ul>
     * <li>Detection of positional parameters ({@code ?})</li>
     * <li>Named parameter conversion ({@code :name} → {@code ?})</li>
     * <li>MyBatis-style parameter conversion ({@code #{name}} → {@code ?})</li>
     * <li>Parameter counting, name mapping, and rejection of mixed placeholder styles</li>
     * </ul>
     *
     * @param cql the non-null raw CQL statement to parse
     * @throws IllegalArgumentException if a recognized data operation contains a named placeholder with an empty name,
     *         mixes parameter styles ({@code ?}, {@code :name}, {@code #{name}}), or contains a MyBatis placeholder
     *         missing its closing brace; also if brackets require masking but no unused pair of mask characters is available
     */
    private ParsedCql(final String cql) throws IllegalArgumentException {
        this.cql = cql.trim();
        Map<Integer, String> localNamedParameters = null;
        hashCode = 31 + this.cql.hashCode();

        // SqlParser does not know CQL dollar-quoted string constants ($$...$$): it would collapse whitespace inside
        // them and treat quote characters, "--", "#" or "/*" inside them as literal/comment starts. Each complete
        // constant is therefore replaced by an opaque placeholder before tokenization and restored when appended.
        // Ordinary quoted CQL literals treat backslashes as data; double them temporarily for the SQL tokenizer.
        // Collection brackets are masked too (see BracketMask), with a private-use pair that the statement does not contain.
        final boolean hasBrackets = this.cql.indexOf('[') >= 0 || this.cql.indexOf(']') >= 0;
        final BracketMask bracketMask = hasBrackets ? BracketMask.of(this.cql) : null;

        if (hasBrackets && bracketMask == null) {
            // Without a mask, markers inside [...] would be silently lost; fail loudly rather than mis-bind.
            throw new IllegalArgumentException(
                    "Cannot parse a CQL statement with brackets that contains every candidate bracket-mask character (U+E000-U+F8FF and all other usable non-ASCII characters)");
        }

        // DDL does not use the tokenized body. Keep the mask-exhaustion check above for compatibility,
        // but avoid masking/copying large function bodies merely to classify their leading keyword.
        if (hasNonOperationPrefix(this.cql)) {
            parameterizedCql = stripTrailingSemicolons(Strings.stripToEmpty(this.cql));
            parameterCount = 0;
            namedParameters = ImmutableMap.empty();
            return;
        }

        final List<String> dollarQuotedStrings = new ArrayList<>(0);
        final String maskedCql = maskCqlStrings(removeDoubleSlashComments(this.cql), dollarQuotedStrings, bracketMask);
        final List<String> words = (bracketMask == null ? CQL_TOKENIZER : bracketMask.tokenizer).tokenize(maskedCql);
        final boolean isOpSqlPrefix = isOpSqlPrefix(words);

        int type = 0; // bit mask: 1 - '?', 2 - ':propName', 4 - '#{propName}'

        if (isOpSqlPrefix) {
            int countOfParameter = 0;
            final StringBuilder sb = Objectory.createStringBuilder();

            try {
                // [0] is the current {...} depth; [1] is 1 while scanning a $$...$$ string across tokens.
                final int[] literalState = new int[2];
                // The open '{' / '[' collection literals and subscripts, innermost last.
                final StringBuilder openContainers = new StringBuilder();
                final TokenReader tokens = new TokenReader(words);

                while (tokens.hasNext()) {
                    String word = tokens.next(literalState);

                    if (word.startsWith("/*")) {
                        // A block comment is a token of its own only in SqlParser's "-- Keep comments" mode. Its text is
                        // not CQL: a '{', '[' or "$$" in it must not open a literal (which would hide every later marker),
                        // and a "::name" in it must not be rewritten into a marker.
                        sb.append(word);
                        continue;
                    }

                    final int prevCurlyDepth = literalState[0];
                    final boolean prevInsideBrackets = openContainers.length() > 0 && openContainers.charAt(openContainers.length() - 1) == '[';
                    final boolean dollarQuotedToken = updateLiteralState(literalState, openContainers, word, bracketMask);
                    // A '{' opened by this very token counts, so a self-contained single-field literal such as
                    // "{street::street}" (balanced braces => prevCurlyDepth and the post-token depth are both 0)
                    // is still recognized. A complete dollar-quoted constant is opaque numeric placeholder text here, so a
                    // marker after it remains visible; only an unterminated dollar quote (state[1] != 0) suppresses the scan.
                    final int embeddedMarkerIndex = literalState[1] == 0 && (prevCurlyDepth > 0 || word.indexOf('{') >= 0)
                            ? indexOfEmbeddedLiteralNamedParameter(word, prevCurlyDepth)
                            : -1;

                    if (!dollarQuotedToken && word.equals(SK.QUESTION_MARK)) {
                        countOfParameter++;

                        type |= 1;
                    } else if (!dollarQuotedToken && word.startsWith(LEFT_OF_IBATIS_NAMED_PARAMETER)) {
                        // Consume each marker and token once. Offsets avoid repeatedly copying the tail of
                        // adjacent markers; a builder is needed only when one marker spans several tokens.
                        int markerStart = 0;

                        while (word.startsWith(LEFT_OF_IBATIS_NAMED_PARAMETER, markerStart)) {
                            int rightBracketIndex = word.indexOf('}', markerStart + 2);
                            final String content;

                            if (rightBracketIndex >= 0) {
                                content = word.substring(markerStart + 2, rightBracketIndex);
                            } else {
                                final StringBuilder markerContent = new StringBuilder();
                                markerContent.append(word, markerStart + 2, word.length());

                                while (tokens.hasNext()) {
                                    word = tokens.next(literalState);
                                    updateLiteralState(literalState, openContainers, word, bracketMask);
                                    rightBracketIndex = word.indexOf('}');

                                    if (rightBracketIndex >= 0) {
                                        markerContent.append(word, 0, rightBracketIndex);
                                        break;
                                    }

                                    markerContent.append(word);
                                }

                                if (rightBracketIndex < 0) {
                                    throw new IllegalArgumentException(
                                            "Malformed iBatis/MyBatis parameter: missing closing '}' for token starting with '#{' in CQL: " + cql);
                                }

                                content = markerContent.toString();
                            }

                            // Restore brackets in property expressions such as #{ids[0]} too.
                            final String namedParameter = unmaskBrackets(extractIbatisNamedParameter(content), bracketMask);

                            if (Strings.isEmpty(namedParameter)) {
                                throw new IllegalArgumentException("Invalid named parameter: #{" + content + "}. Parameter name cannot be empty.");
                            }

                            if (localNamedParameters == null) {
                                localNamedParameters = new HashMap<>();
                            }

                            localNamedParameters.put(countOfParameter++, namedParameter);
                            sb.append(SK.QUESTION_MARK);
                            markerStart = rightBracketIndex + 1;
                            type |= 4;
                        }

                        word = word.substring(markerStart);

                        // A native named value glued to a MyBatis literal key remains in the tail
                        // ("#{k}::v" -> "::v"). Mark it so the common check rejects the mixed binding styles.
                        if (literalState[1] == 0 && (prevCurlyDepth > 0 || word.indexOf('{') >= 0)
                                && indexOfEmbeddedLiteralNamedParameter(word, prevCurlyDepth) >= 0) {
                            type |= 2;
                        }
                    } else if (embeddedMarkerIndex >= 0) {
                        // Without whitespace, a UDT field separator and its named marker are tokenized together,
                        // for example "{street::street". Preserve the first ':' and replace only the marker.
                        final int markerIndex = embeddedMarkerIndex;
                        final int parameterNameEnd = namedParameterNameEnd(word, markerIndex + 2);
                        final String namedParameter = word.substring(markerIndex + 2, parameterNameEnd);

                        if (Strings.isEmpty(namedParameter)) {
                            throw new IllegalArgumentException("Invalid named parameter in literal token: " + word);
                        }

                        if (localNamedParameters == null) {
                            localNamedParameters = new HashMap<>();
                        }

                        localNamedParameters.put(countOfParameter++, namedParameter);
                        word = word.substring(0, markerIndex + 1) + SK.QUESTION_MARK + word.substring(parameterNameEnd);
                        type |= 2;
                    } else if (!dollarQuotedToken && word.length() >= 1 && word.charAt(0) == _PREFIX_OF_NAMED_PARAMETER) {
                        // Inside a map/UDT literal, a ':' token is normally a key/value or field/value separator.
                        // A separate ':name' token immediately following that separator is nevertheless a genuine
                        // value bind marker (valid for UDT fields). Use prevCurlyDepth so a token such as ":2}" that
                        // closes an unspaced map literal is still treated as literal syntax rather than a parameter.
                        // Directly inside [...] (even one nested in braces) a ':' cannot be a map/UDT separator.
                        if (prevCurlyDepth == 0 || prevInsideBrackets || tokens.followsLiteralValueSeparator()) {
                            if (word.length() == 1) {
                                throw new IllegalArgumentException("Invalid named parameter: " + word + ". Parameter name cannot be empty.");
                            } else {
                                final int parameterNameEnd = namedParameterNameEnd(word, 1);
                                final String namedParameter = word.substring(1, parameterNameEnd);

                                if (Strings.isEmpty(namedParameter)) {
                                    throw new IllegalArgumentException("Invalid named parameter: " + word + ". Parameter name cannot be empty.");
                                }

                                if (localNamedParameters == null) {
                                    localNamedParameters = new HashMap<>();
                                }

                                localNamedParameters.put(countOfParameter++, namedParameter);

                                word = SK.QUESTION_MARK + word.substring(parameterNameEnd);

                                type |= 2;
                            }
                        }
                    }

                    if (Integer.bitCount(type) > 1) {
                        throw new IllegalArgumentException("Cannot mix parameter styles ('?', ':propName', '#{propName}') in the same CQL statement: " + cql);
                    }

                    appendRestoredCqlStrings(sb, word, dollarQuotedStrings, bracketMask);
                }

                final String normalized = stripTrailingSemicolons(Strings.stripToEmpty(sb.toString()));
                parameterizedCql = this.cql.equals(normalized) ? this.cql : normalized;
                parameterCount = countOfParameter;
            } finally {
                Objectory.recycle(sb);
            }
        } else {
            parameterizedCql = stripTrailingSemicolons(Strings.stripToEmpty(this.cql));
            parameterCount = 0;
        }

        this.namedParameters = localNamedParameters == null ? ImmutableMap.empty() : ImmutableMap.wrap(localNamedParameters);
    }

    /**
     * Updates brace/dollar-quote state and the stack of open <code>'&#123;'</code>/{@code '['} containers (innermost
     * last; brackets appear here as the characters of {@code bracketMask}, and are not tracked when it is
     * {@code null}), and reports whether any part of this token is dollar quoted.
     */
    private static boolean updateLiteralState(final int[] state, final StringBuilder openContainers, final String word, final BracketMask bracketMask) {
        boolean dollarQuotedToken = state[1] != 0;
        char quoteChar = 0; // 0 = outside any string/identifier literal; otherwise the opening quote character

        for (int i = 0, len = word.length(); i < len; i++) {
            final char ch = word.charAt(i);

            if (state[1] != 0) {
                if (ch == '$' && i + 1 < len && word.charAt(i + 1) == '$') {
                    dollarQuotedToken = true;
                    state[1] = 0;
                    i++;
                }
            } else if (quoteChar != 0) {
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
            } else if (ch == '$' && i + 1 < len && word.charAt(i + 1) == '$') {
                dollarQuotedToken = true;
                state[1] = 1;
                i++;
            } else if (ch == '{') {
                state[0]++;
                openContainers.append('{');
            } else if (ch == '}' && state[0] > 0) {
                state[0]--;
                final int index = openContainers.lastIndexOf("{");
                openContainers.setLength(index < 0 ? 0 : index); // also drops any '[' left unclosed inside the braces
            } else if (bracketMask != null && ch == bracketMask.left) {
                openContainers.append('[');
            } else if (bracketMask != null && ch == bracketMask.right && openContainers.length() > 0
                    && openContainers.charAt(openContainers.length() - 1) == '[') {
                openContainers.setLength(openContainers.length() - 1);
            }
        }

        return dollarQuotedToken;
    }

    /**
     * Finds the first ':' in an unspaced literal field/value separator plus named marker
     * ({@code field::name}).
     *
     * <p>The brace depth is tracked <i>within</i> the token, starting from {@code startDepth} (the depth
     * before this token). Testing the depth at the marker itself — rather than the depth before or after
     * the whole token — is what lets a self-contained literal such as {@code "{street::street}"}, whose
     * braces balance inside one token, be recognized just like the multi-token <code>"&#123;street::street"</code>
     * form. Braces inside quoted literals are not counted, so a string such as <code>"'a&#123;b'"</code> cannot
     * raise the depth.</p>
     *
     * @param word the token to scan
     * @param startDepth the map/UDT literal brace depth in effect before this token
     * @return the index of the separator ':' preceding the marker, or -1 if this token carries none
     */
    private static int indexOfEmbeddedLiteralNamedParameter(final String word, final int startDepth) {
        char quoteChar = 0;
        int depth = startDepth;

        for (int i = 0, len = word.length() - 1; i < len; i++) {
            final char ch = word.charAt(i);

            if (quoteChar != 0) {
                if (ch == '\\') {
                    i++;
                } else if (ch == quoteChar) {
                    if (i + 1 < word.length() && word.charAt(i + 1) == quoteChar) {
                        i++;
                    } else {
                        quoteChar = 0;
                    }
                }
            } else if (ch == '\'' || ch == '"') {
                quoteChar = ch;
            } else if (ch == '{') {
                depth++;
            } else if (ch == '}') {
                if (depth > 0) {
                    depth--;
                }
            } else if (depth > 0 && ch == ':' && word.charAt(i + 1) == ':' && i + 2 < word.length() && word.charAt(i + 2) != '}') {
                return i;
            }
        }

        return -1;
    }

    /** Reads split token spans without copying the token list or shifting its unconsumed suffix. */
    private static final class TokenReader {
        private final List<String> words;
        private int index;
        private String pendingWord;
        private int pendingStart;
        private boolean previousLiteralValueSeparator;
        private boolean lastLiteralValueSeparator;

        TokenReader(final List<String> words) {
            this.words = words;
        }

        boolean hasNext() {
            return pendingWord != null || index < words.size();
        }

        String next(final int[] literalState) {
            final String word = pendingWord == null ? words.get(index++) : pendingWord;
            final int start = pendingWord == null ? 0 : pendingStart;
            final int splitIndex = literalState[1] != 0 || word.startsWith("/*", start) ? -1 : indexOfGluedLiteralIbatisParameter(word, start, literalState[0]);
            final String result;

            if (splitIndex > start) {
                result = word.substring(start, splitIndex);
                pendingWord = word;
                pendingStart = splitIndex;
            } else {
                result = start == 0 ? word : word.substring(start);
                pendingWord = null;
            }

            previousLiteralValueSeparator = lastLiteralValueSeparator;
            final boolean endsWithSeparator = result.endsWith(PREFIX_OF_NAMED_PARAMETER);

            // Only a trailing ':' matters to the native-marker branch. Most tokens cannot change
            // a false state, so avoid classifying every ordinary token as whitespace/comment text.
            if ((lastLiteralValueSeparator || endsWithSeparator) && !isCommentOrSpaceToken(result)) {
                lastLiteralValueSeparator = endsWithSeparator;
            }

            return result;
        }

        boolean followsLiteralValueSeparator() {
            return previousLiteralValueSeparator;
        }
    }

    /**
     * Finds a MyBatis marker glued to a literal's opening brace or map/UDT field separator ({@code {street:#{street}}}): returns
     * the index of the {@code '#'} that directly follows a literal <code>&#123;</code> or a {@code ':'} at brace depth &gt; 0, or -1. As in
     * {@link #indexOfEmbeddedLiteralNamedParameter(String, int)}, the depth is tracked within the token from
     * {@code startDepth}, and quoted literals are skipped. A token that itself starts with a marker is scanned too, so
     * the value of a marker key ({@code #{k}:#{v}}) is split off as well; for a key marker spanning several tokens, the
     * token holding its closing brace (<code>TEXT&#125;:#&#123;v&#125;</code>) is scanned from a depth that still
     * counts the marker's own <code>&#123;</code>, so the {@code ':'} after that brace is at the depth of the enclosing
     * literal.
     *
     * @param word the token containing the unconsumed span
     * @param fromIndex the inclusive start of the unconsumed span
     * @param startDepth the brace depth in effect before that span
     * @return the marker's index in {@code word}, or {@code -1} if no split is needed
     */
    private static int indexOfGluedLiteralIbatisParameter(final String word, final int fromIndex, final int startDepth) {
        if (word.indexOf(LEFT_OF_IBATIS_NAMED_PARAMETER, fromIndex + 1) < 0) {
            return -1;
        }

        char quoteChar = 0;
        int depth = startDepth;

        for (int i = fromIndex, len = word.length(); i < len; i++) {
            final char ch = word.charAt(i);

            if (quoteChar != 0) {
                if (ch == '\\') {
                    i++;
                } else if (ch == quoteChar) {
                    if (i + 1 < len && word.charAt(i + 1) == quoteChar) {
                        i++;
                    } else {
                        quoteChar = 0;
                    }
                }
            } else if (ch == '\'' || ch == '"') {
                quoteChar = ch;
            } else if (ch == '{') {
                depth++;
                // Split a marker key at the opening literal brace as well as a marker value after ':'.
                // The opening brace of a MyBatis property is not itself a literal boundary.
                if ((i == fromIndex || word.charAt(i - 1) != '#') && word.startsWith(LEFT_OF_IBATIS_NAMED_PARAMETER, i + 1)) {
                    return i + 1;
                }
            } else if (ch == '}') {
                if (depth > 0) {
                    depth--;
                }
            } else if (depth > 0 && ch == ':' && word.startsWith(LEFT_OF_IBATIS_NAMED_PARAMETER, i + 1)) {
                return i + 1;
            }
        }

        return -1;
    }

    /** Excludes braces that SqlParser may leave attached to a named marker at the end of a UDT token. */
    private static int namedParameterNameEnd(final String word, final int parameterNameStart) {
        int end = word.length();

        while (end > parameterNameStart && word.charAt(end - 1) == '}') {
            end--;
        }

        return end;
    }

    /**
     * Removes CQL {@code //} line comments before tokenization. SqlParser already removes {@code --} and block
     * comments, but treats the two slash characters as ordinary tokens, which otherwise hides a leading DML keyword.
     * Existing quoted literals, {@code --} comments, and block comments are copied verbatim so slash pairs inside
     * those regions are never mistaken for the start of a {@code //} comment.
     */
    private static String removeDoubleSlashComments(final String cql) {
        if (!cql.contains("//")) {
            return cql;
        }

        final StringBuilder result = new StringBuilder(cql.length());

        for (int i = 0, len = cql.length(); i < len; i++) {
            final char ch = cql.charAt(i);

            if (ch == '\'' || ch == '"') {
                final char quoteChar = ch;
                result.append(ch);

                for (i++; i < len; i++) {
                    final char quotedChar = cql.charAt(i);
                    result.append(quotedChar);

                    if (quotedChar == quoteChar) {
                        if (i + 1 < len && cql.charAt(i + 1) == quoteChar) {
                            result.append(cql.charAt(++i));
                        } else {
                            break;
                        }
                    }
                }
            } else if (ch == '$' && i + 1 < len && cql.charAt(i + 1) == '$') {
                final int closingIndex = cql.indexOf("$$", i + 2);
                final int end = closingIndex < 0 ? len : closingIndex + 2;
                result.append(cql, i, end);
                i = end - 1;
            } else if (ch == '-' && i + 1 < len && cql.charAt(i + 1) == '-') {
                final int start = i;
                i += 2;

                while (i < len && cql.charAt(i) != '\r' && cql.charAt(i) != '\n') {
                    i++;
                }

                result.append(cql, start, i);

                if (i < len) {
                    result.append(cql.charAt(i));
                }
            } else if (ch == '/' && i + 1 < len && cql.charAt(i + 1) == '*') {
                final int closingIndex = cql.indexOf("*/", i + 2);
                final int end = closingIndex < 0 ? len : closingIndex + 2;
                result.append(cql, i, end);
                i = end - 1;
            } else if (ch == '/' && i + 1 < len && cql.charAt(i + 1) == '/') {
                i += 2;

                while (i < len && cql.charAt(i) != '\r' && cql.charAt(i) != '\n') {
                    i++;
                }

                if (i < len) {
                    result.append(cql.charAt(i));
                }
            } else {
                result.append(ch);
            }
        }

        return result.toString();
    }

    /**
     * Replaces every complete dollar-quoted string constant ({@code $$...$$}) with a numeric placeholder
     * ({@code $$0$$}, {@code $$1$$}, ...) and appends the original constants, in order, to
     * {@code dollarQuotedStrings}. The placeholders contain no separator, quote or comment characters, so
     * SqlParser keeps each one as a single token that {@link #updateLiteralState(int[], StringBuilder, String, BracketMask)}
     * still recognizes as dollar quoted. Backslashes in ordinary quoted literals/identifiers are doubled temporarily
     * because SqlParser recognizes backslash escaping while CQL does not. If {@code bracketMask} is not {@code null},
     * every {@code [} / {@code ]} outside quoted regions, dollar-quoted constants and comments is replaced by its
     * left / right character. A {@code $$} inside an ordinary quoted region or comment is left untouched, and an
     * unterminated {@code $$} is left as-is.
     * {@code //} comments must already have been removed (see {@link #removeDoubleSlashComments(String)}).
     */
    private static String maskCqlStrings(final String cql, final List<String> dollarQuotedStrings, final BracketMask bracketMask) {
        final boolean maskBrackets = bracketMask != null;

        if (!maskBrackets && !cql.contains("$$") && cql.indexOf('\\') < 0) {
            return cql;
        }

        final StringBuilder result = new StringBuilder(cql.length());

        for (int i = 0, len = cql.length(); i < len; i++) {
            final char ch = cql.charAt(i);

            if (ch == '\'' || ch == '"') {
                final char quoteChar = ch;
                result.append(ch);

                for (i++; i < len; i++) {
                    final char quotedChar = cql.charAt(i);
                    result.append(quotedChar);

                    if (quotedChar == '\\') {
                        result.append('\\');
                    } else if (quotedChar == quoteChar) {
                        if (i + 1 < len && cql.charAt(i + 1) == quoteChar) {
                            result.append(cql.charAt(++i));
                        } else {
                            break;
                        }
                    }
                }
            } else if (ch == '$' && i + 1 < len && cql.charAt(i + 1) == '$') {
                final int closingIndex = cql.indexOf("$$", i + 2);

                if (closingIndex < 0) {
                    result.append(cql, i, len);
                    break;
                }

                result.append("$$").append(dollarQuotedStrings.size()).append("$$");
                dollarQuotedStrings.add(cql.substring(i, closingIndex + 2));
                i = closingIndex + 1;
            } else if (ch == '-' && i + 1 < len && cql.charAt(i + 1) == '-') {
                final int start = i;

                while (i < len && cql.charAt(i) != '\r' && cql.charAt(i) != '\n') {
                    i++;
                }

                result.append(cql, start, i);

                if (i < len) {
                    result.append(cql.charAt(i));
                }
            } else if (ch == '/' && i + 1 < len && cql.charAt(i + 1) == '*') {
                final int closingIndex = cql.indexOf("*/", i + 2);
                final int end = closingIndex < 0 ? len : closingIndex + 2;
                result.append(cql, i, end);
                i = end - 1;
            } else if (maskBrackets && ch == '[') {
                result.append(bracketMask.left);
            } else if (maskBrackets && ch == ']') {
                result.append(bracketMask.right);
            } else {
                result.append(ch);
            }
        }

        return result.toString();
    }

    /**
     * Appends a token to {@code result}, reversing {@link #maskCqlStrings(String, List, BracketMask)}: restores
     * dollar-quoted constants and masked brackets, and removes the extra backslash from every encoded pair
     * inside an ordinary quoted region.
     * Placeholder-like text inside an ordinary quoted region, and comment tokens (retained only in "-- Keep comments"
     * mode), are left untouched.
     */
    private static void appendRestoredCqlStrings(final StringBuilder result, final String word, final List<String> dollarQuotedStrings,
            final BracketMask bracketMask) {
        final boolean maskBrackets = bracketMask != null;

        if (isCommentOrSpaceToken(word) || (word.indexOf("$$") < 0 && word.indexOf('\\') < 0
                && !(maskBrackets && (word.indexOf(bracketMask.left) >= 0 || word.indexOf(bracketMask.right) >= 0)))) {
            result.append(word);
            return;
        }

        char quoteChar = 0;

        for (int i = 0, len = word.length(); i < len; i++) {
            final char ch = word.charAt(i);

            if (quoteChar != 0) {
                result.append(ch);

                if (ch == '\\' && i + 1 < len && word.charAt(i + 1) == '\\') {
                    i++;
                } else if (ch == quoteChar) {
                    if (i + 1 < len && word.charAt(i + 1) == quoteChar) {
                        result.append(word.charAt(++i));
                    } else {
                        quoteChar = 0;
                    }
                }
            } else if (ch == '\'' || ch == '"') {
                quoteChar = ch;
                result.append(ch);
            } else if (ch == '$' && i + 1 < len && word.charAt(i + 1) == '$') {
                int digitEnd = i + 2;

                // At most 9 digits, so Integer.parseInt cannot overflow.
                while (digitEnd < len && digitEnd - i < 11 && word.charAt(digitEnd) >= '0' && word.charAt(digitEnd) <= '9') {
                    digitEnd++;
                }

                final int index = digitEnd > i + 2 && digitEnd + 1 < len && word.charAt(digitEnd) == '$' && word.charAt(digitEnd + 1) == '$'
                        ? Integer.parseInt(word, i + 2, digitEnd, 10)
                        : -1;

                if (index >= 0 && index < dollarQuotedStrings.size()) {
                    result.append(dollarQuotedStrings.get(index));
                    i = digitEnd + 1;
                } else {
                    result.append(ch);
                }
            } else if (maskBrackets && ch == bracketMask.left) {
                result.append('[');
            } else if (maskBrackets && ch == bracketMask.right) {
                result.append(']');
            } else {
                result.append(ch);
            }
        }
    }

    /** Replaces the masked bracket characters in {@code text} with {@code [} / {@code ]}; returns it as-is if {@code bracketMask} is {@code null}. */
    private static String unmaskBrackets(final String text, final BracketMask bracketMask) {
        return bracketMask == null ? text : text.replace(bracketMask.left, '[').replace(bracketMask.right, ']');
    }

    /**
     * A pair of unused non-ASCII characters standing in for {@code [} / {@code ]} during tokenization,
     * plus the tokenizer that treats them as single-character separators. Private-use characters are preferred.
     */
    private static final class BracketMask {
        final char left;
        final char right;
        final SqlParser.Tokenizer tokenizer;

        BracketMask(final char left, final char right) {
            this.left = left;
            this.right = right;
            // The default separators also include PostgreSQL JSON operators that start with '?' ("?-", "?|", "?&",
            // "?#", "?-|", "?||"). CQL has none of them, and keeping them would glue a bind marker to the next
            // character, so that "a = ?-1" yields the token "?-" and the marker is not counted.
            // CQL's slice operator ".." (for example "m[:from..:to]") is added as a separator: '.' and ':' are not
            // separators, so ":from..:to" would otherwise be ONE token, read as a single marker named "from..:to".
            this.tokenizer = SqlParser.tokenizer(CQL_TOKENIZER.tokenizerConfig().toBuilder().withSeparator(left).withSeparator(right).build());
        }

        /**
         * Returns the cached default mask, or, if the statement already contains one of its characters (anywhere,
         * including literals and comments), a mask built from the first two usable characters the statement does not
         * contain, so that a real character is never mistaken for a masked bracket on restore. Private-use characters
         * are tried first, then any other non-ASCII BMP character that SqlParser gives no meaning of its own (not a
         * surrogate, whitespace or identifier character). Returns {@code null} if fewer than two usable characters
         * are absent from the statement.
         */
        static BracketMask of(final String cql) {
            if (cql.indexOf(DEFAULT_MASKED_LEFT_BRACKET) < 0 && cql.indexOf(DEFAULT_MASKED_RIGHT_BRACKET) < 0) {
                return DEFAULT_BRACKET_MASK;
            }

            final BitSet used = new BitSet(Character.MAX_VALUE + 1);

            for (int i = 0, len = cql.length(); i < len; i++) {
                used.set(cql.charAt(i));
            }

            char left = 0;

            for (int ch = DEFAULT_MASKED_LEFT_BRACKET; ch <= LAST_PRIVATE_USE_CHAR; ch++) {
                if (!used.get(ch)) {
                    if (left == 0) {
                        left = (char) ch;
                    } else {
                        return new BracketMask(left, (char) ch);
                    }
                }
            }

            for (int ch = 0x80; ch <= Character.MAX_VALUE; ch++) {
                if ((ch < DEFAULT_MASKED_LEFT_BRACKET || ch > LAST_PRIVATE_USE_CHAR) && !used.get(ch) && isUsableMaskChar((char) ch)) {
                    if (left == 0) {
                        left = (char) ch;
                    } else {
                        return new BracketMask(left, (char) ch);
                    }
                }
            }

            return null;
        }

        // A mask character must stay a lone separator token: SqlParser joins identifier characters into words (and
        // e.g. only accepts '?' as a marker when no identifier character follows it) and skips whitespace.
        private static boolean isUsableMaskChar(final char ch) {
            return !Character.isSurrogate(ch) && !Character.isWhitespace(ch) && !Character.isSpaceChar(ch) && !Character.isUnicodeIdentifierPart(ch);
        }
    }

    /**
     * Rules out DML for ordinary leading words such as CREATE/ALTER without tokenizing the body.
     * Possible DML initials and unusual prefixes are left to the full tokenizer. This deliberately
     * avoids scanning/comparing the keyword twice on the common DML path.
     */
    private static boolean hasNonOperationPrefix(final String cql) {
        int start = 0;
        final int len = cql.length();

        while (start < len) {
            final char ch = cql.charAt(start);

            if (ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z') {
                // Initials of SELECT, INSERT, UPDATE, DELETE, MERGE and BEGIN. Returning false only means "not ruled
                // out": the statement is then tokenized and classified by isOpSqlPrefix, which also rejects other
                // keywords that start with one of these letters (for example DROP and USE).
                return switch (ch) {
                    case 'S', 's', 'I', 'i', 'U', 'u', 'D', 'd', 'M', 'm', 'B', 'b' -> false;
                    default -> true;
                };
            } else if (Character.isWhitespace(ch)) {
                start++;
            } else if (cql.startsWith("--", start) || cql.startsWith("//", start)) {
                start += 2;

                while (start < len && cql.charAt(start) != '\r' && cql.charAt(start) != '\n') {
                    start++;
                }
            } else if (cql.startsWith("/*", start)) {
                final int end = cql.indexOf("*/", start + 2);

                if (end < 0) {
                    return true;
                }

                start = end + 2;
            } else {
                return false;
            }
        }

        return true;
    }

    /**
     * Returns {@code true} if the first non-comment, non-whitespace token of the parsed statement is one of the
     * recognized data-operation keywords ({@code SELECT}, {@code INSERT}, {@code UPDATE}, {@code DELETE},
     * {@code MERGE}, or {@code BEGIN} for a Cassandra batch). Only such statements have their parameter
     * placeholders detected and normalized. Other statements (DDL such as
     * {@code CREATE}/{@code ALTER}/{@code DROP}) still receive statement-level normalization,
     * but their contents are not scanned for placeholders.
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

    /** Returns {@code true} if the token is blank or starts a comment ({@code --}, {@code //}, or a block comment). */
    private static boolean isCommentOrSpaceToken(final String word) {
        return Strings.isBlank(word) || word.startsWith("--") || word.startsWith("//") || word.startsWith("/*");
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
     * Parses a CQL statement, reusing a cached result when available.
     *
     * <p>Inputs of at most {@value #MAX_CACHEABLE_CQL_LENGTH} UTF-16 code units are eligible for the
     * 1,000-entry cache. The length check and cache lookup use the raw input, including surrounding
     * whitespace. Longer inputs are parsed directly without a cache lookup or insertion.</p>
     *
     * <p>On a cache hit, this method returns the retained, unexpired instance. On a miss, it parses
     * the input and attempts to cache the new result. Concurrent misses for identical text may return
     * distinct but equal instances; eviction and expiration can also cause reparsing. Callers must not
     * rely on reference identity. See {@link #equals(Object)} for value equality.</p>
     *
     * <p>Empty and whitespace-only inputs are accepted and produce empty normalized CQL with zero parameters.
     * Recognized data operations are processed as described in {@link #parameterizedCql()}; this method
     * does not validate the full CQL grammar.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ParsedCql parsed = ParsedCql.parse("SELECT * FROM users WHERE id = :userId;");
     * parsed.parameterizedCql();       // "SELECT * FROM users WHERE id = ?"
     * parsed.parameterCount();         // 1
     * parsed.namedParameters().get(0); // "userId"
     *
     * ParsedCql.parse(null);                                    // throws IllegalArgumentException
     * ParsedCql.parse("SELECT * FROM u WHERE a = ? AND b = :x"); // throws: mixed parameter styles
     * ParsedCql.parse("SELECT * FROM u WHERE id = #{}");         // throws: empty parameter name
     * }</pre>
     *
     * @param cql the non-null CQL text; the untrimmed string is used as the cache key when eligible
     * @return an immutable parsed result, newly created or retrieved from the cache
     * @throws IllegalArgumentException if {@code cql} is null; if a recognized data operation contains an empty
     *         named placeholder, mixed parameter styles, or a MyBatis placeholder missing its closing brace;
     *         or if brackets require masking but no unused pair of mask characters is available
     */
    public static ParsedCql parse(final String cql) throws IllegalArgumentException {
        N.checkArgNotNull(cql, cs.cql);

        // Admission: an oversized input is parsed directly and is neither looked up nor retained.
        if (cql.length() > MAX_CACHEABLE_CQL_LENGTH) {
            return new ParsedCql(cql);
        }

        ParsedCql result = null;

        final PoolableAdapter<ParsedCql> w = pool.get(cql);

        if (w != null) {
            result = w.value();
        }

        if (result != null) {
            return result;
        }

        result = new ParsedCql(cql);

        pool.put(cql, Poolable.wrap(result, LIVE_TIME, MAX_IDLE_TIME));

        return result;
    }

    /**
     * Returns the original CQL statement (with leading and trailing whitespace trimmed).
     *
     * <p>This method returns the CQL statement as it was provided to the parser, after
     * applying {@link String#trim()} to the input. Internal formatting,
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
     * }</pre>
     *
     * @return the trimmed original CQL statement string
     */
    public String originalCql() {
        return cql;
    }

    /**
     * Returns the normalized CQL text with recognized named placeholders replaced by {@code ?}.
     *
     * <p>This method returns the processed version of the CQL statement where:</p>
     * <ul>
     * <li>For recognized data operations, named parameters ({@code :name}) are converted to {@code ?}</li>
     * <li>For recognized data operations, MyBatis-style parameters ({@code #{name}}) are converted to {@code ?}</li>
     * <li>For recognized data operations, comments are removed and each run of whitespace outside quoted
     *     literals, quoted identifiers, and dollar-quoted ({@code $$...$$}) string constants is collapsed to a single space; the
     *     contents of these quoted regions are kept verbatim (a statement that starts
     *     with the line {@code -- Keep comments} keeps its block comments, verbatim and unscanned)</li>
     * <li>Leading and trailing whitespace is stripped</li>
     * <li>All trailing semicolons (and any whitespace between or around them) are removed</li>
     * </ul>
     * <p>Inside CQL string literals and quoted identifiers, doubled quotes escape the quote character;
     * backslashes are ordinary characters and are preserved without escaping the following quote.</p>
     * <p>Statements whose first keyword is not a recognized data operation are not scanned for
     * placeholders; only surrounding whitespace and trailing semicolons are removed.</p>
     *
     * <p>Markers inside list literals and subscripts are handled like any other marker: for example
     * {@code l + [:a, :b]}, {@code m[:k] = :v}, the slice {@code m[:from..:to]} and {@code l + [#{a}]} become
     * {@code l + [?, ?]}, {@code m[?] = ?}, {@code m[?..?]} and {@code l + [?]}, respectively. Every
     * {@code ?} inside {@code [...]} is included in {@link #parameterCount()}.</p>
     *
     * <p>MyBatis markers inside braces are recognized after the opening brace, a field separator, whitespace, or
     * {@code ,}, including {@code {street:#{street}}} and {@code {#{k,jdbcType=VARCHAR}:#{v}}}. Markers in quoted text
     * remain unchanged, and mixing MyBatis and native parameter styles is rejected even without separating whitespace.</p>
     * <p>MyBatis property names are trimmed, and attributes after the first comma are ignored: for example,
     * {@code #{ userId, jdbcType=BIGINT }} contributes the name {@code userId} to {@link #namedParameters()}.</p>
     *
     * <p><b>Known limitation.</b> Inside braces, a native named marker ({@code :name}) is recognized only when it follows a map/UDT field
     * separator ({@code {street: :street}}, {@code {street::street}}) or sits directly inside a {@code [...]} nested in
     * the braces. A native named marker written directly after <code>&#123;</code> or after {@code ,} inside braces — for example
     * <code>tags + &#123;:tag&#125;</code> or
     * <code>&#123;:a, :b&#125;</code> — is <i>not</i> rewritten and is <i>not</i> counted: <code>&#123;</code>
     * does not start a new token, and a named marker after {@code ,} inside braces is taken for a map key. Such a
     * statement reaches the driver with the native marker still in it and will not bind correctly.
     * Binding a whole collection, for example {@code tags = tags + ?} with a {@code Set} value, avoids this
     * parser limitation.</p>
     *
     * <p>Marker recognition does not verify whether Cassandra accepts a marker in that location. Callers use
     * this text for statement preparation; grammar and server-version compatibility are outside this parser's scope.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Named parameters converted to '?', trailing ';' removed
     * ParsedCql.parse("SELECT * FROM u WHERE name = :userName AND id = :userId;").parameterizedCql();
     * // returns "SELECT * FROM u WHERE name = ? AND id = ?"
     *
     * // MyBatis-style #{name} also converted to '?'
     * ParsedCql.parse("INSERT INTO u (id, name, email) VALUES (#{id}, #{name}, #{email})").parameterizedCql();
     * // returns "INSERT INTO u (id, name, email) VALUES (?, ?, ?)"
     *
     * // Positional parameters unchanged; trailing whitespace stripped
     * ParsedCql.parse("INSERT INTO u (id, name) VALUES (?, ?)  ").parameterizedCql();
     * // returns "INSERT INTO u (id, name) VALUES (?, ?)"
     *
     * // No parameters: returned as-is (after strip/semicolon handling)
     * ParsedCql.parse("SELECT * FROM u").parameterizedCql();
     * // returns "SELECT * FROM u"
     *
     * // Comments removed and whitespace runs collapsed, except inside string constants
     * ParsedCql.parse("SELECT *\n  FROM u -- note\n WHERE a = 'x   y' AND b = $$p   q$$").parameterizedCql();
     * // returns "SELECT * FROM u WHERE a = 'x   y' AND b = $$p   q$$"
     * }</pre>
     *
     * @return the normalized CQL text; empty if the input normalizes to an empty statement
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
     * <p>Indexes are zero-based and follow placeholder occurrence order. Repeated names have a separate
     * entry at each position; map iteration order is not specified. For example, two occurrences of
     * {@code :id} produce entries {@code 0 -> "id"} and {@code 1 -> "id"}.</p>
     *
     * <p>The map is empty for positional-only statements, statements without recognized placeholders,
     * and statement types whose placeholders are not scanned.</p>
     *
     * <p>The returned map is unmodifiable; attempts to modify it throw
     * {@link UnsupportedOperationException}. It reflects the mapping computed at construction time.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<Integer, String> m1 = ParsedCql.parse("SELECT * FROM u WHERE name = :userName AND status = :status").namedParameters();
     * m1.get(0);   // returns "userName"
     * m1.get(1);   // returns "status"
     *
     * Map<Integer, String> m2 = ParsedCql.parse("INSERT INTO u (id, name, email) VALUES (#{id}, #{name}, #{email})").namedParameters();
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
     * @return an immutable map from zero-based parameter position to name, without {@code :} or {@code #{...}} delimiters;
     *         empty if no named placeholders were recognized
     */
    public ImmutableMap<Integer, String> namedParameters() {
        return namedParameters;
    }

    /**
     * Returns the number of recognized parameter occurrences in the CQL statement.
     *
     * <p>For recognized data operations, this count represents the number of parameter placeholders
     * that need to be bound when executing the statement. It includes all supported types:
     * positional ({@code ?}), named ({@code :name}), and MyBatis-style ({@code #{name}}).</p>
     * <p>Each occurrence counts separately, including repeated occurrences of the same name. Marker-like
     * text inside quoted regions and comments is excluded. See {@link #parameterizedCql()} for marker limitations.</p>
     * <p>Other statement types are not scanned and therefore report zero.</p>
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
     * @return the number of recognized placeholders, always non-negative
     */
    public int parameterCount() {
        return parameterCount;
    }

    /**
     * Returns the hash code for this ParsedCql instance.
     *
     * <p>The hash code is computed from the original CQL after surrounding whitespace has been
     * trimmed, matching the definition used by {@link #equals(Object)}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ParsedCql a = ParsedCql.parse("SELECT 1 FROM t WHERE id = ?");
     * ParsedCql b = ParsedCql.parse("SELECT 1 FROM t WHERE id = ?");
     * boolean sameHash = a.hashCode() == b.hashCode(); // true (same CQL text)
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
     * <p>Two ParsedCql instances are considered equal if and only if their original CQL strings
     * are identical after leading and trailing whitespace is trimmed. Internal whitespace and
     * trailing semicolons remain significant.</p>
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
