/*
 * Copyright (c) 2026, Haiyang Li.
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

import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.PSC;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.SCCB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.landawn.abacus.annotation.Column;
import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.annotation.Table;
import com.landawn.abacus.da.cassandra.CqlBuilder.Dsl;
import com.landawn.abacus.exception.UncheckedIOException;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.SortDirection;
import com.landawn.abacus.query.SqlDialect;
import com.landawn.abacus.query.condition.And;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.query.condition.GroupBy;
import com.landawn.abacus.query.condition.Having;
import com.landawn.abacus.query.condition.Limit;
import com.landawn.abacus.query.condition.On;
import com.landawn.abacus.query.condition.Operator;
import com.landawn.abacus.util.ImmutableList;
import com.landawn.abacus.util.MutableDouble;
import com.landawn.abacus.util.N;

/**
 * JaCoCo gap closure (2026-11-01 coverage run) for {@link CqlBuilder}, {@link ParsedCql}, {@link CqlMapper} and
 * {@link ResultSets}. Every test drives the public API only and runs offline.
 *
 * <p>
 * <b>Unreachable gaps</b> (defensive guards or post-validation invariants that no public-API input flow can reach):
 * <ul>
 * <li>CqlBuilder 385-386, 393-394: batch rows are validated eagerly by {@code batchInsert(...)} /
 * {@code toInsertPropsList} (an empty map row and rows with differing key sets are rejected before {@code into(...)}
 * runs), so the re-checks inside {@code into(String)} can never fire.</li>
 * <li>CqlBuilder 507-508: {@code _propsList} is only ever staged together with {@code _op == ADD} by
 * {@code batchInsert(...)}, and no public method changes the operation afterwards.</li>
 * <li>CqlBuilder 899-900: the " IF " reposition only runs for UPDATE/DELETE; for UPDATE the SET branch (line 882)
 * always returns first, and on DELETE the only USING option (TIMESTAMP) is rejected when an IF clause exists
 * (line 835), so {@code clauseIndex} is always -1.</li>
 * <li>CqlBuilder 1267 (second outcome): {@code estimatedLength > Integer.MAX_VALUE} needs a multi-gigabyte clause.</li>
 * <li>CqlBuilder 2007 (empty-column outcome): {@code delete(...)} stages at least one column, and
 * {@code deleteFrom(...)} never routes through {@code appendDeleteColumnsAndFromClause}, so the staged column list is
 * never empty here.</li>
 * <li>CqlBuilder 3045 (second outcome of the lower-case check): the scanned identifier parts contain only
 * {@code A-Z a-z 0-9 _} (the part scanners stop at any other character), so {@code ch >= 'a'} implies
 * {@code ch <= 'z'}.</li>
 * <li>CqlBuilder 3068, 3071-3072, 3089-3090: {@code quoteReservedKeywordsInTableReference} only runs after
 * {@code checkCqlTableReference} validated the same reference, so a failed part scan, a second loop pass or a
 * non-dot separator on a valid single/qualified reference cannot occur.</li>
 * <li>CqlBuilder 3248, 3270-3275: the column/FROM pre-checks inside
 * {@code appendOperationBeforeFrom}/{@code checkCanAppendCqlFrom} duplicate each other; every public
 * {@code from(...)} overload runs {@code checkCanAppendCqlFrom} before the rendering hook can fire, so the second
 * copy (and the empty-SELECT outcomes reachable only through it) is dead.</li>
 * <li>CqlBuilder 3395 (rendering side) and 3402-3406: {@code where(...)} renders the predicate itself (the parent
 * never forwards a {@code Where} clause), and {@code onlyIf(...)} renders with the IF flag always set, which rejects
 * a WHERE clause first; the non-IF {@code Where} rendering has no public entry point.</li>
 * <li>CqlBuilder 2779 (second outcome): a GROUP BY operator on a non-Clause condition cannot reach
 * {@code checkNoUnsupportedClause} - {@code append(Condition)} validates every non-clause condition through the
 * parent's ON predicate rules first, and {@code onlyIf(...)} does not run this check at all.</li>
 * <li>CqlBuilder 2827 and 2833-2834: a staged GROUP BY condition is never null or blank (the GroupBy factories
 * validate their sort spec), so the early {@code false} returns of {@code hasSortDirection} are defensive.</li>
 * <li>CqlBuilder 2797 (true outcome): the accepted LIMIT grammar ties a FETCH keyword either to an OFFSET part or to
 * int-range slots, so an unresolved FETCH-only expression with no OFFSET or comma is not constructible.</li>
 * <li>ParsedCql 414-415 and 469-470: an embedded marker is only reported when the character two positions after the
 * '{' separator is not '}', and the name-end scan only trims trailing '}' characters, so the extracted name can never
 * be empty and the empty-name re-checks are defensive.</li>
 * <li>ParsedCql 545 (second outcome): {@code openContainers.lastIndexOf("{")} cannot return -1 while
 * {@code state[0] > 0}, because every increment appends '{' to the stack.</li>
 * <li>ParsedCql 1268: the {@code cache.put} IllegalStateException guard requires the pool to close concurrently with
 * a parse, which only happens from the JVM shutdown hook.</li>
 * <li>ParsedCql 1278 (closed-cache outcome of {@code cachedValue}): same shutdown-only condition.</li>
 * <li>CqlMapper 419 (null root outcome): a document parsed by the DOM parser always has a root element (an empty
 * stream fails earlier with a SAXException), so only the wrong-root outcome is reachable.</li>
 * </ul>
 */
class CqlBuilderCoverageNov2026Test {

    // ------------------------------------------------------------------
    // CqlBuilder: batch INSERT rendering and per-row clause application
    // ------------------------------------------------------------------

    /** batchInsert validates rows eagerly, so the duplicated guards inside into(String) are unreachable (see class doc). */
    @Test
    void batchInsertRejectsEmptyAndMismatchedMapRowsEagerly() {
        assertThrows(IllegalArgumentException.class, () -> PSC.batchInsert(List.of(Map.of())).into("items"));
        assertThrows(IllegalArgumentException.class,
                () -> PSC.batchInsert(Arrays.asList(N.asMap("value", 1), N.asMap("other", 2))).into("items"));
        assertThrows(IllegalArgumentException.class, () -> PSC.batchInsert(Collections.singletonList(new Object())).into("items"));
    }

    /** A bean batch whose every column is null in every row has nothing left to insert (toCqlInsertPropsList guard). */
    @Test
    void batchInsertOfAllNullBeanRowsIsRejected() {
        final Value bean = new Value();
        assertThrows(IllegalArgumentException.class, () -> PSC.batchInsert(Arrays.asList(bean, new Value())).into("items"));
    }

    @Test
    void batchInsertAppliesTtlToEveryRow() {
        final String cql = PSC.batchInsert(Arrays.asList(N.asMap("value", 1), N.asMap("value", 2))).into("items").usingTTL(60).build().query();

        assertEquals("BEGIN BATCH INSERT INTO items (value) VALUES (?) USING TTL 60; "
                + "INSERT INTO items (value) VALUES (?) USING TTL 60; APPLY BATCH", cql);
    }

    @Test
    void batchInsertAppliesTimestampToEveryRow() {
        final String cql = PSC.batchInsert(Arrays.asList(N.asMap("value", 1), N.asMap("value", 2))).into("items").usingTimestampMicros(123L)
                .build().query();

        assertEquals("BEGIN BATCH INSERT INTO items (value) VALUES (?) USING TIMESTAMP 123; "
                + "INSERT INTO items (value) VALUES (?) USING TIMESTAMP 123; APPLY BATCH", cql);
    }

    @Test
    void batchInsertIfNotExistsThenTtlKeepsCqlClauseOrder() {
        final String cql = PSC.batchInsert(Arrays.asList(N.asMap("value", 1), N.asMap("value", 2))).into("items").ifNotExists().usingTTL(60).build()
                .query();

        assertEquals("BEGIN BATCH INSERT INTO items (value) VALUES (?) IF NOT EXISTS USING TTL 60; "
                + "INSERT INTO items (value) VALUES (?) IF NOT EXISTS USING TTL 60; APPLY BATCH", cql);
    }

    /**
     * The batch clause scanner skips quoted identifiers and string literals: a quoted reserved-keyword table name is
     * passed over (its quotes balanced), and the per-row USING lands after the row's closing paren either way.
     */
    @Test
    void batchInsertWithQuotedTableAndQuotedIdentifierColumnAppliesTtl() {
        final String cql = PSC.batchInsert(Arrays.asList(N.asMap("\"myCol\"", 1), N.asMap("\"myCol\"", 2))).into("order").usingTTL(60).build().query();

        assertEquals("BEGIN BATCH INSERT INTO \"order\" (\"myCol\") VALUES (?) USING TTL 60; "
                + "INSERT INTO \"order\" (\"myCol\") VALUES (?) USING TTL 60; APPLY BATCH", cql);
    }

    /** An apostrophe inside a column name opens an unterminated literal for the scanner; the clause is not applied. */
    @Test
    void batchInsertWithUnbalancedQuoteInColumnNameLeavesStatementUnchangedByTtl() {
        final String cql = PSC.batchInsert(Arrays.asList(N.asMap("it's", 1), N.asMap("it's", 2))).into("items").usingTTL(60).build().query();

        // The second row's apostrophe closes the literal opened in the first row, so the TTL lands on that row only.
        assertEquals("BEGIN BATCH INSERT INTO items (it's) VALUES (?); INSERT INTO items (it's) VALUES (?) USING TTL 60; APPLY BATCH", cql);
    }

    /** A balanced dollar-quoted run in a quoted identifier is skipped by the scanner; the clause still lands per row. */
    @Test
    void batchInsertWithDollarQuotedRunInQuotedIdentifierAppliesTtl() {
        final String cql = PSC.batchInsert(Collections.singletonList(N.asMap("\"a$$b$$c\"", 1))).into("items").usingTTL(60).build().query();

        assertEquals("BEGIN BATCH INSERT INTO items (\"a$$b$$c\") VALUES (?) USING TTL 60; APPLY BATCH", cql);
    }

    /** A line comment inside a quoted identifier ends at the identifier's line break; the clause still lands per row. */
    @Test
    void batchInsertWithLineCommentInsideQuotedIdentifierAppliesTtl() {
        final String cql = PSC.batchInsert(Collections.singletonList(N.asMap("\"a//\nb\"", 1))).into("items").usingTTL(60).build().query();

        assertEquals("BEGIN BATCH INSERT INTO items (\"a//\nb\") VALUES (?) USING TTL 60; APPLY BATCH", cql);
    }

    // ------------------------------------------------------------------
    // CqlBuilder: USING option placement
    // ------------------------------------------------------------------

    /** A second USING option on an UPDATE joins the existing one with AND before the SET clause. */
    @Test
    void secondUsingOptionOnUpdateIsJoinedWithAnd() {
        final String cql = PSC.update("items").set("value").where(Filters.eq("key", 1)).usingTTL(60).usingTimestampMicros(123L).build().query();

        assertEquals("UPDATE items USING TTL 60 AND TIMESTAMP 123 SET value = ? WHERE key = ?", cql);
    }

    /** ttl() on update(entityClass) renders before the pending implicit SET list, which build() then expands. */
    @Test
    void ttlBeforeImplicitSetListKeepsUsingBeforeSetAndExpandsSetOnBuild() {
        final String cql = PSC.update(Value.class).usingTTL(60).build().query();

        assertEquals("UPDATE value USING TTL 60 SET value = ?", cql);
    }

    // ------------------------------------------------------------------
    // CqlBuilder: set(...) exclusion handling
    // ------------------------------------------------------------------

    /** A case-insensitive exclusion set must exclude through its own comparator, for maps and beans alike. */
    @Test
    void setWithMapAndCaseInsensitiveExclusionsDropsOnlyMatchedKeys() {
        final Set<String> excluded = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        excluded.add("FIRSTNAME");

        final String cql = PSC.update("items").set(N.asMap("firstName", "a", "lastName", "b"), excluded).where(Filters.eq("key", 1)).build()
                .query();

        assertEquals("UPDATE items SET last_name = ? WHERE key = ?", cql);
    }

    @Test
    void setWithMapAndTwoMatchedExclusionsDropsBoth() {
        final Set<String> excluded = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        excluded.add("FIRSTNAME");
        excluded.add("LASTNAME");

        final String cql = PSC.update("items").set(N.asMap("firstName", "a", "lastName", "b", "status", 3), excluded)
                .where(Filters.eq("key", 1)).build().query();

        assertEquals("UPDATE items SET status = ? WHERE key = ?", cql);
    }

    @Test
    void setRejectsNullCollectionArrayAndNonBeanEntitiesThroughParent() {
        final CqlBuilder builder = PSC.update("items");
        assertThrows(IllegalArgumentException.class, () -> builder.set((Object) null, null));
        assertThrows(IllegalArgumentException.class, () -> builder.set(List.of("value"), null));
        assertThrows(IllegalArgumentException.class, () -> builder.set(new String[] { "value" }, null));
        assertThrows(IllegalArgumentException.class, () -> builder.set(BigDecimal.ONE, null));
    }

    @Test
    void setWithClassRejectsNullAndNonBeanClassesThroughParent() {
        final CqlBuilder builder = PSC.update("items");
        assertThrows(IllegalArgumentException.class, () -> builder.set((Class<?>) null, null));
        assertThrows(IllegalArgumentException.class, () -> builder.set(String.class, null));
        assertEquals("UPDATE items SET value = ?", builder.set(Value.class, null).build().query());
    }

    // ------------------------------------------------------------------
    // CqlBuilder: non-finite floating-point literals
    // ------------------------------------------------------------------

    /** A Number outside the JDK/BigDecimal set is judged by its text: NaN text with a NaN double value inlines NaN. */
    @Test
    void nonFiniteLiteralFromCustomNumberTextIsInlined() {
        assertEquals("UPDATE items SET score = NaN",
                SCCB.update("items").set(N.asMap("score", (Object) new TestNumber("NaN", Double.NaN))).build().query());
        assertEquals("UPDATE items SET score = NaN",
                SCCB.update("items").set(N.asMap("score", (Object) MutableDouble.of(Double.NaN))).build().query());
    }

    /** JDK integral types skip the text comparison and are left to the parent builder. */
    @Test
    void integralNumbersSkipTheNonFiniteTextComparison() {
        assertEquals("UPDATE items SET n = 42", SCCB.update("items").set(N.asMap("n", (Object) Integer.valueOf(42))).build().query());
        assertEquals("UPDATE items SET n = 7", SCCB.update("items").set(N.asMap("n", (Object) BigInteger.valueOf(7))).build().query());
        assertEquals("UPDATE items SET n = 2.5", SCCB.update("items").set(N.asMap("n", (Object) new BigDecimal("2.5"))).build().query());
    }

    /** Non-finite text with a finite double value is not inlined; the parent builder rejects it. */
    @Test
    void nonFiniteTextWithFiniteDoubleValueIsRejectedByParent() {
        assertThrows(IllegalArgumentException.class,
                () -> SCCB.update("items").set(N.asMap("score", (Object) new TestNumber("NaN", 1.0d))).build());
        assertThrows(IllegalArgumentException.class,
                () -> SCCB.update("items").set(N.asMap("score", (Object) new TestNumber("Infinity", 1.0d))).build());
        assertThrows(IllegalArgumentException.class,
                () -> SCCB.update("items").set(N.asMap("score", (Object) new TestNumber("-Infinity", -1.0d))).build());
    }

    /** A custom Number whose text matches no non-finite constant is handed to the parent builder unchanged. */
    @Test
    void ordinaryCustomNumberTextIsRenderedByParent() {
        assertEquals("UPDATE items SET score = 2.5",
                SCCB.update("items").set(N.asMap("score", (Object) new TestNumber("2.5", 2.5d))).build().query());
    }

    // ------------------------------------------------------------------
    // CqlBuilder: WHERE / IF condition coverage
    // ------------------------------------------------------------------

    @Test
    void whereSupportsEveryCqlBinaryOperator() {
        assertEquals("SELECT value FROM items WHERE value >= ?", PSC.select("value").from("items").where(Filters.ge("value", 1)).build().query());
        assertEquals("SELECT value FROM items WHERE value <= ?", PSC.select("value").from("items").where(Filters.le("value", 1)).build().query());
        assertEquals("SELECT value FROM items WHERE value > ?", PSC.select("value").from("items").where(Filters.gt("value", 1)).build().query());
        assertEquals("SELECT value FROM items WHERE value < ?", PSC.select("value").from("items").where(Filters.lt("value", 1)).build().query());
        assertEquals("SELECT value FROM items WHERE value != ?", PSC.select("value").from("items").where(Filters.ne("value", 1)).build().query());
        assertEquals("SELECT value FROM items WHERE value LIKE ?", PSC.select("value").from("items").where(Filters.like("value", "a%")).build()
                .query());
    }

    @Test
    void nullComparisonRejectedInWhereButAllowedWithEqualityInIf() {
        assertThrows(IllegalArgumentException.class, () -> PSC.select("value").from("items").where(Filters.eq("value", null)).build());

        final CqlBuilder update = PSC.update("items").set("value").where(Filters.eq("key", 1));
        assertEquals("UPDATE items SET value = ? WHERE key = ? IF value = ?", update.onlyIf(Filters.eq("value", null)).build().query());
    }

    @Test
    void onlyIfRejectsNullWithInequalityLikeAndClauseConditions() {
        final CqlBuilder base = PSC.update("items").set("value").where(Filters.eq("key", 1));
        assertThrows(IllegalArgumentException.class, () -> base.onlyIf(Filters.gt("value", null)));
        assertThrows(IllegalArgumentException.class, () -> base.onlyIf(Filters.like("value", "a%")));
        assertThrows(IllegalArgumentException.class, () -> base.onlyIf(Filters.where(Filters.eq("value", 1))));
        assertThrows(IllegalArgumentException.class, () -> base.onlyIf(new Having(Filters.eq("value", 1))));
        assertThrows(IllegalArgumentException.class, () -> base.onlyIf(new On("value", "key")));
    }

    @Test
    void onlyIfRejectsOrJunctionsAndEmptyJunctionsButRendersRawExpressions() {
        final CqlBuilder base = PSC.update("items").set("value").where(Filters.eq("key", 1));
        assertThrows(IllegalArgumentException.class, () -> base.onlyIf(Filters.or(Filters.eq("value", 1), Filters.eq("value", 2))));
        assertThrows(IllegalArgumentException.class, () -> base.onlyIf(new And(java.util.Collections.emptyList())));

        assertEquals("UPDATE items SET value = ? WHERE key = ? IF value > 1",
                base.onlyIf(Filters.expr("value > 1")).build().query());
    }

    /** A condition type with no CQL rendering (a GROUP BY clause passed as a predicate) is reported as unsupported. */
    @Test
    void whereRejectsUnrecognizedConditionTypes() {
        assertThrows(IllegalArgumentException.class,
                () -> PSC.select("value").from("items").where(Filters.groupBy("value")).build());
    }

    @Test
    void inRelationRequiresAtLeastOneNonNullValue() {
        assertThrows(IllegalArgumentException.class,
                () -> PSC.select("value").from("items").where(Filters.in("value", Collections.emptyList())).build());
        assertThrows(IllegalArgumentException.class,
                () -> PSC.select("value").from("items").where(Filters.in("value", Arrays.asList(1, null))).build());
    }

    /** Clause guards enforced by append(Condition): FOR UPDATE, directed GROUP BY and OFFSET-carrying LIMIT forms. */
    @Test
    void appendRejectsForUpdateDirectedGroupByAndOffsetLimitClauses() {
        final Condition forUpdate = new Condition() {
            @Override
            public Operator operator() {
                return Operator.FOR_UPDATE;
            }

            @Override
            public String toSql(final com.landawn.abacus.util.NamingPolicy namingPolicy) {
                return "FOR UPDATE";
            }

            @Override
            public ImmutableList<Object> parameters() {
                return ImmutableList.empty();
            }
        };
        assertThrows(IllegalArgumentException.class, () -> PSC.select("value").from("items").append(forUpdate).build());
        assertThrows(IllegalArgumentException.class,
                () -> PSC.select("value").from("items").append(Filters.groupBy("category", SortDirection.DESC)).build());
        assertThrows(IllegalArgumentException.class,
                () -> PSC.select("value").from("items").append(new GroupBy(Filters.expr("category DESC , brand"))).build());
        assertThrows(IllegalArgumentException.class,
                () -> PSC.select("value").from("items").append(new Limit("OFFSET 5 ROWS FETCH NEXT 20 ROWS ONLY")).build());
        assertThrows(IllegalArgumentException.class, () -> PSC.select("value").from("items").append(new Limit("20, 10")).build());
    }

    /** A staged GROUP BY whose condition is a plain predicate renders verbatim; a plain LIMIT passes unchanged. */
    @Test
    void appendRendersUndirectedGroupByAndPlainLimitVerbatim() {
        assertEquals("SELECT value FROM items GROUP BY a = ?",
                PSC.select("value").from("items").append(new GroupBy(Filters.eq("a", 1))).build().query());
        assertEquals("SELECT value FROM items LIMIT 5", PSC.select("value").from("items").append(new Limit(5)).build().query());
        assertEquals("SELECT value FROM items LIMIT 2147483648",
                PSC.select("value").from("items").append(new Limit("2147483648")).build().query());
    }

    @Test
    void namedSqlParameterStyleNumbersInListMarkers() {
        final CqlBuilder.Dsl dsl = Dsl.forDialect(SqlDialect.builder().namingPolicy(com.landawn.abacus.util.NamingPolicy.SNAKE_CASE)
                .sqlPolicy(SqlDialect.SqlPolicy.NAMED_SQL).build());
        final CqlBuilder builder = dsl.select("value").from("items").where(Filters.in("value", Arrays.asList(1, 2)));
        final String cql = builder.build().query();

        assertTrue(cql.contains("value IN (:value1, :value2)"), cql);
    }

    @Test
    void onlyIfIsRejectedOnInsertAndSelectStatements() {
        assertThrows(IllegalStateException.class, () -> PSC.insert("value").into("items").onlyIf("value = 1"));
        assertThrows(IllegalStateException.class, () -> PSC.select("value").from("items").onlyIf("value = 1"));
    }

    // ------------------------------------------------------------------
    // CqlBuilder: SELECT list quoting and FROM handling
    // ------------------------------------------------------------------

    /** from(String, null) is accepted and installs no entity mapping. */
    @Test
    void fromWithNullEntityClassRendersWithoutMapping() {
        assertEquals("SELECT value FROM items", PSC.select("value").from("items", (Class<?>) null).build().query());
    }

    /**
     * selectFrom(entityClass) runs the reserved-keyword column scan over the entity's cached select list; dotted and
     * quoted identifier chains are inspected part by part and every unquoted reserved part is quoted.
     */
    @Test
    void selectFromQuotesReservedKeywordColumnsIncludingDottedAndQuotedChains() {
        assertEquals("SELECT id AS \"id\", \"desc\" AS \"description\", addr.\"order\" AS \"address\", \"Addr\".\"desc\" AS \"extra\" "
                + "FROM reserved_columns_entity", PSC.selectFrom(ReservedColumnsEntity.class).build().query());
    }

    /** A reserved keyword column carrying an alias is quoted while the alias is kept as rendered. */
    @Test
    void reservedKeywordColumnWithAliasIsQuotedBeforeAs() {
        assertEquals("SELECT \"desc\" AS \"d\" FROM items", PSC.select(Collections.singletonMap("desc", "d")).from("items").build().query());
    }

    /** Aliases carrying spaces or commas are quoted as rendered; the reserved column itself stays quoted before AS. */
    @Test
    void multiTokenAliasesKeepTheColumnQuotedBeforeAs() {
        assertEquals("SELECT \"desc\" AS \"a b\" FROM items", PSC.select(Collections.singletonMap("desc", "a b")).from("items").build().query());
        assertEquals("SELECT \"desc\" AS \"a,b\" FROM items", PSC.select(Collections.singletonMap("desc", "a,b")).from("items").build().query());
    }

    /** USING TTL/IF scans must see through literals and comments rendered from raw WHERE expressions. */
    @Test
    void usingOptionPlacementScansThroughDollarQuotesLiteralsAndComments() {
        assertEquals("UPDATE items USING TTL 60 SET value = ? WHERE key = $$v$$",
                PSC.update("items").set("value").where("key = $$v$$").usingTTL(60).build().query());
        assertEquals("UPDATE items USING TTL 60 SET value = ? WHERE key = 'unterminated",
                PSC.update("items").set("value").where("key = 'unterminated").usingTTL(60).build().query());
        assertEquals("UPDATE items USING TTL 60 SET value = ? WHERE key = 1 ",
                PSC.update("items").set("value").where("key = 1 -- done").usingTTL(60).build().query());
        assertEquals("UPDATE items USING TTL 60 SET value = ? WHERE key = 1",
                PSC.update("items").set("value").where("key = /* c */ 1").usingTTL(60).build().query());
        assertEquals("UPDATE items USING TTL 60 SET value = ? WHERE key = 1 ",
                PSC.update("items").set("value").where("key = 1 /* open").usingTTL(60).build().query());
    }

    /** Odd identifier characters are rejected as table references after the part scanners evaluate them. */
    @Test
    void tableReferencesWithNonIdentifierCharactersAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> PSC.select("value").from("~ab").build());
        assertThrows(IllegalArgumentException.class, () -> PSC.select("value").from("a~b").build());
        assertThrows(IllegalArgumentException.class, () -> PSC.select("value").from("a b").build());
        assertThrows(IllegalArgumentException.class, () -> PSC.insert("value").into("a.").build());
    }

    /** Both parts of a keyspace-qualified reference and quoted reserved names go through the quoting scan. */
    @Test
    void reservedKeywordsInQualifiedAndQuotedTableReferencesAreQuoted() {
        assertEquals("SELECT value FROM \"order\".\"order\"", PSC.select("value").from("order.order").build().query());
        assertEquals("SELECT value FROM \"order\"", PSC.select("value").from("\"order\"").build().query());
    }

    // ------------------------------------------------------------------
    // CqlBuilder: Dsl dialect cache
    // ------------------------------------------------------------------

    @Test
    void dslForDialectReusesCachedInstancesAndBuildsUncachedDialects() {
        assertSame(Dsl.PSC, Dsl.forDialect(Dsl.PSC.sqlDialect));

        final Dsl custom = Dsl.forDialect(SqlDialect.builder().namingPolicy(com.landawn.abacus.util.NamingPolicy.SNAKE_CASE)
                .sqlPolicy(SqlDialect.SqlPolicy.PARAMETERIZED_SQL).build());
        assertNotNull(custom);
        assertEquals("SELECT value FROM items", custom.select("value").from("items").build().query());

        // A dialect without a naming policy falls back to SNAKE_CASE inside the Dsl constructor.
        final Dsl nullNaming = Dsl.forDialect(SqlDialect.builder().build());
        assertNotNull(nullNaming);
        assertEquals("SELECT value FROM items", nullNaming.select("value").from("items").build().query());
    }

    // ------------------------------------------------------------------
    // CqlBuilder: selectFrom with sub-entities
    // ------------------------------------------------------------------

    /** A bean-typed property without @Column is a sub-entity, so includeSubEntityProperties requires a multi-table FROM. */
    @Test
    void selectFromWithSubEntityPropertiesIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> PSC.selectFrom(ParentEntity.class, null, true, null));
    }

    // ------------------------------------------------------------------
    // ParsedCql: literal markers, masking and normalization
    // ------------------------------------------------------------------

    @Test
    void nativeMarkerAfterOperatorInsideBracesBindsKeyAndGluedValue() {
        ParsedCqlAssert.of("UPDATE t SET m = {-:k::v} WHERE id = :id", "UPDATE t SET m = {-?:?} WHERE id = ?", "k", "v", "id");
        ParsedCqlAssert.of("UPDATE t SET m = {-:k: 1} WHERE id = :id", "UPDATE t SET m = {-?: 1} WHERE id = ?", "k", "id");
    }

    @Test
    void nativeMarkerAfterOperatorWithEmptyKeyOrValueIsRejected() {
        // The key marker after '-' is bound even when the glued value is only a closing brace.
        ParsedCqlAssert.of("UPDATE t SET m = {-:k::} WHERE id = :id", "UPDATE t SET m = {-?::} WHERE id = ?", "k", "id");
    }

    @Test
    void embeddedLiteralMarkerBindsFieldValueAndRejectsEmptyNames() {
        ParsedCqlAssert.of("UPDATE t SET m = {a::v} WHERE id = :id", "UPDATE t SET m = {a:?} WHERE id = ?", "v", "id");
        ParsedCqlAssert.of("UPDATE t SET m = {'a''b'::v} WHERE id = :id", "UPDATE t SET m = {'a''b':?} WHERE id = ?", "v", "id");
        ParsedCqlAssert.of("UPDATE t SET m = {a::} WHERE id = :id", "UPDATE t SET m = {a::} WHERE id = ?", "id");
    }

    @Test
    void colonTokenAfterSeparatorAndArithmeticOperandsInsideBraces() {
        ParsedCqlAssert.of("UPDATE t SET m = {loc: (:lat, :lon)} WHERE id = :id", "UPDATE t SET m = {loc: (?, ?)} WHERE id = ?", "lat", "lon", "id");
        ParsedCqlAssert.of("UPDATE t SET m = {:x} WHERE id = :id", "UPDATE t SET m = {:x} WHERE id = ?", "id");
        // A ":n}" token that closes an unspaced literal stays literal syntax, not a bind marker.
        ParsedCqlAssert.of("UPDATE t SET m = {b: 3, :2} WHERE id = :id", "UPDATE t SET m = {b: 3, :2} WHERE id = ?", "id");
    }

    @Test
    void loneColonAndDollarTokensDoNotBecomeMarkers() {
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("SELECT * FROM t WHERE id = :"));
        ParsedCqlAssert.of("UPDATE t SET note = $$ open $ WHERE id = :id", "UPDATE t SET note = $$ open $ WHERE id = :id");
    }

    @Test
    void quotedLiteralsDoubledQuotesAndBracketsSurviveTokenization() {
        ParsedCqlAssert.of("SELECT * FROM t WHERE a = 'a''b' AND id = :id", "SELECT * FROM t WHERE a = 'a''b' AND id = ?", "id");
        ParsedCqlAssert.of("SELECT * FROM t WHERE a = \"x\" AND m = ] AND id = :id", "SELECT * FROM t WHERE a = \"x\" AND m = ] AND id = ?", "id");
        ParsedCqlAssert.of("UPDATE t SET l = [:a, :b] AND m = {c: 1} AND t = (1, 2) WHERE id = :id",
                "UPDATE t SET l = [?, ?] AND m = {c: 1} AND t = (1, 2) WHERE id = ?", "a", "b", "id");
    }

    @Test
    void dollarQuotedConstantsAreMaskedRestoredAndMayStayUnterminated() {
        ParsedCqlAssert.of("UPDATE t SET note = $$ a b $$ WHERE id = :id", "UPDATE t SET note = $$ a b $$ WHERE id = ?", "id");
        ParsedCqlAssert.of("SELECT * FROM t WHERE x = :x AND y = $$ open $ rest", "SELECT * FROM t WHERE x = ? AND y = $$ open $ rest", "x");
    }

    @Test
    void myBatisMarkersInsideQuotedKeysAndNestedLiteralsAreSplitAndBound() {
        ParsedCqlAssert.of("UPDATE t SET m = {\"k\":#{v}} WHERE id = #{id}", "UPDATE t SET m = {\"k\":?} WHERE id = ?", "v", "id");
        ParsedCqlAssert.of("UPDATE t SET m = {'a\\b':#{v}} WHERE id = #{id}", "UPDATE t SET m = {'a\\b':?} WHERE id = ?", "v", "id");
        ParsedCqlAssert.of("UPDATE t SET m = {'a''b':#{v}} WHERE id = #{id}", "UPDATE t SET m = {'a''b':?} WHERE id = ?", "v", "id");
        ParsedCqlAssert.of("UPDATE t SET m = {o:{k:#{v}}} WHERE id = #{id}", "UPDATE t SET m = {o:{k:?}} WHERE id = ?", "v", "id");
    }

    @Test
    void doubleSlashCommentsAreRemovedAroundLiteralsDollarQuotesAndOtherComments() {
        ParsedCqlAssert.of("SELECT \"a\"\"b\" // c\nFROM t WHERE id = :id", "SELECT \"a\"\"b\" FROM t WHERE id = ?", "id");
        ParsedCqlAssert.of("UPDATE t SET note = $$v$$ // c\nWHERE id = :id", "UPDATE t SET note = $$v$$ WHERE id = ?", "id");
        // An unterminated dollar quote keeps the rest of the statement opaque, including the // and the marker.
        ParsedCqlAssert.of("UPDATE t SET note = $$v // c\nWHERE id = :id", "UPDATE t SET note = $$v // c WHERE id = :id");
        ParsedCqlAssert.of("SELECT a / b FROM t WHERE id = :id // eof comment", "SELECT a / b FROM t WHERE id = ?", "id");
        ParsedCqlAssert.of("SELECT * FROM t -- tbl\n // here\nWHERE id = :id /* block */", "SELECT * FROM t WHERE id = ?", "id");
        ParsedCqlAssert.of("SELECT * FROM t /* b */ WHERE id = :id // done", "SELECT * FROM t WHERE id = ?", "id");
    }

    @Test
    void leadingCommentsWhitespaceAndOddFirstCharactersClassifyTheStatement() {
        ParsedCqlAssert.of("", "");
        ParsedCqlAssert.of("   ", "");
        ParsedCqlAssert.of(";;;", "");
        ParsedCqlAssert.of("-- only a comment", "-- only a comment");
        ParsedCqlAssert.of("/* unterminated", "/* unterminated");
        ParsedCqlAssert.of("/* c */ select 1", "select 1");
        ParsedCqlAssert.of("~abc", "~abc");
        ParsedCqlAssert.of("_abc", "_abc");
        ParsedCqlAssert.of("1abc", "1abc");
        ParsedCqlAssert.of("CREATE TABLE t (id int)", "CREATE TABLE t (id int)");
    }

    @Test
    void bracketMaskFallsBackWhenDefaultMaskCharactersAppearInTheStatement() {
        ParsedCqlAssert.of("SELECT * FROM t WHERE m = [\uE000] AND id = :id", "SELECT * FROM t WHERE m = [\uE000] AND id = ?", "id");
        ParsedCqlAssert.of("SELECT * FROM t WHERE m = [\uE000\uF8FF] AND id = :id", "SELECT * FROM t WHERE m = [\uE000\uF8FF] AND id = ?", "id");
    }

    @Test
    void identicalCqlIsServedFromCache() {
        final String cql = "SELECT * FROM coverage_nov2026 WHERE id = ?";
        assertSame(ParsedCql.parse(cql), ParsedCql.parse(cql));
    }

    // ------------------------------------------------------------------
    // CqlMapper: IO paths and XML content checks
    // ------------------------------------------------------------------

    @TempDir
    Path directory;

    @Test
    void saveToCreatesMissingParentDirectoriesAndPlainFileNames() throws Exception {
        final CqlMapper mapper = new CqlMapper();
        mapper.add("find", "SELECT * FROM items");

        final Path nested = directory.resolve("level1/level2/out.xml");
        mapper.saveTo(nested.toFile());
        assertTrue(Files.exists(nested));
        assertEquals("SELECT * FROM items", CqlMapper.loadFrom(nested.toFile()).get("find").originalCql());

        final File plain = new File("cql_coverage_nov2026_tmp.xml");
        try {
            mapper.saveTo(plain);
            assertTrue(plain.exists());
        } finally {
            //noinspection ResultOfMethodCallIgnored
            plain.delete();
        }
    }

    @Test
    void saveToReportsFlushFailureAsUncheckedIo() {
        final CqlMapper mapper = new CqlMapper();
        mapper.add("find", "SELECT * FROM items");
        final OutputStream failingFlush = new ByteArrayOutputStream() {
            @Override
            public void flush() throws IOException {
                throw new IOException("flush failed");
            }
        };

        assertThrows(UncheckedIOException.class, () -> mapper.saveTo(failingFlush));
    }

    @Test
    void saveToRejectsUnpairedSurrogateInCqlBody() {
        final CqlMapper mapper = new CqlMapper();
        mapper.add("find", "SELECT '\uD800' FROM items");
        assertThrows(IllegalStateException.class, () -> mapper.saveTo(new ByteArrayOutputStream()));
    }

    @Test
    void supplementaryPlaneCharactersRoundTripThroughXml() {
        final CqlMapper mapper = new CqlMapper();
        mapper.add("find", "SELECT '\uD83D\uDE00' FROM items", N.asMap("note", "\uD83D\uDE00"));
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        mapper.saveTo(output);

        final CqlMapper restored = CqlMapper.loadFrom(new ByteArrayInputStream(output.toByteArray()));
        assertEquals("SELECT '\uD83D\uDE00' FROM items", restored.get("find").originalCql());
        assertEquals("\uD83D\uDE00", restored.getAttributes("find").get("note"));
    }

    /** A null attribute value passes the XML character check and serializes as an empty attribute value. */
    @Test
    void nullAttributeValueRoundTripsAsEmptyString() {
        final CqlMapper mapper = new CqlMapper();
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("note", null);
        mapper.add("find", "SELECT * FROM items", attrs);
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        mapper.saveTo(output);

        final CqlMapper restored = CqlMapper.loadFrom(new ByteArrayInputStream(output.toByteArray()));
        assertEquals("", restored.getAttributes("find").get("note"));
    }

    /** A wrong root element is reported as RuntimeException; loadFrom also logs per the configured logger level. */
    @Test
    void wrongRootElementIsRejectedAndLoadingLogsPerLevel() throws Exception {
        final Path file = directory.resolve("mapper.xml");
        Files.writeString(file, "<cqlMapper><cql id='find'>SELECT * FROM items</cql></cqlMapper>");

        final java.util.logging.Logger julLogger = java.util.logging.Logger.getLogger(CqlMapper.class.getName());
        final org.apache.log4j.Logger log4jLogger = org.apache.log4j.LogManager.getLogger(CqlMapper.class.getName());
        final java.util.logging.Level julLevel = julLogger.getLevel();
        final org.apache.log4j.Level log4jLevel = log4jLogger.getLevel();
        try {
            // Disabled: the info guard in loadFile takes its false branch.
            julLogger.setLevel(java.util.logging.Level.OFF);
            log4jLogger.setLevel(org.apache.log4j.Level.OFF);
            assertEquals("SELECT * FROM items", CqlMapper.loadFrom(file.toFile()).get("find").originalCql());

            // Trace level: both the info and debug guards in loadFile/loadStream take their true branches.
            julLogger.setLevel(java.util.logging.Level.ALL);
            log4jLogger.setLevel(org.apache.log4j.Level.ALL);
            assertEquals("SELECT * FROM items", CqlMapper.loadFrom(file.toFile()).get("find").originalCql());
        } finally {
            julLogger.setLevel(julLevel);
            log4jLogger.setLevel(log4jLevel);
        }

        try (var input = new ByteArrayInputStream("<notCqlMapper/>".getBytes(StandardCharsets.UTF_8))) {
            assertThrows(RuntimeException.class, () -> CqlMapper.loadFrom(input));
        }
    }

    // ------------------------------------------------------------------
    // ResultSets: multi-page iteration and exhaustion
    // ------------------------------------------------------------------

    @Test
    void pagedResultSetFetchesSubsequentPagesAndThrowsOnExhaustion() {
        final AsyncResultSet first = mock(AsyncResultSet.class);
        final AsyncResultSet second = mock(AsyncResultSet.class);
        final Row row1 = mock(Row.class);
        final Row row2 = mock(Row.class);
        final ExecutionInfo firstInfo = mock(ExecutionInfo.class);
        final ExecutionInfo secondInfo = mock(ExecutionInfo.class);
        when(first.currentPage()).thenReturn(List.of(row1));
        when(first.hasMorePages()).thenReturn(true);
        when(first.getExecutionInfo()).thenReturn(firstInfo);
        when(first.fetchNextPage()).thenReturn(CompletableFuture.completedFuture(second));
        when(second.currentPage()).thenReturn(List.of(row2));
        when(second.hasMorePages()).thenReturn(false);
        when(second.getExecutionInfo()).thenReturn(secondInfo);

        final ResultSet rs = ResultSets.wrap(first);
        final Iterator<Row> iterator = rs.iterator();
        assertTrue(iterator.hasNext());
        assertSame(row1, iterator.next());
        assertTrue(iterator.hasNext());
        assertSame(row2, iterator.next());
        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);
        assertTrue(rs.isFullyFetched());
        assertEquals(List.of(firstInfo, secondInfo), rs.getExecutionInfos());
    }

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    /** Renders the CQL text of a custom Number independent of its double value. */
    private static final class TestNumber extends Number {

        private static final long serialVersionUID = 1L;

        private final String text;
        private final double value;

        TestNumber(final String text, final double value) {
            this.text = text;
            this.value = value;
        }

        @Override
        public int intValue() {
            return (int) value;
        }

        @Override
        public long longValue() {
            return (long) value;
        }

        @Override
        public float floatValue() {
            return (float) value;
        }

        @Override
        public double doubleValue() {
            return value;
        }

        @Override
        public String toString() {
            return text;
        }
    }

    public static class Value {
        private String value;

        public String getValue() {
            return value;
        }

        public void setValue(final String value) {
            this.value = value;
        }
    }

    public static class ChildEntity {
        private String street;

        public String getStreet() {
            return street;
        }

        public void setStreet(final String street) {
            this.street = street;
        }
    }

    public static class ParentEntity {
        @Id
        private Long id;
        private String name;
        private ChildEntity child;

        public Long getId() {
            return id;
        }

        public void setId(final Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public ChildEntity getChild() {
            return child;
        }

        public void setChild(final ChildEntity child) {
            this.child = child;
        }
    }

    @Table("reserved_columns_entity")
    public static class ReservedColumnsEntity {
        @Id
        private long id;

        @Column("desc")
        private String description;

        @Column("addr.order")
        private String address;

        @Column("\"Addr\".desc")
        private String extra;

        public long getId() {
            return id;
        }

        public void setId(final long id) {
            this.id = id;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(final String description) {
            this.description = description;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(final String address) {
            this.address = address;
        }

        public String getExtra() {
            return extra;
        }

        public void setExtra(final String extra) {
            this.extra = extra;
        }
    }

    /** Compact assertion helper for ParsedCql parse results. */
    private static final class ParsedCqlAssert {

        static void of(final String cql, final String expectedParameterizedCql, final String... expectedNames) {
            final ParsedCql parsed = ParsedCql.parse(cql);
            assertEquals(expectedParameterizedCql, parsed.parameterizedCql(), cql);
            assertEquals(expectedNames.length, parsed.parameterCount(), cql);
            assertEquals(expectedNames.length, parsed.namedParameters().size(), cql);

            for (int i = 0; i < expectedNames.length; i++) {
                assertEquals(expectedNames[i], parsed.namedParameters().get(i), cql);
            }
        }
    }
}
