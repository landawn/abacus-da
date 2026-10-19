/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ParsedCqlPerformanceRegressionTest {

    @Test
    void manyGluedLiteralMarkersKeepAllNamesAndDelimiters() {
        final int fieldCount = 4_000;
        final StringBuilder query = new StringBuilder("UPDATE t SET address={");
        final StringBuilder expected = new StringBuilder("UPDATE t SET address={");

        for (int i = 0; i < fieldCount; i++) {
            if (i > 0) {
                query.append(',');
                expected.append(',');
            }

            query.append('f').append(i).append(":#{p").append(i).append('}');
            expected.append('f').append(i).append(":?");
        }

        final ParsedCql parsed = ParsedCql.parse(query.append("} WHERE id=#{id}").toString());
        assertEquals(expected.append("} WHERE id=?").toString(), parsed.parameterizedCql());
        assertEquals(fieldCount + 1, parsed.parameterCount());

        for (int i = 0; i < fieldCount; i++) {
            assertEquals("p" + i, parsed.namedParameters().get(i));
        }

        assertEquals("id", parsed.namedParameters().get(fieldCount));
    }

    @Test
    void longMyBatisMetadataAndAdjacentMarkersKeepTheirBoundaries() {
        final String metadata = "jdbcType=TEXT,".repeat(4_000);
        final ParsedCql parsed = ParsedCql.parse("SELECT * FROM t WHERE id=#{ ids[0]," + metadata + "mode=IN}#{next}#{ last }");
        assertEquals("SELECT * FROM t WHERE id=???", parsed.parameterizedCql());
        assertEquals(3, parsed.parameterCount());
        assertEquals("ids[0]", parsed.namedParameters().get(0));
        assertEquals("next", parsed.namedParameters().get(1));
        assertEquals("last", parsed.namedParameters().get(2));
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("SELECT * FROM t WHERE id=#{id," + metadata));
    }

    @Test
    void canonicalPositionalQueriesShareTheirTextAndEmptyMetadata() {
        final ParsedCql first = ParsedCql.parse("SELECT * FROM shared_text_one WHERE id = ?");
        final ParsedCql second = ParsedCql.parse("SELECT * FROM shared_text_two WHERE id = ?");
        final ParsedCql ddl = ParsedCql.parse("CREATE TABLE shared_text_ddl (id int PRIMARY KEY)");
        assertSame(first.originalCql(), first.parameterizedCql());
        assertSame(first.namedParameters(), second.namedParameters());
        assertSame(first.namedParameters(), ddl.namedParameters());
        assertTrue(first.namedParameters().isEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = { "\uE000", "\uE001", "\uE002", "\uF8FF" })
    void privateUseCharactersWithoutBracketsRemainLiteralText(final String character) {
        final ParsedCql parsed = ParsedCql.parse("SELECT '" + character + "' FROM t WHERE id = :id");
        assertEquals("SELECT '" + character + "' FROM t WHERE id = ?", parsed.parameterizedCql());
        assertEquals(1, parsed.parameterCount());
        assertEquals("id", parsed.namedParameters().get(0));
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "/* SELECT :ignored */ ", "-- SELECT :ignored\n", "// SELECT :ignored\r\n", "-- Keep comments\n/* :ignored */ " })
    void largeDdlWithLeadingCommentsRetainsItsOriginalBody(final String prefix) {
        final String query = prefix + "CREATE FUNCTION f() RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE java AS $$"
                + "return \"[:ignored] #{ignored} ?\"; ".repeat(3_000) + "$$";
        final ParsedCql parsed = ParsedCql.parse(query + ";;  ");
        assertEquals(query, parsed.parameterizedCql());
        assertEquals(0, parsed.parameterCount());
        assertTrue(parsed.namedParameters().isEmpty());
    }

    @Test
    void oversizedQueriesAreParsedWithoutBeingRetainedByTheCache() {
        final String query = "SELECT '" + "x".repeat(70_000) + "' FROM t WHERE id=:id";
        final ParsedCql first = ParsedCql.parse(query);
        final ParsedCql second = ParsedCql.parse(query);
        assertNotSame(first, second);
        assertEquals(first, second);
        assertEquals(query.replace(":id", "?"), first.parameterizedCql());
        assertEquals("id", first.namedParameters().get(0));

        // The retained key includes padding even when the trimmed query is small.
        final String padded = " ".repeat(70_000) + "SELECT * FROM padded_cache_key WHERE id=?";
        assertNotSame(ParsedCql.parse(padded), ParsedCql.parse(padded));
    }

    @Test
    void denseNamedQueriesRespectTheCacheEntryMemoryLimit() {
        final String query = "SELECT * FROM dense_cache_key WHERE id IN (" + ":id,".repeat(8_000) + ":last)";
        final ParsedCql first = ParsedCql.parse(query);
        assertNotSame(first, ParsedCql.parse(query));
        assertEquals(8_001, first.parameterCount());
        assertEquals("last", first.namedParameters().get(8_000));
    } 
}
