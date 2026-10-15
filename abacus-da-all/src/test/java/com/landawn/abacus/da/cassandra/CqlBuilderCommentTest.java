package com.landawn.abacus.da.cassandra;

import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.SCCB;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.landawn.abacus.query.condition.SqlExpression;

class CqlBuilderCommentTest {

    @ParameterizedTest
    @MethodSource("batchExpressions")
    void batchClausesIgnoreQuotedAndCommentedSeparators(final String expression, final String expectedValue) {
        final String query = SCCB.batchInsert(List.of(Map.of("value", SqlExpression.of(expression)), Map.of("value", SqlExpression.of(expression))))
                .into("items")
                .usingTTL(60)
                .ifNotExists()
                .build()
                .query();
        final String statement = "INSERT INTO items (value) VALUES (" + expectedValue + ") IF NOT EXISTS USING TTL 60; ";
        assertEquals("BEGIN BATCH " + statement + statement + "APPLY BATCH", query);
    }

    @ParameterizedTest
    @MethodSource("insertExpressions")
    void insertClausesIgnoreQuotedAndCommentedUsingTokens(final String expression, final String expectedValue) {
        final String query = SCCB.insert(Map.of("value", SqlExpression.of(expression))).into("items").usingTTL(60).ifNotExists().build().query();
        assertEquals("INSERT INTO items (value) VALUES (" + expectedValue + ") IF NOT EXISTS USING TTL 60", query);
    }

    @ParameterizedTest
    @ValueSource(strings = { "1 /* ; USING ignored */", "1 -- ; USING ignored\n", "1 // ; USING ignored\r\n" })
    void rawTimestampCommentsDoNotSplitBatchStatementsOrHideLaterOptions(final String timestamp) {
        final String query = SCCB.batchInsert(List.of(Map.of("value", 1), Map.of("value", 2)))
                .into("items")
                .usingTimestamp(timestamp)
                .usingTTL(60)
                .build()
                .query();
        assertEquals("BEGIN BATCH INSERT INTO items (value) VALUES (1) USING TIMESTAMP " + timestamp
                + " AND TTL 60; INSERT INTO items (value) VALUES (2) USING TIMESTAMP " + timestamp + " AND TTL 60; APPLY BATCH", query);
        assertEquals("INSERT INTO items (value) VALUES (1) USING TIMESTAMP " + timestamp + " AND TTL 60",
                SCCB.insert(Map.of("value", 1)).into("items").usingTimestamp(timestamp).usingTTL(60).build().query());
    }

    private static java.util.stream.Stream<Arguments> batchExpressions() {
        return java.util.stream.Stream.of(Arguments.of("1 /* ; USING fake */", "1 "), Arguments.of("1 -- ; USING fake\n", "1 "),
                Arguments.of("$$a;b$$", "$$a;b$$"), Arguments.of("'O''Brien; USING fake'", "'O''Brien; USING fake'"));
    }

    private static java.util.stream.Stream<Arguments> insertExpressions() {
        return java.util.stream.Stream.of(Arguments.of("1 /* USING fake */", "1 "), Arguments.of("1 -- USING fake\n", "1 "),
                Arguments.of("'a''b USING fake'", "'a''b USING fake'"));
    }
}
