/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ParsedCqlDeepReview2026LiteralMarkerTest {

    @Test
    void dollarQuotedKeyDoesNotHideEmbeddedNamedValue() {
        assertParsed("UPDATE t SET m = {$$k$$::v} WHERE id = :id", "UPDATE t SET m = {$$k$$:?} WHERE id = ?", "v", "id");
        assertParsed("UPDATE t SET m = { $$k$$::v } WHERE id = :id", "UPDATE t SET m = { $$k$$:? } WHERE id = ?", "v", "id");
        assertParsed("UPDATE t SET m = {outer:{$$k$$::v}} WHERE id = :id", "UPDATE t SET m = {outer:{$$k$$:?}} WHERE id = ?", "v", "id");
        assertParsed("UPDATE t SET m = {$$a::b$$::v} WHERE id = :id", "UPDATE t SET m = {$$a::b$$:?} WHERE id = ?", "v", "id");
    }

    @Test
    void unterminatedDollarQuoteStillSuppressesEmbeddedMarkerScan() {
        assertParsed("SELECT * FROM t WHERE x = :x AND y = $$ {a::b}", "SELECT * FROM t WHERE x = ? AND y = $$ {a::b}", "x");
    }

    @Test
    void gluedNativeValueAfterMyBatisKeyIsRejectedAsMixed() {
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("UPDATE t SET m = { #{k}::v } WHERE id = #{id}"));
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("UPDATE t SET m = { #{k,jdbcType=TEXT}::v } WHERE id = #{id}"));
    }

    @ParameterizedTest
    @ValueSource(strings = { "UPDATE t SET m = {#{k}::v} WHERE id = :id", "UPDATE t SET m = {#{k}::v} WHERE id = #{id}",
            "UPDATE t SET m = {#{k,jdbcType=TEXT}::v} WHERE id = :id", "UPDATE t SET m = {#{ k }::v} WHERE id = :id",
            "UPDATE t SET m = {outer:{#{k}::v}} WHERE id = :id", "UPDATE t SET m = {#{k}:?} WHERE id = #{id}", "UPDATE t SET m = {#{k}:#{v}} WHERE id = :id",
            "UPDATE t SET m = {#{k}:#{v}} WHERE id = ?" })
    void unspacedMyBatisKeysCannotHideMixedParameterStyles(final String cql) {
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse(cql));
    }

    @Test
    void unspacedMyBatisKeysAndValuesAreAllBoundInOrder() {
        assertParsed("UPDATE t SET m = {#{k}:#{v}} WHERE id = #{id}", "UPDATE t SET m = {?:?} WHERE id = ?", "k", "v", "id");
        assertParsed("UPDATE t SET m = {#{k,jdbcType=TEXT}:#{v,jdbcType=INT}} WHERE id = #{id}", "UPDATE t SET m = {?:?} WHERE id = ?", "k", "v", "id");
        assertParsed("UPDATE t SET m = {#{ k }:#{ v }} WHERE id = #{id}", "UPDATE t SET m = {?:?} WHERE id = ?", "k", "v", "id");
        assertParsed("UPDATE t SET m = {outer:{#{k}:#{v}}} WHERE id = #{id}", "UPDATE t SET m = {outer:{?:?}} WHERE id = ?", "k", "v", "id");
        assertParsed("UPDATE t SET m = {#{k1}:#{v1},#{k2}:#{v2}} WHERE id = #{id}", "UPDATE t SET m = {?:?,?:?} WHERE id = ?", "k1", "v1", "k2", "v2", "id");
        assertParsed("UPDATE t SET s = {#{a},#{b}} WHERE id = #{id}", "UPDATE t SET s = {?,?} WHERE id = ?", "a", "b", "id");
    }

    @Test
    void quotedMarkersAndMyBatisPropertyContentsRemainOpaque() {
        assertParsed("UPDATE t SET m = {'{#{k}::v}':#{v}} WHERE id = #{id}", "UPDATE t SET m = {'{#{k}::v}':?} WHERE id = ?", "v", "id");
        assertParsed("UPDATE t SET note = $${#{k}::v}$$ WHERE id = :id", "UPDATE t SET note = $${#{k}::v}$$ WHERE id = ?", "id");
        assertParsed("UPDATE t SET m = {#{a::b}:#{v}} WHERE id = #{id}", "UPDATE t SET m = {?:?} WHERE id = ?", "a::b", "v", "id");
        assertParsed("UPDATE t SET m = {#{keys[0]}:#{values[1]}} WHERE id = #{id}", "UPDATE t SET m = {?:?} WHERE id = ?", "keys[0]", "values[1]", "id");
    }

    @ParameterizedTest
    @ValueSource(strings = { "UPDATE t SET m = {#{}:#{v}} WHERE id = #{id}", "UPDATE t SET m = {#{k}:#{}} WHERE id = #{id}", "UPDATE t SET m = {#{k",
            "UPDATE t SET m = {#{k}:#{v" })
    void malformedUnspacedMarkersAreRejected(final String cql) {
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse(cql));
    }

    @Test
    void doubleColonInsideMyBatisPropertyIsNotMixedStyle() {
        assertParsed("UPDATE t SET m = { #{a::b}:#{v} } WHERE id = #{id}", "UPDATE t SET m = { ?:? } WHERE id = ?", "a::b", "v", "id");
    }

    private static void assertParsed(final String cql, final String expectedCql, final String... expectedNames) {
        final ParsedCql parsed = ParsedCql.parse(cql);
        assertEquals(expectedCql, parsed.parameterizedCql(), cql);
        assertEquals(expectedNames.length, parsed.parameterCount(), cql);
        assertEquals(expectedNames.length, parsed.namedParameters().size(), cql);

        for (int i = 0; i < expectedNames.length; i++) {
            assertEquals(expectedNames[i], parsed.namedParameters().get(i), cql);
        }
    }
}
