/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Review 2026-11-03 (slice s10): {@link CqlMapper} load-time id message and a round trip of the statement forms whose
 * {@link ParsedCql} handling was fixed this round.
 */
class CqlMapperReview20261103Test {

    private static CqlMapper load(final String xml) {
        return CqlMapper.loadFrom(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
    }

    /** An empty id was reported as a missing attribute although the attribute is present. */
    @Test
    void emptyIdIsReportedAsEmpty() {
        final IllegalArgumentException empty = assertThrows(IllegalArgumentException.class,
                () -> load("<cqlMapper><cql id=\"\">SELECT 1 FROM t</cql></cqlMapper>"));
        final IllegalArgumentException missing = assertThrows(IllegalArgumentException.class, () -> load("<cqlMapper><cql>SELECT 1 FROM t</cql></cqlMapper>"));

        assertAll(() -> assertTrue(empty.getMessage().contains("Missing or empty required 'id' attribute"), empty.getMessage()),
                () -> assertTrue(empty.getMessage().endsWith("in: input stream"), empty.getMessage()),
                () -> assertTrue(missing.getMessage().contains("Missing or empty required 'id' attribute"), missing.getMessage()));
    }

    /** Statements with a dollar-quoted map value after a MyBatis key, or a nested map-key literal, load and save intact. */
    @Test
    void fixedMarkerFormsRoundTripThroughXml() {
        final CqlMapper mapper = load("<cqlMapper><cql id=\"a\">UPDATE t SET m = {#{k}:$$v$$} WHERE id = #{id}</cql>"
                + "<cql id=\"b\">UPDATE t SET m = {{street: :s}:1} WHERE k = :k</cql></cqlMapper>");

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        mapper.saveTo(os);
        final CqlMapper reloaded = load(os.toString(StandardCharsets.UTF_8));

        assertAll(() -> assertEquals(List.of("a", "b"), List.copyOf(reloaded.ids())),
                () -> assertEquals("UPDATE t SET m = {?:$$v$$} WHERE id = ?", reloaded.get("a").parameterizedCql()),
                () -> assertEquals(List.of("k", "id"), List.of(reloaded.get("a").namedParameters().get(0), reloaded.get("a").namedParameters().get(1))),
                () -> assertEquals("UPDATE t SET m = {{street: ?}:1} WHERE k = ?", reloaded.get("b").parameterizedCql()),
                () -> assertEquals(List.of("s", "k"), List.of(reloaded.get("b").namedParameters().get(0), reloaded.get("b").namedParameters().get(1))),
                () -> assertEquals(mapper, reloaded));
    }
}
