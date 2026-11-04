/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Review 2026-11-03 (slice s10): regressions found by a differential of {@link ParsedCql} against Cassandra 5.0's own CQL
 * parser (bind-marker count and names of the original statement versus those of the parameterized text). Every expected
 * parameterized statement below was checked to parse with Cassandra's parser and to carry the same bind markers as the input.
 */
class ParsedCqlReview20261103Test {

    /** A MyBatis map key glued to a dollar-quoted value used to stay in the CQL verbatim and uncounted. */
    @Test
    void myBatisMapKeyGluedToDollarQuotedValueIsRewritten() {
        assertAll(parsed("UPDATE t SET m = {#{k}:$$v$$} WHERE id = #{id}", "UPDATE t SET m = {?:$$v$$} WHERE id = ?", "k", "id"),
                parsed("UPDATE t SET m = {'a':#{x},#{k}:$$v w$$} WHERE id = #{id}", "UPDATE t SET m = {'a':?,?:$$v w$$} WHERE id = ?", "x", "k", "id"),
                parsed("UPDATE t SET m = {#{k1}:$$a$$,#{k2,jdbcType=TEXT}:$$b$$} WHERE id = #{id}", "UPDATE t SET m = {?:$$a$$,?:$$b$$} WHERE id = ?", "k1",
                        "k2", "id"),
                parsed("UPDATE t SET m = {#{k}:$$a:'b$$} WHERE id = #{id}", "UPDATE t SET m = {?:$$a:'b$$} WHERE id = ?", "k", "id"));
    }

    /** Pins the unchanged rule that nothing inside an unterminated dollar quote is a marker. */
    @Test
    void myBatisMarkerInsideUnterminatedDollarQuoteIsStillIgnored() {
        assertAll(parsed("SELECT * FROM t WHERE v = $$abc #{x}", "SELECT * FROM t WHERE v = $$abc #{x}"),
                parsed("SELECT * FROM t WHERE k = #{k} AND v = $$abc #{x}:$$", "SELECT * FROM t WHERE k = ? AND v = $$abc #{x}:$$", "k"));
    }

    /**
     * A native marker glued to a dollar-quoted map value, as a marker ending a nested literal map key ({@code {{f: :x}:$$v$$}})
     * or a map-key marker after an operator ({@code {-:k:$$v$$}}), used to stay in the CQL verbatim and uncounted: the whole
     * token held a complete dollar-quoted constant, so the native-marker branch skipped it.
     */
    @Test
    void nativeMarkerGluedToDollarQuotedValueIsRewritten() {
        assertAll(parsed("UPDATE t SET m = {{f: :x}:$$v$$} WHERE k = :k", "UPDATE t SET m = {{f: ?}:$$v$$} WHERE k = ?", "x", "k"),
                parsed("UPDATE t SET m = {-:k:$$v$$} WHERE k = :kk", "UPDATE t SET m = {-?:$$v$$} WHERE k = ?", "k", "kk"),
                parsed("UPDATE t SET m = {1 * :k:$$v w$$} WHERE k = :kk", "UPDATE t SET m = {1 * ?:$$v w$$} WHERE k = ?", "k", "kk"),
                parsed("UPDATE t SET m = {{f: :x}:$$a$$, {f: :y}:$$b:c$$} WHERE k = :k", "UPDATE t SET m = {{f: ?}:$$a$$, {f: ?}:$$b:c$$} WHERE k = ?", "x",
                        "y", "k"),
                parsed("UPDATE t SET m = {{a: {b: :x}}:$$v$$} WHERE k = :k", "UPDATE t SET m = {{a: {b: ?}}:$$v$$} WHERE k = ?", "x", "k"),
                // The embedded form ("f::x", no space) was already rewritten; pinned alongside.
                parsed("UPDATE t SET m = {{f::x}:$$v$$} WHERE k = :k", "UPDATE t SET m = {{f:?}:$$v$$} WHERE k = ?", "x", "k"));
    }

    /**
     * Pins the unchanged rule that a {@code :name} inside a dollar-quoted (or quoted) constant is literal text, also next to a
     * rewritten marker, and that a token whose marker name runs into the dollar quote is not a marker.
     */
    @Test
    void nativeMarkerInsideDollarQuoteIsStillIgnored() {
        assertAll(parsed("SELECT * FROM t WHERE k = :k AND v = $$ :x $$", "SELECT * FROM t WHERE k = ? AND v = $$ :x $$", "k"),
                parsed("SELECT * FROM t WHERE k = :k AND v = $$:x$$", "SELECT * FROM t WHERE k = ? AND v = $$:x$$", "k"),
                parsed("UPDATE t SET m = {{f: :x}:$$:y$$} WHERE k = :k", "UPDATE t SET m = {{f: ?}:$$:y$$} WHERE k = ?", "x", "k"),
                parsed("UPDATE t SET m = {-:k:$$a :y}:$$} WHERE k = :kk", "UPDATE t SET m = {-?:$$a :y}:$$} WHERE k = ?", "k", "kk"),
                parsed("UPDATE t SET m = {{f: :x}:'$$v'} WHERE k = :k", "UPDATE t SET m = {{f: ?}:'$$v'} WHERE k = ?", "x", "k"),
                parsed("SELECT * FROM t WHERE a = 'p$$q' AND k = :k", "SELECT * FROM t WHERE a = 'p$$q' AND k = ?", "k"),
                // An unterminated dollar quote hides every later marker, as before.
                parsed("SELECT * FROM t WHERE k = :k AND v = $$abc {{f: :x}:1}", "SELECT * FROM t WHERE k = ? AND v = $$abc {{f: :x}:1}", "k"),
                // A marker name glued to the dollar quote itself (":x$$v$$") is not split: the token stays literal text.
                parsed("SELECT * FROM t WHERE k = :k AND v = :x$$v$$", "SELECT * FROM t WHERE k = ? AND v = :x$$v$$", "k"));
    }

    /**
     * A native marker glued to an UNTERMINATED dollar quote ({@code {{f: :x}:$$a::b ...}, {@code {-:k:$$a::b}) used to have the
     * {@code ::b} inside that quote rewritten into a bind marker. The marker before the quote is still rewritten (as for
     * {@code {#{k}:$$a::b}), but nothing from the opening {@code $$} on is a marker.
     */
    @Test
    void gluedMarkerInsideUnterminatedDollarQuoteAfterNativeMarkerIsIgnored() {
        assertAll(parsed("UPDATE t SET m = {{f: :x}:$$a::b WHERE k = :k", "UPDATE t SET m = {{f: ?}:$$a::b WHERE k = :k", "x"),
                parsed("UPDATE t SET m = {-:k:$$a::b c::d", "UPDATE t SET m = {-?:$$a::b c::d", "k"),
                parsed("UPDATE t SET m = {1 * :k:$$a::b}", "UPDATE t SET m = {1 * ?:$$a::b}", "k"),
                // The MyBatis form already behaved so; pinned alongside.
                parsed("UPDATE t SET m = {#{k}:$$a::b WHERE id = #{id}", "UPDATE t SET m = {?:$$a::b WHERE id = #{id}", "k"),
                // A complete dollar-quoted value keeps the glued markers around it.
                parsed("UPDATE t SET m = {{f: :x}:$$a::b$$, {f: :y}::v} WHERE k = :k", "UPDATE t SET m = {{f: ?}:$$a::b$$, {f: ?}:?} WHERE k = ?", "x", "y",
                        "v", "k"),
                parsed("UPDATE t SET m = {-:k::v}, n = $$a::b$$ WHERE k = :kk", "UPDATE t SET m = {-?:?}, n = $$a::b$$ WHERE k = ?", "k", "v", "kk"));
    }

    /**
     * An unspaced {@code ::} followed by a dollar-quoted constant used to register a marker named after the masked placeholder
     * ({@code $$0$$}) and put {@code ?} in place of the constant. It is now literal text, as {@code :x$$v$$} is; a marker before it
     * in the same token is still rewritten.
     */
    @Test
    void doubleColonBeforeDollarQuoteIsNotAMarker() {
        assertAll(parsed("UPDATE t SET m = {f::$$v$$} WHERE k = :k", "UPDATE t SET m = {f::$$v$$} WHERE k = ?", "k"),
                parsed("UPDATE t SET m = {{f: :x}::$$v$$} WHERE k = :k", "UPDATE t SET m = {{f: ?}::$$v$$} WHERE k = ?", "x", "k"),
                parsed("UPDATE t SET m = {-:k::$$v$$} WHERE k = :kk", "UPDATE t SET m = {-?::$$v$$} WHERE k = ?", "k", "kk"),
                // An unspaced named value is still rewritten.
                parsed("UPDATE t SET m = {f::v, g: $$w$$} WHERE k = :k", "UPDATE t SET m = {f:?, g: $$w$$} WHERE k = ?", "v", "k"));
    }

    /**
     * An unspaced {@code ::name} whose unquoted name runs into a complete dollar-quoted constant ({@code {f::v$$a$$}}) used to
     * register a marker named after the masked placeholder ({@code v$$0$$}) and drop the constant from the CQL. As the
     * top-level {@code :x$$v$$}, it now stays literal text, on the embedded, native-key and MyBatis-key paths alike; other
     * markers around it are still rewritten.
     */
    @Test
    void gluedMarkerNameRunningIntoDollarQuoteIsNotAMarker() {
        assertAll(parsed("UPDATE t SET m = {f::yf$$a$$} WHERE k = :k", "UPDATE t SET m = {f::yf$$a$$} WHERE k = ?", "k"),
                parsed("UPDATE t SET m = {f::yf$$a$$ } WHERE k = :k", "UPDATE t SET m = {f::yf$$a$$ } WHERE k = ?", "k"),
                parsed("UPDATE t SET m = {{f: :x}::yf$$a$$} WHERE k = :k", "UPDATE t SET m = {{f: ?}::yf$$a$$} WHERE k = ?", "x", "k"),
                parsed("UPDATE t SET m = {-:k::v$$a$$} WHERE k = :kk", "UPDATE t SET m = {-?::v$$a$$} WHERE k = ?", "k", "kk"),
                parsed("UPDATE t SET m = {{a::y$$q$$}::w} WHERE k = :k", "UPDATE t SET m = {{a::y$$q$$}:?} WHERE k = ?", "w", "k"),
                // Not a native marker, so no longer a mix of parameter styles either.
                parsed("UPDATE t SET m = {#{k}::v$$a$$} WHERE id = #{id}", "UPDATE t SET m = {?::v$$a$$} WHERE id = ?", "k", "id"),
                // Unchanged: a quoted name is an identifier that may contain "$$"; the top-level form stays literal.
                parsed("UPDATE t SET m = {f::\"a$$b\"} WHERE k = :k", "UPDATE t SET m = {f:?} WHERE k = ?", "\"a$$b\"", "k"),
                parsed("SELECT * FROM t WHERE k = :k AND v = :x$$v$$", "SELECT * FROM t WHERE k = ? AND v = :x$$v$$", "k"));
    }

    /**
     * A native marker ending a nested literal that is itself a map key ({@code {{f: :x}:1}}) used to swallow the closing
     * brace, the glued separator and the value into its name, leaving unbalanced CQL.
     */
    @Test
    void nativeMarkerEndingNestedLiteralMapKeyStopsAtClosingBrace() {
        assertAll(parsed("UPDATE t SET m = {{street: :s}: 1} WHERE k = :k", "UPDATE t SET m = {{street: ?}: 1} WHERE k = ?", "s", "k"),
                parsed("UPDATE t SET m = {{street: :s}:1} WHERE k = :k", "UPDATE t SET m = {{street: ?}:1} WHERE k = ?", "s", "k"),
                parsed("UPDATE t SET m = {{street::s}:1} WHERE k = :k", "UPDATE t SET m = {{street:?}:1} WHERE k = ?", "s", "k"),
                parsed("UPDATE t SET m = {{a: {b: :x}}:1} WHERE k = :k", "UPDATE t SET m = {{a: {b: ?}}:1} WHERE k = ?", "x", "k"),
                parsed("UPDATE t SET m = {{street: :s}:'v'} WHERE k = :k", "UPDATE t SET m = {{street: ?}:'v'} WHERE k = ?", "s", "k"));
    }

    /** The same nested key glued to a named value: both markers are rewritten, in order. */
    @Test
    void nestedLiteralMapKeyGluedToNamedValueRewritesBothMarkers() {
        assertAll(parsed("UPDATE t SET m = {{street::s}::v} WHERE k = :k", "UPDATE t SET m = {{street:?}:?} WHERE k = ?", "s", "v", "k"),
                parsed("UPDATE t SET m = {{street: :s}::v} WHERE k = :k", "UPDATE t SET m = {{street: ?}:?} WHERE k = ?", "s", "v", "k"),
                parsed("UPDATE t SET m = {{a::x}::y, {a::z}::w} WHERE k = :k", "UPDATE t SET m = {{a:?}:?, {a:?}:?} WHERE k = ?", "x", "y", "z", "w", "k"));
    }

    /** Pins forms whose names already ended correctly: quoted names, plain trailing braces, operator-position keys. */
    @Test
    void unchangedMarkerNameEnds() {
        assertAll(parsed("UPDATE t SET m = {{street: :\"S\"}:1} WHERE k = :k", "UPDATE t SET m = {{street: ?}:1} WHERE k = ?", "\"S\"", "k"),
                parsed("UPDATE t SET a = {street: :s}, b = {c: {d::e}} WHERE k = :k", "UPDATE t SET a = {street: ?}, b = {c: {d:?}} WHERE k = ?", "s", "e",
                        "k"),
                parsed("UPDATE t SET m = {-:k: 1} WHERE id = :id", "UPDATE t SET m = {-?: 1} WHERE id = ?", "k", "id"),
                parsed("UPDATE t SET m = {-:k::v} WHERE id = :id", "UPDATE t SET m = {-?:?} WHERE id = ?", "k", "v", "id"),
                parsed("UPDATE t SET m = {'a::b'::x} WHERE id = :id", "UPDATE t SET m = {'a::b':?} WHERE id = ?", "x", "id"),
                parsed("UPDATE t SET m = {(1, 2)::x} WHERE id = :id", "UPDATE t SET m = {(1, 2):?} WHERE id = ?", "x", "id"),
                parsed("SELECT * FROM t WHERE k = :k AND c IN (:a, :b)", "SELECT * FROM t WHERE k = ? AND c IN (?, ?)", "k", "a", "b"));
    }

    private static Executable parsed(final String cql, final String expectedCql, final String... expectedNames) {
        return () -> {
            final ParsedCql parsed = ParsedCql.parse(cql);
            assertEquals(expectedCql, parsed.parameterizedCql(), cql);
            assertEquals(expectedNames.length, parsed.parameterCount(), cql);
            assertEquals(expectedNames.length, parsed.namedParameters().size(), cql);

            for (int i = 0; i < expectedNames.length; i++) {
                assertEquals(expectedNames[i], parsed.namedParameters().get(i), cql + " #" + i);
            }
        };
    }
}
