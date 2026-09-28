/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;

public class ParsedCqlTest extends TestBase {

    // ---------------------------------------------------------------------------------------------
    // parse(): positional, named, MyBatis-style, caching.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testParse_PositionalParameters() {
        final ParsedCql parsed = ParsedCql.parse("SELECT * FROM users WHERE id = ? AND status = ?");
        assertNotNull(parsed);
        assertEquals(2, parsed.parameterCount());
        assertTrue(parsed.namedParameters().isEmpty());
        assertEquals("SELECT * FROM users WHERE id = ? AND status = ?", parsed.parameterizedCql());
    }

    @Test
    public void testParse_NamedParameters() {
        final ParsedCql parsed = ParsedCql.parse("SELECT * FROM users WHERE id = :userId AND status = :status");
        assertEquals(2, parsed.parameterCount());
        assertEquals("userId", parsed.namedParameters().get(0));
        assertEquals("status", parsed.namedParameters().get(1));
        // Parameterized CQL must convert :name to ?.
        assertTrue(parsed.parameterizedCql().contains("?"), parsed.parameterizedCql());
        assertFalse(parsed.parameterizedCql().contains(":"), parsed.parameterizedCql());
    }

    @Test
    public void testParse_BareNamedParameterMarkerThrows() {
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("SELECT * FROM users WHERE id = :"));
    }

    @Test
    public void testParse_MapLiteralColonIsNotNamedParameter() {
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET m = {'a': 'b'} WHERE id = ?");

        assertEquals(1, parsed.parameterCount());
        assertEquals("UPDATE t SET m = {'a': 'b'} WHERE id = ?", parsed.parameterizedCql());
    }

    @Test
    public void testParse_UnspacedMapLiteralColonIsNotNamedParameter_withPositionalParam() {
        // Regression: an unspaced map literal like {'a':1,'b':2} must NOT have its ':' treated as named parameters.
        // Before the fix this threw "Cannot mix parameter styles" because :1 / :2} were registered as named params
        // alongside the real '?'.
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET m = {'a':1,'b':2} WHERE id = ?");

        assertEquals(1, parsed.parameterCount());
        assertTrue(parsed.namedParameters().isEmpty(), parsed.namedParameters().toString());
        assertEquals("UPDATE t SET m = {'a':1,'b':2} WHERE id = ?", parsed.parameterizedCql());
    }

    @Test
    public void testParse_UnspacedMapLiteralColonIsNotNamedParameter_noParam() {
        // Regression: with no real parameter, an unspaced map literal must round-trip unchanged (was corrupted to
        // "{'a'?,'b'? WHERE id = 5" with parameterCount==2 before the fix).
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET m = {'a':1,'b':2} WHERE id = 5");

        assertEquals(0, parsed.parameterCount());
        assertTrue(parsed.namedParameters().isEmpty(), parsed.namedParameters().toString());
        assertEquals("UPDATE t SET m = {'a':1,'b':2} WHERE id = 5", parsed.parameterizedCql());
    }

    @Test
    public void testParse_NamedParameterAfterMapLiteralStillWorks() {
        // The ':param' AFTER a closed map literal must still be recognized as a named parameter.
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET m = {'a':1} WHERE id = :userId");

        assertEquals(1, parsed.parameterCount());
        assertEquals("userId", parsed.namedParameters().get(0));
        assertTrue(parsed.parameterizedCql().contains("{'a':1}"), parsed.parameterizedCql());
        assertTrue(parsed.parameterizedCql().endsWith("WHERE id = ?"), parsed.parameterizedCql());
    }

    @Test
    public void testParse_NamedParametersInUdtLiteralValues() {
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET address = {street: :street, zip: :zip} WHERE id = :id");

        assertEquals(3, parsed.parameterCount());
        assertEquals("street", parsed.namedParameters().get(0));
        assertEquals("zip", parsed.namedParameters().get(1));
        assertEquals("id", parsed.namedParameters().get(2));
        assertEquals("UPDATE t SET address = {street: ?, zip: ?} WHERE id = ?", parsed.parameterizedCql());
    }

    @Test
    public void testParse_UnspacedNamedParametersInUdtLiteralValues() {
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET address={street::street,zip::zip} WHERE id=:id");

        assertEquals(3, parsed.parameterCount());
        assertEquals("street", parsed.namedParameters().get(0));
        assertEquals("zip", parsed.namedParameters().get(1));
        assertEquals("UPDATE t SET address={street:?,zip:?} WHERE id=?", parsed.parameterizedCql());
    }

    @Test
    public void testParse_LeadingDoubleSlashCommentStillParsesParameters() {
        final ParsedCql parsed = ParsedCql.parse("// lookup by id\nSELECT * FROM users WHERE id = :id");

        assertEquals(1, parsed.parameterCount());
        assertEquals("id", parsed.namedParameters().get(0));
        assertTrue(parsed.parameterizedCql().endsWith("WHERE id = ?"), parsed.parameterizedCql());
    }

    @Test
    public void testParse_DoubleSlashInsideLeadingBlockCommentIsNotTreatedAsLineComment() {
        final ParsedCql parsed = ParsedCql.parse("/* comment containing // text\nstill a comment */\nSELECT * FROM users WHERE id = :id");

        assertEquals(1, parsed.parameterCount());
        assertEquals("id", parsed.namedParameters().get(0));
        assertTrue(parsed.parameterizedCql().endsWith("WHERE id = ?"), parsed.parameterizedCql());
    }

    @Test
    public void testParse_BraceInDollarQuotedStringDoesNotSwallowFollowingParameter() {
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET note = $$open { brace$$ WHERE id = :id");

        assertEquals(1, parsed.parameterCount());
        assertEquals("id", parsed.namedParameters().get(0));
        assertTrue(parsed.parameterizedCql().endsWith("WHERE id = ?"), parsed.parameterizedCql());
    }

    @Test
    public void testParse_ParameterSyntaxInsideDollarQuotedStringIsLiteralText() {
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET note = $$literal :named ? #{ibatis}$$ WHERE id = :id");

        assertEquals(1, parsed.parameterCount());
        assertEquals("id", parsed.namedParameters().get(0));
        assertEquals("UPDATE t SET note = $$literal :named ? #{ibatis}$$ WHERE id = ?", parsed.parameterizedCql());
    }

    @Test
    public void testParse_OpenBraceInStringLiteral_doesNotSwallowNamedParam() {
        // Regression: a '{' that appears INSIDE a string literal must not raise the map/UDT-literal brace depth.
        // Before the fix, updateCurlyDepth counted the '{' inside 'a{b', so the depth stayed > 0 and the following
        // ':id' was silently treated as "inside a map literal" and never converted to '?'.
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET note = 'a{b' WHERE id = :id");

        assertEquals(1, parsed.parameterCount());
        assertEquals("id", parsed.namedParameters().get(0));
        assertTrue(parsed.parameterizedCql().contains("'a{b'"), parsed.parameterizedCql());
        assertTrue(parsed.parameterizedCql().endsWith("WHERE id = ?"), parsed.parameterizedCql());
    }

    @Test
    public void testParse_CloseBraceInMapStringValue_doesNotMisparse() {
        // Regression: a '}' inside a string VALUE of a map literal must not prematurely drop the brace depth.
        // Before the fix the '}' in 'a}b' dropped depth to 0, so the following ':' map separator was seen at
        // depth 0 and registered as a spurious named parameter, throwing "Cannot mix parameter styles".
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET m = {'k':'a}b','n':5} WHERE id = ?");

        assertEquals(1, parsed.parameterCount());
        assertTrue(parsed.namedParameters().isEmpty(), parsed.namedParameters().toString());
        assertTrue(parsed.parameterizedCql().contains("{'k':'a}b','n':5}"), parsed.parameterizedCql());
        assertTrue(parsed.parameterizedCql().endsWith("WHERE id = ?"), parsed.parameterizedCql());
    }

    @Test
    public void testParse_JsonStringValueWithBraces_namedParamStillRecognized() {
        // Realistic case: storing a JSON fragment (balanced or not) in a string column, followed by a named param.
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET data = '{\"k\":1' WHERE id = :id");

        assertEquals(1, parsed.parameterCount());
        assertEquals("id", parsed.namedParameters().get(0));
        assertTrue(parsed.parameterizedCql().contains("'{\"k\":1'"), parsed.parameterizedCql());
        assertTrue(parsed.parameterizedCql().endsWith("WHERE id = ?"), parsed.parameterizedCql());
    }

    @Test
    public void testParse_IbatisStyleParameters() {
        final ParsedCql parsed = ParsedCql.parse("SELECT * FROM users WHERE id = #{userId} AND status = #{status}");
        assertEquals(2, parsed.parameterCount());
        assertEquals("userId", parsed.namedParameters().get(0));
        assertEquals("status", parsed.namedParameters().get(1));
        assertTrue(parsed.parameterizedCql().contains("?"), parsed.parameterizedCql());
        assertFalse(parsed.parameterizedCql().contains("#{"), parsed.parameterizedCql());
    }

    @Test
    public void testParse_BatchNamedParameters() {
        final ParsedCql parsed = ParsedCql.parse(
                "BEGIN UNLOGGED BATCH INSERT INTO users (id, name) VALUES (:id1, :name1); INSERT INTO users (id, name) VALUES (:id2, :name2); APPLY BATCH;");

        assertEquals("BEGIN UNLOGGED BATCH INSERT INTO users (id, name) VALUES (?, ?); INSERT INTO users (id, name) VALUES (?, ?); APPLY BATCH",
                parsed.parameterizedCql());
        assertEquals(4, parsed.parameterCount());
        assertEquals("id1", parsed.namedParameters().get(0));
        assertEquals("name2", parsed.namedParameters().get(3));
    }

    @Test
    public void testParse_NoParameters() {
        final ParsedCql parsed = ParsedCql.parse("SELECT * FROM users");
        assertEquals(0, parsed.parameterCount());
        assertTrue(parsed.namedParameters().isEmpty());
    }

    @Test
    public void testParse_TrailingSemicolonRemoved() {
        final ParsedCql parsed = ParsedCql.parse("SELECT id FROM users WHERE id = ?;");
        assertFalse(parsed.parameterizedCql().endsWith(";"), parsed.parameterizedCql());
    }

    @Test
    public void testParse_MultipleTrailingSemicolonsRemoved() {
        // All trailing semicolons (and any whitespace between/around them) are stripped, not just one.
        assertEquals("SELECT id FROM users WHERE id = ?", ParsedCql.parse("SELECT id FROM users WHERE id = ?;;").parameterizedCql());
        assertEquals("SELECT id FROM users WHERE id = ?", ParsedCql.parse("SELECT id FROM users WHERE id = ? ; ; ").parameterizedCql());
    }

    @Test
    public void testParse_IbatisParameterWithJdbcTypeUsesNameOnly() {
        // MyBatis-style metadata after the first comma (e.g. jdbcType) is dropped; only the property name remains.
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET n = #{name,jdbcType=TEXT} WHERE id = #{id}");
        assertEquals(2, parsed.parameterCount());
        assertEquals("name", parsed.namedParameters().get(0));
        assertEquals("id", parsed.namedParameters().get(1));
        assertFalse(parsed.parameterizedCql().contains("#{"), parsed.parameterizedCql());
    }

    @Test
    public void testParse_NullCqlThrows() {
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse(null));
    }

    @Test
    public void testParse_CachesIdenticalCql() {
        // The same CQL text must return the cached (same-reference) instance.
        final String cql = "SELECT * FROM cache_test_users WHERE id = ? AND age > ?";
        final ParsedCql p1 = ParsedCql.parse(cql);
        final ParsedCql p2 = ParsedCql.parse(cql);
        assertSame(p1, p2);
    }

    @Test
    public void testParse_MixedParameterStylesThrows() {
        // ? + :name mix is forbidden.
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("SELECT * FROM u WHERE id = ? AND status = :status"));
    }

    @Test
    public void testParse_MixedQuestionAndIbatisStylesThrows() {
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("SELECT * FROM u WHERE id = ? AND status = #{status}"));
    }

    @Test
    public void testParse_MixedNamedAndIbatisStylesThrows() {
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("SELECT * FROM u WHERE id = :id AND status = #{status}"));
    }

    @Test
    public void testParse_EmptyIbatisParameterNameThrows() {
        // "#{}" is too short — empty name is rejected.
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("SELECT * FROM u WHERE id = #{}"));
    }

    @Test
    public void testParse_MalformedIbatisParameterThrows() {
        // "#{" with no closing '}' is rejected.
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("SELECT * FROM u WHERE id = #{abc"));
    }

    @Test
    public void testParse_DDLStatementSkipsParameterDetection() {
        // Non-DML (e.g. CREATE) is stored as-is — no parameter scanning, count=0.
        final ParsedCql parsed = ParsedCql.parse("CREATE TABLE foo (id INT PRIMARY KEY, name TEXT)");
        assertEquals(0, parsed.parameterCount());
        assertTrue(parsed.namedParameters().isEmpty());
    }

    @Test
    public void testParse_InsertStatement() {
        final ParsedCql parsed = ParsedCql.parse("INSERT INTO users_parsed_insert (id, name, email) VALUES (?, ?, ?)");
        assertEquals(3, parsed.parameterCount());
    }

    @Test
    public void testParse_UpdateStatement() {
        final ParsedCql parsed = ParsedCql.parse("UPDATE users_parsed_update SET name = ? WHERE id = ?");
        assertEquals(2, parsed.parameterCount());
    }

    @Test
    public void testParse_DeleteStatement() {
        final ParsedCql parsed = ParsedCql.parse("DELETE FROM users_parsed_delete WHERE id = ?");
        assertEquals(1, parsed.parameterCount());
    }

    // ---------------------------------------------------------------------------------------------
    // Getters: originalCql, parameterizedCql, namedParameters, parameterCount.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testOriginalCql_TrimsWhitespace() {
        // originalCql() returns the trimmed input.
        final ParsedCql parsed = ParsedCql.parse("   SELECT * FROM users_original_cql_trim WHERE id = ?   ");
        assertEquals("SELECT * FROM users_original_cql_trim WHERE id = ?", parsed.originalCql());
    }

    // ---------------------------------------------------------------------------------------------
    // equals / hashCode / toString: equality is by CQL only.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testEquals_SameInstance() {
        final ParsedCql p = ParsedCql.parse("SELECT 1 FROM equals_same_instance");
        assertTrue(p.equals(p));
    }

    @Test
    public void testEquals_SameCql() {
        final ParsedCql p1 = ParsedCql.parse("SELECT 1 FROM equals_same_cql_a");
        final ParsedCql p2 = ParsedCql.parse("SELECT 1 FROM equals_same_cql_a");
        assertTrue(p1.equals(p2));
    }

    @Test
    public void testEquals_DifferentCql() {
        final ParsedCql p1 = ParsedCql.parse("SELECT 1 FROM equals_diff_a");
        final ParsedCql p2 = ParsedCql.parse("SELECT 1 FROM equals_diff_b");
        assertFalse(p1.equals(p2));
    }

    @Test
    public void testEquals_DifferentType() {
        final ParsedCql p = ParsedCql.parse("SELECT 1 FROM equals_diff_type");
        assertFalse(p.equals("not a ParsedCql"));
        assertFalse(p.equals(null));
    }

    @Test
    public void testHashCode_ConsistentWithEquals() {
        final ParsedCql p1 = ParsedCql.parse("SELECT 1 FROM hash_consistent");
        final ParsedCql p2 = ParsedCql.parse("SELECT 1 FROM hash_consistent");
        assertEquals(p1.hashCode(), p2.hashCode());
    }

    @Test
    public void testToString_ContainsExpectedFragments() {
        final ParsedCql parsed = ParsedCql.parse("SELECT 1 FROM tostring_test");
        final String s = parsed.toString();
        assertTrue(s.contains("[cql]"), s);
        assertTrue(s.contains("[parameterizedCql]"), s);
        assertTrue(s.contains("tostring_test"), s);
    }

    @Test
    public void testParse_SingleFieldUnspacedUdtLiteral_NamedParameterIsConverted() {
        // Regression: a single-field literal such as {street::street} is ONE token whose braces balance, so the
        // depth before and after the token are both 0. Gating the embedded-marker scan on those depths dropped
        // the parameter, leaving a native ':street' marker mixed with '?' — which Cassandra rejects. The depth
        // is now tracked WITHIN the token, so this behaves like the multi-field form.
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET u = {street::street} WHERE id = :id");

        assertEquals("UPDATE t SET u = {street:?} WHERE id = ?", parsed.parameterizedCql());
        assertEquals(2, parsed.parameterCount());
        assertEquals("street", parsed.namedParameters().get(0));
        assertEquals("id", parsed.namedParameters().get(1));
    }

    @Test
    public void testParse_NestedSingleFieldUdtLiteral_NamedParameterIsConverted() {
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET u = {a:{b::inner}} WHERE id = :id");

        assertEquals("UPDATE t SET u = {a:{b:?}} WHERE id = ?", parsed.parameterizedCql());
        assertEquals(2, parsed.parameterCount());
        assertEquals("inner", parsed.namedParameters().get(0));
        assertEquals("id", parsed.namedParameters().get(1));
    }

    @Test
    public void testParse_MultiFieldUnspacedUdtLiteral_StillConverted() {
        // The multi-token form (first token leaves an unbalanced '{') must keep working after the depth change.
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET u = {street::street, city::city} WHERE id = :id");

        assertEquals("UPDATE t SET u = {street:?, city:?} WHERE id = ?", parsed.parameterizedCql());
        assertEquals(3, parsed.parameterCount());
    }

    @Test
    public void testParse_DoubleColonOutsideLiteral_IsNotTreatedAsEmbeddedMarker() {
        // The embedded-marker scan requires brace depth > 0 at the marker, so a cast-like '::' at depth 0 is
        // left alone rather than being rewritten.
        final ParsedCql parsed = ParsedCql.parse("SELECT a::text FROM t WHERE id = :id");

        assertEquals("SELECT a::text FROM t WHERE id = ?", parsed.parameterizedCql());
        assertEquals(1, parsed.parameterCount());
        assertEquals("id", parsed.namedParameters().get(0));
    }

    @Test
    public void testParse_DoubleColonInsideQuotedKey_IsNotTreatedAsEmbeddedMarker() {
        // Braces inside quoted literals must not raise the depth, and a '::' inside a quoted key must not match.
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET m = {'a::b':1} WHERE id = :id");

        assertEquals("UPDATE t SET m = {'a::b':1} WHERE id = ?", parsed.parameterizedCql());
        assertEquals(1, parsed.parameterCount());
        assertEquals("id", parsed.namedParameters().get(0));
    }

    // ---------------------------------------------------------------------------------------------
    // Dollar-quoted string constants ($$...$$) are opaque: SqlParser knows nothing about dollar quoting.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testParse_DollarQuotedString_WhitespaceIsPreserved() {
        // Regression: the tokenizer collapsed the whitespace run inside the constant, silently changing the value
        // written to Cassandra ("a    b" became "a b", a tab became a space).
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET note = $$a    b\tc$$ WHERE id = :id");

        assertEquals("UPDATE t SET note = $$a    b\tc$$ WHERE id = ?", parsed.parameterizedCql());
        assertEquals(1, parsed.parameterCount());
        assertEquals("id", parsed.namedParameters().get(0));
    }

    @Test
    public void testParse_DollarQuotedString_CommentMarkersAreLiteralText() {
        // Regression: '#', '--' and '/*' inside the constant were taken for comment starts, truncating the statement
        // to "UPDATE t SET note = $$Issue" (count 0).
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET note = $$Issue #5 -- see /* x$$ WHERE id = ?");

        assertEquals("UPDATE t SET note = $$Issue #5 -- see /* x$$ WHERE id = ?", parsed.parameterizedCql());
        assertEquals(1, parsed.parameterCount());
    }

    @Test
    public void testParse_DollarQuotedString_QuoteCharactersAreLiteralText() {
        // Regression: a quote inside the constant (the very reason to use $$ quoting) opened a quoted literal that
        // swallowed the rest of the statement, so the trailing #{id} marker was never converted or counted.
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET note = $$it's \"x\" [y$$ WHERE id = #{id}");

        assertEquals("UPDATE t SET note = $$it's \"x\" [y$$ WHERE id = ?", parsed.parameterizedCql());
        assertEquals(1, parsed.parameterCount());
        assertEquals("id", parsed.namedParameters().get(0));
    }

    @Test
    public void testParse_DollarSignsInsideQuotedLiteral_AreNotTreatedAsDollarQuotedString() {
        // '$$' inside a single-quoted literal is ordinary text (including text that looks like an internal
        // placeholder); a real $$ constant later in the statement is still kept verbatim.
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET a = 'x$$0$$y', b = $$z  z$$ WHERE id = :id");

        assertEquals("UPDATE t SET a = 'x$$0$$y', b = $$z  z$$ WHERE id = ?", parsed.parameterizedCql());
        assertEquals(1, parsed.parameterCount());
        assertEquals("id", parsed.namedParameters().get(0));
    }

    @Test
    public void testParse_BackslashesAreLiteralCqlCharacters() {
        for (final String literal : new String[] { "'C:\\'", "'C:\\\\'", "'a\\''b'", "'\\$$0$$//tail'" }) {
            final ParsedCql parsed = ParsedCql.parse("SELECT * FROM files WHERE path = " + literal + " AND id = :id");
            assertEquals("SELECT * FROM files WHERE path = " + literal + " AND id = ?", parsed.parameterizedCql());
            assertEquals(1, parsed.parameterCount());
            assertEquals("id", parsed.namedParameters().get(0));
        }

        final ParsedCql commented = ParsedCql.parse("UPDATE files SET path = 'C:\\' // comment with :unused\n WHERE id = #{id}");
        assertEquals("UPDATE files SET path = 'C:\\' WHERE id = ?", commented.parameterizedCql());
        assertEquals(1, commented.parameterCount());

        final ParsedCql identifier = ParsedCql.parse("SELECT \"path\\\" FROM files WHERE id = :id");
        assertEquals("SELECT \"path\\\" FROM files WHERE id = ?", identifier.parameterizedCql());
        assertEquals(1, identifier.parameterCount());
    }

    // ---------------------------------------------------------------------------------------------
    // Bind markers inside CQL list literals and subscripts ([...]).
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testParse_PositionalMarkersInsideListLiteralsAndSubscripts_areCounted() {
        // Regression: SqlParser keeps a whole [...] group as one token, so every '?' inside it was left out of
        // parameterCount (these returned 1, 2, 1, 1 and 1).
        assertEquals(3, ParsedCql.parse("UPDATE t SET l = l + [?, ?] WHERE id = ?").parameterCount());
        assertEquals(3, ParsedCql.parse("UPDATE t SET l[?] = ? WHERE id = ?").parameterCount());
        assertEquals(3, ParsedCql.parse("INSERT INTO t (id, l) VALUES (?, [?, ?])").parameterCount());
        assertEquals(4, ParsedCql.parse("UPDATE t SET l = [[?, ?], [?]] WHERE id = ?").parameterCount());
        assertEquals(3, ParsedCql.parse("UPDATE t SET m = {'k': [?, ?]} WHERE id = ?").parameterCount());

        final ParsedCql parsed = ParsedCql.parse("SELECT * FROM t WHERE m[?]>=? AND l[?]<>? ALLOW FILTERING");
        assertEquals(4, parsed.parameterCount());
        assertEquals("SELECT * FROM t WHERE m[?]>=? AND l[?]<>? ALLOW FILTERING", parsed.parameterizedCql());
        assertTrue(parsed.namedParameters().isEmpty());
    }

    @Test
    public void testParse_CloseBracketInsideStringOfListLiteral_doesNotSwallowRestOfStatement() {
        // Regression: the bracket group ended at the ']' inside 'b]', and the following "'] WHERE id = ?" was then
        // read as an unterminated string literal, so even the top-level marker was lost (parameterCount 0).
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET l = ['a?', ?, 'b]'] WHERE id = ?");

        assertEquals(2, parsed.parameterCount());
        assertEquals("UPDATE t SET l = ['a?', ?, 'b]'] WHERE id = ?", parsed.parameterizedCql());
    }

    @Test
    public void testParse_NamedAndMyBatisMarkersInsideBrackets_areRewritten() {
        final ParsedCql named = ParsedCql.parse("UPDATE t SET l = l + [:x, :y] WHERE id = :id");
        assertEquals("UPDATE t SET l = l + [?, ?] WHERE id = ?", named.parameterizedCql());
        assertEquals(3, named.parameterCount());
        assertEquals("x", named.namedParameters().get(0));
        assertEquals("y", named.namedParameters().get(1));
        assertEquals("id", named.namedParameters().get(2));

        final ParsedCql subscript = ParsedCql.parse("UPDATE t SET m[:k] = :v WHERE id = :id");
        assertEquals("UPDATE t SET m[?] = ? WHERE id = ?", subscript.parameterizedCql());
        assertEquals("k", subscript.namedParameters().get(0));
        assertEquals("v", subscript.namedParameters().get(1));

        // A ':' directly inside [...] cannot be a map separator, even when the list is a map value.
        final ParsedCql inMap = ParsedCql.parse("UPDATE t SET m = {'k': [:a, :b]} WHERE id = :id");
        assertEquals("UPDATE t SET m = {'k': [?, ?]} WHERE id = ?", inMap.parameterizedCql());
        assertEquals(3, inMap.parameterCount());

        final ParsedCql myBatis = ParsedCql.parse("UPDATE t SET l = [#{a}, #{b}] WHERE id = #{id}");
        assertEquals("UPDATE t SET l = [?, ?] WHERE id = ?", myBatis.parameterizedCql());
        assertEquals("a", myBatis.namedParameters().get(0));
        assertEquals("id", myBatis.namedParameters().get(2));
    }

    @Test
    public void testParse_UdtLiteralInsideListLiteral_isNotCorrupted() {
        // Regression: "[{street::street}]" was one token, so the embedded-marker rewrite took "street}]" as the
        // parameter name and dropped the closing "}]" from the CQL ("... [{street:? WHERE id = ?").
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET l = [{street::street}] WHERE id = :id");

        assertEquals("UPDATE t SET l = [{street:?}] WHERE id = ?", parsed.parameterizedCql());
        assertEquals("street", parsed.namedParameters().get(0));
        assertEquals("id", parsed.namedParameters().get(1));
    }

    @Test
    public void testParse_BracketsInsideQuotesAndComments_areLeftAlone() {
        final ParsedCql quoted = ParsedCql.parse("SELECT \"col[1]\" FROM t WHERE a = 'x[y' AND b = :b");
        assertEquals("SELECT \"col[1]\" FROM t WHERE a = 'x[y' AND b = ?", quoted.parameterizedCql());
        assertEquals(1, quoted.parameterCount());

        final ParsedCql commented = ParsedCql.parse("UPDATE t SET l = [? /* ? ] */, ?] WHERE id = ? -- [?]");
        assertEquals("UPDATE t SET l = [? , ?] WHERE id = ?", commented.parameterizedCql());
        assertEquals(3, commented.parameterCount());
    }

    @Test
    public void testParse_PositionalMarkerFollowedByMinus_isCounted() {
        // Regression: SqlParser's PostgreSQL JSON operator "?-" (which CQL does not have) glued the marker to the
        // minus sign, so the '?' in "?-1" was not counted (these returned 1 and 0).
        final ParsedCql parsed = ParsedCql.parse("SELECT * FROM t WHERE a = ?-1 AND id = ?");
        assertEquals(2, parsed.parameterCount());
        assertEquals("SELECT * FROM t WHERE a = ?-1 AND id = ?", parsed.parameterizedCql());

        assertEquals(1, ParsedCql.parse("SELECT * FROM t WHERE a = ?- 1").parameterCount());
    }

    @Test
    public void testParse_PositionalMarkerInsideBracketsMixedWithNamed_throws() {
        // The '?' inside [...] is a positional marker like any other, so mixing it with ':name' is rejected.
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("UPDATE t SET l = [?] WHERE id = :id"));
    }

    @Test
    public void testParse_PrivateUseMaskCharactersInLiteral_doNotDisableBracketMarkers() {
        // Regression: a U+E000/U+E001 anywhere in the statement used to switch bracket masking off, so the marker
        // inside [...] was neither rewritten nor counted (parameterCount 1). The characters must also survive verbatim.
        for (final char ch : new char[] { '', '' }) {
            final ParsedCql parsed = ParsedCql.parse("UPDATE t SET l = [:a] WHERE id = :id AND n = 'x" + ch + "y'");
            assertEquals("UPDATE t SET l = [?] WHERE id = ? AND n = 'x" + ch + "y'", parsed.parameterizedCql());
            assertEquals(2, parsed.parameterCount());
            assertEquals("a", parsed.namedParameters().get(0));
            assertEquals("id", parsed.namedParameters().get(1));
        }
    }

    @Test
    public void testParse_PrivateUseMaskCharactersInComment_doNotDisableBracketMarkers() {
        for (final char ch : new char[] { '', '' }) {
            final ParsedCql lineComment = ParsedCql.parse("UPDATE t SET l = [:a] WHERE id = :id -- " + ch);
            assertEquals("UPDATE t SET l = [?] WHERE id = ?", lineComment.parameterizedCql());
            assertEquals(2, lineComment.parameterCount());

            final ParsedCql blockComment = ParsedCql.parse("UPDATE t SET l = l + [?, ?] /* " + ch + " */ WHERE id = ?");
            assertEquals(3, blockComment.parameterCount());
            assertEquals("UPDATE t SET l = l + [?, ?] WHERE id = ?", blockComment.parameterizedCql());
        }
    }

    @Test
    public void testParse_AllLowPrivateUseCharactersPresent_usesAnotherFreePairForMasking() {
        // Both default mask characters and the next candidates are taken, so a later free pair must be used.
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET l = [?, ?] WHERE id = ? AND n = ''");
        assertEquals("UPDATE t SET l = [?, ?] WHERE id = ? AND n = ''", parsed.parameterizedCql());
        assertEquals(3, parsed.parameterCount());
    }

    @Test
    public void testParse_EveryPrivateUseCharacterPresent_stillParsesBracketMarkers() {
        // Regression: with all of U+E000-U+F8FF in a comment, no private-use mask pair was left and bracket handling
        // was silently disabled again ("[:a]" kept, parameterCount 1).
        final StringBuilder sb = new StringBuilder();

        for (char ch = ''; ch <= ''; ch++) {
            sb.append(ch);
        }

        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET l = [:a] WHERE id = :id /* " + sb + " */");
        assertEquals("UPDATE t SET l = [?] WHERE id = ?", parsed.parameterizedCql());
        assertEquals(2, parsed.parameterCount());
        assertEquals("a", parsed.namedParameters().get(0));

        final ParsedCql literal = ParsedCql.parse("UPDATE t SET l = [?, ?] WHERE id = ? AND n = '" + sb + "'");
        assertEquals("UPDATE t SET l = [?, ?] WHERE id = ? AND n = '" + sb + "'", literal.parameterizedCql());
        assertEquals(3, literal.parameterCount());
    }

    @Test
    public void testParse_EveryMaskCandidateCharacterPresent_withBrackets_throwsInsteadOfLosingMarkers() {
        // Every non-ASCII, non-surrogate BMP character: no mask character is left at all. With brackets present the
        // statement cannot be parsed correctly, so it must be rejected rather than silently mis-parsed.
        final StringBuilder sb = new StringBuilder();

        for (int ch = 0x80; ch <= Character.MAX_VALUE; ch++) {
            if (!Character.isSurrogate((char) ch)) {
                sb.append((char) ch);
            }
        }

        final String comment = " /* " + sb + " */";
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("UPDATE t SET l = [:a] WHERE id = :id" + comment));

        // Without brackets nothing needs masking, so the same statement still parses.
        final ParsedCql noBrackets = ParsedCql.parse("UPDATE t SET l = :a WHERE id = :id" + comment);
        assertEquals("UPDATE t SET l = ? WHERE id = ?", noBrackets.parameterizedCql());
        assertEquals(2, noBrackets.parameterCount());
    }
}
