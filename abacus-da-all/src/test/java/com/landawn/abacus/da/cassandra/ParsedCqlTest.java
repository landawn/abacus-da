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

    // ---- 2026-09-29 sliceR ----

    @Test
    public void testParse_NamedMarkersAroundSliceRange_areSeparateMarkers() {
        // Regression: ':' and '.' are not token separators, so ":from..:to" was one token, read as a single marker
        // named "from..:to" (parameterCount 2 instead of 3); "[..:to]" was not recognized at all.
        final ParsedCql parsed = ParsedCql.parse("SELECT m[:from..:to] FROM t WHERE id = :id");
        assertEquals("SELECT m[?..?] FROM t WHERE id = ?", parsed.parameterizedCql());
        assertEquals(3, parsed.parameterCount());
        assertEquals("from", parsed.namedParameters().get(0));
        assertEquals("to", parsed.namedParameters().get(1));
        assertEquals("id", parsed.namedParameters().get(2));

        final ParsedCql openStart = ParsedCql.parse("SELECT m[..:to] FROM t WHERE id = :id");
        assertEquals("SELECT m[..?] FROM t WHERE id = ?", openStart.parameterizedCql());
        assertEquals(2, openStart.parameterCount());
        assertEquals("to", openStart.namedParameters().get(0));

        final ParsedCql openEnd = ParsedCql.parse("SELECT m[:from..] FROM t WHERE id = :id");
        assertEquals("SELECT m[?..] FROM t WHERE id = ?", openEnd.parameterizedCql());
        assertEquals("from", openEnd.namedParameters().get(0));

        // Literal slices and ".." inside a string are untouched.
        assertEquals("SELECT s[1..3] FROM t WHERE a = '..:x' AND id = ?", ParsedCql.parse("SELECT s[1..3] FROM t WHERE a = '..:x' AND id = ?").parameterizedCql());
    }

    @Test
    public void testParse_KeepCommentsMode_blockCommentContentIsNotScanned() {
        // Regression: in SqlParser's "-- Keep comments" mode a retained block comment was scanned like CQL: a '{' or
        // "$$" in it hid every later marker (":b" left in the statement, parameterCount 1), and a "::y" in it was
        // rewritten into a phantom marker.
        final ParsedCql brace = ParsedCql.parse("-- Keep comments\nSELECT * FROM t WHERE a = :a /* { */ AND b = :b");
        assertEquals("SELECT * FROM t WHERE a = ? /* { */ AND b = ?", brace.parameterizedCql());
        assertEquals(2, brace.parameterCount());
        assertEquals("b", brace.namedParameters().get(1));

        final ParsedCql dollar = ParsedCql.parse("-- Keep comments\nSELECT * FROM t WHERE a = :a /* $$ */ AND b = :b");
        assertEquals("SELECT * FROM t WHERE a = ? /* $$ */ AND b = ?", dollar.parameterizedCql());
        assertEquals(2, dollar.parameterCount());

        final ParsedCql udt = ParsedCql.parse("-- Keep comments\nSELECT * FROM t WHERE a = :a /* {x::y} */ AND b = :b");
        assertEquals("SELECT * FROM t WHERE a = ? /* {x::y} */ AND b = ?", udt.parameterizedCql());
        assertEquals(2, udt.parameterCount());
        assertEquals("b", udt.namedParameters().get(1));
    }

    @Test
    public void testParse_IbatisMarkerWithSubscript_keepsBracketsInName() {
        // Regression: the bracket masking leaked into the property expression of a MyBatis marker, so "#{ids[0]}"
        // yielded the name "ids0" instead of "ids[0]".
        final ParsedCql parsed = ParsedCql.parse("SELECT * FROM t WHERE id = #{ids[0]} AND l = [#{x}] AND n = #{a[1].name}");
        assertEquals("SELECT * FROM t WHERE id = ? AND l = [?] AND n = ?", parsed.parameterizedCql());
        assertEquals(3, parsed.parameterCount());
        assertEquals("ids[0]", parsed.namedParameters().get(0));
        assertEquals("x", parsed.namedParameters().get(1));
        assertEquals("a[1].name", parsed.namedParameters().get(2));
    }

    // ---- 2026-10-02 sliceR ----

    @Test
    public void testParse_IbatisMarkerGluedToLiteralFieldSeparator_isRewritten() {
        // Regression: '#', '{' and ':' are not token separators, so in "{street:#{street}}" the MyBatis marker was part
        // of the field token and reached the driver unconverted (parameterCount 1). The spaced form and the named
        // "{street::street}" form were already handled.
        final ParsedCql udt = ParsedCql.parse("UPDATE t SET addr = {street:#{street}, city:#{city}} WHERE id = #{id}");
        assertEquals("UPDATE t SET addr = {street:?, city:?} WHERE id = ?", udt.parameterizedCql());
        assertEquals(3, udt.parameterCount());
        assertEquals("street", udt.namedParameters().get(0));
        assertEquals("city", udt.namedParameters().get(1));
        assertEquals("id", udt.namedParameters().get(2));

        final ParsedCql map = ParsedCql.parse("UPDATE t SET m = m + {'k':#{v}} WHERE id = #{id}");
        assertEquals("UPDATE t SET m = m + {'k':?} WHERE id = ?", map.parameterizedCql());
        assertEquals(2, map.parameterCount());
        assertEquals("v", map.namedParameters().get(0));

        final ParsedCql nested = ParsedCql.parse("UPDATE t SET u = {a:{b:#{x[0]}}} WHERE id = #{id}");
        assertEquals("UPDATE t SET u = {a:{b:?}} WHERE id = ?", nested.parameterizedCql());
        assertEquals("x[0]", nested.namedParameters().get(0));

        // Inside a quoted literal it is data; a glued marker now takes part in the mixed-style check.
        assertEquals("UPDATE t SET u = {'a:#{x}':1} WHERE id = ?", ParsedCql.parse("UPDATE t SET u = {'a:#{x}':1} WHERE id = #{id}").parameterizedCql());
        assertEquals(1, ParsedCql.parse("UPDATE t SET u = {a:'#{x}'} WHERE id = #{id}").parameterCount());
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("UPDATE t SET u = {a:#{x}} WHERE id = :id"));
    }

    // ---- 2026-10-02 verifyPC ----

    private static void assertIbatisParsed(final String cql, final String expectedCql, final String... expectedNames) {
        final ParsedCql parsed = ParsedCql.parse(cql);
        assertEquals(expectedCql, parsed.parameterizedCql(), cql);
        assertEquals(expectedNames.length, parsed.parameterCount(), cql);
        assertEquals(expectedNames.length, parsed.namedParameters().size(), cql);

        for (int i = 0; i < expectedNames.length; i++) {
            assertEquals(expectedNames[i], parsed.namedParameters().get(i), cql);
        }
    }

    @Test
    public void testParse_IbatisMarkerGluedAfterMarkerKey_isRewritten() {
        // Regression: a map key marker with a glued value marker ("#{k}:#{v}") is one token that starts with "#{"; only
        // the key was rewritten and "#{v}" reached the driver unconverted and uncounted.
        assertIbatisParsed("UPDATE t SET m = m + {'a':#{x}, #{k}:#{v}} WHERE id = #{id}", "UPDATE t SET m = m + {'a':?, ?:?} WHERE id = ?", "x", "k",
                "v", "id");
        assertIbatisParsed("UPDATE t SET m = m + { #{k1}:#{v1}, #{k2}:#{v2} } WHERE id = #{id}", "UPDATE t SET m = m + { ?:?, ?:? } WHERE id = ?", "k1",
                "v1", "k2", "v2", "id");
        assertIbatisParsed("UPDATE t SET m = m + { #{k}:{a:#{v}} } WHERE id = #{id}", "UPDATE t SET m = m + { ?:{a:?} } WHERE id = ?", "k", "v", "id");
        // A quoted value after the marker key stays data.
        assertIbatisParsed("UPDATE t SET m = m + { #{k}:'x:#{y}' } WHERE id = #{id}", "UPDATE t SET m = m + { ?:'x:#{y}' } WHERE id = ?", "k", "id");
    }

    @Test
    public void testParse_IbatisMarkerGluedToLiteralFieldSeparator_allShapes() {
        // The glued form in every position: unspaced fields, nested literals, a list inside the literal and a literal
        // inside a list, a double-quoted key, a space before ':', WHERE / IF clauses, a USING clause and a BATCH.
        assertIbatisParsed("INSERT INTO t (id, a) VALUES (#{id}, {a:#{a},b:#{b},c:#{c}})", "INSERT INTO t (id, a) VALUES (?, {a:?,b:?,c:?})", "id", "a",
                "b", "c");
        assertIbatisParsed("UPDATE t SET u = {a:{b:#{x},c:#{y}},d:#{z}} WHERE id = #{id}", "UPDATE t SET u = {a:{b:?,c:?},d:?} WHERE id = ?", "x", "y",
                "z", "id");
        assertIbatisParsed("UPDATE t SET u = {s:[#{x}],street:#{a}} WHERE id = #{id}", "UPDATE t SET u = {s:[?],street:?} WHERE id = ?", "x", "a", "id");
        assertIbatisParsed("UPDATE t SET l = [{k:#{x}}] WHERE id = #{id}", "UPDATE t SET l = [{k:?}] WHERE id = ?", "x", "id");
        assertIbatisParsed("UPDATE t SET m = {\"q\":#{v}} WHERE id = #{id}", "UPDATE t SET m = {\"q\":?} WHERE id = ?", "v", "id");
        assertIbatisParsed("UPDATE t SET u = {a :#{x}} WHERE id = #{id}", "UPDATE t SET u = {a :?} WHERE id = ?", "x", "id");
        assertIbatisParsed("SELECT * FROM t WHERE m = {k:#{v}} AND x = #{x}", "SELECT * FROM t WHERE m = {k:?} AND x = ?", "v", "x");
        assertIbatisParsed("UPDATE t SET m = m + {k:#{v}} WHERE id = #{id} IF m = {k:#{old}}", "UPDATE t SET m = m + {k:?} WHERE id = ? IF m = {k:?}", "v",
                "id", "old");
        assertIbatisParsed("INSERT INTO t (id, a) VALUES (#{id}, {s:#{a}}) USING TTL #{ttl}", "INSERT INTO t (id, a) VALUES (?, {s:?}) USING TTL ?", "id",
                "a", "ttl");
        assertIbatisParsed("BEGIN BATCH INSERT INTO t (id, a) VALUES (#{id}, {s:#{a}}); INSERT INTO t (id, a) VALUES (#{id2}, {s:#{b}}); APPLY BATCH",
                "BEGIN BATCH INSERT INTO t (id, a) VALUES (?, {s:?}); INSERT INTO t (id, a) VALUES (?, {s:?}); APPLY BATCH", "id", "a", "id2", "b");

        // A glued marker mixed with positional markers is rejected like any other MyBatis marker.
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("INSERT INTO t (id, a) VALUES (?, {a:#{x}})"));
    }

    @Test
    public void testParse_GluedIbatisLookalikeInStringOrComment_isNotSplit() {
        // Pins: ":#{" inside a string, a dollar-quoted constant or a kept block comment is data, not a marker.
        assertIbatisParsed("SELECT * FROM t WHERE x = '{a:#{b}}' AND y = #{y}", "SELECT * FROM t WHERE x = '{a:#{b}}' AND y = ?", "y");
        assertIbatisParsed("SELECT * FROM t WHERE x = $${a:#{b}}$$ AND y = #{y}", "SELECT * FROM t WHERE x = $${a:#{b}}$$ AND y = ?", "y");
        assertIbatisParsed("-- Keep comments\nSELECT * FROM t /* {a:#{b}} */ WHERE y = #{y}", "SELECT * FROM t /* {a:#{b}} */ WHERE y = ?", "y");

        // Documented limitation (see parameterizedCql()): a marker directly after '{' is not rewritten; with a space it is.
        assertIbatisParsed("UPDATE t SET tags = tags + {#{tag}} WHERE id = #{id}", "UPDATE t SET tags = tags + {#{tag}} WHERE id = ?", "id");
        assertIbatisParsed("UPDATE t SET tags = tags + { #{tag} } WHERE id = #{id}", "UPDATE t SET tags = tags + { ? } WHERE id = ?", "tag", "id");
    }

    // ---- 2026-10-03 fixPC ----

    /** Asserts that {@code cql} parses exactly like {@code singleTokenForm}, the same statement with the markers unspaced. */
    private static void assertParsedLike(final String cql, final String singleTokenForm) {
        final ParsedCql parsed = ParsedCql.parse(cql);
        final ParsedCql expected = ParsedCql.parse(singleTokenForm);
        assertEquals(expected.parameterizedCql(), parsed.parameterizedCql(), cql);
        assertEquals(expected.parameterCount(), parsed.parameterCount(), cql);
        assertEquals(expected.namedParameters(), parsed.namedParameters(), cql);
    }

    @Test
    public void testParse_IbatisValueGluedAfterKeyMarkerWithMetadata_isRewritten() {
        // Regression: a key marker with MyBatis metadata spans several tokens ("#{k" "," "jdbcType" "=" "TEXT}:#{v}"). The
        // glued-marker split ran only on the first of them, so the value marker in the token holding the key's '}' was
        // appended verbatim ("{ ?:#{v} }"): invalid CQL, and "v" missing from the bindings.
        assertIbatisParsed("UPDATE t SET m = m + { #{k,jdbcType=TEXT}:#{v} } WHERE id = #{id}", "UPDATE t SET m = m + { ?:? } WHERE id = ?", "k", "v", "id");
        assertIbatisParsed("UPDATE t SET m = m + { #{k,javaType=String}:#{v} } WHERE id = #{id}", "UPDATE t SET m = m + { ?:? } WHERE id = ?", "k", "v", "id");
        assertIbatisParsed("UPDATE t SET m = m + { #{k,javaType=String,jdbcType=TEXT}:#{v} } WHERE id = #{id}", "UPDATE t SET m = m + { ?:? } WHERE id = ?",
                "k", "v", "id");
        assertIbatisParsed("UPDATE t SET m = m + { #{k, javaType=String, jdbcType=VARCHAR}:#{v} } WHERE id = #{id}",
                "UPDATE t SET m = m + { ?:? } WHERE id = ?", "k", "v", "id");
        // Metadata on both markers; the parameterizedCql() Javadoc example.
        assertIbatisParsed("UPDATE t SET m = m + { #{k,jdbcType=TEXT}:#{v,jdbcType=INT} } WHERE id = #{id}", "UPDATE t SET m = m + { ?:? } WHERE id = ?",
                "k", "v", "id");
        assertIbatisParsed("UPDATE t SET m = m + { #{k, jdbcType=VARCHAR}:#{v} } WHERE id = #{id}", "UPDATE t SET m = m + { ?:? } WHERE id = ?", "k", "v",
                "id");
    }

    @Test
    public void testParse_IbatisValueGluedAfterKeyMarkerWithWhitespace_isRewritten() {
        // Same defect with whitespace inside the key marker's braces: "#{ k }:#{v}" is tokenized "#{" " " "k" " " "}:#{v}".
        assertIbatisParsed("UPDATE t SET m = m + { #{ k }:#{v} } WHERE id = #{id}", "UPDATE t SET m = m + { ?:? } WHERE id = ?", "k", "v", "id");
        assertIbatisParsed("UPDATE t SET m = m + { #{k }:#{v}} WHERE id = #{id}", "UPDATE t SET m = m + { ?:?} WHERE id = ?", "k", "v", "id");
        assertIbatisParsed("UPDATE t SET m = m + { #{ k}:#{v} } WHERE id = #{id}", "UPDATE t SET m = m + { ?:? } WHERE id = ?", "k", "v", "id");
        // Whitespace in both markers, and whitespace together with metadata.
        assertIbatisParsed("UPDATE t SET m = m + { #{ k }:#{ v } } WHERE id = #{id}", "UPDATE t SET m = m + { ?:? } WHERE id = ?", "k", "v", "id");
        assertIbatisParsed("UPDATE t SET m = m + { #{ k , jdbcType=TEXT }:#{ v ,jdbcType=INT} } WHERE id = #{id}", "UPDATE t SET m = m + { ?:? } WHERE id = ?",
                "k", "v", "id");

        // Pins: whitespace only inside the VALUE marker already worked (the key "#{k}:" is then a single token).
        assertIbatisParsed("UPDATE t SET m = m + { #{k}:#{ v } } WHERE id = #{id}", "UPDATE t SET m = m + { ?:? } WHERE id = ?", "k", "v", "id");
        assertIbatisParsed("UPDATE t SET m = m + { #{k}:#{v } } WHERE id = #{id}", "UPDATE t SET m = m + { ?:? } WHERE id = ?", "k", "v", "id");
    }

    @Test
    public void testParse_IbatisValueGluedAfterMultiTokenKey_multiplePairsAndNesting() {
        assertIbatisParsed("UPDATE t SET m = m + { #{k1,jdbcType=TEXT}:#{v1}, #{k2}:#{v2,jdbcType=INT} } WHERE id = #{id}",
                "UPDATE t SET m = m + { ?:?, ?:? } WHERE id = ?", "k1", "v1", "k2", "v2", "id");
        assertIbatisParsed("UPDATE t SET m = m + { #{k1 }:#{v1},#{k2 }:#{v2} } WHERE id = #{id}", "UPDATE t SET m = m + { ?:?,?:? } WHERE id = ?", "k1",
                "v1", "k2", "v2", "id");

        // A nested literal as the value: its glued markers are found after a multi-token key as well.
        assertIbatisParsed("UPDATE t SET m = m + { #{ k }:{a:#{x}} } WHERE id = #{id}", "UPDATE t SET m = m + { ?:{a:?} } WHERE id = ?", "k", "x", "id");
        assertIbatisParsed("UPDATE t SET m = m + { #{k,jdbcType=TEXT}:{a:#{x},b:#{y}} } WHERE id = #{id}", "UPDATE t SET m = m + { ?:{a:?,b:?} } WHERE id = ?",
                "k", "x", "y", "id");
        // The map literal nested in another literal.
        assertIbatisParsed("UPDATE t SET m = {'a':{ #{ k }:#{v}}} WHERE id = #{id}", "UPDATE t SET m = {'a':{ ?:?}} WHERE id = ?", "k", "v", "id");
        // Pin: the spaced nested literal already worked.
        assertIbatisParsed("UPDATE t SET m = m + { #{k}:{ a:#{x} } } WHERE id = #{id}", "UPDATE t SET m = m + { ?:{ a:? } } WHERE id = ?", "k", "x", "id");

        // A chained tail is handled like the single-token form "#{k}:#{v}:#{w}".
        assertParsedLike("UPDATE t SET m = m + { #{k,jdbcType=TEXT}:#{v}:#{w} } WHERE id = #{id}", "UPDATE t SET m = m + { #{k}:#{v}:#{w} } WHERE id = #{id}");
        assertParsedLike("UPDATE t SET m = m + { #{ k }:#{v}:#{w} } WHERE id = #{id}", "UPDATE t SET m = m + { #{k}:#{v}:#{w} } WHERE id = #{id}");
        assertEquals(4, ParsedCql.parse("UPDATE t SET m = m + { #{ k }:#{v}:#{w} } WHERE id = #{id}").parameterCount());
    }

    @Test
    public void testParse_IbatisValueGluedAfterMultiTokenKey_inEveryClause() {
        assertIbatisParsed("INSERT INTO t (id, m) VALUES (#{id}, { #{k,jdbcType=TEXT}:#{v} })", "INSERT INTO t (id, m) VALUES (?, { ?:? })", "id", "k", "v");
        assertIbatisParsed("SELECT * FROM t WHERE m = { #{ k }:#{v} } AND id = #{id}", "SELECT * FROM t WHERE m = { ?:? } AND id = ?", "k", "v", "id");
        assertIbatisParsed("UPDATE t SET m = m + { #{ k }:#{v} } WHERE id = #{id} IF m = { #{ k2 }:#{v2} }",
                "UPDATE t SET m = m + { ?:? } WHERE id = ? IF m = { ?:? }", "k", "v", "id", "k2", "v2");
        assertIbatisParsed(
                "BEGIN BATCH UPDATE t SET m = m + { #{ k }:#{v} } WHERE id = #{id}; INSERT INTO t (id, m) VALUES (#{id2}, { #{k2,jdbcType=TEXT}:#{v2} }); APPLY BATCH",
                "BEGIN BATCH UPDATE t SET m = m + { ?:? } WHERE id = ?; INSERT INTO t (id, m) VALUES (?, { ?:? }); APPLY BATCH", "k", "v", "id", "id2", "k2",
                "v2");
    }

    @Test
    public void testParse_IbatisValueGluedAfterMultiTokenKey_isValidatedLikeSingleTokenForm() {
        // The split-off value marker is validated like the one of the single-token form "{ #{k}:#{} }" instead of being
        // passed through verbatim.
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("UPDATE t SET m = m + { #{ k }:#{} } WHERE id = #{id}"));
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("UPDATE t SET m = m + { #{k}:#{} } WHERE id = #{id}"));
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("UPDATE t SET m = m + { #{ k }:#{v WHERE id = 1"));
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("UPDATE t SET m = m + { #{k}:#{v WHERE id = 1"));

        // Mixed styles are still rejected.
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("UPDATE t SET m = m + { #{k,jdbcType=TEXT}:#{v} } WHERE id = ?"));
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("UPDATE t SET m = m + { #{ k }:#{v} } WHERE id = :id"));
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("UPDATE t SET m = m + { #{ k }:? } WHERE id = #{id}"));
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("UPDATE t SET m = m + { #{ k }:#{v}, 'a':? } WHERE id = #{id}"));
    }

    @Test
    public void testParse_IbatisMarkerAfterMultiTokenKey_outsideBracesOrInLiteral_isNotSplit() {
        // Pins: outside braces ':' is no field separator, so the tail stays as it is, exactly as for the single-token form.
        assertIbatisParsed("SELECT * FROM t WHERE x = #{a,jdbcType=INT}:#{b} AND id = #{id}", "SELECT * FROM t WHERE x = ?:#{b} AND id = ?", "a", "id");
        assertParsedLike("SELECT * FROM t WHERE x = #{ a }:#{b} AND id = #{id}", "SELECT * FROM t WHERE x = #{a}:#{b} AND id = #{id}");
        assertIbatisParsed("UPDATE t SET l = l + [ #{k,jdbcType=TEXT}:#{v} ] WHERE id = #{id}", "UPDATE t SET l = l + [ ?:#{v} ] WHERE id = ?", "k", "id");

        // Pins: a lookalike in a string, a dollar-quoted constant or a kept block comment after a multi-token key is data.
        assertIbatisParsed("UPDATE t SET m = m + { #{ k }:'x:#{v}' } WHERE id = #{id}", "UPDATE t SET m = m + { ?:'x:#{v}' } WHERE id = ?", "k", "id");
        assertIbatisParsed("UPDATE t SET m = m + { #{ k }:{a:'#{x}'} } WHERE id = #{id}", "UPDATE t SET m = m + { ?:{a:'#{x}'} } WHERE id = ?", "k", "id");
        assertIbatisParsed("UPDATE t SET m = m + { #{k,jdbcType=TEXT}:$$x:#{v}$$ } WHERE id = #{id}", "UPDATE t SET m = m + { ?:$$x:#{v}$$ } WHERE id = ?",
                "k", "id");
        assertIbatisParsed("-- Keep comments\nUPDATE t SET m = m + { #{ k }/* :#{x} */:#{v} } WHERE id = #{id}",
                "UPDATE t SET m = m + { ?/* :#{x} */:? } WHERE id = ?", "k", "v", "id");
        // A kept block comment pulled into an (unclosed) marker is never split, so a marker inside it is never bound.
        assertFalse(ParsedCql.parse("-- Keep comments\nUPDATE t SET m = m + { #{ k /* }:#{x} */ }:#{v} } WHERE id = #{id}").namedParameters().containsValue("x"));
    }

    // ---- 2026-10-04 coverageCQ ----

    @Test
    public void testParse_coverageCQ_gluedIbatisMarker_everyKeyKindAndStatementShape() {
        // The glued-marker split for key kinds and statement shapes the earlier tests do not use. assertAll reports every
        // case, so the HEAD run lists each shape whose "#{...}" reached the driver unconverted.
        org.junit.jupiter.api.Assertions.assertAll(
                // Keys: dollar-quoted (masked to one "$$n$$" token glued to the marker), doubled-quote, backslash-ending
                // (a backslash is data in CQL), numeric, negative, non-ASCII, and a marker key after an unspaced ','.
                () -> assertIbatisParsed("UPDATE t SET m = m + {$$k$$:#{v}} WHERE id = #{id}", "UPDATE t SET m = m + {$$k$$:?} WHERE id = ?", "v", "id"),
                () -> assertIbatisParsed("UPDATE t SET m = m + { $$k$$:#{v} } WHERE id = #{id}", "UPDATE t SET m = m + { $$k$$:? } WHERE id = ?", "v", "id"),
                () -> assertIbatisParsed("UPDATE t SET m = m + {'it''s':#{v}} WHERE id = #{id}", "UPDATE t SET m = m + {'it''s':?} WHERE id = ?", "v", "id"),
                () -> assertIbatisParsed("UPDATE t SET m = m + {\"it\"\"s\":#{v}} WHERE id = #{id}", "UPDATE t SET m = m + {\"it\"\"s\":?} WHERE id = ?", "v",
                        "id"),
                () -> assertIbatisParsed("UPDATE t SET m = m + {'a\\':#{v}} WHERE id = #{id}", "UPDATE t SET m = m + {'a\\':?} WHERE id = ?", "v", "id"),
                () -> assertIbatisParsed("UPDATE t SET m = m + {1:#{v}} WHERE id = #{id}", "UPDATE t SET m = m + {1:?} WHERE id = ?", "v", "id"),
                () -> assertIbatisParsed("UPDATE t SET m = m + {-1:#{v}} WHERE id = #{id}", "UPDATE t SET m = m + {-1:?} WHERE id = ?", "v", "id"),
                () -> assertIbatisParsed("UPDATE t SET m = m + {ключ:#{v}} WHERE id = #{id}",
                        "UPDATE t SET m = m + {ключ:?} WHERE id = ?", "v", "id"),
                () -> assertIbatisParsed("UPDATE t SET m = m + {'a':1,#{k}:#{v}} WHERE id = #{id}", "UPDATE t SET m = m + {'a':1,?:?} WHERE id = ?", "k", "v",
                        "id"),
                // Values: metadata or whitespace inside the glued marker, and the same name twice.
                () -> assertIbatisParsed("UPDATE t SET m = m + {k:#{v,jdbcType=INT}} WHERE id = #{id}", "UPDATE t SET m = m + {k:?} WHERE id = ?", "v", "id"),
                () -> assertIbatisParsed("UPDATE t SET m = m + {k:#{v, jdbcType=INT}} WHERE id = #{id}", "UPDATE t SET m = m + {k:?} WHERE id = ?", "v", "id"),
                () -> assertIbatisParsed("UPDATE t SET m = m + {k:#{ v }} WHERE id = #{id}", "UPDATE t SET m = m + {k:?} WHERE id = ?", "v", "id"),
                () -> assertIbatisParsed("UPDATE t SET m = m + {k:#{v},k2:#{v}} WHERE id = #{id}", "UPDATE t SET m = m + {k:?,k2:?} WHERE id = ?", "v", "v",
                        "id"),
                // Statement shapes: two literals, literals inside a list and a tuple, three nesting levels, no WHERE (with
                // and without ';'), IF NOT EXISTS / IF EXISTS.
                () -> assertIbatisParsed("UPDATE t SET m = m + {k:#{v}}, n = n + {k2:#{w}} WHERE id = #{id}",
                        "UPDATE t SET m = m + {k:?}, n = n + {k2:?} WHERE id = ?", "v", "w", "id"),
                () -> assertIbatisParsed("UPDATE t SET l = l + [{a:#{x}}, {b:#{y}}] WHERE id = #{id}", "UPDATE t SET l = l + [{a:?}, {b:?}] WHERE id = ?", "x",
                        "y", "id"),
                () -> assertIbatisParsed("UPDATE t SET tup = (#{a}, {k:#{v}}) WHERE id = #{id}", "UPDATE t SET tup = (?, {k:?}) WHERE id = ?", "a", "v", "id"),
                () -> assertIbatisParsed("UPDATE t SET u = {a:{b:{c:#{x}}}} WHERE id = #{id}", "UPDATE t SET u = {a:{b:{c:?}}} WHERE id = ?", "x", "id"),
                () -> assertIbatisParsed("UPDATE t SET m = m + {k:#{v}}", "UPDATE t SET m = m + {k:?}", "v"),
                () -> assertIbatisParsed("UPDATE t SET m = m + {k:#{v}};", "UPDATE t SET m = m + {k:?}", "v"),
                () -> assertIbatisParsed("INSERT INTO t (id, m) VALUES (#{id}, {k:#{v}}) IF NOT EXISTS", "INSERT INTO t (id, m) VALUES (?, {k:?}) IF NOT EXISTS",
                        "id", "v"),
                () -> assertIbatisParsed("UPDATE t SET m = m + {k:#{v}} WHERE id = #{id} IF EXISTS", "UPDATE t SET m = m + {k:?} WHERE id = ? IF EXISTS", "v",
                        "id"),
                // Block comments next to the glued marker: removed, or kept as tokens of their own ("-- Keep comments").
                () -> assertIbatisParsed("UPDATE t SET m = m + {k/* c */:#{v}} WHERE id = #{id}", "UPDATE t SET m = m + {k :?} WHERE id = ?", "v", "id"),
                () -> assertIbatisParsed("-- Keep comments\nUPDATE t SET m = m + {k/* c */:#{v}} WHERE id = #{id}",
                        "UPDATE t SET m = m + {k/* c */:?} WHERE id = ?", "v", "id"),
                () -> assertIbatisParsed("-- Keep comments\nUPDATE t SET m = m + {k:#{v}/* :#{b} */} WHERE id = #{id}",
                        "UPDATE t SET m = m + {k:?/* :#{b} */} WHERE id = ?", "v", "id"),
                // An empty glued marker is rejected like the spaced form "{k: #{}}".
                () -> assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("UPDATE t SET m = m + {k:#{}} WHERE id = #{id}")));

        // Pins (unchanged from HEAD): the spaced form; ":#{" inside a string whose quote is doubled; after an
        // unterminated "$$" the rest of the statement is dollar-quoted text and nothing in it is split or bound.
        assertIbatisParsed("UPDATE t SET u = {a: #{x}, b: #{y}} WHERE id = #{id}", "UPDATE t SET u = {a: ?, b: ?} WHERE id = ?", "x", "y", "id");
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("UPDATE t SET m = m + {k: #{}} WHERE id = #{id}"));
        assertIbatisParsed("UPDATE t SET m = m + {'a'':#{x}':1} WHERE id = #{id}", "UPDATE t SET m = m + {'a'':#{x}':1} WHERE id = ?", "id");
        assertIbatisParsed("SELECT * FROM t WHERE x = #{x} AND y = $$ {a:#{b}}", "SELECT * FROM t WHERE x = ? AND y = $$ {a:#{b}}", "x");
        assertIbatisParsed("SELECT * FROM t WHERE x = #{x} AND y = $$ { #{ k }:#{b} }", "SELECT * FROM t WHERE x = ? AND y = $$ { #{ k }:#{b} }", "x");
    }
}
