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
}
