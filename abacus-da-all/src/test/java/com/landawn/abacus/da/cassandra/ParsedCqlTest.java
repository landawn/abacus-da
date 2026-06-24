/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;

public class ParsedCqlTest extends TestBase {

    // ---------------------------------------------------------------------------------------------
    // parse(): positional, named, MyBatis-style, caching, attrs handling.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testParse_PositionalParameters() {
        final ParsedCql parsed = ParsedCql.parse("SELECT * FROM users WHERE id = ? AND status = ?", null);
        assertNotNull(parsed);
        assertEquals(2, parsed.parameterCount());
        assertTrue(parsed.namedParameters().isEmpty());
        assertEquals("SELECT * FROM users WHERE id = ? AND status = ?", parsed.getParameterizedCql());
    }

    @Test
    public void testParse_NamedParameters() {
        final ParsedCql parsed = ParsedCql.parse("SELECT * FROM users WHERE id = :userId AND status = :status", null);
        assertEquals(2, parsed.parameterCount());
        assertEquals("userId", parsed.namedParameters().get(0));
        assertEquals("status", parsed.namedParameters().get(1));
        // Parameterized CQL must convert :name to ?.
        assertTrue(parsed.getParameterizedCql().contains("?"), parsed.getParameterizedCql());
        assertFalse(parsed.getParameterizedCql().contains(":"), parsed.getParameterizedCql());
    }

    @Test
    public void testParse_BareNamedParameterMarkerThrows() {
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("SELECT * FROM users WHERE id = :", null));
    }

    @Test
    public void testParse_MapLiteralColonIsNotNamedParameter() {
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET m = {'a': 'b'} WHERE id = ?", null);

        assertEquals(1, parsed.parameterCount());
        assertEquals("UPDATE t SET m = {'a': 'b'} WHERE id = ?", parsed.getParameterizedCql());
    }

    @Test
    public void testParse_UnspacedMapLiteralColonIsNotNamedParameter_withPositionalParam() {
        // Regression: an unspaced map literal like {'a':1,'b':2} must NOT have its ':' treated as named parameters.
        // Before the fix this threw "Cannot mix parameter styles" because :1 / :2} were registered as named params
        // alongside the real '?'.
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET m = {'a':1,'b':2} WHERE id = ?", null);

        assertEquals(1, parsed.parameterCount());
        assertTrue(parsed.namedParameters().isEmpty(), parsed.namedParameters().toString());
        assertEquals("UPDATE t SET m = {'a':1,'b':2} WHERE id = ?", parsed.getParameterizedCql());
    }

    @Test
    public void testParse_UnspacedMapLiteralColonIsNotNamedParameter_noParam() {
        // Regression: with no real parameter, an unspaced map literal must round-trip unchanged (was corrupted to
        // "{'a'?,'b'? WHERE id = 5" with parameterCount==2 before the fix).
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET m = {'a':1,'b':2} WHERE id = 5", null);

        assertEquals(0, parsed.parameterCount());
        assertTrue(parsed.namedParameters().isEmpty(), parsed.namedParameters().toString());
        assertEquals("UPDATE t SET m = {'a':1,'b':2} WHERE id = 5", parsed.getParameterizedCql());
    }

    @Test
    public void testParse_NamedParameterAfterMapLiteralStillWorks() {
        // The ':param' AFTER a closed map literal must still be recognized as a named parameter.
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET m = {'a':1} WHERE id = :userId", null);

        assertEquals(1, parsed.parameterCount());
        assertEquals("userId", parsed.namedParameters().get(0));
        assertTrue(parsed.getParameterizedCql().contains("{'a':1}"), parsed.getParameterizedCql());
        assertTrue(parsed.getParameterizedCql().endsWith("WHERE id = ?"), parsed.getParameterizedCql());
    }

    @Test
    public void testParse_OpenBraceInStringLiteral_doesNotSwallowNamedParam() {
        // Regression: a '{' that appears INSIDE a string literal must not raise the map/UDT-literal brace depth.
        // Before the fix, updateCurlyDepth counted the '{' inside 'a{b', so the depth stayed > 0 and the following
        // ':id' was silently treated as "inside a map literal" and never converted to '?'.
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET note = 'a{b' WHERE id = :id", null);

        assertEquals(1, parsed.parameterCount());
        assertEquals("id", parsed.namedParameters().get(0));
        assertTrue(parsed.getParameterizedCql().contains("'a{b'"), parsed.getParameterizedCql());
        assertTrue(parsed.getParameterizedCql().endsWith("WHERE id = ?"), parsed.getParameterizedCql());
    }

    @Test
    public void testParse_CloseBraceInMapStringValue_doesNotMisparse() {
        // Regression: a '}' inside a string VALUE of a map literal must not prematurely drop the brace depth.
        // Before the fix the '}' in 'a}b' dropped depth to 0, so the following ':' map separator was seen at
        // depth 0 and registered as a spurious named parameter, throwing "Cannot mix parameter styles".
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET m = {'k':'a}b','n':5} WHERE id = ?", null);

        assertEquals(1, parsed.parameterCount());
        assertTrue(parsed.namedParameters().isEmpty(), parsed.namedParameters().toString());
        assertTrue(parsed.getParameterizedCql().contains("{'k':'a}b','n':5}"), parsed.getParameterizedCql());
        assertTrue(parsed.getParameterizedCql().endsWith("WHERE id = ?"), parsed.getParameterizedCql());
    }

    @Test
    public void testParse_JsonStringValueWithBraces_namedParamStillRecognized() {
        // Realistic case: storing a JSON fragment (balanced or not) in a string column, followed by a named param.
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET data = '{\"k\":1' WHERE id = :id", null);

        assertEquals(1, parsed.parameterCount());
        assertEquals("id", parsed.namedParameters().get(0));
        assertTrue(parsed.getParameterizedCql().contains("'{\"k\":1'"), parsed.getParameterizedCql());
        assertTrue(parsed.getParameterizedCql().endsWith("WHERE id = ?"), parsed.getParameterizedCql());
    }

    @Test
    public void testParse_IbatisStyleParameters() {
        final ParsedCql parsed = ParsedCql.parse("SELECT * FROM users WHERE id = #{userId} AND status = #{status}", null);
        assertEquals(2, parsed.parameterCount());
        assertEquals("userId", parsed.namedParameters().get(0));
        assertEquals("status", parsed.namedParameters().get(1));
        assertTrue(parsed.getParameterizedCql().contains("?"), parsed.getParameterizedCql());
        assertFalse(parsed.getParameterizedCql().contains("#{"), parsed.getParameterizedCql());
    }

    @Test
    public void testParse_NoParameters() {
        final ParsedCql parsed = ParsedCql.parse("SELECT * FROM users", null);
        assertEquals(0, parsed.parameterCount());
        assertTrue(parsed.namedParameters().isEmpty());
    }

    @Test
    public void testParse_TrailingSemicolonRemoved() {
        final ParsedCql parsed = ParsedCql.parse("SELECT id FROM users WHERE id = ?;", null);
        assertFalse(parsed.getParameterizedCql().endsWith(";"), parsed.getParameterizedCql());
    }

    @Test
    public void testParse_MultipleTrailingSemicolonsRemoved() {
        // All trailing semicolons (and any whitespace between/around them) are stripped, not just one.
        assertEquals("SELECT id FROM users WHERE id = ?", ParsedCql.parse("SELECT id FROM users WHERE id = ?;;", null).getParameterizedCql());
        assertEquals("SELECT id FROM users WHERE id = ?", ParsedCql.parse("SELECT id FROM users WHERE id = ? ; ; ", null).getParameterizedCql());
    }

    @Test
    public void testParse_IbatisParameterWithJdbcTypeUsesNameOnly() {
        // MyBatis-style metadata after the first comma (e.g. jdbcType) is dropped; only the property name remains.
        final ParsedCql parsed = ParsedCql.parse("UPDATE t SET n = #{name,jdbcType=TEXT} WHERE id = #{id}", null);
        assertEquals(2, parsed.parameterCount());
        assertEquals("name", parsed.namedParameters().get(0));
        assertEquals("id", parsed.namedParameters().get(1));
        assertFalse(parsed.getParameterizedCql().contains("#{"), parsed.getParameterizedCql());
    }

    @Test
    public void testParse_NullCqlThrows() {
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse(null, null));
    }

    @Test
    public void testParse_CachesIdenticalCql() {
        // Same CQL with null attrs must return the cached (same-reference) instance.
        final String cql = "SELECT * FROM cache_test_users WHERE id = ? AND age > ?";
        final ParsedCql p1 = ParsedCql.parse(cql, null);
        final ParsedCql p2 = ParsedCql.parse(cql, null);
        assertSame(p1, p2);
    }

    @Test
    public void testParse_BypassesCacheWithAttributes() {
        // Non-empty attrs must always produce a fresh instance.
        final String cql = "INSERT INTO sessions_unique_for_attr_test (id, token) VALUES (?, ?)";
        final Map<String, String> attrs1 = new HashMap<>();
        attrs1.put("consistency", "QUORUM");
        final Map<String, String> attrs2 = new HashMap<>();
        attrs2.put("consistency", "LOCAL_ONE");

        final ParsedCql p1 = ParsedCql.parse(cql, attrs1);
        final ParsedCql p2 = ParsedCql.parse(cql, attrs2);
        assertNotSame(p1, p2);
        assertEquals("QUORUM", p1.getAttributes().get("consistency"));
        assertEquals("LOCAL_ONE", p2.getAttributes().get("consistency"));
    }

    @Test
    public void testParse_MixedParameterStylesThrows() {
        // ? + :name mix is forbidden.
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("SELECT * FROM u WHERE id = ? AND status = :status", null));
    }

    @Test
    public void testParse_MixedQuestionAndIbatisStylesThrows() {
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("SELECT * FROM u WHERE id = ? AND status = #{status}", null));
    }

    @Test
    public void testParse_MixedNamedAndIbatisStylesThrows() {
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("SELECT * FROM u WHERE id = :id AND status = #{status}", null));
    }

    @Test
    public void testParse_EmptyIbatisParameterNameThrows() {
        // "#{}" is too short — empty name is rejected.
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("SELECT * FROM u WHERE id = #{}", null));
    }

    @Test
    public void testParse_DDLStatementSkipsParameterDetection() {
        // Non-DML (e.g. CREATE) is stored as-is — no parameter scanning, count=0.
        final ParsedCql parsed = ParsedCql.parse("CREATE TABLE foo (id INT PRIMARY KEY, name TEXT)", null);
        assertEquals(0, parsed.parameterCount());
        assertTrue(parsed.namedParameters().isEmpty());
    }

    @Test
    public void testParse_InsertStatement() {
        final ParsedCql parsed = ParsedCql.parse("INSERT INTO users_parsed_insert (id, name, email) VALUES (?, ?, ?)", null);
        assertEquals(3, parsed.parameterCount());
    }

    @Test
    public void testParse_UpdateStatement() {
        final ParsedCql parsed = ParsedCql.parse("UPDATE users_parsed_update SET name = ? WHERE id = ?", null);
        assertEquals(2, parsed.parameterCount());
    }

    @Test
    public void testParse_DeleteStatement() {
        final ParsedCql parsed = ParsedCql.parse("DELETE FROM users_parsed_delete WHERE id = ?", null);
        assertEquals(1, parsed.parameterCount());
    }

    // ---------------------------------------------------------------------------------------------
    // Getters: originalCql, getParameterizedCql, namedParameters, parameterCount, getAttributes.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testOriginalCql_TrimsWhitespace() {
        // originalCql() returns the trimmed input.
        final ParsedCql parsed = ParsedCql.parse("   SELECT * FROM users_original_cql_trim WHERE id = ?   ", null);
        assertEquals("SELECT * FROM users_original_cql_trim WHERE id = ?", parsed.originalCql());
    }

    @Test
    public void testGetAttributes_EmptyByDefault() {
        final ParsedCql parsed = ParsedCql.parse("SELECT 1 FROM system.local", null);
        assertNotNull(parsed.getAttributes());
        assertTrue(parsed.getAttributes().isEmpty());
    }

    @Test
    public void testGetAttributes_PassedAttributes() {
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("timeout", "5000");
        attrs.put("consistency", "QUORUM");
        final ParsedCql parsed = ParsedCql.parse("SELECT * FROM attr_passed_users WHERE id = ?", attrs);
        assertEquals("5000", parsed.getAttributes().get("timeout"));
        assertEquals("QUORUM", parsed.getAttributes().get("consistency"));
    }

    // ---------------------------------------------------------------------------------------------
    // equals / hashCode / toString: equality is by CQL only.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testEquals_SameInstance() {
        final ParsedCql p = ParsedCql.parse("SELECT 1 FROM equals_same_instance", null);
        assertTrue(p.equals(p));
    }

    @Test
    public void testEquals_SameCql() {
        final ParsedCql p1 = ParsedCql.parse("SELECT 1 FROM equals_same_cql_a", null);
        final ParsedCql p2 = ParsedCql.parse("SELECT 1 FROM equals_same_cql_a", null);
        assertTrue(p1.equals(p2));
    }

    @Test
    public void testEquals_DifferentCql() {
        final ParsedCql p1 = ParsedCql.parse("SELECT 1 FROM equals_diff_a", null);
        final ParsedCql p2 = ParsedCql.parse("SELECT 1 FROM equals_diff_b", null);
        assertFalse(p1.equals(p2));
    }

    @Test
    public void testEquals_DifferentType() {
        final ParsedCql p = ParsedCql.parse("SELECT 1 FROM equals_diff_type", null);
        assertFalse(p.equals("not a ParsedCql"));
        assertFalse(p.equals(null));
    }

    @Test
    public void testHashCode_ConsistentWithEquals() {
        final ParsedCql p1 = ParsedCql.parse("SELECT 1 FROM hash_consistent", null);
        final ParsedCql p2 = ParsedCql.parse("SELECT 1 FROM hash_consistent", null);
        assertEquals(p1.hashCode(), p2.hashCode());
    }

    @Test
    public void testToString_ContainsExpectedFragments() {
        final ParsedCql parsed = ParsedCql.parse("SELECT 1 FROM tostring_test", null);
        final String s = parsed.toString();
        assertTrue(s.contains("[cql]"), s);
        assertTrue(s.contains("[parameterizedCql]"), s);
        assertTrue(s.contains("[Attributes]"), s);
        assertTrue(s.contains("tostring_test"), s);
    }
}
