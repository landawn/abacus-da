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
