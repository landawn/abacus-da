/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;

public class CqlMapperTest extends TestBase {

    // ---------------------------------------------------------------------------------------------
    // Construction: default ctor + add/get + isEmpty.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testDefaultConstructor_IsEmpty() {
        final CqlMapper m = new CqlMapper();
        assertTrue(m.isEmpty());
        assertTrue(m.cqlIds().isEmpty());
    }

    @Test
    public void testAddParsedCql_StoresAndRetrieves() {
        final CqlMapper m = new CqlMapper();
        final ParsedCql parsed = ParsedCql.parse("SELECT * FROM mapper_test_add_parsed WHERE id = ?");
        m.add("findById", parsed);
        assertEquals(parsed, m.get("findById"));
        assertFalse(m.isEmpty());
        // No attributes supplied => empty (never null for a present id).
        assertTrue(m.getAttributes("findById").isEmpty());
    }

    @Test
    public void testAddParsedCql_DuplicateIdThrows() {
        final CqlMapper m = new CqlMapper();
        final ParsedCql p1 = ParsedCql.parse("SELECT * FROM mapper_test_replace_a WHERE id = ?");
        final ParsedCql p2 = ParsedCql.parse("SELECT * FROM mapper_test_replace_b WHERE id = ?");
        m.add("dup", p1);
        // add(String, ParsedCql) rejects a reused id (consistent with the String overloads + SqlMapper).
        assertThrows(IllegalArgumentException.class, () -> m.add("dup", p2));
        // The original mapping is left intact.
        assertEquals(p1, m.get("dup"));
    }

    @Test
    public void testAddParsedCql_NullParsedCqlThrows() {
        final CqlMapper m = new CqlMapper();
        assertThrows(IllegalArgumentException.class, () -> m.add("findById", (ParsedCql) null));
        assertThrows(IllegalArgumentException.class, () -> m.add("findById", (ParsedCql) null, new HashMap<>()));
        // Nothing was stored.
        assertTrue(m.isEmpty());
    }

    @Test
    public void testAddParsedCql_WithAttributes() {
        final CqlMapper m = new CqlMapper();
        final ParsedCql parsed = ParsedCql.parse("SELECT * FROM mapper_test_add_parsed_attrs WHERE id = ?");
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("consistency", "QUORUM");
        m.add("findById", parsed, attrs);
        assertEquals(parsed, m.get("findById"));
        assertEquals("QUORUM", m.getAttributes("findById").get("consistency"));
    }

    @Test
    public void testAddCqlString_StoresParsedCqlAndAttributes() {
        final CqlMapper m = new CqlMapper();
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("timeout", "3000");
        m.add("findUsers", "SELECT * FROM mapper_test_add_string WHERE id = ?", attrs);
        final ParsedCql parsed = m.get("findUsers");
        assertNotNull(parsed);
        assertEquals(1, parsed.parameterCount());
        // Attributes live on the mapper, keyed by id (not on ParsedCql).
        assertEquals("3000", m.getAttributes("findUsers").get("timeout"));
    }

    @Test
    public void testAddCqlString_NoAttributesOverload() {
        final CqlMapper m = new CqlMapper();
        m.add("findAll", "SELECT * FROM mapper_test_add_no_attrs");
        assertNotNull(m.get("findAll"));
        assertTrue(m.getAttributes("findAll").isEmpty());
    }

    @Test
    public void testGetAttributes_AbsentIdReturnsNull() {
        final CqlMapper m = new CqlMapper();
        assertNull(m.getAttributes("noSuchId"));
    }

    @Test
    public void testRemove_AlsoClearsAttributes() {
        final CqlMapper m = new CqlMapper();
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("timeout", "1000");
        m.add("k", "SELECT * FROM mapper_test_remove_attrs WHERE id = ?", attrs);
        m.remove("k");
        assertNull(m.get("k"));
        assertNull(m.getAttributes("k"));
    }

    @Test
    public void testAddCqlString_DuplicateIdThrows() {
        final CqlMapper m = new CqlMapper();
        m.add("k1", "SELECT * FROM mapper_test_dup_id WHERE id = ?", null);
        assertThrows(IllegalArgumentException.class, () -> m.add("k1", "SELECT * FROM mapper_test_dup_id_2 WHERE id = ?", null));
    }

    @Test
    public void testGet_MissingReturnsNull() {
        final CqlMapper m = new CqlMapper();
        assertNull(m.get("nonexistent"));
    }

    @Test
    public void testContainsId() {
        final CqlMapper m = new CqlMapper();
        m.add("findById", "SELECT * FROM mapper_test_contains WHERE id = ?", null);
        assertTrue(m.containsId("findById"));
        assertFalse(m.containsId("missing"));
        assertFalse(m.containsId(null));
    }

    @Test
    public void testSize_ReflectsAddAndRemove() {
        final CqlMapper m = new CqlMapper();
        assertEquals(0, m.size());
        m.add("a", "SELECT * FROM mapper_test_size_a", null);
        m.add("b", "SELECT * FROM mapper_test_size_b", null);
        assertEquals(2, m.size());
        m.remove("a");
        assertEquals(1, m.size());
    }

    @Test
    public void testKeySet_ContainsAddedIds() {
        final CqlMapper m = new CqlMapper();
        m.add("alpha", "SELECT * FROM mapper_test_keyset_a", null);
        m.add("beta", "SELECT * FROM mapper_test_keyset_b", null);
        final Set<String> keys = m.cqlIds();
        assertEquals(2, keys.size());
        assertTrue(keys.contains("alpha"));
        assertTrue(keys.contains("beta"));
    }

    @Test
    public void testKeySet_IsReadOnly() {
        final CqlMapper m = new CqlMapper();
        m.add("k", "SELECT * FROM mapper_test_keyset_readonly", null);
        // keySet() is a read-only live view: it reflects the mapper but cannot mutate it.
        assertThrows(UnsupportedOperationException.class, () -> m.cqlIds().clear());
        assertThrows(UnsupportedOperationException.class, () -> m.cqlIds().remove("k"));
        assertTrue(m.containsId("k"));
    }

    @Test
    public void testRemove_DeletesEntry() {
        final CqlMapper m = new CqlMapper();
        m.add("k", "SELECT * FROM mapper_test_remove WHERE id = ?", null);
        m.remove("k");
        assertNull(m.get("k"));
        assertTrue(m.isEmpty());
    }

    @Test
    public void testRemove_MissingIsNoop() {
        final CqlMapper m = new CqlMapper();
        // No exception expected for an absent key.
        m.remove("nope");
        assertTrue(m.isEmpty());
    }

    // ---------------------------------------------------------------------------------------------
    // copy(): independent map sharing immutable ParsedCql values.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testCopy_ReturnsIndependentInstance() {
        final CqlMapper m1 = new CqlMapper();
        m1.add("k", "SELECT * FROM mapper_test_copy WHERE id = ?", null);
        final CqlMapper m2 = m1.copy();
        assertNotSame(m1, m2);
        // Both contain the same entry initially.
        assertEquals(m1.get("k"), m2.get("k"));
        // Mutating m2 does not affect m1.
        m2.remove("k");
        assertNotNull(m1.get("k"));
        assertNull(m2.get("k"));
    }

    @Test
    public void testCopy_EmptyMapper() {
        final CqlMapper m1 = new CqlMapper();
        final CqlMapper m2 = m1.copy();
        assertTrue(m2.isEmpty());
        assertNotSame(m1, m2);
    }

    @Test
    public void testCopy_PreservesAttributes() {
        final CqlMapper m1 = new CqlMapper();
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("timeout", "2000");
        m1.add("k", "SELECT * FROM mapper_test_copy_attrs WHERE id = ?", attrs);
        final CqlMapper m2 = m1.copy();
        assertEquals("2000", m2.getAttributes("k").get("timeout"));
        // Independent: removing from the copy leaves the original's attributes intact.
        m2.remove("k");
        assertEquals("2000", m1.getAttributes("k").get("timeout"));
        assertNull(m2.getAttributes("k"));
    }

    // ---------------------------------------------------------------------------------------------
    // Static load(...) factories: exercised only via input-validation paths (XmlUtil needs
    // jakarta.xml.bind at runtime, which is not on the test classpath here).
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testLoad_String_NullThrows() {
        assertThrows(IllegalArgumentException.class, () -> CqlMapper.load((String) null));
    }

    @Test
    public void testLoad_String_EmptyThrows() {
        assertThrows(IllegalArgumentException.class, () -> CqlMapper.load(""));
    }

    @Test
    public void testLoad_String_BlankThrows() {
        // Non-empty but resolves to no paths after splitting/trimming.
        assertThrows(IllegalArgumentException.class, () -> CqlMapper.load("   "));
    }

    @Test
    public void testLoad_String_MissingFileThrows() {
        assertThrows(RuntimeException.class, () -> CqlMapper.load("/definitely/not/an/existing/path/cql-mapper-load-missing.xml"));
    }

    @Test
    public void testLoad_Files_NullArrayThrows() {
        assertThrows(IllegalArgumentException.class, () -> CqlMapper.load((File[]) null));
    }

    @Test
    public void testLoad_Files_EmptyArrayThrows() {
        assertThrows(IllegalArgumentException.class, () -> CqlMapper.load(new File[0]));
    }

    @Test
    public void testLoad_Files_NullElementThrows() {
        assertThrows(IllegalArgumentException.class, () -> CqlMapper.load((File) null));
    }

    @Test
    public void testLoad_InputStream_NullThrows() {
        assertThrows(IllegalArgumentException.class, () -> CqlMapper.load((InputStream) null));
    }

    // TODO: round-trip saveTo/loadFrom/load tests skipped — XmlUtil requires jakarta.xml.bind at runtime, which is not on the test classpath.

    // ---------------------------------------------------------------------------------------------
    // equals / hashCode / toString.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testEquals_SameContent() {
        final CqlMapper m1 = new CqlMapper();
        final CqlMapper m2 = new CqlMapper();
        m1.add("k", "SELECT 1 FROM equals_same_content", null);
        m2.add("k", "SELECT 1 FROM equals_same_content", null);
        assertEquals(m1, m2);
        assertEquals(m1.hashCode(), m2.hashCode());
    }

    @Test
    public void testEquals_DifferentContent() {
        final CqlMapper m1 = new CqlMapper();
        final CqlMapper m2 = new CqlMapper();
        m1.add("k", "SELECT 1 FROM equals_diff_content_a", null);
        m2.add("k", "SELECT 1 FROM equals_diff_content_b", null);
        assertNotEquals(m1, m2);
    }

    @Test
    public void testEquals_DifferentAttributes() {
        // Same id + CQL but different metadata attributes => not equal (attributes are part of mapper state).
        final CqlMapper m1 = new CqlMapper();
        final CqlMapper m2 = new CqlMapper();
        final Map<String, String> a1 = new HashMap<>();
        a1.put("timeout", "1000");
        final Map<String, String> a2 = new HashMap<>();
        a2.put("timeout", "2000");
        m1.add("k", "SELECT 1 FROM equals_diff_attrs", a1);
        m2.add("k", "SELECT 1 FROM equals_diff_attrs", a2);
        assertNotEquals(m1, m2);
    }

    @Test
    public void testEquals_SameInstance() {
        final CqlMapper m = new CqlMapper();
        assertEquals(m, m);
    }

    @Test
    public void testEquals_DifferentType() {
        final CqlMapper m = new CqlMapper();
        assertFalse(m.equals("not a mapper"));
        assertFalse(m.equals(null));
    }

    @Test
    public void testToString_RepresentsInternalMap() {
        final CqlMapper m = new CqlMapper();
        m.add("foo", "SELECT 1 FROM tostring_internal_map", null);
        final String s = m.toString();
        assertNotNull(s);
        assertTrue(s.contains("foo"), s);
    }

    // ---------------------------------------------------------------------------------------------
    // Constants surface.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testConstants() {
        assertEquals("cqlMapper", CqlMapper.CQL_MAPPER);
        assertEquals("cql", CqlMapper.CQL);
        assertEquals("id", CqlMapper.ID);
    }
}
