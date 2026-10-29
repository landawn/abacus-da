/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;

/**
 * Dedicated unit tests for the abstract base class {@link AnyOperationWithAttributes}.
 * Tests are exercised through {@link AnyGet} (a concrete subclass) to drive
 * coverage of the inherited attribute/id/priority methods.
 */
public class AnyOperationWithAttributesTest extends TestBase {

    // Construction and configuration

    @Test
    public void testConstructor_nonNull_succeeds() {
        Get g = new Get(Bytes.toBytes("row"));
        TestOwa op = new TestOwa(g);
        assertNotNull(op);
        // Round-trip a set/get to confirm the wrapped instance is reachable.
        op.setId("x");
        assertEquals("x", op.getId());
    }

    // Constructor null-check
    @Test
    public void testConstructor_nullOwa_throws() {
        assertThrows(IllegalArgumentException.class, () -> new TestOwa(null));
    }

    // Updates and deletes

    @Test
    public void testGetAttributesMap_afterSet_containsKeys() {
        AnyGet get = AnyGet.of("row").setAttribute("a", "1").setAttribute("b", "2");
        Map<String, byte[]> map = get.getAttributesMap();
        assertEquals(2, map.size());
        assertTrue(map.containsKey("a"));
        assertTrue(map.containsKey("b"));
    }

    @Test
    public void testSetAttribute_intValue_storedAsBytes() {
        AnyGet get = AnyGet.of("row").setAttribute("count", 42);
        byte[] stored = get.getAttribute("count");
        assertNotNull(stored);
        // HBaseExecutor.toValueBytes(int 42) uses Bytes.toBytes("42") (String form).
        assertArrayEquals(Bytes.toBytes("42"), stored);
    }

    @Test
    public void testSetAttribute_nullValue_clearsAttribute() {
        AnyGet get = AnyGet.of("row").setAttribute("k", "v");
        assertNotNull(get.getAttribute("k"));
        get.setAttribute("k", null);
        assertNull(get.getAttribute("k"));
    }

    @Test
    public void testSetAttribute_overwriteSameKey() {
        AnyGet get = AnyGet.of("row");
        get.setAttribute("k", "v1");
        get.setAttribute("k", "v2");
        assertArrayEquals(Bytes.toBytes("v2"), get.getAttribute("k"));
    }

    // setAttribute / getAttribute
    @Test
    public void testSetAttribute_stringValue_andGetAttribute() {
        AnyGet get = AnyGet.of("row");
        AnyGet returned = get.setAttribute("k1", "hello");
        assertSame(get, returned, "setAttribute must return this for chaining");

        byte[] v = get.getAttribute("k1");
        assertArrayEquals(Bytes.toBytes("hello"), v);
    }

    // setId / getId
    @Test
    public void testSetId_andGetId() {
        AnyGet get = AnyGet.of("row");
        AnyGet returned = get.setId("my-op-id");
        assertSame(get, returned);
        assertEquals("my-op-id", get.getId());
    }

    @Test
    public void testSetId_emptyString_isAllowed() {
        AnyGet get = AnyGet.of("row").setId("");
        assertEquals("", get.getId());
    }

    @Test
    public void testSetId_nullId_throwsIae() {
        // A null id is rejected up front instead of surfacing HBase's NPE from Bytes.toBytes(null).
        AnyGet get = AnyGet.of("row");
        assertThrows(IllegalArgumentException.class, () -> get.setId(null));
        assertNull(get.getId());
    }

    // setPriority / getPriority
    @Test
    public void testSetPriority_andGetPriority() {
        AnyGet get = AnyGet.of("row");
        AnyGet returned = get.setPriority(50);
        assertSame(get, returned);
        assertEquals(50, get.getPriority());
    }

    @Test
    public void testSetPriority_highValue() {
        AnyGet get = AnyGet.of("row").setPriority(1000);
        assertEquals(1000, get.getPriority());
    }

    @Test
    public void testSetPriority_negativeValue() {
        AnyGet get = AnyGet.of("row").setPriority(-1);
        assertEquals(-1, get.getPriority());
    }

    @Test
    public void testSetPriority_zero() {
        AnyGet get = AnyGet.of("row").setPriority(0);
        assertEquals(0, get.getPriority());
    }

    // Queries and results

    @Test
    public void testGetAttribute_missingKey_returnsNull() {
        AnyGet get = AnyGet.of("row");
        assertNull(get.getAttribute("missing"));
    }

    @Test
    public void testGetAttribute_nullKey_returnsNull() {
        // HBase returns null for a missing/null attribute key rather than throwing.
        AnyGet get = AnyGet.of("row");
        assertNull(get.getAttribute(null));
    }

    // getAttributesMap
    @Test
    public void testGetAttributesMap_initiallyEmpty() {
        AnyGet get = AnyGet.of("row");
        Map<String, byte[]> map = get.getAttributesMap();
        assertNotNull(map);
        assertEquals(0, map.size());
    }

    @Test
    public void testGetId_initiallyNull() {
        AnyGet get = AnyGet.of("row");
        assertNull(get.getId());
    }

    // Additional contracts

    // Fluent chaining of multiple inherited setters
    @Test
    public void testFluentChaining_attributesIdPriority() {
        AnyGet get = AnyGet.of("row").setAttribute("k1", "v1").setAttribute("k2", "v2").setId("trace-id").setPriority(10);

        assertEquals("trace-id", get.getId());
        assertEquals(10, get.getPriority());
        assertArrayEquals(Bytes.toBytes("v1"), get.getAttribute("k1"));
        assertArrayEquals(Bytes.toBytes("v2"), get.getAttribute("k2"));
        // Both user attributes must be reachable in the attributes map (id may
        // also be stored as an internal HBase attribute, so we don't assert ==).
        assertTrue(get.getAttributesMap().containsKey("k1"));
        assertTrue(get.getAttributesMap().containsKey("k2"));
    }

    // ---- 2026-10-27 sliceP ----

    /** Pins the setAttribute @param value doc: an empty value wrapper converts to null bytes and removes the attribute. */
    @Test
    public void testSetAttribute_emptyValueWrapper_removesAttribute_sliceP() {
        final java.util.List<Object> emptyWrappers = java.util.Arrays.asList(java.util.Optional.empty(), java.util.OptionalLong.empty(),
                com.landawn.abacus.util.u.Optional.empty(), com.landawn.abacus.util.u.Nullable.empty(), com.landawn.abacus.util.Holder.of(null));

        org.junit.jupiter.api.Assertions.assertAll(emptyWrappers.stream().map(w -> (org.junit.jupiter.api.function.Executable) () -> {
            final AnyGet get = AnyGet.of("row").setAttribute("k", "v").setAttribute("other", "x");
            assertSame(get, get.setAttribute("k", w));
            assertNull(get.getAttribute("k"), String.valueOf(w));
            assertArrayEquals(Bytes.toBytes("x"), get.getAttribute("other"));
        }));
    }

    /** Pins the setId @param doc: setId cannot clear an id, but setAttribute(ID_ATRIBUTE, null) removes it. */
    @Test
    public void testSetId_clearedOnlyThroughIdAttribute_sliceP() {
        final AnyGet get = AnyGet.of("row").setId("trace-1");

        assertThrows(IllegalArgumentException.class, () -> get.setId(null));
        assertEquals("trace-1", get.getId());
        get.setAttribute(org.apache.hadoop.hbase.client.OperationWithAttributes.ID_ATRIBUTE, null);
        assertNull(get.getId());
    }

    // Minimal concrete subclass to drive the constructor null-guard directly.
    private static final class TestOwa extends AnyOperationWithAttributes<TestOwa> {
        protected TestOwa(final org.apache.hadoop.hbase.client.OperationWithAttributes owa) {
            super(owa);
        }
    }
}
