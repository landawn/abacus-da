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

    // setAttribute / getAttribute
    @Test
    public void testSetAttribute_stringValue_andGetAttribute() {
        AnyGet get = AnyGet.of("row");
        AnyGet returned = get.setAttribute("k1", "hello");
        assertSame(get, returned, "setAttribute must return this for chaining");

        byte[] v = get.getAttribute("k1");
        assertArrayEquals(Bytes.toBytes("hello"), v);
    }

    @Test
    public void testSetAttribute_nullValue_clearsAttribute() {
        AnyGet get = AnyGet.of("row").setAttribute("k", "v");
        assertNotNull(get.getAttribute("k"));
        get.setAttribute("k", null);
        assertNull(get.getAttribute("k"));
    }

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

    @Test
    public void testSetAttribute_overwriteSameKey() {
        AnyGet get = AnyGet.of("row");
        get.setAttribute("k", "v1");
        get.setAttribute("k", "v2");
        assertArrayEquals(Bytes.toBytes("v2"), get.getAttribute("k"));
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
    public void testGetAttributesMap_afterSet_containsKeys() {
        AnyGet get = AnyGet.of("row").setAttribute("a", "1").setAttribute("b", "2");
        Map<String, byte[]> map = get.getAttributesMap();
        assertEquals(2, map.size());
        assertTrue(map.containsKey("a"));
        assertTrue(map.containsKey("b"));
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
    public void testGetId_initiallyNull() {
        AnyGet get = AnyGet.of("row");
        assertNull(get.getId());
    }

    @Test
    public void testSetId_nullId_throwsNpe() {
        // HBase's OperationWithAttributes.setId(null) throws NPE; this is the
        // observable behavior of the wrapped Operation.
        AnyGet get = AnyGet.of("row");
        assertThrows(NullPointerException.class, () -> get.setId(null));
    }

    @Test
    public void testSetId_emptyString_isAllowed() {
        AnyGet get = AnyGet.of("row").setId("");
        assertEquals("", get.getId());
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

    // Constructor null-check
    @Test
    public void testConstructor_nullOwa_throws() {
        assertThrows(IllegalArgumentException.class, () -> new TestOwa(null));
    }

    @Test
    public void testConstructor_nonNull_succeeds() {
        Get g = new Get(Bytes.toBytes("row"));
        TestOwa op = new TestOwa(g);
        assertNotNull(op);
        // Round-trip a set/get to confirm the wrapped instance is reachable.
        op.setId("x");
        assertEquals("x", op.getId());
    }

    @Test
    public void testSetAttribute_intValue_storedAsBytes() {
        AnyGet get = AnyGet.of("row").setAttribute("count", 42);
        byte[] stored = get.getAttribute("count");
        assertNotNull(stored);
        // HBaseExecutor.toValueBytes(int 42) uses Bytes.toBytes("42") (String form).
        assertArrayEquals(Bytes.toBytes("42"), stored);
    }

    // Minimal concrete subclass to drive the constructor null-guard directly.
    private static final class TestOwa extends AnyOperationWithAttributes<TestOwa> {
        protected TestOwa(final org.apache.hadoop.hbase.client.OperationWithAttributes owa) {
            super(owa);
        }
    }
}
