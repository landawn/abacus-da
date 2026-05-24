/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;

/**
 * Pure unit tests for {@link AnyAppend} (no live HBase). Exercises factory
 * methods, fluent builder API, and accessors inherited from {@link AnyMutation}.
 */
public class AnyAppendTest extends TestBase {

    // ---------------------------------------------------------------------
    // Factory methods
    // ---------------------------------------------------------------------

    @Test
    public void testOf_stringRowKey() {
        AnyAppend a = AnyAppend.of("row-1");
        assertNotNull(a);
        assertArrayEquals(Bytes.toBytes("row-1"), a.getRow());
    }

    @Test
    public void testOf_byteArrayRowKey() {
        byte[] row = Bytes.toBytes("row-bytes");
        AnyAppend a = AnyAppend.of(row);
        assertArrayEquals(row, a.getRow());
    }

    @Test
    public void testOf_byteArrayWithOffsetAndLength() {
        byte[] data = Bytes.toBytes("abcdefgh");
        AnyAppend a = AnyAppend.of(data, 1, 3);
        assertArrayEquals(Bytes.toBytes("bcd"), a.getRow());
    }

    @Test
    public void testOf_withTimestampAndFamilyMap() {
        long ts = 5L;
        NavigableMap<byte[], List<Cell>> familyMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        AnyAppend a = AnyAppend.of(Bytes.toBytes("row"), ts, familyMap);
        assertArrayEquals(Bytes.toBytes("row"), a.getRow());
        assertEquals(ts, a.getTimestamp());
    }

    @Test
    public void testOf_copyExistingAppend() {
        Append original = new Append(Bytes.toBytes("orig"));
        original.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("v"));

        AnyAppend copy = AnyAppend.of(original);

        assertNotNull(copy);
        assertNotSame(original, copy.val());
        assertArrayEquals(Bytes.toBytes("orig"), copy.getRow());
        assertTrue(copy.has("cf", "q"));
    }

    // ---------------------------------------------------------------------
    // val()
    // ---------------------------------------------------------------------

    @Test
    public void testVal_returnsUnderlyingAppend() {
        AnyAppend a = AnyAppend.of("row");
        Append underlying = a.val();
        assertNotNull(underlying);
        assertArrayEquals(Bytes.toBytes("row"), underlying.getRow());
    }

    @Test
    public void testVal_consistentAcrossCalls() {
        AnyAppend a = AnyAppend.of("row");
        assertSame(a.val(), a.val());
    }

    // ---------------------------------------------------------------------
    // addColumn variants
    // ---------------------------------------------------------------------

    @Test
    public void testAddColumn_string_returnsSelf() {
        AnyAppend a = AnyAppend.of("row");
        AnyAppend returned = a.addColumn("cf", "q", "v");
        assertSame(a, returned);
        assertTrue(a.has("cf", "q"));
    }

    @Test
    public void testAddColumn_byteArray() {
        AnyAppend a = AnyAppend.of("row").addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("v"));
        assertTrue(a.has("cf", "q"));
    }

    @Test
    public void testAddColumn_chainingAddsMultipleCells() {
        AnyAppend a = AnyAppend.of("row").addColumn("cf", "q1", "v1").addColumn("cf", "q2", "v2");
        assertEquals(2, a.size());
        assertEquals(1, a.numFamilies());
    }

    @Test
    public void testAddColumn_nonStringValue_doesNotThrow() {
        assertDoesNotThrow(() -> AnyAppend.of("row").addColumn("cf", "q", 12345));
    }

    // ---------------------------------------------------------------------
    // add(Cell)
    // ---------------------------------------------------------------------

    @Test
    public void testAdd_cell_returnsSelf() {
        byte[] row = Bytes.toBytes("row");
        Cell cell = new KeyValue(row, Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("v"));
        AnyAppend a = AnyAppend.of("row");
        AnyAppend returned = a.add(cell);
        assertSame(a, returned);
        assertTrue(a.has("cf", "q"));
    }

    // ---------------------------------------------------------------------
    // Time range
    // ---------------------------------------------------------------------

    @Test
    public void testSetTimeRange_andGetter() {
        AnyAppend a = AnyAppend.of("row").setTimeRange(100L, 200L);
        TimeRange range = a.getTimeRange();
        assertEquals(100L, range.getMin());
        assertEquals(200L, range.getMax());
    }

    // ---------------------------------------------------------------------
    // setReturnResults / isReturnResults
    // ---------------------------------------------------------------------

    @Test
    public void testSetReturnResults_false_andGetter() {
        AnyAppend a = AnyAppend.of("row").setReturnResults(false);
        assertFalse(a.isReturnResults());
    }

    @Test
    public void testSetReturnResults_true_andGetter() {
        AnyAppend a = AnyAppend.of("row").setReturnResults(true);
        assertTrue(a.isReturnResults());
    }

    // ---------------------------------------------------------------------
    // setAttribute
    // ---------------------------------------------------------------------

    @Test
    public void testSetAttribute_andGetter() {
        AnyAppend a = AnyAppend.of("row").setAttribute("trace_id", Bytes.toBytes("trace-1"));
        assertArrayEquals(Bytes.toBytes("trace-1"), a.getAttribute("trace_id"));
    }

    // ---------------------------------------------------------------------
    // Mutation inherited: durability, timestamp, TTL, family map
    // ---------------------------------------------------------------------

    @Test
    public void testSetDurability_andGetter() {
        AnyAppend a = AnyAppend.of("row").setDurability(Durability.SKIP_WAL);
        assertEquals(Durability.SKIP_WAL, a.getDurability());
    }

    @Test
    public void testSetTimestamp_andGetter() {
        AnyAppend a = AnyAppend.of("row").setTimestamp(42L);
        assertEquals(42L, a.getTimestamp());
    }

    @Test
    public void testSetTTL_andGetter() {
        AnyAppend a = AnyAppend.of("row").setTTL(86_400_000L);
        assertEquals(86_400_000L, a.getTTL());
    }

    @Test
    public void testGetFamilyCellMap_afterAddColumn() {
        AnyAppend a = AnyAppend.of("row").addColumn("cf1", "q1", "v1").addColumn("cf2", "q2", "v2");
        NavigableMap<byte[], List<Cell>> map = a.getFamilyCellMap();
        assertEquals(2, map.size());
    }

    @Test
    public void testSize_andNumFamilies() {
        AnyAppend a = AnyAppend.of("row").addColumn("cf1", "q1", "v1").addColumn("cf2", "q2", "v2").addColumn("cf2", "q3", "v3");
        assertEquals(3, a.size());
        assertEquals(2, a.numFamilies());
    }

    @Test
    public void testGetFingerprint() {
        AnyAppend a = AnyAppend.of("row").addColumn("cf", "q", "v");
        assertNotNull(a.getFingerprint());
    }

    // ---------------------------------------------------------------------
    // equals, hashCode, toString
    // ---------------------------------------------------------------------

    @Test
    public void testEquals_sameInstance() {
        AnyAppend a = AnyAppend.of("row");
        assertEquals(a, a);
    }

    @Test
    public void testEquals_nonAnyAppendObject_returnsFalse() {
        AnyAppend a = AnyAppend.of("row");
        assertFalse(a.equals("not an AnyAppend"));
        assertFalse(a.equals(null));
    }

    @Test
    public void testHashCode_matchesUnderlyingAppend() {
        AnyAppend a = AnyAppend.of("row");
        assertEquals(a.val().hashCode(), a.hashCode());
    }

    @Test
    public void testToString_notNull() {
        AnyAppend a = AnyAppend.of("row").addColumn("cf", "q", "v");
        String s = a.toString();
        assertNotNull(s);
    }

    // ---------------------------------------------------------------------
    // Copy independence
    // ---------------------------------------------------------------------

    @Test
    public void testOf_copyExistingAppend_isIndependent() {
        Append orig = new Append(Bytes.toBytes("k"));
        orig.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q1"), Bytes.toBytes("v1"));
        AnyAppend copy = AnyAppend.of(orig);

        copy.addColumn("cf", "q2", "v2");
        assertTrue(copy.has("cf", "q2"));
        assertFalse(orig.has(Bytes.toBytes("cf"), Bytes.toBytes("q2")));
    }
}
