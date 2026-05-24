/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;

/**
 * Pure unit tests for {@link AnyIncrement} (no live HBase). Exercises factory
 * methods and the fluent builder API.
 */
public class AnyIncrementTest extends TestBase {

    // ---------------------------------------------------------------------
    // Factory methods
    // ---------------------------------------------------------------------

    @Test
    public void testOf_stringRowKey() {
        AnyIncrement inc = AnyIncrement.of("row-1");
        assertNotNull(inc);
        assertArrayEquals(Bytes.toBytes("row-1"), inc.getRow());
    }

    @Test
    public void testOf_byteArrayRowKey() {
        byte[] row = Bytes.toBytes("row-bytes");
        AnyIncrement inc = AnyIncrement.of(row);
        assertArrayEquals(row, inc.getRow());
    }

    @Test
    public void testOf_byteArrayWithOffsetAndLength() {
        byte[] data = Bytes.toBytes("abcdefgh");
        AnyIncrement inc = AnyIncrement.of(data, 1, 3);
        assertArrayEquals(Bytes.toBytes("bcd"), inc.getRow());
    }

    @Test
    public void testOf_withTimestampAndFamilyMap() {
        long ts = 1L;
        NavigableMap<byte[], List<Cell>> familyMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        AnyIncrement inc = AnyIncrement.of(Bytes.toBytes("row"), ts, familyMap);
        assertArrayEquals(Bytes.toBytes("row"), inc.getRow());
        assertEquals(ts, inc.getTimestamp());
    }

    @Test
    public void testOf_copyExistingIncrement() {
        Increment original = new Increment(Bytes.toBytes("orig"));
        original.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q"), 1L);

        AnyIncrement copy = AnyIncrement.of(original);

        assertNotNull(copy);
        assertNotSame(original, copy.val());
        assertArrayEquals(Bytes.toBytes("orig"), copy.getRow());
        assertTrue(copy.hasFamilies());
    }

    // ---------------------------------------------------------------------
    // val()
    // ---------------------------------------------------------------------

    @Test
    public void testVal_returnsUnderlyingIncrement() {
        AnyIncrement inc = AnyIncrement.of("row");
        Increment underlying = inc.val();
        assertNotNull(underlying);
        assertArrayEquals(Bytes.toBytes("row"), underlying.getRow());
    }

    @Test
    public void testVal_consistentAcrossCalls() {
        AnyIncrement inc = AnyIncrement.of("row");
        assertSame(inc.val(), inc.val());
    }

    // ---------------------------------------------------------------------
    // addColumn variants
    // ---------------------------------------------------------------------

    @Test
    public void testAddColumn_string_returnsSelf() {
        AnyIncrement inc = AnyIncrement.of("row");
        AnyIncrement returned = inc.addColumn("cf", "q", 1L);
        assertSame(inc, returned);
        assertTrue(inc.hasFamilies());
    }

    @Test
    public void testAddColumn_byteArray() {
        AnyIncrement inc = AnyIncrement.of("row").addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q"), 5L);
        assertTrue(inc.hasFamilies());
    }

    @Test
    public void testAddColumn_negativeAmount_decrement() {
        AnyIncrement inc = AnyIncrement.of("row").addColumn("cf", "q", -3L);
        assertTrue(inc.hasFamilies());
    }

    @Test
    public void testAddColumn_chaining() {
        AnyIncrement inc = AnyIncrement.of("row").addColumn("cf", "q1", 1L).addColumn("cf", "q2", 2L).addColumn("cf2", "q3", 3L);
        Map<byte[], NavigableMap<byte[], Long>> map = inc.getFamilyMapOfLongs();
        assertEquals(2, map.size());
    }

    // ---------------------------------------------------------------------
    // hasFamilies and getFamilyMapOfLongs
    // ---------------------------------------------------------------------

    @Test
    public void testHasFamilies_emptyIncrement_returnsFalse() {
        AnyIncrement inc = AnyIncrement.of("row");
        assertFalse(inc.hasFamilies());
    }

    @Test
    public void testHasFamilies_afterAddColumn_returnsTrue() {
        AnyIncrement inc = AnyIncrement.of("row").addColumn("cf", "q", 1L);
        assertTrue(inc.hasFamilies());
    }

    @Test
    public void testGetFamilyMapOfLongs_emptyByDefault() {
        AnyIncrement inc = AnyIncrement.of("row");
        Map<byte[], NavigableMap<byte[], Long>> map = inc.getFamilyMapOfLongs();
        assertNotNull(map);
        assertEquals(0, map.size());
    }

    @Test
    public void testGetFamilyMapOfLongs_returnsAmounts() {
        AnyIncrement inc = AnyIncrement.of("row").addColumn("cf", "q", 7L);
        Map<byte[], NavigableMap<byte[], Long>> map = inc.getFamilyMapOfLongs();
        assertEquals(1, map.size());
        NavigableMap<byte[], Long> col = map.get(Bytes.toBytes("cf"));
        assertNotNull(col);
        assertEquals(Long.valueOf(7L), col.get(Bytes.toBytes("q")));
    }

    // ---------------------------------------------------------------------
    // Time range
    // ---------------------------------------------------------------------

    @Test
    public void testSetTimeRange_andGetter() {
        AnyIncrement inc = AnyIncrement.of("row").setTimeRange(100L, 200L);
        TimeRange range = inc.getTimeRange();
        assertNotNull(range);
        assertEquals(100L, range.getMin());
        assertEquals(200L, range.getMax());
    }

    @Test
    public void testSetTimeRange_invalidRange_throws() {
        assertThrows(IllegalArgumentException.class, () -> AnyIncrement.of("row").setTimeRange(200L, 100L));
    }

    // ---------------------------------------------------------------------
    // setReturnResults / isReturnResults
    // ---------------------------------------------------------------------

    @Test
    public void testSetReturnResults_false_andGetter() {
        AnyIncrement inc = AnyIncrement.of("row").setReturnResults(false);
        assertFalse(inc.isReturnResults());
    }

    @Test
    public void testSetReturnResults_true_andGetter() {
        AnyIncrement inc = AnyIncrement.of("row").setReturnResults(true);
        assertTrue(inc.isReturnResults());
    }

    // ---------------------------------------------------------------------
    // setAttribute
    // ---------------------------------------------------------------------

    @Test
    public void testSetAttribute_andGetter() {
        AnyIncrement inc = AnyIncrement.of("row").setAttribute("trace_id", Bytes.toBytes("trace-1"));
        assertArrayEquals(Bytes.toBytes("trace-1"), inc.getAttribute("trace_id"));
    }

    @Test
    public void testSetAttribute_nullValue_doesNotThrow() {
        AnyIncrement inc = AnyIncrement.of("row");
        inc.setAttribute("k", null);
        // After setting null, retrieved value should be null
        assertEquals(null, inc.getAttribute("k"));
    }

    // ---------------------------------------------------------------------
    // Mutation inherited: durability, timestamp, TTL
    // ---------------------------------------------------------------------

    @Test
    public void testSetDurability_andGetter() {
        AnyIncrement inc = AnyIncrement.of("row").setDurability(Durability.SKIP_WAL);
        assertEquals(Durability.SKIP_WAL, inc.getDurability());
    }

    @Test
    public void testSetTimestamp_andGetter() {
        AnyIncrement inc = AnyIncrement.of("row").setTimestamp(42L);
        assertEquals(42L, inc.getTimestamp());
    }

    @Test
    public void testSetTTL_andGetter() {
        AnyIncrement inc = AnyIncrement.of("row").setTTL(86_400_000L);
        assertEquals(86_400_000L, inc.getTTL());
    }

    // ---------------------------------------------------------------------
    // equals, hashCode, toString
    // ---------------------------------------------------------------------

    @Test
    public void testEquals_sameInstance() {
        AnyIncrement inc = AnyIncrement.of("row");
        assertEquals(inc, inc);
    }

    @Test
    public void testEquals_nonAnyIncrementObject_returnsFalse() {
        AnyIncrement inc = AnyIncrement.of("row");
        assertFalse(inc.equals("not an AnyIncrement"));
        assertFalse(inc.equals(null));
    }

    @Test
    public void testHashCode_matchesUnderlyingIncrement() {
        AnyIncrement inc = AnyIncrement.of("row");
        assertEquals(inc.val().hashCode(), inc.hashCode());
    }

    @Test
    public void testToString_notNull() {
        AnyIncrement inc = AnyIncrement.of("row").addColumn("cf", "q", 1L);
        String s = inc.toString();
        assertNotNull(s);
    }

    // ---------------------------------------------------------------------
    // Copy independence
    // ---------------------------------------------------------------------

    @Test
    public void testOf_copyExistingIncrement_isIndependent() {
        Increment orig = new Increment(Bytes.toBytes("k"));
        orig.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q"), 1L);
        AnyIncrement copy = AnyIncrement.of(orig);
        copy.addColumn("cf2", "q2", 2L);

        assertEquals(2, copy.getFamilyMapOfLongs().size());
        // Original increment should still have only 1 family
        assertEquals(1, orig.getFamilyMapOfLongs().size());
    }
}
