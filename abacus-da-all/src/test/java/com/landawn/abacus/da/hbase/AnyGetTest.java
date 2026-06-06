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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;

/**
 * Pure unit tests for {@link AnyGet} (no live HBase). Exercises factory
 * methods, the fluent builder API, and accessors inherited from
 * {@link AnyQuery}.
 */
public class AnyGetTest extends TestBase {

    // ---------------------------------------------------------------------
    // Factory methods: of(...) variants
    // ---------------------------------------------------------------------

    @Test
    public void testOf_stringRowKey() {
        AnyGet get = AnyGet.of("row-1");
        assertNotNull(get);
        assertArrayEquals(Bytes.toBytes("row-1"), get.getRow());
    }

    @Test
    public void testOf_byteArrayRowKey() {
        byte[] row = Bytes.toBytes("row-bytes");
        AnyGet get = AnyGet.of(row);
        assertArrayEquals(row, get.getRow());
    }

    @Test
    public void testOf_withOffsetAndLength() {
        AnyGet get = AnyGet.of("abcdefgh", 1, 3);
        assertArrayEquals(Bytes.toBytes("bcd"), get.getRow());
    }

    @Test
    public void testOf_byteBufferRowKey() {
        ByteBuffer buf = ByteBuffer.wrap(Bytes.toBytes("row-bb"));
        AnyGet get = AnyGet.of(buf);
        assertArrayEquals(Bytes.toBytes("row-bb"), get.getRow());
    }

    @Test
    public void testOf_copyExistingGet() {
        Get original = new Get(Bytes.toBytes("orig"));
        original.addFamily(Bytes.toBytes("cf"));

        AnyGet copy = AnyGet.of(original);

        assertNotNull(copy);
        assertArrayEquals(Bytes.toBytes("orig"), copy.getRow());
        assertTrue(copy.hasFamilies());
    }

    // ---------------------------------------------------------------------
    // val()
    // ---------------------------------------------------------------------

    @Test
    public void testVal_returnsUnderlyingGet() {
        AnyGet get = AnyGet.of("row");
        Get underlying = get.val();
        assertNotNull(underlying);
        assertArrayEquals(Bytes.toBytes("row"), underlying.getRow());
    }

    @Test
    public void testVal_consistentAcrossCalls() {
        AnyGet get = AnyGet.of("row");
        assertSame(get.val(), get.val());
    }

    // ---------------------------------------------------------------------
    // addFamily variants
    // ---------------------------------------------------------------------

    @Test
    public void testAddFamily_string() {
        AnyGet get = AnyGet.of("row");
        AnyGet returned = get.addFamily("cf");
        assertSame(get, returned);
        assertTrue(get.hasFamilies());
        assertEquals(1, get.numFamilies());
    }

    @Test
    public void testAddFamily_byteArray() {
        AnyGet get = AnyGet.of("row").addFamily(Bytes.toBytes("cf"));
        assertTrue(get.hasFamilies());
        assertEquals(1, get.numFamilies());
    }

    @Test
    public void testAddFamily_multipleFamilies() {
        AnyGet get = AnyGet.of("row").addFamily("cf1").addFamily("cf2");
        assertEquals(2, get.numFamilies());
    }

    // ---------------------------------------------------------------------
    // addColumn variants
    // ---------------------------------------------------------------------

    @Test
    public void testAddColumn_string() {
        AnyGet get = AnyGet.of("row").addColumn("cf", "q");
        assertTrue(get.hasFamilies());
    }

    @Test
    public void testAddColumn_byteArray() {
        AnyGet get = AnyGet.of("row").addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q"));
        assertTrue(get.hasFamilies());
    }

    // ---------------------------------------------------------------------
    // Family map accessors
    // ---------------------------------------------------------------------

    @Test
    public void testGetFamilyMap() {
        AnyGet get = AnyGet.of("row").addColumn("cf1", "q1").addFamily("cf2");
        Map<byte[], NavigableSet<byte[]>> map = get.getFamilyMap();
        assertNotNull(map);
        assertEquals(2, map.size());
    }

    @Test
    public void testHasFamilies_emptyGet_returnsFalse() {
        AnyGet get = AnyGet.of("row");
        assertFalse(get.hasFamilies());
        assertEquals(0, get.numFamilies());
    }

    @Test
    public void testFamilySet() {
        AnyGet get = AnyGet.of("row").addFamily("cf1").addFamily("cf2");
        Set<byte[]> families = get.familySet();
        assertEquals(2, families.size());
    }

    // ---------------------------------------------------------------------
    // CheckExistenceOnly
    // ---------------------------------------------------------------------

    @Test
    public void testSetCheckExistenceOnly_andGetter() {
        AnyGet get = AnyGet.of("row");
        AnyGet returned = get.setCheckExistenceOnly(true);
        assertSame(get, returned);
        assertTrue(get.isCheckExistenceOnly());
    }

    @Test
    public void testSetCheckExistenceOnly_defaultsFalse() {
        AnyGet get = AnyGet.of("row");
        assertFalse(get.isCheckExistenceOnly());
    }

    // ---------------------------------------------------------------------
    // Time range / timestamp
    // ---------------------------------------------------------------------

    @Test
    public void testSetTimeRange_andGetter() {
        AnyGet get = AnyGet.of("row").setTimeRange(100L, 200L);
        TimeRange range = get.getTimeRange();
        assertNotNull(range);
        assertEquals(100L, range.getMin());
        assertEquals(200L, range.getMax());
    }

    @Test
    public void testSetTimeRange_invalidRange_throws() {
        // maxStamp < minStamp should throw IllegalArgumentException (wrapped IOException)
        assertThrows(IllegalArgumentException.class, () -> AnyGet.of("row").setTimeRange(200L, 100L));
    }

    @Test
    public void testSetTimestamp_persists() {
        AnyGet get = AnyGet.of("row").setTimestamp(123L);
        TimeRange range = get.getTimeRange();
        assertEquals(123L, range.getMin());
    }

    // ---------------------------------------------------------------------
    // Version control
    // ---------------------------------------------------------------------

    @Test
    public void testReadVersions_setsMaxVersions() {
        AnyGet get = AnyGet.of("row").readVersions(5);
        assertEquals(5, get.getMaxVersions());
    }

    @Test
    public void testReadVersions_zero_throws() {
        assertThrows(IllegalArgumentException.class, () -> AnyGet.of("row").readVersions(0));
    }

    @Test
    public void testReadAllVersions_setsMaxToIntegerMax() {
        AnyGet get = AnyGet.of("row").readAllVersions();
        assertEquals(Integer.MAX_VALUE, get.getMaxVersions());
    }

    // ---------------------------------------------------------------------
    // Result-limiting settings
    // ---------------------------------------------------------------------

    @Test
    public void testSetMaxResultsPerColumnFamily() {
        AnyGet get = AnyGet.of("row").setMaxResultsPerColumnFamily(50);
        assertEquals(50, get.getMaxResultsPerColumnFamily());
    }

    @Test
    public void testSetRowOffsetPerColumnFamily() {
        AnyGet get = AnyGet.of("row").setRowOffsetPerColumnFamily(10);
        assertEquals(10, get.getRowOffsetPerColumnFamily());
    }

    @Test
    public void testSetCacheBlocks_andGetter() {
        AnyGet get = AnyGet.of("row").setCacheBlocks(false);
        assertFalse(get.getCacheBlocks());
        get.setCacheBlocks(true);
        assertTrue(get.getCacheBlocks());
    }

    // ---------------------------------------------------------------------
    // AnyQuery inherited: filter, consistency, isolation, replica, lcfod
    // ---------------------------------------------------------------------

    @Test
    public void testSetFilter_andGetter() {
        FirstKeyOnlyFilter filter = new FirstKeyOnlyFilter();
        AnyGet get = AnyGet.of("row").setFilter(filter);
        assertSame(filter, get.getFilter());
    }

    @Test
    public void testSetConsistency_andGetter() {
        AnyGet get = AnyGet.of("row").setConsistency(Consistency.TIMELINE);
        assertEquals(Consistency.TIMELINE, get.getConsistency());
    }

    @Test
    public void testSetReplicaId_andGetter() {
        AnyGet get = AnyGet.of("row").setReplicaId(2);
        assertEquals(2, get.getReplicaId());
    }

    @Test
    public void testSetIsolationLevel_andGetter() {
        AnyGet get = AnyGet.of("row").setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
        assertEquals(IsolationLevel.READ_UNCOMMITTED, get.getIsolationLevel());
    }

    @Test
    public void testSetLoadColumnFamiliesOnDemand_andGetters() {
        AnyGet get = AnyGet.of("row").setLoadColumnFamiliesOnDemand(true);
        assertEquals(Boolean.TRUE, get.getLoadColumnFamiliesOnDemandValue());
        assertTrue(get.doLoadColumnFamiliesOnDemand());
    }

    @Test
    public void testSetColumnFamilyTimeRange_string() {
        AnyGet get = AnyGet.of("row").setColumnFamilyTimeRange("cf", 100L, 200L);
        Map<byte[], TimeRange> map = get.getColumnFamilyTimeRange();
        assertEquals(1, map.size());
    }

    @Test
    public void testSetColumnFamilyTimeRange_byteArray() {
        AnyGet get = AnyGet.of("row").setColumnFamilyTimeRange(Bytes.toBytes("cf"), 100L, 200L);
        Map<byte[], TimeRange> map = get.getColumnFamilyTimeRange();
        assertEquals(1, map.size());
    }

    // ---------------------------------------------------------------------
    // compareTo
    // ---------------------------------------------------------------------

    @Test
    public void testCompareTo_sameRow_returnsZero() {
        AnyGet a = AnyGet.of("k");
        AnyGet b = AnyGet.of("k");
        assertEquals(0, a.compareTo(b));
    }

    @Test
    public void testCompareTo_differentRows() {
        AnyGet a = AnyGet.of("a");
        AnyGet b = AnyGet.of("b");
        assertTrue(a.compareTo(b) < 0);
        assertTrue(b.compareTo(a) > 0);
    }

    // ---------------------------------------------------------------------
    // equals, hashCode, toString
    // ---------------------------------------------------------------------

    @Test
    public void testEquals_sameInstance() {
        AnyGet get = AnyGet.of("row");
        assertEquals(get, get);
    }

    @Test
    public void testEquals_matchesUnderlyingGetEquals() {
        AnyGet g1 = AnyGet.of("row");
        AnyGet g2 = AnyGet.of("row");
        assertEquals(g1.val().equals(g2.val()), g1.equals(g2));
    }

    @Test
    public void testEquals_nonAnyGetObject_returnsFalse() {
        AnyGet get = AnyGet.of("row");
        assertFalse(get.equals("not an AnyGet"));
        assertFalse(get.equals(null));
    }

    @Test
    public void testHashCode_matchesUnderlyingGet() {
        AnyGet get = AnyGet.of("row");
        assertEquals(get.val().hashCode(), get.hashCode());
    }

    @Test
    public void testToString_notNull() {
        AnyGet get = AnyGet.of("row").addColumn("cf", "q");
        String s = get.toString();
        assertNotNull(s);
    }

    // ---------------------------------------------------------------------
    // toGet(Collection) static method
    // ---------------------------------------------------------------------

    @Test
    public void testToGet_collection() {
        AnyGet g1 = AnyGet.of("r1").addFamily("cf");
        AnyGet g2 = AnyGet.of("r2");

        List<Get> gets = AnyGet.toGet(Arrays.asList(g1, g2));

        assertEquals(2, gets.size());
        assertSame(g1.val(), gets.get(0));
        assertSame(g2.val(), gets.get(1));
    }

    @Test
    public void testToGet_emptyCollection() {
        List<Get> gets = AnyGet.toGet(Collections.<AnyGet> emptyList());
        assertEquals(0, gets.size());
    }

    @Test
    public void testToGet_nullCollection_throws() {
        assertThrows(IllegalArgumentException.class, () -> AnyGet.toGet(null));
    }

    @Test
    public void testToGet_collectionWithNull_throws() {
        // Null elements are rejected with IllegalArgumentException (consistent with AnyDelete.toDelete / AnyPut.toPut).
        AnyGet g1 = AnyGet.of("r1");
        assertThrows(IllegalArgumentException.class, () -> AnyGet.toGet(Arrays.asList(g1, null)));
    }

    @Test
    public void testOf_existingGet_wrapsSameInstance() {
        // AnyGet.of(Get) wraps the provided Get directly (does NOT deep-copy).
        Get orig = new Get(Bytes.toBytes("k"));
        orig.addFamily(Bytes.toBytes("cf1"));
        AnyGet wrapped = AnyGet.of(orig);
        wrapped.addFamily("cf2");
        // val() returns the same underlying Get
        assertSame(orig, wrapped.val());
        assertEquals(2, wrapped.numFamilies());
    }
}
