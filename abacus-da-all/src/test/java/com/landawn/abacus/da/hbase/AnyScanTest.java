/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;

/**
 * Pure unit tests for {@link AnyScan} (no live HBase). Exercises the fluent
 * builder API and accessor methods inherited from {@link AnyQuery}.
 */
public class AnyScanTest extends TestBase {

    // ---------------------------------------------------------------------
    // Factory methods: create(), of(...) variants
    // ---------------------------------------------------------------------

    @Test
    public void testCreate_defaultConfiguration() {
        AnyScan scan = AnyScan.create();
        assertNotNull(scan);
        assertNotNull(scan.val());
        assertFalse(scan.hasFamilies());
        assertEquals(0, scan.numFamilies());
    }

    @Test
    public void testOf_stringStartRow() {
        AnyScan scan = AnyScan.of("start-row");
        assertNotNull(scan);
        assertArrayEquals(Bytes.toBytes("start-row"), scan.getStartRow());
    }

    @Test
    public void testOf_startAndStopRow() {
        AnyScan scan = AnyScan.of("a", "z");
        assertArrayEquals(Bytes.toBytes("a"), scan.getStartRow());
        assertArrayEquals(Bytes.toBytes("z"), scan.getStopRow());
    }

    @Test
    public void testOf_startRowWithFilter() {
        Filter filter = new PrefixFilter(Bytes.toBytes("user_"));
        AnyScan scan = AnyScan.of("user_001", filter);
        assertArrayEquals(Bytes.toBytes("user_001"), scan.getStartRow());
        assertSame(filter, scan.getFilter());
        assertTrue(scan.hasFilter());
    }

    @Test
    public void testOf_wrappingExistingScan() {
        Scan raw = new Scan();
        raw.setCaching(99);
        AnyScan scan = AnyScan.of(raw);
        assertSame(raw, scan.val());
        assertEquals(99, scan.getCaching());
    }

    @Test
    public void testOf_fromGet() {
        Get get = new Get(Bytes.toBytes("row-from-get"));
        AnyScan scan = AnyScan.of(get);
        assertNotNull(scan);
        assertNotNull(scan.val());
        assertArrayEquals(Bytes.toBytes("row-from-get"), scan.getStartRow());
    }

    // ---------------------------------------------------------------------
    // val() and equality
    // ---------------------------------------------------------------------

    @Test
    public void testVal_returnsUnderlyingScan() {
        AnyScan scan = AnyScan.create();
        assertNotNull(scan.val());
        assertSame(scan.val(), scan.val());
    }

    @Test
    public void testEquals_sameInstance() {
        AnyScan scan = AnyScan.create();
        assertEquals(scan, scan);
    }

    @Test
    public void testEquals_nonAnyScanObject_returnsFalse() {
        AnyScan scan = AnyScan.create();
        assertFalse(scan.equals("not an AnyScan"));
        assertFalse(scan.equals(null));
    }

    @Test
    public void testEquals_matchesUnderlyingScanEquals() {
        AnyScan s1 = AnyScan.create();
        AnyScan s2 = AnyScan.create();
        assertEquals(s1.val().equals(s2.val()), s1.equals(s2));
    }

    @Test
    public void testHashCode_matchesUnderlyingScan() {
        AnyScan scan = AnyScan.create();
        assertEquals(scan.val().hashCode(), scan.hashCode());
    }

    @Test
    public void testToString_notNull() {
        AnyScan scan = AnyScan.create().addFamily("cf");
        assertNotNull(scan.toString());
    }

    // ---------------------------------------------------------------------
    // isGetScan / hasFamilies / numFamilies / getFamilies
    // ---------------------------------------------------------------------

    @Test
    public void testIsGetScan_returnsBoolean() {
        AnyScan scan = AnyScan.create();
        assertFalse(scan.isGetScan());
    }

    @Test
    public void testHasFamilies_initiallyFalse() {
        AnyScan scan = AnyScan.create();
        assertFalse(scan.hasFamilies());
    }

    @Test
    public void testHasFamilies_afterAddFamily() {
        AnyScan scan = AnyScan.create().addFamily("cf");
        assertTrue(scan.hasFamilies());
    }

    @Test
    public void testGetFamilies_afterAddFamilies() {
        AnyScan scan = AnyScan.create().addFamily("cf1").addFamily("cf2");
        byte[][] families = scan.getFamilies();
        assertNotNull(families);
        assertEquals(2, families.length);
    }

    // ---------------------------------------------------------------------
    // addFamily / addColumn variants
    // ---------------------------------------------------------------------

    @Test
    public void testAddFamily_string_returnsSelf() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.addFamily("cf");
        assertSame(scan, returned);
        assertEquals(1, scan.numFamilies());
    }

    @Test
    public void testAddFamily_byteArray() {
        AnyScan scan = AnyScan.create().addFamily(Bytes.toBytes("cf"));
        assertEquals(1, scan.numFamilies());
    }

    @Test
    public void testAddColumn_string() {
        AnyScan scan = AnyScan.create().addColumn("cf", "q");
        assertEquals(1, scan.numFamilies());
    }

    @Test
    public void testAddColumn_byteArray() {
        AnyScan scan = AnyScan.create().addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q"));
        assertEquals(1, scan.numFamilies());
    }

    @Test
    public void testGetFamilyMap_returnsMap() {
        AnyScan scan = AnyScan.create().addColumn("cf", "q");
        Map<byte[], NavigableSet<byte[]>> map = scan.getFamilyMap();
        assertNotNull(map);
        assertEquals(1, map.size());
    }

    @Test
    public void testSetFamilyMap_returnsSelf() {
        AnyScan scan = AnyScan.create();
        Map<byte[], NavigableSet<byte[]>> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        NavigableSet<byte[]> qualifiers = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        qualifiers.add(Bytes.toBytes("q"));
        map.put(Bytes.toBytes("cf"), qualifiers);

        AnyScan returned = scan.setFamilyMap(map);
        assertSame(scan, returned);
        assertEquals(1, scan.numFamilies());
    }

    // ---------------------------------------------------------------------
    // setColumnFamilyTimeRange
    // ---------------------------------------------------------------------

    @Test
    public void testSetColumnFamilyTimeRange_string() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setColumnFamilyTimeRange("cf", 100L, 200L);
        assertSame(scan, returned);
        Map<byte[], TimeRange> tr = scan.getColumnFamilyTimeRange();
        assertNotNull(tr);
        assertEquals(1, tr.size());
    }

    @Test
    public void testSetColumnFamilyTimeRange_byteArray() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setColumnFamilyTimeRange(Bytes.toBytes("cf"), 100L, 200L);
        assertSame(scan, returned);
    }

    // ---------------------------------------------------------------------
    // Time range / timestamp setters and accessors
    // ---------------------------------------------------------------------

    @Test
    public void testSetTimeRange_returnsSelf() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setTimeRange(100L, 200L);
        assertSame(scan, returned);
        TimeRange tr = scan.getTimeRange();
        assertNotNull(tr);
        assertEquals(100L, tr.getMin());
        assertEquals(200L, tr.getMax());
    }

    @Test
    public void testSetTimeRange_invalidRange_throwsIllegalArgument() {
        AnyScan scan = AnyScan.create();
        // max < min wraps the IOException as IllegalArgumentException.
        assertThrows(IllegalArgumentException.class, () -> scan.setTimeRange(200L, 100L));
    }

    @Test
    public void testSetTimestamp_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setTimestamp(12345L);
        assertSame(scan, returned);
        TimeRange tr = scan.getTimeRange();
        assertEquals(12345L, tr.getMin());
    }

    @Test
    public void testGetTimeRange_default() {
        AnyScan scan = AnyScan.create();
        assertNotNull(scan.getTimeRange());
    }

    // ---------------------------------------------------------------------
    // start / stop row accessors and setters
    // ---------------------------------------------------------------------

    @Test
    public void testIncludeStartRow_defaultTrue() {
        AnyScan scan = AnyScan.create();
        assertTrue(scan.includeStartRow());
    }

    @Test
    public void testIncludeStopRow_defaultFalse() {
        AnyScan scan = AnyScan.create();
        assertFalse(scan.includeStopRow());
    }

    @Test
    public void testGetStartRow_emptyByDefault() {
        AnyScan scan = AnyScan.create();
        assertNotNull(scan.getStartRow());
        assertEquals(0, scan.getStartRow().length);
    }

    @Test
    public void testGetStopRow_emptyByDefault() {
        AnyScan scan = AnyScan.create();
        assertNotNull(scan.getStopRow());
        assertEquals(0, scan.getStopRow().length);
    }

    @Test
    public void testWithStartRow_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.withStartRow("start");
        assertSame(scan, returned);
        assertArrayEquals(Bytes.toBytes("start"), scan.getStartRow());
    }

    @Test
    public void testWithStartRow_inclusiveExclusive() {
        AnyScan scan = AnyScan.create().withStartRow("start", false);
        assertFalse(scan.includeStartRow());
        assertArrayEquals(Bytes.toBytes("start"), scan.getStartRow());
    }

    @Test
    public void testWithStopRow_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.withStopRow("stop");
        assertSame(scan, returned);
        assertArrayEquals(Bytes.toBytes("stop"), scan.getStopRow());
    }

    @Test
    public void testWithStopRow_inclusive() {
        AnyScan scan = AnyScan.create().withStopRow("stop", true);
        assertTrue(scan.includeStopRow());
        assertArrayEquals(Bytes.toBytes("stop"), scan.getStopRow());
    }

    @Test
    public void testSetRowPrefixFilter_returnsSelf() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setRowPrefixFilter("prefix_");
        assertSame(scan, returned);
    }

    @Test
    public void testSetStartStopRowForPrefixScan() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setStartStopRowForPrefixScan(Bytes.toBytes("user_"));
        assertSame(scan, returned);
        assertArrayEquals(Bytes.toBytes("user_"), scan.getStartRow());
    }

    // ---------------------------------------------------------------------
    // readVersions / readAllVersions / getMaxVersions
    // ---------------------------------------------------------------------

    @Test
    public void testReadVersions_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.readVersions(5);
        assertSame(scan, returned);
        assertEquals(5, scan.getMaxVersions());
    }

    @Test
    public void testReadVersions_lessThanOne_storedAsIs() {
        // Documented divergence from AnyGet.readVersions, which throws IAE for versions < 1:
        // AnyScan mirrors the HBase client and stores the value unchanged.
        assertEquals(0, AnyScan.create().readVersions(0).getMaxVersions());
        assertEquals(-1, AnyScan.create().readVersions(-1).getMaxVersions());
    }

    @Test
    public void testReadAllVersions_returnsSelf() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.readAllVersions();
        assertSame(scan, returned);
        assertEquals(Integer.MAX_VALUE, scan.getMaxVersions());
    }

    // ---------------------------------------------------------------------
    // batch / caching / cacheBlocks / resultSize / limits
    // ---------------------------------------------------------------------

    @Test
    public void testSetBatch_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setBatch(100);
        assertSame(scan, returned);
        assertEquals(100, scan.getBatch());
    }

    @Test
    public void testSetMaxResultsPerColumnFamily() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setMaxResultsPerColumnFamily(10);
        assertSame(scan, returned);
        assertEquals(10, scan.getMaxResultsPerColumnFamily());
    }

    @Test
    public void testSetRowOffsetPerColumnFamily() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setRowOffsetPerColumnFamily(5);
        assertSame(scan, returned);
        assertEquals(5, scan.getRowOffsetPerColumnFamily());
    }

    @Test
    public void testSetCaching_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setCaching(50);
        assertSame(scan, returned);
        assertEquals(50, scan.getCaching());
    }

    @Test
    public void testSetCacheBlocks_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setCacheBlocks(false);
        assertSame(scan, returned);
        assertFalse(scan.getCacheBlocks());
    }

    @Test
    public void testSetMaxResultSize_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setMaxResultSize(1024L * 1024L);
        assertSame(scan, returned);
        assertEquals(1024L * 1024L, scan.getMaxResultSize());
    }

    @Test
    public void testSetLimit_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setLimit(100);
        assertSame(scan, returned);
        assertEquals(100, scan.getLimit());
    }

    @Test
    public void testSetOneRowLimit() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setOneRowLimit();
        assertSame(scan, returned);
        assertEquals(1, scan.getLimit());
    }

    // ---------------------------------------------------------------------
    // hasFilter / filter setters (inherited from AnyQuery)
    // ---------------------------------------------------------------------

    @Test
    public void testHasFilter_initiallyFalse() {
        AnyScan scan = AnyScan.create();
        assertFalse(scan.hasFilter());
    }

    @Test
    public void testSetFilter_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        Filter filter = new PrefixFilter(Bytes.toBytes("foo"));
        AnyScan returned = scan.setFilter(filter);
        assertSame(scan, returned);
        assertTrue(scan.hasFilter());
        assertSame(filter, scan.getFilter());
    }

    // ---------------------------------------------------------------------
    // Reversed / partial results / raw
    // ---------------------------------------------------------------------

    @Test
    public void testIsReversed_defaultFalse() {
        AnyScan scan = AnyScan.create();
        assertFalse(scan.isReversed());
    }

    @Test
    public void testSetReversed_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setReversed(true);
        assertSame(scan, returned);
        assertTrue(scan.isReversed());
    }

    @Test
    public void testGetAllowPartialResults_defaultFalse() {
        AnyScan scan = AnyScan.create();
        assertFalse(scan.getAllowPartialResults());
    }

    @Test
    public void testSetAllowPartialResults_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setAllowPartialResults(true);
        assertSame(scan, returned);
        assertTrue(scan.getAllowPartialResults());
    }

    @Test
    public void testIsRaw_defaultFalse() {
        AnyScan scan = AnyScan.create();
        assertFalse(scan.isRaw());
    }

    @Test
    public void testSetRaw_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setRaw(true);
        assertSame(scan, returned);
        assertTrue(scan.isRaw());
    }

    // ---------------------------------------------------------------------
    // Scan metrics
    // ---------------------------------------------------------------------

    @Test
    public void testIsScanMetricsEnabled_defaultFalse() {
        AnyScan scan = AnyScan.create();
        assertFalse(scan.isScanMetricsEnabled());
    }

    @Test
    public void testSetScanMetricsEnabled_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setScanMetricsEnabled(true);
        assertSame(scan, returned);
        assertTrue(scan.isScanMetricsEnabled());
    }

    // ---------------------------------------------------------------------
    // Async prefetch
    // ---------------------------------------------------------------------

    @Test
    public void testIsAsyncPrefetch_initial() {
        AnyScan scan = AnyScan.create();
        // Initially null or false depending on HBase defaults; just verify no NPE.
        Boolean async = scan.isAsyncPrefetch();
        assertTrue(async == null || async instanceof Boolean);
    }

    @Test
    public void testSetAsyncPrefetch_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setAsyncPrefetch(true);
        assertSame(scan, returned);
        assertEquals(Boolean.TRUE, scan.isAsyncPrefetch());
    }

    // ---------------------------------------------------------------------
    // ReadType
    // ---------------------------------------------------------------------

    @Test
    public void testGetReadType_default() {
        AnyScan scan = AnyScan.create();
        assertEquals(ReadType.DEFAULT, scan.getReadType());
    }

    @Test
    public void testSetReadType_stream() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setReadType(ReadType.STREAM);
        assertSame(scan, returned);
        assertEquals(ReadType.STREAM, scan.getReadType());
    }

    @Test
    public void testSetReadType_pread() {
        AnyScan scan = AnyScan.create().setReadType(ReadType.PREAD);
        assertEquals(ReadType.PREAD, scan.getReadType());
    }

    // ---------------------------------------------------------------------
    // Cursor results
    // ---------------------------------------------------------------------

    @Test
    public void testIsNeedCursorResult_defaultFalse() {
        AnyScan scan = AnyScan.create();
        assertFalse(scan.isNeedCursorResult());
    }

    @Test
    public void testSetNeedCursorResult_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setNeedCursorResult(true);
        assertSame(scan, returned);
        assertTrue(scan.isNeedCursorResult());
    }

    // ---------------------------------------------------------------------
    // Inherited from AnyQuery: consistency, replicaId, isolationLevel, etc.
    // ---------------------------------------------------------------------

    @Test
    public void testSetConsistency_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setConsistency(Consistency.TIMELINE);
        assertSame(scan, returned);
        assertEquals(Consistency.TIMELINE, scan.getConsistency());
    }

    @Test
    public void testSetReplicaId_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setReplicaId(1);
        assertSame(scan, returned);
        assertEquals(1, scan.getReplicaId());
    }

    @Test
    public void testSetIsolationLevel_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
        assertSame(scan, returned);
        assertEquals(IsolationLevel.READ_UNCOMMITTED, scan.getIsolationLevel());
    }

    @Test
    public void testSetLoadColumnFamiliesOnDemand_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setLoadColumnFamiliesOnDemand(true);
        assertSame(scan, returned);
        assertEquals(Boolean.TRUE, scan.getLoadColumnFamiliesOnDemandValue());
        assertTrue(scan.doLoadColumnFamiliesOnDemand());
    }

    @Test
    public void testGetACL_initiallyNull() {
        AnyScan scan = AnyScan.create();
        assertNull(scan.getACL());
    }

    // ---------------------------------------------------------------------
    // Chaining sanity check
    // ---------------------------------------------------------------------

    @Test
    public void testChaining_multipleConfigurations() {
        AnyScan scan = AnyScan.create()
                .withStartRow("a")
                .withStopRow("z")
                .addFamily("cf")
                .setLimit(100)
                .setCaching(10)
                .setReversed(true)
                .setReadType(ReadType.STREAM);

        assertArrayEquals(Bytes.toBytes("a"), scan.getStartRow());
        assertArrayEquals(Bytes.toBytes("z"), scan.getStopRow());
        assertEquals(1, scan.numFamilies());
        assertEquals(100, scan.getLimit());
        assertEquals(10, scan.getCaching());
        assertTrue(scan.isReversed());
        assertEquals(ReadType.STREAM, scan.getReadType());
    }

    @Test
    public void testOf_wrappingPreservesIndependence() {
        Scan raw = new Scan();
        raw.setCaching(50);
        AnyScan scan = AnyScan.of(raw);
        scan.setCaching(75);
        // The wrapped Scan should reflect changes through AnyScan.
        assertEquals(75, raw.getCaching());
        assertNotSame(scan.val(), new Scan());
    }
}
