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
import org.apache.hadoop.hbase.client.Cursor;
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

    // Factories and insert operations

    @Test
    public void testAddColumn_byteArray() {
        AnyScan scan = AnyScan.create().addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q"));
        assertEquals(1, scan.numFamilies());
    }

    @Test
    public void testAddColumn_string() {
        AnyScan scan = AnyScan.create().addColumn("cf", "q");
        assertEquals(1, scan.numFamilies());
    }

    // Factory methods: create(), of(...) variants

    @Test
    public void testCreate_defaultConfiguration() {
        AnyScan scan = AnyScan.create();
        assertNotNull(scan);
        assertNotNull(scan.val());
        assertFalse(scan.hasFamilies());
        assertEquals(0, scan.numFamilies());
    }

    @Test
    public void testCreateScanFromCursor_nullCursor_throwsIae() {
        assertThrows(IllegalArgumentException.class, () -> AnyScan.createScanFromCursor((Cursor) null));
    }

    // Updates and deletes

    @Test
    public void testSetAllowPartialResults_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setAllowPartialResults(true);
        assertSame(scan, returned);
        assertTrue(scan.getAllowPartialResults());
    }

    @Test
    public void testSetAsyncPrefetch_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setAsyncPrefetch(true);
        assertSame(scan, returned);
        assertEquals(Boolean.TRUE, scan.isAsyncPrefetch());
    }

    // batch / caching / cacheBlocks / resultSize / limits

    @Test
    public void testSetBatch_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setBatch(100);
        assertSame(scan, returned);
        assertEquals(100, scan.getBatch());
    }

    @Test
    public void testSetCacheBlocks_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setCacheBlocks(false);
        assertSame(scan, returned);
        assertFalse(scan.getCacheBlocks());
    }

    @Test
    public void testSetCaching_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setCaching(50);
        assertSame(scan, returned);
        assertEquals(50, scan.getCaching());
    }

    @Test
    public void testSetColumnFamilyTimeRange_byteArray() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setColumnFamilyTimeRange(Bytes.toBytes("cf"), 100L, 200L);
        assertSame(scan, returned);
    }

    // setColumnFamilyTimeRange

    @Test
    public void testSetColumnFamilyTimeRange_string() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setColumnFamilyTimeRange("cf", 100L, 200L);
        assertSame(scan, returned);
        Map<byte[], TimeRange> tr = scan.getColumnFamilyTimeRange();
        assertNotNull(tr);
        assertEquals(1, tr.size());
    }

    // Inherited from AnyQuery: consistency, replicaId, isolationLevel, etc.

    @Test
    public void testSetConsistency_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setConsistency(Consistency.TIMELINE);
        assertSame(scan, returned);
        assertEquals(Consistency.TIMELINE, scan.getConsistency());
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

    @Test
    public void testSetFilter_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        Filter filter = new PrefixFilter(Bytes.toBytes("foo"));
        AnyScan returned = scan.setFilter(filter);
        assertSame(scan, returned);
        assertTrue(scan.hasFilter());
        assertSame(filter, scan.getFilter());
    }

    @Test
    public void testSetIsolationLevel_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
        assertSame(scan, returned);
        assertEquals(IsolationLevel.READ_UNCOMMITTED, scan.getIsolationLevel());
    }

    @Test
    public void testSetLimit_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setLimit(100);
        assertSame(scan, returned);
        assertEquals(100, scan.getLimit());
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
    public void testSetMaxResultSize_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setMaxResultSize(1024L * 1024L);
        assertSame(scan, returned);
        assertEquals(1024L * 1024L, scan.getMaxResultSize());
    }

    @Test
    public void testSetMaxResultsPerColumnFamily() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setMaxResultsPerColumnFamily(10);
        assertSame(scan, returned);
        assertEquals(10, scan.getMaxResultsPerColumnFamily());
    }

    @Test
    public void testSetNeedCursorResult_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setNeedCursorResult(true);
        assertSame(scan, returned);
        assertTrue(scan.isNeedCursorResult());
    }

    @Test
    public void testSetOneRowLimit() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setOneRowLimit();
        assertSame(scan, returned);
        assertEquals(1, scan.getLimit());
    }

    @Test
    public void testSetRaw_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setRaw(true);
        assertSame(scan, returned);
        assertTrue(scan.isRaw());
    }

    /**
     * A {@code null} read type is rejected at the call site: the wrapped HBase {@code Scan} would
     * store it silently and only fail with a bare {@code NullPointerException} when the scan is
     * finally encoded for the server.
     */
    @Test
    public void testSetReadType_null_throwsIAE() {
        AnyScan scan = AnyScan.create();
        assertThrows(IllegalArgumentException.class, () -> scan.setReadType(null));
        assertEquals(ReadType.DEFAULT, scan.getReadType(), "A rejected value must not have been applied");
    }

    @Test
    public void testSetReadType_pread() {
        AnyScan scan = AnyScan.create().setReadType(ReadType.PREAD);
        assertEquals(ReadType.PREAD, scan.getReadType());
    }

    @Test
    public void testSetReadType_stream() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setReadType(ReadType.STREAM);
        assertSame(scan, returned);
        assertEquals(ReadType.STREAM, scan.getReadType());
    }

    @Test
    public void testSetReplicaId_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setReplicaId(1);
        assertSame(scan, returned);
        assertEquals(1, scan.getReplicaId());
    }

    @Test
    public void testSetReversed_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setReversed(true);
        assertSame(scan, returned);
        assertTrue(scan.isReversed());
    }

    @Test
    public void testSetRowOffsetPerColumnFamily() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setRowOffsetPerColumnFamily(5);
        assertSame(scan, returned);
        assertEquals(5, scan.getRowOffsetPerColumnFamily());
    }

    @Test
    public void testSetRowPrefixFilter_returnsSelf() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setRowPrefixFilter("prefix_");
        assertSame(scan, returned);
    }

    @Test
    public void testSetScanMetricsEnabled_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setScanMetricsEnabled(true);
        assertSame(scan, returned);
        assertTrue(scan.isScanMetricsEnabled());
    }

    @Test
    public void testSetStartStopRowForPrefixScan() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setStartStopRowForPrefixScan(Bytes.toBytes("user_"));
        assertSame(scan, returned);
        assertArrayEquals(Bytes.toBytes("user_"), scan.getStartRow());
    }

    @Test
    public void testSetTimeRange_invalidRange_throwsIllegalArgument() {
        AnyScan scan = AnyScan.create();
        // max < min wraps the IOException as IllegalArgumentException.
        assertThrows(IllegalArgumentException.class, () -> scan.setTimeRange(200L, 100L));
    }

    // Time range / timestamp setters and accessors

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
    public void testSetTimestamp_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.setTimestamp(12345L);
        assertSame(scan, returned);
        TimeRange tr = scan.getTimeRange();
        assertEquals(12345L, tr.getMin());
    }

    // Queries and results

    @Test
    public void testEquals_matchesUnderlyingScanEquals() {
        AnyScan s1 = AnyScan.create();
        AnyScan s2 = AnyScan.create();
        assertEquals(s1.val().equals(s2.val()), s1.equals(s2));
    }

    @Test
    public void testEquals_nonAnyScanObject_returnsFalse() {
        AnyScan scan = AnyScan.create();
        assertFalse(scan.equals("not an AnyScan"));
        assertFalse(scan.equals(null));
    }

    @Test
    public void testGetACL_initiallyNull() {
        AnyScan scan = AnyScan.create();
        assertNull(scan.getACL());
    }

    @Test
    public void testGetAllowPartialResults_defaultFalse() {
        AnyScan scan = AnyScan.create();
        assertFalse(scan.getAllowPartialResults());
    }

    @Test
    public void testGetFamilies_afterAddFamilies() {
        AnyScan scan = AnyScan.create().addFamily("cf1").addFamily("cf2");
        byte[][] families = scan.getFamilies();
        assertNotNull(families);
        assertEquals(2, families.length);
    }

    @Test
    public void testGetFamilyMap_returnsMap() {
        AnyScan scan = AnyScan.create().addColumn("cf", "q");
        Map<byte[], NavigableSet<byte[]>> map = scan.getFamilyMap();
        assertNotNull(map);
        assertEquals(1, map.size());
    }

    // ReadType

    @Test
    public void testGetReadType_default() {
        AnyScan scan = AnyScan.create();
        assertEquals(ReadType.DEFAULT, scan.getReadType());
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
    public void testGetTimeRange_default() {
        AnyScan scan = AnyScan.create();
        assertNotNull(scan.getTimeRange());
    }

    // hasFilter / filter setters (inherited from AnyQuery)

    @Test
    public void testHasFilter_initiallyFalse() {
        AnyScan scan = AnyScan.create();
        assertFalse(scan.hasFilter());
    }

    @Test
    public void testHashCode_matchesUnderlyingScan() {
        AnyScan scan = AnyScan.create();
        assertEquals(scan.val().hashCode(), scan.hashCode());
    }

    // isGetScan / hasFamilies / numFamilies / getFamilies

    @Test
    public void testIsGetScan_returnsBoolean() {
        AnyScan scan = AnyScan.create();
        assertFalse(scan.isGetScan());
    }

    // Scan metrics

    @Test
    public void testIsScanMetricsEnabled_defaultFalse() {
        AnyScan scan = AnyScan.create();
        assertFalse(scan.isScanMetricsEnabled());
    }

    @Test
    public void testOf_fromGet() {
        Get get = new Get(Bytes.toBytes("row-from-get"));
        AnyScan scan = AnyScan.of(get);
        assertNotNull(scan);
        assertNotNull(scan.val());
        assertArrayEquals(Bytes.toBytes("row-from-get"), scan.getStartRow());
    }

    @Test
    public void testOf_nullGet_throwsIae() {
        assertThrows(IllegalArgumentException.class, () -> AnyScan.of((Get) null));
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

    // val() and equality

    @Test
    public void testVal_returnsUnderlyingScan() {
        AnyScan scan = AnyScan.create();
        assertNotNull(scan.val());
        assertSame(scan.val(), scan.val());
    }

    // Additional contracts

    @Test
    public void testAddFamily_byteArray() {
        AnyScan scan = AnyScan.create().addFamily(Bytes.toBytes("cf"));
        assertEquals(1, scan.numFamilies());
    }

    // addFamily / addColumn variants

    @Test
    public void testAddFamily_string_returnsSelf() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.addFamily("cf");
        assertSame(scan, returned);
        assertEquals(1, scan.numFamilies());
    }

    // Chaining sanity check

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
    public void testEquals_sameInstance() {
        AnyScan scan = AnyScan.create();
        assertEquals(scan, scan);
    }

    @Test
    public void testHasFamilies_afterAddFamily() {
        AnyScan scan = AnyScan.create().addFamily("cf");
        assertTrue(scan.hasFamilies());
    }

    @Test
    public void testHasFamilies_initiallyFalse() {
        AnyScan scan = AnyScan.create();
        assertFalse(scan.hasFamilies());
    }

    // start / stop row accessors and setters

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

    // Async prefetch

    @Test
    public void testIsAsyncPrefetch_initial() {
        AnyScan scan = AnyScan.create();
        // Initially null or false depending on HBase defaults; just verify no NPE.
        Boolean async = scan.isAsyncPrefetch();
        assertTrue(async == null || async instanceof Boolean);
    }

    // Cursor results

    @Test
    public void testIsNeedCursorResult_defaultFalse() {
        AnyScan scan = AnyScan.create();
        assertFalse(scan.isNeedCursorResult());
    }

    @Test
    public void testIsRaw_defaultFalse() {
        AnyScan scan = AnyScan.create();
        assertFalse(scan.isRaw());
    }

    // Reversed / partial results / raw

    @Test
    public void testIsReversed_defaultFalse() {
        AnyScan scan = AnyScan.create();
        assertFalse(scan.isReversed());
    }

    @Test
    public void testOf_startAndStopRow() {
        AnyScan scan = AnyScan.of("a", "z");
        assertArrayEquals(Bytes.toBytes("a"), scan.getStartRow());
        assertArrayEquals(Bytes.toBytes("z"), scan.getStopRow());
    }

    @Test
    public void testOf_stringStartRow() {
        AnyScan scan = AnyScan.of("start-row");
        assertNotNull(scan);
        assertArrayEquals(Bytes.toBytes("start-row"), scan.getStartRow());
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

    @Test
    public void testReadAllVersions_returnsSelf() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.readAllVersions();
        assertSame(scan, returned);
        assertEquals(Integer.MAX_VALUE, scan.getMaxVersions());
    }

    @Test
    public void testReadVersions_lessThanOne_storedAsIs() {
        // Documented divergence from AnyGet.readVersions, which throws IAE for versions < 1:
        // AnyScan mirrors the HBase client and stores the value unchanged.
        assertEquals(0, AnyScan.create().readVersions(0).getMaxVersions());
        assertEquals(-1, AnyScan.create().readVersions(-1).getMaxVersions());
    }

    // readVersions / readAllVersions / getMaxVersions

    @Test
    public void testReadVersions_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.readVersions(5);
        assertSame(scan, returned);
        assertEquals(5, scan.getMaxVersions());
    }

    @Test
    public void testToString_notNull() {
        AnyScan scan = AnyScan.create().addFamily("cf");
        assertNotNull(scan.toString());
    }

    @Test
    public void testWithStartRow_inclusiveExclusive() {
        AnyScan scan = AnyScan.create().withStartRow("start", false);
        assertFalse(scan.includeStartRow());
        assertArrayEquals(Bytes.toBytes("start"), scan.getStartRow());
    }

    @Test
    public void testWithStartRow_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.withStartRow("start");
        assertSame(scan, returned);
        assertArrayEquals(Bytes.toBytes("start"), scan.getStartRow());
    }

    @Test
    public void testWithStopRow_inclusive() {
        AnyScan scan = AnyScan.create().withStopRow("stop", true);
        assertTrue(scan.includeStopRow());
        assertArrayEquals(Bytes.toBytes("stop"), scan.getStopRow());
    }

    @Test
    public void testWithStopRow_returnsSelfAndPersists() {
        AnyScan scan = AnyScan.create();
        AnyScan returned = scan.withStopRow("stop");
        assertSame(scan, returned);
        assertArrayEquals(Bytes.toBytes("stop"), scan.getStopRow());
    }

    // ---- 2026-10-20 sliceF ----

    /**
     * Pins the setStartStopRowForPrefixScan doc: the prefix becomes the inclusive start row and the closest next key the
     * exclusive stop row, and both bounds (and their inclusivity) replace whatever was set earlier.
     */
    @Test
    public void testSetStartStopRowForPrefixScan_boundsReplaceEarlierOnes_startInclusiveStopExclusive_sliceF() {
        final AnyScan scan = AnyScan.create().withStartRow("a", false).withStopRow("z", true);
        assertFalse(scan.includeStartRow());
        assertTrue(scan.includeStopRow());

        assertSame(scan, scan.setStartStopRowForPrefixScan("user_"));

        assertArrayEquals(Bytes.toBytes("user_"), scan.getStartRow());
        assertTrue(scan.includeStartRow());
        // '_' (0x5F) + 1 = '`' (0x60): the first key that no longer carries the prefix
        assertArrayEquals(Bytes.toBytes("user`"), scan.getStopRow());
        assertFalse(scan.includeStopRow());

        // trailing 0xFF bytes are dropped before the increment; an all-0xFF prefix runs to the end of the table
        scan.setStartStopRowForPrefixScan(new byte[] { 'a', (byte) 0xFF });
        assertArrayEquals(new byte[] { 'b' }, scan.getStopRow());
        scan.setStartStopRowForPrefixScan(new byte[] { (byte) 0xFF });
        assertArrayEquals(new byte[] { (byte) 0xFF }, scan.getStartRow());
        assertEquals(0, scan.getStopRow().length);

        // a null prefix resets both bounds
        scan.setStartStopRowForPrefixScan((Object) null);
        assertEquals(0, scan.getStartRow().length);
        assertEquals(0, scan.getStopRow().length);
    }

    /** Pins the setRowPrefixFilter doc: same bound-replacement rules as setStartStopRowForPrefixScan, no Filter installed. */
    @SuppressWarnings("deprecation")
    @Test
    public void testSetRowPrefixFilter_sameBoundRulesAsPrefixScan_sliceF() {
        final AnyScan scan = AnyScan.create().withStartRow("a", false).withStopRow("z", true);

        assertSame(scan, scan.setRowPrefixFilter("user_"));

        assertArrayEquals(Bytes.toBytes("user_"), scan.getStartRow());
        assertTrue(scan.includeStartRow());
        assertArrayEquals(Bytes.toBytes("user`"), scan.getStopRow());
        assertFalse(scan.includeStopRow());
        assertNull(scan.getFilter());

        scan.setRowPrefixFilter((Object) null);
        assertEquals(0, scan.getStartRow().length);
        assertEquals(0, scan.getStopRow().length);
    }

    private static org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.Scan toProto(final AnyScan scan) throws java.io.IOException {
        return org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil.toScan(scan.val());
    }

    /**
     * Pins the null-family docs of addFamily/addColumn/setColumnFamilyTimeRange: the null is accepted and stored here,
     * but HBase cannot serialize it, so building the request fails with NullPointerException.
     */
    @Test
    public void testNullFamily_acceptedHereButFailsWhenScanRequestIsBuilt_sliceF() {
        assertEquals(1, AnyScan.create().addFamily((String) null).numFamilies());

        assertThrows(NullPointerException.class, () -> toProto(AnyScan.create().addFamily((String) null)));
        assertThrows(NullPointerException.class, () -> toProto(AnyScan.create().addFamily((byte[]) null)));
        assertThrows(NullPointerException.class, () -> toProto(AnyScan.create().addColumn((String) null, "q")));
        assertThrows(NullPointerException.class, () -> toProto(AnyScan.create().addColumn((byte[]) null, Bytes.toBytes("q"))));
        assertThrows(NullPointerException.class, () -> toProto(AnyScan.create().setColumnFamilyTimeRange((String) null, 1L, 2L)));
        assertThrows(NullPointerException.class, () -> toProto(AnyScan.create().setColumnFamilyTimeRange((byte[]) null, 1L, 2L)));
    }

    /** Pins the "an empty or null family is accepted here" doc: the empty family is serialized as an empty family name. */
    @Test
    public void testEmptyFamily_isSerializedWhenScanRequestIsBuilt_sliceF() throws Exception {
        final org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.Scan wholeFamily = toProto(AnyScan.create().addFamily(""));
        assertEquals(1, wholeFamily.getColumnCount());
        assertTrue(wholeFamily.getColumn(0).getFamily().isEmpty());
        assertEquals(0, wholeFamily.getColumn(0).getQualifierCount());

        final org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.Scan oneColumn = toProto(
                AnyScan.create().addColumn(new byte[0], Bytes.toBytes("q")).setColumnFamilyTimeRange("", 1L, 2L));
        assertEquals(1, oneColumn.getColumnCount());
        assertTrue(oneColumn.getColumn(0).getFamily().isEmpty());
        assertArrayEquals(Bytes.toBytes("q"), oneColumn.getColumn(0).getQualifier(0).toByteArray());
        assertEquals(1, oneColumn.getCfTimeRangeCount());
        assertTrue(oneColumn.getCfTimeRange(0).getColumnFamily().isEmpty());
    }

    /** Pins the setScanMetricsEnabled doc: disabling also turns off per-region metrics enabled on the wrapped Scan. */
    @Test
    public void testSetScanMetricsEnabled_false_alsoDisablesByRegionMetrics_sliceF() {
        final AnyScan scan = AnyScan.create().setScanMetricsEnabled(true);
        scan.val().setEnableScanMetricsByRegion(true);
        assertTrue(scan.isScanMetricsEnabled());
        assertTrue(scan.val().isScanMetricsByRegionEnabled());

        assertSame(scan, scan.setScanMetricsEnabled(false));

        assertFalse(scan.isScanMetricsEnabled());
        assertFalse(scan.val().isScanMetricsByRegionEnabled());

        // re-enabling scan metrics does not re-enable the per-region ones
        scan.setScanMetricsEnabled(true);
        assertTrue(scan.isScanMetricsEnabled());
        assertFalse(scan.val().isScanMetricsByRegionEnabled());
    }

    // ---- 2026-10-24 sliceN ----

    /**
     * A null start/stop row is documented as an open bound, and HBase recognizes an unbounded row only as the EMPTY
     * array (its client compares with {@code Bytes.equals(row, HConstants.EMPTY_START_ROW / EMPTY_END_ROW)}, which is
     * false for null). Every row-bound entry point must therefore store the empty row for null, keeping the requested
     * inclusivity and the other bound.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testNullRowBound_isStoredAsTheEmptyUnboundedRow_sliceN() {
        final Filter filter = new PrefixFilter(Bytes.toBytes("u"));

        org.junit.jupiter.api.Assertions.assertAll( //
                () -> assertUnboundedStart_sliceN(AnyScan.create().withStartRow((Object) null), true),
                () -> assertUnboundedStart_sliceN(AnyScan.create().withStartRow((Object) null, false), false),
                () -> assertUnboundedStop_sliceN(AnyScan.create().withStopRow((Object) null), false),
                () -> assertUnboundedStop_sliceN(AnyScan.create().withStopRow((Object) null, true), true),
                () -> assertUnboundedStart_sliceN(AnyScan.of((Object) null), true), () -> assertUnboundedStart_sliceN(AnyScan.of((Object) null, "z"), true),
                () -> assertArrayEquals(Bytes.toBytes("z"), AnyScan.of((Object) null, "z").getStopRow()),
                () -> assertUnboundedStop_sliceN(AnyScan.of("a", (Object) null), false),
                () -> assertArrayEquals(Bytes.toBytes("a"), AnyScan.of("a", (Object) null).getStartRow()),
                () -> assertFalse(AnyScan.of((Object) null, (Object) null).isGetScan()),
                () -> assertUnboundedStart_sliceN(AnyScan.of((Object) null, filter), true),
                () -> assertSame(filter, AnyScan.of((Object) null, filter).getFilter()),
                // a null bound replaces an earlier explicit bound
                () -> assertUnboundedStart_sliceN(AnyScan.create().withStartRow("a").withStartRow((Object) null), true),
                () -> assertUnboundedStop_sliceN(AnyScan.create().withStopRow("z", true).withStopRow((Object) null), false));
    }

    /**
     * Pins the consequence through HBase's own (package-private) sync-scanner decisions in hbase-client 2.6.x, on a
     * table whose first region is {@code ["", "m")}: with a null stop row, {@code ClientSimpleScanner.setNewStartKey}
     * ended a forward scan after that first region; with a null start row, {@code ReversedScannerCallable.prepare} did
     * not treat the start as unbounded, so a reversed scan began at the first region and then ended; an exclusive null
     * start row threw {@code NullPointerException} from {@code createClosestRowAfter} when the scanner opened.
     */
    @Test
    public void testNullRowBound_scansWholeTableInHBaseScannerDecisions_sliceN() throws Exception {
        final Class<?> connectionUtils = Class.forName("org.apache.hadoop.hbase.client.ConnectionUtils");
        final java.lang.reflect.Method noMoreResultsForScan = connectionUtils.getDeclaredMethod("noMoreResultsForScan", Scan.class,
                org.apache.hadoop.hbase.client.RegionInfo.class);
        final java.lang.reflect.Method isEmptyStartRow = connectionUtils.getDeclaredMethod("isEmptyStartRow", byte[].class);
        noMoreResultsForScan.setAccessible(true);
        isEmptyStartRow.setAccessible(true);

        final org.apache.hadoop.hbase.client.RegionInfo firstRegion = org.apache.hadoop.hbase.client.RegionInfoBuilder
                .newBuilder(org.apache.hadoop.hbase.TableName.valueOf("t"))
                .setStartKey(new byte[0])
                .setEndKey(Bytes.toBytes("m"))
                .build();

        org.junit.jupiter.api.Assertions.assertAll( //
                () -> assertEquals(Boolean.FALSE, noMoreResultsForScan.invoke(null, AnyScan.create().val(), firstRegion)),
                () -> assertEquals(Boolean.FALSE, noMoreResultsForScan.invoke(null, AnyScan.create().withStopRow((Object) null).val(), firstRegion)),
                () -> assertEquals(Boolean.TRUE, isEmptyStartRow.invoke(null, AnyScan.create().withStartRow((Object) null).setReversed(true).getStartRow())),
                () -> assertEquals(Boolean.TRUE, isEmptyStartRow.invoke(null, AnyScan.create().withStartRow((Object) null, false).getStartRow())));
    }

    private static void assertUnboundedStart_sliceN(final AnyScan scan, final boolean inclusive) {
        assertTrue(Bytes.equals(org.apache.hadoop.hbase.HConstants.EMPTY_START_ROW, scan.getStartRow()), "start row must be the empty row");
        assertEquals(inclusive, scan.includeStartRow());
    }

    private static void assertUnboundedStop_sliceN(final AnyScan scan, final boolean inclusive) {
        assertTrue(Bytes.equals(org.apache.hadoop.hbase.HConstants.EMPTY_END_ROW, scan.getStopRow()), "stop row must be the empty row");
        assertEquals(inclusive, scan.includeStopRow());
    }

    // ---- 2026-10-24 verify8 ----

    /**
     * An empty value wrapper converts to null row-key bytes (HBaseExecutor.toRowKeyBytes, like a null row key), so as a
     * row bound it must be stored as the empty, unbounded row too, never as a null row: a null stop row silently ended a
     * forward scan after its first region (see the sliceN tests above).
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testEmptyValueWrapperRowBound_isStoredAsTheEmptyUnboundedRow_verify8() {
        final Filter filter = new PrefixFilter(Bytes.toBytes("u"));

        org.junit.jupiter.api.Assertions.assertAll( //
                () -> assertUnboundedStart_sliceN(AnyScan.create().withStartRow(java.util.Optional.empty()), true),
                () -> assertUnboundedStart_sliceN(AnyScan.create().withStartRow(com.landawn.abacus.util.u.Nullable.empty(), false), false),
                () -> assertUnboundedStop_sliceN(AnyScan.create().withStopRow(java.util.OptionalInt.empty()), false),
                () -> assertUnboundedStop_sliceN(AnyScan.create().withStopRow(com.landawn.abacus.util.Holder.of(null), true), true),
                () -> assertUnboundedStart_sliceN(AnyScan.of(java.util.Optional.empty()), true),
                () -> assertUnboundedStart_sliceN(AnyScan.of(java.util.Optional.empty(), java.util.Optional.empty()), true),
                () -> assertUnboundedStop_sliceN(AnyScan.of(java.util.Optional.empty(), java.util.Optional.empty()), false),
                () -> assertUnboundedStart_sliceN(AnyScan.of(com.landawn.abacus.util.u.Optional.empty(), filter), true),
                // a present wrapper is still converted through its value
                () -> assertArrayEquals(Bytes.toBytes("a"), AnyScan.create().withStartRow(java.util.Optional.of("a")).getStartRow()));
    }

    // ---- 2026-10-24 r2 sliceN ----

    /**
     * Pins the documented prefix-scan caveat: the computed bounds are a forward scan's ({@code [prefix, next-prefix)}) and are
     * not swapped by {@code setReversed(true)}, called before or after. A reversed scan reads from its start row (upper bound)
     * down to its stop row (lower bound, exclusive), and here the stop row sorts above the start row, so no row qualifies.
     */
    @Test
    public void testPrefixScanBounds_areNotSwappedForAReversedScan_r2sliceN() {
        final AnyScan reversedAfter = AnyScan.create().setStartStopRowForPrefixScan("user_").setReversed(true);
        final AnyScan reversedBefore = AnyScan.create().setReversed(true).setStartStopRowForPrefixScan("user_");

        org.junit.jupiter.api.Assertions.assertAll( //
                () -> assertTrue(reversedAfter.isReversed()), //
                () -> assertTrue(reversedBefore.isReversed()), //
                () -> assertArrayEquals(Bytes.toBytes("user_"), reversedAfter.getStartRow()),
                () -> assertArrayEquals(Bytes.toBytes("user`"), reversedAfter.getStopRow()),
                () -> assertArrayEquals(Bytes.toBytes("user_"), reversedBefore.getStartRow()),
                () -> assertArrayEquals(Bytes.toBytes("user`"), reversedBefore.getStopRow()), () -> assertTrue(reversedAfter.includeStartRow()), //
                () -> assertFalse(reversedAfter.includeStopRow()),
                // the reversed range (stop, start] is empty: the stop row sorts above the start row
                () -> assertTrue(Bytes.compareTo(reversedAfter.getStopRow(), reversedAfter.getStartRow()) > 0));
    }

    // ---- 2026-10-27 sliceN ----

    /**
     * Pins the setLimit doc: HBase puts a row limit into the scan request only for a positive limit, so 0 and a negative
     * limit (the default -1) do not limit the scan on the server.
     */
    @Test
    public void testSetLimit_onlyAPositiveLimitIsSentToTheServer_sliceN() {
        final byte[] region = Bytes.toBytes("region");

        org.junit.jupiter.api.Assertions.assertAll( //
                () -> assertEquals(5, buildOpenScanRequest_sliceN(region, AnyScan.create().setLimit(5)).getLimitOfRows()),
                () -> assertFalse(buildOpenScanRequest_sliceN(region, AnyScan.create().setLimit(0)).hasLimitOfRows()),
                () -> assertFalse(buildOpenScanRequest_sliceN(region, AnyScan.create()).hasLimitOfRows()),
                () -> assertFalse(buildOpenScanRequest_sliceN(region, AnyScan.create().setLimit(-5)).hasLimitOfRows()));
    }

    /**
     * Pins the setLimit doc's {@code 0} case through HBase 2.6's real synchronous scanner (the one HBaseExecutor's tables
     * return): with a fake RPC layer that serves three rows on every call, a limit of 0 yields exactly the rows of the first
     * server response and then reports the end of the scan - neither "no rows" nor "no limit".
     */
    @Test
    public void testSetLimitZero_syncScannerReturnsOnlyTheFirstServerResponse_sliceN() throws Exception {
        final org.apache.hadoop.conf.Configuration conf = org.apache.hadoop.hbase.HBaseConfiguration.create();
        final org.apache.hadoop.hbase.client.ClusterConnection conn = org.mockito.Mockito.mock(org.apache.hadoop.hbase.client.ClusterConnection.class);
        org.mockito.Mockito.when(conn.getConfiguration()).thenReturn(conf);

        final org.apache.hadoop.hbase.ipc.RpcControllerFactory controllerFactory = org.mockito.Mockito
                .mock(org.apache.hadoop.hbase.ipc.RpcControllerFactory.class);
        org.mockito.Mockito.when(controllerFactory.newController()).thenReturn(org.mockito.Mockito.mock(org.apache.hadoop.hbase.ipc.HBaseRpcController.class));

        final java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        @SuppressWarnings("unchecked")
        final org.apache.hadoop.hbase.client.RpcRetryingCaller<org.apache.hadoop.hbase.client.Result[]> caller = org.mockito.Mockito
                .mock(org.apache.hadoop.hbase.client.RpcRetryingCaller.class);
        org.mockito.Mockito.when(caller.callWithoutRetries(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyInt())).thenAnswer(inv -> {
            final int call = calls.incrementAndGet();
            final org.apache.hadoop.hbase.client.Result[] rows = new org.apache.hadoop.hbase.client.Result[3];

            for (int i = 0; i < rows.length; i++) {
                rows[i] = org.apache.hadoop.hbase.client.Result.create(new org.apache.hadoop.hbase.Cell[] {
                        new org.apache.hadoop.hbase.KeyValue(Bytes.toBytes("r" + call + i), Bytes.toBytes("f"), Bytes.toBytes("q"), Bytes.toBytes("v")) });
            }

            return rows;
        });

        final org.apache.hadoop.hbase.client.RpcRetryingCallerFactory callerFactory = org.mockito.Mockito
                .mock(org.apache.hadoop.hbase.client.RpcRetryingCallerFactory.class);
        org.mockito.Mockito.when(callerFactory.<org.apache.hadoop.hbase.client.Result[]> newCaller()).thenReturn(caller);

        final java.lang.reflect.Constructor<org.apache.hadoop.hbase.client.ConnectionConfiguration> connConfigCtor = org.apache.hadoop.hbase.client.ConnectionConfiguration.class
                .getDeclaredConstructor(org.apache.hadoop.conf.Configuration.class);
        connConfigCtor.setAccessible(true);

        final AnyScan anyScan = AnyScan.create().setLimit(0).setCaching(3);
        // HTable.getScanner hands the scanner a copy of the caller's Scan
        final Scan scan = new Scan(anyScan.val());

        try (org.apache.hadoop.hbase.client.ResultScanner scanner = new org.apache.hadoop.hbase.client.ClientSimpleScanner(conf, scan, anyScan.val(),
                org.apache.hadoop.hbase.TableName.valueOf("t"), conn, callerFactory, controllerFactory,
                org.mockito.Mockito.mock(java.util.concurrent.ExecutorService.class), 1000, 1000, 1000, connConfigCtor.newInstance(conf),
                java.util.Collections.emptyMap())) {
            int rows = 0;

            while (scanner.next() != null && rows < 20) {
                rows++;
            }

            assertEquals(3, rows, "a limit of 0 must return the first server response's rows, then end the scan");
        }
    }

    /**
     * Pins the setBatch doc: a filter whose hasFilterRow() is true conflicts with batching in both directions - setBatch
     * throws for any value once such a filter is set, and the inherited setFilter throws once a positive batch is set.
     */
    @Test
    public void testSetBatch_hasFilterRowFilterConflictsInBothDirections_sliceN() {
        org.junit.jupiter.api.Assertions.assertAll( //
                () -> assertThrows(org.apache.hadoop.hbase.filter.IncompatibleFilterException.class,
                        () -> AnyScan.create().setBatch(10).setFilter(new org.apache.hadoop.hbase.filter.PageFilter(5))),
                () -> assertThrows(org.apache.hadoop.hbase.filter.IncompatibleFilterException.class,
                        () -> AnyScan.create().setFilter(new org.apache.hadoop.hbase.filter.PageFilter(5)).setBatch(-1)),
                () -> assertNotNull(AnyScan.create().setBatch(-1).setFilter(new org.apache.hadoop.hbase.filter.PageFilter(5)).getFilter()),
                () -> assertNotNull(AnyScan.create().setBatch(10).setFilter(new PrefixFilter(Bytes.toBytes("u"))).getFilter()));
    }

    /**
     * Pins the setFamilyMap doc: the map is retained by reference, so only a Bytes.BYTES_COMPARATOR map keeps later adds
     * correct - a HashMap gains a duplicate family entry, a naturally ordered TreeMap throws ClassCastException.
     */
    @Test
    public void testSetFamilyMap_mapMustBeOrderedByBytesComparator_sliceN() {
        final Map<byte[], NavigableSet<byte[]>> comparatorMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        comparatorMap.put(Bytes.toBytes("cf"), null);
        final Map<byte[], NavigableSet<byte[]>> hashMap = new java.util.HashMap<>();
        hashMap.put(Bytes.toBytes("cf"), null);

        org.junit.jupiter.api.Assertions.assertAll( //
                () -> assertSame(comparatorMap, AnyScan.create().setFamilyMap(comparatorMap).getFamilyMap()),
                () -> assertEquals(1, AnyScan.create().setFamilyMap(comparatorMap).addFamily("cf").addColumn("cf", "q").numFamilies()),
                () -> assertEquals(2, AnyScan.create().setFamilyMap(hashMap).addFamily("cf").numFamilies()),
                () -> assertThrows(ClassCastException.class, () -> AnyScan.create().setFamilyMap(new TreeMap<>()).addFamily("cf")),
                () -> assertThrows(ClassCastException.class, () -> AnyScan.create().setFamilyMap(new TreeMap<>()).addColumn("cf", "q")));
    }

    private static org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest buildOpenScanRequest_sliceN(final byte[] region,
            final AnyScan scan) throws java.io.IOException {
        return org.apache.hadoop.hbase.shaded.protobuf.RequestConverter.buildScanRequest(region, scan.val(), 100, false);
    }

    // ---- 2026-10-27 r2O ----

    /**
     * Pins the row-bound @throws docs of the Object row-bound sites: an HBase operation or Any* wrapper passed as a bound or a
     * prefix is rejected (HBaseExecutor.toRowKeyBytes) instead of becoming its bean JSON; a null bound is still unbounded.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testRowBounds_operationAsRowKey_throwsIae_r2O() {
        final Object anyGet = AnyGet.of("r");
        final Object scan = new Scan();
        final Filter filter = null;

        org.junit.jupiter.api.Assertions.assertAll(() -> assertThrows(IllegalArgumentException.class, () -> AnyScan.of(anyGet)),
                () -> assertThrows(IllegalArgumentException.class, () -> AnyScan.of("a", scan)),
                () -> assertThrows(IllegalArgumentException.class, () -> AnyScan.of(anyGet, filter)),
                () -> assertThrows(IllegalArgumentException.class, () -> AnyScan.create().withStartRow(anyGet)),
                () -> assertThrows(IllegalArgumentException.class, () -> AnyScan.create().withStartRow(scan, false)),
                () -> assertThrows(IllegalArgumentException.class, () -> AnyScan.create().withStopRow(anyGet)),
                () -> assertThrows(IllegalArgumentException.class, () -> AnyScan.create().withStopRow(anyGet, true)),
                () -> assertThrows(IllegalArgumentException.class, () -> AnyScan.create().setRowPrefixFilter(scan)),
                () -> assertThrows(IllegalArgumentException.class, () -> AnyScan.create().setStartStopRowForPrefixScan(anyGet)),
                () -> assertEquals(0, AnyScan.create().withStopRow((Object) null, true).getStopRow().length));
    }
}
