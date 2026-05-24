/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;

public class AnyMutationTest extends TestBase {

    @Test
    public void testCellScanner_iteratesAllCellsAcrossFamilies() throws Exception {
        AnyPut put = AnyPut.of("row").addColumn("cf1", "q1", "v1").addColumn("cf1", "q2", "v2").addColumn("cf2", "q3", "v3");

        CellScanner scanner = put.cellScanner();
        assertNotNull(scanner);
        int count = 0;
        while (scanner.advance()) {
            assertNotNull(scanner.current());
            count++;
        }
        assertEquals(3, count);
    }

    @Test
    public void testGetFingerprint_returnsNonNullMap() {
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", "v");
        Map<String, Object> fp = put.getFingerprint();
        assertNotNull(fp);
        assertTrue(fp.size() > 0);
    }

    @Test
    public void testSetDurability_returnsSelfAndPersists() {
        AnyPut put = AnyPut.of("row");
        AnyPut returned = put.setDurability(Durability.SKIP_WAL);
        assertSame(put, returned);
        assertEquals(Durability.SKIP_WAL, put.getDurability());
    }

    @Test
    public void testSetDurability_allLevels() {
        AnyDelete del = AnyDelete.of("row");
        for (Durability d : Durability.values()) {
            del.setDurability(d);
            assertEquals(d, del.getDurability());
        }
    }

    @Test
    public void testGetFamilyCellMap_emptyMutation() {
        AnyPut put = AnyPut.of("row");
        NavigableMap<byte[], List<Cell>> map = put.getFamilyCellMap();
        assertNotNull(map);
        assertEquals(0, map.size());
    }

    @Test
    public void testGetFamilyCellMap_twoFamilies() {
        AnyPut put = AnyPut.of("row").addColumn("cf1", "q1", "v1").addColumn("cf2", "q2", "v2");
        NavigableMap<byte[], List<Cell>> map = put.getFamilyCellMap();
        assertEquals(2, map.size());
    }

    @Test
    public void testSetTimestamp_returnsSelfAndPersists() {
        AnyPut put = AnyPut.of("row");
        long ts = 12345678L;
        AnyPut returned = put.setTimestamp(ts);
        assertSame(put, returned);
        assertEquals(ts, put.getTimestamp());
    }

    @Test
    public void testSetClusterIds_returnsSelfAndPersists() {
        AnyPut put = AnyPut.of("row");
        List<UUID> ids = Arrays.asList(UUID.randomUUID(), UUID.randomUUID());
        AnyPut returned = put.setClusterIds(ids);
        assertSame(put, returned);
        assertEquals(2, put.getClusterIds().size());
    }

    @Test
    public void testGetClusterIds_initiallyNullOrEmpty() {
        AnyPut put = AnyPut.of("row");
        List<UUID> ids = put.getClusterIds();
        if (ids != null) {
            assertEquals(0, ids.size());
        }
    }

    @Test
    public void testSetCellVisibility_andGetCellVisibility() throws DeserializationException {
        AnyPut put = AnyPut.of("row");
        CellVisibility vis = new CellVisibility("PUBLIC");
        AnyPut returned = put.setCellVisibility(vis);
        assertSame(put, returned);
        CellVisibility got = put.getCellVisibility();
        assertNotNull(got);
        assertEquals("PUBLIC", got.getExpression());
    }

    @Test
    public void testGetCellVisibility_initiallyNull() throws DeserializationException {
        AnyPut put = AnyPut.of("row");
        assertNull(put.getCellVisibility());
    }

    @Test
    public void testSetACL_singleUser_returnsSelfAndStoresBytes() {
        AnyPut put = AnyPut.of("row");
        AnyPut returned = put.setACL("alice", new Permission(Permission.Action.READ));
        assertSame(put, returned);
        assertNotNull(put.getACL());
    }

    @Test
    public void testSetACL_multipleUsers_returnsSelfAndStoresBytes() {
        AnyPut put = AnyPut.of("row");
        Map<String, Permission> acl = new HashMap<>();
        acl.put("alice", new Permission(Permission.Action.READ, Permission.Action.WRITE));
        acl.put("bob", new Permission(Permission.Action.READ));
        AnyPut returned = put.setACL(acl);
        assertSame(put, returned);
        assertNotNull(put.getACL());
    }

    @Test
    public void testGetACL_initiallyNull() {
        AnyPut put = AnyPut.of("row");
        assertNull(put.getACL());
    }

    @Test
    public void testSetTTL_returnsSelfAndPersists() {
        AnyPut put = AnyPut.of("row");
        long ttl = 86400000L;
        AnyPut returned = put.setTTL(ttl);
        assertSame(put, returned);
        assertEquals(ttl, put.getTTL());
    }

    @Test
    public void testGetTTL_default_isLongMaxValue() {
        AnyPut put = AnyPut.of("row");
        assertEquals(Long.MAX_VALUE, put.getTTL());
    }

    @Test
    public void testGet_stringFamilyQualifier_returnsCells() {
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", "v");
        List<Cell> cells = put.get("cf", "q");
        assertEquals(1, cells.size());
    }

    @Test
    public void testGet_byteArrayFamilyQualifier_returnsCells() {
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", "v");
        List<Cell> cells = put.get(Bytes.toBytes("cf"), Bytes.toBytes("q"));
        assertEquals(1, cells.size());
    }

    @Test
    public void testGet_missing_returnsEmptyList() {
        AnyPut put = AnyPut.of("row");
        List<Cell> cells = put.get("cf", "missing");
        assertNotNull(cells);
        assertEquals(0, cells.size());
    }

    @Test
    public void testHas_stringFamilyQualifier_true() {
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", "v");
        assertTrue(put.has("cf", "q"));
    }

    @Test
    public void testHas_stringFamilyQualifier_false() {
        AnyPut put = AnyPut.of("row");
        assertFalse(put.has("cf", "q"));
    }

    @Test
    public void testHas_stringFamilyQualifierTimestamp() {
        long ts = 7L;
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", ts, "v");
        assertTrue(put.has("cf", "q", ts));
        assertFalse(put.has("cf", "q", ts + 1));
    }

    @Test
    public void testHas_stringFamilyQualifierValue() {
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", "v");
        assertTrue(put.has("cf", "q", "v"));
        assertFalse(put.has("cf", "q", "other"));
    }

    @Test
    public void testHas_stringFamilyQualifierTimestampValue() {
        long ts = 7L;
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", ts, "v");
        assertTrue(put.has("cf", "q", ts, "v"));
        assertFalse(put.has("cf", "q", ts, "other"));
    }

    @Test
    public void testHas_byteArrayFamilyQualifier() {
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", "v");
        assertTrue(put.has(Bytes.toBytes("cf"), Bytes.toBytes("q")));
        assertFalse(put.has(Bytes.toBytes("cf"), Bytes.toBytes("missing")));
    }

    @Test
    public void testHas_byteArrayFamilyQualifierTimestamp() {
        long ts = 100L;
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", ts, "v");
        assertTrue(put.has(Bytes.toBytes("cf"), Bytes.toBytes("q"), ts));
    }

    @Test
    public void testHas_byteArrayFamilyQualifierValue() {
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", "v");
        assertTrue(put.has(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("v")));
        assertFalse(put.has(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("other")));
    }

    @Test
    public void testHas_byteArrayFamilyQualifierTimestampValue() {
        long ts = 100L;
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", ts, "v");
        assertTrue(put.has(Bytes.toBytes("cf"), Bytes.toBytes("q"), ts, Bytes.toBytes("v")));
        assertFalse(put.has(Bytes.toBytes("cf"), Bytes.toBytes("q"), ts, Bytes.toBytes("other")));
    }

    @Test
    public void testGetRow_matchesRowKey() {
        AnyPut put = AnyPut.of("the-row");
        assertArrayEquals(Bytes.toBytes("the-row"), put.getRow());
    }

    @Test
    public void testIsEmpty_emptyAndNonEmpty() {
        AnyPut empty = AnyPut.of("row");
        assertTrue(empty.isEmpty());

        AnyPut nonEmpty = AnyPut.of("row").addColumn("cf", "q", "v");
        assertFalse(nonEmpty.isEmpty());
    }

    @Test
    public void testSize_countsAllCells() {
        AnyPut put = AnyPut.of("row").addColumn("cf1", "q1", "v1").addColumn("cf1", "q2", "v2").addColumn("cf2", "q3", "v3");
        assertEquals(3, put.size());
    }

    @Test
    public void testNumFamilies_countsDistinctFamilies() {
        AnyPut put = AnyPut.of("row").addColumn("cf1", "q1", "v1").addColumn("cf1", "q2", "v2").addColumn("cf2", "q3", "v3");
        assertEquals(2, put.numFamilies());
    }

    @Test
    public void testHeapSize_returnsPositive() {
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", "v");
        assertTrue(put.heapSize() > 0L);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testCompareTo_sameRow_returnsZero() {
        AnyPut a = AnyPut.of("k");
        AnyPut b = AnyPut.of("k");
        assertEquals(0, a.compareTo(b.val()));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testCompareTo_differentRows_returnsNonZero() {
        AnyPut a = AnyPut.of("k1");
        AnyPut b = AnyPut.of("k2");
        assertTrue(a.compareTo(b.val()) != 0);
    }

    @Test
    public void testInheritedMethods_onAnyDelete() {
        // HBase forbids setTTL on Delete; only Durability and getRow are safe here.
        AnyDelete del = AnyDelete.of("row").setDurability(Durability.ASYNC_WAL);
        assertEquals(Durability.ASYNC_WAL, del.getDurability());
        assertArrayEquals(Bytes.toBytes("row"), del.getRow());
    }

    @Test
    public void testInheritedMethods_onAnyAppend() {
        AnyAppend app = AnyAppend.of("row").addColumn("cf", "q", "v");
        assertArrayEquals(Bytes.toBytes("row"), app.getRow());
        assertTrue(app.has("cf", "q"));
        assertFalse(app.isEmpty());
        assertEquals(1, app.size());
        assertEquals(1, app.numFamilies());
    }

    @Test
    public void testInheritedMethods_onAnyIncrement() {
        AnyIncrement inc = AnyIncrement.of("row").addColumn("cf", "q", 5L);
        assertArrayEquals(Bytes.toBytes("row"), inc.getRow());
        assertTrue(inc.has("cf", "q"));
        assertFalse(inc.isEmpty());
        assertEquals(1, inc.size());
    }

    @Test
    public void testConstructor_nullMutation_throws() {
        assertThrows(IllegalArgumentException.class, () -> new TestMutation(null));
    }

    @Test
    public void testConstructor_nonNullMutation_succeeds() {
        Put p = new Put(Bytes.toBytes("row"));
        TestMutation m = new TestMutation(p);
        assertNotNull(m);
        assertArrayEquals(Bytes.toBytes("row"), m.getRow());
    }

    private static final class TestMutation extends AnyMutation<TestMutation> {
        protected TestMutation(final org.apache.hadoop.hbase.client.Mutation mutation) {
            super(mutation);
        }
    }
}
