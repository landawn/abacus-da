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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;

/**
 * Pure unit tests for {@link AnyDelete} (no live HBase). Exercises the fluent
 * builder API and accessor methods inherited from {@link AnyMutation}.
 */
public class AnyDeleteTest extends TestBase {

    // ---------------------------------------------------------------------
    // Factory methods: of(...) variants
    // ---------------------------------------------------------------------

    @Test
    public void testOf_stringRowKey() {
        AnyDelete delete = AnyDelete.of("row-1");
        assertNotNull(delete);
        assertArrayEquals(Bytes.toBytes("row-1"), delete.getRow());
    }

    @Test
    public void testOf_byteArrayRowKey() {
        byte[] row = Bytes.toBytes("row-bytes");
        AnyDelete delete = AnyDelete.of(row);
        assertArrayEquals(row, delete.getRow());
    }

    @Test
    public void testOf_withTimestamp() {
        long ts = 1_700_000_000_000L;
        AnyDelete delete = AnyDelete.of("row-ts", ts);
        assertEquals(ts, delete.getTimestamp());
        assertArrayEquals(Bytes.toBytes("row-ts"), delete.getRow());
    }

    @Test
    public void testOf_withOffsetAndLength() {
        AnyDelete delete = AnyDelete.of("abcdefgh", 1, 3);
        assertArrayEquals(Bytes.toBytes("bcd"), delete.getRow());
    }

    @Test
    public void testOf_withOffsetLengthAndTimestamp() {
        long ts = 12345L;
        AnyDelete delete = AnyDelete.of("abcdefgh", 0, 3, ts);
        assertArrayEquals(Bytes.toBytes("abc"), delete.getRow());
        assertEquals(ts, delete.getTimestamp());
    }

    @Test
    public void testOf_withTimestampAndFamilyMap() {
        long ts = 99999L;
        NavigableMap<byte[], List<Cell>> familyMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        AnyDelete delete = AnyDelete.of("row-fam", ts, familyMap);
        assertArrayEquals(Bytes.toBytes("row-fam"), delete.getRow());
        assertEquals(ts, delete.getTimestamp());
    }

    @Test
    public void testOf_copyExistingDelete() {
        Delete original = new Delete(Bytes.toBytes("orig"));
        original.addFamily(Bytes.toBytes("cf"));

        AnyDelete copy = AnyDelete.of(original);

        assertNotNull(copy);
        assertNotSame(original, copy.val(), "Copy should be a different Delete instance");
        assertArrayEquals(Bytes.toBytes("orig"), copy.getRow());
    }

    // ---------------------------------------------------------------------
    // val()
    // ---------------------------------------------------------------------

    @Test
    public void testVal_returnsUnderlyingDelete() {
        AnyDelete delete = AnyDelete.of("row");
        Delete underlying = delete.val();
        assertNotNull(underlying);
        assertArrayEquals(Bytes.toBytes("row"), underlying.getRow());
    }

    @Test
    public void testVal_consistentAcrossCalls() {
        AnyDelete delete = AnyDelete.of("row");
        assertSame(delete.val(), delete.val());
    }

    // ---------------------------------------------------------------------
    // addFamily variants
    // ---------------------------------------------------------------------

    @Test
    public void testAddFamily_string_returnsSelf() {
        AnyDelete delete = AnyDelete.of("row");
        AnyDelete returned = delete.addFamily("cf");
        assertSame(delete, returned);
        assertEquals(1, delete.numFamilies());
    }

    @Test
    public void testAddFamily_stringWithTimestamp() {
        AnyDelete delete = AnyDelete.of("row").addFamily("cf", 100L);
        assertEquals(1, delete.numFamilies());
    }

    @Test
    public void testAddFamily_byteArray() {
        AnyDelete delete = AnyDelete.of("row").addFamily(Bytes.toBytes("cf"));
        assertEquals(1, delete.numFamilies());
    }

    @Test
    public void testAddFamily_byteArrayWithTimestamp() {
        AnyDelete delete = AnyDelete.of("row").addFamily(Bytes.toBytes("cf"), 200L);
        assertEquals(1, delete.numFamilies());
    }

    @Test
    public void testAddFamily_multipleFamilies() {
        AnyDelete delete = AnyDelete.of("row").addFamily("cf1").addFamily("cf2");
        assertEquals(2, delete.numFamilies());
    }

    // ---------------------------------------------------------------------
    // addFamilyVersion variants
    // ---------------------------------------------------------------------

    @Test
    public void testAddFamilyVersion_string() {
        AnyDelete delete = AnyDelete.of("row").addFamilyVersion("cf", 12345L);
        assertEquals(1, delete.numFamilies());
    }

    @Test
    public void testAddFamilyVersion_byteArray() {
        AnyDelete delete = AnyDelete.of("row").addFamilyVersion(Bytes.toBytes("cf"), 12345L);
        assertEquals(1, delete.numFamilies());
    }

    // ---------------------------------------------------------------------
    // addColumn variants
    // ---------------------------------------------------------------------

    @Test
    public void testAddColumn_string() {
        AnyDelete delete = AnyDelete.of("row").addColumn("cf", "q");
        assertEquals(1, delete.numFamilies());
    }

    @Test
    public void testAddColumn_stringWithTimestamp() {
        AnyDelete delete = AnyDelete.of("row").addColumn("cf", "q", 100L);
        assertEquals(1, delete.numFamilies());
    }

    @Test
    public void testAddColumn_byteArray() {
        AnyDelete delete = AnyDelete.of("row").addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q"));
        assertEquals(1, delete.numFamilies());
    }

    @Test
    public void testAddColumn_byteArrayWithTimestamp() {
        AnyDelete delete = AnyDelete.of("row").addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q"), 100L);
        assertEquals(1, delete.numFamilies());
    }

    @Test
    public void testAddColumns_string() {
        AnyDelete delete = AnyDelete.of("row").addColumns("cf", "q");
        assertEquals(1, delete.numFamilies());
    }

    @Test
    public void testAddColumns_stringWithTimestamp() {
        AnyDelete delete = AnyDelete.of("row").addColumns("cf", "q", 100L);
        assertEquals(1, delete.numFamilies());
    }

    @Test
    public void testAddColumns_byteArray() {
        AnyDelete delete = AnyDelete.of("row").addColumns(Bytes.toBytes("cf"), Bytes.toBytes("q"));
        assertEquals(1, delete.numFamilies());
    }

    @Test
    public void testAddColumns_byteArrayWithTimestamp() {
        AnyDelete delete = AnyDelete.of("row").addColumns(Bytes.toBytes("cf"), Bytes.toBytes("q"), 100L);
        assertEquals(1, delete.numFamilies());
    }

    @Test
    public void testChaining_multipleOperations() {
        AnyDelete delete = AnyDelete.of("row").addFamily("cf1").addColumn("cf2", "q1").addColumns("cf3", "q2");
        assertEquals(3, delete.numFamilies());
    }

    // ---------------------------------------------------------------------
    // Mutation accessors
    // ---------------------------------------------------------------------

    @Test
    public void testGetRow_matchesConstructorRowKey() {
        AnyDelete delete = AnyDelete.of("the-row-key");
        assertArrayEquals(Bytes.toBytes("the-row-key"), delete.getRow());
    }

    @Test
    public void testGetFamilyCellMap_returnsNotNull() {
        AnyDelete delete = AnyDelete.of("row").addFamily("cf");
        NavigableMap<byte[], List<Cell>> map = delete.getFamilyCellMap();
        assertNotNull(map);
        assertEquals(1, map.size());
    }

    @Test
    public void testGetFingerprint() {
        AnyDelete delete = AnyDelete.of("row").addFamily("cf");
        assertNotNull(delete.getFingerprint());
    }

    @Test
    public void testSize_afterAddingFamilies() {
        AnyDelete delete = AnyDelete.of("row").addFamily("cf1").addFamily("cf2");
        assertEquals(2, delete.size());
    }

    @Test
    public void testHeapSize_returnsPositive() {
        AnyDelete delete = AnyDelete.of("row").addFamily("cf");
        assertTrue(delete.heapSize() > 0L);
    }

    // ---------------------------------------------------------------------
    // Durability / TTL / Timestamp setters
    // ---------------------------------------------------------------------

    @Test
    public void testSetDurability_returnsSelfAndPersists() {
        AnyDelete delete = AnyDelete.of("row");
        AnyDelete returned = delete.setDurability(Durability.SKIP_WAL);
        assertSame(delete, returned);
        assertEquals(Durability.SKIP_WAL, delete.getDurability());
    }

    @Test
    public void testSetTimestamp_returnsSelfAndPersists() {
        AnyDelete delete = AnyDelete.of("row");
        long ts = 12345L;
        AnyDelete returned = delete.setTimestamp(ts);
        assertSame(delete, returned);
        assertEquals(ts, delete.getTimestamp());
    }

    @Test
    public void testSetTTL_unsupportedForDelete_throws() {
        // HBase's Delete class explicitly rejects setTTL.
        AnyDelete delete = AnyDelete.of("row");
        assertThrows(UnsupportedOperationException.class, () -> delete.setTTL(86_400_000L));
    }

    // ---------------------------------------------------------------------
    // equals, hashCode, toString
    // ---------------------------------------------------------------------

    @Test
    public void testEquals_sameInstance() {
        AnyDelete delete = AnyDelete.of("row").addFamily("cf");
        assertEquals(delete, delete);
    }

    @Test
    public void testEquals_matchesUnderlyingDeleteEquals() {
        AnyDelete d1 = AnyDelete.of("row");
        AnyDelete d2 = AnyDelete.of("row");
        assertEquals(d1.val().equals(d2.val()), d1.equals(d2));
    }

    @Test
    public void testEquals_nonAnyDeleteObject_returnsFalse() {
        AnyDelete delete = AnyDelete.of("row");
        assertFalse(delete.equals("not an AnyDelete"));
        assertFalse(delete.equals(null));
    }

    @Test
    public void testHashCode_matchesUnderlyingDelete() {
        AnyDelete delete = AnyDelete.of("row");
        assertEquals(delete.val().hashCode(), delete.hashCode());
    }

    @Test
    public void testToString_notNull() {
        AnyDelete delete = AnyDelete.of("row").addFamily("cf");
        String s = delete.toString();
        assertNotNull(s);
    }

    // ---------------------------------------------------------------------
    // toDelete(Collection) static method
    // ---------------------------------------------------------------------

    @Test
    public void testToDelete_collection() {
        AnyDelete d1 = AnyDelete.of("r1").addFamily("cf");
        AnyDelete d2 = AnyDelete.of("r2").addColumn("cf", "q");

        List<Delete> deletes = AnyDelete.toDelete(Arrays.asList(d1, d2));

        assertEquals(2, deletes.size());
        assertSame(d1.val(), deletes.get(0));
        assertSame(d2.val(), deletes.get(1));
    }

    @Test
    public void testToDelete_emptyCollection() {
        List<Delete> deletes = AnyDelete.toDelete(Collections.<AnyDelete> emptyList());
        assertEquals(0, deletes.size());
    }

    @Test
    public void testToDelete_nullCollection_throws() {
        assertThrows(IllegalArgumentException.class, () -> AnyDelete.toDelete(null));
    }

    @Test
    public void testToDelete_collectionWithNull_throws() {
        AnyDelete d1 = AnyDelete.of("r1");
        assertThrows(IllegalArgumentException.class, () -> AnyDelete.toDelete(Arrays.asList(d1, null)));
    }

    // ---------------------------------------------------------------------
    // Independence from copied Delete
    // ---------------------------------------------------------------------

    @Test
    public void testOf_copyExistingDelete_isIndependent() {
        Delete orig = new Delete(Bytes.toBytes("k"));
        orig.addFamily(Bytes.toBytes("cf"));
        AnyDelete copy = AnyDelete.of(orig);

        copy.addFamily("cf2");
        assertEquals(2, copy.numFamilies());
        // Original should still have only 1 family
        assertEquals(1, orig.getFamilyCellMap().size());
    }
}
