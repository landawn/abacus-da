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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.annotation.Column;
import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.da.hbase.annotation.ColumnFamily;
import com.landawn.abacus.util.NamingPolicy;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Pure unit tests for {@link AnyPut} (no live HBase). Exercises the fluent
 * builder API, entity-based factory methods, and accessor methods inherited
 * from {@link AnyMutation}.
 */
public class AnyPutTest extends TestBase {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @ColumnFamily("info")
    public static class SimpleUser {
        @Id
        private String userId;
        private String name;
        private String email;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class UserWithCustomColumn {
        @Id
        private String id;
        @Column("user_name")
        @ColumnFamily("profile")
        private String name;
    }

    // ---------------------------------------------------------------------
    // Factory methods: of(Object) and variants
    // ---------------------------------------------------------------------

    @Test
    public void testOf_stringRowKey() {
        AnyPut put = AnyPut.of("row-1");
        assertNotNull(put);
        assertArrayEquals(Bytes.toBytes("row-1"), put.getRow());
    }

    @Test
    public void testOf_byteArrayRowKey() {
        byte[] row = Bytes.toBytes("row-bytes");
        AnyPut put = AnyPut.of(row);
        assertArrayEquals(row, put.getRow());
    }

    @Test
    public void testOf_withTimestamp() {
        long ts = 1_700_000_000_000L;
        AnyPut put = AnyPut.of("row-ts", ts);
        assertEquals(ts, put.getTimestamp());
        assertArrayEquals(Bytes.toBytes("row-ts"), put.getRow());
    }

    @Test
    public void testOf_withOffsetAndLength() {
        AnyPut put = AnyPut.of("abcdefgh", 1, 3);
        assertArrayEquals(Bytes.toBytes("bcd"), put.getRow());
    }

    @Test
    public void testOf_withOffsetLengthAndTimestamp() {
        long ts = 12345L;
        AnyPut put = AnyPut.of("abcdefgh", 0, 3, ts);
        assertArrayEquals(Bytes.toBytes("abc"), put.getRow());
        assertEquals(ts, put.getTimestamp());
    }

    @Test
    public void testOf_withRowIsImmutable() {
        AnyPut put = AnyPut.of("row-imm", true);
        assertArrayEquals(Bytes.toBytes("row-imm"), put.getRow());
    }

    @Test
    public void testOf_withTimestampAndRowIsImmutable() {
        long ts = 99999L;
        AnyPut put = AnyPut.of("row-imm-ts", ts, true);
        assertArrayEquals(Bytes.toBytes("row-imm-ts"), put.getRow());
        assertEquals(ts, put.getTimestamp());
    }

    @Test
    public void testOf_byteBufferRowKey() {
        ByteBuffer rowBuf = ByteBuffer.wrap(Bytes.toBytes("row-bb"));
        AnyPut put = AnyPut.of(rowBuf);
        assertArrayEquals(Bytes.toBytes("row-bb"), put.getRow());
    }

    @Test
    public void testOf_byteBufferRowKeyWithTimestamp() {
        ByteBuffer rowBuf = ByteBuffer.wrap(Bytes.toBytes("row-bb-ts"));
        long ts = 555L;
        AnyPut put = AnyPut.of(rowBuf, ts);
        assertArrayEquals(Bytes.toBytes("row-bb-ts"), put.getRow());
        assertEquals(ts, put.getTimestamp());
    }

    @Test
    public void testOf_copyExistingPut() {
        Put original = new Put(Bytes.toBytes("orig"));
        original.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("v"));

        AnyPut copy = AnyPut.of(original);

        assertNotNull(copy);
        assertNotSame(original, copy.val(), "Copy should be a different Put instance");
        assertArrayEquals(Bytes.toBytes("orig"), copy.getRow());
        assertTrue(copy.has("cf", "q"));
    }

    // ---------------------------------------------------------------------
    // create(entity) factory methods
    // ---------------------------------------------------------------------

    @Test
    public void testCreate_entity_defaultNamingPolicy() {
        SimpleUser user = new SimpleUser("user-1", "Alice", "alice@x");
        AnyPut put = AnyPut.create(user);

        assertNotNull(put);
        assertArrayEquals(Bytes.toBytes("user-1"), put.getRow());
        assertTrue(put.has("info", "name"));
        assertTrue(put.has("info", "email"));
    }

    @Test
    public void testCreate_entity_withNamingPolicy() {
        SimpleUser user = new SimpleUser("user-2", "Bob", "bob@x");
        AnyPut put = AnyPut.create(user, NamingPolicy.SNAKE_CASE);

        assertNotNull(put);
        assertArrayEquals(Bytes.toBytes("user-2"), put.getRow());
    }

    @Test
    public void testCreate_entity_nullEntity_throws() {
        assertThrows(IllegalArgumentException.class, () -> AnyPut.create(null));
    }

    @Test
    public void testCreate_entity_nullNamingPolicy_throws() {
        SimpleUser user = new SimpleUser("u", "n", "e");
        assertThrows(IllegalArgumentException.class, () -> AnyPut.create(user, (NamingPolicy) null));
    }

    @Test
    public void testCreate_collectionOfEntities() {
        List<SimpleUser> users = Arrays.asList(new SimpleUser("u1", "A", "a@x"), new SimpleUser("u2", "B", "b@x"));
        List<AnyPut> puts = AnyPut.create(users);

        assertEquals(2, puts.size());
        assertArrayEquals(Bytes.toBytes("u1"), puts.get(0).getRow());
        assertArrayEquals(Bytes.toBytes("u2"), puts.get(1).getRow());
    }

    @Test
    public void testCreate_collectionOfEntities_passesThroughAnyPut() {
        AnyPut preBuilt = AnyPut.of("pre").addColumn("cf", "q", "v");
        List<Object> mixed = Arrays.asList(new SimpleUser("u1", "A", "a@x"), preBuilt);

        List<AnyPut> puts = AnyPut.create(mixed);

        assertEquals(2, puts.size());
        assertSame(preBuilt, puts.get(1), "AnyPut instances must be passed through unchanged");
    }

    @Test
    public void testCreate_collectionWithNamingPolicy() {
        List<SimpleUser> users = Arrays.asList(new SimpleUser("u1", "A", "a@x"));
        List<AnyPut> puts = AnyPut.create(users, NamingPolicy.SNAKE_CASE);
        assertEquals(1, puts.size());
    }

    @Test
    public void testCreate_collection_nullCollection_throws() {
        assertThrows(IllegalArgumentException.class, () -> AnyPut.create((java.util.Collection<?>) null));
    }

    @Test
    public void testCreate_entity_withSelectPropNames() {
        SimpleUser user = new SimpleUser("u1", "Alice", "alice@x");
        AnyPut put = AnyPut.create(user, Arrays.asList("name"));

        assertNotNull(put);
        assertArrayEquals(Bytes.toBytes("u1"), put.getRow());
        assertTrue(put.has("info", "name"));
        assertFalse(put.has("info", "email"), "email should not be present when only 'name' is selected");
    }

    @Test
    public void testCreate_entity_withSelectPropNames_nullSelect_includesAll() {
        SimpleUser user = new SimpleUser("u1", "Alice", "alice@x");
        AnyPut put = AnyPut.create(user, (java.util.Collection<String>) null);

        assertNotNull(put);
        assertTrue(put.has("info", "name"));
        assertTrue(put.has("info", "email"));
    }

    @Test
    public void testCreate_entity_withSelectPropNames_andNamingPolicy() {
        SimpleUser user = new SimpleUser("u1", "Alice", "alice@x");
        AnyPut put = AnyPut.create(user, Arrays.asList("name"), NamingPolicy.CAMEL_CASE);
        assertNotNull(put);
        assertArrayEquals(Bytes.toBytes("u1"), put.getRow());
    }

    @Test
    public void testCreate_collection_withSelectPropNames() {
        List<SimpleUser> users = Arrays.asList(new SimpleUser("u1", "A", "a@x"), new SimpleUser("u2", "B", "b@x"));
        List<AnyPut> puts = AnyPut.create(users, Arrays.asList("name"));
        assertEquals(2, puts.size());
        assertTrue(puts.get(0).has("info", "name"));
        assertFalse(puts.get(0).has("info", "email"));
    }

    @Test
    public void testCreate_collection_withSelectPropNames_andNamingPolicy() {
        List<SimpleUser> users = Arrays.asList(new SimpleUser("u1", "A", "a@x"));
        List<AnyPut> puts = AnyPut.create(users, Arrays.asList("name"), NamingPolicy.CAMEL_CASE);
        assertEquals(1, puts.size());
    }

    @Test
    public void testCreate_entity_skipsNullPropertyValues() {
        SimpleUser user = new SimpleUser("u1", "Alice", null);
        AnyPut put = AnyPut.create(user);

        assertTrue(put.has("info", "name"));
        assertFalse(put.has("info", "email"), "Null property values must not be added");
    }

    // ---------------------------------------------------------------------
    // val() and underlying Put exposure
    // ---------------------------------------------------------------------

    @Test
    public void testVal_returnsUnderlyingPut() {
        AnyPut put = AnyPut.of("row");
        Put underlying = put.val();
        assertNotNull(underlying);
        assertArrayEquals(Bytes.toBytes("row"), underlying.getRow());
    }

    @Test
    public void testVal_consistentAcrossCalls() {
        AnyPut put = AnyPut.of("row");
        assertSame(put.val(), put.val());
    }

    // ---------------------------------------------------------------------
    // addColumn variants
    // ---------------------------------------------------------------------

    @Test
    public void testAddColumn_stringFamilyQualifier_returnsSelf() {
        AnyPut put = AnyPut.of("row");
        AnyPut returned = put.addColumn("cf", "q", "hello");
        assertSame(put, returned, "addColumn must return this for chaining");
        assertTrue(put.has("cf", "q"));
        assertTrue(put.has("cf", "q", "hello"));
    }

    @Test
    public void testAddColumn_stringFamilyQualifierWithTimestamp() {
        long ts = 42L;
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", ts, "v");
        assertTrue(put.has("cf", "q", ts));
        assertTrue(put.has("cf", "q", ts, "v"));
    }

    @Test
    public void testAddColumn_byteArrayFamilyQualifier() {
        byte[] fam = Bytes.toBytes("cf");
        byte[] qual = Bytes.toBytes("q");
        byte[] val = Bytes.toBytes("v");
        AnyPut put = AnyPut.of("row").addColumn(fam, qual, val);
        assertTrue(put.has(fam, qual));
        assertTrue(put.has(fam, qual, val));
    }

    @Test
    public void testAddColumn_byteArrayWithTimestamp() {
        byte[] fam = Bytes.toBytes("cf");
        byte[] qual = Bytes.toBytes("q");
        byte[] val = Bytes.toBytes("v");
        long ts = 100L;
        AnyPut put = AnyPut.of("row").addColumn(fam, qual, ts, val);
        assertTrue(put.has(fam, qual, ts));
        assertTrue(put.has(fam, qual, ts, val));
    }

    @Test
    public void testAddColumn_byteBufferQualifierAndValue() {
        byte[] fam = Bytes.toBytes("cf");
        ByteBuffer qual = ByteBuffer.wrap(Bytes.toBytes("q"));
        ByteBuffer val = ByteBuffer.wrap(Bytes.toBytes("v"));
        long ts = 7L;
        AnyPut put = AnyPut.of("row").addColumn(fam, qual, ts, val);
        assertTrue(put.has(Bytes.toBytes("cf"), Bytes.toBytes("q"), ts));
    }

    @Test
    public void testAddColumn_chaining() {
        AnyPut put = AnyPut.of("row").addColumn("cf1", "q1", "v1").addColumn("cf1", "q2", "v2").addColumn("cf2", "q3", "v3");
        assertEquals(3, put.size());
        assertEquals(2, put.numFamilies());
    }

    @Test
    public void testAddColumn_nullValue_doesNotThrow() {
        assertDoesNotThrow(() -> AnyPut.of("row").addColumn("cf", "q", (Object) null));
    }

    @Test
    public void testAddColumn_nonStringValue_convertedToBytes() {
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", 12345);
        // The presence of the cell is observable via has(family, qualifier).
        assertTrue(put.has("cf", "q"));
        // The stored cell value must be the string representation of 12345.
        List<Cell> cells = put.get("cf", "q");
        assertEquals(1, cells.size());
        Cell c = cells.get(0);
        assertEquals("12345", new String(c.getValueArray(), c.getValueOffset(), c.getValueLength()));
    }

    // ---------------------------------------------------------------------
    // add(Cell)
    // ---------------------------------------------------------------------

    @Test
    public void testAdd_cell() throws Exception {
        byte[] row = Bytes.toBytes("row");
        Cell cell = new KeyValue(row, Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("v"));
        AnyPut put = AnyPut.of("row");
        AnyPut returned = put.add(cell);
        assertSame(put, returned);
        assertTrue(put.has("cf", "q"));
    }

    // ---------------------------------------------------------------------
    // AnyMutation accessors (get, has, family map, etc.)
    // ---------------------------------------------------------------------

    @Test
    public void testGet_returnsCellsForFamilyQualifier() {
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", "v");
        List<Cell> cells = put.get("cf", "q");
        assertEquals(1, cells.size());
    }

    @Test
    public void testGet_byteArrayVariant() {
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", "v");
        List<Cell> cells = put.get(Bytes.toBytes("cf"), Bytes.toBytes("q"));
        assertEquals(1, cells.size());
    }

    @Test
    public void testHas_missingColumn_returnsFalse() {
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", "v");
        assertFalse(put.has("cf", "missing"));
        assertFalse(put.has(Bytes.toBytes("cf"), Bytes.toBytes("missing")));
    }

    @Test
    public void testGetFamilyCellMap() {
        AnyPut put = AnyPut.of("row").addColumn("cf1", "q1", "v1").addColumn("cf2", "q2", "v2");
        NavigableMap<byte[], List<Cell>> map = put.getFamilyCellMap();
        assertNotNull(map);
        assertEquals(2, map.size());
    }

    @Test
    public void testGetFamilyCellMap_emptyPut() {
        AnyPut put = AnyPut.of("row");
        NavigableMap<byte[], List<Cell>> map = put.getFamilyCellMap();
        assertNotNull(map);
        assertEquals(0, map.size());
    }

    @Test
    public void testGetFingerprint() {
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", "v");
        assertNotNull(put.getFingerprint());
    }

    @Test
    public void testCellScanner_iteratesCells() throws Exception {
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", "v");
        int count = 0;
        org.apache.hadoop.hbase.CellScanner scanner = put.cellScanner();
        while (scanner.advance()) {
            count++;
        }
        assertEquals(1, count);
    }

    @Test
    public void testIsEmpty_emptyPut() {
        AnyPut put = AnyPut.of("row");
        assertTrue(put.isEmpty());
        assertEquals(0, put.size());
        assertEquals(0, put.numFamilies());
    }

    @Test
    public void testIsEmpty_nonEmptyPut() {
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", "v");
        assertFalse(put.isEmpty());
    }

    @Test
    public void testSize_andNumFamilies() {
        AnyPut put = AnyPut.of("row").addColumn("cf1", "q1", "v1").addColumn("cf1", "q2", "v2").addColumn("cf2", "q3", "v3");
        assertEquals(3, put.size());
        assertEquals(2, put.numFamilies());
    }

    @Test
    public void testHeapSize_returnsPositive() {
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", "v");
        assertTrue(put.heapSize() > 0L);
    }

    @Test
    public void testGet_missingFamilyQualifier_returnsEmpty() {
        AnyPut put = AnyPut.of("row");
        List<Cell> cells = put.get("missing-cf", "missing-q");
        assertNotNull(cells);
        assertEquals(0, cells.size());
    }

    @Test
    public void testHas_byteArrayVariant_withValue() {
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", "v");
        byte[] fam = Bytes.toBytes("cf");
        byte[] qual = Bytes.toBytes("q");
        byte[] val = Bytes.toBytes("v");
        assertTrue(put.has(fam, qual, val));
    }

    // ---------------------------------------------------------------------
    // Durability / TTL / Timestamp setters
    // ---------------------------------------------------------------------

    @Test
    public void testSetDurability_returnsSelfAndPersists() {
        AnyPut put = AnyPut.of("row");
        AnyPut returned = put.setDurability(Durability.SKIP_WAL);
        assertSame(put, returned);
        assertEquals(Durability.SKIP_WAL, put.getDurability());
    }

    @Test
    public void testSetTimestamp_returnsSelfAndPersists() {
        AnyPut put = AnyPut.of("row");
        long ts = 12345L;
        AnyPut returned = put.setTimestamp(ts);
        assertSame(put, returned);
        assertEquals(ts, put.getTimestamp());
    }

    @Test
    public void testSetTTL_returnsSelfAndPersists() {
        AnyPut put = AnyPut.of("row");
        long ttl = 86_400_000L;
        AnyPut returned = put.setTTL(ttl);
        assertSame(put, returned);
        assertEquals(ttl, put.getTTL());
    }

    @Test
    public void testGetClusterIds_initiallyNullOrEmpty() {
        AnyPut put = AnyPut.of("row");
        List<java.util.UUID> ids = put.getClusterIds();
        if (ids != null) {
            assertEquals(0, ids.size());
        }
    }

    @Test
    public void testSetClusterIds_returnsSelf() {
        AnyPut put = AnyPut.of("row");
        AnyPut returned = put.setClusterIds(Arrays.asList(java.util.UUID.randomUUID()));
        assertSame(put, returned);
        assertEquals(1, put.getClusterIds().size());
    }

    // ---------------------------------------------------------------------
    // equals, hashCode, toString
    // ---------------------------------------------------------------------

    @Test
    public void testEquals_sameInstance() {
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", "v");
        assertEquals(put, put);
    }

    @Test
    public void testEquals_matchesUnderlyingPutEquals() {
        // AnyPut.equals delegates to Put.equals: two AnyPut wrappers are equal
        // iff their underlying Put objects are equal.
        AnyPut p1 = AnyPut.of("row");
        AnyPut p2 = AnyPut.of("row");
        assertEquals(p1.val().equals(p2.val()), p1.equals(p2));
    }

    @Test
    public void testEquals_differentRow_areNotEqual() {
        // Two Puts with different row keys are never equal.
        AnyPut p1 = AnyPut.of("row-a");
        AnyPut p2 = AnyPut.of("row-b");
        assertFalse(p1.equals(p2));
    }

    @Test
    public void testEquals_nonAnyPutObject_returnsFalse() {
        AnyPut put = AnyPut.of("row");
        assertFalse(put.equals("not an AnyPut"));
        assertFalse(put.equals(null));
    }

    @Test
    public void testHashCode_matchesUnderlyingPut() {
        AnyPut put = AnyPut.of("row");
        assertEquals(put.val().hashCode(), put.hashCode());
    }

    @Test
    public void testToString_notNull() {
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", "v");
        String s = put.toString();
        assertNotNull(s);
    }

    // ---------------------------------------------------------------------
    // toPut(Collection) static methods
    // ---------------------------------------------------------------------

    @Test
    public void testToPut_collection() {
        SimpleUser user = new SimpleUser("u1", "Alice", "alice@x");
        AnyPut anyPut = AnyPut.of("p1").addColumn("cf", "q", "v");
        List<Object> mixed = Arrays.asList(user, anyPut);

        List<Put> puts = AnyPut.toPut(mixed);

        assertEquals(2, puts.size());
        assertArrayEquals(Bytes.toBytes("u1"), puts.get(0).getRow());
        assertSame(anyPut.val(), puts.get(1), "AnyPut.val() must be returned directly");
    }

    @Test
    public void testToPut_collection_withNamingPolicy() {
        SimpleUser user = new SimpleUser("u1", "Alice", "alice@x");
        List<Put> puts = AnyPut.toPut(Arrays.asList(user), NamingPolicy.CAMEL_CASE);
        assertEquals(1, puts.size());
        assertArrayEquals(Bytes.toBytes("u1"), puts.get(0).getRow());
    }

    @Test
    public void testToPut_nullCollection_throws() {
        assertThrows(IllegalArgumentException.class, () -> AnyPut.toPut(null));
        assertThrows(IllegalArgumentException.class, () -> AnyPut.toPut(null, NamingPolicy.CAMEL_CASE));
    }

    @Test
    public void testToPut_collection_nullNamingPolicy_throws() {
        assertThrows(IllegalArgumentException.class, () -> AnyPut.toPut(Arrays.asList(new SimpleUser("u", "n", "e")), (NamingPolicy) null));
    }

    // ---------------------------------------------------------------------
    // Smoke tests
    // ---------------------------------------------------------------------

    @Test
    public void testGetRow_matchesConstructorRowKey() {
        AnyPut put = AnyPut.of("the-row-key");
        assertArrayEquals(Bytes.toBytes("the-row-key"), put.getRow());
    }

    @Test
    public void testCompareTo_sameRow_returnsZero() {
        AnyPut a = AnyPut.of("k");
        AnyPut b = AnyPut.of("k");
        assertEquals(0, a.val().compareTo(b.val()));
    }

    @Test
    public void testCreate_userWithFieldLevelColumnFamily() {
        UserWithCustomColumn user = new UserWithCustomColumn("u1", "Alice");
        AnyPut put = AnyPut.create(user);

        assertNotNull(put);
        assertArrayEquals(Bytes.toBytes("u1"), put.getRow());
        assertTrue(put.has("profile", "user_name"));
    }

    @Test
    public void testCreate_emptyCollection_returnsEmptyList() {
        List<AnyPut> puts = AnyPut.create(java.util.Collections.emptyList());
        assertEquals(0, puts.size());
    }

    @Test
    public void testToPut_emptyCollection_returnsEmptyList() {
        List<Put> puts = AnyPut.toPut(java.util.Collections.emptyList());
        assertEquals(0, puts.size());
    }

    @Test
    public void testOf_nullValue_inAddColumn() {
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", (Object) null);
        assertEquals(1, put.size());
    }

    @Test
    public void testOf_copyExistingPut_isIndependent() {
        Put orig = new Put(Bytes.toBytes("k"));
        orig.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("v1"));
        AnyPut copy = AnyPut.of(orig);

        copy.addColumn("cf", "q2", "v2");
        assertTrue(copy.has("cf", "q2"));
        assertFalse(orig.has(Bytes.toBytes("cf"), Bytes.toBytes("q2")));
    }

    @Test
    public void testGetACL_initiallyNull() {
        AnyPut put = AnyPut.of("row");
        assertNull(put.getACL());
    }
}
