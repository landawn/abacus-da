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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.codec.KeyValueCodec;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.MutationType;
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

    // Construction and configuration

    @Test
    public void testCreate_collection_withSelectPropNames_andNamingPolicy() {
        List<SimpleUser> users = Arrays.asList(new SimpleUser("u1", "A", "a@x"));
        List<AnyPut> puts = AnyPut.create(users, Arrays.asList("name"), NamingPolicy.CAMEL_CASE);
        assertEquals(1, puts.size());
    }

    @Test
    public void testCreate_collectionWithNamingPolicy() {
        List<SimpleUser> users = Arrays.asList(new SimpleUser("u1", "A", "a@x"));
        List<AnyPut> puts = AnyPut.create(users, NamingPolicy.SNAKE_CASE);
        assertEquals(1, puts.size());
    }

    // create(entity) factory methods

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
    public void testCreate_entity_nullNamingPolicy_throws() {
        SimpleUser user = new SimpleUser("u", "n", "e");
        assertThrows(IllegalArgumentException.class, () -> AnyPut.create(user, (NamingPolicy) null));
    }

    @Test
    public void testCreate_entity_withNamingPolicy() {
        SimpleUser user = new SimpleUser("user-2", "Bob", "bob@x");
        AnyPut put = AnyPut.create(user, NamingPolicy.SNAKE_CASE);

        assertNotNull(put);
        assertArrayEquals(Bytes.toBytes("user-2"), put.getRow());
    }

    @Test
    public void testCreate_entity_withSelectPropNames_andNamingPolicy() {
        SimpleUser user = new SimpleUser("u1", "Alice", "alice@x");
        AnyPut put = AnyPut.create(user, Arrays.asList("name"), NamingPolicy.CAMEL_CASE);
        assertNotNull(put);
        assertArrayEquals(Bytes.toBytes("u1"), put.getRow());
    }

    // Smoke tests

    @Test
    public void testGetRow_matchesConstructorRowKey() {
        AnyPut put = AnyPut.of("the-row-key");
        assertArrayEquals(Bytes.toBytes("the-row-key"), put.getRow());
    }

    @Test
    public void testToPut_collection_nullNamingPolicy_throws() {
        assertThrows(IllegalArgumentException.class, () -> AnyPut.toPut(Arrays.asList(new SimpleUser("u", "n", "e")), (NamingPolicy) null));
    }

    @Test
    public void testToPut_collection_withNamingPolicy() {
        SimpleUser user = new SimpleUser("u1", "Alice", "alice@x");
        List<Put> puts = AnyPut.toPut(Arrays.asList(user), NamingPolicy.CAMEL_CASE);
        assertEquals(1, puts.size());
        assertArrayEquals(Bytes.toBytes("u1"), puts.get(0).getRow());
    }

    // Attribute and key conversion

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

    // Row and entity mapping

    @Test
    public void testCreate_emptyColumnFamilyAnnotationUsesDefaultMapping() {
        UserWithDefaultColumnFamilyAnnotation user = new UserWithDefaultColumnFamilyAnnotation("u1", "Alice");
        AnyPut put = AnyPut.create(user);

        assertNotNull(put);
        assertArrayEquals(Bytes.toBytes("u1"), put.getRow());
        assertTrue(put.has("name", HBaseExecutor.EMPTY_QUALIFIER));
        assertFalse(put.has("name", "name"));
    }

    // Factories and insert operations

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
        assertEquals(qual.limit(), qual.position());
        assertEquals(val.limit(), val.position());
    }

    @Test
    public void addColumnPreservesFluentIdentityAndCellContents() {
        AnyPut put = AnyPut.of("row");
        assertSame(put, put.addColumn("cf1", "q1", "v1").addColumn("cf1", "q2", "v2").addColumn("cf2", "q3", "v3"));

        assertEquals(3, put.size());
        assertEquals(2, put.numFamilies());
        assertArrayEquals(Bytes.toBytes("v1"), org.apache.hadoop.hbase.CellUtil.cloneValue(put.get("cf1", "q1").get(0)));
        assertArrayEquals(Bytes.toBytes("v2"), org.apache.hadoop.hbase.CellUtil.cloneValue(put.get("cf1", "q2").get(0)));
        assertArrayEquals(Bytes.toBytes("v3"), org.apache.hadoop.hbase.CellUtil.cloneValue(put.get("cf2", "q3").get(0)));
    }

    @Test
    public void testAddColumn_duplicateCoordinateQueuesBothCells() {
        AnyPut put = AnyPut.of("row", 100L).addColumn("cf", "q", "first").addColumn("cf", "q", "second");

        assertEquals(2, put.size());
        assertEquals(2, put.get("cf", "q").size());
    }

    @Test
    public void testAddColumn_nullValue_doesNotThrow() {
        assertDoesNotThrow(() -> AnyPut.of("row").addColumn("cf", "q", (Object) null));
    }

    // addColumn variants

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
    public void testCreate_collection_nullCollection_throws() {
        assertThrows(IllegalArgumentException.class, () -> AnyPut.create((java.util.Collection<?>) null));
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
    public void testCreate_emptyCollection_returnsEmptyList() {
        List<AnyPut> puts = AnyPut.create(java.util.Collections.emptyList());
        assertEquals(0, puts.size());
    }

    @Test
    public void testCreate_entity_nullEntity_throws() {
        assertThrows(IllegalArgumentException.class, () -> AnyPut.create(null));
    }

    /**
     * Regression: an entity whose registered row-key property holds a {@code null} VALUE must be
     * rejected with a clear {@link IllegalArgumentException} (previously a bare
     * {@code NullPointerException} escaped from inside HBase's {@code Put} constructor).
     */
    @Test
    public void testCreate_entity_nullRowKeyValue_throwsIAE() {
        SimpleUser user = new SimpleUser(null, "Alice", "alice@x");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> AnyPut.create(user));
        assertTrue(ex.getMessage() != null && ex.getMessage().contains("Row key"), "Exception message must mention the row key, but was: " + ex.getMessage());
    }

    @Test
    public void testCreate_entity_skipsNullPropertyValues() {
        SimpleUser user = new SimpleUser("u1", "Alice", null);
        AnyPut put = AnyPut.create(user);

        assertTrue(put.has("info", "name"));
        assertFalse(put.has("info", "email"), "Null property values must not be added");
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
    public void testCreate_nestedBeanWithPublicFields_writesNestedCells() {
        final PublicFieldAddress address = new PublicFieldAddress();
        address.street = "Main St";
        address.city = "Springfield";

        final EntityWithPublicFieldNestedBean entity = new EntityWithPublicFieldNestedBean();
        entity.setId("r2");
        entity.setAddress(address);

        // Used to write no cells at all: the nested loop only walked getter methods, silently dropping public-field properties.
        final AnyPut put = AnyPut.create(entity);

        assertEquals(2, put.size());
        assertTrue(put.has("address", "street", "Main St"));
        assertTrue(put.has("address", "city", "Springfield"));

        final EntityWithPublicFieldNestedBean back = HBaseExecutor.toEntity(toResult(put.val()), EntityWithPublicFieldNestedBean.class);
        assertNotNull(back.getAddress());
        assertEquals("Main St", back.getAddress().street);
        assertEquals("Springfield", back.getAddress().city);
    }

    @Test
    public void testCreate_topLevelPublicFieldProperty_writtenAndReadBack() {
        final EntityWithPublicFieldProperty entity = new EntityWithPublicFieldProperty();
        entity.setId("r1");
        entity.nickname = "nick";

        // Used to throw NullPointerException (propInfo.getMethod is null for a public-field property).
        final AnyPut put = AnyPut.create(entity);

        assertTrue(put.has("nickname", HBaseExecutor.EMPTY_QUALIFIER, "nick"));

        final EntityWithPublicFieldProperty back = HBaseExecutor.toEntity(toResult(put.val()), EntityWithPublicFieldProperty.class);
        assertEquals("r1", back.getId());
        assertEquals("nick", back.nickname);
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
    public void testOf_nullValue_inAddColumn() {
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", (Object) null);
        assertEquals(1, put.size());
    }

    // Updates and deletes

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
    public void testSetClusterIds_returnsSelf() {
        AnyPut put = AnyPut.of("row");
        AnyPut returned = put.setClusterIds(Arrays.asList(java.util.UUID.randomUUID()));
        assertSame(put, returned);
        assertEquals(1, put.getClusterIds().size());
    }

    // Durability / TTL / Timestamp setters

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

    // Queries and results

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
    public void testGet_missingFamilyQualifier_returnsEmpty() {
        AnyPut put = AnyPut.of("row");
        List<Cell> cells = put.get("missing-cf", "missing-q");
        assertNotNull(cells);
        assertEquals(0, cells.size());
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
    public void testGetFamilyCellMap() {
        AnyPut put = AnyPut.of("row").addColumn("cf1", "q1", "v1").addColumn("cf2", "q2", "v2");
        NavigableMap<byte[], List<Cell>> map = put.getFamilyCellMap();
        assertNotNull(map);
        assertEquals(2, map.size());
    }

    @Test
    public void testGetFingerprint() {
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", "v");
        assertNotNull(put.getFingerprint());
    }

    @Test
    public void testToPut_emptyCollection_returnsEmptyList() {
        List<Put> puts = AnyPut.toPut(java.util.Collections.emptyList());
        assertEquals(0, puts.size());
    }

    // Additional contracts

    // add(Cell)

    @Test
    public void testAdd_cell() throws Exception {
        byte[] row = Bytes.toBytes("row");
        Cell cell = new KeyValue(row, Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("v"));
        AnyPut put = AnyPut.of("row");
        AnyPut returned = put.add(cell);
        assertSame(put, returned);
        assertTrue(put.has("cf", "q"));
    }

    /**
     * {@code AnyPut.add(Cell)} validates the cell up front, so a {@code null} cell is rejected with an
     * {@code IllegalArgumentException} -- the same contract as {@code AnyDelete}/{@code AnyIncrement}/
     * {@code AnyAppend.add(Cell)}.
     */
    @Test
    public void testAdd_nullCell_throwsIae() {
        AnyPut put = AnyPut.of("row");
        assertThrows(IllegalArgumentException.class, () -> put.add((Cell) null));
        assertEquals(0, put.size());
    }

    @Test
    public void testCompareTo_sameRow_returnsZero() {
        AnyPut a = AnyPut.of("k");
        AnyPut b = AnyPut.of("k");
        assertEquals(0, a.val().compareTo(b.val()));
    }

    @Test
    public void testEquals_differentRow_areNotEqual() {
        // Two Puts with different row keys are never equal.
        AnyPut p1 = AnyPut.of("row-a");
        AnyPut p2 = AnyPut.of("row-b");
        assertFalse(p1.equals(p2));
    }

    @Test
    public void testEquals_matchesUnderlyingPutEquals() {
        // AnyPut.equals delegates to Put.equals: two AnyPut wrappers are equal
        // if their underlying Put objects are equal.
        AnyPut p1 = AnyPut.of("row");
        AnyPut p2 = AnyPut.of("row");
        assertEquals(p1.val().equals(p2.val()), p1.equals(p2));
    }

    @Test
    public void testEquals_nonAnyPutObject_returnsFalse() {
        AnyPut put = AnyPut.of("row");
        assertFalse(put.equals("not an AnyPut"));
        assertFalse(put.equals(null));
    }

    // equals, hashCode, toString

    @Test
    public void testEquals_sameInstance() {
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", "v");
        assertEquals(put, put);
    }

    @Test
    public void testHas_byteArrayVariant_withValue() {
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", "v");
        byte[] fam = Bytes.toBytes("cf");
        byte[] qual = Bytes.toBytes("q");
        byte[] val = Bytes.toBytes("v");
        assertTrue(put.has(fam, qual, val));
    }

    // AnyMutation accessors (get, has, family map, etc.)

    @Test
    public void testHas_missingColumn_returnsFalse() {
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", "v");
        assertFalse(put.has("cf", "missing"));
        assertFalse(put.has(Bytes.toBytes("cf"), Bytes.toBytes("missing")));
    }

    @Test
    public void testHashCode_matchesUnderlyingPut() {
        AnyPut put = AnyPut.of("row");
        assertEquals(put.val().hashCode(), put.hashCode());
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
    public void testOf_byteArrayRowKey() {
        byte[] row = Bytes.toBytes("row-bytes");
        AnyPut put = AnyPut.of(row);
        assertArrayEquals(row, put.getRow());
    }

    @Test
    public void testOf_byteBufferRowKey() {
        ByteBuffer rowBuf = ByteBuffer.wrap(Bytes.toBytes("row-bb"));
        AnyPut put = AnyPut.of(rowBuf);
        assertArrayEquals(Bytes.toBytes("row-bb"), put.getRow());
        assertEquals(rowBuf.limit(), rowBuf.position());
    }

    @Test
    public void testOf_byteBufferRowKeyWithTimestamp() {
        ByteBuffer rowBuf = ByteBuffer.wrap(Bytes.toBytes("row-bb-ts"));
        long ts = 555L;
        AnyPut put = AnyPut.of(rowBuf, ts);
        assertArrayEquals(Bytes.toBytes("row-bb-ts"), put.getRow());
        assertEquals(ts, put.getTimestamp());
        assertEquals(rowBuf.limit(), rowBuf.position());
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

    @Test
    public void testOf_copyExistingPut_copiesAttributesAndPriorityButNotDurability() {
        Put orig = new Put(Bytes.toBytes("k"));
        orig.setAttribute("trace", Bytes.toBytes("t1"));
        orig.setTTL(1234L);
        orig.setPriority(7);
        orig.setDurability(Durability.SKIP_WAL);

        AnyPut copy = AnyPut.of(orig);

        assertArrayEquals(Bytes.toBytes("t1"), copy.getAttribute("trace"));
        assertEquals(1234L, copy.getTTL());
        assertEquals(7, copy.getPriority());
        // Documented: HBase's Put(Put) copy constructor resets durability to USE_DEFAULT.
        assertEquals(Durability.USE_DEFAULT, copy.getDurability());

        // The copied attribute map is independent of the source.
        copy.setAttribute("trace", (byte[]) null);
        assertArrayEquals(Bytes.toBytes("t1"), orig.getAttribute("trace"));
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

    /**
     * A {@code null} row key or source {@code Put} is rejected with an {@code IllegalArgumentException}
     * by every {@code of(...)} overload (previously some overloads leaked HBase's
     * {@code NullPointerException}).
     */
    @Test
    public void testOf_nullArguments_throwIae() {
        assertThrows(IllegalArgumentException.class, () -> AnyPut.of((Object) null));
        assertThrows(IllegalArgumentException.class, () -> AnyPut.of((Object) null, 1L));
        assertThrows(IllegalArgumentException.class, () -> AnyPut.of((Object) null, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> AnyPut.of((Object) null, 0, 1, 1L));
        assertThrows(IllegalArgumentException.class, () -> AnyPut.of((Object) null, true));
        assertThrows(IllegalArgumentException.class, () -> AnyPut.of((Object) null, 1L, true));
        assertThrows(IllegalArgumentException.class, () -> AnyPut.of((java.nio.ByteBuffer) null));
        assertThrows(IllegalArgumentException.class, () -> AnyPut.of((java.nio.ByteBuffer) null, 1L));
        assertThrows(IllegalArgumentException.class, () -> AnyPut.of((org.apache.hadoop.hbase.client.Put) null));
    }

    // Factory methods: of(Object) and variants

    @Test
    public void testOf_stringRowKey() {
        AnyPut put = AnyPut.of("row-1");
        assertNotNull(put);
        assertArrayEquals(Bytes.toBytes("row-1"), put.getRow());
    }

    @Test
    public void testOf_withRowIsImmutable() {
        AnyPut put = AnyPut.of("row-imm", true);
        assertArrayEquals(Bytes.toBytes("row-imm"), put.getRow());
    }

    @Test
    public void testOf_withTimestamp() {
        long ts = 1_700_000_000_000L;
        AnyPut put = AnyPut.of("row-ts", ts);
        assertEquals(ts, put.getTimestamp());
        assertArrayEquals(Bytes.toBytes("row-ts"), put.getRow());
    }

    @Test
    public void testOf_withTimestampAndRowIsImmutable() {
        long ts = 99999L;
        AnyPut put = AnyPut.of("row-imm-ts", ts, true);
        assertArrayEquals(Bytes.toBytes("row-imm-ts"), put.getRow());
        assertEquals(ts, put.getTimestamp());
    }

    // toPut(Collection) static methods

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
    public void testToPut_nullCollection_throws() {
        assertThrows(IllegalArgumentException.class, () -> AnyPut.toPut(null));
        assertThrows(IllegalArgumentException.class, () -> AnyPut.toPut(null, NamingPolicy.CAMEL_CASE));
    }

    @Test
    public void testToString_notNull() {
        AnyPut put = AnyPut.of("row").addColumn("cf", "q", "v");
        String s = put.toString();
        assertNotNull(s);
    }

    @Test
    public void testVal_consistentAcrossCalls() {
        AnyPut put = AnyPut.of("row");
        assertSame(put.val(), put.val());
    }

    // val() and underlying Put exposure

    @Test
    public void testVal_returnsUnderlyingPut() {
        AnyPut put = AnyPut.of("row");
        Put underlying = put.val();
        assertNotNull(underlying);
        assertArrayEquals(Bytes.toBytes("row"), underlying.getRow());
    }

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

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @ColumnFamily
    public static class UserWithDefaultColumnFamilyAnnotation {
        @Id
        private String id;
        private String name;
    }

    // ---------------------------------------------------------------------
    // Review 2026-09-22 (slice L): public-field bean properties
    // ---------------------------------------------------------------------

    /** Bean whose properties are public fields without getters/setters. */
    public static class PublicFieldAddress {
        public String street;
        public String city;
    }

    /** Top-level public-field property next to regular getter/setter properties. */
    public static class EntityWithPublicFieldProperty {
        @Id
        private String id;
        public String nickname;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }
    }

    /** Nested bean property whose own properties are public fields. */
    public static class EntityWithPublicFieldNestedBean {
        @Id
        private String id;
        private PublicFieldAddress address;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public PublicFieldAddress getAddress() {
            return address;
        }

        public void setAddress(final PublicFieldAddress address) {
            this.address = address;
        }
    }

    private static org.apache.hadoop.hbase.client.Result toResult(final Put put) {
        final List<Cell> cells = new java.util.ArrayList<>();
        put.getFamilyCellMap().values().forEach(cells::addAll);
        cells.sort(org.apache.hadoop.hbase.CellComparator.getInstance());
        return org.apache.hadoop.hbase.client.Result.create(cells);
    }

    // ---- 2026-10-20 sliceF ----

    /**
     * Pins the addColumn {@code @param family} doc for a single put: a null family is not validated here, and the put
     * fails with NullPointerException while HBase builds the request (ProtobufUtil.toMutation(PUT, put)).
     */
    @Test
    public void testAddColumn_nullFamily_singlePutFailsWhenRequestIsBuilt_sliceF() throws Exception {
        final AnyPut stringOverload = AnyPut.of("row").addColumn((String) null, "q", "v");
        final AnyPut bytesOverload = AnyPut.of("row").addColumn((byte[]) null, Bytes.toBytes("q"), Bytes.toBytes("v"));
        assertEquals(1, stringOverload.size());
        assertEquals(1, bytesOverload.size());

        assertThrows(NullPointerException.class, () -> ProtobufUtil.toMutation(MutationType.PUT, stringOverload.val()));
        assertThrows(NullPointerException.class, () -> ProtobufUtil.toMutation(MutationType.PUT, bytesOverload.val()));

        // a real family builds: one column with one cell
        final MutationProto proto = ProtobufUtil.toMutation(MutationType.PUT, AnyPut.of("row").addColumn("cf", "q", "v").val());
        assertEquals(1, proto.getColumnValueCount());
        assertArrayEquals(Bytes.toBytes("cf"), proto.getColumnValue(0).getFamily().toByteArray());
    }

    /**
     * Pins the addColumn {@code @param family} doc for a batch put: the request proto carries no column data (the cells travel
     * in the RPC cell block, see RequestConverter.buildNoDataRegionAction), and the client's default KeyValueCodec encodes the
     * null family as an empty one, so the server receives a cell with a zero-length family.
     */
    @Test
    public void testAddColumn_nullFamily_batchCellBlockCarriesAnEmptyFamily_sliceF() throws Exception {
        final Put put = AnyPut.of("row").addColumn((byte[]) null, Bytes.toBytes("q"), Bytes.toBytes("v")).val();

        final MutationProto proto = ProtobufUtil.toMutationNoData(MutationType.PUT, put);
        assertEquals(1, proto.getAssociatedCellCount());
        assertEquals(0, proto.getColumnValueCount());

        final Codec codec = new KeyValueCodec();
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final Codec.Encoder encoder = codec.getEncoder(out);
        final CellScanner cells = put.cellScanner();
        while (cells.advance()) {
            encoder.write(cells.current());
        }
        encoder.flush();

        final Codec.Decoder decoder = codec.getDecoder(new ByteArrayInputStream(out.toByteArray()));
        assertTrue(decoder.advance());
        final Cell decoded = decoder.current();
        assertEquals(0, decoded.getFamilyLength());
        assertArrayEquals(new byte[0], CellUtil.cloneFamily(decoded));
        assertArrayEquals(Bytes.toBytes("q"), CellUtil.cloneQualifier(decoded));
        assertArrayEquals(Bytes.toBytes("v"), CellUtil.cloneValue(decoded));
        assertArrayEquals(Bytes.toBytes("row"), CellUtil.cloneRow(decoded));
        assertFalse(decoder.advance());
    }

    /**
     * Pins the of(Put) doc: an attribute whose name is null (Put.setAttribute(null, bytes) stores it in a HashMap) fails
     * the copy with NullPointerException, from the TreeMap HBase's copy constructor collects the attributes into.
     */
    @Test
    public void testOf_copyPutWithNullAttributeName_throwsNpe_sliceF() {
        final Put source = new Put(Bytes.toBytes("k"));
        source.setAttribute(null, Bytes.toBytes("v"));
        assertTrue(source.getAttributesMap().containsKey(null));

        assertThrows(NullPointerException.class, () -> AnyPut.of(source));
    }

    // ---- 2026-10-24 sliceO ----

    /**
     * create(entity) skips a property holding an empty value wrapper (empty Optional/OptionalLong/u.Optional, u.Nullable
     * holding null, Holder of null) like a null one, at the top level and in a flattened nested bean, and an HBaseColumn
     * (alone, in a versioned List or Map) whose value is an empty wrapper writes no cell. On HEAD it threw
     * NullPointerException (N.stringOf of an empty wrapper is null). Present wrappers and a null HBaseColumn value are
     * written as before.
     */
    @Test
    public void testCreate_emptyValueWrappersAreSkippedLikeNull_sliceO() {
        final EmptyWrapperNested nested = new EmptyWrapperNested();
        nested.setCity("c");
        nested.setZip(java.util.Optional.empty());
        nested.setFloor(com.landawn.abacus.util.u.Nullable.empty());
        nested.setTag(com.landawn.abacus.util.HBaseColumn.valueOf(com.landawn.abacus.util.u.Optional.<String> empty(), 11L));
        nested.setLabel(com.landawn.abacus.util.HBaseColumn.valueOf(com.landawn.abacus.util.u.Optional.of("L"), 12L));

        final java.util.Map<Long, com.landawn.abacus.util.HBaseColumn<com.landawn.abacus.util.u.Nullable<String>>> byVersion = new java.util.LinkedHashMap<>();
        byVersion.put(8L, com.landawn.abacus.util.HBaseColumn.valueOf(com.landawn.abacus.util.u.Nullable.<String> empty(), 8L));
        byVersion.put(9L, com.landawn.abacus.util.HBaseColumn.valueOf(com.landawn.abacus.util.u.Nullable.of("v9"), 9L));

        final EmptyWrapperEntity entity = new EmptyWrapperEntity();
        entity.setId("w1");
        entity.setName("n");
        entity.setJdkOptional(java.util.Optional.empty());
        entity.setJdkOptionalLong(java.util.OptionalLong.empty());
        entity.setOptional(com.landawn.abacus.util.u.Optional.empty());
        entity.setNullable(com.landawn.abacus.util.u.Nullable.of((String) null));
        entity.setHolder(com.landawn.abacus.util.Holder.of(null));
        entity.setPresentOptional(java.util.Optional.of("x"));
        entity.setVersioned(com.landawn.abacus.util.HBaseColumn.valueOf(java.util.Optional.<String> empty(), 5L));
        entity.setVersions(Arrays.asList(com.landawn.abacus.util.HBaseColumn.valueOf(java.util.Optional.<String> empty(), 6L),
                com.landawn.abacus.util.HBaseColumn.valueOf(java.util.Optional.of("v7"), 7L)));
        entity.setByVersion(byVersion);
        entity.setNullValueColumn(com.landawn.abacus.util.HBaseColumn.valueOf(null, 10L));
        entity.setNested(nested);

        final AnyPut put = AnyPut.create(entity);

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(7, put.size(), "only the present values get a cell"),
                () -> assertTrue(put.has("cf", "name", "n")), () -> assertTrue(put.has("cf", "presentOptional", "x")),
                () -> assertFalse(put.has("cf", "jdkOptional")), () -> assertFalse(put.has("cf", "jdkOptionalLong")),
                () -> assertFalse(put.has("cf", "optional")), () -> assertFalse(put.has("cf", "nullable")), () -> assertFalse(put.has("cf", "holder")),
                () -> assertFalse(put.has("cf", "versioned")),
                () -> assertEquals(Arrays.asList(7L), put.get("cf", "versions").stream().map(Cell::getTimestamp).toList()),
                () -> assertTrue(put.has("cf", "versions", 7L, "v7")),
                () -> assertEquals(Arrays.asList(9L), put.get("cf", "byVersion").stream().map(Cell::getTimestamp).toList()),
                () -> assertTrue(put.has("cf", "byVersion", 9L, "v9")),
                // a null HBaseColumn value is still written as an empty cell
                () -> assertEquals(0, put.get("cf", "nullValueColumn").getFirst().getValueLength()),
                () -> assertEquals(10L, put.get("cf", "nullValueColumn").getFirst().getTimestamp()), () -> assertTrue(put.has("nf", "city", "c")),
                () -> assertFalse(put.has("nf", "zip")), () -> assertFalse(put.has("nf", "floor")), () -> assertFalse(put.has("nf", "tag")),
                () -> assertTrue(put.has("nf", "label", 12L, "L")));
    }

    @ColumnFamily("cf")
    public static class EmptyWrapperEntity {
        @Id
        private String id;
        private String name;
        private java.util.Optional<String> jdkOptional;
        private java.util.OptionalLong jdkOptionalLong;
        private com.landawn.abacus.util.u.Optional<String> optional;
        private com.landawn.abacus.util.u.Nullable<String> nullable;
        private com.landawn.abacus.util.Holder<String> holder;
        private java.util.Optional<String> presentOptional;
        private com.landawn.abacus.util.HBaseColumn<java.util.Optional<String>> versioned;
        private List<com.landawn.abacus.util.HBaseColumn<java.util.Optional<String>>> versions;
        private java.util.Map<Long, com.landawn.abacus.util.HBaseColumn<com.landawn.abacus.util.u.Nullable<String>>> byVersion;
        private com.landawn.abacus.util.HBaseColumn<String> nullValueColumn;
        @ColumnFamily("nf")
        private EmptyWrapperNested nested;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public java.util.Optional<String> getJdkOptional() {
            return jdkOptional;
        }

        public void setJdkOptional(final java.util.Optional<String> jdkOptional) {
            this.jdkOptional = jdkOptional;
        }

        public java.util.OptionalLong getJdkOptionalLong() {
            return jdkOptionalLong;
        }

        public void setJdkOptionalLong(final java.util.OptionalLong jdkOptionalLong) {
            this.jdkOptionalLong = jdkOptionalLong;
        }

        public com.landawn.abacus.util.u.Optional<String> getOptional() {
            return optional;
        }

        public void setOptional(final com.landawn.abacus.util.u.Optional<String> optional) {
            this.optional = optional;
        }

        public com.landawn.abacus.util.u.Nullable<String> getNullable() {
            return nullable;
        }

        public void setNullable(final com.landawn.abacus.util.u.Nullable<String> nullable) {
            this.nullable = nullable;
        }

        public com.landawn.abacus.util.Holder<String> getHolder() {
            return holder;
        }

        public void setHolder(final com.landawn.abacus.util.Holder<String> holder) {
            this.holder = holder;
        }

        public java.util.Optional<String> getPresentOptional() {
            return presentOptional;
        }

        public void setPresentOptional(final java.util.Optional<String> presentOptional) {
            this.presentOptional = presentOptional;
        }

        public com.landawn.abacus.util.HBaseColumn<java.util.Optional<String>> getVersioned() {
            return versioned;
        }

        public void setVersioned(final com.landawn.abacus.util.HBaseColumn<java.util.Optional<String>> versioned) {
            this.versioned = versioned;
        }

        public List<com.landawn.abacus.util.HBaseColumn<java.util.Optional<String>>> getVersions() {
            return versions;
        }

        public void setVersions(final List<com.landawn.abacus.util.HBaseColumn<java.util.Optional<String>>> versions) {
            this.versions = versions;
        }

        public java.util.Map<Long, com.landawn.abacus.util.HBaseColumn<com.landawn.abacus.util.u.Nullable<String>>> getByVersion() {
            return byVersion;
        }

        public void setByVersion(final java.util.Map<Long, com.landawn.abacus.util.HBaseColumn<com.landawn.abacus.util.u.Nullable<String>>> byVersion) {
            this.byVersion = byVersion;
        }

        public com.landawn.abacus.util.HBaseColumn<String> getNullValueColumn() {
            return nullValueColumn;
        }

        public void setNullValueColumn(final com.landawn.abacus.util.HBaseColumn<String> nullValueColumn) {
            this.nullValueColumn = nullValueColumn;
        }

        public EmptyWrapperNested getNested() {
            return nested;
        }

        public void setNested(final EmptyWrapperNested nested) {
            this.nested = nested;
        }
    }

    public static class EmptyWrapperNested {
        private String city;
        private java.util.Optional<String> zip;
        private com.landawn.abacus.util.u.Nullable<Integer> floor;
        private com.landawn.abacus.util.HBaseColumn<com.landawn.abacus.util.u.Optional<String>> tag;
        private com.landawn.abacus.util.HBaseColumn<com.landawn.abacus.util.u.Optional<String>> label;

        public String getCity() {
            return city;
        }

        public void setCity(final String city) {
            this.city = city;
        }

        public java.util.Optional<String> getZip() {
            return zip;
        }

        public void setZip(final java.util.Optional<String> zip) {
            this.zip = zip;
        }

        public com.landawn.abacus.util.u.Nullable<Integer> getFloor() {
            return floor;
        }

        public void setFloor(final com.landawn.abacus.util.u.Nullable<Integer> floor) {
            this.floor = floor;
        }

        public com.landawn.abacus.util.HBaseColumn<com.landawn.abacus.util.u.Optional<String>> getTag() {
            return tag;
        }

        public void setTag(final com.landawn.abacus.util.HBaseColumn<com.landawn.abacus.util.u.Optional<String>> tag) {
            this.tag = tag;
        }

        public com.landawn.abacus.util.HBaseColumn<com.landawn.abacus.util.u.Optional<String>> getLabel() {
            return label;
        }

        public void setLabel(final com.landawn.abacus.util.HBaseColumn<com.landawn.abacus.util.u.Optional<String>> label) {
            this.label = label;
        }
    }

    // ---- 2026-10-24 apply-hbase ----

    private static final String OPERATION_MESSAGE = "Pass the Put or AnyPut directly";

    /** The HBase operations and Any* wrappers that are not entities; the Put carries an operation id (its bean "row key"). */
    private static List<Object> operationsAndWrappers() {
        final byte[] row = Bytes.toBytes("r1");
        final Put putWithId = new Put(row);
        putWithId.setId("op-1");

        return Arrays.asList(new Put(row), putWithId, new org.apache.hadoop.hbase.client.Get(row), new org.apache.hadoop.hbase.client.Delete(row),
                new org.apache.hadoop.hbase.client.Increment(row), new org.apache.hadoop.hbase.client.Append(row), new org.apache.hadoop.hbase.client.Scan(),
                new org.apache.hadoop.hbase.client.RowMutations(row), AnyPut.of("r1"), AnyDelete.of("r1"), AnyGet.of("r1"), AnyScan.create(),
                AnyIncrement.of("r1"), AnyAppend.of("r1"), AnyRowMutations.of("r1"));
    }

    /**
     * Item 10a: a single-entity create(...) used to map a native Put (or any operation) and an AnyPut/Any* wrapper as a bean whose
     * row key is the operation id: "row key property value is null", or with setId("op-1") a garbage Put whose cells were the
     * operation's durability/familyCellMap/clusterIds/priority. It now rejects them with an IllegalArgumentException that says to
     * pass the Put/AnyPut directly (all four single-entity overloads).
     */
    @Test
    public void testCreate_singleEntity_rejectsHBaseOperationsAndAnyWrappers_applyHbase() {
        org.junit.jupiter.api.Assertions.assertAll(operationsAndWrappers().stream()
                .flatMap(op -> java.util.stream.Stream.<org.junit.jupiter.api.function.Executable> of( //
                        () -> assertTrue(assertThrows(IllegalArgumentException.class, () -> AnyPut.create(op)).getMessage().contains(OPERATION_MESSAGE),
                                op.getClass().getName()),
                        () -> assertTrue(assertThrows(IllegalArgumentException.class, () -> AnyPut.create(op, NamingPolicy.SNAKE_CASE)).getMessage()
                                .contains(OPERATION_MESSAGE), op.getClass().getName()),
                        () -> assertTrue(assertThrows(IllegalArgumentException.class, () -> AnyPut.create(op, Arrays.asList("id"))).getMessage()
                                .contains(OPERATION_MESSAGE), op.getClass().getName()),
                        () -> assertTrue(assertThrows(IllegalArgumentException.class,
                                () -> AnyPut.create(op, (java.util.Collection<String>) null, NamingPolicy.CAMEL_CASE)).getMessage().contains(OPERATION_MESSAGE),
                                op.getClass().getName()))));
    }

    /**
     * Item 10a: the collection factories and toPut pass a native Put element through unchanged (wrapped, not copied, like an
     * AnyPut element; no naming policy or property selection applied), keep the element order, and reject any other operation
     * or Any* wrapper element with the same message. They used to bean-map a Put element (IAE "row key ... null").
     */
    @Test
    public void testCollectionFactories_passNativePutThroughAndRejectOtherOperations_applyHbase() {
        final Put nativePut = new Put(Bytes.toBytes("p1")).addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("v"));
        nativePut.setId("op-1");
        nativePut.setDurability(Durability.SKIP_WAL);
        final AnyPut anyPut = AnyPut.of("a1").addColumn("cf", "q", "v");
        final List<Object> mixed = Arrays.asList(new SimpleUser("u1", "n", "e"), nativePut, anyPut);

        final List<java.util.function.Function<List<?>, List<AnyPut>>> factories = Arrays.asList(AnyPut::create, l -> AnyPut.create(l, NamingPolicy.SNAKE_CASE),
                l -> AnyPut.create(l, Arrays.asList("name")), l -> AnyPut.create(l, Arrays.asList("name"), NamingPolicy.SNAKE_CASE));
        final List<java.util.function.Function<List<?>, List<Put>>> toPuts = Arrays.asList(AnyPut::toPut, l -> AnyPut.toPut(l, NamingPolicy.SNAKE_CASE));

        org.junit.jupiter.api.Assertions
                .assertAll(java.util.stream.Stream.concat(factories.stream().map(factory -> (org.junit.jupiter.api.function.Executable) () -> {
                    final List<AnyPut> puts = factory.apply(mixed);

                    assertEquals(3, puts.size());
                    assertArrayEquals(Bytes.toBytes("u1"), puts.get(0).getRow());
                    assertSame(nativePut, puts.get(1).val());
                    assertEquals(Durability.SKIP_WAL, puts.get(1).getDurability());
                    assertEquals(1, puts.get(1).size());
                    assertSame(anyPut, puts.get(2));

                    for (final Object op : operationsAndWrappers()) {
                        if (!(op instanceof Put) && !(op instanceof AnyPut)) {
                            assertTrue(assertThrows(IllegalArgumentException.class, () -> factory.apply(Arrays.asList(op))).getMessage()
                                    .contains(OPERATION_MESSAGE), op.getClass().getName());
                        }
                    }
                }), toPuts.stream().map(toPut -> (org.junit.jupiter.api.function.Executable) () -> {
                    final List<Put> puts = toPut.apply(mixed);

                    assertEquals(3, puts.size());
                    assertArrayEquals(Bytes.toBytes("u1"), puts.get(0).getRow());
                    assertSame(nativePut, puts.get(1));
                    assertSame(anyPut.val(), puts.get(2));

                    for (final Object op : operationsAndWrappers()) {
                        if (!(op instanceof Put) && !(op instanceof AnyPut)) {
                            assertTrue(
                                    assertThrows(IllegalArgumentException.class, () -> toPut.apply(Arrays.asList(op))).getMessage().contains(OPERATION_MESSAGE),
                                    op.getClass().getName());
                        }
                    }
                })));
    }

    /** Item 10a guard: ordinary entities are still mapped by every factory (the rejection is only for operations). */
    @Test
    public void testCreate_entitiesStillMapped_applyHbase() {
        final SimpleUser user = new SimpleUser("u1", "n", "e");

        org.junit.jupiter.api.Assertions.assertAll(() -> assertTrue(AnyPut.create(user).has("info", "name", "n")),
                () -> assertTrue(AnyPut.create(user, NamingPolicy.SNAKE_CASE).has("info", "name", "n")),
                () -> assertTrue(AnyPut.create(user, Arrays.asList("email")).has("info", "email", "e")),
                () -> assertFalse(AnyPut.create(user, Arrays.asList("email"), NamingPolicy.CAMEL_CASE).has("info", "name")),
                () -> assertTrue(AnyPut.create(Arrays.asList(user)).get(0).has("info", "name", "n")),
                () -> assertTrue(AnyPut.toPut(Arrays.asList(user)).get(0).has(Bytes.toBytes("info"), Bytes.toBytes("name"))));
    }

    // ---- 2026-10-24 r2 sliceO ----

    /**
     * An empty value wrapper converts to no row-key bytes. of(Object) / of(Object, long), and create(entity) for an entity whose
     * row key holds one, failed with a NullPointerException on row.length inside Put(byte[]); they now throw the
     * IllegalArgumentException that of(Object, boolean) and the sliced overloads already threw for the same key (pinned as guards).
     */
    @Test
    public void testOf_emptyValueWrapperRowKey_throwsIllegalArgumentException_r2sliceO() {
        final Object emptyOptional = java.util.Optional.empty();
        final WrapperRowKeyEntity entity = new WrapperRowKeyEntity();
        entity.setId(java.util.Optional.empty());
        entity.setName("n");

        org.junit.jupiter.api.Assertions.assertAll(() -> assertThrows(IllegalArgumentException.class, () -> AnyPut.of(emptyOptional)),
                () -> assertThrows(IllegalArgumentException.class, () -> AnyPut.of(emptyOptional, 5L)),
                () -> assertThrows(IllegalArgumentException.class, () -> AnyPut.of(com.landawn.abacus.util.u.Nullable.of((String) null))),
                () -> assertThrows(IllegalArgumentException.class, () -> AnyPut.of(com.landawn.abacus.util.Holder.of(null))),
                () -> assertThrows(IllegalArgumentException.class, () -> AnyPut.create(entity)),
                () -> assertThrows(IllegalArgumentException.class, () -> AnyPut.create(entity, Arrays.asList("name"))),
                () -> assertThrows(IllegalArgumentException.class, () -> AnyPut.create(Arrays.asList(entity))),
                // guards: the overloads that already threw IllegalArgumentException, and a present wrapper still works
                () -> assertThrows(IllegalArgumentException.class, () -> AnyPut.of(emptyOptional, true)),
                () -> assertThrows(IllegalArgumentException.class, () -> AnyPut.of(emptyOptional, 5L, true)),
                () -> assertThrows(IllegalArgumentException.class, () -> AnyPut.of(emptyOptional, 0, 1)),
                () -> assertArrayEquals(Bytes.toBytes("k"), AnyPut.of(java.util.Optional.of("k")).getRow()), () -> {
                    entity.setId(java.util.Optional.of("k1"));
                    assertArrayEquals(Bytes.toBytes("k1"), AnyPut.create(entity).getRow());
                });
    }

    @ColumnFamily("cf")
    public static class WrapperRowKeyEntity {
        @Id
        private java.util.Optional<String> id;
        private String name;

        public java.util.Optional<String> getId() {
            return id;
        }

        public void setId(final java.util.Optional<String> id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    /**
     * Beans.isBeanClass accepts an @Entity-annotated Map subclass. With an id property, create(...) mapped it through its Map
     * getters: the cells keySet/values/entrySet/size/empty, and its entries never became columns. Every single-entity create,
     * collection factory and toPut now rejects it (as a plain Map already was, by getBeanInfo; guard). As a nested property such a
     * Map is not flattened but written as one JSON cell, unchanged (guard).
     */
    @Test
    public void testCreate_entityAnnotatedMapSubclass_isRejectedNotMappedThroughMapGetters_r2sliceO() {
        final EntityMapWithId map = new EntityMapWithId();
        map.setId("k1");
        map.put("name", "n");

        final List<java.util.function.Supplier<?>> calls = Arrays.asList(() -> AnyPut.create(map), () -> AnyPut.create(map, NamingPolicy.SNAKE_CASE),
                () -> AnyPut.create(map, Arrays.asList("name")), () -> AnyPut.create(map, (java.util.Collection<String>) null, NamingPolicy.CAMEL_CASE),
                () -> AnyPut.create(Arrays.asList(map)), () -> AnyPut.create(Arrays.asList(map), NamingPolicy.SNAKE_CASE),
                () -> AnyPut.create(Arrays.asList(map), Arrays.asList("name")), () -> AnyPut.toPut(Arrays.asList(map)),
                () -> AnyPut.toPut(Arrays.asList(map), NamingPolicy.SNAKE_CASE));

        final MapPropertyEntity holder = new MapPropertyEntity();
        holder.setId("h1");
        final EntityMapWithId nested = new EntityMapWithId();
        nested.put("city", "c");
        holder.setAttrs(nested);

        org.junit.jupiter.api.Assertions.assertAll(java.util.stream.Stream.concat(
                calls.stream()
                        .map(call -> (org.junit.jupiter.api.function.Executable) () -> assertTrue(
                                assertThrows(IllegalArgumentException.class, call::get).getMessage().contains("not one"))),
                java.util.stream.Stream.of(() -> assertThrows(IllegalArgumentException.class, () -> AnyPut.create(new java.util.HashMap<>(map))), () -> {
                    final AnyPut put = AnyPut.create(holder);
                    assertEquals(1, put.size());
                    assertTrue(put.has("cf", "attrs"));
                    assertFalse(put.has("cf", "keySet"));
                })));
    }

    @com.landawn.abacus.annotation.Entity
    @ColumnFamily("cf")
    public static class EntityMapWithId extends java.util.HashMap<String, Object> {
        private static final long serialVersionUID = 1L;
        private String id;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }
    }

    @ColumnFamily("cf")
    public static class MapPropertyEntity {
        @Id
        private String id;
        private EntityMapWithId attrs;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public EntityMapWithId getAttrs() {
            return attrs;
        }

        public void setAttrs(final EntityMapWithId attrs) {
            this.attrs = attrs;
        }
    }
}
