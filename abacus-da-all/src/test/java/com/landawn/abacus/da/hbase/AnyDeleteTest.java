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

    // Construction and configuration

    // Mutation accessors

    @Test
    public void testGetRow_matchesConstructorRowKey() {
        AnyDelete delete = AnyDelete.of("the-row-key");
        assertArrayEquals(Bytes.toBytes("the-row-key"), delete.getRow());
    }

    // Factories and insert operations

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

    // addColumn variants

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
    public void testAddColumns_string() {
        AnyDelete delete = AnyDelete.of("row").addColumns("cf", "q");
        assertEquals(1, delete.numFamilies());
    }

    @Test
    public void testAddColumns_stringWithTimestamp() {
        AnyDelete delete = AnyDelete.of("row").addColumns("cf", "q", 100L);
        assertEquals(1, delete.numFamilies());
    }

    // Updates and deletes

    // add(Cell) -- exercise the residual uncovered method.

    @Test
    public void testAddCell_acceptsDeleteMarkerCell() throws Exception {
        byte[] row = Bytes.toBytes("rk");
        // Construct a delete-family marker cell using KeyValue with Type.DeleteFamily.
        Cell deleteMarker = new org.apache.hadoop.hbase.KeyValue(row, Bytes.toBytes("cf"), null, org.apache.hadoop.hbase.HConstants.LATEST_TIMESTAMP,
                org.apache.hadoop.hbase.KeyValue.Type.DeleteFamily);

        AnyDelete delete = AnyDelete.of("rk");
        AnyDelete returned = delete.add(deleteMarker);
        assertSame(delete, returned);
        assertEquals(1, delete.numFamilies());
    }

    /**
     * Pins the documented default-timestamp contract: the no-timestamp {@code addColumn},
     * {@code addColumns} and {@code addFamily} overloads (String and byte[]) use the timestamp last
     * set via {@code setTimestamp(long)}, with the tombstone type matching each strategy; tombstones
     * added before {@code setTimestamp} keep {@code LATEST_TIMESTAMP}.
     */
    @Test
    public void testDefaultTimestamp_comesFromSetTimestamp_sliceM() {
        AnyDelete delete = AnyDelete.of("row").addColumn("early", "q").setTimestamp(500L);
        delete.addColumn("f", "q").addColumns("f", "q2").addFamily("g");
        delete.addColumn(Bytes.toBytes("h"), Bytes.toBytes("q")).addColumns(Bytes.toBytes("h"), Bytes.toBytes("q2")).addFamily(Bytes.toBytes("k"));

        assertEquals(org.apache.hadoop.hbase.HConstants.LATEST_TIMESTAMP, delete.get("early", "q").get(0).getTimestamp());

        assertEquals(500L, delete.get("f", "q").get(0).getTimestamp());
        assertEquals(Cell.Type.Delete, delete.get("f", "q").get(0).getType());
        assertEquals(500L, delete.get("f", "q2").get(0).getTimestamp());
        assertEquals(Cell.Type.DeleteColumn, delete.get("f", "q2").get(0).getType());
        final Cell family = delete.getFamilyCellMap().get(Bytes.toBytes("g")).get(0);
        assertEquals(500L, family.getTimestamp());
        assertEquals(Cell.Type.DeleteFamily, family.getType());

        assertEquals(500L, delete.get("h", "q").get(0).getTimestamp());
        assertEquals(Cell.Type.Delete, delete.get("h", "q").get(0).getType());
        assertEquals(500L, delete.get("h", "q2").get(0).getTimestamp());
        assertEquals(Cell.Type.DeleteColumn, delete.get("h", "q2").get(0).getType());
        final Cell byteFamily = delete.getFamilyCellMap().get(Bytes.toBytes("k")).get(0);
        assertEquals(500L, byteFamily.getTimestamp());
        assertEquals(Cell.Type.DeleteFamily, byteFamily.getType());
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
    public void testOf_copyExistingDelete() {
        Delete original = new Delete(Bytes.toBytes("orig"));
        original.addFamily(Bytes.toBytes("cf"));

        AnyDelete copy = AnyDelete.of(original);

        assertNotNull(copy);
        assertNotSame(original, copy.val(), "Copy should be a different Delete instance");
        assertArrayEquals(Bytes.toBytes("orig"), copy.getRow());
    }

    @Test
    public void testOf_copyExistingDelete_copiesAttributesAndPriorityButNotDurability() {
        Delete orig = new Delete(Bytes.toBytes("k"));
        orig.setAttribute("trace", Bytes.toBytes("t1"));
        orig.setPriority(7);
        orig.setDurability(Durability.SKIP_WAL);

        AnyDelete copy = AnyDelete.of(orig);

        assertArrayEquals(Bytes.toBytes("t1"), copy.getAttribute("trace"));
        assertEquals(7, copy.getPriority());
        // Documented: HBase's Delete(Delete) copy constructor resets durability to USE_DEFAULT.
        assertEquals(Durability.USE_DEFAULT, copy.getDurability());

        // The copied attribute map is independent of the source.
        copy.setAttribute("trace", (byte[]) null);
        assertArrayEquals(Bytes.toBytes("t1"), orig.getAttribute("trace"));
    }

    // Independence from copied Delete

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

    // Durability / TTL / Timestamp setters

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

    // toDelete(Collection) static method

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
    public void testToDelete_collectionWithNull_throws() {
        AnyDelete d1 = AnyDelete.of("r1");
        assertThrows(IllegalArgumentException.class, () -> AnyDelete.toDelete(Arrays.asList(d1, null)));
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

    // val()

    @Test
    public void testVal_returnsUnderlyingDelete() {
        AnyDelete delete = AnyDelete.of("row");
        Delete underlying = delete.val();
        assertNotNull(underlying);
        assertArrayEquals(Bytes.toBytes("row"), underlying.getRow());
    }

    // Queries and results

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

    // Additional contracts

    /**
     * {@code AnyDelete.add(Cell)} validates the cell up front, so a {@code null} cell is rejected with
     * an {@code IllegalArgumentException} -- the same contract as {@code AnyPut}/{@code AnyIncrement}/
     * {@code AnyAppend.add(Cell)}.
     */
    @Test
    public void testAddCell_nullCell_throwsIae() {
        AnyDelete delete = AnyDelete.of("rk");
        assertThrows(IllegalArgumentException.class, () -> delete.add((Cell) null));
        assertEquals(0, delete.size());
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

    // addFamily variants

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
    public void testAddFamilyVersion_byteArray() {
        AnyDelete delete = AnyDelete.of("row").addFamilyVersion(Bytes.toBytes("cf"), 12345L);
        assertEquals(1, delete.numFamilies());
    }

    // addFamilyVersion variants

    @Test
    public void testAddFamilyVersion_string() {
        AnyDelete delete = AnyDelete.of("row").addFamilyVersion("cf", 12345L);
        assertEquals(1, delete.numFamilies());
    }

    @Test
    public void testChaining_multipleOperations() {
        AnyDelete delete = AnyDelete.of("row").addFamily("cf1").addColumn("cf2", "q1").addColumns("cf3", "q2");
        assertEquals(3, delete.numFamilies());
    }

    // equals, hashCode, toString

    @Test
    public void testEquals_sameInstance() {
        AnyDelete delete = AnyDelete.of("row").addFamily("cf");
        assertEquals(delete, delete);
    }

    @Test
    public void testHeapSize_returnsPositive() {
        AnyDelete delete = AnyDelete.of("row").addFamily("cf");
        assertTrue(delete.heapSize() > 0L);
    }

    @Test
    public void testOf_byteArrayRowKey() {
        byte[] row = Bytes.toBytes("row-bytes");
        AnyDelete delete = AnyDelete.of(row);
        assertArrayEquals(row, delete.getRow());
    }

    /**
     * A {@code null} row key, family map or source {@code Delete} is rejected with an
     * {@code IllegalArgumentException} by every {@code of(...)} overload (previously some overloads
     * leaked HBase's {@code NullPointerException}).
     */
    @Test
    public void testOf_nullArguments_throwIae() {
        final java.util.NavigableMap<byte[], java.util.List<Cell>> familyMap = new java.util.TreeMap<>(Bytes.BYTES_COMPARATOR);
        assertThrows(IllegalArgumentException.class, () -> AnyDelete.of((Object) null));
        assertThrows(IllegalArgumentException.class, () -> AnyDelete.of((Object) null, 1L));
        assertThrows(IllegalArgumentException.class, () -> AnyDelete.of((Object) null, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> AnyDelete.of((Object) null, 0, 1, 1L));
        assertThrows(IllegalArgumentException.class, () -> AnyDelete.of((Object) null, 1L, familyMap));
        assertThrows(IllegalArgumentException.class, () -> AnyDelete.of("rk", 1L, null));
        assertThrows(IllegalArgumentException.class, () -> AnyDelete.of((org.apache.hadoop.hbase.client.Delete) null));
    }

    // Factory methods: of(...) variants

    @Test
    public void testOf_stringRowKey() {
        AnyDelete delete = AnyDelete.of("row-1");
        assertNotNull(delete);
        assertArrayEquals(Bytes.toBytes("row-1"), delete.getRow());
    }

    @Test
    public void testOf_withTimestamp() {
        long ts = 1_700_000_000_000L;
        AnyDelete delete = AnyDelete.of("row-ts", ts);
        assertEquals(ts, delete.getTimestamp());
        assertArrayEquals(Bytes.toBytes("row-ts"), delete.getRow());
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
    public void testOf_withTimestampAndFamilyMap_retainsMapByReferenceAndAcceptsNegativeTimestamp() {
        NavigableMap<byte[], List<Cell>> familyMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        AnyDelete delete = AnyDelete.of("row", -1L, familyMap);

        assertSame(familyMap, delete.getFamilyCellMap());
        // Documented: this overload does not validate the timestamp, unlike the sibling factories.
        assertEquals(-1L, delete.getTimestamp());
        assertThrows(IllegalArgumentException.class, () -> AnyDelete.of("row", -1L));
        assertThrows(IllegalArgumentException.class, () -> AnyDelete.of("row", 0, 3, -1L));
    }

    @Test
    public void testSize_afterAddingFamilies() {
        AnyDelete delete = AnyDelete.of("row").addFamily("cf1").addFamily("cf2");
        assertEquals(2, delete.size());
    }

    @Test
    public void testToString_notNull() {
        AnyDelete delete = AnyDelete.of("row").addFamily("cf");
        String s = delete.toString();
        assertNotNull(s);
    }

    @Test
    public void testVal_consistentAcrossCalls() {
        AnyDelete delete = AnyDelete.of("row");
        assertSame(delete.val(), delete.val());
    }

    // ---- 2026-10-24 sliceP ----

    /**
     * Pins the {@code @param family} docs of every addFamily/addFamilyVersion/addColumn/addColumns overload: a null family is
     * accepted and queued here; a single delete then fails with NullPointerException while HBase builds the request
     * (ProtobufUtil.toMutation(DELETE, delete), as RequestConverter.buildMutateRequest does), while a batch delete carries
     * no column data in the proto (RequestConverter.buildNoDataRegionAction) and the client's default KeyValueCodec writes
     * the tombstone with an empty family.
     */
    @Test
    public void testNullFamily_singleDeleteFailsWhenRequestIsBuilt_batchSendsEmptyFamily_sliceP() throws Exception {
        final byte[] q = Bytes.toBytes("q");
        final List<java.util.function.Supplier<AnyDelete>> overloads = Arrays.asList( //
                () -> AnyDelete.of("row").addFamily((String) null), //
                () -> AnyDelete.of("row").addFamily((String) null, 5L), //
                () -> AnyDelete.of("row").addFamily((byte[]) null), //
                () -> AnyDelete.of("row").addFamily((byte[]) null, 5L), //
                () -> AnyDelete.of("row").addFamilyVersion((String) null, 5L), //
                () -> AnyDelete.of("row").addFamilyVersion((byte[]) null, 5L), //
                () -> AnyDelete.of("row").addColumn((String) null, "q"), //
                () -> AnyDelete.of("row").addColumn((String) null, "q", 5L), //
                () -> AnyDelete.of("row").addColumn((byte[]) null, q), //
                () -> AnyDelete.of("row").addColumn((byte[]) null, q, 5L), //
                () -> AnyDelete.of("row").addColumns((String) null, "q"), //
                () -> AnyDelete.of("row").addColumns((String) null, "q", 5L), //
                () -> AnyDelete.of("row").addColumns((byte[]) null, q), //
                () -> AnyDelete.of("row").addColumns((byte[]) null, q, 5L));

        org.junit.jupiter.api.Assertions.assertAll(overloads.stream().map(overload -> (org.junit.jupiter.api.function.Executable) () -> {
            assertEquals(1, overload.get().size());
            assertThrows(NullPointerException.class, () -> org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil
                    .toMutation(org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.MutationType.DELETE, overload.get().val()));
            final Cell sent = encodeAsBatchCellBlock(overload.get().val());
            assertEquals(0, sent.getFamilyLength());
            assertArrayEquals(Bytes.toBytes("row"), org.apache.hadoop.hbase.CellUtil.cloneRow(sent));
        }));

        // control: a real family builds into one column of the request
        assertEquals(1,
                org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil
                        .toMutation(org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.MutationType.DELETE,
                                AnyDelete.of("row").addFamily("cf").val())
                        .getColumnValueCount());
    }

    /** Encodes the single tombstone of {@code delete} the way a batch delete sends it and decodes it back. */
    private static Cell encodeAsBatchCellBlock(final Delete delete) throws java.io.IOException {
        final org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto proto = org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil
                .toMutationNoData(org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.MutationType.DELETE, delete);
        assertEquals(1, proto.getAssociatedCellCount());
        assertEquals(0, proto.getColumnValueCount());

        final org.apache.hadoop.hbase.codec.Codec codec = new org.apache.hadoop.hbase.codec.KeyValueCodec();
        final java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        final org.apache.hadoop.hbase.codec.Codec.Encoder encoder = codec.getEncoder(out);
        final org.apache.hadoop.hbase.CellScanner cells = delete.cellScanner();
        while (cells.advance()) {
            encoder.write(cells.current());
        }
        encoder.flush();

        final org.apache.hadoop.hbase.codec.Codec.Decoder decoder = codec.getDecoder(new java.io.ByteArrayInputStream(out.toByteArray()));
        assertTrue(decoder.advance());
        final Cell decoded = decoder.current();
        assertFalse(decoder.advance());
        return decoded;
    }

    /**
     * Pins the of(rowKey, timestamp, familyMap) {@code @param timestamp} doc: a negative default timestamp is accepted, but
     * the no-timestamp addFamily/addColumn/addColumns overloads then reject it; explicit-timestamp overloads still work.
     */
    @Test
    public void testNegativeDefaultTimestamp_rejectedByNoTimestampAdders_sliceP() {
        final java.util.function.Supplier<AnyDelete> negative = () -> AnyDelete.of("row", -1L, new TreeMap<byte[], List<Cell>>(Bytes.BYTES_COMPARATOR));
        final byte[] cf = Bytes.toBytes("cf");
        final byte[] q = Bytes.toBytes("q");

        org.junit.jupiter.api.Assertions.assertAll( //
                () -> assertEquals(-1L, negative.get().getTimestamp()),
                () -> assertThrows(IllegalArgumentException.class, () -> negative.get().addFamily("cf")),
                () -> assertThrows(IllegalArgumentException.class, () -> negative.get().addFamily(cf)),
                () -> assertThrows(IllegalArgumentException.class, () -> negative.get().addColumn("cf", "q")),
                () -> assertThrows(IllegalArgumentException.class, () -> negative.get().addColumn(cf, q)),
                () -> assertThrows(IllegalArgumentException.class, () -> negative.get().addColumns("cf", "q")),
                () -> assertThrows(IllegalArgumentException.class, () -> negative.get().addColumns(cf, q)),
                () -> assertEquals(1, negative.get().addFamily("cf", 5L).size()), () -> assertEquals(1, negative.get().addColumn("cf", "q", 5L).size()),
                () -> assertEquals(1, negative.get().addColumns(cf, q, 5L).size()), () -> assertEquals(1, negative.get().addFamilyVersion("cf", 5L).size()));
    }

    // ---- 2026-10-24 apply-hbase ----

    /** Every add* overload, called with {@code family}; the byte[] and String forms of the same family. */
    private static List<java.util.function.Consumer<AnyDelete>> addersFor(final String family) {
        final byte[] f = Bytes.toBytes(family);
        final byte[] q = Bytes.toBytes("q");

        return Arrays.asList(d -> d.addFamily(family), d -> d.addFamily(family, 5L), d -> d.addFamily(f), d -> d.addFamily(f, 5L),
                d -> d.addFamilyVersion(family, 5L), d -> d.addFamilyVersion(f, 5L), d -> d.addColumn(family, "q"), d -> d.addColumn(family, "q", 5L),
                d -> d.addColumn(f, q), d -> d.addColumn(f, q, 5L), d -> d.addColumns(family, "q"), d -> d.addColumns(family, "q", 5L), d -> d.addColumns(f, q),
                d -> d.addColumns(f, q, 5L));
    }

    /**
     * Item 10b: HBase's Delete creates the family's cell list before it builds the delete marker, so an add* that fails on a
     * family over 127 bytes used to leave an EMPTY cell list behind (isEmpty() false, size() 0); a delete sent anyway decoded to a
     * whole-row delete on the server. A failed add* now removes the entry it created and rethrows the original exception, for
     * every overload, on an empty delete and on one that already targets another family.
     */
    @Test
    public void testFailedAdd_removesTheEmptyFamilyEntryItCreated_applyHbase() {
        final String tooLong = "f".repeat(128);
        final byte[] tooLongBytes = Bytes.toBytes(tooLong);

        org.junit.jupiter.api.Assertions
                .assertAll(addersFor(tooLong).stream().flatMap(adder -> java.util.stream.Stream.<org.junit.jupiter.api.function.Executable> of(() -> {
                    final AnyDelete empty = AnyDelete.of("row");
                    final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> adder.accept(empty));

                    assertTrue(e.getMessage().contains("Family"), e.getMessage());
                    assertTrue(empty.isEmpty());
                    assertEquals(0, empty.numFamilies());
                    assertFalse(empty.getFamilyCellMap().containsKey(tooLongBytes));
                }, () -> {
                    final AnyDelete delete = AnyDelete.of("row").addColumn("cf", "q");

                    assertThrows(IllegalArgumentException.class, () -> adder.accept(delete));
                    assertEquals(1, delete.numFamilies());
                    assertEquals(1, delete.size());
                    assertFalse(delete.getFamilyCellMap().containsKey(tooLongBytes));

                    // what the server receives is still the one-column delete, not a whole-row delete
                    final Delete decoded = org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil.toDelete(org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil
                            .toMutation(org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.MutationType.DELETE, delete.val()));
                    assertEquals(1, decoded.size());
                })));
    }

    /**
     * Item 10b guard: an entry that existed before the failed call is never touched (here an empty cell list that the caller's
     * family map already held), and successful adds after a failed one behave as before.
     */
    @Test
    public void testFailedAdd_keepsPreExistingEntries_andLaterAddsWork_applyHbase() {
        final String tooLong = "f".repeat(128);
        final byte[] tooLongBytes = Bytes.toBytes(tooLong);

        org.junit.jupiter.api.Assertions.assertAll(addersFor(tooLong).stream().map(adder -> (org.junit.jupiter.api.function.Executable) () -> {
            final NavigableMap<byte[], List<Cell>> familyMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
            final List<Cell> preExisting = new java.util.ArrayList<>();
            familyMap.put(tooLongBytes, preExisting);
            final AnyDelete delete = AnyDelete.of("row", 7L, familyMap);

            assertThrows(IllegalArgumentException.class, () -> adder.accept(delete));
            assertSame(preExisting, delete.getFamilyCellMap().get(tooLongBytes));
        }));

        org.junit.jupiter.api.Assertions.assertAll(addersFor("cf").stream().map(adder -> (org.junit.jupiter.api.function.Executable) () -> {
            final AnyDelete delete = AnyDelete.of("row");

            assertThrows(IllegalArgumentException.class, () -> addersFor(tooLong).get(0).accept(delete));
            adder.accept(delete);
            assertEquals(1, delete.numFamilies());
            assertEquals(1, delete.size());
            assertTrue(delete.getFamilyCellMap().containsKey(Bytes.toBytes("cf")));
        }));
    }
}
