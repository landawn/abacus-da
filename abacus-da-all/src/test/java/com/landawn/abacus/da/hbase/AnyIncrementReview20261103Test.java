/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

class AnyIncrementReview20261103Test {

    private static Cell cell(final String family, final byte[] value) {
        return CellUtil.createCell(Bytes.toBytes("r"), Bytes.toBytes(family), new byte[0], 1L, KeyValue.Type.Put.getCode(), value);
    }

    // Pins the has(String, String, Object) / has(String, String, long, Object) @param caveat: an increment amount is queued as an
    // 8-byte long, while a boxed number is encoded as its decimal text (as AnyPut stores it), so only the amount's bytes match.
    @Test
    void boxedNumberNeverMatchesAnIncrementAmountButItsBytesDo() {
        final AnyIncrement increment = AnyIncrement.of("r").addColumn("stats", "visits", 5L);
        final AnyPut put = AnyPut.of("r").addColumn("stats", "visits", 5);

        assertAll(() -> assertFalse(increment.has("stats", "visits", Long.valueOf(5))), () -> assertFalse(increment.has("stats", "visits", Integer.valueOf(5))),
                () -> assertTrue(increment.has("stats", "visits", Bytes.toBytes(5L))),
                () -> assertFalse(increment.has("stats", "visits", HConstants.LATEST_TIMESTAMP, Long.valueOf(5))),
                () -> assertTrue(increment.has("stats", "visits", HConstants.LATEST_TIMESTAMP, Bytes.toBytes(5L))),
                // the primitive overload is the timestamp one
                () -> assertFalse(increment.has("stats", "visits", 5L)),
                // AnyPut stores the text, so there the boxed number matches and the 8-byte form does not
                () -> assertTrue(put.has("stats", "visits", Integer.valueOf(5))), () -> assertFalse(put.has("stats", "visits", Bytes.toBytes(5L))));
    }

    // Pins the add(Cell) doc: a cell whose value is not an 8-byte long is queued as-is; decoding the amounts then fails.
    @Test
    void cellWhoseValueIsNotAnEightByteLongIsQueuedButCannotBeDecoded() throws Exception {
        final AnyIncrement increment = AnyIncrement.of("r");

        assertSame(increment, increment.add(cell("stats", Bytes.toBytes("5"))));

        assertAll(() -> assertEquals(1, increment.size()), () -> assertTrue(increment.hasFamilies()),
                () -> assertThrows(IllegalArgumentException.class, increment::getFamilyMapOfLongs),
                () -> assertThrows(IllegalArgumentException.class, increment::toString));

        final AnyIncrement eightBytes = AnyIncrement.of("r").add(cell("stats", Bytes.toBytes(7L)));

        assertEquals(7L, eightBytes.getFamilyMapOfLongs().get(Bytes.toBytes("stats")).get(new byte[0]));
    }

    // Pins the class-doc "Value encoding" paragraph: a counter cell (an 8-byte long, as an increment leaves it) is read as text by
    // entity mapping, so a long/Long property cannot hold it, while a byte[] property keeps the bytes for Bytes.toLong.
    @Test
    void counterCellIsReadAsTextByEntityMapping() {
        final byte[] counter = Bytes.toBytes(5L);
        final Counter entity = new Counter();
        entity.setId("r");
        entity.setHits(5L);

        assertAll(
                // entity mapping and AnyAppend write a number as its text, not as the 8-byte form an increment needs
                () -> assertArrayEquals(Bytes.toBytes("5"), CellUtil.cloneValue(AnyPut.create(entity).get("hits", "").get(0))),
                () -> assertArrayEquals(Bytes.toBytes("5"), CellUtil.cloneValue(AnyAppend.of("r").addColumn("cf", "q", 5L).get("cf", "q").get(0))),
                () -> assertThrows(NumberFormatException.class,
                        () -> HBaseExecutor.toEntity(Result.create(new Cell[] { cell("hits", counter) }), Counter.class)),
                () -> assertThrows(NumberFormatException.class,
                        () -> HBaseExecutor.toEntity(Result.create(new Cell[] { cell("primitiveHits", counter) }), Counter.class)),
                () -> assertEquals(5L, Bytes.toLong(HBaseExecutor.toEntity(Result.create(new Cell[] { cell("raw", counter) }), Counter.class).getRaw())),
                // the text an AnyPut writes for a number reads back into the Long property
                () -> assertEquals(5L, HBaseExecutor.toEntity(Result.create(new Cell[] { cell("hits", Bytes.toBytes("5")) }), Counter.class).getHits()));
    }

    /**
     * Pins the reworded class-doc caveat: the NumberFormatException is not guaranteed. A counter whose eight bytes happen to be
     * ASCII digits decodes as UTF-8 text to a valid number, so a long/Long property silently reads a wrong value (the class doc
     * used to say such a read always throws).
     */
    @Test
    void counterCellWhoseBytesAreAsciiDigitsReadsAsAWrongNumber() {
        final byte[] counter = Bytes.toBytes(0x3132333435363738L);

        assertAll(() -> assertEquals("12345678", Bytes.toString(counter)),
                () -> assertEquals(Long.valueOf(12345678L),
                        HBaseExecutor.toEntity(Result.create(new Cell[] { cell("hits", counter) }), Counter.class).getHits()),
                () -> assertEquals(12345678L,
                        HBaseExecutor.toEntity(Result.create(new Cell[] { cell("primitiveHits", counter) }), Counter.class).getPrimitiveHits()),
                () -> assertEquals(0x3132333435363738L,
                        Bytes.toLong(HBaseExecutor.toEntity(Result.create(new Cell[] { cell("raw", counter) }), Counter.class).getRaw())));
    }

    // Pins the @param qualifier caveat of both addColumn overloads: a null qualifier is the empty qualifier.
    @Test
    void nullQualifierDenotesTheEmptyQualifier() {
        final AnyIncrement fromBytes = AnyIncrement.of("r").addColumn(Bytes.toBytes("cf"), null, 2L);
        final AnyIncrement fromString = AnyIncrement.of("r").addColumn("cf", null, 3L);

        assertAll(() -> assertEquals(2L, fromBytes.getFamilyMapOfLongs().get(Bytes.toBytes("cf")).get(new byte[0])),
                () -> assertEquals(3L, fromString.getFamilyMapOfLongs().get(Bytes.toBytes("cf")).get(new byte[0])), () -> assertTrue(fromString.has("cf", "")),
                () -> assertTrue(fromString.has(Bytes.toBytes("cf"), null)));
    }

    // Re-verifies s03's "no twin needed" for the AnyPut negative-default-timestamp @throws: Increment.addColumn never checks the
    // default timestamp, so an increment built through of(byte[], long, NavigableMap) with a negative one queues the cell with it.
    @Test
    void negativeDefaultTimestampIsNotRejectedByAddColumn() {
        final AnyIncrement increment = AnyIncrement.of(Bytes.toBytes("r"), -1L, new TreeMap<>(Bytes.BYTES_COMPARATOR));

        increment.addColumn("cf", "q", 1L).addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q2"), 2L);

        final List<Cell> cells = increment.get("cf", "q");

        assertAll(() -> assertEquals(-1L, increment.getTimestamp()), () -> assertEquals(1, cells.size()), () -> assertEquals(-1L, cells.get(0).getTimestamp()),
                () -> assertEquals(-1L, increment.get("cf", "q2").get(0).getTimestamp()), () -> assertEquals(2, increment.size()));
    }

    public static class Counter {
        private String id;
        private Long hits;
        private long primitiveHits;
        private byte[] raw;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public Long getHits() {
            return hits;
        }

        public void setHits(final Long hits) {
            this.hits = hits;
        }

        public long getPrimitiveHits() {
            return primitiveHits;
        }

        public void setPrimitiveHits(final long primitiveHits) {
            this.primitiveHits = primitiveHits;
        }

        public byte[] getRaw() {
            return raw;
        }

        public void setRaw(final byte[] raw) {
            this.raw = raw;
        }
    }
}
