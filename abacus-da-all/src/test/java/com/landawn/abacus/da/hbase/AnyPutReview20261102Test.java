/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.annotation.Column;
import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.da.hbase.annotation.ColumnFamily;
import com.landawn.abacus.util.HBaseColumn;
import com.landawn.abacus.util.NamingPolicy;

class AnyPutReview20261102Test {

    private static Result toResult(final AnyPut put) {
        final List<Cell> cells = new ArrayList<>();
        put.val().getFamilyCellMap().values().forEach(cells::addAll);
        cells.sort(CellComparator.getInstance());

        return Result.create(cells);
    }

    private static Inner newInner() {
        final Inner inner = new Inner();
        inner.setA("A");
        inner.setRaw(new byte[] { 1, 2, (byte) 0xFF });
        inner.setQueue(new ArrayBlockingQueue<>(4, false, List.of(HBaseColumn.valueOf("q1", 1L), HBaseColumn.valueOf("q2", 2L))));

        final Map<Long, HBaseColumn<String>> byVersion = new TreeMap<>();
        byVersion.put(5L, HBaseColumn.valueOf("m5", 5L));
        byVersion.put(6L, HBaseColumn.valueOf("m6", 6L));
        inner.setByVersion(byVersion);
        inner.setOne(HBaseColumn.valueOf(7, 9L));
        inner.setOpt(Optional.of("o"));

        return inner;
    }

    private static void assertInner(final String msg, final Inner inner) {
        assertNotNull(inner, msg);
        assertEquals("A", inner.getA(), msg);
        assertArrayEquals(new byte[] { 1, 2, (byte) 0xFF }, inner.getRaw(), msg);
        assertInstanceOf(ArrayBlockingQueue.class, inner.getQueue(), msg);
        assertEquals(List.of(HBaseColumn.valueOf("q2", 2L), HBaseColumn.valueOf("q1", 1L)), new ArrayList<>(inner.getQueue()), msg);
        assertEquals(2, inner.getByVersion().size(), msg);
        assertEquals("m5", inner.getByVersion().get(5L).value(), msg);
        assertEquals("m6", inner.getByVersion().get(6L).value(), msg);
        assertEquals(HBaseColumn.valueOf(7, 9L), inner.getOne(), msg);
        assertEquals(Optional.of("o"), inner.getOpt(), msg);
    }

    // Pins the write/read symmetry of AnyPut.create and HBaseExecutor.toEntity for a flattened nested bean whose properties
    // cover every versioned container shape the read side builds (an ArrayBlockingQueue, which has no no-args constructor, a
    // Map keyed by version, a single HBaseColumn), raw bytes and a present Optional, in an own-family and a class-default
    // family layout, under several naming policies (toEntity reads every policy's names).
    @Test
    void nestedBeanContainersRoundTripUnderEveryLayoutAndNamingPolicy() {
        for (final NamingPolicy namingPolicy : new NamingPolicy[] { NamingPolicy.CAMEL_CASE, NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE }) {
            final OwnFamilies source = new OwnFamilies();
            source.setId("r1");
            source.setInner(newInner());
            source.setUserName("u");
            source.setOther("x");

            final AnyPut put = AnyPut.create(source, namingPolicy);
            final OwnFamilies back = HBaseExecutor.toEntity(toResult(put), OwnFamilies.class);

            assertEquals("r1", back.getId(), namingPolicy.name());
            assertEquals("u", back.getUserName(), namingPolicy.name());
            assertEquals("x", back.getOther(), namingPolicy.name());
            assertInner(namingPolicy.name(), back.getInner());

            final ClassFamily classFamilySource = new ClassFamily();
            classFamilySource.setId("r2");
            classFamilySource.setInner(newInner());
            classFamilySource.setUserName("u");

            final AnyPut classFamilyPut = AnyPut.create(classFamilySource, namingPolicy);
            assertEquals(1, classFamilyPut.numFamilies(), namingPolicy.name());
            assertTrue(classFamilyPut.getFamilyCellMap().containsKey(Bytes.toBytes("cf")), namingPolicy.name());

            final ClassFamily classFamilyBack = HBaseExecutor.toEntity(toResult(classFamilyPut), ClassFamily.class);

            assertEquals("r2", classFamilyBack.getId(), namingPolicy.name());
            assertEquals("u", classFamilyBack.getUserName(), namingPolicy.name());
            assertInner("cf " + namingPolicy.name(), classFamilyBack.getInner());
        }
    }

    // A delete built through of(Object, long, NavigableMap) keeps a negative default timestamp unvalidated; the no-timestamp
    // marker overloads then reuse it and HBase rejects it (documented on each overload), leaving no family entry behind.
    @Test
    void negativeDefaultTimestampIsRejectedByEveryNoTimestampDeleteMarker() {
        final List<Runnable> calls = new ArrayList<>();
        final AnyDelete delete = AnyDelete.of("row", -1L, new TreeMap<>(Bytes.BYTES_COMPARATOR));

        calls.add(() -> delete.addFamily("f"));
        calls.add(() -> delete.addFamily(Bytes.toBytes("f")));
        calls.add(() -> delete.addColumn("f", "q"));
        calls.add(() -> delete.addColumn(Bytes.toBytes("f"), Bytes.toBytes("q")));
        calls.add(() -> delete.addColumns("f", "q"));
        calls.add(() -> delete.addColumns(Bytes.toBytes("f"), Bytes.toBytes("q")));

        for (final Runnable call : calls) {
            assertThrows(IllegalArgumentException.class, call::run);
            assertEquals(0, delete.numFamilies());
        }

        // An explicit, valid timestamp is still accepted.
        delete.addColumn("f", "q", 5L);
        assertEquals(1, delete.size());
    }

    public static class Inner {
        private String a;
        private byte[] raw;
        private ArrayBlockingQueue<HBaseColumn<String>> queue;
        private Map<Long, HBaseColumn<String>> byVersion;
        private HBaseColumn<Integer> one;
        private Optional<String> opt;

        public String getA() {
            return a;
        }

        public void setA(final String a) {
            this.a = a;
        }

        public byte[] getRaw() {
            return raw;
        }

        public void setRaw(final byte[] raw) {
            this.raw = raw;
        }

        public ArrayBlockingQueue<HBaseColumn<String>> getQueue() {
            return queue;
        }

        public void setQueue(final ArrayBlockingQueue<HBaseColumn<String>> queue) {
            this.queue = queue;
        }

        public Map<Long, HBaseColumn<String>> getByVersion() {
            return byVersion;
        }

        public void setByVersion(final Map<Long, HBaseColumn<String>> byVersion) {
            this.byVersion = byVersion;
        }

        public HBaseColumn<Integer> getOne() {
            return one;
        }

        public void setOne(final HBaseColumn<Integer> one) {
            this.one = one;
        }

        public Optional<String> getOpt() {
            return opt;
        }

        public void setOpt(final Optional<String> opt) {
            this.opt = opt;
        }
    }

    public static class OwnFamilies {
        @Id
        private String id;
        private Inner inner;
        private String userName;
        @Column("ux")
        private String other;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public Inner getInner() {
            return inner;
        }

        public void setInner(final Inner inner) {
            this.inner = inner;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(final String userName) {
            this.userName = userName;
        }

        public String getOther() {
            return other;
        }

        public void setOther(final String other) {
            this.other = other;
        }
    }

    @ColumnFamily("cf")
    public static class ClassFamily {
        @Id
        private String id;
        private Inner inner;
        private String userName;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public Inner getInner() {
            return inner;
        }

        public void setInner(final Inner inner) {
            this.inner = inner;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(final String userName) {
            this.userName = userName;
        }
    }
}
