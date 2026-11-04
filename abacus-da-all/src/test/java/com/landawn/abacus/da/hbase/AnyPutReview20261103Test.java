/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.util.HBaseColumn;
import com.landawn.abacus.util.NamingPolicy;

class AnyPutReview20261103Test {

    private static Put negativeTimestampPut() {
        // Put(byte[], long, NavigableMap) is the one native constructor that does not validate its timestamp.
        return new Put(Bytes.toBytes("row"), -1L, new TreeMap<>(Bytes.BYTES_COMPARATOR));
    }

    // Pins the @throws of addColumn(String, String, Object) and addColumn(byte[], byte[], byte[]): an AnyPut that copies (of(Put))
    // or wraps (the collection factories) a native Put with a negative default timestamp keeps it, the no-timestamp overloads
    // reuse it and HBase rejects it, the failed call leaves no family entry behind, and the explicit-timestamp overloads still work.
    @Test
    void negativeDefaultTimestampOfACopiedOrWrappedPutIsRejectedByTheNoTimestampOverloads() {
        final List<Supplier<AnyPut>> sources = new ArrayList<>();
        sources.add(() -> AnyPut.of(negativeTimestampPut()));
        sources.add(() -> AnyPut.create(List.of(negativeTimestampPut())).get(0));
        sources.add(() -> AnyPut.create(List.of(negativeTimestampPut()), NamingPolicy.SNAKE_CASE).get(0));
        sources.add(() -> AnyPut.create(List.of(negativeTimestampPut()), List.of("name")).get(0));
        sources.add(() -> AnyPut.create(List.of(negativeTimestampPut()), List.of("name"), NamingPolicy.SNAKE_CASE).get(0));

        final List<Consumer<AnyPut>> noTimestampAdders = List.of(p -> p.addColumn("f", "q", "v"),
                p -> p.addColumn(Bytes.toBytes("f"), Bytes.toBytes("q"), Bytes.toBytes("v")));

        final List<Executable> checks = new ArrayList<>();

        for (int i = 0; i < sources.size(); i++) {
            final Supplier<AnyPut> source = sources.get(i);
            final String msg = "source " + i;

            checks.add(() -> assertEquals(-1L, source.get().getTimestamp(), msg));

            for (final Consumer<AnyPut> adder : noTimestampAdders) {
                checks.add(() -> {
                    final AnyPut put = source.get();
                    final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> adder.accept(put), msg);

                    assertTrue(e.getMessage().contains("Timestamp cannot be negative"), msg + ": " + e.getMessage());
                    assertEquals(0, put.numFamilies(), msg);
                    assertTrue(put.isEmpty(), msg);
                });
            }

            checks.add(() -> assertEquals(1, source.get().addColumn("f", "q", 5L, "v").size(), msg));
            checks.add(() -> assertEquals(1, source.get().addColumn(Bytes.toBytes("f"), Bytes.toBytes("q"), 5L, Bytes.toBytes("v")).size(), msg));
        }

        assertAll(checks);
    }

    // Pins the documented copy-vs-wrap split the test above relies on: of(Put) copies, a collection factory wraps the same Put.
    @Test
    void ofPutCopiesWhileCollectionFactoriesWrapTheSamePut() {
        final Put source = negativeTimestampPut();

        assertAll(() -> assertNotSame(source, AnyPut.of(source).val()), () -> assertSame(source, AnyPut.create(List.of(source)).get(0).val()),
                () -> assertSame(source, AnyPut.toPut(List.of(source)).get(0)));
    }

    private static BoxedVersions newBoxedVersions() {
        final BoxedVersions entity = new BoxedVersions();
        entity.setId("r1");

        final Box<HBaseColumn<String>> one = new Box<>();
        one.setValue(HBaseColumn.valueOf("v", 5L));
        one.setLabel("L");
        entity.setOne(one);

        final Box<List<HBaseColumn<String>>> many = new Box<>();
        many.setValue(List.of(HBaseColumn.valueOf("a", 1L), HBaseColumn.valueOf("b", 2L)));
        entity.setMany(many);

        final Map<Long, HBaseColumn<String>> versions = new TreeMap<>();
        versions.put(5L, HBaseColumn.valueOf("m5", 5L));
        versions.put(6L, HBaseColumn.valueOf("m6", 6L));
        final Box<Map<Long, HBaseColumn<String>>> byVersion = new Box<>();
        byVersion.setValue(versions);
        entity.setByVersion(byVersion);

        final Box<Integer> num = new Box<>();
        num.setValue(7);
        entity.setNum(num);

        return entity;
    }

    private static void assertCell(final Cell cell, final long expectedTimestamp, final String expectedValue, final String msg) {
        assertEquals(expectedTimestamp, cell.getTimestamp(), msg);
        assertEquals(expectedValue, Bytes.toString(CellUtil.cloneValue(cell)), msg);
    }

    // A generic nested bean's property bound by its owner's declaration to an HBaseColumn (or a Collection/Map of them) is a
    // versioned property, as HBaseExecutor reads it back (with the property's parameterized BeanInfo): it must be written as one
    // cell per version at that version's timestamp. AnyPut walked the class's raw BeanInfo, where T is its erasure (Object),
    // so it wrote one text cell ("5:v", the JSON list...) at the latest timestamp instead.
    @Test
    void genericNestedBeanBoundToHBaseColumnIsWrittenAsVersionedCells() {
        final AnyPut put = AnyPut.create(newBoxedVersions());

        final List<Cell> one = put.get("one", "value");
        final List<Cell> label = put.get("one", "label");
        final List<Cell> many = put.get("many", "value");
        final List<Cell> byVersion = put.get("byVersion", "value");
        final List<Cell> num = put.get("num", "value");

        assertAll(() -> assertEquals(1, one.size()), () -> assertCell(one.get(0), 5L, "v", "one"), //
                () -> assertEquals(1, label.size()), () -> assertCell(label.get(0), HConstants.LATEST_TIMESTAMP, "L", "label"), //
                () -> assertEquals(2, many.size()), () -> assertCell(many.get(0), 1L, "a", "many[0]"), () -> assertCell(many.get(1), 2L, "b", "many[1]"), //
                () -> assertEquals(2, byVersion.size()), () -> assertCell(byVersion.get(0), 5L, "m5", "byVersion[0]"),
                () -> assertCell(byVersion.get(1), 6L, "m6", "byVersion[1]"), //
                // A type argument that is not versioned is written as before.
                () -> assertEquals(1, num.size()), () -> assertCell(num.get(0), HConstants.LATEST_TIMESTAMP, "7", "num"));
    }

    // The write side above is symmetric with HBaseExecutor.toEntity's generic nested-bean read.
    @Test
    void genericNestedBeanBoundToHBaseColumnRoundTrips() {
        final List<Cell> cells = new ArrayList<>();
        AnyPut.create(newBoxedVersions()).val().getFamilyCellMap().values().forEach(cells::addAll);
        cells.sort(CellComparator.getInstance());

        final BoxedVersions back = HBaseExecutor.toEntity(Result.create(cells), BoxedVersions.class);

        assertAll(() -> assertEquals("r1", back.getId()), () -> assertEquals(HBaseColumn.valueOf("v", 5L), back.getOne().getValue()),
                () -> assertEquals("L", back.getOne().getLabel()),
                () -> assertEquals(Set.of(HBaseColumn.valueOf("a", 1L), HBaseColumn.valueOf("b", 2L)), new HashSet<>(back.getMany().getValue())),
                () -> assertEquals(Map.of(5L, HBaseColumn.valueOf("m5", 5L), 6L, HBaseColumn.valueOf("m6", 6L)), new HashMap<>(back.getByVersion().getValue())),
                () -> assertEquals(Integer.valueOf(7), back.getNum().getValue()));
    }

    public static class Box<T> {
        private T value;
        private String label;

        public T getValue() {
            return value;
        }

        public void setValue(final T value) {
            this.value = value;
        }

        public String getLabel() {
            return label;
        }

        public void setLabel(final String label) {
            this.label = label;
        }
    }

    public static class BoxedVersions {
        @Id
        private String id;
        private Box<HBaseColumn<String>> one;
        private Box<List<HBaseColumn<String>>> many;
        private Box<Map<Long, HBaseColumn<String>>> byVersion;
        private Box<Integer> num;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public Box<HBaseColumn<String>> getOne() {
            return one;
        }

        public void setOne(final Box<HBaseColumn<String>> one) {
            this.one = one;
        }

        public Box<List<HBaseColumn<String>>> getMany() {
            return many;
        }

        public void setMany(final Box<List<HBaseColumn<String>>> many) {
            this.many = many;
        }

        public Box<Map<Long, HBaseColumn<String>>> getByVersion() {
            return byVersion;
        }

        public void setByVersion(final Box<Map<Long, HBaseColumn<String>>> byVersion) {
            this.byVersion = byVersion;
        }

        public Box<Integer> getNum() {
            return num;
        }

        public void setNum(final Box<Integer> num) {
            this.num = num;
        }
    }
}
