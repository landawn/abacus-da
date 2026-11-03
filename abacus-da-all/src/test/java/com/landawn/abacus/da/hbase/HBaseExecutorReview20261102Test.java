/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.da.hbase.annotation.ColumnFamily;
import com.landawn.abacus.util.HBaseColumn;

class HBaseExecutorReview20261102Test {

    private static Result toResult(final AnyPut put) {
        final List<Cell> cells = new ArrayList<>();
        put.val().getFamilyCellMap().values().forEach(cells::addAll);
        cells.sort(CellComparator.getInstance());

        return Result.create(cells);
    }

    private static void addVersions(final List<Cell> cells, final String family, final String qualifier, final String... values) {
        for (int i = 0; i < values.length; i++) {
            cells.add(new KeyValue(Bytes.toBytes("row"), Bytes.toBytes(family), Bytes.toBytes(qualifier), values.length - i, Bytes.toBytes(values[i])));
        }
    }

    // An ArrayBlockingQueue versioned property was written by AnyPut.create but reading it threw IllegalArgumentException
    // ("Not able to create instance for collection"): it has no no-args constructor.
    @Test
    void arrayBlockingQueueVersionedPropertyReadsBackEveryVersion() {
        final List<Cell> cells = new ArrayList<>();
        addVersions(cells, "cf", "history", "3", "2", "1");
        addVersions(cells, "nested", "history", "c", "b");
        cells.sort(CellComparator.getInstance());

        final QueueVersions mapped = HBaseExecutor.toEntity(Result.create(cells), QueueVersions.class);

        assertNotNull(mapped);
        assertInstanceOf(ArrayBlockingQueue.class, mapped.getHistory());
        assertEquals(List.of(HBaseColumn.valueOf(3, 3L), HBaseColumn.valueOf(2, 2L), HBaseColumn.valueOf(1, 1L)), new ArrayList<>(mapped.getHistory()));
        assertEquals(0, mapped.getHistory().remainingCapacity());

        assertNotNull(mapped.getNested());
        assertInstanceOf(ArrayBlockingQueue.class, mapped.getNested().getHistory());
        assertEquals(List.of(HBaseColumn.valueOf("c", 2L), HBaseColumn.valueOf("b", 1L)), new ArrayList<>(mapped.getNested().getHistory()));
    }

    @Test
    void arrayBlockingQueueVersionedPropertyRoundTripsThroughAnyPut() {
        final QueueVersions source = new QueueVersions();
        source.setId("r1");
        source.setHistory(new ArrayBlockingQueue<>(8, false, List.of(HBaseColumn.valueOf(10, 5L), HBaseColumn.valueOf(20, 6L))));

        final QueueVersions back = HBaseExecutor.toEntity(toResult(AnyPut.create(source)), QueueVersions.class);

        assertEquals("r1", back.getId());
        assertEquals(List.of(HBaseColumn.valueOf(20, 6L), HBaseColumn.valueOf(10, 5L)), new ArrayList<>(back.getHistory()));
    }

    private static KeyValue cell(final String family, final String qualifier, final long ts, final String value) {
        return new KeyValue(Bytes.toBytes("row"), Bytes.toBytes(family), Bytes.toBytes(qualifier), ts, Bytes.toBytes(value));
    }

    // Pins the single-version dedupe (now keyed by family, then qualifier) and the per-family string reuse: the newest cell
    // of a column wins, the same qualifier in another family is a different column, and families that alternate are each
    // decoded from their own cell.
    @Test
    void singleVersionColumnsKeepTheFirstCellOfEachColumnInEachFamily() {
        final List<Cell> cells = new ArrayList<>();
        cells.add(cell("age", "", 3, "30"));
        cells.add(cell("age", "", 2, "20"));
        cells.add(cell("name", "", 5, "new"));
        cells.add(cell("name", "", 4, "old"));
        cells.add(cell("other", "", 1, "o"));

        final Plain plain = HBaseExecutor.toEntity(Result.create(cells), Plain.class);

        assertEquals("row", plain.getId());
        assertEquals(30, plain.getAge());
        assertEquals("new", plain.getName());
        assertEquals("o", plain.getOther());
    }

    // A Result whose cells are not sorted (Result.create requires sorted cells, but it does not check): families that come
    // back after another family are decoded correctly, and the first cell of a column still wins over a later one.
    @Test
    void unsortedResultKeepsTheFirstCellOfAColumnAndDecodesEveryFamily() {
        final List<Cell> cells = new ArrayList<>();
        cells.add(cell("name", "", 1, "first"));
        cells.add(cell("age", "", 1, "7"));
        cells.add(cell("name", "", 2, "second"));
        cells.add(cell("other", "", 1, "x"));
        cells.add(cell("age", "", 2, "8"));

        final Plain plain = HBaseExecutor.toEntity(Result.create(cells), Plain.class);

        assertEquals("first", plain.getName());
        assertEquals(7, plain.getAge());
        assertEquals("x", plain.getOther());
    }

    @Test
    void sameQualifierInTwoNestedBeanFamiliesIsReadIntoBoth() {
        final List<Cell> cells = new ArrayList<>();
        cells.add(cell("home", "city", 2, "Paris"));
        cells.add(cell("home", "city", 1, "Lyon"));
        cells.add(cell("work", "city", 1, "Berlin"));

        final TwoAddresses mapped = HBaseExecutor.toEntity(Result.create(cells), TwoAddresses.class);

        assertEquals("Paris", mapped.getHome().getCity());
        assertEquals("Berlin", mapped.getWork().getCity());
    }

    public static class Plain {
        @Id
        private String id;
        private String name;
        private int age;
        private String other;

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

        public int getAge() {
            return age;
        }

        public void setAge(final int age) {
            this.age = age;
        }

        public String getOther() {
            return other;
        }

        public void setOther(final String other) {
            this.other = other;
        }
    }

    public static class TwoAddresses {
        @Id
        private String id;
        private Address home;
        private Address work;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public Address getHome() {
            return home;
        }

        public void setHome(final Address home) {
            this.home = home;
        }

        public Address getWork() {
            return work;
        }

        public void setWork(final Address work) {
            this.work = work;
        }
    }

    public static class Address {
        private String city;

        public String getCity() {
            return city;
        }

        public void setCity(final String city) {
            this.city = city;
        }
    }

    @ColumnFamily("cf")
    public static class QueueVersions {
        @Id
        private String id;
        private ArrayBlockingQueue<HBaseColumn<Integer>> history;
        @ColumnFamily("nested")
        private NestedQueueVersions nested;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public ArrayBlockingQueue<HBaseColumn<Integer>> getHistory() {
            return history;
        }

        public void setHistory(final ArrayBlockingQueue<HBaseColumn<Integer>> history) {
            this.history = history;
        }

        public NestedQueueVersions getNested() {
            return nested;
        }

        public void setNested(final NestedQueueVersions nested) {
            this.nested = nested;
        }
    }

    public static class NestedQueueVersions {
        private ArrayBlockingQueue<HBaseColumn<String>> history;

        public ArrayBlockingQueue<HBaseColumn<String>> getHistory() {
            return history;
        }

        public void setHistory(final ArrayBlockingQueue<HBaseColumn<String>> history) {
            this.history = history;
        }
    }
}
