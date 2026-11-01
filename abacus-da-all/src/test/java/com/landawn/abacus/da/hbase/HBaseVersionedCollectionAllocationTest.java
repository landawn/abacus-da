package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.AllocationAssertions;
import com.landawn.abacus.da.hbase.annotation.ColumnFamily;
import com.landawn.abacus.util.HBaseColumn;

class HBaseVersionedCollectionAllocationTest {

    @Test
    void copyOnWriteVersionListsKeepOrderDuplicateValuesAndNullPayloads() {
        final List<Cell> cells = new ArrayList<>();
        addVersions(cells, "cf", "history", "7", "", "7");
        addVersions(cells, "nested", "history", "7", "", "7");
        cells.sort(CellComparator.getInstance());

        final VersionedRow mapped = HBaseExecutor.toEntity(Result.create(cells), VersionedRow.class);
        final List<HBaseColumn<Integer>> expected = List.of(HBaseColumn.valueOf(7, 3L), HBaseColumn.<Integer> valueOf(null, 2L), HBaseColumn.valueOf(7, 1L));

        assertInstanceOf(CopyOnWriteArrayList.class, mapped.getHistory());
        assertEquals(expected, mapped.getHistory());
        assertInstanceOf(CopyOnWriteArrayList.class, mapped.getNested().getHistory());
        assertEquals(expected, mapped.getNested().getHistory());
    }

    @Test
    void immutableBeanReceivesTheCompletedCopyOnWriteList() {
        final List<Cell> cells = new ArrayList<>();
        addVersions(cells, "cf", "history", "3", "2", "1");

        final VersionedRecord mapped = HBaseExecutor.toEntity(Result.create(cells), VersionedRecord.class);

        assertInstanceOf(CopyOnWriteArrayList.class, mapped.history());
        assertEquals(List.of(HBaseColumn.valueOf(3, 3L), HBaseColumn.valueOf(2, 2L), HBaseColumn.valueOf(1, 1L)), mapped.history());
    }

    @Test
    void customCopyOnWriteSubclassKeepsItsPerCellAddBehavior() {
        final List<Cell> cells = new ArrayList<>();
        addVersions(cells, "cf", "history", "3", "2", "1");

        final SubclassVersions mapped = HBaseExecutor.toEntity(Result.create(cells), SubclassVersions.class);

        assertEquals(3, mapped.getHistory().addCalls);
        assertEquals(List.of(HBaseColumn.valueOf(3, 3L), HBaseColumn.valueOf(2, 2L), HBaseColumn.valueOf(1, 1L)), mapped.getHistory());
    }

    @Test
    void copyOnWriteVersionListAllocationGrowsLinearly() {
        final Result small = versionedResult(256);
        final Result large = versionedResult(1024);

        assertEquals(256, HBaseExecutor.toEntity(small, VersionedRow.class).getHistory().size());
        assertEquals(1024, HBaseExecutor.toEntity(large, VersionedRow.class).getHistory().size());
        AllocationAssertions.assertLinearAllocation("HBase copy-on-write versioned columns", () -> HBaseExecutor.toEntity(small, VersionedRow.class),
                () -> HBaseExecutor.toEntity(large, VersionedRow.class));
    }

    private static Result versionedResult(final int size) {
        final List<Cell> cells = new ArrayList<>(size);
        final byte[] row = Bytes.toBytes("row");
        final byte[] family = Bytes.toBytes("cf");
        final byte[] qualifier = Bytes.toBytes("history");
        final byte[] value = Bytes.toBytes("7");

        for (int i = size; i > 0; i--) {
            cells.add(new KeyValue(row, family, qualifier, i, value));
        }

        return Result.create(cells);
    }

    private static void addVersions(final List<Cell> cells, final String family, final String qualifier, final String... values) {
        for (int i = 0; i < values.length; i++) {
            cells.add(new KeyValue(Bytes.toBytes("row"), Bytes.toBytes(family), Bytes.toBytes(qualifier), values.length - i, Bytes.toBytes(values[i])));
        }
    }

    @ColumnFamily("cf")
    public static class VersionedRow {
        private CopyOnWriteArrayList<HBaseColumn<Integer>> history;
        @ColumnFamily("nested")
        private NestedVersions nested;

        public CopyOnWriteArrayList<HBaseColumn<Integer>> getHistory() {
            return history;
        }

        public void setHistory(final CopyOnWriteArrayList<HBaseColumn<Integer>> history) {
            this.history = history;
        }

        public NestedVersions getNested() {
            return nested;
        }

        public void setNested(final NestedVersions nested) {
            this.nested = nested;
        }
    }

    public static class NestedVersions {
        private CopyOnWriteArrayList<HBaseColumn<Integer>> history;

        public CopyOnWriteArrayList<HBaseColumn<Integer>> getHistory() {
            return history;
        }

        public void setHistory(final CopyOnWriteArrayList<HBaseColumn<Integer>> history) {
            this.history = history;
        }
    }

    @ColumnFamily("cf")
    public record VersionedRecord(CopyOnWriteArrayList<HBaseColumn<Integer>> history) {
    }

    @ColumnFamily("cf")
    public static class SubclassVersions {
        private ObservedHistory<HBaseColumn<Integer>> history;

        public ObservedHistory<HBaseColumn<Integer>> getHistory() {
            return history;
        }

        public void setHistory(final ObservedHistory<HBaseColumn<Integer>> history) {
            this.history = history;
        }
    }

    public static class ObservedHistory<E> extends CopyOnWriteArrayList<E> {
        private static final long serialVersionUID = 1L;
        private int addCalls;

        @Override
        public boolean add(final E value) {
            addCalls++;
            return super.add(value);
        }
    }
}
