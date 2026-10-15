package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.client.Cursor;
import org.apache.hadoop.hbase.client.Operation;
import org.apache.hadoop.hbase.client.Result;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.exception.UncheckedIOException;
import com.landawn.abacus.util.HBaseColumn;
import com.landawn.abacus.util.NamingPolicy;

class HBaseWrapperBoundaryTest {

    @Test
    void objectRowKeySliceFactoriesUseOnlyTheSelectedBytes() {
        final byte[] expected = "row".getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(expected, AnyAppend.of("xxrowyy", 2, 3).getRow());
        assertArrayEquals(expected, AnyIncrement.of("xxrowyy", 2, 3).getRow());
        assertFalse(AnyAppend.of("row").equals(AnyAppend.of("another")));
        assertFalse(AnyIncrement.of("row").equals(AnyIncrement.of("another")));
    }

    @Test
    void cursorScanStartsAtTheCursorRowAndRejectsMissingCursor() {
        final Cursor cursor = mock(Cursor.class);
        final byte[] row = { 1, 2, 3 };
        when(cursor.getRow()).thenReturn(row);
        assertArrayEquals(row, AnyScan.createScanFromCursor(cursor).getStartRow());
        assertThrows(IllegalArgumentException.class, () -> AnyScan.createScanFromCursor(null));
    }

    @Test
    void jsonSerializationFailuresKeepTheDriverCauseForBothOverloads() throws Exception {
        final Operation operation = mock(Operation.class);
        final IOException failure = new IOException("serialization failure");
        when(operation.toJSON()).thenThrow(failure);
        when(operation.toJSON(7)).thenThrow(failure);
        final TestOperation wrapper = new TestOperation(operation);
        assertSame(failure, assertThrows(UncheckedIOException.class, wrapper::toJson).getCause());
        assertSame(failure, assertThrows(UncheckedIOException.class, () -> wrapper.toJson(7)).getCause());
    }

    @Test
    void collectionFactoriesRetainExistingPutsAndValidateSelectedBeanProperties() {
        final AnyPut put = AnyPut.of("row").addColumn("family", "name", "Ada");
        assertSame(put, AnyPut.create(List.of(put), NamingPolicy.SNAKE_CASE).getFirst());
        assertSame(put, AnyPut.create(List.of(put), List.of("name")).getFirst());
        assertSame(put, AnyPut.create(List.of(put), List.of("name"), NamingPolicy.SNAKE_CASE).getFirst());
        assertSame(put.val(), AnyPut.toPut(List.of(put), NamingPolicy.SNAKE_CASE).getFirst());
        final HBaseExecutorToValueTest.VersionedEntity entity = new HBaseExecutorToValueTest.VersionedEntity();
        entity.setId(7);
        assertThrows(IllegalArgumentException.class, () -> AnyPut.create(entity, List.of("missing")));
    }

    @Test
    void versionedCollectionsAndMapsRoundTripAtBothBeanLevels() {
        final HBaseExecutorToValueTest.VersionedNested nested = new HBaseExecutorToValueTest.VersionedNested("Ada", HBaseColumn.valueOf("single", 7L),
                List.of(HBaseColumn.valueOf(1.5d, 8L), HBaseColumn.valueOf(2.5d, 6L)), Map.of(9L, HBaseColumn.valueOf("nested", 9L)));
        final HBaseExecutorToValueTest.VersionedEntity entity = new HBaseExecutorToValueTest.VersionedEntity(7L, "plain", HBaseColumn.valueOf("one", 5L),
                List.of(HBaseColumn.valueOf(3.5d, 4L), HBaseColumn.valueOf(4.5d, 2L)), Map.of(3L, HBaseColumn.valueOf("top", 3L)), nested);
        final AnyPut put = AnyPut.create(entity);
        final List<Cell> cells = new ArrayList<>();
        put.val().getFamilyCellMap().values().forEach(cells::addAll);
        cells.sort(CellComparator.getInstance());
        final var restored = HBaseExecutor.toValue(Result.create(cells), HBaseExecutorToValueTest.VersionedEntity.class);
        assertEquals(entity, restored);
        assertEquals(3L, put.get("cf", "byVersion").getFirst().getTimestamp());
        assertEquals(9L, put.get("nf", "byVersion").getFirst().getTimestamp());
    }

    private static final class TestOperation extends AnyOperation<TestOperation> {
        TestOperation(final Operation operation) {
            super(operation);
        }
    }
}
