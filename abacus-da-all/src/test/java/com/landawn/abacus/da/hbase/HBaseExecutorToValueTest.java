/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.annotation.Column;
import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.da.hbase.annotation.ColumnFamily;
import com.landawn.abacus.util.N;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Pure unit tests (no live HBase) for {@link HBaseExecutor#toEntity(Result, Class)} /
 * {@code toValue} bean conversion.
 *
 * <p>Regression test for the double-{@code Result.advance()} bug: the 3-arg {@code toValue}
 * advanced the {@code Result}'s internal cell cursor and then delegated to the 6-arg
 * {@code toValue} which advanced it a second time. For a {@code Result} with exactly one
 * cell, the second {@code advance()} returned {@code false} (cursor index == cells.length),
 * causing the entity to be (incorrectly) treated as empty and {@code null} returned instead
 * of a mapped entity.</p>
 */
public class HBaseExecutorToValueTest {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @ColumnFamily("cf")
    public static class SingleColumnEntity {
        @Id
        private String id;
        @Column("v")
        private String value;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @ColumnFamily
    public static class EntityWithDefaultColumnFamilyAnnotation {
        @Id
        private String id;
        private String value;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @ColumnFamily("cf")
    public static class EntityWithByteBufferProperty {
        @Id
        private String id;
        @Column("data")
        private ByteBuffer data;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @ColumnFamily("cf")
    public static class EntityWithByteBufferRowKey {
        @Id
        private ByteBuffer id;
        @Column("v")
        private String value;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FullName {
        private String firstName;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PostalAddress {
        private String street;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @ColumnFamily("cf")
    public static class CustomerWithTwoNestedBeans {
        @Id
        private String id;
        private FullName name;
        private PostalAddress addr;
    }

    /**
     * A bean-mapped Result that contains exactly ONE cell must still be converted to a
     * populated entity (row key derived from the cell's row, the single column mapped).
     * Before the fix this returned {@code null}.
     */
    @Test
    public void test_toEntity_singleCellResult_isNotTreatedAsEmpty() {
        final byte[] row = Bytes.toBytes("row-1");
        final Cell cell = new KeyValue(row, Bytes.toBytes("cf"), Bytes.toBytes("v"), Bytes.toBytes("hello"));

        final Result result = Result.create(List.<Cell> of(cell));

        final SingleColumnEntity entity = HBaseExecutor.toEntity(result, SingleColumnEntity.class);

        assertNotNull(entity, "Single-cell bean Result must not be converted to null");
        assertEquals("row-1", entity.getId());
        assertEquals("hello", entity.getValue());
    }

    /**
     * Sanity check: a multi-cell bean Result still maps correctly (unchanged behavior).
     */
    @Test
    public void test_toEntity_multiCellResult_stillMaps() {
        final byte[] row = Bytes.toBytes("row-2");
        final Cell c1 = new KeyValue(row, Bytes.toBytes("cf"), Bytes.toBytes("v"), Bytes.toBytes("world"));
        final Cell c2 = new KeyValue(row, Bytes.toBytes("cf"), Bytes.toBytes("v2"), Bytes.toBytes("ignored"));

        final Result result = Result.create(N.asList(c1, c2));

        final SingleColumnEntity entity = HBaseExecutor.toEntity(result, SingleColumnEntity.class);

        assertNotNull(entity);
        assertEquals("row-2", entity.getId());
        assertEquals("world", entity.getValue());
    }

    @Test
    public void test_toEntity_emptyColumnFamilyAnnotationUsesDefaultMapping() {
        final byte[] row = Bytes.toBytes("row-default-cf");
        final Cell cell = new KeyValue(row, Bytes.toBytes("value"), Bytes.toBytes(HBaseExecutor.EMPTY_QUALIFIER), Bytes.toBytes("hello"));

        final Result result = Result.create(List.<Cell> of(cell));

        final EntityWithDefaultColumnFamilyAnnotation entity = HBaseExecutor.toEntity(result, EntityWithDefaultColumnFamilyAnnotation.class);

        assertNotNull(entity);
        assertEquals("row-default-cf", entity.getId());
        assertEquals("hello", entity.getValue());
    }

    /**
     * Sanity check: an empty Result still yields {@code null} (default value) for a bean type.
     */
    @Test
    public void test_toEntity_emptyResult_returnsNull() {
        final Result result = Result.create(List.<Cell> of());

        final SingleColumnEntity entity = HBaseExecutor.toEntity(result, SingleColumnEntity.class);

        org.junit.jupiter.api.Assertions.assertNull(entity);
    }

    /**
     * Non-bean (single value) path regression: the 3-arg {@code toValue} calls
     * {@code result.advance()} as an emptiness probe and then the non-bean branch re-reads
     * the value via a fresh {@code result.cellScanner()}. A single-cell Result must still
     * yield the cell value (not the type default), guarding against a regression where the
     * non-bean branch is "optimized" to reuse the already-advanced internal cursor.
     */
    @Test
    public void test_toEntity_singleCellResult_singleValueType() {
        final byte[] row = Bytes.toBytes("row-3");
        final Cell cell = new KeyValue(row, Bytes.toBytes("cf"), Bytes.toBytes("v"), Bytes.toBytes("scalar-value"));

        final Result result = Result.create(List.<Cell> of(cell));

        final String value = HBaseExecutor.toEntity(result, String.class);

        assertEquals("scalar-value", value);
    }

    @Test
    public void test_toEntity_multiVersionScalarUsesNewestCell() {
        final byte[] row = Bytes.toBytes("row-versions");
        final Cell newest = new KeyValue(row, Bytes.toBytes("cf"), Bytes.toBytes("v"), 200L, Bytes.toBytes("new"));
        final Cell oldest = new KeyValue(row, Bytes.toBytes("cf"), Bytes.toBytes("v"), 100L, Bytes.toBytes("old"));

        final SingleColumnEntity entity = HBaseExecutor.toEntity(Result.create(N.asList(newest, oldest)), SingleColumnEntity.class);

        assertNotNull(entity);
        assertEquals("new", entity.getValue());
    }

    @Test
    public void test_toEntity_rawByteArrayScalarDoesNotDecodeAsString() {
        final byte[] raw = new byte[] { 0, (byte) 0xff, 7 };
        final Cell cell = new KeyValue(Bytes.toBytes("row-bytes"), Bytes.toBytes("cf"), Bytes.toBytes("v"), raw);

        final byte[] actual = HBaseExecutor.toEntity(Result.create(List.<Cell> of(cell)), byte[].class);

        assertArrayEquals(raw, actual);
    }

    /**
     * Non-bean path: an empty Result must yield the type default (null for String), exercising
     * the {@code result.isEmpty()} short-circuit in the 3-arg {@code toValue}.
     */
    @Test
    public void test_toEntity_emptyResult_singleValueType_returnsDefault() {
        final Result result = Result.create(List.<Cell> of());

        final String value = HBaseExecutor.toEntity(result, String.class);

        org.junit.jupiter.api.Assertions.assertNull(value);
    }

    /**
     * Regression: {@link HBaseExecutor#coprocessorService(String, Object)} used to open a
     * {@link Table} via {@code getTable(tableName)} and return the {@link CoprocessorRpcChannel}
     * without ever closing the Table — leaking one Table (and its associated client state) on
     * every call. The returned channel is backed by the underlying {@link Connection}, not the
     * Table, so the Table is safe to close before returning.
     *
     * <p>This test verifies the Table is closed by stubbing {@code Connection}/{@code Table}
     * with Mockito (no live HBase cluster).</p>
     */
    @Test
    public void test_coprocessorService_singleRow_closesTable() throws Exception {
        final Connection conn = mock(Connection.class);
        final Admin admin = mock(Admin.class);
        final Table table = mock(Table.class);
        final CoprocessorRpcChannel channel = mock(CoprocessorRpcChannel.class);

        when(conn.getAdmin()).thenReturn(admin);
        when(conn.getTable(any(TableName.class))).thenReturn(table);
        when(table.coprocessorService(any(byte[].class))).thenReturn(channel);
        HBaseExecutor executor = new HBaseExecutor(conn);

        try {
            final CoprocessorRpcChannel returned = executor.coprocessorService("any-table", "row-1");
            assertSame(channel, returned, "Returned channel must be the one obtained from Table");
        } finally {
            executor.close();
        }

        // The Table acquired for the coprocessorService call must be closed exactly once
        // (executor.close() does not touch any per-call Tables — it only closes Admin + Connection).
        verify(table, times(1)).close();
    }

    // ---------------------------------------------------------------------
    // Regression: toValue must not consume the Result's CellScanner cursor
    // ---------------------------------------------------------------------

    /**
     * Regression: {@code toValue} used to probe emptiness with {@code result.advance()},
     * consuming the {@link Result}'s single internal cell cursor. After a full conversion the
     * cursor sat at end-of-cells, so converting the SAME {@code Result} a second time threw
     * {@link java.util.NoSuchElementException} from {@code Result.advance()}. Both conversions
     * must now succeed and yield equal entities.
     */
    @Test
    public void test_toEntity_sameResultConvertedTwice_bean() {
        final byte[] row = Bytes.toBytes("row-twice");
        final Cell cell = new KeyValue(row, Bytes.toBytes("cf"), Bytes.toBytes("v"), Bytes.toBytes("hello"));
        final Result result = Result.create(List.<Cell> of(cell));

        final SingleColumnEntity first = HBaseExecutor.toEntity(result, SingleColumnEntity.class);
        final SingleColumnEntity second = HBaseExecutor.toEntity(result, SingleColumnEntity.class);

        assertNotNull(first);
        assertNotNull(second);
        assertEquals(first, second, "Converting the same Result twice must yield equal entities");
        assertEquals("row-twice", second.getId());
        assertEquals("hello", second.getValue());
    }

    /**
     * Same double-conversion regression for the non-bean (single value) branch: the probing
     * {@code result.advance()} consumed the cursor there too.
     */
    @Test
    public void test_toEntity_sameResultConvertedTwice_scalar() {
        final byte[] row = Bytes.toBytes("row-twice-scalar");
        final Cell cell = new KeyValue(row, Bytes.toBytes("cf"), Bytes.toBytes("v"), Bytes.toBytes("scalar"));
        final Result result = Result.create(List.<Cell> of(cell));

        final String first = HBaseExecutor.toEntity(result, String.class);
        final String second = HBaseExecutor.toEntity(result, String.class);

        assertEquals("scalar", first);
        assertEquals("scalar", second);
    }

    /**
     * Regression: a caller may legitimately have iterated the {@code Result} (which implements
     * {@code CellScanner}) before handing it to {@code toEntity}. The conversion must reset the
     * cursor internally (via {@code result.cellScanner()}) instead of trusting/advancing the
     * already-exhausted cursor.
     */
    @Test
    public void test_toEntity_afterCallerDrainsCellScanner_stillConverts() throws Exception {
        final byte[] row = Bytes.toBytes("row-drained");
        final Cell c1 = new KeyValue(row, Bytes.toBytes("cf"), Bytes.toBytes("v"), Bytes.toBytes("drained-value"));
        final Cell c2 = new KeyValue(row, Bytes.toBytes("cf"), Bytes.toBytes("v2"), Bytes.toBytes("ignored"));
        final Result result = Result.create(N.asList(c1, c2));

        // Drain the Result's internal cell cursor, as an external caller might.
        while (result.advance()) {
            // consume all cells
        }

        final SingleColumnEntity entity = HBaseExecutor.toEntity(result, SingleColumnEntity.class);

        assertNotNull(entity, "Conversion after the caller drained the cell scanner must still work");
        assertEquals("row-drained", entity.getId());
        assertEquals("drained-value", entity.getValue());
    }

    // ---------------------------------------------------------------------
    // Regression: ByteBuffer properties / row keys are read back as raw bytes
    // ---------------------------------------------------------------------

    /**
     * Regression: a {@link ByteBuffer}-typed property used to be read back through
     * {@code Type.valueOf(String)}, which base64-decoded the UTF-8 string of the raw cell bytes
     * and corrupted the value. The read path must now wrap the raw cell bytes symmetrically with
     * the write side ({@code toValueBytes} stores a ByteBuffer's raw remaining bytes).
     */
    @Test
    public void test_toEntity_byteBufferProperty_wrapsRawCellBytes() {
        final byte[] raw = new byte[] { 0, (byte) 0xff, 7, (byte) 0x80, 65 };
        final Cell cell = new KeyValue(Bytes.toBytes("row-bb"), Bytes.toBytes("cf"), Bytes.toBytes("data"), raw);

        final EntityWithByteBufferProperty entity = HBaseExecutor.toEntity(Result.create(List.<Cell> of(cell)), EntityWithByteBufferProperty.class);

        assertNotNull(entity);
        assertEquals("row-bb", entity.getId());
        assertNotNull(entity.getData(), "ByteBuffer property must be populated");
        assertEquals(ByteBuffer.wrap(raw), entity.getData(), "ByteBuffer property must wrap the raw cell bytes unmodified");
    }

    /**
     * Same symmetry regression for a {@link ByteBuffer}-typed row key: it must wrap the raw row
     * bytes instead of being base64-decoded from their string form.
     */
    @Test
    public void test_toEntity_byteBufferRowKey_wrapsRawRowBytes() {
        final byte[] rawRow = new byte[] { 1, (byte) 0xc3, 0, (byte) 0xfe, 42 };
        final Cell cell = new KeyValue(rawRow, Bytes.toBytes("cf"), Bytes.toBytes("v"), Bytes.toBytes("hello"));

        final EntityWithByteBufferRowKey entity = HBaseExecutor.toEntity(Result.create(List.<Cell> of(cell)), EntityWithByteBufferRowKey.class);

        assertNotNull(entity);
        assertNotNull(entity.getId(), "ByteBuffer row key must be populated");
        assertEquals(ByteBuffer.wrap(rawRow), entity.getId(), "ByteBuffer row key must wrap the raw row bytes unmodified");
        assertEquals("hello", entity.getValue());
    }

    // ---------------------------------------------------------------------
    // Regression: two nested beans sharing one class-level column family
    // ---------------------------------------------------------------------

    /**
     * Regression: with a class-level {@code @ColumnFamily("cf")} and TWO bean-typed properties
     * sharing that family, the family map kept a single empty-qualifier fallback entry which the
     * last-registered bean property overwrote — so on read, cells belonging to the other nested
     * bean were silently dropped. Cells of BOTH nested beans must now be routed to the bean that
     * actually owns the qualifier.
     */
    @Test
    public void test_toEntity_twoNestedBeansSharingColumnFamily_bothPopulated() {
        final byte[] row = Bytes.toBytes("row-multi");
        final Cell nameCell = new KeyValue(row, Bytes.toBytes("cf"), Bytes.toBytes("firstName"), Bytes.toBytes("John"));
        final Cell addrCell = new KeyValue(row, Bytes.toBytes("cf"), Bytes.toBytes("street"), Bytes.toBytes("Main St"));

        final CustomerWithTwoNestedBeans entity = HBaseExecutor.toEntity(Result.create(N.asList(nameCell, addrCell)), CustomerWithTwoNestedBeans.class);

        assertNotNull(entity);
        assertEquals("row-multi", entity.getId());
        assertNotNull(entity.getName(), "Cells of the first nested bean must not be dropped");
        assertEquals("John", entity.getName().getFirstName());
        assertNotNull(entity.getAddr(), "Cells of the second nested bean must not be dropped");
        assertEquals("Main St", entity.getAddr().getStreet());
    }

    // ---------------------------------------------------------------------
    // Regression: multi-cell-to-scalar rejection names the unexpected EXTRA column
    // ---------------------------------------------------------------------

    /**
     * Regression: converting a multi-cell Result to a scalar type is rejected, and the message
     * used to name the FIRST cell (the one that was successfully converted) instead of the
     * unexpected EXTRA column. The message must now name the extra (second) cell's column.
     */
    @Test
    public void test_toEntity_multiCellResultToScalar_messageNamesExtraColumn() {
        final byte[] row = Bytes.toBytes("row-extra");
        final Cell c1 = new KeyValue(row, Bytes.toBytes("cf"), Bytes.toBytes("q1"), Bytes.toBytes("v1"));
        final Cell c2 = new KeyValue(row, Bytes.toBytes("cf"), Bytes.toBytes("q2"), Bytes.toBytes("v2"));
        final Result result = Result.create(N.asList(c1, c2));

        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.toEntity(result, String.class));

        assertNotNull(ex.getMessage());
        assertTrue(ex.getMessage().contains("cf:q2"), "Message must name the unexpected extra column cf:q2, but was: " + ex.getMessage());
    }
}
