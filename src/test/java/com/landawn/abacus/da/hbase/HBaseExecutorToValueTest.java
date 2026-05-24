/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

        try (HBaseExecutor executor = new HBaseExecutor(conn)) {
            final CoprocessorRpcChannel returned = executor.coprocessorService("any-table", "row-1");
            assertSame(channel, returned, "Returned channel must be the one obtained from Table");
        }

        // The Table acquired for the coprocessorService call must be closed exactly once
        // (executor.close() does not touch any per-call Tables — it only closes Admin + Connection).
        verify(table, times(1)).close();
    }
}
