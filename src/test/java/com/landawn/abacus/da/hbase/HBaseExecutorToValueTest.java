/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
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
}
