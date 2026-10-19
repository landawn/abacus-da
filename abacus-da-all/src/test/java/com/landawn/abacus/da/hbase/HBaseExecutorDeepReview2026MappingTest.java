/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.da.hbase.annotation.ColumnFamily;

class HBaseExecutorDeepReview2026MappingTest {

    @Test
    void rowKeyCellDoesNotOverrideTheActualResultRowKey() {
        final byte[] row = Bytes.toBytes("actual-row");
        final List<Cell> cells = List.of(new KeyValue(row, Bytes.toBytes("cf"), Bytes.toBytes("id"), Bytes.toBytes("cell-row")),
                new KeyValue(row, Bytes.toBytes("cf"), Bytes.toBytes("value"), Bytes.toBytes("stored")));

        final RowKeyEntity entity = HBaseExecutor.toEntity(Result.create(cells), RowKeyEntity.class);

        assertNotNull(entity);
        assertEquals("actual-row", entity.getId());
        assertEquals("stored", entity.getValue());
    }

    @Test
    void scalarDeclaredBeforeNestedBeanWithTheSameFamilyRoundTrips() {
        final ScalarFirstEntity source = new ScalarFirstEntity();
        source.setId("row-1");
        source.setName("nick");
        source.setPrimary(new FullName("John"));

        final AnyPut put = AnyPut.create(source);
        final List<Cell> cells = new ArrayList<>();
        put.val().getFamilyCellMap().values().forEach(cells::addAll);
        cells.sort(CellComparator.getInstance());

        final ScalarFirstEntity restored = HBaseExecutor.toEntity(Result.create(cells), ScalarFirstEntity.class);

        assertNotNull(restored);
        assertEquals("row-1", restored.getId());
        assertEquals("nick", restored.getName());
        assertNotNull(restored.getPrimary());
        assertEquals("John", restored.getPrimary().getFirstName());
    }

    @ColumnFamily("cf")
    public static class RowKeyEntity {
        @Id
        private String id;
        private String value;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getValue() {
            return value;
        }

        public void setValue(final String value) {
            this.value = value;
        }
    }

    public static class ScalarFirstEntity {
        @Id
        private String id;
        private String name;

        @ColumnFamily("name")
        private FullName primary;

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

        public FullName getPrimary() {
            return primary;
        }

        public void setPrimary(final FullName primary) {
            this.primary = primary;
        }
    }

    public static class FullName {
        private String firstName;

        public FullName() {
        }

        FullName(final String firstName) {
            this.firstName = firstName;
        }

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(final String firstName) {
            this.firstName = firstName;
        }
    }
}
