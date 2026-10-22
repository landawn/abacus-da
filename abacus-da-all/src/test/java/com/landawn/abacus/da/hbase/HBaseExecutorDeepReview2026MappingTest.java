/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.annotation.AccessFieldByMethod;
import com.landawn.abacus.annotation.Column;
import com.landawn.abacus.annotation.Entity;
import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.annotation.JsonXmlField;
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

    // ---- 2026-10-20 hbnfix: read-only rule vs JSON/XML direction, @Column-mapped read-only nested bean, strict Boolean ----

    private static Result rowOf(final String row, final String family, final String... qualifierValuePairs) {
        final byte[] rowBytes = Bytes.toBytes(row);
        final List<Cell> cells = new ArrayList<>();

        for (int i = 0; i < qualifierValuePairs.length; i += 2) {
            cells.add(new KeyValue(rowBytes, Bytes.toBytes(family), Bytes.toBytes(qualifierValuePairs[i]), Bytes.toBytes(qualifierValuePairs[i + 1])));
        }

        cells.sort(CellComparator.getInstance());

        return Result.create(cells);
    }

    // The JSON/XML direction is not the read-only rule: a declared SERIALIZE_ONLY property whose only write path is its
    // setter (the field is off limits under @AccessFieldByMethod) is still assigned on read, as DynamoDB item mapping does.
    @Test
    void serializeOnlyPropertyWithASetterIsWrittenOnRead() {
        final SerializeOnlyEntity back = HBaseExecutor.toEntity(rowOf("r1", "cf", "nick", "n1", "name", "k"), SerializeOnlyEntity.class);

        assertAll(() -> assertEquals("r1", back.getId()), () -> assertEquals("n1", back.getNick()), () -> assertEquals("k", back.getName()));
    }

    // ... and a read-only property is skipped even when its field declares DESERIALIZE_ONLY: there is nothing to assign
    // through (setPropValue would throw UnsupportedOperationException), and the rest of the row is still read.
    @Test
    void deserializeOnlyReadOnlyPropertyIsSkippedAndTheRestOfTheRowIsRead() {
        final DeserializeOnlyReadOnlyEntity back = HBaseExecutor.toEntity(rowOf("r2", "cf", "code", "stored", "name", "k"),
                DeserializeOnlyReadOnlyEntity.class);

        assertAll(() -> assertEquals("r2", back.getId()), () -> assertEquals("c0", back.getCode()), () -> assertEquals("k", back.getName()));
    }

    // Only a FLATTENED read-only nested bean is filled in place; one mapped as a single @Column cell is skipped like any
    // other read-only property, and the sibling columns are still read.
    @Test
    void readOnlyNestedBeanMappedAsASingleColumnCellIsSkipped() {
        final ColumnMappedReadOnlyNestedHolder back = HBaseExecutor.toEntity(rowOf("r3", "cf", "n", "{\"firstName\":\"Bob\"}", "name", "k"),
                ColumnMappedReadOnlyNestedHolder.class);

        assertAll(() -> assertEquals("r3", back.getId()), () -> assertEquals("k", back.getName()), () -> assertNotNull(back.getNested()),
                () -> assertNull(back.getNested().getFirstName(), "the single-cell nested bean must not be filled in place"));
    }

    private static FlagEntity readFlags(final String flagText, final String boxedText) {
        return HBaseExecutor.toEntity(rowOf("r", "cf", "flag", flagText, "boxed", boxedText), FlagEntity.class);
    }

    @Test
    void booleanCellsFollowTheStrictTextRule() {
        assertAll(() -> assertTrue(readFlags("true", "TRUE").isFlag()), () -> assertEquals(Boolean.TRUE, readFlags("true", "TRUE").getBoxed()),
                () -> assertTrue(readFlags("1", "y").isFlag()), () -> assertEquals(Boolean.TRUE, readFlags("1", "y").getBoxed()),
                () -> assertFalse(readFlags("0", "N").isFlag()), () -> assertEquals(Boolean.FALSE, readFlags("0", "N").getBoxed()),
                () -> assertFalse(readFlags("-1", "false").isFlag()), () -> assertEquals(Boolean.FALSE, readFlags("-1", "false").getBoxed()));
    }

    @Test
    void paddedBooleanCellIsTrimmed() {
        final FlagEntity back = readFlags(" TRUE ", " y ");

        assertAll(() -> assertTrue(back.isFlag()), () -> assertEquals(Boolean.TRUE, back.getBoxed()));
    }

    @Test
    void blankBooleanCellYieldsFalseForPrimitiveAndNullForWrapper() {
        final FlagEntity empty = readFlags("", "");
        final FlagEntity blank = readFlags("  ", " ");

        assertAll(() -> assertEquals("r", empty.getId()), () -> assertFalse(empty.isFlag()), () -> assertNull(empty.getBoxed()),
                () -> assertFalse(blank.isFlag()), () -> assertNull(blank.getBoxed()));
    }

    @Test
    void unrecognizedBooleanTextIsRejected() {
        assertAll(() -> assertThrows(IllegalArgumentException.class, () -> readFlags("yes", "true")),
                () -> assertThrows(IllegalArgumentException.class, () -> readFlags("true", "yes")),
                () -> assertThrows(IllegalArgumentException.class, () -> readFlags("2", "true")));
    }

    @Entity
    @AccessFieldByMethod
    @ColumnFamily("cf")
    public static class SerializeOnlyEntity {
        @Id
        private String id;
        @JsonXmlField(direction = JsonXmlField.Direction.SERIALIZE_ONLY)
        private String nick;
        private String name;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getNick() {
            return nick;
        }

        public void setNick(final String nick) {
            this.nick = nick;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    @Entity
    @AccessFieldByMethod
    @ColumnFamily("cf")
    public static class DeserializeOnlyReadOnlyEntity {
        @Id
        private String id;
        @JsonXmlField(direction = JsonXmlField.Direction.DESERIALIZE_ONLY)
        private final String code = "c0";
        private String name;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getCode() {
            return code;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    @Entity
    @AccessFieldByMethod
    @ColumnFamily("cf")
    public static class ColumnMappedReadOnlyNestedHolder {
        @Id
        private String id;
        @Column("n")
        private final FullName nested = new FullName();
        private String name;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public FullName getNested() {
            return nested;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    @ColumnFamily("cf")
    public static class FlagEntity {
        @Id
        private String id;
        private boolean flag;
        private Boolean boxed;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public boolean isFlag() {
            return flag;
        }

        public void setFlag(final boolean flag) {
            this.flag = flag;
        }

        public Boolean getBoxed() {
            return boxed;
        }

        public void setBoxed(final Boolean boxed) {
            this.boxed = boxed;
        }
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
