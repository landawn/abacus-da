/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase.annotation;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.annotation.Column;
import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.da.hbase.AnyPut;
import com.landawn.abacus.da.hbase.HBaseExecutor;

// Pins the ColumnFamily Javadoc on @Column: without a family annotation a scalar field's @Column value is the
// qualifier in the field-name family, and an entity field with @Column is stored as one cell instead of being
// flattened, whether its family is the field name, the class-level family or a field-level family.
class ColumnFamilyReview20261103Test extends TestBase {

    public static class Name {
        private String firstName;
        private String lastName;

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(final String firstName) {
            this.firstName = firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public void setLastName(final String lastName) {
            this.lastName = lastName;
        }
    }

    public static class Plain {
        @Id
        private String id;
        @Column("g")
        private String guid;
        @Column("nm")
        private Name name;
        private Name flat;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getGuid() {
            return guid;
        }

        public void setGuid(final String guid) {
            this.guid = guid;
        }

        public Name getName() {
            return name;
        }

        public void setName(final Name name) {
            this.name = name;
        }

        public Name getFlat() {
            return flat;
        }

        public void setFlat(final Name flat) {
            this.flat = flat;
        }
    }

    @ColumnFamily("cf")
    public static class Annotated {
        @Id
        private String id;
        @Column("nm")
        private Name name;
        @ColumnFamily("fam2")
        @Column("nm2")
        private Name name2;
        @ColumnFamily("fam3")
        private Name name3;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public Name getName() {
            return name;
        }

        public void setName(final Name name) {
            this.name = name;
        }

        public Name getName2() {
            return name2;
        }

        public void setName2(final Name name2) {
            this.name2 = name2;
        }

        public Name getName3() {
            return name3;
        }

        public void setName3(final Name name3) {
            this.name3 = name3;
        }
    }

    private static Name name() {
        final Name name = new Name();
        name.setFirstName("F");
        name.setLastName("L");
        return name;
    }

    // family:qualifier -> value, sorted
    private static TreeMap<String, String> cells(final AnyPut put) {
        final TreeMap<String, String> cells = new TreeMap<>();

        for (final List<Cell> familyCells : put.val().getFamilyCellMap().values()) {
            for (final Cell cell : familyCells) {
                cells.put(Bytes.toString(CellUtil.cloneFamily(cell)) + ":" + Bytes.toString(CellUtil.cloneQualifier(cell)),
                        Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

        return cells;
    }

    private static <T> T readBack(final AnyPut put, final Class<T> entityClass) {
        final List<Cell> all = new ArrayList<>();
        put.val().getFamilyCellMap().values().forEach(all::addAll);
        all.sort(CellComparator.getInstance());

        return HBaseExecutor.toEntity(Result.create(all), entityClass);
    }

    @Test
    void columnAnnotationGivesTheQualifierAndKeepsAnEntityFieldInOneCell() {
        final Plain plain = new Plain();
        plain.setId("r1");
        plain.setGuid("G");
        plain.setName(name());
        plain.setFlat(name());

        final Annotated annotated = new Annotated();
        annotated.setId("r2");
        annotated.setName(name());
        annotated.setName2(name());
        annotated.setName3(name());

        final AnyPut plainPut = AnyPut.create(plain);
        final AnyPut annotatedPut = AnyPut.create(annotated);
        final TreeMap<String, String> plainCells = cells(plainPut);
        final TreeMap<String, String> annotatedCells = cells(annotatedPut);
        final Plain plainBack = readBack(plainPut, Plain.class);
        final Annotated annotatedBack = readBack(annotatedPut, Annotated.class);

        assertAll(() -> assertEquals("[flat:firstName, flat:lastName, guid:g, name:nm]", plainCells.keySet().toString()),
                () -> assertEquals("G", plainCells.get("guid:g")),
                () -> assertEquals("[cf:nm, fam2:nm2, fam3:firstName, fam3:lastName]", annotatedCells.keySet().toString()),
                () -> assertEquals("G", plainBack.getGuid()), () -> assertEquals("F", plainBack.getName().getFirstName()),
                () -> assertEquals("L", plainBack.getFlat().getLastName()), () -> assertEquals("L", annotatedBack.getName().getLastName()),
                () -> assertEquals("F", annotatedBack.getName2().getFirstName()), () -> assertEquals("L", annotatedBack.getName3().getLastName()));
    }
}
