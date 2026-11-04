/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.hbase;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.annotation.AccessFieldByMethod;
import com.landawn.abacus.annotation.Entity;
import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.util.HBaseColumn;
import com.landawn.abacus.util.N;

class HBaseExecutorReview20261103Test {

    private static KeyValue cell(final String family, final String qualifier, final long ts, final String value) {
        return new KeyValue(Bytes.toBytes("r1"), Bytes.toBytes(family), Bytes.toBytes(qualifier), ts, Bytes.toBytes(value));
    }

    private static Result sorted(final List<Cell> cells) {
        cells.sort(CellComparator.getInstance());

        return Result.create(cells);
    }

    private static Result addressRow() {
        final List<Cell> cells = new ArrayList<>();
        cells.add(cell("home", "city", 1, "Paris"));
        cells.add(cell("home", "zip", 1, "75001"));
        cells.add(cell("home", "notes", 2, "n2"));
        cells.add(cell("home", "notes", 1, "n1"));
        cells.add(cell("name", "", 1, "Ann"));

        return sorted(cells);
    }

    private static Result toResult(final AnyPut put) {
        final List<Cell> cells = new ArrayList<>();
        put.val().getFamilyCellMap().values().forEach(cells::addAll);

        return sorted(cells);
    }

    // A nested property whose getter builds a new view on each call (here from two private fields) was read into a new,
    // then dropped, instance per cell: every cell of the property was lost, also for a row AnyPut.create wrote.
    @Test
    void nestedPropertyWhoseGetterBuildsANewViewReadsBackEveryCell() {
        final ViewHolder read = HBaseExecutor.toEntity(addressRow(), ViewHolder.class);

        final ViewHolder source = new ViewHolder();
        source.setId("r9");
        source.setName("Bob");
        source.setHome(new Address("Lyon", "69001"));

        final ViewHolder roundTrip = HBaseExecutor.toEntity(toResult(AnyPut.create(source)), ViewHolder.class);

        assertAll(() -> assertEquals("r1", read.getId()), () -> assertEquals("Ann", read.getName()), () -> assertEquals("Paris", read.getHome().getCity()),
                () -> assertEquals("75001", read.getHome().getZip()),
                () -> assertEquals(List.of(HBaseColumn.valueOf("n2", 2L), HBaseColumn.valueOf("n1", 1L)), read.getHome().getNotes()),
                () -> assertEquals("r9", roundTrip.getId()), () -> assertEquals("Lyon", roundTrip.getHome().getCity()),
                () -> assertEquals("69001", roundTrip.getHome().getZip()));
    }

    // Same loss for a held nested bean whose getter returns a copy (@AccessFieldByMethod): the filled instance is now handed
    // to the setter after the last cell.
    @Test
    void heldNestedBeanBehindACopyingGetterKeepsEveryCell() {
        final CopyingHolder read = HBaseExecutor.toEntity(addressRow(), CopyingHolder.class);

        assertAll(() -> assertEquals("Ann", read.getName()), () -> assertEquals("Paris", read.getHome().getCity()),
                () -> assertEquals("75001", read.getHome().getZip()),
                () -> assertEquals(List.of(HBaseColumn.valueOf("n2", 2L), HBaseColumn.valueOf("n1", 1L)), read.getHome().getNotes()));
    }

    // Guard: a held nested bean that the property still returns is filled in place and never handed to the setter.
    @Test
    void heldNestedBeanReturnedByAPlainGetterIsFilledInPlaceWithoutASetterCall() {
        final HeldHolder read = HBaseExecutor.toEntity(addressRow(), HeldHolder.class);

        assertAll(() -> assertSame(read.initialHome, read.getHome()), () -> assertEquals(0, read.homeSetterCalls),
                () -> assertEquals("Paris", read.getHome().getCity()), () -> assertEquals("75001", read.getHome().getZip()),
                () -> assertEquals(List.of(HBaseColumn.valueOf("n2", 2L), HBaseColumn.valueOf("n1", 1L)), read.getHome().getNotes()));
    }

    /**
     * A read-only held nested bean behind a copying getter (read-only only under {@code @AccessFieldByMethod}) cannot be
     * assigned back. Before the fix the cells filled the copy the getter returned, so every cell of the property was lost, also
     * for a row AnyPut.create wrote; the instance the field holds is now filled in place.
     */
    @Test
    void readOnlyHeldNestedBeanBehindACopyingGetterKeepsEveryCell() {
        final ReadOnlyCopyingHolder read = HBaseExecutor.toEntity(addressRow(), ReadOnlyCopyingHolder.class);

        final ReadOnlyCopyingHolder source = HBaseExecutor.toEntity(addressRow(), ReadOnlyCopyingHolder.class);
        source.setId("r9");
        final ReadOnlyCopyingHolder roundTrip = HBaseExecutor.toEntity(toResult(AnyPut.create(source)), ReadOnlyCopyingHolder.class);

        assertAll(() -> assertEquals("r1", read.getId()), () -> assertEquals("Ann", read.getName()), () -> assertEquals("Paris", read.getHome().getCity()),
                () -> assertEquals("75001", read.getHome().getZip()),
                () -> assertEquals(List.of(HBaseColumn.valueOf("n2", 2L), HBaseColumn.valueOf("n1", 1L)), read.getHome().getNotes()),
                () -> assertEquals("r9", roundTrip.getId()), () -> assertEquals("Paris", roundTrip.getHome().getCity()),
                () -> assertEquals("75001", roundTrip.getHome().getZip()));
    }

    /** Guard: a read-only held nested bean whose field holds null is still skipped (no setter to assign one), the row is read. */
    @Test
    void readOnlyNullHeldNestedBeanIsStillSkipped() {
        final ReadOnlyNullHolder read = HBaseExecutor.toEntity(addressRow(), ReadOnlyNullHolder.class);

        assertAll(() -> assertEquals("r1", read.getId()), () -> assertEquals("Ann", read.getName()), () -> assertNull(read.getHome()));
    }

    // Box<T> nested-bean properties bound to a versioned column through the property's type arguments. Earlier versions wrote
    // them with the class's BeanInfo, which types T as Object: the HBaseColumn value(s) were written as ONE text cell
    // ("5:7", or a JSON list or map of them) at the put's timestamp.
    private static Result legacyVersionedBoxRow() {
        final List<Cell> cells = new ArrayList<>();
        cells.add(cell("byVersion", "value", 100, N.stringOf(versionMap())));
        cells.add(cell("byVersion", "label", 100, "M"));
        cells.add(cell("versions", "value", 100, N.stringOf(new ArrayList<>(versionMap().values()))));
        cells.add(cell("single", "value", 100, N.stringOf(HBaseColumn.valueOf(7, 5L))));
        cells.add(cell("text", "value", 100, N.stringOf(HBaseColumn.valueOf("t", 5L))));

        return sorted(cells);
    }

    private static Map<Long, HBaseColumn<Integer>> versionMap() {
        final Map<Long, HBaseColumn<Integer>> map = new LinkedHashMap<>();
        map.put(5L, HBaseColumn.valueOf(7, 5L));
        map.put(3L, HBaseColumn.valueOf(6, 3L));

        return map;
    }

    /**
     * Before the fix a row an earlier version wrote for such a property failed to read (NumberFormatException: the whole JSON
     * text decoded as an Integer). Its text cell is now decoded with the property's type, each version at its own version. A
     * String-valued column accepts any text, so its legacy cell reads as that text (as a new value of that text would).
     */
    @Test
    void legacyTextCellOfAGenericVersionedNestedPropertyReadsBackEveryVersion() {
        final VersionedBoxEntity read = HBaseExecutor.toEntity(legacyVersionedBoxRow(), VersionedBoxEntity.class);

        assertAll(() -> assertEquals(Map.of(5L, HBaseColumn.valueOf(7, 5L), 3L, HBaseColumn.valueOf(6, 3L)), read.getByVersion().getValue()),
                () -> assertEquals("M", read.getByVersion().getLabel()),
                () -> assertEquals(List.of(HBaseColumn.valueOf(7, 5L), HBaseColumn.valueOf(6, 3L)), read.getVersions().getValue()),
                () -> assertEquals(HBaseColumn.valueOf(7, 5L), read.getSingle().getValue()),
                () -> assertEquals(HBaseColumn.valueOf("5:t", 100L), read.getText().getValue()));
    }

    /** A legacy text cell next to cells the current version wrote (one per version) in the same column: every version is read. */
    @Test
    void legacyTextCellMixedWithCurrentCellsReadsBackEveryVersion() {
        final List<Cell> cells = new ArrayList<>();
        cells.add(cell("byVersion", "value", 100, N.stringOf(versionMap())));
        cells.add(cell("byVersion", "value", 9, "8"));
        cells.add(cell("versions", "value", 9, "8"));
        cells.add(cell("versions", "value", 100, N.stringOf(new ArrayList<>(versionMap().values()))));

        final VersionedBoxEntity read = HBaseExecutor.toEntity(sorted(cells), VersionedBoxEntity.class);

        assertAll(
                () -> assertEquals(Map.of(9L, HBaseColumn.valueOf(8, 9L), 5L, HBaseColumn.valueOf(7, 5L), 3L, HBaseColumn.valueOf(6, 3L)),
                        read.getByVersion().getValue()),
                () -> assertEquals(List.of(HBaseColumn.valueOf(7, 5L), HBaseColumn.valueOf(6, 3L), HBaseColumn.valueOf(8, 9L)), read.getVersions().getValue()));
    }

    /** Guard: a current-format row round-trips unchanged, and a text that is no legacy value either still fails as before. */
    @Test
    void currentFormatVersionedBoxRoundTripsAndInvalidTextStillFails() {
        final VersionedBoxEntity source = new VersionedBoxEntity();
        source.setId("r3");
        source.setByVersion(new Box<>(versionMap(), "M"));
        source.setVersions(new Box<>(new ArrayList<>(versionMap().values()), null));
        source.setSingle(new Box<>(HBaseColumn.valueOf(7, 5L), null));

        final VersionedBoxEntity roundTrip = HBaseExecutor.toEntity(toResult(AnyPut.create(source)), VersionedBoxEntity.class);

        final List<Cell> invalid = new ArrayList<>();
        invalid.add(cell("single", "value", 1, "abc"));

        final NumberFormatException e = assertThrows(NumberFormatException.class, () -> HBaseExecutor.toEntity(sorted(invalid), VersionedBoxEntity.class));

        assertAll(() -> assertEquals(Map.of(5L, HBaseColumn.valueOf(7, 5L), 3L, HBaseColumn.valueOf(6, 3L)), roundTrip.getByVersion().getValue()),
                () -> assertEquals("M", roundTrip.getByVersion().getLabel()),
                () -> assertEquals(List.of(HBaseColumn.valueOf(7, 5L), HBaseColumn.valueOf(6, 3L)), roundTrip.getVersions().getValue()),
                () -> assertEquals(HBaseColumn.valueOf(7, 5L), roundTrip.getSingle().getValue()), //
                () -> assertEquals(1, e.getSuppressed().length));
    }

    public static class VersionedBoxEntity {
        @Id
        private String id;
        private Box<Map<Long, HBaseColumn<Integer>>> byVersion;
        private Box<List<HBaseColumn<Integer>>> versions;
        private Box<HBaseColumn<Integer>> single;
        private Box<HBaseColumn<String>> text;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public Box<Map<Long, HBaseColumn<Integer>>> getByVersion() {
            return byVersion;
        }

        public void setByVersion(final Box<Map<Long, HBaseColumn<Integer>>> byVersion) {
            this.byVersion = byVersion;
        }

        public Box<List<HBaseColumn<Integer>>> getVersions() {
            return versions;
        }

        public void setVersions(final Box<List<HBaseColumn<Integer>>> versions) {
            this.versions = versions;
        }

        public Box<HBaseColumn<Integer>> getSingle() {
            return single;
        }

        public void setSingle(final Box<HBaseColumn<Integer>> single) {
            this.single = single;
        }

        public Box<HBaseColumn<String>> getText() {
            return text;
        }

        public void setText(final Box<HBaseColumn<String>> text) {
            this.text = text;
        }
    }

    @Entity
    @AccessFieldByMethod
    public static class ReadOnlyNullHolder {
        @Id
        private String id;
        private String name;
        private Address home;

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

        public Address getHome() {
            return home == null ? null : new Address(home);
        }
    }

    /**
     * Pins the default (no {@code @AccessFieldByMethod}): a field-backed property is then not read-only, even with a final field
     * and no setter, and PropInfo.getPropValue reads it through its field, so the instance the field holds is filled in place
     * and keeps every cell; the copying getter is never consulted (the limitation above needs {@code @AccessFieldByMethod}).
     */
    @Test
    void heldNestedBeanBehindACopyingGetterIsFilledThroughItsFieldWithoutAccessFieldByMethod() {
        final FieldAccessCopyingHolder read = HBaseExecutor.toEntity(addressRow(), FieldAccessCopyingHolder.class);

        assertAll(() -> assertSame(read.initialHome, read.home), () -> assertEquals("r1", read.getId()), () -> assertEquals("Ann", read.getName()),
                () -> assertEquals("Paris", read.home.getCity()), () -> assertEquals("75001", read.home.getZip()),
                () -> assertEquals(List.of(HBaseColumn.valueOf("n2", 2L), HBaseColumn.valueOf("n1", 1L)), read.home.getNotes()),
                () -> assertEquals("Paris", read.getHome().getCity()));
    }

    // A nested bean whose type argument the entity binds through its generic superclass (Box<T> box declared by GenericBoxBase<T>,
    // bound to Long by LongBoxEntity) is read with the parameterized BeanInfo too: its T cell was stored as a String.
    @Test
    void nestedBeanTypeArgumentBoundByAGenericSuperclassIsDecodedAsTheBoundType() {
        final List<Cell> cells = new ArrayList<>();
        cells.add(cell("box", "value", 1, "12"));
        cells.add(cell("box", "label", 1, "L"));

        final LongBoxEntity read = HBaseExecutor.toEntity(sorted(cells), LongBoxEntity.class);

        final LongBoxEntity source = new LongBoxEntity();
        source.setId("r2");
        source.setBox(new Box<>(34L, "M"));
        final LongBoxEntity roundTrip = HBaseExecutor.toEntity(toResult(AnyPut.create(source)), LongBoxEntity.class);

        assertAll(() -> assertEquals(Long.valueOf(12L), (Object) read.getBox().getValue()), () -> assertEquals("L", read.getBox().getLabel()),
                () -> assertEquals("r2", roundTrip.getId()), () -> assertEquals(Long.valueOf(34L), (Object) roundTrip.getBox().getValue()),
                () -> assertEquals("M", roundTrip.getBox().getLabel()));
    }

    // The ArrayBlockingQueue versioned property (2026-11-02) also on the immutable path: a record entity and a record nested
    // in a mutable entity, both built from their intermediate results, receive a queue sized to the versions read.
    @Test
    void arrayBlockingQueueVersionedRecordComponentsReadBackEveryVersion() {
        final List<Cell> cells = new ArrayList<>();
        cells.add(cell("history", "", 3, "c"));
        cells.add(cell("history", "", 2, "b"));
        cells.add(cell("history", "", 1, "a"));
        cells.add(cell("label", "", 1, "L"));

        final QueueRecord record = HBaseExecutor.toEntity(sorted(cells), QueueRecord.class);

        final List<Cell> nestedCells = new ArrayList<>();
        nestedCells.add(cell("queue", "history", 2, "y"));
        nestedCells.add(cell("queue", "history", 1, "x"));
        nestedCells.add(cell("queue", "label", 1, "Q"));

        final QueueRecordHolder holder = HBaseExecutor.toEntity(sorted(nestedCells), QueueRecordHolder.class);

        // no version of the property: no queue is created, the component keeps its default
        final List<Cell> labelOnly = new ArrayList<>();
        labelOnly.add(cell("label", "", 1, "only"));

        final QueueRecord noVersions = HBaseExecutor.toEntity(sorted(labelOnly), QueueRecord.class);

        assertAll(() -> assertEquals("only", noVersions.label()), () -> assertNull(noVersions.history()),
                () -> assertInstanceOf(ArrayBlockingQueue.class, record.history()),
                () -> assertEquals(List.of(HBaseColumn.valueOf("c", 3L), HBaseColumn.valueOf("b", 2L), HBaseColumn.valueOf("a", 1L)),
                        new ArrayList<>(record.history())),
                () -> assertEquals(0, record.history().remainingCapacity()), () -> assertEquals("L", record.label()), () -> assertNotNull(holder.getQueue()),
                () -> assertEquals("Q", holder.getQueue().label()),
                () -> assertEquals(List.of(HBaseColumn.valueOf("y", 2L), HBaseColumn.valueOf("x", 1L)), new ArrayList<>(holder.getQueue().history())),
                () -> assertEquals(0, holder.getQueue().history().remainingCapacity()));
    }

    // A row key declared as a type variable of a generic superclass was decoded as the setter's erased parameter type
    // (Object), so a String was stored in a property the entity binds to Long; the other generic property was converted.
    @Test
    void rowKeyDeclaredByAGenericSuperclassIsDecodedAsTheBoundType() {
        final List<Cell> cells = new ArrayList<>();
        cells.add(new KeyValue(Bytes.toBytes("123"), Bytes.toBytes("count"), Bytes.toBytes(""), 1L, Bytes.toBytes("7")));
        cells.add(new KeyValue(Bytes.toBytes("123"), Bytes.toBytes("name"), Bytes.toBytes(""), 1L, Bytes.toBytes("n1")));
        final Result result = sorted(cells);

        final LongKeyEntity read = HBaseExecutor.toEntity(result, LongKeyEntity.class);
        final List<LongKeyEntity> listed = HBaseExecutor.toList(List.of(result), LongKeyEntity.class);

        final LongKeyEntity source = new LongKeyEntity();
        source.setId(5L);
        source.setName("n5");
        final LongKeyEntity roundTrip = HBaseExecutor.toEntity(toResult(AnyPut.create(source)), LongKeyEntity.class);

        assertAll(() -> assertEquals(Long.valueOf(123L), ((GenericKeyBase<?>) read).getId()),
                () -> assertEquals(Long.valueOf(7L), ((GenericKeyBase<?>) read).getCount()), () -> assertEquals("n1", read.getName()),
                () -> assertEquals(Long.valueOf(123L), ((GenericKeyBase<?>) listed.get(0)).getId()),
                () -> assertEquals(Long.valueOf(5L), ((GenericKeyBase<?>) roundTrip).getId()), () -> assertEquals("n5", roundTrip.getName()));
    }

    // Guard: a non-generic row key keeps its declared type (here a primitive long).
    @Test
    void nonGenericRowKeyKeepsItsDeclaredType() {
        final List<Cell> cells = new ArrayList<>();
        cells.add(new KeyValue(Bytes.toBytes("42"), Bytes.toBytes("name"), Bytes.toBytes(""), 1L, Bytes.toBytes("n")));

        final PrimitiveKeyEntity read = HBaseExecutor.toEntity(sorted(cells), PrimitiveKeyEntity.class);

        assertAll(() -> assertEquals(42L, read.getId()), () -> assertEquals("n", read.getName()));
    }

    // A generic nested bean was read with its class's BeanInfo, which types each type-variable property as its erasure: a cell
    // was stored as a String into a property bound to Integer, and a bound such as N extends Number failed the whole row.
    @Test
    void genericNestedBeanPropertiesAreDecodedAsTheirBoundTypes() {
        final List<Cell> cells = new ArrayList<>();
        cells.add(cell("range", "left", 1, "a"));
        cells.add(cell("range", "right", 1, "16"));
        cells.add(cell("range", "versions", 2, "18"));
        cells.add(cell("range", "versions", 1, "17"));

        final GenericNestedEntity read = HBaseExecutor.toEntity(sorted(cells), GenericNestedEntity.class);

        final GenericNestedEntity source = new GenericNestedEntity();
        source.setId("r2");
        source.setRange(new Pair<>("x", 19));
        final GenericNestedEntity roundTrip = HBaseExecutor.toEntity(toResult(AnyPut.create(source)), GenericNestedEntity.class);

        assertAll(() -> assertEquals("a", read.getRange().getLeft()), () -> assertEquals(Integer.valueOf(16), (Object) read.getRange().getRight()),
                () -> assertEquals(List.of(HBaseColumn.valueOf(18, 2L), HBaseColumn.valueOf(17, 1L)), read.getRange().getVersions()),
                () -> assertEquals(Integer.valueOf(19), (Object) roundTrip.getRange().getRight()));
    }

    @Test
    void boundedGenericNestedBeanPropertyIsDecodedAsItsBoundType() {
        final List<Cell> cells = new ArrayList<>();
        cells.add(cell("amount", "value", 1, "12"));

        final BoundedNestedEntity read = HBaseExecutor.toEntity(sorted(cells), BoundedNestedEntity.class);

        assertEquals(Long.valueOf(12L), (Object) read.getAmount().getValue());
    }

    // Guard: a wildcard type argument is not resolved (its bound Number cannot be created from text): the row is still read.
    @Test
    void wildcardNestedBeanIsStillRead() {
        final List<Cell> cells = new ArrayList<>();
        cells.add(cell("amount", "right", 1, "12"));
        cells.add(cell("label", "", 1, "L"));

        final WildcardNestedEntity read = HBaseExecutor.toEntity(sorted(cells), WildcardNestedEntity.class);

        assertAll(() -> assertEquals("L", read.getLabel()), () -> assertNotNull(read.getAmount()));
    }

    public static class Pair<L, R> {
        private L left;
        private R right;
        private List<HBaseColumn<R>> versions;

        public Pair() {
        }

        public Pair(final L left, final R right) {
            this.left = left;
            this.right = right;
        }

        public L getLeft() {
            return left;
        }

        public void setLeft(final L left) {
            this.left = left;
        }

        public R getRight() {
            return right;
        }

        public void setRight(final R right) {
            this.right = right;
        }

        public List<HBaseColumn<R>> getVersions() {
            return versions;
        }

        public void setVersions(final List<HBaseColumn<R>> versions) {
            this.versions = versions;
        }
    }

    public static class NumberBox<N extends Number> {
        private N value;

        public N getValue() {
            return value;
        }

        public void setValue(final N value) {
            this.value = value;
        }
    }

    public static class GenericNestedEntity {
        @Id
        private String id;
        private Pair<String, Integer> range;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public Pair<String, Integer> getRange() {
            return range;
        }

        public void setRange(final Pair<String, Integer> range) {
            this.range = range;
        }
    }

    public static class BoundedNestedEntity {
        @Id
        private String id;
        private NumberBox<Long> amount;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public NumberBox<Long> getAmount() {
            return amount;
        }

        public void setAmount(final NumberBox<Long> amount) {
            this.amount = amount;
        }
    }

    public static class WildcardNestedEntity {
        @Id
        private String id;
        private String label;
        private Pair<String, ? extends Number> amount;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getLabel() {
            return label;
        }

        public void setLabel(final String label) {
            this.label = label;
        }

        public Pair<String, ? extends Number> getAmount() {
            return amount;
        }

        public void setAmount(final Pair<String, ? extends Number> amount) {
            this.amount = amount;
        }
    }

    public static class GenericKeyBase<K> {
        @Id
        private K id;
        private K count;

        public K getId() {
            return id;
        }

        public void setId(final K id) {
            this.id = id;
        }

        public K getCount() {
            return count;
        }

        public void setCount(final K count) {
            this.count = count;
        }
    }

    public static class LongKeyEntity extends GenericKeyBase<Long> {
        private String name;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    public static class PrimitiveKeyEntity {
        @Id
        private long id;
        private String name;

        public long getId() {
            return id;
        }

        public void setId(final long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    public static class Address {
        private String city;
        private String zip;
        private List<HBaseColumn<String>> notes;

        public Address() {
        }

        public Address(final String city, final String zip) {
            this.city = city;
            this.zip = zip;
        }

        Address(final Address other) {
            this(other.city, other.zip);
            notes = other.notes;
        }

        public String getCity() {
            return city;
        }

        public void setCity(final String city) {
            this.city = city;
        }

        public String getZip() {
            return zip;
        }

        public void setZip(final String zip) {
            this.zip = zip;
        }

        public List<HBaseColumn<String>> getNotes() {
            return notes;
        }

        public void setNotes(final List<HBaseColumn<String>> notes) {
            this.notes = notes;
        }
    }

    // "home" has no field: its getter builds a new Address from private state on each call.
    public static class ViewHolder {
        @Id
        private String id;
        private String name;
        private String homeCity;
        private String homeZip;
        private List<HBaseColumn<String>> homeNotes;

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

        public Address getHome() {
            final Address view = new Address(homeCity, homeZip);
            view.setNotes(homeNotes);
            return view;
        }

        public void setHome(final Address home) {
            homeCity = home == null ? null : home.getCity();
            homeZip = home == null ? null : home.getZip();
            homeNotes = home == null ? null : home.getNotes();
        }
    }

    @AccessFieldByMethod
    public static class CopyingHolder {
        @Id
        private String id;
        private String name;
        private Address home = new Address();

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

        public Address getHome() {
            return home == null ? null : new Address(home);
        }

        public void setHome(final Address home) {
            this.home = home == null ? null : new Address(home);
        }
    }

    // @AccessFieldByMethod: without it PropInfo.setPropValue writes the field directly, so homeSetterCalls could not reveal an
    // assignment through the property (verifier v1, 2026-11-03: the guard below was vacuous).
    @AccessFieldByMethod
    public static class HeldHolder {
        @Id
        private String id;
        private String name;
        private Address home = new Address();
        final Address initialHome = home;
        int homeSetterCalls;

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

        public Address getHome() {
            return home;
        }

        public void setHome(final Address home) {
            homeSetterCalls++;
            this.home = home;
        }
    }

    @Entity
    @AccessFieldByMethod
    public static class ReadOnlyCopyingHolder {
        @Id
        private String id;
        private String name;
        private final Address home = new Address();

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

        public Address getHome() {
            return new Address(home);
        }
    }

    // As ReadOnlyCopyingHolder, without @AccessFieldByMethod: "home" is not read-only; its field, not the copying getter, is read.
    @Entity
    public static class FieldAccessCopyingHolder {
        @Id
        private String id;
        private String name;
        private final Address home = new Address();
        final Address initialHome = home;

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

        public Address getHome() {
            return new Address(home);
        }
    }

    public static class Box<T> {
        private T value;
        private String label;

        public Box() {
        }

        public Box(final T value, final String label) {
            this.value = value;
            this.label = label;
        }

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

    public static class GenericBoxBase<T> {
        @Id
        private String id;
        private Box<T> box;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public Box<T> getBox() {
            return box;
        }

        public void setBox(final Box<T> box) {
            this.box = box;
        }
    }

    public static class LongBoxEntity extends GenericBoxBase<Long> {
    }

    public record QueueRecord(ArrayBlockingQueue<HBaseColumn<String>> history, String label) {
    }

    public static class QueueRecordHolder {
        @Id
        private String id;
        private QueueRecord queue;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public QueueRecord getQueue() {
            return queue;
        }

        public void setQueue(final QueueRecord queue) {
            this.queue = queue;
        }
    }
}
