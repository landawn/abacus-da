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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import com.landawn.abacus.annotation.AccessFieldByMethod;
import com.landawn.abacus.annotation.Column;
import com.landawn.abacus.annotation.Entity;
import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.annotation.JsonXmlField;
import com.landawn.abacus.da.hbase.annotation.ColumnFamily;
import com.landawn.abacus.util.HBaseColumn;
import com.landawn.abacus.util.Holder;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.u;

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

    // ---- 2026-10-24 sliceM ----

    private static Result toResult(final AnyPut put) {
        final List<Cell> cells = new ArrayList<>();
        put.val().getFamilyCellMap().values().forEach(cells::addAll);
        cells.sort(CellComparator.getInstance());

        return Result.create(cells);
    }

    // A mapper must read back what it writes under its own naming policy. A qualifier variant ("first_name" for firstName)
    // used to be dropped from the property's family when ANY earlier property, even in another family, claimed that name.
    @Test
    void namingVariantQualifierClaimedInAnotherFamilyIsStillReadBack() {
        final CrossFamilyColumnEntity source = new CrossFamilyColumnEntity();
        source.setId("r1");
        source.setLegacyName("legacy");
        source.setFirstName("first");

        assertAll(Arrays.stream(NamingPolicy.values()).map(policy -> (Executable) () -> {
            final CrossFamilyColumnEntity back = HBaseExecutor.toEntity(toResult(AnyPut.create(source, policy)), CrossFamilyColumnEntity.class);

            assertEquals("legacy", back.getLegacyName(), policy.name());
            assertEquals("first", back.getFirstName(), policy.name());
        }));
    }

    // The same for a family variant: an unannotated scalar is written to ("email_address", "") under SNAKE_CASE; an earlier
    // property's explicit family "email_address" used to swallow that family name, so the cell was ignored on read.
    @Test
    void namingVariantFamilyClaimedByAnEarlierExplicitFamilyIsStillReadBack() {
        final CrossFamilyScalarEntity source = new CrossFamilyScalarEntity();
        source.setId("r2");
        source.setOther("o");
        source.setEmailAddress("a@b.c");

        assertAll(Arrays.stream(NamingPolicy.values()).map(policy -> (Executable) () -> {
            final CrossFamilyScalarEntity back = HBaseExecutor.toEntity(toResult(AnyPut.create(source, policy)), CrossFamilyScalarEntity.class);

            assertEquals("o", back.getOther(), policy.name());
            assertEquals("a@b.c", back.getEmailAddress(), policy.name());
        }));
    }

    // ... also when the claimed family is a flattened nested bean's: the scalar's empty qualifier takes precedence over the
    // nested bean's empty-qualifier fallback, and the nested bean's own cells in that family are still routed to it.
    @Test
    void namingVariantFamilyClaimedByAFlattenedNestedBeanIsStillReadBack() {
        final CrossFamilyNestedEntity source = new CrossFamilyNestedEntity();
        source.setId("r3");
        source.setPrimary(new FullName("John"));
        source.setFullName("John Smith");

        assertAll(Arrays.stream(NamingPolicy.values()).map(policy -> (Executable) () -> {
            final CrossFamilyNestedEntity back = HBaseExecutor.toEntity(toResult(AnyPut.create(source, policy)), CrossFamilyNestedEntity.class);

            assertEquals("John Smith", back.getFullName(), policy.name());
            assertNotNull(back.getPrimary(), policy.name());
            assertEquals("John", back.getPrimary().getFirstName(), policy.name());
        }));
    }

    // A variant never displaces what another property registered in the same family: the explicit @Column("first_name")
    // keeps that qualifier, whichever property is declared first.
    @Test
    void namingVariantQualifierNeverDisplacesAnExplicitColumnOfTheSameFamily() {
        final Result row = rowOf("r4", "cf", "first_name", "explicit", "firstName", "camel");

        final SameFamilyColumnEntity back = HBaseExecutor.toEntity(row, SameFamilyColumnEntity.class);

        assertAll(() -> assertEquals("explicit", back.getLegacyName()), () -> assertEquals("camel", back.getFirstName()));
    }

    // The variant kept above must not capture a flattened nested bean's cell: "user_name" (a variant of userName that the
    // explicit @Column of family "a" also uses) is here the CAMEL_CASE qualifier of the nested bean's own user_name property.
    @Test
    void namingVariantQualifierDoesNotCaptureANestedBeanCellOfTheSameFamily() {
        final NestedQualifierEntity source = new NestedQualifierEntity();
        source.setId("r5");
        source.setLegacy("l");
        source.setInner(new SnakeNamedBean("nested", "x"));
        source.setUserName("top");

        assertAll(Arrays.stream(new NamingPolicy[] { NamingPolicy.CAMEL_CASE, NamingPolicy.NO_CHANGE }).map(policy -> (Executable) () -> {
            final NestedQualifierEntity back = HBaseExecutor.toEntity(toResult(AnyPut.create(source, policy)), NestedQualifierEntity.class);

            assertEquals("l", back.getLegacy(), policy.name());
            assertEquals("top", back.getUserName(), policy.name());
            assertEquals("nested", back.getInner().getUser_name(), policy.name());
            assertEquals("x", back.getInner().getA(), policy.name());
        }));
    }

    private static Object[] emptyValueWrappers() {
        return new Object[] { java.util.Optional.empty(), java.util.OptionalInt.empty(), java.util.OptionalLong.empty(), java.util.OptionalDouble.empty(),
                u.Optional.empty(), u.Nullable.empty(), u.Nullable.of(null), u.OptionalInt.empty(), u.OptionalLong.empty(), u.OptionalDouble.empty(),
                u.OptionalBoolean.empty(), u.OptionalChar.empty(), u.OptionalByte.empty(), u.OptionalShort.empty(), u.OptionalFloat.empty(), Holder.of(null) };
    }

    // N.stringOf returns null for an empty value wrapper, and Bytes.toBytes(null) threw NullPointerException: the value is
    // now converted like null (no bytes), so AnyPut.addColumn(..., Optional.empty()) stores an empty cell instead of failing.
    @Test
    void emptyValueWrapperConvertsToNullBytesLikeNull() {
        assertAll(Arrays.stream(emptyValueWrappers()).map(value -> (Executable) () -> {
            final String label = value.getClass().getName() + " " + value;

            assertNull(HBaseExecutor.toValueBytes(value), label);
            assertNull(HBaseExecutor.toRowKeyBytes(value), label);

            final AnyPut put = AnyPut.of("r").addColumn("f", "q", value);
            assertEquals(1, put.val().get(Bytes.toBytes("f"), Bytes.toBytes("q")).size(), label);
            assertEquals(0, put.val().get(Bytes.toBytes("f"), Bytes.toBytes("q")).get(0).getValueLength(), label);
        }));
    }

    @Test
    void presentValueWrapperConvertsToItsValueBytes() {
        assertAll(() -> assertEquals("a", Bytes.toString(HBaseExecutor.toValueBytes(java.util.Optional.of("a")))),
                () -> assertEquals("3", Bytes.toString(HBaseExecutor.toValueBytes(java.util.OptionalInt.of(3)))),
                () -> assertEquals("b", Bytes.toString(HBaseExecutor.toValueBytes(u.Nullable.of("b")))),
                () -> assertEquals("c", Bytes.toString(HBaseExecutor.toValueBytes(Holder.of("c")))),
                () -> assertEquals("", Bytes.toString(HBaseExecutor.toValueBytes(""))));
    }

    @Test
    void isEmptyValueWrapperRecognizesExactlyTheEmptyWrappers() {
        assertAll(Arrays.stream(emptyValueWrappers())
                .map(value -> (Executable) () -> assertTrue(HBaseExecutor.isEmptyValueWrapper(value), value.getClass().getName() + " " + value)));

        assertAll(Arrays
                .stream(new Object[] { java.util.Optional.of("a"), java.util.OptionalInt.of(0), u.Optional.of("a"), u.Nullable.of("b"),
                        u.OptionalBoolean.of(false), Holder.of("c"), "", 0, new java.util.concurrent.atomic.AtomicReference<>(), new ArrayList<>() })
                .map(value -> (Executable) () -> assertFalse(HBaseExecutor.isEmptyValueWrapper(value), value.getClass().getName() + " " + value)));

        assertFalse(HBaseExecutor.isEmptyValueWrapper(null));
    }

    public static class NestedQualifierEntity {
        @Id
        private String id;
        @ColumnFamily("a")
        @Column("user_name")
        private String legacy;
        @ColumnFamily("b")
        private SnakeNamedBean inner;
        @ColumnFamily("b")
        private String userName;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getLegacy() {
            return legacy;
        }

        public void setLegacy(final String legacy) {
            this.legacy = legacy;
        }

        public SnakeNamedBean getInner() {
            return inner;
        }

        public void setInner(final SnakeNamedBean inner) {
            this.inner = inner;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(final String userName) {
            this.userName = userName;
        }
    }

    public static class SnakeNamedBean {
        private String user_name;
        private String a;

        public SnakeNamedBean() {
        }

        SnakeNamedBean(final String userName, final String a) {
            this.user_name = userName;
            this.a = a;
        }

        public String getUser_name() {
            return user_name;
        }

        public void setUser_name(final String userName) {
            this.user_name = userName;
        }

        public String getA() {
            return a;
        }

        public void setA(final String a) {
            this.a = a;
        }
    }

    public static class CrossFamilyColumnEntity {
        @Id
        private String id;
        @ColumnFamily("a")
        @Column("first_name")
        private String legacyName;
        @ColumnFamily("b")
        private String firstName;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getLegacyName() {
            return legacyName;
        }

        public void setLegacyName(final String legacyName) {
            this.legacyName = legacyName;
        }

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(final String firstName) {
            this.firstName = firstName;
        }
    }

    public static class CrossFamilyScalarEntity {
        @Id
        private String id;
        @ColumnFamily("email_address")
        private String other;
        private String emailAddress;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getOther() {
            return other;
        }

        public void setOther(final String other) {
            this.other = other;
        }

        public String getEmailAddress() {
            return emailAddress;
        }

        public void setEmailAddress(final String emailAddress) {
            this.emailAddress = emailAddress;
        }
    }

    public static class CrossFamilyNestedEntity {
        @Id
        private String id;
        @ColumnFamily("full_name")
        private FullName primary;
        private String fullName;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public FullName getPrimary() {
            return primary;
        }

        public void setPrimary(final FullName primary) {
            this.primary = primary;
        }

        public String getFullName() {
            return fullName;
        }

        public void setFullName(final String fullName) {
            this.fullName = fullName;
        }
    }

    @ColumnFamily("cf")
    public static class SameFamilyColumnEntity {
        @Id
        private String id;
        private String firstName;
        @Column("first_name")
        private String legacyName;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(final String firstName) {
            this.firstName = firstName;
        }

        public String getLegacyName() {
            return legacyName;
        }

        public void setLegacyName(final String legacyName) {
            this.legacyName = legacyName;
        }
    }

    // ---- 2026-10-24 verify8 ----

    // A scalar whose SNAKE_CASE family name is a flattened nested bean's family ("info_data") must not take that family's
    // empty-qualifier slot from the bean when the bean has no other entry there (its own-name qualifiers "x"/"X" belong to
    // explicit @Column properties): the read side then reached the bean's cells only through that slot, so the whole bean
    // was lost under every policy. Both declaration orders of the scalar and the bean.
    @Test
    void scalarFamilyVariantDoesNotDisplaceTheOnlyEntryOfANestedBean() {
        final OnlyFallbackNestedEntity source = new OnlyFallbackNestedEntity();
        source.setId("v1");
        source.setX(new PersonName("John", "Smith", "js"));
        source.setP1("p1");
        source.setP2("p2");
        source.setInfoData("info");

        final OnlyFallbackNestedLateEntity late = new OnlyFallbackNestedLateEntity();
        late.setId("v2");
        late.setP0("p0");
        late.setInfoData("info");
        late.setX(new PersonName("Ann", "Lee", "al"));
        late.setP1("p1");
        late.setP2("p2");

        assertAll(Arrays.stream(NamingPolicy.values()).flatMap(policy -> Arrays.stream(new Executable[] { () -> {
            final OnlyFallbackNestedEntity back = HBaseExecutor.toEntity(toResult(AnyPut.create(source, policy)), OnlyFallbackNestedEntity.class);

            assertNotNull(back.getX(), policy.name());
            assertEquals("John", back.getX().getFirstName(), policy.name());
            assertEquals("Smith", back.getX().getLastName(), policy.name());
            assertEquals("js", back.getX().getUserName(), policy.name());
            assertEquals("p1", back.getP1(), policy.name());
            assertEquals("p2", back.getP2(), policy.name());
        }, () -> {
            final OnlyFallbackNestedLateEntity back = HBaseExecutor.toEntity(toResult(AnyPut.create(late, policy)), OnlyFallbackNestedLateEntity.class);

            assertNotNull(back.getX(), policy.name());
            assertEquals("Ann", back.getX().getFirstName(), policy.name());
            assertEquals("al", back.getX().getUserName(), policy.name());
            assertEquals("p0", back.getP0(), policy.name());
        } })));
    }

    // ... nor when the family holds two flattened nested beans: the read side then picked the owner of a cell among them in
    // map order instead of trying the first bean first, so "user_name" of 'first' (its own CAMEL_CASE qualifier) landed in
    // 'second.userName' (whose SNAKE_CASE variant it also is), even under the default CAMEL_CASE policy.
    @Test
    void scalarFamilyVariantDoesNotChangeRoutingBetweenNestedBeansOfThatFamily() {
        final TwoNestedBeansEntity source = new TwoNestedBeansEntity();
        source.setId("v3");
        source.setFirst(new SnakeNamedBean("first-un", "first-a"));
        source.setSecond(new PersonName("Bob", "Ray", "second-un"));
        source.setInfoData("info");

        assertAll(Arrays.stream(new NamingPolicy[] { NamingPolicy.CAMEL_CASE, NamingPolicy.NO_CHANGE }).map(policy -> (Executable) () -> {
            final TwoNestedBeansEntity back = HBaseExecutor.toEntity(toResult(AnyPut.create(source, policy)), TwoNestedBeansEntity.class);

            assertEquals("first-un", back.getFirst().getUser_name(), policy.name());
            assertEquals("first-a", back.getFirst().getA(), policy.name());
            assertEquals("second-un", back.getSecond().getUserName(), policy.name());
            assertEquals("Bob", back.getSecond().getFirstName(), policy.name());
            assertEquals("info", back.getInfoData(), policy.name());
        }));
    }

    public static class PersonName {
        private String firstName;
        private String lastName;
        private String userName;

        public PersonName() {
        }

        PersonName(final String firstName, final String lastName, final String userName) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.userName = userName;
        }

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

        public String getUserName() {
            return userName;
        }

        public void setUserName(final String userName) {
            this.userName = userName;
        }
    }

    public static class OnlyFallbackNestedEntity {
        @Id
        private String id;
        @ColumnFamily("info_data")
        private PersonName x;
        @ColumnFamily("info_data")
        @Column("x")
        private String p1;
        @ColumnFamily("info_data")
        @Column("X")
        private String p2;
        private String infoData;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public PersonName getX() {
            return x;
        }

        public void setX(final PersonName x) {
            this.x = x;
        }

        public String getP1() {
            return p1;
        }

        public void setP1(final String p1) {
            this.p1 = p1;
        }

        public String getP2() {
            return p2;
        }

        public void setP2(final String p2) {
            this.p2 = p2;
        }

        public String getInfoData() {
            return infoData;
        }

        public void setInfoData(final String infoData) {
            this.infoData = infoData;
        }
    }

    public static class OnlyFallbackNestedLateEntity {
        @Id
        private String id;
        @ColumnFamily("info_data")
        @Column("q")
        private String p0;
        private String infoData;
        @ColumnFamily("info_data")
        private PersonName x;
        @ColumnFamily("info_data")
        @Column("x")
        private String p1;
        @ColumnFamily("info_data")
        @Column("X")
        private String p2;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getP0() {
            return p0;
        }

        public void setP0(final String p0) {
            this.p0 = p0;
        }

        public String getInfoData() {
            return infoData;
        }

        public void setInfoData(final String infoData) {
            this.infoData = infoData;
        }

        public PersonName getX() {
            return x;
        }

        public void setX(final PersonName x) {
            this.x = x;
        }

        public String getP1() {
            return p1;
        }

        public void setP1(final String p1) {
            this.p1 = p1;
        }

        public String getP2() {
            return p2;
        }

        public void setP2(final String p2) {
            this.p2 = p2;
        }
    }

    public static class TwoNestedBeansEntity {
        @Id
        private String id;
        @ColumnFamily("info_data")
        private SnakeNamedBean first;
        @ColumnFamily("info_data")
        private PersonName second;
        private String infoData;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public SnakeNamedBean getFirst() {
            return first;
        }

        public void setFirst(final SnakeNamedBean first) {
            this.first = first;
        }

        public PersonName getSecond() {
            return second;
        }

        public void setSecond(final PersonName second) {
            this.second = second;
        }

        public String getInfoData() {
            return infoData;
        }

        public void setInfoData(final String infoData) {
            this.infoData = infoData;
        }
    }

    // ---- 2026-10-24 apply-hbase ----

    // Item 12a: the twins of the slice-M family-variant fix. A @Column property without a fixed family is written to
    // (formatName(name, policy), its @Column name) and a flattened nested bean without a fixed family to (formatName(name,
    // policy), each nested qualifier). Under SNAKE_CASE that family is "user_name", which the earlier @ColumnFamily("user_name")
    // property registered, so the variant was dropped and both lost their own cells on read.
    @Test
    void columnPropertyAndNestedBeanWithoutFixedFamilyReadBackUnderAFamilyVariantAnEarlierPropertyRegistered() {
        final ColumnVariantEntity column = new ColumnVariantEntity();
        column.setId("c2");
        column.setOther("o");
        column.setUserName("un");

        final NestedVariantEntity nested = new NestedVariantEntity();
        nested.setId("c3");
        nested.setOther("o");
        nested.setUserName(new PersonName("John", "Smith", "js"));

        assertAll(Arrays.stream(NamingPolicy.values()).flatMap(policy -> Arrays.stream(new Executable[] { () -> {
            final ColumnVariantEntity back = HBaseExecutor.toEntity(toResult(AnyPut.create(column, policy)), ColumnVariantEntity.class);

            assertEquals("o", back.getOther(), policy.name());
            assertEquals("un", back.getUserName(), policy.name());
        }, () -> {
            final NestedVariantEntity back = HBaseExecutor.toEntity(toResult(AnyPut.create(nested, policy)), NestedVariantEntity.class);

            assertEquals("o", back.getOther(), policy.name());
            assertNotNull(back.getUserName(), policy.name());
            assertEquals("John", back.getUserName().getFirstName(), policy.name());
            assertEquals("Smith", back.getUserName().getLastName(), policy.name());
            assertEquals("js", back.getUserName().getUserName(), policy.name());
        } })));
    }

    // Item 12a guard: a cell that another property also writes is never taken from it. nested.q writes ("user_name", "q")
    // under every policy, userName (SNAKE_CASE only) too, so that cell still goes to the nested bean; checked under every
    // policy except SNAKE_CASE, where both write the same cell.
    @Test
    void cellThatAnotherPropertyAlsoWritesIsNotTakenFromIt() {
        final SharedQualifierEntity source = new SharedQualifierEntity();
        source.setId("c4");
        source.setOther("o");
        source.setNested(new QHolder("nq"));
        source.setUserName("un");

        assertAll(Arrays.stream(NamingPolicy.values()).filter(policy -> policy != NamingPolicy.SNAKE_CASE).map(policy -> (Executable) () -> {
            final SharedQualifierEntity back = HBaseExecutor.toEntity(toResult(AnyPut.create(source, policy)), SharedQualifierEntity.class);

            assertEquals("o", back.getOther(), policy.name());
            assertEquals("nq", back.getNested().getQ(), policy.name());
            assertEquals("un", back.getUserName(), policy.name());
        }));
    }

    // Item 12 pin (collision rule): "USER_NAME" in family info_data is written by first.user_name AND second.userName (both under
    // SCREAMING_SNAKE_CASE), so whose value the cell holds depends on what was written; such a contested cell keeps exactly the
    // routing it always had (here second, in both declaration orders) instead of a new own-name/declaration-order preference.
    // "a" has a single writer (first.a) and goes to it.
    @Test
    void contestedCellKeepsItsEarlierRoutingWhileASoleWriterCellGoesToItsWriter() {
        final Result row = rowOf("c7", "info_data", "USER_NAME", "v", "a", "x");

        final OwnNameFamilyEntity back = HBaseExecutor.toEntity(row, OwnNameFamilyEntity.class);
        final OwnNameFamilyReversedEntity reversed = HBaseExecutor.toEntity(row, OwnNameFamilyReversedEntity.class);

        assertAll(() -> assertEquals("v", back.getSecond() == null ? null : back.getSecond().getUserName()),
                () -> assertNull(back.getFirst() == null ? null : back.getFirst().getUser_name()),
                () -> assertEquals("x", back.getFirst() == null ? null : back.getFirst().getA()),
                () -> assertEquals("v", reversed.getSecond() == null ? null : reversed.getSecond().getUserName()),
                () -> assertNull(reversed.getFirst() == null ? null : reversed.getFirst().getUser_name()),
                () -> assertEquals("x", reversed.getFirst() == null ? null : reversed.getFirst().getA()));
    }

    public static class ColumnVariantEntity {
        @Id
        private String id;
        @ColumnFamily("user_name")
        private String other;
        @Column("q")
        private String userName;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getOther() {
            return other;
        }

        public void setOther(final String other) {
            this.other = other;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(final String userName) {
            this.userName = userName;
        }
    }

    public static class NestedVariantEntity {
        @Id
        private String id;
        @ColumnFamily("user_name")
        private String other;
        private PersonName userName;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getOther() {
            return other;
        }

        public void setOther(final String other) {
            this.other = other;
        }

        public PersonName getUserName() {
            return userName;
        }

        public void setUserName(final PersonName userName) {
            this.userName = userName;
        }
    }

    public static class QHolder {
        private String q;

        public QHolder() {
        }

        QHolder(final String q) {
            this.q = q;
        }

        public String getQ() {
            return q;
        }

        public void setQ(final String q) {
            this.q = q;
        }
    }

    public static class SharedQualifierEntity {
        @Id
        private String id;
        @ColumnFamily("user_name")
        private String other;
        @ColumnFamily("user_name")
        private QHolder nested;
        @Column("q")
        private String userName;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getOther() {
            return other;
        }

        public void setOther(final String other) {
            this.other = other;
        }

        public QHolder getNested() {
            return nested;
        }

        public void setNested(final QHolder nested) {
            this.nested = nested;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(final String userName) {
            this.userName = userName;
        }
    }

    public static class OwnNameFamilyEntity {
        @Id
        private String id;
        @ColumnFamily("info_data")
        private SnakeNamedBean first;
        @ColumnFamily("info_data")
        private PersonName second;
        private String info_data;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public SnakeNamedBean getFirst() {
            return first;
        }

        public void setFirst(final SnakeNamedBean first) {
            this.first = first;
        }

        public PersonName getSecond() {
            return second;
        }

        public void setSecond(final PersonName second) {
            this.second = second;
        }

        public String getInfo_data() {
            return info_data;
        }

        public void setInfo_data(final String infoData) {
            this.info_data = infoData;
        }
    }

    public static class OwnNameFamilyReversedEntity {
        @Id
        private String id;
        @ColumnFamily("info_data")
        private PersonName second;
        @ColumnFamily("info_data")
        private SnakeNamedBean first;
        private String info_data;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public PersonName getSecond() {
            return second;
        }

        public void setSecond(final PersonName second) {
            this.second = second;
        }

        public SnakeNamedBean getFirst() {
            return first;
        }

        public void setFirst(final SnakeNamedBean first) {
            this.first = first;
        }

        public String getInfo_data() {
            return info_data;
        }

        public void setInfo_data(final String infoData) {
            this.info_data = infoData;
        }
    }

    // ---- 2026-10-24 vapply-hbase ----

    // The row key is never written as a cell, yet the sole-writer map listed it: ("addr", "ID") is its SCREAMING_SNAKE_CASE
    // qualifier in its fixed family "addr", dropped from the family map because x registered "ID" and the nested bean addr may
    // write it. No property writes that cell, so (as in earlier versions and as documented) it goes to the nested bean that has
    // "ID" as a variant (addr.id); routing it to the row key, which ignores mapped cells, dropped it.
    @Test
    void cellListedOnlyForTheRowKeyIsTreatedAsWrittenByNoProperty() {
        final RowKeyVariantEntity back = HBaseExecutor.toEntity(rowOf("r1", "addr", "ID", "v", "street", "s"), RowKeyVariantEntity.class);

        assertAll(() -> assertEquals("r1", back.getId()), () -> assertNotNull(back.getAddr()),
                () -> assertEquals("v", back.getAddr() == null ? null : back.getAddr().getId()),
                () -> assertEquals("s", back.getAddr() == null ? null : back.getAddr().getStreet()));

        final RowKeyVariantEntity source = new RowKeyVariantEntity();
        source.setId("r2");
        source.setX("xv");
        source.setAddr(new IdStreet(null, "st"));

        // (addr.id is not round-tripped: its ("addr", "id") cell is the row key's own coordinate, which ignores it, as before.)
        assertAll(Arrays.stream(NamingPolicy.values()).map(policy -> (Executable) () -> {
            final RowKeyVariantEntity read = HBaseExecutor.toEntity(toResult(AnyPut.create(source, policy)), RowKeyVariantEntity.class);

            assertEquals("r2", read.getId(), policy.name());
            assertEquals("xv", read.getX(), policy.name());
            assertEquals("st", read.getAddr().getStreet(), policy.name());
        }));
    }

    public static class IdStreet {
        private String id;
        private String street;

        public IdStreet() {
        }

        IdStreet(final String id, final String street) {
            this.id = id;
            this.street = street;
        }

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getStreet() {
            return street;
        }

        public void setStreet(final String street) {
            this.street = street;
        }
    }

    public static class RowKeyVariantEntity {
        @ColumnFamily("g")
        @Column("ID")
        private String x;
        private IdStreet addr;
        @Id
        @ColumnFamily("addr")
        private String id;

        public String getX() {
            return x;
        }

        public void setX(final String x) {
            this.x = x;
        }

        public IdStreet getAddr() {
            return addr;
        }

        public void setAddr(final IdStreet addr) {
            this.addr = addr;
        }

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }
    }

    // ---- 2026-10-24 r2 sliceM ----

    // ("user_name", "k") has no entry of its own (x registered family "user_name" first) and is listed for two properties: the
    // row key userName (its SNAKE_CASE family) and userNAME (also "user_name" under SNAKE_CASE). AnyPut.create never writes the
    // row key, so only userNAME writes that cell, yet it was marked contested and a SNAKE_CASE write of userNAME read back as
    // null. (Under UPPER_CAMEL/SCREAMING_SNAKE/KEBAB its cell is one the row key registered itself, which ignores mapped cells.)
    @Test
    void cellSharedOnlyWithTheRowKeyReadsBackIntoItsOneWriter() {
        final RowKeySharedCellEntity source = new RowKeySharedCellEntity();
        source.setX("xv");
        source.setUserName("rk");
        source.setUserNAME("v");

        assertAll(Arrays.stream(NamingPolicy.values()).map(policy -> (Executable) () -> {
            final RowKeySharedCellEntity read = HBaseExecutor.toEntity(toResult(AnyPut.create(source, policy)), RowKeySharedCellEntity.class);

            assertEquals("rk", read.getUserName(), policy.name());
            assertEquals("xv", read.getX(), policy.name());

            if (policy == NamingPolicy.CAMEL_CASE || policy == NamingPolicy.SNAKE_CASE || policy == NamingPolicy.NO_CHANGE) {
                assertEquals("v", read.getUserNAME(), policy.name());
            }
        }));
    }

    public static class RowKeySharedCellEntity {
        @ColumnFamily("user_name")
        private String x;
        @Id
        @Column("k")
        private String userName;
        @Column("k")
        private String userNAME;

        public String getX() {
            return x;
        }

        public void setX(final String x) {
            this.x = x;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(final String userName) {
            this.userName = userName;
        }

        public String getUserNAME() {
            return userNAME;
        }

        public void setUserNAME(final String userNAME) {
            this.userNAME = userNAME;
        }
    }

    // ---- 2026-10-27 sliceM ----

    private static Cell cellOf(final String family, final String qualifier, final long timestamp, final String value) {
        return new KeyValue(Bytes.toBytes("row-x"), Bytes.toBytes(family), Bytes.toBytes(qualifier), timestamp, Bytes.toBytes(value));
    }

    private static Result resultOf(final Cell... cells) {
        final List<Cell> list = new ArrayList<>(Arrays.asList(cells));
        list.sort(CellComparator.getInstance());

        return Result.create(list);
    }

    // AnyPut.create flattens an immutable nested bean (a record, a bean with an all-args constructor, a builder-built bean) into
    // one cell per property, under every naming policy, but toEntity created each nested bean with its no-args constructor and
    // failed the whole row: IllegalArgumentException "No default constructor found".
    @Test
    void immutableNestedBeansReadBackWhatAnyPutCreateWrote() {
        final ImmutableNestedHolder source = new ImmutableNestedHolder();
        source.setId("row-1");
        source.setRecord(new NestedRecord("first", 3));
        source.setAllArgs(new AllArgsBean("second", 4));
        source.setBuilt(BuiltBean.builder().firstName("third").count(5).build());

        assertAll(Arrays.stream(NamingPolicy.values()).map(policy -> (Executable) () -> {
            final ImmutableNestedHolder read = HBaseExecutor.toEntity(toResult(AnyPut.create(source, policy)), ImmutableNestedHolder.class);

            assertEquals("row-1", read.getId(), policy.name());
            assertEquals(new NestedRecord("first", 3), read.getRecord(), policy.name());
            assertEquals("second", read.getAllArgs().getFirstName(), policy.name());
            assertEquals(4, read.getAllArgs().getCount(), policy.name());
            assertEquals("third", read.getBuilt().getFirstName(), policy.name());
            assertEquals(5, read.getBuilt().getCount(), policy.name());
        }));
    }

    // An immutable entity (no row-key property) is built from its intermediate result, which is a builder for a builder-based
    // bean: reading its nested bean through the bean's property threw ClassCastException; a nested record failed as above.
    @Test
    void immutableEntityBuildsItsMutableAndImmutableNestedBeans() {
        final Result result = resultOf(cellOf("name", "", 1, "n1"), cellOf("mutable", "value", 1, "m1"), cellOf("record", "firstName", 1, "r1"),
                cellOf("record", "count", 1, "7"));

        assertAll(() -> {
            final BuiltEntity read = HBaseExecutor.toEntity(result, BuiltEntity.class);

            assertEquals("n1", read.getName());
            assertEquals("m1", read.getMutable().getValue());
            assertEquals(new NestedRecord("r1", 7), read.getRecord());
        }, () -> {
            final RecordEntity read = HBaseExecutor.toEntity(result, RecordEntity.class);

            assertEquals("n1", read.name());
            assertEquals("m1", read.mutable().getValue());
            assertEquals(new NestedRecord("r1", 7), read.record());
        });
    }

    // A nested property without a cell keeps the value of the instance the property already holds, as for a mutable nested bean
    // filled in place (before: ClassCastException writing into the record); a read-only immutable nested bean cannot be filled
    // in place, so its family is skipped and the rest of the row is read (before: ClassCastException).
    @Test
    void immutableNestedBeanKeepsTheHeldInstanceValuesAndAReadOnlyOneIsSkipped() {
        final Result result = resultOf(cellOf("record", "count", 1, "9"), cellOf("fixed", "firstName", 1, "changed"), cellOf("name", "", 1, "k"));

        assertAll(() -> assertEquals(new NestedRecord("default", 9), HBaseExecutor.toEntity(result, DefaultedRecordHolder.class).getRecord()), () -> {
            final ReadOnlyRecordHolder read = HBaseExecutor.toEntity(result, ReadOnlyRecordHolder.class);

            assertEquals(new NestedRecord("fixed", 2), read.getFixed());
            assertEquals("k", read.getName());
        });
    }

    // Versioned components of an immutable nested bean get every version of their cell, as those of a mutable one do.
    @Test
    void versionedComponentsOfAnImmutableNestedBeanReadEveryVersion() {
        final Result result = resultOf(cellOf("versions", "one", 2, "o2"), cellOf("versions", "one", 1, "o1"), cellOf("versions", "list", 4, "l4"),
                cellOf("versions", "list", 3, "l3"), cellOf("versions", "map", 8, "m8"), cellOf("versions", "map", 7, "m7"));

        final VersionedRecord read = HBaseExecutor.toEntity(result, VersionedRecordHolder.class).getVersions();

        assertAll(() -> assertEquals("o2", read.one().value()), () -> assertEquals(2L, read.one().version()),
                () -> assertEquals(Arrays.asList("l4@4", "l3@3"), read.list().stream().map(c -> c.value() + "@" + c.version()).toList()),
                () -> assertEquals(Arrays.asList(7L, 8L), read.map().keySet().stream().sorted().toList()), () -> assertEquals("m8", read.map().get(8L).value()),
                () -> assertEquals("m7", read.map().get(7L).value()));
    }

    // An HBase operation or Any* wrapper passed where a row key is expected was converted through N.stringOf to its bean
    // JSON, which does not even contain its row: AnyGet.of((Object) anyGet), AnyDelete.of(anyDelete) (an attempted copy),
    // AnyPut.of(anyPut) or an AnyScan bound silently addressed that garbage row. Every Object row key goes through
    // toRowKeyBytes, which now rejects it.
    @Test
    void hbaseOperationPassedAsARowKeyIsRejected() {
        final Object anyGet = AnyGet.of("r1");
        final Object anyDelete = AnyDelete.of("r1");
        final Object anyPut = AnyPut.of("r1").addColumn("cf", "q", "v");
        final Object get = new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("r1"));

        final List<Executable> rejected = new ArrayList<>();

        for (final Object op : Arrays.asList(anyGet, anyDelete, anyPut, AnyScan.create(), AnyRowMutations.of("r1"), get,
                new org.apache.hadoop.hbase.client.Delete(Bytes.toBytes("r1")), new org.apache.hadoop.hbase.client.Scan(),
                new org.apache.hadoop.hbase.client.RowMutations(Bytes.toBytes("r1")))) {
            rejected.add(() -> assertTrue(
                    assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.toRowKeyBytes(op)).getMessage().contains(op.getClass().getSimpleName()),
                    op.getClass().getName()));
        }

        rejected.add(() -> assertThrows(IllegalArgumentException.class, () -> AnyGet.of(anyGet)));
        rejected.add(() -> assertThrows(IllegalArgumentException.class, () -> AnyDelete.of(anyDelete)));
        rejected.add(() -> assertThrows(IllegalArgumentException.class, () -> AnyPut.of(anyPut)));
        rejected.add(() -> assertThrows(IllegalArgumentException.class, () -> AnyIncrement.of(get)));
        rejected.add(() -> assertThrows(IllegalArgumentException.class, () -> AnyAppend.of(anyGet)));
        rejected.add(() -> assertThrows(IllegalArgumentException.class, () -> AnyScan.create().withStartRow(anyGet)));
        rejected.add(() -> assertThrows(IllegalArgumentException.class, () -> AnyScan.create().withStopRow(get)));
        rejected.add(() -> assertThrows(IllegalArgumentException.class, () -> AnyScan.create().setRowPrefixFilter(anyPut)));
        // A row-key value still converts.
        rejected.add(() -> assertEquals("r1", Bytes.toString(HBaseExecutor.toRowKeyBytes("r1"))));
        rejected.add(() -> assertEquals("12", Bytes.toString(HBaseExecutor.toRowKeyBytes(12))));

        assertAll(rejected);
    }

    // A versioned property declared as an immutable container got the converted copy setPropValue took of the still empty
    // ArrayList/HashSet/HashMap, while every version was added to the original: it read back empty, at the top level and as a
    // component of a nested record. The mutable List/Map twins read back as before.
    @Test
    void versionedPropertiesDeclaredAsImmutableContainersReadEveryVersion() {
        final Result result = resultOf(cellOf("hist", "", 3, "h3"), cellOf("hist", "", 2, "h2"), cellOf("set", "", 2, "s2"), cellOf("set", "", 1, "s1"),
                cellOf("ver", "", 5, "v5"), cellOf("ver", "", 4, "v4"), cellOf("nested", "hist", 7, "n7"), cellOf("nested", "hist", 6, "n6"),
                cellOf("nested", "name", 1, "nm"));

        final ImmutableVersionedEntity imm = HBaseExecutor.toEntity(result, ImmutableVersionedEntity.class);
        final MutableVersionedEntity mutable = HBaseExecutor.toEntity(result, MutableVersionedEntity.class);

        assertAll(() -> assertEquals(Arrays.asList("h3@3", "h2@2"), versions(imm.getHist())),
                () -> assertEquals(Arrays.asList("s1@1", "s2@2"), versions(imm.getSet()).stream().sorted().toList()),
                () -> assertEquals(Arrays.asList("v4@4", "v5@5"), versions(imm.getVer().values()).stream().sorted().toList()),
                () -> assertEquals(Arrays.asList(4L, 5L), imm.getVer().keySet().stream().sorted().toList()),
                () -> assertEquals(Arrays.asList("n7@7", "n6@6"), versions(imm.getNested().hist())), () -> assertEquals("nm", imm.getNested().name()),
                () -> assertEquals(Arrays.asList("h3@3", "h2@2"), versions(mutable.getHist())),
                () -> assertEquals(Arrays.asList("v4@4", "v5@5"), versions(mutable.getVer().values()).stream().sorted().toList()));
    }

    private static List<String> versions(final java.util.Collection<HBaseColumn<String>> columns) {
        return columns.stream().map(c -> c.value() + "@" + c.version()).toList();
    }

    // Second-pass review (r2M): the remaining branches of the immutable-nested-bean and versioned-container fixes.

    // A versioned container is assigned after the last cell whatever owns it: an immutable target (record or builder), a
    // builder-built nested bean, or a mutable nested bean filled in place whose property is an immutable container (before:
    // the immutable containers read back empty and the builder-built nested bean failed the row).
    @Test
    void versionedPropertiesReadEveryVersionForEveryKindOfOwner() {
        final Result top = resultOf(cellOf("name", "", 1, "n"), cellOf("hist", "", 2, "h2"), cellOf("hist", "", 1, "h1"), cellOf("ihist", "", 4, "i4"),
                cellOf("ihist", "", 3, "i3"), cellOf("iver", "", 8, "w8"), cellOf("iver", "", 7, "w7"), cellOf("one", "", 10, "o10"),
                cellOf("one", "", 9, "o9"));
        final Result nested = resultOf(cellOf("nested", "name", 1, "n"), cellOf("nested", "ihist", 4, "i4"), cellOf("nested", "ihist", 3, "i3"),
                cellOf("nested", "hist", 2, "h2"), cellOf("nested", "hist", 1, "h1"));

        assertAll(() -> {
            final ImmutableVersionedTarget read = HBaseExecutor.toEntity(top, ImmutableVersionedTarget.class);

            assertEquals("n", read.name());
            assertEquals(Arrays.asList("h2@2", "h1@1"), versions(read.hist()));
            assertEquals(Arrays.asList("i4@4", "i3@3"), versions(read.ihist()));
            assertEquals(Arrays.asList(7L, 8L), read.iver().keySet().stream().sorted().toList());
            assertEquals(Arrays.asList("w7@7", "w8@8"), versions(read.iver().values()).stream().sorted().toList());
            assertEquals("o10@10", read.one().value() + "@" + read.one().version());
        }, () -> {
            final BuiltVersioned read = HBaseExecutor.toEntity(top, BuiltVersioned.class);

            assertEquals("n", read.getName());
            assertEquals(Arrays.asList("i4@4", "i3@3"), versions(read.getIhist()));
            assertEquals(Arrays.asList("h2@2", "h1@1"), versions(read.getHist()));
        }, () -> {
            final BuiltVersioned read = HBaseExecutor.toEntity(nested, BuiltVersionedHolder.class).getNested();

            assertEquals("n", read.getName());
            assertEquals(Arrays.asList("i4@4", "i3@3"), versions(read.getIhist()));
            assertEquals(Arrays.asList("h2@2", "h1@1"), versions(read.getHist()));
        }, () -> {
            final ImmutableListValue read = HBaseExecutor.toEntity(nested, ImmutableListValueHolder.class).getNested();

            assertEquals("n", read.getName());
            assertEquals(Arrays.asList("i4@4", "i3@3"), versions(read.getIhist()));
        });
    }

    // An immutable nested bean the property already holds seeds the properties its cells do not set, whether it is built through
    // a builder or an all-args constructor (before: IllegalArgumentException / ClassCastException); a versioned component with
    // cells gets the row's versions and one without keeps the held value; without any cell the held instance is left as is.
    @Test
    void heldImmutableNestedBeansSeedThePropertiesTheirCellsDoNotSet() {
        assertAll(() -> {
            final BuiltBean read = HBaseExecutor.toEntity(resultOf(cellOf("built", "count", 1, "5")), SeededBuiltHolder.class).getBuilt();

            assertEquals("dflt", read.getFirstName());
            assertEquals(5, read.getCount());
        }, () -> {
            final AllArgsBean read = HBaseExecutor.toEntity(resultOf(cellOf("allArgs", "count", 1, "5")), SeededAllArgsHolder.class).getAllArgs();

            assertEquals("dflt", read.getFirstName());
            assertEquals(5, read.getCount());
        }, () -> {
            final ImmutableVersionedRecord read = HBaseExecutor
                    .toEntity(resultOf(cellOf("nested", "hist", 4, "h4"), cellOf("nested", "hist", 3, "h3")), SeededVersionedHolder.class)
                    .getNested();

            assertEquals("dflt", read.name());
            assertEquals(Arrays.asList("h4@4", "h3@3"), versions(read.hist()));
        }, () -> {
            final ImmutableVersionedRecord read = HBaseExecutor.toEntity(resultOf(cellOf("nested", "name", 1, "x")), SeededVersionedHolder.class).getNested();

            assertEquals("x", read.name());
            assertEquals(Arrays.asList("old@1"), versions(read.hist()));
        }, () -> assertEquals(new NestedRecord("default", 1),
                HBaseExecutor.toEntity(resultOf(cellOf("unmapped", "", 1, "x")), DefaultedRecordHolder.class).getRecord()));
    }

    // The cells of one immutable nested bean may come under several naming-policy variants of its family and qualifiers: they
    // all fill the same intermediate result, so it reads exactly as its mutable twin does (the later variant cell wins).
    @Test
    void immutableNestedBeanReachedThroughNameVariantsReadsLikeItsMutableTwin() {
        final Result families = resultOf(cellOf("homeAddress", "firstName", 1, "s1"), cellOf("home_address", "count", 1, "9"));
        final Result qualifiers = resultOf(cellOf("homeAddress", "firstName", 1, "s1"), cellOf("homeAddress", "first_name", 1, "s2"),
                cellOf("homeAddress", "count", 1, "3"));

        assertAll(() -> assertEquals(new NestedRecord("s1", 9), HBaseExecutor.toEntity(families, HomeRecordHolder.class).getHomeAddress()),
                () -> assertEquals("s1:9", HBaseExecutor.toEntity(families, HomeMutableHolder.class).getHomeAddress().toString()),
                () -> assertEquals(new NestedRecord("s2", 3), HBaseExecutor.toEntity(qualifiers, HomeRecordHolder.class).getHomeAddress()),
                () -> assertEquals("s2:3", HBaseExecutor.toEntity(qualifiers, HomeMutableHolder.class).getHomeAddress().toString()));
    }

    // Every Object row-key entry point of the executor and of its mapper converts through toRowKeyBytes, so an HBase operation or
    // Any* wrapper is rejected before the table is called (before: its bean JSON was used as the row key); row-key values convert.
    @Test
    void everyObjectRowKeyEntryPointRejectsAnOperation() throws Exception {
        final org.apache.hadoop.hbase.client.Connection conn = org.mockito.Mockito.mock(org.apache.hadoop.hbase.client.Connection.class);
        final org.apache.hadoop.hbase.client.Table table = org.mockito.Mockito.mock(org.apache.hadoop.hbase.client.Table.class);
        org.mockito.Mockito.when(conn.getTable(org.mockito.ArgumentMatchers.any(org.apache.hadoop.hbase.TableName.class))).thenReturn(table);

        final HBaseExecutor executor = new HBaseExecutor(conn);
        final HBaseExecutor.HBaseMapper<ImmutableNestedHolder, Object> mapper = executor.mapper(ImmutableNestedHolder.class, "t", null);
        final Object op = AnyGet.of("r1");
        final Object get = new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("r1"));
        final byte[] cf = Bytes.toBytes("cf");
        final org.apache.hadoop.hbase.client.Durability durability = org.apache.hadoop.hbase.client.Durability.SYNC_WAL;
        final Class<com.google.protobuf.Service> service = com.google.protobuf.Service.class;
        final org.apache.hadoop.hbase.client.coprocessor.Batch.Call<com.google.protobuf.Service, Object> call = s -> null;
        final org.apache.hadoop.hbase.client.coprocessor.Batch.Callback<Object> callback = (region, row, value) -> {
        };
        final org.apache.hadoop.hbase.client.coprocessor.Batch.Callback<com.google.protobuf.Message> messageCallback = (region, row, value) -> {
        };
        final com.google.protobuf.Message prototype = null;

        final List<Executable> entryPoints = Arrays.asList(() -> executor.exists("t", op), () -> executor.get("t", op),
                () -> executor.get("t", get, ImmutableNestedHolder.class), () -> executor.delete("t", get),
                () -> executor.incrementColumnValue("t", op, "cf", "q", 1L), () -> executor.incrementColumnValue("t", get, "cf", "q", 1L, durability),
                () -> executor.incrementColumnValue("t", op, cf, cf, 1L), () -> executor.incrementColumnValue("t", get, cf, cf, 1L, durability),
                () -> executor.coprocessorService("t", op), () -> executor.coprocessorService("t", service, op, "z", call),
                () -> executor.coprocessorService("t", service, "a", get, call, callback),
                () -> executor.batchCoprocessorService("t", null, null, op, "z", prototype),
                () -> executor.batchCoprocessorService("t", null, null, "a", get, prototype, messageCallback), () -> mapper.exists(op),
                () -> mapper.exists(Arrays.asList("r0", get)), () -> mapper.get(op), () -> mapper.get(Arrays.asList("r0", get)),
                () -> mapper.deleteByRowKey(op), () -> mapper.deleteByRowKey(Arrays.asList("r0", get)), () -> mapper.incrementColumnValue(op, "cf", "q", 1L),
                () -> mapper.incrementColumnValue(get, "cf", "q", 1L, durability), () -> mapper.incrementColumnValue(op, cf, cf, 1L),
                () -> mapper.incrementColumnValue(get, cf, cf, 1L, durability), () -> mapper.coprocessorService(op),
                () -> mapper.coprocessorService(service, op, "z", call), () -> mapper.coprocessorService(service, "a", get, call, callback),
                () -> mapper.batchCoprocessorService(null, null, op, "z", prototype),
                () -> mapper.batchCoprocessorService(null, null, "a", get, prototype, messageCallback));

        final byte[] raw = Bytes.toBytes("r1");
        final List<Executable> checks = new ArrayList<>();

        for (int i = 0; i < entryPoints.size(); i++) {
            final Executable entryPoint = entryPoints.get(i);
            final String name = "entry point #" + i;

            checks.add(() -> assertTrue(assertThrows(IllegalArgumentException.class, entryPoint, name).getMessage().contains("as a row key"), name));
        }

        checks.add(() -> org.junit.jupiter.api.Assertions.assertSame(raw, HBaseExecutor.toRowKeyBytes(raw)));
        checks.add(() -> assertEquals("r1", Bytes.toString(HBaseExecutor.toRowKeyBytes(java.nio.ByteBuffer.wrap(raw)))));
        checks.add(() -> assertEquals("r1", Bytes.toString(HBaseExecutor.toRowKeyBytes(u.Optional.of("r1")))));
        checks.add(() -> assertEquals("7", Bytes.toString(HBaseExecutor.toRowKeyBytes(7L))));
        checks.add(() -> assertNull(HBaseExecutor.toRowKeyBytes(null)));
        checks.add(() -> assertNull(HBaseExecutor.toRowKeyBytes(java.util.Optional.empty())));

        assertAll(checks);
    }

    // Second-pass cross-check (r2X): a present value wrapper converts as the value it holds, so one holding an HBase operation
    // still became the operation's bean JSON row key (before: AnyGet.of(Optional.of(anyGet)) addressed that garbage row). It is
    // rejected like the bare operation, also when nested; empty wrappers and wrappers of a row-key value convert as before.
    @Test
    void presentValueWrapperHoldingAnOperationIsRejectedAsARowKey() {
        final Object get = new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("r1"));
        final Object put = new org.apache.hadoop.hbase.client.Put(Bytes.toBytes("r1"));
        final List<Object> wrapped = Arrays.asList(java.util.Optional.of(AnyGet.of("r1")), java.util.Optional.of(get), u.Optional.of(AnyPut.of("r1")),
                u.Nullable.of(put), Holder.of(AnyDelete.of("r1")), java.util.Optional.of(u.Optional.of(AnyScan.create())),
                u.Nullable.of(Holder.of(new org.apache.hadoop.hbase.client.RowMutations(Bytes.toBytes("r1")))));

        final List<Executable> checks = new ArrayList<>();

        for (final Object key : wrapped) {
            checks.add(() -> {
                final String message = assertThrows(IllegalArgumentException.class, () -> HBaseExecutor.toRowKeyBytes(key), key.toString()).getMessage();

                assertTrue(message.contains("as a row key") && message.contains(key.getClass().getSimpleName()), message);
            });
        }

        checks.add(() -> assertThrows(IllegalArgumentException.class, () -> AnyGet.of(java.util.Optional.of(AnyGet.of("r1")))));
        checks.add(() -> assertThrows(IllegalArgumentException.class, () -> AnyPut.of(u.Nullable.of(put))));
        checks.add(() -> assertThrows(IllegalArgumentException.class, () -> AnyDelete.of(Holder.of(AnyDelete.of("r1")))));
        checks.add(() -> assertThrows(IllegalArgumentException.class, () -> AnyScan.create().withStartRow(u.Optional.of(get))));
        // Unchanged: a wrapper of a row-key value converts as that value, an empty one to null.
        checks.add(() -> assertEquals("r1", Bytes.toString(HBaseExecutor.toRowKeyBytes(java.util.Optional.of("r1")))));
        checks.add(() -> assertEquals("r1", Bytes.toString(HBaseExecutor.toRowKeyBytes(u.Nullable.of("r1")))));
        checks.add(() -> assertEquals("r1", Bytes.toString(HBaseExecutor.toRowKeyBytes(Holder.of(java.util.Optional.of("r1"))))));
        checks.add(() -> assertEquals("7", Bytes.toString(HBaseExecutor.toRowKeyBytes(u.Optional.of(7L)))));
        checks.add(() -> assertEquals("r1", Bytes.toString(AnyGet.of(java.util.Optional.of("r1")).val().getRow())));
        checks.add(() -> assertNull(HBaseExecutor.toRowKeyBytes(u.Nullable.of((Object) null))));
        checks.add(() -> assertNull(HBaseExecutor.toRowKeyBytes(Holder.of(null))));
        checks.add(() -> assertNull(HBaseExecutor.toRowKeyBytes(u.Optional.empty())));

        assertAll(checks);
    }

    // Fix round (fixA): a nested bean created for a row is assigned to its property once, after the last cell.

    private static Result nestedCopyRow() {
        return resultOf(cellOf("held", "count", 1, "4"), cellOf("homeValue", "firstName", 1, "s1"), cellOf("home_value", "count", 1, "9"),
                cellOf("name", "", 1, "n"), cellOf("versioned", "ihist", 4, "i4"), cellOf("versioned", "ihist", 3, "i3"), cellOf("versioned", "name", 1, "vn"));
    }

    // A setter or builder method that copies its nested-bean argument was handed the still empty bean as soon as it was created:
    // a builder kept none of its cells (homeValue null:0, versioned empty), a mutable entity lost those written before it read
    // its copy back (null:9) and the versioned container assigned to the original. The bean is now handed over complete, with
    // its versioned ImmutableList assigned first (also through a family name variant); a held instance is still filled in place
    // (the setter is never called) and the plain-assignment twins read as before.
    @Test
    void nestedBeanCreatedForARowReachesACopyingSetterOrBuilderMethodComplete() {
        assertAll(() -> {
            final CopyingSetterHolder read = HBaseExecutor.toEntity(nestedCopyRow(), CopyingSetterHolder.class);

            assertEquals("s1:9", String.valueOf(read.getHomeValue()));
            assertEquals("vn", read.getVersioned().getName());
            assertEquals(Arrays.asList("i4@4", "i3@3"), versions(read.getVersioned().getIhist()));
            assertEquals("init:4", read.getHeld().toString());
        }, () -> {
            final CopyingBuiltEntity read = HBaseExecutor.toEntity(nestedCopyRow(), CopyingBuiltEntity.class);

            assertEquals("n", read.getName());
            assertEquals("s1:9", String.valueOf(read.getHomeValue()));
            assertEquals("vn", read.getVersioned().getName());
            assertEquals(Arrays.asList("i4@4", "i3@3"), versions(read.getVersioned().getIhist()));
        }, () -> {
            final PlainSetterHolder read = HBaseExecutor.toEntity(nestedCopyRow(), PlainSetterHolder.class);

            assertEquals("s1:9", String.valueOf(read.getHomeValue()));
            assertEquals("vn", read.getVersioned().getName());
            assertEquals(Arrays.asList("i4@4", "i3@3"), versions(read.getVersioned().getIhist()));
            assertEquals("init:4", read.getHeld().toString());
        }, () -> {
            final PlainBuiltEntity read = HBaseExecutor.toEntity(nestedCopyRow(), PlainBuiltEntity.class);

            assertEquals("n", read.getName());
            assertEquals("s1:9", String.valueOf(read.getHomeValue()));
            assertEquals("vn", read.getVersioned().getName());
            assertEquals(Arrays.asList("i4@4", "i3@3"), versions(read.getVersioned().getIhist()));
        });
    }

    // The nested beans of a row live only for that conversion: converting the same Result again reads the same values into new
    // instances.
    @Test
    void sameResultConvertedTwiceGetsEqualButSeparateNestedBeans() {
        final Result row = nestedCopyRow();
        final CopyingSetterHolder first = HBaseExecutor.toEntity(row, CopyingSetterHolder.class);
        final CopyingSetterHolder second = HBaseExecutor.toEntity(row, CopyingSetterHolder.class);
        final CopyingBuiltEntity built1 = HBaseExecutor.toEntity(row, CopyingBuiltEntity.class);
        final CopyingBuiltEntity built2 = HBaseExecutor.toEntity(row, CopyingBuiltEntity.class);

        assertAll(() -> assertEquals("s1:9", String.valueOf(first.getHomeValue())), () -> assertEquals("s1:9", String.valueOf(second.getHomeValue())),
                () -> org.junit.jupiter.api.Assertions.assertNotSame(first.getHomeValue(), second.getHomeValue()),
                () -> assertEquals(Arrays.asList("i4@4", "i3@3"), versions(second.getVersioned().getIhist())),
                () -> assertEquals("s1:9", String.valueOf(built1.getHomeValue())), () -> assertEquals("s1:9", String.valueOf(built2.getHomeValue())),
                () -> org.junit.jupiter.api.Assertions.assertNotSame(built1.getHomeValue(), built2.getHomeValue()),
                () -> assertEquals(Arrays.asList("i4@4", "i3@3"), versions(built2.getVersioned().getIhist())));
    }

    // Pins the documented rule: only a mutable entity's property is read to seed a nested bean, so the nested value an immutable
    // entity's builder starts with is replaced by a bean built from the row's cells alone (its properties without a cell get their
    // defaults), whether that nested bean is immutable or mutable; without any cell of the nested bean the builder's value stays.
    @Test
    void immutableEntityDoesNotSeedANestedBeanFromItsBuilderDefault() {
        final BuilderDefaultsEntity withCells = HBaseExecutor.toEntity(
                resultOf(cellOf("homeValue", "count", 1, "5"), cellOf("name", "", 1, "n"), cellOf("record", "count", 1, "5")), BuilderDefaultsEntity.class);
        final BuilderDefaultsEntity withoutCells = HBaseExecutor.toEntity(resultOf(cellOf("name", "", 1, "n")), BuilderDefaultsEntity.class);

        assertAll(() -> assertEquals("n", withCells.getName()), () -> assertEquals(new NestedRecord(null, 5), withCells.getRecord()),
                () -> assertEquals("null:5", String.valueOf(withCells.getHomeValue())),
                () -> assertEquals(new NestedRecord("builderDefault", 9), withoutCells.getRecord()),
                () -> assertEquals("builderDefault:9", String.valueOf(withoutCells.getHomeValue())));
    }

    public static class CopiedValue {
        private String firstName;
        private int count;

        public CopiedValue() {
        }

        public CopiedValue(final String firstName, final int count) {
            this.firstName = firstName;
            this.count = count;
        }

        static CopiedValue copyOf(final CopiedValue source) {
            return source == null ? null : new CopiedValue(source.firstName, source.count);
        }

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(final String firstName) {
            this.firstName = firstName;
        }

        public int getCount() {
            return count;
        }

        public void setCount(final int count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return firstName + ":" + count;
        }
    }

    public static class CopiedVersioned {
        private String name;
        private com.landawn.abacus.util.ImmutableList<HBaseColumn<String>> ihist;

        static CopiedVersioned copyOf(final CopiedVersioned source) {
            if (source == null) {
                return null;
            }

            final CopiedVersioned copy = new CopiedVersioned();
            copy.name = source.name;
            copy.ihist = source.ihist;
            return copy;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public com.landawn.abacus.util.ImmutableList<HBaseColumn<String>> getIhist() {
            return ihist;
        }

        public void setIhist(final com.landawn.abacus.util.ImmutableList<HBaseColumn<String>> ihist) {
            this.ihist = ihist;
        }
    }

    // @AccessFieldByMethod: the properties are written through the setters, which copy their argument.
    @AccessFieldByMethod
    public static class CopyingSetterHolder {
        @Id
        private String id;
        private CopiedValue homeValue;
        private CopiedVersioned versioned;
        private CopiedValue held = new CopiedValue("init", 1);

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public CopiedValue getHomeValue() {
            return homeValue;
        }

        public void setHomeValue(final CopiedValue homeValue) {
            this.homeValue = CopiedValue.copyOf(homeValue);
        }

        public CopiedVersioned getVersioned() {
            return versioned;
        }

        public void setVersioned(final CopiedVersioned versioned) {
            this.versioned = CopiedVersioned.copyOf(versioned);
        }

        public CopiedValue getHeld() {
            return held;
        }

        public void setHeld(final CopiedValue held) {
            this.held = CopiedValue.copyOf(held);
        }
    }

    @AccessFieldByMethod
    public static class PlainSetterHolder {
        @Id
        private String id;
        private CopiedValue homeValue;
        private CopiedVersioned versioned;
        private CopiedValue held = new CopiedValue("init", 1);

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public CopiedValue getHomeValue() {
            return homeValue;
        }

        public void setHomeValue(final CopiedValue homeValue) {
            this.homeValue = homeValue;
        }

        public CopiedVersioned getVersioned() {
            return versioned;
        }

        public void setVersioned(final CopiedVersioned versioned) {
            this.versioned = versioned;
        }

        public CopiedValue getHeld() {
            return held;
        }

        public void setHeld(final CopiedValue held) {
            this.held = held;
        }
    }

    public static final class CopyingBuiltEntity {
        private final String name;
        private final CopiedValue homeValue;
        private final CopiedVersioned versioned;

        private CopyingBuiltEntity(final String name, final CopiedValue homeValue, final CopiedVersioned versioned) {
            this.name = name;
            this.homeValue = homeValue;
            this.versioned = versioned;
        }

        public String getName() {
            return name;
        }

        public CopiedValue getHomeValue() {
            return homeValue;
        }

        public CopiedVersioned getVersioned() {
            return versioned;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private String name;
            private CopiedValue homeValue;
            private CopiedVersioned versioned;

            public Builder name(final String name) {
                this.name = name;
                return this;
            }

            public Builder homeValue(final CopiedValue homeValue) {
                this.homeValue = CopiedValue.copyOf(homeValue);
                return this;
            }

            public Builder versioned(final CopiedVersioned versioned) {
                this.versioned = CopiedVersioned.copyOf(versioned);
                return this;
            }

            public CopyingBuiltEntity build() {
                return new CopyingBuiltEntity(name, homeValue, versioned);
            }
        }
    }

    public static final class PlainBuiltEntity {
        private final String name;
        private final CopiedValue homeValue;
        private final CopiedVersioned versioned;

        private PlainBuiltEntity(final String name, final CopiedValue homeValue, final CopiedVersioned versioned) {
            this.name = name;
            this.homeValue = homeValue;
            this.versioned = versioned;
        }

        public String getName() {
            return name;
        }

        public CopiedValue getHomeValue() {
            return homeValue;
        }

        public CopiedVersioned getVersioned() {
            return versioned;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private String name;
            private CopiedValue homeValue;
            private CopiedVersioned versioned;

            public Builder name(final String name) {
                this.name = name;
                return this;
            }

            public Builder homeValue(final CopiedValue homeValue) {
                this.homeValue = homeValue;
                return this;
            }

            public Builder versioned(final CopiedVersioned versioned) {
                this.versioned = versioned;
                return this;
            }

            public PlainBuiltEntity build() {
                return new PlainBuiltEntity(name, homeValue, versioned);
            }
        }
    }

    public static final class BuilderDefaultsEntity {
        private final String name;
        private final NestedRecord record;
        private final CopiedValue homeValue;

        private BuilderDefaultsEntity(final String name, final NestedRecord record, final CopiedValue homeValue) {
            this.name = name;
            this.record = record;
            this.homeValue = homeValue;
        }

        public String getName() {
            return name;
        }

        public NestedRecord getRecord() {
            return record;
        }

        public CopiedValue getHomeValue() {
            return homeValue;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private String name;
            private NestedRecord record = new NestedRecord("builderDefault", 9);
            private CopiedValue homeValue = new CopiedValue("builderDefault", 9);

            public Builder name(final String name) {
                this.name = name;
                return this;
            }

            public Builder record(final NestedRecord record) {
                this.record = record;
                return this;
            }

            public Builder homeValue(final CopiedValue homeValue) {
                this.homeValue = homeValue;
                return this;
            }

            public BuilderDefaultsEntity build() {
                return new BuilderDefaultsEntity(name, record, homeValue);
            }
        }
    }

    public record ImmutableVersionedTarget(String name, List<HBaseColumn<String>> hist, com.landawn.abacus.util.ImmutableList<HBaseColumn<String>> ihist,
            com.landawn.abacus.util.ImmutableMap<Long, HBaseColumn<String>> iver, HBaseColumn<String> one) {
    }

    public static final class BuiltVersioned {
        private final String name;
        private final com.landawn.abacus.util.ImmutableList<HBaseColumn<String>> ihist;
        private final List<HBaseColumn<String>> hist;

        private BuiltVersioned(final String name, final com.landawn.abacus.util.ImmutableList<HBaseColumn<String>> ihist,
                final List<HBaseColumn<String>> hist) {
            this.name = name;
            this.ihist = ihist;
            this.hist = hist;
        }

        public String getName() {
            return name;
        }

        public com.landawn.abacus.util.ImmutableList<HBaseColumn<String>> getIhist() {
            return ihist;
        }

        public List<HBaseColumn<String>> getHist() {
            return hist;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private String name;
            private com.landawn.abacus.util.ImmutableList<HBaseColumn<String>> ihist;
            private List<HBaseColumn<String>> hist;

            public Builder name(final String name) {
                this.name = name;
                return this;
            }

            public Builder ihist(final com.landawn.abacus.util.ImmutableList<HBaseColumn<String>> ihist) {
                this.ihist = ihist;
                return this;
            }

            public Builder hist(final List<HBaseColumn<String>> hist) {
                this.hist = hist;
                return this;
            }

            public BuiltVersioned build() {
                return new BuiltVersioned(name, ihist, hist);
            }
        }
    }

    public static class BuiltVersionedHolder {
        @Id
        private String id;
        private BuiltVersioned nested;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public BuiltVersioned getNested() {
            return nested;
        }

        public void setNested(final BuiltVersioned nested) {
            this.nested = nested;
        }
    }

    public static class ImmutableListValue {
        private String name;
        private com.landawn.abacus.util.ImmutableList<HBaseColumn<String>> ihist;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public com.landawn.abacus.util.ImmutableList<HBaseColumn<String>> getIhist() {
            return ihist;
        }

        public void setIhist(final com.landawn.abacus.util.ImmutableList<HBaseColumn<String>> ihist) {
            this.ihist = ihist;
        }
    }

    public static class ImmutableListValueHolder {
        @Id
        private String id;
        private ImmutableListValue nested;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public ImmutableListValue getNested() {
            return nested;
        }

        public void setNested(final ImmutableListValue nested) {
            this.nested = nested;
        }
    }

    public static class SeededBuiltHolder {
        @Id
        private String id;
        private BuiltBean built = BuiltBean.builder().firstName("dflt").count(11).build();

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public BuiltBean getBuilt() {
            return built;
        }

        public void setBuilt(final BuiltBean built) {
            this.built = built;
        }
    }

    public static class SeededAllArgsHolder {
        @Id
        private String id;
        private AllArgsBean allArgs = new AllArgsBean("dflt", 11);

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public AllArgsBean getAllArgs() {
            return allArgs;
        }

        public void setAllArgs(final AllArgsBean allArgs) {
            this.allArgs = allArgs;
        }
    }

    public static class SeededVersionedHolder {
        @Id
        private String id;
        private ImmutableVersionedRecord nested = new ImmutableVersionedRecord(com.landawn.abacus.util.ImmutableList.of(HBaseColumn.valueOf("old", 1L)),
                "dflt");

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public ImmutableVersionedRecord getNested() {
            return nested;
        }

        public void setNested(final ImmutableVersionedRecord nested) {
            this.nested = nested;
        }
    }

    public static class MutableNameCount {
        private String firstName;
        private int count;

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(final String firstName) {
            this.firstName = firstName;
        }

        public int getCount() {
            return count;
        }

        public void setCount(final int count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return firstName + ":" + count;
        }
    }

    public static class HomeRecordHolder {
        @Id
        private String id;
        private NestedRecord homeAddress;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public NestedRecord getHomeAddress() {
            return homeAddress;
        }

        public void setHomeAddress(final NestedRecord homeAddress) {
            this.homeAddress = homeAddress;
        }
    }

    public static class HomeMutableHolder {
        @Id
        private String id;
        private MutableNameCount homeAddress;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public MutableNameCount getHomeAddress() {
            return homeAddress;
        }

        public void setHomeAddress(final MutableNameCount homeAddress) {
            this.homeAddress = homeAddress;
        }
    }

    public record ImmutableVersionedRecord(com.landawn.abacus.util.ImmutableList<HBaseColumn<String>> hist, String name) {
    }

    public static class ImmutableVersionedEntity {
        @Id
        private String id;
        private com.landawn.abacus.util.ImmutableList<HBaseColumn<String>> hist;
        private com.landawn.abacus.util.ImmutableSet<HBaseColumn<String>> set;
        private com.landawn.abacus.util.ImmutableMap<Long, HBaseColumn<String>> ver;
        private ImmutableVersionedRecord nested;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public com.landawn.abacus.util.ImmutableList<HBaseColumn<String>> getHist() {
            return hist;
        }

        public void setHist(final com.landawn.abacus.util.ImmutableList<HBaseColumn<String>> hist) {
            this.hist = hist;
        }

        public com.landawn.abacus.util.ImmutableSet<HBaseColumn<String>> getSet() {
            return set;
        }

        public void setSet(final com.landawn.abacus.util.ImmutableSet<HBaseColumn<String>> set) {
            this.set = set;
        }

        public com.landawn.abacus.util.ImmutableMap<Long, HBaseColumn<String>> getVer() {
            return ver;
        }

        public void setVer(final com.landawn.abacus.util.ImmutableMap<Long, HBaseColumn<String>> ver) {
            this.ver = ver;
        }

        public ImmutableVersionedRecord getNested() {
            return nested;
        }

        public void setNested(final ImmutableVersionedRecord nested) {
            this.nested = nested;
        }
    }

    public static class MutableVersionedEntity {
        @Id
        private String id;
        private List<HBaseColumn<String>> hist;
        private Map<Long, HBaseColumn<String>> ver;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public List<HBaseColumn<String>> getHist() {
            return hist;
        }

        public void setHist(final List<HBaseColumn<String>> hist) {
            this.hist = hist;
        }

        public Map<Long, HBaseColumn<String>> getVer() {
            return ver;
        }

        public void setVer(final Map<Long, HBaseColumn<String>> ver) {
            this.ver = ver;
        }
    }

    public record NestedRecord(String firstName, int count) {
    }

    public record VersionedRecord(HBaseColumn<String> one, List<HBaseColumn<String>> list, Map<Long, HBaseColumn<String>> map) {
    }

    public record RecordEntity(String name, MutableValue mutable, NestedRecord record) {
    }

    public static final class AllArgsBean {
        private final String firstName;
        private final int count;

        public AllArgsBean(final String firstName, final int count) {
            this.firstName = firstName;
            this.count = count;
        }

        public String getFirstName() {
            return firstName;
        }

        public int getCount() {
            return count;
        }
    }

    public static final class BuiltBean {
        private final String firstName;
        private final int count;

        private BuiltBean(final String firstName, final int count) {
            this.firstName = firstName;
            this.count = count;
        }

        public String getFirstName() {
            return firstName;
        }

        public int getCount() {
            return count;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private String firstName;
            private int count;

            public Builder firstName(final String firstName) {
                this.firstName = firstName;
                return this;
            }

            public Builder count(final int count) {
                this.count = count;
                return this;
            }

            public BuiltBean build() {
                return new BuiltBean(firstName, count);
            }
        }
    }

    public static class MutableValue {
        private String value;

        public String getValue() {
            return value;
        }

        public void setValue(final String value) {
            this.value = value;
        }
    }

    public static final class BuiltEntity {
        private final String name;
        private final MutableValue mutable;
        private final NestedRecord record;

        private BuiltEntity(final String name, final MutableValue mutable, final NestedRecord record) {
            this.name = name;
            this.mutable = mutable;
            this.record = record;
        }

        public String getName() {
            return name;
        }

        public MutableValue getMutable() {
            return mutable;
        }

        public NestedRecord getRecord() {
            return record;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private String name;
            private MutableValue mutable;
            private NestedRecord record;

            public Builder name(final String name) {
                this.name = name;
                return this;
            }

            public Builder mutable(final MutableValue mutable) {
                this.mutable = mutable;
                return this;
            }

            public Builder record(final NestedRecord record) {
                this.record = record;
                return this;
            }

            public BuiltEntity build() {
                return new BuiltEntity(name, mutable, record);
            }
        }
    }

    public static class ImmutableNestedHolder {
        @Id
        private String id;
        private NestedRecord record;
        private AllArgsBean allArgs;
        private BuiltBean built;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public NestedRecord getRecord() {
            return record;
        }

        public void setRecord(final NestedRecord record) {
            this.record = record;
        }

        public AllArgsBean getAllArgs() {
            return allArgs;
        }

        public void setAllArgs(final AllArgsBean allArgs) {
            this.allArgs = allArgs;
        }

        public BuiltBean getBuilt() {
            return built;
        }

        public void setBuilt(final BuiltBean built) {
            this.built = built;
        }
    }

    public static class DefaultedRecordHolder {
        @Id
        private String id;
        private NestedRecord record = new NestedRecord("default", 1);

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public NestedRecord getRecord() {
            return record;
        }

        public void setRecord(final NestedRecord record) {
            this.record = record;
        }
    }

    @Entity
    @AccessFieldByMethod
    public static class ReadOnlyRecordHolder {
        @Id
        private String id;
        private final NestedRecord fixed = new NestedRecord("fixed", 2);
        private String name;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public NestedRecord getFixed() {
            return fixed;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    public static class VersionedRecordHolder {
        @Id
        private String id;
        private VersionedRecord versions;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public VersionedRecord getVersions() {
            return versions;
        }

        public void setVersions(final VersionedRecord versions) {
            this.versions = versions;
        }
    }
}
