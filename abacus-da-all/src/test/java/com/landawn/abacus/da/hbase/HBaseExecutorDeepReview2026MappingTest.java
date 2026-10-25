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
}
