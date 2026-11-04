package com.landawn.abacus.da.cassandra.v3;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.landawn.abacus.annotation.Column;
import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.annotation.JsonXmlField;
import com.landawn.abacus.annotation.Table;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.query.condition.Criteria;

/**
 * Regression tests for the 2026-11-03 review of the driver 3.x {@link CassandraExecutor}.
 */
@SuppressWarnings({ "deprecation", "rawtypes" })
public class CassandraExecutorReview20261103Test {

    // Before the fix: a nested Row column of a collection row was added with a bare Collection.add, so a sorted row class (TreeSet)
    // failed with ClassCastException ("ArrayList cannot be cast to Comparable") naming no column, in both row readers (toList's
    // createRowMapper and findFirst's readRow); every other column value already went through addRowValue. A row class that
    // accepts the nested row's list still holds it.
    @Test
    public void testNestedRowColumnRejectedBySortedRowCollectionNamesTheColumn() {
        final Row outer = row(new String[] { "nested" }, row(new String[] { "a" }, "x"));
        final String message = "Column (nested, [x]) is not accepted by the row collection java.util.TreeSet";

        assertAll(() -> {
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toList(result(outer), TreeSet.class));
            assertEquals(message, e.getMessage());
            assertInstanceOf(ClassCastException.class, e.getCause());
        }, () -> {
            try (CassandraTestFixture fixture = new CassandraTestFixture()) {
                fixture.columns(DataType.text());
                final ResultSet resultSet = result(outer);
                when(fixture.session.execute(any(Statement.class))).thenReturn(resultSet);

                final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                        () -> fixture.executor.findFirst(TreeSet.class, "SELECT nested FROM t"));
                assertEquals(message, e.getMessage());
                assertInstanceOf(ClassCastException.class, e.getCause());
            }
        }, () -> {
            final List<ArrayList> rows = CassandraExecutor.toList(result(outer), ArrayList.class);
            assertEquals(List.of(List.of(List.of("x"))), rows);
        });
    }

    // Pins the toList @throws added for collection row classes (509af62 routed column values through addRowValue): a NULL column
    // read as an ArrayBlockingQueue row is an IllegalArgumentException naming the column, with the queue's NPE as the cause.
    @Test
    public void testNullColumnIntoArrayBlockingQueueRowNamesTheColumn() {
        final Row row = row(new String[] { "id", "name" }, 7L, null);

        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toList(result(row), ArrayBlockingQueue.class));

        assertEquals("Column (name, null) is not accepted by the row collection java.util.concurrent.ArrayBlockingQueue", e.getMessage());
        assertInstanceOf(NullPointerException.class, e.getCause());
    }

    // Pins the @throws IllegalStateException added to this class's own gett(Class, Collection, Condition) Javadoc: it appends
    // LIMIT 2, so a condition that already carries a LIMIT fails before any CQL is prepared or executed.
    @Test
    public void testGettRejectsConditionWithItsOwnLimit() {
        final Condition limited = Criteria.builder().where(Filters.eq("accountId", 7L)).limit(5).build();

        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            assertThrows(IllegalStateException.class, () -> fixture.executor.gett(LimitAccount.class, null, limited));
            verify(fixture.session, never()).prepare(org.mockito.ArgumentMatchers.anyString());
            verify(fixture.session, never()).execute(any(Statement.class));
        }
    }

    // Pins the clarified toList contract for a raw-row target: the list ResultSet.all() returns is handed back as is (driver 3
    // returns the immutable Collections.emptyList() for an exhausted result set), while any other target gets a new mutable list.
    @Test
    public void testRawRowTargetReturnsTheDriverAllListEvenWhenEmpty() {
        final ResultSet empty = mock(ResultSet.class);
        final ColumnDefinitions metadata = mock(ColumnDefinitions.class);
        when(empty.getColumnDefinitions()).thenReturn(metadata);
        when(empty.all()).thenReturn(Collections.emptyList());
        when(empty.iterator()).thenAnswer(invocation -> Collections.emptyIterator());

        assertSame(Collections.emptyList(), CassandraExecutor.toList(empty, Row.class));

        final List<Map> maps = CassandraExecutor.toList(empty, Map.class);
        maps.add(Map.of());
        assertEquals(1, maps.size());
    }

    // Pins the UDT decoder @throws extended to collection types: a NULL field decoded into an ArrayBlockingQueue is an
    // IllegalArgumentException naming the field, with the queue's NPE as the cause.
    @Test
    public void testUdtNullFieldIntoArrayBlockingQueueNamesTheField() throws Exception {
        final UserType userType = userType(new String[] { "a", "b" }, DataType.text(), DataType.text());
        final CassandraExecutor.UDTCodec<ArrayBlockingQueue> codec = CassandraExecutor.UDTCodec.create(userType, ArrayBlockingQueue.class);
        final UDTValue value = mock(UDTValue.class);
        when(value.getObject(0)).thenReturn("x");
        when(value.getObject(1)).thenReturn(null);

        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> codec.deserialize(value));

        assertEquals("Field (b, null) is not accepted by the row collection java.util.concurrent.ArrayBlockingQueue", e.getMessage());
        assertInstanceOf(NullPointerException.class, e.getCause());
    }

    private static UserType userType(final String[] names, final DataType... types) throws Exception {
        final Constructor<UserType.Field> fieldConstructor = UserType.Field.class.getDeclaredConstructor(String.class, DataType.class);
        fieldConstructor.setAccessible(true);
        final Collection<UserType.Field> fields = new ArrayList<>();

        for (int i = 0; i < names.length; i++) {
            fields.add(fieldConstructor.newInstance(names[i], types[i]));
        }

        final Constructor<UserType> constructor = UserType.class.getDeclaredConstructor(String.class, String.class, boolean.class, Collection.class,
                ProtocolVersion.class, CodecRegistry.class);
        constructor.setAccessible(true);
        return constructor.newInstance("ks", "pair", false, fields, ProtocolVersion.V4, new CodecRegistry());
    }

    /** Properties backed only by public fields: abacus (BeanInfo), insert/update and toEntity read them, but they have no getter. */
    public static class FieldOnlyBean {
        public String id;
        public byte[] data;
        public String addr;
    }

    // Before the fix: extractData typed a column by the property's getter only, so a property backed by a public field kept the raw
    // driver value (a blob as a ByteBuffer for a byte[] field, an inet as an InetAddress for a String field) where toEntity converts it.
    @Test
    public void testExtractDataConvertsColumnsOfFieldOnlyProperties() throws Exception {
        final java.net.InetAddress address = java.net.InetAddress.getByName("127.0.0.1");
        final Row row = row(new String[] { "id", "data", "addr" }, "1", java.nio.ByteBuffer.wrap(new byte[] { 1, 2 }), address);

        final com.landawn.abacus.util.Dataset dataset = CassandraExecutor.extractData(result(row), FieldOnlyBean.class);
        final FieldOnlyBean entity = CassandraExecutor.toEntity(row, FieldOnlyBean.class);

        assertAll(() -> assertEquals("1", dataset.getColumn("id").get(0)),
                () -> assertArrayEquals(new byte[] { 1, 2 }, (byte[]) dataset.getColumn("data").get(0)),
                () -> assertEquals("127.0.0.1", dataset.getColumn("addr").get(0)), () -> assertArrayEquals(new byte[] { 1, 2 }, entity.data),
                () -> assertEquals("127.0.0.1", entity.addr));
    }

    // Before the fix: a bean of public-field properties bound as named parameters failed with "Missing required parameter: 'id'",
    // although isBeanParameter takes it for a bean and abacus reads its properties.
    @Test
    public void testBeanParameterWithFieldOnlyPropertiesIsBound() {
        final FieldOnlyBean bean = new FieldOnlyBean();
        bean.id = "7";
        bean.addr = "10.0.0.1";

        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.columns(DataType.text(), "id", "addr");
            final ResultSet resultSet = mock(ResultSet.class);
            when(fixture.session.execute(any(Statement.class))).thenReturn(resultSet);

            fixture.executor.execute("INSERT INTO t (id, addr) VALUES (?, ?)", bean);

            assertArrayEquals(new Object[] { "7", "10.0.0.1" }, fixture.values.get(0));
        }
    }

    /** Public-field properties, one declaring its column name with {@code @Column}. */
    public static class ColumnFieldBean {
        public String addr;
        public Long b;
        @Column("user_name")
        public String name;
    }

    public static class GetterBean {
        private String a;
        private Long b;

        public String getA() {
            return a;
        }

        public void setA(final String a) {
            this.a = a;
        }

        public Long getB() {
            return b;
        }

        public void setB(final Long b) {
            this.b = b;
        }
    }

    /**
     * Before the fix: a bean of public-field properties failed with "Missing required parameter" for {@code :name} markers too,
     * and for a field declaring its column name with {@code @Column}. Mirrors the 4.x test: {@code :name} markers, positional
     * markers named by their column and an {@code @Column} name are bound from the fields; an unknown name still fails; a getter
     * bean is bound through its getters as before.
     */
    @Test
    public void testFieldOnlyBeanBindsNamedMarkersColumnNamesAndKeepsGetterBeans() {
        final ColumnFieldBean bean = new ColumnFieldBean();
        bean.addr = "x";
        bean.b = 7L;
        bean.name = "Ada";

        assertAll(() -> {
            try (CassandraTestFixture fixture = new CassandraTestFixture()) {
                fixture.columns(DataType.text(), "addr", "b");
                when(fixture.session.execute(any(Statement.class))).thenReturn(mock(ResultSet.class));

                fixture.executor.execute("SELECT * FROM t WHERE addr = :addr AND b = :b", bean);
                assertArrayEquals(new Object[] { "x", "7" }, fixture.values.get(0));
            }
        }, () -> {
            try (CassandraTestFixture fixture = new CassandraTestFixture()) {
                fixture.columns(DataType.text(), "user_name");
                when(fixture.session.execute(any(Statement.class))).thenReturn(mock(ResultSet.class));

                fixture.executor.execute("SELECT * FROM t WHERE user_name = :user_name", bean);
                fixture.executor.execute("SELECT * FROM t WHERE user_name = ?", bean);
                assertArrayEquals(new Object[] { "Ada" }, fixture.values.get(0));
                assertArrayEquals(new Object[] { "Ada" }, fixture.values.get(1));
            }
        }, () -> {
            try (CassandraTestFixture fixture = new CassandraTestFixture()) {
                fixture.columns(DataType.text(), "addr", "c");

                final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                        () -> fixture.executor.execute("SELECT * FROM t WHERE addr = :addr AND c = :c", bean));
                assertEquals("Missing required parameter: 'c'", e.getMessage());
            }
        }, () -> {
            final GetterBean getterBean = new GetterBean();
            getterBean.setA("y");
            getterBean.setB(8L);

            try (CassandraTestFixture fixture = new CassandraTestFixture()) {
                fixture.columns(DataType.text(), "a", "b");
                when(fixture.session.execute(any(Statement.class))).thenReturn(mock(ResultSet.class));

                fixture.executor.execute("SELECT * FROM t WHERE a = :a AND b = :b", getterBean);
                assertArrayEquals(new Object[] { "y", "8" }, fixture.values.get(0));
            }
        });
    }

    /**
     * Completes the coverage of the field-only extractData fix (before it, the column kept the raw driver value): a field declaring
     * its column name with {@code @Column} is typed by that field, like toEntity, and a column matching no property stays raw.
     */
    @Test
    public void testExtractDataTypesColumnNamedFieldAndKeepsUnmatchedColumnRaw() throws Exception {
        final java.net.InetAddress address = java.net.InetAddress.getByName("127.0.0.1");
        final Row row = row(new String[] { "user_name", "unknown" }, address, address);

        final com.landawn.abacus.util.Dataset dataset = CassandraExecutor.extractData(result(row), ColumnFieldBean.class);

        assertAll(() -> assertEquals("127.0.0.1", dataset.getColumn("user_name").get(0)),
                () -> assertEquals(CassandraExecutor.toEntity(row, ColumnFieldBean.class).name, dataset.getColumn("user_name").get(0)),
                () -> assertSame(address, dataset.getColumn("unknown").get(0)));
    }

    /** A getter-backed property whose getter changes the value, also reachable through an {@code @JsonXmlField} alias. */
    public static class AliasBean {
        @JsonXmlField(aliases = "nm")
        private String name;

        public String getName() {
            return name == null ? null : name.toUpperCase();
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    /**
     * Before the fix: the field-only fallback of the named binding took any BeanInfo property, so the {@code @JsonXmlField} alias
     * {@code nm} of the getter-backed {@code name} bound the raw field ("ada") and bypassed the getter ("ADA"); HEAD failed with
     * "Missing required parameter: 'nm'", which is the behavior again. The property's own name binds through the getter.
     */
    @Test
    public void testGetterBackedPropertyAliasIsNotBoundThroughItsField() {
        final AliasBean bean = new AliasBean();
        bean.setName("ada");

        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.columns(DataType.text(), "name");
            when(fixture.session.execute(any(Statement.class))).thenReturn(mock(ResultSet.class));

            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> fixture.executor.execute("SELECT * FROM t WHERE name = :nm", bean));
            assertEquals("Missing required parameter: 'nm'", e.getMessage());

            fixture.executor.execute("SELECT * FROM t WHERE name = :name", bean);
            assertArrayEquals(new Object[] { "ADA" }, fixture.values.get(0));
        }
    }

    /**
     * Getter-backed properties named after fields that differ from the getters' derived names only by case ({@code URL} with
     * {@code getURL()}, {@code ID} with {@code getID()}), with an {@code @JsonXmlField} name or alias; the getters change the value.
     */
    public static class AcronymBean {
        @JsonXmlField(name = "theId")
        private String ID;
        @JsonXmlField(aliases = "link")
        private String URL;

        public String getID() {
            return ID == null ? null : ID.toUpperCase();
        }

        public void setID(final String id) {
            ID = id;
        }

        public String getURL() {
            return URL == null ? null : URL.toUpperCase();
        }

        public void setURL(final String url) {
            URL = url;
        }
    }

    /**
     * Before this fix (verifier follow-up): a property counted as getter-backed only when its getter's derived name equalled the
     * property name, so {@code URL}/{@code getURL()} and {@code ID}/{@code getID()} counted as field-only, and their
     * {@code @JsonXmlField} name or alias bound the raw field past the getter. They are missing parameters again (as on HEAD); the
     * property name and a prefixed spelling bind through the getter. Same rule as the 4.x executor.
     */
    @Test
    public void testAcronymGetterBackedPropertyIsNotBoundThroughItsField() {
        final AcronymBean bean = new AcronymBean();
        bean.setID("id1");
        bean.setURL("http://x");

        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.columns(DataType.text(), "v");
            when(fixture.session.execute(any(Statement.class))).thenReturn(mock(ResultSet.class));

            assertAll(
                    () -> assertEquals("Missing required parameter: 'theId'",
                            assertThrows(IllegalArgumentException.class, () -> fixture.executor.execute("SELECT * FROM t WHERE v = :theId", bean))
                                    .getMessage()),
                    () -> assertEquals("Missing required parameter: 'link'",
                            assertThrows(IllegalArgumentException.class, () -> fixture.executor.execute("SELECT * FROM t WHERE v = :link", bean)).getMessage()),
                    () -> {
                        fixture.values.clear();
                        fixture.executor.execute("SELECT * FROM t WHERE v = :URL", bean);
                        fixture.executor.execute("SELECT * FROM t WHERE v = :getURL", bean);
                        fixture.executor.execute("SELECT * FROM t WHERE v = :ID", bean);
                        assertArrayEquals(new Object[] { "HTTP://X" }, fixture.values.get(0));
                        assertArrayEquals(new Object[] { "HTTP://X" }, fixture.values.get(1));
                        assertArrayEquals(new Object[] { "ID1" }, fixture.values.get(2));
                    });
        }
    }

    /** A field-only property {@code ID} beside a getter-backed {@code id} whose getter changes the value. */
    public static class CaseSiblingBean {
        public String ID;
        private String id;

        public String getId() {
            return id == null ? null : id + "-getter";
        }

        public void setId(final String id) {
            this.id = id;
        }
    }

    /**
     * Before this fix (pre-existing): a getter found by ignoring case won outright, so {@code :ID} bound {@code getId()} although the
     * bean's exact property {@code ID} is a field-only one. The exact-case field-only name now wins; {@code :id} (and the
     * lower-cased column {@code id} of a positional marker) still bind the getter.
     */
    @Test
    public void testExactCaseFieldOnlyPropertyWinsOverCaseInsensitiveGetter() {
        final CaseSiblingBean bean = new CaseSiblingBean();
        bean.ID = "field";
        bean.setId("x");

        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.columns(DataType.text(), "a", "b");
            when(fixture.session.execute(any(Statement.class))).thenReturn(mock(ResultSet.class));
            fixture.executor.execute("SELECT * FROM t WHERE a = :ID AND b = :id", bean);
            fixture.columns(DataType.text(), "id");
            fixture.executor.execute("SELECT * FROM t WHERE id = ?", bean);

            assertAll(() -> assertArrayEquals(new Object[] { "field", "x-getter" }, fixture.values.get(0)),
                    () -> assertArrayEquals(new Object[] { "x-getter" }, fixture.values.get(1)));
        }
    }

    /**
     * Getter-backed {@code active}/{@code children} beside field-only {@code isActive}/{@code hasChildren}, whose names the getter
     * lookup also answers through its is/has-prefix rule.
     */
    public static class PrefixShadowBean {
        private String active;
        private Long children;
        public String isActive;
        public String hasChildren;

        public String getActive() {
            return active;
        }

        public void setActive(final String active) {
            this.active = active;
        }

        public Long getChildren() {
            return children;
        }

        public void setChildren(final Long children) {
            this.children = children;
        }
    }

    /**
     * Before the fix: the getter lookup ran first and also accepts is/has-prefixed spellings, so {@code :isActive} (and a positional
     * marker of the column {@code isactive}) bound the getter-backed {@code active}, and {@code haschildren} bound {@code children},
     * instead of the field-only properties of exactly those names. Exact getter names bind as before.
     */
    @Test
    public void testFieldOnlyPropertyShadowedByPrefixedGetterIsBound() {
        final PrefixShadowBean bean = new PrefixShadowBean();
        bean.setActive("getter");
        bean.setChildren(3L);
        bean.isActive = "field";
        bean.hasChildren = "kids";

        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.columns(DataType.text(), "isactive", "active");
            when(fixture.session.execute(any(Statement.class))).thenReturn(mock(ResultSet.class));

            fixture.executor.execute("SELECT * FROM t WHERE isactive = :isActive AND active = :active", bean);
            fixture.executor.execute("SELECT * FROM t WHERE isactive = ? AND active = ?", bean);
            fixture.columns(DataType.text(), "haschildren", "children");
            fixture.executor.execute("SELECT * FROM t WHERE haschildren = ? AND children = ?", bean);

            assertAll(() -> assertArrayEquals(new Object[] { "field", "getter" }, fixture.values.get(0)),
                    () -> assertArrayEquals(new Object[] { "field", "getter" }, fixture.values.get(1)),
                    () -> assertArrayEquals(new Object[] { "kids", "3" }, fixture.values.get(2)));
        }
    }

    /**
     * Before the fix: extractData typed the column {@code haschildren} (and {@code isactive}) by the getter-backed property the
     * getter lookup reaches through its has/is-prefix rule ({@code children}, a Long), where toEntity writes the field-only
     * {@code hasChildren} (a String). Columns naming the getter-backed properties exactly keep their getter's type.
     */
    @Test
    public void testExtractDataTypesFieldOnlyPropertyShadowedByPrefixedGetterLikeToEntity() {
        final Row row = row(new String[] { "haschildren", "isactive", "children", "active" }, 5L, 6L, 7L, 8L);

        final com.landawn.abacus.util.Dataset dataset = CassandraExecutor.extractData(result(row), PrefixShadowBean.class);
        final PrefixShadowBean entity = CassandraExecutor.toEntity(row, PrefixShadowBean.class);

        assertAll(() -> assertEquals("5", dataset.getColumn("haschildren").get(0)),
                () -> assertEquals(entity.hasChildren, dataset.getColumn("haschildren").get(0)), () -> assertEquals("6", dataset.getColumn("isactive").get(0)),
                () -> assertEquals(entity.isActive, dataset.getColumn("isactive").get(0)), () -> assertEquals(7L, dataset.getColumn("children").get(0)),
                () -> assertEquals(entity.getChildren(), dataset.getColumn("children").get(0)), () -> assertEquals("8", dataset.getColumn("active").get(0)),
                () -> assertEquals(entity.getActive(), dataset.getColumn("active").get(0)));
    }

    /**
     * Field-only properties reached through their {@code @Column} names, beside getter-backed properties whose getters the
     * prefix/case-tolerant getter lookup also finds for the field-only property names ({@code getActive()} for {@code isActive},
     * {@code getId()} for {@code ID}); the getter-backed ones are Integers, the field-only ones Strings.
     */
    public static class ColumnShadowBean {
        private Integer active;
        private Integer id;
        @Column("active_flag")
        public String isActive;
        @Column("ident")
        public String ID;

        public Integer getActive() {
            return active;
        }

        public void setActive(final Integer active) {
            this.active = active;
        }

        public Integer getId() {
            return id;
        }

        public void setId(final Integer id) {
            this.id = id;
        }
    }

    /**
     * Before this fix: the field-only guard ran only on the raw column/marker name, so the {@code @Column} route (column name to
     * property name, then the getter lookup) handed the field-only {@code isActive} ({@code active_flag}) to {@code getActive()} and
     * {@code ID} ({@code ident}) to {@code getId()}: extractData typed those columns as Integers, and {@code :active_flag},
     * {@code :ident} and positional markers of those columns bound 42 and 43, while toEntity writes the field-only properties.
     */
    @Test
    public void testColumnNameOfFieldOnlyPropertyShadowedByGetterIsTypedAndBoundByThatProperty() {
        final String[] names = { "active_flag", "ident", "active", "id" };
        final Row row = row(names, 5L, 6L, 7L, 8L);

        final com.landawn.abacus.util.Dataset dataset = CassandraExecutor.extractData(result(row), ColumnShadowBean.class);
        final ColumnShadowBean entity = CassandraExecutor.toEntity(row, ColumnShadowBean.class);

        final ColumnShadowBean bean = new ColumnShadowBean();
        bean.isActive = "field";
        bean.ID = "fid";
        bean.setActive(42);
        bean.setId(43);

        assertAll(() -> assertEquals("5", dataset.getColumn("active_flag").get(0)),
                () -> assertEquals(entity.isActive, dataset.getColumn("active_flag").get(0)), () -> assertEquals("6", dataset.getColumn("ident").get(0)),
                () -> assertEquals(entity.ID, dataset.getColumn("ident").get(0)), () -> assertEquals(7, dataset.getColumn("active").get(0)),
                () -> assertEquals(entity.getActive(), dataset.getColumn("active").get(0)), () -> assertEquals(8, dataset.getColumn("id").get(0)),
                () -> assertEquals(entity.getId(), dataset.getColumn("id").get(0)), () -> {
                    try (CassandraTestFixture fixture = new CassandraTestFixture()) {
                        fixture.columns(DataType.text(), names);
                        when(fixture.session.execute(any(Statement.class))).thenReturn(mock(ResultSet.class));

                        fixture.executor.execute("SELECT * FROM t WHERE active_flag = :active_flag AND ident = :ident AND active = :active AND id = :id", bean);
                        fixture.executor.execute("SELECT * FROM t WHERE active_flag = ? AND ident = ? AND active = ? AND id = ?", bean);

                        assertAll(() -> assertArrayEquals(new Object[] { "field", "fid", "42", "43" }, fixture.values.get(0)),
                                () -> assertArrayEquals(new Object[] { "field", "fid", "42", "43" }, fixture.values.get(1)));
                    }
                });
    }

    /**
     * A field-only {@code isActive} whose {@code @Column} name is {@code active_flag}, beside a getter-backed {@code active} and a
     * field-only property named {@code active_flag} itself.
     */
    public static class ColumnAndNameBean {
        private Integer active;
        @Column("active_flag")
        public String isActive;
        public Long active_flag;

        public Integer getActive() {
            return active;
        }

        public void setActive(final Integer active) {
            this.active = active;
        }
    }

    /**
     * Before this fix (verifier follow-up): the field-only property reached through the {@code @Column} route was preferred to the
     * property named after the column, so the column {@code active_flag} was typed as {@code isActive} (a String) and
     * {@code :active_flag} and a positional marker of that column bound {@code isActive}, while toEntity, which resolves the bean's
     * own property names before {@code @Column} names, writes the Long {@code active_flag}.
     */
    @Test
    public void testPropertyNamedAfterTheColumnWinsOverTheColumnNameOfAFieldOnlyProperty() {
        final String[] names = { "active_flag", "active" };
        final Row row = row(names, 5L, 7L);

        final com.landawn.abacus.util.Dataset dataset = CassandraExecutor.extractData(result(row), ColumnAndNameBean.class);
        final ColumnAndNameBean entity = CassandraExecutor.toEntity(row, ColumnAndNameBean.class);

        final ColumnAndNameBean bean = new ColumnAndNameBean();
        bean.isActive = "field";
        bean.active_flag = 9L;
        bean.setActive(42);

        assertAll(() -> assertEquals(5L, dataset.getColumn("active_flag").get(0)),
                () -> assertEquals(entity.active_flag, dataset.getColumn("active_flag").get(0)), () -> assertNull(entity.isActive),
                () -> assertEquals(7, dataset.getColumn("active").get(0)), () -> {
                    try (CassandraTestFixture fixture = new CassandraTestFixture()) {
                        fixture.columns(DataType.text(), names);
                        when(fixture.session.execute(any(Statement.class))).thenReturn(mock(ResultSet.class));

                        fixture.executor.execute("SELECT * FROM t WHERE active_flag = :active_flag AND active = :active", bean);
                        fixture.executor.execute("SELECT * FROM t WHERE active_flag = ? AND active = ?", bean);

                        assertAll(() -> assertArrayEquals(new Object[] { "9", "42" }, fixture.values.get(0)),
                                () -> assertArrayEquals(new Object[] { "9", "42" }, fixture.values.get(1)));
                    }
                });
    }

    /**
     * A field-only property {@code url} beside the getter-backed property {@code URL}, whose getter {@code getURL()} derives the
     * name {@code url} although its property is {@code URL}.
     */
    public static class AcronymSiblingBean {
        public String url;
        private Long URL;

        public Long getURL() {
            return URL;
        }

        public void setURL(final Long url) {
            URL = url;
        }
    }

    /**
     * Before this fix: the field-only guard compared the name against the getter's derived name, so {@code url} counted as the name
     * of {@code getURL()} (property {@code URL}): extractData typed the column {@code url} as a Long and {@code :url} (and a
     * positional marker of the column {@code url}) bound {@code getURL()}, while toEntity writes the field-only {@code url}. The
     * exact-name field-only property now wins; {@code URL} keeps its getter.
     */
    @Test
    public void testExactNameFieldOnlyPropertyWinsOverAcronymGetterOfTheSameDerivedName() {
        final String[] names = { "url", "URL" };
        final Row row = row(names, 5L, 6L);

        final com.landawn.abacus.util.Dataset dataset = CassandraExecutor.extractData(result(row), AcronymSiblingBean.class);
        final AcronymSiblingBean entity = CassandraExecutor.toEntity(row, AcronymSiblingBean.class);

        final AcronymSiblingBean bean = new AcronymSiblingBean();
        bean.url = "field";
        bean.setURL(42L);

        assertAll(() -> assertEquals("5", dataset.getColumn("url").get(0)), () -> assertEquals(entity.url, dataset.getColumn("url").get(0)),
                () -> assertEquals(6L, dataset.getColumn("URL").get(0)), () -> assertEquals(entity.getURL(), dataset.getColumn("URL").get(0)), () -> {
                    try (CassandraTestFixture fixture = new CassandraTestFixture()) {
                        fixture.columns(DataType.text(), names);
                        when(fixture.session.execute(any(Statement.class))).thenReturn(mock(ResultSet.class));

                        fixture.executor.execute("SELECT * FROM t WHERE url = :url AND \"URL\" = :URL", bean);
                        fixture.executor.execute("SELECT * FROM t WHERE url = ? AND \"URL\" = ?", bean);

                        assertAll(() -> assertArrayEquals(new Object[] { "field", "42" }, fixture.values.get(0)),
                                () -> assertArrayEquals(new Object[] { "field", "42" }, fixture.values.get(1)));
                    }
                });
    }

    /** A getter-backed {@code Long} property also reachable through an {@code @JsonXmlField} alias. */
    public static class LongAliasBean {
        @JsonXmlField(aliases = "nm")
        private Long name;

        public Long getName() {
            return name;
        }

        public void setName(final Long name) {
            this.name = name;
        }
    }

    /** A field-only {@code String ID} beside a getter-backed {@code Long id}. */
    public static class CaseTypeSiblingBean {
        public String ID;
        private Long id;

        public Long getId() {
            return id;
        }

        public void setId(final Long id) {
            this.id = id;
        }
    }

    /**
     * Pins the extractData Javadoc: a column no getter answers is typed, as toEntity resolves it, by the property the BeanInfo
     * resolves it to, a getter-backed property reached through its {@code @JsonXmlField} alias included ({@code nm} reads as a Long,
     * while binding {@code :nm} is a missing parameter, see testGetterBackedPropertyAliasIsNotBoundThroughItsField); and a column
     * naming a field-only property exactly ({@code ID}) is typed by it although the getter lookup answers it by ignoring case.
     */
    @Test
    public void testExtractDataTypesAliasOfGetterBackedPropertyAndExactCaseFieldOnlyColumnLikeToEntity() {
        final Row aliasRow = row(new String[] { "nm" }, 1);
        final com.landawn.abacus.util.Dataset aliased = CassandraExecutor.extractData(result(aliasRow), LongAliasBean.class);

        final Row row = row(new String[] { "ID", "id" }, 5, 6);
        final com.landawn.abacus.util.Dataset siblings = CassandraExecutor.extractData(result(row), CaseTypeSiblingBean.class);
        final CaseTypeSiblingBean entity = CassandraExecutor.toEntity(row, CaseTypeSiblingBean.class);

        assertAll(() -> assertEquals(1L, aliased.getColumn("nm").get(0)),
                () -> assertEquals(1L, CassandraExecutor.toEntity(aliasRow, LongAliasBean.class).getName()),
                () -> assertEquals("5", siblings.getColumn("ID").get(0)), () -> assertEquals(entity.ID, siblings.getColumn("ID").get(0)),
                () -> assertEquals(6L, siblings.getColumn("id").get(0)), () -> assertEquals(entity.getId(), siblings.getColumn("id").get(0)));
    }

    public static class Town {
        private String city;

        public String getCity() {
            return city;
        }

        public void setCity(final String city) {
            this.city = city;
        }
    }

    public static class TownBook {
        private List<Town> towns;

        public List<Town> getTowns() {
            return towns;
        }

        public void setTowns(final List<Town> towns) {
            this.towns = towns;
        }
    }

    /** A TownBook whose constructor fills the collection with two towns. */
    public static class PrefilledTownBook extends TownBook {
        public PrefilledTownBook() {
            final Town rome = new Town();
            rome.setCity("Rome");
            final Town oslo = new Town();
            oslo.setCity("Oslo");
            setTowns(new ArrayList<>(List.of(rome, oslo)));
        }
    }

    /** A TownBook whose constructor sets an empty collection. */
    public static class EmptyTownBook extends TownBook {
        public EmptyTownBook() {
            setTowns(new ArrayList<>());
        }
    }

    /** An immutable bean created through its builder, with a collection of nested beans. */
    public static final class BuiltTownBook {
        private final List<Town> towns;

        BuiltTownBook(final List<Town> towns) {
            this.towns = towns;
        }

        public List<Town> getTowns() {
            return towns;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private List<Town> towns;

            public Builder towns(final List<Town> towns) {
                this.towns = towns;
                return this;
            }

            public BuiltTownBook build() {
                return new BuiltTownBook(towns);
            }
        }
    }

    /**
     * Pins the toEntity Javadoc sentence on a dotted column through a collection property (the same as the DynamoDB executors'):
     * it is left to the dependency's path setter, which writes into the first element of the collection, or into a new bean it
     * adds when the collection is missing or empty, and which fails a builder-based target with ClassCastException.
     */
    @Test
    public void testDottedColumnThroughCollectionWritesTheFirstElementAndFailsABuilderTarget() {
        final Row row = row(new String[] { "towns.city" }, "Paris");

        assertAll(() -> {
            final List<Town> towns = CassandraExecutor.toEntity(row, TownBook.class).getTowns();
            assertEquals(1, towns.size());
            assertEquals("Paris", towns.get(0).getCity());
        }, () -> {
            final List<Town> towns = CassandraExecutor.toEntity(row, PrefilledTownBook.class).getTowns();
            assertEquals(2, towns.size());
            assertEquals("Paris", towns.get(0).getCity());
            assertEquals("Oslo", towns.get(1).getCity());
        }, () -> {
            final List<Town> towns = CassandraExecutor.toEntity(row, EmptyTownBook.class).getTowns();
            assertEquals(1, towns.size());
            assertEquals("Paris", towns.get(0).getCity());
        }, () -> assertThrows(ClassCastException.class, () -> CassandraExecutor.toEntity(row, BuiltTownBook.class)));
    }

    /** A property declared by a generic superclass: its getter returns the erased {@code Object}. */
    public static class GenericBase<K> {
        private K key;

        public K getKey() {
            return key;
        }

        public void setKey(final K key) {
            this.key = key;
        }
    }

    public static class BytesKeyed extends GenericBase<byte[]> {
    }

    public static class TextKeyed extends GenericBase<String> {
    }

    public static class OptionalKeyed extends GenericBase<java.util.Optional<String>> {
    }

    public static class GenericArrayBase<K> {
        private K[] keys;

        public K[] getKeys() {
            return keys;
        }

        public void setKeys(final K[] keys) {
            this.keys = keys;
        }
    }

    public static class TextArrayKeyed extends GenericArrayBase<String> {
    }

    // The same fix for a wrapper type variable (the blob converted for the resolved Optional<String>, as toEntity does) and a generic
    // array (a list<text> column held as the resolved String[], where the erased Object[] gave a Dataset value toList could not
    // store as the String[] the property holds).
    @Test
    public void testExtractDataResolvesGenericWrapperAndArrayProperties() {
        final Row blobRow = row(new String[] { "key" }, java.nio.ByteBuffer.wrap(new byte[] { 3, 4 }));
        final Row listRow = row(new String[] { "keys" }, new ArrayList<>(List.of("a", "b")));
        final String base64 = com.landawn.abacus.util.Strings.base64Encode(new byte[] { 3, 4 });

        assertAll(
                () -> assertEquals(java.util.Optional.of(base64), CassandraExecutor.extractData(result(blobRow), OptionalKeyed.class).getColumn("key").get(0)),
                () -> assertEquals(java.util.Optional.of(base64), CassandraExecutor.toEntity(blobRow, OptionalKeyed.class).getKey()),
                () -> assertArrayEquals(new String[] { "a", "b" },
                        (String[]) CassandraExecutor.extractData(result(listRow), TextArrayKeyed.class).getColumn("keys").get(0)));
    }

    // Before the fix: extractData typed the column by the getter's erased return type (Object), which every value is an instance of,
    // so the Dataset kept the raw driver value (a blob as a ByteBuffer for a byte[] key, an inet as an InetAddress for a String key)
    // where toEntity converts it for the property's resolved class.
    @Test
    public void testExtractDataConvertsColumnsOfGenericSuperclassProperties() throws Exception {
        final Row blobRow = row(new String[] { "key" }, java.nio.ByteBuffer.wrap(new byte[] { 3, 4 }));
        final Row inetRow = row(new String[] { "key" }, java.net.InetAddress.getByName("10.0.0.2"));

        assertAll(
                () -> assertArrayEquals(new byte[] { 3, 4 }, (byte[]) CassandraExecutor.extractData(result(blobRow), BytesKeyed.class).getColumn("key").get(0)),
                () -> assertArrayEquals(new byte[] { 3, 4 }, CassandraExecutor.toEntity(blobRow, BytesKeyed.class).getKey()),
                () -> assertEquals("10.0.0.2", CassandraExecutor.extractData(result(inetRow), TextKeyed.class).getColumn("key").get(0)),
                () -> assertEquals("10.0.0.2", CassandraExecutor.toEntity(inetRow, TextKeyed.class).getKey()));
    }

    @Table("limit_accounts_v3")
    public static class LimitAccount {
        @Id
        private Long accountId;
        private String name;

        public Long getAccountId() {
            return accountId;
        }

        public void setAccountId(final Long accountId) {
            this.accountId = accountId;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    private static Row row(final String[] names, final Object... values) {
        final Row row = mock(Row.class);
        final ColumnDefinitions columns = mock(ColumnDefinitions.class);
        when(row.getColumnDefinitions()).thenReturn(columns);
        when(columns.size()).thenReturn(names.length);

        for (int i = 0; i < names.length; i++) {
            when(columns.getName(i)).thenReturn(names[i]);
            when(columns.getType(i)).thenReturn(values[i] instanceof Number ? DataType.bigint() : DataType.text());
            when(row.getObject(i)).thenReturn(values[i]);
        }

        return row;
    }

    private static ResultSet result(final Row... rows) {
        final ResultSet result = mock(ResultSet.class);
        final ColumnDefinitions metadata = rows[0].getColumnDefinitions();
        when(result.getColumnDefinitions()).thenReturn(metadata);
        when(result.getAvailableWithoutFetching()).thenReturn(rows.length);
        when(result.iterator()).thenAnswer(invocation -> Arrays.asList(rows).iterator());
        when(result.all()).thenReturn(Arrays.asList(rows));
        when(result.one()).thenReturn(rows[0]);
        when(result.isExhausted()).thenReturn(true);
        return result;
    }
}
