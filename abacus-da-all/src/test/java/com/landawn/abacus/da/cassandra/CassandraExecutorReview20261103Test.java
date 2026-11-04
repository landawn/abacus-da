package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.internal.core.type.DefaultUserDefinedType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.landawn.abacus.annotation.Column;
import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.annotation.JsonXmlField;
import com.landawn.abacus.annotation.Table;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.query.condition.Criteria;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.Strings;

/**
 * Offline regression tests for the 2026-11-03 review of the driver 4 {@link CassandraExecutor} (mocked driver rows, result sets and
 * session; no live Cassandra).
 */
class CassandraExecutorReview20261103Test {

    /** A bean whose properties are public fields only (no getters or setters), which abacus and toEntity treat as a bean. */
    public static class FieldBean {
        public byte[] data;
        public String addr;
        public LocalDateTime ts;
        public java.util.Optional<String> text;
        @Column("user_name")
        public String name;
        public Integer b;
    }

    public static class GetterBean {
        private String a;
        private Integer b;

        public String getA() {
            return a;
        }

        public void setA(final String a) {
            this.a = a;
        }

        public Integer getB() {
            return b;
        }

        public void setB(final Integer b) {
            this.b = b;
        }
    }

    @Table("limit_users")
    public static class LimitUser {
        @Id
        private Long id;
        private String name;

        public Long getId() {
            return id;
        }

        public void setId(final Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    // RED on HEAD: a nested Row column read into a sorted row collection was added with a bare c.add, so the TreeSet's
    // ClassCastException (a List is not Comparable) did not name the column. Both row readers (createRowMapper: the TreeSet row
    // itself; readRow: a TreeSet read as the component of a typed array row) now go through addRowValue, like every other column.
    @Test
    void nestedRowColumn_sortedRowCollection_failsNamingTheColumn() {
        final Row inner = row(new String[] { "v" }, "Ada");

        assertAll(() -> {
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> CassandraExecutor.toList(result(row(new String[] { "nested" }, inner)), TreeSet.class));

            assertEquals("Column (nested, [Ada]) is not accepted by the row collection java.util.TreeSet", e.getMessage());
            assertInstanceOf(ClassCastException.class, e.getCause());
        }, () -> {
            final Row middle = row(new String[] { "nested" }, inner);
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> CassandraExecutor.toList(result(row(new String[] { "outer" }, middle)), TreeSet[].class));

            assertEquals("Column (nested, [Ada]) is not accepted by the row collection java.util.TreeSet", e.getMessage());
            assertInstanceOf(ClassCastException.class, e.getCause());
        }, () -> {
            // A row collection that accepts the nested row's List keeps it, as before.
            final List<?> list = CassandraExecutor.toList(result(row(new String[] { "nested", "w" }, inner, "x")), List.class).get(0);

            assertEquals(Arrays.asList(List.of("Ada"), "x"), list);
        });
    }

    // Pins the closure of the 2026-11-02 re-review's open finding in this executor: a NULL column (or UDT field) read into a
    // collection row class that rejects null elements (an ArrayBlockingQueue, which newRowCollection sizes to the row) is an
    // IllegalArgumentException naming the column, from both row readers and from a UDTCodec, where it was a bare NPE.
    @Test
    void nullValue_arrayBlockingQueueRow_failsNamingTheColumn() {
        assertAll(() -> {
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> CassandraExecutor.toList(result(row(new String[] { "a", "b" }, "x", null)), ArrayBlockingQueue.class));

            assertEquals("Column (b, null) is not accepted by the row collection java.util.concurrent.ArrayBlockingQueue", e.getMessage());
            assertInstanceOf(NullPointerException.class, e.getCause());
        }, () -> {
            // readRow: an ArrayBlockingQueue read as the component of a typed array row.
            final Row nested = row(new String[] { "c" }, (Object) null);
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> CassandraExecutor.toList(result(row(new String[] { "outer" }, nested)), ArrayBlockingQueue[].class));

            assertEquals("Column (c, null) is not accepted by the row collection java.util.concurrent.ArrayBlockingQueue", e.getMessage());
        }, () -> {
            final UserDefinedType udt = new DefaultUserDefinedType(CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("holder"), false,
                    List.of(CqlIdentifier.fromInternal("v")), List.of(DataTypes.TEXT), AttachmentPoint.NONE);
            final CassandraExecutor.UDTCodec<ArrayBlockingQueue> codec = CassandraExecutor.UDTCodec.create(udt, ArrayBlockingQueue.class);
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> codec.parse("{v:null}"));

            assertEquals("Field (v, null) is not accepted by the row collection java.util.concurrent.ArrayBlockingQueue", e.getMessage());
            assertEquals(List.of("x"), new ArrayList<>(codec.parse("{v:'x'}")));
        });
    }

    // RED on HEAD: a bean whose properties are public fields has no getters, so binding one as the named-parameter source failed
    // with "Missing required parameter: 'a'" although abacus (and this executor's insert/update and toEntity) read such a bean
    // through its BeanInfo. The field values are now bound, for :name markers, positional markers named by their column, and a
    // field declaring its column name with @Column; an unknown name still fails, and a getter-based bean is bound as before.
    @Test
    void publicFieldBean_asNamedParameters_isBound() {
        final FieldBean bean = new FieldBean();
        bean.addr = "x";
        bean.b = 7;
        bean.name = "Ada";

        assertAll(() -> {
            try (Fixture fixture = new Fixture(new String[] { "addr", "b" }, DataTypes.TEXT, DataTypes.INT)) {
                fixture.executor.execute("SELECT * FROM t WHERE addr = :addr AND b = :b", bean);
                assertArrayEquals(new Object[] { "x", 7 }, fixture.binds.get(0));

                fixture.executor.execute("SELECT * FROM t WHERE addr = ? AND b = ?", bean);
                assertArrayEquals(new Object[] { "x", 7 }, fixture.binds.get(1));
            }
        }, () -> {
            try (Fixture fixture = new Fixture(new String[] { "user_name" }, DataTypes.TEXT)) {
                fixture.executor.execute("SELECT * FROM t WHERE user_name = :user_name", bean);
                assertArrayEquals(new Object[] { "Ada" }, fixture.binds.get(0));
            }
        }, () -> {
            try (Fixture fixture = new Fixture(new String[] { "addr", "c" }, DataTypes.TEXT, DataTypes.INT)) {
                final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                        () -> fixture.executor.execute("SELECT * FROM t WHERE addr = :addr AND c = :c", bean));

                assertEquals("Missing required parameter: 'c'", e.getMessage());
            }
        }, () -> {
            final GetterBean getterBean = new GetterBean();
            getterBean.setA("y");
            getterBean.setB(8);

            try (Fixture fixture = new Fixture(new String[] { "a", "b" }, DataTypes.TEXT, DataTypes.INT)) {
                fixture.executor.execute("SELECT * FROM t WHERE a = :a AND b = :b", getterBean);
                assertArrayEquals(new Object[] { "y", 8 }, fixture.binds.get(0));
            }
        });
    }

    // RED on HEAD: extractData typed a bean's columns by getter only, so every column of a public-field property kept its raw driver
    // value (a blob as a ByteBuffer for a byte[] field, an inet as an InetAddress for a String one, a timestamp as an Instant for a
    // LocalDateTime one) where toEntity converts it. The columns are now typed by the field's property, as toEntity resolves them.
    @Test
    void extractData_publicFieldBean_convertsColumnsLikeToEntity() throws Exception {
        final Instant instant = Instant.parse("2024-01-01T10:15:30Z");
        final ByteBuffer blob = ByteBuffer.wrap(new byte[] { 1, 2, 3 });
        final String[] names = { "data", "addr", "ts", "text", "user_name", "unknown" };
        final Object[] values = { blob, InetAddress.getByName("127.0.0.1"), instant, ByteBuffer.wrap(new byte[] { 4, 5 }), "Ada", blob };

        final Dataset dataset = CassandraExecutor.extractData(result(row(names, values)), FieldBean.class);
        dataset.moveToRow(0);

        final FieldBean entity = CassandraExecutor.toEntity(row(names, values), FieldBean.class);

        assertAll(() -> assertArrayEquals(new byte[] { 1, 2, 3 }, (byte[]) dataset.get("data")),
                () -> assertArrayEquals(entity.data, (byte[]) dataset.get("data")), () -> assertEquals("127.0.0.1", dataset.get("addr")),
                () -> assertEquals(entity.addr, dataset.get("addr")),
                () -> assertEquals(LocalDateTime.ofInstant(instant, ZoneId.systemDefault()), dataset.get("ts")),
                () -> assertEquals(entity.ts, dataset.get("ts")),
                () -> assertEquals(java.util.Optional.of(Strings.base64Encode(new byte[] { 4, 5 })), dataset.get("text")),
                () -> assertEquals(entity.text, dataset.get("text")), () -> assertEquals("Ada", dataset.get("user_name")),
                // A column that matches no property keeps the driver value.
                () -> assertSame(blob, dataset.get("unknown")));
    }

    // RED on HEAD (cross-cut s01, generic-superclass erasure): extractData typed a column by the getter's return type, which for a
    // getter declared by a generic superclass is the erasure of its type variable (Object), so the Dataset kept the raw driver value
    // (an Integer for a Long property, an InetAddress for a String one, the blob buffer itself for an Optional<String> one), or an
    // Object[] for a String[] property of a T[] getter, where toEntity converts it for the property's resolved type.
    @Test
    void extractData_genericSuperclassGetter_convertsForTheResolvedPropertyType() throws Exception {
        final InetAddress inet = InetAddress.getByName("127.0.0.1");
        final ByteBuffer blob = ByteBuffer.wrap(new byte[] { 4, 5 });

        final Dataset longs = CassandraExecutor.extractData(result(row(new String[] { "value" }, 5)), LongValue.class);
        final Dataset texts = CassandraExecutor.extractData(result(row(new String[] { "value" }, inet)), TextValue.class);
        final Dataset optionals = CassandraExecutor.extractData(result(row(new String[] { "value" }, blob)), OptionalTextValue.class);
        // T[] getValues() erases to Object[]: a list<text> column became an Object[] where the String[] property needs a String[].
        final Dataset arrays = CassandraExecutor.extractData(result(row(new String[] { "values" }, new ArrayList<>(List.of("a", "b")))), TextArray.class);

        assertAll(() -> assertEquals(5L, longs.moveToRow(0).<Object> get("value")),
                () -> assertEquals(String[].class, arrays.moveToRow(0).get("values").getClass()),
                () -> assertArrayEquals(new String[] { "a", "b" }, (String[]) arrays.moveToRow(0).get("values")),
                () -> assertArrayEquals(
                        CassandraExecutor.toEntity(row(new String[] { "values" }, new ArrayList<>(List.of("a", "b"))), TextArray.class).getValues(),
                        (String[]) arrays.moveToRow(0).get("values")),
                () -> assertEquals(CassandraExecutor.toEntity(row(new String[] { "value" }, 5), LongValue.class).getValue(), longs.moveToRow(0).get("value")),
                () -> assertEquals("127.0.0.1", texts.moveToRow(0).get("value")),
                () -> assertEquals(java.util.Optional.of(Strings.base64Encode(new byte[] { 4, 5 })), optionals.moveToRow(0).get("value")),
                // A getter of a concrete type is unchanged: an Integer for an Integer property.
                () -> assertEquals(7, CassandraExecutor.extractData(result(row(new String[] { "b" }, 7)), GetterBean.class).moveToRow(0).<Object> get("b")));
    }

    public static class ValueBase<T> {
        private T value;

        public T getValue() {
            return value;
        }

        public void setValue(final T value) {
            this.value = value;
        }
    }

    public static class LongValue extends ValueBase<Long> {
    }

    public static class TextValue extends ValueBase<String> {
    }

    public static class OptionalTextValue extends ValueBase<java.util.Optional<String>> {
    }

    public static class ValueArrayBase<T> {
        private T[] values;

        public T[] getValues() {
            return values;
        }

        public void setValues(final T[] values) {
            this.values = values;
        }
    }

    public static class TextArray extends ValueArrayBase<String> {
    }

    // Pins the gett @throws IllegalStateException: gett appends its own LIMIT 2, so a condition that already carries a LIMIT is
    // rejected before any CQL is prepared (see "Row Limits" in CassandraExecutorBase).
    @Test
    void gett_conditionWithLimit_isRejectedBeforeAnyCql() {
        final Condition limited = Criteria.builder().where(Filters.eq("id", 7L)).limit(5).build();

        try (Fixture fixture = new Fixture(new String[] { "id" }, DataTypes.BIGINT)) {
            assertThrows(IllegalStateException.class, () -> fixture.executor.gett(LimitUser.class, null, limited));
            verify(fixture.session, never()).prepare(anyString());
        }
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
     * Getter-backed properties {@code active} and {@code children} beside field-only properties {@code isActive} and
     * {@code hasChildren}, whose names the getter lookup also answers through its is/has-prefix rule.
     */
    public static class PrefixShadowBean {
        private String active;
        private Integer children;
        public String isActive;
        public String hasChildren;

        public String getActive() {
            return active;
        }

        public void setActive(final String active) {
            this.active = active;
        }

        public Integer getChildren() {
            return children;
        }

        public void setChildren(final Integer children) {
            this.children = children;
        }
    }

    /**
     * RED before this fix: the field-only fallback of the named binding took any BeanInfo property, so the {@code @JsonXmlField}
     * alias {@code nm} of the getter-backed {@code name} bound the raw field ("ada") and bypassed the getter ("ADA"); HEAD failed
     * with "Missing required parameter: 'nm'", which is the behavior again. The property's own name still binds through the getter.
     */
    @Test
    void getterBackedPropertyAlias_isNotBoundThroughItsField() {
        final AliasBean bean = new AliasBean();
        bean.setName("ada");

        try (Fixture fixture = new Fixture(new String[] { "name" }, DataTypes.TEXT)) {
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> fixture.executor.execute("SELECT * FROM t WHERE name = :nm", bean));

            assertEquals("Missing required parameter: 'nm'", e.getMessage());

            fixture.executor.execute("SELECT * FROM t WHERE name = :name", bean);
            assertArrayEquals(new Object[] { "ADA" }, fixture.binds.get(0));
        }
    }

    /**
     * RED on HEAD: the getter lookup runs first and also accepts is/has-prefixed spellings, so the marker {@code :isActive} (and a
     * positional marker of the column {@code isactive}) bound the getter-backed {@code active}, and the column {@code haschildren}
     * bound {@code children}, instead of the field-only properties of exactly those names. Exact getter names bind as before.
     */
    @Test
    void fieldOnlyPropertyShadowedByPrefixedGetter_isBound() {
        final PrefixShadowBean bean = new PrefixShadowBean();
        bean.setActive("getter");
        bean.setChildren(3);
        bean.isActive = "field";
        bean.hasChildren = "kids";

        assertAll(() -> {
            try (Fixture fixture = new Fixture(new String[] { "isactive", "active" }, DataTypes.TEXT, DataTypes.TEXT)) {
                fixture.executor.execute("SELECT * FROM t WHERE isactive = :isActive AND active = :active", bean);
                assertArrayEquals(new Object[] { "field", "getter" }, fixture.binds.get(0));

                fixture.executor.execute("SELECT * FROM t WHERE isactive = ? AND active = ?", bean);
                assertArrayEquals(new Object[] { "field", "getter" }, fixture.binds.get(1));
            }
        }, () -> {
            try (Fixture fixture = new Fixture(new String[] { "haschildren", "children" }, DataTypes.TEXT, DataTypes.INT)) {
                fixture.executor.execute("SELECT * FROM t WHERE haschildren = ? AND children = ?", bean);
                assertArrayEquals(new Object[] { "kids", 3 }, fixture.binds.get(0));
            }
        });
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
     * RED before this fix (verifier follow-up): a property counted as getter-backed only when its getter's derived name equalled the
     * property name, so {@code URL}/{@code getURL()} and {@code ID}/{@code getID()} counted as field-only, and their
     * {@code @JsonXmlField} name or alias bound the raw field past the getter. They are missing parameters again (as on HEAD); the
     * property name and a prefixed spelling bind through the getter.
     */
    @Test
    void acronymGetterBackedProperty_isNotBoundThroughItsField() {
        final AcronymBean bean = new AcronymBean();
        bean.setID("id1");
        bean.setURL("http://x");

        try (Fixture fixture = new Fixture(new String[] { "v" }, DataTypes.TEXT)) {
            assertAll(
                    () -> assertEquals("Missing required parameter: 'theId'",
                            assertThrows(IllegalArgumentException.class, () -> fixture.executor.execute("SELECT * FROM t WHERE v = :theId", bean))
                                    .getMessage()),
                    () -> assertEquals("Missing required parameter: 'link'",
                            assertThrows(IllegalArgumentException.class, () -> fixture.executor.execute("SELECT * FROM t WHERE v = :link", bean)).getMessage()),
                    () -> {
                        fixture.binds.clear();
                        fixture.executor.execute("SELECT * FROM t WHERE v = :URL", bean);
                        fixture.executor.execute("SELECT * FROM t WHERE v = :getURL", bean);
                        fixture.executor.execute("SELECT * FROM t WHERE v = :ID", bean);
                        assertArrayEquals(new Object[] { "HTTP://X" }, fixture.binds.get(0));
                        assertArrayEquals(new Object[] { "HTTP://X" }, fixture.binds.get(1));
                        assertArrayEquals(new Object[] { "ID1" }, fixture.binds.get(2));
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
     * RED before this fix (pre-existing): a getter found by ignoring case won outright, so {@code :ID} bound {@code getId()} although
     * the bean's exact property {@code ID} is a field-only one. The exact-case field-only name now wins; {@code :id} (and the
     * lower-cased column {@code id} of a positional marker) still bind the getter.
     */
    @Test
    void exactCaseFieldOnlyProperty_winsOverCaseInsensitiveGetter() {
        final CaseSiblingBean bean = new CaseSiblingBean();
        bean.ID = "field";
        bean.setId("x");

        try (Fixture fixture = new Fixture(new String[] { "a", "b" }, DataTypes.TEXT, DataTypes.TEXT)) {
            fixture.executor.execute("SELECT * FROM t WHERE a = :ID AND b = :id", bean);
            assertArrayEquals(new Object[] { "field", "x-getter" }, fixture.binds.get(0));
        }

        try (Fixture fixture = new Fixture(new String[] { "id" }, DataTypes.TEXT)) {
            fixture.executor.execute("SELECT * FROM t WHERE id = ?", bean);
            assertArrayEquals(new Object[] { "x-getter" }, fixture.binds.get(0));
        }
    }

    /**
     * RED on HEAD: extractData typed the column {@code haschildren} (and {@code isactive}) by the getter-backed property the getter
     * lookup reaches through its has/is-prefix rule ({@code children}, an Integer), where toEntity writes the field-only
     * {@code hasChildren} (a String). Columns naming the getter-backed properties exactly keep their getter's type.
     */
    @Test
    void extractData_fieldOnlyPropertyShadowedByPrefixedGetter_isTypedLikeToEntity() {
        final String[] names = { "haschildren", "isactive", "children", "active" };
        final Object[] values = { 5L, 6L, 7L, 8L };

        final Dataset dataset = CassandraExecutor.extractData(result(row(names, values)), PrefixShadowBean.class);
        dataset.moveToRow(0);
        final PrefixShadowBean entity = CassandraExecutor.toEntity(row(names, values), PrefixShadowBean.class);

        assertAll(() -> assertEquals("5", dataset.get("haschildren")), () -> assertEquals(entity.hasChildren, dataset.get("haschildren")),
                () -> assertEquals("6", dataset.get("isactive")), () -> assertEquals(entity.isActive, dataset.get("isactive")),
                () -> assertEquals(7, dataset.<Object> get("children")), () -> assertEquals(entity.getChildren(), dataset.get("children")),
                () -> assertEquals("8", dataset.get("active")), () -> assertEquals(entity.getActive(), dataset.get("active")));
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
     * RED before this fix: the field-only guard ran only on the raw column/marker name, so the {@code @Column} route (column name to
     * property name, then the getter lookup) handed the field-only {@code isActive} ({@code active_flag}) to {@code getActive()} and
     * {@code ID} ({@code ident}) to {@code getId()}: extractData typed those columns as Integers, and {@code :active_flag},
     * {@code :ident} and positional markers of those columns bound 42 and 43, while toEntity writes the field-only properties.
     */
    @Test
    void columnNameOfFieldOnlyPropertyShadowedByGetter_isTypedAndBoundByThatProperty() {
        final String[] names = { "active_flag", "ident", "active", "id" };
        final Object[] values = { 5L, 6L, 7L, 8L };

        final Dataset dataset = CassandraExecutor.extractData(result(row(names, values)), ColumnShadowBean.class);
        dataset.moveToRow(0);
        final ColumnShadowBean entity = CassandraExecutor.toEntity(row(names, values), ColumnShadowBean.class);

        final ColumnShadowBean bean = new ColumnShadowBean();
        bean.isActive = "field";
        bean.ID = "fid";
        bean.setActive(42);
        bean.setId(43);

        assertAll(() -> assertEquals("5", dataset.get("active_flag")), () -> assertEquals(entity.isActive, dataset.get("active_flag")),
                () -> assertEquals("6", dataset.get("ident")), () -> assertEquals(entity.ID, dataset.get("ident")),
                () -> assertEquals(7, dataset.<Object> get("active")), () -> assertEquals(entity.getActive(), dataset.get("active")),
                () -> assertEquals(8, dataset.<Object> get("id")), () -> assertEquals(entity.getId(), dataset.get("id")), () -> {
                    try (Fixture fixture = new Fixture(names, DataTypes.TEXT, DataTypes.TEXT, DataTypes.INT, DataTypes.INT)) {
                        fixture.executor.execute("SELECT * FROM t WHERE active_flag = :active_flag AND ident = :ident AND active = :active AND id = :id", bean);
                        fixture.executor.execute("SELECT * FROM t WHERE active_flag = ? AND ident = ? AND active = ? AND id = ?", bean);
                        assertAll(() -> assertArrayEquals(new Object[] { "field", "fid", 42, 43 }, fixture.binds.get(0)),
                                () -> assertArrayEquals(new Object[] { "field", "fid", 42, 43 }, fixture.binds.get(1)));
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
     * RED before this fix (verifier follow-up): the field-only property reached through the {@code @Column} route was preferred to
     * the property named after the column, so the column {@code active_flag} was typed as {@code isActive} (a String) and
     * {@code :active_flag} and a positional marker of that column bound {@code isActive}, while toEntity, which resolves the
     * bean's own property names before {@code @Column} names, writes the Long {@code active_flag}.
     */
    @Test
    void propertyNamedAfterTheColumn_winsOverTheColumnNameOfAFieldOnlyProperty() {
        final String[] names = { "active_flag", "active" };
        final Object[] values = { 5L, 7L };

        final Dataset dataset = CassandraExecutor.extractData(result(row(names, values)), ColumnAndNameBean.class);
        dataset.moveToRow(0);
        final ColumnAndNameBean entity = CassandraExecutor.toEntity(row(names, values), ColumnAndNameBean.class);

        final ColumnAndNameBean bean = new ColumnAndNameBean();
        bean.isActive = "field";
        bean.active_flag = 9L;
        bean.setActive(42);

        assertAll(() -> assertEquals(5L, dataset.<Object> get("active_flag")), () -> assertEquals(entity.active_flag, dataset.get("active_flag")),
                () -> assertNull(entity.isActive), () -> assertEquals(7, dataset.<Object> get("active")), () -> {
                    try (Fixture fixture = new Fixture(names, DataTypes.BIGINT, DataTypes.INT)) {
                        fixture.executor.execute("SELECT * FROM t WHERE active_flag = :active_flag AND active = :active", bean);
                        fixture.executor.execute("SELECT * FROM t WHERE active_flag = ? AND active = ?", bean);
                        assertAll(() -> assertArrayEquals(new Object[] { 9L, 42 }, fixture.binds.get(0)),
                                () -> assertArrayEquals(new Object[] { 9L, 42 }, fixture.binds.get(1)));
                    }
                });
    }

    /**
     * A field-only property {@code url} beside the getter-backed property {@code URL}, whose getter {@code getURL()} derives the
     * name {@code url} although its property is {@code URL}.
     */
    public static class AcronymSiblingBean {
        public String url;
        private Integer URL;

        public Integer getURL() {
            return URL;
        }

        public void setURL(final Integer url) {
            URL = url;
        }
    }

    /**
     * RED before this fix: the field-only guard compared the name against the getter's derived name, so {@code url} counted as the
     * name of {@code getURL()} (property {@code URL}): extractData typed the column {@code url} as an Integer and {@code :url} (and a
     * positional marker of the column {@code url}) bound {@code getURL()}, while toEntity writes the field-only {@code url}. The
     * exact-name field-only property now wins; {@code URL} keeps its getter.
     */
    @Test
    void exactNameFieldOnlyProperty_winsOverAcronymGetterOfTheSameDerivedName() {
        final String[] names = { "url", "URL" };
        final Object[] values = { 5L, 6L };

        final Dataset dataset = CassandraExecutor.extractData(result(row(names, values)), AcronymSiblingBean.class);
        dataset.moveToRow(0);
        final AcronymSiblingBean entity = CassandraExecutor.toEntity(row(names, values), AcronymSiblingBean.class);

        final AcronymSiblingBean bean = new AcronymSiblingBean();
        bean.url = "field";
        bean.setURL(42);

        assertAll(() -> assertEquals("5", dataset.get("url")), () -> assertEquals(entity.url, dataset.get("url")),
                () -> assertEquals(6, dataset.<Object> get("URL")), () -> assertEquals(entity.getURL(), dataset.get("URL")), () -> {
                    try (Fixture fixture = new Fixture(names, DataTypes.TEXT, DataTypes.INT)) {
                        fixture.executor.execute("SELECT * FROM t WHERE url = :url AND \"URL\" = :URL", bean);
                        fixture.executor.execute("SELECT * FROM t WHERE url = ? AND \"URL\" = ?", bean);
                        assertAll(() -> assertArrayEquals(new Object[] { "field", 42 }, fixture.binds.get(0)),
                                () -> assertArrayEquals(new Object[] { "field", 42 }, fixture.binds.get(1)));
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

    /** A field-only {@code String ID} beside a getter-backed {@code Integer id}. */
    public static class CaseTypeSiblingBean {
        public String ID;
        private Integer id;

        public Integer getId() {
            return id;
        }

        public void setId(final Integer id) {
            this.id = id;
        }
    }

    /**
     * Pins the extractData Javadoc: a column no getter answers is typed, as toEntity resolves it, by the property the BeanInfo
     * resolves it to, a getter-backed property reached through its {@code @JsonXmlField} alias included ({@code nm} reads as a Long,
     * while binding {@code :nm} is a missing parameter, see getterBackedPropertyAlias_isNotBoundThroughItsField); and a column
     * naming a field-only property exactly ({@code ID}) is typed by it although the getter lookup answers it by ignoring case.
     */
    @Test
    void extractData_aliasOfGetterBackedProperty_andExactCaseFieldOnlyColumn_areTypedLikeToEntity() {
        final Dataset aliased = CassandraExecutor.extractData(result(row(new String[] { "nm" }, 1)), LongAliasBean.class);
        aliased.moveToRow(0);

        final String[] names = { "ID", "id" };
        final Object[] values = { 5L, 6L };
        final Dataset siblings = CassandraExecutor.extractData(result(row(names, values)), CaseTypeSiblingBean.class);
        siblings.moveToRow(0);
        final CaseTypeSiblingBean entity = CassandraExecutor.toEntity(row(names, values), CaseTypeSiblingBean.class);

        assertAll(() -> assertEquals(1L, aliased.<Object> get("nm")),
                () -> assertEquals(1L, CassandraExecutor.toEntity(row(new String[] { "nm" }, 1), LongAliasBean.class).getName()),
                () -> assertEquals("5", siblings.get("ID")), () -> assertEquals(entity.ID, siblings.get("ID")),
                () -> assertEquals(6, siblings.<Object> get("id")), () -> assertEquals(entity.getId(), siblings.get("id")));
    }

    /**
     * Pins the toList contract for a raw-row target: the list {@code ResultSet.all()} returns is handed back as is, the driver's
     * immutable {@code Collections.emptyList()} for an exhausted result set included, while any other target gets a new mutable list.
     */
    @Test
    void rawRowTarget_returnsTheDriverAllListEvenWhenEmpty() {
        final ResultSet empty = mock(ResultSet.class);
        final ColumnDefinitions metadata = mock(ColumnDefinitions.class);
        when(empty.getColumnDefinitions()).thenReturn(metadata);
        when(empty.all()).thenReturn(Collections.emptyList());
        when(empty.iterator()).thenAnswer(invocation -> Collections.emptyIterator());

        final Row row = row(new String[] { "a" }, "x");
        final List<Row> all = new ArrayList<>(List.of(row));
        final ResultSet nonEmpty = result(row);
        when(nonEmpty.all()).thenReturn(all);

        assertAll(() -> assertSame(Collections.emptyList(), CassandraExecutor.toList(empty, Row.class)),
                () -> assertSame(all, CassandraExecutor.toList(nonEmpty, Row.class)), () -> {
                    final List<java.util.Map> maps = CassandraExecutor.toList(empty, java.util.Map.class);
                    maps.add(java.util.Map.of());
                    assertEquals(1, maps.size());
                });
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
    void dottedColumnThroughCollection_writesTheFirstElement_builderTargetFails() {
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

    static Row row(final String[] names, final Object... values) {
        final Row row = mock(Row.class);
        final ColumnDefinitions columns = mock(ColumnDefinitions.class);
        when(row.getColumnDefinitions()).thenReturn(columns);
        when(columns.size()).thenReturn(names.length);

        for (int i = 0; i < names.length; i++) {
            final ColumnDefinition column = mock(ColumnDefinition.class);
            when(columns.get(i)).thenReturn(column);
            when(column.getName()).thenReturn(CqlIdentifier.fromInternal(names[i]));
            when(column.getType()).thenReturn(DataTypes.TEXT);
            when(row.getObject(i)).thenReturn(values[i]);
        }

        return row;
    }

    static ResultSet result(final Row... rows) {
        final ResultSet result = mock(ResultSet.class);
        final ColumnDefinitions metadata = rows[0].getColumnDefinitions();
        when(result.getColumnDefinitions()).thenReturn(metadata);
        when(result.getAvailableWithoutFetching()).thenReturn(rows.length);
        when(result.iterator()).thenAnswer(invocation -> Arrays.asList(rows).iterator());
        when(result.all()).thenReturn(Arrays.asList(rows));
        return result;
    }

    /** An executor over a mocked session whose every prepared statement has the given variables; records each bind's values. */
    private static final class Fixture implements AutoCloseable {
        final CqlSession session = mock(CqlSession.class);
        final PreparedStatement prepared = mock(PreparedStatement.class);
        final List<Object[]> binds = new ArrayList<>();
        final CassandraExecutor executor;

        Fixture(final String[] variableNames, final DataType... variableTypes) {
            final DriverContext context = mock(DriverContext.class);
            when(session.getContext()).thenReturn(context);
            when(context.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT);
            final ColumnDefinitions variables = mock(ColumnDefinitions.class);
            when(variables.size()).thenReturn(variableNames.length);

            for (int i = 0; i < variableNames.length; i++) {
                final ColumnDefinition variable = mock(ColumnDefinition.class);
                when(variables.get(i)).thenReturn(variable);
                when(variable.getName()).thenReturn(CqlIdentifier.fromInternal(variableNames[i]));
                when(variable.getType()).thenReturn(variableTypes[i]);
            }

            when(prepared.getVariableDefinitions()).thenReturn(variables);
            when(prepared.bind(any(Object[].class))).thenAnswer(invocation -> {
                binds.add(((Object[]) invocation.getRawArguments()[0]).clone());
                return mock(BoundStatement.class);
            });
            when(session.prepare(anyString())).thenReturn(prepared);
            when(session.execute(any(Statement.class))).thenReturn(mock(ResultSet.class));
            executor = new CassandraExecutor(session);
        }

        @Override
        public void close() {
            executor.close();
        }
    }
}
