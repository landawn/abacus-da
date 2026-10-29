package com.landawn.abacus.da.cassandra.v3;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.landawn.abacus.query.Filters;

class CassandraRowMappingTest {

    @Test
    void nestedRowsKeepTheirRequestedMapListAndArrayShapes() {
        final Row nested = row(new String[] { "value" }, "Ada");
        final Row outer = row(new String[] { "id", "nested" }, 7L, nested);
        assertEquals(Map.of("id", 7L, "nested", Map.of("value", "Ada")), CassandraExecutor.toMap(outer));
        final Map<String, Object> ordered = CassandraExecutor.toMap(outer, LinkedHashMap::new);
        assertEquals(List.of("id", "nested"), new ArrayList<>(ordered.keySet()));
        assertEquals(LinkedHashMap.class, ordered.get("nested").getClass());
        assertEquals(List.of(List.of(7L, List.of("Ada"))), CassandraExecutor.toList(result(outer), List.class));
        assertEquals(List.of(Map.of("id", 7L, "nested", Map.of("value", "Ada"))), CassandraExecutor.toList(result(outer), Map.class));
        final Object[] array = CassandraExecutor.toList(result(outer), Object[].class).getFirst();
        assertEquals(7L, array[0]);
        assertArrayEquals(new Object[] { "Ada" }, (Object[]) array[1]);
        assertEquals(List.of(7L, "Ada"), CassandraExecutor.toList(result(row(new String[] { "id", "name" }, 7L, "Ada")), ArrayList.class).getFirst());
    }

    @Test
    void nestedBeansUseDeclaredPropertyTypesAndIgnoreUnknownColumns() {
        final Row row = row(new String[] { "child", "values", "mapping", "array", "number", "unknown" }, row(new String[] { "name" }, "Ada"),
                row(new String[] { "a", "b" }, 1L, 2L), row(new String[] { "name" }, "Grace"), row(new String[] { "a", "b" }, "x", "y"),
                row(new String[] { "value" }, "42"), "ignored");
        final Nested value = CassandraExecutor.toEntity(row, Nested.class);
        assertEquals("Ada", value.getChild().getName());
        assertEquals(List.of(1L, 2L), value.getValues());
        assertEquals(Map.of("name", "Grace"), value.getMapping());
        assertArrayEquals(new Object[] { "x", "y" }, value.getArray());
        assertEquals(42, value.getNumber());
        assertEquals("Ada", CassandraExecutor.toList(result(row), Nested.class).getFirst().getChild().getName());
        assertEquals("Ada", ((Child) CassandraExecutor.extractData(result(row), Nested.class).getColumn("child").get(0)).getName());
    }

    @Test
    void scalarMappingConvertsValuesAndRejectsMultipleColumns() {
        assertEquals(Arrays.asList(null, "7", "8"), CassandraExecutor.toList(
                result(row(new String[] { "value" }, (Object) null), row(new String[] { "value" }, 7L), row(new String[] { "value" }, 8L)), String.class));
        assertEquals(List.of(7L, 8L), CassandraExecutor.toList(result(row(new String[] { "value" }, 7L), row(new String[] { "value" }, 8L)), Long.class));
        assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toList(result(row(new String[] { "a", "b" }, 1L, 2L)), Long.class));
        assertArrayEquals(new String[] { "7", "8" }, CassandraExecutor.toList(result(row(new String[] { "a", "b" }, 7L, 8L)), String[].class).getFirst());
    }

    @Test
    void dataExtractionConvertsNestedRowsAndAllowsAnEmptySchema() {
        final Row outer = row(new String[] { "nested", "empty" }, row(new String[] { "value" }, "Ada"), null);
        final var untyped = CassandraExecutor.extractData(result(outer));
        assertArrayEquals(new Object[] { "Ada" }, (Object[]) untyped.getColumn("nested").get(0));
        assertNull(untyped.getColumn("empty").get(0));
        final var mapped = CassandraExecutor.extractData(result(outer), Map.class);
        assertEquals(Map.of("value", "Ada"), mapped.getColumn("nested").get(0));
        assertEquals(0, CassandraExecutor.extractData(result(row(new String[0]))).columnCount());
    }

    @Test
    void customStreamingMapperReceivesTheResultMetadataAndEachRow() {
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.columns(DataType.bigint());
            final Row first = row(new String[] { "name" }, "Ada");
            final Row second = row(new String[] { "name" }, "Grace");
            final ResultSet rows = result(first, second);
            when(fixture.session.execute(any(Statement.class))).thenReturn(rows);
            final BiFunction<ColumnDefinitions, Row, String> mapper = (metadata, row) -> {
                assertSame(first.getColumnDefinitions(), metadata);
                return (String) row.getObject(0);
            };
            try (var values = fixture.executor.stream(new SimpleStatement("SELECT name FROM items"), mapper)) {
                assertEquals(List.of("Ada", "Grace"), values.toList());
            }
            try (var values = fixture.executor.stream("SELECT name FROM items", mapper)) {
                assertEquals(List.of("Ada", "Grace"), values.toList());
            }
        }
    }

    @Test
    void selectedPropertyQueriesMapConsistentlyThroughSyncAndAsyncFacades() throws Exception {
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.columns(DataType.bigint(), "id");
            final ResultSet rows = result(row(new String[] { "name" }, "Ada"));
            when(fixture.session.execute(any(Statement.class))).thenReturn(rows);
            final ResultSetFuture future = mock(ResultSetFuture.class);
            when(future.get()).thenReturn(rows);
            when(fixture.session.executeAsync(any(Statement.class))).thenReturn(future);
            final var condition = Filters.eq("id", 7L);
            assertEquals("Ada", fixture.executor.gett(Child.class, List.of("name"), condition).getName());
            final var async = fixture.executor.async();
            assertSame(fixture.executor, async.sync());
            assertEquals("Ada", async.gett(Child.class, List.of("name"), condition).get().getName());
            assertEquals(List.of("Ada"), async.query(Child.class, List.of("name"), condition).get().getColumn("name"));
            try (var stream = async.stream(Child.class, List.of("name"), condition).get()) {
                assertEquals(List.of("Ada"), stream.map(Child::getName).toList());
            }
            assertThrows(IllegalArgumentException.class, () -> fixture.executor.mapper(null));
            assertThrows(IllegalArgumentException.class, () -> fixture.executor.mapper(Child.class));
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
        when(result.getAvailableWithoutFetching()).thenReturn(-1);
        when(result.iterator()).thenAnswer(invocation -> Arrays.asList(rows).iterator());
        when(result.all()).thenReturn(Arrays.asList(rows));
        when(result.one()).thenReturn(rows[0]);
        when(result.isExhausted()).thenReturn(rows.length == 1);
        return result;
    }

    public static class Child {
        private String name;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    public static class Nested {
        private Child child;
        private List<Object> values;
        private Map<String, Object> mapping;
        private Object[] array;
        private Integer number;

        public Child getChild() {
            return child;
        }

        public void setChild(final Child child) {
            this.child = child;
        }

        public List<Object> getValues() {
            return values;
        }

        public void setValues(final List<Object> values) {
            this.values = values;
        }

        public Map<String, Object> getMapping() {
            return mapping;
        }

        public void setMapping(final Map<String, Object> mapping) {
            this.mapping = mapping;
        }

        public Object[] getArray() {
            return array;
        }

        public void setArray(final Object[] array) {
            this.array = array;
        }

        public Integer getNumber() {
            return number;
        }

        public void setNumber(final Integer number) {
            this.number = number;
        }
    }

    // ---- 2026-10-19 sliceK ----

    // abacus-common 8.2.1 marks a field-backed property read-only when @AccessFieldByMethod forbids the field write and
    // there is no setter; PropInfo.setPropValue then throws UnsupportedOperationException. Like a getter-only property,
    // its column must be skipped on read instead of failing the whole row.
    @Test
    void sliceK_accessFieldByMethodGetterOnlyPropertyIsSkippedOnRead() throws Exception {
        final Row row = row(new String[] { "name", "computed" }, "Ada", "stored");
        final com.datastax.driver.core.UserType userType = sliceKUserType("name", "computed");
        final com.datastax.driver.core.UDTValue udtValue = userType.newValue().setString(0, "Grace").setString(1, "stored");
        final CassandraExecutor.UDTCodec<AccessorOnly> codec = CassandraExecutor.UDTCodec.create(userType, AccessorOnly.class);

        org.junit.jupiter.api.Assertions.assertAll(() -> {
            final AccessorOnly entity = CassandraExecutor.toEntity(row, AccessorOnly.class);
            assertEquals("Ada", entity.getName());
            assertEquals("computed", entity.getComputed());
        }, () -> assertEquals("Ada", CassandraExecutor.toList(result(row), AccessorOnly.class).getFirst().getName()), () -> {
            final AccessorOnly decoded = codec.deserialize(udtValue);
            assertEquals("Grace", decoded.getName());
            assertEquals("computed", decoded.getComputed());
        });
    }

    private static com.datastax.driver.core.UserType sliceKUserType(final String... fieldNames) throws Exception {
        final var fieldConstructor = com.datastax.driver.core.UserType.Field.class.getDeclaredConstructor(String.class, DataType.class);
        fieldConstructor.setAccessible(true);
        final List<com.datastax.driver.core.UserType.Field> fields = new ArrayList<>();

        for (final String fieldName : fieldNames) {
            fields.add(fieldConstructor.newInstance(fieldName, DataType.text()));
        }

        final var typeConstructor = com.datastax.driver.core.UserType.class.getDeclaredConstructor(String.class, String.class, boolean.class,
                java.util.Collection.class, com.datastax.driver.core.ProtocolVersion.class, com.datastax.driver.core.CodecRegistry.class);
        typeConstructor.setAccessible(true);
        return typeConstructor.newInstance("ks", "accessor_only", false, fields, com.datastax.driver.core.ProtocolVersion.V4,
                new com.datastax.driver.core.CodecRegistry());
    }

    @com.landawn.abacus.annotation.Entity
    @com.landawn.abacus.annotation.AccessFieldByMethod
    public static class AccessorOnly {
        private String name;
        private String computed = "computed";

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public String getComputed() {
            return computed;
        }
    }

    // ---- 2026-10-20 rounify ----

    // An explicit DESERIALIZE_ONLY direction cannot make a read-only property writable (setPropValue still throws), so its
    // column is skipped like any other read-only one, as by the HBase, DynamoDB and MongoDB executors; it used to fail the row.
    @Test
    void rounify_deserializeOnlyReadOnlyPropertyIsSkippedAndSiblingsAreRead() throws Exception {
        final Row row = row(new String[] { "other", "name" }, "stored", "Ada");
        final com.datastax.driver.core.UserType userType = sliceKUserType("name", "other");
        final com.datastax.driver.core.UDTValue udtValue = userType.newValue().setString(0, "Grace").setString(1, "stored");
        final CassandraExecutor.UDTCodec<DeserializeOnlyReadOnly> codec = CassandraExecutor.UDTCodec.create(userType, DeserializeOnlyReadOnly.class);

        org.junit.jupiter.api.Assertions.assertAll(() -> {
            final DeserializeOnlyReadOnly entity = CassandraExecutor.toEntity(row, DeserializeOnlyReadOnly.class);
            assertEquals("Ada", entity.getName());
            assertEquals("init", entity.getOther());
        }, () -> {
            final DeserializeOnlyReadOnly listed = CassandraExecutor.toList(result(row), DeserializeOnlyReadOnly.class).getFirst();
            assertEquals("Ada", listed.getName());
            assertEquals("init", listed.getOther());
        }, () -> {
            final DeserializeOnlyReadOnly decoded = codec.deserialize(udtValue);
            assertEquals("Grace", decoded.getName());
            assertEquals("init", decoded.getOther());
        });
    }

    @com.landawn.abacus.annotation.Entity
    public static class DeserializeOnlyReadOnly {
        private String name;
        @com.landawn.abacus.annotation.AccessFieldByMethod
        @com.landawn.abacus.annotation.JsonXmlField(direction = com.landawn.abacus.annotation.JsonXmlField.Direction.DESERIALIZE_ONLY)
        private String other = "init";

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public String getOther() {
            return other;
        }
    }

    // Twin of cassandra.CassandraRowMappingTest#udtCodecReusedAcrossValuesKeepsFieldBindings_sliceH: the codec resolves the property
    // bound to each UDT field once (the cached fieldPropInfos); every later value must still map by field position, including a
    // field bound through @Column, and must not see an earlier value's data.
    @Test
    @SuppressWarnings("rawtypes")
    void rounify_udtCodecReusedAcrossValuesKeepsFieldBindings() throws Exception {
        final com.datastax.driver.core.UserType userType = rounifyUserType("counted", "name", DataType.text(), "order_count", DataType.cint());
        final CassandraExecutor.UDTCodec<RounifyCounted> beanCodec = CassandraExecutor.UDTCodec.create(userType, RounifyCounted.class);
        final CassandraExecutor.UDTCodec<Map> mapCodec = CassandraExecutor.UDTCodec.create(userType, Map.class);
        final com.datastax.driver.core.ProtocolVersion v4 = com.datastax.driver.core.ProtocolVersion.V4;
        final List<org.junit.jupiter.api.function.Executable> checks = new ArrayList<>();

        for (int i = 1; i <= 3; i++) {
            final int n = i;
            final RounifyCounted source = new RounifyCounted("n" + n, n);

            checks.add(() -> {
                // The @Column-bound property is written to its field's position.
                final com.datastax.driver.core.UDTValue udtValue = beanCodec.serialize(source);
                assertEquals("n" + n, udtValue.getString(0));
                assertEquals(n, udtValue.getInt(1));
                final RounifyCounted decoded = beanCodec.deserialize(beanCodec.serialize(source, v4), v4);
                assertEquals("n" + n, decoded.getName());
                assertEquals(n, decoded.getOrderCount());
            });
            checks.add(() -> {
                final Map<String, Object> map = new LinkedHashMap<>();
                map.put("name", "m" + n);
                map.put("order_count", n == 2 ? null : n);
                assertEquals(map, mapCodec.deserialize(mapCodec.serialize(map, v4), v4));
                assertEquals(map, mapCodec.deserialize(beanCodec.serialize(new RounifyCounted("m" + n, n == 2 ? null : n), v4), v4));
            });
        }

        org.junit.jupiter.api.Assertions.assertAll(checks);
    }

    private static com.datastax.driver.core.UserType rounifyUserType(final String typeName, final Object... fieldNamesAndTypes) throws Exception {
        final var fieldConstructor = com.datastax.driver.core.UserType.Field.class.getDeclaredConstructor(String.class, DataType.class);
        fieldConstructor.setAccessible(true);
        final List<com.datastax.driver.core.UserType.Field> fields = new ArrayList<>();

        for (int i = 0; i < fieldNamesAndTypes.length; i += 2) {
            fields.add(fieldConstructor.newInstance((String) fieldNamesAndTypes[i], (DataType) fieldNamesAndTypes[i + 1]));
        }

        final var typeConstructor = com.datastax.driver.core.UserType.class.getDeclaredConstructor(String.class, String.class, boolean.class,
                java.util.Collection.class, com.datastax.driver.core.ProtocolVersion.class, com.datastax.driver.core.CodecRegistry.class);
        typeConstructor.setAccessible(true);
        return typeConstructor.newInstance("ks", typeName, false, fields, com.datastax.driver.core.ProtocolVersion.V4,
                new com.datastax.driver.core.CodecRegistry());
    }

    public static class RounifyCounted {
        private String name;
        @com.landawn.abacus.annotation.Column("order_count")
        private Integer orderCount;

        public RounifyCounted() {
        }

        RounifyCounted(final String name, final Integer orderCount) {
            this.name = name;
            this.orderCount = orderCount;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public Integer getOrderCount() {
            return orderCount;
        }

        public void setOrderCount(final Integer orderCount) {
            this.orderCount = orderCount;
        }
    }

    // ---- 2026-10-19 verify2 ----

    // The driver's UDTValue.toString() asks the registry for the codec of the value's user type by CQL type only, which is the
    // registered UDTCodec, and passes it the raw UDTValue; format(T) mapped it as a bean and threw ClassCastException.
    @Test
    void verify2_udtValueToStringWithRegisteredUdtCodec() throws Exception {
        final com.datastax.driver.core.CodecRegistry registry = new com.datastax.driver.core.CodecRegistry();
        final var fieldConstructor = com.datastax.driver.core.UserType.Field.class.getDeclaredConstructor(String.class, DataType.class);
        fieldConstructor.setAccessible(true);
        final List<com.datastax.driver.core.UserType.Field> fields = List.of(fieldConstructor.newInstance("street", DataType.text()),
                fieldConstructor.newInstance("city", DataType.text()));
        final var typeConstructor = com.datastax.driver.core.UserType.class.getDeclaredConstructor(String.class, String.class, boolean.class,
                java.util.Collection.class, com.datastax.driver.core.ProtocolVersion.class, com.datastax.driver.core.CodecRegistry.class);
        typeConstructor.setAccessible(true);
        final com.datastax.driver.core.UserType userType = typeConstructor.newInstance("ks", "verify2_addr", false, fields,
                com.datastax.driver.core.ProtocolVersion.V4, registry);
        final CassandraExecutor.UDTCodec<Verify2Addr> codec = CassandraExecutor.UDTCodec.create(userType, Verify2Addr.class);
        registry.register(codec);

        final com.datastax.driver.core.UDTValue udtValue = userType.newValue().setString(0, "Main St").setString(1, "Springfield");
        final String literal = com.datastax.driver.core.TypeCodec.userType(userType).format(udtValue);
        final Verify2Addr bean = new Verify2Addr();
        bean.setStreet("Main St");
        bean.setCity("Springfield");
        @SuppressWarnings({ "rawtypes", "unchecked" })
        final org.junit.jupiter.api.function.ThrowingSupplier<String> rawFormat = () -> ((com.datastax.driver.core.TypeCodec) codec).format(udtValue);

        org.junit.jupiter.api.Assertions.assertAll(() -> assertSame(codec, registry.codecFor(userType)), () -> assertEquals(literal, udtValue.toString()),
                () -> assertEquals(literal, rawFormat.get()), () -> assertEquals(literal, codec.format(bean)), () -> assertEquals("NULL", codec.format(null)));
    }

    public static class Verify2Addr {
        private String street;
        private String city;

        public String getStreet() {
            return street;
        }

        public void setStreet(final String street) {
            this.street = street;
        }

        public String getCity() {
            return city;
        }

        public void setCity(final String city) {
            this.city = city;
        }
    }

    // ---- 2026-10-20 cassfix ----

    // format(T) passes a raw UDTValue straight to the driver's UDT codec, but serialize(T, ProtocolVersion) still called
    // serialize(value), which cast the UDTValue to T and threw ClassCastException when the driver handed this codec a UDTValue
    // looked up by CQL type (v4 twin: cassandra.CassandraRowMappingTest#cassfix_encodeOfDriverUdtValueThroughRegistryLookup).
    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    void cassfix_serializeOfDriverUdtValueThroughRegistryLookup() throws Exception {
        final com.datastax.driver.core.CodecRegistry registry = new com.datastax.driver.core.CodecRegistry();
        final var fieldConstructor = com.datastax.driver.core.UserType.Field.class.getDeclaredConstructor(String.class, DataType.class);
        fieldConstructor.setAccessible(true);
        final List<com.datastax.driver.core.UserType.Field> fields = List.of(fieldConstructor.newInstance("street", DataType.text()),
                fieldConstructor.newInstance("city", DataType.text()));
        final var typeConstructor = com.datastax.driver.core.UserType.class.getDeclaredConstructor(String.class, String.class, boolean.class,
                java.util.Collection.class, com.datastax.driver.core.ProtocolVersion.class, com.datastax.driver.core.CodecRegistry.class);
        typeConstructor.setAccessible(true);
        final com.datastax.driver.core.UserType userType = typeConstructor.newInstance("ks", "cassfix_addr", false, fields,
                com.datastax.driver.core.ProtocolVersion.V4, registry);
        final CassandraExecutor.UDTCodec<Verify2Addr> codec = CassandraExecutor.UDTCodec.create(userType, Verify2Addr.class);
        registry.register(codec);

        final com.datastax.driver.core.ProtocolVersion version = com.datastax.driver.core.ProtocolVersion.V4;
        final com.datastax.driver.core.UDTValue udtValue = userType.newValue().setString(0, "Main St").setString(1, "Springfield");
        final java.nio.ByteBuffer expected = com.datastax.driver.core.TypeCodec.userType(userType).serialize(udtValue, version);
        final Verify2Addr bean = new Verify2Addr();
        bean.setStreet("Main St");
        bean.setCity("Springfield");
        final com.datastax.driver.core.TypeCodec lookedUp = registry.codecFor(userType);

        org.junit.jupiter.api.Assertions.assertAll(() -> assertSame(codec, lookedUp), () -> assertEquals(expected, lookedUp.serialize(udtValue, version)),
                () -> assertEquals(expected, codec.serialize(bean, version)),
                () -> assertEquals("Springfield",
                        codec.deserialize(((com.datastax.driver.core.TypeCodec) codec).serialize(udtValue, version), version).getCity()),
                () -> assertNull(codec.serialize(null, version)));
    }

    // extractData adopts its per-column lists through RowDataset.fromOwnedColumns (no defensive copy): the Dataset must still
    // be a normal mutable Dataset that is independent of the driver rows and shares nothing with another extraction.
    @Test
    void cassfix_extractedDatasetIsMutableAndOwnsItsColumns() {
        final Row first = row(new String[] { "id", "name" }, 1L, "Ada");
        final Row second = row(new String[] { "id", "name" }, 2L, "Grace");
        final com.landawn.abacus.util.Dataset dataset = CassandraExecutor.extractData(result(first, second));
        final com.landawn.abacus.util.Dataset other = CassandraExecutor.extractData(result(first, second));

        when(first.getObject(1)).thenReturn("changed after extraction");

        org.junit.jupiter.api.Assertions.assertAll(() -> org.junit.jupiter.api.Assertions.assertFalse(dataset.isFrozen()),
                () -> assertEquals(List.of("Ada", "Grace"), dataset.getColumn("name")),
                () -> org.junit.jupiter.api.Assertions.assertNotSame(dataset.getColumn("id"), other.getColumn("id")));

        dataset.addRow(new Object[] { 3L, "Linus" });
        dataset.set(0, 1, "Ada L.");
        dataset.removeRow(1);
        dataset.addColumn("upper", "name", (final String name) -> name.toUpperCase());

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(2, dataset.size()), () -> assertEquals(List.of(1L, 3L), dataset.getColumn("id")),
                () -> assertEquals(List.of("Ada L.", "Linus"), dataset.getColumn("name")),
                () -> assertEquals(List.of("ADA L.", "LINUS"), dataset.getColumn("upper")),
                // The second extraction (and its column lists) is untouched.
                () -> assertEquals(2, other.size()), () -> assertEquals(List.of(1L, 2L), other.getColumn("id")),
                () -> assertEquals(List.of("Ada", "Grace"), other.getColumn("name")), () -> assertEquals(List.of("id", "name"), other.columnNames()));
    }

    // ---- 2026-10-24 r2 sliceK ----

    // Beans.isBeanClass accepts an @Entity-annotated Map subclass, and extractData tested it before the Map check: its columns
    // were typed by the Map's getters, so a nested row became an Object[], and one in a column named like a getter ("empty", from
    // isEmpty()) was converted to that getter's boolean (IllegalArgumentException), instead of maps as for any other map target
    // (toList and readRow test the Map first).
    @Test
    void r2sliceK_extractDataTreatsAnEntityAnnotatedMapAsAMapTarget() {
        final Row outer = row(new String[] { "nested", "empty", "name" }, row(new String[] { "value" }, "Ada"), row(new String[] { "value" }, "Grace"), "x");

        org.junit.jupiter.api.Assertions.assertAll(() -> {
            final com.landawn.abacus.util.Dataset dataset = CassandraExecutor.extractData(result(outer), R2EntityMap.class);
            assertEquals(Map.of("value", "Ada"), dataset.getColumn("nested").get(0));
            assertEquals(Map.of("value", "Grace"), dataset.getColumn("empty").get(0));
            assertEquals("x", dataset.getColumn("name").get(0));
        }, () -> assertEquals(Map.of("value", "Ada"),
                CassandraExecutor.extractData(result(row(new String[] { "nested" }, row(new String[] { "value" }, "Ada"))), R2EntityMap.class)
                        .getColumn("nested")
                        .get(0)),
                // The shapes of a plain Map target and of toList for the same class (pinned).
                () -> assertEquals(Map.of("value", "Ada"), CassandraExecutor.extractData(result(outer), LinkedHashMap.class).getColumn("nested").get(0)),
                () -> assertEquals(Map.of("value", "Ada"), CassandraExecutor.toList(result(outer), R2EntityMap.class).get(0).get("nested")));
    }

    @com.landawn.abacus.annotation.Entity
    public static class R2EntityMap extends LinkedHashMap<String, Object> {
        private static final long serialVersionUID = 1L;
    }

    // ---- 2026-10-27 sliceK ----

    // toEntity documents IllegalArgumentException for a class that is not a bean, but an @Entity-annotated Map subclass (which
    // Beans.isBeanClass accepts) was built as an immutable bean of its Map accessors and failed with UnsupportedOperationException
    // ("No all arguments constructor found"). Every Map type is now rejected alike (the 4.x executor's rule); the row readers still
    // fill such a Map row type (pinned).
    @Test
    void sliceK_toEntityRejectsEveryMapTypeWithIllegalArgumentException() {
        final Row row = row(new String[] { "name" }, "Ada");

        org.junit.jupiter.api.Assertions.assertAll(() -> {
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toEntity(row, R2EntityMap.class));
            org.junit.jupiter.api.Assertions.assertTrue(e.getMessage().contains("toMap"), e.getMessage());
        }, () -> assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toEntity(row, LinkedHashMap.class)),
                () -> assertEquals(Map.of("name", "Ada"), CassandraExecutor.toList(result(row), R2EntityMap.class).get(0)),
                () -> assertEquals(R2EntityMap.class, CassandraExecutor.toList(result(row), R2EntityMap.class).get(0).getClass()));
    }

    // N.newCollection / IntFunctions.ofMap return a mutable stand-in for an immutable row class (ArrayList for ImmutableList,
    // HashSet for ImmutableSet, HashMap for ImmutableMap, TreeMap for ImmutableSortedMap), and that stand-in was handed back as the
    // row: toList(rs, ImmutableMap.class) returned a List<ImmutableMap> holding HashMaps (ClassCastException only where a row was
    // read), and a nested row read into an ImmutableMap property type by extractData was a HashMap. Rows now are of the class asked.
    @Test
    void sliceK_immutableRowClassesGetRowsOfThatClass() {
        final Row row = row(new String[] { "id", "name" }, 7L, "Ada");
        final List<org.junit.jupiter.api.function.Executable> checks = new ArrayList<>();

        for (final Class<?> rowClass : new Class<?>[] { com.landawn.abacus.util.ImmutableMap.class, com.landawn.abacus.util.ImmutableSortedMap.class,
                com.landawn.abacus.util.ImmutableList.class, com.landawn.abacus.util.ImmutableSet.class }) {
            checks.add(() -> {
                final Object mapped = CassandraExecutor.toList(result(row), rowClass).get(0);
                org.junit.jupiter.api.Assertions.assertInstanceOf(rowClass, mapped, rowClass.getName());
                assertEquals(Map.class.isAssignableFrom(rowClass) ? Map.of("id", 7L, "name", "Ada")
                        : rowClass == com.landawn.abacus.util.ImmutableSet.class ? java.util.Set.of(7L, "Ada") : List.of(7L, "Ada"), mapped);
            });
        }

        checks.add(() -> {
            final com.landawn.abacus.util.Dataset dataset = CassandraExecutor
                    .extractData(result(row(new String[] { "nested" }, row(new String[] { "value" }, "Ada"))), ImmutableHolder.class);
            org.junit.jupiter.api.Assertions.assertInstanceOf(com.landawn.abacus.util.ImmutableMap.class, dataset.getColumn("nested").get(0));
        });

        // A UDTCodec for an immutable collection or map class decoded to the stand-in (N.newCollection/N.newMap) as well.
        final com.datastax.driver.core.ProtocolVersion v4 = com.datastax.driver.core.ProtocolVersion.V4;
        checks.add(() -> {
            final CassandraExecutor.UDTCodec<com.landawn.abacus.util.ImmutableList> listCodec = CassandraExecutor.UDTCodec.create(textUserType("v"),
                    com.landawn.abacus.util.ImmutableList.class);
            final Object list = listCodec.deserialize(listCodec.serialize(com.landawn.abacus.util.ImmutableList.of("x"), v4), v4);
            org.junit.jupiter.api.Assertions.assertInstanceOf(com.landawn.abacus.util.ImmutableList.class, list);
            assertEquals(List.of("x"), list);
        });
        checks.add(() -> {
            final CassandraExecutor.UDTCodec<com.landawn.abacus.util.ImmutableMap> mapCodec = CassandraExecutor.UDTCodec.create(textUserType("v"),
                    com.landawn.abacus.util.ImmutableMap.class);
            final Object map = mapCodec.deserialize(mapCodec.serialize(com.landawn.abacus.util.ImmutableMap.of("v", "x"), v4), v4);
            org.junit.jupiter.api.Assertions.assertInstanceOf(com.landawn.abacus.util.ImmutableMap.class, map);
            assertEquals(Map.of("v", "x"), map);
        });

        // Pins: mutable row classes keep the instance the factory created.
        checks.add(() -> assertEquals(LinkedHashMap.class, CassandraExecutor.toList(result(row), LinkedHashMap.class).get(0).getClass()));
        checks.add(() -> assertEquals(ArrayList.class, CassandraExecutor.toList(result(row), List.class).get(0).getClass()));

        org.junit.jupiter.api.Assertions.assertAll(checks);
    }

    /** A one-field {@code text} UDT built through the driver's package-private constructors. */
    private static com.datastax.driver.core.UserType textUserType(final String fieldName) throws Exception {
        final var fieldConstructor = com.datastax.driver.core.UserType.Field.class.getDeclaredConstructor(String.class, DataType.class);
        fieldConstructor.setAccessible(true);
        final var typeConstructor = com.datastax.driver.core.UserType.class.getDeclaredConstructor(String.class, String.class, boolean.class,
                java.util.Collection.class, com.datastax.driver.core.ProtocolVersion.class, com.datastax.driver.core.CodecRegistry.class);
        typeConstructor.setAccessible(true);
        return typeConstructor.newInstance("ks", "text_holder", false, List.of(fieldConstructor.newInstance(fieldName, DataType.text())),
                com.datastax.driver.core.ProtocolVersion.V4, new com.datastax.driver.core.CodecRegistry());
    }

    public static class ImmutableHolder {
        private com.landawn.abacus.util.ImmutableMap<String, Object> nested;

        public com.landawn.abacus.util.ImmutableMap<String, Object> getNested() {
            return nested;
        }

        public void setNested(final com.landawn.abacus.util.ImmutableMap<String, Object> nested) {
            this.nested = nested;
        }
    }

    // ---- 2026-10-27 r2K ----

    // The test above reaches readRow's map branch (a nested row of an extractData column) and createRowMapper's collection and map
    // branches (toList), but not readRow's collection branch: a nested row of an extractData column typed ImmutableList was the
    // ArrayList stand-in on HEAD. Pinned: toEntity (whose setter already converted the stand-in) and a List property, which keeps it.
    @Test
    void r2K_aNestedRowReadIntoAnImmutableCollectionTypeIsOfThatType() {
        final Row nested = row(new String[] { "id", "name" }, 7L, "Ada");

        org.junit.jupiter.api.Assertions.assertAll(() -> {
            final com.landawn.abacus.util.Dataset dataset = CassandraExecutor.extractData(result(row(new String[] { "list" }, nested)),
                    ImmutableCollectionHolder.class);
            org.junit.jupiter.api.Assertions.assertInstanceOf(com.landawn.abacus.util.ImmutableList.class, dataset.getColumn("list").get(0));
            assertEquals(List.of(7L, "Ada"), dataset.getColumn("list").get(0));
        }, () -> {
            final ImmutableCollectionHolder holder = CassandraExecutor.toEntity(row(new String[] { "set" }, nested), ImmutableCollectionHolder.class);
            org.junit.jupiter.api.Assertions.assertInstanceOf(com.landawn.abacus.util.ImmutableSet.class, holder.getSet());
            assertEquals(java.util.Set.of(7L, "Ada"), holder.getSet());
        }, () -> assertEquals(ArrayList.class,
                CassandraExecutor.toEntity(row(new String[] { "plain" }, nested), ImmutableCollectionHolder.class).getPlain().getClass()));
    }

    // r2X (second-pass cross-check): a row class the factories cannot create and N.convert cannot copy their stand-in into (EnumMap,
    // whose keys must be enums; AbstractQueue) fails with IllegalArgumentException from toRowClass, through toList (createRowMapper)
    // and findFirst (readRow), where HEAD returned the HashMap / LinkedList stand-in typed as that class. Same as BigQuery/DynamoDB.
    @Test
    void r2X_aRowClassTheStandInCannotBecomeFailsWithIllegalArgumentException() {
        final Row row = row(new String[] { "id", "name" }, 7L, "Ada");
        final ResultSet rows = result(row);

        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toList(rows, java.util.EnumMap.class)),
                () -> assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toList(rows, java.util.AbstractQueue.class)), () -> {
                    try (CassandraTestFixture fixture = new CassandraTestFixture()) {
                        when(fixture.session.execute(any(Statement.class))).thenReturn(rows);
                        assertThrows(IllegalArgumentException.class, () -> fixture.executor.findFirst(java.util.EnumMap.class, "SELECT id, name FROM t"));
                        assertThrows(IllegalArgumentException.class, () -> fixture.executor.findFirst(java.util.AbstractQueue.class, "SELECT id, name FROM t"));
                    }
                },
                // Pin: a mutable row class keeps the factory's own instance.
                () -> assertEquals(java.util.LinkedList.class, CassandraExecutor.toList(rows, java.util.LinkedList.class).get(0).getClass()));
    }

    public static class ImmutableCollectionHolder {
        private com.landawn.abacus.util.ImmutableList<Object> list;
        private com.landawn.abacus.util.ImmutableSet<Object> set;
        private List<Object> plain;

        public com.landawn.abacus.util.ImmutableList<Object> getList() {
            return list;
        }

        public void setList(final com.landawn.abacus.util.ImmutableList<Object> list) {
            this.list = list;
        }

        public com.landawn.abacus.util.ImmutableSet<Object> getSet() {
            return set;
        }

        public void setSet(final com.landawn.abacus.util.ImmutableSet<Object> set) {
            this.set = set;
        }

        public List<Object> getPlain() {
            return plain;
        }

        public void setPlain(final List<Object> plain) {
            this.plain = plain;
        }
    }
}
