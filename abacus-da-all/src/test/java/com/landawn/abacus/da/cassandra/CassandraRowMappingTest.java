package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;

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

    private static Row row(final String[] names, final Object... values) {
        final Row row = mock(Row.class);
        final ColumnDefinitions columns = mock(ColumnDefinitions.class);
        when(row.getColumnDefinitions()).thenReturn(columns);
        when(columns.size()).thenReturn(names.length);
        for (int i = 0; i < names.length; i++) {
            final ColumnDefinition column = mock(ColumnDefinition.class);
            when(columns.get(i)).thenReturn(column);
            when(column.getName()).thenReturn(CqlIdentifier.fromInternal(names[i]));
            when(column.getType()).thenReturn(values[i] instanceof Number ? DataTypes.BIGINT : DataTypes.TEXT);
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

    // ---- 2026-10-19 sliceH ----

    @Test
    void fieldBackedReadOnlyPropertyIsSkippedOnRead_sliceH() {
        // abacus-common 8.2.1 makes an @AccessFieldByMethod field without a setter read-only (setPropValue throws
        // UnsupportedOperationException) although it has a backing field; the field == null test missed it, so reading the
        // generated SELECT's column failed the whole row. It is skipped like a getter-only property.
        final com.datastax.oss.driver.api.core.type.UserDefinedType userType = new com.datastax.oss.driver.internal.core.type.DefaultUserDefinedType(
                CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("accessor_type"), false,
                List.of(CqlIdentifier.fromInternal("id"), CqlIdentifier.fromInternal("total")), List.of(DataTypes.TEXT, DataTypes.TEXT));
        final CassandraExecutor.UDTCodec<SliceHAccessorEntity> codec = CassandraExecutor.UDTCodec.create(userType, SliceHAccessorEntity.class);
        final SliceHAccessorEntity source = new SliceHAccessorEntity();
        source.setId("u1");

        org.junit.jupiter.api.Assertions.assertAll(() -> {
            final SliceHAccessorEntity entity = CassandraExecutor.toEntity(row(new String[] { "id", "total" }, "a", "stored"), SliceHAccessorEntity.class);
            assertEquals("a", entity.getId());
            assertEquals("computed", entity.getTotal());
        }, () -> {
            final List<SliceHAccessorEntity> list = CassandraExecutor.toList(result(row(new String[] { "id", "total" }, "b", "stored")),
                    SliceHAccessorEntity.class);
            assertEquals("b", list.getFirst().getId());
            assertEquals("computed", list.getFirst().getTotal());
        }, () -> {
            final SliceHAccessorEntity decoded = codec.decode(codec.encode(source, com.datastax.oss.driver.api.core.ProtocolVersion.V4),
                    com.datastax.oss.driver.api.core.ProtocolVersion.V4);
            assertEquals("u1", decoded.getId());
            assertEquals("computed", decoded.getTotal());
        },
                // An explicit DESERIALIZE_ONLY direction cannot make the read-only property writable: it is skipped too (as by the
                // HBase, DynamoDB and MongoDB executors) instead of failing the whole row with UnsupportedOperationException.
                () -> {
                    final SliceHAccessorEntity entity = CassandraExecutor.toEntity(row(new String[] { "other", "id" }, "stored", "c"),
                            SliceHAccessorEntity.class);
                    assertEquals("c", entity.getId());
                    assertEquals("other", entity.getOther());
                }, () -> {
                    final List<SliceHAccessorEntity> list = CassandraExecutor.toList(result(row(new String[] { "other", "id" }, "stored", "d")),
                            SliceHAccessorEntity.class);
                    assertEquals("d", list.getFirst().getId());
                    assertEquals("other", list.getFirst().getOther());
                });
    }

    @Test
    void udtCodecReusedAcrossValuesKeepsFieldBindings_sliceH() {
        // The codec resolves its field names and bean properties once; every later value must still map by field position,
        // including a field bound through @Column, and must not see an earlier value's data.
        final com.datastax.oss.driver.api.core.type.UserDefinedType userType = new com.datastax.oss.driver.internal.core.type.DefaultUserDefinedType(
                CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("counted"), false,
                List.of(CqlIdentifier.fromInternal("name"), CqlIdentifier.fromInternal("order_count")), List.of(DataTypes.TEXT, DataTypes.INT));
        final CassandraExecutor.UDTCodec<SliceHCounted> beanCodec = CassandraExecutor.UDTCodec.create(userType, SliceHCounted.class);
        final CassandraExecutor.UDTCodec<Map> mapCodec = CassandraExecutor.UDTCodec.create(userType, Map.class);
        final com.datastax.oss.driver.api.core.ProtocolVersion v4 = com.datastax.oss.driver.api.core.ProtocolVersion.V4;

        final List<org.junit.jupiter.api.function.Executable> checks = new ArrayList<>();

        for (int i = 1; i <= 3; i++) {
            final int n = i;
            final SliceHCounted source = new SliceHCounted();
            source.setName("n" + n);
            source.setOrderCount(n);

            checks.add(() -> {
                final SliceHCounted decoded = beanCodec.decode(beanCodec.encode(source, v4), v4);
                assertEquals("n" + n, decoded.getName());
                assertEquals(n, decoded.getOrderCount());
            });
            checks.add(() -> {
                final Map<String, Object> map = new LinkedHashMap<>();
                map.put("name", "m" + n);
                map.put("order_count", n == 2 ? null : n);
                assertEquals(map, mapCodec.decode(mapCodec.encode(map, v4), v4));
                assertEquals(map, mapCodec.decode(beanCodec.encode(n == 2 ? new SliceHCounted("m2", null) : new SliceHCounted("m" + n, n), v4), v4));
            });
        }

        org.junit.jupiter.api.Assertions.assertAll(checks);
    }

    public static class SliceHCounted {
        private String name;
        @com.landawn.abacus.annotation.Column("order_count")
        private Integer orderCount;

        public SliceHCounted() {
        }

        SliceHCounted(final String name, final Integer orderCount) {
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

    @com.landawn.abacus.annotation.Entity
    public static class SliceHAccessorEntity {
        private String id;
        @com.landawn.abacus.annotation.AccessFieldByMethod
        private String total = "computed";
        @com.landawn.abacus.annotation.AccessFieldByMethod
        @com.landawn.abacus.annotation.JsonXmlField(direction = com.landawn.abacus.annotation.JsonXmlField.Direction.DESERIALIZE_ONLY)
        private String other = "other";

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getTotal() {
            return total;
        }

        public String getOther() {
            return other;
        }
    }

    // ---- 2026-10-19 verify2 ----

    // CodecRegistry.codecFor(DataType) returns the registered UDTCodec for its user type, so formatting a UdtValue read from a
    // row through it passed the raw driver value to format(T), which mapped it as a bean and threw ClassCastException.
    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    void verify2_formatOfDriverUdtValueThroughRegistryLookup() {
        final com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry registry = new com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry(
                "verify2");
        final com.datastax.oss.driver.api.core.type.UserDefinedType userType = new com.datastax.oss.driver.internal.core.type.DefaultUserDefinedType(
                CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("verify2_counted"), false,
                List.of(CqlIdentifier.fromInternal("name"), CqlIdentifier.fromInternal("order_count")), List.of(DataTypes.TEXT, DataTypes.INT));
        userType.attach(new com.datastax.oss.driver.api.core.detach.AttachmentPoint() {
            @Override
            public com.datastax.oss.driver.api.core.ProtocolVersion getProtocolVersion() {
                return com.datastax.oss.driver.api.core.ProtocolVersion.V4;
            }

            @Override
            public com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry getCodecRegistry() {
                return registry;
            }
        });
        final CassandraExecutor.UDTCodec<SliceHCounted> codec = CassandraExecutor.UDTCodec.create(userType, SliceHCounted.class);
        registry.register(codec);

        final com.datastax.oss.driver.api.core.data.UdtValue udtValue = userType.newValue().setString(0, "n1").setInt(1, 7);
        final String literal = udtValue.getFormattedContents();
        final com.datastax.oss.driver.api.core.type.codec.TypeCodec lookedUp = registry.codecFor(userType);

        org.junit.jupiter.api.Assertions.assertAll(() -> org.junit.jupiter.api.Assertions.assertSame(codec, lookedUp),
                () -> assertEquals(literal, lookedUp.format(udtValue)), () -> assertEquals(literal, codec.format(new SliceHCounted("n1", 7))),
                () -> assertEquals("NULL", codec.format(null)));
    }

    // ---- 2026-10-20 cassfix ----

    // format(T) passes a raw UdtValue straight to the driver's UDT codec, but encode(T, ProtocolVersion) (and the deprecated
    // serialize(T, ProtocolVersion) delegating to it) still called serialize(value), which cast the UdtValue to T and threw
    // ClassCastException when the driver handed this codec a UdtValue looked up by CQL type.
    @Test
    @SuppressWarnings({ "rawtypes", "unchecked", "deprecation" })
    void cassfix_encodeOfDriverUdtValueThroughRegistryLookup() {
        final com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry registry = new com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry(
                "cassfix");
        final com.datastax.oss.driver.api.core.type.UserDefinedType userType = new com.datastax.oss.driver.internal.core.type.DefaultUserDefinedType(
                CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("cassfix_counted"), false,
                List.of(CqlIdentifier.fromInternal("name"), CqlIdentifier.fromInternal("order_count")), List.of(DataTypes.TEXT, DataTypes.INT));
        userType.attach(new com.datastax.oss.driver.api.core.detach.AttachmentPoint() {
            @Override
            public com.datastax.oss.driver.api.core.ProtocolVersion getProtocolVersion() {
                return com.datastax.oss.driver.api.core.ProtocolVersion.V4;
            }

            @Override
            public com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry getCodecRegistry() {
                return registry;
            }
        });
        final CassandraExecutor.UDTCodec<SliceHCounted> codec = CassandraExecutor.UDTCodec.create(userType, SliceHCounted.class);
        registry.register(codec);

        final com.datastax.oss.driver.api.core.data.UdtValue udtValue = userType.newValue().setString(0, "n1").setInt(1, 7);
        final com.datastax.oss.driver.api.core.ProtocolVersion version = com.datastax.oss.driver.api.core.ProtocolVersion.V4;
        final java.nio.ByteBuffer expected = com.datastax.oss.driver.api.core.type.codec.TypeCodecs.udtOf(userType).encode(udtValue, version);
        final com.datastax.oss.driver.api.core.type.codec.TypeCodec lookedUp = registry.codecFor(userType);

        org.junit.jupiter.api.Assertions.assertAll(() -> org.junit.jupiter.api.Assertions.assertSame(codec, lookedUp),
                () -> assertEquals(expected, lookedUp.encode(udtValue, version)),
                () -> assertEquals(expected, ((CassandraExecutor.UDTCodec) codec).serialize(udtValue, version)),
                () -> assertEquals(expected, codec.encode(new SliceHCounted("n1", 7), version)),
                () -> assertEquals("n1", codec.decode((java.nio.ByteBuffer) lookedUp.encode(udtValue, version), version).getName()),
                () -> assertNull(codec.encode(null, version)));
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
}
