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
}
