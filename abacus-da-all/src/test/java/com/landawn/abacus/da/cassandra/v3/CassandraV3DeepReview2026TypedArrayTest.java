package com.landawn.abacus.da.cassandra.v3;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.GettableData;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

class CassandraV3DeepReview2026TypedArrayTest {

    @Test
    void toListMapsNestedRowsToTypedArrayComponentsAndPreservesUntypedShape() {
        final Row outer = row(new String[] { "nested" }, row(new String[] { "first", "second" }, 7L, 8L));

        final List<String[][]> typed = CassandraExecutor.toList(result(outer), String[][].class);
        assertEquals(1, typed.size());
        assertEquals(1, typed.getFirst().length);
        assertArrayEquals(new String[] { "7", "8" }, typed.getFirst()[0]);

        final List<Object[]> untyped = CassandraExecutor.toList(result(outer), Object[].class);
        assertEquals(1, untyped.size());
        assertArrayEquals(new Object[] { 7L, 8L }, (Object[]) untyped.getFirst()[0]);
    }

    @Test
    void toEntityMapsNestedRowToTypedArrayProperty() {
        final Row outer = row(new String[] { "nested" }, row(new String[] { "first", "second" }, 7L, 8L));

        final TypedArrayBean bean = CassandraExecutor.toEntity(outer, TypedArrayBean.class);

        assertArrayEquals(new String[] { "7", "8" }, bean.getNested());
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 2 })
    void toListPreservesNestedRowsAssignableToArrayComponents(final int columnCount) {
        final Row child = columnCount == 1 ? row(new String[] { "name" }, "Ada") : row(new String[] { "first", "second" }, 7L, 8L);
        final Row outer = row(new String[] { "nested" }, child);

        final Row[] rows = CassandraExecutor.toList(result(outer), Row[].class).getFirst();
        final GettableData[] data = CassandraExecutor.toList(result(outer), GettableData[].class).getFirst();

        assertEquals(1, rows.length);
        assertSame(child, rows[0]);
        assertEquals(1, data.length);
        assertSame(child, data[0]);
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 2 })
    void toEntityPreservesNestedRowsAssignableToArrayProperties(final int columnCount) {
        final Row child = columnCount == 1 ? row(new String[] { "name" }, "Ada") : row(new String[] { "first", "second" }, 7L, 8L);
        final Row elements = row(new String[] { "element" }, child);
        final Row outer = row(new String[] { "rows", "data" }, elements, elements);

        final RowArrayBean bean = CassandraExecutor.toEntity(outer, RowArrayBean.class);

        assertEquals(1, bean.getRows().length);
        assertSame(child, bean.getRows()[0]);
        assertEquals(1, bean.getData().length);
        assertSame(child, bean.getData()[0]);
    }

    private static ResultSet result(final Row... rows) {
        final ResultSet result = mock(ResultSet.class);
        final ColumnDefinitions columns = rows[0].getColumnDefinitions();
        when(result.getColumnDefinitions()).thenReturn(columns);
        when(result.iterator()).thenAnswer(invocation -> Arrays.asList(rows).iterator());
        return result;
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

    public static class TypedArrayBean {
        private String[] nested;

        public String[] getNested() {
            return nested;
        }

        public void setNested(final String[] nested) {
            this.nested = nested;
        }
    }

    public static class RowArrayBean {
        private Row[] rows;
        private GettableData[] data;

        public Row[] getRows() {
            return rows;
        }

        public void setRows(final Row[] rows) {
            this.rows = rows;
        }

        public GettableData[] getData() {
            return data;
        }

        public void setData(final GettableData[] data) {
            this.data = data;
        }
    }
}
