package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

class CassandraExecutorDeepReview2026MappingTest {

    @Test
    void nestedRowsUseTypedArrayComponentTypeInListAndEntityMapping() {
        final Row childRow = row(new String[] { "name" }, "Ada");
        final Row elementRow = row(new String[] { "element" }, childRow);

        final Child[] listed = CassandraExecutor.toList(result(elementRow), Child[].class).get(0);
        assertEquals(1, listed.length);
        assertEquals("Ada", listed[0].getName());

        final ArrayHolder holder = CassandraExecutor.toEntity(row(new String[] { "children" }, elementRow), ArrayHolder.class);
        assertEquals(1, holder.getChildren().length);
        assertEquals("Ada", holder.getChildren()[0].getName());

        assertSame(childRow, CassandraExecutor.toList(result(elementRow), Row[].class).get(0)[0]);
        final Object[] untyped = CassandraExecutor.toList(result(elementRow), Object[].class).get(0);
        assertArrayEquals(new Object[] { "Ada" }, (Object[]) untyped[0]);
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
            when(row.getObject(i)).thenReturn(values[i]);
        }

        return row;
    }

    private static ResultSet result(final Row... rows) {
        final ResultSet result = mock(ResultSet.class);
        final ColumnDefinitions columns = rows[0].getColumnDefinitions();
        when(result.getColumnDefinitions()).thenReturn(columns);
        when(result.getAvailableWithoutFetching()).thenReturn(rows.length);
        when(result.iterator()).thenAnswer(invocation -> Arrays.asList(rows).iterator());
        when(result.one()).thenReturn(rows[0]);
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

    public static class ArrayHolder {
        private Child[] children;

        public Child[] getChildren() {
            return children;
        }

        public void setChildren(final Child[] children) {
            this.children = children;
        }
    }
}
