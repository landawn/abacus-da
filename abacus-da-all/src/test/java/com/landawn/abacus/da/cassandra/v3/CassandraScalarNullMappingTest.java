package com.landawn.abacus.da.cassandra.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;

class CassandraScalarNullMappingTest {
    private static final String QUERY = "SELECT value FROM nullable_values";

    @ParameterizedTest(name = "{0} and {1}")
    @MethodSource("scalarTypes")
    void listsAndStreamsDistinguishPrimitiveDefaultsFromWrapperNulls(final Class<?> primitiveType, final Class<?> boxedType, final Object value,
            final Object defaultValue) throws Exception {
        // The first null precedes runtime-type caching; later nulls exercise both cached mapper paths.
        final List<Object> values = Arrays.asList(null, value, null, defaultValue, null);
        final Object[] textValues = values.stream().map(element -> element == null ? null : element.toString()).toArray();

        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            for (final Class<?> target : List.of(primitiveType, boxedType)) {
                final List<Object> expected = target.isPrimitive() ? List.of(defaultValue, value, defaultValue, defaultValue, defaultValue) : values;
                assertEquals(target.isPrimitive() ? List.of(defaultValue, defaultValue) : Arrays.asList(null, null),
                        CassandraExecutor.toList(result(null, null), target));

                for (final Object[] input : List.of(values.toArray(), textValues)) {
                    assertEquals(expected, CassandraExecutor.toList(result(input), target));
                    stubQuery(fixture, input);

                    try (var stream = fixture.executor.stream(target, QUERY)) {
                        assertEquals(expected, stream.toList());
                    }
                    try (var stream = fixture.executor.async().stream(target, QUERY).get(5, TimeUnit.SECONDS)) {
                        assertEquals(expected, stream.toList());
                    }
                }
            }
        }
    }

    @ParameterizedTest(name = "{0} and {1}")
    @MethodSource("scalarTypes")
    void singleValueQueriesDistinguishPrimitiveDefaultsFromWrapperNulls(final Class<?> primitiveType, final Class<?> boxedType, final Object value,
            final Object defaultValue) throws Exception {
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            stubQuery(fixture, (Object) null);

            // Primitive class tokens request a default value; wrapper classes preserve the database NULL.
            assertEquals(defaultValue, fixture.executor.queryForSingleValue(primitiveType, QUERY).get());
            assertEquals(defaultValue, fixture.executor.queryForSingleNonNull(primitiveType, QUERY).get());
            assertEquals(defaultValue, fixture.executor.async().queryForSingleValue(primitiveType, QUERY).get(5, TimeUnit.SECONDS).get());
            assertEquals(defaultValue, fixture.executor.async().queryForSingleNonNull(primitiveType, QUERY).get(5, TimeUnit.SECONDS).get());

            final var nullable = fixture.executor.queryForSingleValue(boxedType, QUERY);
            assertTrue(nullable.isPresent());
            assertNull(nullable.get());
            assertThrows(NullPointerException.class, () -> fixture.executor.queryForSingleNonNull(boxedType, QUERY));

            final var asyncNullable = fixture.executor.async().queryForSingleValue(boxedType, QUERY).get(5, TimeUnit.SECONDS);
            assertTrue(asyncNullable.isPresent());
            assertNull(asyncNullable.get());
            final ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> fixture.executor.async().queryForSingleNonNull(boxedType, QUERY).get(5, TimeUnit.SECONDS));
            assertInstanceOf(NullPointerException.class, failure.getCause());
        }
    }

    @ParameterizedTest(name = "{0} and {1}")
    @MethodSource("scalarTypes")
    void conditionQueriesDistinguishPrimitiveDefaultsFromWrapperNulls(final Class<?> primitiveType, final Class<?> boxedType, final Object value,
            final Object defaultValue) throws Exception {
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            stubQuery(fixture, (Object) null);

            // The inherited condition overloads must use the same conversion policy as raw CQL queries.
            assertEquals(defaultValue, fixture.executor.queryForSingleValue(ScalarValue.class, primitiveType, "value", null).get());
            assertEquals(defaultValue, fixture.executor.queryForSingleNonNull(ScalarValue.class, primitiveType, "value", null).get());
            assertEquals(defaultValue,
                    fixture.executor.async().queryForSingleValue(ScalarValue.class, primitiveType, "value", null).get(5, TimeUnit.SECONDS).get());
            assertEquals(defaultValue,
                    fixture.executor.async().queryForSingleNonNull(ScalarValue.class, primitiveType, "value", null).get(5, TimeUnit.SECONDS).get());

            final var nullable = fixture.executor.queryForSingleValue(ScalarValue.class, boxedType, "value", null);
            assertTrue(nullable.isPresent());
            assertNull(nullable.get());
            assertThrows(NullPointerException.class, () -> fixture.executor.queryForSingleNonNull(ScalarValue.class, boxedType, "value", null));

            final var asyncNullable = fixture.executor.async().queryForSingleValue(ScalarValue.class, boxedType, "value", null).get(5, TimeUnit.SECONDS);
            assertTrue(asyncNullable.isPresent());
            assertNull(asyncNullable.get());
            final ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> fixture.executor.async().queryForSingleNonNull(ScalarValue.class, boxedType, "value", null).get(5, TimeUnit.SECONDS));
            assertInstanceOf(NullPointerException.class, failure.getCause());
        }
    }

    @ParameterizedTest(name = "{0} and {1}")
    @MethodSource("scalarTypes")
    void nonNullValuesIncludingPrimitiveDefaultsRemainPresent(final Class<?> primitiveType, final Class<?> boxedType, final Object value,
            final Object defaultValue) throws Exception {
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            for (final Object expected : List.of(value, defaultValue)) {
                stubQuery(fixture, expected);

                for (final Class<?> target : List.of(primitiveType, boxedType)) {
                    assertEquals(expected, fixture.executor.queryForSingleValue(target, QUERY).get());
                    assertEquals(expected, fixture.executor.queryForSingleNonNull(target, QUERY).get());
                    assertEquals(expected, fixture.executor.async().queryForSingleValue(target, QUERY).get(5, TimeUnit.SECONDS).get());
                    assertEquals(expected, fixture.executor.async().queryForSingleNonNull(target, QUERY).get(5, TimeUnit.SECONDS).get());
                    assertEquals(expected, fixture.executor.queryForSingleValue(ScalarValue.class, target, "value", null).get());
                    assertEquals(expected, fixture.executor.queryForSingleNonNull(ScalarValue.class, target, "value", null).get());
                    assertEquals(expected,
                            fixture.executor.async().queryForSingleValue(ScalarValue.class, target, "value", null).get(5, TimeUnit.SECONDS).get());
                    assertEquals(expected,
                            fixture.executor.async().queryForSingleNonNull(ScalarValue.class, target, "value", null).get(5, TimeUnit.SECONDS).get());
                }
            }
        }
    }

    @ParameterizedTest(name = "{0} and {1}")
    @MethodSource("scalarTypes")
    void noRowsRemainDistinctFromNullColumnsAndPrimitiveDefaults(final Class<?> primitiveType, final Class<?> boxedType, final Object value,
            final Object defaultValue) throws Exception {
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            stubQuery(fixture);

            for (final Class<?> target : List.of(primitiveType, boxedType)) {
                assertTrue(CassandraExecutor.toList(result(), target).isEmpty());
                assertTrue(fixture.executor.queryForSingleValue(target, QUERY).isEmpty());
                assertTrue(fixture.executor.queryForSingleNonNull(target, QUERY).isEmpty());
                assertTrue(fixture.executor.async().queryForSingleValue(target, QUERY).get(5, TimeUnit.SECONDS).isEmpty());
                assertTrue(fixture.executor.async().queryForSingleNonNull(target, QUERY).get(5, TimeUnit.SECONDS).isEmpty());
                assertTrue(fixture.executor.queryForSingleValue(ScalarValue.class, target, "value", null).isEmpty());
                assertTrue(fixture.executor.queryForSingleNonNull(ScalarValue.class, target, "value", null).isEmpty());
                assertTrue(fixture.executor.async().queryForSingleValue(ScalarValue.class, target, "value", null).get(5, TimeUnit.SECONDS).isEmpty());
                assertTrue(fixture.executor.async().queryForSingleNonNull(ScalarValue.class, target, "value", null).get(5, TimeUnit.SECONDS).isEmpty());

                try (var stream = fixture.executor.stream(target, QUERY)) {
                    assertTrue(stream.toList().isEmpty());
                }
                try (var stream = fixture.executor.async().stream(target, QUERY).get(5, TimeUnit.SECONDS)) {
                    assertTrue(stream.toList().isEmpty());
                }
            }
        }
    }

    static Stream<Arguments> scalarTypes() {
        return Stream.of(Arguments.of(boolean.class, Boolean.class, true, false), Arguments.of(char.class, Character.class, 'X', '\0'),
                Arguments.of(byte.class, Byte.class, (byte) 7, (byte) 0), Arguments.of(short.class, Short.class, (short) 7, (short) 0),
                Arguments.of(int.class, Integer.class, 7, 0), Arguments.of(long.class, Long.class, 7L, 0L), Arguments.of(float.class, Float.class, 7.5f, 0.0f),
                Arguments.of(double.class, Double.class, 7.5d, 0.0d));
    }

    public record ScalarValue(int value) {
    }

    private static void stubQuery(final CassandraTestFixture fixture, final Object... values) throws Exception {
        final ResultSet rows = result(values);
        when(fixture.session.execute(any(Statement.class))).thenReturn(rows);
        final ResultSetFuture future = mock(ResultSetFuture.class);
        when(future.get()).thenReturn(rows);
        when(future.get(anyLong(), any(TimeUnit.class))).thenReturn(rows);
        when(future.isDone()).thenReturn(true);
        when(fixture.session.executeAsync(any(Statement.class))).thenReturn(future);
    }

    private static ResultSet result(final Object... values) {
        final ColumnDefinitions columns = mock(ColumnDefinitions.class);
        when(columns.size()).thenReturn(1);
        when(columns.getName(0)).thenReturn("value");
        final List<Row> rows = Arrays.stream(values).map(value -> {
            final Row row = mock(Row.class);
            when(row.getColumnDefinitions()).thenReturn(columns);
            when(row.getObject(0)).thenReturn(value);
            return row;
        }).toList();
        final ResultSet result = mock(ResultSet.class);
        when(result.getColumnDefinitions()).thenReturn(columns);
        when(result.getAvailableWithoutFetching()).thenReturn(rows.size());
        when(result.iterator()).thenAnswer(invocation -> rows.iterator());
        when(result.one()).thenReturn(rows.isEmpty() ? null : rows.getFirst());
        return result;
    }
}
