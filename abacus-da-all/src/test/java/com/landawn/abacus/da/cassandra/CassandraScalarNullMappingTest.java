package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

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

                    assertEquals(expected, fixture.executor.list(target, QUERY));
                    assertEquals(expected, fixture.executor.async().list(target, QUERY).get(5, TimeUnit.SECONDS));
                    try (var stream = fixture.executor.stream(target, QUERY)) {
                        assertEquals(expected, stream.toList());
                    }
                    try (var stream = fixture.executor.stream(target, fixture.bound)) {
                        assertEquals(expected, stream.toList());
                    }
                    try (var stream = fixture.executor.async().stream(target, QUERY).get(5, TimeUnit.SECONDS)) {
                        assertEquals(expected, stream.toList());
                    }
                    try (var stream = fixture.executor.async().stream(target, fixture.bound).get(5, TimeUnit.SECONDS)) {
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

            final Row nullRow = rows(columns(), (Object) null).getFirst();
            assertEquals(defaultValue, fixture.executor.readFirstColumn(nullRow, primitiveType));
            assertNull(fixture.executor.readFirstColumn(nullRow, boxedType));

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
                assertTrue(fixture.executor.list(target, QUERY).isEmpty());
                assertTrue(fixture.executor.async().list(target, QUERY).get(5, TimeUnit.SECONDS).isEmpty());
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
                try (var stream = fixture.executor.stream(target, fixture.bound)) {
                    assertTrue(stream.toList().isEmpty());
                }
                try (var stream = fixture.executor.async().stream(target, QUERY).get(5, TimeUnit.SECONDS)) {
                    assertTrue(stream.toList().isEmpty());
                }
                try (var stream = fixture.executor.async().stream(target, fixture.bound).get(5, TimeUnit.SECONDS)) {
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

    private static void stubQuery(final CassandraTestFixture fixture, final Object... values) {
        when(fixture.session.execute(any(Statement.class))).thenAnswer(invocation -> result(values));
        when(fixture.session.executeAsync(any(Statement.class))).thenAnswer(invocation -> CompletableFuture.completedFuture(asyncResult(values)));
    }

    private static ColumnDefinitions columns() {
        final ColumnDefinitions columns = mock(ColumnDefinitions.class);
        final ColumnDefinition column = mock(ColumnDefinition.class);
        when(columns.size()).thenReturn(1);
        when(columns.get(0)).thenReturn(column);
        when(column.getName()).thenReturn(CqlIdentifier.fromInternal("value"));
        return columns;
    }

    private static List<Row> rows(final ColumnDefinitions columns, final Object... values) {
        return Arrays.stream(values).map(value -> {
            final Row row = mock(Row.class);
            when(row.getColumnDefinitions()).thenReturn(columns);
            when(row.getObject(0)).thenReturn(value);
            return row;
        }).toList();
    }

    private static ResultSet result(final Object... values) {
        final ColumnDefinitions columns = columns();
        final List<Row> rows = rows(columns, values);
        final ListIterator<Row> iterator = rows.listIterator();
        final ResultSet result = mock(ResultSet.class);
        when(result.getColumnDefinitions()).thenReturn(columns);
        when(result.getAvailableWithoutFetching()).thenAnswer(invocation -> rows.size() - iterator.nextIndex());
        when(result.iterator()).thenReturn(iterator);
        when(result.one()).thenAnswer(invocation -> iterator.hasNext() ? iterator.next() : null);
        return result;
    }

    private static AsyncResultSet asyncResult(final Object... values) {
        final ColumnDefinitions columns = columns();
        final List<Row> rows = rows(columns, values);
        final int firstPageSize = Math.min(1, rows.size());
        // Put the first null on its own page so later values exercise the same mapper after a page fetch.
        final AsyncResultSet nextPage = rows.size() <= firstPageSize ? null : page(columns, rows.subList(firstPageSize, rows.size()), null);
        return page(columns, rows.subList(0, firstPageSize), nextPage);
    }

    private static AsyncResultSet page(final ColumnDefinitions columns, final List<Row> rows, final AsyncResultSet nextPage) {
        final ListIterator<Row> iterator = rows.listIterator();
        final AsyncResultSet page = mock(AsyncResultSet.class);
        when(page.getColumnDefinitions()).thenReturn(columns);
        when(page.currentPage()).thenReturn(() -> iterator);
        when(page.remaining()).thenAnswer(invocation -> rows.size() - iterator.nextIndex());
        when(page.hasMorePages()).thenReturn(nextPage != null);
        if (nextPage != null) {
            when(page.fetchNextPage()).thenReturn(CompletableFuture.completedFuture(nextPage));
        }
        return page;
    }

    private static final class CassandraTestFixture implements AutoCloseable {
        final CqlSession session = mock(CqlSession.class);
        final BoundStatement bound = mock(BoundStatement.class);
        final CassandraExecutor executor;

        CassandraTestFixture() {
            final DriverContext context = mock(DriverContext.class);
            when(session.getContext()).thenReturn(context);
            when(context.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT);
            final PreparedStatement prepared = mock(PreparedStatement.class);
            final ColumnDefinitions noVariables = mock(ColumnDefinitions.class);
            when(noVariables.size()).thenReturn(0);
            when(prepared.getVariableDefinitions()).thenReturn(noVariables);
            when(prepared.bind(any(Object[].class))).thenReturn(bound);
            when(session.prepare(anyString())).thenReturn(prepared);
            executor = new CassandraExecutor(session);
        }

        @Override
        public void close() {
            executor.close();
        }
    }

    // ---- 2026-10-19 sliceH ----

    @ParameterizedTest(name = "{0} and {1}")
    @MethodSource("scalarTypes")
    void findFirstDistinguishesPrimitiveDefaultsFromWrapperNulls_sliceH(final Class<?> primitiveType, final Class<?> boxedType, final Object value,
            final Object defaultValue) throws Exception {
        // The synchronous findFirst read its row through readRow, which returned null for a NULL column even for a primitive
        // class token, so Optional.of(null) threw NullPointerException while the asynchronous findFirst, list, stream and
        // queryForSingleValue all returned the primitive default.
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            stubQuery(fixture, (Object) null);

            org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(defaultValue, fixture.executor.findFirst(primitiveType, QUERY).get()),
                    () -> assertEquals(defaultValue, fixture.executor.async().findFirst(primitiveType, QUERY).get(5, TimeUnit.SECONDS).get()),
                    () -> assertThrows(NullPointerException.class, () -> fixture.executor.findFirst(boxedType, QUERY)),
                    () -> assertInstanceOf(NullPointerException.class,
                            assertThrows(ExecutionException.class, () -> fixture.executor.async().findFirst(boxedType, QUERY).get(5, TimeUnit.SECONDS))
                                    .getCause()));

            stubQuery(fixture, value);

            org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(value, fixture.executor.findFirst(primitiveType, QUERY).get()),
                    () -> assertEquals(value, fixture.executor.findFirst(boxedType, QUERY).get()));
        }
    }

    // ---- 2026-10-20 cassfix ----

    @ParameterizedTest(name = "{0} and {1}")
    @MethodSource("scalarTypes")
    void fetchOnlyOneAndAsyncGetReachTheSameNullDefaults_cassfix(final Class<?> primitiveType, final Class<?> boxedType, final Object value,
            final Object defaultValue) throws Exception {
        // fetchOnlyOne (the async get/gett path, which the public API reaches only with a bean target, see
        // beanPropertiesReadNullAsDefaultsOnEveryEntryPoint_cassfix) maps its row through the same readRow as findFirst: a NULL
        // column must give the primitive default for a primitive class token and null for a wrapper.
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(defaultValue, fixture.executor.fetchOnlyOne(primitiveType, result((Object) null))),
                    () -> assertNull(fixture.executor.fetchOnlyOne(boxedType, result((Object) null))),
                    () -> assertNull(fixture.executor.fetchOnlyOne(primitiveType, result())),
                    () -> assertEquals(value, fixture.executor.fetchOnlyOne(primitiveType, result(value))),
                    () -> assertEquals(value, fixture.executor.fetchOnlyOne(boxedType, result(value))),
                    () -> assertThrows(com.landawn.abacus.exception.DuplicateResultException.class,
                            () -> fixture.executor.fetchOnlyOne(boxedType, result(value, value))));
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("optionalLikeTypes")
    void optionalLikeTargetsReadNullAsEmptyOnEveryEntryPoint_cassfix(final Class<?> optionalType, final Object empty) throws Exception {
        // The documented contract: a NULL column read into an Optional/Nullable/OptionalInt-like target is the type's empty value,
        // never null, on the synchronous, asynchronous, list, stream and fetchOnlyOne paths alike.
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            stubQuery(fixture, null, null);

            org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(empty, fixture.executor.findFirst(optionalType, QUERY).get()),
                    () -> assertEquals(empty, fixture.executor.async().findFirst(optionalType, QUERY).get(5, TimeUnit.SECONDS).get()),
                    () -> assertEquals(empty, fixture.executor.queryForSingleValue(optionalType, QUERY).get()),
                    () -> assertEquals(empty, fixture.executor.queryForSingleNonNull(optionalType, QUERY).get()),
                    () -> assertEquals(List.of(empty, empty), fixture.executor.list(optionalType, QUERY)),
                    () -> assertEquals(List.of(empty, empty), fixture.executor.async().list(optionalType, QUERY).get(5, TimeUnit.SECONDS)),
                    () -> assertEquals(List.of(empty, empty), CassandraExecutor.toList(result(null, null), optionalType)),
                    () -> assertEquals(empty, fixture.executor.fetchOnlyOne(optionalType, result((Object) null))), () -> {
                        try (var stream = fixture.executor.stream(optionalType, QUERY)) {
                            assertEquals(List.of(empty, empty), stream.toList());
                        }
                    }, () -> {
                        try (var stream = fixture.executor.async().stream(optionalType, QUERY).get(5, TimeUnit.SECONDS)) {
                            assertEquals(List.of(empty, empty), stream.toList());
                        }
                    });
        }
    }

    static Stream<Arguments> optionalLikeTypes() {
        return Stream.of(Arguments.of(com.landawn.abacus.util.u.Optional.class, com.landawn.abacus.util.u.Optional.empty()),
                Arguments.of(com.landawn.abacus.util.u.Nullable.class, com.landawn.abacus.util.u.Nullable.empty()),
                Arguments.of(com.landawn.abacus.util.u.OptionalInt.class, com.landawn.abacus.util.u.OptionalInt.empty()),
                Arguments.of(com.landawn.abacus.util.u.OptionalLong.class, com.landawn.abacus.util.u.OptionalLong.empty()),
                Arguments.of(com.landawn.abacus.util.u.OptionalBoolean.class, com.landawn.abacus.util.u.OptionalBoolean.empty()),
                Arguments.of(java.util.Optional.class, java.util.Optional.empty()));
    }

    @org.junit.jupiter.api.Test
    void beanPropertiesReadNullAsDefaultsOnEveryEntryPoint_cassfix() throws Exception {
        // A NULL column into an int/long/boolean property is the primitive default and into an Optional/Nullable property the
        // empty value, whether the bean is read by findFirst, list, stream, the async get/gett (fetchOnlyOne) or toEntity.
        final String[] names = { "intValue", "longValue", "booleanValue", "boxedValue", "optionalValue", "nullableValue", "optionalIntValue" };

        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            when(fixture.session.execute(any(Statement.class))).thenAnswer(invocation -> nullRow(names));
            when(fixture.session.executeAsync(any(Statement.class))).thenAnswer(invocation -> CompletableFuture.completedFuture(nullRowPage(names)));

            final List<NullDefaultsBean> read = new java.util.ArrayList<>();
            read.add(fixture.executor.findFirst(NullDefaultsBean.class, QUERY).get());
            read.add(fixture.executor.async().findFirst(NullDefaultsBean.class, QUERY).get(5, TimeUnit.SECONDS).get());
            read.add(fixture.executor.async()
                    .gett(NullDefaultsBean.class, List.of("intValue"), com.landawn.abacus.query.Filters.expr("id = 1"))
                    .get(5, TimeUnit.SECONDS));
            read.add(fixture.executor.async()
                    .get(NullDefaultsBean.class, List.of("intValue"), com.landawn.abacus.query.Filters.expr("id = 1"))
                    .get(5, TimeUnit.SECONDS)
                    .get());
            read.add(fixture.executor.list(NullDefaultsBean.class, QUERY).getFirst());
            read.add(fixture.executor.async().list(NullDefaultsBean.class, QUERY).get(5, TimeUnit.SECONDS).getFirst());
            read.add(fixture.executor.query(NullDefaultsBean.class, QUERY).getRow(0, NullDefaultsBean.class));
            read.add(fixture.executor.fetchOnlyOne(NullDefaultsBean.class, nullRow(names)));
            read.add(CassandraExecutor.toEntity(nullRow(names).one(), NullDefaultsBean.class));
            try (var stream = fixture.executor.stream(NullDefaultsBean.class, QUERY)) {
                read.add(stream.first().get());
            }
            try (var stream = fixture.executor.async().stream(NullDefaultsBean.class, QUERY).get(5, TimeUnit.SECONDS)) {
                read.add(stream.first().get());
            }

            assertEquals(11, read.size());

            for (final NullDefaultsBean bean : read) {
                org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(0, bean.getIntValue()), () -> assertEquals(0L, bean.getLongValue()),
                        () -> assertEquals(false, bean.isBooleanValue()), () -> assertNull(bean.getBoxedValue()),
                        () -> assertEquals(com.landawn.abacus.util.u.Optional.empty(), bean.getOptionalValue()),
                        () -> assertEquals(com.landawn.abacus.util.u.Nullable.empty(), bean.getNullableValue()),
                        () -> assertEquals(com.landawn.abacus.util.u.OptionalInt.empty(), bean.getOptionalIntValue()));
            }
        }
    }

    /** One row whose every named column is NULL (the mock's default). */
    private static ResultSet nullRow(final String... names) {
        final ColumnDefinitions columns = nullColumns(names);
        final List<Row> rows = rows(columns, (Object) null);
        final ListIterator<Row> iterator = rows.listIterator();
        final ResultSet result = mock(ResultSet.class);
        when(result.getColumnDefinitions()).thenReturn(columns);
        when(result.getAvailableWithoutFetching()).thenAnswer(invocation -> rows.size() - iterator.nextIndex());
        when(result.iterator()).thenReturn(iterator);
        when(result.one()).thenAnswer(invocation -> iterator.hasNext() ? iterator.next() : null);
        return result;
    }

    private static AsyncResultSet nullRowPage(final String... names) {
        final ColumnDefinitions columns = nullColumns(names);
        return page(columns, rows(columns, (Object) null), null);
    }

    private static ColumnDefinitions nullColumns(final String... names) {
        final ColumnDefinitions columns = mock(ColumnDefinitions.class);
        when(columns.size()).thenReturn(names.length);
        for (int i = 0; i < names.length; i++) {
            final ColumnDefinition column = mock(ColumnDefinition.class);
            when(columns.get(i)).thenReturn(column);
            when(column.getName()).thenReturn(CqlIdentifier.fromInternal(names[i]));
        }
        return columns;
    }

    /** Every property starts non-default so a skipped (rather than NULL-assigned) column would be caught. */
    public static class NullDefaultsBean {
        private int intValue = 7;
        private long longValue = 7L;
        private boolean booleanValue = true;
        private Integer boxedValue = 7;
        private com.landawn.abacus.util.u.Optional<String> optionalValue = com.landawn.abacus.util.u.Optional.of("x");
        private com.landawn.abacus.util.u.Nullable<String> nullableValue = com.landawn.abacus.util.u.Nullable.of("x");
        private com.landawn.abacus.util.u.OptionalInt optionalIntValue = com.landawn.abacus.util.u.OptionalInt.of(7);

        public int getIntValue() {
            return intValue;
        }

        public void setIntValue(final int intValue) {
            this.intValue = intValue;
        }

        public long getLongValue() {
            return longValue;
        }

        public void setLongValue(final long longValue) {
            this.longValue = longValue;
        }

        public boolean isBooleanValue() {
            return booleanValue;
        }

        public void setBooleanValue(final boolean booleanValue) {
            this.booleanValue = booleanValue;
        }

        public Integer getBoxedValue() {
            return boxedValue;
        }

        public void setBoxedValue(final Integer boxedValue) {
            this.boxedValue = boxedValue;
        }

        public com.landawn.abacus.util.u.Optional<String> getOptionalValue() {
            return optionalValue;
        }

        public void setOptionalValue(final com.landawn.abacus.util.u.Optional<String> optionalValue) {
            this.optionalValue = optionalValue;
        }

        public com.landawn.abacus.util.u.Nullable<String> getNullableValue() {
            return nullableValue;
        }

        public void setNullableValue(final com.landawn.abacus.util.u.Nullable<String> nullableValue) {
            this.nullableValue = nullableValue;
        }

        public com.landawn.abacus.util.u.OptionalInt getOptionalIntValue() {
            return optionalIntValue;
        }

        public void setOptionalIntValue(final com.landawn.abacus.util.u.OptionalInt optionalIntValue) {
            this.optionalIntValue = optionalIntValue;
        }
    }

    // ---- 2026-10-20 rounify ----

    // Read-path half of the strict Boolean Conversion contract documented on CassandraExecutorBase and CassandraExecutor (the bind
    // path is covered by CassandraExecutorDeepReview2026BooleanBindTest): the synchronous single-value, list and bean entry points
    // convert a column value with one rule, so the values a boolean column never yields (a Long, a Double, an int other than
    // 1/0/-1, free text) are rejected everywhere instead of being coerced differently by each path.
    @org.junit.jupiter.api.Test
    void booleanReadsFollowTheStrictConversionRuleOnEverySyncEntryPoint_rounify() throws Exception {
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            for (final Object[] accepted : new Object[][] { { -1, false }, { 0, false }, { 1, true }, { (short) 1, true }, { (byte) 1, true }, { " Y ", true },
                    { " true", true }, { "", false }, { "  ", false }, { Boolean.TRUE, true } }) {
                final Object value = accepted[0];
                final Boolean expected = (Boolean) accepted[1];
                stubQuery(fixture, value);

                org.junit.jupiter.api.Assertions.assertAll(String.valueOf(value),
                        () -> assertEquals(expected, fixture.executor.findFirst(Boolean.class, QUERY).get()),
                        () -> assertEquals(expected, fixture.executor.findFirst(boolean.class, QUERY).get()),
                        () -> assertEquals(expected, fixture.executor.queryForBoolean(QUERY).get()),
                        () -> assertEquals(expected, fixture.executor.queryForSingleValue(Boolean.class, QUERY).get()),
                        () -> assertEquals(List.of(expected), fixture.executor.list(boolean.class, QUERY)),
                        () -> assertEquals(List.of(expected), fixture.executor.list(Boolean.class, QUERY)),
                        () -> assertEquals(List.of(expected), CassandraExecutor.toList(result(value), boolean.class)),
                        () -> assertEquals(List.of(expected), CassandraExecutor.toList(result(value), Boolean.class)),
                        () -> assertEquals(expected, CassandraExecutor.toEntity(result(value).one(), BoxedFlag.class).getValue()),
                        () -> assertEquals(expected, CassandraExecutor.toEntity(result(value).one(), PrimitiveFlag.class).isValue()));
            }

            for (final Object rejected : new Object[] { 2, 1L, 1.0d, "yes" }) {
                stubQuery(fixture, rejected);

                org.junit.jupiter.api.Assertions.assertAll(String.valueOf(rejected),
                        () -> assertThrows(IllegalArgumentException.class, () -> fixture.executor.findFirst(Boolean.class, QUERY)),
                        () -> assertThrows(IllegalArgumentException.class, () -> fixture.executor.findFirst(boolean.class, QUERY)),
                        () -> assertThrows(IllegalArgumentException.class, () -> fixture.executor.queryForBoolean(QUERY)),
                        () -> assertThrows(IllegalArgumentException.class, () -> fixture.executor.queryForSingleValue(Boolean.class, QUERY)),
                        () -> assertThrows(IllegalArgumentException.class, () -> fixture.executor.list(boolean.class, QUERY)),
                        () -> assertThrows(IllegalArgumentException.class, () -> fixture.executor.list(Boolean.class, QUERY)),
                        () -> assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toList(result(rejected), boolean.class)),
                        () -> assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toList(result(rejected), Boolean.class)),
                        () -> assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toEntity(result(rejected).one(), BoxedFlag.class)),
                        () -> assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toEntity(result(rejected).one(), PrimitiveFlag.class)));
            }

            // NULL is null for a Boolean (a null Nullable) and false for a boolean, on every path; queryForBoolean unboxes the present
            // null to false (CassandraExecutorBase.boolean_mapper), it is empty only for no row.
            stubQuery(fixture, (Object) null);

            org.junit.jupiter.api.Assertions.assertAll("NULL", () -> assertEquals(Boolean.FALSE, fixture.executor.findFirst(boolean.class, QUERY).get()),
                    () -> assertEquals(false, fixture.executor.queryForBoolean(QUERY).get()),
                    () -> assertTrue(fixture.executor.queryForSingleValue(Boolean.class, QUERY).isNull()),
                    () -> assertEquals(Arrays.asList((Object) null), fixture.executor.list(Boolean.class, QUERY)),
                    () -> assertEquals(List.of(false), fixture.executor.list(boolean.class, QUERY)),
                    () -> assertEquals(Arrays.asList((Object) null), CassandraExecutor.toList(result((Object) null), Boolean.class)),
                    () -> assertEquals(List.of(false), CassandraExecutor.toList(result((Object) null), boolean.class)),
                    () -> assertNull(CassandraExecutor.toEntity(result((Object) null).one(), BoxedFlag.class).getValue()),
                    () -> assertEquals(false, CassandraExecutor.toEntity(result((Object) null).one(), PrimitiveFlag.class).isValue()));
        }
    }

    public static class BoxedFlag {
        private Boolean value;

        public Boolean getValue() {
            return value;
        }

        public void setValue(final Boolean value) {
            this.value = value;
        }
    }

    public static class PrimitiveFlag {
        private boolean value = true; // so a NULL read is seen to assign false

        public boolean isValue() {
            return value;
        }

        public void setValue(final boolean value) {
            this.value = value;
        }
    }
}
