package com.landawn.abacus.da.cassandra.v3;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UserType;
import com.landawn.abacus.util.N;

/**
 * Driver 3 conversions decided in the 2026-10-24 apply round (driver 4 twin:
 * {@code cassandra.CassandraExecutorDeepReview2026BindBlobRowConversionTest}):
 * <ul>
 * <li>{@code N.registerConverter(Row.class, ...)} never fired (abacus looks converters up by class and superclass, not
 * interface), so {@code N.convert(row, Bean.class)} threw {@code ParsingException} for the driver's {@code ArrayBackedRow};</li>
 * <li>a fractional value bound to an integer column was truncated ({@code 1.5} to {@code 1}), and a finite number beyond the
 * float range bound to a {@code float} column became {@code Infinity};</li>
 * <li>base64 text bound to a {@code blob} column bound an EMPTY blob, and a blob read into a {@code String} was {@code ""}.</li>
 * </ul>
 * Driver statements are mocked; rows for the {@code N.convert} tests are real driver rows; conversion is real.
 */
class CassandraV3DeepReview2026BindBlobRowConversionTest {

    // ---- 2026-10-24 apply-cass ----

    private static final String QUERY = "SELECT v FROM t";
    private static final String UPDATE = "UPDATE t SET v = ? WHERE id = 1";
    private static final byte[] BYTES = { 1, 2, 3 };
    private static final String BASE64 = "AQID";

    // WHY (vapply-cass): the row converter is registered by CassandraExecutor's static initializer, and building a driver row does
    // not initialize that class, so the N.convert tests failed when run alone (or first). Initialize it explicitly.
    @BeforeAll
    static void initializeExecutorClass() throws ClassNotFoundException {
        Class.forName(CassandraExecutor.class.getName(), true, CassandraExecutor.class.getClassLoader());
    }

    @Test
    void nConvertMapsADriverRowAsToListDoes() throws Exception {
        final Row row = driverRow(new String[] { "name", "age" }, new DataType[] { DataType.text(), DataType.cint() },
                TypeCodec.varchar().serialize("Ada", ProtocolVersion.V4), TypeCodec.cint().serialize(36, ProtocolVersion.V4));
        final Row oneColumn = driverRow(new String[] { "age" }, new DataType[] { DataType.cint() }, TypeCodec.cint().serialize(36, ProtocolVersion.V4));

        assertAll(() -> assertEquals("com.datastax.driver.core.ArrayBackedRow", row.getClass().getName()), () -> {
            final Person person = N.convert(row, Person.class);
            assertEquals("Ada", person.getName());
            assertEquals(36, person.getAge());
        }, () -> assertEquals(Map.of("name", "Ada", "age", 36), N.convert(row, Map.class)),
                () -> assertArrayEquals(new Object[] { "Ada", 36 }, N.convert(row, Object[].class)),
                () -> assertArrayEquals(new String[] { "Ada", "36" }, N.convert(row, String[].class)),
                () -> assertEquals(List.of("Ada", 36), N.convert(row, List.class)),
                // A one-column row converts to a single value, as findFirst(Long.class, ...) reads it.
                () -> assertEquals(36L, N.convert(oneColumn, Long.class)), () -> assertEquals(36, (int) N.convert(oneColumn, int.class)),
                () -> assertEquals("36", N.convert(oneColumn, String.class)),
                // A single-value type for a multi-column row is rejected as by toList/findFirst.
                () -> assertTrue(
                        assertThrows(IllegalArgumentException.class, () -> N.convert(row, String.class)).getMessage().contains("Column count must be 1")));
    }

    /** Pins: targets the row already is an instance of return the row itself; the executor's own row mapping is unchanged. */
    @Test
    void nConvertToARowSupertypeAndTheExecutorsRowMappingAreUnaffected() throws Exception {
        final Row row = driverRow(new String[] { "name", "age" }, new DataType[] { DataType.text(), DataType.cint() },
                TypeCodec.varchar().serialize("Ada", ProtocolVersion.V4), TypeCodec.cint().serialize(36, ProtocolVersion.V4));

        assertAll(() -> assertSame(row, N.convert(row, Object.class)), () -> assertSame(row, N.convert(row, Row.class)),
                () -> assertSame(row, N.convert(row, row.getClass())), () -> {
                    final Person person = CassandraExecutor.toEntity(row, Person.class);
                    assertEquals("Ada", person.getName());
                    assertEquals(36, person.getAge());
                }, () -> assertEquals(Map.of("name", "Ada", "age", 36), CassandraExecutor.toMap(row)));
    }

    @Test
    void fractionalValuesBoundToIntegerColumnsAreRejected() {
        final DataType[] integerTypes = { DataType.tinyint(), DataType.smallint(), DataType.cint(), DataType.bigint(), DataType.varint(), DataType.counter() };
        final Object[] fractions = { 1.5d, -0.5d, 1.5f, new BigDecimal("1.5"), new BigDecimal("-2.25") };
        final List<Executable> checks = new ArrayList<>();

        for (final DataType type : integerTypes) {
            for (final Object value : fractions) {
                checks.add(() -> {
                    try (CassandraTestFixture fixture = new CassandraTestFixture()) {
                        fixture.columns(type, "v");
                        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> fixture.executor.execute(UPDATE, value),
                                value + " -> " + type);
                        assertTrue(e.getMessage().contains("parameter 'v' (index 0)"), e.getMessage());
                        assertTrue(e.getMessage().contains("value " + value + " "), e.getMessage());
                        assertTrue(e.getMessage().contains("CQL type " + type), e.getMessage());
                        assertEquals(0, fixture.values.size());
                    }
                });
            }
        }

        // Every parameter form; the message names the marker or column and the index.
        checks.add(() -> {
            try (CassandraTestFixture fixture = new CassandraTestFixture()) {
                fixture.columns(DataType.cint(), "v");
                assertThrows(IllegalArgumentException.class, () -> fixture.executor.execute(UPDATE, List.of(1.5d)));
                assertThrows(IllegalArgumentException.class, () -> fixture.executor.execute(UPDATE, (Object) new Object[] { 1.5d }));
                assertThrows(IllegalArgumentException.class, () -> fixture.executor.execute("UPDATE t SET v = :v WHERE id = 1", N.asMap("v", 1.5d)));
                assertThrows(IllegalArgumentException.class, () -> fixture.executor.execute("UPDATE t SET v = :v WHERE id = 1", new DoubleHolder(1.5d)));
                final IllegalArgumentException named = assertThrows(IllegalArgumentException.class,
                        () -> fixture.executor.execute("UPDATE t SET v = #{amount} WHERE id = 1", N.asMap("amount", 1.5d)));
                assertTrue(named.getMessage().contains("parameter 'amount' (index 0)"), named.getMessage());
                assertEquals(0, fixture.values.size());
            }
        });
        checks.add(() -> {
            try (CassandraTestFixture fixture = new CassandraTestFixture()) {
                when(fixture.columns.size()).thenReturn(2);
                when(fixture.columns.getName(0)).thenReturn("a");
                when(fixture.columns.getName(1)).thenReturn("b");
                when(fixture.columns.getType(0)).thenReturn(DataType.text());
                when(fixture.columns.getType(1)).thenReturn(DataType.bigint());
                final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                        () -> fixture.executor.execute("UPDATE t SET a = ?, b = ? WHERE id = 1", "x", 2.5d));
                assertTrue(e.getMessage().contains("parameter 'b' (index 1)"), e.getMessage());
                assertTrue(e.getMessage().contains("it would be bound as 2"), e.getMessage());
            }
        });
        // Driver 3 binds a number to a time column as a Long of nanoseconds: a fractional one was truncated as well.
        checks.add(() -> {
            try (CassandraTestFixture fixture = new CassandraTestFixture()) {
                fixture.columns(DataType.time(), "v");
                final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> fixture.executor.execute(UPDATE, 1.5d));
                assertTrue(e.getMessage().contains("CQL type time"), e.getMessage());
            }
        });
        checks.add(() -> assertEquals(2L, boundValue(DataType.time(), 2.0d)));

        assertAll(checks);
    }

    @Test
    void finiteNumbersBeyondTheFloatOrDoubleRangeAreRejected() {
        final BigInteger huge = BigInteger.TEN.pow(400);
        final List<Executable> checks = new ArrayList<>();

        for (final Object value : new Object[] { 1e300d, -1e39d, new BigDecimal("1e300"), huge }) {
            checks.add(() -> assertRejected(DataType.cfloat(), value, "float"));
        }

        for (final Object value : new Object[] { new BigDecimal("1e400"), new BigDecimal("-1e400"), huge }) {
            checks.add(() -> assertRejected(DataType.cdouble(), value, "double"));
        }

        assertAll(checks);
    }

    /** Pins: exact conversions, rounding, non-finite values, out-of-range integers and unconvertible values keep their behavior. */
    @Test
    void exactNumberConversionsAndOtherBindRulesAreUnchanged() {
        assertAll(() -> assertEquals(2, boundValue(DataType.cint(), 2.0d)), () -> assertEquals(3L, boundValue(DataType.bigint(), new BigDecimal("3"))),
                () -> assertEquals(3, boundValue(DataType.cint(), new BigDecimal("3.000"))),
                () -> assertEquals(300, boundValue(DataType.cint(), new BigDecimal("3E+2"))),
                () -> assertEquals(BigInteger.valueOf(-4), boundValue(DataType.varint(), -4.0f)), () -> assertEquals(0, boundValue(DataType.cint(), -0.0d)),
                () -> assertEquals((short) 7, boundValue(DataType.smallint(), 7L)), () -> assertEquals(7L, boundValue(DataType.counter(), 7)),
                () -> assertEquals(1.5d, boundValue(DataType.cdouble(), 1.5f)), () -> assertEquals(0.1f, boundValue(DataType.cfloat(), 0.1d)),
                () -> assertEquals(Float.POSITIVE_INFINITY, boundValue(DataType.cfloat(), Double.POSITIVE_INFINITY)),
                () -> assertEquals(Float.NaN, boundValue(DataType.cfloat(), Double.NaN)),
                () -> assertEquals(new BigDecimal("1.5"), boundValue(DataType.decimal(), 1.5d)),
                // An integral value out of range, and a non-finite value for an integer column, still throw ArithmeticException.
                () -> assertThrows(ArithmeticException.class, () -> boundValue(DataType.cint(), 1L << 40)),
                () -> assertThrows(ArithmeticException.class, () -> boundValue(DataType.tinyint(), 128)),
                () -> assertThrows(ArithmeticException.class, () -> boundValue(DataType.cint(), Double.NaN)),
                // A value the conversion rejects is still bound unchanged (the driver then reports it).
                () -> assertEquals("abc", boundValue(DataType.cint(), "abc")), () -> assertEquals("1.5", boundValue(DataType.cint(), "1.5")));
    }

    @Test
    void base64TextBoundToABlobColumnIsDecodedIntoAReadableBuffer() {
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.columns(DataType.blob(), "v");
            fixture.executor.execute(UPDATE, BASE64);
            fixture.executor.execute(UPDATE, List.of(BASE64));
            fixture.executor.execute("UPDATE t SET v = :v WHERE id = 1", N.asMap("v", BASE64));
            fixture.executor.execute(UPDATE, "");

            assertAll(() -> assertEquals(4, fixture.values.size()), () -> assertBuffer(BYTES, fixture.values.get(0)[0]),
                    () -> assertBuffer(BYTES, fixture.values.get(1)[0]), () -> assertBuffer(BYTES, fixture.values.get(2)[0]),
                    () -> assertBuffer(new byte[0], fixture.values.get(3)[0]));
        }
    }

    @Test
    void textThatIsNotBase64IsRejectedForABlobColumn() {
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.columns(DataType.blob(), "v");
            final List<Executable> checks = new ArrayList<>();

            for (final Object parameter : new Object[] { "not base64!", List.of("AQ=D"), N.asMap("v", "#") }) {
                checks.add(() -> {
                    final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                            () -> fixture.executor.execute(parameter instanceof Map ? "UPDATE t SET v = :v WHERE id = 1" : UPDATE, parameter));
                    assertTrue(e.getMessage().contains("parameter 'v' (index 0)"), e.getMessage());
                    assertTrue(e.getMessage().contains("CQL type blob"), e.getMessage());
                    assertTrue(e.getMessage().contains("not valid base64"), e.getMessage());
                });
            }

            assertAll(checks);
            assertEquals(0, fixture.values.size());
        }
    }

    /** Pins: byte[] and ByteBuffer bind values for a blob column are unchanged. */
    @Test
    void bytesAndBuffersBoundToABlobColumnAreUnchanged() {
        final ByteBuffer buffer = ByteBuffer.wrap(BYTES);

        assertAll(() -> assertSame(buffer, boundValue(DataType.blob(), buffer)), () -> assertBuffer(BYTES, boundValue(DataType.blob(), BYTES)));
    }

    @Test
    void blobReadIntoStringTargetsIsTheBase64OfTheRemainingBytes() {
        final ByteBuffer positioned = ByteBuffer.wrap(new byte[] { 9, 2, 3 });
        positioned.get();

        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            when(fixture.session.execute(any(Statement.class))).thenAnswer(invocation -> rows(DataType.blob(), ByteBuffer.wrap(BYTES), ByteBuffer.wrap(BYTES)));

            assertAll(() -> assertEquals(BASE64, fixture.executor.findFirst(String.class, QUERY).get()),
                    () -> assertEquals(BASE64, fixture.executor.queryForSingleValue(String.class, QUERY).get()),
                    () -> assertEquals(BASE64, fixture.executor.queryForString(QUERY).get()),
                    () -> assertEquals(List.of(BASE64, BASE64), fixture.executor.list(String.class, QUERY)),
                    () -> assertEquals(BASE64, fixture.executor.findFirst(String[].class,
                            QUERY).get()[0]),
                    () -> assertEquals(List.of(BASE64), CassandraExecutor.toList(rows(DataType.blob(), ByteBuffer.wrap(BYTES)), String.class)),
                    () -> assertEquals(BASE64, CassandraExecutor.toEntity(rows(DataType.blob(), ByteBuffer.wrap(BYTES)).one(), Text.class).getV()),
                    () -> assertEquals(List.of(BASE64),
                            CassandraExecutor.extractData(rows(DataType.blob(), ByteBuffer.wrap(BYTES)), Text.class).getColumn("v")),
                    () -> assertEquals(BASE64,
                            CassandraExecutor.toEntity(rows(new String[] { "nested.v" }, DataType.blob(), new Object[][] { { ByteBuffer.wrap(BYTES) } }).one(),
                                    TextHolder.class).getNested().getV()),
                    // The remaining bytes are encoded, and the driver's buffer is not consumed.
                    () -> {
                        assertEquals("AgM=", CassandraExecutor.toList(rows(DataType.blob(), positioned), String.class).get(0));
                        assertEquals(1, positioned.position());
                    });
        }
    }

    /** Pins: byte[] and ByteBuffer targets of a blob column are unchanged. */
    @Test
    void blobReadIntoBytesAndBufferTargetsIsUnchanged() {
        final ByteBuffer buffer = ByteBuffer.wrap(BYTES);

        assertAll(() -> assertArrayEquals(BYTES, CassandraExecutor.toList(rows(DataType.blob(), buffer), byte[].class).get(0)),
                () -> assertSame(buffer, CassandraExecutor.toList(rows(DataType.blob(), buffer), ByteBuffer.class).get(0)),
                () -> assertEquals(0, buffer.position()));
    }

    @Test
    void aBufferBoundToATextColumnIsItsBase64Text() {
        assertEquals(BASE64, boundValue(DataType.text(), ByteBuffer.wrap(BYTES)));
    }

    @Test
    void udtBlobFieldsUseTheSameBase64Rule() throws Exception {
        final UserType blobType = userType(new CodecRegistry(), "v", DataType.blob());
        final CassandraExecutor.UDTCodec<Map> mapCodec = CassandraExecutor.UDTCodec.create(blobType, Map.class);
        final CassandraExecutor.UDTCodec<Text> textCodec = CassandraExecutor.UDTCodec.create(blobType, Text.class);

        assertAll(
                // Text written to a blob field is decoded (it failed with CodecNotFoundException).
                () -> assertEquals(ByteBuffer.wrap(BYTES),
                        mapCodec.deserialize(mapCodec.serialize(Map.of("v", BASE64), ProtocolVersion.V4), ProtocolVersion.V4).get("v")),
                () -> assertEquals(ByteBuffer.wrap(BYTES),
                        mapCodec.deserialize(textCodec.serialize(new Text(BASE64), ProtocolVersion.V4), ProtocolVersion.V4).get("v")),
                () -> {
                    final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                            () -> mapCodec.serialize(Map.of("v", "not base64!"), ProtocolVersion.V4));
                    assertTrue(e.getMessage().contains("UDT field 'v' (index 0)"), e.getMessage());
                    assertTrue(e.getMessage().contains("not valid base64"), e.getMessage());
                },
                // A blob field read into a String bean property is base64 ("" before); a Map keeps the driver's buffer.
                () -> assertEquals(BASE64, textCodec.deserialize(mapCodec.serialize(Map.of("v", BYTES), ProtocolVersion.V4), ProtocolVersion.V4).getV()),
                () -> assertEquals(ByteBuffer.wrap(BYTES),
                        mapCodec.deserialize(mapCodec.serialize(Map.of("v", BYTES), ProtocolVersion.V4), ProtocolVersion.V4).get("v")));
    }

    private static void assertRejected(final DataType type, final Object value, final String cqlType) {
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.columns(type, "v");
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> fixture.executor.execute(UPDATE, value),
                    value + " -> " + type);
            assertTrue(e.getMessage().contains("parameter 'v' (index 0)"), e.getMessage());
            assertTrue(e.getMessage().contains("CQL type " + cqlType), e.getMessage());
            assertTrue(e.getMessage().contains("Infinity"), e.getMessage());
            assertEquals(0, fixture.values.size());
        }
    }

    private static Object boundValue(final DataType type, final Object value) {
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.columns(type, "v");
            fixture.executor.execute(UPDATE, value);
            final Object[] bind = fixture.values.get(0);
            assertEquals(1, bind.length);
            return bind[0];
        }
    }

    private static void assertBuffer(final byte[] expected, final Object actual) {
        final ByteBuffer buffer = assertInstanceOf(ByteBuffer.class, actual);
        assertEquals(0, buffer.position());
        final byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        assertArrayEquals(expected, bytes);
    }

    /** A real driver 3 row (an {@code ArrayBackedRow}, built through its package-private factory) of the given column types. */
    private static Row driverRow(final String[] names, final DataType[] types, final ByteBuffer... values) throws Exception {
        final Constructor<ColumnDefinitions.Definition> definitionConstructor = ColumnDefinitions.Definition.class.getDeclaredConstructor(String.class,
                String.class, String.class, DataType.class);
        definitionConstructor.setAccessible(true);
        final ColumnDefinitions.Definition[] definitions = new ColumnDefinitions.Definition[names.length];

        for (int i = 0; i < names.length; i++) {
            definitions[i] = definitionConstructor.newInstance("ks", "t", names[i], types[i]);
        }

        final Constructor<ColumnDefinitions> columnsConstructor = ColumnDefinitions.class.getDeclaredConstructor(ColumnDefinitions.Definition[].class,
                CodecRegistry.class);
        columnsConstructor.setAccessible(true);
        final ColumnDefinitions columns = columnsConstructor.newInstance(definitions, new CodecRegistry());
        final Class<?> rowClass = Class.forName("com.datastax.driver.core.ArrayBackedRow");
        final Method fromData = rowClass.getDeclaredMethod("fromData", ColumnDefinitions.class, Class.forName("com.datastax.driver.core.Token$Factory"),
                ProtocolVersion.class, List.class);
        fromData.setAccessible(true);

        return (Row) fromData.invoke(null, columns, null, ProtocolVersion.V4, Arrays.asList(values));
    }

    private static UserType userType(final CodecRegistry registry, final Object... namesAndTypes) throws Exception {
        final var fieldConstructor = UserType.Field.class.getDeclaredConstructor(String.class, DataType.class);
        fieldConstructor.setAccessible(true);
        final List<UserType.Field> fields = new ArrayList<>();

        for (int i = 0; i < namesAndTypes.length; i += 2) {
            fields.add(fieldConstructor.newInstance(namesAndTypes[i], namesAndTypes[i + 1]));
        }

        final var typeConstructor = UserType.class.getDeclaredConstructor(String.class, String.class, boolean.class, Collection.class, ProtocolVersion.class,
                CodecRegistry.class);
        typeConstructor.setAccessible(true);
        return typeConstructor.newInstance("ks", "value_holder", false, fields, ProtocolVersion.V4, registry);
    }

    /** Rows of one column {@code v} of {@code type}, one row per value. */
    private static ResultSet rows(final DataType type, final Object... values) {
        return rows(new String[] { "v" }, type, Arrays.stream(values).map(value -> new Object[] { value }).toArray(Object[][]::new));
    }

    private static ResultSet rows(final String[] names, final DataType type, final Object[][] values) {
        final ColumnDefinitions columns = mock(ColumnDefinitions.class);
        when(columns.size()).thenReturn(names.length);

        for (int i = 0; i < names.length; i++) {
            when(columns.getName(i)).thenReturn(names[i]);
            when(columns.getType(i)).thenReturn(type);
        }

        final List<Row> rows = Arrays.stream(values).map(rowValues -> {
            final Row row = mock(Row.class);
            when(row.getColumnDefinitions()).thenReturn(columns);

            for (int i = 0; i < rowValues.length; i++) {
                when(row.getObject(i)).thenReturn(rowValues[i]);
            }

            return row;
        }).toList();
        final ListIterator<Row> iterator = rows.listIterator();
        final ResultSet result = mock(ResultSet.class);
        when(result.getColumnDefinitions()).thenReturn(columns);
        when(result.getAvailableWithoutFetching()).thenAnswer(invocation -> rows.size() - iterator.nextIndex());
        when(result.iterator()).thenReturn(iterator);
        when(result.one()).thenAnswer(invocation -> iterator.hasNext() ? iterator.next() : null);
        when(result.isExhausted()).thenAnswer(invocation -> !iterator.hasNext());
        return result;
    }

    public static class Person {
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(final int age) {
            this.age = age;
        }
    }

    public static class DoubleHolder {
        private double v;

        public DoubleHolder() {
        }

        public DoubleHolder(final double v) {
            this.v = v;
        }

        public double getV() {
            return v;
        }

        public void setV(final double v) {
            this.v = v;
        }
    }

    public static class Text {
        private String v;

        public Text() {
        }

        public Text(final String v) {
            this.v = v;
        }

        public String getV() {
            return v;
        }

        public void setV(final String v) {
            this.v = v;
        }
    }

    public static class TextHolder {
        private Text nested;

        public Text getNested() {
            return nested;
        }

        public void setNested(final Text nested) {
            this.nested = nested;
        }
    }

    // ---- 2026-10-24 r2 sliceK ----

    /**
     * The lossy check tested only {@code Double}, {@code Float} and {@code BigDecimal} sources, but {@code N.convert} truncates any
     * {@link Number}: an abacus {@code Fraction} {@code 3/2}, a {@code MutableDouble}/{@code MutableFloat} {@code 1.5} or a
     * {@code DoubleAdder} {@code 1.5} was still bound as {@code 1} to an integer column (and as 1 ns to a {@code time} column).
     */
    @Test
    void r2sliceK_fractionalNumbersOfEveryTypeAreRejectedForIntegerColumns() {
        final DataType[] integerTypes = { DataType.tinyint(), DataType.smallint(), DataType.cint(), DataType.bigint(), DataType.varint(), DataType.counter(),
                DataType.time() };
        final java.util.concurrent.atomic.DoubleAdder adder = new java.util.concurrent.atomic.DoubleAdder();
        adder.add(1.5);
        final Object[] fractions = { com.landawn.abacus.util.Fraction.of(3, 2), com.landawn.abacus.util.MutableDouble.of(1.5),
                com.landawn.abacus.util.MutableFloat.of(-1.5f), adder };
        final List<Executable> checks = new ArrayList<>();

        for (final DataType type : integerTypes) {
            for (final Object value : fractions) {
                checks.add(() -> {
                    try (CassandraTestFixture fixture = new CassandraTestFixture()) {
                        fixture.columns(type, "v");
                        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> fixture.executor.execute(UPDATE, value),
                                value + " -> " + type);
                        assertTrue(e.getMessage().contains("parameter 'v' (index 0)"), e.getMessage());
                        assertTrue(e.getMessage().contains("CQL type " + type), e.getMessage());
                        assertTrue(e.getMessage().contains("it would be bound as"), e.getMessage());
                        assertEquals(0, fixture.values.size());
                    }
                });
            }
        }

        assertAll(checks);
    }

    /**
     * Every source other than {@code Double}/{@code Float} was taken for finite, so an infinite {@code MutableDouble},
     * {@code MutableFloat} or {@code DoubleAdder} bound to a {@code float} or {@code double} column, which keeps the value
     * ({@code Infinity}), was rejected "without changing it: it would be bound as Infinity".
     */
    @Test
    void r2sliceK_infiniteNumbersOfEveryTypeBindToFloatingColumns() {
        final java.util.concurrent.atomic.DoubleAdder infinite = new java.util.concurrent.atomic.DoubleAdder();
        infinite.add(Double.POSITIVE_INFINITY);

        assertAll(
                () -> assertEquals(Double.POSITIVE_INFINITY,
                        boundValue(DataType.cdouble(), com.landawn.abacus.util.MutableDouble.of(Double.POSITIVE_INFINITY))),
                () -> assertEquals(Float.POSITIVE_INFINITY, boundValue(DataType.cfloat(), com.landawn.abacus.util.MutableDouble.of(Double.POSITIVE_INFINITY))),
                () -> assertEquals(Float.NEGATIVE_INFINITY, boundValue(DataType.cfloat(), com.landawn.abacus.util.MutableFloat.of(Float.NEGATIVE_INFINITY))),
                () -> assertEquals(Double.POSITIVE_INFINITY, boundValue(DataType.cdouble(), infinite)),
                () -> assertEquals(Float.NaN, boundValue(DataType.cfloat(), com.landawn.abacus.util.MutableDouble.of(Double.NaN))));
    }

    /** Pins: exact values of other number types bind, a finite one beyond the float range and a non-finite one for an integer fail. */
    @Test
    void r2sliceK_otherNumberTypesKeepTheirExactAndFailingConversions() {
        assertAll(() -> assertEquals(2, boundValue(DataType.cint(), com.landawn.abacus.util.Fraction.of(4, 2))),
                () -> assertEquals(2, boundValue(DataType.cint(), com.landawn.abacus.util.MutableDouble.of(2.0))),
                () -> assertEquals(7L, boundValue(DataType.bigint(), new java.util.concurrent.atomic.AtomicLong(7))),
                () -> assertEquals(BigInteger.valueOf(7), boundValue(DataType.varint(), com.landawn.abacus.util.MutableInt.of(7))),
                () -> assertEquals(3L, boundValue(DataType.time(), com.landawn.abacus.util.Fraction.of(6, 2))),
                () -> assertEquals(1.5d, boundValue(DataType.cdouble(), com.landawn.abacus.util.Fraction.of(3, 2))),
                () -> assertRejected(DataType.cfloat(), com.landawn.abacus.util.MutableDouble.of(1e300), "float"),
                () -> assertThrows(ArithmeticException.class, () -> boundValue(DataType.cint(), com.landawn.abacus.util.MutableDouble.of(Double.NaN))),
                () -> assertThrows(ArithmeticException.class, () -> boundValue(DataType.bigint(), com.landawn.abacus.util.MutableDouble.of(1e300))));
    }

    /**
     * A collection or map column read into a parameterized container property kept the driver's element types (heap pollution,
     * {@code ClassCastException} where an element was used), on real driver 3 rows: a {@code List<byte[]>} held {@code ByteBuffer}s,
     * a {@code List<LocalDateTime>} {@code Date}s, a {@code Set<String>} {@code InetAddress}es, a {@code List<Long>} {@code Integer}s;
     * an array property ({@code String[]}, {@code byte[][]}) got {@code N.convert}'s per-element results ({@code "/127.0.0.1"},
     * {@code null}). The v4 twin is {@code cassandra.CassandraExecutorDeepReview2026BindBlobRowConversionTest} (r2 sliceH).
     */
    @Test
    void r2sliceK_collectionColumnElementsAreConvertedToTheDeclaredElementType() throws Exception {
        final java.net.InetAddress local = java.net.InetAddress.getByAddress(new byte[] { 127, 0, 0, 1 });
        final java.net.InetAddress other = java.net.InetAddress.getByAddress(new byte[] { 10, 0, 0, 2 });
        final java.util.Date first = new java.util.Date(1_577_934_245_000L);
        final java.util.Date second = new java.util.Date(1_623_053_350_123L);
        final ProtocolVersion v = ProtocolVersion.V4;
        final ByteBuffer blobs = TypeCodec.list(TypeCodec.blob()).serialize(List.of(ByteBuffer.wrap(BYTES)), v);
        final ByteBuffer hosts = TypeCodec.set(TypeCodec.inet()).serialize(new java.util.LinkedHashSet<>(List.of(local, other)), v);
        final ByteBuffer stamps = TypeCodec.list(TypeCodec.timestamp()).serialize(List.of(first, second), v);
        final Map<String, java.net.InetAddress> hostMap = new java.util.LinkedHashMap<>();
        hostMap.put("a", local);
        hostMap.put("b", other);

        final Containers c = CassandraExecutor.toEntity(driverRow(
                new String[] { "blobs", "blobText", "blobArray", "stamps", "hosts", "sortedHosts", "hostArray", "hostMap", "longs", "nestedStamps" },
                new DataType[] { DataType.list(DataType.blob()), DataType.list(DataType.blob()), DataType.list(DataType.blob()),
                        DataType.list(DataType.timestamp()), DataType.set(DataType.inet()), DataType.set(DataType.inet()), DataType.set(DataType.inet()),
                        DataType.map(DataType.text(), DataType.inet()), DataType.list(DataType.cint()), DataType.list(DataType.list(DataType.timestamp())) },
                blobs, blobs.duplicate(), blobs.duplicate(), stamps, hosts, hosts.duplicate(), hosts.duplicate(),
                TypeCodec.map(TypeCodec.varchar(), TypeCodec.inet()).serialize(hostMap, v), TypeCodec.list(TypeCodec.cint()).serialize(List.of(1, 2), v),
                TypeCodec.list(TypeCodec.list(TypeCodec.timestamp())).serialize(List.of(List.of(first), List.of(second)), v)), Containers.class);
        final java.time.LocalDateTime firstLocal = java.time.LocalDateTime.ofInstant(first.toInstant(), java.time.ZoneId.systemDefault());
        final java.time.LocalDateTime secondLocal = java.time.LocalDateTime.ofInstant(second.toInstant(), java.time.ZoneId.systemDefault());

        assertAll(() -> {
            assertEquals(1, c.blobs.size());
            assertArrayEquals(BYTES, assertInstanceOf(byte[].class, (Object) c.blobs.get(0)));
        }, () -> assertEquals(List.of(BASE64), c.blobText), () -> {
            assertEquals(1, c.blobArray.length);
            assertArrayEquals(BYTES, c.blobArray[0]);
        }, () -> assertEquals(List.of(firstLocal, secondLocal), c.stamps),
                // The driver's iteration order is kept (a LinkedHashSet/LinkedHashMap), and a sorted declared type is honored.
                () -> assertEquals(List.of("127.0.0.1", "10.0.0.2"), new ArrayList<>(c.hosts)), () -> assertInstanceOf(java.util.LinkedHashSet.class, c.hosts),
                () -> assertEquals(new java.util.TreeSet<>(List.of("127.0.0.1", "10.0.0.2")), c.sortedHosts),
                () -> assertInstanceOf(java.util.TreeSet.class, c.sortedHosts), () -> assertArrayEquals(new String[] { "127.0.0.1", "10.0.0.2" }, c.hostArray),
                () -> assertEquals(List.of("a", "b"), new ArrayList<>(c.hostMap.keySet())),
                () -> assertEquals(List.of("127.0.0.1", "10.0.0.2"), new ArrayList<>(c.hostMap.values())), () -> assertEquals(List.of(1L, 2L), c.longs),
                () -> assertEquals(List.of(List.of(firstLocal), List.of(secondLocal)), c.nestedStamps));
    }

    @Test
    void r2sliceK_nestedPathAndUdtBeanContainerPropertiesConvertTheirElements() throws Exception {
        final java.net.InetAddress local = java.net.InetAddress.getByAddress(new byte[] { 127, 0, 0, 1 });
        final ContainerHolder holder = CassandraExecutor.toEntity(driverRow(new String[] { "inner.hosts" }, new DataType[] { DataType.set(DataType.inet()) },
                TypeCodec.set(TypeCodec.inet()).serialize(java.util.Set.of(local), ProtocolVersion.V4)), ContainerHolder.class);

        final UserType listType = userType(new CodecRegistry(), "v", DataType.list(DataType.blob()));
        final CassandraExecutor.UDTCodec<Map> mapCodec = CassandraExecutor.UDTCodec.create(listType, Map.class);
        final CassandraExecutor.UDTCodec<BlobList> beanCodec = CassandraExecutor.UDTCodec.create(listType, BlobList.class);
        final BlobList decoded = beanCodec.deserialize(mapCodec.serialize(Map.of("v", List.of(ByteBuffer.wrap(BYTES))), ProtocolVersion.V4),
                ProtocolVersion.V4);

        assertAll(() -> assertEquals(java.util.Set.of("127.0.0.1"), holder.inner.hosts), () -> {
            assertEquals(1, decoded.v.size());
            assertArrayEquals(BYTES, assertInstanceOf(byte[].class, (Object) decoded.v.get(0)));
        });
    }

    /** Pins: a container whose elements already have the declared type is the driver's own instance (no copy); UDT elements are kept. */
    @Test
    void r2sliceK_containersWhoseElementsFitAreSetUnchanged() throws Exception {
        final List<Object> buffers = new ArrayList<>(List.of(ByteBuffer.wrap(BYTES)));
        final List<Object> dates = new ArrayList<>(List.of(new java.util.Date(0)));
        final List<Object> texts = new ArrayList<>(List.of("a"));
        final List<Object> udts = new ArrayList<>(List.of(userType(new CodecRegistry(), "v", DataType.text()).newValue().setString("v", "x")));
        final Fits fits = CassandraExecutor.toEntity(rows(new String[] { "buffers", "dates", "raw", "objects", "texts", "udts" }, DataType.blob(),
                new Object[][] { { buffers, dates, buffers, dates, texts, udts } }).one(), Fits.class);

        assertAll(() -> assertSame(buffers, fits.buffers), () -> assertSame(dates, fits.dates), () -> assertSame(buffers, fits.raw),
                () -> assertSame(dates, fits.objects), () -> assertSame(texts, fits.texts), () -> assertSame(udts, fits.udts));
    }

    @Test
    void r2sliceK_anElementThatCannotBeConvertedFailsTheRowAsATopLevelValueDoes() {
        final RuntimeException topLevel = assertThrows(RuntimeException.class,
                () -> CassandraExecutor.toEntity(rows(new String[] { "number" }, DataType.text(), new Object[][] { { "abc" } }).one(), NumberHolder.class));
        final RuntimeException element = assertThrows(RuntimeException.class, () -> CassandraExecutor
                .toEntity(rows(new String[] { "numbers" }, DataType.text(), new Object[][] { { new ArrayList<>(List.of("abc")) } }).one(), NumberHolder.class));

        assertEquals(topLevel.getClass(), element.getClass());
    }

    public static class Containers {
        public List<byte[]> blobs;
        public List<String> blobText;
        public byte[][] blobArray;
        public List<java.time.LocalDateTime> stamps;
        public java.util.Set<String> hosts;
        public java.util.SortedSet<String> sortedHosts;
        public String[] hostArray;
        public Map<String, String> hostMap;
        public List<Long> longs;
        public List<List<java.time.LocalDateTime>> nestedStamps;
    }

    public static class ContainerHolder {
        public Containers inner;
    }

    public static class BlobList {
        public List<byte[]> v;
    }

    @SuppressWarnings("rawtypes")
    public static class Fits {
        public List<ByteBuffer> buffers;
        public List<java.util.Date> dates;
        public List raw;
        public List<Object> objects;
        public List<String> texts;
        public List<Text> udts;
    }

    public static class NumberHolder {
        public Integer number;
        public List<Integer> numbers;
    }

    // ---- 2026-10-24 r2 verify5 ----

    // A scalar element for a parameterized container element type was converted to the raw class (N.convert(3, List.class)), so
    // a set<int> column into a List<List<Long>> property became [[3], [1]] holding Integers, where the HEAD code (setPropValue's
    // N.convert of the whole set to the declared type) gave [[3L], [1L]]; likewise a map<text, int> into a Map<String, List<Long>>.
    // The v4 twin is cassandra.CassandraExecutorDeepReview2026BindBlobRowConversionTest (r2 verify5).
    @Test
    void r2verify5_scalarElementsForContainerElementTypesGetTheFullDeclaredType() throws Exception {
        final ProtocolVersion v = ProtocolVersion.V4;
        final Map<String, Integer> counts = new java.util.LinkedHashMap<>();
        counts.put("b", 2);
        counts.put("a", 1);

        final NestedElements n = CassandraExecutor.toEntity(driverRow(new String[] { "longLists", "textLists", "longListMap" },
                new DataType[] { DataType.set(DataType.cint()), DataType.set(DataType.bigint()), DataType.map(DataType.text(), DataType.cint()) },
                TypeCodec.set(TypeCodec.cint()).serialize(new java.util.LinkedHashSet<>(List.of(3, 1)), v),
                TypeCodec.set(TypeCodec.bigint()).serialize(new java.util.LinkedHashSet<>(List.of(3L, 1L)), v),
                TypeCodec.map(TypeCodec.varchar(), TypeCodec.cint()).serialize(counts, v)), NestedElements.class);

        assertAll(() -> assertEquals(List.of(List.of(3L), List.of(1L)), n.longLists), () -> assertInstanceOf(Long.class, n.longLists.get(0).get(0)),
                () -> assertEquals(List.of(List.of("3"), List.of("1")), n.textLists), () -> assertInstanceOf(String.class, n.textLists.get(0).get(0)),
                () -> assertEquals(List.of("b", "a"), new ArrayList<>(n.longListMap.keySet())),
                () -> assertEquals(List.of(List.of(2L), List.of(1L)), new ArrayList<>(n.longListMap.values())),
                () -> assertInstanceOf(Long.class, n.longListMap.get("b").get(0)));
    }

    public static class NestedElements {
        public List<List<Long>> longLists;
        public List<List<String>> textLists;
        public Map<String, List<Long>> longListMap;
    }

    // ---- 2026-10-27 sliceK ----

    // N.convert(value, ByteBuffer.class) base64-decodes String.valueOf(value) into a buffer positioned at its limit, so a non-text
    // value whose text happened to be base64 (true, 1234, 12345678L, a StringBuilder) was bound to a blob or custom column as an EMPTY
    // blob, while one whose text is not (false, 5) was kept and rejected by the driver. Such a value is now always kept (a CharBuffer
    // included; a StringBuilder or StringBuffer is text and decoded like a String, see sbfix below).
    @Test
    void sliceK_nonTextValuesBoundToABlobColumnAreKeptNotBoundAsAnEmptyBlob() {
        final List<Executable> checks = new ArrayList<>();

        for (final DataType type : new DataType[] { DataType.blob(), DataType.custom("com.example.BlobType") }) {
            for (final Object value : new Object[] { true, 1234, 12345678L, java.nio.CharBuffer.wrap(BASE64), false, 5 }) {
                checks.add(() -> assertSame(value, boundValue(type, value), value + " -> " + type));
            }

            // Text, bytes and buffers keep their rules.
            checks.add(() -> assertBuffer(BYTES, boundValue(type, BASE64)));
            checks.add(() -> assertBuffer(BYTES, boundValue(type, BYTES)));
            checks.add(() -> {
                final ByteBuffer buffer = ByteBuffer.wrap(BYTES);
                assertSame(buffer, boundValue(type, buffer));
            });
        }

        assertAll(checks);
    }

    // N.convert reads a value bound to a map column as JSON text and fails with ParsingException, not IllegalArgumentException, for
    // text that is not a JSON object; that escaped the keep-the-original fallback, so the bind failed with ParsingException instead of
    // binding the value unchanged for the driver to reject (CodecNotFoundException), as the prepareStatement documentation states.
    @Test
    void sliceK_aValueThatIsNotAJsonObjectIsKeptForAMapColumn() {
        final DataType mapType = DataType.map(DataType.text(), DataType.cint());

        assertAll(() -> assertEquals("abc", boundValue(mapType, "abc")), () -> assertEquals(5, boundValue(mapType, 5)),
                () -> assertEquals("[1,2]", boundValue(mapType, "[1,2]")),
                // Pin: JSON object text still converts.
                () -> assertEquals(Map.of("a", 1), boundValue(mapType, "{\"a\": 1}")));
    }

    /** Pins the documented ArithmeticException for a NaN or infinite value bound to a decimal column (not only integer columns). */
    @Test
    void sliceK_nanAndInfiniteValuesForADecimalColumnFailWithArithmeticException() {
        assertAll(() -> assertThrows(ArithmeticException.class, () -> boundValue(DataType.decimal(), Double.NaN)),
                () -> assertThrows(ArithmeticException.class, () -> boundValue(DataType.decimal(), Float.NEGATIVE_INFINITY)),
                () -> assertThrows(ArithmeticException.class, () -> boundValue(DataType.varint(), Double.POSITIVE_INFINITY)),
                () -> assertEquals(new BigDecimal("1.5"), boundValue(DataType.decimal(), 1.5d)));
    }

    // A blob read into a target or bean property without a blob form was converted (N.convert) from the empty text before the
    // buffer's position: a Long, UUID, Date, Map or String[] property became null, an int 0, an Optional<String> Optional[""] and a
    // CharSequence "", so the blob was silently lost. A CharSequence now receives the base64 text and any other such target is
    // rejected, top-level, single-value, extractData and UDT field alike (the 4.x executor's rule). (fixD: an Optional, Nullable or
    // Holder now holds the blob converted to its element type: the Optional<String> gets the base64 text, see the fixD tests.)
    @Test
    void sliceK_aBlobReadIntoATypeWithoutABlobFormIsRejectedNotLost() throws Exception {
        final List<Executable> checks = new ArrayList<>();

        for (final String property : new String[] { "l", "i", "u", "d", "m", "sa" }) {
            checks.add(() -> assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toEntity(blobRow(property), BlobTargets.class), property));
        }

        checks.add(() -> assertEquals(java.util.Optional.of(BASE64), CassandraExecutor.toEntity(blobRow("os"), BlobTargets.class).os));

        for (final Class<?> target : new Class<?>[] { Long.class, long.class, java.util.UUID.class, java.util.Date.class, int[].class }) {
            checks.add(() -> assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toList(rows(DataType.blob(), ByteBuffer.wrap(BYTES)), target),
                    target.getName()));
        }

        checks.add(() -> assertEquals(BASE64, CassandraExecutor.toEntity(blobRow("cs"), BlobTargets.class).cs.toString()));
        checks.add(() -> assertEquals(BASE64, CassandraExecutor.toList(rows(DataType.blob(), ByteBuffer.wrap(BYTES)), CharSequence.class).get(0).toString()));
        // extractData types a column by the bean's getter (LongHolder.getValue()).
        checks.add(() -> assertThrows(IllegalArgumentException.class, () -> CassandraExecutor
                .extractData(rows(new String[] { "value" }, DataType.blob(), new Object[][] { { ByteBuffer.wrap(BYTES) } }), LongHolder.class)));

        final UserType blobType = userType(new CodecRegistry(), "value", DataType.blob());
        final CassandraExecutor.UDTCodec<Map> mapCodec = CassandraExecutor.UDTCodec.create(blobType, Map.class);
        final CassandraExecutor.UDTCodec<LongHolder> longCodec = CassandraExecutor.UDTCodec.create(blobType, LongHolder.class);
        checks.add(() -> assertThrows(IllegalArgumentException.class,
                () -> longCodec.deserialize(mapCodec.serialize(Map.of("value", BYTES), ProtocolVersion.V4), ProtocolVersion.V4)));

        // Pins: Object, collection, byte[] and String targets are unchanged.
        checks.add(() -> {
            final BlobTargets targets = CassandraExecutor.toEntity(blobRow("o"), BlobTargets.class);
            assertEquals(ByteBuffer.wrap(BYTES), targets.o);
        });
        checks.add(() -> assertEquals(List.of(ByteBuffer.wrap(BYTES)), CassandraExecutor.toEntity(blobRow("lo"), BlobTargets.class).lo));
        checks.add(() -> assertArrayEquals(BYTES, CassandraExecutor.toEntity(blobRow("b"), BlobTargets.class).b));
        checks.add(() -> assertEquals(BASE64, CassandraExecutor.toEntity(blobRow("s"), BlobTargets.class).s));

        assertAll(checks);
    }

    // A ByteBuffer bound to a column without a blob form was converted (N.convert) from the empty text before its position, into
    // null for an int, bigint, timestamp, uuid, time or map column, so a NULL was written. It is now bound unchanged, which the driver
    // rejects (CodecNotFoundException), as for a boolean or date column, whose conversion already failed.
    @Test
    void sliceK_aBufferBoundToAColumnWithoutABlobFormIsKeptNotBoundAsNull() {
        final ByteBuffer buffer = ByteBuffer.wrap(BYTES);
        final List<Executable> checks = new ArrayList<>();

        for (final DataType type : new DataType[] { DataType.cint(), DataType.bigint(), DataType.timestamp(), DataType.uuid(), DataType.time(),
                DataType.map(DataType.text(), DataType.cint()), DataType.cboolean(), DataType.date() }) {
            checks.add(() -> assertSame(buffer, boundValue(type, buffer), type.toString()));
        }

        // Pins: a text column gets the base64 text, and a list<blob> column the buffer as its single element.
        checks.add(() -> assertEquals(BASE64, boundValue(DataType.text(), buffer)));
        checks.add(() -> assertEquals(List.of(buffer), boundValue(DataType.list(DataType.blob()), buffer)));

        assertAll(checks);
    }

    private static Row blobRow(final String column) {
        return rows(new String[] { column }, DataType.blob(), new Object[][] { { ByteBuffer.wrap(BYTES) } }).one();
    }

    public static class BlobTargets {
        public Long l;
        public int i;
        public java.util.UUID u;
        public java.util.Date d;
        public Map<String, Object> m;
        public String[] sa;
        public java.util.Optional<String> os;
        public CharSequence cs;
        public Object o;
        public List<Object> lo;
        public byte[] b;
        public String s;
    }

    public static class LongHolder {
        private Long value;

        public Long getValue() {
            return value;
        }

        public void setValue(final Long value) {
            this.value = value;
        }
    }

    // (verify) The blob rules above must leave application conversions registered with abacus-common alone. A value of an
    // application class with a converter to ByteBuffer (N.registerConverter) was bound by that converter; the first blob-bind rule
    // kept it unchanged (CodecNotFoundException). Only values of built-in classes are kept now (pinned: Boolean, Integer).
    @Test
    void sliceK_anApplicationValueWithAConverterToByteBufferIsBoundByIt() {
        example.cassandra.BlobFixtures.register();

        // BlobBox is a record, which a single parameter would supply as a bean of named values: pass it in a one-element list.
        assertAll(() -> assertBuffer(BYTES, boundValue(DataType.blob(), List.of(new example.cassandra.BlobFixtures.BlobBox(BYTES)))),
                () -> assertBuffer(BYTES, boundValue(DataType.custom("com.example.BlobType"), List.of(new example.cassandra.BlobFixtures.BlobBox(BYTES)))),
                () -> assertSame(Boolean.TRUE, boundValue(DataType.blob(), Boolean.TRUE)), () -> assertEquals(1234, boundValue(DataType.blob(), 1234)));
    }

    // (verify) A blob read into a target whose abacus Type the application registered (TypeFactory.registerType) was converted
    // by that Type; the blob rule rejected it as a target without a blob form. Only targets with abacus-common's own Types are.
    @Test
    void sliceK_aBlobReadIntoAnApplicationTypeUsesItsRegisteredType() throws Exception {
        example.cassandra.BlobFixtures.register();

        assertAll(
                () -> assertArrayEquals(BYTES,
                        CassandraExecutor.toList(rows(DataType.blob(), ByteBuffer.wrap(BYTES)), example.cassandra.BlobFixtures.TypedBox.class).get(0).bytes()),
                () -> assertArrayEquals(BYTES, CassandraExecutor.toEntity(blobRow("box"), TypedBoxHolder.class).box.bytes()), () -> {
                    final List<example.cassandra.BlobFixtures.TypedBox> boxes = CassandraExecutor.toEntity(rows(new String[] { "boxes" },
                            DataType.list(DataType.blob()), new Object[][] { { new ArrayList<>(List.of(ByteBuffer.wrap(BYTES))) } }).one(),
                            TypedBoxHolder.class).boxes;
                    assertEquals(1, boxes.size());
                    assertArrayEquals(BYTES, boxes.get(0).bytes());
                },
                // Pin: a target with abacus-common's own Type is still rejected.
                () -> assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toEntity(blobRow("l"), BlobTargets.class)));
    }

    // (verify) A blob read into a collection bean property whose element type the buffer is not (List<byte[]>, List<String>,
    // Set<String>) was silently null (setPropValue's N.convert of the buffer to the collection type); it now holds the blob as its
    // only element converted to that type, and an element type without a blob form (List<Long>) fails the row. A UDT blob field
    // read into such a property alike. A collection whose element type the buffer is (List<Object>) is unchanged (pinned).
    @Test
    void sliceK_aBlobReadIntoACollectionPropertyOfAnotherElementTypeHoldsItConverted() throws Exception {
        final UserType blobType = userType(new CodecRegistry(), "bytesList", DataType.blob());
        final CassandraExecutor.UDTCodec<Map> mapCodec = CassandraExecutor.UDTCodec.create(blobType, Map.class);
        final CassandraExecutor.UDTCodec<BlobCollections> beanCodec = CassandraExecutor.UDTCodec.create(blobType, BlobCollections.class);

        assertAll(() -> {
            final List<byte[]> bytesList = CassandraExecutor.toEntity(blobRow("bytesList"), BlobCollections.class).bytesList;
            assertEquals(1, bytesList.size());
            assertArrayEquals(BYTES, bytesList.get(0));
        }, () -> assertEquals(List.of(BASE64), CassandraExecutor.toEntity(blobRow("texts"), BlobCollections.class).texts),
                () -> assertEquals(java.util.Set.of(BASE64), CassandraExecutor.toEntity(blobRow("textSet"), BlobCollections.class).textSet),
                () -> assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toEntity(blobRow("longs"), BlobCollections.class)), () -> {
                    final List<byte[]> bytesList = beanCodec.deserialize(mapCodec.serialize(Map.of("bytesList", BYTES), ProtocolVersion.V4),
                            ProtocolVersion.V4).bytesList;
                    assertEquals(1, bytesList.size());
                    assertArrayEquals(BYTES, bytesList.get(0));
                }, () -> assertEquals(List.of(BASE64), CassandraExecutor.toEntity(blobRow("nested.texts"), BlobCollectionsHolder.class).nested.texts), () -> {
                    final List<List<byte[]>> nestedBytes = CassandraExecutor.toEntity(rows(new String[] { "nestedBytes" }, DataType.list(DataType.blob()),
                            new Object[][] { { new ArrayList<>(List.of(ByteBuffer.wrap(BYTES))) } }).one(), BlobCollections.class).nestedBytes;
                    assertEquals(1, nestedBytes.size());
                    assertEquals(1, nestedBytes.get(0).size());
                    assertArrayEquals(BYTES, nestedBytes.get(0).get(0));
                },
                // Pins: a collection whose element type the buffer is holds the buffer.
                () -> assertEquals(List.of(ByteBuffer.wrap(BYTES)), CassandraExecutor.toEntity(blobRow("objects"), BlobCollections.class).objects),
                () -> assertEquals(List.of(ByteBuffer.wrap(BYTES)), CassandraExecutor.toEntity(blobRow("buffers"), BlobCollections.class).buffers));
    }

    public static class BlobCollectionsHolder {
        public BlobCollections nested;
    }

    // (verify) A collection element whose type is a wrapper with type arguments was converted to the raw wrapper class:
    // N.convert(7, Optional.class) put an Optional<String> into a List<Optional<Long>> (heap pollution, ClassCastException only
    // where the element was used). It now gets the full declared type, as a top-level property of that type does.
    @Test
    void sliceK_wrapperElementTypesKeepTheirTypeArguments() throws Exception {
        final ProtocolVersion v = ProtocolVersion.V4;
        final WrapperElements w = CassandraExecutor.toEntity(driverRow(new String[] { "optLongs", "nulStrs", "optMap" },
                new DataType[] { DataType.list(DataType.cint()), DataType.list(DataType.cint()), DataType.map(DataType.text(), DataType.cint()) },
                TypeCodec.list(TypeCodec.cint()).serialize(List.of(7), v), TypeCodec.list(TypeCodec.cint()).serialize(List.of(7), v),
                TypeCodec.map(TypeCodec.varchar(), TypeCodec.cint()).serialize(Map.of("a", 7), v)), WrapperElements.class);

        assertAll(() -> assertInstanceOf(Long.class, ((com.landawn.abacus.util.u.Optional<?>) w.optLongs.get(0)).get()),
                () -> assertEquals(com.landawn.abacus.util.u.Optional.of(7L), w.optLongs.get(0)),
                () -> assertInstanceOf(Long.class, ((com.landawn.abacus.util.u.Optional<?>) w.optMap.get("a")).get()),
                // Pin: a wrapper whose type argument the raw conversion happened to produce is unchanged.
                () -> assertEquals(com.landawn.abacus.util.u.Nullable.of("7"), w.nulStrs.get(0)));
    }

    public static class WrapperElements {
        public List<com.landawn.abacus.util.u.Optional<Long>> optLongs;
        public List<com.landawn.abacus.util.u.Nullable<String>> nulStrs;
        public Map<String, com.landawn.abacus.util.u.Optional<Long>> optMap;
    }

    public static class TypedBoxHolder {
        public example.cassandra.BlobFixtures.TypedBox box;
        public List<example.cassandra.BlobFixtures.TypedBox> boxes;
    }

    public static class BlobCollections {
        private List<byte[]> bytesList;
        public List<String> texts;
        public java.util.Set<String> textSet;
        public List<Long> longs;
        public List<Object> objects;
        public List<ByteBuffer> buffers;
        public List<List<byte[]>> nestedBytes;

        public List<byte[]> getBytesList() {
            return bytesList;
        }

        public void setBytesList(final List<byte[]> bytesList) {
            this.bytesList = bytesList;
        }
    }

    // (verify2) An EMPTY Optional-like value bound to a blob or custom column was bound unchanged by the blob rule (the driver's
    // CodecNotFoundException), where HEAD's N.convert bound NULL, as an int or text column still does, so an entity insert with an
    // empty Optional<ByteBuffer> or Nullable<byte[]> property failed. An empty wrapper binds NULL; a present one binds its content by
    // the blob rules (HEAD bound an empty blob for a present Optional<ByteBuffer>); one whose content has no blob form stays rejected.
    @Test
    void sliceK_emptyOptionalLikeValuesBindNullForBlobColumns() {
        final List<Executable> checks = new ArrayList<>();

        for (final DataType type : new DataType[] { DataType.blob(), DataType.custom("com.example.BlobType") }) {
            for (final Object empty : new Object[] { java.util.Optional.empty(), com.landawn.abacus.util.u.Optional.empty(),
                    com.landawn.abacus.util.u.Nullable.empty(), com.landawn.abacus.util.u.Nullable.of((Object) null), com.landawn.abacus.util.Holder.of(null),
                    java.util.OptionalInt.empty(), com.landawn.abacus.util.u.OptionalInt.empty() }) {
                checks.add(() -> org.junit.jupiter.api.Assertions.assertNull(boundValue(type, empty), empty + " -> " + type));
            }

            checks.add(() -> assertBuffer(BYTES, boundValue(type, java.util.Optional.of(ByteBuffer.wrap(BYTES)))));
            checks.add(() -> assertBuffer(BYTES, boundValue(type, com.landawn.abacus.util.u.Nullable.of(BYTES))));
            checks.add(() -> assertBuffer(BYTES, boundValue(type, java.util.Optional.of(BASE64))));
            checks.add(() -> {
                final java.util.Optional<Boolean> present = java.util.Optional.of(true);
                assertSame(present, boundValue(type, present));
            });
        }

        checks.add(() -> {
            try (CassandraTestFixture fixture = new CassandraTestFixture()) {
                fixture.columns(DataType.blob(), "id", "data", "raw");
                final OptionalBlobDoc doc = new OptionalBlobDoc();
                doc.setId(1L);
                doc.setData(java.util.Optional.empty());
                doc.setRaw(com.landawn.abacus.util.u.Nullable.empty());
                fixture.executor.insert(doc);
                assertArrayEquals(new Object[] { 1L, null, null }, fixture.values.get(0));
            }
        });

        assertAll(checks);
    }

    public static class OptionalBlobDoc {
        @com.landawn.abacus.annotation.Id
        private long id;
        private java.util.Optional<ByteBuffer> data;
        private com.landawn.abacus.util.u.Nullable<byte[]> raw;

        public long getId() {
            return id;
        }

        public void setId(final long id) {
            this.id = id;
        }

        public java.util.Optional<ByteBuffer> getData() {
            return data;
        }

        public void setData(final java.util.Optional<ByteBuffer> data) {
            this.data = data;
        }

        public com.landawn.abacus.util.u.Nullable<byte[]> getRaw() {
            return raw;
        }

        public void setRaw(final com.landawn.abacus.util.u.Nullable<byte[]> raw) {
            this.raw = raw;
        }
    }

    // ---- 2026-10-27 verify2 ----

    // Converting a value of an application class (to apply a registered converter) reopened the EMPTY-blob write for one without a
    // converter: N.convert base64-decodes its text into a buffer positioned at its limit, so a driver Duration ("1mo2d3ns"), a Guava
    // UnsignedLong ("1234") or a BSON ObjectId (24 hex digits) was bound to a blob or custom column as an EMPTY blob. Such a value is
    // now bound unchanged (the driver rejects it); a registered converter still applies, also for an empty blob (pinned).
    @Test
    void verify2_applicationValuesWithoutABlobFormAreBoundUnchanged() {
        example.cassandra.BlobFixtures.register();
        final List<Executable> checks = new ArrayList<>();

        for (final DataType type : new DataType[] { DataType.blob(), DataType.custom("com.example.BlobType") }) {
            for (final Object value : new Object[] { com.datastax.driver.core.Duration.newInstance(1, 2, 3),
                    com.google.common.primitives.UnsignedLong.valueOf(1234), new org.bson.types.ObjectId("0123456789abcdef01234567") }) {
                // A single-element list supplies the one positional value (a single bean-like parameter would be read as named values).
                checks.add(() -> assertSame(value, boundValue(type, List.of(value)), value + " -> " + type));
            }

            checks.add(() -> assertBuffer(BYTES, boundValue(type, List.of(new example.cassandra.BlobFixtures.BlobBox(BYTES)))));
            checks.add(() -> assertBuffer(new byte[0], boundValue(type, List.of(new example.cassandra.BlobFixtures.BlobBox(new byte[0])))));
        }

        assertAll(checks);
    }

    // ---- 2026-10-27 r2K ----

    // Branch coverage of the final blob-bind rules (second-pass review). The tests above cover the java.util.Optional, OptionalInt,
    // u.Optional, u.Nullable, u.OptionalInt and Holder branches of wrappedValue; here every other wrapper kind (empty: NULL; present
    // without a blob form: the wrapper itself), nested and converter-backed content, invalid base64 content (IllegalArgumentException),
    // and the built-in-class gate for JDK/abacus values and arrays whose text is valid base64 ("1234": an EMPTY blob on HEAD), next to
    // a third-party enum whose name is valid base64, which is converted and then kept by the position guard.
    @Test
    void r2K_everyValueWrapperAndBuiltInClassFollowsTheBlobBindRules() {
        example.cassandra.BlobFixtures.register();
        final List<Executable> checks = new ArrayList<>();

        for (final DataType type : new DataType[] { DataType.blob(), DataType.custom("com.example.BlobType") }) {
            for (final Object empty : new Object[] { java.util.OptionalLong.empty(), java.util.OptionalDouble.empty(),
                    com.landawn.abacus.util.u.OptionalBoolean.empty(), com.landawn.abacus.util.u.OptionalChar.empty(),
                    com.landawn.abacus.util.u.OptionalByte.empty(), com.landawn.abacus.util.u.OptionalShort.empty(),
                    com.landawn.abacus.util.u.OptionalLong.empty(), com.landawn.abacus.util.u.OptionalFloat.empty(),
                    com.landawn.abacus.util.u.OptionalDouble.empty(), java.util.Optional.of(java.util.Optional.empty()) }) {
                checks.add(
                        () -> org.junit.jupiter.api.Assertions.assertNull(boundValue(type, java.util.Collections.singletonList(empty)), empty + " -> " + type));
            }

            for (final Object present : new Object[] { java.util.OptionalLong.of(1234), java.util.OptionalDouble.of(1.5),
                    com.landawn.abacus.util.u.OptionalBoolean.of(true), com.landawn.abacus.util.u.OptionalLong.of(1234),
                    java.util.Optional.of(com.google.common.primitives.UnsignedLong.valueOf(1234)) }) {
                checks.add(() -> assertSame(present, boundValue(type, java.util.Collections.singletonList(present)), present + " -> " + type));
            }

            checks.add(() -> assertBuffer(BYTES,
                    boundValue(type, java.util.Collections.singletonList(java.util.Optional.of(java.util.Optional.of(ByteBuffer.wrap(BYTES)))))));
            checks.add(() -> assertBuffer(BYTES,
                    boundValue(type, java.util.Collections.singletonList(java.util.Optional.of(new example.cassandra.BlobFixtures.BlobBox(BYTES))))));
            checks.add(() -> assertBuffer(BYTES, boundValue(type, java.util.Collections.singletonList(com.landawn.abacus.util.Holder.of(BYTES)))));
            checks.add(() -> {
                final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                        () -> boundValue(type, java.util.Collections.singletonList(com.landawn.abacus.util.u.Nullable.of("not base64!"))));
                assertTrue(e.getMessage().contains("parameter 'v' (index 0)") && e.getMessage().contains("not valid base64"), e.getMessage());
            });

            for (final Object value : new Object[] { new BigDecimal("1234"), new java.util.concurrent.atomic.AtomicLong(1234),
                    com.landawn.abacus.util.MutableInt.of(1234), Thread.State.NEW, new int[] { 1 }, new Integer[] { 1 },
                    com.datastax.driver.core.ConsistencyLevel.ANY }) {
                checks.add(() -> assertSame(value, boundValue(type, java.util.Collections.singletonList(value)), value + " -> " + type));
            }
        }

        assertAll(checks);
    }

    // Branch coverage of the final blob-read rule (convertValue through toEntity and a single-value target): an array whose component
    // type the buffer is (Object[]) holds the buffer, a type the buffer is (Comparable) keeps it, another type that a String is
    // (Serializable) gets the base64 text, and a boxed byte array, an enum or a bean has no blob form (IllegalArgumentException).
    @Test
    void r2K_blobReadTargetsFollowTheFinalRules() {
        assertAll(
                () -> assertArrayEquals(new Object[] { ByteBuffer.wrap(BYTES) }, CassandraExecutor.toEntity(blobRow("objects"), R2KBlobTargets.class).objects),
                () -> assertEquals(ByteBuffer.wrap(BYTES), CassandraExecutor.toEntity(blobRow("comparable"), R2KBlobTargets.class).comparable),
                () -> assertEquals(BASE64, CassandraExecutor.toEntity(blobRow("serializable"), R2KBlobTargets.class).serializable),
                () -> assertEquals(BASE64, CassandraExecutor.toList(rows(DataType.blob(), ByteBuffer.wrap(BYTES)), java.io.Serializable.class).get(0)),
                () -> assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toEntity(blobRow("boxedBytes"), R2KBlobTargets.class)),
                () -> assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toEntity(blobRow("state"), R2KBlobTargets.class)),
                () -> assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toEntity(blobRow("person"), R2KBlobTargets.class)));
    }

    public static class R2KBlobTargets {
        public Object[] objects;
        public Comparable<?> comparable;
        public java.io.Serializable serializable;
        public Byte[] boxedBytes;
        public Thread.State state;
        public Person person;
    }

    // A Type that TypeFactory.registerType builds from to/from-string functions is an anonymous TypeFactory class, which converts a
    // value from its text only (the empty base64 text before a driver blob's position): HEAD read a blob into such a class as the
    // value of "" (TextBox[text=]), silently. isBuiltinType counts it as an abacus Type, so the read is rejected; a Type class of
    // the application's own (TypedBox) still converts the buffer (pinned above).
    @Test
    void r2K_aBlobReadIntoAClassWithAFunctionBuiltTypeIsRejected() {
        example.cassandra.BlobFixtures.register();

        assertAll(
                () -> assertTrue(N.typeOf(example.cassandra.BlobFixtures.TextBox.class).getClass().getName().startsWith("com.landawn.abacus.type."),
                        N.typeOf(example.cassandra.BlobFixtures.TextBox.class).getClass().getName()),
                () -> assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toEntity(blobRow("box"), TextBoxHolder.class)),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> CassandraExecutor.toList(rows(DataType.blob(), ByteBuffer.wrap(BYTES)), example.cassandra.BlobFixtures.TextBox.class)));
    }

    // The UDT TIMESTAMP-field fallback converts a value without a timestamp codec with convertValue(value, Date.class): a ByteBuffer
    // there was converted from its empty text into null, so the field was written as NULL (HEAD). The blob rule rejects the
    // conversion, so the driver's lookup failure is thrown with the IllegalArgumentException suppressed, as for any other value
    // the Date conversion rejects.
    @Test
    void r2K_aBufferForATimestampUdtFieldKeepsTheLookupFailure() throws Exception {
        final CassandraExecutor.UDTCodec<Map> mapCodec = CassandraExecutor.UDTCodec.create(userType(new CodecRegistry(), "ts", DataType.timestamp()),
                Map.class);

        final com.datastax.driver.core.exceptions.CodecNotFoundException rejected = assertThrows(
                com.datastax.driver.core.exceptions.CodecNotFoundException.class,
                () -> mapCodec.serialize(Map.of("ts", ByteBuffer.wrap(BYTES)), ProtocolVersion.V4));

        assertAll(() -> assertEquals(DataType.timestamp(), rejected.getCqlType()), () -> assertEquals(1, rejected.getSuppressed().length),
                () -> assertInstanceOf(IllegalArgumentException.class, rejected.getSuppressed()[0]),
                () -> assertTrue(rejected.getSuppressed()[0].getMessage().contains("Cannot convert a blob"), rejected.getSuppressed()[0].getMessage()));
    }

    public static class TextBoxHolder {
        public example.cassandra.BlobFixtures.TextBox box;
    }

    // ---- 2026-10-27 fixD ----

    // fixD (decision G): the blob-column bind rule binds a present Optional-like value's content (an Optional<ByteBuffer>, a
    // Nullable<byte[]>, a Holder<ByteBuffer>, the base64 text of an Optional<String>), but a blob read into such a property was
    // rejected with IllegalArgumentException (HEAD: an EMPTY value), so an entity written through that rule could not be read back.
    // The blob is now converted to the wrapper's declared element type by the top-level blob rules and wrapped; NULL stays empty.
    // Same rule as the 4.x executor.
    @Test
    void fixD_optionalLikePropertiesReadBackTheBlobTheirValueWasBoundFrom() {
        final String[] names = { "id", "data", "bytes", "holder", "text" };
        final Object[] bound;

        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.columns(DataType.blob(), names);
            final FixDOptionalBlobs entity = new FixDOptionalBlobs();
            entity.setId(1L);
            entity.setData(java.util.Optional.of(ByteBuffer.wrap(BYTES)));
            entity.setBytes(com.landawn.abacus.util.u.Nullable.of(BYTES));
            entity.setHolder(com.landawn.abacus.util.Holder.of(ByteBuffer.wrap(BYTES)));
            entity.setText(com.landawn.abacus.util.u.Optional.of(BASE64));
            fixture.executor.insert(entity);
            bound = fixture.values.get(0);
        }

        final FixDOptionalBlobs read = CassandraExecutor.toEntity(rows(names, DataType.blob(), new Object[][] { bound }).one(), FixDOptionalBlobs.class);
        final FixDOptionalBlobs nulls = CassandraExecutor.toEntity(rows(names, DataType.blob(), new Object[][] { { 1L, null, null, null, null } }).one(),
                FixDOptionalBlobs.class);

        assertAll(() -> assertEquals(1L, bound[0]), () -> assertBuffer(BYTES, bound[1]), () -> assertBuffer(BYTES, bound[2]),
                () -> assertBuffer(BYTES, bound[3]), () -> assertBuffer(BYTES, bound[4]),
                // Read back: the same content, each in its declared element type.
                () -> assertBuffer(BYTES, read.getData().get()), () -> assertArrayEquals(BYTES, read.getBytes().get()),
                () -> assertBuffer(BYTES, read.getHolder().value()), () -> assertEquals(com.landawn.abacus.util.u.Optional.of(BASE64), read.getText()),
                // Pins: a NULL column is an empty wrapper (a Holder of null), as before.
                () -> assertEquals(java.util.Optional.empty(), nulls.getData()),
                () -> assertEquals(com.landawn.abacus.util.u.Nullable.empty(), nulls.getBytes()), () -> assertEquals(null, nulls.getHolder().value()),
                () -> assertEquals(com.landawn.abacus.util.u.Optional.empty(), nulls.getText()));
    }

    // fixD (decision G): every read path that knows the declared wrapper type converts the blob to that element type: a dotted column, a
    // UDT field read into a bean, a collection element, a nested wrapper, extractData (the getter's declared type) and a wrapper of a
    // collection or a StringBuilder. A single-value target without a declared element type (Optional.class, Nullable.class, ...) holds
    // the buffer. An element type without a blob form (Optional<Long>, List<Optional<Long>>) and a primitive Optional (OptionalInt,
    // u.OptionalLong, which hold no object) stay rejected. Same rule as the 4.x executor.
    @Test
    void fixD_everyReadPathWrapsTheBlobConvertedToTheDeclaredElementType() throws Exception {
        final ByteBuffer buffer = ByteBuffer.wrap(BYTES);
        final UserType blobType = userType(new CodecRegistry(), "v", DataType.blob());
        final CassandraExecutor.UDTCodec<Map> mapCodec = CassandraExecutor.UDTCodec.create(blobType, Map.class);
        final CassandraExecutor.UDTCodec<FixDUdtBytes> udtCodec = CassandraExecutor.UDTCodec.create(blobType, FixDUdtBytes.class);
        final List<Executable> checks = new ArrayList<>();

        checks.add(
                () -> assertBuffer(BYTES, CassandraExecutor.toEntity(rows(new String[] { "nested.data" }, DataType.blob(), new Object[][] { { buffer } }).one(),
                        FixDOptionalBlobsHolder.class).nested.getData().get()));
        checks.add(
                () -> assertArrayEquals(BYTES, udtCodec.deserialize(mapCodec.serialize(Map.of("v", BYTES), ProtocolVersion.V4), ProtocolVersion.V4).v.get()));
        checks.add(() -> {
            final FixDWrapperElements e = CassandraExecutor
                    .toEntity(
                            rows(new String[] { "buffers", "texts" }, DataType.list(DataType.blob()),
                                    new Object[][] { { new ArrayList<>(List.of(buffer)), new ArrayList<>(List.of(buffer)) } }).one(),
                            FixDWrapperElements.class);
            assertSame(buffer, e.buffers.get(0).get());
            assertEquals(com.landawn.abacus.util.u.Nullable.of(BASE64), e.texts.get(0));
        });
        checks.add(() -> {
            final FixDNestedWrappers n = CassandraExecutor.toEntity(
                    rows(new String[] { "nested", "list", "builder" }, DataType.blob(), new Object[][] { { buffer, buffer, buffer } }).one(),
                    FixDNestedWrappers.class);
            assertArrayEquals(BYTES, n.nested.get().get());
            assertArrayEquals(BYTES, n.list.get().get(0));
            assertInstanceOf(StringBuilder.class, n.builder.get());
            assertEquals(BASE64, n.builder.get().toString());
        });
        checks.add(() -> assertEquals(List.of(java.util.Optional.of(BASE64)),
                CassandraExecutor.extractData(rows(DataType.blob(), buffer), FixDOptionalTextBean.class).getColumn("v")));
        // No declared element type: the wrapper holds the buffer itself.
        checks.add(() -> assertSame(buffer, CassandraExecutor.toList(rows(DataType.blob(), buffer), java.util.Optional.class).get(0).get()));
        checks.add(() -> assertSame(buffer, CassandraExecutor.toList(rows(DataType.blob(), buffer), com.landawn.abacus.util.u.Optional.class).get(0).get()));
        checks.add(() -> assertSame(buffer, CassandraExecutor.toList(rows(DataType.blob(), buffer), com.landawn.abacus.util.u.Nullable.class).get(0).get()));
        checks.add(() -> assertSame(buffer, CassandraExecutor.toList(rows(DataType.blob(), buffer), com.landawn.abacus.util.Holder.class).get(0).value()));
        checks.add(() -> {
            try (CassandraTestFixture fixture = new CassandraTestFixture()) {
                when(fixture.session.execute(any(Statement.class))).thenAnswer(invocation -> rows(DataType.blob(), buffer));
                assertSame(buffer, fixture.executor.queryForSingleValue(java.util.Optional.class, QUERY).get().get());
            }
        });

        // Rejections: no blob form for the element type, or a primitive Optional.
        for (final String property : new String[] { "longs", "optionalInt", "optionalLong" }) {
            checks.add(() -> assertBlobRejected(() -> CassandraExecutor.toEntity(blobRow(property), FixDRejected.class), property));
        }

        checks.add(() -> assertBlobRejected(() -> CassandraExecutor.toEntity(
                rows(new String[] { "longList" }, DataType.list(DataType.blob()), new Object[][] { { new ArrayList<>(List.of(buffer)) } }).one(),
                FixDRejected.class), "List<Optional<Long>>"));
        checks.add(() -> assertBlobRejected(() -> CassandraExecutor.extractData(rows(DataType.blob(), buffer), FixDOptionalLongBean.class), "Dataset"));
        checks.add(() -> assertBlobRejected(() -> CassandraExecutor.toList(rows(DataType.blob(), buffer), java.util.OptionalInt.class), "OptionalInt.class"));
        assertAll(checks);
    }

    // fixD (finding D): the blob read rule gave the base64 text to a target that a String is an instance of (CharSequence,
    // Serializable, ...), but rejected a StringBuilder or StringBuffer, which are CharSequences too (HEAD: an empty one). They now get
    // the base64 text in their own type; a CharSequence class abacus does not build from text (CharBuffer) stays rejected. A target the
    // buffer itself is (Comparable, Object) keeps the buffer. Same rule as the 4.x executor.
    @Test
    void fixD_aBlobReadIntoAStringBuilderOrStringBufferIsItsBase64Text() {
        final ByteBuffer buffer = ByteBuffer.wrap(BYTES);
        final List<Executable> checks = new ArrayList<>();

        checks.add(() -> {
            final StringBuilder builder = CassandraExecutor.toList(rows(DataType.blob(), buffer), StringBuilder.class).get(0);
            assertEquals(BASE64, builder.toString());
            assertEquals(0, buffer.position());
        });
        checks.add(() -> assertEquals(BASE64, CassandraExecutor.toList(rows(DataType.blob(), buffer), StringBuffer.class).get(0).toString()));
        checks.add(() -> {
            final FixDBuilders b = CassandraExecutor
                    .toEntity(rows(new String[] { "builder", "buffer" }, DataType.blob(), new Object[][] { { buffer, buffer } }).one(), FixDBuilders.class);
            assertEquals(BASE64, b.builder.toString());
            assertEquals(BASE64, b.buffer.toString());
        });
        checks.add(() -> {
            try (CassandraTestFixture fixture = new CassandraTestFixture()) {
                when(fixture.session.execute(any(Statement.class))).thenAnswer(invocation -> rows(DataType.blob(), buffer));
                assertEquals(BASE64, fixture.executor.queryForSingleValue(StringBuilder.class, QUERY).get().toString());
            }
        });
        // Pins: a String-satisfied target gets the base64 String; a type the buffer is keeps it; CharBuffer has no blob form.
        checks.add(() -> assertEquals(BASE64, CassandraExecutor.toList(rows(DataType.blob(), buffer), java.io.Serializable.class).get(0)));
        checks.add(() -> assertEquals(BASE64, CassandraExecutor.toList(rows(DataType.blob(), buffer), CharSequence.class).get(0)));
        checks.add(() -> assertSame(buffer, CassandraExecutor.toList(rows(DataType.blob(), buffer), Comparable.class).get(0)));
        checks.add(() -> assertBlobRejected(() -> CassandraExecutor.toList(rows(DataType.blob(), buffer), java.nio.CharBuffer.class), "CharBuffer"));
        assertAll(checks);
    }

    // vfixD: extractData took a wrapper property's element type from the getter's generic return type alone, which keeps the type
    // variable of a generic superclass (Optional<T> on a class extending a Base<String>) and has none for a raw Optional getter of an
    // Optional<String> field, so its Dataset column held an Optional of the buffer where toEntity holds the Optional<String> of the
    // base64 text (and an Optional<Long> declared through Base<Long> was not rejected). It now uses the property's type, as toEntity
    // does; a property whose type is of another class than its getter's (@Type("String") on an Optional getter) keeps the getter's
    // own type, so the column still holds values of the getter's class. Same rule as the 4.x executor.
    @Test
    void fixD_extractDataConvertsABlobForTheWrapperTypeThePropertyDeclares() {
        final ByteBuffer buffer = ByteBuffer.wrap(BYTES);
        final List<Executable> checks = new ArrayList<>();

        for (final Class<?> beanClass : new Class<?>[] { FixDGenericText.class, FixDRawGetter.class }) {
            checks.add(() -> assertEquals(List.of(java.util.Optional.of(BASE64)),
                    CassandraExecutor.extractData(rows(DataType.blob(), buffer), beanClass).getColumn("v"), beanClass.getSimpleName()));
        }

        checks.add(() -> assertBlobRejected(() -> CassandraExecutor.extractData(rows(DataType.blob(), buffer), FixDGenericLong.class),
                "Optional<Long> via Base<Long>"));
        // Pins: toEntity reads the same value; a getter whose property type is of another class keeps the getter's declared type.
        checks.add(() -> assertEquals(java.util.Optional.of(BASE64),
                CassandraExecutor.toEntity(rows(DataType.blob(), buffer).one(), FixDGenericText.class).getV()));
        checks.add(() -> assertEquals(List.of(java.util.Optional.of(BASE64)),
                CassandraExecutor.extractData(rows(DataType.blob(), buffer), FixDTypedGetter.class).getColumn("v")));
        assertAll(checks);
    }

    // sbfix (outside review, issue 2): a blob reads into a StringBuilder or StringBuffer as its base64 text (fixD), but binding one
    // back to a blob column (or a custom-type column, or a blob UDT field) left it unchanged, so the driver rejected it
    // (CodecNotFoundException): only a String was decoded. Such text is now decoded like a String, in a wrapper too, and text that
    // is not base64 is rejected the same way. A CharBuffer, which abacus does not build from text, still has no blob form. Same
    // rule as the 4.x executor.
    @Test
    void sbfix_aStringBuilderOrStringBufferBoundToABlobIsDecodedLikeAString() {
        final List<Executable> checks = new ArrayList<>();

        for (final DataType type : new DataType[] { DataType.blob(), DataType.custom("com.example.BlobType") }) {
            checks.add(() -> assertBuffer(BYTES, boundValue(type, new StringBuilder(BASE64))));
            checks.add(() -> assertBuffer(BYTES, boundValue(type, new StringBuffer(BASE64))));
            checks.add(() -> assertBuffer(new byte[0], boundValue(type, new StringBuilder())));
            checks.add(() -> assertBuffer(BYTES, boundValue(type, java.util.Collections.singletonList(java.util.Optional.of(new StringBuilder(BASE64))))));
            checks.add(() -> assertBuffer(BYTES,
                    boundValue(type, java.util.Collections.singletonList(com.landawn.abacus.util.u.Nullable.of(new StringBuffer(BASE64))))));
            checks.add(() -> assertBuffer(BYTES,
                    boundValue(type, java.util.Collections.singletonList(com.landawn.abacus.util.Holder.of(new StringBuilder(BASE64))))));
            checks.add(() -> assertBuffer(BYTES,
                    boundValue(type, java.util.Collections.singletonList(com.landawn.abacus.util.u.Optional.of(new StringBuffer(BASE64))))));
            // Pin: a CharBuffer is bound unchanged, in a wrapper too.
            checks.add(() -> {
                final java.nio.CharBuffer chars = java.nio.CharBuffer.wrap(BASE64);
                assertSame(chars, boundValue(type, chars), type.toString());
            });
            checks.add(() -> {
                final java.util.Optional<java.nio.CharBuffer> wrapped = java.util.Optional.of(java.nio.CharBuffer.wrap(BASE64));
                assertSame(wrapped, boundValue(type, java.util.Collections.singletonList(wrapped)), type.toString());
            });

            // Text that is not base64 is rejected with the parameter named, bare or in a wrapper.
            for (final Object invalid : new Object[] { new StringBuilder("not base64!"), new StringBuffer("AQ=D"),
                    java.util.Optional.of(new StringBuilder("#")), com.landawn.abacus.util.Holder.of(new StringBuffer("#")) }) {
                checks.add(() -> {
                    final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                            () -> boundValue(type, java.util.Collections.singletonList(invalid)), invalid + " -> " + type);
                    assertTrue(e.getMessage().contains("parameter 'v' (index 0)"), e.getMessage());
                    assertTrue(e.getMessage().contains("not valid base64"), e.getMessage());
                });
            }
        }

        // Positional, collection and named parameters all decode it; the value itself is not changed.
        checks.add(() -> {
            final StringBuilder builder = new StringBuilder(BASE64);

            try (CassandraTestFixture fixture = new CassandraTestFixture()) {
                fixture.columns(DataType.blob(), "v");
                fixture.executor.execute(UPDATE, builder);
                fixture.executor.execute(UPDATE, List.of(new StringBuffer(BASE64)));
                fixture.executor.execute("UPDATE t SET v = :v WHERE id = 1", N.asMap("v", new StringBuilder(BASE64)));

                assertEquals(3, fixture.values.size());
                fixture.values.forEach(bind -> assertBuffer(BYTES, bind[0]));
            }

            assertEquals(BASE64, builder.toString());
        });

        // Round trip: a blob read into StringBuilder/StringBuffer properties binds back to the same bytes on insert.
        checks.add(() -> {
            final ByteBuffer buffer = ByteBuffer.wrap(BYTES);
            final SbFixTextBlobs entity = CassandraExecutor
                    .toEntity(rows(new String[] { "builder", "buffer" }, DataType.blob(), new Object[][] { { buffer, buffer } }).one(), SbFixTextBlobs.class);
            assertEquals(BASE64, entity.getBuilder().toString());
            assertEquals(BASE64, entity.getBuffer().toString());
            entity.setId(1L);

            try (CassandraTestFixture fixture = new CassandraTestFixture()) {
                fixture.columns(DataType.blob(), "id", "builder", "buffer");
                fixture.executor.insert(entity);

                final Object[] bind = fixture.values.get(0);
                assertEquals(1L, bind[0]);
                assertBuffer(BYTES, bind[1]);
                assertBuffer(BYTES, bind[2]);
            }
        });
        assertAll(checks);
    }

    // sbfix: a StringBuilder or StringBuffer written to a blob UDT field is decoded as a String is (it failed with
    // CodecNotFoundException); invalid text is rejected naming the field.
    @Test
    void sbfix_aStringBuilderOrStringBufferWrittenToABlobUdtFieldIsDecoded() throws Exception {
        final UserType blobType = userType(new CodecRegistry(), "v", DataType.blob());
        final CassandraExecutor.UDTCodec<Map> mapCodec = CassandraExecutor.UDTCodec.create(blobType, Map.class);
        final CassandraExecutor.UDTCodec<SbFixBuilderField> beanCodec = CassandraExecutor.UDTCodec.create(blobType, SbFixBuilderField.class);

        assertAll(
                () -> assertEquals(ByteBuffer.wrap(BYTES),
                        mapCodec.deserialize(mapCodec.serialize(Map.of("v", new StringBuilder(BASE64)), ProtocolVersion.V4), ProtocolVersion.V4).get("v")),
                () -> assertEquals(ByteBuffer.wrap(BYTES),
                        mapCodec.deserialize(mapCodec.serialize(Map.of("v", new StringBuffer(BASE64)), ProtocolVersion.V4), ProtocolVersion.V4).get("v")),
                () -> {
                    final SbFixBuilderField bean = new SbFixBuilderField();
                    bean.setV(new StringBuilder(BASE64));
                    assertEquals(ByteBuffer.wrap(BYTES), mapCodec.deserialize(beanCodec.serialize(bean, ProtocolVersion.V4), ProtocolVersion.V4).get("v"));
                    // Round trip: the field reads back into the StringBuilder property as the same text.
                    assertEquals(BASE64, beanCodec.deserialize(beanCodec.serialize(bean, ProtocolVersion.V4), ProtocolVersion.V4).getV().toString());
                }, () -> {
                    final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                            () -> mapCodec.serialize(Map.of("v", new StringBuffer("not base64!")), ProtocolVersion.V4));
                    assertTrue(e.getMessage().contains("UDT field 'v' (index 0)"), e.getMessage());
                    assertTrue(e.getMessage().contains("not valid base64"), e.getMessage());
                },
                // Pin: a CharBuffer still has no blob form.
                () -> assertThrows(com.datastax.driver.core.exceptions.CodecNotFoundException.class,
                        () -> mapCodec.serialize(Map.of("v", java.nio.CharBuffer.wrap(BASE64)), ProtocolVersion.V4)));
    }

    // sbfix (outside review): the rejection message for a blob read into a target without a blob form claimed a complete list of
    // the targets a blob converts to ("converts only to byte[], ByteBuffer or String"; then a longer list that still missed
    // collections, wrappers of containers, Object/Comparable and application types). It now names the rejected type and gives
    // examples without claiming the list is complete; the class documentation has the full rule. Same rule as the 4.x executor.
    @Test
    void sbfix_theBlobRejectionMessageNamesTheTypeAndGivesExamples() {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> CassandraExecutor.toList(rows(DataType.blob(), ByteBuffer.wrap(BYTES)), Long.class));

        assertAll(
                () -> assertTrue(e.getMessage().contains("Cannot convert a blob (ByteBuffer of 3 bytes) to java.lang.Long: that type has no blob form"),
                        e.getMessage()),
                () -> assertTrue(e.getMessage()
                        .contains("for example, as byte[], ByteBuffer or its base64 text, alone or in a collection or an Optional," + " Nullable or Holder"),
                        e.getMessage()),
                () -> org.junit.jupiter.api.Assertions.assertFalse(e.getMessage().contains("only"), e.getMessage()));
    }

    public static class SbFixTextBlobs {
        private long id;
        private StringBuilder builder;
        private StringBuffer buffer;

        public long getId() {
            return id;
        }

        public void setId(final long id) {
            this.id = id;
        }

        public StringBuilder getBuilder() {
            return builder;
        }

        public void setBuilder(final StringBuilder builder) {
            this.builder = builder;
        }

        public StringBuffer getBuffer() {
            return buffer;
        }

        public void setBuffer(final StringBuffer buffer) {
            this.buffer = buffer;
        }
    }

    public static class SbFixBuilderField {
        private StringBuilder v;

        public StringBuilder getV() {
            return v;
        }

        public void setV(final StringBuilder v) {
            this.v = v;
        }
    }

    // wrapcodec (outside review): codec acceptance was checked only on the wrapper, and a present Optional/Nullable/Holder was then
    // converted by abacus rules alone. An Optional<WrapPayload> was therefore bound as the wrapper to a blob or custom-type column (the
    // driver has no codec for it: CodecNotFoundException) although a registered TypeCodec<WrapPayload> binds the bare value, and for
    // a text column it was silently stored as abacus's text of the value (JSON: {"n": 7}) instead of the codec's ("P:7"). The
    // content is now offered to the codecs first, for every column type and through nested wrappers. Same rule as the 4.x executor.
    @Test
    void wrapcodec_aWrappedValueBindsThroughTheCodecRegisteredForItsContent() {
        final DataType custom = DataType.custom("com.example.PayloadType");
        final CodecRegistry registry = payloadRegistry(custom);
        final WrapPayload payload = new WrapPayload(7);
        final List<Executable> checks = new ArrayList<>();

        for (final DataType type : new DataType[] { DataType.blob(), custom, DataType.text() }) {
            // Pin: the bare value was always bound as is, for its codec.
            checks.add(() -> assertSame(payload, boundValue(registry, type, payload), "bare -> " + type));

            for (final Object wrapped : new Object[] { java.util.Optional.of(payload), com.landawn.abacus.util.u.Optional.of(payload),
                    com.landawn.abacus.util.u.Nullable.of(payload), com.landawn.abacus.util.Holder.of(payload),
                    java.util.Optional.of(com.landawn.abacus.util.u.Nullable.of(payload)) }) {
                checks.add(() -> assertSame(payload, boundValue(registry, type, wrapped), wrapped + " -> " + type));
            }
        }

        // The codec stores its own form, which is not abacus's text of the value (what the wrapped value was bound as before).
        checks.add(() -> assertEquals("P:7",
                TypeCodec.varchar().deserialize(registry.codecFor(DataType.text(), payload).serialize(payload, ProtocolVersion.V4), ProtocolVersion.V4)));
        checks.add(() -> assertEquals("{\"n\": 7}", N.convert(payload, String.class)));
        // An entity insert binds an Optional<WrapPayload> property's content for its codec.
        checks.add(() -> {
            try (CassandraTestFixture fixture = new CassandraTestFixture(registry)) {
                fixture.columns(custom, "id", "payload");
                final WrapPayloadEntity entity = new WrapPayloadEntity();
                entity.setId(1L);
                entity.setPayload(java.util.Optional.of(payload));
                fixture.executor.insert(entity);

                assertEquals(1L, fixture.values.get(0)[0]);
                assertSame(payload, fixture.values.get(0)[1]);
            }
        });
        // Pins: without a codec for the content, the wrapped value follows the bare value's rules: kept in its wrapper for a blob
        // column (the driver rejects it), converted by N.convert for a text column; an empty wrapper still binds NULL.
        checks.add(() -> {
            final java.util.Optional<WrapPayload> wrapped = java.util.Optional.of(payload);
            assertSame(wrapped, boundValue(DataType.blob(), java.util.Collections.singletonList(wrapped)));
        });
        checks.add(() -> assertEquals("{\"n\": 7}", boundValue(DataType.text(), java.util.Collections.singletonList(java.util.Optional.of(payload)))));
        checks.add(() -> org.junit.jupiter.api.Assertions.assertNull(boundValue(registry, custom, java.util.Optional.empty())));
        checks.add(() -> org.junit.jupiter.api.Assertions.assertNull(boundValue(registry, DataType.text(), java.util.Optional.empty())));
        assertAll(checks);
    }

    // wrapcodec: a codec registered for the wrapper class itself is still asked first, so it keeps receiving the wrapper.
    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    void wrapcodec_aCodecRegisteredForTheWrapperItselfStillTakesPrecedence() {
        final CodecRegistry registry = payloadRegistry(DataType.custom("com.example.PayloadType"));
        registry.register(new TypeCodec<java.util.Optional>(DataType.text(), java.util.Optional.class) {
            @Override
            public ByteBuffer serialize(final java.util.Optional value, final ProtocolVersion protocolVersion) {
                return value == null ? null : TypeCodec.varchar().serialize("OPT:" + value.orElse(null), protocolVersion);
            }

            @Override
            public java.util.Optional deserialize(final ByteBuffer bytes, final ProtocolVersion protocolVersion) {
                return java.util.Optional.ofNullable(TypeCodec.varchar().deserialize(bytes, protocolVersion));
            }

            @Override
            public java.util.Optional parse(final String value) {
                return java.util.Optional.ofNullable(value);
            }

            @Override
            public String format(final java.util.Optional value) {
                return String.valueOf(value);
            }
        });
        final java.util.Optional<WrapPayload> wrapped = java.util.Optional.of(new WrapPayload(7));

        assertSame(wrapped, boundValue(registry, DataType.text(), wrapped));
    }

    // wrapcodec: a present wrapper's content goes through the same conversion rules as a bare value, so a wrapped number that the
    // column would change is rejected (Optional.of(1.5) was bound to an int column as 1; a bare 1.5 was already rejected), and a
    // wrapped date/time value for a time column is its time of day.
    @Test
    void wrapcodec_aWrappedValueIsConvertedByTheBareValueRules() {
        final List<Executable> checks = new ArrayList<>();

        for (final Object[] lossy : new Object[][] { { DataType.cint(), java.util.Optional.of(1.5), "Double value 1.5" },
                { DataType.cint(), com.landawn.abacus.util.u.Nullable.of(com.landawn.abacus.util.Fraction.of(3, 2)), "Fraction value 3/2" },
                { DataType.bigint(), java.util.Optional.of(java.util.Optional.of(2.5)), "Double value 2.5" },
                { DataType.cfloat(), com.landawn.abacus.util.Holder.of(1e300), "Double value 1.0E300" } }) {
            checks.add(() -> {
                final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                        () -> boundValue((DataType) lossy[0], java.util.Collections.singletonList(lossy[1])), lossy[1].toString());
                assertTrue(e.getMessage().contains("Cannot bind " + lossy[2]), e.getMessage());
                assertTrue(e.getMessage().contains("parameter 'v' (index 0)"), e.getMessage());
            });
        }

        final long nanos = java.time.LocalTime.of(1, 2, 3).toNanoOfDay();
        checks.add(() -> assertEquals(nanos,
                boundValue(DataType.time(), java.util.Collections.singletonList(java.util.Optional.of(java.time.LocalTime.of(1, 2, 3))))));
        checks.add(() -> assertEquals(nanos, boundValue(DataType.time(),
                java.util.Collections.singletonList(com.landawn.abacus.util.Holder.of(java.time.LocalDateTime.of(2020, 1, 1, 1, 2, 3))))));
        // Pins: content that converts exactly, or is of the column's type, binds as before; content without a form for the column
        // keeps its wrapper (the driver rejects it); an empty wrapper is NULL for a scalar column.
        checks.add(() -> assertEquals(1.5, boundValue(DataType.cdouble(), java.util.Collections.singletonList(java.util.Optional.of(1.5)))));
        checks.add(() -> assertEquals(5L, boundValue(DataType.bigint(), java.util.Collections.singletonList(java.util.Optional.of(5)))));
        checks.add(() -> assertEquals(5, boundValue(DataType.cint(), java.util.Collections.singletonList(java.util.OptionalInt.of(5)))));
        checks.add(() -> assertEquals("x", boundValue(DataType.text(), java.util.Collections.singletonList(com.landawn.abacus.util.Holder.of("x")))));
        checks.add(() -> {
            final java.util.Optional<String> text = java.util.Optional.of("abc");
            assertSame(text, boundValue(DataType.cint(), java.util.Collections.singletonList(text)));
        });
        checks.add(() -> org.junit.jupiter.api.Assertions
                .assertNull(boundValue(DataType.cint(), java.util.Collections.singletonList(java.util.Optional.empty()))));
        assertAll(checks);
    }

    // A registry with a WrapPayload codec for text (written "P:<n>"), blob and the given custom type (one byte, <n>).
    private static CodecRegistry payloadRegistry(final DataType custom) {
        return new CodecRegistry().register(payloadCodec(DataType.text()), payloadCodec(DataType.blob()), payloadCodec(custom));
    }

    private static TypeCodec<WrapPayload> payloadCodec(final DataType cqlType) {
        final boolean text = cqlType.equals(DataType.text());

        return new TypeCodec<>(cqlType, WrapPayload.class) {
            @Override
            public ByteBuffer serialize(final WrapPayload value, final ProtocolVersion protocolVersion) {
                if (value == null) {
                    return null;
                }

                return text ? TypeCodec.varchar().serialize("P:" + value.n(), protocolVersion) : ByteBuffer.wrap(new byte[] { (byte) value.n() });
            }

            @Override
            public WrapPayload deserialize(final ByteBuffer bytes, final ProtocolVersion protocolVersion) {
                if (bytes == null) {
                    return null;
                }

                return new WrapPayload(text ? Integer.parseInt(TypeCodec.varchar().deserialize(bytes, protocolVersion).substring(2)) : bytes.duplicate().get());
            }

            @Override
            public WrapPayload parse(final String value) {
                return value == null ? null : new WrapPayload(Integer.parseInt(value));
            }

            @Override
            public String format(final WrapPayload value) {
                return value == null ? "NULL" : String.valueOf(value.n());
            }
        };
    }

    // The value bound for marker 'v' with the given codec registry (a single-element list, so a wrapper is never a bean container).
    private static Object boundValue(final CodecRegistry registry, final DataType type, final Object value) {
        try (CassandraTestFixture fixture = new CassandraTestFixture(registry)) {
            fixture.columns(type, "v");
            fixture.executor.execute(UPDATE, java.util.Collections.singletonList(value));
            final Object[] bind = fixture.values.get(0);
            assertEquals(1, bind.length);
            return bind[0];
        }
    }

    // An application value class that a registered codec maps to a column (abacus's own text of it is JSON).
    public static final class WrapPayload {
        private final int n;

        public WrapPayload(final int n) {
            this.n = n;
        }

        public int n() {
            return n;
        }

        @Override
        public String toString() {
            return "WrapPayload(" + n + ")";
        }
    }

    public static class WrapPayloadEntity {
        private long id;
        private java.util.Optional<WrapPayload> payload = java.util.Optional.empty();

        public long getId() {
            return id;
        }

        public void setId(final long id) {
            this.id = id;
        }

        public java.util.Optional<WrapPayload> getPayload() {
            return payload;
        }

        public void setPayload(final java.util.Optional<WrapPayload> payload) {
            this.payload = payload;
        }
    }

    private static void assertBlobRejected(final Executable executable, final String message) {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, executable, message);
        assertTrue(e.getMessage().contains("Cannot convert a blob"), message + ": " + e.getMessage());
    }

    public static class FixDOptionalBlobs {
        @com.landawn.abacus.annotation.Id
        private long id;
        private java.util.Optional<ByteBuffer> data = java.util.Optional.empty();
        private com.landawn.abacus.util.u.Nullable<byte[]> bytes = com.landawn.abacus.util.u.Nullable.empty();
        private com.landawn.abacus.util.Holder<ByteBuffer> holder = com.landawn.abacus.util.Holder.of(null);
        private com.landawn.abacus.util.u.Optional<String> text = com.landawn.abacus.util.u.Optional.empty();

        public long getId() {
            return id;
        }

        public void setId(final long id) {
            this.id = id;
        }

        public java.util.Optional<ByteBuffer> getData() {
            return data;
        }

        public void setData(final java.util.Optional<ByteBuffer> data) {
            this.data = data;
        }

        public com.landawn.abacus.util.u.Nullable<byte[]> getBytes() {
            return bytes;
        }

        public void setBytes(final com.landawn.abacus.util.u.Nullable<byte[]> bytes) {
            this.bytes = bytes;
        }

        public com.landawn.abacus.util.Holder<ByteBuffer> getHolder() {
            return holder;
        }

        public void setHolder(final com.landawn.abacus.util.Holder<ByteBuffer> holder) {
            this.holder = holder;
        }

        public com.landawn.abacus.util.u.Optional<String> getText() {
            return text;
        }

        public void setText(final com.landawn.abacus.util.u.Optional<String> text) {
            this.text = text;
        }
    }

    public static class FixDOptionalBlobsHolder {
        public FixDOptionalBlobs nested = new FixDOptionalBlobs();
    }

    public static class FixDUdtBytes {
        public java.util.Optional<byte[]> v;
    }

    public static class FixDWrapperElements {
        public List<java.util.Optional<ByteBuffer>> buffers;
        public List<com.landawn.abacus.util.u.Nullable<String>> texts;
    }

    public static class FixDNestedWrappers {
        public java.util.Optional<java.util.Optional<byte[]>> nested;
        public java.util.Optional<List<byte[]>> list;
        public java.util.Optional<StringBuilder> builder;
    }

    public static class FixDOptionalTextBean {
        private java.util.Optional<String> v;

        public java.util.Optional<String> getV() {
            return v;
        }

        public void setV(final java.util.Optional<String> v) {
            this.v = v;
        }
    }

    public static class FixDOptionalLongBean {
        private java.util.Optional<Long> v;

        public java.util.Optional<Long> getV() {
            return v;
        }

        public void setV(final java.util.Optional<Long> v) {
            this.v = v;
        }
    }

    public static class FixDRejected {
        public java.util.Optional<Long> longs;
        public java.util.OptionalInt optionalInt;
        public com.landawn.abacus.util.u.OptionalLong optionalLong;
        public List<java.util.Optional<Long>> longList;
    }

    public static class FixDBuilders {
        public StringBuilder builder;
        public StringBuffer buffer;
    }

    public static class FixDGenericBase<T> {
        private java.util.Optional<T> v;

        public java.util.Optional<T> getV() {
            return v;
        }

        public void setV(final java.util.Optional<T> v) {
            this.v = v;
        }
    }

    public static class FixDGenericText extends FixDGenericBase<String> {
    }

    public static class FixDGenericLong extends FixDGenericBase<Long> {
    }

    public static class FixDRawGetter {
        private java.util.Optional<String> v;

        @SuppressWarnings("rawtypes")
        public java.util.Optional getV() {
            return v;
        }

        public void setV(final java.util.Optional<String> v) {
            this.v = v;
        }
    }

    public static class FixDTypedGetter {
        private java.util.Optional<String> v;

        @com.landawn.abacus.annotation.Type("String")
        public java.util.Optional<String> getV() {
            return v;
        }

        public void setV(final java.util.Optional<String> v) {
            this.v = v;
        }
    }
}
