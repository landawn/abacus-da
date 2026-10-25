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
}
