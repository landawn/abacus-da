package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinition;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinitions;
import com.datastax.oss.driver.internal.core.cql.DefaultRow;
import com.datastax.oss.driver.internal.core.type.DefaultUserDefinedType;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.landawn.abacus.util.Fraction;
import com.landawn.abacus.util.MutableDouble;
import com.landawn.abacus.util.MutableFloat;
import com.landawn.abacus.util.N;

/**
 * Driver 4 conversions decided in the 2026-10-24 apply round (driver 3 twin:
 * {@code cassandra.v3.CassandraV3DeepReview2026BindBlobRowConversionTest}):
 * <ul>
 * <li>{@code N.registerConverter(Row.class, ...)} never fired (abacus looks converters up by class and superclass, not
 * interface), so {@code N.convert(row, Bean.class)} threw {@code ParsingException} for the driver's {@code DefaultRow};</li>
 * <li>a fractional value bound to an integer column was truncated ({@code 1.5} to {@code 1}), and a finite number beyond the
 * float range bound to a {@code float} column became {@code Infinity};</li>
 * <li>base64 text bound to a {@code blob} column bound an EMPTY blob, and a blob read into a {@code String} was {@code ""}.</li>
 * </ul>
 * Driver statements are mocked; rows for the {@code N.convert} tests are real driver rows; conversion is real.
 */
class CassandraExecutorDeepReview2026BindBlobRowConversionTest {

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
    void nConvertMapsADriverRowAsToListDoes() {
        final Row row = driverRow(new String[] { "name", "age" }, new int[] { ProtocolConstants.DataType.VARCHAR, ProtocolConstants.DataType.INT },
                TypeCodecs.TEXT.encode("Ada", ProtocolVersion.DEFAULT), TypeCodecs.INT.encode(36, ProtocolVersion.DEFAULT));
        final Row oneColumn = driverRow(new String[] { "age" }, new int[] { ProtocolConstants.DataType.INT },
                TypeCodecs.INT.encode(36, ProtocolVersion.DEFAULT));

        assertAll(() -> assertInstanceOf(DefaultRow.class, row), () -> {
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
    void nConvertToARowSupertypeAndTheExecutorsRowMappingAreUnaffected() {
        final Row row = driverRow(new String[] { "name", "age" }, new int[] { ProtocolConstants.DataType.VARCHAR, ProtocolConstants.DataType.INT },
                TypeCodecs.TEXT.encode("Ada", ProtocolVersion.DEFAULT), TypeCodecs.INT.encode(36, ProtocolVersion.DEFAULT));

        assertAll(() -> assertSame(row, N.convert(row, Object.class)), () -> assertSame(row, N.convert(row, Row.class)),
                () -> assertSame(row, N.convert(row, DefaultRow.class)), () -> {
                    final Person person = CassandraExecutor.toEntity(row, Person.class);
                    assertEquals("Ada", person.getName());
                    assertEquals(36, person.getAge());
                }, () -> assertEquals(Map.of("name", "Ada", "age", 36), CassandraExecutor.toMap(row)));
    }

    @Test
    void fractionalValuesBoundToIntegerColumnsAreRejected() {
        final DataType[] integerTypes = { DataTypes.TINYINT, DataTypes.SMALLINT, DataTypes.INT, DataTypes.BIGINT, DataTypes.VARINT, DataTypes.COUNTER };
        final Object[] fractions = { 1.5d, -0.5d, 1.5f, new BigDecimal("1.5"), new BigDecimal("-2.25") };
        final List<Executable> checks = new ArrayList<>();

        for (final DataType type : integerTypes) {
            for (final Object value : fractions) {
                checks.add(() -> {
                    try (Fixture fixture = new Fixture(type)) {
                        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> fixture.executor.execute(UPDATE, value),
                                value + " -> " + type);
                        assertTrue(e.getMessage().contains("parameter 'v' (index 0)"), e.getMessage());
                        assertTrue(e.getMessage().contains("value " + value + " "), e.getMessage());
                        assertTrue(e.getMessage().contains("CQL type " + type.asCql(false, true)), e.getMessage());
                        assertEquals(0, fixture.binds.size());
                    }
                });
            }
        }

        // Every parameter form; the message names the marker or column and the index.
        checks.add(() -> {
            try (Fixture fixture = new Fixture(DataTypes.INT)) {
                assertThrows(IllegalArgumentException.class, () -> fixture.executor.execute(UPDATE, List.of(1.5d)));
                assertThrows(IllegalArgumentException.class, () -> fixture.executor.execute(UPDATE, (Object) new Object[] { 1.5d }));
                assertThrows(IllegalArgumentException.class, () -> fixture.executor.execute("UPDATE t SET v = :v WHERE id = 1", N.asMap("v", 1.5d)));
                assertThrows(IllegalArgumentException.class, () -> fixture.executor.execute("UPDATE t SET v = :v WHERE id = 1", new DoubleHolder(1.5d)));
                final IllegalArgumentException named = assertThrows(IllegalArgumentException.class,
                        () -> fixture.executor.execute("UPDATE t SET v = #{amount} WHERE id = 1", N.asMap("amount", 1.5d)));
                assertTrue(named.getMessage().contains("parameter 'amount' (index 0)"), named.getMessage());
                assertEquals(0, fixture.binds.size());
            }
        });
        checks.add(() -> {
            try (Fixture fixture = new Fixture(new String[] { "a", "b" }, DataTypes.TEXT, DataTypes.BIGINT)) {
                final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                        () -> fixture.executor.execute("UPDATE t SET a = ?, b = ? WHERE id = 1", "x", 2.5d));
                assertTrue(e.getMessage().contains("parameter 'b' (index 1)"), e.getMessage());
                assertTrue(e.getMessage().contains("it would be bound as 2"), e.getMessage());
            }
        });

        assertAll(checks);
    }

    @Test
    void finiteNumbersBeyondTheFloatOrDoubleRangeAreRejected() {
        final BigInteger huge = BigInteger.TEN.pow(400);
        final List<Executable> checks = new ArrayList<>();

        for (final Object value : new Object[] { 1e300d, -1e39d, new BigDecimal("1e300"), huge }) {
            checks.add(() -> assertRejected(DataTypes.FLOAT, value, "float"));
        }

        for (final Object value : new Object[] { new BigDecimal("1e400"), new BigDecimal("-1e400"), huge }) {
            checks.add(() -> assertRejected(DataTypes.DOUBLE, value, "double"));
        }

        assertAll(checks);
    }

    /** Pins: exact conversions, rounding, non-finite values, out-of-range integers and unconvertible values keep their behavior. */
    @Test
    void exactNumberConversionsAndOtherBindRulesAreUnchanged() {
        assertAll(() -> assertEquals(2, boundValue(DataTypes.INT, 2.0d)), () -> assertEquals(3L, boundValue(DataTypes.BIGINT, new BigDecimal("3"))),
                () -> assertEquals(3, boundValue(DataTypes.INT, new BigDecimal("3.000"))),
                () -> assertEquals(300, boundValue(DataTypes.INT, new BigDecimal("3E+2"))),
                () -> assertEquals(BigInteger.valueOf(-4), boundValue(DataTypes.VARINT, -4.0f)), () -> assertEquals(0, boundValue(DataTypes.INT, -0.0d)),
                () -> assertEquals((short) 7, boundValue(DataTypes.SMALLINT, 7L)), () -> assertEquals(7L, boundValue(DataTypes.COUNTER, 7)),
                () -> assertEquals(1.5d, boundValue(DataTypes.DOUBLE, 1.5f)), () -> assertEquals(0.1f, boundValue(DataTypes.FLOAT, 0.1d)),
                () -> assertEquals(Float.POSITIVE_INFINITY, boundValue(DataTypes.FLOAT, Double.POSITIVE_INFINITY)),
                () -> assertEquals(Float.NaN, boundValue(DataTypes.FLOAT, Double.NaN)),
                () -> assertEquals(new BigDecimal("1.5"), boundValue(DataTypes.DECIMAL, 1.5d)),
                // An integral value out of range, and a non-finite value for an integer column, still throw ArithmeticException.
                () -> assertThrows(ArithmeticException.class, () -> boundValue(DataTypes.INT, 1L << 40)),
                () -> assertThrows(ArithmeticException.class, () -> boundValue(DataTypes.TINYINT, 128)),
                () -> assertThrows(ArithmeticException.class, () -> boundValue(DataTypes.INT, Double.NaN)),
                // A value the conversion rejects is still bound unchanged (the driver then reports it).
                () -> assertEquals("abc", boundValue(DataTypes.INT, "abc")), () -> assertEquals("1.5", boundValue(DataTypes.INT, "1.5")));
    }

    @Test
    void base64TextBoundToABlobColumnIsDecodedIntoAReadableBuffer() {
        try (Fixture fixture = new Fixture(DataTypes.BLOB)) {
            fixture.executor.execute(UPDATE, BASE64);
            fixture.executor.execute(UPDATE, List.of(BASE64));
            fixture.executor.execute("UPDATE t SET v = :v WHERE id = 1", N.asMap("v", BASE64));
            fixture.executor.execute(UPDATE, "");

            assertAll(() -> assertEquals(4, fixture.binds.size()), () -> assertBuffer(BYTES, fixture.binds.get(0)[0]),
                    () -> assertBuffer(BYTES, fixture.binds.get(1)[0]), () -> assertBuffer(BYTES, fixture.binds.get(2)[0]),
                    () -> assertBuffer(new byte[0], fixture.binds.get(3)[0]));
        }
    }

    @Test
    void textThatIsNotBase64IsRejectedForABlobColumn() {
        try (Fixture fixture = new Fixture(DataTypes.BLOB)) {
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
            assertEquals(0, fixture.binds.size());
        }
    }

    /** Pins: byte[] and ByteBuffer bind values for a blob column are unchanged. */
    @Test
    void bytesAndBuffersBoundToABlobColumnAreUnchanged() {
        final ByteBuffer buffer = ByteBuffer.wrap(BYTES);

        assertAll(() -> assertSame(buffer, boundValue(DataTypes.BLOB, buffer)), () -> assertBuffer(BYTES, boundValue(DataTypes.BLOB, BYTES)));
    }

    @Test
    void blobReadIntoStringTargetsIsTheBase64OfTheRemainingBytes() {
        final ByteBuffer positioned = ByteBuffer.wrap(new byte[] { 9, 2, 3 });
        positioned.get();

        try (Fixture fixture = new Fixture(DataTypes.BLOB)) {
            when(fixture.session.execute(any(Statement.class))).thenAnswer(invocation -> rows(DataTypes.BLOB, ByteBuffer.wrap(BYTES), ByteBuffer.wrap(BYTES)));

            assertAll(() -> assertEquals(BASE64, fixture.executor.findFirst(String.class, QUERY).get()),
                    () -> assertEquals(BASE64, fixture.executor.queryForSingleValue(String.class, QUERY).get()),
                    () -> assertEquals(BASE64, fixture.executor.queryForString(QUERY).get()),
                    () -> assertEquals(List.of(BASE64, BASE64), fixture.executor.list(String.class, QUERY)),
                    () -> assertEquals(BASE64, ((String[]) fixture.executor.findFirst(String[].class, QUERY).get())[0]),
                    () -> assertEquals(List.of(BASE64), CassandraExecutor.toList(rows(DataTypes.BLOB, ByteBuffer.wrap(BYTES)), String.class)),
                    () -> assertEquals(BASE64, CassandraExecutor.toEntity(rows(DataTypes.BLOB, ByteBuffer.wrap(BYTES)).one(), Text.class).getV()),
                    () -> assertEquals(List.of(BASE64), CassandraExecutor.extractData(rows(DataTypes.BLOB, ByteBuffer.wrap(BYTES)), Text.class).getColumn("v")),
                    () -> assertEquals(BASE64, CassandraExecutor
                            .toEntity(rows(new String[] { "nested.v" }, DataTypes.BLOB, new Object[][] { { ByteBuffer.wrap(BYTES) } }).one(), TextHolder.class)
                            .getNested()
                            .getV()),
                    // The remaining bytes are encoded, and the driver's buffer is not consumed.
                    () -> {
                        assertEquals("AgM=", CassandraExecutor.toList(rows(DataTypes.BLOB, positioned), String.class).get(0));
                        assertEquals(1, positioned.position());
                    });
        }
    }

    /** Pins: byte[] and ByteBuffer targets of a blob column are unchanged. */
    @Test
    void blobReadIntoBytesAndBufferTargetsIsUnchanged() {
        final ByteBuffer buffer = ByteBuffer.wrap(BYTES);

        assertAll(() -> assertArrayEquals(BYTES, CassandraExecutor.toList(rows(DataTypes.BLOB, buffer), byte[].class).get(0)),
                () -> assertSame(buffer, CassandraExecutor.toList(rows(DataTypes.BLOB, buffer), ByteBuffer.class).get(0)),
                () -> assertEquals(0, buffer.position()));
    }

    @Test
    void aBufferBoundToATextColumnIsItsBase64Text() {
        assertEquals(BASE64, boundValue(DataTypes.TEXT, ByteBuffer.wrap(BYTES)));
    }

    @Test
    void udtBlobFieldsUseTheSameBase64Rule() {
        final UserDefinedType blobType = fieldType(DataTypes.BLOB);
        final CassandraExecutor.UDTCodec<Map> mapCodec = CassandraExecutor.UDTCodec.create(blobType, Map.class);
        final CassandraExecutor.UDTCodec<Text> textCodec = CassandraExecutor.UDTCodec.create(blobType, Text.class);

        assertAll(
                // Text written to a blob field is decoded (it failed with CodecNotFoundException).
                () -> assertEquals(ByteBuffer.wrap(BYTES),
                        mapCodec.decode(mapCodec.encode(Map.of("v", BASE64), ProtocolVersion.V4), ProtocolVersion.V4).get("v")),
                () -> assertEquals(ByteBuffer.wrap(BYTES),
                        mapCodec.decode(textCodec.encode(new Text(BASE64), ProtocolVersion.V4), ProtocolVersion.V4).get("v")),
                () -> {
                    final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                            () -> mapCodec.encode(Map.of("v", "not base64!"), ProtocolVersion.V4));
                    assertTrue(e.getMessage().contains("UDT field 'v' (index 0)"), e.getMessage());
                    assertTrue(e.getMessage().contains("not valid base64"), e.getMessage());
                },
                // A blob field read into a String bean property is base64 ("" before); a Map keeps the driver's buffer.
                () -> assertEquals(BASE64, textCodec.decode(mapCodec.encode(Map.of("v", BYTES), ProtocolVersion.V4), ProtocolVersion.V4).getV()),
                () -> assertEquals(ByteBuffer.wrap(BYTES),
                        mapCodec.decode(mapCodec.encode(Map.of("v", BYTES), ProtocolVersion.V4), ProtocolVersion.V4).get("v")));
    }

    private static void assertRejected(final DataType type, final Object value, final String cqlType) {
        try (Fixture fixture = new Fixture(type)) {
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> fixture.executor.execute(UPDATE, value),
                    value + " -> " + type);
            assertTrue(e.getMessage().contains("parameter 'v' (index 0)"), e.getMessage());
            assertTrue(e.getMessage().contains("CQL type " + cqlType), e.getMessage());
            assertTrue(e.getMessage().contains("Infinity"), e.getMessage());
            assertEquals(0, fixture.binds.size());
        }
    }

    private static Object boundValue(final DataType type, final Object value) {
        try (Fixture fixture = new Fixture(type)) {
            fixture.executor.execute(UPDATE, value);
            final Object[] bind = fixture.binds.get(0);
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

    /** A real driver row (as the driver decodes it) of the given primitive protocol column types. */
    private static Row driverRow(final String[] names, final int[] protocolTypes, final ByteBuffer... values) {
        final List<ColumnDefinition> definitions = new ArrayList<>();

        for (int i = 0; i < names.length; i++) {
            definitions
                    .add(new DefaultColumnDefinition(new ColumnSpec("ks", "t", names[i], i, RawType.PRIMITIVES.get(protocolTypes[i])), AttachmentPoint.NONE));
        }

        return new DefaultRow(DefaultColumnDefinitions.valueOf(definitions), Arrays.asList(values), AttachmentPoint.NONE);
    }

    private static UserDefinedType fieldType(final DataType fieldType) {
        final CodecRegistry registry = new DefaultCodecRegistry("apply-cass-udt");

        return new DefaultUserDefinedType(CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("value_holder"), false,
                List.of(CqlIdentifier.fromInternal("v")), List.of(fieldType), new AttachmentPoint() {
                    @Override
                    public ProtocolVersion getProtocolVersion() {
                        return ProtocolVersion.V4;
                    }

                    @Override
                    public CodecRegistry getCodecRegistry() {
                        return registry;
                    }
                });
    }

    /** Rows of one column {@code v} of {@code type}, one row per value. */
    private static ResultSet rows(final DataType type, final Object... values) {
        return rows(new String[] { "v" }, type, Arrays.stream(values).map(value -> new Object[] { value }).toArray(Object[][]::new));
    }

    private static ResultSet rows(final String[] names, final DataType type, final Object[][] values) {
        final ColumnDefinitions columns = mock(ColumnDefinitions.class);
        when(columns.size()).thenReturn(names.length);

        for (int i = 0; i < names.length; i++) {
            final ColumnDefinition column = mock(ColumnDefinition.class);
            when(columns.get(i)).thenReturn(column);
            when(column.getName()).thenReturn(CqlIdentifier.fromInternal(names[i]));
            when(column.getType()).thenReturn(type);
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
        return result;
    }

    private static final class Fixture implements AutoCloseable {
        final CqlSession session = mock(CqlSession.class);
        final PreparedStatement prepared = mock(PreparedStatement.class);
        final List<Object[]> binds = new ArrayList<>();
        final CassandraExecutor executor;

        Fixture(final DataType variableType) {
            this(new String[] { "v" }, variableType);
        }

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

    // ---- 2026-10-24 r2 sliceH ----

    // The lossy-bind rule tested only Double, Float and BigDecimal sources: any other fractional Number (an abacus Fraction or
    // MutableDouble, a DoubleAdder), which N.convert truncates the same way, was still bound as 1, and an infinite Number other
    // than a Double or Float (which stays infinite in a float or double column) was rejected "without changing it".
    @Test
    void r2_otherFractionalNumbersBoundToIntegerColumnsAreRejected() {
        final DoubleAdder adder = new DoubleAdder();
        adder.add(1.5);
        final List<Executable> checks = new ArrayList<>();

        for (final Object value : new Object[] { Fraction.of(3, 2), MutableDouble.of(1.5), MutableFloat.of(-2.5f), adder }) {
            for (final DataType type : new DataType[] { DataTypes.SMALLINT, DataTypes.INT, DataTypes.BIGINT, DataTypes.VARINT }) {
                checks.add(() -> {
                    try (Fixture fixture = new Fixture(type)) {
                        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> fixture.executor.execute(UPDATE, value),
                                value + " -> " + type);
                        assertTrue(e.getMessage().contains("parameter 'v' (index 0)"), e.getMessage());
                        assertTrue(e.getMessage().contains("CQL type " + type.asCql(false, true) + " without changing it"), e.getMessage());
                        assertEquals(0, fixture.binds.size());
                    }
                });
            }
        }

        assertAll(checks);
    }

    @Test
    void r2_infiniteNumbersOfOtherTypesStayInfiniteInFloatAndDoubleColumns() {
        final DoubleAdder negativeInfinity = new DoubleAdder();
        negativeInfinity.add(Double.NEGATIVE_INFINITY);

        assertAll(() -> assertEquals(Float.POSITIVE_INFINITY, boundValue(DataTypes.FLOAT, MutableDouble.of(Double.POSITIVE_INFINITY))),
                () -> assertEquals(Double.NEGATIVE_INFINITY, boundValue(DataTypes.DOUBLE, MutableDouble.of(Double.NEGATIVE_INFINITY))),
                () -> assertEquals(Double.POSITIVE_INFINITY, boundValue(DataTypes.DOUBLE, MutableFloat.of(Float.POSITIVE_INFINITY))),
                () -> assertEquals(Float.NEGATIVE_INFINITY, boundValue(DataTypes.FLOAT, negativeInfinity)),
                // Pins: a finite one beyond the float range is still rejected; integral values of these types still bind.
                () -> assertRejected(DataTypes.FLOAT, MutableDouble.of(1e300), "float"), () -> assertEquals(2, boundValue(DataTypes.INT, Fraction.of(4, 2))),
                () -> assertEquals(-3L, boundValue(DataTypes.BIGINT, MutableDouble.of(-3.0))),
                () -> assertEquals(1L << 40, boundValue(DataTypes.BIGINT, new AtomicLong(1L << 40))),
                () -> assertEquals(1.5f, boundValue(DataTypes.FLOAT, Fraction.of(3, 2))));
    }

    // A collection or map column read into a parameterized container property kept the driver's element types (heap pollution):
    // a List<byte[]> held ByteBuffers, a List<LocalDateTime> Instants, a Set<String> InetAddresses, and they failed with
    // ClassCastException where used; an array property (String[], byte[][]) got N.convert's per-element results ("/127.0.0.1", null).
    @Test
    void r2_collectionColumnElementsAreConvertedToTheDeclaredElementType() throws UnknownHostException {
        final InetAddress local = InetAddress.getByAddress(new byte[] { 127, 0, 0, 1 });
        final InetAddress other = InetAddress.getByAddress(new byte[] { 10, 0, 0, 2 });
        final Instant first = Instant.parse("2020-01-02T03:04:05Z");
        final Instant second = Instant.parse("2021-06-07T08:09:10.123Z");
        final RawType blob = RawType.PRIMITIVES.get(ProtocolConstants.DataType.BLOB);
        final RawType inet = RawType.PRIMITIVES.get(ProtocolConstants.DataType.INET);
        final RawType timestamp = RawType.PRIMITIVES.get(ProtocolConstants.DataType.TIMESTAMP);
        final ProtocolVersion v = ProtocolVersion.DEFAULT;
        final ByteBuffer blobs = TypeCodecs.listOf(TypeCodecs.BLOB).encode(List.of(ByteBuffer.wrap(BYTES)), v);
        final ByteBuffer hosts = TypeCodecs.setOf(TypeCodecs.INET).encode(new LinkedHashSet<>(List.of(local, other)), v);
        final ByteBuffer stamps = TypeCodecs.listOf(TypeCodecs.TIMESTAMP).encode(List.of(first, second), v);
        final Map<String, InetAddress> hostMap = new LinkedHashMap<>();
        hostMap.put("a", local);
        hostMap.put("b", other);

        final Containers c = CassandraExecutor.toEntity(containerRow(
                new String[] { "blobs", "blobText", "blobArray", "stamps", "hosts", "sortedHosts", "hostArray", "hostMap", "longs", "nestedStamps" },
                new RawType[] { new RawType.RawList(blob), new RawType.RawList(blob), new RawType.RawList(blob), new RawType.RawList(timestamp),
                        new RawType.RawSet(inet), new RawType.RawSet(inet), new RawType.RawSet(inet),
                        new RawType.RawMap(RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR), inet),
                        new RawType.RawList(RawType.PRIMITIVES.get(ProtocolConstants.DataType.INT)), new RawType.RawList(new RawType.RawList(timestamp)) },
                blobs, blobs.duplicate(), blobs.duplicate(), stamps, hosts, hosts.duplicate(), hosts.duplicate(),
                TypeCodecs.mapOf(TypeCodecs.TEXT, TypeCodecs.INET).encode(hostMap, v), TypeCodecs.listOf(TypeCodecs.INT).encode(List.of(1, 2), v),
                TypeCodecs.listOf(TypeCodecs.listOf(TypeCodecs.TIMESTAMP)).encode(List.of(List.of(first), List.of(second)), v)), Containers.class);
        final LocalDateTime firstLocal = LocalDateTime.ofInstant(first, ZoneId.systemDefault());
        final LocalDateTime secondLocal = LocalDateTime.ofInstant(second, ZoneId.systemDefault());

        assertAll(() -> {
            assertEquals(1, c.blobs.size());
            assertArrayEquals(BYTES, assertInstanceOf(byte[].class, (Object) c.blobs.get(0)));
        }, () -> assertEquals(List.of(BASE64), c.blobText), () -> {
            assertEquals(1, c.blobArray.length);
            assertArrayEquals(BYTES, c.blobArray[0]);
        }, () -> assertEquals(List.of(firstLocal, secondLocal), c.stamps),
                // The driver's iteration order is kept (a LinkedHashSet/LinkedHashMap), and a sorted declared type is honored.
                () -> assertEquals(List.of("127.0.0.1", "10.0.0.2"), new ArrayList<>(c.hosts)), () -> assertInstanceOf(LinkedHashSet.class, c.hosts),
                () -> assertEquals(new TreeSet<>(List.of("127.0.0.1", "10.0.0.2")), c.sortedHosts), () -> assertInstanceOf(TreeSet.class, c.sortedHosts),
                () -> assertArrayEquals(new String[] { "127.0.0.1", "10.0.0.2" }, c.hostArray),
                () -> assertEquals(List.of("a", "b"), new ArrayList<>(c.hostMap.keySet())),
                () -> assertEquals(List.of("127.0.0.1", "10.0.0.2"), new ArrayList<>(c.hostMap.values())), () -> assertEquals(List.of(1L, 2L), c.longs),
                () -> assertEquals(List.of(List.of(firstLocal), List.of(secondLocal)), c.nestedStamps));
    }

    @Test
    void r2_nestedPathAndUdtBeanContainerPropertiesConvertTheirElements() throws UnknownHostException {
        final InetAddress local = InetAddress.getByAddress(new byte[] { 127, 0, 0, 1 });
        final ContainerHolder holder = CassandraExecutor.toEntity(
                containerRow(new String[] { "inner.hosts" }, new RawType[] { new RawType.RawSet(RawType.PRIMITIVES.get(ProtocolConstants.DataType.INET)) },
                        TypeCodecs.setOf(TypeCodecs.INET).encode(Set.of(local), ProtocolVersion.DEFAULT)),
                ContainerHolder.class);

        final UserDefinedType listType = fieldType(DataTypes.listOf(DataTypes.BLOB));
        final CassandraExecutor.UDTCodec<Map> mapCodec = CassandraExecutor.UDTCodec.create(listType, Map.class);
        final CassandraExecutor.UDTCodec<BlobList> beanCodec = CassandraExecutor.UDTCodec.create(listType, BlobList.class);
        final BlobList decoded = beanCodec.decode(mapCodec.encode(Map.of("v", List.of(ByteBuffer.wrap(BYTES))), ProtocolVersion.V4), ProtocolVersion.V4);

        assertAll(() -> assertEquals(Set.of("127.0.0.1"), holder.inner.hosts), () -> {
            assertEquals(1, decoded.v.size());
            assertArrayEquals(BYTES, assertInstanceOf(byte[].class, (Object) decoded.v.get(0)));
        });
    }

    /** Pins: a container whose elements already have the declared type is the driver's own instance (no copy); UDT elements are kept. */
    @Test
    void r2_containersWhoseElementsFitAreSetUnchanged() {
        final List<Object> buffers = new ArrayList<>(List.of(ByteBuffer.wrap(BYTES)));
        final List<Object> instants = new ArrayList<>(List.of(Instant.EPOCH));
        final List<Object> texts = new ArrayList<>(List.of("a"));
        final List<Object> udts = new ArrayList<>(List.of(fieldType(DataTypes.TEXT).newValue("x")));
        final Fits fits = CassandraExecutor.toEntity(rows(new String[] { "buffers", "instants", "raw", "objects", "texts", "udts" }, DataTypes.BLOB,
                new Object[][] { { buffers, instants, buffers, instants, texts, udts } }).one(), Fits.class);

        assertAll(() -> assertSame(buffers, fits.buffers), () -> assertSame(instants, fits.instants), () -> assertSame(buffers, fits.raw),
                () -> assertSame(instants, fits.objects), () -> assertSame(texts, fits.texts), () -> assertSame(udts, fits.udts));
    }

    @Test
    void r2_anElementThatCannotBeConvertedFailsTheRowAsATopLevelValueDoes() {
        final RuntimeException topLevel = assertThrows(RuntimeException.class,
                () -> CassandraExecutor.toEntity(rows(new String[] { "number" }, DataTypes.TEXT, new Object[][] { { "abc" } }).one(), NumberHolder.class));
        final RuntimeException element = assertThrows(RuntimeException.class, () -> CassandraExecutor
                .toEntity(rows(new String[] { "numbers" }, DataTypes.TEXT, new Object[][] { { new ArrayList<>(List.of("abc")) } }).one(), NumberHolder.class));

        assertEquals(topLevel.getClass(), element.getClass());
    }

    // Beans.isBeanClass accepts an @Entity-annotated Map subclass, and extractData tested it before the Map check: the columns were
    // typed by the Map's getters, so a nested row became an Object[], and one in a column named like a getter ("empty", from
    // isEmpty()) was read as that getter's boolean (IllegalArgumentException), instead of a map as for any other map target.
    @Test
    void r2_extractDataTreatsAnEntityAnnotatedMapAsAMapTarget() {
        final Row ada = rows(new String[] { "value" }, DataTypes.TEXT, new Object[][] { { "Ada" } }).one();
        final Row grace = rows(new String[] { "value" }, DataTypes.TEXT, new Object[][] { { "Grace" } }).one();
        final Object[][] values = { { ada, grace, "x" } };
        final String[] names = { "nested", "empty", "name" };

        assertAll(() -> {
            final com.landawn.abacus.util.Dataset dataset = CassandraExecutor.extractData(rows(names, DataTypes.TEXT, values), R2HEntityMap.class);
            assertEquals(Map.of("value", "Ada"), dataset.getColumn("nested").get(0));
            assertEquals(Map.of("value", "Grace"), dataset.getColumn("empty").get(0));
            assertEquals("x", dataset.getColumn("name").get(0));
        },
                // Pins: a plain Map target and toList for the same class give these shapes.
                () -> assertEquals(Map.of("value", "Ada"),
                        CassandraExecutor.extractData(rows(names, DataTypes.TEXT, values), LinkedHashMap.class).getColumn("nested").get(0)),
                () -> assertEquals(Map.of("value", "Grace"),
                        CassandraExecutor.toList(rows(names, DataTypes.TEXT, values), R2HEntityMap.class).get(0).get("empty")));
    }

    @com.landawn.abacus.annotation.Entity
    public static class R2HEntityMap extends LinkedHashMap<String, Object> {
        private static final long serialVersionUID = 1L;
    }

    /** A real driver row (as the driver decodes it) of the given raw column types. */
    private static Row containerRow(final String[] names, final RawType[] types, final ByteBuffer... values) {
        final List<ColumnDefinition> definitions = new ArrayList<>();

        for (int i = 0; i < names.length; i++) {
            definitions.add(new DefaultColumnDefinition(new ColumnSpec("ks", "t", names[i], i, types[i]), AttachmentPoint.NONE));
        }

        return new DefaultRow(DefaultColumnDefinitions.valueOf(definitions), Arrays.asList(values), AttachmentPoint.NONE);
    }

    public static class Containers {
        public List<byte[]> blobs;
        public List<String> blobText;
        public byte[][] blobArray;
        public List<LocalDateTime> stamps;
        public Set<String> hosts;
        public SortedSet<String> sortedHosts;
        public String[] hostArray;
        public Map<String, String> hostMap;
        public List<Long> longs;
        public List<List<LocalDateTime>> nestedStamps;
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
        public List<Instant> instants;
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
    @Test
    void r2verify5_scalarElementsForContainerElementTypesGetTheFullDeclaredType() {
        final RawType intType = RawType.PRIMITIVES.get(ProtocolConstants.DataType.INT);
        final RawType bigintType = RawType.PRIMITIVES.get(ProtocolConstants.DataType.BIGINT);
        final ProtocolVersion v = ProtocolVersion.DEFAULT;
        final Map<String, Integer> counts = new LinkedHashMap<>();
        counts.put("b", 2);
        counts.put("a", 1);

        final NestedElements n = CassandraExecutor.toEntity(containerRow(new String[] { "longLists", "textLists", "longListMap" },
                new RawType[] { new RawType.RawSet(intType), new RawType.RawSet(bigintType),
                        new RawType.RawMap(RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR), intType) },
                TypeCodecs.setOf(TypeCodecs.INT).encode(new LinkedHashSet<>(List.of(3, 1)), v),
                TypeCodecs.setOf(TypeCodecs.BIGINT).encode(new LinkedHashSet<>(List.of(3L, 1L)), v),
                TypeCodecs.mapOf(TypeCodecs.TEXT, TypeCodecs.INT).encode(counts, v)), NestedElements.class);

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
