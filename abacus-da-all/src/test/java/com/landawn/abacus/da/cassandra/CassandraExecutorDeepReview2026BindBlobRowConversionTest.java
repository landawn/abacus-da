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
import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
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
            this(CodecRegistry.DEFAULT, variableNames, variableTypes);
        }

        Fixture(final CodecRegistry registry, final String[] variableNames, final DataType... variableTypes) {
            final DriverContext context = mock(DriverContext.class);
            when(session.getContext()).thenReturn(context);
            when(context.getCodecRegistry()).thenReturn(registry);
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

    // ---- 2026-10-27 sliceH ----

    // N.convert converts a ByteBuffer to any target other than String/byte[] from the base64 of the bytes BEFORE its position: "" for
    // a buffer at position 0. A buffer bound to an int, bigint, uuid, timestamp, ... column was therefore converted to null and bound
    // as NULL (a null written in place of the value); the driver itself rejects such a value. It is now bound unchanged.
    @Test
    void sliceH_aBufferBoundToANonBlobColumnIsBoundUnchanged() {
        final DataType[] types = { DataTypes.INT, DataTypes.BIGINT, DataTypes.UUID, DataTypes.TIMEUUID, DataTypes.TIMESTAMP, DataTypes.DATE, DataTypes.TIME,
                DataTypes.DECIMAL, DataTypes.VARINT, DataTypes.DOUBLE, DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT) };
        final List<Executable> checks = new ArrayList<>();

        for (final DataType type : types) {
            checks.add(() -> {
                final ByteBuffer buffer = ByteBuffer.wrap(BYTES);
                assertSame(buffer, boundValue(type, buffer), type.asCql(false, true));
            });
        }

        assertAll(checks);
    }

    // N.convert(value, ByteBuffer.class) base64-decodes the value's text into a buffer positioned at its limit: a value whose text
    // happens to be base64 (true, 1234, a StringBuilder "AQID") was bound to a blob column as an EMPTY blob. Of the JDK and abacus
    // classes only byte[] and text (a String, StringBuilder or StringBuffer, see sbfix below) have a blob form; any other such value,
    // a CharBuffer included, is bound unchanged, so the driver rejects it (application values and Optional-like wrappers follow their
    // own rules, tested below).
    @Test
    void sliceH_nonTextValuesBoundToABlobColumnAreBoundUnchanged() {
        final Object[] values = { Boolean.TRUE, 1234, 12345678L, new BigDecimal("1234"), java.nio.CharBuffer.wrap(BASE64),
                com.landawn.abacus.util.MutableInt.of(1234), new AtomicLong(1234) };
        final List<Executable> checks = new ArrayList<>();

        for (final Object value : values) {
            checks.add(() -> assertSame(value, boundValue(DataTypes.BLOB, value), value.getClass().getSimpleName()));
        }

        // Pins: byte[] and base64 String text keep their blob form.
        checks.add(() -> assertBuffer(BYTES, boundValue(DataTypes.BLOB, BYTES)));
        checks.add(() -> assertBuffer(BYTES, boundValue(DataTypes.BLOB, BASE64)));
        assertAll(checks);
    }

    // The same empty text made a blob read into a number, date/time, UUID, Map, Optional-like or String[] target (or bean property) null,
    // 0 or empty, silently. Such a target has no blob form and is rejected; a CharSequence target gets the base64 text like a String.
    // (fixD: an Optional, Nullable or Holder target now holds the blob converted to its element type; a primitive Optional is rejected.)
    @Test
    void sliceH_blobReadIntoATargetWithoutABlobFormIsRejected() {
        final Class<?>[] targets = { Long.class, long.class, Integer.class, java.util.UUID.class, Instant.class, java.time.LocalDate.class,
                java.util.Date.class, java.util.OptionalInt.class };
        final List<Executable> checks = new ArrayList<>();

        for (final Class<?> target : targets) {
            checks.add(() -> assertBlobRejected(() -> CassandraExecutor.toList(rows(DataTypes.BLOB, ByteBuffer.wrap(BYTES)), target), target.getName()));
        }

        // A Map or String[] class is a row shape for toList; as a single-value target (first column) it has no blob form either.
        for (final Class<?> target : new Class<?>[] { Long.class, Map.class, String[].class }) {
            checks.add(() -> {
                try (Fixture fixture = new Fixture(DataTypes.BLOB)) {
                    when(fixture.session.execute(any(Statement.class))).thenAnswer(invocation -> rows(DataTypes.BLOB, ByteBuffer.wrap(BYTES)));
                    assertBlobRejected(() -> fixture.executor.queryForSingleValue(target, QUERY), "queryForSingleValue " + target.getName());
                }
            });
        }

        checks.add(() -> assertBlobRejected(() -> CassandraExecutor.toEntity(rows(DataTypes.BLOB, ByteBuffer.wrap(BYTES)).one(), SliceHLong.class), "Long"));
        checks.add(() -> assertBlobRejected(() -> CassandraExecutor.toEntity(rows(DataTypes.BLOB, ByteBuffer.wrap(BYTES)).one(), SliceHInt.class), "int"));
        checks.add(() -> assertBlobRejected(() -> CassandraExecutor.toEntity(rows(DataTypes.BLOB, ByteBuffer.wrap(BYTES)).one(), SliceHUuid.class), "UUID"));
        checks.add(() -> assertBlobRejected(() -> CassandraExecutor
                .toEntity(rows(new String[] { "nested.v" }, DataTypes.BLOB, new Object[][] { { ByteBuffer.wrap(BYTES) } }).one(), SliceHLongHolder.class),
                "nested Long"));
        // extractData types a column by the bean's getter.
        checks.add(
                () -> assertBlobRejected(() -> CassandraExecutor.extractData(rows(DataTypes.BLOB, ByteBuffer.wrap(BYTES)), SliceHLongBean.class), "Dataset"));

        // Pins: CharSequence gets the base64 text; byte[], ByteBuffer, Object, collection and Object[] targets keep the blob.
        checks.add(() -> assertEquals(BASE64, CassandraExecutor.toList(rows(DataTypes.BLOB, ByteBuffer.wrap(BYTES)), CharSequence.class).get(0)));
        checks.add(() -> assertEquals(BASE64, CassandraExecutor.toEntity(rows(DataTypes.BLOB, ByteBuffer.wrap(BYTES)).one(), SliceHText.class).v));
        checks.add(() -> assertArrayEquals(BYTES, CassandraExecutor.toList(rows(DataTypes.BLOB, ByteBuffer.wrap(BYTES)), byte[].class).get(0)));
        checks.add(() -> {
            final ByteBuffer buffer = ByteBuffer.wrap(BYTES);

            try (Fixture fixture = new Fixture(DataTypes.BLOB)) {
                when(fixture.session.execute(any(Statement.class))).thenAnswer(invocation -> rows(DataTypes.BLOB, buffer));
                assertSame(buffer, fixture.executor.queryForSingleValue(Object.class, QUERY).get());
                assertEquals(List.of(buffer), fixture.executor.queryForSingleValue(List.class, QUERY).get());
                assertArrayEquals(new Object[] { buffer }, fixture.executor.queryForSingleValue(Object[].class, QUERY).get());
            }
        });

        assertAll(checks);
    }

    // A UDT timestamp field converted a ByteBuffer value through the same empty text into a null Instant and wrote NULL; it now
    // fails with the driver's CodecNotFoundException, as a value the timestamp conversion rejects does.
    @Test
    void sliceH_aBufferForAUdtTimestampFieldIsRejected() {
        final CassandraExecutor.UDTCodec<Map> mapCodec = CassandraExecutor.UDTCodec.create(fieldType(DataTypes.TIMESTAMP), Map.class);

        assertThrows(com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException.class,
                () -> mapCodec.encode(Map.of("v", ByteBuffer.wrap(BYTES)), ProtocolVersion.V4));
    }

    // Beans.isBeanClass accepts an @Entity-annotated Map subclass, so toEntity built it as an immutable bean of its Map accessors and
    // failed with UnsupportedOperationException ("No all arguments constructor found"), not the IllegalArgumentException documented
    // (and thrown for a plain Map class) for a class that is not a bean.
    @Test
    void sliceH_toEntityRejectsEveryMapType() {
        final ResultSet resultSet = rows(new String[] { "name" }, DataTypes.TEXT, new Object[][] { { "Ada" }, { "Ada" } });
        final Row row = resultSet.one();

        assertAll(() -> {
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toEntity(row, R2HEntityMap.class));
            assertTrue(e.getMessage().contains("Not a bean class"), e.getMessage());
            assertTrue(e.getMessage().contains("toMap"), e.getMessage());
        }, () -> assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toEntity(row, LinkedHashMap.class)),
                // Pin: the row readers still fill an @Entity Map subclass.
                () -> assertEquals("Ada", CassandraExecutor.toList(resultSet, R2HEntityMap.class).get(0).get("name")));
    }

    // N.convert reads a value bound to a map column as JSON text; text that is not a JSON object failed with ParsingException, which
    // escaped the keep-the-original fallback (it catches ClassCastException and IllegalArgumentException only), so the bind threw
    // ParsingException instead of binding the value for the driver to reject (CodecNotFoundException), as documented.
    @Test
    void sliceH_aValueThatIsNotAJsonObjectIsKeptForAMapColumn() {
        final DataType mapType = DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT);

        assertAll(() -> assertSame("abc", boundValue(mapType, "abc")), () -> assertSame(Instant.EPOCH, boundValue(mapType, Instant.EPOCH)),
                // Pin: JSON object text still converts to a map.
                () -> assertEquals(Map.of("a", 1), boundValue(mapType, "{\"a\": 1}")));
    }

    // N.newCollection / IntFunctions.ofMap create a mutable stand-in (ArrayList, HashSet, HashMap, TreeMap) for an immutable abacus
    // class, so rows read as ImmutableMap/ImmutableList/ImmutableSet/ImmutableSortedMap were HashMaps etc. typed as that class (heap
    // pollution), and a UDTCodec for such a class decoded to the stand-in. Each row is now an instance of the requested class.
    @Test
    void sliceH_immutableRowClassesGetRowsOfThatClass() {
        final String[] names = { "name", "age" };
        final Object[][] values = { { "Ada", "36" }, { "Grace", "45" } };
        final List<Executable> checks = new ArrayList<>();

        for (final Class<?> rowClass : new Class<?>[] { com.landawn.abacus.util.ImmutableMap.class, com.landawn.abacus.util.ImmutableSortedMap.class,
                com.landawn.abacus.util.ImmutableList.class, com.landawn.abacus.util.ImmutableSet.class }) {
            checks.add(() -> {
                final List<?> rows = CassandraExecutor.toList(rows(names, DataTypes.TEXT, values), rowClass);
                assertEquals(2, rows.size(), rowClass.getSimpleName());
                rows.forEach(row -> assertInstanceOf(rowClass, row, rowClass.getSimpleName()));
                assertEquals(
                        Map.class.isAssignableFrom(rowClass) ? Map.of("name", "Ada", "age", "36")
                                : rowClass == com.landawn.abacus.util.ImmutableSet.class ? Set.of("Ada", "36") : List.of("Ada", "36"),
                        rows.get(0), rowClass.getSimpleName());
            });
            checks.add(() -> {
                try (Fixture fixture = new Fixture(DataTypes.TEXT)) {
                    when(fixture.session.execute(any(Statement.class))).thenAnswer(invocation -> rows(names, DataTypes.TEXT, values));
                    assertInstanceOf(rowClass, fixture.executor.findFirst(rowClass, QUERY).get(), "findFirst " + rowClass.getSimpleName());
                }
            });
        }

        final UserDefinedType textType = fieldType(DataTypes.TEXT);
        final CassandraExecutor.UDTCodec<com.landawn.abacus.util.ImmutableList> listCodec = CassandraExecutor.UDTCodec.create(textType,
                com.landawn.abacus.util.ImmutableList.class);
        final CassandraExecutor.UDTCodec<com.landawn.abacus.util.ImmutableMap> mapCodec = CassandraExecutor.UDTCodec.create(textType,
                com.landawn.abacus.util.ImmutableMap.class);
        checks.add(() -> assertInstanceOf(com.landawn.abacus.util.ImmutableList.class,
                listCodec.decode(listCodec.encode(com.landawn.abacus.util.ImmutableList.of("x"), ProtocolVersion.V4), ProtocolVersion.V4)));
        checks.add(() -> {
            final Object decoded = mapCodec.decode(mapCodec.encode(com.landawn.abacus.util.ImmutableMap.of("v", "x"), ProtocolVersion.V4), ProtocolVersion.V4);
            assertInstanceOf(com.landawn.abacus.util.ImmutableMap.class, decoded);
            assertEquals(Map.of("v", "x"), decoded);
        });

        // Pins: mutable and @Entity Map row classes are unchanged; ImmutableBiMap still fails before reading a value.
        checks.add(() -> assertInstanceOf(LinkedHashMap.class, CassandraExecutor.toList(rows(names, DataTypes.TEXT, values), LinkedHashMap.class).get(0)));
        checks.add(() -> assertInstanceOf(R2HEntityMap.class, CassandraExecutor.toList(rows(names, DataTypes.TEXT, values), R2HEntityMap.class).get(0)));
        checks.add(() -> assertThrows(IllegalArgumentException.class,
                () -> CassandraExecutor.toList(rows(names, DataTypes.TEXT, values), com.landawn.abacus.util.ImmutableBiMap.class)));
        assertAll(checks);
    }

    // verify1 (regression in this round's blob-column bind rule): a value of an application class with a converter to ByteBuffer
    // registered with abacus (a protobuf ByteString-like wrapper) went through N.convert, which applies the converter, and bound
    // its bytes; the new rule bound it unchanged (CodecNotFoundException). Only JDK and abacus values, which cannot have a
    // converter, are bound unchanged.
    @Test
    void sliceH_applicationValuesWithAConverterAreConvertedForABlobColumn() {
        example.cassandra.BlobFixtures.register();

        try (Fixture fixture = new Fixture(new String[] { "v", "id" }, DataTypes.BLOB, DataTypes.INT)) {
            fixture.executor.execute("UPDATE t SET v = ? WHERE id = ?", new example.cassandra.BlobFixtures.BlobBox(BYTES), 1);
            fixture.executor.execute("UPDATE t SET v = ? WHERE id = ?", Boolean.TRUE, 1);

            assertAll(() -> assertBuffer(BYTES, fixture.binds.get(0)[0]),
                    // Pin: a JDK value without a blob form is still bound unchanged.
                    () -> assertSame(Boolean.TRUE, fixture.binds.get(1)[0]));
        }
    }

    // verify1 (regression in this round's blob read rule): a class whose abacus Type the application registered may build its
    // value from the ByteBuffer itself (N.convert hands it the buffer); the new rule rejected every non-blob target.
    @Test
    void sliceH_applicationTypesBuildTheirValueFromABlob() {
        example.cassandra.BlobFixtures.register();

        assertAll(
                () -> assertArrayEquals(BYTES,
                        CassandraExecutor.toList(rows(DataTypes.BLOB, ByteBuffer.wrap(BYTES)), example.cassandra.BlobFixtures.TypedBox.class).get(0).bytes()),
                () -> assertArrayEquals(BYTES, CassandraExecutor.toEntity(rows(DataTypes.BLOB, ByteBuffer.wrap(BYTES)).one(), SliceHTypedBox.class).v.bytes()),
                () -> assertArrayEquals(BYTES,
                        CassandraExecutor.toEntity(rows(DataTypes.listOf(DataTypes.BLOB), new ArrayList<>(List.of(ByteBuffer.wrap(BYTES)))).one(),
                                SliceHTypedBoxes.class).v.get(0).bytes()));
    }

    // verify1 (pre-existing): a blob read into a property declared as a collection of an element type (List<byte[]>, List<String>,
    // Set<String>, List<List<byte[]>> elements) went through N.convert's empty-text path and became null ([null] for an element).
    // It is now the collection of that one blob, converted to the element type as a top-level blob is.
    @Test
    void sliceH_aBlobForACollectionPropertyIsThatCollectionOfTheConvertedBlob() {
        final String[] names = { "bytes", "texts", "textSet", "buffers" };
        final Object[][] values = { { ByteBuffer.wrap(BYTES), ByteBuffer.wrap(BYTES), ByteBuffer.wrap(BYTES), ByteBuffer.wrap(BYTES) } };
        final SliceHBlobCollections c = CassandraExecutor.toEntity(rows(names, DataTypes.BLOB, values).one(), SliceHBlobCollections.class);
        final SliceHBlobCollectionsHolder holder = CassandraExecutor.toEntity(
                rows(new String[] { "inner.texts" }, DataTypes.BLOB, new Object[][] { { ByteBuffer.wrap(BYTES) } }).one(), SliceHBlobCollectionsHolder.class);
        final SliceHNestedBlobs nested = CassandraExecutor
                .toEntity(rows(DataTypes.listOf(DataTypes.BLOB), new ArrayList<>(List.of(ByteBuffer.wrap(BYTES)))).one(), SliceHNestedBlobs.class);
        final CassandraExecutor.UDTCodec<Map> mapCodec = CassandraExecutor.UDTCodec.create(fieldType(DataTypes.BLOB), Map.class);
        final CassandraExecutor.UDTCodec<SliceHUdtTexts> udtCodec = CassandraExecutor.UDTCodec.create(fieldType(DataTypes.BLOB), SliceHUdtTexts.class);

        assertAll(() -> assertEquals(1, c.bytes.size()), () -> assertArrayEquals(BYTES, c.bytes.get(0)), () -> assertEquals(List.of(BASE64), c.texts),
                () -> assertEquals(Set.of(BASE64), c.textSet), () -> assertEquals(List.of(ByteBuffer.wrap(BYTES)), c.buffers),
                () -> assertEquals(List.of(BASE64), holder.inner.texts), () -> assertArrayEquals(BYTES, nested.v.get(0).get(0)),
                () -> assertEquals(List.of(BASE64), udtCodec.decode(mapCodec.encode(Map.of("v", BYTES), ProtocolVersion.V4), ProtocolVersion.V4).v),
                // An element type without a blob form is rejected, as at top level.
                () -> assertBlobRejected(() -> CassandraExecutor.toEntity(rows(DataTypes.BLOB, ByteBuffer.wrap(BYTES)).one(), SliceHBlobLongs.class),
                        "List<Long>"));
    }

    // verify1 (pre-existing heap pollution): a scalar element for a parameterized element type other than a collection or map was
    // converted to the raw class, so a list<int> column read into a List<Optional<Long>> held Optional values of the wrong element
    // type (Optional<String> "7"), where a top-level Optional<Long> property holds an Optional<Long>.
    @Test
    void sliceH_scalarElementsForParameterizedElementTypesKeepTheirTypeArguments() {
        final Map<String, Integer> counts = new LinkedHashMap<>();
        counts.put("a", 7);
        final SliceHOptionalElements e = CassandraExecutor.toEntity(
                rows(new String[] { "list", "map", "single" }, DataTypes.INT, new Object[][] { { new ArrayList<>(List.of(7)), counts, 7 } }).one(),
                SliceHOptionalElements.class);

        assertAll(() -> assertEquals(java.util.Optional.of(7L), e.list.get(0)), () -> assertInstanceOf(Long.class, e.list.get(0).get()),
                () -> assertEquals(java.util.Optional.of(7L), e.map.get("a")), () -> assertInstanceOf(Long.class, e.map.get("a").get()),
                // Pin: the top-level property, which these elements now match.
                () -> assertEquals(java.util.Optional.of(7L), e.single));
    }

    // verify1 (delta): a value of an application class goes through convertValue so that a registered converter applies; without
    // one, N.convert base64-decodes the value's text into a buffer positioned at its limit, so a driver CqlDuration ("1h"), a Guava
    // UnsignedLong (1234) or a BSON ObjectId (24 hex digits) was bound as an EMPTY blob again. Such a buffer has no blob content: the
    // value is bound unchanged and the driver rejects it.
    @Test
    void verify1_applicationValuesWithoutABlobFormAreBoundUnchanged() {
        example.cassandra.BlobFixtures.register();
        final Object[] values = { com.datastax.oss.driver.api.core.data.CqlDuration.from("1h"), com.google.common.primitives.UnsignedLong.valueOf(1234),
                new org.bson.types.ObjectId("0123456789abcdef01234567") };
        final List<Executable> checks = new ArrayList<>();

        for (final DataType type : new DataType[] { DataTypes.BLOB, DataTypes.custom("org.example.X") }) {
            for (final Object value : values) {
                checks.add(() -> {
                    try (Fixture fixture = new Fixture(new String[] { "v", "id" }, type, DataTypes.INT)) {
                        fixture.executor.execute("UPDATE t SET v = ? WHERE id = ?", value, 1);
                        assertSame(value, fixture.binds.get(0)[0], value.getClass().getSimpleName() + " -> " + type.asCql(false, true));
                    }
                });
            }
        }

        // Pin: a registered converter's buffer is still bound.
        checks.add(() -> {
            try (Fixture fixture = new Fixture(new String[] { "v", "id" }, DataTypes.BLOB, DataTypes.INT)) {
                fixture.executor.execute("UPDATE t SET v = ? WHERE id = ?", new example.cassandra.BlobFixtures.BlobBox(BYTES), 1);
                assertBuffer(BYTES, fixture.binds.get(0)[0]);
            }
        });
        assertAll(checks);
    }

    // verify2 (regression in this round's blob-column bind rule): an EMPTY Optional-like value bound to a blob or custom column was
    // bound as NULL (N.convert maps it to null, as for an int or text column); the rule binding JDK and abacus values unchanged made
    // it fail with CodecNotFoundException, also for an entity insert with an empty Optional<ByteBuffer> or Nullable<byte[]> property.
    // An empty wrapper binds NULL again, and a present one binds its content by the blob rules (N.convert had bound an empty blob).
    @Test
    void sliceH_optionalLikeValuesForABlobColumnBindTheirContent() {
        final Object[] empties = { java.util.Optional.empty(), com.landawn.abacus.util.u.Optional.empty(), com.landawn.abacus.util.u.Nullable.empty(),
                com.landawn.abacus.util.u.Nullable.of(null), com.landawn.abacus.util.Holder.of(null), java.util.OptionalInt.empty(),
                com.landawn.abacus.util.u.OptionalInt.empty() };
        final List<Executable> checks = new ArrayList<>();

        for (final DataType type : new DataType[] { DataTypes.BLOB, DataTypes.custom("org.apache.cassandra.db.marshal.BytesType") }) {
            for (final Object empty : empties) {
                checks.add(() -> assertEquals(null, boundWithKey(type, empty), empty + " -> " + type.asCql(false, true)));
            }
        }

        final ByteBuffer buffer = ByteBuffer.wrap(BYTES);
        final java.util.Optional<Boolean> present = java.util.Optional.of(true);
        checks.add(() -> assertSame(buffer, boundWithKey(DataTypes.BLOB, java.util.Optional.of(buffer))));
        checks.add(() -> assertBuffer(BYTES, boundWithKey(DataTypes.BLOB, com.landawn.abacus.util.u.Nullable.of(BYTES))));
        checks.add(() -> assertBuffer(BYTES, boundWithKey(DataTypes.BLOB, com.landawn.abacus.util.Holder.of(BASE64))));
        // A present value without a blob form is bound unchanged, in its wrapper, for the driver to reject.
        checks.add(() -> assertSame(present, boundWithKey(DataTypes.BLOB, present)));
        // Pin: an empty wrapper for another column type binds NULL, as before.
        checks.add(() -> assertEquals(null, boundWithKey(DataTypes.TEXT, java.util.Optional.empty())));
        // An entity insert with empty Optional<ByteBuffer> and Nullable<byte[]> properties binds NULL for both.
        checks.add(() -> {
            try (Fixture fixture = new Fixture(new String[] { "id", "data", "bytes" }, DataTypes.INT, DataTypes.BLOB, DataTypes.BLOB)) {
                final SliceHOptionalBlobs entity = new SliceHOptionalBlobs();
                entity.setId(1);
                fixture.executor.insert(entity);

                assertEquals(Arrays.asList(1, null, null), Arrays.asList(fixture.binds.get(0)));
            }
        });
        assertAll(checks);
    }

    // r2H (second-pass review): the remaining branches of the final blob-column bind rule (every other wrapper kind of wrappedValue,
    // the built-in gate for enums, the position guard's boundary). A present wrapper's content follows the
    // rules of a bare value: an application value is converted by its registered converter, one without a converter stays in its
    // wrapper (the driver rejects it), text that is not base64 is rejected, a nested wrapper is unwrapped, and a primitive Optional's
    // number has no blob form. A converter's EMPTY buffer (position 0) is an empty blob and is bound; only a buffer with bytes before
    // its position and none after it (N.convert's decoding of the value's text) makes the value be bound unchanged.
    @Test
    void r2H_wrapperContentAndConverterBuffersFollowTheBareValueRules() {
        example.cassandra.BlobFixtures.register();
        final ByteBuffer buffer = ByteBuffer.wrap(BYTES);
        final java.util.Optional<Object> noConverter = java.util.Optional.of(com.datastax.oss.driver.api.core.data.CqlDuration.from("1h"));
        final List<Executable> checks = new ArrayList<>();

        // The wrapper kinds the optional-like test above does not cover: empty -> NULL; present without a blob form -> the wrapper.
        for (final Object empty : new Object[] { java.util.OptionalLong.empty(), java.util.OptionalDouble.empty(),
                com.landawn.abacus.util.u.OptionalBoolean.empty(), com.landawn.abacus.util.u.OptionalChar.empty(),
                com.landawn.abacus.util.u.OptionalByte.empty(), com.landawn.abacus.util.u.OptionalShort.empty(), com.landawn.abacus.util.u.OptionalLong.empty(),
                com.landawn.abacus.util.u.OptionalFloat.empty(), com.landawn.abacus.util.u.OptionalDouble.empty(),
                java.util.Optional.of(java.util.Optional.empty()) }) {
            checks.add(() -> assertEquals(null, boundWithKey(DataTypes.BLOB, empty), String.valueOf(empty)));
        }

        for (final Object present : new Object[] { java.util.OptionalInt.of(5), java.util.OptionalLong.of(1234), java.util.OptionalDouble.of(1.5),
                com.landawn.abacus.util.u.OptionalBoolean.of(true), com.landawn.abacus.util.u.OptionalChar.of('A'),
                com.landawn.abacus.util.u.OptionalByte.of((byte) 5), com.landawn.abacus.util.u.OptionalShort.of((short) 1234),
                com.landawn.abacus.util.u.OptionalLong.of(1234), com.landawn.abacus.util.u.OptionalFloat.of(1.5f),
                com.landawn.abacus.util.u.OptionalDouble.of(1.5), noConverter }) {
            checks.add(() -> assertSame(present, boundWithKey(DataTypes.BLOB, present), String.valueOf(present)));
        }

        // A JDK enum whose name is valid base64 ("NEW") is built-in and kept; a driver enum ("ANY") is an application class, converted
        // from its text and then kept by the position guard (HEAD bound both as an EMPTY blob).
        for (final Object value : new Object[] { Thread.State.NEW, com.datastax.oss.driver.api.core.DefaultConsistencyLevel.ANY }) {
            checks.add(() -> assertSame(value, boundWithKey(DataTypes.BLOB, value), String.valueOf(value)));
        }

        checks.add(() -> assertBuffer(BYTES, boundWithKey(DataTypes.BLOB, java.util.Optional.of(new example.cassandra.BlobFixtures.BlobBox(BYTES)))));
        checks.add(() -> assertSame(buffer, boundWithKey(DataTypes.BLOB, java.util.Optional.of(java.util.Optional.of(buffer)))));
        checks.add(() -> {
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> boundWithKey(DataTypes.BLOB, com.landawn.abacus.util.u.Nullable.of("not base64!")));
            assertTrue(e.getMessage().contains("parameter 'v' (index 0)") && e.getMessage().contains("not valid base64"), e.getMessage());
        });
        checks.add(() -> assertBuffer(new byte[0], boundWithKey(DataTypes.BLOB, new example.cassandra.BlobFixtures.EmptyBox())));
        assertAll(checks);
    }

    // r2H (second-pass review): TypeFactory.registerType(Class, Function, Function) builds the Type itself (an anonymous class in
    // com.landawn.abacus.type) that converts a value from its text only; for a blob that text is the empty base64 of the bytes before
    // the buffer's position, which HEAD handed to the from-string function (a TextBox of ""). Such a Type counts as an abacus one, so
    // the blob is rejected; only a Type class of the application's own (TypedBox, above) receives the buffer. A Serializable target,
    // which a String satisfies, gets the base64 text as a CharSequence does (HEAD: "").
    @Test
    void r2H_aBlobIsRejectedForATypeRegisteredFromStringFunctions() {
        example.cassandra.BlobFixtures.register();

        assertAll(
                () -> assertBlobRejected(
                        () -> CassandraExecutor.toList(rows(DataTypes.BLOB, ByteBuffer.wrap(BYTES)), example.cassandra.BlobFixtures.TextBox.class), "TextBox"),
                () -> assertBlobRejected(() -> CassandraExecutor.toEntity(rows(DataTypes.BLOB, ByteBuffer.wrap(BYTES)).one(), R2HTextBoxBean.class),
                        "TextBox property"),
                // Pins: that Type still converts text, and a Serializable target gets the blob's base64 text.
                () -> assertEquals("abc", CassandraExecutor.toList(rows(DataTypes.TEXT, "abc"), example.cassandra.BlobFixtures.TextBox.class).get(0).text()),
                () -> assertEquals(BASE64, CassandraExecutor.toList(rows(DataTypes.BLOB, ByteBuffer.wrap(BYTES)), java.io.Serializable.class).get(0)));
    }

    // r2X (second-pass cross-check): a row class the factories cannot create and N.convert cannot copy their stand-in into (EnumMap,
    // whose keys must be enums; AbstractQueue) fails with IllegalArgumentException from asRowClass, through toList (createRowMapper)
    // and findFirst (readRow), where HEAD returned the HashMap / LinkedList stand-in typed as that class. Same as BigQuery/DynamoDB.
    @Test
    void r2X_aRowClassTheStandInCannotBecomeFailsWithIllegalArgumentException() {
        final String[] names = { "name", "age" };
        final Object[][] values = { { "Ada", "36" } };

        assertAll(
                () -> assertThrows(IllegalArgumentException.class,
                        () -> CassandraExecutor.toList(rows(names, DataTypes.TEXT, values), java.util.EnumMap.class)),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> CassandraExecutor.toList(rows(names, DataTypes.TEXT, values), java.util.AbstractQueue.class)),
                () -> {
                    try (Fixture fixture = new Fixture(DataTypes.TEXT)) {
                        when(fixture.session.execute(any(Statement.class))).thenAnswer(invocation -> rows(names, DataTypes.TEXT, values));
                        assertThrows(IllegalArgumentException.class, () -> fixture.executor.findFirst(java.util.EnumMap.class, QUERY));
                        assertThrows(IllegalArgumentException.class, () -> fixture.executor.findFirst(java.util.AbstractQueue.class, QUERY));
                    }
                },
                // Pin: a mutable row class keeps the factory's own instance.
                () -> assertEquals(java.util.LinkedList.class,
                        CassandraExecutor.toList(rows(names, DataTypes.TEXT, values), java.util.LinkedList.class).get(0).getClass()));
    }

    // ---- 2026-10-27 fixD ----

    // fixD (decision G): the blob-column bind rule binds a present Optional-like value's content (an Optional<ByteBuffer>, a
    // Nullable<byte[]>, a Holder<ByteBuffer>, the base64 text of an Optional<String>), but a blob read into such a property was
    // rejected with IllegalArgumentException (HEAD: an EMPTY value), so an entity written through that rule could not be read back.
    // The blob is now converted to the wrapper's declared element type by the top-level blob rules and wrapped; NULL stays empty.
    @Test
    void fixD_optionalLikePropertiesReadBackTheBlobTheirValueWasBoundFrom() {
        final String[] names = { "id", "data", "bytes", "holder", "text" };
        final Object[] bound;

        try (Fixture fixture = new Fixture(names, DataTypes.INT, DataTypes.BLOB, DataTypes.BLOB, DataTypes.BLOB, DataTypes.BLOB)) {
            final FixDOptionalBlobs entity = new FixDOptionalBlobs();
            entity.setId(1);
            entity.setData(java.util.Optional.of(ByteBuffer.wrap(BYTES)));
            entity.setBytes(com.landawn.abacus.util.u.Nullable.of(BYTES));
            entity.setHolder(com.landawn.abacus.util.Holder.of(ByteBuffer.wrap(BYTES)));
            entity.setText(com.landawn.abacus.util.u.Optional.of(BASE64));
            fixture.executor.insert(entity);
            bound = fixture.binds.get(0);
        }

        final FixDOptionalBlobs read = CassandraExecutor.toEntity(rows(names, DataTypes.BLOB, new Object[][] { bound }).one(), FixDOptionalBlobs.class);
        final FixDOptionalBlobs nulls = CassandraExecutor.toEntity(rows(names, DataTypes.BLOB, new Object[][] { { 1, null, null, null, null } }).one(),
                FixDOptionalBlobs.class);

        assertAll(() -> assertEquals(1, bound[0]), () -> assertBuffer(BYTES, bound[1]), () -> assertBuffer(BYTES, bound[2]),
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
    // u.OptionalLong, which hold no object) stay rejected.
    @Test
    void fixD_everyReadPathWrapsTheBlobConvertedToTheDeclaredElementType() {
        final ByteBuffer buffer = ByteBuffer.wrap(BYTES);
        final CassandraExecutor.UDTCodec<Map> mapCodec = CassandraExecutor.UDTCodec.create(fieldType(DataTypes.BLOB), Map.class);
        final CassandraExecutor.UDTCodec<FixDUdtBytes> udtCodec = CassandraExecutor.UDTCodec.create(fieldType(DataTypes.BLOB), FixDUdtBytes.class);
        final List<Executable> checks = new ArrayList<>();

        checks.add(
                () -> assertBuffer(BYTES, CassandraExecutor.toEntity(rows(new String[] { "nested.data" }, DataTypes.BLOB, new Object[][] { { buffer } }).one(),
                        FixDOptionalBlobsHolder.class).nested.getData().get()));
        checks.add(() -> assertArrayEquals(BYTES, udtCodec.decode(mapCodec.encode(Map.of("v", BYTES), ProtocolVersion.V4), ProtocolVersion.V4).v.get()));
        checks.add(() -> {
            final FixDWrapperElements e = CassandraExecutor
                    .toEntity(
                            rows(new String[] { "buffers", "texts" }, DataTypes.listOf(DataTypes.BLOB),
                                    new Object[][] { { new ArrayList<>(List.of(buffer)), new ArrayList<>(List.of(buffer)) } }).one(),
                            FixDWrapperElements.class);
            assertSame(buffer, e.buffers.get(0).get());
            assertEquals(com.landawn.abacus.util.u.Nullable.of(BASE64), e.texts.get(0));
        });
        checks.add(() -> {
            final FixDNestedWrappers n = CassandraExecutor.toEntity(
                    rows(new String[] { "nested", "list", "builder" }, DataTypes.BLOB, new Object[][] { { buffer, buffer, buffer } }).one(),
                    FixDNestedWrappers.class);
            assertArrayEquals(BYTES, n.nested.get().get());
            assertArrayEquals(BYTES, n.list.get().get(0));
            assertInstanceOf(StringBuilder.class, n.builder.get());
            assertEquals(BASE64, n.builder.get().toString());
        });
        checks.add(() -> assertEquals(List.of(java.util.Optional.of(BASE64)),
                CassandraExecutor.extractData(rows(DataTypes.BLOB, buffer), FixDOptionalTextBean.class).getColumn("v")));
        // No declared element type: the wrapper holds the buffer itself.
        checks.add(() -> assertSame(buffer, CassandraExecutor.toList(rows(DataTypes.BLOB, buffer), java.util.Optional.class).get(0).get()));
        checks.add(() -> assertSame(buffer, CassandraExecutor.toList(rows(DataTypes.BLOB, buffer), com.landawn.abacus.util.u.Optional.class).get(0).get()));
        checks.add(() -> assertSame(buffer, CassandraExecutor.toList(rows(DataTypes.BLOB, buffer), com.landawn.abacus.util.u.Nullable.class).get(0).get()));
        checks.add(() -> assertSame(buffer, CassandraExecutor.toList(rows(DataTypes.BLOB, buffer), com.landawn.abacus.util.Holder.class).get(0).value()));
        checks.add(() -> {
            try (Fixture fixture = new Fixture(DataTypes.BLOB)) {
                when(fixture.session.execute(any(Statement.class))).thenAnswer(invocation -> rows(DataTypes.BLOB, buffer));
                assertSame(buffer, fixture.executor.queryForSingleValue(java.util.Optional.class, QUERY).get().get());
            }
        });

        // Rejections: no blob form for the element type, or a primitive Optional.
        for (final String property : new String[] { "longs", "optionalInt", "optionalLong" }) {
            checks.add(() -> assertBlobRejected(
                    () -> CassandraExecutor.toEntity(rows(new String[] { property }, DataTypes.BLOB, new Object[][] { { buffer } }).one(), FixDRejected.class),
                    property));
        }

        checks.add(() -> assertBlobRejected(() -> CassandraExecutor.toEntity(
                rows(new String[] { "longList" }, DataTypes.listOf(DataTypes.BLOB), new Object[][] { { new ArrayList<>(List.of(buffer)) } }).one(),
                FixDRejected.class), "List<Optional<Long>>"));
        checks.add(() -> assertBlobRejected(() -> CassandraExecutor.extractData(rows(DataTypes.BLOB, buffer), FixDOptionalLongBean.class), "Dataset"));
        checks.add(() -> assertBlobRejected(() -> CassandraExecutor.toList(rows(DataTypes.BLOB, buffer), java.util.OptionalInt.class), "OptionalInt.class"));
        assertAll(checks);
    }

    // fixD (finding D): the blob read rule gave the base64 text to a target that a String is an instance of (CharSequence,
    // Serializable, ...), but rejected a StringBuilder or StringBuffer, which are CharSequences too (HEAD: an empty one). They now get
    // the base64 text in their own type; a CharSequence class abacus does not build from text (CharBuffer) stays rejected. A target the
    // buffer itself is (Comparable, Object) keeps the buffer.
    @Test
    void fixD_aBlobReadIntoAStringBuilderOrStringBufferIsItsBase64Text() {
        final ByteBuffer buffer = ByteBuffer.wrap(BYTES);
        final List<Executable> checks = new ArrayList<>();

        checks.add(() -> {
            final StringBuilder builder = CassandraExecutor.toList(rows(DataTypes.BLOB, buffer), StringBuilder.class).get(0);
            assertEquals(BASE64, builder.toString());
            assertEquals(0, buffer.position());
        });
        checks.add(() -> assertEquals(BASE64, CassandraExecutor.toList(rows(DataTypes.BLOB, buffer), StringBuffer.class).get(0).toString()));
        checks.add(() -> {
            final FixDBuilders b = CassandraExecutor
                    .toEntity(rows(new String[] { "builder", "buffer" }, DataTypes.BLOB, new Object[][] { { buffer, buffer } }).one(), FixDBuilders.class);
            assertEquals(BASE64, b.builder.toString());
            assertEquals(BASE64, b.buffer.toString());
        });
        checks.add(() -> {
            try (Fixture fixture = new Fixture(DataTypes.BLOB)) {
                when(fixture.session.execute(any(Statement.class))).thenAnswer(invocation -> rows(DataTypes.BLOB, buffer));
                assertEquals(BASE64, fixture.executor.queryForSingleValue(StringBuilder.class, QUERY).get().toString());
            }
        });
        // Pins: a String-satisfied target gets the base64 String; a type the buffer is keeps it; CharBuffer has no blob form.
        checks.add(() -> assertEquals(BASE64, CassandraExecutor.toList(rows(DataTypes.BLOB, buffer), java.io.Serializable.class).get(0)));
        checks.add(() -> assertEquals(BASE64, CassandraExecutor.toList(rows(DataTypes.BLOB, buffer), CharSequence.class).get(0)));
        checks.add(() -> assertSame(buffer, CassandraExecutor.toList(rows(DataTypes.BLOB, buffer), Comparable.class).get(0)));
        checks.add(() -> assertBlobRejected(() -> CassandraExecutor.toList(rows(DataTypes.BLOB, buffer), java.nio.CharBuffer.class), "CharBuffer"));
        assertAll(checks);
    }

    // vfixD: extractData took a wrapper property's element type from the getter's generic return type alone, which keeps the type
    // variable of a generic superclass (Optional<T> on a class extending a Base<String>) and has none for a raw Optional getter of an
    // Optional<String> field, so its Dataset column held an Optional of the buffer where toEntity holds the Optional<String> of the
    // base64 text (and an Optional<Long> declared through Base<Long> was not rejected). It now uses the property's type, as toEntity
    // does; a property whose type is of another class than its getter's (@Type("String") on an Optional getter) keeps the getter's
    // own type, so the column still holds values of the getter's class.
    @Test
    void fixD_extractDataConvertsABlobForTheWrapperTypeThePropertyDeclares() {
        final ByteBuffer buffer = ByteBuffer.wrap(BYTES);
        final List<Executable> checks = new ArrayList<>();

        for (final Class<?> beanClass : new Class<?>[] { FixDGenericText.class, FixDRawGetter.class }) {
            checks.add(() -> assertEquals(List.of(java.util.Optional.of(BASE64)),
                    CassandraExecutor.extractData(rows(DataTypes.BLOB, buffer), beanClass).getColumn("v"), beanClass.getSimpleName()));
        }

        checks.add(() -> assertBlobRejected(() -> CassandraExecutor.extractData(rows(DataTypes.BLOB, buffer), FixDGenericLong.class),
                "Optional<Long> via Base<Long>"));
        // Pins: toEntity reads the same value; a getter whose property type is of another class keeps the getter's declared type.
        checks.add(() -> assertEquals(java.util.Optional.of(BASE64),
                CassandraExecutor.toEntity(rows(DataTypes.BLOB, buffer).one(), FixDGenericText.class).getV()));
        checks.add(() -> assertEquals(List.of(java.util.Optional.of(BASE64)),
                CassandraExecutor.extractData(rows(DataTypes.BLOB, buffer), FixDTypedGetter.class).getColumn("v")));
        assertAll(checks);
    }

    // sbfix (outside review, issue 2): a blob reads into a StringBuilder or StringBuffer as its base64 text (fixD), but binding one
    // back to a blob column (or a custom-type column, or a blob UDT field) left it unchanged, so the driver rejected it
    // (CodecNotFoundException): only a String was decoded. Such text is now decoded like a String, in a wrapper too, and text that
    // is not base64 is rejected the same way. A CharBuffer, which abacus does not build from text, still has no blob form.
    @Test
    void sbfix_aStringBuilderOrStringBufferBoundToABlobIsDecodedLikeAString() {
        final List<Executable> checks = new ArrayList<>();

        for (final DataType type : new DataType[] { DataTypes.BLOB, DataTypes.custom("org.apache.cassandra.db.marshal.BytesType") }) {
            final String name = type.asCql(false, true);
            checks.add(() -> assertBuffer(BYTES, boundValue(type, new StringBuilder(BASE64))));
            checks.add(() -> assertBuffer(BYTES, boundValue(type, new StringBuffer(BASE64))));
            checks.add(() -> assertBuffer(new byte[0], boundValue(type, new StringBuilder())));
            checks.add(() -> assertBuffer(BYTES, boundWithKey(type, java.util.Optional.of(new StringBuilder(BASE64)))));
            checks.add(() -> assertBuffer(BYTES, boundWithKey(type, com.landawn.abacus.util.u.Nullable.of(new StringBuffer(BASE64)))));
            checks.add(() -> assertBuffer(BYTES, boundWithKey(type, com.landawn.abacus.util.Holder.of(new StringBuilder(BASE64)))));
            checks.add(() -> assertBuffer(BYTES, boundWithKey(type, com.landawn.abacus.util.u.Optional.of(new StringBuffer(BASE64)))));
            // Pin: a CharBuffer is bound unchanged, in a wrapper too.
            checks.add(() -> {
                final java.nio.CharBuffer chars = java.nio.CharBuffer.wrap(BASE64);
                assertSame(chars, boundValue(type, chars), name);
            });
            checks.add(() -> {
                final java.util.Optional<java.nio.CharBuffer> wrapped = java.util.Optional.of(java.nio.CharBuffer.wrap(BASE64));
                assertSame(wrapped, boundWithKey(type, wrapped), name);
            });
        }

        // Positional, collection and named parameters all decode it; the value itself is not changed.
        checks.add(() -> {
            final StringBuilder builder = new StringBuilder(BASE64);

            try (Fixture fixture = new Fixture(DataTypes.BLOB)) {
                fixture.executor.execute(UPDATE, builder);
                fixture.executor.execute(UPDATE, List.of(new StringBuffer(BASE64)));
                fixture.executor.execute("UPDATE t SET v = :v WHERE id = 1", N.asMap("v", new StringBuilder(BASE64)));

                assertEquals(3, fixture.binds.size());
                fixture.binds.forEach(bind -> assertBuffer(BYTES, bind[0]));
            }

            assertEquals(BASE64, builder.toString());
        });

        // Text that is not base64 is rejected with the parameter named, bare or in a wrapper, before anything is bound.
        for (final Object invalid : new Object[] { new StringBuilder("not base64!"), new StringBuffer("AQ=D"), java.util.Optional.of(new StringBuilder("#")),
                com.landawn.abacus.util.Holder.of(new StringBuffer("#")) }) {
            checks.add(() -> {
                try (Fixture fixture = new Fixture(new String[] { "v", "id" }, DataTypes.BLOB, DataTypes.INT)) {
                    final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                            () -> fixture.executor.execute("UPDATE t SET v = ? WHERE id = ?", invalid, 1), invalid.toString());
                    assertTrue(e.getMessage().contains("parameter 'v' (index 0)"), e.getMessage());
                    assertTrue(e.getMessage().contains("not valid base64"), e.getMessage());
                    assertEquals(0, fixture.binds.size());
                }
            });
        }

        // Round trip: a blob read into StringBuilder/StringBuffer properties binds back to the same bytes on insert.
        checks.add(() -> {
            final ByteBuffer buffer = ByteBuffer.wrap(BYTES);
            final SbFixTextBlobs entity = CassandraExecutor
                    .toEntity(rows(new String[] { "builder", "buffer" }, DataTypes.BLOB, new Object[][] { { buffer, buffer } }).one(), SbFixTextBlobs.class);
            assertEquals(BASE64, entity.getBuilder().toString());
            assertEquals(BASE64, entity.getBuffer().toString());
            entity.setId(1);

            try (Fixture fixture = new Fixture(new String[] { "id", "builder", "buffer" }, DataTypes.INT, DataTypes.BLOB, DataTypes.BLOB)) {
                fixture.executor.insert(entity);

                final Object[] bind = fixture.binds.get(0);
                assertEquals(1, bind[0]);
                assertBuffer(BYTES, bind[1]);
                assertBuffer(BYTES, bind[2]);
            }
        });
        assertAll(checks);
    }

    // sbfix: a StringBuilder or StringBuffer written to a blob UDT field is decoded as a String is (it failed with
    // CodecNotFoundException); invalid text is rejected naming the field.
    @Test
    void sbfix_aStringBuilderOrStringBufferWrittenToABlobUdtFieldIsDecoded() {
        final UserDefinedType blobType = fieldType(DataTypes.BLOB);
        final CassandraExecutor.UDTCodec<Map> mapCodec = CassandraExecutor.UDTCodec.create(blobType, Map.class);
        final CassandraExecutor.UDTCodec<SbFixBuilderField> beanCodec = CassandraExecutor.UDTCodec.create(blobType, SbFixBuilderField.class);

        assertAll(
                () -> assertEquals(ByteBuffer.wrap(BYTES),
                        mapCodec.decode(mapCodec.encode(Map.of("v", new StringBuilder(BASE64)), ProtocolVersion.V4), ProtocolVersion.V4).get("v")),
                () -> assertEquals(ByteBuffer.wrap(BYTES),
                        mapCodec.decode(mapCodec.encode(Map.of("v", new StringBuffer(BASE64)), ProtocolVersion.V4), ProtocolVersion.V4).get("v")),
                () -> {
                    final SbFixBuilderField bean = new SbFixBuilderField();
                    bean.setV(new StringBuilder(BASE64));
                    assertEquals(ByteBuffer.wrap(BYTES), mapCodec.decode(beanCodec.encode(bean, ProtocolVersion.V4), ProtocolVersion.V4).get("v"));
                    // Round trip: the field reads back into the StringBuilder property as the same text.
                    assertEquals(BASE64, beanCodec.decode(beanCodec.encode(bean, ProtocolVersion.V4), ProtocolVersion.V4).getV().toString());
                }, () -> {
                    final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                            () -> mapCodec.encode(Map.of("v", new StringBuffer("not base64!")), ProtocolVersion.V4));
                    assertTrue(e.getMessage().contains("UDT field 'v' (index 0)"), e.getMessage());
                    assertTrue(e.getMessage().contains("not valid base64"), e.getMessage());
                },
                // Pin: a CharBuffer still has no blob form.
                () -> assertThrows(com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException.class,
                        () -> mapCodec.encode(Map.of("v", java.nio.CharBuffer.wrap(BASE64)), ProtocolVersion.V4)));
    }

    // sbfix (outside review): the rejection message for a blob read into a target without a blob form claimed a complete list of
    // the targets a blob converts to ("converts only to byte[], ByteBuffer or String"; then a longer list that still missed
    // collections, wrappers of containers, Object/Comparable and application types). It now names the rejected type and gives
    // examples without claiming the list is complete; the class documentation has the full rule.
    @Test
    void sbfix_theBlobRejectionMessageNamesTheTypeAndGivesExamples() {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> CassandraExecutor.toList(rows(DataTypes.BLOB, ByteBuffer.wrap(BYTES)), Long.class));

        assertAll(
                () -> assertTrue(e.getMessage().contains("Cannot convert a blob (ByteBuffer of 3 bytes) to java.lang.Long: that type has no blob form"),
                        e.getMessage()),
                () -> assertTrue(e.getMessage()
                        .contains("for example, as byte[], ByteBuffer or its base64 text, alone or in a collection or an Optional," + " Nullable or Holder"),
                        e.getMessage()),
                () -> org.junit.jupiter.api.Assertions.assertFalse(e.getMessage().contains("only"), e.getMessage()));
    }

    // wrapcodec (outside review): codec acceptance was checked only on the wrapper, and a present Optional/Nullable/Holder was then
    // converted by abacus rules alone. An Optional<WrapPayload> was therefore bound as the wrapper to a blob or custom-type column (the
    // driver has no codec for it: CodecNotFoundException) although a registered TypeCodec<WrapPayload> binds the bare value, and for
    // a text column it was silently stored as abacus's text of the value (JSON: {"n": 7}) instead of the codec's ("P:7"). The
    // content is now offered to the codecs first, for every column type and through nested wrappers.
    @Test
    void wrapcodec_aWrappedValueBindsThroughTheCodecRegisteredForItsContent() {
        final DataType custom = DataTypes.custom("com.example.PayloadType");
        final MutableCodecRegistry registry = payloadRegistry(custom);
        final WrapPayload payload = new WrapPayload(7);
        final List<Executable> checks = new ArrayList<>();

        for (final DataType type : new DataType[] { DataTypes.BLOB, custom, DataTypes.TEXT }) {
            final String name = type.asCql(false, true);
            // Pin: the bare value was always bound as is, for its codec.
            checks.add(() -> assertSame(payload, boundWithKey(registry, type, payload), "bare -> " + name));

            for (final Object wrapped : new Object[] { java.util.Optional.of(payload), com.landawn.abacus.util.u.Optional.of(payload),
                    com.landawn.abacus.util.u.Nullable.of(payload), com.landawn.abacus.util.Holder.of(payload),
                    java.util.Optional.of(com.landawn.abacus.util.u.Nullable.of(payload)) }) {
                checks.add(() -> assertSame(payload, boundWithKey(registry, type, wrapped), wrapped + " -> " + name));
            }
        }

        // The codec stores its own form, which is not abacus's text of the value (what the wrapped value was bound as before).
        checks.add(() -> assertEquals("P:7",
                TypeCodecs.TEXT.decode(registry.codecFor(DataTypes.TEXT, payload).encode(payload, ProtocolVersion.V4), ProtocolVersion.V4)));
        checks.add(() -> assertEquals("{\"n\": 7}", N.convert(payload, String.class)));
        // An entity insert binds an Optional<WrapPayload> property's content for its codec.
        checks.add(() -> {
            try (Fixture fixture = new Fixture(registry, new String[] { "id", "payload" }, DataTypes.INT, custom)) {
                final WrapPayloadEntity entity = new WrapPayloadEntity();
                entity.setId(1);
                entity.setPayload(java.util.Optional.of(payload));
                fixture.executor.insert(entity);

                assertEquals(1, fixture.binds.get(0)[0]);
                assertSame(payload, fixture.binds.get(0)[1]);
            }
        });
        // Pins: without a codec for the content, the wrapped value follows the bare value's rules: kept in its wrapper for a blob
        // column (the driver rejects it), converted by N.convert for a text column; an empty wrapper still binds NULL.
        checks.add(() -> {
            final java.util.Optional<WrapPayload> wrapped = java.util.Optional.of(payload);
            assertSame(wrapped, boundWithKey(DataTypes.BLOB, wrapped));
        });
        checks.add(() -> assertEquals("{\"n\": 7}", boundWithKey(DataTypes.TEXT, java.util.Optional.of(payload))));
        checks.add(() -> assertEquals(null, boundWithKey(registry, custom, java.util.Optional.empty())));
        checks.add(() -> assertEquals(null, boundWithKey(registry, DataTypes.TEXT, java.util.Optional.empty())));
        assertAll(checks);
    }

    // wrapcodec: a codec registered for the wrapper class itself is still asked first, so it keeps receiving the wrapper.
    @Test
    void wrapcodec_aCodecRegisteredForTheWrapperItselfStillTakesPrecedence() {
        final MutableCodecRegistry registry = payloadRegistry(DataTypes.custom("com.example.PayloadType"));
        registry.register(new MappingCodec<String, java.util.Optional<WrapPayload>>(TypeCodecs.TEXT, new GenericType<java.util.Optional<WrapPayload>>() {
        }) {
            @Override
            protected java.util.Optional<WrapPayload> innerToOuter(final String value) {
                return java.util.Optional.ofNullable(value == null ? null : new WrapPayload(Integer.parseInt(value.substring(4))));
            }

            @Override
            protected String outerToInner(final java.util.Optional<WrapPayload> value) {
                return value == null ? null : value.map(p -> "OPT:" + p.n()).orElse(null);
            }
        });
        final java.util.Optional<WrapPayload> wrapped = java.util.Optional.of(new WrapPayload(7));

        assertSame(wrapped, boundWithKey(registry, DataTypes.TEXT, wrapped));
    }

    // wrapcodec: a present wrapper's content goes through the same conversion rules as a bare value, so a wrapped number that the
    // column would change is rejected (Optional.of(1.5) was bound to an int column as 1; a bare 1.5 was already rejected).
    @Test
    void wrapcodec_aWrappedValueIsConvertedByTheBareValueRules() {
        final List<Executable> checks = new ArrayList<>();

        for (final Object[] lossy : new Object[][] { { DataTypes.INT, java.util.Optional.of(1.5), "Double value 1.5" },
                { DataTypes.INT, com.landawn.abacus.util.u.Nullable.of(Fraction.of(3, 2)), "Fraction value 3/2" },
                { DataTypes.BIGINT, java.util.Optional.of(java.util.Optional.of(2.5)), "Double value 2.5" },
                { DataTypes.FLOAT, com.landawn.abacus.util.Holder.of(1e300), "Double value 1.0E300" } }) {
            checks.add(() -> {
                final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> boundWithKey((DataType) lossy[0], lossy[1]),
                        lossy[1].toString());
                assertTrue(e.getMessage().contains("Cannot bind " + lossy[2]), e.getMessage());
                assertTrue(e.getMessage().contains("parameter 'v' (index 0)"), e.getMessage());
            });
        }

        // Pins: content that converts exactly, or is of the column's type, binds as before; content without a form for the column
        // keeps its wrapper (the driver rejects it); an empty wrapper is NULL for a scalar column.
        checks.add(() -> assertEquals(1.5, boundWithKey(DataTypes.DOUBLE, java.util.Optional.of(1.5))));
        checks.add(() -> assertEquals(5L, boundWithKey(DataTypes.BIGINT, java.util.Optional.of(5))));
        checks.add(() -> assertEquals(5, boundWithKey(DataTypes.INT, java.util.OptionalInt.of(5))));
        checks.add(() -> assertEquals("x", boundWithKey(DataTypes.TEXT, com.landawn.abacus.util.Holder.of("x"))));
        checks.add(() -> {
            final java.util.Optional<String> text = java.util.Optional.of("abc");
            assertSame(text, boundWithKey(DataTypes.INT, text));
        });
        checks.add(() -> assertEquals(null, boundWithKey(DataTypes.INT, java.util.Optional.empty())));
        checks.add(() -> assertEquals(null, boundWithKey(DataTypes.TEXT, com.landawn.abacus.util.u.Nullable.empty())));
        assertAll(checks);
    }

    // A registry with a WrapPayload codec for text (written "P:<n>"), blob and the given custom type (one byte, <n>).
    private static MutableCodecRegistry payloadRegistry(final DataType custom) {
        final MutableCodecRegistry registry = new DefaultCodecRegistry("wrapcodec");
        registry.register(new MappingCodec<String, WrapPayload>(TypeCodecs.TEXT, GenericType.of(WrapPayload.class)) {
            @Override
            protected WrapPayload innerToOuter(final String value) {
                return value == null ? null : new WrapPayload(Integer.parseInt(value.substring(2)));
            }

            @Override
            protected String outerToInner(final WrapPayload value) {
                return value == null ? null : "P:" + value.n();
            }
        }, payloadBlobCodec(TypeCodecs.BLOB), payloadBlobCodec(TypeCodecs.custom(custom)));
        return registry;
    }

    private static TypeCodec<WrapPayload> payloadBlobCodec(final TypeCodec<ByteBuffer> inner) {
        return new MappingCodec<ByteBuffer, WrapPayload>(inner, GenericType.of(WrapPayload.class)) {
            @Override
            protected WrapPayload innerToOuter(final ByteBuffer value) {
                return value == null ? null : new WrapPayload(value.duplicate().get());
            }

            @Override
            protected ByteBuffer outerToInner(final WrapPayload value) {
                return value == null ? null : ByteBuffer.wrap(new byte[] { (byte) value.n() });
            }
        };
    }

    private static Object boundWithKey(final CodecRegistry registry, final DataType type, final Object value) {
        try (Fixture fixture = new Fixture(registry, new String[] { "v", "id" }, type, DataTypes.INT)) {
            fixture.executor.execute("UPDATE t SET v = ? WHERE id = ?", value, 1);
            return fixture.binds.get(0)[0];
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
        private int id;
        private java.util.Optional<WrapPayload> payload = java.util.Optional.empty();

        public int getId() {
            return id;
        }

        public void setId(final int id) {
            this.id = id;
        }

        public java.util.Optional<WrapPayload> getPayload() {
            return payload;
        }

        public void setPayload(final java.util.Optional<WrapPayload> payload) {
            this.payload = payload;
        }
    }

    // The value bound for marker 'v' of "UPDATE t SET v = ? WHERE id = ?" (two markers, so a single wrapper is never a bean container).
    private static Object boundWithKey(final DataType type, final Object value) {
        try (Fixture fixture = new Fixture(new String[] { "v", "id" }, type, DataTypes.INT)) {
            fixture.executor.execute("UPDATE t SET v = ? WHERE id = ?", value, 1);
            return fixture.binds.get(0)[0];
        }
    }

    private static void assertBlobRejected(final Executable executable, final String message) {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, executable, message);
        assertTrue(e.getMessage().contains("Cannot convert a blob"), message + ": " + e.getMessage());
    }

    public static class SliceHLong {
        public Long v = 99L;
    }

    public static class SliceHInt {
        public int v = 99;
    }

    public static class SliceHUuid {
        public java.util.UUID v = java.util.UUID.randomUUID();
    }

    public static class SliceHText {
        public CharSequence v;
    }

    public static class SliceHLongHolder {
        public SliceHLong nested = new SliceHLong();
    }

    public static class SliceHLongBean {
        private Long v;

        public Long getV() {
            return v;
        }

        public void setV(final Long v) {
            this.v = v;
        }
    }

    public static class SliceHTypedBox {
        public example.cassandra.BlobFixtures.TypedBox v;
    }

    public static class SliceHTypedBoxes {
        public List<example.cassandra.BlobFixtures.TypedBox> v;
    }

    public static class SliceHBlobCollections {
        public List<byte[]> bytes;
        public List<String> texts;
        public Set<String> textSet;
        public List<ByteBuffer> buffers;
    }

    public static class SliceHBlobCollectionsHolder {
        public SliceHBlobCollections inner = new SliceHBlobCollections();
    }

    public static class SliceHNestedBlobs {
        public List<List<byte[]>> v;
    }

    public static class SliceHBlobLongs {
        public List<Long> v;
    }

    public static class SliceHUdtTexts {
        public List<String> v;
    }

    public static class SliceHOptionalElements {
        public List<java.util.Optional<Long>> list;
        public Map<String, java.util.Optional<Long>> map;
        public java.util.Optional<Long> single;
    }

    public static class SbFixTextBlobs {
        private int id;
        private StringBuilder builder;
        private StringBuffer buffer;

        public int getId() {
            return id;
        }

        public void setId(final int id) {
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

    public static class SliceHOptionalBlobs {
        private int id;
        private java.util.Optional<ByteBuffer> data = java.util.Optional.empty();
        private com.landawn.abacus.util.u.Nullable<byte[]> bytes = com.landawn.abacus.util.u.Nullable.empty();

        public int getId() {
            return id;
        }

        public void setId(final int id) {
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
    }

    public static class R2HTextBoxBean {
        public example.cassandra.BlobFixtures.TextBox v;
    }

    public static class FixDOptionalBlobs {
        private int id;
        private java.util.Optional<ByteBuffer> data = java.util.Optional.empty();
        private com.landawn.abacus.util.u.Nullable<byte[]> bytes = com.landawn.abacus.util.u.Nullable.empty();
        private com.landawn.abacus.util.Holder<ByteBuffer> holder = com.landawn.abacus.util.Holder.of(null);
        private com.landawn.abacus.util.u.Optional<String> text = com.landawn.abacus.util.u.Optional.empty();

        public int getId() {
            return id;
        }

        public void setId(final int id) {
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
