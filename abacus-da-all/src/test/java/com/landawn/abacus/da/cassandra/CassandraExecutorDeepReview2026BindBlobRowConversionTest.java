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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

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
}
