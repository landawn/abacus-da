package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.junit.jupiter.api.Test;

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
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.type.DefaultUserDefinedType;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import com.landawn.abacus.util.N;

/**
 * Driver 4 values whose {@code N.convert} conversion parses the value's {@code toString()} (abacus-common 8.2.1+): a
 * {@code timestamp} {@link Instant} read into a {@code LocalDate}/{@code LocalTime}/{@code OffsetTime}, a {@code date}
 * {@link LocalDate} read into an {@code Instant} or a {@code java.time} date-time, and a {@code LocalDate} bound to a
 * {@code timestamp} column all threw ({@code DateTimeParseException}, or "Conversion returned java.lang.String" for
 * {@code OffsetTime}); an {@code inet} {@link InetAddress} read into a {@code String}, or bound to a {@code text} column, became
 * {@code "/127.0.0.1"} ({@code InetAddress.toString()}). Expected values are computed in the JVM default zone, so the tests hold in
 * any zone. Driver objects are mocked; parameter and column conversion are real.
 */
class CassandraExecutorDeepReview2026TemporalInetConversionTest {

    // ---- 2026-10-24 sliceH ----

    private static final String QUERY = "SELECT v FROM events";
    private static final String UPDATE = "UPDATE events SET v = ? WHERE id = 1";
    private static final Instant INSTANT = Instant.parse("2023-11-14T22:13:20.123Z");
    private static final LocalDate DATE = LocalDate.of(2023, 11, 14);

    @Test
    void timestampColumnsReadIntoLocalDateLocalTimeAndOffsetTimeTargetsInTheDefaultZone() {
        final ZoneId zone = ZoneId.systemDefault();
        final LocalDate expectedDate = LocalDate.ofInstant(INSTANT, zone);
        final LocalTime expectedTime = LocalTime.ofInstant(INSTANT, zone);
        final OffsetTime expectedOffsetTime = OffsetTime.ofInstant(INSTANT, zone);

        try (Fixture fixture = new Fixture(DataTypes.TIMESTAMP)) {
            when(fixture.session.execute(any(Statement.class))).thenAnswer(invocation -> rows(DataTypes.TIMESTAMP, INSTANT, INSTANT));

            assertAll(() -> assertEquals(expectedDate, fixture.executor.findFirst(LocalDate.class, QUERY).get()),
                    () -> assertEquals(expectedTime, fixture.executor.findFirst(LocalTime.class, QUERY).get()),
                    () -> assertEquals(expectedOffsetTime, fixture.executor.findFirst(OffsetTime.class, QUERY).get()),
                    () -> assertEquals(expectedDate, fixture.executor.queryForSingleValue(LocalDate.class, QUERY).get()),
                    () -> assertEquals(expectedTime, fixture.executor.queryForSingleNonNull(LocalTime.class, QUERY).get()),
                    () -> assertEquals(List.of(expectedDate, expectedDate), fixture.executor.list(LocalDate.class, QUERY)),
                    () -> assertEquals(List.of(expectedTime, expectedTime),
                            CassandraExecutor.toList(rows(DataTypes.TIMESTAMP, INSTANT, INSTANT), LocalTime.class)),
                    () -> {
                        try (var stream = fixture.executor.stream(OffsetTime.class, QUERY)) {
                            assertEquals(List.of(expectedOffsetTime, expectedOffsetTime), stream.toList());
                        }
                    }, () -> assertEquals(expectedDate, ((LocalDate[]) fixture.executor.findFirst(LocalDate[].class, QUERY).get())[0]),
                    // The same date and time of day as the LocalDateTime read, and as the driver 3 executor's N.convert(Date, ...).
                    () -> assertEquals(LocalDateTime.ofInstant(INSTANT, zone).toLocalDate(), expectedDate),
                    () -> assertEquals(N.convert(Date.from(INSTANT), LocalDate.class), expectedDate),
                    () -> assertEquals(N.convert(Date.from(INSTANT), LocalTime.class), expectedTime),
                    // Bean properties: toEntity, findFirst, extractData's per-column conversion.
                    () -> {
                        final TimeParts bean = CassandraExecutor.toEntity(timeParts(INSTANT).one(), TimeParts.class);
                        assertEquals(expectedDate, bean.getDay());
                        assertEquals(expectedTime, bean.getClock());
                        assertEquals(expectedOffsetTime, bean.getOffsetClock());
                    },
                    () -> assertEquals(List.of(expectedDate, expectedDate),
                            CassandraExecutor.extractData(timeParts(INSTANT, INSTANT), TimeParts.class).getColumn("day")),
                    () -> assertEquals(List.of(expectedOffsetTime),
                            CassandraExecutor.extractData(timeParts(INSTANT), TimeParts.class).getColumn("offset_clock")),
                    () -> assertEquals(expectedTime, CassandraExecutor.toList(timeParts(INSTANT), TimeParts.class).get(0).getClock()));
        }
    }

    @Test
    void dateColumnsReadIntoInstantAndJavaTimeDateTimeTargetsAsTheStartOfThatDay() {
        final ZonedDateTime startOfDay = DATE.atStartOfDay(ZoneId.systemDefault());

        try (Fixture fixture = new Fixture(DataTypes.DATE)) {
            when(fixture.session.execute(any(Statement.class))).thenAnswer(invocation -> rows(DataTypes.DATE, DATE, DATE));

            assertAll(() -> assertEquals(DATE.atStartOfDay(), fixture.executor.findFirst(LocalDateTime.class, QUERY).get()),
                    () -> assertEquals(startOfDay.toInstant(), fixture.executor.findFirst(Instant.class, QUERY).get()),
                    () -> assertEquals(startOfDay, fixture.executor.queryForSingleValue(ZonedDateTime.class, QUERY).get()),
                    () -> assertEquals(startOfDay.toOffsetDateTime(), fixture.executor.queryForSingleNonNull(OffsetDateTime.class, QUERY).get()),
                    () -> assertEquals(List.of(DATE.atStartOfDay(), DATE.atStartOfDay()), fixture.executor.list(LocalDateTime.class, QUERY)),
                    () -> assertEquals(List.of(startOfDay.toInstant(), startOfDay.toInstant()),
                            CassandraExecutor.toList(rows(DataTypes.DATE, DATE, DATE), Instant.class)),
                    // The instant a Date target of the same column already received (N.convert(LocalDate, Date.class)).
                    () -> assertEquals(startOfDay.toInstant(), fixture.executor.findFirst(Date.class, QUERY).get().toInstant()), () -> {
                        final DateParts bean = CassandraExecutor.toEntity(dateParts(DATE).one(), DateParts.class);
                        assertEquals(DATE.atStartOfDay(), bean.getLocal());
                        assertEquals(startOfDay.toInstant(), bean.getInstant());
                        assertEquals(startOfDay, bean.getZoned());
                        assertEquals(startOfDay.toOffsetDateTime(), bean.getOffset());
                    },
                    () -> assertEquals(List.of(startOfDay.toInstant()), CassandraExecutor.extractData(dateParts(DATE), DateParts.class).getColumn("instant")),
                    () -> assertEquals(List.of(startOfDay), CassandraExecutor.extractData(dateParts(DATE), DateParts.class).getColumn("zoned")));
        }
    }

    @Test
    void localDateBoundToATimestampColumnIsTheStartOfThatDayInTheDefaultZone() {
        final Instant expected = DATE.atStartOfDay(ZoneId.systemDefault()).toInstant();

        try (Fixture fixture = new Fixture(DataTypes.TIMESTAMP)) {
            assertAll(() -> {
                fixture.executor.execute(UPDATE, DATE);
                assertEquals(expected, assertInstanceOf(Instant.class, fixture.lastBoundValue()));
            }, () -> {
                fixture.executor.execute(UPDATE, List.of(DATE));
                assertEquals(expected, fixture.lastBoundValue());
            }, () -> {
                fixture.executor.execute(UPDATE, N.asMap("v", DATE));
                assertEquals(expected, fixture.lastBoundValue());
            },
                    // The instant the driver 3 executor binds for it: N.convert(LocalDate, Date.class).
                    () -> assertEquals(N.convert(DATE, Date.class).toInstant(), expected));
        }
    }

    @Test
    void inetColumnsReadIntoStringTargetsAsTheAddressText() throws UnknownHostException {
        // getByAddress never resolves a name, so the driver-decoded value's toString() is "/127.0.0.1".
        final InetAddress v4 = InetAddress.getByAddress(new byte[] { 127, 0, 0, 1 });
        final InetAddress v6 = InetAddress.getByAddress(new byte[] { 0x20, 0x01, 0x0d, (byte) 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 });
        assertEquals("/127.0.0.1", v4.toString());

        try (Fixture fixture = new Fixture(DataTypes.INET)) {
            when(fixture.session.execute(any(Statement.class))).thenAnswer(invocation -> rows(DataTypes.INET, v4, v6));

            assertAll(() -> assertEquals("127.0.0.1", fixture.executor.findFirst(String.class, QUERY).get()),
                    () -> assertEquals("127.0.0.1", fixture.executor.queryForSingleValue(String.class, QUERY).get()),
                    () -> assertEquals("127.0.0.1", fixture.executor.queryForString(QUERY).get()),
                    () -> assertEquals(List.of("127.0.0.1", v6.getHostAddress()), fixture.executor.list(String.class, QUERY)),
                    () -> assertEquals("127.0.0.1", ((String[]) fixture.executor.findFirst(String[].class, QUERY).get())[0]),
                    () -> assertEquals(List.of("127.0.0.1"), CassandraExecutor.toList(rows(DataTypes.INET, v4), String.class)),
                    () -> assertEquals("127.0.0.1", CassandraExecutor.toEntity(rows(DataTypes.INET, v4).one(), Host.class).getV()),
                    () -> assertEquals(List.of("127.0.0.1", v6.getHostAddress()),
                            CassandraExecutor.extractData(rows(DataTypes.INET, v4, v6), Host.class).getColumn("v")),
                    // Pinned: an InetAddress target or property keeps the driver value.
                    () -> assertEquals(v4, fixture.executor.findFirst(InetAddress.class, QUERY).get()));
        }
    }

    @Test
    void inetAddressBoundToATextColumnIsTheAddressText() throws UnknownHostException {
        final InetAddress v4 = InetAddress.getByAddress(new byte[] { 127, 0, 0, 1 });

        try (Fixture fixture = new Fixture(DataTypes.TEXT)) {
            assertAll(() -> {
                fixture.executor.execute(UPDATE, v4);
                assertEquals("127.0.0.1", fixture.lastBoundValue());
            }, () -> {
                fixture.executor.execute(UPDATE, List.of(v4));
                assertEquals("127.0.0.1", fixture.lastBoundValue());
            });
        }
    }

    @Test
    void udtFieldsUseTheSameTemporalAndInetRules() throws UnknownHostException {
        final ZoneId zone = ZoneId.systemDefault();
        final UserDefinedType timestampType = fieldType(DataTypes.TIMESTAMP);
        final UserDefinedType dateType = fieldType(DataTypes.DATE);
        final UserDefinedType inetType = fieldType(DataTypes.INET);
        final CassandraExecutor.UDTCodec<Map> timestampMapCodec = CassandraExecutor.UDTCodec.create(timestampType, Map.class);
        final InetAddress v4 = InetAddress.getByAddress(new byte[] { 10, 1, 2, 3 });

        final ByteBuffer encodedInstant = timestampMapCodec.encode(Map.of("v", INSTANT), ProtocolVersion.V4);
        final CassandraExecutor.UDTCodec<DayValue> dayCodec = CassandraExecutor.UDTCodec.create(timestampType, DayValue.class);
        final CassandraExecutor.UDTCodec<ClockValue> clockCodec = CassandraExecutor.UDTCodec.create(timestampType, ClockValue.class);
        final CassandraExecutor.UDTCodec<LocalDateTimeValue> dateTimeCodec = CassandraExecutor.UDTCodec.create(dateType, LocalDateTimeValue.class);
        final CassandraExecutor.UDTCodec<Map> dateMapCodec = CassandraExecutor.UDTCodec.create(dateType, Map.class);
        final CassandraExecutor.UDTCodec<TextValue> textCodec = CassandraExecutor.UDTCodec.create(inetType, TextValue.class);
        final CassandraExecutor.UDTCodec<Map> inetMapCodec = CassandraExecutor.UDTCodec.create(inetType, Map.class);

        // A LocalDate written into a TIMESTAMP field converts as a bound value does (it failed with CodecNotFoundException).
        assertAll(
                () -> assertEquals(DATE.atStartOfDay(zone).toInstant(),
                        timestampMapCodec.decode(timestampMapCodec.encode(Map.of("v", DATE), ProtocolVersion.V4), ProtocolVersion.V4).get("v")),
                // A TIMESTAMP field read into LocalDate/LocalTime bean properties (DateTimeParseException before).
                () -> assertEquals(LocalDate.ofInstant(INSTANT, zone), dayCodec.decode(encodedInstant, ProtocolVersion.V4).getV()),
                () -> assertEquals(LocalTime.ofInstant(INSTANT, zone), clockCodec.decode(encodedInstant, ProtocolVersion.V4).getV()),
                // A DATE field read into a LocalDateTime property is the start of that day.
                () -> assertEquals(DATE.atStartOfDay(),
                        dateTimeCodec.decode(dateMapCodec.encode(Map.of("v", DATE), ProtocolVersion.V4), ProtocolVersion.V4).getV()),
                // An INET field read into a String property is the address text.
                () -> assertEquals("10.1.2.3", textCodec.decode(inetMapCodec.encode(Map.of("v", v4), ProtocolVersion.V4), ProtocolVersion.V4).getV()));
    }

    private static UserDefinedType fieldType(final DataType fieldType) {
        final CodecRegistry registry = new DefaultCodecRegistry("slice-h-udt");

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

    /** One row per instant, with the timestamp columns {@code day}, {@code clock} and {@code offset_clock} all holding it. */
    private static ResultSet timeParts(final Instant... values) {
        return rows(new String[] { "day", "clock", "offset_clock" }, DataTypes.TIMESTAMP,
                Arrays.stream(values).map(value -> new Object[] { value, value, value }).toArray(Object[][]::new));
    }

    /** One row per date, with the date columns {@code local}, {@code instant}, {@code zoned} and {@code offset} all holding it. */
    private static ResultSet dateParts(final LocalDate... values) {
        return rows(new String[] { "local", "instant", "zoned", "offset" }, DataTypes.DATE,
                Arrays.stream(values).map(value -> new Object[] { value, value, value, value }).toArray(Object[][]::new));
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

    public static class TimeParts {
        private LocalDate day;
        private LocalTime clock;
        private OffsetTime offsetClock;

        public LocalDate getDay() {
            return day;
        }

        public void setDay(final LocalDate day) {
            this.day = day;
        }

        public LocalTime getClock() {
            return clock;
        }

        public void setClock(final LocalTime clock) {
            this.clock = clock;
        }

        public OffsetTime getOffsetClock() {
            return offsetClock;
        }

        public void setOffsetClock(final OffsetTime offsetClock) {
            this.offsetClock = offsetClock;
        }
    }

    public static class DateParts {
        private LocalDateTime local;
        private Instant instant;
        private ZonedDateTime zoned;
        private OffsetDateTime offset;

        public LocalDateTime getLocal() {
            return local;
        }

        public void setLocal(final LocalDateTime local) {
            this.local = local;
        }

        public Instant getInstant() {
            return instant;
        }

        public void setInstant(final Instant instant) {
            this.instant = instant;
        }

        public ZonedDateTime getZoned() {
            return zoned;
        }

        public void setZoned(final ZonedDateTime zoned) {
            this.zoned = zoned;
        }

        public OffsetDateTime getOffset() {
            return offset;
        }

        public void setOffset(final OffsetDateTime offset) {
            this.offset = offset;
        }
    }

    public static class Host {
        private String v;

        public String getV() {
            return v;
        }

        public void setV(final String v) {
            this.v = v;
        }
    }

    public static class DayValue {
        private LocalDate v;

        public LocalDate getV() {
            return v;
        }

        public void setV(final LocalDate v) {
            this.v = v;
        }
    }

    public static class ClockValue {
        private LocalTime v;

        public LocalTime getV() {
            return v;
        }

        public void setV(final LocalTime v) {
            this.v = v;
        }
    }

    public static class LocalDateTimeValue {
        private LocalDateTime v;

        public LocalDateTime getV() {
            return v;
        }

        public void setV(final LocalDateTime v) {
            this.v = v;
        }
    }

    public static class TextValue {
        private String v;

        public String getV() {
            return v;
        }

        public void setV(final String v) {
            this.v = v;
        }
    }

    /** One bind variable {@code v} of the given type; every {@code bind(...)} call records the values it receives. */
    private static final class Fixture implements AutoCloseable {
        final CqlSession session = mock(CqlSession.class);
        final PreparedStatement prepared = mock(PreparedStatement.class);
        final List<Object[]> binds = new ArrayList<>();
        final CassandraExecutor executor;

        Fixture(final DataType variableType) {
            final DriverContext context = mock(DriverContext.class);
            when(session.getContext()).thenReturn(context);
            when(context.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT);
            final ColumnDefinitions variables = mock(ColumnDefinitions.class);
            final ColumnDefinition variable = mock(ColumnDefinition.class);
            when(variables.size()).thenReturn(1);
            when(variables.get(0)).thenReturn(variable);
            when(variable.getName()).thenReturn(CqlIdentifier.fromInternal("v"));
            when(variable.getType()).thenReturn(variableType);
            when(prepared.getVariableDefinitions()).thenReturn(variables);
            when(prepared.bind(any(Object[].class))).thenAnswer(invocation -> {
                binds.add(((Object[]) invocation.getRawArguments()[0]).clone());
                return mock(BoundStatement.class);
            });
            when(session.prepare(anyString())).thenReturn(prepared);
            when(session.execute(any(Statement.class))).thenReturn(mock(ResultSet.class));
            executor = new CassandraExecutor(session);
        }

        Object lastBoundValue() {
            final Object[] bind = binds.get(binds.size() - 1);
            assertEquals(1, bind.length);
            return bind[0];
        }

        @Override
        public void close() {
            executor.close();
        }
    }

    // ---- 2026-10-24 verify2 ----

    /**
     * A dotted (nested-path) column is stored through {@code BeanInfo.setPropValue(path)}, which skipped toPropValue: an inet
     * leaf got {@code "/127.0.0.1"}, a timestamp into a {@code LocalDate} leaf and a date into a {@code LocalDateTime} leaf threw
     * {@code DateTimeParseException}, and a BLOB into a {@code byte[]} leaf became {@code null}. The last property of the path
     * now converts as a top-level property does.
     */
    @Test
    void nestedPathColumnsConvertForTheLastPropertyAsTopLevelColumnsDo() throws UnknownHostException {
        final InetAddress v4 = InetAddress.getByAddress(new byte[] { 127, 0, 0, 1 });

        assertAll(() -> assertEquals("127.0.0.1", verify2Nested("nested.host", v4).getNested().getHost()),
                () -> assertEquals(LocalDate.ofInstant(INSTANT, ZoneId.systemDefault()), verify2Nested("nested.day", INSTANT).getNested().getDay()),
                () -> assertEquals(DATE.atStartOfDay(), verify2Nested("nested.local", DATE).getNested().getLocal()),
                () -> assertArrayEquals(new byte[] { 1, 2, 3 }, verify2Nested("nested.blob", ByteBuffer.wrap(new byte[] { 1, 2, 3 })).getNested().getBlob()),
                // An intermediate bean that already exists receives the converted value as well.
                () -> assertEquals("127.0.0.1", verify2Nested("preset.host", v4).getPreset().getHost()));
    }

    /** Pins: nested values that already converted, a NULL column, unresolvable paths and read-only path members behave as before. */
    @Test
    void nestedPathColumnsKeepTheirOtherBehavior() throws UnknownHostException {
        final InetAddress v4 = InetAddress.getByAddress(new byte[] { 127, 0, 0, 1 });

        assertAll(() -> assertEquals("n", verify2Nested("nested.name", "n").getNested().getName()),
                () -> assertEquals(7, verify2Nested("nested.count", 7).getNested().getCount()),
                () -> assertEquals(7, verify2Nested("nested.count", 7L).getNested().getCount()),
                // A null intermediate bean is created; a NULL column leaves the leaf null.
                () -> assertEquals(null, verify2Nested("nested.host", null).getNested().getHost()),
                // A path that does not resolve, or whose last property is read-only, is ignored without creating the intermediate bean.
                () -> assertEquals(null, verify2Nested("nested.nope", "x").getNested()),
                () -> assertEquals(null, verify2Nested("nested.computed", INSTANT).getNested()),
                // A read-only (getter-only) intermediate property whose value is null: the value is dropped without an exception.
                () -> assertEquals(null, verify2Nested("fixed.host", v4).getFixed()));
    }

    private static Verify2Holder verify2Nested(final String column, final Object value) {
        return CassandraExecutor.toEntity(rows(new String[] { column }, DataTypes.TEXT, new Object[][] { { value } }).one(), Verify2Holder.class);
    }

    public static class Verify2Holder {
        private Verify2Leaves nested;
        private Verify2Leaves preset = new Verify2Leaves();

        public Verify2Leaves getNested() {
            return nested;
        }

        public void setNested(final Verify2Leaves nested) {
            this.nested = nested;
        }

        public Verify2Leaves getPreset() {
            return preset;
        }

        public void setPreset(final Verify2Leaves preset) {
            this.preset = preset;
        }

        public Verify2Leaves getFixed() {
            return null;
        }
    }

    public static class Verify2Leaves {
        private String host;
        private LocalDate day;
        private LocalDateTime local;
        private byte[] blob;
        private String name;
        private int count;

        public String getHost() {
            return host;
        }

        public void setHost(final String host) {
            this.host = host;
        }

        public LocalDate getDay() {
            return day;
        }

        public void setDay(final LocalDate day) {
            this.day = day;
        }

        public LocalDateTime getLocal() {
            return local;
        }

        public void setLocal(final LocalDateTime local) {
            this.local = local;
        }

        public byte[] getBlob() {
            return blob;
        }

        public void setBlob(final byte[] blob) {
            this.blob = blob;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public int getCount() {
            return count;
        }

        public void setCount(final int count) {
            this.count = count;
        }

        public LocalDate getComputed() {
            return LocalDate.EPOCH;
        }
    }
}
