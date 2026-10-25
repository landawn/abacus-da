package com.landawn.abacus.da.cassandra.v3;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.landawn.abacus.util.N;

/**
 * Driver 3 values whose {@code N.convert} conversion (and so {@code PropInfo.setPropValue}'s) was wrong (v4 twin:
 * {@code cassandra.CassandraExecutorDeepReview2026TemporalInetConversionTest}): an {@code inet} {@link InetAddress} read into a
 * {@code String}, or bound to a {@code text} column, became {@code "/127.0.0.1"} ({@code InetAddress.toString()}); a {@code date}
 * value (the driver's {@link LocalDate}) read into a {@code java.time} date-time threw {@code DateTimeParseException}; a
 * {@code timestamp} value (a {@link Date}) read into an {@code OffsetTime} threw "Conversion returned java.lang.String". Expected
 * values are computed in the JVM default zone, so the tests hold in any zone. Driver objects are mocked; conversion is real.
 */
class CassandraV3DeepReview2026TemporalInetConversionTest {

    // ---- 2026-10-24 sliceK ----

    private static final String QUERY = "SELECT v FROM events";
    private static final String UPDATE = "UPDATE events SET v = ? WHERE id = 1";
    private static final LocalDate DATE = LocalDate.fromYearMonthDay(2023, 11, 14);
    private static final java.time.LocalDate DAY = java.time.LocalDate.of(2023, 11, 14);
    private static final Date TIMESTAMP = new Date(1_700_000_000_123L);

    @Test
    void inetColumnsReadIntoStringTargetsAsTheAddressText() throws UnknownHostException {
        // getByAddress never resolves a name, so the driver-decoded value's toString() is "/127.0.0.1".
        final InetAddress v4 = InetAddress.getByAddress(new byte[] { 127, 0, 0, 1 });
        final InetAddress v6 = InetAddress.getByAddress(new byte[] { 0x20, 0x01, 0x0d, (byte) 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 });
        assertEquals("/127.0.0.1", v4.toString());

        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            when(fixture.session.execute(any(Statement.class))).thenAnswer(invocation -> rows(DataType.inet(), v4, v6));
            when(fixture.session.executeAsync(any(Statement.class))).thenAnswer(invocation -> future(rows(DataType.inet(), v4, v6)));

            assertAll(() -> assertEquals("127.0.0.1", fixture.executor.findFirst(String.class, QUERY).get()),
                    () -> assertEquals("127.0.0.1", fixture.executor.queryForSingleValue(String.class, QUERY).get()),
                    () -> assertEquals("127.0.0.1", fixture.executor.queryForString(QUERY).get()),
                    () -> assertEquals(List.of("127.0.0.1", v6.getHostAddress()), fixture.executor.list(String.class, QUERY)),
                    () -> assertEquals("127.0.0.1", fixture.executor.findFirst(String[].class, QUERY).get()[0]),
                    () -> assertEquals("127.0.0.1", fixture.executor.async().findFirst(String.class, QUERY).get().get()),
                    () -> assertEquals(List.of("127.0.0.1"), CassandraExecutor.toList(rows(DataType.inet(), v4), String.class)),
                    () -> assertEquals("127.0.0.1", CassandraExecutor.toEntity(rows(DataType.inet(), v4).one(), Host.class).getV()),
                    () -> assertEquals(List.of("127.0.0.1", v6.getHostAddress()),
                            CassandraExecutor.extractData(rows(DataType.inet(), v4, v6), Host.class).getColumn("v")),
                    // Pinned: an InetAddress target keeps the driver value.
                    () -> assertSame(v4, fixture.executor.findFirst(InetAddress.class, QUERY).get()));
        }
    }

    @Test
    void inetAddressBoundToATextColumnIsTheAddressText() throws UnknownHostException {
        final InetAddress v4 = InetAddress.getByAddress(new byte[] { 127, 0, 0, 1 });

        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.columns(DataType.text(), "v");
            when(fixture.session.executeAsync(any(Statement.class))).thenAnswer(invocation -> future(mock(ResultSet.class)));

            fixture.executor.execute(UPDATE, v4);
            fixture.executor.execute(UPDATE, List.of(v4));
            fixture.executor.execute(UPDATE, N.asMap("v", v4));
            fixture.executor.async().execute(UPDATE, v4);

            assertAll(() -> assertEquals(4, fixture.values.size()), () -> assertEquals(List.of("127.0.0.1"), Arrays.asList(fixture.values.get(0))),
                    () -> assertEquals(List.of("127.0.0.1"), Arrays.asList(fixture.values.get(1))),
                    () -> assertEquals(List.of("127.0.0.1"), Arrays.asList(fixture.values.get(2))),
                    () -> assertEquals(List.of("127.0.0.1"), Arrays.asList(fixture.values.get(3))));
        }

        // Pinned: an InetAddress bound to an inet column is bound unchanged.
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.columns(DataType.inet(), "v");
            fixture.executor.execute(UPDATE, v4);
            assertSame(v4, fixture.values.get(0)[0]);
        }
    }

    @Test
    void dateColumnsReadIntoJavaTimeDateTimeTargetsAsTheStartOfThatDay() {
        final ZonedDateTime startOfDay = DAY.atStartOfDay(ZoneId.systemDefault());

        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            when(fixture.session.execute(any(Statement.class))).thenAnswer(invocation -> rows(DataType.date(), DATE, DATE));
            when(fixture.session.executeAsync(any(Statement.class))).thenAnswer(invocation -> future(rows(DataType.date(), DATE, DATE)));

            assertAll(() -> assertEquals(DAY.atStartOfDay(), fixture.executor.findFirst(LocalDateTime.class, QUERY).get()),
                    () -> assertEquals(startOfDay.toInstant(), fixture.executor.findFirst(Instant.class, QUERY).get()),
                    () -> assertEquals(startOfDay, fixture.executor.queryForSingleValue(ZonedDateTime.class, QUERY).get()),
                    () -> assertEquals(startOfDay.toOffsetDateTime(), fixture.executor.queryForSingleNonNull(OffsetDateTime.class, QUERY).get()),
                    () -> assertEquals(List.of(DAY.atStartOfDay(), DAY.atStartOfDay()), fixture.executor.list(LocalDateTime.class, QUERY)),
                    () -> assertEquals(startOfDay.toInstant(), fixture.executor.findFirst(Instant[].class, QUERY).get()[0]),
                    () -> assertEquals(startOfDay.toInstant(), fixture.executor.async().queryForSingleValue(Instant.class, QUERY).get().get()),
                    () -> assertEquals(List.of(startOfDay.toInstant(), startOfDay.toInstant()),
                            CassandraExecutor.toList(rows(DataType.date(), DATE, DATE), Instant.class)),
                    // The instant a Date target of the same column already received, and the unchanged java.time.LocalDate read.
                    () -> assertEquals(startOfDay.toInstant(), fixture.executor.findFirst(Date.class, QUERY).get().toInstant()),
                    () -> assertEquals(DAY, fixture.executor.findFirst(java.time.LocalDate.class, QUERY).get()), () -> {
                        final DateParts bean = CassandraExecutor.toEntity(dateParts(DATE).one(), DateParts.class);
                        assertEquals(DAY.atStartOfDay(), bean.getLocal());
                        assertEquals(startOfDay.toInstant(), bean.getInstant());
                        assertEquals(startOfDay, bean.getZoned());
                        assertEquals(startOfDay.toOffsetDateTime(), bean.getOffset());
                    },
                    () -> assertEquals(List.of(startOfDay.toInstant()), CassandraExecutor.extractData(dateParts(DATE), DateParts.class).getColumn("instant")),
                    () -> assertEquals(List.of(DAY.atStartOfDay()), CassandraExecutor.extractData(dateParts(DATE), DateParts.class).getColumn("local")),
                    () -> assertEquals(DAY.atStartOfDay(), CassandraExecutor.toList(dateParts(DATE), DateParts.class).get(0).getLocal()));
        }
    }

    @Test
    void dateBeforeTheGregorianCutoverUsesTheDriversProlepticDay() {
        // The driver's LocalDate is proleptic Gregorian, like java.time; the day comes from its epoch-day count.
        final LocalDate early = LocalDate.fromYearMonthDay(1000, 3, 1);
        final java.time.LocalDate expected = java.time.LocalDate.of(1000, 3, 1);

        assertAll(() -> assertEquals(expected.toEpochDay(), early.getDaysSinceEpoch()),
                () -> assertEquals(expected.atStartOfDay(), CassandraExecutor.toList(rows(DataType.date(), early), LocalDateTime.class).get(0)));
    }

    @Test
    void timestampColumnsReadIntoOffsetTimeTargetsInTheDefaultZone() {
        final OffsetTime expected = OffsetTime.ofInstant(TIMESTAMP.toInstant(), ZoneId.systemDefault());

        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            when(fixture.session.execute(any(Statement.class))).thenAnswer(invocation -> rows(DataType.timestamp(), TIMESTAMP, TIMESTAMP));

            assertAll(() -> assertEquals(expected, fixture.executor.findFirst(OffsetTime.class, QUERY).get()),
                    () -> assertEquals(expected, fixture.executor.queryForSingleValue(OffsetTime.class, QUERY).get()),
                    () -> assertEquals(List.of(expected, expected), fixture.executor.list(OffsetTime.class, QUERY)), () -> {
                        try (var stream = fixture.executor.stream(OffsetTime.class, QUERY)) {
                            assertEquals(List.of(expected, expected), stream.toList());
                        }
                    }, () -> assertEquals(expected, CassandraExecutor.toEntity(rows(DataType.timestamp(), TIMESTAMP).one(), Clock.class).getV()),
                    () -> assertEquals(List.of(expected), CassandraExecutor.extractData(rows(DataType.timestamp(), TIMESTAMP), Clock.class).getColumn("v")),
                    // The same time of day as the (unchanged) LocalTime read of that column.
                    () -> assertEquals(expected.toLocalTime(), fixture.executor.findFirst(LocalTime.class, QUERY).get()));
        }
    }

    @Test
    void udtBeanPropertiesUseTheSameDateTimestampAndInetRules() throws Exception {
        final InetAddress v4 = InetAddress.getByAddress(new byte[] { 10, 1, 2, 3 });
        final ZonedDateTime startOfDay = DAY.atStartOfDay(ZoneId.systemDefault());
        final UserType userType = userType(new CodecRegistry(), "local", DataType.date(), "instant", DataType.date(), "zoned", DataType.date(), "offset",
                DataType.date(), "host", DataType.inet(), "clock", DataType.timestamp());
        final UDTValue raw = userType.newValue()
                .setDate("local", DATE)
                .setDate("instant", DATE)
                .setDate("zoned", DATE)
                .setDate("offset", DATE)
                .setInet("host", v4)
                .setTimestamp("clock", TIMESTAMP);
        final ByteBuffer encoded = TypeCodec.userType(userType).serialize(raw, ProtocolVersion.V4);

        final UdtParts parts = CassandraExecutor.UDTCodec.create(userType, UdtParts.class).deserialize(encoded, ProtocolVersion.V4);

        assertAll(() -> assertEquals(DAY.atStartOfDay(), parts.getLocal()), () -> assertEquals(startOfDay.toInstant(), parts.getInstant()),
                () -> assertEquals(startOfDay, parts.getZoned()), () -> assertEquals(startOfDay.toOffsetDateTime(), parts.getOffset()),
                () -> assertEquals("10.1.2.3", parts.getHost()),
                () -> assertEquals(OffsetTime.ofInstant(TIMESTAMP.toInstant(), ZoneId.systemDefault()), parts.getClock()));
    }

    private static UserType userType(final CodecRegistry registry, final Object... namesAndTypes) throws Exception {
        final var fieldConstructor = UserType.Field.class.getDeclaredConstructor(String.class, DataType.class);
        fieldConstructor.setAccessible(true);
        final List<UserType.Field> fields = new java.util.ArrayList<>();

        for (int i = 0; i < namesAndTypes.length; i += 2) {
            fields.add(fieldConstructor.newInstance(namesAndTypes[i], namesAndTypes[i + 1]));
        }

        final var typeConstructor = UserType.class.getDeclaredConstructor(String.class, String.class, boolean.class, Collection.class, ProtocolVersion.class,
                CodecRegistry.class);
        typeConstructor.setAccessible(true);
        return typeConstructor.newInstance("ks", "parts", false, fields, ProtocolVersion.V4, registry);
    }

    private static ResultSetFuture future(final ResultSet resultSet) throws Exception {
        final ResultSetFuture future = mock(ResultSetFuture.class);
        when(future.get()).thenReturn(resultSet);
        return future;
    }

    /** Rows of one column {@code v} of {@code type}, one row per value. */
    private static ResultSet rows(final DataType type, final Object... values) {
        return rows(new String[] { "v" }, type, Arrays.stream(values).map(value -> new Object[] { value }).toArray(Object[][]::new));
    }

    /** One row per date, with the date columns {@code local}, {@code instant}, {@code zoned} and {@code offset} all holding it. */
    private static ResultSet dateParts(final LocalDate... values) {
        return rows(new String[] { "local", "instant", "zoned", "offset" }, DataType.date(),
                Arrays.stream(values).map(value -> new Object[] { value, value, value, value }).toArray(Object[][]::new));
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

    public static class Host {
        private String v;

        public String getV() {
            return v;
        }

        public void setV(final String v) {
            this.v = v;
        }
    }

    public static class Clock {
        private OffsetTime v;

        public OffsetTime getV() {
            return v;
        }

        public void setV(final OffsetTime v) {
            this.v = v;
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

    public static class UdtParts extends DateParts {
        private String host;
        private OffsetTime clock;

        public String getHost() {
            return host;
        }

        public void setHost(final String host) {
            this.host = host;
        }

        public OffsetTime getClock() {
            return clock;
        }

        public void setClock(final OffsetTime clock) {
            this.clock = clock;
        }
    }

    // ---- 2026-10-24 verify2 ----

    /**
     * A dotted (nested-path) column is stored through {@code BeanInfo.setPropValue(path)}, which skipped the top-level
     * conversions: an inet leaf got {@code "/127.0.0.1"}, a date into a {@code LocalDateTime} leaf threw
     * {@code DateTimeParseException}, a timestamp into an {@code OffsetTime} leaf failed, a time's nanosecond count was read as
     * epoch milliseconds, and a BLOB into a {@code byte[]} leaf became {@code null}. The last property of the path now converts
     * as a top-level property does.
     */
    @Test
    void nestedPathColumnsConvertForTheLastPropertyAsTopLevelColumnsDo() throws UnknownHostException {
        final InetAddress v4 = InetAddress.getByAddress(new byte[] { 127, 0, 0, 1 });
        final LocalTime clock = LocalTime.of(1, 2, 3, 456_789_000);

        assertAll(() -> assertEquals("127.0.0.1", verify2Nested("nested.host", DataType.inet(), v4).getNested().getHost()),
                () -> assertEquals(DAY.atStartOfDay(), verify2Nested("nested.local", DataType.date(), DATE).getNested().getLocal()),
                () -> assertEquals(OffsetTime.ofInstant(TIMESTAMP.toInstant(), ZoneId.systemDefault()),
                        verify2Nested("nested.offsetClock", DataType.timestamp(), TIMESTAMP).getNested().getOffsetClock()),
                () -> assertEquals(clock, verify2Nested("nested.clock", DataType.time(), clock.toNanoOfDay()).getNested().getClock()),
                () -> assertArrayEquals(new byte[] { 1, 2, 3 },
                        verify2Nested("nested.blob", DataType.blob(), ByteBuffer.wrap(new byte[] { 1, 2, 3 })).getNested().getBlob()),
                // An intermediate bean that already exists receives the converted value as well.
                () -> assertEquals("127.0.0.1", verify2Nested("preset.host", DataType.inet(), v4).getPreset().getHost()));
    }

    /** Pins: nested values that already converted, a NULL column, unresolvable paths and read-only path members behave as before. */
    @Test
    void nestedPathColumnsKeepTheirOtherBehavior() throws UnknownHostException {
        final InetAddress v4 = InetAddress.getByAddress(new byte[] { 127, 0, 0, 1 });

        assertAll(() -> assertEquals("n", verify2Nested("nested.name", DataType.text(), "n").getNested().getName()),
                () -> assertEquals(7, verify2Nested("nested.count", DataType.cint(), 7).getNested().getCount()),
                // A bigint Long into an int leaf is not a time of day (only a time column's Long is).
                () -> assertEquals(7, verify2Nested("nested.count", DataType.bigint(), 7L).getNested().getCount()),
                // A null intermediate bean is created; a NULL column leaves the leaf null.
                () -> assertEquals(null, verify2Nested("nested.host", DataType.inet(), null).getNested().getHost()),
                // A path that does not resolve, or whose last property is read-only, is ignored without creating the intermediate bean.
                () -> assertEquals(null, verify2Nested("nested.nope", DataType.text(), "x").getNested()),
                () -> assertEquals(null, verify2Nested("nested.computed", DataType.timestamp(), TIMESTAMP).getNested()),
                // A read-only (getter-only) intermediate property whose value is null: the value is dropped without an exception.
                () -> assertEquals(null, verify2Nested("fixed.host", DataType.inet(), v4).getFixed()));
    }

    private static Verify2Holder verify2Nested(final String column, final DataType type, final Object value) {
        return CassandraExecutor.toEntity(rows(new String[] { column }, type, new Object[][] { { value } }).one(), Verify2Holder.class);
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
        private LocalDateTime local;
        private OffsetTime offsetClock;
        private LocalTime clock;
        private byte[] blob;
        private String name;
        private int count;

        public String getHost() {
            return host;
        }

        public void setHost(final String host) {
            this.host = host;
        }

        public LocalDateTime getLocal() {
            return local;
        }

        public void setLocal(final LocalDateTime local) {
            this.local = local;
        }

        public OffsetTime getOffsetClock() {
            return offsetClock;
        }

        public void setOffsetClock(final OffsetTime offsetClock) {
            this.offsetClock = offsetClock;
        }

        public LocalTime getClock() {
            return clock;
        }

        public void setClock(final LocalTime clock) {
            this.clock = clock;
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

        public java.time.LocalDate getComputed() {
            return java.time.LocalDate.EPOCH;
        }
    }

    // ---- 2026-10-24 r2 sliceK ----

    private static final LocalDate FIRST = LocalDate.fromYearMonthDay(1, 1, 1);
    private static final LocalDate EARLY = LocalDate.fromYearMonthDay(999, 3, 1);
    private static final LocalDate LATE = LocalDate.fromYearMonthDay(10000, 1, 1);

    /**
     * The driver's {@code LocalDate.toString()} has no four-digit year outside 1000-9999 ({@code "1-01-01"} for the common
     * {@code 0001-01-01} sentinel), and {@code N.convert} parsed that text: such a {@code date} column could not be read into a
     * {@code java.time.LocalDate} ({@code DateTimeParseException}) or a {@code Date}, {@code Timestamp} or {@code Calendar}
     * ({@code IllegalArgumentException}), on any read path. The 4.x executor, which decodes a {@code java.time.LocalDate}, reads it.
     */
    @Test
    void r2sliceK_dateColumnsOutsideTheFourDigitYearsReadIntoDateTargets() throws Exception {
        final java.time.LocalDate firstDay = java.time.LocalDate.of(1, 1, 1);
        final java.time.LocalDate earlyDay = java.time.LocalDate.of(999, 3, 1);
        final long firstMillis = firstDay.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
        final long earlyMillis = earlyDay.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();

        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            when(fixture.session.execute(any(Statement.class))).thenAnswer(invocation -> rows(DataType.date(), FIRST, EARLY, LATE));
            when(fixture.session.executeAsync(any(Statement.class))).thenAnswer(invocation -> future(rows(DataType.date(), FIRST, EARLY, LATE)));

            assertAll(() -> assertEquals(firstDay, fixture.executor.findFirst(java.time.LocalDate.class, QUERY).get()),
                    () -> assertEquals(List.of(firstDay, earlyDay, java.time.LocalDate.of(10000, 1, 1)),
                            fixture.executor.list(java.time.LocalDate.class, QUERY)),
                    () -> assertEquals(firstDay, fixture.executor.queryForSingleValue(java.time.LocalDate.class, QUERY).get()),
                    () -> assertEquals(firstDay, fixture.executor.findFirst(java.time.LocalDate[].class, QUERY).get()[0]),
                    () -> assertEquals(firstDay, fixture.executor.async().findFirst(java.time.LocalDate.class, QUERY).get().get()),
                    () -> assertEquals(firstMillis, fixture.executor.findFirst(Date.class, QUERY).get().getTime()),
                    () -> assertEquals(firstMillis, fixture.executor.findFirst(java.sql.Timestamp.class, QUERY).get().getTime()),
                    () -> assertEquals(firstMillis, fixture.executor.findFirst(java.sql.Date.class, QUERY).get().getTime()),
                    () -> assertEquals(firstMillis, fixture.executor.findFirst(java.util.Calendar.class, QUERY).get().getTimeInMillis()),
                    () -> assertEquals(List.of(earlyDay), CassandraExecutor.toList(rows(DataType.date(), EARLY), java.time.LocalDate.class)), () -> {
                        final DayParts bean = CassandraExecutor.toEntity(dayParts(EARLY).one(), DayParts.class);
                        assertEquals(earlyDay, bean.getDay());
                        assertEquals(earlyMillis, bean.getDate().getTime());
                    }, () -> assertEquals(List.of(earlyDay), CassandraExecutor.extractData(dayParts(EARLY), DayParts.class).getColumn("day")),
                    () -> assertEquals(earlyDay,
                            CassandraExecutor
                                    .toEntity(rows(new String[] { "nested.day" }, DataType.date(), new Object[][] { { EARLY } }).one(), DayHolder.class)
                                    .getNested()
                                    .getDay()),
                    () -> {
                        final UserType userType = userType(new CodecRegistry(), "day", DataType.date(), "date", DataType.date());
                        final UDTValue raw = userType.newValue().setDate("day", FIRST).setDate("date", FIRST);
                        final DayParts parts = CassandraExecutor.UDTCodec.create(userType, DayParts.class)
                                .deserialize(TypeCodec.userType(userType).serialize(raw, ProtocolVersion.V4), ProtocolVersion.V4);
                        assertEquals(firstDay, parts.getDay());
                        assertEquals(firstMillis, parts.getDate().getTime());
                    });
        }
    }

    /** Pins: a four-digit year converts as before, a String target keeps the driver's text, and a Date target supports 1 to 9999. */
    @Test
    void r2sliceK_dateColumnConversionsKeepTheirOtherBehavior() {
        final long startOfDay = DAY.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();

        assertAll(() -> assertEquals(List.of(DAY), CassandraExecutor.toList(rows(DataType.date(), DATE), java.time.LocalDate.class)),
                () -> assertEquals(startOfDay, CassandraExecutor.toList(rows(DataType.date(), DATE), Date.class).get(0).getTime()),
                () -> assertEquals(startOfDay, CassandraExecutor.toList(rows(DataType.date(), DATE), java.util.Calendar.class).get(0).getTimeInMillis()),
                () -> assertEquals(List.of("2023-11-14", "1-01-01"), CassandraExecutor.toList(rows(DataType.date(), DATE, FIRST), String.class)),
                () -> assertSame(FIRST, CassandraExecutor.toList(rows(DataType.date(), FIRST), LocalDate.class).get(0)), () -> org.junit.jupiter.api.Assertions
                        .assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toList(rows(DataType.date(), LATE), Date.class)));
    }

    private static ResultSet dayParts(final LocalDate value) {
        return rows(new String[] { "day", "date" }, DataType.date(), new Object[][] { { value, value } });
    }

    public static class DayParts {
        private java.time.LocalDate day;
        private Date date;

        public java.time.LocalDate getDay() {
            return day;
        }

        public void setDay(final java.time.LocalDate day) {
            this.day = day;
        }

        public Date getDate() {
            return date;
        }

        public void setDate(final Date date) {
            this.date = date;
        }
    }

    public static class DayHolder {
        private DayParts nested;

        public DayParts getNested() {
            return nested;
        }

        public void setNested(final DayParts nested) {
            this.nested = nested;
        }
    }

    /**
     * {@code PropInfo.setPropValue} stored a driver collection or map as is when the property's raw class accepted it, so a typed
     * container property held the driver's element types (heap pollution, {@code ClassCastException} where an element was used): a
     * {@code List<byte[]>} or {@code List<String>} of a {@code list<blob>} held {@code ByteBuffer}s, a {@code List<LocalDateTime>}
     * of a {@code list<timestamp>} held {@code Date}s, a {@code Set<String>} of a {@code set<inet>} held {@code InetAddress}es, a
     * {@code List<java.time.LocalDate>} of a {@code list<date>} held driver {@code LocalDate}s, a {@code List<LocalTime>} of a
     * {@code list<time>} held {@code Long} nanoseconds. Elements now convert as a top-level value of the element type does.
     */
    @Test
    void r2sliceK_typedContainerPropertiesConvertTheirElements() throws Exception {
        final InetAddress v4 = InetAddress.getByAddress(new byte[] { 127, 0, 0, 1 });
        final LocalTime clock = LocalTime.of(1, 2, 3, 456_789_000);
        final LocalDateTime stamp = LocalDateTime.ofInstant(TIMESTAMP.toInstant(), ZoneId.systemDefault());
        final ResultSet result = typedRow(new String[] { "blobs", "blobTexts", "stamps", "hosts", "days", "clocks", "byName", "nestedBlobs", "hostArray" },
                new DataType[] { DataType.list(DataType.blob()), DataType.list(DataType.blob()), DataType.list(DataType.timestamp()),
                        DataType.set(DataType.inet()), DataType.list(DataType.date()), DataType.list(DataType.time()),
                        DataType.map(DataType.text(), DataType.date()), DataType.list(DataType.list(DataType.blob())), DataType.list(DataType.inet()) },
                new Object[] { new java.util.ArrayList<>(List.of(ByteBuffer.wrap(new byte[] { 1, 2 }))),
                        new java.util.ArrayList<>(List.of(ByteBuffer.wrap(new byte[] { 1, 2 }))), new java.util.ArrayList<>(List.of(TIMESTAMP)),
                        new java.util.LinkedHashSet<>(List.of(v4)), new java.util.ArrayList<>(List.of(DATE)),
                        new java.util.ArrayList<>(List.of(clock.toNanoOfDay())), new java.util.LinkedHashMap<>(java.util.Map.of("a", DATE)),
                        new java.util.ArrayList<>(List.of(new java.util.ArrayList<>(List.of(ByteBuffer.wrap(new byte[] { 3 }))))),
                        new java.util.ArrayList<>(List.of(v4)) });
        final Bag bag = CassandraExecutor.toEntity(result.one(), Bag.class);
        // The same rules for a dotted (nested-path) column and a UDT field read into a bean property.
        final ResultSet nested = typedRow(new String[] { "inner.blobs" }, new DataType[] { DataType.list(DataType.blob()) },
                new Object[] { new java.util.ArrayList<>(List.of(ByteBuffer.wrap(new byte[] { 4 }))) });
        final UserType userType = userType(new CodecRegistry(), "hosts", DataType.set(DataType.inet()), "clocks", DataType.list(DataType.time()));
        final UDTValue raw = userType.newValue().setSet("hosts", java.util.Set.of(v4)).setList("clocks", List.of(clock.toNanoOfDay()));
        final Bag decoded = CassandraExecutor.UDTCodec.create(userType, Bag.class)
                .deserialize(TypeCodec.userType(userType).serialize(raw, ProtocolVersion.V4), ProtocolVersion.V4);

        assertAll(() -> assertArrayEquals(new byte[] { 1, 2 }, bag.getBlobs().get(0)), () -> assertEquals(List.of("AQI="), bag.getBlobTexts()),
                () -> assertEquals(List.of(stamp), bag.getStamps()), () -> assertEquals(java.util.Set.of("127.0.0.1"), bag.getHosts()),
                () -> assertEquals(java.util.LinkedHashSet.class, bag.getHosts().getClass()), () -> assertEquals(List.of(DAY), bag.getDays()),
                () -> assertEquals(List.of(clock), bag.getClocks()), () -> assertEquals(java.util.Map.of("a", DAY), bag.getByName()),
                () -> assertArrayEquals(new byte[] { 3 }, bag.getNestedBlobs().get(0).get(0)),
                () -> assertArrayEquals(new String[] { "127.0.0.1" }, bag.getHostArray()),
                () -> assertArrayEquals(new byte[] { 4 }, CassandraExecutor.toEntity(nested.one(), BagHolder.class).getInner().getBlobs().get(0)),
                () -> assertEquals(java.util.Set.of("127.0.0.1"), decoded.getHosts()), () -> assertEquals(List.of(clock), decoded.getClocks()));
    }

    /** Pins: a container whose elements already have the declared types (or that declares none) is set as the driver's instance. */
    @Test
    void r2sliceK_containersWhoseElementsFitAreSetUnchanged() {
        final List<Object> longs = new java.util.ArrayList<>(List.of(1L, 2L));
        final java.util.Map<String, Object> raw = new java.util.LinkedHashMap<>(java.util.Map.of("a", 1L));
        final ResultSet result = typedRow(new String[] { "longs", "raw", "anyList" },
                new DataType[] { DataType.list(DataType.bigint()), DataType.map(DataType.text(), DataType.bigint()), DataType.list(DataType.blob()) },
                new Object[] { longs, raw, new java.util.ArrayList<>(List.of(ByteBuffer.wrap(new byte[] { 1 }))) });
        final Fits fits = CassandraExecutor.toEntity(result.one(), Fits.class);

        assertAll(() -> assertSame(longs, fits.getLongs()), () -> assertSame(raw, fits.getRaw()),
                () -> assertEquals(ByteBuffer.wrap(new byte[] { 1 }), fits.getAnyList().get(0)));
    }

    private static ResultSet typedRow(final String[] names, final DataType[] types, final Object[] values) {
        final ColumnDefinitions columns = mock(ColumnDefinitions.class);
        when(columns.size()).thenReturn(names.length);
        final Row row = mock(Row.class);
        when(row.getColumnDefinitions()).thenReturn(columns);

        for (int i = 0; i < names.length; i++) {
            when(columns.getName(i)).thenReturn(names[i]);
            when(columns.getType(i)).thenReturn(types[i]);
            when(row.getObject(i)).thenReturn(values[i]);
        }

        final ResultSet result = mock(ResultSet.class);
        when(result.getColumnDefinitions()).thenReturn(columns);
        when(result.one()).thenReturn(row);
        return result;
    }

    public static class Bag {
        private List<byte[]> blobs;
        private List<String> blobTexts;
        private List<LocalDateTime> stamps;
        private java.util.Set<String> hosts;
        private List<java.time.LocalDate> days;
        private List<LocalTime> clocks;
        private java.util.Map<String, java.time.LocalDate> byName;
        private List<List<byte[]>> nestedBlobs;
        private String[] hostArray;

        public List<byte[]> getBlobs() {
            return blobs;
        }

        public void setBlobs(final List<byte[]> blobs) {
            this.blobs = blobs;
        }

        public List<String> getBlobTexts() {
            return blobTexts;
        }

        public void setBlobTexts(final List<String> blobTexts) {
            this.blobTexts = blobTexts;
        }

        public List<LocalDateTime> getStamps() {
            return stamps;
        }

        public void setStamps(final List<LocalDateTime> stamps) {
            this.stamps = stamps;
        }

        public java.util.Set<String> getHosts() {
            return hosts;
        }

        public void setHosts(final java.util.Set<String> hosts) {
            this.hosts = hosts;
        }

        public List<java.time.LocalDate> getDays() {
            return days;
        }

        public void setDays(final List<java.time.LocalDate> days) {
            this.days = days;
        }

        public List<LocalTime> getClocks() {
            return clocks;
        }

        public void setClocks(final List<LocalTime> clocks) {
            this.clocks = clocks;
        }

        public java.util.Map<String, java.time.LocalDate> getByName() {
            return byName;
        }

        public void setByName(final java.util.Map<String, java.time.LocalDate> byName) {
            this.byName = byName;
        }

        public List<List<byte[]>> getNestedBlobs() {
            return nestedBlobs;
        }

        public void setNestedBlobs(final List<List<byte[]>> nestedBlobs) {
            this.nestedBlobs = nestedBlobs;
        }

        public String[] getHostArray() {
            return hostArray;
        }

        public void setHostArray(final String[] hostArray) {
            this.hostArray = hostArray;
        }
    }

    public static class BagHolder {
        private Bag inner;

        public Bag getInner() {
            return inner;
        }

        public void setInner(final Bag inner) {
            this.inner = inner;
        }
    }

    public static class Fits {
        private List<Long> longs;
        private java.util.Map<String, Object> raw;
        private List<?> anyList;

        public List<Long> getLongs() {
            return longs;
        }

        public void setLongs(final List<Long> longs) {
            this.longs = longs;
        }

        public java.util.Map<String, Object> getRaw() {
            return raw;
        }

        public void setRaw(final java.util.Map<String, Object> raw) {
            this.raw = raw;
        }

        public List<?> getAnyList() {
            return anyList;
        }

        public void setAnyList(final List<?> anyList) {
            this.anyList = anyList;
        }
    }
}
