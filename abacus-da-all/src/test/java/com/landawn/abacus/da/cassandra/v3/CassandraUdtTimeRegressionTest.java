package com.landawn.abacus.da.cassandra.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.stubbing.Answer;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.google.common.reflect.TypeToken;
import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.util.ClassUtil;

public class CassandraUdtTimeRegressionTest extends TestBase {

    // Metadata and validation

    @Test
    void registeredDateCodecFailuresRemainUnchangedForSqlSubclasses() throws Exception {
        final AtomicReference<RuntimeException> serializerFailure = new AtomicReference<>();
        final CodecRegistry registry = new CodecRegistry().register(dateCodec(987_654_321L, serializerFailure));
        final CassandraExecutor.UDTCodec<Map> codec = CassandraExecutor.UDTCodec.create(timeType(registry), Map.class);

        for (final Date value : List.of(new Date(0), new java.sql.Time(0), java.sql.Timestamp.from(Instant.EPOCH))) {
            for (final RuntimeException failure : List.of(new IllegalStateException("serializer failed"),
                    new CodecNotFoundException("matching field type", DataType.time(), TypeToken.of(value.getClass())))) {
                serializerFailure.set(failure);
                assertSame(failure, assertThrows(RuntimeException.class, () -> codec.serialize(Map.of("time", value))));
                assertSame(failure, assertThrows(RuntimeException.class, () -> codec.format(Map.of("time", value))));
            }
        }
    }

    @Test
    void registeredDateCodecAcceptsSqlSubclassesInEveryUdtInputShape() throws Exception {
        final long marker = 987_654_321L;
        final CodecRegistry registry = new CodecRegistry();
        final UserType userType = timeType(registry);
        final CassandraExecutor.UDTCodec<Map> mapCodec = CassandraExecutor.UDTCodec.create(userType, Map.class);
        final CassandraExecutor.UDTCodec<List> listCodec = CassandraExecutor.UDTCodec.create(userType, List.class);
        final CassandraExecutor.UDTCodec<LegacyTimedValue> beanCodec = CassandraExecutor.UDTCodec.create(userType, LegacyTimedValue.class);
        registry.register(dateCodec(marker, new AtomicReference<>()));

        for (final Date value : List.of(new Date(0), new java.sql.Time(0), java.sql.Timestamp.from(Instant.EPOCH))) {
            final LegacyTimedValue bean = new LegacyTimedValue();
            bean.setTime(value);
            assertEquals(marker, mapCodec.serialize(Map.of("time", value)).getTime(0), value.getClass().getName());
            assertEquals(marker, listCodec.serialize(List.of(value)).getTime(0), value.getClass().getName());
            assertEquals(marker, beanCodec.serialize(bean).getTime(0), value.getClass().getName());
            assertEquals(marker, mapCodec.parse(mapCodec.format(Map.of("time", value))).get("time"));
        }
    }

    @Test
    void registeredTimeFieldCodecTakesPrecedenceOverFallback() throws Exception {
        final LocalTime time = LocalTime.of(12, 34, 56, 123);
        final AtomicReference<RuntimeException> serializerFailure = new AtomicReference<>();
        final CodecRegistry registry = new CodecRegistry().register(new TypeCodec<>(DataType.time(), LocalTime.class) {
            @Override
            public ByteBuffer serialize(final LocalTime value, final ProtocolVersion protocolVersion) {
                if (serializerFailure.get() != null) {
                    throw serializerFailure.get();
                }
                return TypeCodec.time().serialize(value == null ? null : value.toNanoOfDay() + 1, protocolVersion);
            }

            @Override
            public LocalTime deserialize(final ByteBuffer bytes, final ProtocolVersion protocolVersion) {
                final Long value = TypeCodec.time().deserialize(bytes, protocolVersion);
                return value == null ? null : LocalTime.ofNanoOfDay(value - 1);
            }

            @Override
            public LocalTime parse(final String value) {
                final Long parsed = TypeCodec.time().parse(value);
                return parsed == null ? null : LocalTime.ofNanoOfDay(parsed - 1);
            }

            @Override
            public String format(final LocalTime value) {
                return TypeCodec.time().format(value == null ? null : value.toNanoOfDay() + 1);
            }
        });

        final CassandraExecutor.UDTCodec<Map> codec = CassandraExecutor.UDTCodec.create(timeType(registry), Map.class);
        assertEquals(time.toNanoOfDay() + 1, codec.serialize(Map.of("time", time)).getTime(0));

        for (final RuntimeException failure : List.of(new IllegalStateException("serializer failed"),
                new CodecNotFoundException("unspecified type", DataType.time(), null),
                new CodecNotFoundException("different CQL type", DataType.text(), TypeToken.of(LocalTime.class)),
                new CodecNotFoundException("different Java type", DataType.time(), TypeToken.of(String.class)),
                new CodecNotFoundException("matching field type", DataType.time(), TypeToken.of(LocalTime.class)))) {
            serializerFailure.set(failure);
            assertSame(failure, assertThrows(RuntimeException.class, () -> codec.serialize(Map.of("time", time))));
            assertSame(failure, assertThrows(RuntimeException.class, () -> codec.format(Map.of("time", time))));
        }
    }

    // Resource lifecycle

    @Test
    void timeFallbackDoesNotChangeNonTimeFieldFailures() throws Exception {
        final CodecRegistry registry = new CodecRegistry().register(dateCodec(987_654_321L, new AtomicReference<>()));
        final CassandraExecutor.UDTCodec<Map> codec = CassandraExecutor.UDTCodec.create(fieldType(registry, DataType.text()), Map.class);

        for (final Date value : List.of(new Date(0), new java.sql.Time(0), java.sql.Timestamp.from(Instant.EPOCH))) {
            final CodecNotFoundException failure = assertThrows(CodecNotFoundException.class, () -> codec.serialize(Map.of("time", value)));
            assertEquals(DataType.text(), failure.getCqlType());
            assertEquals(TypeToken.of(value.getClass()), failure.getJavaType());
        }
    }

    @Test
    void unsupportedIntegerTimeFieldPreservesOriginalLookupFailure() throws Exception {
        assertUnsupportedTimeValue(123, Integer.class);
    }

    @Test
    void unsupportedStringTimeFieldPreservesOriginalLookupFailure() throws Exception {
        assertUnsupportedTimeValue("12:34:56", String.class);
    }

    // Additional contracts

    @Test
    void changedDefaultZoneKeepsCapturedDateZoneAndCurrentInstantZone(@TempDir final Path tempDir) throws Exception {
        final Path output = tempDir.resolve("zone-probe.txt");
        final Path arguments = tempDir.resolve("zone-probe.args");
        final String executable = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        final String classPath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        // The test classpath approaches the Windows command-line limit, so pass it in a launcher argument file. Quoted
        // argument-file values treat backslashes as escapes, and the launcher reads the file in the native encoding.
        Files.writeString(arguments, "-cp\n\"" + classPath.replace("\\", "\\\\") + "\"\n",
                java.nio.charset.Charset.forName(System.getProperty("native.encoding")));
        final Process process = new ProcessBuilder(executable, "-Xmx256m", "-XX:+UseSerialGC", "-Duser.timezone=UTC", "@" + arguments,
                ZoneChangeProbe.class.getName()).redirectErrorStream(true).redirectOutput(output.toFile()).start();

        try {
            assertTrue(process.waitFor(30, TimeUnit.SECONDS), "Forked time-zone probe timed out");
            assertEquals(0, process.exitValue(), () -> {
                try {
                    return Files.readString(output);
                } catch (final java.io.IOException e) {
                    return e.toString();
                }
            });
        } finally {
            process.destroyForcibly();
        }
    }

    @Test
    void dateCalendarAndJavaTimeBeanPropertiesReadBackTheirTimeOfDay() throws Exception {
        final UserType userType = timeType(new CodecRegistry());
        final Instant instant = Instant.parse("2026-10-13T12:34:56.123456789Z");
        final LocalTime local = LocalTime.ofInstant(instant, ZoneId.systemDefault());

        // Decoding read the nanosecond count as epoch milliseconds: a Date written from 2026 came back in the year 2606.
        final LegacyTimedValue legacy = new LegacyTimedValue();
        legacy.setTime(Date.from(instant));
        final Date date = roundTrip(userType, LegacyTimedValue.class, legacy).getTime();
        assertEquals(LocalDate.EPOCH.atTime(local.truncatedTo(ChronoUnit.MILLIS)), LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault()));

        final TimestampTimedValue timestamp = new TimestampTimedValue();
        timestamp.setTime(java.sql.Timestamp.from(instant));
        assertEquals(LocalDate.EPOCH.atTime(local), roundTrip(userType, TimestampTimedValue.class, timestamp).getTime().toLocalDateTime());

        final CalendarTimedValue calendar = new CalendarTimedValue();
        final Calendar east = Calendar.getInstance(TimeZone.getTimeZone("GMT+05:30"));
        east.setTimeInMillis(instant.toEpochMilli());
        calendar.setTime(east);
        final Calendar decoded = roundTrip(userType, CalendarTimedValue.class, calendar).getTime();
        assertEquals(LocalDate.EPOCH.atTime(18, 4, 56, 123_000_000), LocalDateTime.ofInstant(decoded.toInstant(), decoded.getTimeZone().toZoneId()));

        final InstantTimedValue instantBean = new InstantTimedValue();
        instantBean.setTime(instant);
        assertEquals(LocalDate.EPOCH.atTime(local).atZone(ZoneId.systemDefault()).toInstant(),
                roundTrip(userType, InstantTimedValue.class, instantBean).getTime());

        final LocalDateTimeTimedValue dateTime = new LocalDateTimeTimedValue();
        dateTime.setTime(LocalDateTime.of(2026, 10, 13, 12, 34, 56, 789));
        assertEquals(LocalDateTime.of(1970, 1, 1, 12, 34, 56, 789), roundTrip(userType, LocalDateTimeTimedValue.class, dateTime).getTime());
    }

    @Test
    void legacySqlTimeUdtFieldUsesNanosecondsSinceMidnight() throws Exception {
        final UserType userType = timeType(new CodecRegistry());
        final java.sql.Time time = java.sql.Time.valueOf("12:34:56");
        time.setTime(time.getTime() + 123);

        assertEquals(LocalTime.of(12, 34, 56, 123_000_000).toNanoOfDay(),
                CassandraExecutor.UDTCodec.create(userType, Map.class).serialize(Map.of("time", time)).getTime(0));
    }

    @Test
    void localTimeUdtFieldsRoundTripForBeansMapsAndCollections() throws Exception {
        final UserType userType = timeType(new CodecRegistry());
        final LocalTime time = LocalTime.of(12, 34, 56, 123_456_789);
        final CassandraExecutor.UDTCodec<TimedValue> beanCodec = CassandraExecutor.UDTCodec.create(userType, TimedValue.class);
        final TimedValue bean = new TimedValue();
        bean.setTime(time);

        assertEquals(time.toNanoOfDay(), beanCodec.serialize(bean).getTime(0));
        assertEquals(time, beanCodec.deserialize(beanCodec.serialize(bean)).getTime());
        assertEquals(time.toNanoOfDay(), CassandraExecutor.UDTCodec.create(userType, Map.class).serialize(Map.of("time", time)).getTime(0));
        assertEquals(time.toNanoOfDay(), CassandraExecutor.UDTCodec.create(userType, List.class).serialize(List.of(time)).getTime(0));
    }

    @Test
    void nativeLongTimeValuesRemainNanosecondsAndTimestampFallbackKeepsNanos() throws Exception {
        final CassandraExecutor.UDTCodec<Map> codec = CassandraExecutor.UDTCodec.create(timeType(new CodecRegistry()), Map.class);

        for (final long nanos : new long[] { 0, 123, LocalTime.MAX.toNanoOfDay() }) {
            assertEquals(nanos, codec.serialize(Map.of("time", nanos)).getTime(0));
        }

        final LocalTime localTime = LocalTime.of(12, 34, 56, 123_456_789);
        final java.sql.Timestamp timestamp = java.sql.Timestamp.from(LocalDate.of(2026, 10, 14).atTime(localTime).atZone(ZoneId.systemDefault()).toInstant());
        assertEquals(localTime.toNanoOfDay(), codec.serialize(Map.of("time", timestamp)).getTime(0));
    }

    @Test
    void timeCodecLookupUsesTheUdtsCurrentRegistry() throws Exception {
        final CodecRegistry firstRegistry = new CodecRegistry().register(dateCodec(1, new AtomicReference<>()));
        final CodecRegistry secondRegistry = new CodecRegistry().register(dateCodec(2, new AtomicReference<>()));
        final UserType firstType = timeType(firstRegistry);
        final CassandraExecutor.UDTCodec<Map> firstCodec = CassandraExecutor.UDTCodec.create(firstType, Map.class);
        final CassandraExecutor.UDTCodec<Map> secondCodec = CassandraExecutor.UDTCodec.create(timeType(secondRegistry), Map.class);
        final Map<String, java.sql.Time> value = Map.of("time", new java.sql.Time(0));

        assertEquals(1, firstCodec.serialize(value).getTime(0));
        assertEquals(2, secondCodec.serialize(value).getTime(0));
        assertEquals(1, firstCodec.serialize(value).getTime(0));

        // Driver metadata can replace the registry on an existing UserType. A cached accessor must not cache its result.
        final var setter = UserType.class.getDeclaredMethod("setCodecRegistry", CodecRegistry.class);
        setter.setAccessible(true);
        setter.invoke(firstType, secondRegistry);
        assertEquals(2, firstCodec.serialize(value).getTime(0));
    }

    @Test
    void timeColumnsReadIntoDateAndJavaTimeTypesKeepTheirTimeOfDay() {
        final LocalTime time = LocalTime.of(12, 34, 56, 123_456_789);
        final LocalDateTime expected = LocalDate.EPOCH.atTime(time);

        assertEquals(expected, CassandraExecutor.toList(timeRows(time), LocalDateTime.class).get(0));
        assertEquals(expected.atZone(ZoneId.systemDefault()).toInstant(), CassandraExecutor.toList(timeRows(time), Instant.class).get(0));
        assertEquals(expected, CassandraExecutor.toList(timeRows(time), java.sql.Timestamp.class).get(0).toLocalDateTime());
        assertEquals(expected.truncatedTo(ChronoUnit.MILLIS),
                LocalDateTime.ofInstant(CassandraExecutor.toList(timeRows(time), Date.class).get(0).toInstant(), ZoneId.systemDefault()));
        assertEquals(time, CassandraExecutor.toList(timeRows(time), LocalTime.class).get(0));
        // A non-time column keeps its value: only a time column's nanosecond count is a time of day.
        final com.datastax.driver.core.ResultSet bigint = timeRows(time);
        when(bigint.getColumnDefinitions().getType(0)).thenReturn(DataType.bigint());
        assertEquals(new Date(time.toNanoOfDay()), CassandraExecutor.toList(bigint, Date.class).get(0));
    }

    @Test
    void timeRegistryLookupDoesNotMakeTheSharedReflectionCacheAccessible() throws Exception {
        final CassandraExecutor.UDTCodec<Map> codec = CassandraExecutor.UDTCodec.create(timeType(new CodecRegistry()), Map.class);
        final UDTValue encoded = codec.serialize(Map.of("time", new java.sql.Time(0)));

        // The TIME lookup reads the protected registry accessor reflectively. ClassUtil caches and shares Method
        // instances, so the executor must make only a private copy accessible.
        assertFalse(ClassUtil.getDeclaredMethod(UDTValue.class, "getCodecRegistry").canAccess(encoded));
    }

    @Test
    void timeUdtBoundariesAndNullsRetainDriverAndBeanRepresentations() throws Exception {
        final UserType userType = timeType(new CodecRegistry());
        final CassandraExecutor.UDTCodec<TimedValue> beanCodec = CassandraExecutor.UDTCodec.create(userType, TimedValue.class);
        final CassandraExecutor.UDTCodec<Map> mapCodec = CassandraExecutor.UDTCodec.create(userType, Map.class);
        final CassandraExecutor.UDTCodec<List> listCodec = CassandraExecutor.UDTCodec.create(userType, List.class);

        for (final LocalTime time : List.of(LocalTime.MIN, LocalTime.MAX)) {
            final TimedValue bean = new TimedValue();
            bean.setTime(time);
            final var encoded = beanCodec.serialize(bean);
            assertEquals(time.toNanoOfDay(), encoded.getTime(0));
            assertEquals(time, beanCodec.deserialize(encoded).getTime());
            assertEquals(time.toNanoOfDay(), mapCodec.deserialize(encoded).get("time"));
            assertEquals(time.toNanoOfDay(), listCodec.deserialize(encoded).get(0));
        }

        final var nullField = mapCodec.serialize(Collections.singletonMap("time", null));
        assertTrue(nullField.isNull(0));
        assertNull(beanCodec.deserialize(nullField).getTime());
        assertNull(beanCodec.serialize(null));
        assertNull(beanCodec.deserialize(null));
    }

    @Test
    void timeUdtFallbackUsesEachValueLocalTimeAndZone() throws Exception {
        final UserType userType = timeType(new CodecRegistry());
        final CassandraExecutor.UDTCodec<Map> codec = CassandraExecutor.UDTCodec.create(userType, Map.class);
        final LocalTime time = LocalTime.of(12, 34, 56, 123_000_000);
        final LocalDate date = LocalDate.of(2026, 10, 12);
        final ZoneOffset offset = ZoneOffset.ofHoursMinutes(5, 30);
        final var instant = date.atTime(time).atZone(ZoneId.systemDefault()).toInstant();
        final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(offset));
        calendar.clear();
        calendar.set(2026, Calendar.OCTOBER, 12, 12, 34, 56);
        calendar.set(Calendar.MILLISECOND, 123);

        for (final Object value : List.of(date.atTime(time), time.atOffset(offset), date.atTime(time).atOffset(offset), date.atTime(time).atZone(offset),
                instant, Date.from(instant), calendar)) {
            assertEquals(time.toNanoOfDay(), codec.serialize(Map.of("time", value)).getTime(0), value.getClass().getName());
        }
    }

    public static final class ZoneChangeProbe {
        public static void main(final String[] args) throws Exception {
            // This runs in a separate JVM so default-zone changes and Abacus's captured static zone cannot leak into other tests.
            Class.forName("com.landawn.abacus.type.AbstractTemporalType");
            TimeZone.setDefault(TimeZone.getTimeZone("GMT+09:00"));
            final CassandraExecutor.UDTCodec<Map> codec = CassandraExecutor.UDTCodec.create(timeType(new CodecRegistry()), Map.class);

            for (final Date value : List.of(new Date(0), new java.sql.Time(0), java.sql.Timestamp.from(Instant.EPOCH))) {
                assertEquals(0, codec.serialize(Map.of("time", value)).getTime(0));
            }
            assertEquals(LocalTime.of(9, 0).toNanoOfDay(), codec.serialize(Map.of("time", Instant.EPOCH)).getTime(0));
        }
    }

    private static TypeCodec<Date> dateCodec(final long marker, final AtomicReference<RuntimeException> serializerFailure) {
        return new TypeCodec<>(DataType.time(), Date.class) {
            @Override
            public ByteBuffer serialize(final Date value, final ProtocolVersion protocolVersion) {
                if (serializerFailure.get() != null) {
                    throw serializerFailure.get();
                }
                return TypeCodec.time().serialize(value == null ? null : marker, protocolVersion);
            }

            @Override
            public Date deserialize(final ByteBuffer bytes, final ProtocolVersion protocolVersion) {
                final Long nanos = TypeCodec.time().deserialize(bytes, protocolVersion);
                return nanos == null ? null : new Date(nanos);
            }

            @Override
            public Date parse(final String value) {
                final Long nanos = TypeCodec.time().parse(value);
                return nanos == null ? null : new Date(nanos);
            }

            @Override
            public String format(final Date value) {
                return TypeCodec.time().format(value == null ? null : marker);
            }
        };
    }

    private static <T> void assertUnsupportedTimeValue(final T value, final Class<T> javaType) throws Exception {
        final CodecRegistry registry = spy(new CodecRegistry());
        final AtomicReference<CodecNotFoundException> lookupFailure = new AtomicReference<>();
        // Capture the real driver's failure so the assertions reject a replacement exception as well as an unintended fallback.
        final Answer<Object> recordFailure = invocation -> {
            try {
                return invocation.callRealMethod();
            } catch (final CodecNotFoundException e) {
                lookupFailure.set(e);
                throw e;
            }
        };
        doAnswer(recordFailure).when(registry).codecFor(DataType.time(), value);
        doAnswer(recordFailure).when(registry).codecFor(DataType.time(), javaType);

        final CassandraExecutor.UDTCodec<Map> codec = CassandraExecutor.UDTCodec.create(timeType(registry), Map.class);
        final Map<String, T> fields = Map.of("time", value);
        final CodecNotFoundException serializeFailure = assertThrows(CodecNotFoundException.class, () -> codec.serialize(fields, ProtocolVersion.V4));
        assertSame(lookupFailure.get(), serializeFailure);
        assertEquals(DataType.time(), serializeFailure.getCqlType());
        assertEquals(TypeToken.of(javaType), serializeFailure.getJavaType());

        lookupFailure.set(null);
        final CodecNotFoundException formatFailure = assertThrows(CodecNotFoundException.class, () -> codec.format(fields));
        assertSame(lookupFailure.get(), formatFailure);
        assertEquals(DataType.time(), formatFailure.getCqlType());
        assertEquals(TypeToken.of(javaType), formatFailure.getJavaType());
    }

    private static <T> T roundTrip(final UserType userType, final Class<T> beanClass, final T bean) {
        final CassandraExecutor.UDTCodec<T> codec = CassandraExecutor.UDTCodec.create(userType, beanClass);
        return codec.deserialize(codec.serialize(bean));
    }

    private static com.datastax.driver.core.ResultSet timeRows(final LocalTime time) {
        final com.datastax.driver.core.ResultSet resultSet = mock(com.datastax.driver.core.ResultSet.class);
        final com.datastax.driver.core.ColumnDefinitions columns = mock(com.datastax.driver.core.ColumnDefinitions.class);
        final com.datastax.driver.core.Row row = mock(com.datastax.driver.core.Row.class);
        when(resultSet.getColumnDefinitions()).thenReturn(columns);
        when(columns.size()).thenReturn(1);
        when(columns.getName(0)).thenReturn("time");
        when(columns.getType(0)).thenReturn(DataType.time());
        when(row.getObject(0)).thenReturn(time.toNanoOfDay());
        when(row.getColumnDefinitions()).thenReturn(columns);
        when(resultSet.iterator()).thenReturn(List.of(row).iterator());
        when(resultSet.all()).thenReturn(List.of(row));
        when(resultSet.getAvailableWithoutFetching()).thenReturn(1);
        return resultSet;
    }

    private static UserType timeType(final CodecRegistry registry) throws Exception {
        return fieldType(registry, DataType.time());
    }

    private static UserType fieldType(final CodecRegistry registry, final DataType fieldType) throws Exception {
        final var fieldConstructor = UserType.Field.class.getDeclaredConstructor(String.class, DataType.class);
        fieldConstructor.setAccessible(true);
        final var typeConstructor = UserType.class.getDeclaredConstructor(String.class, String.class, boolean.class, Collection.class, ProtocolVersion.class,
                CodecRegistry.class);
        typeConstructor.setAccessible(true);
        return typeConstructor.newInstance("ks", "timed_value", false, List.of(fieldConstructor.newInstance("time", fieldType)), ProtocolVersion.V4, registry);
    }

    public static final class TimedValue {
        private LocalTime time;

        public LocalTime getTime() {
            return time;
        }

        public void setTime(final LocalTime time) {
            this.time = time;
        }
    }

    public static final class TimestampTimedValue {
        private java.sql.Timestamp time;

        public java.sql.Timestamp getTime() {
            return time;
        }

        public void setTime(final java.sql.Timestamp value) {
            time = value;
        }
    }

    public static final class CalendarTimedValue {
        private Calendar time;

        public Calendar getTime() {
            return time;
        }

        public void setTime(final Calendar value) {
            time = value;
        }
    }

    public static final class InstantTimedValue {
        private Instant time;

        public Instant getTime() {
            return time;
        }

        public void setTime(final Instant value) {
            time = value;
        }
    }

    public static final class LocalDateTimeTimedValue {
        private LocalDateTime time;

        public LocalDateTime getTime() {
            return time;
        }

        public void setTime(final LocalDateTime value) {
            time = value;
        }
    }

    public static final class LegacyTimedValue {
        private Date time;

        public Date getTime() {
            return time;
        }

        public void setTime(final Date value) {
            time = value;
        }
    }

    // ---- 2026-10-20 rounify ----

    @Test
    void timestampUdtFieldsAcceptJavaTimeValuesAndReadIntoLocalDateTime_rounify() throws Exception {
        // Driver 3 has no java.time codecs, so a LocalDateTime, ZonedDateTime, OffsetDateTime or Instant in a TIMESTAMP field failed
        // with CodecNotFoundException although the same value bound to a timestamp column converts to the driver's Date (v4 twin:
        // cassandra.CassandraUdtTimeRegressionTest#timestampUdtFieldsAcceptJavaTimeValuesAndReadIntoLocalDateTime_cassfix).
        final UserType type = fieldType(new CodecRegistry(), DataType.timestamp());
        final CassandraExecutor.UDTCodec<Map> mapCodec = CassandraExecutor.UDTCodec.create(type, Map.class);
        final CassandraExecutor.UDTCodec<List> listCodec = CassandraExecutor.UDTCodec.create(type, List.class);
        final CassandraExecutor.UDTCodec<LocalDateTimeTimedValue> beanCodec = CassandraExecutor.UDTCodec.create(type, LocalDateTimeTimedValue.class);
        final CassandraExecutor.UDTCodec<ZonedDateTimedValue> zonedBeanCodec = CassandraExecutor.UDTCodec.create(type, ZonedDateTimedValue.class);
        final LocalDateTime local = LocalDateTime.of(2026, 10, 13, 12, 34, 56, 123_000_000);
        final Instant localInstant = local.atZone(ZoneId.systemDefault()).toInstant(); // the JVM default zone, not a hard-coded one
        final Instant instant = Instant.parse("2026-10-13T12:34:56.123Z");
        final ZonedDateTime tokyo = instant.atZone(ZoneId.of("Asia/Tokyo"));
        final OffsetDateTime offset = instant.atOffset(ZoneOffset.ofHours(5));

        for (final Map.Entry<Object, Instant> example : List.<Map.Entry<Object, Instant>> of(Map.entry(local, localInstant), Map.entry(tokyo, instant),
                Map.entry(offset, instant), Map.entry(instant, instant))) {
            final Object value = example.getKey();
            final Instant expected = example.getValue();
            final ByteBuffer encoded = mapCodec.serialize(Map.of("time", value), ProtocolVersion.V4);

            // The driver reads a timestamp field as a Date.
            assertEquals(Date.from(expected), mapCodec.deserialize(encoded, ProtocolVersion.V4).get("time"), value.getClass().getName());
            assertEquals(Date.from(expected), listCodec.deserialize(listCodec.serialize(List.of(value), ProtocolVersion.V4), ProtocolVersion.V4).get(0));
            final LocalDateTime decodedLocal = beanCodec.deserialize(encoded, ProtocolVersion.V4).getTime();
            final ZonedDateTime decodedZoned = zonedBeanCodec.deserialize(encoded, ProtocolVersion.V4).getTime();
            assertEquals(LocalDateTime.ofInstant(expected, ZoneId.systemDefault()), decodedLocal);
            // A zoned property is read in the JVM default zone, so it agrees with the LocalDateTime read.
            assertEquals(expected, decodedZoned.toInstant());
            assertEquals(ZoneId.systemDefault(), decodedZoned.getZone());
            assertEquals(decodedLocal, decodedZoned.toLocalDateTime());
        }

        final LocalDateTimeTimedValue bean = new LocalDateTimeTimedValue();
        bean.setTime(local);
        assertEquals(Date.from(localInstant), beanCodec.serialize(bean).getTimestamp(0));
        assertEquals(local, roundTrip(type, LocalDateTimeTimedValue.class, bean).getTime());
        final ZonedDateTimedValue zonedBean = new ZonedDateTimedValue();
        zonedBean.setTime(tokyo);
        assertEquals(Date.from(instant), zonedBeanCodec.serialize(zonedBean).getTimestamp(0));
        assertEquals(tokyo.withZoneSameInstant(ZoneId.systemDefault()), roundTrip(type, ZonedDateTimedValue.class, zonedBean).getTime());

        // A value the Date conversion rejects keeps the driver's lookup failure (the conversion failure is attached as suppressed).
        final CodecNotFoundException rejected = assertThrows(CodecNotFoundException.class, () -> mapCodec.serialize(Map.of("time", "not a timestamp")));
        assertEquals(DataType.timestamp(), rejected.getCqlType());
        assertEquals(TypeToken.of(String.class), rejected.getJavaType());
        assertEquals(1, rejected.getSuppressed().length);
        assertTrue(rejected.getSuppressed()[0] instanceof IllegalArgumentException);
    }

    public static final class ZonedDateTimedValue {
        private ZonedDateTime time;

        public ZonedDateTime getTime() {
            return time;
        }

        public void setTime(final ZonedDateTime value) {
            time = value;
        }
    }
}
