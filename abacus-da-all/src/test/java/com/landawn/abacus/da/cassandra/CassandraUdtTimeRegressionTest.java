package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.DefaultUserDefinedType;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;

public class CassandraUdtTimeRegressionTest {
    @Test
    void sqlTimeUdtFieldsRoundTripForBeansMapsAndCollections() {
        final UserDefinedType type = fieldType(DataTypes.TIME, new DefaultCodecRegistry("sql-time-udt"));
        final CassandraExecutor.UDTCodec<SqlTimedValue> beanCodec = CassandraExecutor.UDTCodec.create(type, SqlTimedValue.class);
        final CassandraExecutor.UDTCodec<Map> mapCodec = CassandraExecutor.UDTCodec.create(type, Map.class);
        final CassandraExecutor.UDTCodec<List> listCodec = CassandraExecutor.UDTCodec.create(type, List.class);
        final java.sql.Time time = java.sql.Time.valueOf("12:34:56");
        time.setTime(time.getTime() + 123);
        final long originalMillis = time.getTime();
        final LocalTime expected = LocalTime.of(12, 34, 56, 123_000_000);
        final SqlTimedValue bean = new SqlTimedValue();
        bean.setTime(time);

        assertEquals(expected, beanCodec.serialize(bean).getLocalTime(0));
        assertEquals(originalMillis, beanCodec.decode(beanCodec.encode(bean, ProtocolVersion.V4), ProtocolVersion.V4).getTime().getTime());
        assertEquals(originalMillis, beanCodec.parse(beanCodec.format(bean)).getTime().getTime());
        assertEquals(expected, mapCodec.decode(mapCodec.encode(Map.of("time", time), ProtocolVersion.V4), ProtocolVersion.V4).get("time"));
        assertEquals(expected, listCodec.decode(listCodec.encode(List.of(time), ProtocolVersion.V4), ProtocolVersion.V4).get(0));
        assertEquals(originalMillis, time.getTime());
    }

    @Test
    void dateTimestampAndCalendarFieldsRetainPrecisionAndTheirTimeZones() {
        final UserDefinedType type = fieldType(DataTypes.TIME, new DefaultCodecRegistry("legacy-time-udt"));
        final CassandraExecutor.UDTCodec<Map> mapCodec = CassandraExecutor.UDTCodec.create(type, Map.class);
        final CassandraExecutor.UDTCodec<List> listCodec = CassandraExecutor.UDTCodec.create(type, List.class);
        final CassandraExecutor.UDTCodec<LocalTimedValue> beanCodec = CassandraExecutor.UDTCodec.create(type, LocalTimedValue.class);
        final Instant instant = Instant.parse("2026-10-13T12:34:56.123456789Z");
        final Date date = Date.from(instant);
        final Date beforeEpoch = new Date(-1);
        final java.sql.Timestamp timestamp = java.sql.Timestamp.from(instant);
        final Calendar east = calendar(instant, "GMT+05:30");
        final Calendar west = calendar(instant, "GMT-07:00");

        for (final Map.Entry<Object, LocalTime> example : List.<Map.Entry<Object, LocalTime>> of(
                Map.entry(date, LocalTime.ofInstant(date.toInstant(), ZoneId.systemDefault())),
                Map.entry(beforeEpoch, LocalTime.ofInstant(Instant.ofEpochMilli(-1), ZoneId.systemDefault())),
                Map.entry(timestamp, LocalTime.ofInstant(instant, ZoneId.systemDefault())), Map.entry(east, LocalTime.of(18, 4, 56, 123_000_000)),
                Map.entry(west, LocalTime.of(5, 34, 56, 123_000_000)))) {
            final Object value = example.getKey();
            final LocalTime expected = example.getValue();
            final long originalMillis = value instanceof Calendar ? ((Calendar) value).getTimeInMillis() : ((Date) value).getTime();
            final ByteBuffer encoded = mapCodec.encode(Map.of("time", value), ProtocolVersion.V4);

            assertEquals(expected, mapCodec.decode(encoded, ProtocolVersion.V4).get("time"), value.getClass().getName());
            assertEquals(expected, beanCodec.decode(encoded, ProtocolVersion.V4).getTime());
            assertEquals(expected, listCodec.decode(listCodec.encode(List.of(value), ProtocolVersion.V4), ProtocolVersion.V4).get(0));
            assertEquals(originalMillis, value instanceof Calendar ? ((Calendar) value).getTimeInMillis() : ((Date) value).getTime());
        }

        assertEquals(123_456_789, timestamp.getNanos());
        assertEquals("GMT+05:30", east.getTimeZone().getID());
        assertEquals("GMT-07:00", west.getTimeZone().getID());
    }

    @Test
    void dateTimestampAndCalendarBeanPropertiesReadBackTheirTimeOfDay() {
        final UserDefinedType type = fieldType(DataTypes.TIME, new DefaultCodecRegistry("legacy-bean-time-udt"));
        final Instant instant = Instant.parse("2026-10-13T12:34:56.123456789Z");
        final LocalTime local = LocalTime.ofInstant(instant, ZoneId.systemDefault());

        // Decoding a time into these properties failed after a successful encode: N.convert parsed the LocalTime's text.
        final DateTimedValue dateBean = new DateTimedValue();
        dateBean.setTime(Date.from(instant));
        final Date date = roundTrip(type, DateTimedValue.class, dateBean).getTime();
        assertEquals(LocalDate.EPOCH.atTime(local.truncatedTo(ChronoUnit.MILLIS)), LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault()));

        final TimestampTimedValue timestampBean = new TimestampTimedValue();
        timestampBean.setTime(java.sql.Timestamp.from(instant));
        final java.sql.Timestamp timestamp = roundTrip(type, TimestampTimedValue.class, timestampBean).getTime();
        assertEquals(LocalDate.EPOCH.atTime(local), timestamp.toLocalDateTime());

        final CalendarTimedValue calendarBean = new CalendarTimedValue();
        calendarBean.setTime(calendar(instant, "GMT+05:30"));
        final Calendar decoded = roundTrip(type, CalendarTimedValue.class, calendarBean).getTime();
        assertEquals(LocalDate.EPOCH.atTime(18, 4, 56, 123_000_000), LocalDateTime.ofInstant(decoded.toInstant(), decoded.getTimeZone().toZoneId()));
    }

    @Test
    void timeColumnsReadIntoDateAndJavaTimeTypesKeepTheirTimeOfDay() {
        final LocalTime time = LocalTime.of(12, 34, 56, 123_456_789);
        final LocalDateTime expected = LocalDate.EPOCH.atTime(time);

        assertEquals(expected, CassandraExecutor.toList(timeRows(time), LocalDateTime.class).get(0));
        assertEquals(expected.atZone(ZoneId.systemDefault()).toInstant(), CassandraExecutor.toList(timeRows(time), Instant.class).get(0));
        assertEquals(expected.atZone(ZoneId.systemDefault()), CassandraExecutor.toList(timeRows(time), ZonedDateTime.class).get(0));
        assertEquals(expected, CassandraExecutor.toList(timeRows(time), java.sql.Timestamp.class).get(0).toLocalDateTime());
        assertEquals(expected.truncatedTo(ChronoUnit.MILLIS),
                LocalDateTime.ofInstant(CassandraExecutor.toList(timeRows(time), Date.class).get(0).toInstant(), ZoneId.systemDefault()));
        assertEquals(time, CassandraExecutor.toList(timeRows(time), LocalTime.class).get(0));
        assertEquals(expected.truncatedTo(ChronoUnit.MILLIS), LocalDateTime.ofInstant(
                ((Date) CassandraExecutor.extractData(timeRows(time), DateTimedValue.class).getColumn("time").get(0)).toInstant(), ZoneId.systemDefault()));
    }

    @Test
    void nativeLocalTimeBoundsAndNullFieldsKeepDriverRepresentations() {
        final UserDefinedType type = fieldType(DataTypes.TIME, new DefaultCodecRegistry("native-time-udt"));
        final CassandraExecutor.UDTCodec<LocalTimedValue> beanCodec = CassandraExecutor.UDTCodec.create(type, LocalTimedValue.class);
        final CassandraExecutor.UDTCodec<Map> mapCodec = CassandraExecutor.UDTCodec.create(type, Map.class);
        final CassandraExecutor.UDTCodec<List> listCodec = CassandraExecutor.UDTCodec.create(type, List.class);

        for (final LocalTime time : List.of(LocalTime.MIN, LocalTime.MAX, LocalTime.of(12, 34, 56, 123_456_789))) {
            final LocalTimedValue bean = new LocalTimedValue();
            bean.setTime(time);
            final ByteBuffer encoded = beanCodec.encode(bean, ProtocolVersion.V4);
            assertEquals(time, beanCodec.decode(encoded, ProtocolVersion.V4).getTime());
            assertEquals(time, mapCodec.decode(encoded, ProtocolVersion.V4).get("time"));
            assertEquals(time, listCodec.decode(encoded, ProtocolVersion.V4).get(0));
        }

        final var nullField = mapCodec.serialize(Collections.singletonMap("time", null));
        assertTrue(nullField.isNull(0));
        assertNull(beanCodec.deserialize(nullField).getTime());
        assertTrue(beanCodec.serialize(new LocalTimedValue()).isNull(0));
        assertTrue(listCodec.serialize(Collections.singletonList(null)).isNull(0));
        assertNull(beanCodec.encode(null, ProtocolVersion.V4));
        assertNull(beanCodec.decode(null, ProtocolVersion.V4));
        assertEquals("NULL", beanCodec.format(null));
    }

    @Test
    void registeredLegacyTimeCodecHasPriorityAndSerializerFailuresPropagate() {
        final AtomicReference<RuntimeException> serializerFailure = new AtomicReference<>();
        final LocalTime marker = LocalTime.of(1, 2, 3, 987_654_321);
        final DefaultCodecRegistry registry = new DefaultCodecRegistry("custom-time-udt");
        registry.register(new TypeCodec<java.sql.Time>() {
            @Override
            public GenericType<java.sql.Time> getJavaType() {
                return GenericType.of(java.sql.Time.class);
            }

            @Override
            public DataType getCqlType() {
                return DataTypes.TIME;
            }

            @Override
            public ByteBuffer encode(final java.sql.Time value, final ProtocolVersion version) {
                if (serializerFailure.get() != null) {
                    throw serializerFailure.get();
                }
                return TypeCodecs.TIME.encode(value == null ? null : marker, version);
            }

            @Override
            public java.sql.Time decode(final ByteBuffer value, final ProtocolVersion version) {
                final LocalTime decoded = TypeCodecs.TIME.decode(value, version);
                return decoded == null ? null : java.sql.Time.valueOf(decoded);
            }

            @Override
            public String format(final java.sql.Time value) {
                return TypeCodecs.TIME.format(value == null ? null : marker);
            }

            @Override
            public java.sql.Time parse(final String value) {
                final LocalTime parsed = TypeCodecs.TIME.parse(value);
                return parsed == null ? null : java.sql.Time.valueOf(parsed);
            }
        });
        final UserDefinedType type = fieldType(DataTypes.TIME, registry);
        final CassandraExecutor.UDTCodec<Map> codec = CassandraExecutor.UDTCodec.create(type, Map.class);
        final java.sql.Time value = java.sql.Time.valueOf("12:34:56");
        assertEquals(marker, codec.decode(codec.encode(Map.of("time", value), ProtocolVersion.V4), ProtocolVersion.V4).get("time"));

        for (final RuntimeException failure : List.of(new IllegalStateException("serializer failed"),
                new CodecNotFoundException(DataTypes.TIME, GenericType.of(java.sql.Time.class)))) {
            serializerFailure.set(failure);
            assertSame(failure, assertThrows(RuntimeException.class, () -> codec.encode(Map.of("time", value), ProtocolVersion.V4)));
            assertSame(failure, assertThrows(RuntimeException.class, () -> codec.format(Map.of("time", value))));
        }
    }

    @Test
    void unsupportedTimeValuesKeepTheOriginalLookupException() {
        final AtomicReference<CodecNotFoundException> lookupFailure = new AtomicReference<>();
        final CodecRegistry registry = recordingRegistry(lookupFailure);
        final CassandraExecutor.UDTCodec<Map> codec = CassandraExecutor.UDTCodec.create(fieldType(DataTypes.TIME, registry), Map.class);
        final LocalTime time = LocalTime.of(12, 34, 56);
        final var dateTime = LocalDate.of(2026, 10, 13).atTime(time);

        for (final Object value : List.of("12:34:56", 123, 123L, dateTime, time.atOffset(ZoneOffset.UTC), dateTime.atOffset(ZoneOffset.UTC),
                dateTime.atZone(ZoneOffset.UTC), Instant.EPOCH)) {
            lookupFailure.set(null);
            final CodecNotFoundException actual = assertThrows(CodecNotFoundException.class, () -> codec.encode(Map.of("time", value), ProtocolVersion.V4));
            assertNotNull(lookupFailure.get());
            assertSame(lookupFailure.get(), actual, value.getClass().getName());
        }
    }

    @Test
    void legacyDateValuesDoNotEnableFallbackForOtherCqlTypes() {
        final AtomicReference<CodecNotFoundException> lookupFailure = new AtomicReference<>();
        final CodecRegistry registry = recordingRegistry(lookupFailure);
        final CassandraExecutor.UDTCodec<Map> codec = CassandraExecutor.UDTCodec.create(fieldType(DataTypes.TEXT, registry), Map.class);

        for (final Object value : List.of(new Date(-1), java.sql.Time.valueOf("12:34:56"), calendar(Instant.EPOCH, "GMT+05:30"))) {
            lookupFailure.set(null);
            final CodecNotFoundException actual = assertThrows(CodecNotFoundException.class, () -> codec.encode(Map.of("time", value), ProtocolVersion.V4));
            assertNotNull(lookupFailure.get());
            assertSame(lookupFailure.get(), actual);
        }
    }

    private static <T> T roundTrip(final UserDefinedType type, final Class<T> beanClass, final T bean) {
        final CassandraExecutor.UDTCodec<T> codec = CassandraExecutor.UDTCodec.create(type, beanClass);
        return codec.decode(codec.encode(bean, ProtocolVersion.V4), ProtocolVersion.V4);
    }

    private static ResultSet timeRows(final LocalTime time) {
        final ResultSet resultSet = mock(ResultSet.class);
        final ColumnDefinitions columns = mock(ColumnDefinitions.class);
        final ColumnDefinition column = mock(ColumnDefinition.class);
        final Row row = mock(Row.class);
        when(resultSet.getColumnDefinitions()).thenReturn(columns);
        when(columns.size()).thenReturn(1);
        when(columns.get(0)).thenReturn(column);
        when(column.getName()).thenReturn(CqlIdentifier.fromInternal("time"));
        when(row.getObject(0)).thenReturn(time);
        when(row.getColumnDefinitions()).thenReturn(columns);
        when(resultSet.iterator()).thenReturn(List.of(row).iterator());
        when(resultSet.all()).thenReturn(List.of(row));
        when(resultSet.getAvailableWithoutFetching()).thenReturn(1);
        return resultSet;
    }

    private static Calendar calendar(final Instant instant, final String zone) {
        final Calendar result = Calendar.getInstance(TimeZone.getTimeZone(zone));
        result.setTimeInMillis(instant.toEpochMilli());
        return result;
    }

    private static CodecRegistry recordingRegistry(final AtomicReference<CodecNotFoundException> lookupFailure) {
        final DefaultCodecRegistry registry = spy(new DefaultCodecRegistry("lookup-time-udt"));
        doAnswer(invocation -> {
            try {
                return invocation.callRealMethod();
            } catch (final CodecNotFoundException failure) {
                lookupFailure.set(failure);
                throw failure;
            }
        }).when(registry).codecFor(any(DataType.class), any(Object.class));
        return registry;
    }

    private static UserDefinedType fieldType(final DataType fieldType, final CodecRegistry registry) {
        return new DefaultUserDefinedType(CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("timed_value"), false,
                List.of(CqlIdentifier.fromInternal("time")), List.of(fieldType), new AttachmentPoint() {
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

    public static final class SqlTimedValue {
        private java.sql.Time time;

        public java.sql.Time getTime() {
            return time;
        }

        public void setTime(final java.sql.Time value) {
            time = value;
        }
    }

    public static final class DateTimedValue {
        private Date time;

        public Date getTime() {
            return time;
        }

        public void setTime(final Date value) {
            time = value;
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

    public static final class LocalTimedValue {
        private LocalTime time;

        public LocalTime getTime() {
            return time;
        }

        public void setTime(final LocalTime value) {
            time = value;
        }
    }
}
