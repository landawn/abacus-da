package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

/**
 * {@code timestamp} columns and {@code java.time}: abacus-common 8.2.1's {@code N.convert} parses a {@code LocalDateTime}'s or
 * {@code ZonedDateTime}'s {@code toString()} as an {@code Instant} and fails with {@code DateTimeParseException} (which the bind
 * loop does not catch), and rejects an {@code Instant}'s trailing {@code Z} for a {@code LocalDateTime} target. The executor now
 * converts these itself; a {@code LocalDateTime} denotes its wall-clock time in the JVM default zone (as in the driver 3 executor
 * and a raw {@code CqlBuilder} literal). Driver objects are mocked; parameter and column conversion are real.
 */
class CassandraExecutorDeepReview2026TimestampBindTest {

    private static final String UPDATE = "UPDATE events SET ts = ? WHERE id = 1";
    private static final String QUERY = "SELECT ts FROM events";
    private static final Instant INSTANT = Instant.parse("2023-11-14T22:13:20.123Z");

    @Test
    void javaTimeValuesBoundToATimestampColumnReachBindAsInstants() {
        final LocalDateTime local = LocalDateTime.of(2023, 11, 14, 14, 13, 20, 123_000_000);
        final Instant localInstant = local.atZone(ZoneId.systemDefault()).toInstant(); // the JVM default zone, not a hard-coded one
        final ZonedDateTime tokyo = INSTANT.atZone(ZoneId.of("Asia/Tokyo"));
        final OffsetDateTime offset = INSTANT.atOffset(ZoneOffset.ofHours(5));

        try (Fixture fixture = new Fixture()) {
            fixture.executor.execute(UPDATE, local);
            fixture.executor.execute(UPDATE, tokyo);
            fixture.executor.execute(UPDATE, offset);
            fixture.executor.execute(UPDATE, INSTANT);
            fixture.executor.execute(UPDATE, Date.from(INSTANT));
            fixture.executor.execute(UPDATE, INSTANT.toEpochMilli());

            final List<Object> bound = fixture.boundValues();

            assertAll(() -> assertEquals(6, bound.size()), () -> assertEquals(localInstant, assertInstanceOf(Instant.class, bound.get(0))),
                    () -> assertEquals(localInstant.toEpochMilli(), ((Instant) bound.get(0)).toEpochMilli()),
                    () -> assertEquals(INSTANT, assertInstanceOf(Instant.class, bound.get(1))),
                    () -> assertEquals(INSTANT, assertInstanceOf(Instant.class, bound.get(2))), () -> assertEquals(INSTANT, bound.get(3)),
                    () -> assertEquals(INSTANT, assertInstanceOf(Instant.class, bound.get(4))),
                    () -> assertEquals(INSTANT, assertInstanceOf(Instant.class, bound.get(5))));
        }
    }

    @Test
    void timestampColumnsReadIntoLocalDateTimeTargetsAndProperties() {
        final LocalDateTime expected = LocalDateTime.ofInstant(INSTANT, ZoneId.systemDefault());

        try (Fixture fixture = new Fixture()) {
            when(fixture.session.execute(any(Statement.class))).thenAnswer(invocation -> result(INSTANT, INSTANT));

            assertAll(() -> assertEquals(expected, fixture.executor.findFirst(LocalDateTime.class, QUERY).get()),
                    () -> assertEquals(expected, fixture.executor.queryForSingleValue(LocalDateTime.class, QUERY).get()),
                    () -> assertEquals(expected, fixture.executor.queryForSingleNonNull(LocalDateTime.class, QUERY).get()),
                    () -> assertEquals(List.of(expected, expected), fixture.executor.list(LocalDateTime.class, QUERY)),
                    () -> assertEquals(List.of(expected, expected), CassandraExecutor.toList(result(INSTANT, INSTANT), LocalDateTime.class)), () -> {
                        try (var stream = fixture.executor.stream(LocalDateTime.class, QUERY)) {
                            assertEquals(List.of(expected, expected), stream.toList());
                        }
                    },
                    // Bean property, through toEntity and through extractData's per-column conversion.
                    () -> assertEquals(expected, fixture.executor.findFirst(Event.class, QUERY).get().getTs()),
                    () -> assertEquals(expected, CassandraExecutor.toEntity(result(INSTANT).one(), Event.class).getTs()),
                    () -> assertEquals(List.of(expected, expected), CassandraExecutor.extractData(result(INSTANT, INSTANT), Event.class).getColumn("ts")),
                    () -> assertEquals(expected, fixture.executor.query(Event.class, QUERY).getRow(0, Event.class).getTs()),
                    // Pinned: Instant into Date/Long.
                    () -> assertEquals(Date.from(INSTANT), fixture.executor.findFirst(Date.class, QUERY).get()),
                    () -> assertEquals(INSTANT.toEpochMilli(), fixture.executor.findFirst(Long.class, QUERY).get()));
        }
    }

    @Test
    void timestampColumnsReadIntoZonedTargetsInTheJvmDefaultZone() {
        // N.convert(Instant, ZonedDateTime/OffsetDateTime.class) parses Instant.toString() and so gave offset UTC; the executor now
        // expresses the (preserved) instant in the JVM default zone, like its LocalDateTime read and like the driver 3 executor.
        final ZoneId zone = ZoneId.systemDefault();
        final LocalDateTime local = LocalDateTime.ofInstant(INSTANT, zone);
        final ZonedDateTime expectedZoned = INSTANT.atZone(zone);
        final OffsetDateTime expectedOffset = OffsetDateTime.ofInstant(INSTANT, zone);

        try (Fixture fixture = new Fixture()) {
            when(fixture.session.execute(any(Statement.class))).thenAnswer(invocation -> result(INSTANT, INSTANT));

            final ZonedDateTime zoned = fixture.executor.findFirst(ZonedDateTime.class, QUERY).get();
            final OffsetDateTime offset = fixture.executor.findFirst(OffsetDateTime.class, QUERY).get();
            final ZonedEvent bean = CassandraExecutor.toEntity(zonedEventRows(INSTANT).one(), ZonedEvent.class);

            assertAll(() -> assertEquals(zone, zoned.getZone()), () -> assertEquals(INSTANT, zoned.toInstant()),
                    () -> assertEquals(local, zoned.toLocalDateTime()), () -> assertEquals(expectedZoned, zoned),
                    () -> assertEquals(zone.getRules().getOffset(INSTANT), offset.getOffset()), () -> assertEquals(INSTANT, offset.toInstant()),
                    () -> assertEquals(local, offset.toLocalDateTime()), () -> assertEquals(expectedOffset, offset),
                    () -> assertEquals(List.of(expectedZoned, expectedZoned), fixture.executor.list(ZonedDateTime.class, QUERY)),
                    () -> assertEquals(List.of(expectedOffset, expectedOffset), fixture.executor.list(OffsetDateTime.class, QUERY)),
                    () -> assertEquals(List.of(expectedZoned, expectedZoned), CassandraExecutor.toList(result(INSTANT, INSTANT), ZonedDateTime.class)),
                    // Bean properties, through toEntity and extractData.
                    () -> assertEquals(expectedZoned, bean.getZoned()), () -> assertEquals(expectedOffset, bean.getOffset()),
                    () -> assertEquals(local, bean.getZoned().toLocalDateTime()), () -> assertEquals(local, bean.getOffset().toLocalDateTime()),
                    () -> assertEquals(List.of(expectedZoned, expectedZoned),
                            CassandraExecutor.extractData(zonedEventRows(INSTANT, INSTANT), ZonedEvent.class).getColumn("zoned")),
                    () -> assertEquals(List.of(expectedOffset, expectedOffset),
                            CassandraExecutor.extractData(zonedEventRows(INSTANT, INSTANT), ZonedEvent.class).getColumn("offset")));

            when(fixture.session.execute(any(Statement.class))).thenAnswer(invocation -> zonedEventRows(INSTANT));
            final ZonedEvent found = fixture.executor.findFirst(ZonedEvent.class, QUERY).get();

            assertAll(() -> assertEquals(expectedZoned, found.getZoned()), () -> assertEquals(expectedOffset, found.getOffset()));
        }
    }

    /** Rows with two timestamp columns, {@code zoned} and {@code offset}, both holding the given instant. */
    private static ResultSet zonedEventRows(final Instant... values) {
        final ColumnDefinitions columns = mock(ColumnDefinitions.class);
        when(columns.size()).thenReturn(2);
        final String[] names = { "zoned", "offset" };
        for (int i = 0; i < names.length; i++) {
            final ColumnDefinition column = mock(ColumnDefinition.class);
            when(columns.get(i)).thenReturn(column);
            when(column.getName()).thenReturn(CqlIdentifier.fromInternal(names[i]));
            when(column.getType()).thenReturn(DataTypes.TIMESTAMP);
        }
        final List<Row> rows = Arrays.stream(values).map(value -> {
            final Row row = mock(Row.class);
            when(row.getColumnDefinitions()).thenReturn(columns);
            when(row.getObject(0)).thenReturn(value);
            when(row.getObject(1)).thenReturn(value);
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

    public static class Event {
        private LocalDateTime ts;

        public LocalDateTime getTs() {
            return ts;
        }

        public void setTs(final LocalDateTime ts) {
            this.ts = ts;
        }
    }

    public static class ZonedEvent {
        private ZonedDateTime zoned;
        private OffsetDateTime offset;

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

    private static ResultSet result(final Object... values) {
        final ColumnDefinitions columns = mock(ColumnDefinitions.class);
        final ColumnDefinition column = mock(ColumnDefinition.class);
        when(columns.size()).thenReturn(1);
        when(columns.get(0)).thenReturn(column);
        when(column.getName()).thenReturn(CqlIdentifier.fromInternal("ts"));
        when(column.getType()).thenReturn(DataTypes.TIMESTAMP);
        final List<Row> rows = Arrays.stream(values).map(value -> {
            final Row row = mock(Row.class);
            when(row.getColumnDefinitions()).thenReturn(columns);
            when(row.getObject(0)).thenReturn(value);
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

    /** One {@code timestamp} bind variable; every {@code bind(...)} call records the values it receives. */
    private static final class Fixture implements AutoCloseable {
        final CqlSession session = mock(CqlSession.class);
        final PreparedStatement prepared = mock(PreparedStatement.class);
        final List<Object[]> binds = new ArrayList<>();
        final CassandraExecutor executor;

        Fixture() {
            final DriverContext context = mock(DriverContext.class);
            when(session.getContext()).thenReturn(context);
            when(context.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT);
            final ColumnDefinitions variables = mock(ColumnDefinitions.class);
            final ColumnDefinition ts = mock(ColumnDefinition.class);
            when(variables.size()).thenReturn(1);
            when(variables.get(0)).thenReturn(ts);
            when(ts.getName()).thenReturn(CqlIdentifier.fromInternal("ts"));
            when(ts.getType()).thenReturn(DataTypes.TIMESTAMP);
            when(prepared.getVariableDefinitions()).thenReturn(variables);
            when(prepared.bind(any(Object[].class))).thenAnswer(invocation -> {
                binds.add(((Object[]) invocation.getRawArguments()[0]).clone());
                return mock(BoundStatement.class);
            });
            when(session.prepare(anyString())).thenReturn(prepared);
            when(session.execute(any(Statement.class))).thenReturn(mock(ResultSet.class));
            executor = new CassandraExecutor(session);
        }

        List<Object> boundValues() {
            final List<Object> values = new ArrayList<>(binds.size());

            for (final Object[] bind : binds) {
                assertEquals(1, bind.length);
                values.add(bind[0]);
            }

            return values;
        }

        @Override
        public void close() {
            executor.close();
        }
    }
}
