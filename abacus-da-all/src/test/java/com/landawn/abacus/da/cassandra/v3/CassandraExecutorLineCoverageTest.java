package com.landawn.abacus.da.cassandra.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.logging.Level;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.mapping.annotations.Table;
import com.landawn.abacus.annotation.Column;
import com.landawn.abacus.annotation.JsonXmlField;
import com.landawn.abacus.da.MockitoTestBase;
import com.landawn.abacus.util.u.OptionalByte;
import com.landawn.abacus.util.u.OptionalChar;
import com.landawn.abacus.util.u.OptionalDouble;
import com.landawn.abacus.util.u.OptionalFloat;
import com.landawn.abacus.util.u.OptionalInt;
import com.landawn.abacus.util.u.OptionalShort;
import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.util.ContinuableFuture;

/**
 * Driver 3 line coverage for {@link CassandraExecutor} and the memoized failure in {@link AsyncCassandraExecutor}.
 */
class CassandraExecutorLineCoverageTest extends MockitoTestBase {

    private static final LocalTime TIME_OF_DAY = LocalTime.of(1, 2, 3);
    private static final String PARAMETERLESS = "SELECT 1";

    @Test
    void registerRowConverterReturnsWhenTheDriverRowClassIsAbsent() throws Exception {
        final Method register = CassandraExecutor.class.getDeclaredMethod("registerRowConverter", String.class, BiFunction.class);
        register.setAccessible(true);
        final BiFunction<Row, Class<?>, Object> converter = (row, type) -> row;

        assertNull(register.invoke(null, "com.landawn.abacus.da.cassandra.v3.NoSuchDriverRow", converter));
    }

    @Test
    void mapperReportsAMissingKeyspaceNamedOnTheTable() {
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            final Metadata metadata = mock(Metadata.class);
            when(fixture.cluster.getMetadata()).thenReturn(metadata);

            final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> fixture.executor.mapper(LineItem.class));

            assertEquals("Keyspace line_ks does not exist", exception.getMessage());
        }
    }

    @Test
    void bindKeepsTextThatIsNotAJsonObjectForAMapColumnWhenDebugIsOn() {
        final Runnable restore = enableDebug(CassandraExecutor.class);
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.columns(DataType.map(DataType.text(), DataType.cint()), "v");
            fixture.executor.execute("INSERT INTO line_rows (v) VALUES (?)", "abc");

            assertEquals(1, fixture.values.size());
            assertSame("abc", fixture.values.get(0)[0]);
        } finally {
            restore.run();
        }
    }

    @Test
    void toEntityCopiesEarlierElementsWhenALaterOneConvertsAndReadsANestedRow() {
        final List<Object> labels = List.of("keep", 2);
        final Map<String, Object> attributes = new LinkedHashMap<>();
        attributes.put("a", "keep");
        attributes.put("b", 2);
        final List<String> payload = List.of("same");
        final List<String> notes = new ArrayList<>();
        notes.add(null);
        final Row inner = row(new String[] { "value" }, "Ada");
        final Row nested = row(new String[] { "child" }, inner);
        final Row source = row(new String[] { "labels", "attributes", "payload", "notes", "nested" }, labels, attributes, payload, notes, nested);

        final ContainerBean entity = CassandraExecutor.toEntity(source, ContainerBean.class);

        assertEquals(List.of("keep", "2"), entity.getLabels());
        assertEquals(Map.of("a", "keep", "b", "2"), entity.getAttributes());
        assertSame(payload, entity.getPayload());
        assertSame(notes, entity.getNotes());
        assertEquals(List.of(List.of("Ada")), entity.getNested());
    }

    @Test
    void toEntityReadsATimeColumnIntoOffsetTimeZonedDateTimeAndOffsetDateTime() {
        final long nanos = TIME_OF_DAY.toNanoOfDay();
        final LocalDateTime onEpoch = LocalDate.EPOCH.atTime(TIME_OF_DAY);
        final Row source = row(DataType.time(), new String[] { "offsetTime", "zonedDateTime", "offsetDateTime" }, nanos, nanos, nanos);

        final TimeBean entity = CassandraExecutor.toEntity(source, TimeBean.class);

        assertEquals(TIME_OF_DAY.atOffset(ZoneId.systemDefault().getRules().getOffset(onEpoch)), entity.getOffsetTime());
        assertEquals(onEpoch.atZone(ZoneId.systemDefault()), entity.getZonedDateTime());
        assertEquals(onEpoch.atZone(ZoneId.systemDefault()).toOffsetDateTime(), entity.getOffsetDateTime());
    }

    @Test
    void readRowOfANullRowReturnsTheTargetDefault() throws Exception {
        final Method readRow = CassandraExecutor.class.getDeclaredMethod("readRow", Row.class, Class.class);
        readRow.setAccessible(true);

        assertNull(readRow.invoke(null, null, String.class));
        assertEquals(0, readRow.invoke(null, null, int.class));
    }

    @Test
    void prepareLogsTheCqlWhenDebugIsEnabled() {
        final Runnable restore = enableDebug(CassandraExecutor.class);
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.executor.execute(PARAMETERLESS);

            verify(fixture.session).prepare(eq(PARAMETERLESS));
        } finally {
            restore.run();
        }
    }

    @Test
    void setUdtFieldWritesASetAMapANestedUdtAndATuple() throws Exception {
        final CodecRegistry registry = new CodecRegistry();
        final UserType nestedType = userType(registry, "line_nested", "name", DataType.text());
        final UDTValue nested = nestedType.newValue();
        nested.setString(0, "ada");
        final TupleType tupleType = TupleType.of(ProtocolVersion.V4, registry, DataType.text());
        final TupleValue tuple = tupleType.newValue("ada");
        final UserType userType = userType(registry, "line_composite", "tags", DataType.set(DataType.text()), "attrs",
                DataType.map(DataType.text(), DataType.text()), "nested", nestedType, "pair", tupleType);
        final Set<String> tags = Set.of("a");
        final Map<String, String> attrs = Map.of("k", "v");
        final CassandraExecutor.UDTCodec<List> codec = CassandraExecutor.UDTCodec.create(userType, List.class);

        final UDTValue value = codec.serialize(List.of(tags, attrs, nested, tuple));

        assertEquals(tags, value.getSet(0, String.class));
        assertEquals(attrs, value.getMap(1, String.class, String.class));
        assertEquals("ada", value.getUDTValue(2).getString(0));
        assertEquals("ada", value.getTupleValue(3).getString(0));
    }

    @Test
    void udtFieldDescriptionNamesAnIndexPastTheLastField() throws Exception {
        final UserType userType = userType(new CodecRegistry(), "line_one", "name", DataType.text());
        final Method describe = CassandraExecutor.class.getDeclaredMethod("udtFieldDescription", UDTValue.class, int.class);
        describe.setAccessible(true);

        assertEquals("at index 3", describe.invoke(null, userType.newValue(), 3));
    }

    @Test
    void serializeRejectsACollectionLongerThanTheUdt() throws Exception {
        final UserType userType = userType(new CodecRegistry(), "line_one", "name", DataType.text());
        final CassandraExecutor.UDTCodec<List> codec = CassandraExecutor.UDTCodec.create(userType, List.class);

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> codec.serialize(List.of("a", "b")));

        assertEquals("Collection size cannot exceed UDT field count: expected at most " + userType.size() + " but got 2", exception.getMessage());
    }

    @Test
    void udtCodecCreateResolvesTheUserTypeFromClusterMetadata() throws Exception {
        final UserType userType = userType(new CodecRegistry(), "line_type", "name", DataType.text());
        final Cluster cluster = mock(Cluster.class);
        final Metadata metadata = mock(Metadata.class);
        final KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
        when(cluster.getMetadata()).thenReturn(metadata);
        when(metadata.getKeyspace("line_ks")).thenReturn(keyspace);
        when(keyspace.getUserType("line_type")).thenReturn(userType);

        final CassandraExecutor.UDTCodec<Map> codec = CassandraExecutor.UDTCodec.create(cluster, "line_ks", "line_type", Map.class);
        final IllegalArgumentException missingClass = assertThrows(IllegalArgumentException.class,
                () -> CassandraExecutor.UDTCodec.create(cluster, "line_ks", "line_type", null));

        assertSame(userType, codec.getCqlType());
        assertEquals("'javaClazz' cannot be null", missingClass.getMessage());
    }

    @Test
    void streamReplaysTheCachedIteratorFailure() throws Exception {
        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            final ResultSet rows = mock(ResultSet.class);
            final IllegalStateException cursorFailed = new IllegalStateException("cursor failed");
            when(rows.iterator()).thenThrow(cursorFailed);
            final ResultSetFuture future = mock(ResultSetFuture.class);
            when(future.get()).thenReturn(rows);
            when(future.isDone()).thenReturn(true);
            when(fixture.session.executeAsync(any(Statement.class))).thenReturn(future);

            final ContinuableFuture<?> streamed = fixture.executor.async().stream(new SimpleStatement("SELECT id FROM line_items"), (definitions, row) -> "x");
            final ExecutionException first = assertThrows(ExecutionException.class, streamed::get);
            final ExecutionException second = assertThrows(ExecutionException.class, streamed::get);

            assertSame(cursorFailed, first.getCause());
            assertSame(cursorFailed, second.getCause());
            verify(rows, times(1)).iterator();
        }
    }

    @Test
    void presentPrimitiveOptionalsBindTheirContentOnABlobColumn() {
        final OptionalChar character = OptionalChar.of('A');
        final OptionalByte bite = OptionalByte.of((byte) 2);
        final OptionalShort small = OptionalShort.of((short) 3);
        final OptionalInt number = OptionalInt.of(4);
        final OptionalFloat fraction = OptionalFloat.of(1.5f);
        final OptionalDouble wide = OptionalDouble.of(2.5d);

        try (CassandraTestFixture fixture = new CassandraTestFixture()) {
            fixture.columns(DataType.blob(), "v");

            fixture.executor.execute("UPDATE t SET v = ? WHERE id = 1", List.of(character));
            fixture.executor.execute("UPDATE t SET v = ? WHERE id = 1", List.of(bite));
            fixture.executor.execute("UPDATE t SET v = ? WHERE id = 1", List.of(small));
            fixture.executor.execute("UPDATE t SET v = ? WHERE id = 1", List.of(number));
            fixture.executor.execute("UPDATE t SET v = ? WHERE id = 1", List.of(fraction));
            fixture.executor.execute("UPDATE t SET v = ? WHERE id = 1", List.of(wide));
            fixture.executor.execute("UPDATE t SET v = ? WHERE id = 1", List.of(OptionalInt.empty()));

            assertEquals(7, fixture.values.size());
            assertSame(character, fixture.values.get(0)[0]);
            assertSame(bite, fixture.values.get(1)[0]);
            assertSame(small, fixture.values.get(2)[0]);
            assertSame(number, fixture.values.get(3)[0]);
            assertSame(fraction, fixture.values.get(4)[0]);
            assertSame(wide, fixture.values.get(5)[0]);
            assertNull(fixture.values.get(6)[0]);
        }
    }

    @Test
    void offsetTimeReadsADateAndATimestamp() {
        final long millis = 1_700_000_000_000L;
        final Date date = new Date(millis);
        final Timestamp timestamp = new Timestamp(millis + 5);

        final TimeBean fromDate = CassandraExecutor.toEntity(row(DataType.timestamp(), new String[] { "offsetTime" }, date), TimeBean.class);
        final TimeBean fromTimestamp = CassandraExecutor.toEntity(row(DataType.timestamp(), new String[] { "offsetTime" }, timestamp), TimeBean.class);

        assertEquals(OffsetTime.ofInstant(Instant.ofEpochMilli(date.getTime()), ZoneId.systemDefault()), fromDate.getOffsetTime());
        assertEquals(OffsetTime.ofInstant(timestamp.toInstant(), ZoneId.systemDefault()), fromTimestamp.getOffsetTime());
    }

    @Test
    void caseFoldedColumnFillsTheAnnotatedProperty() {
        final SharpFoldAlias entity = CassandraExecutor.toEntity(row(new String[] { "STRASSE" }, "Ada"), SharpFoldAlias.class);

        assertEquals("Ada", entity.getCode());
        assertNull(entity.getStraße());
    }

    private static Row row(final String[] names, final Object... values) {
        return row(null, names, values);
    }

    private static Row row(final DataType type, final String[] names, final Object... values) {
        final ColumnDefinitions columns = mock(ColumnDefinitions.class);
        when(columns.size()).thenReturn(names.length);

        for (int i = 0; i < names.length; i++) {
            when(columns.getName(i)).thenReturn(names[i]);
            when(columns.getType(i)).thenReturn(type);
        }

        final Row row = mock(Row.class);
        when(row.getColumnDefinitions()).thenReturn(columns);

        for (int i = 0; i < values.length; i++) {
            when(row.getObject(i)).thenReturn(values[i]);
        }

        return row;
    }

    private static UserType userType(final CodecRegistry registry, final String typeName, final Object... fieldNamesAndTypes) throws Exception {
        final Constructor<UserType.Field> fieldConstructor = UserType.Field.class.getDeclaredConstructor(String.class, DataType.class);
        fieldConstructor.setAccessible(true);
        final List<UserType.Field> fields = new ArrayList<>();

        for (int i = 0; i < fieldNamesAndTypes.length; i += 2) {
            fields.add(fieldConstructor.newInstance(fieldNamesAndTypes[i], fieldNamesAndTypes[i + 1]));
        }

        final Constructor<UserType> typeConstructor = UserType.class.getDeclaredConstructor(String.class, String.class, boolean.class, Collection.class,
                ProtocolVersion.class, CodecRegistry.class);
        typeConstructor.setAccessible(true);
        return typeConstructor.newInstance("line_ks", typeName, false, fields, ProtocolVersion.V4, registry);
    }

    private static Runnable enableDebug(final Class<?> type) {
        final Logger logger = LoggerFactory.getLogger(type);

        if (logger.isDebugEnabled()) {
            return () -> {
            };
        }

        final java.util.logging.Logger jul = java.util.logging.Logger.getLogger(type.getName());
        final Level previous = jul.getLevel();
        jul.setLevel(Level.ALL);

        if (!logger.isDebugEnabled()) {
            enableLog4jOrLogback(type.getName(), logger);
        }

        assertTrue(logger.isDebugEnabled(), "debug is not enabled for " + type.getName() + "; logger is " + logger.getClass().getName());
        return () -> jul.setLevel(previous);
    }

    private static void enableLog4jOrLogback(final String name, final Logger logger) {
        try {
            final Class<?> levelClass = Class.forName("org.apache.logging.log4j.Level");
            final Object debug = levelClass.getField("DEBUG").get(null);
            final Class<?> configurator = Class.forName("org.apache.logging.log4j.core.config.Configurator");
            configurator.getMethod("setLevel", String.class, levelClass).invoke(null, name, debug);
        } catch (final ClassNotFoundException ignored) {
            // Log4j 2 is not the backend.
        } catch (final ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }

        if (logger.isDebugEnabled()) {
            return;
        }

        try {
            final Class<?> classic = Class.forName("ch.qos.logback.classic.Logger");
            final Object slf4j = Class.forName("org.slf4j.LoggerFactory").getMethod("getLogger", String.class).invoke(null, name);

            if (classic.isInstance(slf4j)) {
                final Class<?> levelClass = Class.forName("ch.qos.logback.classic.Level");
                classic.getMethod("setLevel", levelClass).invoke(slf4j, levelClass.getField("DEBUG").get(null));
            }
        } catch (final ClassNotFoundException ignored) {
            // Logback is not the backend.
        } catch (final ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    @Table(keyspace = "line_ks", name = "line_items")
    public static class LineItem {
        private Long id;

        public Long getId() {
            return id;
        }

        public void setId(final Long id) {
            this.id = id;
        }
    }

    public static class ContainerBean {
        private List<String> labels;
        private Map<String, String> attributes;
        private Object payload;
        private List<String> notes;
        private List<Object> nested;

        public List<String> getLabels() {
            return labels;
        }

        public void setLabels(final List<String> labels) {
            this.labels = labels;
        }

        public Map<String, String> getAttributes() {
            return attributes;
        }

        public void setAttributes(final Map<String, String> attributes) {
            this.attributes = attributes;
        }

        public Object getPayload() {
            return payload;
        }

        public void setPayload(final Object payload) {
            this.payload = payload;
        }

        public List<String> getNotes() {
            return notes;
        }

        public void setNotes(final List<String> notes) {
            this.notes = notes;
        }

        public List<Object> getNested() {
            return nested;
        }

        public void setNested(final List<Object> nested) {
            this.nested = nested;
        }
    }

    public static class TimeBean {
        private OffsetTime offsetTime;
        private ZonedDateTime zonedDateTime;
        private OffsetDateTime offsetDateTime;

        public OffsetTime getOffsetTime() {
            return offsetTime;
        }

        public void setOffsetTime(final OffsetTime offsetTime) {
            this.offsetTime = offsetTime;
        }

        public ZonedDateTime getZonedDateTime() {
            return zonedDateTime;
        }

        public void setZonedDateTime(final ZonedDateTime zonedDateTime) {
            this.zonedDateTime = zonedDateTime;
        }

        public OffsetDateTime getOffsetDateTime() {
            return offsetDateTime;
        }

        public void setOffsetDateTime(final OffsetDateTime offsetDateTime) {
            this.offsetDateTime = offsetDateTime;
        }
    }

    /**
     * {@code straße} occupies the column key, and its JSON tags are a fixed name, so {@code STRASSE} is only in the
     * column-to-property map.
     */
    public static class SharpFoldAlias {
        @JsonXmlField(name = "fixed")
        private String straße;

        @Column("straße")
        private String code;

        public String getStraße() {
            return straße;
        }

        public void setStraße(final String straße) {
            this.straße = straße;
        }

        public String getCode() {
            return code;
        }

        public void setCode(final String code) {
            this.code = code;
        }
    }
}
