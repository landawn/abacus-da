package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.logging.Level;

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
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.landawn.abacus.da.MockitoTestBase;
import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.util.u.Optional;

/**
 * Driver 4 line coverage for {@link CassandraExecutor}: row-converter registration, blob and map binding,
 * time-of-day reads, container copies, a null prepared-variable name, and the prepare debug line.
 */
class CassandraExecutorLineCoverageTest extends MockitoTestBase {

    private static final LocalTime TIME_OF_DAY = LocalTime.of(1, 2, 3);
    private static final String PARAMETERLESS = "SELECT 1";

    @Test
    void registerRowConverterReturnsWhenTheDriverRowClassIsAbsent() throws Exception {
        final Method register = CassandraExecutor.class.getDeclaredMethod("registerRowConverter", String.class, BiFunction.class);
        register.setAccessible(true);
        final BiFunction<Row, Class<?>, Object> converter = (row, type) -> row;

        assertNull(register.invoke(null, "com.landawn.abacus.da.cassandra.NoSuchDriverRow", converter));
    }

    @Test
    void convertValueTurnsALocalTimeIntoSqlTimeWithItsMilliseconds() {
        final LocalTime localTime = LocalTime.of(1, 2, 3, 456_000_000);
        final java.sql.Time expected = java.sql.Time.valueOf(localTime);
        expected.setTime(expected.getTime() + 456);

        final java.sql.Time actual = CassandraExecutor.convertValue(localTime, java.sql.Time.class);

        assertEquals(expected.getTime(), actual.getTime());
        assertEquals(expected, actual);
    }

    @Test
    void bindKeepsAnIntArrayForABlobColumn() {
        final int[] values = { 1, 2 };

        try (SessionFixture fixture = new SessionFixture(DataTypes.BLOB)) {
            fixture.executor.execute("INSERT INTO line_rows (v) VALUES (?)", (Object) values);

            assertEquals(1, fixture.binds.size());
            assertSame(values, fixture.binds.get(0)[0]);
        }
    }

    @Test
    void bindKeepsTextThatIsNotAJsonObjectForAMapColumnWhenDebugIsOn() {
        final Runnable restore = enableDebug(CassandraExecutor.class);
        try (SessionFixture fixture = new SessionFixture(DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT))) {
            fixture.executor.execute("INSERT INTO line_rows (v) VALUES (?)", "abc");

            assertEquals(1, fixture.binds.size());
            assertSame("abc", fixture.binds.get(0)[0]);
        } finally {
            restore.run();
        }
    }

    @Test
    void queryForSingleNonNullReadsATimeColumnAsLocalTimeOffsetTimeAndOffsetDateTime() {
        final LocalDateTime onEpoch = LocalDate.EPOCH.atTime(TIME_OF_DAY);
        final OffsetTime offsetTime = TIME_OF_DAY.atOffset(ZoneId.systemDefault().getRules().getOffset(onEpoch));
        final OffsetDateTime offsetDateTime = onEpoch.atZone(ZoneId.systemDefault()).toOffsetDateTime();

        try (SessionFixture fixture = new SessionFixture(DataTypes.TIME)) {
            final Row row = mock(Row.class);
            when(row.getObject(0)).thenReturn(TIME_OF_DAY);
            final ResultSet resultSet = mock(ResultSet.class);
            when(resultSet.one()).thenReturn(row);
            when(fixture.session.execute(any(Statement.class))).thenReturn(resultSet);

            final Optional<LocalTime> local = fixture.executor.queryForSingleNonNull(LocalTime.class, "SELECT v FROM line_rows");
            final Optional<OffsetTime> offset = fixture.executor.queryForSingleNonNull(OffsetTime.class, "SELECT v FROM line_rows");
            final Optional<OffsetDateTime> dateTime = fixture.executor.queryForSingleNonNull(OffsetDateTime.class, "SELECT v FROM line_rows");

            assertSame(TIME_OF_DAY, local.get());
            assertEquals(offsetTime, offset.get());
            assertEquals(offsetDateTime, dateTime.get());
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
    void readRowOfANullRowReturnsTheTargetDefault() throws Exception {
        final Method readRow = CassandraExecutor.class.getDeclaredMethod("readRow", Class.class, Row.class);
        readRow.setAccessible(true);

        assertNull(readRow.invoke(null, String.class, null));
        assertEquals(0, readRow.invoke(null, int.class, null));
    }

    @Test
    void prepareStatementRejectsANullPreparedVariableNameFromAMapAndABean() {
        final CqlIdentifier missing = mock(CqlIdentifier.class);
        when(missing.asInternal()).thenReturn(null);

        try (SessionFixture fixture = new SessionFixture(new CqlIdentifier[] { missing, CqlIdentifier.fromInternal("right") }, DataTypes.TEXT, DataTypes.TEXT)) {
            final String query = "UPDATE line_rows SET left = ?, right = ?";
            final IllegalArgumentException fromMap = assertThrows(IllegalArgumentException.class,
                    () -> fixture.executor.execute(query, Map.of("left", "a", "right", "b")));
            final IllegalArgumentException fromBean = assertThrows(IllegalArgumentException.class,
                    () -> fixture.executor.execute(query, new NamedBindBean()));

            assertEquals("Parameter name at index 0 is null", fromMap.getMessage());
            assertEquals("Parameter name at index 0 is null", fromBean.getMessage());
            assertEquals(0, fixture.binds.size());
        }
    }

    @Test
    void prepareLogsTheCqlWhenDebugIsEnabled() {
        final Runnable restore = enableDebug(CassandraExecutor.class);
        try (SessionFixture fixture = new SessionFixture()) {
            fixture.executor.execute(PARAMETERLESS);

            verify(fixture.session).prepare(eq(PARAMETERLESS));
        } finally {
            restore.run();
        }
    }

    private static Row row(final String[] names, final Object... values) {
        final ColumnDefinitions columns = mock(ColumnDefinitions.class);
        when(columns.size()).thenReturn(names.length);

        for (int i = 0; i < names.length; i++) {
            final ColumnDefinition column = mock(ColumnDefinition.class);
            when(columns.get(i)).thenReturn(column);
            when(column.getName()).thenReturn(CqlIdentifier.fromInternal(names[i]));
        }

        final Row row = mock(Row.class);
        when(row.getColumnDefinitions()).thenReturn(columns);

        for (int i = 0; i < values.length; i++) {
            when(row.getObject(i)).thenReturn(values[i]);
        }

        return row;
    }

    /**
     * The executor's logger is chosen once at class initialization. Debug lines run only when that backend says
     * debug is on, so turn the matching backend up and fail if it still is not.
     */
    private static Runnable enableDebug(final Class<?> type) {
        final Logger logger = LoggerFactory.getLogger(type);

        if (logger.isDebugEnabled()) {
            return () -> {
            };
        }

        final java.util.logging.Logger jul = java.util.logging.Logger.getLogger(type.getName());
        final Level previous = jul.getLevel();
        jul.setLevel(Level.ALL);
        final Runnable restoreJul = () -> jul.setLevel(previous);

        if (!logger.isDebugEnabled()) {
            enableLog4jOrLogback(type.getName(), logger);
        }

        assertTrue(logger.isDebugEnabled(), "debug is not enabled for " + type.getName() + "; logger is " + logger.getClass().getName());
        return restoreJul;
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

    private static final class SessionFixture implements AutoCloseable {
        final CqlSession session = mock(CqlSession.class);
        final List<Object[]> binds = new ArrayList<>();
        final CassandraExecutor executor;

        SessionFixture(final DataType... variableTypes) {
            this(defaultNames(variableTypes.length), variableTypes);
        }

        SessionFixture(final CqlIdentifier[] names, final DataType... variableTypes) {
            final DriverContext context = mock(DriverContext.class);
            when(session.getContext()).thenReturn(context);
            when(context.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT);
            final PreparedStatement prepared = mock(PreparedStatement.class);
            final ColumnDefinitions variables = mock(ColumnDefinitions.class);
            when(variables.size()).thenReturn(variableTypes.length);

            for (int i = 0; i < variableTypes.length; i++) {
                final ColumnDefinition variable = mock(ColumnDefinition.class);
                when(variables.get(i)).thenReturn(variable);
                when(variable.getName()).thenReturn(names[i]);
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

        private static CqlIdentifier[] defaultNames(final int count) {
            final CqlIdentifier[] names = new CqlIdentifier[count];

            for (int i = 0; i < count; i++) {
                names[i] = CqlIdentifier.fromInternal("v" + i);
            }

            return names;
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

    public static class NamedBindBean {
        private String left;
        private String right;

        public String getLeft() {
            return left;
        }

        public void setLeft(final String left) {
            this.left = left;
        }

        public String getRight() {
            return right;
        }

        public void setRight(final String right) {
            this.right = right;
        }
    }
}
