package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
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
import com.landawn.abacus.annotation.Column;
import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.da.cassandra.CassandraExecutor.StatementSettings;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.N;

/**
 * Closes the 2026-11-01 JaCoCo gaps of the driver-v4 {@code CassandraExecutor}, {@code CassandraExecutorBase} and
 * {@code AsyncCassandraExecutorBase} that the neighboring fixtures had left. Everything runs against mocked driver-v4
 * objects, offline; conversion and statement generation are real.
 *
 * <h2>Unreachable gaps</h2>
 * The following reported gaps are not reachable through normal public-API input flow and were skipped:
 * <ul>
 * <li>CassandraExecutor 383-384 (registerRowConverter catch): the only registered internal class name,
 * {@code com.datastax.oss.driver.internal.core.cql.DefaultRow}, is always present with the driver-v4 artifacts on the
 * classpath; the guard exists for other driver versions.</li>
 * <li>CassandraExecutor 387 (false outcome of {@code Row.class.isAssignableFrom(rowClass)}): the single registered name
 * always resolves to a Row implementation.</li>
 * <li>CassandraExecutorBase 432-433 / 462-463: the {@code putIfAbsent} result of {@code getKeyNames}/{@code getKeyNameSet}
 * is only non-null when another thread registers the same class between this thread's {@code get} and its
 * {@code putIfAbsent}; a single-threaded call always wins its own put.</li>
 * <li>CassandraExecutor 1255-1256: no caller can hand {@code convertValue} a {@code LocalTime} with a
 * {@code java.sql.Time} target: {@code decodeValue} routes that pair through {@code isTimeOfDayTarget} first, the bind
 * path's TIME columns map to {@code LocalTime}, and the UDT path targets {@code LocalTime}/{@code Instant} only.</li>
 * <li>CassandraExecutor 1499 (one outcome: a converted buffer positioned past byte 0 with bytes remaining): only an
 * application converter can produce such a buffer; the shared fixtures (BlobFixtures) return flipped or empty buffers, and
 * a new converter class declared in this file would live under {@code com.landawn.abacus.*}, where
 * {@code isBuiltinClass} short-circuits before any converter ran.</li>
 * <li>CassandraExecutor 1560 (namedParameters == null): the only caller of {@code toBindValue} passes
 * {@code parseCql.namedParameters()}, which is never null.</li>
 * <li>CassandraExecutor 1564 (variables.get(index) == null): a null variable NPEs at
 * {@code columnDefinitions.get(i).getType()} in the caller's loop before any conversion could fail.</li>
 * <li>CassandraExecutor 1906-1907: every {@code readRow} call site guarantees a non-null row (guards at 809, 1038, 1922,
 * 1936, 2003, 2021, 2274/3375 and the array path).</li>
 * <li>CassandraExecutor 1988, 1993, 1994 (null-rowClass arms): every public entry rejects a null target class before the
 * row-mapper factory runs.</li>
 * <li>CassandraExecutor 2969-2970, 2989-2990 (null parameter name): named parameters come from parsed markers, which are
 * never null; the defensive throws cannot fire.</li>
 * <li>CassandraExecutor 2943/3036 ({@code javaClass == null}): every driver protocol code is mapped in
 * {@code protocolCodeDataType} (lines 394-423), so a real column type always yields a class.</li>
 * <li>CassandraExecutor 3168/3183 (rethrow arms) and 3172/3187 (closed-concurrently arms) of
 * {@code getOrPrepareStatement}: the pool only throws {@code IllegalStateException} when closed, and a closed pool is
 * skipped outright at 3160/3179; the catch arms need the pool to close concurrently between the two checks.</li>
 * <li>CassandraExecutor 3795, 3856, 3937, 4005, 4141, 4218 (switch defaults) and the matching default arms of
 * 3781/3845/3923/3994/4118/4201: the constructors only accept SNAKE_CASE, SCREAMING_SNAKE_CASE and CAMEL_CASE, so the
 * default cases are dead.</li>
 * <li>CassandraExecutor 3800-3803 (the IllegalStateException catch of prepareInsertThroughBuilder): every input that
 * stages no column is rejected eagerly by {@code CqlBuilder.insert} itself (an empty map, a non-bean) with an
 * IllegalArgumentException, and anything else stages at least one column, so {@code into} never throws.</li>
 * <li>CassandraExecutor 3608 (the beanInfo-refresh outcome): {@code fieldPropInfos} re-resolves only if ParserUtil hands
 * out a second BeanInfo instance for the same class; no public API forces that within a JVM.</li>
 * <li>CassandraExecutor 1416 (a named module outside the boot layer): classpath classes live in the unnamed module or in
 * boot-layer system modules; no application module exists on the test classpath.</li>
 * <li>CassandraExecutor 1422 (a javax.* package in an unnamed module): the only javax classes reachable here are in
 * java.* modules, caught earlier at 1416-1417.</li>
 * </ul>
 */
class CassandraExecutorCoverageNov2026Test extends CassandraExecutorBaseTestSupport {

    private static final String UPDATE_ONE_V = "UPDATE t SET v = ? WHERE id = 1";
    private static final String INSERT_ID_NAME = "INSERT INTO t (id, name) VALUES (?, ?)";
    private static final String BYTES_BASE64 = "AQID";
    private static final byte[] BYTES = { 1, 2, 3 };

    // ------------------------------------------------------------------
    // CassandraExecutor.registerRowConverter tail (pins; the guard arms themselves are unreachable)
    // ------------------------------------------------------------------

    @Test
    void rowConverterRegistrationStillAppliesToDriverRows() {
        final Row row = realDriverRow(new String[] { "name" }, "Ada");

        assertArrayEquals(new Object[] { "Ada" }, (Object[]) N.convert(row, Object[].class));
    }

    // ------------------------------------------------------------------
    // CassandraExecutor.extractData / toEntity property-name mapping (787, 1019-1021)
    // ------------------------------------------------------------------

    @Test
    void snakeCaseColumnNamesResolveThroughTheColumnToPropertyNameMap() {
        // @Column("uname") renames the column beyond what the property-info lookup can fuzzy-match, so both readers must
        // fall back to the column-to-property-name map (787, 1019-1021); "ghost_col" matches no property at all.
        final ResultSet resultSet = rows(new String[] { "uname", "ghost_col" }, DataTypes.TEXT, new Object[][] { { "Ada", "x" } });
        final Dataset dataset = CassandraExecutor.extractData(resultSet, UnamePerson.class);

        assertEquals("Ada", dataset.getColumn("uname").get(0));

        final Row row = driverRow(new String[] { "uname", "ghost_col" }, DataTypes.TEXT, "Grace", "y");
        final UnamePerson person = CassandraExecutor.toEntity(row, UnamePerson.class);

        assertEquals("Grace", person.getUserName());
    }

    /** 809, 812, 814, 817: the extractData row loop's nested-row, null, wrapper-blob and decoded-blob arms. */
    @Test
    void extractDataRowLoopConvertsNestedRowsNullsAndBlobs() {
        final Row nested = driverRow(new String[] { "name" }, DataTypes.TEXT, "Ada");
        final ResultSet resultSet = rows(new String[] { "inner", "blobOpt", "blobBytes", "extra" },
                new DataType[] { DataTypes.TEXT, DataTypes.BLOB, DataTypes.BLOB, DataTypes.TEXT },
                new Object[][] { { nested, ByteBuffer.wrap(BYTES), ByteBuffer.wrap(BYTES), null } });
        final Dataset dataset = CassandraExecutor.extractData(resultSet, NovDatasetBean.class);

        assertAll(() -> assertEquals("Ada", ((NestedPerson) dataset.getColumn("inner").get(0)).getName()),
                () -> assertEquals(Optional.of(BYTES_BASE64), dataset.getColumn("blobOpt").get(0)),
                () -> assertArrayEquals(BYTES, (byte[]) dataset.getColumn("blobBytes").get(0)),
                () -> assertNull(dataset.getColumn("extra").get(0)));

        // A Map target keeps nested rows as maps (the isMap arm of 812).
        final ResultSet mapRows = rows(new String[] { "nested", "plain" }, DataTypes.TEXT, new Object[][] { { nested, "x" } });
        final Dataset mapDataset = CassandraExecutor.extractData(mapRows, Map.class);

        assertEquals(Map.of("name", "Ada"), mapDataset.getColumn("nested").get(0));
    }

    /** 1353: the declared wrapper type of a generic-superclass property drives the blob element conversion. */
    @Test
    void wrapperPropTypeOfAGenericSuperclassResolvesTheDeclaredElementType() {
        final ResultSet resultSet = rows("v", DataTypes.BLOB, new Object[] { ByteBuffer.wrap(BYTES) });
        final Dataset dataset = CassandraExecutor.extractData(resultSet, NovDerivedBean.class);

        assertEquals(Optional.of(BYTES_BASE64), dataset.getColumn("v").get(0));
    }

    /** 1069: a dotted column whose leaf property is read-only keeps the value without failing the row. */
    @Test
    void readOnlyNestedLeafColumnKeepsItsValueWithoutFailingTheRow() {
        final Row row = driverRow(new String[] { "inner.name", "inner.computed" }, DataTypes.TEXT, "Ada", "computed-value");
        final NovHolder holder = CassandraExecutor.toEntity(row, NovHolder.class);

        assertAll(() -> assertEquals("Ada", holder.getInner().getName()), () -> assertEquals("computed-value", holder.getInner().getComputed()));
    }

    // ------------------------------------------------------------------
    // CassandraExecutor.convertValue chain (1214, 1278, 1283, 1290)
    // ------------------------------------------------------------------

    /** The false arms of the convertValue chain via NULL single-column reads; the true sides are pinned alongside. */
    @Test
    void nullSingleColumnReadsFallThroughConvertValueToNullDefaults() {
        assertAll(() -> assertEquals(Arrays.asList((byte[]) null), CassandraExecutor.toList(rows("v", DataTypes.INT, new Object[] { null }), byte[].class)),
                () -> assertEquals(Arrays.asList((LocalDateTime) null),
                        CassandraExecutor.toList(rows("v", DataTypes.INT, new Object[] { null }), LocalDateTime.class)),
                () -> assertEquals(Arrays.asList((ZonedDateTime) null),
                        CassandraExecutor.toList(rows("v", DataTypes.INT, new Object[] { null }), ZonedDateTime.class)),
                () -> assertEquals(Arrays.asList((OffsetDateTime) null),
                        CassandraExecutor.toList(rows("v", DataTypes.INT, new Object[] { null }), OffsetDateTime.class)),
                () -> assertArrayEquals(BYTES, CassandraExecutor.toList(rows("v", DataTypes.BLOB, new Object[] { ByteBuffer.wrap(BYTES) }), byte[].class).get(0)),
                () -> assertEquals(LocalDateTime.ofInstant(Instant.EPOCH, java.time.ZoneId.systemDefault()),
                        CassandraExecutor.toList(rows("v", DataTypes.TIMESTAMP, new Object[] { Instant.EPOCH }), LocalDateTime.class).get(0)),
                () -> assertEquals(Instant.EPOCH.atZone(java.time.ZoneId.systemDefault()),
                        CassandraExecutor.toList(rows("v", DataTypes.TIMESTAMP, new Object[] { Instant.EPOCH }), ZonedDateTime.class).get(0)),
                () -> assertEquals(Instant.EPOCH.atOffset(java.time.ZoneId.systemDefault().getRules().getOffset(Instant.EPOCH)),
                        CassandraExecutor.toList(rows("v", DataTypes.TIMESTAMP, new Object[] { Instant.EPOCH }), OffsetDateTime.class).get(0)));
    }

    // ------------------------------------------------------------------
    // CassandraExecutor.wrappedValue / toParameterValue wrappers (1325)
    // ------------------------------------------------------------------

    @Test
    void abacusOptionalIntWrappersBindTheirContentOrNull() {
        try (Fixture fixture = new Fixture(new String[] { "v" }, DataTypes.INT)) {
            fixture.executor.execute(UPDATE_ONE_V, com.landawn.abacus.util.u.OptionalInt.of(3));
            fixture.executor.execute(UPDATE_ONE_V, com.landawn.abacus.util.u.OptionalInt.empty());

            assertAll(() -> assertEquals(3, fixture.binds.get(0)[0]), () -> assertNull(fixture.binds.get(1)[0]));
        }
    }

    // ------------------------------------------------------------------
    // CassandraExecutor.toBindValue classification (1397, 1406-1422, 1491-1493)
    // ------------------------------------------------------------------

    /** Which values keep their form for a blob column: text decodes, JDK/abacus built-ins bind unchanged. */
    @Test
    void blobColumnBindValueClassification() {
        final Runnable restoreDebug = enableDebug(CassandraExecutor.class);

        try (Fixture fixture = new Fixture(new String[] { "v", "id" }, DataTypes.BLOB, DataTypes.INT);
                Fixture intFixture = new Fixture(new String[] { "v" }, DataTypes.INT)) {
            final StringBuilder builder = new StringBuilder(BYTES_BASE64);
            final java.nio.CharBuffer charBuffer = java.nio.CharBuffer.wrap(BYTES_BASE64);
            final Integer number = 1234;
            final long[] longs = { 1L, 2L };
            final String[] strings = { "a", "b" };
            final com.landawn.abacus.util.MutableInt mutableInt = com.landawn.abacus.util.MutableInt.of(7);
            final jdk.jfr.Recording recording = new jdk.jfr.Recording();

            fixture.executor.execute("UPDATE t SET v = ?, id = ?", BYTES_BASE64, 1);
            fixture.executor.execute("UPDATE t SET v = ?, id = ?", builder, 1);
            fixture.executor.execute("UPDATE t SET v = ?, id = ?", charBuffer, 1);
            fixture.executor.execute("UPDATE t SET v = ?, id = ?", number, 1);
            fixture.executor.execute("UPDATE t SET v = ?, id = ?", longs, 1);
            fixture.executor.execute("UPDATE t SET v = ?, id = ?", strings, 1);
            fixture.executor.execute("UPDATE t SET v = ?, id = ?", mutableInt, 1);
            fixture.executor.execute("UPDATE t SET v = ?, id = ?", recording, 1);
            // 1491-1493: a failed conversion is logged at debug level and the original value is kept.
            intFixture.executor.execute(UPDATE_ONE_V, "abc");

            assertAll(() -> assertEquals(BYTES_BASE64, boundBufferText(fixture.binds.get(0)[0])),
                    () -> assertEquals(BYTES_BASE64, boundBufferText(fixture.binds.get(1)[0])),
                    () -> assertSame(charBuffer, fixture.binds.get(2)[0]), () -> assertSame(number, fixture.binds.get(3)[0]),
                    () -> assertSame(longs, fixture.binds.get(4)[0]), () -> assertSame(strings, fixture.binds.get(5)[0]),
                    () -> assertSame(mutableInt, fixture.binds.get(6)[0]), () -> assertSame(recording, fixture.binds.get(7)[0]),
                    () -> assertEquals("abc", intFixture.binds.get(0)[0]));
        } finally {
            restoreDebug.run();
        }
    }

    private static String boundBufferText(final Object bound) {
        final ByteBuffer buffer = assertInstanceOf(ByteBuffer.class, bound);
        final byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);

        return java.util.Base64.getEncoder().encodeToString(bytes);
    }

    /** 1499: a conversion with no blob content left binds the original value; a real buffer binds converted. */
    @Test
    void emptyConvertedBlobBuffersBindTheOriginalValue() {
        example.cassandra.BlobFixtures.register();

        try (Fixture fixture = new Fixture(new String[] { "v", "id" }, DataTypes.BLOB, DataTypes.INT)) {
            final com.datastax.oss.driver.api.core.data.CqlDuration duration = com.datastax.oss.driver.api.core.data.CqlDuration.from("1h");

            fixture.executor.execute("UPDATE t SET v = ?, id = ?", duration, 1);
            fixture.executor.execute("UPDATE t SET v = ?, id = ?", new example.cassandra.BlobFixtures.EmptyBox(), 1);
            fixture.executor.execute("UPDATE t SET v = ?, id = ?", ByteBuffer.wrap(BYTES), 1);

            assertAll(() -> assertSame(duration, fixture.binds.get(0)[0]),
                    () -> assertInstanceOf(ByteBuffer.class, fixture.binds.get(1)[0]),
                    () -> assertEquals(ByteBuffer.wrap(BYTES), fixture.binds.get(2)[0]));
        }
    }

    /** 1545: the BigDecimal lossy-number rule, including its zero and whole-number arms. */
    @Test
    void bigDecimalLossyNumberRule() {
        try (Fixture fixture = new Fixture(new String[] { "v", "id" }, DataTypes.INT, DataTypes.INT)) {
            fixture.executor.execute("UPDATE t SET v = ?, id = ?", new BigDecimal("1.00"), 1);
            fixture.executor.execute("UPDATE t SET v = ?, id = ?", new BigDecimal("0.00"), 1);

            final IllegalArgumentException lossy = assertThrows(IllegalArgumentException.class,
                    () -> fixture.executor.execute("UPDATE t SET v = ?, id = ?", new BigDecimal("1.5"), 1));

            assertTrue(lossy.getMessage().contains("without changing it"), lossy.getMessage());
            assertAll(() -> assertEquals(1, fixture.binds.get(0)[0]), () -> assertEquals(0, fixture.binds.get(1)[0]),
                    () -> assertEquals(2, fixture.binds.size()));
        }
    }

    /** 1565, 1568: a variable without a usable name is reported as "parameter at index i". */
    @Test
    void unnamedVariableIsReportedAsParameterAtIndex() {
        try (Fixture fixture = new UnnamedVariableFixture()) {
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> fixture.executor.execute(UPDATE_ONE_V, 1.5d));

            assertTrue(e.getMessage().contains("parameter at index 0"), e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // CassandraExecutor.decodeValue / isTimeOfDayTarget / fromTimeOfDay (1587, 1620-1664)
    // ------------------------------------------------------------------

    @Test
    void timeColumnReadsIntoEverySupportedTarget() {
        final LocalTime time = LocalTime.of(1, 2, 3);
        final LocalDateTime onEpoch = LocalDate.EPOCH.atTime(time);
        final ZonedDateTime zoned = onEpoch.atZone(java.time.ZoneId.systemDefault());
        final Object[] cell = new Object[] { time };

        assertAll(() -> assertEquals(time, CassandraExecutor.toList(rows("v", DataTypes.TIME, cell), LocalTime.class).get(0)),
                () -> assertEquals(Time.valueOf(time), CassandraExecutor.toList(rows("v", DataTypes.TIME, cell), Time.class).get(0)),
                () -> assertEquals(onEpoch, CassandraExecutor.toList(rows("v", DataTypes.TIME, cell), LocalDateTime.class).get(0)),
                () -> assertEquals(time.atOffset(java.time.ZoneId.systemDefault().getRules().getOffset(onEpoch)),
                        CassandraExecutor.toList(rows("v", DataTypes.TIME, cell), java.time.OffsetTime.class).get(0)),
                () -> assertEquals(zoned, CassandraExecutor.toList(rows("v", DataTypes.TIME, cell), ZonedDateTime.class).get(0)),
                () -> assertEquals(zoned.toOffsetDateTime(), CassandraExecutor.toList(rows("v", DataTypes.TIME, cell), OffsetDateTime.class).get(0)),
                () -> assertEquals(zoned.toInstant(), CassandraExecutor.toList(rows("v", DataTypes.TIME, cell), Instant.class).get(0)),
                () -> assertEquals(zoned.toInstant(), ((Date) CassandraExecutor.toList(rows("v", DataTypes.TIME, cell), Date.class).get(0)).toInstant()),
                () -> assertEquals(zoned.toInstant(), ((Calendar) CassandraExecutor.toList(rows("v", DataTypes.TIME, cell), Calendar.class).get(0)).toInstant()),
                () -> assertEquals("01:02:03", CassandraExecutor.toList(rows("v", DataTypes.TIME, cell), String.class).get(0)));
    }

    // ------------------------------------------------------------------
    // CassandraExecutor.toPropValue routing (1690-1697)
    // ------------------------------------------------------------------

    @Test
    void entityPropertyValuesRouteThroughDecodeValueWhereTheSetterWouldFail() {
        final LocalTime time = LocalTime.of(1, 2, 3);
        final LocalDate date = LocalDate.of(2020, 1, 2);
        final Instant instant = Instant.parse("2021-06-07T08:09:10Z");
        final Row row = driverRow(
                new String[] { "timeAsTime", "timeAsLocal", "timeAsText", "blobAsBytes", "blobAsBuffer", "dateAsInstant", "stampAsDate" },
                new DataType[] { DataTypes.TIME, DataTypes.TIME, DataTypes.TIME, DataTypes.BLOB, DataTypes.BLOB, DataTypes.DATE, DataTypes.TIMESTAMP },
                time, time, time, ByteBuffer.wrap(BYTES), ByteBuffer.wrap(BYTES), date, instant);

        final TemporalRoutingBean bean = CassandraExecutor.toEntity(row, TemporalRoutingBean.class);

        assertAll(() -> assertEquals(Time.valueOf(time), bean.getTimeAsTime()), () -> assertEquals(time, bean.getTimeAsLocal()),
                () -> assertEquals("01:02:03", bean.getTimeAsText()), () -> assertArrayEquals(BYTES, bean.getBlobAsBytes()),
                () -> assertEquals(ByteBuffer.wrap(BYTES), bean.getBlobAsBuffer()),
                () -> assertEquals(date.atStartOfDay(java.time.ZoneId.systemDefault()).toInstant(), bean.getDateAsInstant()),
                () -> assertEquals(instant.atZone(java.time.ZoneId.systemDefault()).toLocalDate(), bean.getStampAsDate()));
    }

    // ------------------------------------------------------------------
    // CassandraExecutor.toContainerValue / toElementValue (1744-1859)
    // ------------------------------------------------------------------

    @Test
    void containerColumnsConvertElementsToTheDeclaredTypes() {
        final Map<String, Object> mapCell = new LinkedHashMap<>();
        mapCell.put("a", 1L);
        mapCell.put("b", 2);

        final com.datastax.oss.driver.api.core.data.UdtValue udtCell = Mockito.mock(com.datastax.oss.driver.api.core.data.UdtValue.class);
        final Row row = driverRow(
                new String[] { "longs", "ints", "raw", "rawBlob", "hosts", "counts", "raw2", "maps", "rawUdts" },
                new DataType[] { DataTypes.listOf(DataTypes.BIGINT), DataTypes.listOf(DataTypes.INT), DataTypes.listOf(DataTypes.INT), DataTypes.BLOB,
                        DataTypes.setOf(DataTypes.INT), DataTypes.mapOf(DataTypes.TEXT, DataTypes.BIGINT),
                        DataTypes.mapOf(DataTypes.TEXT, DataTypes.TEXT), DataTypes.listOf(DataTypes.mapOf(DataTypes.TEXT, DataTypes.BIGINT)),
                        DataTypes.listOf(DataTypes.TEXT) },
                Arrays.asList(1L, 2), Arrays.asList(1, null), Arrays.asList(1, 2), ByteBuffer.wrap(BYTES), new LinkedHashSet<>(Arrays.asList(1, 2)),
                mapCell, new LinkedHashMap<>(Map.of("a", "x")), Arrays.asList(new LinkedHashMap<>(Map.of("a", 1L))), Arrays.asList(udtCell));

        final ContainerBean bean = CassandraExecutor.toEntity(row, ContainerBean.class);

        assertAll(
                // The first element fits, the second changes: the elements before it are copied (1764-1765).
                () -> assertEquals(Arrays.asList(1L, 2L), bean.longs),
                // A null element is kept (1835-1836) and nothing is copied when nothing changes.
                () -> assertEquals(Arrays.asList(1, null), bean.ints),
                // A raw collection declares no element type: the driver's list is kept (1747-1748).
                () -> assertEquals(Arrays.asList(1, 2), bean.raw),
                // A blob for a raw collection becomes that collection of the buffer (1851-1861).
                () -> assertEquals(Arrays.asList(ByteBuffer.wrap(BYTES)), bean.rawBlob),
                // A set column converts its elements and delivers a LinkedHashSet (1778).
                () -> assertEquals(new LinkedHashSet<>(Arrays.asList("1", "2")), bean.hosts),
                // A map whose second entry changes copies the first entry (1798-1808), both converted.
                () -> assertEquals(1L, bean.counts.get("a")), () -> assertEquals(2L, bean.counts.get("b")),
                // A map where nothing changes is kept as is (1813).
                () -> assertEquals(Map.of("a", "x"), bean.raw2),
                // A nested map element for a SortedMap element type converts to the container class (1841-1844).
                () -> assertEquals(new TreeMap<>(Map.of("a", 1L)), bean.maps.get(0)), () -> assertInstanceOf(TreeMap.class, bean.maps.get(0)),
                // A UDT element is kept as the driver decoded it (1847).
                () -> assertSame(udtCell, bean.rawUdts.get(0)));
    }

    /** 1936-1937: a nested Row cell inside a row read as a collection flattens to a list. */
    @Test
    void nestedRowInsideACollectionRowBecomesAList() {
        final Row nested = driverRow(new String[] { "name" }, DataTypes.TEXT, "Ada");
        final ResultSet resultSet = rows(new String[] { "nested", "plain" }, DataTypes.TEXT, new Object[][] { { nested, "x" } });

        // One entry per row; the row itself is the collection of its cells.
        assertEquals(List.of(Arrays.asList(Arrays.asList("Ada"), "x")), CassandraExecutor.toList(resultSet, List.class));
    }

    // ------------------------------------------------------------------
    // CassandraExecutor public read APIs (2276, 3331)
    // ------------------------------------------------------------------

    @Test
    void findFirstWithoutRowsReturnsEmpty() {
        try (Fixture fixture = new Fixture(new String[] { "v" }, DataTypes.TEXT)) {
            Mockito.when(fixture.session.execute(Mockito.any(Statement.class))).thenReturn(Mockito.mock(ResultSet.class));

            assertEquals(com.landawn.abacus.util.u.Optional.empty(), fixture.executor.findFirst(String.class, "SELECT v FROM t WHERE id = 1"));
        }
    }

    @Test
    void streamRowMapperCachesTheColumnDefinitions() {
        try (Fixture fixture = new Fixture(new String[] { "v" }, DataTypes.TEXT)) {
            Mockito.when(fixture.session.execute(Mockito.any(Statement.class)))
                    .thenAnswer(invocation -> rows(new String[] { "v" }, DataTypes.TEXT, new Object[][] { { "Ada" }, { "Grace" } }));
            final AtomicInteger applied = new AtomicInteger();

            try (com.landawn.abacus.util.stream.Stream<String> stream = fixture.executor.stream("SELECT v FROM t", (cds, row) -> {
                applied.incrementAndGet();
                return cds.size() + "/" + row.getObject(0);
            })) {
                assertEquals(List.of("1/Ada", "1/Grace"), stream.toList());
            }

            assertEquals(2, applied.get());
        }
    }

    // ------------------------------------------------------------------
    // CassandraExecutor.close (2610) and batch builders (2652, 2692, 2734, 2778)
    // ------------------------------------------------------------------

    @Test
    void closeSkipsAnAlreadyClosedSession() {
        final CqlSession session = Mockito.mock(CqlSession.class);
        Mockito.when(session.isClosed()).thenReturn(true);
        Mockito.when(session.getContext()).thenReturn(Mockito.mock(com.datastax.oss.driver.api.core.context.DriverContext.class));
        final CassandraExecutor executor = new CassandraExecutor(session);

        assertDoesNotThrow(executor::close);
        Mockito.verify(session, Mockito.never()).close();
    }

    private static void assertDoesNotThrow(final Runnable action) {
        action.run();
    }

    @Test
    void batchBuildersAcceptANullBatchType() {
        final TestEntity entity = new TestEntity();
        entity.setId(1L);
        entity.setName("Ada");

        try (Fixture insertFixture = new Fixture(new String[] { "id", "name" }, DataTypes.INT, DataTypes.TEXT);
                Fixture updateFixture = new Fixture(new String[] { "name", "id" }, DataTypes.TEXT, DataTypes.INT)) {
            final ResultSet first = insertFixture.executor.batchInsert(Arrays.asList(entity), null);
            final ResultSet second = insertFixture.executor.batchInsert(TestEntity.class, Arrays.asList(N.asMap("id", 1, "name", "Grace")), null);
            final ResultSet third = updateFixture.executor.batchUpdate(Arrays.asList(entity), Arrays.asList("name"), null);
            final ResultSet fourth = updateFixture.executor.batchUpdate(TestEntity.class, Arrays.asList(N.asMap("id", 1, "name", "Grace")), null);

            assertAll(() -> assertNotNull(first), () -> assertNotNull(second), () -> assertNotNull(third), () -> assertNotNull(fourth),
                    () -> assertEquals(2, insertFixture.binds.size()), () -> assertEquals(2, updateFixture.binds.size()),
                    () -> assertArrayEquals(new Object[] { 1, "Ada" }, insertFixture.binds.get(0)),
                    () -> assertArrayEquals(new Object[] { "Ada", 1 }, updateFixture.binds.get(0)));
        }
    }

    // ------------------------------------------------------------------
    // CassandraExecutor.prepareStatement parameter handling (2923-3041)
    // ------------------------------------------------------------------

    /** A single empty container supplies zero parameters for a parameterless statement; a value is rejected. */
    @Test
    void parameterlessStatementsAcceptASingleEmptyContainerOnly() {
        try (ZeroVariableFixture fixture = new ZeroVariableFixture()) {
            final String insert = "INSERT INTO t (v) VALUES (1)";

            fixture.executor.execute(insert, Map.of());
            fixture.executor.execute(insert, List.of());

            final IllegalArgumentException tooMany = assertThrows(IllegalArgumentException.class, () -> fixture.executor.execute(insert, "x"));

            assertTrue(tooMany.getMessage().contains("Too many parameters"), tooMany.getMessage());
            assertEquals(2, fixture.noArgBinds.get());
        }
    }

    /** 2943-2951: a single null parameter, and a single-element list whose element fits, bind through. */
    @Test
    void singleParameterShortcutsBindNullAndSingleElementLists() {
        try (Fixture fixture = new Fixture(new String[] { "v" }, DataTypes.INT)) {
            fixture.executor.execute(UPDATE_ONE_V, (Object) null);
            fixture.executor.execute(UPDATE_ONE_V, List.of(5));
            fixture.executor.execute(UPDATE_ONE_V, Arrays.asList((Object) null));

            assertAll(() -> assertNull(fixture.binds.get(0)[0]), () -> assertEquals(5, fixture.binds.get(1)[0]),
                    () -> assertNull(fixture.binds.get(2)[0]));
        }
    }

    /** 2957 (false arm), 3011-3016, 3026, 3019-3021: single Object[]/Collection parameters expand; a scalar is rejected. */
    @Test
    void singleContainerParametersExpandToPositionalValues() {
        try (Fixture fixture = new Fixture(new String[] { "id", "name" }, DataTypes.INT, DataTypes.TEXT)) {
            fixture.executor.execute(INSERT_ID_NAME, (Object) new Object[] { 1, "Ada" });
            fixture.executor.execute(INSERT_ID_NAME, (Object) Arrays.asList(2, "Grace"));

            final IllegalArgumentException notEnough = assertThrows(IllegalArgumentException.class,
                    () -> fixture.executor.execute(INSERT_ID_NAME, (Object) "only"));

            assertTrue(notEnough.getMessage().contains("Not enough parameters"), notEnough.getMessage());
            assertAll(() -> assertArrayEquals(new Object[] { 1, "Ada" }, fixture.binds.get(0)),
                    () -> assertArrayEquals(new Object[] { 2, "Grace" }, fixture.binds.get(1)));
        }
    }

    /** 2975-2977: a named-parameter map missing a key is rejected; 2996-3001: bean parameters resolve @Column names. */
    @Test
    void mapAndBeanParametersValidateAndResolveTheirNames() {
        try (Fixture namedFixture = new Fixture(new String[] { "v", "id" }, DataTypes.INT, DataTypes.INT);
                Fixture beanFixture = new Fixture(new String[] { "uname", "id" }, DataTypes.TEXT, DataTypes.INT)) {
            final IllegalArgumentException missing = assertThrows(IllegalArgumentException.class,
                    () -> namedFixture.executor.execute("UPDATE t SET v = :v WHERE id = :id", N.asMap("v", 1)));

            assertTrue(missing.getMessage().contains("Missing required parameter: 'id'"), missing.getMessage());

            final UnamePerson person = new UnamePerson();
            person.setUserName("Ada");
            person.setId(1);
            beanFixture.executor.execute("INSERT INTO t (uname, id) VALUES (?, ?)", person);

            assertArrayEquals(new Object[] { "Ada", 1 }, beanFixture.binds.get(0));
        }
    }

    /** 3034-3041: a null in the multi-parameter loop stays null; a registered value codec accepts without conversion. */
    @Test
    void multiParameterLoopKeepsNullsAndAcceptsRegisteredCodecs() {
        try (Fixture fixture = new Fixture(new String[] { "id", "name" }, DataTypes.INT, DataTypes.TEXT);
                Fixture codecFixture = new ZonedDateTimeCodecFixture(new String[] { "v", "id" }, DataTypes.TIMESTAMP, DataTypes.INT)) {
            fixture.executor.execute(INSERT_ID_NAME, 1, null);

            final ZonedDateTime zdt = ZonedDateTime.parse("2021-06-07T08:09:10+02:00");
            codecFixture.executor.execute("UPDATE t SET v = ?, id = ?", zdt, 1);

            assertAll(() -> assertNull(fixture.binds.get(0)[1]), () -> assertEquals(1, fixture.binds.get(0)[0]),
                    () -> assertSame(zdt, codecFixture.binds.get(0)[0]), () -> assertEquals(1, codecFixture.binds.get(0)[1]));
        }
    }

    /** 3128-3141: without a value-specific codec, a ZonedDateTime converts to its instant for a timestamp column. */
    @Test
    void valuesWithoutARegisteredCodecConvertForTheColumn() {
        final ZonedDateTime zdt = ZonedDateTime.parse("2021-06-07T08:09:10+02:00");

        try (Fixture plain = new Fixture(new String[] { "v" }, DataTypes.TIMESTAMP)) {
            plain.executor.execute(UPDATE_ONE_V, zdt);

            assertEquals(zdt.toInstant(), plain.binds.get(0)[0]);
        }
    }

    // ------------------------------------------------------------------
    // CassandraExecutor.getOrPrepareStatement (3157-3189) / configStatement (3239-3262)
    // ------------------------------------------------------------------

    @Test
    void longQueriesBypassTheStatementPool() {
        final String longQuery = "SELECT v FROM t WHERE id = ? AND note = '" + "x".repeat(1100) + "'";

        try (Fixture fixture = new Fixture(new String[] { "v" }, DataTypes.TEXT)) {
            fixture.executor.execute(longQuery, "a");

            assertTrue(longQuery.length() > 1024);
            Mockito.verify(fixture.session).prepare(longQuery);
        }
    }

    @Test
    void executesAfterClosePrepareWithoutThePool() {
        final String query = "SELECT v FROM t WHERE id = ?";

        try (Fixture fixture = new Fixture(new String[] { "v" }, DataTypes.TEXT)) {
            fixture.executor.execute(query, "a");
            fixture.executor.close();
            fixture.executor.execute(query, "b");

            Mockito.verify(fixture.session, Mockito.times(2)).prepare(query);
        }
    }

    @Test
    void statementSettingsAreAppliedToBoundStatements() {
        final StatementSettings traceOnly = StatementSettings.builder().traceQuery(Boolean.TRUE).build();
        final StatementSettings all = StatementSettings.builder().consistency(ConsistencyLevel.LOCAL_ONE)
                .serialConsistency(ConsistencyLevel.LOCAL_SERIAL).fetchSize(50).timeout(java.time.Duration.ofSeconds(2))
                .traceQuery(Boolean.TRUE).build();

        try (Fixture traceFixture = new SettingsFixture(traceOnly, new String[] { "v" }, DataTypes.TEXT);
                Fixture allFixture = new SettingsFixture(all, new String[] { "v" }, DataTypes.TEXT)) {
            assertAll(() -> assertNotNull(traceFixture.executor.execute("SELECT v FROM t WHERE id = ?", "a")),
                    () -> assertNotNull(allFixture.executor.execute("SELECT v FROM t WHERE id = ?", "b")));
        }
    }

    // ------------------------------------------------------------------
    // CassandraExecutor.UDTCodec anonymous-class tail (3412, 3608-3621, 3676, 3735)
    // ------------------------------------------------------------------

    /** Snake_case UDT fields bind through the mapped property; fields without a property are written null and ignored. */
    @Test
    void udtCodecBindsSnakeCaseFieldsThroughTheMappedPropertyAndSkipsUnknownOnes() {
        final UserDefinedType type = fieldType(new String[] { "first_name", "ghost" }, DataTypes.TEXT, DataTypes.TEXT);
        final CassandraExecutor.UDTCodec<SnakePerson> beanCodec = CassandraExecutor.UDTCodec.create(type, SnakePerson.class);
        @SuppressWarnings("rawtypes")
        final CassandraExecutor.UDTCodec<Map> mapCodec = CassandraExecutor.UDTCodec.create(type, Map.class);
        final Map<String, Object> source = new LinkedHashMap<>();
        source.put("first_name", "Ada");
        source.put("ghost", "ignored");

        final SnakePerson person = beanCodec.decode(mapCodec.encode(source, ProtocolVersion.V4), ProtocolVersion.V4);
        final Map<?, ?> written = mapCodec.decode(beanCodec.encode(new SnakePerson("Grace"), ProtocolVersion.V4), ProtocolVersion.V4);

        assertAll(() -> assertEquals("Ada", person.getFirstName()), () -> assertEquals("Grace", written.get("first_name")),
                () -> assertNull(written.get("ghost")));
    }

    /** 3412 (false arm): a byte[] for a non-blob UDT field keeps the driver's codec lookup failure. */
    @Test
    void byteArrayForANonBlobUdtFieldKeepsTheCodecLookupFailure() {
        final UserDefinedType type = fieldType(new String[] { "v" }, DataTypes.TEXT);
        @SuppressWarnings("rawtypes")
        final CassandraExecutor.UDTCodec<Map> mapCodec = CassandraExecutor.UDTCodec.create(type, Map.class);

        assertThrows(CodecNotFoundException.class, () -> mapCodec.encode(Map.of("v", BYTES), ProtocolVersion.V4));
    }

    // ------------------------------------------------------------------
    // CassandraExecutorBase delete-name guards (1539, 1609, 1675, 1750)
    // ------------------------------------------------------------------

    @Test
    void syncDeleteRejectsEmptyPropNames() {
        final TestEntity entity = new TestEntity();
        entity.setId(1L);
        entity.setName("Ada");
        final Condition where = Filters.eq("id", 1L);
        final List<String> empty = new ArrayList<>();

        try (Fixture fixture = new Fixture(new String[] { "id" }, DataTypes.INT)) {
            assertAll(() -> assertNotNull(fixture.executor.delete(entity, (Collection<String>) null)),
                    () -> assertThrows(IllegalArgumentException.class, () -> fixture.executor.delete(entity, empty)),
                    () -> assertNotNull(fixture.executor.delete(TestEntity.class, (Collection<String>) null, 1L)),
                    () -> assertThrows(IllegalArgumentException.class, () -> fixture.executor.delete(TestEntity.class, empty, 1L)),
                    () -> assertNotNull(fixture.executor.delete(TestEntity.class, (Collection<String>) null, where)),
                    () -> assertThrows(IllegalArgumentException.class, () -> fixture.executor.delete(TestEntity.class, empty, where)),
                    () -> assertNotNull(fixture.executor.batchDelete(Arrays.asList(entity), null)),
                    () -> assertThrows(IllegalArgumentException.class, () -> fixture.executor.batchDelete(Arrays.asList(entity), empty)));
        }
    }

    /** 1091, 1170, 1248, 1336: the same guards on the async facade, thrown synchronously. */
    @Test
    void asyncDeleteRejectsEmptyPropNamesSynchronously() throws Exception {
        final TestEntity entity = new TestEntity();
        entity.setId(1L);
        entity.setName("Ada");
        final Condition where = Filters.eq("id", 1L);
        final List<String> empty = new ArrayList<>();
        final AsyncCassandraExecutorBase<TestRow, TestResultSet, TestStatement, TestPreparedStatement, TestBatchType> async = executor.async();

        assertAll(() -> assertNotNull(async.delete(entity, (Collection<String>) null).get()),
                () -> assertThrows(IllegalArgumentException.class, () -> async.delete(entity, empty)),
                () -> assertNotNull(async.delete(TestEntity.class, (Collection<String>) null, 1L).get()),
                () -> assertThrows(IllegalArgumentException.class, () -> async.delete(TestEntity.class, empty, 1L)),
                () -> assertNotNull(async.delete(TestEntity.class, (Collection<String>) null, where).get()),
                () -> assertThrows(IllegalArgumentException.class, () -> async.delete(TestEntity.class, empty, where)),
                () -> assertNotNull(async.batchDelete(Arrays.asList(entity), null).get()),
                () -> assertThrows(IllegalArgumentException.class, () -> async.batchDelete(Arrays.asList(entity), empty)));
    }

    // ------------------------------------------------------------------
    // CassandraExecutorBase key handling (567-577, 588-597, 640-642, 661-663, 1159-1161, 3756-3799, 4026-4042)
    // ------------------------------------------------------------------

    @Test
    void entityToConditionRequiresKeyNamesAndValues() {
        final TestEntity noId = new TestEntity();
        final CompositeKeyEntity missingPart = new CompositeKeyEntity();
        missingPart.setSessionId("s2");

        assertAll(() -> assertThrows(IllegalArgumentException.class, () -> TestCassandraExecutor.exposedEntityToCondition(new SliceNKeylessEntity())),
                () -> assertThrows(IllegalArgumentException.class, () -> TestCassandraExecutor.exposedEntityToCondition(noId)),
                () -> assertThrows(IllegalArgumentException.class, () -> TestCassandraExecutor.exposedEntityToCondition(new NovStringKeyEntity(""))),
                () -> assertThrows(IllegalArgumentException.class, () -> TestCassandraExecutor.exposedEntityToCondition(missingPart)), () -> {
                    final CompositeKeyEntity both = new CompositeKeyEntity();
                    both.setUserId("u1");
                    both.setSessionId("s1");
                    assertNotNull(TestCassandraExecutor.exposedEntityToCondition(both));
                });
    }

    @Test
    void entityToConditionCollectionRequiresKeyNamesAndValues() {
        final TestEntity noId = new TestEntity();
        final TestEntity valid = new TestEntity();
        valid.setId(1L);

        assertAll(
                () -> assertThrows(IllegalArgumentException.class,
                        () -> TestCassandraExecutor.exposedEntityToConditionCollection(SliceNKeylessEntity.class, Arrays.asList(new SliceNKeylessEntity()))),
                () -> assertThrows(IllegalArgumentException.class, () -> TestCassandraExecutor.exposedEntityToConditionCollection(TestEntity.class, Arrays.asList(noId))),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> TestCassandraExecutor.exposedEntityToConditionCollection(NovStringKeyEntity.class, Arrays.asList(new NovStringKeyEntity("")))),
                () -> assertNotNull(TestCassandraExecutor.exposedEntityToConditionCollection(TestEntity.class, Arrays.asList(valid))));
    }

    /** 1159-1161: a Map entity whose keys are not property names is rejected on entity-only update. */
    @Test
    void mapEntityUpdateRejectsNonStringKeys() {
        final NovEntityMap entity = new NovEntityMap();
        entity.put("id", 1);
        // A non-String key in the (String-keyed) map entity: exactly the entry the guard exists for.
        @SuppressWarnings({ "rawtypes", "unchecked" })
        final Map rawEntity = entity;
        rawEntity.put(42, "not a property name");

        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> executor.update(entity));

        assertTrue(e.getMessage().contains("Map entity keys must be property names"), e.getMessage());
    }

    /** 3756-3760, 3781-3799: a Map entity insert goes through the naming policy's builder. */
    @Test
    void mapEntityInsertGoesThroughTheBuilder() {
        final NovEntityMap filled = new NovEntityMap();
        filled.put("id", 1);
        filled.put("name", "Ada");

        assertNotNull(executor.exposedPrepareInsert((Object) filled));
    }

    /** 4026-4028, 4040-4042: batch map updates require a key and non-empty key values. */
    @Test
    void batchMapUpdateValidatesTheKey() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class,
                        () -> executor.prepareBatchMapUpdateForTest(SliceNKeylessEntity.class, N.asMap("name", "x"))),
                () -> assertThrows(IllegalArgumentException.class, () -> executor.prepareBatchMapUpdateForTest(TestEntity.class, N.asMap("id", null, "name", "x"))),
                () -> assertThrows(IllegalArgumentException.class, () -> executor.prepareBatchMapUpdateForTest(TestEntity.class, N.asMap("id", "", "name", "x"))),
                () -> assertNotNull(executor.prepareBatchMapUpdateForTest(TestEntity.class, N.asMap("id", 1, "name", "x"))));
    }

    // ------------------------------------------------------------------
    // fixtures
    // ------------------------------------------------------------------

    /** A driver session whose prepared statements declare the given variables; every bind is recorded. */
    private static class Fixture implements AutoCloseable {
        final CqlSession session = Mockito.mock(CqlSession.class);
        final PreparedStatement prepared = Mockito.mock(PreparedStatement.class);
        final List<Object[]> binds = new ArrayList<>();
        final CassandraExecutor executor;

        Fixture(final String[] variableNames, final DataType... variableTypes) {
            this(CodecRegistry.DEFAULT, null, variableNames, variableTypes);
        }

        Fixture(final CodecRegistry registry, final StatementSettings settings, final String[] variableNames, final DataType... variableTypes) {
            final com.datastax.oss.driver.api.core.context.DriverContext context = Mockito
                    .mock(com.datastax.oss.driver.api.core.context.DriverContext.class);
            Mockito.when(session.getContext()).thenReturn(context);
            Mockito.when(context.getCodecRegistry()).thenReturn(registry);

            final ColumnDefinitions variables = Mockito.mock(ColumnDefinitions.class);
            Mockito.when(variables.size()).thenReturn(variableNames.length);

            for (int i = 0; i < variableNames.length; i++) {
                final ColumnDefinition variable = Mockito.mock(ColumnDefinition.class);
                Mockito.when(variables.get(i)).thenReturn(variable);
                Mockito.when(variable.getName()).thenReturn(CqlIdentifier.fromInternal(variableNames[i]));
                Mockito.when(variable.getType()).thenReturn(variableTypes[i]);
            }

            Mockito.when(prepared.getVariableDefinitions()).thenReturn(variables);
            Mockito.when(prepared.bind(Mockito.any(Object[].class))).thenAnswer(invocation -> {
                binds.add(((Object[]) invocation.getRawArguments()[0]).clone());
                return Mockito.mock(BoundStatement.class);
            });
            Mockito.when(prepared.bind()).thenReturn(Mockito.mock(BoundStatement.class));
            Mockito.when(session.prepare(Mockito.anyString())).thenReturn(prepared);
            Mockito.when(session.execute(Mockito.any(Statement.class))).thenReturn(Mockito.mock(ResultSet.class));

            executor = settings == null ? new CassandraExecutor(session) : new CassandraExecutor(session, settings);
            // The eager async facade performs a warm-up empty bind during construction; drop it so the asserts below see
            // only the binds their own executes made.
            binds.clear();
        }

        @Override
        public void close() {
            executor.close();
        }
    }

    /** A fixture whose prepared statement has no variables and whose no-arg bind is counted. */
    private static final class ZeroVariableFixture extends Fixture {
        final AtomicInteger noArgBinds = new AtomicInteger();

        ZeroVariableFixture() {
            super(new String[0]);

            final PreparedStatement statement = prepared;
            Mockito.when(statement.bind()).thenAnswer(invocation -> {
                noArgBinds.incrementAndGet();
                return Mockito.mock(BoundStatement.class);
            });
        }
    }

    /** A fixture whose single variable carries no name, so bind errors report "parameter at index i". */
    private static final class UnnamedVariableFixture extends Fixture {
        UnnamedVariableFixture() {
            super(CodecRegistry.DEFAULT, null, new String[] { "v" }, DataTypes.INT);

            final ColumnDefinition variable = prepared.getVariableDefinitions().get(0);
            Mockito.when(variable.getName()).thenReturn(null);
        }
    }

    /** A fixture bound to a registry that holds a ZonedDateTime-to-Instant codec. */
    private static final class ZonedDateTimeCodecFixture extends Fixture {
        ZonedDateTimeCodecFixture(final String[] variableNames, final DataType... variableTypes) {
            super(zonedDateTimeRegistry(), null, variableNames, variableTypes);
        }

        private static MutableCodecRegistry zonedDateTimeRegistry() {
            final DefaultCodecRegistry registry = new DefaultCodecRegistry("cov-nov-2026");
            registry.register(new ZonedDateTimeCodec());

            return registry;
        }
    }

    /** A fixture whose bound statements are fluent mocks, so every configured setting can be applied. */
    private static final class SettingsFixture extends Fixture {
        SettingsFixture(final StatementSettings settings, final String[] variableNames, final DataType... variableTypes) {
            super(CodecRegistry.DEFAULT, settings, variableNames, variableTypes);

            final PreparedStatement statement = prepared;
            final BoundStatement selfConfiguring = Mockito.mock(BoundStatement.class, Mockito.RETURNS_SELF);
            Mockito.when(statement.bind(Mockito.any(Object[].class))).thenAnswer(invocation -> {
                binds.add(((Object[]) invocation.getRawArguments()[0]).clone());
                return selfConfiguring;
            });
        }
    }

    /** TIMESTAMP column codec accepting ZonedDateTime values as their instant (as an application MappingCodec would). */
    private static class ZonedDateTimeCodec extends MappingCodec<Instant, ZonedDateTime> {
        ZonedDateTimeCodec() {
            super(TypeCodecs.TIMESTAMP, GenericType.of(ZonedDateTime.class));
        }

        @Override
        protected ZonedDateTime innerToOuter(final Instant instant) {
            return instant == null ? null : instant.atZone(java.time.ZoneId.systemDefault());
        }

        @Override
        protected Instant outerToInner(final ZonedDateTime zonedDateTime) {
            return zonedDateTime == null ? null : zonedDateTime.toInstant();
        }
    }

    // row / UDT helpers (same shape as the neighboring 2026 fixtures)

    private static ResultSet rows(final String[] names, final DataType type, final Object[][] values) {
        return rows(names, java.util.Collections.nCopies(names.length, type).toArray(new DataType[0]), values);
    }

    private static ResultSet rows(final String name, final DataType type, final Object[] singleRowValues) {
        return rows(new String[] { name }, type, new Object[][] { singleRowValues });
    }

    private static ResultSet rows(final String[] names, final DataType[] types, final Object[][] values) {
        final ColumnDefinitions columns = Mockito.mock(ColumnDefinitions.class);
        Mockito.when(columns.size()).thenReturn(names.length);

        for (int i = 0; i < names.length; i++) {
            final ColumnDefinition column = Mockito.mock(ColumnDefinition.class);
            Mockito.when(columns.get(i)).thenReturn(column);
            Mockito.when(column.getName()).thenReturn(CqlIdentifier.fromInternal(names[i]));
            Mockito.when(column.getType()).thenReturn(types[i]);
        }

        final List<Row> rowList = Arrays.stream(values).map(rowValues -> {
            final Row row = Mockito.mock(Row.class);
            Mockito.when(row.getColumnDefinitions()).thenReturn(columns);

            for (int i = 0; i < rowValues.length; i++) {
                Mockito.when(row.getObject(i)).thenReturn(rowValues[i]);
            }

            return row;
        }).toList();

        final ListIterator<Row> iterator = rowList.listIterator();
        final ResultSet result = Mockito.mock(ResultSet.class);
        Mockito.when(result.getColumnDefinitions()).thenReturn(columns);
        Mockito.when(result.getAvailableWithoutFetching()).thenAnswer(invocation -> rowList.size() - iterator.nextIndex());
        Mockito.when(result.iterator()).thenReturn(iterator);
        Mockito.when(result.one()).thenAnswer(invocation -> iterator.hasNext() ? iterator.next() : null);
        return result;
    }

    /** A row mock with TEXT-typed columns. */
    private static Row driverRow(final String[] names, final DataType type, final Object... values) {
        return driverRow(names, java.util.Collections.nCopies(names.length, type).toArray(new DataType[0]), values);
    }

    /** A row mock with per-column types. */
    private static Row driverRow(final String[] names, final DataType[] types, final Object... values) {
        final ColumnDefinitions columns = Mockito.mock(ColumnDefinitions.class);
        Mockito.when(columns.size()).thenReturn(names.length);

        for (int i = 0; i < names.length; i++) {
            final ColumnDefinition column = Mockito.mock(ColumnDefinition.class);
            Mockito.when(columns.get(i)).thenReturn(column);
            Mockito.when(column.getName()).thenReturn(CqlIdentifier.fromInternal(names[i]));
            Mockito.when(column.getType()).thenReturn(types[i]);
        }

        final Row row = Mockito.mock(Row.class);
        Mockito.when(row.getColumnDefinitions()).thenReturn(columns);

        for (int i = 0; i < values.length; i++) {
            Mockito.when(row.getObject(i)).thenReturn(values[i]);
        }

        return row;
    }

    /** A real driver row (as the driver decodes it) of VARCHAR columns, for tests that need the concrete row class. */
    private static Row realDriverRow(final String[] names, final Object... values) {
        final List<ColumnDefinition> definitions = new ArrayList<>();

        for (int i = 0; i < names.length; i++) {
            definitions.add(new DefaultColumnDefinition(
                    new ColumnSpec("ks", "t", names[i], i, RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR)), AttachmentPoint.NONE));
        }

        final List<ByteBuffer> encoded = Arrays.stream(values)
                .map(value -> TypeCodecs.TEXT.encode(String.valueOf(value), ProtocolVersion.DEFAULT)).toList();

        return new DefaultRow(DefaultColumnDefinitions.valueOf(definitions), encoded, AttachmentPoint.NONE);
    }

    private static UserDefinedType fieldType(final String[] fieldNames, final DataType... fieldTypes) {
        final CodecRegistry registry = new DefaultCodecRegistry("cov-nov-2026-udt");
        final List<CqlIdentifier> names = Arrays.stream(fieldNames).map(CqlIdentifier::fromInternal).toList();

        return new DefaultUserDefinedType(CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("value_holder"), false, names,
                Arrays.asList(fieldTypes), new AttachmentPoint() {
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

    /** Turns the executor's logger up to debug for the duration of one test (the abacus facade runs over JUL here). */
    private static Runnable enableDebug(final Class<?> type) {
        final com.landawn.abacus.logging.Logger logger = com.landawn.abacus.logging.LoggerFactory.getLogger(type);

        if (logger.isDebugEnabled()) {
            return () -> {
            };
        }

        final java.util.logging.Logger jul = java.util.logging.Logger.getLogger(type.getName());
        final java.util.logging.Level previous = jul.getLevel();
        jul.setLevel(java.util.logging.Level.ALL);

        return () -> jul.setLevel(previous);
    }

    // beans

    public static class UnamePerson {
        @Column("uname")
        private String userName;

        private int id;

        public String getUserName() {
            return userName;
        }

        public void setUserName(final String userName) {
            this.userName = userName;
        }

        public int getId() {
            return id;
        }

        public void setId(final int id) {
            this.id = id;
        }
    }

    public static class SnakePerson {
        private String firstName;
        private int id;

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(final String firstName) {
            this.firstName = firstName;
        }

        public int getId() {
            return id;
        }

        public void setId(final int id) {
            this.id = id;
        }

        public SnakePerson() {
        }

        public SnakePerson(final String firstName) {
            this.firstName = firstName;
        }
    }

    public static class NovDatasetBean {
        private NestedPerson inner;
        private Optional<String> blobOpt;
        private byte[] blobBytes;
        private String extra;

        public NestedPerson getInner() {
            return inner;
        }

        public void setInner(final NestedPerson inner) {
            this.inner = inner;
        }

        public Optional<String> getBlobOpt() {
            return blobOpt;
        }

        public void setBlobOpt(final Optional<String> blobOpt) {
            this.blobOpt = blobOpt;
        }

        public byte[] getBlobBytes() {
            return blobBytes;
        }

        public void setBlobBytes(final byte[] blobBytes) {
            this.blobBytes = blobBytes;
        }

        public String getExtra() {
            return extra;
        }

        public void setExtra(final String extra) {
            this.extra = extra;
        }
    }

    public static class NestedPerson {
        private String name;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    public static class NovBaseBean<T> {
        private Optional<T> v;

        public Optional<T> getV() {
            return v;
        }

        public void setV(final Optional<T> v) {
            this.v = v;
        }
    }

    public static class NovDerivedBean extends NovBaseBean<String> {
    }

    public static class NovHolder {
        private NestedReadOnlyInner inner;

        public NestedReadOnlyInner getInner() {
            return inner;
        }

        public void setInner(final NestedReadOnlyInner inner) {
            this.inner = inner;
        }
    }

    public static class NestedReadOnlyInner {
        private String name;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        // getter-only: a read-only (computed) property
        public String getComputed() {
            return "computed-value";
        }
    }

    public static class TemporalRoutingBean {
        private Time timeAsTime;
        private LocalTime timeAsLocal;
        private String timeAsText;
        private byte[] blobAsBytes;
        private ByteBuffer blobAsBuffer;
        private Instant dateAsInstant;
        private LocalDate stampAsDate;

        public Time getTimeAsTime() {
            return timeAsTime;
        }

        public void setTimeAsTime(final Time timeAsTime) {
            this.timeAsTime = timeAsTime;
        }

        public LocalTime getTimeAsLocal() {
            return timeAsLocal;
        }

        public void setTimeAsLocal(final LocalTime timeAsLocal) {
            this.timeAsLocal = timeAsLocal;
        }

        public String getTimeAsText() {
            return timeAsText;
        }

        public void setTimeAsText(final String timeAsText) {
            this.timeAsText = timeAsText;
        }

        public byte[] getBlobAsBytes() {
            return blobAsBytes;
        }

        public void setBlobAsBytes(final byte[] blobAsBytes) {
            this.blobAsBytes = blobAsBytes;
        }

        public ByteBuffer getBlobAsBuffer() {
            return blobAsBuffer;
        }

        public void setBlobAsBuffer(final ByteBuffer blobAsBuffer) {
            this.blobAsBuffer = blobAsBuffer;
        }

        public Instant getDateAsInstant() {
            return dateAsInstant;
        }

        public void setDateAsInstant(final Instant dateAsInstant) {
            this.dateAsInstant = dateAsInstant;
        }

        public LocalDate getStampAsDate() {
            return stampAsDate;
        }

        public void setStampAsDate(final LocalDate stampAsDate) {
            this.stampAsDate = stampAsDate;
        }
    }

    public static class ContainerBean {
        public List<Long> longs;
        public List<Integer> ints;
        public Collection<Integer> raw;
        public Collection rawBlob;
        public Set<String> hosts;
        public Map<String, Long> counts;
        public Map<String, Object> raw2;
        public List<SortedMap<String, Long>> maps;
        public List<Object> rawUdts;
    }

    @com.landawn.abacus.annotation.Entity
    public static class NovEntityMap extends LinkedHashMap<String, Object> {
        private static final long serialVersionUID = 1L;
    }

    public static class NovStringKeyEntity {
        @Id
        private String code;

        public NovStringKeyEntity() {
        }

        public NovStringKeyEntity(final String code) {
            this.code = code;
        }

        public String getCode() {
            return code;
        }

        public void setCode(final String code) {
            this.code = code;
        }
    }
}
