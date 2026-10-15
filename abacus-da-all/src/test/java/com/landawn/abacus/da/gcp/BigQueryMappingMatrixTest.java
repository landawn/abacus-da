package com.landawn.abacus.da.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.N;

public class BigQueryMappingMatrixTest extends BigQueryExecutorTestSupport {

    // ---- 2026-10-04 coverageBQ ----

    // Conversion matrix. Review after review found a value shape crossing two conversion rules (a type variable bound to a
    // text type, a typed-container property of a REPEATED STRUCT element bean, a record element, a STRUCT inside a REPEATED
    // value without a schema, ...), each fixed where it was found. This runs every property kind - scalar (INT64 -> long /
    // Long / BigDecimal, STRING -> String / StringBuilder / enum), binary (BYTES -> byte[] / ByteBuffer), temporal (TIMESTAMP
    // -> Instant / Date / LocalDateTime, TIME -> java.sql.Time / LocalTime, DATE -> LocalDate), STRUCT bean (with TIMESTAMP,
    // BYTES and nested STRUCT fields) and typed container (STRUCT -> Map<String, Long> / List<Long>, REPEATED INT64 ->
    // List<Long>) - in every position: plain property; type variable bound by a parameterized property (Box<X>); type
    // variable bound by a subclass (XBox extends Box<X>) as a property and as the row; REPEATED element (List<X>, X[]);
    // property of a REPEATED STRUCT element bean; REPEATED STRUCT generic element (List<Box<X>>, List<XBox>); record
    // component (record as the row, as a STRUCT property and as a List<record> element). Each runs over both payload shapes
    // (as the BigQuery client decodes them - REPEATED values and every record schema-less - and hand-built - REPEATED values
    // as plain Lists, records with their sub-schema attached) and four read paths (toEntity, the registered N.convert
    // converter, toList and the bean-class Dataset), asserting the exact Java type and value of every cell. Mismatches are
    // collected so one run reports every failing combination.
    // Not representable, so excluded: a primitive long as a type argument (Box<X>, XBox, List<X>, List<Box<X>>, List<XBox>),
    // and a REPEATED column as the element of a REPEATED column (BigQuery has no ARRAY<ARRAY<...>>: the REPEATED INT64 ->
    // List<Long> kind is not a List<X> / X[] element).
    // 208 combinations x 2 shapes x 4 paths = 1664 checks. On HEAD 1086 fail; before fixBQ2 340; before the text type-variable
    // fix 54. The matrix itself found the STRUCT -> List<Long> kind as a REPEATED element (List<List<Long>>, List<Long>[]):
    // the element was unwrapped as a Map and rejected by the element codec (also on HEAD) - fixed in convertRepeatedValue,
    // see testRepeatedStructIntoCollectionOrArrayElementsReadsFieldValueLists_coverageBQ.
    @Test
    public void testConversionMatrix_coverageBQ() throws Exception {
        final java.time.Instant instant = java.time.Instant.ofEpochSecond(1_718_900_000L, 123_456_000L);
        final FixBqLoc loc = new FixBqLoc();
        loc.setLat(37.5);
        loc.setLng(-122.25);
        final MxBean bean = new MxBean();
        bean.setName("n");
        bean.setNum(7L);
        bean.setAt(instant);
        bean.setBlob(new byte[] { 1, 2, 3 });
        bean.setLoc(loc);

        final FieldList beanFields = FieldList.of(Field.of("name", StandardSQLTypeName.STRING), Field.of("num", StandardSQLTypeName.INT64),
                Field.of("at", StandardSQLTypeName.TIMESTAMP), Field.of("blob", StandardSQLTypeName.BYTES),
                Field.newBuilder("loc", StandardSQLTypeName.STRUCT, fixBqLocFields()).build());
        final FieldList xyFields = FieldList.of(Field.of("x", StandardSQLTypeName.INT64), Field.of("y", StandardSQLTypeName.INT64));
        final FieldList abFields = FieldList.of(Field.of("a", StandardSQLTypeName.INT64), Field.of("b", StandardSQLTypeName.INT64));
        final String timestamp = "1718900000.123456";

        final List<MxKind> kinds = Arrays.asList( //
                MxKind.scalar("lng", long.class, StandardSQLTypeName.INT64, "12", 12L, null, MxLongRec.class),
                MxKind.scalar("lngW", Long.class, StandardSQLTypeName.INT64, "12", 12L, MxLongBox.class, MxLongWRec.class),
                MxKind.scalar("dec", BigDecimal.class, StandardSQLTypeName.INT64, "12", new BigDecimal("12"), MxDecimalBox.class, MxDecimalRec.class),
                MxKind.scalar("str", String.class, StandardSQLTypeName.STRING, "abc", "abc", MxStringBox.class, MxStringRec.class),
                MxKind.scalar("sb", StringBuilder.class, StandardSQLTypeName.STRING, "abc", new StringBuilder("abc"), MxBuilderBox.class, MxBuilderRec.class),
                MxKind.scalar("enm", FixBq2Color.class, StandardSQLTypeName.STRING, "GREEN", FixBq2Color.GREEN, MxColorBox.class, MxColorRec.class),
                MxKind.scalar("bytes", byte[].class, StandardSQLTypeName.BYTES, "AQID", new byte[] { 1, 2, 3 }, MxBytesBox.class, MxBytesRec.class),
                MxKind.scalar("buf", java.nio.ByteBuffer.class, StandardSQLTypeName.BYTES, "AQID", java.nio.ByteBuffer.wrap(new byte[] { 1, 2, 3 }),
                        MxBufferBox.class, MxBufferRec.class),
                MxKind.scalar("instant", java.time.Instant.class, StandardSQLTypeName.TIMESTAMP, timestamp, instant, MxInstantBox.class, MxInstantRec.class),
                MxKind.scalar("date", java.util.Date.class, StandardSQLTypeName.TIMESTAMP, timestamp, new java.util.Date(1_718_900_000_123L), MxDateBox.class,
                        MxDateRec.class),
                MxKind.scalar("ldt", java.time.LocalDateTime.class, StandardSQLTypeName.TIMESTAMP, timestamp,
                        java.time.LocalDateTime.ofInstant(instant, java.time.ZoneId.systemDefault()), MxLdtBox.class, MxLdtRec.class),
                MxKind.scalar("time", java.sql.Time.class, StandardSQLTypeName.TIME, "16:13:20.123456",
                        new java.sql.Time(java.sql.Time.valueOf(java.time.LocalTime.of(16, 13, 20)).getTime() + 123), MxTimeBox.class, MxTimeRec.class),
                MxKind.scalar("ltime", java.time.LocalTime.class, StandardSQLTypeName.TIME, "16:13:20.123456", java.time.LocalTime.of(16, 13, 20, 123_456_000),
                        MxLocalTimeBox.class, MxLocalTimeRec.class),
                MxKind.scalar("ldate", java.time.LocalDate.class, StandardSQLTypeName.DATE, "2024-01-02", java.time.LocalDate.of(2024, 1, 2),
                        MxLocalDateBox.class, MxLocalDateRec.class),
                new MxKind("bean", MxBean.class, null, beanFields, false,
                        sdk -> mxRecord(sdk, beanFields, verifyCell("n"), verifyCell("7"), verifyCell(timestamp), verifyCell("AQID"),
                                mxRecord(sdk, fixBqLocFields(), verifyCell("37.5"), verifyCell("-122.25"))),
                        bean, MxBeanBox.class, MxBeanRec.class),
                new MxKind("map", Map.class, null, xyFields, false, sdk -> mxRecord(sdk, xyFields, verifyCell("1"), verifyCell("2")), Map.of("x", 1L, "y", 2L),
                        MxMapBox.class, MxMapRec.class),
                new MxKind("pair", List.class, null, abFields, false, sdk -> mxRecord(sdk, abFields, verifyCell("1"), verifyCell("2")), Arrays.asList(1L, 2L),
                        MxListBox.class, MxListRec.class),
                new MxKind("codes", List.class, StandardSQLTypeName.INT64, null, true, sdk -> mxRepeated(sdk, verifyCell("1"), verifyCell("2")),
                        Arrays.asList(1L, 2L), MxListBox.class, MxListRec.class));

        final MxCell direct = (kind, column, sdk) -> kind.cell.apply(sdk);
        final MxCell struct = (kind, column, sdk) -> mxRecord(sdk, column.getSubFields(), kind.cell.apply(sdk));
        final MxCell repeated = (kind, column, sdk) -> mxRepeated(sdk, kind.cell.apply(sdk), kind.cell.apply(sdk));
        final MxCell repeatedStruct = (kind, column, sdk) -> mxRepeated(sdk, mxRecord(sdk, column.getSubFields(), kind.cell.apply(sdk)),
                mxRecord(sdk, column.getSubFields(), kind.cell.apply(sdk)));
        final java.util.function.Predicate<MxKind> all = kind -> true;
        final java.util.function.Predicate<MxKind> typeArgument = kind -> kind.boxClass != null;

        final List<MxPosition> positions = Arrays.asList( //
                new MxPosition("plain property", kind -> MxPlain.class, false, all, kind -> kind.field(kind.name, false), direct, (kind, v) -> v,
                        MxPosition.SINGLE),
                new MxPosition("Box<X> property", kind -> MxBoxes.class, false, typeArgument, kind -> mxStruct(kind.name, kind.field("value", false)), struct,
                        (kind, v) -> mxField(v, "value"), MxPosition.SINGLE),
                new MxPosition("XBox property", kind -> MxSubs.class, false, typeArgument, kind -> mxStruct(kind.name, kind.field("value", false)), struct,
                        (kind, v) -> mxField(v, "value"), MxPosition.SINGLE),
                new MxPosition("XBox row", kind -> kind.boxClass, true, typeArgument, kind -> kind.field("value", false), direct,
                        (kind, v) -> mxField(v, "value"), MxPosition.SINGLE),
                new MxPosition("List<X> element", kind -> MxLists.class, false, kind -> kind.boxClass != null && !kind.repeatedColumn,
                        kind -> kind.field(kind.name, true), repeated, (kind, v) -> v, MxPosition.LIST),
                new MxPosition("X[] element", kind -> MxArrays.class, false, kind -> !kind.repeatedColumn, kind -> kind.field(kind.name, true), repeated,
                        (kind, v) -> v, MxPosition.ARRAY),
                new MxPosition("REPEATED STRUCT element bean property", kind -> MxItems.class, false, all,
                        kind -> mxRepeatedStruct("items", kind.field(kind.name, false)), repeatedStruct, (kind, v) -> mxEach(v, e -> mxField(e, kind.name)),
                        MxPosition.LIST),
                new MxPosition("List<Box<X>> element", kind -> MxBoxLists.class, false, typeArgument,
                        kind -> mxRepeatedStruct(kind.name, kind.field("value", false)), repeatedStruct, (kind, v) -> mxEach(v, e -> mxField(e, "value")),
                        MxPosition.LIST),
                new MxPosition("List<XBox> element", kind -> MxSubLists.class, false, typeArgument,
                        kind -> mxRepeatedStruct(kind.name, kind.field("value", false)), repeatedStruct, (kind, v) -> mxEach(v, e -> mxField(e, "value")),
                        MxPosition.LIST),
                new MxPosition("record row", kind -> kind.recordClass, true, all, kind -> kind.field("v", false), direct, (kind, v) -> mxField(v, "v"),
                        MxPosition.SINGLE),
                new MxPosition("record STRUCT property", kind -> MxRecs.class, false, all, kind -> mxStruct(kind.name, kind.field("v", false)), struct,
                        (kind, v) -> mxField(v, "v"), MxPosition.SINGLE),
                new MxPosition("List<record> element", kind -> MxRecLists.class, false, all, kind -> mxRepeatedStruct(kind.name, kind.field("v", false)),
                        repeatedStruct, (kind, v) -> mxEach(v, e -> mxField(e, "v")), MxPosition.LIST));

        final List<String> mismatches = new ArrayList<>();
        int combinations = 0;
        int checks = 0;

        for (final MxPosition position : positions) {
            for (final MxKind kind : kinds) {
                if (!position.applies.test(kind)) {
                    continue;
                }

                combinations++;
                final Field column = position.column.apply(kind);
                final FieldList fields = FieldList.of(column);
                final Class<?> target = position.target.apply(kind);
                final String expected = mxDescribe(position.expected(kind));

                for (final boolean sdk : new boolean[] { true, false }) {
                    final FieldValueList row = FieldValueList.of(Arrays.asList(position.cell.of(kind, column, sdk)), fields);
                    final Map<String, java.util.concurrent.Callable<Object>> paths = new LinkedHashMap<>();
                    paths.put("toEntity", () -> position.fromResult(kind, column, BigQueryExecutor.toEntity(fields, row, target)));
                    paths.put("N.convert", () -> position.fromResult(kind, column, N.convert(row, target)));
                    paths.put("toList", () -> {
                        fixBqStubResult(fields, row);
                        return position.fromResult(kind, column, BigQueryExecutor.toList(mockTableResult, target).get(0));
                    });
                    paths.put("extractData", () -> {
                        fixBqStubResult(fields, row);
                        return position.fromColumn(kind, BigQueryExecutor.extractData(mockTableResult, target).getColumn(column.getName()).get(0));
                    });

                    for (final Map.Entry<String, java.util.concurrent.Callable<Object>> path : paths.entrySet()) {
                        checks++;
                        String actual;

                        try {
                            actual = mxDescribe(path.getValue().call());
                        } catch (final Exception | Error e) {
                            actual = "threw " + e;
                        }

                        if (!expected.equals(actual)) {
                            mismatches.add(position.name + " | " + kind.name + " | " + (sdk ? "client shape" : "plain shape") + " | " + path.getKey()
                                    + ": expected " + expected + " but was " + actual);
                        }
                    }
                }
            }
        }

        assertEquals(208, combinations);
        assertEquals(208 * 2 * 4, checks);
        assertTrue(mismatches.isEmpty(), mismatches.size() + " of " + checks + " checks failed (" + combinations
                + " position x kind combinations, 2 payload shapes, 4 read paths):\n" + String.join("\n", mismatches));
    }

    // A REPEATED STRUCT element read into a Collection or array element type is the list of its field values (as a STRUCT
    // cell read into a Collection property is), then typed by the codec. It was unwrapped as a Map, which a typed element
    // codec rejected (List<List<Long>>, Set<List<Long>>, Long[][], List<Long[]>, long[][], List<Set<Integer>>:
    // NumberFormatException "{"a": "1", "b": "2"} is not a valid Long"), turned into its JSON text (List<List<String>>), or
    // stored as a HashMap where a List / Object[] element was declared (List<List<Object>>, List<Object[]>). Every path that
    // shares convertRepeatedValue: toEntity, list rows, the bean-class Dataset, single-value reads and typed array rows; both
    // payload shapes. Pins: Object, Map and raw-List element targets keep the Map elements. RED on HEAD.
    @Test
    public void testRepeatedStructIntoCollectionOrArrayElementsReadsFieldValueLists_coverageBQ() throws Exception {
        final FieldList ab = FieldList.of(Field.of("a", StandardSQLTypeName.INT64), Field.of("b", StandardSQLTypeName.INT64));
        final List<Field> columns = new ArrayList<>();

        for (final String column : new String[] { "long_lists", "string_lists", "object_lists", "long_list_set", "long_arrays", "long_array_list",
                "object_array_list", "primitive_arrays", "integer_sets", "objects", "maps" }) {
            columns.add(verifyRepeatedStruct(column, ab));
        }

        final FieldList fields = FieldList.of(columns);
        final String valueMaps = mxDescribe(java.util.Collections.singletonList(Map.of("a", "1", "b", "2")));

        for (final boolean sdk : new boolean[] { true, false }) {
            final List<FieldValue> cells = new ArrayList<>();

            for (int i = 0; i < columns.size(); i++) {
                cells.add(mxRepeated(sdk, mxRecord(sdk, ab, verifyCell("1"), verifyCell("2"))));
            }

            final FieldValueList row = FieldValueList.of(cells, fields);
            final java.util.function.Consumer<MxStructElements> check = holder -> {
                assertEquals(mxDescribe(Arrays.asList(Arrays.asList(1L, 2L))), mxDescribe(holder.getLongLists()));
                assertEquals(mxDescribe(Arrays.asList(Arrays.asList("1", "2"))), mxDescribe(holder.getStringLists()));
                assertEquals(mxDescribe(Arrays.asList(Arrays.asList("1", "2"))), mxDescribe(holder.getObjectLists()));
                assertEquals(1, holder.getLongListSet().size());
                assertEquals(mxDescribe(Arrays.asList(1L, 2L)), mxDescribe(holder.getLongListSet().iterator().next()));
                assertEquals(mxDescribe(new Long[][] { { 1L, 2L } }), mxDescribe(holder.getLongArrays()));
                assertEquals(mxDescribe(java.util.Collections.singletonList(new Long[] { 1L, 2L })), mxDescribe(holder.getLongArrayList()));
                assertEquals(mxDescribe(java.util.Collections.singletonList(new Object[] { "1", "2" })), mxDescribe(holder.getObjectArrayList()));
                assertEquals(mxDescribe(new long[][] { { 1L, 2L } }), mxDescribe(holder.getPrimitiveArrays()));
                assertEquals(Arrays.asList(Set.of(1, 2)), holder.getIntegerSets());
                assertEquals(valueMaps, mxDescribe(holder.getObjects()));
                assertEquals(valueMaps, mxDescribe(holder.getMaps()));
            };

            check.accept(BigQueryExecutor.toEntity(fields, row, MxStructElements.class));

            fixBqStubResult(fields, row);
            check.accept(BigQueryExecutor.toList(mockTableResult, MxStructElements.class).get(0));

            final Dataset ds = BigQueryExecutor.extractData(mockTableResult, MxStructElements.class);
            assertEquals(mxDescribe(Arrays.asList(Arrays.asList(1L, 2L))), mxDescribe(ds.getColumn("long_lists").get(0)));
            assertEquals(mxDescribe(new Long[][] { { 1L, 2L } }), mxDescribe(ds.getColumn("long_arrays").get(0)));
            assertEquals(valueMaps, mxDescribe(ds.getColumn("objects").get(0)));

            final FieldList one = FieldList.of(columns.get(0));
            fixBqStubResult(one, FieldValueList.of(Arrays.asList(cells.get(0)), one));
            assertEquals(mxDescribe(new Long[][] { { 1L, 2L } }), mxDescribe(executor.queryForSingleValue(Long[][].class, "SELECT long_lists FROM t").get()));
            assertEquals(mxDescribe(new List<?>[] { Arrays.asList("1", "2") }),
                    mxDescribe(executor.queryForSingleValue(List[].class, "SELECT long_lists FROM t").get()));
            assertEquals(mxDescribe(new Long[][] { { 1L, 2L } }), mxDescribe(BigQueryExecutor.toList(mockTableResult, Long[][][].class).get(0)[0]));
            assertEquals(mxDescribe(new Object[][] { { "1", "2" } }), mxDescribe(BigQueryExecutor.toList(mockTableResult, Object[][][].class).get(0)[0]));
            // untyped targets keep the Map elements
            assertEquals(valueMaps, mxDescribe(executor.queryForSingleValue(List.class, "SELECT long_lists FROM t").get()));
            assertEquals(valueMaps, mxDescribe(BigQueryExecutor.toList(mockTableResult, Object[].class).get(0)[0]));
            assertEquals(mxDescribe(new Object[] { Map.of("a", "1", "b", "2") }),
                    mxDescribe(BigQueryExecutor.toList(mockTableResult, Object[][].class).get(0)[0]));
        }
    }

    // A REPEATED STRUCT column read as a bean or record array outside a bean property - queryForSingleValue /
    // queryForSingleNonNull, a typed array row (list rows and the registered converter) - maps each element like a STRUCT
    // property (TIMESTAMP decoded, INT64 converted for the record's long component), and a STRUCT column read into a record
    // by queryForSingleValue converts its components; both payload shapes. RED on HEAD (the JSON codec handed the
    // epoch-seconds text to the element, the client shape had no schema, and the record failed "argument type mismatch").
    @Test
    public void testRepeatedStructBeanAndRecordArraysOutsideBeanProperties_coverageBQ() throws Exception {
        final FieldList visitFields = verifyVisitFields();
        final FieldList fields = FieldList.of(verifyRepeatedStruct("visits", visitFields));
        final FieldList structFields = FieldList.of(fixBq2Struct("visit", visitFields));
        final java.time.Instant seenAt = java.time.Instant.ofEpochSecond(1_718_900_000L, 123_456_000L);
        final VisitRecord expectedRecord = new VisitRecord("SF", 94105L, seenAt);

        for (final boolean sdk : new boolean[] { true, false }) {
            final FieldValue visit = mxRecord(sdk, visitFields, verifyCell("SF"), verifyCell("94105"), verifyCell("1718900000.123456"));
            final FieldValueList row = FieldValueList.of(Arrays.asList(mxRepeated(sdk, visit, visit)), fields);
            final java.util.function.Consumer<Visit[]> check = visits -> {
                assertEquals(2, visits.length);
                assertEquals("SF", visits[1].getCity());
                assertEquals(94105L, visits[1].getZip());
                assertEquals(seenAt, visits[1].getSeenAt());
            };

            fixBqStubResult(fields, row);
            check.accept(executor.queryForSingleValue(Visit[].class, "SELECT visits FROM t").get());
            check.accept(executor.queryForSingleNonNull(Visit[].class, "SELECT visits FROM t").get());
            check.accept(BigQueryExecutor.toList(mockTableResult, Visit[][].class).get(0)[0]);
            check.accept(N.convert(row, Visit[][].class)[0]);
            assertEquals(Arrays.asList(expectedRecord, expectedRecord),
                    Arrays.asList(executor.queryForSingleValue(VisitRecord[].class, "SELECT visits FROM t").get()));
            assertEquals(Arrays.asList(expectedRecord, expectedRecord),
                    Arrays.asList(BigQueryExecutor.toList(mockTableResult, VisitRecord[][].class).get(0)[0]));

            fixBqStubResult(structFields, FieldValueList.of(Arrays.asList(visit), structFields));
            assertEquals(expectedRecord, executor.queryForSingleValue(VisitRecord.class, "SELECT visit FROM t").get());
            assertEquals(expectedRecord, executor.queryForSingleNonNull(VisitRecord.class, "SELECT visit FROM t").get());
        }
    }

    // A STRUCT column is read with its column's sub-schema on the remaining single-value / raw paths: a one-field STRUCT read
    // as a scalar (queryForSingleValue and single-column list rows) unwraps the REPEATED or STRUCT field inside it (the client
    // gives neither a schema of its own), a NULL nested field gives a primitive target its default, and an Object[] row, an
    // Object[] single value and the raw (null-class) Dataset flatten a STRUCT that carries no schema. A FieldValueList target
    // keeps the raw record (pin). RED on HEAD ("No schema is attached ...").
    @Test
    public void testStructColumnSchemaThreadedIntoScalarObjectArrayAndRawDatasetReads_coverageBQ() throws Exception {
        // STRUCT<tags ARRAY<STRING>>: the client attaches the schema of a top-level STRUCT, not of the REPEATED value in it
        final FieldList tagsSub = FieldList.of(fixBqRepeatedField("tags", StandardSQLTypeName.STRING));
        final FieldList wrapField = FieldList.of(fixBq2Struct("wrap", tagsSub));
        final FieldValue wrap = FieldValue.of(FieldValue.Attribute.RECORD,
                FieldValueList.of(Arrays.asList(fixBqRepeated(verifyCell("a"), verifyCell("b"))), tagsSub));
        fixBqStubResult(wrapField, FieldValueList.of(Arrays.asList(wrap), wrapField));

        assertEquals("[\"a\", \"b\"]", executor.queryForSingleValue(String.class, "SELECT wrap FROM t").get());
        assertEquals("[\"a\", \"b\"]", BigQueryExecutor.toList(mockTableResult, String.class).get(0));
        assertSame(wrap.getValue(), executor.queryForSingleValue(FieldValueList.class, "SELECT wrap FROM t").get());

        // STRUCT<inner STRUCT<n INT64>> without a schema on either record
        final FieldList innerSub = FieldList.of(fixBq2Struct("inner", FieldList.of(Field.of("n", StandardSQLTypeName.INT64))));
        final FieldList outerField = FieldList.of(fixBq2Struct("outer", innerSub));
        fixBqStubResult(outerField, FieldValueList.of(Arrays.asList(fixBqRecord(fixBqRecord(verifyCell("5")))), outerField));

        assertEquals(Long.valueOf(5), executor.queryForSingleValue(Long.class, "SELECT outer FROM t").get());
        assertEquals(Long.valueOf(5), BigQueryExecutor.toList(mockTableResult, Long.class).get(0));

        fixBqStubResult(outerField, FieldValueList.of(Arrays.asList(fixBqRecord(fixBqRecord(verifyCell(null)))), outerField));
        assertEquals(Long.valueOf(0), executor.queryForSingleValue(long.class, "SELECT outer FROM t").get());
        assertNull(executor.queryForSingleValue(Long.class, "SELECT outer FROM t").get());

        // a schema-less STRUCT with a nested STRUCT, flattened to Object[]
        final FieldList homeField = FieldList
                .of(fixBq2Struct("home", FieldList.of(Field.of("city", StandardSQLTypeName.STRING), fixBq2Struct("loc", fixBqLocFields()))));
        fixBqStubResult(homeField,
                FieldValueList.of(Arrays.asList(fixBqRecord(verifyCell("Home"), fixBqRecord(verifyCell("1.0"), verifyCell("2.0")))), homeField));
        final String flattened = mxDescribe(new Object[] { "Home", new Object[] { "1.0", "2.0" } });

        assertEquals(flattened, mxDescribe(BigQueryExecutor.toList(mockTableResult, Object[].class).get(0)[0]));
        assertEquals(flattened, mxDescribe(executor.queryForSingleValue(Object[].class, "SELECT home FROM t").get()));
        assertEquals(flattened, mxDescribe(BigQueryExecutor.extractData(mockTableResult, null).getColumn("home").get(0)));
    }

    // TIME -> java.sql.Time (milliseconds kept) on the read paths the earlier TIME tests do not reach: the registered N.convert
    // converter of a one-column row, queryForSingleNonNull, stream rows, and a REPEATED TIME read as Time[] by
    // queryForSingleValue. RED on HEAD (the java.sql.Time conversion rejected the fractional seconds).
    @Test
    public void testTimeCellOnConverterNonNullStreamAndArraySingleValuePaths_coverageBQ() throws Exception {
        final long expectedMillis = java.sql.Time.valueOf(java.time.LocalTime.of(16, 13, 20)).getTime() + 123;
        final FieldList fields = FieldList.of(Field.of("open_at", StandardSQLTypeName.TIME));
        final FieldValueList row = FieldValueList.of(Arrays.asList(verifyCell("16:13:20.123456")), fields);

        assertEquals(expectedMillis, N.convert(row, java.sql.Time.class).getTime());

        fixBqStubResult(fields, row);
        assertEquals(expectedMillis, executor.queryForSingleNonNull(java.sql.Time.class, "SELECT open_at FROM t").get().getTime());
        assertEquals(expectedMillis, executor.stream(java.sql.Time.class, "SELECT open_at FROM t").toList().get(0).getTime());

        final FieldList slotsField = FieldList.of(fixBqRepeatedField("slots", StandardSQLTypeName.TIME));
        fixBqStubResult(slotsField, FieldValueList.of(Arrays.asList(fixBqRepeated(verifyCell("16:13:20.123456"), verifyCell("00:00:00"))), slotsField));
        final java.sql.Time[] slots = executor.queryForSingleValue(java.sql.Time[].class, "SELECT slots FROM t").get();
        assertEquals(expectedMillis, slots[0].getTime());
        assertEquals(java.sql.Time.valueOf(java.time.LocalTime.MIDNIGHT).getTime(), slots[1].getTime());
    }

    // A STRUCT read with the sub-schema of its column is checked against it: a record whose value count differs from that
    // sub-schema is rejected with the schema/row count message on every threaded path - a STRUCT bean property, a generic
    // bean property and a REPEATED STRUCT element bean (readBean), an Object[] row and a single-value read - instead of
    // failing with an index error (toEntity) or dropping the extra value (array rows). RED on HEAD ("No schema is attached").
    @Test
    public void testThreadedStructSchemaWidthMismatchReportsFieldCounts_coverageBQ() throws Exception {
        final FieldList fields = FieldList
                .of(fixBq2Struct("home", FieldList.of(Field.of("city", StandardSQLTypeName.STRING), fixBq2Struct("loc", fixBqLocFields()))));
        final FieldValueList row = FieldValueList
                .of(Arrays.asList(fixBqRecord(verifyCell("Home"), fixBqRecord(verifyCell("1.0"), verifyCell("2.0")), verifyCell("extra"))), fields);
        final String twoOfThree = "Schema field count (2) does not match row value count (3)";

        assertEquals(twoOfThree,
                assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.toEntity(fields, row, FixBqHomeHolder.class)).getMessage());
        fixBqStubResult(fields, row);
        assertEquals(twoOfThree, assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.toList(mockTableResult, Object[].class)).getMessage());
        assertEquals(twoOfThree,
                assertThrows(IllegalArgumentException.class, () -> executor.queryForSingleValue(Map.class, "SELECT home FROM t")).getMessage());

        final FieldList sub = fixBq2ValueFields(StandardSQLTypeName.INT64);
        final FieldValue twoValues = fixBqRecord(verifyCell("5"), verifyCell("6"));
        final String oneOfTwo = "Schema field count (1) does not match row value count (2)";
        final FieldList boxField = FieldList.of(fixBq2Struct("box", sub));
        final FieldList boxesField = FieldList.of(verifyRepeatedStruct("boxes", sub));

        assertEquals(oneOfTwo, assertThrows(IllegalArgumentException.class,
                () -> BigQueryExecutor.toEntity(boxField, FieldValueList.of(Arrays.asList(twoValues), boxField), FixBq2BoxHolder.class)).getMessage());
        assertEquals(oneOfTwo, assertThrows(IllegalArgumentException.class,
                () -> BigQueryExecutor.toEntity(boxesField, FieldValueList.of(Arrays.asList(fixBqRepeated(twoValues)), boxesField), FixBq2LongBoxHolder.class))
                        .getMessage());
    }

    @lombok.Data
    public static class MxStructElements {
        private List<List<Long>> longLists;
        private List<List<String>> stringLists;
        private List<List<Object>> objectLists;
        private Set<List<Long>> longListSet;
        private Long[][] longArrays;
        private List<Long[]> longArrayList;
        private List<Object[]> objectArrayList;
        private long[][] primitiveArrays;
        private List<Set<Integer>> integerSets;
        private List<Object> objects;
        private List<Map<String, Object>> maps;
    }

    private interface MxCell {
        FieldValue of(MxKind kind, Field column, boolean sdk);
    }

    // A property kind: the column type (sqlType, or a STRUCT of sub when sub is set; a REPEATED column when repeatedColumn),
    // the cell as each payload shape holds it, the expected Java value, and the Box subclass / record binding its type.
    private static final class MxKind {
        final String name;
        final Class<?> component;
        final StandardSQLTypeName sqlType;
        final FieldList sub;
        final boolean repeatedColumn;
        final java.util.function.Function<Boolean, FieldValue> cell;
        final Object expected;
        final Class<?> boxClass;
        final Class<?> recordClass;

        MxKind(final String name, final Class<?> component, final StandardSQLTypeName sqlType, final FieldList sub, final boolean repeatedColumn,
                final java.util.function.Function<Boolean, FieldValue> cell, final Object expected, final Class<?> boxClass, final Class<?> recordClass) {
            this.name = name;
            this.component = component;
            this.sqlType = sqlType;
            this.sub = sub;
            this.repeatedColumn = repeatedColumn;
            this.cell = cell;
            this.expected = expected;
            this.boxClass = boxClass;
            this.recordClass = recordClass;
        }

        static MxKind scalar(final String name, final Class<?> component, final StandardSQLTypeName sqlType, final String text, final Object expected,
                final Class<?> boxClass, final Class<?> recordClass) {
            return new MxKind(name, component, sqlType, null, false, sdk -> verifyCell(text), expected, boxClass, recordClass);
        }

        Field field(final String fieldName, final boolean repeated) {
            final Field.Builder builder = sub == null ? Field.newBuilder(fieldName, sqlType) : Field.newBuilder(fieldName, StandardSQLTypeName.STRUCT, sub);

            return builder.setMode(repeated || repeatedColumn ? Field.Mode.REPEATED : Field.Mode.NULLABLE).build();
        }
    }

    // A position: the target class (a holder bean whose property is named after the column, or the row type itself), the
    // column for a kind, how the kind's cell is wrapped, and how the checked value is reached from the property value.
    private static final class MxPosition {
        static final int SINGLE = 0;
        static final int LIST = 1;
        static final int ARRAY = 2;

        final String name;
        final java.util.function.Function<MxKind, Class<?>> target;
        final boolean row;
        final java.util.function.Predicate<MxKind> applies;
        final java.util.function.Function<MxKind, Field> column;
        final MxCell cell;
        final java.util.function.BiFunction<MxKind, Object, Object> unwrap;
        final int expectedShape;

        MxPosition(final String name, final java.util.function.Function<MxKind, Class<?>> target, final boolean row,
                final java.util.function.Predicate<MxKind> applies, final java.util.function.Function<MxKind, Field> column, final MxCell cell,
                final java.util.function.BiFunction<MxKind, Object, Object> unwrap, final int expectedShape) {
            this.name = name;
            this.target = target;
            this.row = row;
            this.applies = applies;
            this.column = column;
            this.cell = cell;
            this.unwrap = unwrap;
            this.expectedShape = expectedShape;
        }

        // Two elements in every REPEATED position.
        Object expected(final MxKind kind) {
            if (expectedShape == SINGLE) {
                return kind.expected;
            } else if (expectedShape == LIST) {
                return Arrays.asList(kind.expected, kind.expected);
            }

            final Object array = java.lang.reflect.Array.newInstance(kind.component, 2);
            java.lang.reflect.Array.set(array, 0, kind.expected);
            java.lang.reflect.Array.set(array, 1, kind.expected);
            return array;
        }

        Object fromResult(final MxKind kind, final Field column, final Object result) {
            return unwrap.apply(kind, row ? result : mxField(result, column.getName()));
        }

        // A Dataset column holds the property value (for a row position, the value itself).
        Object fromColumn(final MxKind kind, final Object columnValue) {
            return row ? columnValue : unwrap.apply(kind, columnValue);
        }
    }

    private static Field mxStruct(final String name, final Field subField) {
        return Field.newBuilder(name, StandardSQLTypeName.STRUCT, subField).build();
    }

    private static Field mxRepeatedStruct(final String name, final Field subField) {
        return Field.newBuilder(name, StandardSQLTypeName.STRUCT, subField).setMode(Field.Mode.REPEATED).build();
    }

    private static Object mxField(final Object bean, final String name) {
        if (bean == null) {
            throw new IllegalStateException("null instead of an object holding '" + name + "'");
        }

        for (Class<?> cls = bean.getClass(); cls != null; cls = cls.getSuperclass()) {
            try {
                final java.lang.reflect.Field field = cls.getDeclaredField(name);
                field.setAccessible(true);
                return field.get(bean);
            } catch (final NoSuchFieldException e) {
                // declared in a superclass
            } catch (final IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        }

        throw new IllegalStateException(bean.getClass().getName() + " (" + bean + ") has no field '" + name + "'");
    }

    private static List<Object> mxEach(final Object list, final java.util.function.Function<Object, Object> mapper) {
        if (!(list instanceof List)) {
            throw new IllegalStateException("not a List: " + (list == null ? null : list.getClass().getName()));
        }

        final List<Object> result = new ArrayList<>();

        for (final Object element : (List<?>) list) {
            result.add(mapper.apply(element));
        }

        return result;
    }

    // The exact type and value of a cell: "Long:12", "byte[]:[1, 2, 3]", "Map{String:x=Long:1, ...}", "List[...]",
    // "Instant[][...]", and a bean of this test class as its fields.
    private static String mxDescribe(final Object value) {
        if (value == null) {
            return "null";
        }

        final Class<?> cls = value.getClass();

        if (value instanceof final byte[] bytes) {
            return "byte[]:" + Arrays.toString(bytes);
        } else if (value instanceof final java.nio.ByteBuffer buffer) {
            final byte[] bytes = new byte[buffer.remaining()];
            buffer.duplicate().get(bytes);
            return "ByteBuffer:" + Arrays.toString(bytes);
        } else if (value instanceof final Map<?, ?> map) {
            final java.util.TreeMap<String, String> entries = new java.util.TreeMap<>();
            map.forEach((k, v) -> entries.put(mxDescribe(k), mxDescribe(v)));
            return "Map" + entries;
        } else if (value instanceof final List<?> list) {
            final List<String> elements = new ArrayList<>();
            list.forEach(e -> elements.add(mxDescribe(e)));
            return "List" + elements;
        } else if (cls.isArray()) {
            final List<String> elements = new ArrayList<>();

            for (int i = 0, len = java.lang.reflect.Array.getLength(value); i < len; i++) {
                elements.add(mxDescribe(java.lang.reflect.Array.get(value, i)));
            }

            return cls.getComponentType().getSimpleName() + "[]" + elements;
        } else if (value instanceof final java.util.Date date) {
            return cls.getSimpleName() + ":" + date.getTime();
        } else if ((cls.getEnclosingClass() == BigQueryMappingMatrixTest.class || cls.getEnclosingClass() == BigQueryExecutorTestSupport.class)
                && !cls.isEnum()) {
            final java.util.TreeMap<String, String> fields = new java.util.TreeMap<>();

            for (Class<?> c = cls; c != Object.class; c = c.getSuperclass()) {
                for (final java.lang.reflect.Field field : c.getDeclaredFields()) {
                    if (!java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                        fields.put(field.getName(), mxDescribe(mxField(value, field.getName())));
                    }
                }
            }

            return cls.getSimpleName() + fields;
        }

        return cls.getSimpleName() + ":" + value;
    }

    @lombok.Data
    public static class MxBean {
        private String name;
        private long num;
        private java.time.Instant at;
        private byte[] blob;
        private FixBqLoc loc;
    }

    public static class MxLongBox extends FixBq2Box<Long> {
    }

    public static class MxDecimalBox extends FixBq2Box<BigDecimal> {
    }

    public static class MxStringBox extends FixBq2Box<String> {
    }

    public static class MxBuilderBox extends FixBq2Box<StringBuilder> {
    }

    public static class MxColorBox extends FixBq2Box<FixBq2Color> {
    }

    public static class MxBytesBox extends FixBq2Box<byte[]> {
    }

    public static class MxBufferBox extends FixBq2Box<java.nio.ByteBuffer> {
    }

    public static class MxInstantBox extends FixBq2Box<java.time.Instant> {
    }

    public static class MxDateBox extends FixBq2Box<java.util.Date> {
    }

    public static class MxLdtBox extends FixBq2Box<java.time.LocalDateTime> {
    }

    public static class MxTimeBox extends FixBq2Box<java.sql.Time> {
    }

    public static class MxLocalTimeBox extends FixBq2Box<java.time.LocalTime> {
    }

    public static class MxLocalDateBox extends FixBq2Box<java.time.LocalDate> {
    }

    public static class MxBeanBox extends FixBq2Box<MxBean> {
    }

    public static class MxMapBox extends FixBq2Box<Map<String, Long>> {
    }

    public static class MxListBox extends FixBq2Box<List<Long>> {
    }

    public record MxLongRec(long v) {
    }

    public record MxLongWRec(Long v) {
    }

    public record MxDecimalRec(BigDecimal v) {
    }

    public record MxStringRec(String v) {
    }

    public record MxBuilderRec(StringBuilder v) {
    }

    public record MxColorRec(FixBq2Color v) {
    }

    public record MxBytesRec(byte[] v) {
    }

    public record MxBufferRec(java.nio.ByteBuffer v) {
    }

    public record MxInstantRec(java.time.Instant v) {
    }

    public record MxDateRec(java.util.Date v) {
    }

    public record MxLdtRec(java.time.LocalDateTime v) {
    }

    public record MxTimeRec(java.sql.Time v) {
    }

    public record MxLocalTimeRec(java.time.LocalTime v) {
    }

    public record MxLocalDateRec(java.time.LocalDate v) {
    }

    public record MxBeanRec(MxBean v) {
    }

    public record MxMapRec(Map<String, Long> v) {
    }

    public record MxListRec(List<Long> v) {
    }

    @lombok.Data
    public static class MxPlain {
        private long lng;
        private Long lngW;
        private BigDecimal dec;
        private String str;
        private StringBuilder sb;
        private FixBq2Color enm;
        private byte[] bytes;
        private java.nio.ByteBuffer buf;
        private java.time.Instant instant;
        private java.util.Date date;
        private java.time.LocalDateTime ldt;
        private java.sql.Time time;
        private java.time.LocalTime ltime;
        private java.time.LocalDate ldate;
        private MxBean bean;
        private Map<String, Long> map;
        private List<Long> pair;
        private List<Long> codes;
    }

    @lombok.Data
    public static class MxBoxes {
        private FixBq2Box<Long> lngW;
        private FixBq2Box<BigDecimal> dec;
        private FixBq2Box<String> str;
        private FixBq2Box<StringBuilder> sb;
        private FixBq2Box<FixBq2Color> enm;
        private FixBq2Box<byte[]> bytes;
        private FixBq2Box<java.nio.ByteBuffer> buf;
        private FixBq2Box<java.time.Instant> instant;
        private FixBq2Box<java.util.Date> date;
        private FixBq2Box<java.time.LocalDateTime> ldt;
        private FixBq2Box<java.sql.Time> time;
        private FixBq2Box<java.time.LocalTime> ltime;
        private FixBq2Box<java.time.LocalDate> ldate;
        private FixBq2Box<MxBean> bean;
        private FixBq2Box<Map<String, Long>> map;
        private FixBq2Box<List<Long>> pair;
        private FixBq2Box<List<Long>> codes;
    }

    @lombok.Data
    public static class MxSubs {
        private MxLongBox lngW;
        private MxDecimalBox dec;
        private MxStringBox str;
        private MxBuilderBox sb;
        private MxColorBox enm;
        private MxBytesBox bytes;
        private MxBufferBox buf;
        private MxInstantBox instant;
        private MxDateBox date;
        private MxLdtBox ldt;
        private MxTimeBox time;
        private MxLocalTimeBox ltime;
        private MxLocalDateBox ldate;
        private MxBeanBox bean;
        private MxMapBox map;
        private MxListBox pair;
        private MxListBox codes;
    }

    @lombok.Data
    public static class MxLists {
        private List<Long> lngW;
        private List<BigDecimal> dec;
        private List<String> str;
        private List<StringBuilder> sb;
        private List<FixBq2Color> enm;
        private List<byte[]> bytes;
        private List<java.nio.ByteBuffer> buf;
        private List<java.time.Instant> instant;
        private List<java.util.Date> date;
        private List<java.time.LocalDateTime> ldt;
        private List<java.sql.Time> time;
        private List<java.time.LocalTime> ltime;
        private List<java.time.LocalDate> ldate;
        private List<MxBean> bean;
        private List<Map<String, Long>> map;
        private List<List<Long>> pair;
    }

    @lombok.Data
    public static class MxArrays {
        private long[] lng;
        private Long[] lngW;
        private BigDecimal[] dec;
        private String[] str;
        private StringBuilder[] sb;
        private FixBq2Color[] enm;
        private byte[][] bytes;
        private java.nio.ByteBuffer[] buf;
        private java.time.Instant[] instant;
        private java.util.Date[] date;
        private java.time.LocalDateTime[] ldt;
        private java.sql.Time[] time;
        private java.time.LocalTime[] ltime;
        private java.time.LocalDate[] ldate;
        private MxBean[] bean;
        private Map<String, Long>[] map;
        private List<Long>[] pair;
    }

    @lombok.Data
    public static class MxItems {
        private List<MxPlain> items;
    }

    @lombok.Data
    public static class MxBoxLists {
        private List<FixBq2Box<Long>> lngW;
        private List<FixBq2Box<BigDecimal>> dec;
        private List<FixBq2Box<String>> str;
        private List<FixBq2Box<StringBuilder>> sb;
        private List<FixBq2Box<FixBq2Color>> enm;
        private List<FixBq2Box<byte[]>> bytes;
        private List<FixBq2Box<java.nio.ByteBuffer>> buf;
        private List<FixBq2Box<java.time.Instant>> instant;
        private List<FixBq2Box<java.util.Date>> date;
        private List<FixBq2Box<java.time.LocalDateTime>> ldt;
        private List<FixBq2Box<java.sql.Time>> time;
        private List<FixBq2Box<java.time.LocalTime>> ltime;
        private List<FixBq2Box<java.time.LocalDate>> ldate;
        private List<FixBq2Box<MxBean>> bean;
        private List<FixBq2Box<Map<String, Long>>> map;
        private List<FixBq2Box<List<Long>>> pair;
        private List<FixBq2Box<List<Long>>> codes;
    }

    @lombok.Data
    public static class MxSubLists {
        private List<MxLongBox> lngW;
        private List<MxDecimalBox> dec;
        private List<MxStringBox> str;
        private List<MxBuilderBox> sb;
        private List<MxColorBox> enm;
        private List<MxBytesBox> bytes;
        private List<MxBufferBox> buf;
        private List<MxInstantBox> instant;
        private List<MxDateBox> date;
        private List<MxLdtBox> ldt;
        private List<MxTimeBox> time;
        private List<MxLocalTimeBox> ltime;
        private List<MxLocalDateBox> ldate;
        private List<MxBeanBox> bean;
        private List<MxMapBox> map;
        private List<MxListBox> pair;
        private List<MxListBox> codes;
    }

    @lombok.Data
    public static class MxRecs {
        private MxLongRec lng;
        private MxLongWRec lngW;
        private MxDecimalRec dec;
        private MxStringRec str;
        private MxBuilderRec sb;
        private MxColorRec enm;
        private MxBytesRec bytes;
        private MxBufferRec buf;
        private MxInstantRec instant;
        private MxDateRec date;
        private MxLdtRec ldt;
        private MxTimeRec time;
        private MxLocalTimeRec ltime;
        private MxLocalDateRec ldate;
        private MxBeanRec bean;
        private MxMapRec map;
        private MxListRec pair;
        private MxListRec codes;
    }

    @lombok.Data
    public static class MxRecLists {
        private List<MxLongRec> lng;
        private List<MxLongWRec> lngW;
        private List<MxDecimalRec> dec;
        private List<MxStringRec> str;
        private List<MxBuilderRec> sb;
        private List<MxColorRec> enm;
        private List<MxBytesRec> bytes;
        private List<MxBufferRec> buf;
        private List<MxInstantRec> instant;
        private List<MxDateRec> date;
        private List<MxLdtRec> ldt;
        private List<MxTimeRec> time;
        private List<MxLocalTimeRec> ltime;
        private List<MxLocalDateRec> ldate;
        private List<MxBeanRec> bean;
        private List<MxMapRec> map;
        private List<MxListRec> pair;
        private List<MxListRec> codes;
    }
}
