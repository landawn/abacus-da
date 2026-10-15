package com.landawn.abacus.da.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.N;

public class BigQueryGenericMappingTest extends BigQueryExecutorTestSupport {

    // A generic element bean of a REPEATED STRUCT (List<GenericValue<Long>>) gets its type argument: the JSON codec
    // resolved it before; mapping the element through toEntity by raw class left the value as the cell text "9", so the
    // element is mapped with its type arguments (fixBQ2). Matches HEAD.
    @Test
    public void testRepeatedStructGenericElementBeanResolvesTypeArgument() {
        final FieldList sub = FieldList.of(Field.of("value", StandardSQLTypeName.INT64));
        final FieldValue repeated = FieldValue.of(FieldValue.Attribute.REPEATED,
                Arrays.asList(FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(Arrays.asList(verifyCell("9")), sub))));
        final FieldList fields = FieldList.of(verifyRepeatedStruct("values", sub), verifyRepeatedStruct("value_array", sub));

        final GenericValueHolder holder = BigQueryExecutor.toEntity(fields, FieldValueList.of(Arrays.asList(repeated, repeated), fields),
                GenericValueHolder.class);

        final Object listValue = holder.getValues().get(0).getValue();
        final Object arrayValue = holder.getValueArray()[0].getValue();
        assertEquals(Long.valueOf(9), listValue);
        assertEquals(Long.valueOf(9), arrayValue);
    }

    // FixBq2LongBox elements of REPEATED STRUCT columns (List, array, Set), as the client decodes them (schema-less records
    // in a schema-less REPEATED value) and as a plain List of records: toEntity, the registered converter, list rows and the
    // bean-class Dataset. RED before the fix (value "5"); HEAD: the plain-List shape matched (JSON codec), the client shape threw.
    @Test
    public void testLongBoxRepeatedStructElementsConvertTypeVariableProperty_fixBQ2() throws Exception {
        final FieldList sub = fixBq2ValueFields(StandardSQLTypeName.INT64);
        final FieldList fields = FieldList.of(verifyRepeatedStruct("boxes", sub), verifyRepeatedStruct("box_array", sub), verifyRepeatedStruct("box_set", sub));
        final java.util.function.Consumer<FixBq2LongBoxHolder> check = holder -> {
            assertEquals(2, holder.getBoxes().size());
            assertEquals(5L, fixBq2ValueOf(holder.getBoxes().get(0)));
            assertNull(fixBq2ValueOf(holder.getBoxes().get(1)));
            assertEquals(2, holder.getBoxArray().length);
            assertEquals(5L, fixBq2ValueOf(holder.getBoxArray()[0]));
            final Set<Object> setValues = new java.util.HashSet<>();
            for (final FixBq2LongBox box : holder.getBoxSet()) {
                setValues.add(fixBq2ValueOf(box));
            }
            assertEquals(new java.util.HashSet<>(Arrays.asList(5L, null)), setValues);
        };

        final FieldValue sdkShape = fixBqRepeated(fixBqRecord(verifyCell("5")), fixBqRecord(verifyCell(null)));
        final FieldValue plainShape = FieldValue.of(FieldValue.Attribute.REPEATED,
                Arrays.asList(FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(Arrays.asList(verifyCell("5")), sub)),
                        FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(Arrays.asList(verifyCell(null)), sub))));

        for (final FieldValue repeated : Arrays.asList(sdkShape, plainShape)) {
            final FieldValueList row = FieldValueList.of(Arrays.asList(repeated, repeated, repeated), fields);

            check.accept(BigQueryExecutor.toEntity(fields, row, FixBq2LongBoxHolder.class));
            check.accept(N.convert(row, FixBq2LongBoxHolder.class));

            fixBqStubResult(fields, row);
            check.accept(BigQueryExecutor.toList(mockTableResult, FixBq2LongBoxHolder.class).get(0));

            final Dataset ds = BigQueryExecutor.extractData(mockTableResult, FixBq2LongBoxHolder.class);
            assertEquals(5L, fixBq2ValueOf((FixBq2Box<?>) ((List<?>) ds.getColumn("boxes").get(0)).get(0)));
            assertEquals(5L, fixBq2ValueOf(((FixBq2LongBox[]) ds.getColumn("box_array").get(0))[0]));
        }
    }

    // FixBq2LongBox as a row type (toEntity with and without a schema argument, the registered converter, list and stream
    // rows, queryForSingleValue of a STRUCT column), as a STRUCT property, and as a property of a REPEATED STRUCT element
    // bean (client shape), plus the bean-class Dataset. RED before the fix and on HEAD (value "5").
    @Test
    public void testLongBoxRowsAndStructPropertiesConvertTypeVariableProperty_fixBQ2() throws Exception {
        final FieldList sub = fixBq2ValueFields(StandardSQLTypeName.INT64);
        final FieldValueList row = FieldValueList.of(Arrays.asList(verifyCell("5")), sub);

        assertEquals(5L, fixBq2ValueOf(BigQueryExecutor.toEntity(sub, row, FixBq2LongBox.class)));
        assertEquals(5L, fixBq2ValueOf(BigQueryExecutor.toEntity(row, FixBq2LongBox.class)));
        assertEquals(5L, fixBq2ValueOf(N.convert(row, FixBq2LongBox.class)));

        fixBqStubResult(sub, row);
        assertEquals(5L, fixBq2ValueOf(BigQueryExecutor.toList(mockTableResult, FixBq2LongBox.class).get(0)));
        assertEquals(5L, fixBq2ValueOf(executor.stream(FixBq2LongBox.class, "SELECT value FROM t").toList().get(0)));
        assertEquals(5L, BigQueryExecutor.extractData(mockTableResult, FixBq2LongBox.class).getColumn("value").get(0));

        final FieldList structColumn = FieldList.of(fixBq2Struct("long_box", sub));
        fixBqStubResult(structColumn, FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.RECORD, row)), structColumn));
        assertEquals(5L, fixBq2ValueOf(executor.queryForSingleValue(FixBq2LongBox.class, "SELECT long_box FROM t").get()));

        final FieldList itemFields = FieldList.of(Field.of("name", StandardSQLTypeName.STRING), fixBq2Struct("long_box", sub));
        final FieldList fields = FieldList.of(fixBq2Struct("long_box", sub), verifyRepeatedStruct("items", itemFields));
        final FieldValueList holderRow = FieldValueList
                .of(Arrays.asList(fixBqRecord(verifyCell("6")), fixBqRepeated(fixBqRecord(verifyCell("i1"), fixBqRecord(verifyCell("7"))))), fields);

        final FixBq2BoxHolder holder = BigQueryExecutor.toEntity(fields, holderRow, FixBq2BoxHolder.class);
        assertEquals(6L, fixBq2ValueOf(holder.getLongBox()));
        assertEquals("i1", holder.getItems().get(0).getName());
        assertEquals(7L, fixBq2ValueOf(holder.getItems().get(0).getLongBox()));

        fixBqStubResult(fields, holderRow);
        final Dataset ds = BigQueryExecutor.extractData(mockTableResult, FixBq2BoxHolder.class);
        assertEquals(6L, fixBq2ValueOf((FixBq2Box<?>) ds.getColumn("long_box").get(0)));
        assertEquals(7L, fixBq2ValueOf(((FixBq2Item) ((List<?>) ds.getColumn("items").get(0)).get(0)).getLongBox()));
    }

    // A FixBq2Box<Long> / FixBq2Box<Instant> STRUCT property - at top level, in a STRUCT bean and in the (non-generic) element
    // bean of a REPEATED STRUCT column (client shape) - is mapped with its type arguments: INT64 converted, TIMESTAMP decoded.
    // RED before the fix (raw-class mapping kept the cell text) and on HEAD.
    @Test
    public void testParameterizedBeanStructPropertiesMappedWithTypeArguments_fixBQ2() throws Exception {
        final java.time.Instant seenAt = java.time.Instant.ofEpochSecond(1_718_900_000L, 123_456_000L);
        final FieldList itemFields = FieldList.of(Field.of("name", StandardSQLTypeName.STRING),
                fixBq2Struct("box", fixBq2ValueFields(StandardSQLTypeName.INT64)), fixBq2Struct("seen", fixBq2ValueFields(StandardSQLTypeName.TIMESTAMP)));
        final FieldList fields = FieldList.of(fixBq2Struct("box", fixBq2ValueFields(StandardSQLTypeName.INT64)), fixBq2Struct("item", itemFields),
                verifyRepeatedStruct("items", itemFields));
        final FieldValue item = fixBqRecord(verifyCell("i1"), fixBqRecord(verifyCell("8")), fixBqRecord(verifyCell("1718900000.123456")));
        final FieldValueList row = FieldValueList.of(Arrays.asList(fixBqRecord(verifyCell("5")), item, fixBqRepeated(item, item)), fields);

        final java.util.function.Consumer<FixBq2Item> checkItem = it -> {
            assertEquals("i1", it.getName());
            assertEquals(8L, fixBq2ValueOf(it.getBox()));
            assertEquals(seenAt, fixBq2ValueOf(it.getSeen()));
        };
        final java.util.function.Consumer<FixBq2BoxHolder> check = holder -> {
            assertEquals(5L, fixBq2ValueOf(holder.getBox()));
            checkItem.accept(holder.getItem());
            assertEquals(2, holder.getItems().size());
            checkItem.accept(holder.getItems().get(1));
        };

        check.accept(BigQueryExecutor.toEntity(fields, row, FixBq2BoxHolder.class));
        check.accept(N.convert(row, FixBq2BoxHolder.class));

        fixBqStubResult(fields, row);
        check.accept(BigQueryExecutor.toList(mockTableResult, FixBq2BoxHolder.class).get(0));

        final Dataset ds = BigQueryExecutor.extractData(mockTableResult, FixBq2BoxHolder.class);
        assertEquals(5L, fixBq2ValueOf((FixBq2Box<?>) ds.getColumn("box").get(0)));
        checkItem.accept((FixBq2Item) ds.getColumn("item").get(0));
        checkItem.accept((FixBq2Item) ((List<?>) ds.getColumn("items").get(0)).get(0));
    }

    // Generic element beans of REPEATED STRUCT columns (client shape) are mapped with their type arguments, so a
    // FixBq2Box<Instant> TIMESTAMP value and a FixBq2Box<byte[]> BYTES value are decoded and a FixBq2Box<FixBqLoc> value is
    // mapped to the bean - all three failed through the JSON codec before - and FixBq2Box<Long> keeps its Long. RED before the
    // fix and on HEAD.
    @Test
    public void testGenericElementBeansDecodedWithTypeArguments_fixBQ2() throws Exception {
        final java.time.Instant seenAt = java.time.Instant.ofEpochSecond(1_718_900_000L, 123_456_000L);
        final FieldList longSub = fixBq2ValueFields(StandardSQLTypeName.INT64);
        final FieldList fields = FieldList.of(verifyRepeatedStruct("longs", longSub), verifyRepeatedStruct("long_array", longSub),
                verifyRepeatedStruct("instants", fixBq2ValueFields(StandardSQLTypeName.TIMESTAMP)),
                verifyRepeatedStruct("blobs", fixBq2ValueFields(StandardSQLTypeName.BYTES)),
                verifyRepeatedStruct("locs", FieldList.of(fixBq2Struct("value", fixBqLocFields()))));
        final FieldValue longs = fixBqRepeated(fixBqRecord(verifyCell("5")), fixBqRecord(verifyCell("6")));
        final FieldValueList row = FieldValueList.of(Arrays.asList(longs, longs, fixBqRepeated(fixBqRecord(verifyCell("1718900000.123456"))),
                fixBqRepeated(fixBqRecord(verifyCell("AQID"))), fixBqRepeated(fixBqRecord(fixBqRecord(verifyCell("37.5"), verifyCell("-122.25"))))), fields);

        final java.util.function.Consumer<FixBq2GenericElementHolder> check = holder -> {
            assertEquals(6L, fixBq2ValueOf(holder.getLongs().get(1)));
            assertEquals(5L, fixBq2ValueOf(holder.getLongArray()[0]));
            assertEquals(1, holder.getInstants().size());
            assertEquals(seenAt, fixBq2ValueOf(holder.getInstants().iterator().next()));
            assertTrue(Arrays.equals(new byte[] { 1, 2, 3 }, (byte[]) fixBq2ValueOf(holder.getBlobs().get(0))));
            final Object loc = fixBq2ValueOf(holder.getLocs().get(0));
            assertTrue(loc instanceof FixBqLoc, String.valueOf(loc));
            assertEquals(-122.25, ((FixBqLoc) loc).getLng());
        };

        check.accept(BigQueryExecutor.toEntity(fields, row, FixBq2GenericElementHolder.class));
        check.accept(N.convert(row, FixBq2GenericElementHolder.class));

        fixBqStubResult(fields, row);
        check.accept(BigQueryExecutor.toList(mockTableResult, FixBq2GenericElementHolder.class).get(0));

        final Dataset ds = BigQueryExecutor.extractData(mockTableResult, FixBq2GenericElementHolder.class);
        assertEquals(seenAt, fixBq2ValueOf((FixBq2Box<?>) ((Collection<?>) ds.getColumn("instants").get(0)).iterator().next()));
        assertEquals(5L, fixBq2ValueOf(((FixBq2Box<?>[]) ds.getColumn("long_array").get(0))[0]));
    }

    // A generic bean whose properties use its type variable in a container (List<T>, Map<String, T>) or alone (T), and a
    // nested generic bean (FixBq2Box<FixBq2Box<Long>>), as STRUCT properties (RED before the fix and on HEAD: raw Strings /
    // a String instead of the inner box) and as REPEATED STRUCT elements (client shape; typed through the JSON codec before,
    // now through the type arguments).
    @Test
    public void testGenericBeanContainerAndNestedGenericProperties_fixBQ2() throws Exception {
        final FieldList seriesFields = FieldList.of(fixBqRepeatedField("items", StandardSQLTypeName.INT64),
                fixBq2Struct("by_key", FieldList.of(Field.of("x", StandardSQLTypeName.INT64))), Field.of("first", StandardSQLTypeName.INT64));
        final FieldList nestedFields = FieldList.of(fixBq2Struct("value", fixBq2ValueFields(StandardSQLTypeName.INT64)));
        final FieldList fields = FieldList.of(fixBq2Struct("series", seriesFields), verifyRepeatedStruct("series_list", seriesFields),
                fixBq2Struct("nested", nestedFields), verifyRepeatedStruct("nested_list", nestedFields));
        final FieldValue series = fixBqRecord(fixBqRepeated(verifyCell("1"), verifyCell("2")), fixBqRecord(verifyCell("11")), verifyCell("4"));
        final FieldValue nested = fixBqRecord(fixBqRecord(verifyCell("7")));
        final FieldValueList row = FieldValueList.of(Arrays.asList(series, fixBqRepeated(series), nested, fixBqRepeated(nested)), fields);

        final java.util.function.Consumer<FixBq2Series<?>> checkSeries = s -> {
            assertEquals(Arrays.asList(1L, 2L), new ArrayList<>(s.getItems()));
            final Object byKey = ((Map<?, ?>) s.getByKey()).get("x");
            assertEquals(11L, byKey);
            final Object first = s.getFirst();
            assertEquals(4L, first);
        };
        final java.util.function.Consumer<FixBq2Box<?>> checkNested = box -> {
            final Object inner = fixBq2ValueOf(box);
            assertTrue(inner instanceof FixBq2Box, String.valueOf(inner));
            assertEquals(7L, fixBq2ValueOf((FixBq2Box<?>) inner));
        };
        final java.util.function.Consumer<FixBq2BoxHolder> check = holder -> {
            checkSeries.accept(holder.getSeries());
            checkSeries.accept(holder.getSeriesList().get(0));
            checkNested.accept(holder.getNested());
            checkNested.accept(holder.getNestedList().get(0));
        };

        check.accept(BigQueryExecutor.toEntity(fields, row, FixBq2BoxHolder.class));
        check.accept(N.convert(row, FixBq2BoxHolder.class));

        fixBqStubResult(fields, row);
        check.accept(BigQueryExecutor.toList(mockTableResult, FixBq2BoxHolder.class).get(0));

        final Dataset ds = BigQueryExecutor.extractData(mockTableResult, FixBq2BoxHolder.class);
        checkSeries.accept((FixBq2Series<?>) ds.getColumn("series").get(0));
        checkNested.accept((FixBq2Box<?>) ds.getColumn("nested").get(0));
    }

    // Pin: a raw FixBq2Box, FixBq2Box<?>, FixBq2Box<Object> or FixBq2Box<String> - as a STRUCT property, a REPEATED STRUCT
    // element or a row - binds T to Object (or String), so the value keeps the cell text as before. GREEN before the fix and
    // on HEAD.
    @Test
    @SuppressWarnings("rawtypes")
    public void testRawAndObjectBoundGenericBeansKeepCellText_fixBQ2() throws Exception {
        final FieldList sub = fixBq2ValueFields(StandardSQLTypeName.INT64);
        final FieldList fields = FieldList.of(fixBq2Struct("raw", sub), fixBq2Struct("wild", sub), fixBq2Struct("object", sub), fixBq2Struct("text", sub),
                verifyRepeatedStruct("raws", sub), verifyRepeatedStruct("wilds", sub), verifyRepeatedStruct("objects", sub));
        final FieldValue struct = FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(Arrays.asList(verifyCell("5")), sub));
        final FieldValue repeated = FieldValue.of(FieldValue.Attribute.REPEATED, Arrays.asList(struct));
        final FieldValueList row = FieldValueList.of(Arrays.asList(struct, struct, struct, struct, repeated, repeated, repeated), fields);

        final FixBq2RawHolder holder = BigQueryExecutor.toEntity(fields, row, FixBq2RawHolder.class);
        assertEquals("5", fixBq2ValueOf(holder.getRaw()));
        assertEquals("5", fixBq2ValueOf(holder.getWild()));
        assertEquals("5", fixBq2ValueOf(holder.getObject()));
        assertEquals("5", fixBq2ValueOf(holder.getText()));
        assertEquals("5", fixBq2ValueOf(holder.getRaws().get(0)));
        assertEquals("5", fixBq2ValueOf(holder.getWilds().get(0)));
        assertEquals("5", fixBq2ValueOf(holder.getObjects().get(0)));
        assertEquals("5", fixBq2ValueOf(BigQueryExecutor.toEntity(sub, (FieldValueList) struct.getValue(), FixBq2Box.class)));
    }

    // Pin: ordinary typed properties read as before - converted (now up front instead of after a failed assignment, also past
    // the 100 failures after which PropInfo.setPropValue converts first), NULL cells giving null or the primitive default, an
    // Object property keeping the cell text, and a REPEATED column into a String property (plain or isJsonRawValue) giving
    // the same JSON text. GREEN before the fix and on HEAD.
    @Test
    public void testOrdinaryTypedPropertiesUnchanged_fixBQ2() {
        final FieldList fields = FieldList.of(Field.of("id", StandardSQLTypeName.INT64), Field.of("count", StandardSQLTypeName.INT64),
                Field.of("active", StandardSQLTypeName.BOOL), Field.of("score", StandardSQLTypeName.FLOAT64), Field.of("amount", StandardSQLTypeName.NUMERIC),
                Field.of("day", StandardSQLTypeName.DATE), Field.of("seen_at", StandardSQLTypeName.TIMESTAMP), Field.of("color", StandardSQLTypeName.STRING),
                Field.of("anything", StandardSQLTypeName.INT64), fixBqRepeatedField("tags_text", StandardSQLTypeName.STRING),
                fixBqRepeatedField("tags_json", StandardSQLTypeName.STRING), Field.of("initial", StandardSQLTypeName.STRING));
        final FieldValue tags = FieldValue.of(FieldValue.Attribute.REPEATED, Arrays.asList(verifyCell("a"), verifyCell("b")));
        final FieldValueList row = FieldValueList.of(Arrays.asList(verifyCell("7"), verifyCell("3"), verifyCell("true"), verifyCell("1.5"), verifyCell("12.34"),
                verifyCell("2024-01-02"), verifyCell("1718900000.123456"), verifyCell("GREEN"), verifyCell("9"), tags, tags, verifyCell("Q")), fields);

        for (int i = 0; i < 150; i++) {
            final FixBq2Typed typed = BigQueryExecutor.toEntity(fields, row, FixBq2Typed.class);
            assertEquals(7L, typed.getId());
            assertEquals(Integer.valueOf(3), typed.getCount());
            assertTrue(typed.isActive());
            assertEquals(1.5, typed.getScore());
            assertEquals(new BigDecimal("12.34"), typed.getAmount());
            assertEquals(java.time.LocalDate.of(2024, 1, 2), typed.getDay());
            assertEquals(1_718_900_000_123L, typed.getSeenAt().getTime());
            assertEquals(FixBq2Color.GREEN, typed.getColor());
            assertEquals("9", typed.getAnything());
            assertEquals("[\"a\", \"b\"]", typed.getTagsText());
            assertEquals("[\"a\", \"b\"]", typed.getTagsJson());
            assertEquals('Q', typed.getInitial());
        }

        final FieldValueList nulls = FieldValueList.of(Arrays.asList(verifyCell(null), verifyCell(null), verifyCell(null), verifyCell(null), verifyCell(null),
                verifyCell(null), verifyCell(null), verifyCell(null), verifyCell(null), tags, tags, verifyCell(null)), fields);
        final FixBq2Typed typed = BigQueryExecutor.toEntity(fields, nulls, FixBq2Typed.class);
        assertEquals(0L, typed.getId());
        assertNull(typed.getCount());
        assertFalse(typed.isActive());
        assertEquals(0.0, typed.getScore());
        assertNull(typed.getAmount());
        assertNull(typed.getSeenAt());
        assertNull(typed.getColor());
        assertNull(typed.getAnything());
        assertEquals('\0', typed.getInitial());
    }

    // ---- 2026-10-04 main (type variables bound to text types) ----

    // The up-front conversion of fixBQ2 skipped every text-typed property (to keep setPropValue's isJsonRawValue handling),
    // but a type variable bound to a text type has an erased Object field that takes any value as is: a
    // List<FixBq2Box<StringBuilder>> element held the cell String and a List<FixBq2Box<String>> element whose value came
    // from a REPEATED STRING sub-field held the List - both threw ClassCastException on read (the JSON codec used before
    // typed them). A subclass binding (FixBq3SbBox extends FixBq2Box<StringBuilder>) and a FixBq2Box<StringBuilder> STRUCT
    // property had the same erased field. Text properties declared as text keep going through setPropValue (pin in
    // testOrdinaryTypedPropertiesUnchanged_fixBQ2).
    @Test
    public void testTypeVariablesBoundToTextTypesAreConverted() throws Exception {
        final FieldList valueSub = fixBq2ValueFields(StandardSQLTypeName.STRING);
        final FieldList fields = FieldList.of(verifyRepeatedStruct("builders", valueSub),
                verifyRepeatedStruct("texts", FieldList.of(fixBqRepeatedField("value", StandardSQLTypeName.STRING))),
                verifyRepeatedStruct("sb_boxes", valueSub), fixBq2Struct("builder", valueSub));
        final FieldValueList row = FieldValueList
                .of(Arrays.asList(fixBqRepeated(fixBqRecord(verifyCell("x"))), fixBqRepeated(fixBqRecord(fixBqRepeated(verifyCell("a"), verifyCell("b")))),
                        fixBqRepeated(fixBqRecord(verifyCell("y"))), fixBqRecord(verifyCell("z"))), fields);

        final java.util.function.Consumer<FixBq3TextHolder> check = holder -> {
            final Object builder = fixBq2ValueOf(holder.getBuilders().get(0));
            assertTrue(builder instanceof StringBuilder, String.valueOf(builder == null ? null : builder.getClass()));
            assertEquals("x", builder.toString());

            final Object text = fixBq2ValueOf(holder.getTexts().get(0));
            assertTrue(text instanceof String, String.valueOf(text == null ? null : text.getClass()));
            assertTrue(((String) text).contains("a") && ((String) text).contains("b"), (String) text);

            final Object sbBox = fixBq2ValueOf(holder.getSbBoxes().get(0));
            assertTrue(sbBox instanceof StringBuilder, String.valueOf(sbBox == null ? null : sbBox.getClass()));
            assertEquals("y", sbBox.toString());

            final Object structBuilder = fixBq2ValueOf(holder.getBuilder());
            assertTrue(structBuilder instanceof StringBuilder, String.valueOf(structBuilder == null ? null : structBuilder.getClass()));
            assertEquals("z", structBuilder.toString());
        };

        check.accept(BigQueryExecutor.toEntity(fields, row, FixBq3TextHolder.class));

        fixBqStubResult(fields, row);
        check.accept(BigQueryExecutor.toList(mockTableResult, FixBq3TextHolder.class).get(0));

        // A subclass binding read as the row itself.
        final FieldList boxFields = FieldList.of(Field.of("value", StandardSQLTypeName.STRING));
        final Object rowValue = fixBq2ValueOf(
                BigQueryExecutor.toEntity(boxFields, FieldValueList.of(Arrays.asList(verifyCell("w")), boxFields), FixBq3SbBox.class));
        assertTrue(rowValue instanceof StringBuilder, String.valueOf(rowValue == null ? null : rowValue.getClass()));
        assertEquals("w", rowValue.toString());
    }

    @Test
    public void testStringTypedStructContainersConvertStructuredValues() throws Exception {
        // String is not always the raw cell type: nested REPEATED/STRUCT values must become JSON text before assignment.
        final FieldList childFields = FieldList.of(Field.of("label", StandardSQLTypeName.STRING));
        final FieldList contentFields = FieldList.of(fixBqRepeatedField("tags", StandardSQLTypeName.STRING),
                Field.newBuilder("child", StandardSQLTypeName.STRUCT, childFields).build(), Field.of("text", StandardSQLTypeName.STRING),
                Field.of("missing", StandardSQLTypeName.STRING));
        final FieldList propertyFields = FieldList.of(Field.newBuilder("attributes", StandardSQLTypeName.STRUCT, contentFields).build(),
                Field.newBuilder("values", StandardSQLTypeName.STRUCT, contentFields).build(),
                Field.newBuilder("raw_map", StandardSQLTypeName.STRUCT, contentFields).build(),
                Field.newBuilder("raw_list", StandardSQLTypeName.STRUCT, contentFields).build());
        final FieldList fields = FieldList.of(verifyRepeatedStruct("items", propertyFields),
                Field.newBuilder("single", StandardSQLTypeName.STRUCT, propertyFields).build());

        for (final boolean sdk : new boolean[] { true, false }) {
            final FieldValue content = mxRecord(sdk, contentFields, mxRepeated(sdk, verifyCell("a"), verifyCell("b")),
                    mxRecord(sdk, childFields, verifyCell("nested")), verifyCell("plain"), verifyCell(null));
            final FieldValue item = mxRecord(sdk, propertyFields, content, content, content, content);
            final FieldValueList row = FieldValueList.of(Arrays.asList(mxRepeated(sdk, item), item), fields);
            final java.util.function.Consumer<StringStructPropertyHolder> check = holder -> {
                assertStringStructProperties(holder.getItems().get(0));
                assertStringStructProperties(holder.getSingle());
            };

            check.accept(BigQueryExecutor.toEntity(fields, row, StringStructPropertyHolder.class));
            check.accept(N.convert(row, StringStructPropertyHolder.class));
            fixBqStubResult(fields, row);
            check.accept(BigQueryExecutor.toList(mockTableResult, StringStructPropertyHolder.class).get(0));
            final Dataset ds = BigQueryExecutor.extractData(mockTableResult, StringStructPropertyHolder.class);
            assertStringStructProperties((StringStructProperties) ((List<?>) ds.getColumn("items").get(0)).get(0));
            assertStringStructProperties((StringStructProperties) ds.getColumn("single").get(0));
        }
    }

    @Test
    public void testRepeatedStructBeansHonorPropertyFormats() throws Exception {
        // Direct bean mapping must keep the annotation-aware conversion formerly supplied by the JSON codec.
        final FieldList valueFields = formattedValueFields();
        final FieldList fields = FieldList.of(verifyRepeatedStruct("items", valueFields),
                Field.newBuilder("single", StandardSQLTypeName.STRUCT, valueFields).build());

        for (final boolean sdk : new boolean[] { true, false }) {
            final FieldValue item = mxRecord(sdk, valueFields, verifyCell("03/10/2026"), verifyCell("03/10/2026"), verifyCell("1718900000.123456"),
                    verifyCell("1,234.50"), verifyCell("AQID"));
            final FieldValue empty = mxRecord(sdk, valueFields, verifyCell(null), verifyCell(null), verifyCell(null), verifyCell(null), verifyCell(null));
            final FieldValueList row = FieldValueList.of(Arrays.asList(mxRepeated(sdk, item, empty), item), fields);
            final java.util.function.Consumer<FormattedValueHolder> check = holder -> {
                assertFormattedValue(holder.getItems().get(0));
                assertFormattedValue(holder.getSingle());
                assertNull(holder.getItems().get(1).getDate());
                assertNull(holder.getItems().get(1).getDay());
                assertNull(holder.getItems().get(1).getTimestamp());
                assertNull(holder.getItems().get(1).getAmount());
                assertNull(holder.getItems().get(1).getBytes());
            };

            check.accept(BigQueryExecutor.toEntity(fields, row, FormattedValueHolder.class));
            check.accept(N.convert(row, FormattedValueHolder.class));
            fixBqStubResult(fields, row);
            check.accept(BigQueryExecutor.toList(mockTableResult, FormattedValueHolder.class).get(0));
            final Dataset ds = BigQueryExecutor.extractData(mockTableResult, FormattedValueHolder.class);
            assertFormattedValue((FormattedValue) ((List<?>) ds.getColumn("items").get(0)).get(0));
            assertFormattedValue((FormattedValue) ds.getColumn("single").get(0));

            // Direct bean-class Dataset columns use the same property reader as nested beans.
            final FieldValueList direct = FieldValueList.of(((FieldValueList) item.getValue()), valueFields);
            fixBqStubResult(valueFields, direct);
            final Dataset directData = BigQueryExecutor.extractData(mockTableResult, FormattedValue.class);
            assertEquals(java.util.Date.from(java.time.Instant.parse("2026-10-03T00:00:00Z")), directData.getColumn("date").get(0));
            assertEquals(new BigDecimal("1234.50"), directData.getColumn("amount").get(0));
        }
    }

    @Test
    public void testFormattedDateRejectsInvalidText() throws Exception {
        final FieldList valueFields = FieldList.of(Field.of("date", StandardSQLTypeName.STRING));
        final FieldList fields = FieldList.of(verifyRepeatedStruct("items", valueFields));
        final FieldValueList row = FieldValueList.of(Arrays.asList(fixBqRepeated(fixBqRecord(verifyCell("not-a-date")))), fields);
        assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.toEntity(fields, row, FormattedValueHolder.class));
        fixBqStubResult(fields, row);
        assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.extractData(mockTableResult, FormattedValueHolder.class));
    }

    public static class GenericValue<T> {
        private T value;

        public T getValue() {
            return value;
        }

        public void setValue(final T value) {
            this.value = value;
        }
    }

    public static class GenericValueHolder {
        private List<GenericValue<Long>> values;
        private GenericValue<Long>[] valueArray;

        public List<GenericValue<Long>> getValues() {
            return values;
        }

        public void setValues(final List<GenericValue<Long>> values) {
            this.values = values;
        }

        public GenericValue<Long>[] getValueArray() {
            return valueArray;
        }

        public void setValueArray(final GenericValue<Long>[] valueArray) {
            this.valueArray = valueArray;
        }
    }

    // ---- 2026-10-04 fixBQ2 ----
    // A type-variable property (T value of FixBq2Box<T>) has an erased Object field, and PropInfo.setPropValue converts a value
    // only when storing it fails, so the field silently took the raw cell text: a FixBq2LongBox (extends FixBq2Box<Long>)
    // element of a REPEATED STRUCT column, mapped through toEntity, held "5" and getValue() threw ClassCastException (the JSON
    // codec used before gave a Long), and a FixBq2Box<Long> STRUCT property, mapped by raw class, held "5" as well. Values that
    // are not of the resolved property type are now converted up front, and parameterized bean types are mapped with their
    // type arguments - also for generic REPEATED STRUCT elements, whose TIMESTAMP/BYTES fields are now decoded.

    // Reads the value through FixBq2Box<?> so no cast to the type argument is inserted: a wrong value type fails the
    // assertion instead of throwing ClassCastException.
    private static Object fixBq2ValueOf(final FixBq2Box<?> box) {
        return box.getValue();
    }

    public static class FixBq2GenericElementHolder {
        private List<FixBq2Box<Long>> longs;
        private FixBq2Box<Long>[] longArray;
        private Set<FixBq2Box<java.time.Instant>> instants;
        private List<FixBq2Box<byte[]>> blobs;
        private List<FixBq2Box<FixBqLoc>> locs;

        public List<FixBq2Box<Long>> getLongs() {
            return longs;
        }

        public void setLongs(final List<FixBq2Box<Long>> longs) {
            this.longs = longs;
        }

        public FixBq2Box<Long>[] getLongArray() {
            return longArray;
        }

        public void setLongArray(final FixBq2Box<Long>[] longArray) {
            this.longArray = longArray;
        }

        public Set<FixBq2Box<java.time.Instant>> getInstants() {
            return instants;
        }

        public void setInstants(final Set<FixBq2Box<java.time.Instant>> instants) {
            this.instants = instants;
        }

        public List<FixBq2Box<byte[]>> getBlobs() {
            return blobs;
        }

        public void setBlobs(final List<FixBq2Box<byte[]>> blobs) {
            this.blobs = blobs;
        }

        public List<FixBq2Box<FixBqLoc>> getLocs() {
            return locs;
        }

        public void setLocs(final List<FixBq2Box<FixBqLoc>> locs) {
            this.locs = locs;
        }
    }

    @SuppressWarnings("rawtypes")
    public static class FixBq2RawHolder {
        private FixBq2Box raw;
        private FixBq2Box<?> wild;
        private FixBq2Box<Object> object;
        private FixBq2Box<String> text;
        private List<FixBq2Box> raws;
        private List<FixBq2Box<?>> wilds;
        private List<FixBq2Box<Object>> objects;

        public FixBq2Box getRaw() {
            return raw;
        }

        public void setRaw(final FixBq2Box raw) {
            this.raw = raw;
        }

        public FixBq2Box<?> getWild() {
            return wild;
        }

        public void setWild(final FixBq2Box<?> wild) {
            this.wild = wild;
        }

        public FixBq2Box<Object> getObject() {
            return object;
        }

        public void setObject(final FixBq2Box<Object> object) {
            this.object = object;
        }

        public FixBq2Box<String> getText() {
            return text;
        }

        public void setText(final FixBq2Box<String> text) {
            this.text = text;
        }

        public List<FixBq2Box> getRaws() {
            return raws;
        }

        public void setRaws(final List<FixBq2Box> raws) {
            this.raws = raws;
        }

        public List<FixBq2Box<?>> getWilds() {
            return wilds;
        }

        public void setWilds(final List<FixBq2Box<?>> wilds) {
            this.wilds = wilds;
        }

        public List<FixBq2Box<Object>> getObjects() {
            return objects;
        }

        public void setObjects(final List<FixBq2Box<Object>> objects) {
            this.objects = objects;
        }
    }

    public static class FixBq2Typed {
        private long id;
        private Integer count;
        private boolean active;
        private double score;
        private BigDecimal amount;
        private java.time.LocalDate day;
        private java.util.Date seenAt;
        private FixBq2Color color;
        private Object anything;
        private String tagsText;
        @com.landawn.abacus.annotation.JsonXmlField(isJsonRawValue = true)
        private String tagsJson;
        private char initial;

        public long getId() {
            return id;
        }

        public void setId(final long id) {
            this.id = id;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(final Integer count) {
            this.count = count;
        }

        public boolean isActive() {
            return active;
        }

        public void setActive(final boolean active) {
            this.active = active;
        }

        public double getScore() {
            return score;
        }

        public void setScore(final double score) {
            this.score = score;
        }

        public BigDecimal getAmount() {
            return amount;
        }

        public void setAmount(final BigDecimal amount) {
            this.amount = amount;
        }

        public java.time.LocalDate getDay() {
            return day;
        }

        public void setDay(final java.time.LocalDate day) {
            this.day = day;
        }

        public java.util.Date getSeenAt() {
            return seenAt;
        }

        public void setSeenAt(final java.util.Date seenAt) {
            this.seenAt = seenAt;
        }

        public FixBq2Color getColor() {
            return color;
        }

        public void setColor(final FixBq2Color color) {
            this.color = color;
        }

        public Object getAnything() {
            return anything;
        }

        public void setAnything(final Object anything) {
            this.anything = anything;
        }

        public String getTagsText() {
            return tagsText;
        }

        public void setTagsText(final String tagsText) {
            this.tagsText = tagsText;
        }

        public String getTagsJson() {
            return tagsJson;
        }

        public void setTagsJson(final String tagsJson) {
            this.tagsJson = tagsJson;
        }

        public char getInitial() {
            return initial;
        }

        public void setInitial(final char initial) {
            this.initial = initial;
        }
    }

    public static class FixBq3SbBox extends FixBq2Box<StringBuilder> {
    }

    public static class FixBq3TextHolder {
        private List<FixBq2Box<StringBuilder>> builders;
        private List<FixBq2Box<String>> texts;
        private List<FixBq3SbBox> sbBoxes;
        private FixBq2Box<StringBuilder> builder;

        public List<FixBq2Box<StringBuilder>> getBuilders() {
            return builders;
        }

        public void setBuilders(final List<FixBq2Box<StringBuilder>> builders) {
            this.builders = builders;
        }

        public List<FixBq2Box<String>> getTexts() {
            return texts;
        }

        public void setTexts(final List<FixBq2Box<String>> texts) {
            this.texts = texts;
        }

        public List<FixBq3SbBox> getSbBoxes() {
            return sbBoxes;
        }

        public void setSbBoxes(final List<FixBq3SbBox> sbBoxes) {
            this.sbBoxes = sbBoxes;
        }

        public FixBq2Box<StringBuilder> getBuilder() {
            return builder;
        }

        public void setBuilder(final FixBq2Box<StringBuilder> builder) {
            this.builder = builder;
        }
    }

    private static void assertStringStructProperties(final StringStructProperties value) {
        final String tags = N.toJson(Arrays.asList("a", "b"));
        final String child = N.toJson(Map.of("label", "nested"));
        assertEquals(tags, value.getAttributes().get("tags"));
        assertEquals(child, value.getAttributes().get("child"));
        assertEquals("plain", value.getAttributes().get("text"));
        assertTrue(value.getAttributes().containsKey("missing"));
        assertNull(value.getAttributes().get("missing"));
        // Collection-shaped STRUCTs represent nested STRUCTs by their values, rather than their field names.
        assertEquals(Arrays.asList(tags, N.toJson(Arrays.asList("nested")), "plain", null), value.getValues());
        assertEquals(Arrays.asList("a", "b"), value.getRawMap().get("tags"));
        assertEquals(Map.of("label", "nested"), value.getRawMap().get("child"));
        assertEquals(Arrays.asList(Arrays.asList("a", "b"), Arrays.asList("nested"), "plain", null), value.getRawList());
    }

    private static FieldList formattedValueFields() {
        return FieldList.of(Field.of("date", StandardSQLTypeName.STRING), Field.of("day", StandardSQLTypeName.STRING),
                Field.of("timestamp", StandardSQLTypeName.TIMESTAMP), Field.of("amount", StandardSQLTypeName.STRING),
                Field.of("bytes", StandardSQLTypeName.BYTES));
    }

    private static void assertFormattedValue(final FormattedValue value) {
        assertEquals(java.util.Date.from(java.time.Instant.parse("2026-10-03T00:00:00Z")), value.getDate());
        assertEquals(java.time.LocalDate.of(2026, 10, 3), value.getDay());
        // Native TIMESTAMP/BYTES decoding takes precedence over text formatting, preserving microseconds and binary bytes.
        assertEquals(java.time.Instant.ofEpochSecond(1_718_900_000L, 123_456_000), value.getTimestamp().toInstant());
        assertEquals(new BigDecimal("1234.50"), value.getAmount());
        assertTrue(Arrays.equals(new byte[] { 1, 2, 3 }, value.getBytes()));
    }

    @lombok.Data
    public static class StringStructProperties {
        private Map<String, String> attributes;
        private List<String> values;
        private Map<String, Object> rawMap;
        private List<Object> rawList;
    }

    @lombok.Data
    public static class StringStructPropertyHolder {
        private List<StringStructProperties> items;
        private StringStructProperties single;
    }

    @lombok.Data
    public static class FormattedValue {
        @com.landawn.abacus.annotation.JsonXmlField(dateFormat = "dd/MM/yyyy", timeZone = "UTC")
        private java.util.Date date;
        @com.landawn.abacus.annotation.JsonXmlField(dateFormat = "dd/MM/yyyy", timeZone = "UTC")
        private java.time.LocalDate day;
        @com.landawn.abacus.annotation.JsonXmlField(dateFormat = "dd/MM/yyyy", timeZone = "UTC")
        private java.sql.Timestamp timestamp;
        @com.landawn.abacus.annotation.JsonXmlField(numberFormat = "#,##0.00")
        private BigDecimal amount;
        private byte[] bytes;
    }

    @lombok.Data
    public static class FormattedValueHolder {
        private List<FormattedValue> items;
        private FormattedValue single;
    }
}
