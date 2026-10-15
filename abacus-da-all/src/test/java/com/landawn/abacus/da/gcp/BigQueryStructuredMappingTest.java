package com.landawn.abacus.da.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableResult;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.N;

public class BigQueryStructuredMappingTest extends BigQueryExecutorTestSupport {

    // Row and entity mapping

    // Regression: extractData must unwrap REPEATED columns the same way — the Dataset cell is a List
    // of plain values, not a List of FieldValue wrappers.
    @Test
    public void testExtractData_UnwrapsRepeatedFieldValues_EntityTarget() {
        final Field idField = Field.of("id", StandardSQLTypeName.INT64);
        final Field tagsField = Field.newBuilder("tags", StandardSQLTypeName.STRING).setMode(Field.Mode.REPEATED).build();
        final FieldList fields = FieldList.of(idField, tagsField);
        final Schema schema = Schema.of(fields);

        final List<FieldValueList> rows = new ArrayList<>();
        rows.add(FieldValueList.of(
                Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "7"),
                        FieldValue.of(FieldValue.Attribute.REPEATED,
                                Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "a"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "b")))),
                fields));

        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(rows);

        final Dataset dataset = BigQueryExecutor.extractData(mockTableResult, TestEntityWithTags.class);

        assertEquals(1, dataset.size());
        assertEquals(Arrays.asList("a", "b"), dataset.getColumn("tags").get(0));
    }

    // A getter-only property inherited from an @Entity superclass is in propInfoList, so insert/update write it and the
    // generated SELECT projects it; reading that column back threw UnsupportedOperationException for the whole row.
    @Test
    public void testGetterOnlyPropertyColumnIsSkippedOnRead() throws Exception {
        when(mockTableResult.getSchema()).thenReturn(null);
        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<>());
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);
        executor.list(ComputedSubEntity.class, Filters.eq("id", "1"));
        assertTrue(captureGeneratedSql().contains("computed"), "the generated SELECT projects the getter-only column");

        final FieldList fields = FieldList.of(Field.of("id", StandardSQLTypeName.STRING), Field.of("name", StandardSQLTypeName.STRING),
                Field.of("computed", StandardSQLTypeName.STRING));
        final FieldValueList row = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "n"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "c-n")), fields);

        final ComputedSubEntity entity = BigQueryExecutor.toEntity(fields, row, ComputedSubEntity.class);
        assertEquals("1", entity.getId());
        assertEquals("n", entity.getName());
        assertEquals("c-n", entity.getComputed());
    }

    // Pins (unchanged behavior): a non-repeated STRUCT carrying its schema, without REPEATED sub-fields, reads as before on
    // every path; an Object target keeps the raw record; a FieldValueList-typed property keeps the raw STRUCT or REPEATED
    // value (which is a FieldValueList as the client decodes it).
    @Test
    public void testNonRepeatedStructAndRawFieldValueListPropertiesUnchanged() throws Exception {
        final FieldList homeFields = FieldList.of(Field.of("city", StandardSQLTypeName.STRING),
                Field.newBuilder("loc", StandardSQLTypeName.STRUCT, fixBqLocFields()).build());
        final FieldList fields = FieldList.of(Field.newBuilder("home", StandardSQLTypeName.STRUCT, homeFields).build());
        final FieldValue home = FieldValue.of(FieldValue.Attribute.RECORD,
                FieldValueList.of(Arrays.asList(verifyCell("Home"),
                        FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(Arrays.asList(verifyCell("1.0"), verifyCell("2.0")), fixBqLocFields()))),
                        homeFields));
        final FieldValueList row = FieldValueList.of(Arrays.asList(home), fields);

        assertEquals(2.0, BigQueryExecutor.toEntity(fields, row, FixBqHomeHolder.class).getHome().getLoc().getLng());
        final Map<?, ?> map = (Map<?, ?>) BigQueryExecutor.toMap(fields, row).get("home");
        assertEquals("2.0", ((Map<?, ?>) map.get("loc")).get("lng"));

        fixBqStubResult(fields, row);
        assertEquals(map, BigQueryExecutor.toList(mockTableResult, Map.class).get(0).get("home"));
        assertEquals(Arrays.asList("Home", Arrays.asList("1.0", "2.0")), BigQueryExecutor.toList(mockTableResult, List.class).get(0).get(0));
        assertEquals("Home", ((Object[]) BigQueryExecutor.toList(mockTableResult, Object[].class).get(0)[0])[0]);
        assertEquals("Home", executor.queryForSingleValue(FixBqHome.class, "SELECT home FROM t").get().getCity());
        assertEquals(map, executor.queryForSingleValue(Map.class, "SELECT home FROM t").get());
        assertSame(home.getValue(), executor.queryForSingleValue(Object.class, "SELECT home FROM t").get());
        assertSame(home.getValue(), BigQueryExecutor.toList(mockTableResult, Object.class).get(0));
        assertEquals("Home", ((Object[]) BigQueryExecutor.extractData(mockTableResult, null).getColumn("home").get(0))[0]);

        final FieldList rawFields = FieldList.of(fields.get(0), fixBqRepeatedField("tags", StandardSQLTypeName.STRING));
        final FieldValue tags = fixBqRepeated(verifyCell("a"));
        final FieldValueList rawRow = FieldValueList.of(Arrays.asList(home, tags), rawFields);
        final FixBqRaw raw = BigQueryExecutor.toEntity(rawFields, rawRow, FixBqRaw.class);
        assertSame(home.getValue(), raw.getHome());
        assertSame(tags.getValue(), raw.getTags());

        fixBqStubResult(rawFields, rawRow);
        final Dataset ds = BigQueryExecutor.extractData(mockTableResult, FixBqRaw.class);
        assertSame(home.getValue(), ds.getColumn("home").get(0));
        assertSame(tags.getValue(), ds.getColumn("tags").get(0));
    }

    // A non-repeated STRUCT is read with its column's sub-schema whether or not the record carries one (the client attaches
    // it outside REPEATED values only), including a REPEATED sub-field inside it, on every read path.
    @Test
    public void testNonRepeatedStructReadWithColumnSchemaWithOrWithoutAttachedSchema() throws Exception {
        final FieldList homeFields = FieldList.of(Field.of("city", StandardSQLTypeName.STRING),
                Field.newBuilder("loc", StandardSQLTypeName.STRUCT, fixBqLocFields()).build(), fixBqRepeatedField("codes", StandardSQLTypeName.INT64));
        final FieldList fields = FieldList.of(Field.newBuilder("home", StandardSQLTypeName.STRUCT, homeFields).build());
        final FieldValue attached = FieldValue.of(FieldValue.Attribute.RECORD,
                FieldValueList.of(Arrays.asList(verifyCell("Home"),
                        FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(Arrays.asList(verifyCell("1.0"), verifyCell("2.0")), fixBqLocFields())),
                        fixBqRepeated(verifyCell("5"))), homeFields));
        final FieldValue detached = fixBqRecord(verifyCell("Home"), fixBqRecord(verifyCell("1.0"), verifyCell("2.0")), fixBqRepeated(verifyCell("5")));

        for (final FieldValue home : Arrays.asList(attached, detached)) {
            final FieldValueList row = FieldValueList.of(Arrays.asList(home), fields);
            final FixBqHome bean = BigQueryExecutor.toEntity(fields, row, FixBqHomeHolder.class).getHome();
            assertEquals("Home", bean.getCity());
            assertEquals(2.0, bean.getLoc().getLng());
            assertEquals(Arrays.asList(5L), bean.getCodes());

            final Map<?, ?> map = (Map<?, ?>) BigQueryExecutor.toMap(fields, row).get("home");
            assertEquals("Home", map.get("city"));
            assertEquals("1.0", ((Map<?, ?>) map.get("loc")).get("lat"));
            assertEquals(Arrays.asList("5"), map.get("codes"));
            final List<Object> values = Arrays.asList("Home", Arrays.asList("1.0", "2.0"), Arrays.asList("5"));

            fixBqStubResult(fields, row);
            assertEquals(map, BigQueryExecutor.toList(mockTableResult, Map[].class).get(0)[0]);
            assertEquals(values, BigQueryExecutor.toList(mockTableResult, List.class).get(0).get(0));
            assertEquals(2.0, executor.queryForSingleValue(FixBqHome.class, "SELECT home FROM t").get().getLoc().getLng());
            assertEquals(map, executor.queryForSingleValue(Map.class, "SELECT home FROM t").get());
            assertEquals(values, executor.queryForSingleValue(List.class, "SELECT home FROM t").get());
            assertEquals(map, BigQueryExecutor.extractData(mockTableResult, Map.class).getColumn("home").get(0));
            assertEquals(Arrays.asList(5L),
                    ((FixBqHome) BigQueryExecutor.extractData(mockTableResult, FixBqHomeHolder.class).getColumn("home").get(0)).getCodes());
        }
    }

    // A record collects its values in a constructor-argument array that PropInfo.setPropValue fills without
    // converting, so every non-String component failed with "argument type mismatch" - a record row target as well as
    // a REPEATED STRUCT -> List<record> property (which the JSON codec used to map when no TIMESTAMP/BYTES sub-field was
    // involved, and which the element-wise mapping must keep working).
    @Test
    public void testRecordTargetsConvertCellTextToComponentTypes() {
        final FieldList fields = verifyVisitFields();
        final FieldValueList row = (FieldValueList) verifyVisit("SF", "94105").getValue();
        final java.time.Instant expected = java.time.Instant.ofEpochSecond(1_718_900_000L, 123_456_000L);

        assertEquals(new VisitRecord("SF", 94105L, expected), BigQueryExecutor.toEntity(fields, row, VisitRecord.class));

        when(mockTableResult.getSchema()).thenReturn(Schema.of(fields));
        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.iterateAll()).thenReturn(Arrays.asList(row));
        assertEquals(new VisitRecord("SF", 94105L, expected), BigQueryExecutor.toList(mockTableResult, VisitRecord.class).get(0));

        // REPEATED STRUCT -> List<record>, with and without a TIMESTAMP sub-field
        final FieldList holderFields = FieldList.of(verifyRepeatedStruct("visits", fields));
        final RecordHolder holder = BigQueryExecutor.toEntity(holderFields,
                FieldValueList.of(
                        Arrays.asList(FieldValue.of(FieldValue.Attribute.REPEATED, Arrays.asList(verifyVisit("SF", "94105"), verifyVisit("LA", "90001")))),
                        holderFields),
                RecordHolder.class);
        assertEquals(Arrays.asList(new VisitRecord("SF", 94105L, expected), new VisitRecord("LA", 90001L, expected)), holder.getVisits());

        final FieldList plain = FieldList.of(Field.of("city", StandardSQLTypeName.STRING), Field.of("zip", StandardSQLTypeName.INT64));
        final FieldList plainHolderFields = FieldList.of(verifyRepeatedStruct("visits", plain));
        final RecordHolder plainHolder = BigQueryExecutor.toEntity(plainHolderFields, FieldValueList.of(
                Arrays.asList(FieldValue.of(FieldValue.Attribute.REPEATED,
                        Arrays.asList(
                                FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(Arrays.asList(verifyCell("SF"), verifyCell("94105")), plain))))),
                plainHolderFields), RecordHolder.class);
        assertEquals(Arrays.asList(new VisitRecord("SF", 94105L, null)), plainHolder.getVisits());
    }

    // REPEATED STRUCT -> bean containers: every container kind, empty arrays, NULL sub-fields, and REPEATED STRUCT nested
    // inside a STRUCT / inside another REPEATED STRUCT element all map each element like a STRUCT property (decoded
    // TIMESTAMP sub-fields), not through the JSON codec.
    @Test
    public void testRepeatedStructBeanElementsInEveryContainerAndNesting() {
        final FieldList visitFields = verifyVisitFields();
        final java.time.Instant expected = java.time.Instant.ofEpochSecond(1_718_900_000L, 123_456_000L);
        final FieldValue twoVisits = FieldValue.of(FieldValue.Attribute.REPEATED, Arrays.asList(verifyVisit("SF", "94105"), verifyVisit("LA", null)));
        final FieldValue noVisits = FieldValue.of(FieldValue.Attribute.REPEATED, new ArrayList<>());

        final FieldList containerFields = FieldList.of(verifyRepeatedStruct("visit_set", visitFields), verifyRepeatedStruct("visit_coll", visitFields),
                verifyRepeatedStruct("visits", visitFields), verifyRepeatedStruct("visit_array", visitFields));
        final VisitContainers containers = BigQueryExecutor.toEntity(containerFields,
                FieldValueList.of(Arrays.asList(twoVisits, twoVisits, noVisits, noVisits), containerFields), VisitContainers.class);

        assertEquals(2, containers.getVisitSet().size());
        assertTrue(containers.getVisitSet() instanceof Set);
        assertEquals(2, containers.getVisitColl().size());
        for (final Visit visit : containers.getVisitColl()) {
            assertEquals(expected, visit.getSeenAt());
        }
        assertEquals(0L, new ArrayList<>(containers.getVisitColl()).get(1).getZip()); // NULL INT64 -> primitive default
        assertEquals(0, containers.getVisits().size());
        assertEquals(0, containers.getVisitArray().length);

        // STRUCT<name, visits ARRAY<STRUCT>> -> bean, and ARRAY<STRUCT<name, visits ARRAY<STRUCT>>> -> List<bean>
        final FieldList tripFields = FieldList.of(Field.of("name", StandardSQLTypeName.STRING), verifyRepeatedStruct("visits", visitFields));
        final FieldValue trip = FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(Arrays.asList(verifyCell("t1"), twoVisits), tripFields));
        final FieldList tripHolderFields = FieldList.of(Field.newBuilder("trip", StandardSQLTypeName.STRUCT, tripFields).build(),
                verifyRepeatedStruct("trips", tripFields));
        final TripHolder tripHolder = BigQueryExecutor.toEntity(tripHolderFields,
                FieldValueList.of(Arrays.asList(trip, FieldValue.of(FieldValue.Attribute.REPEATED, Arrays.asList(trip, trip))), tripHolderFields),
                TripHolder.class);

        assertEquals("t1", tripHolder.getTrip().getName());
        assertEquals(expected, tripHolder.getTrip().getVisits().get(0).getSeenAt());
        assertEquals(2, tripHolder.getTrips().size());
        assertEquals("LA", tripHolder.getTrips().get(1).getVisits().get(1).getCity());
        assertEquals(expected, tripHolder.getTrips().get(1).getVisits().get(1).getSeenAt());
    }

    // A REPEATED STRUCT column mapped to a List<Bean>/Bean[] property went through the JSON codec, which handed the
    // element's raw TIMESTAMP (epoch seconds) / BYTES (base64) text to the bean and failed ("Cannot parse
    // \"1718900000.123456\""), while the same STRUCT mapped to a single bean property was decoded. Each element is now
    // mapped like a STRUCT property.
    @Test
    public void testRepeatedStructColumnMapsBeanElementsWithDecodedFields() {
        final FieldList sub = FieldList.of(Field.of("city", StandardSQLTypeName.STRING), Field.of("zip", StandardSQLTypeName.INT64),
                Field.of("seen_at", StandardSQLTypeName.TIMESTAMP), Field.of("blob", StandardSQLTypeName.BYTES));
        final FieldValue element = FieldValue.of(FieldValue.Attribute.RECORD,
                FieldValueList.of(
                        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "SF"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "94105"),
                                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1718900000.123456"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "AQID")),
                        sub));
        final FieldValue repeated = FieldValue.of(FieldValue.Attribute.REPEATED, Arrays.asList(element, element));
        final FieldList fields = FieldList.of(Field.newBuilder("visits", StandardSQLTypeName.STRUCT, sub).setMode(Field.Mode.REPEATED).build(),
                Field.newBuilder("visit_array", StandardSQLTypeName.STRUCT, sub).setMode(Field.Mode.REPEATED).build());
        final FieldValueList row = FieldValueList.of(Arrays.asList(repeated, repeated), fields);

        final java.time.Instant expected = java.time.Instant.ofEpochSecond(1_718_900_000L, 123_456_000L);
        final java.util.function.Consumer<VisitHolder> check = holder -> {
            assertEquals(2, holder.getVisits().size());
            assertEquals(2, holder.getVisitArray().length);
            for (final Visit visit : Arrays.asList(holder.getVisits().get(1), holder.getVisitArray()[1])) {
                assertEquals("SF", visit.getCity());
                assertEquals(94105L, visit.getZip());
                assertEquals(expected, visit.getSeenAt());
                assertTrue(Arrays.equals(new byte[] { 1, 2, 3 }, visit.getBlob()));
            }
        };

        check.accept(BigQueryExecutor.toEntity(fields, row, VisitHolder.class));

        when(mockTableResult.getSchema()).thenReturn(Schema.of(fields));
        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.iterateAll()).thenReturn(Arrays.asList(row));
        check.accept(BigQueryExecutor.toList(mockTableResult, VisitHolder.class).get(0));

        final Dataset ds = BigQueryExecutor.extractData(mockTableResult, VisitHolder.class);
        assertEquals(expected, ((Visit) ((List<?>) ds.getColumn("visits").get(0)).get(0)).getSeenAt());

        // Map rows keep the raw cell values
        assertEquals("1718900000.123456", ((Map<?, ?>) ((List<?>) BigQueryExecutor.toMap(fields, row).get("visits")).get(0)).get("seen_at"));
    }

    // The nested-schema regression on its own (a plain List of records, as hand-built data often is): a REPEATED STRUCT
    // element mapped through toEntity read its nested STRUCT properties with readRow(record), which needs a schema attached
    // to the record, and a record inside a REPEATED value has none. Element beans without TIMESTAMP/BYTES fields mapped
    // through the JSON codec before the elements were mapped through toEntity, so this matches HEAD.
    @Test
    public void testRepeatedStructElementNestedStructsWithoutAttachedSchema() {
        final FieldList placeFields = FieldList.of(Field.of("city", StandardSQLTypeName.STRING),
                Field.newBuilder("loc", StandardSQLTypeName.STRUCT, fixBqLocFields()).build(),
                Field.newBuilder("loc_map", StandardSQLTypeName.STRUCT, fixBqLocFields()).build(),
                Field.newBuilder("counts", StandardSQLTypeName.STRUCT, Field.of("x", StandardSQLTypeName.INT64)).build());
        final FieldList fields = FieldList.of(verifyRepeatedStruct("places", placeFields));
        final FieldValue places = FieldValue.of(FieldValue.Attribute.REPEATED, Arrays.asList(fixBqRecord(verifyCell("SF"),
                fixBqRecord(verifyCell("37.5"), verifyCell("-122.25")), fixBqRecord(verifyCell("1.5"), verifyCell("2.5")), fixBqRecord(verifyCell("11")))));

        final FixBqPlace place = BigQueryExecutor.toEntity(fields, FieldValueList.of(Arrays.asList(places), fields), FixBqPlaceHolder.class).getPlaces().get(0);

        assertEquals("SF", place.getCity());
        assertEquals(37.5, place.getLoc().getLat());
        assertEquals("1.5", place.getLocMap().get("lat"));
        final Object count = place.getCounts().get("x");
        assertEquals(Long.valueOf(11), count);
    }

    // A Map/Collection property with typed elements of an element bean of a REPEATED STRUCT receives typed values, as it
    // did through the JSON codec before the elements were mapped through toEntity. Matches HEAD.
    @Test
    public void testRepeatedStructElementTypedContainerPropertiesMatchHead() {
        // without "pair" (STRUCT -> List<Long>), which the JSON codec could not read from an element
        final FieldList all = verifyContainerFields();
        final FieldValueList allRow = verifyContainerRow(all);
        final FieldList sub = FieldList.of(all.get(0), all.get(2), all.get(3));
        final FieldList fields = FieldList.of(verifyRepeatedStruct("items", sub));
        final FieldValue repeated = FieldValue.of(FieldValue.Attribute.REPEATED,
                Arrays.asList(FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(Arrays.asList(allRow.get(0), allRow.get(2), allRow.get(3)), sub))));

        final TypedContainers item = BigQueryExecutor.toEntity(fields, FieldValueList.of(Arrays.asList(repeated), fields), TypedContainersHolder.class)
                .getItems()
                .get(0);

        final Object count = item.getCounts().get("x");
        final Object nestedCount = item.getNested().get("m").get("x");
        assertEquals(Long.valueOf(11), count);
        assertEquals(Long.valueOf(11), nestedCount);
        assertEquals("11", item.getRaw().get("x")); // Map<String, Object> keeps the cell text
    }

    // Element types that are not beans keep the previous mapping: Map elements receive the raw cell values, and a value
    // type with bean-style accessors (GregorianCalendar) is not populated from same-named sub-fields - it is rejected
    // as before rather than silently mapped to calendars holding the current time.
    @Test
    public void testRepeatedStructNonBeanElementTypesKeepPreviousMapping() {
        final FieldList visitFields = verifyVisitFields();
        final FieldValue visits = FieldValue.of(FieldValue.Attribute.REPEATED, Arrays.asList(verifyVisit("SF", "94105")));
        final FieldList mapFields = FieldList.of(verifyRepeatedStruct("visits", visitFields));

        final MapElementHolder holder = BigQueryExecutor.toEntity(mapFields, FieldValueList.of(Arrays.asList(visits), mapFields), MapElementHolder.class);
        assertEquals("94105", holder.getVisits().get(0).get("zip"));
        assertEquals("1718900000.123456", holder.getVisits().get(0).get("seen_at"));

        final FieldList calendarFields = FieldList.of(verifyRepeatedStruct("calendars", FieldList.of(Field.of("time_in_millis", StandardSQLTypeName.INT64))));
        final FieldValueList calendarRow = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.REPEATED, Arrays.asList(
                FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(Arrays.asList(verifyCell("1000")), calendarFields.get(0).getSubFields()))))),
                calendarFields);
        assertThrows(RuntimeException.class, () -> BigQueryExecutor.toEntity(calendarFields, calendarRow, MapElementHolder.class));
    }

    // REPEATED STRUCT -> List<Bean> / Bean[] whose element bean has nested STRUCT properties (bean, Map<String, Object>,
    // Map<String, Long>), a REPEATED property and a STRUCT property holding a REPEATED: none of those records carries a
    // schema, so each is read with the sub-schema of its enclosing column. toEntity, the registered converter, list rows and
    // the bean-class Dataset.
    @Test
    public void testSdkRepeatedStructIntoBeanElementsWithNestedStructs() throws Exception {
        final FieldList fields = FieldList.of(Field.of("id", StandardSQLTypeName.INT64), verifyRepeatedStruct("visits", fixBqVisitFields()),
                verifyRepeatedStruct("visit_array", fixBqVisitFields()));
        final FieldValueList row = FieldValueList
                .of(Arrays.asList(verifyCell("5"), fixBqRepeated(fixBqVisit("SF"), fixBqVisit("LA")), fixBqRepeated(fixBqVisit("NY"))), fields);
        final java.time.Instant seenAt = java.time.Instant.ofEpochSecond(1_718_900_000L, 123_456_000L);

        final java.util.function.Consumer<FixBqVisit> checkVisit = visit -> {
            assertEquals(seenAt, visit.getSeenAt());
            assertTrue(Arrays.equals(new byte[] { 1, 2, 3 }, visit.getBlob()));
            assertEquals(37.5, visit.getLoc().getLat());
            assertEquals(-122.25, visit.getLoc().getLng());
            assertEquals("1.5", visit.getLocMap().get("lat")); // Map<String, Object> keeps the cell text
            final Object count = visit.getCounts().get("x");
            assertEquals(Long.valueOf(11), count);
            assertEquals(Arrays.asList(1L, 2L), visit.getCodes());
            assertEquals("s1", visit.getStop().getName());
            assertEquals(Arrays.asList(7L, 8L), visit.getStop().getCodes());
        };
        final java.util.function.Consumer<FixBqVisitHolder> check = holder -> {
            assertEquals(5L, holder.getId());
            assertEquals(2, holder.getVisits().size());
            assertEquals("SF", holder.getVisits().get(0).getCity());
            assertEquals("LA", holder.getVisits().get(1).getCity());
            assertEquals(1, holder.getVisitArray().length);
            assertEquals("NY", holder.getVisitArray()[0].getCity());
            checkVisit.accept(holder.getVisits().get(1));
            checkVisit.accept(holder.getVisitArray()[0]);
        };

        check.accept(BigQueryExecutor.toEntity(fields, row, FixBqVisitHolder.class));
        check.accept(BigQueryExecutor.toEntity(row, FixBqVisitHolder.class)); // top-level schema read off the row
        check.accept(N.convert(row, FixBqVisitHolder.class));

        fixBqStubResult(fields, row);
        check.accept(BigQueryExecutor.toList(mockTableResult, FixBqVisitHolder.class).get(0));

        final Dataset ds = BigQueryExecutor.extractData(mockTableResult, FixBqVisitHolder.class);
        final FixBqVisit fromDataset = (FixBqVisit) ((List<?>) ds.getColumn("visits").get(0)).get(1);
        assertEquals("LA", fromDataset.getCity());
        checkVisit.accept(fromDataset);
        checkVisit.accept(((FixBqVisit[]) ds.getColumn("visit_array").get(0))[0]);
    }

    // A STRUCT cell is a FieldValueList, which IS a List. Since abacus-common 8.1, N.convert returns a source that is
    // already an instance of the target unchanged (skipping the registered readRow converter), so
    // queryForSingleValue/queryForSingleNonNull handed List/Collection targets the raw FieldValue wrappers.
    @Test
    public void testSingleValueStructCellIntoCollectionTargetIsDecoded() throws Exception {
        final FieldList sub = FieldList.of(Field.of("a", StandardSQLTypeName.INT64), Field.of("b", StandardSQLTypeName.STRING));
        final FieldValueList struct = FieldValueList
                .of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "x")), sub);
        final FieldList fields = FieldList.of(Field.newBuilder("s", StandardSQLTypeName.STRUCT, sub).build());
        final FieldValueList row = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.RECORD, struct)), fields);
        final TableResult result = mock(TableResult.class);
        when(result.getSchema()).thenReturn(Schema.of(fields));
        when(result.getTotalRows()).thenReturn(1L);
        when(result.getValues()).thenReturn(Arrays.asList(row));
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(result);

        assertEquals(Arrays.asList("1", "x"), executor.queryForSingleValue(List.class, "SELECT s").get());
        assertEquals(Arrays.asList("1", "x"), executor.queryForSingleNonNull(Collection.class, "SELECT s").get());
        // Map targets were never affected: the converter still runs for a non-List target.
        assertEquals("x", ((Map<?, ?>) executor.queryForSingleValue(Map.class, "SELECT s").get()).get("b"));
    }

    // A STRUCT cell mapped to a Map/Collection property with typed elements (Map<String, Long>, List<Long>) is converted
    // to those types on every bean path - a STRUCT property, a row, list rows and the bean-class Dataset - instead of
    // holding the raw cell text, consistently with REPEATED STRUCT elements. Untyped containers keep the cell text.
    @Test
    public void testStructIntoTypedContainerPropertyConvertsElementTypes() {
        final FieldList fields = verifyContainerFields();
        final FieldValueList row = verifyContainerRow(fields);
        final java.util.function.Consumer<TypedContainers> check = item -> {
            final Object count = item.getCounts().get("x");
            final List<Object> pair = new ArrayList<>(item.getPair());
            final Object nestedCount = item.getNested().get("m").get("x");
            assertEquals(Long.valueOf(11), count);
            assertEquals(Arrays.asList(1L, 2L), pair);
            assertEquals(Long.valueOf(11), nestedCount);
            assertEquals("11", item.getRaw().get("x"));
        };

        check.accept(BigQueryExecutor.toEntity(fields, row, TypedContainers.class));

        final FieldList holderFields = FieldList.of(Field.newBuilder("single", StandardSQLTypeName.STRUCT, fields).build());
        check.accept(
                BigQueryExecutor
                        .toEntity(holderFields, FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.RECORD, row)), holderFields),
                                TypedContainersHolder.class)
                        .getSingle());

        when(mockTableResult.getSchema()).thenReturn(Schema.of(fields));
        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.iterateAll()).thenReturn(Arrays.asList(row));
        check.accept(BigQueryExecutor.toList(mockTableResult, TypedContainers.class).get(0));

        final Dataset ds = BigQueryExecutor.extractData(mockTableResult, TypedContainers.class);
        final Object datasetCount = ((Map<?, ?>) ds.getColumn("counts").get(0)).get("x");
        assertEquals(Long.valueOf(11), datasetCount);
        assertEquals("11", ((Map<?, ?>) ds.getColumn("raw").get(0)).get("x"));
    }

    // Regression: toEntity must unwrap REPEATED columns. FieldValue.getValue() of a REPEATED column
    // returns a List<FieldValue>; the old code stored that list as-is into a List-typed property, so
    // the raw FieldValue wrappers leaked into e.g. a List<String> property (heap pollution surfacing
    // as ClassCastException at the call site). The fixed path mirrors the toMap unwrapping.
    @Test
    public void testToEntity_UnwrapsRepeatedFieldValues() {
        final Field idField = Field.of("id", StandardSQLTypeName.INT64);
        final Field tagsField = Field.newBuilder("tags", StandardSQLTypeName.STRING).setMode(Field.Mode.REPEATED).build();
        final FieldList fields = FieldList.of(idField, tagsField);
        final FieldValueList row = FieldValueList.of(
                Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "7"),
                        FieldValue.of(FieldValue.Attribute.REPEATED,
                                Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "a"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "b")))),
                fields);

        final TestEntityWithTags entity = BigQueryExecutor.toEntity(fields, row, TestEntityWithTags.class);

        assertEquals(7, entity.getId());
        assertEquals(Arrays.asList("a", "b"), entity.getTags());
        assertTrue(entity.getTags().get(0) instanceof String, "REPEATED elements must be plain values, not FieldValue wrappers");
    }

    @Test
    public void testToMap_UnwrapsRepeatedFieldValues() {
        final Field repeated = Field.newBuilder("tags", StandardSQLTypeName.STRING).setMode(Field.Mode.REPEATED).build();
        final FieldList fields = FieldList.of(repeated);
        final FieldValueList row = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.REPEATED,
                Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "a"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "b")))), fields);

        final Map<String, Object> map = BigQueryExecutor.toMap(fields, row, com.landawn.abacus.util.IntFunctions.ofLinkedHashMap());

        assertEquals(Arrays.asList("a", "b"), map.get("tags"));
    }

    // A REPEATED TIMESTAMP / BYTES cell holds epoch-seconds / base64 text per element; a typed array row whose component
    // is itself an array (Instant[][], byte[][][]) must decode each element by schema instead of handing the raw text
    // to N.convert (DateTimeParseException / NumberFormatException).
    @Test
    public void testTypedArrayRowTargetDecodesRepeatedTimestampAndBytesElements() {
        final Field tsField = Field.newBuilder("ts", StandardSQLTypeName.TIMESTAMP).setMode(Field.Mode.REPEATED).build();
        final FieldValueList tsRow = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.REPEATED, Arrays
                .asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1718900000.123456"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1718900001")))),
                FieldList.of(tsField));
        when(mockTableResult.getSchema()).thenReturn(Schema.of(tsField));
        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.iterateAll()).thenReturn(Arrays.asList(tsRow));

        final java.time.Instant[] expected = { java.time.Instant.ofEpochSecond(1_718_900_000L, 123_456_000L), java.time.Instant.ofEpochSecond(1_718_900_001L) };
        assertTrue(Arrays.equals(expected, N.convert(tsRow, java.time.Instant[][].class)[0]));
        assertTrue(Arrays.equals(expected, BigQueryExecutor.toList(mockTableResult, java.time.Instant[][].class).get(0)[0]));

        final Field bytesField = Field.newBuilder("b", StandardSQLTypeName.BYTES).setMode(Field.Mode.REPEATED).build();
        final FieldValueList bytesRow = FieldValueList.of(
                Arrays.asList(FieldValue.of(FieldValue.Attribute.REPEATED,
                        Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "AQID"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "BAU=")))),
                FieldList.of(bytesField));
        final byte[][] binary = N.convert(bytesRow, byte[][][].class)[0];
        assertTrue(Arrays.equals(new byte[] { 1, 2, 3 }, binary[0]));
        assertTrue(Arrays.equals(new byte[] { 4, 5 }, binary[1]));

        // Object[] rows still keep the raw repeated values as a List
        assertEquals(Arrays.asList("AQID", "BAU="), N.convert(bytesRow, Object[].class)[0]);
    }

    // Updates and deletes

    @Test
    public void testRepeatedBinaryAndTimestampDatasetColumnsKeepTypedElements() {
        final FieldList fields = repeatedBinaryTimestampFields();
        when(mockTableResult.getSchema()).thenReturn(Schema.of(fields));
        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.iterateAll()).thenReturn(List.of(repeatedBinaryTimestampRow(fields)));

        final Dataset result = BigQueryExecutor.extractData(mockTableResult, RepeatedBinaryTimestampEntity.class);

        final List<?> bytes = (List<?>) result.getColumn("data").get(0);
        assertTrue(Arrays.equals(new byte[] { 1, 2, 3 }, (byte[]) bytes.get(0)));
        final List<?> times = (List<?>) result.getColumn("timestamps").get(0);
        assertEquals(java.time.Instant.ofEpochSecond(1_718_900_000L, 123_456_000L), ((java.sql.Timestamp) times.get(0)).toInstant());
        assertEquals(java.time.Instant.ofEpochSecond(-1, 999_999_000), ((java.time.Instant[]) result.getColumn("instants").get(0))[1]);
    }

    // Queries and results

    @Test
    public void testRepeatedBinaryAndTimestampEmptyAndNullValuesRemainDistinct() {
        final FieldList fields = repeatedBinaryTimestampFields();
        final FieldValue empty = FieldValue.of(FieldValue.Attribute.REPEATED, List.of());
        final FieldValue nil = FieldValue.of(FieldValue.Attribute.PRIMITIVE, null);
        final FieldValue binary = FieldValue.of(FieldValue.Attribute.REPEATED, List.of(nil, FieldValue.of(FieldValue.Attribute.PRIMITIVE, "")));
        final FieldValue times = FieldValue.of(FieldValue.Attribute.REPEATED, List.of(nil, FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0")));
        final FieldValueList row = FieldValueList.of(List.of(binary, binary, times, times, empty), fields);
        final RepeatedBinaryTimestampEntity result = BigQueryExecutor.toEntity(fields, row, RepeatedBinaryTimestampEntity.class);

        assertNull(result.getData().get(0));
        assertEquals(0, result.getData().get(1).length);
        assertNull(result.getBuffers().get(0));
        assertEquals(0, result.getBuffers().get(1).remaining());
        assertNull(result.getTimestamps().get(0));
        assertEquals(java.time.Instant.EPOCH, result.getTimestamps().get(1).toInstant());
        assertNull(result.getInstants()[0]);
        assertEquals(java.time.Instant.EPOCH, result.getInstants()[1]);
        assertTrue(result.getLabels().isEmpty());

        final RepeatedBinaryTimestampEntity emptyResult = BigQueryExecutor.toEntity(fields,
                FieldValueList.of(List.of(empty, empty, empty, empty, empty), fields), RepeatedBinaryTimestampEntity.class);
        assertTrue(emptyResult.getData().isEmpty());
        assertTrue(emptyResult.getBuffers().isEmpty());
        assertTrue(emptyResult.getTimestamps().isEmpty());
        assertEquals(0, emptyResult.getInstants().length);
    }

    // Regression of the REPEATED dispatch fix: a REPEATED cell read into an explicitly requested FieldValueList was unwrapped
    // and then rebuilt through N.convert, which failed with "No default constructor found in class: ...FieldValueList". The
    // raw payload is returned as is - for queryForSingleValue, queryForSingleNonNull and the element of a FieldValueList[]
    // array row - like a FieldValueList-typed bean property; other targets still get the unwrapped elements. (A FieldValueList
    // ROW type, toList(result, FieldValueList.class), goes through the collection-row path and is not supported.)
    @Test
    public void testSdkRepeatedColumnReadIntoFieldValueListKeepsRawPayload() throws Exception {
        final FieldList tagsField = FieldList.of(fixBqRepeatedField("tags", StandardSQLTypeName.STRING));
        final FieldValue tags = fixBqRepeated(verifyCell("a"), verifyCell("b"));
        fixBqStubResult(tagsField, FieldValueList.of(Arrays.asList(tags), tagsField));

        assertSame(tags.getValue(), executor.queryForSingleValue(FieldValueList.class, "SELECT tags FROM t").get());
        assertSame(tags.getValue(), executor.queryForSingleNonNull(FieldValueList.class, "SELECT tags FROM t").get());
        assertSame(tags.getValue(), BigQueryExecutor.toList(mockTableResult, FieldValueList[].class).get(0)[0]);
        assertEquals(Arrays.asList("a", "b"), executor.queryForSingleValue(List.class, "SELECT tags FROM t").get());

        final FieldList visitsField = FieldList.of(verifyRepeatedStruct("visits", fixBqVisitFields()));
        final FieldValue visits = fixBqRepeated(fixBqVisit("SF"));
        fixBqStubResult(visitsField, FieldValueList.of(Arrays.asList(visits), visitsField));

        assertSame(visits.getValue(), executor.queryForSingleValue(FieldValueList.class, "SELECT visits FROM t").get());
        assertSame(visits.getValue(), executor.queryForSingleNonNull(FieldValueList.class, "SELECT visits FROM t").get());
        assertEquals("SF", ((Map<?, ?>) executor.queryForSingleValue(List.class, "SELECT visits FROM t").get().get(0)).get("city"));
    }

    // REPEATED and REPEATED STRUCT columns in Map rows (toMap, list(Map), Dataset of Maps / raw values), List rows and
    // Object[] rows: the element values are unwrapped, STRUCT elements as Maps with their nested STRUCT and REPEATED values.
    @Test
    public void testSdkRepeatedColumnsInMapListAndObjectArrayRows() throws Exception {
        final FieldList fields = FieldList.of(Field.of("id", StandardSQLTypeName.INT64), fixBqRepeatedField("tags", StandardSQLTypeName.STRING),
                verifyRepeatedStruct("visits", fixBqVisitFields()));
        final FieldValueList row = FieldValueList
                .of(Arrays.asList(verifyCell("5"), fixBqRepeated(verifyCell("a"), verifyCell("b")), fixBqRepeated(fixBqVisit("SF"))), fields);

        final Map<String, Object> map = BigQueryExecutor.toMap(fields, row);
        assertEquals("5", map.get("id"));
        assertEquals(Arrays.asList("a", "b"), map.get("tags"));
        final Map<?, ?> visit = (Map<?, ?>) ((List<?>) map.get("visits")).get(0);
        assertEquals("SF", visit.get("city"));
        assertEquals("1718900000.123456", visit.get("seen_at")); // Map rows keep the raw cell text
        assertEquals("37.5", ((Map<?, ?>) visit.get("loc")).get("lat"));
        assertEquals(Arrays.asList("1", "2"), visit.get("codes"));
        assertEquals(Arrays.asList("7", "8"), ((Map<?, ?>) visit.get("stop")).get("codes"));

        assertEquals(map, BigQueryExecutor.toMap(row)); // top-level schema read off the row
        assertEquals(map, N.convert(row, Map.class));

        fixBqStubResult(fields, row);
        assertEquals(map, BigQueryExecutor.toList(mockTableResult, Map.class).get(0));
        assertEquals(Arrays.asList("5", map.get("tags"), map.get("visits")), BigQueryExecutor.toList(mockTableResult, List.class).get(0));
        assertEquals(Arrays.asList("5", map.get("tags"), map.get("visits")), Arrays.asList(BigQueryExecutor.toList(mockTableResult, Object[].class).get(0)));

        for (final Class<?> datasetClass : Arrays.<Class<?>> asList(Map.class, null)) {
            final Dataset ds = BigQueryExecutor.extractData(mockTableResult, datasetClass);
            assertEquals(map.get("tags"), ds.getColumn("tags").get(0));
            assertEquals(map.get("visits"), ds.getColumn("visits").get(0));
        }
    }

    // Additional contracts

    @Test
    public void testRepeatedBinaryAndTimestampSequentialValuesUseLinearTraversal() {
        // Count linked-list traversal work instead of using a timing-sensitive performance assertion.
        final long[] traversedNodes = { 0 };
        final List<FieldValue> values = new java.util.LinkedList<>() {
            @Override
            public FieldValue get(final int index) {
                traversedNodes[0] += Math.min(index, size() - index - 1) + 1L;
                return super.get(index);
            }

            @Override
            public java.util.ListIterator<FieldValue> listIterator(final int index) {
                final java.util.ListIterator<FieldValue> iterator = super.listIterator(index);
                return new java.util.ListIterator<>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public FieldValue next() {
                        traversedNodes[0]++;
                        return iterator.next();
                    }

                    @Override
                    public boolean hasPrevious() {
                        return iterator.hasPrevious();
                    }

                    @Override
                    public FieldValue previous() {
                        traversedNodes[0]++;
                        return iterator.previous();
                    }

                    @Override
                    public int nextIndex() {
                        return iterator.nextIndex();
                    }

                    @Override
                    public int previousIndex() {
                        return iterator.previousIndex();
                    }

                    @Override
                    public void remove() {
                        iterator.remove();
                    }

                    @Override
                    public void set(final FieldValue value) {
                        iterator.set(value);
                    }

                    @Override
                    public void add(final FieldValue value) {
                        iterator.add(value);
                    }
                };
            }
        };
        for (int i = 0; i < 128; i++) {
            values.add(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "AQID"));
        }
        final FieldList fields = FieldList.of(Field.newBuilder("data", StandardSQLTypeName.BYTES).setMode(Field.Mode.REPEATED).build());
        final FieldValueList row = FieldValueList.of(List.of(FieldValue.of(FieldValue.Attribute.REPEATED, values)), fields);

        final RepeatedBinaryTimestampEntity result = BigQueryExecutor.toEntity(fields, row, RepeatedBinaryTimestampEntity.class);

        assertEquals(values.size(), result.getData().size());
        result.getData().forEach(bytes -> assertTrue(Arrays.equals(new byte[] { 1, 2, 3 }, bytes)));
        assertTrue(traversedNodes[0] <= values.size() * 2L, "Repeated values must be traversed in linear work");
    }

    @Test
    public void testRepeatedBinaryAndTimestampValuesMapAcrossEveryResultRow() {
        final FieldList fields = repeatedBinaryTimestampFields();
        final FieldValueList row = repeatedBinaryTimestampRow(fields);
        when(mockTableResult.getSchema()).thenReturn(Schema.of(fields));
        when(mockTableResult.getTotalRows()).thenReturn(2L);
        when(mockTableResult.iterateAll()).thenReturn(List.of(row, row));

        final List<RepeatedBinaryTimestampEntity> result = BigQueryExecutor.toList(mockTableResult, RepeatedBinaryTimestampEntity.class);

        assertEquals(2, result.size());
        result.forEach(BigQueryStructuredMappingTest::assertRepeatedBinaryTimestampEntity);
    }

    @Test
    public void testRepeatedBinaryAndTimestampValuesMapToTypedBeanProperties() {
        final FieldList fields = repeatedBinaryTimestampFields();
        final FieldValueList row = repeatedBinaryTimestampRow(fields);

        assertRepeatedBinaryTimestampEntity(BigQueryExecutor.toEntity(fields, row, RepeatedBinaryTimestampEntity.class));
        // Raw mappings keep the wire values rather than applying bean-specific element conversion.
        assertEquals(List.of("AQID", "BAU="), BigQueryExecutor.toMap(fields, row).get("data"));
    }

    // A REPEATED column read as a single value - queryForSingleValue / queryForSingleNonNull and single-column scalar rows -
    // yields its unwrapped (and, for a typed array, decoded) element values.
    @Test
    public void testSdkRepeatedColumnSingleValueReads() throws Exception {
        final FieldList tagsField = FieldList.of(fixBqRepeatedField("tags", StandardSQLTypeName.STRING));
        fixBqStubResult(tagsField, FieldValueList.of(Arrays.asList(fixBqRepeated(verifyCell("a"), verifyCell("b"))), tagsField));

        assertEquals(Arrays.asList("a", "b"), executor.queryForSingleValue(List.class, "SELECT tags FROM t").get());
        assertEquals(Set.of("a", "b"), executor.queryForSingleValue(Set.class, "SELECT tags FROM t").get());
        assertTrue(Arrays.equals(new String[] { "a", "b" }, executor.queryForSingleValue(String[].class, "SELECT tags FROM t").get()));
        assertEquals(Arrays.asList("a", "b"), executor.queryForSingleNonNull(List.class, "SELECT tags FROM t").get());
        assertEquals(Arrays.asList("a", "b"), executor.queryForSingleValue(Object.class, "SELECT tags FROM t").get());
        assertEquals(Arrays.asList("a", "b"), BigQueryExecutor.toList(mockTableResult, Object.class).get(0)); // scalar row mapper

        final FieldList timesField = FieldList.of(fixBqRepeatedField("times", StandardSQLTypeName.TIMESTAMP));
        fixBqStubResult(timesField, FieldValueList.of(Arrays.asList(fixBqRepeated(verifyCell("1718900000.123456"))), timesField));
        assertTrue(Arrays.equals(new java.time.Instant[] { java.time.Instant.ofEpochSecond(1_718_900_000L, 123_456_000L) },
                executor.queryForSingleValue(java.time.Instant[].class, "SELECT times FROM t").get()));

        final FieldList blobsField = FieldList.of(fixBqRepeatedField("blobs", StandardSQLTypeName.BYTES));
        fixBqStubResult(blobsField, FieldValueList.of(Arrays.asList(fixBqRepeated(verifyCell("AQID"))), blobsField));
        assertTrue(Arrays.equals(new byte[] { 1, 2, 3 }, executor.queryForSingleValue(byte[][].class, "SELECT blobs FROM t").get()[0]));

        final FieldList visitsField = FieldList.of(verifyRepeatedStruct("visits", fixBqVisitFields()));
        fixBqStubResult(visitsField, FieldValueList.of(Arrays.asList(fixBqRepeated(fixBqVisit("SF"))), visitsField));
        final Map<?, ?> visit = (Map<?, ?>) executor.queryForSingleValue(List.class, "SELECT visits FROM t").get().get(0);
        assertEquals("SF", visit.get("city"));
        assertEquals("-122.25", ((Map<?, ?>) visit.get("loc")).get("lng"));
        assertEquals(Arrays.asList("7", "8"), ((Map<?, ?>) visit.get("stop")).get("codes"));
    }

    // REPEATED columns read into typed-array rows (each cell decoded and converted to the component type), Object[] rows and
    // List rows (the unwrapped element values).
    @Test
    public void testSdkRepeatedColumnsIntoTypedArrayAndCollectionRows() throws Exception {
        final java.time.Instant seenAt = java.time.Instant.ofEpochSecond(1_718_900_000L, 123_456_000L);

        final FieldList timesField = FieldList.of(fixBqRepeatedField("times", StandardSQLTypeName.TIMESTAMP));
        final FieldValueList timesRow = FieldValueList.of(Arrays.asList(fixBqRepeated(verifyCell("1718900000.123456"))), timesField);
        fixBqStubResult(timesField, timesRow);
        assertTrue(Arrays.equals(new java.time.Instant[] { seenAt }, BigQueryExecutor.toList(mockTableResult, java.time.Instant[][].class).get(0)[0]));
        assertEquals(Arrays.asList("1718900000.123456"), BigQueryExecutor.toList(mockTableResult, Object[].class).get(0)[0]);
        assertEquals(Arrays.asList(Arrays.asList("1718900000.123456")), BigQueryExecutor.toList(mockTableResult, List.class).get(0));
        assertTrue(Arrays.equals(new java.time.Instant[] { seenAt }, N.convert(timesRow, java.time.Instant[][].class)[0]));

        final FieldList blobsField = FieldList.of(fixBqRepeatedField("blobs", StandardSQLTypeName.BYTES));
        fixBqStubResult(blobsField, FieldValueList.of(Arrays.asList(fixBqRepeated(verifyCell("AQID"))), blobsField));
        assertTrue(Arrays.equals(new byte[] { 1, 2, 3 }, BigQueryExecutor.toList(mockTableResult, byte[][][].class).get(0)[0][0]));

        final FieldList slotsField = FieldList.of(fixBqRepeatedField("slots", StandardSQLTypeName.TIME));
        fixBqStubResult(slotsField, FieldValueList.of(Arrays.asList(fixBqRepeated(verifyCell("16:13:20.123"))), slotsField));
        assertEquals(java.sql.Time.valueOf(java.time.LocalTime.of(16, 13, 20)).getTime() + 123,
                BigQueryExecutor.toList(mockTableResult, java.sql.Time[][].class).get(0)[0][0].getTime());

        final FieldList numsField = FieldList.of(fixBqRepeatedField("nums", StandardSQLTypeName.INT64));
        fixBqStubResult(numsField, FieldValueList.of(Arrays.asList(fixBqRepeated(verifyCell("1"), verifyCell("2"))), numsField));
        assertTrue(Arrays.equals(new Long[] { 1L, 2L }, BigQueryExecutor.toList(mockTableResult, Long[][].class).get(0)[0]));
        assertTrue(Arrays.equals(new String[] { "1", "2" }, BigQueryExecutor.toList(mockTableResult, String[][].class).get(0)[0]));
        assertEquals(Arrays.asList(Arrays.asList("1", "2")), executor.stream(List.class, "SELECT nums FROM t").toList().get(0));
    }

    // REPEATED STRING / INT64 / TIMESTAMP / BYTES / TIME columns into List, Set and array bean properties (TIMESTAMP, BYTES
    // and TIME elements decoded) on the toEntity, list-row and bean-Dataset paths.
    @Test
    public void testSdkRepeatedPrimitiveColumnsIntoBeanProperties() throws Exception {
        final FieldList fields = FieldList.of(fixBqRepeatedField("tags", StandardSQLTypeName.STRING),
                fixBqRepeatedField("tag_array", StandardSQLTypeName.STRING), fixBqRepeatedField("nums", StandardSQLTypeName.INT64),
                fixBqRepeatedField("num_array", StandardSQLTypeName.INT64), fixBqRepeatedField("times", StandardSQLTypeName.TIMESTAMP),
                fixBqRepeatedField("time_array", StandardSQLTypeName.TIMESTAMP), fixBqRepeatedField("blobs", StandardSQLTypeName.BYTES),
                fixBqRepeatedField("slots", StandardSQLTypeName.TIME));
        final FieldValue tags = fixBqRepeated(verifyCell("a"), verifyCell("b"));
        final FieldValue nums = fixBqRepeated(verifyCell("1"), verifyCell("2"));
        final FieldValue times = fixBqRepeated(verifyCell("1718900000.123456"));
        final FieldValueList row = FieldValueList.of(Arrays.asList(tags, tags, nums, nums, times, times, fixBqRepeated(verifyCell("AQID")),
                fixBqRepeated(verifyCell("16:13:20.123"), verifyCell("00:00:00"))), fields);
        final java.time.Instant seenAt = java.time.Instant.ofEpochSecond(1_718_900_000L, 123_456_000L);
        final long slotMillis = java.sql.Time.valueOf(java.time.LocalTime.of(16, 13, 20)).getTime() + 123;

        final java.util.function.Consumer<FixBqPrimitives> check = bean -> {
            assertEquals(Arrays.asList("a", "b"), bean.getTags());
            assertTrue(Arrays.equals(new String[] { "a", "b" }, bean.getTagArray()));
            assertEquals(Set.of(1L, 2L), bean.getNums());
            assertTrue(Arrays.equals(new Long[] { 1L, 2L }, bean.getNumArray()));
            assertEquals(Arrays.asList(seenAt), bean.getTimes());
            assertTrue(Arrays.equals(new java.time.Instant[] { seenAt }, bean.getTimeArray()));
            assertTrue(Arrays.equals(new byte[] { 1, 2, 3 }, bean.getBlobs().get(0)));
            assertEquals(Set.of(new java.sql.Time(slotMillis), java.sql.Time.valueOf(java.time.LocalTime.MIDNIGHT)), bean.getSlots());
        };

        check.accept(BigQueryExecutor.toEntity(fields, row, FixBqPrimitives.class));
        check.accept(N.convert(row, FixBqPrimitives.class));

        fixBqStubResult(fields, row);
        check.accept(BigQueryExecutor.toList(mockTableResult, FixBqPrimitives.class).get(0));

        final Dataset ds = BigQueryExecutor.extractData(mockTableResult, FixBqPrimitives.class);
        assertEquals(Arrays.asList(seenAt), ds.getColumn("times").get(0));
        assertTrue(Arrays.equals(new Long[] { 1L, 2L }, (Long[]) ds.getColumn("num_array").get(0)));
        assertTrue(Arrays.equals(new byte[] { 1, 2, 3 }, (byte[]) ((List<?>) ds.getColumn("blobs").get(0)).get(0)));
    }

    private static FieldList repeatedBinaryTimestampFields() {
        return FieldList.of(Field.newBuilder("data", StandardSQLTypeName.BYTES).setMode(Field.Mode.REPEATED).build(),
                Field.newBuilder("buffers", StandardSQLTypeName.BYTES).setMode(Field.Mode.REPEATED).build(),
                Field.newBuilder("timestamps", StandardSQLTypeName.TIMESTAMP).setMode(Field.Mode.REPEATED).build(),
                Field.newBuilder("instants", StandardSQLTypeName.TIMESTAMP).setMode(Field.Mode.REPEATED).build(),
                Field.newBuilder("labels", StandardSQLTypeName.BYTES).setMode(Field.Mode.REPEATED).build());
    }

    private static FieldValueList repeatedBinaryTimestampRow(final FieldList fields) {
        final FieldValue bytes = FieldValue.of(FieldValue.Attribute.REPEATED,
                List.of(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "AQID"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "BAU=")));
        final FieldValue times = FieldValue.of(FieldValue.Attribute.REPEATED,
                List.of(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1718900000.123456"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "-0.000001")));
        return FieldValueList.of(List.of(bytes, bytes, times, times, bytes), fields);
    }

    private static void assertRepeatedBinaryTimestampEntity(final RepeatedBinaryTimestampEntity entity) {
        assertTrue(Arrays.equals(new byte[] { 1, 2, 3 }, entity.getData().get(0)));
        assertTrue(Arrays.equals(new byte[] { 4, 5 }, entity.getData().get(1)));
        assertEquals(java.nio.ByteBuffer.wrap(new byte[] { 1, 2, 3 }), entity.getBuffers().get(0));
        final java.time.Instant expected = java.time.Instant.ofEpochSecond(1_718_900_000L, 123_456_000L);
        assertEquals(expected, entity.getTimestamps().get(0).toInstant());
        assertEquals(expected, entity.getInstants()[0]);
        assertEquals(java.time.Instant.ofEpochSecond(-1, 999_999_000), entity.getTimestamps().get(1).toInstant());
        assertEquals(List.of("AQID", "BAU="), entity.getLabels());
    }

    @com.landawn.abacus.annotation.Entity
    public static class ComputedBaseEntity {
        @com.landawn.abacus.annotation.Id
        private String id;
        private String name;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public String getComputed() {
            return "c-" + name;
        }
    }

    public static class ComputedSubEntity extends ComputedBaseEntity {
    }

    public static class RepeatedBinaryTimestampEntity {
        private List<byte[]> data;
        private List<java.nio.ByteBuffer> buffers;
        private List<java.sql.Timestamp> timestamps;
        private java.time.Instant[] instants;
        private List<String> labels;

        public List<byte[]> getData() {
            return data;
        }

        public void setData(final List<byte[]> data) {
            this.data = data;
        }

        public List<java.nio.ByteBuffer> getBuffers() {
            return buffers;
        }

        public void setBuffers(final List<java.nio.ByteBuffer> buffers) {
            this.buffers = buffers;
        }

        public List<java.sql.Timestamp> getTimestamps() {
            return timestamps;
        }

        public void setTimestamps(final List<java.sql.Timestamp> timestamps) {
            this.timestamps = timestamps;
        }

        public java.time.Instant[] getInstants() {
            return instants;
        }

        public void setInstants(final java.time.Instant[] instants) {
            this.instants = instants;
        }

        public List<String> getLabels() {
            return labels;
        }

        public void setLabels(final List<String> labels) {
            this.labels = labels;
        }
    }

    // Entity with a List<String> property targeted by a REPEATED column.
    public static class TestEntityWithTags {
        private int id;
        private List<String> tags;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public List<String> getTags() {
            return tags;
        }

        public void setTags(List<String> tags) {
            this.tags = tags;
        }
    }

    public static class VisitHolder {
        private List<Visit> visits;
        private Visit[] visitArray;

        public List<Visit> getVisits() {
            return visits;
        }

        public void setVisits(final List<Visit> visits) {
            this.visits = visits;
        }

        public Visit[] getVisitArray() {
            return visitArray;
        }

        public void setVisitArray(final Visit[] visitArray) {
            this.visitArray = visitArray;
        }
    }

    private static FieldValue verifyVisit(final String city, final String zip) {
        return FieldValue.of(FieldValue.Attribute.RECORD,
                FieldValueList.of(Arrays.asList(verifyCell(city), verifyCell(zip), verifyCell("1718900000.123456")), verifyVisitFields()));
    }

    private static FieldList verifyContainerFields() {
        return FieldList.of(Field.newBuilder("counts", StandardSQLTypeName.STRUCT, Field.of("x", StandardSQLTypeName.INT64)).build(),
                Field.newBuilder("pair", StandardSQLTypeName.STRUCT, Field.of("a", StandardSQLTypeName.INT64), Field.of("b", StandardSQLTypeName.INT64))
                        .build(),
                Field.newBuilder("nested", StandardSQLTypeName.STRUCT,
                        Field.newBuilder("m", StandardSQLTypeName.STRUCT, Field.of("x", StandardSQLTypeName.INT64)).build()).build(),
                Field.newBuilder("raw", StandardSQLTypeName.STRUCT, Field.of("x", StandardSQLTypeName.INT64)).build());
    }

    private static FieldValueList verifyContainerRow(final FieldList fields) {
        final FieldValue x11 = FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(Arrays.asList(verifyCell("11")), fields.get(0).getSubFields()));

        return FieldValueList.of(Arrays.asList(x11,
                FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(Arrays.asList(verifyCell("1"), verifyCell("2")), fields.get(1).getSubFields())),
                FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(Arrays.asList(x11), fields.get(2).getSubFields())), x11), fields);
    }

    public static class TypedContainers {
        private Map<String, Long> counts;
        private List<Long> pair;
        private Map<String, Map<String, Long>> nested;
        private Map<String, Object> raw;

        public Map<String, Long> getCounts() {
            return counts;
        }

        public void setCounts(final Map<String, Long> counts) {
            this.counts = counts;
        }

        public List<Long> getPair() {
            return pair;
        }

        public void setPair(final List<Long> pair) {
            this.pair = pair;
        }

        public Map<String, Map<String, Long>> getNested() {
            return nested;
        }

        public void setNested(final Map<String, Map<String, Long>> nested) {
            this.nested = nested;
        }

        public Map<String, Object> getRaw() {
            return raw;
        }

        public void setRaw(final Map<String, Object> raw) {
            this.raw = raw;
        }
    }

    public static class TypedContainersHolder {
        private List<TypedContainers> items;
        private TypedContainers single;

        public List<TypedContainers> getItems() {
            return items;
        }

        public void setItems(final List<TypedContainers> items) {
            this.items = items;
        }

        public TypedContainers getSingle() {
            return single;
        }

        public void setSingle(final TypedContainers single) {
            this.single = single;
        }
    }

    public static class RecordHolder {
        private List<VisitRecord> visits;

        public List<VisitRecord> getVisits() {
            return visits;
        }

        public void setVisits(final List<VisitRecord> visits) {
            this.visits = visits;
        }
    }

    public static class VisitContainers {
        private Set<Visit> visitSet;
        private Collection<Visit> visitColl;
        private List<Visit> visits;
        private Visit[] visitArray;

        public Set<Visit> getVisitSet() {
            return visitSet;
        }

        public void setVisitSet(final Set<Visit> visitSet) {
            this.visitSet = visitSet;
        }

        public Collection<Visit> getVisitColl() {
            return visitColl;
        }

        public void setVisitColl(final Collection<Visit> visitColl) {
            this.visitColl = visitColl;
        }

        public List<Visit> getVisits() {
            return visits;
        }

        public void setVisits(final List<Visit> visits) {
            this.visits = visits;
        }

        public Visit[] getVisitArray() {
            return visitArray;
        }

        public void setVisitArray(final Visit[] visitArray) {
            this.visitArray = visitArray;
        }
    }

    public static class Trip {
        private String name;
        private List<Visit> visits;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public List<Visit> getVisits() {
            return visits;
        }

        public void setVisits(final List<Visit> visits) {
            this.visits = visits;
        }
    }

    public static class TripHolder {
        private Trip trip;
        private List<Trip> trips;

        public Trip getTrip() {
            return trip;
        }

        public void setTrip(final Trip trip) {
            this.trip = trip;
        }

        public List<Trip> getTrips() {
            return trips;
        }

        public void setTrips(final List<Trip> trips) {
            this.trips = trips;
        }
    }

    public static class MapElementHolder {
        private List<Map<String, Object>> visits;
        private List<java.util.GregorianCalendar> calendars;

        public List<Map<String, Object>> getVisits() {
            return visits;
        }

        public void setVisits(final List<Map<String, Object>> visits) {
            this.visits = visits;
        }

        public List<java.util.GregorianCalendar> getCalendars() {
            return calendars;
        }

        public void setCalendars(final List<java.util.GregorianCalendar> calendars) {
            this.calendars = calendars;
        }
    }

    private static FieldList fixBqVisitFields() {
        return FieldList
                .of(Field.of("city", StandardSQLTypeName.STRING), Field.of("seen_at", StandardSQLTypeName.TIMESTAMP),
                        Field.of("blob", StandardSQLTypeName.BYTES), Field.newBuilder("loc", StandardSQLTypeName.STRUCT, fixBqLocFields()).build(),
                        Field.newBuilder("loc_map", StandardSQLTypeName.STRUCT, fixBqLocFields()).build(),
                        Field.newBuilder("counts", StandardSQLTypeName.STRUCT, Field.of("x", StandardSQLTypeName.INT64)).build(),
                        fixBqRepeatedField("codes", StandardSQLTypeName.INT64), Field
                                .newBuilder("stop", StandardSQLTypeName.STRUCT, Field.of("name", StandardSQLTypeName.STRING),
                                        fixBqRepeatedField("codes", StandardSQLTypeName.INT64))
                                .build());
    }

    // One element of a REPEATED STRUCT<fixBqVisitFields> column as the client decodes it: no schema at any level, a REPEATED
    // sub-field, and a STRUCT sub-field holding a REPEATED (REPEATED in STRUCT in REPEATED).
    private static FieldValue fixBqVisit(final String city) {
        return fixBqRecord(verifyCell(city), verifyCell("1718900000.123456"), verifyCell("AQID"), fixBqRecord(verifyCell("37.5"), verifyCell("-122.25")),
                fixBqRecord(verifyCell("1.5"), verifyCell("2.5")), fixBqRecord(verifyCell("11")), fixBqRepeated(verifyCell("1"), verifyCell("2")),
                fixBqRecord(verifyCell("s1"), fixBqRepeated(verifyCell("7"), verifyCell("8"))));
    }

    public static class FixBqPrimitives {
        private List<String> tags;
        private String[] tagArray;
        private Set<Long> nums;
        private Long[] numArray;
        private List<java.time.Instant> times;
        private java.time.Instant[] timeArray;
        private List<byte[]> blobs;
        private Set<java.sql.Time> slots;

        public List<String> getTags() {
            return tags;
        }

        public void setTags(final List<String> tags) {
            this.tags = tags;
        }

        public String[] getTagArray() {
            return tagArray;
        }

        public void setTagArray(final String[] tagArray) {
            this.tagArray = tagArray;
        }

        public Set<Long> getNums() {
            return nums;
        }

        public void setNums(final Set<Long> nums) {
            this.nums = nums;
        }

        public Long[] getNumArray() {
            return numArray;
        }

        public void setNumArray(final Long[] numArray) {
            this.numArray = numArray;
        }

        public List<java.time.Instant> getTimes() {
            return times;
        }

        public void setTimes(final List<java.time.Instant> times) {
            this.times = times;
        }

        public java.time.Instant[] getTimeArray() {
            return timeArray;
        }

        public void setTimeArray(final java.time.Instant[] timeArray) {
            this.timeArray = timeArray;
        }

        public List<byte[]> getBlobs() {
            return blobs;
        }

        public void setBlobs(final List<byte[]> blobs) {
            this.blobs = blobs;
        }

        public Set<java.sql.Time> getSlots() {
            return slots;
        }

        public void setSlots(final Set<java.sql.Time> slots) {
            this.slots = slots;
        }
    }

    public static class FixBqStop {
        private String name;
        private List<Long> codes;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public List<Long> getCodes() {
            return codes;
        }

        public void setCodes(final List<Long> codes) {
            this.codes = codes;
        }
    }

    public static class FixBqPlace {
        private String city;
        private FixBqLoc loc;
        private Map<String, Object> locMap;
        private Map<String, Long> counts;

        public String getCity() {
            return city;
        }

        public void setCity(final String city) {
            this.city = city;
        }

        public FixBqLoc getLoc() {
            return loc;
        }

        public void setLoc(final FixBqLoc loc) {
            this.loc = loc;
        }

        public Map<String, Object> getLocMap() {
            return locMap;
        }

        public void setLocMap(final Map<String, Object> locMap) {
            this.locMap = locMap;
        }

        public Map<String, Long> getCounts() {
            return counts;
        }

        public void setCounts(final Map<String, Long> counts) {
            this.counts = counts;
        }
    }

    public static class FixBqVisit extends FixBqPlace {
        private java.time.Instant seenAt;
        private byte[] blob;
        private List<Long> codes;
        private FixBqStop stop;

        public java.time.Instant getSeenAt() {
            return seenAt;
        }

        public void setSeenAt(final java.time.Instant seenAt) {
            this.seenAt = seenAt;
        }

        public byte[] getBlob() {
            return blob;
        }

        public void setBlob(final byte[] blob) {
            this.blob = blob;
        }

        public List<Long> getCodes() {
            return codes;
        }

        public void setCodes(final List<Long> codes) {
            this.codes = codes;
        }

        public FixBqStop getStop() {
            return stop;
        }

        public void setStop(final FixBqStop stop) {
            this.stop = stop;
        }
    }

    public static class FixBqVisitHolder {
        private long id;
        private List<FixBqVisit> visits;
        private FixBqVisit[] visitArray;

        public long getId() {
            return id;
        }

        public void setId(final long id) {
            this.id = id;
        }

        public List<FixBqVisit> getVisits() {
            return visits;
        }

        public void setVisits(final List<FixBqVisit> visits) {
            this.visits = visits;
        }

        public FixBqVisit[] getVisitArray() {
            return visitArray;
        }

        public void setVisitArray(final FixBqVisit[] visitArray) {
            this.visitArray = visitArray;
        }
    }

    public static class FixBqPlaceHolder {
        private List<FixBqPlace> places;

        public List<FixBqPlace> getPlaces() {
            return places;
        }

        public void setPlaces(final List<FixBqPlace> places) {
            this.places = places;
        }
    }

    public static class FixBqRaw {
        private FieldValueList home;
        private FieldValueList tags;

        public FieldValueList getHome() {
            return home;
        }

        public void setHome(final FieldValueList home) {
            this.home = home;
        }

        public FieldValueList getTags() {
            return tags;
        }

        public void setTags(final FieldValueList tags) {
            this.tags = tags;
        }
    }
}
