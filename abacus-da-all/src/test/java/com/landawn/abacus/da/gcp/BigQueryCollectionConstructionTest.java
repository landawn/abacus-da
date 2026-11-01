package com.landawn.abacus.da.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableResult;
import com.landawn.abacus.annotation.JsonXmlField;
import com.landawn.abacus.da.AllocationAssertions;
import com.landawn.abacus.util.N;

class BigQueryCollectionConstructionTest {
    @Test
    void boundedQueueRowsWorkForListStreamAndRegisteredConversion() throws Exception {
        final FieldValueList row = stringRow("alpha", "beta");
        final TableResult result = result(row);
        final BigQuery client = mock(BigQuery.class);
        when(client.query(any(QueryJobConfiguration.class))).thenReturn(result);
        final BigQueryExecutor executor = new BigQueryExecutor(client);

        final ArrayBlockingQueue<?> eager = BigQueryExecutor.toList(result, ArrayBlockingQueue.class).get(0);
        final ArrayBlockingQueue<?> streamed = executor.stream(ArrayBlockingQueue.class, "SELECT a, b FROM dataset.t").toList().get(0);
        final ArrayBlockingQueue<?> converted = N.convert(row, ArrayBlockingQueue.class);

        for (final ArrayBlockingQueue<?> queue : List.of(eager, streamed, converted)) {
            assertEquals(List.of("alpha", "beta"), new ArrayList<>(queue));
            assertEquals(0, queue.remainingCapacity());
        }
    }

    @Test
    void emptyQueueRowHasUsableCapacity() {
        final ArrayBlockingQueue<?> queue = BigQueryExecutor.toList(result(stringRow()), ArrayBlockingQueue.class).get(0);

        assertTrue(queue.isEmpty());
        assertEquals(1, queue.remainingCapacity());
    }

    @Test
    void collectionNullRulesArePreserved() {
        final FieldValueList row = stringRow("alpha", null, "alpha");

        final CopyOnWriteArrayList<?> values = BigQueryExecutor.toList(result(row), CopyOnWriteArrayList.class).get(0);

        assertEquals(Arrays.asList("alpha", null, "alpha"), values);
        assertThrows(NullPointerException.class, () -> BigQueryExecutor.toList(result(row), ArrayBlockingQueue.class));
    }

    @Test
    void repeatedNativeValuesKeepRequestedCollectionTypeAndPrecision() {
        final Field field = Field.newBuilder("values", StandardSQLTypeName.TIMESTAMP).setMode(Field.Mode.REPEATED).build();
        final FieldList fields = FieldList.of(field);
        final FieldValue repeated = FieldValue.of(FieldValue.Attribute.REPEATED, FieldValueList.of(List.of(cell("1.123456"), cell("2.654321"))));
        final FieldValueList row = FieldValueList.of(List.of(repeated), fields);
        final List<Instant> expected = List.of(Instant.ofEpochSecond(1, 123456000), Instant.ofEpochSecond(2, 654321000));

        final QueueValues queue = BigQueryExecutor.toEntity(fields, row, QueueValues.class);
        final CopyOnWriteValues copyOnWrite = BigQueryExecutor.toEntity(fields, row, CopyOnWriteValues.class);

        assertInstanceOf(ArrayBlockingQueue.class, queue.values());
        assertEquals(expected, new ArrayList<>(queue.values()));
        assertInstanceOf(CopyOnWriteArrayList.class, copyOnWrite.values());
        assertEquals(expected, copyOnWrite.values());
    }

    @Test
    void emptyRepeatedNativeValuesProduceAnEmptyQueue() {
        final FieldList fields = FieldList.of(Field.newBuilder("values", StandardSQLTypeName.TIMESTAMP).setMode(Field.Mode.REPEATED).build());
        final FieldValueList row = FieldValueList.of(List.of(FieldValue.of(FieldValue.Attribute.REPEATED, FieldValueList.of(List.of()))), fields);

        final QueueValues result = BigQueryExecutor.toEntity(fields, row, QueueValues.class);

        assertTrue(result.values().isEmpty());
        assertEquals(1, result.values().remainingCapacity());
    }

    @Test
    void repeatedCodecValuesIntoQueuePropertiesAreSizedForEveryElement() {
        final FieldList longs = repeatedFields(StandardSQLTypeName.INT64);
        final FieldValueList longRow = repeatedRow(longs, cell("1"), cell("2"));
        final FieldList strings = repeatedFields(StandardSQLTypeName.STRING);
        final FieldValueList stringRow = repeatedRow(strings, cell("a"), cell("b"), cell("a"));

        final ArrayBlockingQueue<Long> recordQueue = BigQueryExecutor.toEntity(longs, longRow, LongQueueRecord.class).values();
        final ArrayBlockingQueue<Long> beanQueue = BigQueryExecutor.toEntity(longs, longRow, LongQueueBean.class).getValues();
        final ArrayBlockingQueue<String> stringQueue = BigQueryExecutor.toEntity(strings, stringRow, StringQueueRecord.class).values();

        for (final ArrayBlockingQueue<Long> queue : List.of(recordQueue, beanQueue)) {
            assertEquals(List.of(1L, 2L), new ArrayList<>(queue));
            assertEquals(0, queue.remainingCapacity());
        }

        assertEquals(List.of("a", "b", "a"), new ArrayList<>(stringQueue));
        assertEquals(0, stringQueue.remainingCapacity());
    }

    @Test
    void emptyRepeatedCodecValuesProduceUsableQueues() {
        final FieldList fields = repeatedFields(StandardSQLTypeName.INT64);
        final FieldValueList row = repeatedRow(fields);

        for (final ArrayBlockingQueue<?> queue : List.of(BigQueryExecutor.toEntity(fields, row, LongQueueRecord.class).values(),
                BigQueryExecutor.toEntity(fields, row, LongQueueBean.class).getValues(),
                BigQueryExecutor.toEntity(fields, row, RawQueueRecord.class).values())) {
            assertTrue(queue.isEmpty());
            assertEquals(1, queue.remainingCapacity());
        }
    }

    @Test
    void rawQueuePropertyKeepsTheRawCellValues() {
        final FieldList fields = repeatedFields(StandardSQLTypeName.INT64);

        final ArrayBlockingQueue<?> queue = BigQueryExecutor.toEntity(fields, repeatedRow(fields, cell("1"), cell("2")), RawQueueRecord.class).values();

        assertEquals(List.of("1", "2"), new ArrayList<>(queue));
        assertEquals(0, queue.remainingCapacity());
    }

    @Test
    void structIntoTypedQueuePropertyConvertsItsFieldValues() {
        final FieldList subFields = FieldList.of(Field.of("a", StandardSQLTypeName.INT64), Field.of("b", StandardSQLTypeName.INT64));
        final FieldList fields = FieldList.of(Field.newBuilder("values", StandardSQLTypeName.STRUCT, subFields).build());
        final FieldValueList row = FieldValueList
                .of(List.of(FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(List.of(cell("1"), cell("2")), subFields))), fields);

        final ArrayBlockingQueue<Long> queue = BigQueryExecutor.toEntity(fields, row, LongQueueRecord.class).values();

        assertEquals(List.of(1L, 2L), new ArrayList<>(queue));
        assertEquals(0, queue.remainingCapacity());
    }

    @Test
    void repeatedStructsIntoQueueOfListsKeepTheNestedElementType() {
        final FieldList subFields = FieldList.of(Field.of("a", StandardSQLTypeName.INT64), Field.of("b", StandardSQLTypeName.INT64));
        final FieldList fields = FieldList.of(Field.newBuilder("values", StandardSQLTypeName.STRUCT, subFields).setMode(Field.Mode.REPEATED).build());
        final FieldValueList row = repeatedRow(fields, FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(List.of(cell("1"), cell("2")), subFields)),
                FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(List.of(cell("3"), cell("4")), subFields)));

        final ArrayBlockingQueue<List<Long>> queue = BigQueryExecutor.toEntity(fields, row, NestedQueueRecord.class).values();

        assertEquals(List.of(List.of(1L, 2L), List.of(3L, 4L)), new ArrayList<>(queue));
    }

    @Test
    void queuePropertiesRejectNullRepeatedElements() {
        final FieldList fields = repeatedFields(StandardSQLTypeName.INT64);

        assertThrows(NullPointerException.class, () -> BigQueryExecutor.toEntity(fields, repeatedRow(fields, cell("1"), cell(null)), LongQueueRecord.class));
    }

    @Test
    void recordWithCodecDecodedRepeatedValuesKeepsTypedElements() {
        final FieldList longs = repeatedFields(StandardSQLTypeName.INT64);
        final FieldValueList row = repeatedRow(longs, cell("1"), cell("2"));

        assertEquals(List.of(1L, 2L), BigQueryExecutor.toEntity(longs, row, LongListRecord.class).values());
        assertEquals(List.of("1", "2"), BigQueryExecutor.toEntity(longs, row, RawListRecord.class).values());
    }

    @Test
    void scalarRecordStillHonorsConfiguredPropertyType() {
        final FieldList fields = FieldList.of(Field.of("value", StandardSQLTypeName.STRING));
        final FieldValueList row = FieldValueList.of(List.of(cell("7")), fields);

        assertEquals(7L, BigQueryExecutor.toEntity(fields, row, TypedValue.class).value());
    }

    @Test
    void copyOnWriteRowAllocationGrowsLinearly() {
        final FieldValueList small = wideRow(128);
        final FieldValueList large = wideRow(512);
        BigQueryExecutor.toMap(small); // Initialize the registered FieldValueList converter before measuring it.

        AllocationAssertions.assertLinearAllocation("BigQuery copy-on-write rows", () -> N.convert(small, CopyOnWriteArrayList.class),
                () -> N.convert(large, CopyOnWriteArrayList.class));
    }

    private static FieldValueList wideRow(final int size) {
        final String[] values = new String[size];
        Arrays.fill(values, "value");
        return stringRow(values);
    }

    private static FieldValueList stringRow(final String... values) {
        final List<Field> fields = new ArrayList<>();
        final List<FieldValue> cells = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            fields.add(Field.of("c" + i, StandardSQLTypeName.STRING));
            cells.add(cell(values[i]));
        }
        return FieldValueList.of(cells, FieldList.of(fields));
    }

    private static FieldList repeatedFields(final StandardSQLTypeName type) {
        return FieldList.of(Field.newBuilder("values", type).setMode(Field.Mode.REPEATED).build());
    }

    private static FieldValueList repeatedRow(final FieldList fields, final FieldValue... elements) {
        return FieldValueList.of(List.of(FieldValue.of(FieldValue.Attribute.REPEATED, FieldValueList.of(Arrays.asList(elements)))), fields);
    }

    private static FieldValue cell(final String value) {
        return FieldValue.of(FieldValue.Attribute.PRIMITIVE, value);
    }

    private static TableResult result(final FieldValueList row) {
        final TableResult result = mock(TableResult.class);
        when(result.getSchema()).thenReturn(Schema.of(BigQueryExecutor.getSchema(row)));
        when(result.getTotalRows()).thenReturn(1L);
        when(result.iterateAll()).thenReturn(List.of(row));
        return result;
    }

    public record QueueValues(ArrayBlockingQueue<Instant> values) {
    }

    public record CopyOnWriteValues(CopyOnWriteArrayList<Instant> values) {
    }

    public record LongQueueRecord(ArrayBlockingQueue<Long> values) {
    }

    public record StringQueueRecord(ArrayBlockingQueue<String> values) {
    }

    public record NestedQueueRecord(ArrayBlockingQueue<List<Long>> values) {
    }

    @SuppressWarnings("rawtypes")
    public record RawQueueRecord(ArrayBlockingQueue values) {
    }

    public record LongListRecord(List<Long> values) {
    }

    @SuppressWarnings("rawtypes")
    public record RawListRecord(List values) {
    }

    public static class LongQueueBean {
        private ArrayBlockingQueue<Long> values;

        public ArrayBlockingQueue<Long> getValues() {
            return values;
        }

        public void setValues(final ArrayBlockingQueue<Long> values) {
            this.values = values;
        }
    }

    public record TypedValue(@JsonXmlField(type = "java.lang.Long") Object value) {
    }
}
