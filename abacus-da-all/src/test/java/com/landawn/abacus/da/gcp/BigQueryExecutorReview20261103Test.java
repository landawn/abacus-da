package com.landawn.abacus.da.gcp;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableResult;
import com.landawn.abacus.da.AllocationAssertions;
import com.landawn.abacus.da.gcp.BigQueryExecutorDottedPathReview20261102Test.Address;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.N;

/**
 * Review 2026-11-03 regressions for {@link BigQueryExecutor}.
 */
public class BigQueryExecutorReview20261103Test extends BigQueryExecutorTestSupport {

    private static FieldList abFields() {
        return FieldList.of(Field.of("a", StandardSQLTypeName.INT64), Field.of("b", StandardSQLTypeName.STRING));
    }

    // (a = 1, b = NULL)
    private static FieldValueList nullCellRow() {
        return FieldValueList.of(Arrays.asList(verifyCell("1"), verifyCell(null)), abFields());
    }

    private static void assertRejectsField(final Class<? extends Throwable> cause, final String field, final Executable call) {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, call);
        assertInstanceOf(cause, e.getCause(), e.toString());
        assertTrue(e.getMessage().startsWith("Field (" + field + ", "), e.getMessage());
    }

    // A NULL cell read into a null-rejecting row map (ConcurrentHashMap, Hashtable) or row collection (ArrayBlockingQueue,
    // ArrayDeque, ConcurrentLinkedQueue, TreeSet) failed with the JDK container's bare NullPointerException, without saying which
    // column the requested row type rejected. The Cassandra and DynamoDB executors name the column; BigQuery now does as well, on
    // every row path (toList, stream, the registered N.convert converter, and the public toMap with a caller's map supplier).
    @Test
    public void nullCellInNullRejectingRowContainerNamesTheField() throws Exception {
        fixBqStubResult(abFields(), nullCellRow());
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        final List<Class<?>> rowClasses = Arrays.asList(ConcurrentHashMap.class, Hashtable.class, ArrayBlockingQueue.class, ArrayDeque.class,
                ConcurrentLinkedQueue.class, TreeSet.class);
        final List<Executable> checks = new ArrayList<>();

        for (final Class<?> rowClass : rowClasses) {
            checks.add(() -> assertRejectsField(NullPointerException.class, "b", () -> BigQueryExecutor.toList(mockTableResult, rowClass)));
            checks.add(() -> assertRejectsField(NullPointerException.class, "b", () -> executor.stream(rowClass, "SELECT a, b FROM ds.t").toList()));
            checks.add(() -> assertRejectsField(NullPointerException.class, "b", () -> N.convert(nullCellRow(), rowClass)));
        }

        checks.add(() -> assertRejectsField(NullPointerException.class, "b",
                () -> BigQueryExecutor.toMap(abFields(), nullCellRow(), n -> new ConcurrentHashMap<>())));
        checks.add(() -> assertRejectsField(NullPointerException.class, "b", () -> BigQueryExecutor.toMap(nullCellRow(), n -> new Hashtable<>())));

        assertAll(checks);
    }

    // A STRUCT nested in a map row is read with the same map supplier, so a NULL field of the STRUCT names that field.
    @Test
    public void nullStructFieldInNullRejectingMapNamesTheNestedField() {
        final FieldList sub = FieldList.of(Field.of("x", StandardSQLTypeName.INT64), Field.of("y", StandardSQLTypeName.INT64));
        final FieldList fields = FieldList.of(Field.of("id", StandardSQLTypeName.STRING), Field.newBuilder("pt", StandardSQLTypeName.STRUCT, sub).build());
        final FieldValueList row = FieldValueList.of(Arrays.asList(verifyCell("k"), fixBqRecord(verifyCell("1"), verifyCell(null))), fields);

        assertRejectsField(NullPointerException.class, "y", () -> BigQueryExecutor.toMap(fields, row, n -> new ConcurrentHashMap<>()));
    }

    // A row value that a sorted row collection cannot compare (a REPEATED value next to text) names its field too.
    @Test
    public void uncomparableCellInSortedRowCollectionNamesTheField() {
        final FieldList fields = FieldList.of(Field.of("a", StandardSQLTypeName.STRING), fixBqRepeatedField("r", StandardSQLTypeName.INT64));
        final FieldValueList row = FieldValueList.of(Arrays.asList(verifyCell("1"), fixBqRepeated(verifyCell("2"))), fields);

        assertRejectsField(ClassCastException.class, "r", () -> N.convert(row, TreeSet.class));
    }

    // Unchanged: containers that accept null keep the NULL cell, and a null map from the supplier is still the documented NPE.
    @Test
    public void nullAcceptingRowContainersAndNullSupplierResultAreUnchanged() {
        final Map<?, ?> map = N.convert(nullCellRow(), HashMap.class);
        final List<?> list = N.convert(nullCellRow(), ArrayList.class);

        assertAll(() -> assertEquals(2, map.size()), () -> assertEquals("1", map.get("a")), () -> assertTrue(map.containsKey("b")),
                () -> assertNull(map.get("b")), () -> assertEquals(Arrays.asList("1", null), list),
                () -> assertThrows(NullPointerException.class, () -> BigQueryExecutor.toMap(abFields(), nullCellRow(), n -> null)));
    }

    // ---------------------------------------------------------------------------------------------------------------------
    // DML results: getTotalRows() is the affected-row count on a result that carries a schema
    // ---------------------------------------------------------------------------------------------------------------------

    /** A DML result as google-cloud-bigquery 2.74.0 builds it for jobs.query / Arrow: a schema, and totalRows = affected rows. */
    private static TableResult dmlResult(final long affectedRows, final List<FieldValueList> rows) {
        final TableResult result = mock(TableResult.class);
        when(result.getSchema()).thenReturn(Schema.of(abFields()));
        when(result.getTotalRows()).thenReturn(affectedRows);
        when(result.getNumDmlAffectedRows()).thenReturn(affectedRows);
        when(result.iterateAll()).thenReturn(rows);
        when(result.getValues()).thenReturn(rows);
        return result;
    }

    /**
     * Before the fix: toList and extractData skipped their row-count overflow guard only for a null schema, but the client
     * reports a DML statement's affected-row count as getTotalRows() on a result WITH a schema, so more than
     * Integer.MAX_VALUE affected rows failed both with ArithmeticException. A DML result now reads the rows it holds (none).
     */
    @Test
    public void dmlResultWithSchemaAndHugeAffectedRowCountIsEmpty() {
        final long affected = Integer.MAX_VALUE + 10L;

        assertAll(() -> assertTrue(BigQueryExecutor.toList(dmlResult(affected, List.of()), TestEntity.class).isEmpty()),
                () -> assertTrue(BigQueryExecutor.toList(dmlResult(affected, List.of()), Object[].class).isEmpty()), () -> {
                    final Dataset ds = BigQueryExecutor.extractData(dmlResult(affected, List.of()), TestEntity.class);
                    assertEquals(0, ds.size());
                    assertEquals(List.of("a", "b"), ds.columnNames());
                });
    }

    /**
     * Before the fix: a DML result's affected-row count pre-sized the result list (and every Dataset column), allocating
     * 4 bytes or more per affected row for a statement that returns no rows (tens of MB here, GBs for a large UPDATE).
     */
    @Test
    public void dmlResultDoesNotPreSizeToTheAffectedRowCount() {
        AllocationAssertions.assumeAllocationCountersAvailable();
        final TableResult none = dmlResult(0, List.of());
        final TableResult many = dmlResult(10_000_000L, List.of());

        AllocationAssertions.assertAllocationOverhead("BigQuery toList of a DML result", () -> BigQueryExecutor.toList(none, Object[].class),
                () -> BigQueryExecutor.toList(many, Object[].class), 1 << 20);
        AllocationAssertions.assertAllocationOverhead("BigQuery extractData of a DML result", () -> BigQueryExecutor.extractData(none, null),
                () -> BigQueryExecutor.extractData(many, null), 1 << 20);
    }

    /** A SELECT result (no DML count) whose getTotalRows() reports {@code totalRows} but whose pages hold no rows. */
    private static TableResult selectResultWithoutRows(final long totalRows) {
        final TableResult result = mock(TableResult.class);
        when(result.getSchema()).thenReturn(Schema.of(abFields()));
        when(result.getTotalRows()).thenReturn(totalRows);
        // Mockito answers 0L for the unstubbed Long getter, which would make this a DML result.
        when(result.getNumDmlAffectedRows()).thenReturn(null);
        when(result.iterateAll()).thenReturn(List.of());
        when(result.getValues()).thenReturn(List.of());
        return result;
    }

    /**
     * Before the fix: a SELECT result pre-sized the result list (and every Dataset column) to getTotalRows(), which is only a
     * sizing hint (an Arrow result reports the read session's estimate): 10 million reported rows allocated tens of MB before
     * a single row was read. The pre-sized capacity is now capped at 16384 rows.
     */
    @Test
    public void selectResultDoesNotPreSizeBeyondTheCap() {
        AllocationAssertions.assumeAllocationCountersAvailable();
        final TableResult one = selectResultWithoutRows(1);
        final TableResult many = selectResultWithoutRows(10_000_000L);

        AllocationAssertions.assertAllocationOverhead("BigQuery toList of a SELECT result", () -> BigQueryExecutor.toList(one, Object[].class),
                () -> BigQueryExecutor.toList(many, Object[].class), 1 << 20);
        AllocationAssertions.assertAllocationOverhead("BigQuery extractData of a SELECT result", () -> BigQueryExecutor.extractData(one, null),
                () -> BigQueryExecutor.extractData(many, null), 1 << 20);
    }

    /**
     * A result with a DML count is decided by its rows, not by getTotalRows(): rows it does hold are still read (the count is
     * not a row count either way), and queryForSingleValue/queryForSingleNonNull read the first row although getTotalRows()
     * is 0. Before the fix those two returned empty for such a result without looking at its rows.
     */
    @Test
    public void dmlCountedResultIsDecidedByItsRows() throws Exception {
        final TableResult withRow = dmlResult(0, List.of(nullCellRow()));
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(withRow);

        assertAll(() -> assertEquals(1, BigQueryExecutor.toList(withRow, Object[].class).size()),
                () -> assertEquals(1, BigQueryExecutor.extractData(withRow, null).size()),
                () -> assertEquals(Long.valueOf(1), executor.queryForSingleValue(Long.class, "SELECT a, b FROM ds.t").get()),
                () -> assertEquals(Long.valueOf(1), executor.queryForSingleNonNull(Long.class, "SELECT a, b FROM ds.t").get()));

        final TableResult noRows = dmlResult(7, List.of());
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(noRows);

        assertAll(() -> assertFalse(executor.queryForSingleValue(Long.class, "UPDATE ds.t SET a = 1 WHERE TRUE").isPresent()),
                () -> assertFalse(executor.queryForSingleNonNull(Long.class, "UPDATE ds.t SET a = 1 WHERE TRUE").isPresent()));
    }

    /**
     * Unchanged: a SELECT result (no DML count) reporting no rows takes the queryForSingleValue/queryForSingleNonNull shortcut
     * and never reads its values (here stubbed with a row, so reading them would return a value).
     */
    @Test
    public void selectResultWithNoRowsTakesTheSingleValueShortcut() throws Exception {
        fixBqStubResult(abFields(), nullCellRow());
        when(mockTableResult.getTotalRows()).thenReturn(0L);

        assertAll(() -> assertFalse(executor.queryForSingleValue(Long.class, "SELECT a, b FROM ds.t").isPresent()),
                () -> assertFalse(executor.queryForSingleNonNull(Long.class, "SELECT a, b FROM ds.t").isPresent()));
        verify(mockTableResult, never()).getValues();

        // with one reported row the values are read
        when(mockTableResult.getTotalRows()).thenReturn(1L);
        assertEquals(Long.valueOf(1), executor.queryForSingleValue(Long.class, "SELECT a, b FROM ds.t").get());
        verify(mockTableResult).getValues();
    }

    /** Unchanged: a SELECT result (no DML count) is sized by getTotalRows(), and a count above Integer.MAX_VALUE still fails. */
    @Test
    public void selectResultIsUnaffected() throws Exception {
        fixBqStubResult(abFields(), nullCellRow()); // no DML count: stubbed null by the fixture

        assertAll(() -> assertEquals(1, BigQueryExecutor.toList(mockTableResult, Object[].class).size()),
                () -> assertEquals(1, BigQueryExecutor.extractData(mockTableResult, null).size()));

        when(mockTableResult.getTotalRows()).thenReturn(0L);
        assertTrue(BigQueryExecutor.toList(mockTableResult, Object[].class).isEmpty());

        when(mockTableResult.getTotalRows()).thenReturn(Integer.MAX_VALUE + 1L);
        assertAll(() -> assertThrows(ArithmeticException.class, () -> BigQueryExecutor.toList(mockTableResult, Object[].class)),
                () -> assertThrows(ArithmeticException.class, () -> BigQueryExecutor.extractData(mockTableResult, null)));
    }

    // ---------------------------------------------------------------------------------------------------------------------
    // ArrayBlockingQueue property: a rejected NULL element names its field
    // ---------------------------------------------------------------------------------------------------------------------

    private static FieldValueList structRow(final FieldList fields, final FieldList subFields, final String a, final String b) {
        return FieldValueList.of(List.of(FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(Arrays.asList(verifyCell(a), verifyCell(b)), subFields))),
                fields);
    }

    /**
     * Before the fix: a STRUCT with a NULL field read into an ArrayBlockingQueue property failed with the bare
     * NullPointerException of queue.addAll, while a row of that class names the rejected column. It now names the sub-field.
     */
    @Test
    public void nullStructFieldIntoQueuePropertyNamesTheField() {
        final FieldList subFields = FieldList.of(Field.of("a", StandardSQLTypeName.INT64), Field.of("b", StandardSQLTypeName.INT64));
        final FieldList fields = FieldList.of(Field.newBuilder("values", StandardSQLTypeName.STRUCT, subFields).build());

        assertAll(
                () -> assertRejectsField(NullPointerException.class, "b",
                        () -> BigQueryExecutor.toEntity(fields, structRow(fields, subFields, "1", null),
                                BigQueryCollectionConstructionTest.LongQueueRecord.class)),
                () -> assertRejectsField(NullPointerException.class, "a",
                        () -> BigQueryExecutor.toEntity(fields, structRow(fields, subFields, null, "2"),
                                BigQueryCollectionConstructionTest.RawQueueRecord.class)),
                () -> assertRejectsField(NullPointerException.class, "b", () -> BigQueryExecutor.toEntity(fields, structRow(fields, subFields, "1", null),
                        BigQueryCollectionConstructionTest.LongQueueBean.class)));

        // Positive: a STRUCT without NULL fields still fills the queue.
        assertEquals(List.of(1L, 2L), new ArrayList<>(
                BigQueryExecutor.toEntity(fields, structRow(fields, subFields, "1", "2"), BigQueryCollectionConstructionTest.LongQueueRecord.class).values()));
    }

    /** A NULL element of a decoded REPEATED value (TIMESTAMP into ArrayBlockingQueue&lt;Instant&gt;) names the field the same way. */
    @Test
    public void nullDecodedRepeatedElementIntoQueuePropertyNamesTheField() {
        final FieldList fields = FieldList.of(Field.newBuilder("values", StandardSQLTypeName.TIMESTAMP).setMode(Field.Mode.REPEATED).build());
        final FieldValueList row = FieldValueList
                .of(List.of(FieldValue.of(FieldValue.Attribute.REPEATED, FieldValueList.of(Arrays.asList(verifyCell("1.5"), verifyCell(null))))), fields);

        assertRejectsField(NullPointerException.class, "values",
                () -> BigQueryExecutor.toEntity(fields, row, BigQueryCollectionConstructionTest.QueueValues.class));
    }

    public record AddressTreeSet(java.util.TreeSet<Address> addresses) {
    }

    public record AddressList2(List<Address> addresses) {
    }

    public record InstantDeque(ArrayDeque<java.time.Instant> values) {
    }

    /**
     * Before the fix: a REPEATED STRUCT read into a TreeSet&lt;Bean&gt; property (a bean that is not Comparable) failed with the
     * TreeSet's bare ClassCastException, and a NULL decoded element in an ArrayDeque property with a bare NullPointerException.
     * Both now fail with IllegalArgumentException naming the field, as rows of those classes do (addRowValue).
     */
    @Test
    public void decodedRepeatedElementRejectedByAnyPropertyCollectionNamesTheField() {
        final FieldList sub = FieldList.of(Field.of("city", StandardSQLTypeName.STRING), Field.of("zip", StandardSQLTypeName.INT64));
        final FieldList fields = FieldList.of(Field.newBuilder("addresses", StandardSQLTypeName.STRUCT, sub).setMode(Field.Mode.REPEATED).build());
        final FieldValueList row = FieldValueList.of(
                List.of(FieldValue.of(FieldValue.Attribute.REPEATED,
                        FieldValueList.of(
                                List.of(FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(List.of(verifyCell("Paris"), verifyCell("1")), sub)))))),
                fields);

        final FieldList tsFields = FieldList.of(Field.newBuilder("values", StandardSQLTypeName.TIMESTAMP).setMode(Field.Mode.REPEATED).build());
        final FieldValueList tsRow = FieldValueList
                .of(List.of(FieldValue.of(FieldValue.Attribute.REPEATED, FieldValueList.of(Arrays.asList(verifyCell("1.5"), verifyCell(null))))), tsFields);

        assertAll(() -> assertRejectsField(ClassCastException.class, "addresses", () -> BigQueryExecutor.toEntity(fields, row, AddressTreeSet.class)),
                () -> assertRejectsField(NullPointerException.class, "values", () -> BigQueryExecutor.toEntity(tsFields, tsRow, InstantDeque.class)),
                // Positive: a collection that accepts the element is filled as before.
                () -> assertEquals("Paris", BigQueryExecutor.toEntity(fields, row, AddressList2.class).addresses().get(0).getCity()));
    }

    // ---------------------------------------------------------------------------------------------------------------------
    // Any parameterized container property built by the JSON codec: a rejected element names its field
    // ---------------------------------------------------------------------------------------------------------------------

    public record LongDeque(ArrayDeque<Long> values) {
    }

    public record LongTreeSet(TreeSet<Long> values) {
    }

    public record LongLinkedQueue(ConcurrentLinkedQueue<Long> values) {
    }

    public record LongConcurrentMap(ConcurrentHashMap<String, Long> values) {
    }

    public record ListTreeSet(TreeSet<List<Long>> values) {
    }

    public record MapTreeSet(TreeSet<Map<String, Long>> values) {
    }

    @SuppressWarnings("rawtypes")
    public record RawTreeSet(TreeSet values) {
    }

    public static class LongDequeBean {
        private ArrayDeque<Long> values;

        public ArrayDeque<Long> getValues() {
            return values;
        }

        public void setValues(final ArrayDeque<Long> values) {
            this.values = values;
        }
    }

    /**
     * Before the fix: only an ArrayBlockingQueue property named the NULL field of a STRUCT it rejected; any other parameterized
     * null-rejecting container (ArrayDeque&lt;Long&gt;, TreeSet&lt;Long&gt;, ConcurrentLinkedQueue&lt;Long&gt;,
     * ConcurrentHashMap&lt;String, Long&gt;), built by the property's JSON codec, failed with the container's bare
     * NullPointerException, while the raw classes (ArrayDeque, ConcurrentHashMap&lt;String, Object&gt;) name the field.
     */
    @Test
    public void nullStructFieldIntoAnyTypedContainerPropertyNamesTheField() throws Exception {
        final FieldList subFields = FieldList.of(Field.of("a", StandardSQLTypeName.INT64), Field.of("b", StandardSQLTypeName.INT64));
        final FieldList fields = FieldList.of(Field.newBuilder("values", StandardSQLTypeName.STRUCT, subFields).build());
        final List<Class<?>> targets = List.of(LongDeque.class, LongTreeSet.class, LongLinkedQueue.class, LongConcurrentMap.class, LongDequeBean.class);
        final List<Executable> checks = new ArrayList<>();

        for (final Class<?> target : targets) {
            checks.add(() -> assertRejectsField(NullPointerException.class, "b",
                    () -> BigQueryExecutor.toEntity(fields, structRow(fields, subFields, "1", null), target)));
            checks.add(() -> assertRejectsField(NullPointerException.class, "a",
                    () -> BigQueryExecutor.toEntity(fields, structRow(fields, subFields, null, "2"), target)));
        }

        // the bean-class Dataset of extractData converts the STRUCT column the same way
        fixBqStubResult(fields, structRow(fields, subFields, "1", null));
        checks.add(() -> assertRejectsField(NullPointerException.class, "b", () -> BigQueryExecutor.extractData(mockTableResult, LongDequeBean.class)));

        // Positive: a STRUCT without NULL fields still fills each container as before.
        final FieldValueList okRow = structRow(fields, subFields, "2", "1");
        checks.add(() -> assertEquals(List.of(2L, 1L), new ArrayList<>(BigQueryExecutor.toEntity(fields, okRow, LongDeque.class).values())));
        checks.add(() -> assertEquals(List.of(1L, 2L), new ArrayList<>(BigQueryExecutor.toEntity(fields, okRow, LongTreeSet.class).values())));
        checks.add(() -> assertEquals(List.of(2L, 1L), new ArrayList<>(BigQueryExecutor.toEntity(fields, okRow, LongLinkedQueue.class).values())));
        checks.add(() -> assertEquals(Map.of("a", 2L, "b", 1L), BigQueryExecutor.toEntity(fields, okRow, LongConcurrentMap.class).values()));
        checks.add(() -> assertEquals(List.of(2L, 1L), new ArrayList<>(BigQueryExecutor.toEntity(fields, okRow, LongDequeBean.class).getValues())));

        assertAll(checks);
    }

    /**
     * Before the fix: an element a TreeSet property built by the codec cannot compare failed with the TreeSet's bare
     * ClassCastException: a STRUCT of two ARRAY fields read into a TreeSet&lt;List&lt;Long&gt;&gt;, and a REPEATED STRUCT read into a
     * TreeSet&lt;Map&lt;String, Long&gt;&gt; or a raw TreeSet. All now name the field, with the ClassCastException as the cause.
     */
    @Test
    public void uncomparableElementIntoTypedTreeSetPropertyNamesTheField() {
        final FieldList listSub = FieldList.of(fixBqRepeatedField("x", StandardSQLTypeName.INT64), fixBqRepeatedField("y", StandardSQLTypeName.INT64));
        final FieldList listFields = FieldList.of(Field.newBuilder("values", StandardSQLTypeName.STRUCT, listSub).build());
        final FieldValueList listRow = FieldValueList.of(List.of(FieldValue.of(FieldValue.Attribute.RECORD,
                FieldValueList.of(List.of(fixBqRepeated(verifyCell("1")), fixBqRepeated(verifyCell("2"))), listSub))), listFields);

        final FieldList mapSub = FieldList.of(Field.of("a", StandardSQLTypeName.INT64), Field.of("b", StandardSQLTypeName.INT64));
        final FieldList mapFields = FieldList.of(verifyRepeatedStruct("values", mapSub));
        final FieldValueList mapRow = FieldValueList.of(List.of(fixBqRepeated(fixBqRecord(verifyCell("1"), verifyCell("2")))), mapFields);

        assertAll(() -> assertRejectsField(ClassCastException.class, "x", () -> BigQueryExecutor.toEntity(listFields, listRow, ListTreeSet.class)),
                () -> assertRejectsField(ClassCastException.class, "values", () -> BigQueryExecutor.toEntity(mapFields, mapRow, MapTreeSet.class)),
                // a raw TreeSet property is filled through the same codec (TreeSet<Object>)
                () -> assertRejectsField(ClassCastException.class, "values", () -> BigQueryExecutor.toEntity(mapFields, mapRow, RawTreeSet.class)));
    }

    public record MapDeque(ArrayDeque<Map<String, Long>> values) {
    }

    public record NestedConcurrentMap(ConcurrentHashMap<String, ConcurrentHashMap<String, Long>> values) {
    }

    /**
     * Fallback, as documented: the field is named only when the property's own container rejects an element decodable to its
     * element type. A NULL field next to one that cannot be decoded to the element type (2 is not a Map) keeps the codec's
     * NullPointerException, with the failed diagnosis suppressed; a rejection inside a nested container (a NULL field of an inner
     * STRUCT read into the inner ConcurrentHashMap) propagates the codec's exception as is.
     */
    @Test
    public void undiagnosableRejectionPropagatesTheCodecException() {
        final FieldList abSub = FieldList.of(Field.of("a", StandardSQLTypeName.INT64), Field.of("b", StandardSQLTypeName.INT64));
        final FieldList abFields = FieldList.of(Field.newBuilder("values", StandardSQLTypeName.STRUCT, abSub).build());

        final FieldList inner = FieldList.of(Field.of("c", StandardSQLTypeName.INT64), Field.of("d", StandardSQLTypeName.INT64));
        final FieldList subFields = FieldList.of(Field.of("a", StandardSQLTypeName.INT64), Field.newBuilder("n", StandardSQLTypeName.STRUCT, inner).build());
        final FieldList fields = FieldList.of(Field.newBuilder("values", StandardSQLTypeName.STRUCT, subFields).build());
        final FieldValue innerRecord = FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(Arrays.asList(verifyCell("1"), verifyCell(null)), inner));
        final FieldValueList row = FieldValueList
                .of(List.of(FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(Arrays.asList(verifyCell("1"), innerRecord), subFields))), fields);

        assertAll(() -> {
            final NullPointerException e = assertThrows(NullPointerException.class,
                    () -> BigQueryExecutor.toEntity(abFields, structRow(abFields, abSub, null, "2"), MapDeque.class));
            assertTrue(e.getSuppressed().length > 0, e.toString());
        }, () -> {
            final RuntimeException e = assertThrows(RuntimeException.class, () -> BigQueryExecutor.toEntity(fields, row, NestedConcurrentMap.class));
            assertFalse(String.valueOf(e.getMessage()).startsWith("Field ("), e.toString());
        });
    }

    /** A NULL element of a codec-converted REPEATED value (INT64 into ArrayDeque&lt;Long&gt; / TreeSet&lt;Long&gt;) names the field the same way. */
    @Test
    public void nullRepeatedElementIntoTypedContainerPropertyNamesTheField() {
        final FieldList fields = FieldList.of(fixBqRepeatedField("values", StandardSQLTypeName.INT64));
        final FieldValueList row = FieldValueList.of(List.of(fixBqRepeated(verifyCell("1"), verifyCell(null))), fields);
        final FieldValueList okRow = FieldValueList.of(List.of(fixBqRepeated(verifyCell("2"), verifyCell("1"))), fields);

        assertAll(() -> assertRejectsField(NullPointerException.class, "values", () -> BigQueryExecutor.toEntity(fields, row, LongDeque.class)),
                () -> assertRejectsField(NullPointerException.class, "values", () -> BigQueryExecutor.toEntity(fields, row, LongTreeSet.class)),
                () -> assertRejectsField(NullPointerException.class, "values", () -> BigQueryExecutor.toEntity(fields, row, LongDequeBean.class)),
                () -> assertEquals(List.of(1L, 2L), new ArrayList<>(BigQueryExecutor.toEntity(fields, okRow, LongTreeSet.class).values())));
    }

    // ---------------------------------------------------------------------------------------------------------------------
    // Dotted path through a collection property (documented in toEntity, as in DynamoDBExecutorBase)
    // ---------------------------------------------------------------------------------------------------------------------

    public static class AddressList {
        private List<Address> addresses;

        public AddressList() {
        }

        public List<Address> getAddresses() {
            return addresses;
        }

        public void setAddresses(final List<Address> addresses) {
            this.addresses = addresses;
        }
    }

    public static class PrefilledAddressList extends AddressList {
        public PrefilledAddressList() {
            final Address first = new Address();
            first.setCity("old");
            final Address second = new Address();
            second.setCity("second");
            setAddresses(new ArrayList<>(List.of(first, second)));
        }
    }

    public static class EmptyAddressList extends AddressList {
        public EmptyAddressList() {
            setAddresses(new ArrayList<>());
        }
    }

    public static final class BuiltAddressList {
        private final List<Address> addresses;

        BuiltAddressList(final List<Address> addresses) {
            this.addresses = addresses;
        }

        public List<Address> getAddresses() {
            return addresses;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private List<Address> addresses;

            public Builder addresses(final List<Address> addresses) {
                this.addresses = addresses;
                return this;
            }

            public BuiltAddressList build() {
                return new BuiltAddressList(addresses);
            }
        }
    }

    private static <T> T readCity(final Class<T> targetClass) {
        final FieldList fields = FieldList.of(Field.of("addresses.city", StandardSQLTypeName.STRING));
        return BigQueryExecutor.toEntity(fields, FieldValueList.of(List.of(verifyCell("Paris")), fields), targetClass);
    }

    /**
     * Pins the documented behavior of a dotted path through a collection property: written into the first element, or into a
     * new bean added to a missing or empty collection; a builder-based target fails the row with ClassCastException.
     */
    @Test
    public void dottedPathThroughCollectionPropertyWritesTheFirstElement() {
        final List<Address> prefilled = readCity(PrefilledAddressList.class).getAddresses();
        final List<Address> empty = readCity(EmptyAddressList.class).getAddresses();
        final List<Address> missing = readCity(AddressList.class).getAddresses();

        assertAll(() -> assertEquals(2, prefilled.size()), () -> assertEquals("Paris", prefilled.get(0).getCity()),
                () -> assertEquals("second", prefilled.get(1).getCity()), () -> assertEquals(1, empty.size()),
                () -> assertEquals("Paris", empty.get(0).getCity()), () -> assertEquals(1, missing.size()),
                () -> assertEquals("Paris", missing.get(0).getCity()), () -> assertThrows(ClassCastException.class, () -> readCity(BuiltAddressList.class)));
    }
}
