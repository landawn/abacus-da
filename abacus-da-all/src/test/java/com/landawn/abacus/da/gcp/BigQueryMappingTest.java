package com.landawn.abacus.da.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.u.Nullable;

public class BigQueryMappingTest extends BigQueryExecutorTestSupport {

    // 2024-06-20T16:13:20.123456Z
    private static final long TS_MICROS = 1_718_900_000_123_456L;

    // Row and entity mapping

    // extractData with null schema returns empty Dataset
    @Test
    public void testExtractData_NullSchemaReturnsEmpty() {
        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockTableResult.getSchema()).thenReturn(null);

        Dataset ds = BigQueryExecutor.extractData(mockTableResult, TestEntity.class);
        assertNotNull(ds);
        assertEquals(0, ds.size());
    }

    // extractData with null target class
    @Test
    public void testExtractData_NullTargetClass() throws Exception {
        Field f1 = Field.of("id", StandardSQLTypeName.INT64);
        FieldList fields = FieldList.of(f1);
        Schema schema = Schema.of(fields);

        List<FieldValueList> rows = new ArrayList<>();
        rows.add(FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "55")), fields));

        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(rows);

        Dataset ds = BigQueryExecutor.extractData(mockTableResult, null);
        assertNotNull(ds);
        assertEquals(1, ds.size());
    }

    // extractData Map class branch
    @Test
    public void testExtractData_AsMapClass() throws Exception {
        Field f1 = Field.of("id", StandardSQLTypeName.INT64);
        Field f2 = Field.of("name", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(f1, f2);
        Schema schema = Schema.of(fields);

        List<FieldValueList> rows = new ArrayList<>();
        rows.add(FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "x")),
                fields));

        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(rows);

        Dataset ds = BigQueryExecutor.extractData(mockTableResult, Map.class);
        assertNotNull(ds);
        assertEquals(1, ds.size());
        assertTrue(ds.containsColumn("id"));
    }

    @Test
    public void testExtractData_DecodesBytesAndTimestampCellsForEntityColumns() {
        final FieldList fields = binaryTimestampFields();
        when(mockTableResult.getSchema()).thenReturn(Schema.of(fields));
        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.iterateAll()).thenReturn(Arrays.asList(binaryTimestampRow(fields)));

        final Dataset ds = BigQueryExecutor.extractData(mockTableResult, BinaryTimestampEntity.class);

        assertTrue(Arrays.equals(BINARY_PAYLOAD, (byte[]) ds.getColumn("data").get(0)));
        final java.sql.Timestamp ts = (java.sql.Timestamp) ds.getColumn("created_time").get(0);
        assertEquals(123_456_000, ts.getNanos());
        assertEquals(TS_MICROS / 1000, ts.getTime());
        assertEquals(java.time.Instant.ofEpochSecond(1_718_900_000L, 123_456_000L), ds.getColumn("seen_at").get(0));
    }

    @Test
    public void testExtractData_NestedFieldValueListInColumn() {
        Field innerF = Field.of("a", StandardSQLTypeName.STRING);
        FieldList innerFields = FieldList.of(innerF);
        FieldValueList innerList = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "x")), innerFields);
        Field outer = Field.newBuilder("nested", StandardSQLTypeName.STRUCT, innerFields).build();
        FieldList outerFields = FieldList.of(outer);
        FieldValueList row = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.RECORD, innerList)), outerFields);

        Schema schema = Schema.of(outer);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(Arrays.asList(row));
        when(mockTableResult.getTotalRows()).thenReturn(1L);

        // Use Map.class as target so columnClasses[i] is Map.class — branch hits readRow path
        Dataset ds = BigQueryExecutor.extractData(mockTableResult, Map.class);
        assertNotNull(ds);
        assertEquals(1, ds.size());
    }

    // Regression: with a non-bean target class (e.g. Object[]), scalar column values must stay raw.
    // The old code set columnClasses[i] = Object[].class for every column, forcing each scalar through
    // N.convert(value, Object[].class), which mangled plain values like "abc".
    @Test
    public void testExtractData_ScalarPassthroughForNonBeanTarget() {
        final Field idField = Field.of("id", StandardSQLTypeName.INT64);
        final Field nameField = Field.of("name", StandardSQLTypeName.STRING);
        final FieldList fields = FieldList.of(idField, nameField);
        final Schema schema = Schema.of(fields);

        final List<FieldValueList> rows = new ArrayList<>();
        rows.add(FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "abc")),
                fields));

        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(rows);

        final Dataset dataset = BigQueryExecutor.extractData(mockTableResult, Object[].class);

        assertEquals(1, dataset.size());
        // Raw scalar values, not values wrapped/mangled into Object[].
        assertEquals("1", dataset.getColumn("id").get(0));
        assertEquals("abc", dataset.getColumn("name").get(0));
    }

    @Test
    public void testExtractDataWithEmptyTableResult() {
        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockTableResult.getSchema()).thenReturn(null);

        Dataset dataset = BigQueryExecutor.extractData(mockTableResult, TestEntity.class);

        assertNotNull(dataset);
        assertEquals(0, dataset.size());
    }

    @Test
    public void testExtractDataWithNullSchemaAndMapClassNonZeroRows() {
        when(mockTableResult.getTotalRows()).thenReturn(3L);
        when(mockTableResult.getSchema()).thenReturn(null);

        Dataset dataset = BigQueryExecutor.extractData(mockTableResult, Map.class);

        assertNotNull(dataset);
        assertEquals(0, dataset.size());
    }

    @Test
    public void testExtractDataWithNullSchemaButNonZeroRows() {
        // Simulates a DML statement result (INSERT/UPDATE/DELETE): BigQuery returns a
        // TableResult whose getSchema() is null while getTotalRows() reflects the
        // affected-row count (> 0). The guard must not dereference the null schema.
        when(mockTableResult.getTotalRows()).thenReturn(5L);
        when(mockTableResult.getSchema()).thenReturn(null);

        Dataset dataset = BigQueryExecutor.extractData(mockTableResult, TestEntity.class);

        assertNotNull(dataset);
        assertEquals(0, dataset.size());
        assertEquals(0, dataset.columnCount());
    }

    @Test
    public void testExtractDataWithMapClass() {
        Field field1 = Field.of("id", StandardSQLTypeName.INT64);
        Field field2 = Field.of("name", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(field1, field2);
        Schema schema = Schema.of(fields);

        List<FieldValueList> rows = new ArrayList<>();
        rows.add(FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Name1")),
                fields));

        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(rows);

        Dataset dataset = BigQueryExecutor.extractData(mockTableResult, Map.class);

        assertNotNull(dataset);
        assertEquals(2, dataset.columnCount());
        assertEquals(1, dataset.size());
    }

    @Test
    public void testExtractDataWithTableResultAndClass() {
        Field field1 = Field.of("id", StandardSQLTypeName.INT64);
        Field field2 = Field.of("name", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(field1, field2);
        Schema schema = Schema.of(fields);

        List<FieldValueList> rows = new ArrayList<>();
        rows.add(FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Name1")),
                fields));

        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(rows);

        Dataset dataset = BigQueryExecutor.extractData(mockTableResult, TestEntity.class);

        assertNotNull(dataset);
        assertEquals(2, dataset.columnCount());
        assertEquals(1, dataset.size());
        assertTrue(dataset.columnNames().contains("id"));
        assertTrue(dataset.columnNames().contains("name"));
    }

    @Test
    public void testSingleColumnRows_DecodeBytesAndTimestampCells() throws Exception {
        final FieldList bytesFields = FieldList.of(Field.of("data", StandardSQLTypeName.BYTES));
        final FieldValueList bytesRow = FieldValueList
                .of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, java.util.Base64.getEncoder().encodeToString(BINARY_PAYLOAD))), bytesFields);
        when(mockTableResult.getSchema()).thenReturn(Schema.of(bytesFields));
        when(mockTableResult.getTotalRows()).thenReturn(2L);
        when(mockTableResult.iterateAll()).thenReturn(Arrays.asList(bytesRow, bytesRow));

        // scalar row mapper: every row (not just the first) must be decoded
        final List<byte[]> blobs = BigQueryExecutor.toList(mockTableResult, byte[].class);
        assertEquals(2, blobs.size());
        assertTrue(Arrays.equals(BINARY_PAYLOAD, blobs.get(0)));
        assertTrue(Arrays.equals(BINARY_PAYLOAD, blobs.get(1)));

        // queryForSingleValue / queryForSingleNonNull
        final FieldList tsFields = FieldList.of(Field.of("max_ts", StandardSQLTypeName.TIMESTAMP));
        final FieldValueList tsRow = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1.718900000123456E9")), tsFields);
        final TableResult tsResult = mock(TableResult.class);
        when(tsResult.getSchema()).thenReturn(Schema.of(tsFields));
        when(tsResult.getTotalRows()).thenReturn(1L);
        when(tsResult.getValues()).thenReturn(Arrays.asList(tsRow));
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(tsResult);

        final Nullable<java.sql.Timestamp> max = executor.queryForSingleValue(java.sql.Timestamp.class, "SELECT MAX(ts) FROM t");
        assertEquals(123_456_000, max.get().getNanos());
        assertEquals(TS_MICROS / 1000, max.get().getTime());
        assertEquals(com.google.cloud.Timestamp.ofTimeMicroseconds(TS_MICROS),
                executor.queryForSingleNonNull(com.google.cloud.Timestamp.class, "SELECT MAX(ts) FROM t").get());
        // numeric / String targets are not reinterpreted
        assertEquals("1.718900000123456E9", executor.queryForSingleValue(String.class, "SELECT MAX(ts) FROM t").get());
    }

    @Test
    public void testSingleValueRow_DecodesTimestampViaReadRowConverter() {
        final FieldList tsFields = FieldList.of(Field.of("ts", StandardSQLTypeName.TIMESTAMP));
        final FieldValueList tsRow = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1.718900000123456E9")), tsFields);

        // N.convert(FieldValueList, X) is routed through the registered readRow converter
        final java.sql.Timestamp ts = N.convert(tsRow, java.sql.Timestamp.class);

        assertEquals(TS_MICROS / 1000, ts.getTime());
        assertEquals(123_456_000, ts.getNanos());

        // int64-timestamp result format (DataFormatOptions.useInt64Timestamp): the cell holds epoch micros,
        // which a plain N.convert would misread as epoch millis (year 56439)
        final FieldValueList microsRow = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, String.valueOf(TS_MICROS), true)),
                tsFields);
        assertEquals(java.time.Instant.ofEpochSecond(1_718_900_000L, 123_456_000L), N.convert(microsRow, java.time.Instant.class));
    }

    @Test
    public void testToEntity_DecodesBytesAndTimestampCells() {
        final FieldList fields = binaryTimestampFields();

        assertBinaryTimestampEntity(BigQueryExecutor.toEntity(fields, binaryTimestampRow(fields), BinaryTimestampEntity.class));
    }

    // Regression: a RECORD value assigned to a List-typed property must be converted through readRow
    // (plain values). FieldValueList IS a List, so the old plain assignability check stored the raw
    // FieldValue wrappers into the List property instead of converting.
    @Test
    public void testToEntity_RecordIntoListTypedProperty_ConvertedToPlainValues() {
        final Field innerX = Field.of("x", StandardSQLTypeName.STRING);
        final Field innerY = Field.of("y", StandardSQLTypeName.STRING);
        final FieldList innerFields = FieldList.of(innerX, innerY);
        final FieldValueList innerRow = FieldValueList
                .of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "v1"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "v2")), innerFields);

        final Field idField = Field.of("id", StandardSQLTypeName.INT64);
        final Field structField = Field.of("struct", StandardSQLTypeName.STRUCT, innerFields);
        final FieldList fields = FieldList.of(idField, structField);
        final FieldValueList row = FieldValueList
                .of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"), FieldValue.of(FieldValue.Attribute.RECORD, innerRow)), fields);

        final TestEntityWithStructList entity = BigQueryExecutor.toEntity(fields, row, TestEntityWithStructList.class);

        assertEquals(1, entity.getId());
        assertEquals(Arrays.asList("v1", "v2"), entity.getStruct());
        assertTrue(entity.getStruct().get(0) instanceof String, "RECORD elements must be plain values, not FieldValue wrappers");
    }

    @Test
    public void testToEntityWithInvalidEntityClass() {
        Field field = Field.of("value", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(field);

        FieldValueList fieldValueList = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Value")), fields);

        assertThrows(IllegalArgumentException.class, () -> {
            BigQueryExecutor.toEntity(fields, fieldValueList, String.class);
        });
    }

    @Test
    public void testToEntityWithDottedPropertyName() {
        Field field1 = Field.of("id", StandardSQLTypeName.INT64);
        Field field2 = Field.of("nested.property", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(field1, field2);

        FieldValueList fieldValueList = FieldValueList
                .of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "123"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "NestedValue")), fields);

        TestEntity entity = BigQueryExecutor.toEntity(fields, fieldValueList, TestEntity.class);

        assertNotNull(entity);
        assertEquals(123, entity.getId());
    }

    @Test
    public void testToEntityWithFieldListFieldValueListAndClass() {
        Field field1 = Field.of("id", StandardSQLTypeName.INT64);
        Field field2 = Field.of("name", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(field1, field2);

        FieldValueList fieldValueList = FieldValueList
                .of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "456"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "TestName2")), fields);

        TestEntity entity = BigQueryExecutor.toEntity(fields, fieldValueList, TestEntity.class);

        assertNotNull(entity);
        assertEquals(456, entity.getId());
        assertEquals("TestName2", entity.getName());
    }

    @Test
    public void testToEntityWithFieldValueListAndClass() {
        Field field1 = Field.of("id", StandardSQLTypeName.INT64);
        Field field2 = Field.of("name", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(field1, field2);

        FieldValueList fieldValueList = FieldValueList
                .of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "789"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "TestName3")), fields);

        TestEntity entity = BigQueryExecutor.toEntity(fieldValueList, TestEntity.class);

        assertNotNull(entity);
        assertEquals(789, entity.getId());
        assertEquals("TestName3", entity.getName());
    }

    @Test
    public void testToEntityWithNestedFieldValueList() {
        Field nestedField1 = Field.of("nestedId", StandardSQLTypeName.INT64);
        Field nestedField2 = Field.of("nestedName", StandardSQLTypeName.STRING);
        FieldList nestedFields = FieldList.of(nestedField1, nestedField2);

        FieldValueList nestedFieldValueList = FieldValueList.of(
                Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "999"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "NestedName")), nestedFields);

        Field field1 = Field.of("id", StandardSQLTypeName.INT64);
        Field field2 = Field.of("nested", StandardSQLTypeName.STRUCT, nestedFields);
        FieldList fields = FieldList.of(field1, field2);

        FieldValueList fieldValueList = FieldValueList.of(
                Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "123"), FieldValue.of(FieldValue.Attribute.RECORD, nestedFieldValueList)), fields);

        TestEntityWithNested entity = BigQueryExecutor.toEntity(fields, fieldValueList, TestEntityWithNested.class);

        assertNotNull(entity);
        assertEquals(123, entity.getId());
        assertNotNull(entity.getNested());
    }

    @Test
    public void testToEntityWithSchemaFieldValueListAndClass() {
        Field field1 = Field.of("id", StandardSQLTypeName.INT64);
        Field field2 = Field.of("name", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(field1, field2);
        Schema schema = Schema.of(fields);

        FieldValueList fieldValueList = FieldValueList
                .of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "123"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "TestName")), fields);

        TestEntity entity = BigQueryExecutor.toEntity(schema, fieldValueList, TestEntity.class);

        assertNotNull(entity);
        assertEquals(123, entity.getId());
        assertEquals("TestName", entity.getName());
    }

    // toList with null schema returns empty list
    @Test
    public void testToList_NullSchema() throws Exception {
        when(mockTableResult.getTotalRows()).thenReturn(5L);
        when(mockTableResult.getSchema()).thenReturn(null);

        List<TestEntity> result = BigQueryExecutor.toList(mockTableResult, TestEntity.class);
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    // readRow Collection branch (via toList with List class)
    @Test
    public void testToList_AsCollectionClass() throws Exception {
        Field f1 = Field.of("id", StandardSQLTypeName.INT64);
        Field f2 = Field.of("name", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(f1, f2);
        Schema schema = Schema.of(fields);

        List<FieldValueList> rows = new ArrayList<>();
        rows.add(FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "n")),
                fields));

        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(rows);

        @SuppressWarnings({ "unchecked", "rawtypes" })
        List<List> result = BigQueryExecutor.toList(mockTableResult, (Class) List.class);
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).size());
    }

    // readRow Map branch via toList with Map class
    @Test
    public void testToList_AsMapClass() throws Exception {
        Field f1 = Field.of("id", StandardSQLTypeName.INT64);
        FieldList fields = FieldList.of(f1);
        Schema schema = Schema.of(fields);

        List<FieldValueList> rows = new ArrayList<>();
        rows.add(FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "42")), fields));

        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(rows);

        @SuppressWarnings({ "unchecked", "rawtypes" })
        List<Map> result = BigQueryExecutor.toList(mockTableResult, (Class) Map.class);
        assertEquals(1, result.size());
        assertEquals("42", result.get(0).get("id"));
    }

    // readRow single-value branch
    @Test
    public void testToList_AsSingleValueClass() throws Exception {
        Field f1 = Field.of("id", StandardSQLTypeName.INT64);
        FieldList fields = FieldList.of(f1);
        Schema schema = Schema.of(fields);

        List<FieldValueList> rows = new ArrayList<>();
        rows.add(FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "100")), fields));
        rows.add(FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "200")), fields));

        when(mockTableResult.getTotalRows()).thenReturn(2L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(rows);

        List<Long> result = BigQueryExecutor.toList(mockTableResult, Long.class);
        assertEquals(2, result.size());
        assertEquals(Long.valueOf(100L), result.get(0));
        assertEquals(Long.valueOf(200L), result.get(1));
    }

    @Test
    public void testToList_DecodesBytesAndTimestampCells() {
        final FieldList fields = binaryTimestampFields();
        when(mockTableResult.getSchema()).thenReturn(Schema.of(fields));
        when(mockTableResult.getTotalRows()).thenReturn(2L);
        when(mockTableResult.iterateAll()).thenReturn(Arrays.asList(binaryTimestampRow(fields), binaryTimestampRow(fields)));

        final List<BinaryTimestampEntity> list = BigQueryExecutor.toList(mockTableResult, BinaryTimestampEntity.class);

        assertEquals(2, list.size());
        list.forEach(BigQueryMappingTest::assertBinaryTimestampEntity);
    }

    @Test
    public void testToList_ReadRow_WithFieldValueList_AsObjectArray() throws Exception {
        // Force readRow's "Object[]" branch with a nested FieldValueList field.
        Field nested = Field.of("inner", StandardSQLTypeName.STRING);
        FieldList nestedFields = FieldList.of(nested);
        FieldValueList nestedRow = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "v")), nestedFields);

        Field outer = Field.of("outer", StandardSQLTypeName.STRUCT, nestedFields);
        FieldList outerFields = FieldList.of(outer);
        Schema schema = Schema.of(outerFields);

        List<FieldValueList> rows = new ArrayList<>();
        rows.add(FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.RECORD, nestedRow)), outerFields));

        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(rows);

        List<Object[]> result = BigQueryExecutor.toList(mockTableResult, Object[].class);
        assertEquals(1, result.size());
        // The nested struct should be expanded to its own array.
        assertTrue(result.get(0)[0] instanceof Object[]);
    }

    @Test
    public void testToList_WithIntegerClass() throws Exception {
        // Drives the "single-column convert" branch of createRowMapper for basic types.
        Field f = Field.of("c", StandardSQLTypeName.INT64);
        FieldList fields = FieldList.of(f);
        Schema schema = Schema.of(fields);

        List<FieldValueList> rows = new ArrayList<>();
        rows.add(FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1")), fields));
        rows.add(FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2")), fields));

        when(mockTableResult.getTotalRows()).thenReturn(2L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(rows);

        List<Integer> values = BigQueryExecutor.toList(mockTableResult, Integer.class);
        assertEquals(2, values.size());
        assertEquals(Integer.valueOf(1), values.get(0));
        assertEquals(Integer.valueOf(2), values.get(1));
    }

    @Test
    public void testToListAndExtractDataRejectNullTableResult() {
        assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.toList(null, TestEntity.class));
        assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.extractData(null, TestEntity.class));
    }

    @Test
    public void testToListWithEmptyTableResult() {
        when(mockTableResult.getTotalRows()).thenReturn(0L);

        List<TestEntity> list = BigQueryExecutor.toList(mockTableResult, TestEntity.class);

        assertNotNull(list);
        assertEquals(0, list.size());
    }

    @Test
    public void testToListWithNullSchemaButNonZeroRows() {
        // Same DML-result shape as the extractData case: getSchema() is null while
        // getTotalRows() is the affected-row count (> 0). toList must not dereference
        // the null schema (previously NPE'd at schema.getFields()).
        when(mockTableResult.getTotalRows()).thenReturn(7L);
        when(mockTableResult.getSchema()).thenReturn(null);

        List<TestEntity> list = BigQueryExecutor.toList(mockTableResult, TestEntity.class);

        assertNotNull(list);
        assertTrue(list.isEmpty());
    }

    @Test
    public void testToListWithDifferentRowClasses() throws Exception {
        Field field = Field.of("value", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(field);
        Schema schema = Schema.of(fields);

        List<FieldValueList> rows = new ArrayList<>();
        rows.add(FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Value1")), fields));

        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(rows);

        // Test with String class
        List<String> stringList = BigQueryExecutor.toList(mockTableResult, String.class);
        assertNotNull(stringList);
        assertEquals(1, stringList.size());
        assertEquals("Value1", stringList.get(0));

        // Test with Map class
        List<Map> mapList = BigQueryExecutor.toList(mockTableResult, Map.class);
        assertNotNull(mapList);
        assertEquals(1, mapList.size());

        // Test with Object[] class
        List<Object[]> arrayList = BigQueryExecutor.toList(mockTableResult, Object[].class);
        assertNotNull(arrayList);
        assertEquals(1, arrayList.size());

        // Test with List class
        List<List> listList = BigQueryExecutor.toList(mockTableResult, List.class);
        assertNotNull(listList);
        assertEquals(1, listList.size());
    }

    @Test
    public void testToListWithListClassPopulatesElements() throws Exception {
        // Same regression as testToListWithObjectArrayPopulatesElements but for the
        // Collection branch of createRowMapper.
        Field field1 = Field.of("id", StandardSQLTypeName.INT64);
        Field field2 = Field.of("name", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(field1, field2);
        Schema schema = Schema.of(fields);

        List<FieldValueList> rows = new ArrayList<>();
        rows.add(FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "100"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Carol")),
                fields));

        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(rows);

        List<List> listList = BigQueryExecutor.toList(mockTableResult, List.class);

        assertNotNull(listList);
        assertEquals(1, listList.size());
        assertEquals(2, listList.get(0).size());
        assertEquals("100", listList.get(0).get(0));
        assertEquals("Carol", listList.get(0).get(1));
    }

    @Test
    public void testToListWithObjectArrayPopulatesElements() throws Exception {
        // Regression: createRowMapper(rowClass, fields) used by toList previously left
        // fieldCount=0 when a non-null fields argument was supplied, so each row was
        // mapped to an empty Object[]. The row content was silently dropped.
        Field field1 = Field.of("id", StandardSQLTypeName.INT64);
        Field field2 = Field.of("name", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(field1, field2);
        Schema schema = Schema.of(fields);

        List<FieldValueList> rows = new ArrayList<>();
        rows.add(FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "42"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Alice")),
                fields));
        rows.add(FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "43"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Bob")),
                fields));

        when(mockTableResult.getTotalRows()).thenReturn(2L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(rows);

        List<Object[]> arrayList = BigQueryExecutor.toList(mockTableResult, Object[].class);

        assertNotNull(arrayList);
        assertEquals(2, arrayList.size());
        // Each row must produce a length-2 array, not an empty one.
        assertEquals(2, arrayList.get(0).length);
        assertEquals("42", arrayList.get(0)[0]);
        assertEquals("Alice", arrayList.get(0)[1]);
        assertEquals(2, arrayList.get(1).length);
        assertEquals("43", arrayList.get(1)[0]);
        assertEquals("Bob", arrayList.get(1)[1]);
    }

    @Test
    public void testToListWithTableResultAndClass() throws Exception {
        Field field1 = Field.of("id", StandardSQLTypeName.INT64);
        Field field2 = Field.of("name", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(field1, field2);
        Schema schema = Schema.of(fields);

        List<FieldValueList> rows = new ArrayList<>();
        rows.add(FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Name1")),
                fields));
        rows.add(FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Name2")),
                fields));

        when(mockTableResult.getTotalRows()).thenReturn(2L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(rows);

        List<TestEntity> list = BigQueryExecutor.toList(mockTableResult, TestEntity.class);

        assertNotNull(list);
        assertEquals(2, list.size());
        assertEquals(1, list.get(0).getId());
        assertEquals("Name1", list.get(0).getName());
        assertEquals(2, list.get(1).getId());
        assertEquals("Name2", list.get(1).getName());
    }

    @Test
    public void testToMap_WithLinkedHashMapSupplier() {
        // Use a supplier other than the default HashMap to drive the IntFunction branch.
        Field f1 = Field.of("id", StandardSQLTypeName.INT64);
        Field f2 = Field.of("name", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(f1, f2);

        FieldValueList fvl = FieldValueList
                .of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "9"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Z")), fields);

        Map<String, Object> map = BigQueryExecutor.toMap(fields, fvl, com.landawn.abacus.util.IntFunctions.ofLinkedHashMap());
        assertTrue(map instanceof LinkedHashMap);
        assertEquals("9", map.get("id"));
        assertEquals("Z", map.get("name"));
    }

    @Test
    public void testToMapWithNullMapSupplier() {
        FieldList fields = FieldList.of(Field.of("id", StandardSQLTypeName.INT64));
        FieldValueList row = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "9")), fields);

        assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.toMap(fields, row, null));
        assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.toMap(row, null));
    }

    @Test
    public void testToMapWithFieldListAndFieldValueList() {
        Field field1 = Field.of("id", StandardSQLTypeName.INT64);
        Field field2 = Field.of("name", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(field1, field2);

        FieldValueList fieldValueList = FieldValueList
                .of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "456"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "TestName2")), fields);

        Map<String, Object> map = BigQueryExecutor.toMap(fields, fieldValueList);

        assertNotNull(map);
        assertEquals("456", map.get("id"));
        assertEquals("TestName2", map.get("name"));
    }

    @Test
    public void testToMapWithFieldListFieldValueListAndSupplier() {
        Field field1 = Field.of("id", StandardSQLTypeName.INT64);
        Field field2 = Field.of("name", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(field1, field2);

        FieldValueList fieldValueList = FieldValueList
                .of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "789"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "TestName3")), fields);

        Map<String, Object> map = BigQueryExecutor.toMap(fields, fieldValueList, HashMap::new);

        assertNotNull(map);
        assertTrue(map instanceof HashMap);
        assertEquals("789", map.get("id"));
        assertEquals("TestName3", map.get("name"));
    }

    @Test
    public void testToMapWithFieldValueList() {
        Field field1 = Field.of("id", StandardSQLTypeName.INT64);
        Field field2 = Field.of("name", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(field1, field2);

        FieldValueList fieldValueList = FieldValueList
                .of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "111"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "TestName4")), fields);

        Map<String, Object> map = BigQueryExecutor.toMap(fieldValueList);

        assertNotNull(map);
        assertEquals("111", map.get("id"));
        assertEquals("TestName4", map.get("name"));
    }

    @Test
    public void testToMapWithFieldValueListAndSupplier() {
        Field field1 = Field.of("id", StandardSQLTypeName.INT64);
        Field field2 = Field.of("name", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(field1, field2);

        FieldValueList fieldValueList = FieldValueList
                .of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "222"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "TestName5")), fields);

        Map<String, Object> map = BigQueryExecutor.toMap(fieldValueList, HashMap::new);

        assertNotNull(map);
        assertTrue(map instanceof HashMap);
        assertEquals("222", map.get("id"));
        assertEquals("TestName5", map.get("name"));
    }

    @Test
    public void testToMapWithNestedFieldValueList() {
        Field nestedField = Field.of("nestedValue", StandardSQLTypeName.STRING);
        FieldList nestedFields = FieldList.of(nestedField);

        FieldValueList nestedFieldValueList = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "NestedValue")), nestedFields);

        Field field1 = Field.of("id", StandardSQLTypeName.INT64);
        Field field2 = Field.of("nested", StandardSQLTypeName.STRUCT, nestedFields);
        FieldList fields = FieldList.of(field1, field2);

        FieldValueList fieldValueList = FieldValueList.of(
                Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "123"), FieldValue.of(FieldValue.Attribute.RECORD, nestedFieldValueList)), fields);

        Map<String, Object> map = BigQueryExecutor.toMap(fields, fieldValueList);

        assertNotNull(map);
        assertEquals("123", map.get("id"));
        assertTrue(map.get("nested") instanceof Map);
    }

    @Test
    public void testToMapWithSchemaAndFieldValueList() {
        Field field1 = Field.of("id", StandardSQLTypeName.INT64);
        Field field2 = Field.of("name", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(field1, field2);
        Schema schema = Schema.of(fields);

        FieldValueList fieldValueList = FieldValueList
                .of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "123"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "TestName")), fields);

        Map<String, Object> map = BigQueryExecutor.toMap(schema, fieldValueList);

        assertNotNull(map);
        assertEquals("123", map.get("id"));
        assertEquals("TestName", map.get("name"));
    }

    // A typed array row target (e.g. Long[]) received the raw cell text and failed with ArrayStoreException in both
    // the list/stream row mapper and readRow (the registered FieldValueList converter). Each cell is now decoded and
    // converted to the array's component type; Object[] rows still keep the raw cell values.
    @Test
    public void testTypedArrayRowTargetConvertsCellsToComponentType() {
        final FieldList fields = FieldList.of(Field.of("id", StandardSQLTypeName.INT64), Field.of("n", StandardSQLTypeName.INT64));
        final FieldValueList row = FieldValueList
                .of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, null)), fields);
        when(mockTableResult.getSchema()).thenReturn(Schema.of(fields));
        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.iterateAll()).thenReturn(Arrays.asList(row));

        assertTrue(Arrays.equals(new Long[] { 1L, null }, BigQueryExecutor.toList(mockTableResult, Long[].class).get(0)));
        assertTrue(Arrays.equals(new Object[] { "1", null }, BigQueryExecutor.toList(mockTableResult, Object[].class).get(0)));
        assertTrue(Arrays.equals(new String[] { "1", null }, BigQueryExecutor.toList(mockTableResult, String[].class).get(0)));
        assertTrue(Arrays.equals(new Integer[] { 1, null }, N.convert(row, Integer[].class)));

        // BYTES / TIMESTAMP cells are decoded for binary / date-time component types
        final FieldList typedFields = FieldList.of(Field.of("b", StandardSQLTypeName.BYTES), Field.of("t", StandardSQLTypeName.TIMESTAMP));
        final FieldValueList typedRow = FieldValueList.of(
                Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "AQID"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1718900000.123456")),
                typedFields);
        final byte[][] binary = N.convert(FieldValueList.of(Arrays.asList(typedRow.get(0)), FieldList.of(typedFields.get(0))), byte[][].class);
        assertTrue(Arrays.equals(new byte[] { 1, 2, 3 }, binary[0]));
        final java.time.Instant[] instants = N.convert(FieldValueList.of(Arrays.asList(typedRow.get(1)), FieldList.of(typedFields.get(1))),
                java.time.Instant[].class);
        assertEquals(java.time.Instant.ofEpochSecond(1_718_900_000L, 123_456_000L), instants[0]);

        // a nested STRUCT cell is converted to a non-Object component type (Map) instead of an Object[]
        final FieldList sub = FieldList.of(Field.of("a", StandardSQLTypeName.INT64));
        final FieldValueList struct = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "5")), sub);
        final FieldValueList structRow = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.RECORD, struct)),
                FieldList.of(Field.newBuilder("s", StandardSQLTypeName.STRUCT, sub).build()));
        assertEquals("5", N.convert(structRow, Map[].class)[0].get("a"));
        assertTrue(Arrays.equals(new Object[] { "5" }, (Object[]) N.convert(structRow, Object[].class)[0]));
    }

    // Queries and results

    @Test
    public void testGetSchema_NullRowThrowsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.getSchema(null));
    }

    @Test
    public void testGetSchema_FieldValueListWithSchema() {
        Field f1 = Field.of("a", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(f1);
        FieldValueList row = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "x")), fields);

        Map<String, Object> result = BigQueryExecutor.toMap(row);
        assertNotNull(result);
        assertEquals("x", result.get("a"));
    }

    @Test
    public void testGetSchema_RowWithoutSchemaThrowsIllegalArgumentException() {
        final FieldValueList row = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "x")), new Field[0]);

        assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.getSchema(row));
        assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.toMap(row));
    }

    // Additional contracts

    @Test
    public void testExplicitSchemaConversionsRejectMismatchedRowWidth() {
        final FieldList oneField = FieldList.of(Field.of("id", StandardSQLTypeName.INT64));
        final FieldList twoFields = FieldList.of(Field.of("id", StandardSQLTypeName.INT64), Field.of("name", StandardSQLTypeName.STRING));
        final FieldValueList twoValues = FieldValueList
                .of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Alice")), twoFields);
        final FieldValueList oneValue = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1")), oneField);

        assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.toMap(oneField, twoValues));
        assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.toMap(twoFields, oneValue));
        assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.toEntity(oneField, twoValues, TestEntity.class));
        assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.toEntity(twoFields, oneValue, TestEntity.class));
    }

    // TIME edge values ("00:00:00", "23:59:59.999999" truncated to .999, "12:00:00.5") read into java.sql.Time across the
    // property, REPEATED (array / Set) and typed-array row paths, and a java.sql.Time bound by this executor (which
    // writes HH:mm:ss.mmm000) reads back to the same instant.
    @Test
    public void testTimeCellEdgeValuesAndParameterRoundTrip() {
        final String[] cells = { "00:00:00", "23:59:59.999999", "12:00:00.5" };
        final long[] expected = { java.sql.Time.valueOf(java.time.LocalTime.MIDNIGHT).getTime(),
                java.sql.Time.valueOf(java.time.LocalTime.of(23, 59, 59)).getTime() + 999, java.sql.Time.valueOf(java.time.LocalTime.NOON).getTime() + 500 };

        final FieldList fields = FieldList.of(Field.newBuilder("times", StandardSQLTypeName.TIME).setMode(Field.Mode.REPEATED).build(),
                Field.newBuilder("time_set", StandardSQLTypeName.TIME).setMode(Field.Mode.REPEATED).build());
        final FieldValue repeated = FieldValue.of(FieldValue.Attribute.REPEATED,
                Arrays.asList(verifyCell(cells[0]), verifyCell(cells[1]), verifyCell(cells[2])));
        final TimeEdgeHolder holder = BigQueryExecutor.toEntity(fields, FieldValueList.of(Arrays.asList(repeated, repeated), fields), TimeEdgeHolder.class);

        assertEquals(3, holder.getTimeSet().size());
        final FieldList single = FieldList.of(Field.of("t", StandardSQLTypeName.TIME));
        when(mockTableResult.getSchema()).thenReturn(Schema.of(single));
        when(mockTableResult.getTotalRows()).thenReturn(1L);

        for (int i = 0; i < cells.length; i++) {
            assertEquals(expected[i], holder.getTimes()[i].getTime(), cells[i]);
            assertTrue(holder.getTimeSet().contains(new java.sql.Time(expected[i])), cells[i]);

            when(mockTableResult.iterateAll()).thenReturn(Arrays.asList(FieldValueList.of(Arrays.asList(verifyCell(cells[i])), single)));
            assertEquals(expected[i], BigQueryExecutor.toList(mockTableResult, java.sql.Time[].class).get(0)[0].getTime(), cells[i]);

            final java.sql.Time time = new java.sql.Time(expected[i]);
            final String bound = BigQueryExecutor.buildQueryParameterValue(time).get(0).getValue();
            when(mockTableResult.iterateAll()).thenReturn(Arrays.asList(FieldValueList.of(Arrays.asList(verifyCell(bound)), single)));
            assertEquals(time.getTime(), BigQueryExecutor.toList(mockTableResult, java.sql.Time.class).get(0).getTime(), bound);
        }
    }

    // BigQuery renders a TIME cell as HH:MM:SS[.ffffff]. The java.sql.Time conversion rejected the fractional part, so a
    // TIME value with sub-second precision - including one written by binding a java.sql.Time with milliseconds - could
    // not be read back into a java.sql.Time property / target.
    @Test
    public void testTimeCellWithFractionalSecondsReadsIntoSqlTime() throws Exception {
        final long expectedMillis = java.sql.Time.valueOf(java.time.LocalTime.of(16, 13, 20)).getTime() + 123;
        final FieldList fields = FieldList.of(Field.of("open_at", StandardSQLTypeName.TIME),
                Field.newBuilder("slots", StandardSQLTypeName.TIME).setMode(Field.Mode.REPEATED).build());
        final FieldValueList row = FieldValueList.of(
                Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "16:13:20.123456"), FieldValue.of(FieldValue.Attribute.REPEATED, Arrays
                        .asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "16:13:20.123"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "16:13:20")))),
                fields);

        final TimeHolder holder = BigQueryExecutor.toEntity(fields, row, TimeHolder.class);
        assertEquals(expectedMillis, holder.getOpenAt().getTime());
        assertEquals(expectedMillis, holder.getSlots().get(0).getTime());
        // a whole-second value is the same instant the standard conversion gives
        assertEquals(N.convert("16:13:20", java.sql.Time.class).getTime(), holder.getSlots().get(1).getTime());

        // single-value targets: list row mapper and queryForSingleValue
        final FieldList timeField = FieldList.of(fields.get(0));
        final FieldValueList timeRow = FieldValueList.of(Arrays.asList(row.get(0)), timeField);
        when(mockTableResult.getSchema()).thenReturn(Schema.of(timeField));
        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.iterateAll()).thenReturn(Arrays.asList(timeRow));
        when(mockTableResult.getValues()).thenReturn(Arrays.asList(timeRow));
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        assertEquals(expectedMillis, BigQueryExecutor.toList(mockTableResult, java.sql.Time.class).get(0).getTime());
        assertEquals(expectedMillis, executor.queryForSingleValue(java.sql.Time.class, "SELECT open_at FROM t").get().getTime());
        // other targets keep their conversion
        assertEquals(java.time.LocalTime.of(16, 13, 20, 123_456_000), executor.queryForSingleValue(java.time.LocalTime.class, "SELECT open_at FROM t").get());
        assertEquals("16:13:20.123456", executor.queryForSingleValue(String.class, "SELECT open_at FROM t").get());
    }

    // ===== 2026-09-22 deep review (slice S): BYTES / TIMESTAMP cell decoding + ByteBuffer binding =====
    // BigQuery returns BYTES cells as base64 text and TIMESTAMP cells as epoch seconds (or epoch micros);
    // N.convert can't decode either (base64 -> NumberFormatException, epoch seconds -> parse failure).

    private static final byte[] BINARY_PAYLOAD = { 1, 2, 3, (byte) 200 };

    private static FieldList binaryTimestampFields() {
        return FieldList.of(Field.of("id", StandardSQLTypeName.INT64), Field.of("data", StandardSQLTypeName.BYTES),
                Field.of("buffer", StandardSQLTypeName.BYTES), Field.of("created_time", StandardSQLTypeName.TIMESTAMP),
                Field.of("upd_time", StandardSQLTypeName.TIMESTAMP), Field.of("seen_at", StandardSQLTypeName.TIMESTAMP),
                Field.of("label", StandardSQLTypeName.BYTES));
    }

    private static FieldValueList binaryTimestampRow(final FieldList fields) {
        final String b64 = java.util.Base64.getEncoder().encodeToString(BINARY_PAYLOAD);
        return FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "7"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, b64),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, b64), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1.718900000123456E9"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1718900000.123456"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1.718900000123456E9"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, b64)), fields);
    }

    private static void assertBinaryTimestampEntity(final BinaryTimestampEntity e) {
        assertEquals(7L, e.getId());
        assertTrue(Arrays.equals(BINARY_PAYLOAD, e.getData()));
        final byte[] fromBuffer = new byte[e.getBuffer().remaining()];
        e.getBuffer().duplicate().get(fromBuffer);
        assertTrue(Arrays.equals(BINARY_PAYLOAD, fromBuffer));
        assertEquals(TS_MICROS / 1000, e.getCreatedTime().getTime());
        assertEquals(123_456_000, e.getCreatedTime().getNanos());
        assertEquals(java.util.Date.class, e.getUpdTime().getClass());
        assertEquals(TS_MICROS / 1000, e.getUpdTime().getTime());
        assertEquals(java.time.Instant.ofEpochSecond(1_718_900_000L, 123_456_000L), e.getSeenAt());
        // String targets keep the raw cell text
        assertEquals(java.util.Base64.getEncoder().encodeToString(BINARY_PAYLOAD), e.getLabel());
    }

    public static class BinaryTimestampEntity {
        private long id;
        private byte[] data;
        private java.nio.ByteBuffer buffer;
        private java.sql.Timestamp createdTime;
        private java.util.Date updTime;
        private java.time.Instant seenAt;
        private String label;

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public byte[] getData() {
            return data;
        }

        public void setData(byte[] data) {
            this.data = data;
        }

        public java.nio.ByteBuffer getBuffer() {
            return buffer;
        }

        public void setBuffer(java.nio.ByteBuffer buffer) {
            this.buffer = buffer;
        }

        public java.sql.Timestamp getCreatedTime() {
            return createdTime;
        }

        public void setCreatedTime(java.sql.Timestamp createdTime) {
            this.createdTime = createdTime;
        }

        public java.util.Date getUpdTime() {
            return updTime;
        }

        public void setUpdTime(java.util.Date updTime) {
            this.updTime = updTime;
        }

        public java.time.Instant getSeenAt() {
            return seenAt;
        }

        public void setSeenAt(java.time.Instant seenAt) {
            this.seenAt = seenAt;
        }

        public String getLabel() {
            return label;
        }

        public void setLabel(String label) {
            this.label = label;
        }
    }

    public static class TestEntityWithNested {
        private int id;
        private NestedEntity nested;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public NestedEntity getNested() {
            return nested;
        }

        public void setNested(NestedEntity nested) {
            this.nested = nested;
        }
    }

    public static class NestedEntity {
        private int nestedId;
        private String nestedName;

        public int getNestedId() {
            return nestedId;
        }

        public void setNestedId(int nestedId) {
            this.nestedId = nestedId;
        }

        public String getNestedName() {
            return nestedName;
        }

        public void setNestedName(String nestedName) {
            this.nestedName = nestedName;
        }
    }

    // Entity with a List-typed property targeted by a RECORD (STRUCT) column.
    public static class TestEntityWithStructList {
        private int id;
        private List<String> struct;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public List<String> getStruct() {
            return struct;
        }

        public void setStruct(List<String> struct) {
            this.struct = struct;
        }
    }

    public static class TimeHolder {
        private java.sql.Time openAt;
        private List<java.sql.Time> slots;

        public java.sql.Time getOpenAt() {
            return openAt;
        }

        public void setOpenAt(final java.sql.Time openAt) {
            this.openAt = openAt;
        }

        public List<java.sql.Time> getSlots() {
            return slots;
        }

        public void setSlots(final List<java.sql.Time> slots) {
            this.slots = slots;
        }
    }

    public static class TimeEdgeHolder {
        private java.sql.Time[] times;
        private Set<java.sql.Time> timeSet;

        public java.sql.Time[] getTimes() {
            return times;
        }

        public void setTimes(final java.sql.Time[] times) {
            this.times = times;
        }

        public Set<java.sql.Time> getTimeSet() {
            return timeSet;
        }

        public void setTimeSet(final Set<java.sql.Time> timeSet) {
            this.timeSet = timeSet;
        }
    }
}
