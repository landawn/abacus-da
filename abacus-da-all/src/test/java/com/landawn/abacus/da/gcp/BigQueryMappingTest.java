package com.landawn.abacus.da.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
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

    // ---- 2026-10-19 sliceL ----

    // A REPEATED column mapped to a non-container property (String, StringBuilder, CharSequence) left the bean-class Dataset
    // column holding the unwrapped ArrayList, while toEntity stores the property's own type (the list's text). Both paths
    // must give the column the property's declared type.
    @Test
    public void testExtractDataConvertsRepeatedColumnToNonContainerPropertyType_sliceL() throws Exception {
        final String[] names = { "text", "builder", "chars" };
        final List<Field> fieldList = new ArrayList<>();
        final List<FieldValue> cells = new ArrayList<>();

        for (final String name : names) {
            fieldList.add(fixBqRepeatedField(name, StandardSQLTypeName.STRING));
            cells.add(fixBqRepeated(verifyCell("a"), verifyCell("b")));
        }

        final FieldList fields = FieldList.of(fieldList);
        final FieldValueList row = FieldValueList.of(cells, fields);
        fixBqStubResult(fields, row);

        final SliceLTextHolder entity = BigQueryExecutor.toEntity(fields, row, SliceLTextHolder.class);
        final Dataset ds = BigQueryExecutor.extractData(mockTableResult, SliceLTextHolder.class);

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(String.class, ds.getColumn("text").get(0).getClass()),
                () -> assertEquals(entity.getText(), ds.getColumn("text").get(0)),
                () -> assertEquals(StringBuilder.class, ds.getColumn("builder").get(0).getClass()),
                () -> assertEquals(entity.getBuilder().toString(), ds.getColumn("builder").get(0).toString()),
                () -> assertTrue(ds.getColumn("chars").get(0) instanceof CharSequence, String.valueOf(ds.getColumn("chars").get(0).getClass())),
                () -> assertEquals(entity.getChars().toString(), ds.getColumn("chars").get(0).toString()));
    }

    // The same REPEATED column mapped to a scalar property that cannot take it fails in toEntity; the Dataset column must
    // not silently hold the ArrayList instead.
    @Test
    public void testExtractDataRejectsRepeatedColumnForScalarPropertyLikeToEntity_sliceL() throws Exception {
        final FieldList fields = FieldList.of(fixBqRepeatedField("count", StandardSQLTypeName.INT64));
        final FieldValueList row = FieldValueList.of(Arrays.asList(fixBqRepeated(verifyCell("1"), verifyCell("2"))), fields);
        fixBqStubResult(fields, row);

        assertThrows(NumberFormatException.class, () -> BigQueryExecutor.toEntity(fields, row, SliceLTextHolder.class));
        assertThrows(NumberFormatException.class, () -> BigQueryExecutor.extractData(mockTableResult, SliceLTextHolder.class));
    }

    // A primitive property column takes the decoded boxed value (no per-cell conversion) and keeps it.
    @Test
    public void testExtractDataPrimitivePropertyColumnHoldsBoxedValue_sliceL() throws Exception {
        final FieldList fields = FieldList.of(Field.of("id", StandardSQLTypeName.INT64), Field.of("name", StandardSQLTypeName.STRING));
        fixBqStubResult(fields, FieldValueList.of(Arrays.asList(verifyCell("42"), verifyCell("n")), fields));

        final Dataset ds = BigQueryExecutor.extractData(mockTableResult, TestEntity.class);

        assertEquals(Integer.valueOf(42), ds.getColumn("id").get(0));
        assertEquals("n", ds.getColumn("name").get(0));

        // The Dataset adopts its freshly built column lists (no copy) and stays an ordinary mutable Dataset.
        ds.set(0, 1, "m");
        assertEquals("m", ds.getColumn("name").get(0));
        assertEquals(java.util.Arrays.asList("id", "name"), ds.columnNames());
    }

    // The scalar row mapper of toList/list/stream named the row type as "class java.lang.Long" and not the column count;
    // the message now matches readRow's (N.convert of a row) for the same input.
    @Test
    public void testScalarRowMapperFieldCountMessageNamesTypeAndColumnCount_sliceL() throws Exception {
        final FieldList fields = FieldList.of(Field.of("a", StandardSQLTypeName.INT64), Field.of("b", StandardSQLTypeName.INT64));
        fixBqStubResult(fields, FieldValueList.of(Arrays.asList(verifyCell("1"), verifyCell("2")), fields));

        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.toList(mockTableResult, Long.class));

        assertEquals("Field count must be 1 to map a row to the single-value type: java.lang.Long, but the row has 2 columns", e.getMessage());
    }

    // Pins the documented abacus-query 4.9.5 behavior (class Javadoc, Naming Policy): after a backslash-escaped quote in a raw
    // Filters.expr, names are emitted as written (4.9.4 converted bonusPoints to bonus_points); names before it, and every
    // name in an expression with a doubled-quote literal, are still translated.
    @Test
    public void testRawExpressionNamesAfterBackslashEscapedQuoteAreNotTranslated_sliceL() throws Exception {
        when(mockTableResult.getSchema()).thenReturn(null);
        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        executor.list(SliceLExprEntity.class, Arrays.asList("accountId"), com.landawn.abacus.query.Filters.expr("firstName = 'O\\'Brien' AND bonusPoints > 1"));
        assertEquals("SELECT account_id AS `accountId` FROM slice_l_expr_entity WHERE first_name = 'O\\'Brien' AND bonusPoints > 1", captureGeneratedSql());

        org.mockito.Mockito.clearInvocations(mockBigQuery);
        executor.list(SliceLExprEntity.class, Arrays.asList("accountId"), com.landawn.abacus.query.Filters.expr("bonusPoints > 1 AND firstName = 'ab'"));
        assertEquals("SELECT account_id AS `accountId` FROM slice_l_expr_entity WHERE bonus_points > 1 AND first_name = 'ab'", captureGeneratedSql());
    }

    public static class SliceLExprEntity {
        private String accountId;
        private String firstName;
        private Long bonusPoints;

        public String getAccountId() {
            return accountId;
        }

        public void setAccountId(final String accountId) {
            this.accountId = accountId;
        }

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(final String firstName) {
            this.firstName = firstName;
        }

        public Long getBonusPoints() {
            return bonusPoints;
        }

        public void setBonusPoints(final Long bonusPoints) {
            this.bonusPoints = bonusPoints;
        }
    }

    // Pins the documented read-only rule under abacus-common 8.2: a field restricted by @AccessFieldByMethod without a setter
    // is read-only, so its column is skipped whatever its direction: the default and an explicit BOTH (SERIALIZE_ONLY), and
    // (2026-10-20 rounify) an explicit DESERIALIZE_ONLY, which cannot make it writable and used to fail the row with
    // UnsupportedOperationException. extractData keeps that column raw, as for any column toEntity ignores.
    @Test
    public void testReadOnlyAccessFieldByMethodPropertyColumns_sliceL() {
        final FieldList fields = FieldList.of(Field.of("code", StandardSQLTypeName.INT64), Field.of("id", StandardSQLTypeName.STRING));
        final FieldValueList row = FieldValueList.of(Arrays.asList(verifyCell("n/a"), verifyCell("1")), fields);

        final SliceLReadOnlySub skipped = BigQueryExecutor.toEntity(fields, row, SliceLReadOnlySub.class);
        assertEquals("1", skipped.getId());
        assertEquals("init", skipped.getCode());
        assertEquals("init", BigQueryExecutor.toEntity(fields, row, SliceLReadOnlyBothSub.class).getCode());

        final SliceLDeserializeOnlySub deserializeOnly = BigQueryExecutor.toEntity(fields, row, SliceLDeserializeOnlySub.class);
        assertEquals("1", deserializeOnly.getId());
        assertEquals("init", deserializeOnly.getCode());

        final TableResult result = mock(TableResult.class);
        when(result.getSchema()).thenReturn(Schema.of(fields));
        when(result.getTotalRows()).thenReturn(1L);
        when(result.iterateAll()).thenReturn(List.of(row));

        final Dataset dataset = BigQueryExecutor.extractData(result, SliceLDeserializeOnlySub.class);
        assertEquals(List.of("n/a"), dataset.getColumn("code"));
        assertEquals(List.of("1"), dataset.getColumn("id"));
    }

    @com.landawn.abacus.annotation.Entity
    public static class SliceLReadOnlyBase {
        private String id;
        @com.landawn.abacus.annotation.AccessFieldByMethod
        private String code = "init";

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getCode() {
            return code;
        }
    }

    public static class SliceLReadOnlySub extends SliceLReadOnlyBase {
    }

    @com.landawn.abacus.annotation.Entity
    public static class SliceLReadOnlyBothBase {
        private String id;
        @com.landawn.abacus.annotation.AccessFieldByMethod
        @com.landawn.abacus.annotation.JsonXmlField(direction = com.landawn.abacus.annotation.JsonXmlField.Direction.BOTH)
        private String code = "init";

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getCode() {
            return code;
        }
    }

    public static class SliceLReadOnlyBothSub extends SliceLReadOnlyBothBase {
    }

    @com.landawn.abacus.annotation.Entity
    public static class SliceLDeserializeOnlyBase {
        private String id;
        @com.landawn.abacus.annotation.AccessFieldByMethod
        @com.landawn.abacus.annotation.JsonXmlField(direction = com.landawn.abacus.annotation.JsonXmlField.Direction.DESERIALIZE_ONLY)
        private String code = "init";

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getCode() {
            return code;
        }
    }

    public static class SliceLDeserializeOnlySub extends SliceLDeserializeOnlyBase {
    }

    public static class SliceLTextHolder {
        private String text;
        private StringBuilder builder;
        private CharSequence chars;
        private Long count;

        public String getText() {
            return text;
        }

        public void setText(final String text) {
            this.text = text;
        }

        public StringBuilder getBuilder() {
            return builder;
        }

        public void setBuilder(final StringBuilder builder) {
            this.builder = builder;
        }

        public CharSequence getChars() {
            return chars;
        }

        public void setChars(final CharSequence chars) {
            this.chars = chars;
        }

        public Long getCount() {
            return count;
        }

        public void setCount(final Long count) {
            this.count = count;
        }
    }

    // ---- 2026-10-20 bqro ----

    // A REPEATED column mapped to a read-only (getter-only, SERIALIZE_ONLY) property is ignored by toEntity; extractData kept the
    // unwrapped List for it until the REPEATED-to-property-type conversion converted it too and failed the whole read
    // (NumberFormatException: ["1", "2"] is not a valid Long). Writable properties still convert as before.
    @Test
    public void testExtractDataKeepsRepeatedColumnOfReadOnlyPropertyUnconverted_bqro() throws Exception {
        final FieldList fields = FieldList.of(fixBqRepeatedField("count", StandardSQLTypeName.INT64), fixBqRepeatedField("text", StandardSQLTypeName.STRING),
                Field.of("total", StandardSQLTypeName.INT64));
        // SDK shape: a REPEATED cell holds a schema-less FieldValueList (FieldValue.fromPb -> FieldValueList.fromPb(list, null)).
        final FieldValueList row = FieldValueList
                .of(Arrays.asList(fixBqRepeated(verifyCell("1"), verifyCell("2")), fixBqRepeated(verifyCell("a"), verifyCell("b")), verifyCell("7")), fields);
        fixBqStubResult(fields, row);

        final BqroReadOnlySub entity = BigQueryExecutor.toEntity(fields, row, BqroReadOnlySub.class);
        final Dataset ds = BigQueryExecutor.extractData(mockTableResult, BqroReadOnlySub.class);

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(Long.valueOf(9), entity.getCount()),
                () -> assertEquals(Long.valueOf(7), entity.getTotal()),
                () -> assertTrue(ds.getColumn("count").get(0) instanceof List, String.valueOf(ds.getColumn("count").get(0))),
                () -> assertEquals(2, ((List<?>) ds.getColumn("count").get(0)).size()),
                () -> assertEquals(String.class, ds.getColumn("text").get(0).getClass()), () -> assertEquals(entity.getText(), ds.getColumn("text").get(0)),
                () -> assertEquals(Long.valueOf(7), ds.getColumn("total").get(0)));
    }

    @com.landawn.abacus.annotation.Entity
    public static class BqroReadOnlyBase {
        private String text;
        private Long total;

        public String getText() {
            return text;
        }

        public void setText(final String text) {
            this.text = text;
        }

        public Long getTotal() {
            return total;
        }

        public void setTotal(final Long total) {
            this.total = total;
        }

        public Long getCount() {
            return 9L;
        }
    }

    public static class BqroReadOnlySub extends BqroReadOnlyBase {
    }

    // ---- 2026-10-20 sliceF: the class Javadoc's strict Boolean rule ----

    public static class BqBoolBean {
        private Boolean flag;
        private boolean enabled;

        public Boolean getFlag() {
            return flag;
        }

        public void setFlag(final Boolean flag) {
            this.flag = flag;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(final boolean enabled) {
            this.enabled = enabled;
        }
    }

    private static FieldList bqBoolFields(final StandardSQLTypeName type) {
        return FieldList.of(Field.of("flag", type), Field.of("enabled", type));
    }

    /** Row (flagText, enabledText) of two {@code type} cells read into BqBoolBean's Boolean and boolean properties. */
    private static BqBoolBean bqBoolEntity(final StandardSQLTypeName type, final String flagText, final String enabledText) {
        final FieldList fields = bqBoolFields(type);
        final FieldValueList row = FieldValueList
                .of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, flagText), FieldValue.of(FieldValue.Attribute.PRIMITIVE, enabledText)), fields);

        return BigQueryExecutor.toEntity(fields, row, BqBoolBean.class);
    }

    /** One-column, one-row result of a {@code type} cell with {@code text}, served by both the mock BigQuery and the mock TableResult. */
    private void stubSingleBqCell(final StandardSQLTypeName type, final String text) throws Exception {
        final FieldList fields = FieldList.of(Field.of("v", type));
        final List<FieldValueList> rows = Arrays.asList(FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, text)), fields));
        when(mockTableResult.getSchema()).thenReturn(Schema.of(fields));
        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.getValues()).thenReturn(rows);
        when(mockTableResult.iterateAll()).thenReturn(rows);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);
    }

    @Test
    public void testToEntity_strictBoolean_blankOrEmptyStringReadsLikeNull_sliceF() {
        for (final String text : new String[] { "", "  " }) {
            final BqBoolBean bean = bqBoolEntity(StandardSQLTypeName.STRING, text, text);
            assertNull(bean.getFlag(), "Boolean property for \"" + text + "\"");
            assertFalse(bean.isEnabled(), "boolean property for \"" + text + "\"");
        }
    }

    @Test
    public void testToEntity_strictBoolean_paddedTokensConvert_sliceF() {
        for (final String text : new String[] { " Y", " true ", "1", "y", "TRUE" }) {
            final BqBoolBean bean = bqBoolEntity(StandardSQLTypeName.STRING, text, text);
            assertEquals(Boolean.TRUE, bean.getFlag(), "Boolean property for \"" + text + "\"");
            assertTrue(bean.isEnabled(), "boolean property for \"" + text + "\"");
        }

        for (final String text : new String[] { "0", "-1", "n", " N ", "False" }) {
            final BqBoolBean bean = bqBoolEntity(StandardSQLTypeName.STRING, text, text);
            assertEquals(Boolean.FALSE, bean.getFlag(), "Boolean property for \"" + text + "\"");
            assertFalse(bean.isEnabled(), "boolean property for \"" + text + "\"");
        }
    }

    @Test
    public void testToEntity_strictBoolean_otherTextThrowsIllegalArgument_sliceF() {
        // the other cell is a valid token, so the failing property is the one under test
        assertThrows(IllegalArgumentException.class, () -> bqBoolEntity(StandardSQLTypeName.STRING, "yes", "1"));
        assertThrows(IllegalArgumentException.class, () -> bqBoolEntity(StandardSQLTypeName.STRING, "1", "yes"));
        assertThrows(IllegalArgumentException.class, () -> bqBoolEntity(StandardSQLTypeName.STRING, "T", "1"));
        assertThrows(IllegalArgumentException.class, () -> bqBoolEntity(StandardSQLTypeName.STRING, "1", "T"));
        assertThrows(IllegalArgumentException.class, () -> bqBoolEntity(StandardSQLTypeName.INT64, "2", "1"));
        assertThrows(IllegalArgumentException.class, () -> bqBoolEntity(StandardSQLTypeName.INT64, "1", "2"));
        assertThrows(IllegalArgumentException.class, () -> bqBoolEntity(StandardSQLTypeName.FLOAT64, "1.0", "1"));
        assertThrows(IllegalArgumentException.class, () -> bqBoolEntity(StandardSQLTypeName.FLOAT64, "1", "1.0"));
    }

    @Test
    public void testToEntity_strictBoolean_boolCellConvertsAndNullCellIsNullOrFalse_sliceF() {
        final BqBoolBean trueFalse = bqBoolEntity(StandardSQLTypeName.BOOL, "true", "false");
        assertEquals(Boolean.TRUE, trueFalse.getFlag());
        assertFalse(trueFalse.isEnabled());

        final BqBoolBean falseTrue = bqBoolEntity(StandardSQLTypeName.BOOL, "false", "true");
        assertEquals(Boolean.FALSE, falseTrue.getFlag());
        assertTrue(falseTrue.isEnabled());

        final BqBoolBean nulls = bqBoolEntity(StandardSQLTypeName.BOOL, null, null);
        assertNull(nulls.getFlag());
        assertFalse(nulls.isEnabled());
    }

    /**
     * The documented difference from the bean path: queryForSingleValue/queryForSingleNonNull, toList and stream read blank or
     * empty STRING text as {@code false} for a Boolean as well as for a boolean value class.
     */
    @Test
    public void testSingleValueReads_strictBoolean_blankIsFalseForBothBooleanAndPrimitive_sliceF() throws Exception {
        final QueryJobConfiguration cfg = QueryJobConfiguration.newBuilder("SELECT v FROM t").build();

        for (final String text : new String[] { "", "  " }) {
            stubSingleBqCell(StandardSQLTypeName.STRING, text);
            final String label = "\"" + text + "\"";

            final Nullable<Boolean> boxed = executor.queryForSingleValue(Boolean.class, "SELECT v FROM t");
            assertTrue(boxed.isPresent(), label);
            assertEquals(Boolean.FALSE, boxed.orElse(null), label);
            assertEquals(Boolean.FALSE, executor.queryForSingleValue(boolean.class, "SELECT v FROM t").orElse(null), label);

            assertEquals(Boolean.FALSE, executor.queryForSingleNonNull(Boolean.class, "SELECT v FROM t").get(), label);
            assertEquals(Boolean.FALSE, executor.queryForSingleNonNull(boolean.class, "SELECT v FROM t").get(), label);

            assertEquals(Arrays.asList(Boolean.FALSE), BigQueryExecutor.toList(mockTableResult, Boolean.class), label);
            assertEquals(Arrays.asList(Boolean.FALSE), BigQueryExecutor.toList(mockTableResult, boolean.class), label);

            assertEquals(Arrays.asList(Boolean.FALSE), executor.stream(Boolean.class, cfg).toList(), label);
            assertEquals(Arrays.asList(Boolean.FALSE), executor.stream(boolean.class, cfg).toList(), label);
        }
    }

    /** The single-value reads apply the same strict rule otherwise: padded tokens convert, anything else is rejected, NULL is null/false. */
    @Test
    public void testSingleValueReads_strictBoolean_tokensAndRejectionsAndNull_sliceF() throws Exception {
        final QueryJobConfiguration cfg = QueryJobConfiguration.newBuilder("SELECT v FROM t").build();

        stubSingleBqCell(StandardSQLTypeName.STRING, " Y");
        assertEquals(Boolean.TRUE, executor.queryForSingleValue(Boolean.class, "SELECT v FROM t").orElse(null));
        assertEquals(Arrays.asList(Boolean.TRUE), BigQueryExecutor.toList(mockTableResult, boolean.class));
        assertEquals(Arrays.asList(Boolean.TRUE), executor.stream(Boolean.class, cfg).toList());

        stubSingleBqCell(StandardSQLTypeName.BOOL, "false");
        assertEquals(Boolean.FALSE, executor.queryForSingleNonNull(Boolean.class, "SELECT v FROM t").get());

        stubSingleBqCell(StandardSQLTypeName.STRING, "yes");
        assertThrows(IllegalArgumentException.class, () -> executor.queryForSingleValue(Boolean.class, "SELECT v FROM t"));
        assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.toList(mockTableResult, Boolean.class));
        assertThrows(IllegalArgumentException.class, () -> executor.stream(boolean.class, cfg).toList());

        stubSingleBqCell(StandardSQLTypeName.INT64, "2");
        assertThrows(IllegalArgumentException.class, () -> executor.queryForSingleNonNull(boolean.class, "SELECT v FROM t"));

        stubSingleBqCell(StandardSQLTypeName.FLOAT64, "1.0");
        assertThrows(IllegalArgumentException.class, () -> executor.queryForSingleValue(boolean.class, "SELECT v FROM t"));

        stubSingleBqCell(StandardSQLTypeName.STRING, null);
        final Nullable<Boolean> nullBoxed = executor.queryForSingleValue(Boolean.class, "SELECT v FROM t");
        assertTrue(nullBoxed.isPresent());
        assertNull(nullBoxed.orElse(null));
        assertEquals(Boolean.FALSE, executor.queryForSingleValue(boolean.class, "SELECT v FROM t").orElse(null));
        assertEquals(Arrays.asList((Boolean) null), BigQueryExecutor.toList(mockTableResult, Boolean.class));
        assertEquals(Arrays.asList(Boolean.FALSE), BigQueryExecutor.toList(mockTableResult, boolean.class));
    }

    // ---- 2026-10-24 sliceL ----

    private static final com.google.cloud.Date SLICEL_JAN_2 = com.google.cloud.Date.fromYearMonthDay(2024, 1, 2);
    private static final com.google.cloud.Date SLICEL_MAR_4 = com.google.cloud.Date.fromYearMonthDay(2024, 3, 4);

    private static SliceLGoogleDateBean sliceLGoogleDateEntity(final Field field, final FieldValue cell) {
        final FieldList fields = FieldList.of(field);

        return BigQueryExecutor.toEntity(fields, FieldValueList.of(Arrays.asList(cell), fields), SliceLGoogleDateBean.class);
    }

    // The client's shape of a REPEATED DATE cell: a schema-less FieldValueList with the REPEATED attribute.
    private static FieldValue sliceLRepeatedDates() {
        return FieldValue.of(FieldValue.Attribute.REPEATED, FieldValueList
                .of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2024-01-02"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2024-03-04"))));
    }

    /**
     * The executor binds a {@code com.google.cloud.Date} as a native DATE parameter, so a DATE cell must read back into one.
     * abacus-common has no conversion to that class: a bean property, a single-value read and a typed array row failed with
     * "Conversion returned java.lang.String for com.google.cloud.Date", a {@code com.google.cloud.Date[]} property with a
     * ParsingException, and a {@code List<com.google.cloud.Date>} property silently held the String cell texts
     * (ClassCastException on element access).
     */
    @Test
    public void testDateCellReadsIntoGoogleCloudDate_sliceL() throws Exception {
        final Field repeatedField = Field.newBuilder("days", StandardSQLTypeName.DATE).setMode(Field.Mode.REPEATED).build();
        final Field repeatedArrayField = Field.newBuilder("dayArray", StandardSQLTypeName.DATE).setMode(Field.Mode.REPEATED).build();
        final QueryJobConfiguration cfg = QueryJobConfiguration.newBuilder("SELECT v FROM t").build();

        org.junit.jupiter.api.Assertions
                .assertAll(
                        () -> assertEquals(SLICEL_JAN_2,
                                sliceLGoogleDateEntity(Field.of("day", StandardSQLTypeName.DATE), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2024-01-02"))
                                        .getDay(),
                                "bean property"),
                        () -> assertNull(
                                sliceLGoogleDateEntity(Field.of("day", StandardSQLTypeName.DATE), FieldValue.of(FieldValue.Attribute.PRIMITIVE, null)).getDay(),
                                "NULL cell"),
                        () -> assertEquals(Arrays.asList(SLICEL_JAN_2, SLICEL_MAR_4), sliceLGoogleDateEntity(repeatedField, sliceLRepeatedDates()).getDays(),
                                "List<com.google.cloud.Date> property elements"),
                        () -> org.junit.jupiter.api.Assertions.assertArrayEquals(new com.google.cloud.Date[] { SLICEL_JAN_2, SLICEL_MAR_4 },
                                sliceLGoogleDateEntity(repeatedArrayField, sliceLRepeatedDates()).getDayArray(), "com.google.cloud.Date[] property"),
                        () -> {
                            final FieldList fields = FieldList.of(Field.of("day", StandardSQLTypeName.DATE));
                            final List<FieldValueList> rows = Arrays
                                    .asList(FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2024-01-02")), fields));
                            final TableResult tableResult = mock(TableResult.class);
                            when(tableResult.getSchema()).thenReturn(Schema.of(fields));
                            when(tableResult.getTotalRows()).thenReturn(1L);
                            when(tableResult.iterateAll()).thenReturn(rows);

                            assertEquals(Arrays.asList(SLICEL_JAN_2), BigQueryExecutor.extractData(tableResult, SliceLGoogleDateBean.class).getColumn("day"),
                                    "bean-class Dataset column");
                        }, () -> {
                            stubSingleBqCell(StandardSQLTypeName.DATE, "2024-01-02");
                            assertEquals(SLICEL_JAN_2, executor.queryForSingleValue(com.google.cloud.Date.class, "SELECT v FROM t").orElse(null),
                                    "queryForSingleValue");
                            assertEquals(SLICEL_JAN_2, executor.queryForSingleNonNull(com.google.cloud.Date.class, "SELECT v FROM t").get(),
                                    "queryForSingleNonNull");
                            assertEquals(Arrays.asList(SLICEL_JAN_2), BigQueryExecutor.toList(mockTableResult, com.google.cloud.Date.class), "toList");
                            assertEquals(Arrays.asList(SLICEL_JAN_2), executor.stream(com.google.cloud.Date.class, cfg).toList(), "stream");
                            org.junit.jupiter.api.Assertions.assertArrayEquals(new com.google.cloud.Date[] { SLICEL_JAN_2 },
                                    BigQueryExecutor.toList(mockTableResult, com.google.cloud.Date[].class).get(0), "typed array row");
                        },
                        // The cell text is exactly what the same value binds as: the write and read sides agree.
                        () -> assertEquals("2024-01-02", BigQueryExecutor.buildQueryParameterValue(SLICEL_JAN_2).get(0).getValue(), "DATE parameter text"));
    }

    /**
     * Pins the class Javadoc's advice for a table or column name that is a GoogleSQL reserved keyword: generated names are
     * not quoted (only the AS aliases are), and a backtick-quoted {@code @Table}/{@code @Column} name is emitted verbatim in
     * every generated statement, including a property name inside a raw {@code Filters.expr}.
     */
    @Test
    public void testReservedKeywordNamesNeedBacktickQuotedAnnotations_sliceL() throws Exception {
        when(mockTableResult.getSchema()).thenReturn(null);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        executor.list(SliceLKeywordOrder.class,
                com.landawn.abacus.query.Filters.and(com.landawn.abacus.query.Filters.eq("from", 1), com.landawn.abacus.query.Filters.expr("from > 0")));
        assertEquals("SELECT `from` AS `from`, name AS `name` FROM `order` WHERE (`from` = ?) AND (`from` > 0)", captureGeneratedSql());

        org.mockito.Mockito.clearInvocations(mockBigQuery);
        final SliceLKeywordOrder order = new SliceLKeywordOrder();
        order.setFrom(1);
        order.setName("a");
        executor.insert(order);
        assertEquals("INSERT INTO `order` (`from`, name) VALUES (?, ?)", captureGeneratedSql());

        org.mockito.Mockito.clearInvocations(mockBigQuery);
        executor.update(order, N.asSet("from"));
        assertEquals("UPDATE `order` SET name = ? WHERE `from` = ?", captureGeneratedSql());

        org.mockito.Mockito.clearInvocations(mockBigQuery);
        executor.delete(SliceLKeywordOrder.class, com.landawn.abacus.query.Filters.eq("from", 1));
        assertEquals("DELETE FROM `order` WHERE `from` = ?", captureGeneratedSql());

        // Without the annotations, the keyword names are emitted bare (a syntax error in BigQuery).
        org.mockito.Mockito.clearInvocations(mockBigQuery);
        executor.list(SliceLBareKeywordOrder.class, com.landawn.abacus.query.Filters.eq("from", 1));
        assertEquals("SELECT from AS `from` FROM order WHERE from = ?", captureGeneratedSql());
    }

    /**
     * Pins the class Javadoc's zone rule: a TIMESTAMP read into a local or zoned java.time target is the instant in the JVM
     * default time zone (the expected values are derived from that zone, so the test holds in any zone).
     */
    @Test
    public void testTimestampIntoLocalAndZonedJavaTimeTargetsUsesJvmDefaultZone_sliceL() throws Exception {
        final java.time.Instant instant = java.time.Instant.ofEpochSecond(1_718_900_000L, 123_456_000L);
        final java.time.ZoneId zone = java.time.ZoneId.systemDefault();
        final java.time.ZonedDateTime zoned = java.time.ZonedDateTime.ofInstant(instant, zone);
        final FieldList fields = FieldList.of(Field.of("zoned", StandardSQLTypeName.TIMESTAMP), Field.of("offset", StandardSQLTypeName.TIMESTAMP),
                Field.of("localDateTime", StandardSQLTypeName.TIMESTAMP), Field.of("localDate", StandardSQLTypeName.TIMESTAMP),
                Field.of("localTime", StandardSQLTypeName.TIMESTAMP));
        final FieldValue cell = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1718900000.123456");
        final SliceLZoneBean bean = BigQueryExecutor.toEntity(fields, FieldValueList.of(Arrays.asList(cell, cell, cell, cell, cell), fields),
                SliceLZoneBean.class);

        stubSingleBqCell(StandardSQLTypeName.TIMESTAMP, "1718900000.123456");

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(zoned, bean.getZoned()),
                () -> assertEquals(java.time.OffsetDateTime.ofInstant(instant, zone), bean.getOffset()),
                () -> assertEquals(zoned.toLocalDateTime(), bean.getLocalDateTime()), () -> assertEquals(zoned.toLocalDate(), bean.getLocalDate()),
                () -> assertEquals(zoned.toLocalTime(), bean.getLocalTime()),
                () -> assertEquals(zoned, executor.queryForSingleValue(java.time.ZonedDateTime.class, "SELECT v FROM t").orElse(null)),
                () -> assertEquals(java.time.OffsetDateTime.ofInstant(instant, zone),
                        executor.queryForSingleValue(java.time.OffsetDateTime.class, "SELECT v FROM t").orElse(null)));
    }

    public static class SliceLZoneBean {
        private java.time.ZonedDateTime zoned;
        private java.time.OffsetDateTime offset;
        private java.time.LocalDateTime localDateTime;
        private java.time.LocalDate localDate;
        private java.time.LocalTime localTime;

        public java.time.ZonedDateTime getZoned() {
            return zoned;
        }

        public void setZoned(final java.time.ZonedDateTime zoned) {
            this.zoned = zoned;
        }

        public java.time.OffsetDateTime getOffset() {
            return offset;
        }

        public void setOffset(final java.time.OffsetDateTime offset) {
            this.offset = offset;
        }

        public java.time.LocalDateTime getLocalDateTime() {
            return localDateTime;
        }

        public void setLocalDateTime(final java.time.LocalDateTime localDateTime) {
            this.localDateTime = localDateTime;
        }

        public java.time.LocalDate getLocalDate() {
            return localDate;
        }

        public void setLocalDate(final java.time.LocalDate localDate) {
            this.localDate = localDate;
        }

        public java.time.LocalTime getLocalTime() {
            return localTime;
        }

        public void setLocalTime(final java.time.LocalTime localTime) {
            this.localTime = localTime;
        }
    }

    public static class SliceLGoogleDateBean {
        private com.google.cloud.Date day;
        private List<com.google.cloud.Date> days;
        private com.google.cloud.Date[] dayArray;

        public com.google.cloud.Date getDay() {
            return day;
        }

        public void setDay(final com.google.cloud.Date day) {
            this.day = day;
        }

        public List<com.google.cloud.Date> getDays() {
            return days;
        }

        public void setDays(final List<com.google.cloud.Date> days) {
            this.days = days;
        }

        public com.google.cloud.Date[] getDayArray() {
            return dayArray;
        }

        public void setDayArray(final com.google.cloud.Date[] dayArray) {
            this.dayArray = dayArray;
        }
    }

    @com.landawn.abacus.annotation.Table("`order`")
    public static class SliceLKeywordOrder {
        @com.landawn.abacus.annotation.Column("`from`")
        private int from;
        private String name;

        public int getFrom() {
            return from;
        }

        public void setFrom(final int from) {
            this.from = from;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    @com.landawn.abacus.annotation.Table("order")
    public static class SliceLBareKeywordOrder {
        private int from;

        public int getFrom() {
            return from;
        }

        public void setFrom(final int from) {
            this.from = from;
        }
    }

    // ---- 2026-10-24 r2 sliceL ----

    private static final FieldList R2SLICEL_PAIR = FieldList.of(Field.of("a", StandardSQLTypeName.INT64), Field.of("b", StandardSQLTypeName.STRING));

    /**
     * A STRUCT cell mapped to an {@code Object} property (or {@code Serializable}, the non-Iterable types the record already
     * is) keeps the raw record, as {@code queryForSingleValue(Object.class, ...)} does. It was read as a single-value row:
     * a STRUCT of two or more fields failed the whole row with "Field count must be 1 to map a row to the single-value type:
     * java.lang.Object" (toEntity, bean-class toList and extractData, records), and a one-field STRUCT silently became the
     * text of its first field. A Collection-typed property still receives the field values.
     */
    @Test
    public void testStructCellReadsIntoObjectPropertyAsRawRecord_r2sliceL() throws Exception {
        final FieldList fields = FieldList.of(Field.of("id", StandardSQLTypeName.INT64), Field.of("payload", StandardSQLTypeName.STRUCT, R2SLICEL_PAIR));
        // The client's shape of a top-level STRUCT cell: the record carries its sub-schema.
        final FieldValueList struct = FieldValueList.of(Arrays.asList(verifyCell("1"), verifyCell("x")), R2SLICEL_PAIR);
        final FieldValueList row = FieldValueList.of(Arrays.asList(verifyCell("7"), FieldValue.of(FieldValue.Attribute.RECORD, struct)), fields);
        final FieldList oneField = FieldList.of(Field.of("a", StandardSQLTypeName.INT64));
        final FieldList oneFieldFields = FieldList.of(Field.of("id", StandardSQLTypeName.INT64), Field.of("payload", StandardSQLTypeName.STRUCT, oneField));
        final FieldValueList oneFieldStruct = FieldValueList.of(Arrays.asList(verifyCell("1")), oneField);
        final FieldValueList oneFieldRow = FieldValueList.of(Arrays.asList(verifyCell("7"), FieldValue.of(FieldValue.Attribute.RECORD, oneFieldStruct)),
                oneFieldFields);

        org.junit.jupiter.api.Assertions.assertAll(() -> {
            final R2SliceLObjectHolder holder = BigQueryExecutor.toEntity(fields, row, R2SliceLObjectHolder.class);
            org.junit.jupiter.api.Assertions.assertSame(struct, holder.getPayload(), "Object property");
            assertEquals("x", ((FieldValueList) holder.getPayload()).get("b").getStringValue());
            assertEquals(7L, holder.getId());
        }, () -> org.junit.jupiter.api.Assertions.assertSame(struct, BigQueryExecutor.toEntity(fields, row, R2SliceLSerializableHolder.class).getPayload(),
                "Serializable property"),
                () -> org.junit.jupiter.api.Assertions.assertSame(struct, BigQueryExecutor.toEntity(fields, row, R2SliceLObjectRecord.class).payload(),
                        "record component"),
                () -> org.junit.jupiter.api.Assertions.assertSame(oneFieldStruct,
                        BigQueryExecutor.toEntity(oneFieldFields, oneFieldRow, R2SliceLObjectHolder.class).getPayload(), "one-field STRUCT"),
                () -> {
                    fixBqStubResult(fields, row);
                    org.junit.jupiter.api.Assertions.assertSame(struct,
                            BigQueryExecutor.toList(mockTableResult, R2SliceLObjectHolder.class).get(0).getPayload(), "toList bean rows");
                    org.junit.jupiter.api.Assertions.assertSame(struct,
                            BigQueryExecutor.extractData(mockTableResult, R2SliceLObjectHolder.class).getColumn("payload").get(0), "bean-class Dataset");
                },
                // Unchanged: a Collection-typed property gets the field values, not the FieldValue wrappers, and the
                // single-value Object read, which the property now matches, keeps the record.
                () -> assertEquals(Arrays.asList("1", "x"), BigQueryExecutor.toEntity(fields, row, R2SliceLListHolder.class).getPayload(), "List property"),
                () -> {
                    final FieldList single = FieldList.of(fields.get(1));
                    final List<FieldValueList> rows = Arrays
                            .asList(FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.RECORD, struct)), single));
                    when(mockTableResult.getSchema()).thenReturn(Schema.of(single));
                    when(mockTableResult.getTotalRows()).thenReturn(1L);
                    when(mockTableResult.getValues()).thenReturn(rows);
                    when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);
                    org.junit.jupiter.api.Assertions.assertSame(struct, executor.queryForSingleValue(Object.class, "SELECT payload FROM t").orElse(null),
                            "queryForSingleValue(Object.class)");
                });
    }

    /**
     * {@code Beans.isBeanClass} accepts an {@code @Entity}-annotated {@code Map} subclass, so the public {@code toEntity} mapped
     * such a target as a bean: its entries stayed empty and only a column matching a bean property was assigned, while
     * {@code toList}, {@code stream}, {@code extractData} and {@code N.convert} fill the same class with one entry per column.
     */
    @Test
    public void testToEntityOfAnnotatedMapSubclassFillsItsEntries_r2sliceL() throws Exception {
        final FieldList fields = FieldList.of(Field.of("code", StandardSQLTypeName.INT64), Field.of("label", StandardSQLTypeName.STRING));
        final FieldValueList row = FieldValueList.of(Arrays.asList(verifyCell("1"), verifyCell("L")), fields);
        final Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("code", "1");
        expected.put("label", "L");

        org.junit.jupiter.api.Assertions.assertAll(() -> {
            final R2SliceLMapEntity entity = BigQueryExecutor.toEntity(fields, row, R2SliceLMapEntity.class);
            assertEquals(expected, entity);
            assertNull(entity.getLabel(), "bean property not assigned");
        }, () -> assertEquals(expected, BigQueryExecutor.toEntity(Schema.of(fields), row, R2SliceLMapEntity.class), "Schema overload"),
                () -> assertEquals(expected, BigQueryExecutor.toEntity(row, R2SliceLMapEntity.class), "schema-from-row overload"), () -> {
                    fixBqStubResult(fields, row);
                    assertEquals(BigQueryExecutor.toList(mockTableResult, R2SliceLMapEntity.class).get(0),
                            BigQueryExecutor.toEntity(fields, row, R2SliceLMapEntity.class), "same as a toList row");
                }, () -> org.junit.jupiter.api.Assertions.assertInstanceOf(R2SliceLMapEntity.class,
                        BigQueryExecutor.toEntity(fields, row, R2SliceLMapEntity.class)));
    }

    /**
     * Pins the class Javadoc: the fields of a STRUCT read into a typed {@code Map}/{@code Collection} property are converted
     * from their cell text, without the native TIMESTAMP/BYTES decoding, while the same STRUCT mapped to a bean is decoded.
     */
    @Test
    public void testStructFieldsOfTypedContainerPropertyAreConvertedFromText_r2sliceL() throws Exception {
        final FieldList sub = FieldList.of(Field.of("seenAt", StandardSQLTypeName.TIMESTAMP));
        final FieldList fields = FieldList.of(Field.of("times", StandardSQLTypeName.STRUCT, sub), Field.of("visit", StandardSQLTypeName.STRUCT, sub));
        final FieldValue cell = FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(Arrays.asList(verifyCell("1718900000.5")), sub));

        assertThrows(RuntimeException.class, () -> BigQueryExecutor.toEntity(FieldList.of(fields.get(0)),
                FieldValueList.of(Arrays.asList(cell), FieldList.of(fields.get(0))), R2SliceLTimesHolder.class));
        assertEquals(java.time.Instant.ofEpochSecond(1_718_900_000L, 500_000_000L),
                BigQueryExecutor
                        .toEntity(FieldList.of(fields.get(1)), FieldValueList.of(Arrays.asList(cell), FieldList.of(fields.get(1))), R2SliceLTimesHolder.class)
                        .getVisit()
                        .getSeenAt());
    }

    public static class R2SliceLObjectHolder {
        private long id;
        private Object payload;

        public long getId() {
            return id;
        }

        public void setId(final long id) {
            this.id = id;
        }

        public Object getPayload() {
            return payload;
        }

        public void setPayload(final Object payload) {
            this.payload = payload;
        }
    }

    public static class R2SliceLSerializableHolder {
        private java.io.Serializable payload;

        public java.io.Serializable getPayload() {
            return payload;
        }

        public void setPayload(final java.io.Serializable payload) {
            this.payload = payload;
        }
    }

    public static class R2SliceLListHolder {
        private List<Object> payload;

        public List<Object> getPayload() {
            return payload;
        }

        public void setPayload(final List<Object> payload) {
            this.payload = payload;
        }
    }

    public record R2SliceLObjectRecord(long id, Object payload) {
    }

    public static class R2SliceLTimesHolder {
        private Map<String, java.time.Instant> times;
        private Visit visit;

        public Map<String, java.time.Instant> getTimes() {
            return times;
        }

        public void setTimes(final Map<String, java.time.Instant> times) {
            this.times = times;
        }

        public Visit getVisit() {
            return visit;
        }

        public void setVisit(final Visit visit) {
            this.visit = visit;
        }
    }

    @com.landawn.abacus.annotation.Entity
    public static class R2SliceLMapEntity extends LinkedHashMap<String, Object> {
        private static final long serialVersionUID = 1L;
        private String label;

        public String getLabel() {
            return label;
        }

        public void setLabel(final String label) {
            this.label = label;
        }
    }

    // ---- 2026-10-24 r2 main ----

    /**
     * An {@code Iterable<Long>} property is not given the raw STRUCT record: the record's elements are {@code FieldValue}
     * wrappers, so the property held them and failed with ClassCastException only when iterated. It fails at read time
     * again, as before the raw-record rule for {@code Object}/{@code Serializable} properties.
     */
    @Test
    public void testStructCellIntoIterablePropertyIsNotTheRawRecord_r2main() throws Exception {
        final FieldList fields = FieldList.of(Field.of("payload", StandardSQLTypeName.STRUCT, R2SLICEL_PAIR));
        final FieldValueList struct = FieldValueList.of(Arrays.asList(verifyCell("1"), verifyCell("x")), R2SLICEL_PAIR);
        final FieldValueList row = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.RECORD, struct)), fields);

        assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.toEntity(fields, row, R2MainIterableHolder.class));
    }

    public static class R2MainIterableHolder {
        private Iterable<Long> payload;

        public Iterable<Long> getPayload() {
            return payload;
        }

        public void setPayload(final Iterable<Long> payload) {
            this.payload = payload;
        }
    }

    // ---- 2026-10-27 sliceL ----

    /**
     * The client decodes a RANGE cell into a {@code Range} (Attribute.RANGE), not text. A String target received the Range's
     * toString(), AutoValue's debug text {@code Range{startInner=2020-01-01, endInner=null, type=FieldElementType{type=DATE}}};
     * it now receives the RANGE literal BigQuery sent ({@code UNBOUNDED} for an open bound), on every read path. A Range or
     * Object target and raw rows keep the Range itself. RED on HEAD for every text case.
     */
    @Test
    public void testRangeCellReadsAsRangeLiteralIntoTextTargets_sliceL() throws Exception {
        final com.google.cloud.bigquery.FieldElementType dateElement = com.google.cloud.bigquery.FieldElementType.newBuilder().setType("DATE").build();
        final com.google.cloud.bigquery.Range open = com.google.cloud.bigquery.Range.of("[2020-01-01, UNBOUNDED)", dateElement);
        final com.google.cloud.bigquery.Range closed = com.google.cloud.bigquery.Range.of("[2020-01-01, 2020-12-31)", dateElement);
        final FieldList fields = FieldList.of(Field.newBuilder("period", StandardSQLTypeName.RANGE).setRangeElementType(dateElement).build(),
                Field.newBuilder("raw", StandardSQLTypeName.RANGE).setRangeElementType(dateElement).build());
        final FieldValueList row = FieldValueList
                .of(Arrays.asList(FieldValue.of(FieldValue.Attribute.RANGE, open), FieldValue.of(FieldValue.Attribute.RANGE, closed)), fields);
        final FieldList single = FieldList.of(fields.get(1));
        final FieldValueList singleRow = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.RANGE, closed)), single);

        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.getSchema()).thenReturn(Schema.of(single));
        when(mockTableResult.getValues()).thenReturn(List.of(singleRow));
        when(mockTableResult.iterateAll()).thenReturn(List.of(singleRow));
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        final TableResult twoColumns = mock(TableResult.class);
        when(twoColumns.getTotalRows()).thenReturn(1L);
        when(twoColumns.getSchema()).thenReturn(Schema.of(fields));
        when(twoColumns.iterateAll()).thenReturn(List.of(row));

        final SliceLRangeHolder holder = BigQueryExecutor.toEntity(fields, row, SliceLRangeHolder.class);
        final Dataset ds = BigQueryExecutor.extractData(twoColumns, SliceLRangeHolder.class);

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals("[2020-01-01, UNBOUNDED)", holder.getPeriod()),
                () -> assertTrue(holder.getRaw() == closed, String.valueOf(holder.getRaw())), () -> assertEquals("[2020-01-01, UNBOUNDED)", ds.get(0, 0)),
                () -> assertEquals("[2020-01-01, 2020-12-31)", executor.queryForSingleValue(String.class, "SELECT raw FROM t").orElseNull()),
                () -> assertEquals("[2020-01-01, 2020-12-31)", executor.list(String.class, "SELECT raw FROM t").get(0)),
                () -> assertEquals("[2020-01-01, 2020-12-31)", executor.list(String[].class, "SELECT raw FROM t").get(0)[0]),
                () -> assertEquals("[2020-01-01, 2020-12-31)", N.convert(singleRow, String.class)),
                () -> assertTrue(executor.queryForSingleValue(com.google.cloud.bigquery.Range.class, "SELECT raw FROM t").orElseNull() == closed),
                () -> assertTrue(executor.list(Object[].class, "SELECT raw FROM t").get(0)[0] == closed));
    }

    /**
     * Review 2026-10-27 r2E: the RANGE literal for every bound shape (open start, both open, a NULL bound, which Range.of reads like
     * UNBOUNDED) and for TIMESTAMP and DATETIME elements, whose bound text is kept as sent; for CharSequence targets other than String;
     * and for a RANGE field of a STRUCT read into a nested bean's String property (RED on HEAD). A Map row keeps the Range itself.
     */
    @Test
    public void testRangeLiteralBoundsElementTypesAndNestedTargets_r2E() throws Exception {
        final com.google.cloud.bigquery.FieldElementType date = com.google.cloud.bigquery.FieldElementType.newBuilder().setType("DATE").build();
        final com.google.cloud.bigquery.FieldElementType timestamp = com.google.cloud.bigquery.FieldElementType.newBuilder().setType("TIMESTAMP").build();
        final com.google.cloud.bigquery.FieldElementType datetime = com.google.cloud.bigquery.FieldElementType.newBuilder().setType("DATETIME").build();
        final java.util.function.BiFunction<String, com.google.cloud.bigquery.FieldElementType, FieldValueList> rangeRow = (text, type) -> FieldValueList.of(
                Arrays.asList(FieldValue.of(FieldValue.Attribute.RANGE, com.google.cloud.bigquery.Range.of(text, type))),
                FieldList.of(Field.newBuilder("r", StandardSQLTypeName.RANGE).setRangeElementType(type).build()));

        final Field rangeField = Field.newBuilder("r", StandardSQLTypeName.RANGE).setRangeElementType(date).build();
        final FieldList structFields = FieldList.of(Field.of("s", StandardSQLTypeName.STRUCT, FieldList.of(rangeField)));
        final FieldValueList structRow = FieldValueList.of(
                Arrays.asList(FieldValue.of(FieldValue.Attribute.RECORD,
                        FieldValueList.of(Arrays
                                .asList(FieldValue.of(FieldValue.Attribute.RANGE, com.google.cloud.bigquery.Range.of("[2021-01-01, UNBOUNDED)", date)))))),
                structFields);
        final FieldValueList mapRow = rangeRow.apply("[2020-01-01, 2020-12-31)", date);

        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertEquals("[UNBOUNDED, 2020-12-31)", N.convert(rangeRow.apply("[UNBOUNDED, 2020-12-31)", date), String.class)),
                () -> assertEquals("[UNBOUNDED, UNBOUNDED)", N.convert(rangeRow.apply("[UNBOUNDED, UNBOUNDED)", date), String.class)),
                () -> assertEquals("[UNBOUNDED, 2020-12-31)", N.convert(rangeRow.apply("[NULL, 2020-12-31)", date), String.class)),
                () -> assertEquals("[2020-01-01 12:00:00+00, UNBOUNDED)",
                        N.convert(rangeRow.apply("[2020-01-01 12:00:00+00, UNBOUNDED)", timestamp), String.class)),
                () -> assertEquals("[2020-01-01T10:00:00.123456, 2020-01-02T00:00:00)",
                        N.convert(rangeRow.apply("[2020-01-01T10:00:00.123456, 2020-01-02T00:00:00)", datetime), String.class)),
                () -> assertEquals("[2020-01-01, 2020-12-31)", N.convert(rangeRow.apply("[2020-01-01, 2020-12-31)", date), StringBuilder.class).toString()),
                () -> assertEquals("[2020-01-01, 2020-12-31)", N.convert(rangeRow.apply("[2020-01-01, 2020-12-31)", date), CharSequence.class)),
                () -> assertTrue(BigQueryExecutor.toMap(mapRow).get("r") == mapRow.get(0).getValue()),
                () -> assertEquals("[2021-01-01, UNBOUNDED)", BigQueryExecutor.toEntity(structFields, structRow, R2ERangeOuter.class).getS().getR()));
    }

    public static class R2ERangeOuter {
        private R2ERangeInner s;

        public R2ERangeInner getS() {
            return s;
        }

        public void setS(final R2ERangeInner s) {
            this.s = s;
        }
    }

    public static class R2ERangeInner {
        private String r;

        public String getR() {
            return r;
        }

        public void setR(final String r) {
            this.r = r;
        }
    }

    public static class SliceLRangeHolder {
        private String period;
        private Object raw;

        public String getPeriod() {
            return period;
        }

        public void setPeriod(final String period) {
            this.period = period;
        }

        public Object getRaw() {
            return raw;
        }

        public void setRaw(final Object raw) {
            this.raw = raw;
        }
    }

    /**
     * Pins the toEntity(FieldList, ...) Javadoc: a dotted column can only create a nested bean that has a no-argument
     * constructor, so a nested record fails the read with IllegalArgumentException, while the same record read from a STRUCT
     * column is mapped.
     */
    @Test
    public void testDottedColumnCannotCreateNestedRecordButStructCan_sliceL() throws Exception {
        final FieldList dotted = FieldList.of(Field.of("name", StandardSQLTypeName.STRING), Field.of("addr.city", StandardSQLTypeName.STRING));
        final FieldValueList dottedRow = FieldValueList.of(Arrays.asList(verifyCell("n"), verifyCell("SF")), dotted);
        final FieldList sub = FieldList.of(Field.of("city", StandardSQLTypeName.STRING));
        final FieldList struct = FieldList.of(Field.of("name", StandardSQLTypeName.STRING), Field.of("addr", StandardSQLTypeName.STRUCT, sub));
        final FieldValueList structRow = FieldValueList
                .of(Arrays.asList(verifyCell("n"), FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(Arrays.asList(verifyCell("SF"))))), struct);

        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.toEntity(dotted, dottedRow, SliceLAddrHolder.class)),
                () -> assertEquals(new SliceLAddr("SF"), BigQueryExecutor.toEntity(struct, structRow, SliceLAddrHolder.class).getAddr()));
    }

    public record SliceLAddr(String city) {
    }

    public static class SliceLAddrHolder {
        private String name;
        private SliceLAddr addr;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public SliceLAddr getAddr() {
            return addr;
        }

        public void setAddr(final SliceLAddr addr) {
            this.addr = addr;
        }
    }
}
