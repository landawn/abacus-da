package com.landawn.abacus.da.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableResult;
import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.u.Nullable;
import com.landawn.abacus.util.stream.Stream;

public class BigQueryExecutorTest extends TestBase {

    @Mock
    private BigQuery mockBigQuery;

    @Mock
    private TableResult mockTableResult;

    @Mock
    private Schema mockSchema;

    @Mock
    private FieldList mockFieldList;

    @Mock
    private FieldValueList mockFieldValueList;

    private BigQueryExecutor executor;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        executor = new BigQueryExecutor(mockBigQuery);
    }

    @Test
    public void testConstructorWithBigQuery() {
        BigQueryExecutor executor = new BigQueryExecutor(mockBigQuery);
        assertNotNull(executor);
        assertEquals(mockBigQuery, executor.bigQuery());
    }

    @Test
    public void testConstructorWithBigQueryAndNamingPolicy() {
        BigQueryExecutor executor = new BigQueryExecutor(mockBigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        assertNotNull(executor);
        assertEquals(mockBigQuery, executor.bigQuery());
    }

    @Test
    public void testBigQuery() {
        assertEquals(mockBigQuery, executor.bigQuery());
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
    public void testToListWithEmptyTableResult() {
        when(mockTableResult.getTotalRows()).thenReturn(0L);

        List<TestEntity> list = BigQueryExecutor.toList(mockTableResult, TestEntity.class);

        assertNotNull(list);
        assertEquals(0, list.size());
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
    public void testExtractDataWithEmptyTableResult() {
        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockTableResult.getSchema()).thenReturn(null);

        Dataset dataset = BigQueryExecutor.extractData(mockTableResult, TestEntity.class);

        assertNotNull(dataset);
        assertEquals(0, dataset.size());
    }

    @Test
    public void testInsertEntity() throws Exception {
        TestEntity entity = new TestEntity();
        entity.setId(1);
        entity.setName("Test");

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        TableResult result = executor.insert(entity);

        assertEquals(mockTableResult, result);
        verify(mockBigQuery).query(any(QueryJobConfiguration.class));
    }

    @Test
    public void testInsertWithClassAndProps() throws Exception {
        Map<String, Object> props = new HashMap<>();
        props.put("id", 1);
        props.put("name", "Test");

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        TableResult result = executor.insert(TestEntity.class, props);

        assertEquals(mockTableResult, result);
        verify(mockBigQuery).query(any(QueryJobConfiguration.class));
    }

    @Test
    public void testUpdateEntity() throws Exception {
        TestEntity entity = new TestEntity();
        entity.setId(1);
        entity.setName("UpdatedTest");

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        TableResult result = executor.update(entity);

        assertEquals(mockTableResult, result);
        verify(mockBigQuery).query(any(QueryJobConfiguration.class));
    }

    @Test
    public void testUpdateEntityWithPrimaryKeyNames() throws Exception {
        TestEntity entity = new TestEntity();
        entity.setId(1);
        entity.setName("UpdatedTest");
        Set<String> primaryKeyNames = N.asSet("id");

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        TableResult result = executor.update(entity, primaryKeyNames);

        assertEquals(mockTableResult, result);
        verify(mockBigQuery).query(any(QueryJobConfiguration.class));
    }

    @Test
    public void testUpdateWithClassPropsAndCondition() throws Exception {
        Map<String, Object> props = new HashMap<>();
        props.put("name", "UpdatedTest");
        Condition condition = Filters.eq("id", 1);

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        TableResult result = executor.update(TestEntity.class, props, condition);

        assertEquals(mockTableResult, result);
        verify(mockBigQuery).query(any(QueryJobConfiguration.class));
    }

    @Test
    public void testDeleteEntity() throws Exception {
        TestEntity entity = new TestEntity();
        entity.setId(1);

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        TableResult result = executor.delete(entity);

        assertEquals(mockTableResult, result);
        verify(mockBigQuery).query(any(QueryJobConfiguration.class));
    }

    @Test
    public void testDeleteWithClassAndIds() throws Exception {
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        TableResult result = executor.delete(TestEntity.class, 1);

        assertEquals(mockTableResult, result);
        verify(mockBigQuery).query(any(QueryJobConfiguration.class));
    }

    @Test
    public void testDeleteWithClassAndCondition() throws Exception {
        Condition condition = Filters.eq("id", 1);

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        TableResult result = executor.delete(TestEntity.class, condition);

        assertEquals(mockTableResult, result);
        verify(mockBigQuery).query(any(QueryJobConfiguration.class));
    }

    @Test
    public void testExistsWithClassAndIds() throws Exception {
        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        boolean exists = executor.exists(TestEntity.class, 1);

        assertTrue(exists);
        verify(mockBigQuery).query(any(QueryJobConfiguration.class));
    }

    @Test
    public void testExistsWithClassAndCondition() throws Exception {
        Condition condition = Filters.eq("id", 1);

        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        boolean exists = executor.exists(TestEntity.class, condition);

        assertFalse(exists);
        verify(mockBigQuery).query(any(QueryJobConfiguration.class));
    }

    @Test
    public void testQueryForSingleResultWithTargetClass() throws Exception {
        Field field = Field.of("name", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(field);

        FieldValueList row = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "TestName")), fields);

        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.getValues()).thenReturn(Arrays.asList(row));
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Nullable<String> result = executor.queryForSingleResult(TestEntity.class, String.class, "name", Filters.eq("id", 1));

        assertTrue(result.isPresent());
        assertEquals("TestName", result.get());
    }

    @Test
    public void testQueryForSingleResultWithQuery() throws Exception {
        Field field = Field.of("count", StandardSQLTypeName.INT64);
        FieldList fields = FieldList.of(field);

        FieldValueList row = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "5")), fields);

        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.getValues()).thenReturn(Arrays.asList(row));
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Nullable<Integer> result = executor.queryForSingleResult(Integer.class, "SELECT COUNT(*) FROM test_table", new Object[0]);

        assertTrue(result.isPresent());
        assertEquals(5, result.get());
    }

    @Test
    public void testQueryWithClassAndCondition() throws Exception {
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
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Dataset dataset = executor.query(TestEntity.class, Filters.eq("id", 1));

        assertNotNull(dataset);
        assertEquals(1, dataset.size());
    }

    @Test
    public void testQueryWithClassSelectPropsAndCondition() throws Exception {
        Field field = Field.of("name", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(field);
        Schema schema = Schema.of(fields);

        List<FieldValueList> rows = new ArrayList<>();
        rows.add(FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Name1")), fields));

        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(rows);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Collection<String> selectProps = Arrays.asList("name");
        Dataset dataset = executor.query(TestEntity.class, selectProps, Filters.eq("id", 1));

        assertNotNull(dataset);
        assertEquals(1, dataset.size());
        assertEquals(1, dataset.columnCount());
    }

    @Test
    public void testQueryWithClassQueryAndParameters() throws Exception {
        Field field1 = Field.of("id", StandardSQLTypeName.INT64);
        Field field2 = Field.of("name", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(field1, field2);
        Schema schema = Schema.of(fields);

        List<FieldValueList> rows = new ArrayList<>();

        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(rows);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Dataset dataset = executor.query(TestEntity.class, "SELECT * FROM test_table WHERE id = ?", 1);

        assertNotNull(dataset);
        assertEquals(0, dataset.size());
    }

    @Test
    public void testListWithClassAndCondition() throws Exception {
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
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        List<TestEntity> list = executor.list(TestEntity.class, Filters.eq("id", 1));

        assertNotNull(list);
        assertEquals(1, list.size());
    }

    @Test
    public void testListWithClassSelectPropsAndCondition() throws Exception {
        Field field = Field.of("name", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(field);
        Schema schema = Schema.of(fields);

        List<FieldValueList> rows = new ArrayList<>();
        rows.add(FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Name1")), fields));

        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(rows);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Collection<String> selectProps = Arrays.asList("name");
        List<Map> list = executor.list(Map.class, selectProps, Filters.eq("id", 1));

        assertNotNull(list);
        assertEquals(1, list.size());
    }

    @Test
    public void testListWithClassQueryAndParameters() throws Exception {
        Field field1 = Field.of("id", StandardSQLTypeName.INT64);
        Field field2 = Field.of("name", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(field1, field2);
        Schema schema = Schema.of(fields);

        List<FieldValueList> rows = new ArrayList<>();

        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(rows);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        List<TestEntity> list = executor.list(TestEntity.class, "SELECT * FROM test_table WHERE id = ?", 1);

        assertNotNull(list);
        assertEquals(0, list.size());
    }

    @Test
    public void testStreamWithClassAndCondition() throws Exception {
        List<FieldValueList> rows = new ArrayList<>();

        when(mockTableResult.iterateAll()).thenReturn(rows);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Stream<TestEntity> stream = executor.stream(TestEntity.class, Filters.eq("id", 1));

        assertNotNull(stream);
        assertEquals(0, stream.count());
    }

    @Test
    public void testStreamWithClassSelectPropsAndCondition() throws Exception {
        List<FieldValueList> rows = new ArrayList<>();

        when(mockTableResult.iterateAll()).thenReturn(rows);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Collection<String> selectProps = Arrays.asList("name");
        Stream<TestEntity> stream = executor.stream(TestEntity.class, selectProps, Filters.eq("id", 1));

        assertNotNull(stream);
        assertEquals(0, stream.count());
    }

    @Test
    public void testStreamWithClassQueryAndParameters() throws Exception {
        List<FieldValueList> rows = new ArrayList<>();

        when(mockTableResult.iterateAll()).thenReturn(rows);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Stream<TestEntity> stream = executor.stream(TestEntity.class, "SELECT * FROM test_table WHERE id = ?", 1);

        assertNotNull(stream);
        assertEquals(0, stream.count());
    }

    @Test
    public void testStreamWithClassAndQueryJobConfiguration() throws Exception {
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder("SELECT * FROM test_table").build();
        List<FieldValueList> rows = new ArrayList<>();

        when(mockTableResult.iterateAll()).thenReturn(rows);
        when(mockBigQuery.query(queryConfig)).thenReturn(mockTableResult);

        Stream<TestEntity> stream = executor.stream(TestEntity.class, queryConfig);

        assertNotNull(stream);
        assertEquals(0, stream.count());
    }

    @Test
    public void testStreamWithQueryJobConfiguration() throws Exception {
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder("SELECT * FROM test_table").build();
        List<FieldValueList> rows = new ArrayList<>();

        when(mockTableResult.iterateAll()).thenReturn(rows);
        when(mockBigQuery.query(queryConfig)).thenReturn(mockTableResult);

        Stream<FieldValueList> stream = executor.stream(queryConfig);

        assertNotNull(stream);
        assertEquals(0, stream.count());
    }

    @Test
    public void testExecuteWithQueryAndParameters() throws Exception {
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        TableResult result = executor.execute("SELECT * FROM test_table WHERE id = ?", 1);

        assertEquals(mockTableResult, result);
        verify(mockBigQuery).query(any(QueryJobConfiguration.class));
    }

    //    @Test
    //    public void testExecuteWithExceptionHandling() throws Exception {
    //        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenThrow(new JobException("Test exception"));
    //        
    //        assertThrows(RuntimeException.class, () -> {
    //            executor.execute("SELECT * FROM test_table", new Object[0]);
    //        });
    //    }

    @Test
    public void testBuildQueryParameterValueWithDifferentTypes() {
        Object[] parameters = new Object[] { "string", true, 'c', (byte) 1, (short) 2, 3, 4L, 5.0f, 6.0d, new BigDecimal("7.0"), new java.util.Date(),
                new byte[] { 1, 2, 3 } };

        List<QueryParameterValue> values = BigQueryExecutor.buildQueryParameterValue(parameters);

        assertNotNull(values);
        assertEquals(parameters.length, values.size());
    }

    @Test
    public void testBuildQueryParameterValueWithEmptyParameters() {
        List<QueryParameterValue> values = BigQueryExecutor.buildQueryParameterValue();

        assertNotNull(values);
        assertEquals(0, values.size());
    }

    @Test
    public void testBuildQueryParameterValueWithComplexTypes() {
        List<String> list = Arrays.asList("a", "b", "c");
        Map<String, Object> map = new HashMap<>();
        map.put("key", "value");
        TestEntity entity = new TestEntity();
        entity.setId(1);

        Object[] parameters = new Object[] { list, map, entity, new String[] { "x", "y" } };

        List<QueryParameterValue> values = BigQueryExecutor.buildQueryParameterValue(parameters);

        assertNotNull(values);
        assertEquals(parameters.length, values.size());
    }

    @Test
    public void testBuildQueryParameterValueRejectsUntypedNull() {
        assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.buildQueryParameterValue("abc", null));
    }

    @Test
    public void testBuildQueryParameterValueAcceptsTypedNull() {
        final QueryParameterValue typedNull = QueryParameterValue.int64((Long) null);

        List<QueryParameterValue> values = BigQueryExecutor.buildQueryParameterValue(typedNull);

        assertEquals(1, values.size());
        assertTrue(values.get(0) == typedNull);
    }

    @Test
    public void testToEntityWithNestedFieldValueList() {
        Field nestedField1 = Field.of("nestedId", StandardSQLTypeName.INT64);
        Field nestedField2 = Field.of("nestedName", StandardSQLTypeName.STRING);
        FieldList nestedFields = FieldList.of(nestedField1, nestedField2);

        FieldValueList nestedFieldValueList = FieldValueList.of(
                Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "999"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "NestedName")), nestedFields);

        Field field1 = Field.of("id", StandardSQLTypeName.INT64);
        Field field2 = Field.of("nested", StandardSQLTypeName.STRUCT);
        FieldList fields = FieldList.of(field1, field2);

        FieldValueList fieldValueList = FieldValueList.of(
                Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "123"), FieldValue.of(FieldValue.Attribute.RECORD, nestedFieldValueList)), fields);

        TestEntityWithNested entity = BigQueryExecutor.toEntity(fields, fieldValueList, TestEntityWithNested.class);

        assertNotNull(entity);
        assertEquals(123, entity.getId());
        assertNotNull(entity.getNested());
    }

    @Test
    public void testToMapWithNestedFieldValueList() {
        Field nestedField = Field.of("nestedValue", StandardSQLTypeName.STRING);
        FieldList nestedFields = FieldList.of(nestedField);

        FieldValueList nestedFieldValueList = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "NestedValue")), nestedFields);

        Field field1 = Field.of("id", StandardSQLTypeName.INT64);
        Field field2 = Field.of("nested", StandardSQLTypeName.STRUCT);
        FieldList fields = FieldList.of(field1, field2);

        FieldValueList fieldValueList = FieldValueList.of(
                Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "123"), FieldValue.of(FieldValue.Attribute.RECORD, nestedFieldValueList)), fields);

        Map<String, Object> map = BigQueryExecutor.toMap(fields, fieldValueList);

        assertNotNull(map);
        assertEquals("123", map.get("id"));
        assertTrue(map.get("nested") instanceof Map);
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
    public void testExecutorWithDifferentNamingPolicies() throws Exception {
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        // Test SNAKE_CASE (default)
        BigQueryExecutor executor1 = new BigQueryExecutor(mockBigQuery, NamingPolicy.SNAKE_CASE);
        TestEntity entity1 = new TestEntity();
        entity1.setId(1);
        executor1.insert(entity1);

        // Test SCREAMING_SNAKE_CASE
        BigQueryExecutor executor2 = new BigQueryExecutor(mockBigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        TestEntity entity2 = new TestEntity();
        entity2.setId(2);
        executor2.insert(entity2);

        // Test CAMEL_CASE
        BigQueryExecutor executor3 = new BigQueryExecutor(mockBigQuery, NamingPolicy.CAMEL_CASE);
        TestEntity entity3 = new TestEntity();
        entity3.setId(3);
        executor3.insert(entity3);

        verify(mockBigQuery, times(3)).query(any(QueryJobConfiguration.class));
    }

    @Test
    public void testUnsupportedNamingPolicy() {
        BigQueryExecutor executorWithUnsupportedPolicy = new BigQueryExecutor(mockBigQuery, NamingPolicy.NO_CHANGE);
        TestEntity entity = new TestEntity();
        entity.setId(1);

        assertThrows(RuntimeException.class, () -> {
            executorWithUnsupportedPolicy.insert(entity);
        });
    }

    @Test
    public void testUpdateWithEmptyPrimaryKeyNames() {
        TestEntity entity = new TestEntity();
        entity.setId(1);
        Set<String> emptyKeyNames = N.toSet();

        assertThrows(IllegalArgumentException.class, () -> {
            executor.update(entity, emptyKeyNames);
        });
    }

    @Test
    public void testUpdateWithEmptyProps() {
        Map<String, Object> emptyProps = new HashMap<>();

        assertThrows(IllegalArgumentException.class, () -> {
            executor.update(TestEntity.class, emptyProps, Filters.eq("id", 1));
        });
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
    public void testQueryForSingleResultWithEmptyResult() throws Exception {
        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Nullable<String> result = executor.queryForSingleResult(String.class, "SELECT name FROM test_table WHERE id = ?", 999);

        assertFalse(result.isPresent());
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

    // Test entity classes
    public static class TestEntity {
        private int id;
        private String name;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
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
}
