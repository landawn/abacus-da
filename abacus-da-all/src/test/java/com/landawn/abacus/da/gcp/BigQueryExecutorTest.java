package com.landawn.abacus.da.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
    public void testqueryForSingleValueWithTargetClass() throws Exception {
        Field field = Field.of("name", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(field);

        FieldValueList row = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "TestName")), fields);

        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.getValues()).thenReturn(Arrays.asList(row));
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Nullable<String> result = executor.queryForSingleValue(TestEntity.class, String.class, "name", Filters.eq("id", 1));

        assertTrue(result.isPresent());
        assertEquals("TestName", result.get());
    }

    @Test
    public void testqueryForSingleValueWithQuery() throws Exception {
        Field field = Field.of("count", StandardSQLTypeName.INT64);
        FieldList fields = FieldList.of(field);

        FieldValueList row = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "5")), fields);

        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.getValues()).thenReturn(Arrays.asList(row));
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Nullable<Integer> result = executor.queryForSingleValue(Integer.class, "SELECT COUNT(*) FROM test_table", new Object[0]);

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
        List<TestEntity> list = executor.list(TestEntity.class, selectProps, Filters.eq("id", 1));

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
    public void testqueryForSingleValueWithEmptyResult() throws Exception {
        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Nullable<String> result = executor.queryForSingleValue(String.class, "SELECT name FROM test_table WHERE id = ?", 999);

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
    public void testExtractDataWithNullSchemaAndMapClassNonZeroRows() {
        when(mockTableResult.getTotalRows()).thenReturn(3L);
        when(mockTableResult.getSchema()).thenReturn(null);

        Dataset dataset = BigQueryExecutor.extractData(mockTableResult, Map.class);

        assertNotNull(dataset);
        assertEquals(0, dataset.size());
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
    public void testQueryForSingleValueWithPositiveTotalRowsButEmptyValues() throws Exception {
        // DML-result shape: getTotalRows() returns the affected-row count (> 0) while
        // getValues() is empty. queryForSingleValue used to dispatch off totalRows and then
        // call iterator().next() on an empty iterable, throwing NoSuchElementException.
        when(mockTableResult.getTotalRows()).thenReturn(5L);
        when(mockTableResult.getValues()).thenReturn(java.util.Collections.<FieldValueList> emptyList());
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Nullable<String> result = executor.queryForSingleValue(String.class, "UPDATE test_table SET name = ? WHERE id < ?", "x", 10);

        assertFalse(result.isPresent());
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

    // ---------- Additional coverage: constructor validation ----------

    @Test
    public void testConstructor_NullBigQuery() {
        assertThrows(IllegalArgumentException.class, () -> new BigQueryExecutor(null));
    }

    @Test
    public void testConstructor_NullNamingPolicy() {
        assertThrows(IllegalArgumentException.class, () -> new BigQueryExecutor(mockBigQuery, null));
    }

    @Test
    public void testConstructor_NullBigQueryWithNamingPolicy() {
        assertThrows(IllegalArgumentException.class, () -> new BigQueryExecutor(null, NamingPolicy.SNAKE_CASE));
    }

    // ---------- Insert / update / delete with non-default naming policies ----------
    // Drives the SCREAMING_SNAKE_CASE and CAMEL_CASE branches of prepareInsert(Class,Map),
    // prepareUpdate(Class,Map,Condition) and prepareDelete(Class,Condition).

    @Test
    public void testInsertWithMap_ScreamingSnakeCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        Map<String, Object> props = new HashMap<>();
        props.put("id", 1);
        props.put("name", "X");

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        TableResult result = exec.insert(TestEntity.class, props);
        assertSame(mockTableResult, result);
    }

    @Test
    public void testInsertWithMap_CamelCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.CAMEL_CASE);
        Map<String, Object> props = new HashMap<>();
        props.put("id", 1);
        props.put("name", "X");

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        assertSame(mockTableResult, exec.insert(TestEntity.class, props));
    }

    @Test
    public void testInsertWithMap_UnsupportedNamingPolicy() {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.NO_CHANGE);
        Map<String, Object> props = new HashMap<>();
        props.put("id", 1);

        assertThrows(IllegalStateException.class, () -> exec.insert(TestEntity.class, props));
    }

    @Test
    public void testUpdateEntity_ScreamingSnakeCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        TestEntity entity = new TestEntity();
        entity.setId(1);
        entity.setName("X");

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);
        assertSame(mockTableResult, exec.update(entity));
    }

    @Test
    public void testUpdateEntity_CamelCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.CAMEL_CASE);
        TestEntity entity = new TestEntity();
        entity.setId(1);
        entity.setName("X");

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);
        assertSame(mockTableResult, exec.update(entity));
    }

    @Test
    public void testUpdateWithMap_ScreamingSnakeCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        Map<String, Object> props = new HashMap<>();
        props.put("name", "Y");

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);
        assertSame(mockTableResult, exec.update(TestEntity.class, props, Filters.eq("id", 1)));
    }

    @Test
    public void testUpdateWithMap_CamelCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.CAMEL_CASE);
        Map<String, Object> props = new HashMap<>();
        props.put("name", "Y");

        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);
        assertSame(mockTableResult, exec.update(TestEntity.class, props, Filters.eq("id", 1)));
    }

    @Test
    public void testUpdateWithMap_UnsupportedNamingPolicy() {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.NO_CHANGE);
        Map<String, Object> props = new HashMap<>();
        props.put("name", "Y");
        assertThrows(IllegalStateException.class, () -> exec.update(TestEntity.class, props, Filters.eq("id", 1)));
    }

    @Test
    public void testDelete_ScreamingSnakeCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);
        assertSame(mockTableResult, exec.delete(TestEntity.class, Filters.eq("id", 1)));
    }

    @Test
    public void testDelete_CamelCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.CAMEL_CASE);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);
        assertSame(mockTableResult, exec.delete(TestEntity.class, Filters.eq("id", 1)));
    }

    @Test
    public void testDelete_UnsupportedNamingPolicy() {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.NO_CHANGE);
        assertThrows(IllegalStateException.class, () -> exec.delete(TestEntity.class, Filters.eq("id", 1)));
    }

    // ---------- prepareQuery (covers PAC / PLC branches via query()) ----------

    @Test
    public void testQuery_ScreamingSnakeCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        Field f = Field.of("id", StandardSQLTypeName.INT64);
        Schema schema = Schema.of(FieldList.of(f));

        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<FieldValueList>());
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Dataset ds = exec.query(TestEntity.class, Filters.eq("id", 1));
        assertNotNull(ds);
    }

    @Test
    public void testQuery_CamelCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.CAMEL_CASE);
        Field f = Field.of("id", StandardSQLTypeName.INT64);
        Schema schema = Schema.of(FieldList.of(f));

        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<FieldValueList>());
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Dataset ds = exec.query(TestEntity.class, Filters.eq("id", 1));
        assertNotNull(ds);
    }

    @Test
    public void testQuery_WithSelectProps_ScreamingSnakeCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        Field f = Field.of("name", StandardSQLTypeName.STRING);
        Schema schema = Schema.of(FieldList.of(f));

        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<FieldValueList>());
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Dataset ds = exec.query(TestEntity.class, Arrays.asList("name"), null);
        assertNotNull(ds);
    }

    @Test
    public void testQuery_WithSelectProps_CamelCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.CAMEL_CASE);
        Field f = Field.of("name", StandardSQLTypeName.STRING);
        Schema schema = Schema.of(FieldList.of(f));

        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<FieldValueList>());
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Dataset ds = exec.query(TestEntity.class, Arrays.asList("name"), null);
        assertNotNull(ds);
    }

    @Test
    public void testQuery_NullCondition() throws Exception {
        // whereClause == null exercises the "no where clause" path in prepareQuery.
        Field f = Field.of("id", StandardSQLTypeName.INT64);
        Schema schema = Schema.of(FieldList.of(f));

        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<FieldValueList>());
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Dataset ds = executor.query(TestEntity.class, (Condition) null);
        assertNotNull(ds);
    }

    @Test
    public void testQuery_UnsupportedNamingPolicy() {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.NO_CHANGE);
        assertThrows(IllegalStateException.class, () -> exec.query(TestEntity.class, Filters.eq("id", 1)));
    }

    // ---------- stream(Class, QueryJobConfiguration) / stream(QueryJobConfiguration) error paths ----------

    @Test
    public void testStreamWithQueryJobConfiguration_InterruptedException() throws Exception {
        QueryJobConfiguration cfg = QueryJobConfiguration.newBuilder("SELECT 1").build();
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenThrow(new InterruptedException("interrupted"));

        // Clear interrupt flag first (the executor sets it on InterruptedException).
        Thread.interrupted();
        assertThrows(RuntimeException.class, () -> executor.stream(cfg));
        // The catch handler re-interrupts the thread; clear it so we don't leak state to later tests.
        Thread.interrupted();
    }

    @Test
    public void testStreamWithClassAndQueryJobConfiguration_InterruptedException() throws Exception {
        QueryJobConfiguration cfg = QueryJobConfiguration.newBuilder("SELECT 1").build();
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenThrow(new InterruptedException("interrupted"));

        Thread.interrupted();
        assertThrows(RuntimeException.class, () -> executor.stream(TestEntity.class, cfg));
        Thread.interrupted();
    }

    @Test
    public void testExecute_InterruptedException() throws Exception {
        // execute(String, Object...) must convert InterruptedException to RuntimeException.
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenThrow(new InterruptedException("interrupted"));

        Thread.interrupted();
        assertThrows(RuntimeException.class, () -> executor.execute("SELECT 1"));
        Thread.interrupted();
    }

    // ---------- idsToCondition / entityToCondition edge cases ----------

    @Test
    public void testDelete_WithIds_MismatchCount() {
        // Single-key entity should reject more than one ID.
        assertThrows(IllegalArgumentException.class, () -> executor.delete(TestEntity.class, 1, 2));
    }

    @Test
    public void testDelete_WithIds_EmptyIds() {
        assertThrows(IllegalArgumentException.class, () -> executor.delete(TestEntity.class, new Object[0]));
    }

    @Test
    public void testDelete_WithIds_NullIdValue() {
        // idsToCondition's @NotEmpty check rejects a null-only varargs array.
        assertThrows(IllegalArgumentException.class, () -> executor.delete(TestEntity.class, (Object[]) null));
    }

    @Test
    public void testDelete_EntityNoKeyValue() {
        // Entity with the @Id-equivalent key but a default (zero) primitive int is still considered present;
        // however an entity whose only key is null/empty string should fail entityToCondition.
        TestEntityWithStringKey entity = new TestEntityWithStringKey();
        entity.setId(""); // empty string is treated as missing
        assertThrows(IllegalArgumentException.class, () -> executor.delete(entity));
    }

    @Test
    public void testDelete_EntityNullKeyValue() {
        TestEntityWithStringKey entity = new TestEntityWithStringKey();
        entity.setId(null);
        assertThrows(IllegalArgumentException.class, () -> executor.delete(entity));
    }

    // ---------- buildQueryParameterValue: additional type coverage ----------

    @Test
    public void testBuildQueryParameterValueWithSqlTypes() {
        // Drives the java.sql.Date / Time / Timestamp branches and com.google.cloud.Timestamp.
        Object[] params = new Object[] { new java.sql.Date(0L), new java.sql.Time(0L), new java.sql.Timestamp(0L),
                com.google.cloud.Timestamp.ofTimeSecondsAndNanos(0L, 0) };

        List<QueryParameterValue> values = BigQueryExecutor.buildQueryParameterValue(params);
        assertEquals(4, values.size());
    }

    @Test
    public void testBuildQueryParameterValueWithGoogleDate() {
        Object[] params = new Object[] { com.google.cloud.Date.fromYearMonthDay(2024, 1, 1) };
        List<QueryParameterValue> values = BigQueryExecutor.buildQueryParameterValue(params);
        assertEquals(1, values.size());
    }

    @Test
    public void testBuildQueryParameterValue_NullParametersArray() {
        // Passing a null array is treated as no parameters.
        List<QueryParameterValue> values = BigQueryExecutor.buildQueryParameterValue((Object[]) null);
        assertNotNull(values);
        assertEquals(0, values.size());
    }

    // ---------- toMap / toEntity using supplier IntFunctions ----------

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

    // ---------- toList for non-bean basic types (single-column) ----------

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

    // ---------- queryForSingleValue overloads ----------

    @Test
    public void testQueryForSingleValue_WithCondition_NullValue() throws Exception {
        // Row exists but the column value is null: result is present-but-null, not empty.
        Field f = Field.of("name", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(f);

        FieldValueList row = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, (String) null)), fields);

        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.getValues()).thenReturn(Arrays.asList(row));
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Nullable<String> result = executor.queryForSingleValue(TestEntity.class, String.class, "name", Filters.eq("id", 1));
        assertTrue(result.isPresent());
        assertNull(result.orElse(null));
    }

    // ---------- exists with whereClause null ----------

    @Test
    public void testExists_NullWhereClause_NoRows() throws Exception {
        // Null whereClause passes through prepareQuery without a WHERE.
        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        assertFalse(executor.exists(TestEntity.class, (Condition) null));
    }

    @Test
    public void testExists_NullWhereClause_HasRows() throws Exception {
        when(mockTableResult.getTotalRows()).thenReturn(3L);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        assertTrue(executor.exists(TestEntity.class, (Condition) null));
    }

    // ---------- getSchema / NullPointer scenarios for extractData ----------

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

    // ---------- queryForSingleValue via TestEntityWithStringKey condition path ----------

    @Test
    public void testQueryForSingleValue_ByQuery_NoRows() throws Exception {
        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Nullable<Long> result = executor.queryForSingleValue(Long.class, "SELECT COUNT(*) FROM t WHERE id = ?", 0);
        assertFalse(result.isPresent());
    }

    @Test
    public void testListWithClassAndCondition_NoMatching() throws Exception {
        Field f = Field.of("id", StandardSQLTypeName.INT64);
        FieldList fields = FieldList.of(f);
        Schema schema = Schema.of(fields);

        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<FieldValueList>());
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        List<TestEntity> list = executor.list(TestEntity.class, Filters.eq("id", -1));
        assertNotNull(list);
        assertEquals(0, list.size());
    }

    // ---------- Stream classes / pipeline through real query() ----------

    @Test
    public void testStreamWithClassAndCondition_ReturnsRows() throws Exception {
        Field f1 = Field.of("id", StandardSQLTypeName.INT64);
        Field f2 = Field.of("name", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(f1, f2);

        List<FieldValueList> rows = new ArrayList<>();
        rows.add(FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "10"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Eve")),
                fields));

        when(mockTableResult.iterateAll()).thenReturn(rows);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Stream<TestEntity> stream = executor.stream(TestEntity.class, Filters.eq("id", 10));
        List<TestEntity> collected = stream.toList();
        assertEquals(1, collected.size());
        assertEquals(10, collected.get(0).getId());
        assertEquals("Eve", collected.get(0).getName());
    }

    // ---------- bigQuery() accessor returns the same instance ----------

    @Test
    public void testBigQuery_ReturnsSameInstance() {
        BigQuery another = mock(BigQuery.class);
        BigQueryExecutor exec = new BigQueryExecutor(another);
        assertSame(another, exec.bigQuery());
    }

    // ---------- Coverage gap fillers: readRow / extractData / stream / execute ----------

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

    // toList with null schema returns empty list
    @Test
    public void testToList_NullSchema() throws Exception {
        when(mockTableResult.getTotalRows()).thenReturn(5L);
        when(mockTableResult.getSchema()).thenReturn(null);

        List<TestEntity> result = BigQueryExecutor.toList(mockTableResult, TestEntity.class);
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    // extractData with null schema returns empty Dataset
    @Test
    public void testExtractData_NullSchemaReturnsEmpty() {
        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockTableResult.getSchema()).thenReturn(null);

        Dataset ds = BigQueryExecutor.extractData(mockTableResult, TestEntity.class);
        assertNotNull(ds);
        assertEquals(0, ds.size());
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

    // entityToCondition: null entity throws (NPE because delete dereferences before reaching entityToCondition)
    @Test
    public void testEntityToCondition_NullEntityThrows() {
        // delete(null).getClass() triggers NPE before entityToCondition is invoked.
        assertThrows(NullPointerException.class, () -> executor.delete((Object) null));
    }

    // entityToCondition: empty key throws
    @Test
    public void testEntityToCondition_EmptyKeyThrows() {
        TestEntityWithStringKey entity = new TestEntityWithStringKey();
        entity.setId(""); // empty string -> no value
        assertThrows(IllegalArgumentException.class, () -> executor.delete(entity));
    }

    // idsToCondition: empty ids throws
    @Test
    public void testIdsToCondition_EmptyIdsThrows() {
        assertThrows(IllegalArgumentException.class, () -> executor.exists(TestEntity.class, new Object[0]));
    }

    // idsToCondition: more ids than keys throws
    @Test
    public void testIdsToCondition_TooManyIdsThrows() {
        assertThrows(IllegalArgumentException.class, () -> executor.exists(TestEntity.class, 1, 2, 3));
    }

    // stream(Class, QueryJobConfiguration) returns rows
    @Test
    public void testStream_WithClassAndQueryConfig() throws Exception {
        Field f1 = Field.of("id", StandardSQLTypeName.INT64);
        Field f2 = Field.of("name", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(f1, f2);

        List<FieldValueList> rows = new ArrayList<>();
        rows.add(FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "9"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "John")),
                fields));

        when(mockTableResult.iterateAll()).thenReturn(rows);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        QueryJobConfiguration config = QueryJobConfiguration.newBuilder("SELECT id, name FROM t").build();
        Stream<TestEntity> stream = executor.stream(TestEntity.class, config);
        List<TestEntity> collected = stream.toList();
        assertEquals(1, collected.size());
        assertEquals(9, collected.get(0).getId());
    }

    // stream(QueryJobConfiguration) returns raw FieldValueList
    @Test
    public void testStream_RawFieldValueList() throws Exception {
        Field f1 = Field.of("id", StandardSQLTypeName.INT64);
        FieldList fields = FieldList.of(f1);

        List<FieldValueList> rows = new ArrayList<>();
        rows.add(FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "11")), fields));

        when(mockTableResult.iterateAll()).thenReturn(rows);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        QueryJobConfiguration config = QueryJobConfiguration.newBuilder("SELECT id FROM t").build();
        Stream<FieldValueList> stream = executor.stream(config);
        List<FieldValueList> collected = stream.toList();
        assertEquals(1, collected.size());
    }

    // execute(String, Object...) with positional parameters
    @Test
    public void testExecute_WithParameters() throws Exception {
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        TableResult result = executor.execute("SELECT * FROM t WHERE id = ?", 42);
        assertNotNull(result);
        verify(mockBigQuery).query(any(QueryJobConfiguration.class));
    }

    // ===== readRow branches via stream(Class, QueryJobConfiguration) — hit ObjectArray, Collection, Map, single-value, bean =====
    private List<FieldValueList> oneTwoColumnRow(Object v1, Object v2) {
        Field f1 = Field.of("id", StandardSQLTypeName.INT64);
        Field f2 = Field.of("name", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(f1, f2);
        FieldValueList row = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, String.valueOf(v1)),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, String.valueOf(v2))), fields);
        return Arrays.asList(row);
    }

    @Test
    public void testStream_AsObjectArrayClass() throws Exception {
        when(mockTableResult.iterateAll()).thenReturn(oneTwoColumnRow(1, "Alice"));
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        QueryJobConfiguration cfg = QueryJobConfiguration.newBuilder("SELECT id, name FROM t").build();
        Stream<Object[]> stream = executor.stream(Object[].class, cfg);
        List<Object[]> result = stream.toList();
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).length);
    }

    @Test
    public void testStream_AsCollectionClass() throws Exception {
        when(mockTableResult.iterateAll()).thenReturn(oneTwoColumnRow(1, "Bob"));
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        QueryJobConfiguration cfg = QueryJobConfiguration.newBuilder("SELECT id, name FROM t").build();
        @SuppressWarnings({ "unchecked", "rawtypes" })
        Stream<List> stream = executor.stream((Class) List.class, cfg);
        List<List> result = stream.toList();
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).size());
    }

    @Test
    public void testStream_AsMapClass() throws Exception {
        when(mockTableResult.iterateAll()).thenReturn(oneTwoColumnRow(1, "Carol"));
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        QueryJobConfiguration cfg = QueryJobConfiguration.newBuilder("SELECT id, name FROM t").build();
        @SuppressWarnings({ "unchecked", "rawtypes" })
        Stream<Map> stream = executor.stream((Class) Map.class, cfg);
        List<Map> result = stream.toList();
        assertEquals(1, result.size());
        assertNotNull(result.get(0).get("id"));
    }

    @Test
    public void testStream_AsSingleValueClass() throws Exception {
        Field f1 = Field.of("v", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(f1);
        List<FieldValueList> rows = Arrays.asList(FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "hello")), fields));
        when(mockTableResult.iterateAll()).thenReturn(rows);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        QueryJobConfiguration cfg = QueryJobConfiguration.newBuilder("SELECT v FROM t").build();
        Stream<String> stream = executor.stream(String.class, cfg);
        List<String> result = stream.toList();
        assertEquals(1, result.size());
        assertEquals("hello", result.get(0));
    }

    @Test
    public void testStream_AsSingleValueClass_MultiColumnThrows() throws Exception {
        when(mockTableResult.iterateAll()).thenReturn(oneTwoColumnRow(1, "X"));
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        QueryJobConfiguration cfg = QueryJobConfiguration.newBuilder("SELECT id, name FROM t").build();
        Stream<String> stream = executor.stream(String.class, cfg);
        assertThrows(IllegalArgumentException.class, () -> stream.toList());
    }

    // ===== readRow with FieldValueList of FieldValueList (nested) via stream Object[] =====
    @Test
    public void testStream_AsObjectArray_NestedFieldValueList() throws Exception {
        Field inner1 = Field.of("a", StandardSQLTypeName.STRING);
        FieldList innerFields = FieldList.of(inner1);
        FieldValueList innerList = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "x")), innerFields);

        Field outer = Field.newBuilder("s", StandardSQLTypeName.STRUCT, innerFields).build();
        FieldList outerFields = FieldList.of(outer);
        FieldValueList row = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.RECORD, innerList)), outerFields);

        when(mockTableResult.iterateAll()).thenReturn(Arrays.asList(row));
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        QueryJobConfiguration cfg = QueryJobConfiguration.newBuilder("SELECT s FROM t").build();
        Stream<Object[]> stream = executor.stream(Object[].class, cfg);
        List<Object[]> result = stream.toList();
        assertEquals(1, result.size());
        // first column is a nested FieldValueList -> readRow recursively returns Object[]
        assertTrue(result.get(0)[0] instanceof Object[]);
    }

    // ===== entityToCondition composite-key (multiple key fields) =====
    @Test
    public void testEntityToCondition_CompositeKey() throws Exception {
        CompositeKeyEntity e = new CompositeKeyEntity();
        e.setIdA("1");
        e.setIdB("2");
        e.setValue("v");
        // delete(entity) invokes entityToCondition internally
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);
        assertDoesNotThrowExt(() -> executor.delete(e));
    }

    @Test
    public void testEntityToCondition_CompositeKey_NullValueThrows() {
        CompositeKeyEntity e = new CompositeKeyEntity();
        e.setIdA("1");
        // idB is null
        e.setValue("v");
        assertThrows(IllegalArgumentException.class, () -> executor.delete(e));
    }

    // ===== idsToCondition composite-key =====
    @Test
    public void testIdsToCondition_CompositeKey() throws Exception {
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);
        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<>());
        // exists(class, id1, id2) drives idsToCondition; non-matching id count throws else proceeds
        assertDoesNotThrowExt(() -> executor.exists(CompositeKeyEntity.class, "1", "2"));
    }

    @Test
    public void testIdsToCondition_CompositeKeyMismatchThrows() {
        assertThrows(IllegalArgumentException.class, () -> executor.exists(CompositeKeyEntity.class, "1"));
    }

    // ===== getSchema(FieldValueList) - exercised via toMap(FieldValueList) =====
    @Test
    public void testGetSchema_FieldValueListWithSchema() {
        Field f1 = Field.of("a", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(f1);
        FieldValueList row = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "x")), fields);

        Map<String, Object> result = BigQueryExecutor.toMap(row);
        assertNotNull(result);
        assertEquals("x", result.get("a"));
    }

    // ===== Constructor validation =====
    @Test
    public void testConstructor_NullBigQueryThrows() {
        assertThrows(IllegalArgumentException.class, () -> new BigQueryExecutor(null));
    }

    @Test
    public void testConstructor_NullNamingPolicyThrows() {
        assertThrows(IllegalArgumentException.class, () -> new BigQueryExecutor(mockBigQuery, null));
    }

    // ===== extractData branches: value of type FieldValueList in column =====
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

    // ===== prepareUpdate via update(entity) — exercises composite-key path =====
    @Test
    public void testUpdate_EntityWithCompositeKey() throws Exception {
        CompositeKeyEntity e = new CompositeKeyEntity();
        e.setIdA("1");
        e.setIdB("2");
        e.setValue("v");
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);
        // Should not throw - composite key has both values set
        assertDoesNotThrowExt(() -> executor.update(e));
    }

    // Helper: assertDoesNotThrow returns Object; here we wrap to handle checked exceptions cleanly
    private static void assertDoesNotThrowExt(Runnable r) {
        try {
            r.run();
        } catch (Throwable t) {
            throw new AssertionError("Expected no exception, got: " + t, t);
        }
    }

    // Composite-key entity (2 @Id fields) used to exercise composite-key branches
    public static class CompositeKeyEntity {
        @com.landawn.abacus.annotation.Id
        private String idA;
        @com.landawn.abacus.annotation.Id
        private String idB;
        private String value;

        public String getIdA() {
            return idA;
        }

        public void setIdA(String idA) {
            this.idA = idA;
        }

        public String getIdB() {
            return idB;
        }

        public void setIdB(String idB) {
            this.idB = idB;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    // Entity with a String id used to exercise entityToCondition's empty/null-key error paths.
    public static class TestEntityWithStringKey {
        private String id;
        private String value;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
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
