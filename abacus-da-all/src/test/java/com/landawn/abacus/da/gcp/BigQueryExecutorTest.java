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
import org.mockito.ArgumentCaptor;
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

    // ---------------------------------------------------------------------------------------------
    // Regression: DML must generate POSITIONAL ('?') SQL, not named (':name') SQL. execute(...) binds
    // parameters positionally via setPositionalParameters, so a ':name' placeholder cannot be bound and
    // BigQuery rejects the statement. (Bug: insert/update/delete used the named-SQL builders NSC/NAC/NLC
    // instead of the positional PSC/PAC/PLC used by the SELECT path.) The existing DML tests above only
    // verify delegation, which is why this stayed latent.
    // ---------------------------------------------------------------------------------------------

    private String captureGeneratedSql() throws Exception {
        final ArgumentCaptor<QueryJobConfiguration> captor = ArgumentCaptor.forClass(QueryJobConfiguration.class);
        verify(mockBigQuery).query(captor.capture());
        return captor.getValue().getQuery();
    }

    @Test
    public void testInsertEntityGeneratesPositionalSql() throws Exception {
        TestEntity entity = new TestEntity();
        entity.setId(1);
        entity.setName("Test");
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        executor.insert(entity);

        final String sql = captureGeneratedSql();
        assertTrue(sql.contains("?"), sql);
        assertFalse(sql.contains(":"), sql);
    }

    @Test
    public void testInsertClassPropsGeneratesPositionalSql() throws Exception {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("id", 1);
        props.put("name", "Test");
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        executor.insert(TestEntity.class, props);

        final String sql = captureGeneratedSql();
        assertTrue(sql.contains("?"), sql);
        assertFalse(sql.contains(":"), sql);
    }

    @Test
    public void testUpdateEntityWithKeysGeneratesPositionalSql() throws Exception {
        TestEntity entity = new TestEntity();
        entity.setId(1);
        entity.setName("UpdatedTest");
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        executor.update(entity, N.asSet("id"));

        final String sql = captureGeneratedSql();
        assertTrue(sql.contains("?"), sql);
        assertFalse(sql.contains(":"), sql);
    }

    @Test
    public void testUpdateEntity_NullPropertiesExcludedFromSetClause() throws Exception {
        // Regression: a null non-key property used to flow into the parameter list as a raw null,
        // which buildQueryParameterValue rejects with IAE — entity update was unusable for entities
        // with null fields. Null props are now excluded from SET (mirroring insert semantics).
        TestEntityWithEmail entity = new TestEntityWithEmail();
        entity.setId(1);
        entity.setName("OnlyName");
        // email left null
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        executor.update(entity, N.asSet("id"));

        final String sql = captureGeneratedSql();
        assertTrue(sql.contains("SET name = ? WHERE"), sql);
        assertFalse(sql.contains("email = ?"), sql);
    }

    @Test
    public void testUpdateEntity_AllNonKeyPropsNullThrows() {
        TestEntityWithEmail entity = new TestEntityWithEmail();
        entity.setId(1);

        assertThrows(IllegalArgumentException.class, () -> executor.update(entity, N.asSet("id")));
    }

    @Test
    public void testUpdateClassPropsConditionGeneratesPositionalSql() throws Exception {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("name", "UpdatedTest");
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        executor.update(TestEntity.class, props, Filters.eq("id", 1));

        final String sql = captureGeneratedSql();
        assertTrue(sql.contains("?"), sql);
        assertFalse(sql.contains(":"), sql);
    }

    @Test
    public void testDeleteClassConditionGeneratesPositionalSql() throws Exception {
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        executor.delete(TestEntity.class, Filters.eq("id", 1));

        final String sql = captureGeneratedSql();
        assertTrue(sql.contains("?"), sql);
        assertFalse(sql.contains(":"), sql);
    }

    // ---------------------------------------------------------------------------------------------
    // Regression: SELECT must alias columns with BigQuery backticks (`id`), not ANSI double quotes
    // ("id"). BigQuery/GoogleSQL parses a double-quoted token as a STRING LITERAL. The BigQuery
    // SqlBuilder variants are configured with IdentifierQuote.BACKTICK so no fragile SQL text
    // post-processing is needed.
    // ---------------------------------------------------------------------------------------------
    @Test
    public void testQueryGeneratesBacktickQuotedAliasesNotDoubleQuotes() throws Exception {
        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockTableResult.getSchema()).thenReturn(Schema.of(FieldList.of(Field.of("id", StandardSQLTypeName.INT64))));
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<FieldValueList>());
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        executor.query(TestEntity.class, Filters.eq("id", 1));

        final String sql = captureGeneratedSql();
        assertFalse(sql.contains("\""), "alias must not be ANSI double-quoted (BigQuery reads it as a string literal): " + sql);
        assertTrue(sql.contains("`"), "alias must be backtick-quoted for BigQuery: " + sql);
    }

    /**
     * A raw GoogleSQL expression may legitimately contain a double-quoted string literal. Configuring
     * identifier quoting at builder creation must leave that caller-supplied literal untouched.
     */
    @Test
    public void testQueryPreservesDoubleQuotedLiteralInRawCondition() throws Exception {
        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockTableResult.getSchema()).thenReturn(Schema.of(FieldList.of(Field.of("id", StandardSQLTypeName.INT64))));
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<FieldValueList>());
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        executor.query(TestEntity.class, Filters.expr("name = \"Alice\""));

        final String sql = captureGeneratedSql();
        assertTrue(sql.contains("\"Alice\""), "double-quoted string literal must be preserved: " + sql);
        assertTrue(sql.contains("`"), "generated aliases must still use BigQuery backticks: " + sql);
    }

    @Test
    public void testQueryDoesNotRewriteAliasLikeTextInsideLiteral() throws Exception {
        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockTableResult.getSchema()).thenReturn(Schema.of(FieldList.of(Field.of("id", StandardSQLTypeName.INT64))));
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<FieldValueList>());
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        executor.query(TestEntity.class, Filters.expr("note = 'AS \"not_an_alias\"'"));

        final String sql = captureGeneratedSql();
        assertTrue(sql.contains("'AS \"not_an_alias\"'"), "quoted raw-expression text must be preserved: " + sql);
        assertTrue(sql.contains("`"), "generated aliases must still use backticks: " + sql);
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

        final ArgumentCaptor<QueryJobConfiguration> captor = ArgumentCaptor.forClass(QueryJobConfiguration.class);
        verify(mockBigQuery).query(captor.capture());
        assertTrue(captor.getValue().getQuery().contains("LIMIT 1"), captor.getValue().getQuery());
    }

    @Test
    public void testQueryForSingleNonNullWithTargetClassUsesLimitOne() throws Exception {
        final FieldList fields = FieldList.of(Field.of("name", StandardSQLTypeName.STRING));
        final FieldValueList row = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "TestName")), fields);

        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.getValues()).thenReturn(Arrays.asList(row));
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        assertEquals("TestName", executor.queryForSingleNonNull(TestEntity.class, String.class, "name", Filters.eq("id", 1)).get());

        final ArgumentCaptor<QueryJobConfiguration> captor = ArgumentCaptor.forClass(QueryJobConfiguration.class);
        verify(mockBigQuery).query(captor.capture());
        assertTrue(captor.getValue().getQuery().contains("LIMIT 1"), captor.getValue().getQuery());
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
        // The constructor now fails fast on an unsupported policy (was a deferred ISE on first DML).
        assertThrows(IllegalArgumentException.class, () -> new BigQueryExecutor(mockBigQuery, NamingPolicy.NO_CHANGE));
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
        // The constructor now fails fast on an unsupported policy (was a deferred ISE on insert).
        assertThrows(IllegalArgumentException.class, () -> new BigQueryExecutor(mockBigQuery, NamingPolicy.NO_CHANGE));
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
        // The constructor now fails fast on an unsupported policy (was a deferred ISE on update).
        assertThrows(IllegalArgumentException.class, () -> new BigQueryExecutor(mockBigQuery, NamingPolicy.NO_CHANGE));
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
        // The constructor now fails fast on an unsupported policy (was a deferred ISE on delete).
        assertThrows(IllegalArgumentException.class, () -> new BigQueryExecutor(mockBigQuery, NamingPolicy.NO_CHANGE));
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
        // The constructor now fails fast on an unsupported policy (was a deferred ISE on query).
        assertThrows(IllegalArgumentException.class, () -> new BigQueryExecutor(mockBigQuery, NamingPolicy.NO_CHANGE));
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
        Object[] params = new Object[] { java.sql.Date.valueOf("2024-01-01"), java.sql.Time.valueOf("12:34:56"), new java.sql.Timestamp(1000L),
                com.google.cloud.Timestamp.ofTimeSecondsAndNanos(1L, 0) };

        List<QueryParameterValue> values = BigQueryExecutor.buildQueryParameterValue(params);
        assertEquals(4, values.size());
        assertEquals(StandardSQLTypeName.DATE, values.get(0).getType());
        assertEquals("2024-01-01", values.get(0).getValue());
        assertEquals(StandardSQLTypeName.TIME, values.get(1).getType());
        assertEquals("12:34:56.000000", values.get(1).getValue());
        assertEquals(StandardSQLTypeName.TIMESTAMP, values.get(2).getType());
        assertEquals(QueryParameterValue.timestamp(1_000_000L).getValue(), values.get(2).getValue());
        assertEquals(QueryParameterValue.timestamp(1_000_000L).getValue(), values.get(3).getValue());
    }

    @Test
    public void testBuildQueryParameterValueWithUtilDateUsesTimestampMicros() {
        final List<QueryParameterValue> values = BigQueryExecutor.buildQueryParameterValue(new java.util.Date(1000L));

        assertEquals(1, values.size());
        assertEquals(StandardSQLTypeName.TIMESTAMP, values.get(0).getType());
        assertEquals(QueryParameterValue.timestamp(1_000_000L).getValue(), values.get(0).getValue());
    }

    /**
     * Regression: binding a com.google.cloud.Timestamp previously went through toDate().getTime(),
     * which truncates to millisecond precision — the microsecond component that BigQuery TIMESTAMP
     * carries was silently dropped (...123456 µs became ...123000 µs), so a WHERE equality on a value
     * read back from BigQuery could fail to match. The binding now computes
     * seconds * 1_000_000 + nanos / 1_000.
     */
    @Test
    public void testBuildQueryParameterValueWithGoogleTimestampPreservesMicros() {
        final com.google.cloud.Timestamp ts = com.google.cloud.Timestamp.ofTimeSecondsAndNanos(1_700_000_000L, 123_456_000);

        final List<QueryParameterValue> values = BigQueryExecutor.buildQueryParameterValue(ts);

        assertEquals(1, values.size());
        assertEquals(StandardSQLTypeName.TIMESTAMP, values.get(0).getType());
        // 1_700_000_000 s + 123_456_000 ns == 1_700_000_000_123_456 µs (not ...123_000 µs).
        assertEquals(QueryParameterValue.timestamp(1_700_000_000_123_456L).getValue(), values.get(0).getValue());
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

    @Test
    public void testToMap_UnwrapsRepeatedFieldValues() {
        final Field repeated = Field.newBuilder("tags", StandardSQLTypeName.STRING).setMode(Field.Mode.REPEATED).build();
        final FieldList fields = FieldList.of(repeated);
        final FieldValueList row = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.REPEATED,
                Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "a"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "b")))), fields);

        final Map<String, Object> map = BigQueryExecutor.toMap(fields, row, com.landawn.abacus.util.IntFunctions.ofLinkedHashMap());

        assertEquals(Arrays.asList("a", "b"), map.get("tags"));
    }

    // ---------------------------------------------------------------------------------------------
    // Regression: toEntity must unwrap REPEATED columns. FieldValue.getValue() of a REPEATED column
    // returns a List<FieldValue>; the old code stored that list as-is into a List-typed property, so
    // the raw FieldValue wrappers leaked into e.g. a List<String> property (heap pollution surfacing
    // as ClassCastException at the call site). The fixed path mirrors the toMap unwrapping.
    // ---------------------------------------------------------------------------------------------
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

    // delete(Object)/update(Object): a null entity is rejected eagerly with IAE (matches insert(Object)).
    @Test
    public void testDeleteObject_NullEntityThrows() {
        assertThrows(IllegalArgumentException.class, () -> executor.delete((Object) null));
    }

    @Test
    public void testUpdateObject_NullEntityThrows() {
        assertThrows(IllegalArgumentException.class, () -> executor.update((Object) null));
    }

    @Test
    public void testUpdateObjectWithKeys_NullEntityThrows() {
        // The 2-arg update(Object, Set) overload must reject a null entity the same way (IAE), not NPE.
        assertThrows(IllegalArgumentException.class, () -> executor.update((Object) null, N.asSet("id")));
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

    // idsToCondition: a class with no @Id/id-named property gets a descriptive message
    // (was a self-contradictory "provided 1 IDs but expected 1 key [id]")
    @Test
    public void testIdsToCondition_KeylessClassThrowsDescriptiveIAE() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.idsToCondition(KeylessEntity.class, "x"));
        assertTrue(ex.getMessage().contains("No @Id-annotated or id-named property"));
    }

    // idsToCondition: a null targetClass is rejected with IAE (was a bare NPE from the key-name cache)
    @Test
    public void testIdsToCondition_NullTargetClassThrowsIAE() {
        assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.idsToCondition(null, "x"));
        assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.idsToCondition(null, new Object[0]));
    }

    // prepareUpdate: a null key value fails fast with a clear message
    // (was a confusing generic error from buildQueryParameterValue)
    @Test
    public void testUpdateEntityWithNullKeyValueThrowsDescriptiveIAE() {
        TestEntityWithStringKey entity = new TestEntityWithStringKey();
        entity.setValue("v"); // id left null

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> executor.update(entity, N.asSet("id")));
        assertTrue(ex.getMessage().contains("No property value specified in entity for key names"));
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

    @Test
    public void testGetSchema_NullRowThrowsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.getSchema(null));
    }

    @Test
    public void testGetSchema_RowWithoutSchemaThrowsIllegalArgumentException() {
        final FieldValueList row = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "x")), new Field[0]);

        assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.getSchema(row));
        assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.toMap(row));
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

    // ===== Null-argument guards =====

    /**
     * {@code targetClass} feeds the key-name cache (a ConcurrentHashMap), so an unchecked null used to
     * surface as a bare NullPointerException from the map rather than as an IllegalArgumentException.
     */
    @Test
    public void testKeyBasedMethodsRejectNullTargetClass() {
        assertThrows(IllegalArgumentException.class, () -> executor.delete((Class<?>) null, "id1"));
        assertThrows(IllegalArgumentException.class, () -> executor.exists((Class<?>) null, "id1"));
        assertThrows(IllegalArgumentException.class, () -> executor.exists((Class<?>) null, Filters.eq("id", 1)));
    }

    /** A null job configuration is not checked by the BigQuery client; reject it at the call site. */
    @Test
    public void testStreamRejectsNullQueryJobConfiguration() {
        assertThrows(IllegalArgumentException.class, () -> executor.stream((QueryJobConfiguration) null));
        assertThrows(IllegalArgumentException.class, () -> executor.stream(TestEntity.class, (QueryJobConfiguration) null));
    }

    @Test
    public void testToListAndExtractDataRejectNullTableResult() {
        assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.toList(null, TestEntity.class));
        assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.extractData(null, TestEntity.class));
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

    // ===== 2026-09-22 deep review (slice S): BYTES / TIMESTAMP cell decoding + ByteBuffer binding =====
    // BigQuery returns BYTES cells as base64 text and TIMESTAMP cells as epoch seconds (or epoch micros);
    // N.convert can't decode either (base64 -> NumberFormatException, epoch seconds -> parse failure).

    private static final byte[] BINARY_PAYLOAD = { 1, 2, 3, (byte) 200 };
    // 2024-06-20T16:13:20.123456Z
    private static final long TS_MICROS = 1_718_900_000_123_456L;

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

    @Test
    public void testToEntity_DecodesBytesAndTimestampCells() {
        final FieldList fields = binaryTimestampFields();

        assertBinaryTimestampEntity(BigQueryExecutor.toEntity(fields, binaryTimestampRow(fields), BinaryTimestampEntity.class));
    }

    @Test
    public void testToList_DecodesBytesAndTimestampCells() {
        final FieldList fields = binaryTimestampFields();
        when(mockTableResult.getSchema()).thenReturn(Schema.of(fields));
        when(mockTableResult.getTotalRows()).thenReturn(2L);
        when(mockTableResult.iterateAll()).thenReturn(Arrays.asList(binaryTimestampRow(fields), binaryTimestampRow(fields)));

        final List<BinaryTimestampEntity> list = BigQueryExecutor.toList(mockTableResult, BinaryTimestampEntity.class);

        assertEquals(2, list.size());
        list.forEach(BigQueryExecutorTest::assertBinaryTimestampEntity);
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
    public void testSingleColumnRows_DecodeBytesAndTimestampCells() throws Exception {
        final FieldList bytesFields = FieldList.of(Field.of("data", StandardSQLTypeName.BYTES));
        final FieldValueList bytesRow = FieldValueList.of(
                Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, java.util.Base64.getEncoder().encodeToString(BINARY_PAYLOAD))), bytesFields);
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
    public void testBuildQueryParameterValue_ByteBufferBindsRemainingBytes() {
        final java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(new byte[] { 9, 1, 2, 3 });
        buffer.get(); // position 1: the remaining bytes are {1, 2, 3}

        final List<QueryParameterValue> values = BigQueryExecutor.buildQueryParameterValue(buffer);

        assertEquals(StandardSQLTypeName.BYTES, values.get(0).getType());
        assertEquals(QueryParameterValue.bytes(new byte[] { 1, 2, 3 }).getValue(), values.get(0).getValue());
        assertEquals(1, buffer.position()); // not consumed
    }

    @Test
    public void testRepeatedBinaryAndTimestampValuesMapToTypedBeanProperties() {
        final FieldList fields = repeatedBinaryTimestampFields();
        final FieldValueList row = repeatedBinaryTimestampRow(fields);

        assertRepeatedBinaryTimestampEntity(BigQueryExecutor.toEntity(fields, row, RepeatedBinaryTimestampEntity.class));
        // Raw mappings keep the wire values rather than applying bean-specific element conversion.
        assertEquals(List.of("AQID", "BAU="), BigQueryExecutor.toMap(fields, row).get("data"));
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
        result.forEach(BigQueryExecutorTest::assertRepeatedBinaryTimestampEntity);
    }

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
                    @Override public boolean hasNext() { return iterator.hasNext(); }
                    @Override public FieldValue next() { traversedNodes[0]++; return iterator.next(); }
                    @Override public boolean hasPrevious() { return iterator.hasPrevious(); }
                    @Override public FieldValue previous() { traversedNodes[0]++; return iterator.previous(); }
                    @Override public int nextIndex() { return iterator.nextIndex(); }
                    @Override public int previousIndex() { return iterator.previousIndex(); }
                    @Override public void remove() { iterator.remove(); }
                    @Override public void set(final FieldValue value) { iterator.set(value); }
                    @Override public void add(final FieldValue value) { iterator.add(value); }
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

    // update(entity, keys) rejects a null or empty key value (the same "null or empty" rule as entityToCondition
    // and the sibling executors); a whitespace-only key value is not blank-checked and is bound as a parameter.
    @Test
    public void testUpdateEntityWithKeys_EmptyKeyValueRejectedButWhitespaceKeyValueBound() throws Exception {
        TestEntityWithStringKey emptyKey = new TestEntityWithStringKey();
        emptyKey.setId("");
        emptyKey.setValue("v");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> executor.update(emptyKey, N.asSet("id")));
        assertTrue(ex.getMessage().contains("No property value specified in entity for key names"));
        verify(mockBigQuery, times(0)).query(any(QueryJobConfiguration.class));

        TestEntityWithStringKey whitespaceKey = new TestEntityWithStringKey();
        whitespaceKey.setId(" ");
        whitespaceKey.setValue("v");
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        executor.update(whitespaceKey, N.asSet("id"));

        final ArgumentCaptor<QueryJobConfiguration> captor = ArgumentCaptor.forClass(QueryJobConfiguration.class);
        verify(mockBigQuery).query(captor.capture());
        assertEquals("UPDATE test_entity_with_string_key SET value = ? WHERE id = ?", captor.getValue().getQuery());
        assertEquals(2, captor.getValue().getPositionalParameters().size());
        assertEquals(" ", captor.getValue().getPositionalParameters().get(1).getValue());
    }

    // ===== 2026-09-27 deep review (slice S) =====
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

    // ---- 2026-09-29 sliceS ----
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

    // A REPEATED TIMESTAMP / BYTES cell holds epoch-seconds / base64 text per element; a typed array row whose component
    // is itself an array (Instant[][], byte[][][]) must decode each element by schema instead of handing the raw text
    // to N.convert (DateTimeParseException / NumberFormatException).
    @Test
    public void testTypedArrayRowTargetDecodesRepeatedTimestampAndBytesElements() {
        final Field tsField = Field.newBuilder("ts", StandardSQLTypeName.TIMESTAMP).setMode(Field.Mode.REPEATED).build();
        final FieldValueList tsRow = FieldValueList.of(
                Arrays.asList(FieldValue.of(FieldValue.Attribute.REPEATED, Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1718900000.123456"),
                        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1718900001")))),
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

    // A getter-only property inherited from an @Entity superclass is in propInfoList, so insert/update write it and the
    // generated SELECT projects it; reading that column back threw UnsupportedOperationException for the whole row.
    @Test
    public void testGetterOnlyPropertyColumnIsSkippedOnRead() throws Exception {
        when(mockTableResult.getSchema()).thenReturn(null);
        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<FieldValueList>());
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
    // Entity with no @Id annotation and no id-named property (keyless)
    public static class KeylessEntity {
        private String name;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

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
    public static class TestEntityWithEmail {
        private int id;
        private String name;
        private String email;

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

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }
    }

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

    // ---- 2026-10-02 sliceS ----
    // A REPEATED STRUCT column mapped to a List<Bean>/Bean[] property went through the JSON codec, which handed the
    // element's raw TIMESTAMP (epoch seconds) / BYTES (base64) text to the bean and failed ("Cannot parse
    // \"1718900000.123456\""), while the same STRUCT mapped to a single bean property was decoded. Each element is now
    // mapped like a STRUCT property.
    @Test
    public void testRepeatedStructColumnMapsBeanElementsWithDecodedFields() {
        final FieldList sub = FieldList.of(Field.of("city", StandardSQLTypeName.STRING), Field.of("zip", StandardSQLTypeName.INT64),
                Field.of("seen_at", StandardSQLTypeName.TIMESTAMP), Field.of("blob", StandardSQLTypeName.BYTES));
        final FieldValue element = FieldValue.of(FieldValue.Attribute.RECORD,
                FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "SF"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "94105"),
                        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1718900000.123456"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "AQID")), sub));
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

    // BigQuery renders a TIME cell as HH:MM:SS[.ffffff]. The java.sql.Time conversion rejected the fractional part, so a
    // TIME value with sub-second precision - including one written by binding a java.sql.Time with milliseconds - could
    // not be read back into a java.sql.Time property / target.
    @Test
    public void testTimeCellWithFractionalSecondsReadsIntoSqlTime() throws Exception {
        final long expectedMillis = java.sql.Time.valueOf(java.time.LocalTime.of(16, 13, 20)).getTime() + 123;
        final FieldList fields = FieldList.of(Field.of("open_at", StandardSQLTypeName.TIME),
                Field.newBuilder("slots", StandardSQLTypeName.TIME).setMode(Field.Mode.REPEATED).build());
        final FieldValueList row = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "16:13:20.123456"),
                FieldValue.of(FieldValue.Attribute.REPEATED, Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "16:13:20.123"),
                        FieldValue.of(FieldValue.Attribute.PRIMITIVE, "16:13:20")))),
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

    // update(entity) on a class without key properties reported "primaryKeyNames cannot be null or empty", naming a
    // parameter the caller never passed; it now reports the missing keys like delete(entity).
    @Test
    public void testUpdateEntityWithoutKeyPropertiesReportsMissingKeyNames() throws Exception {
        final KeylessEntity entity = new KeylessEntity();
        entity.setName("n");

        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> executor.update(entity));

        assertTrue(ex.getMessage().contains("No key names defined for entity class: KeylessEntity"), ex.getMessage());
        assertEquals(ex.getMessage(), assertThrows(IllegalArgumentException.class, () -> executor.delete(entity)).getMessage());
        verify(mockBigQuery, times(0)).query(any(QueryJobConfiguration.class));
    }

    public static class Visit {
        private String city;
        private long zip;
        private java.time.Instant seenAt;
        private byte[] blob;

        public String getCity() {
            return city;
        }

        public void setCity(final String city) {
            this.city = city;
        }

        public long getZip() {
            return zip;
        }

        public void setZip(final long zip) {
            this.zip = zip;
        }

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

    // ---- 2026-10-02 verifyBQ ----
    private static FieldValue verifyCell(final String value) {
        return FieldValue.of(FieldValue.Attribute.PRIMITIVE, value);
    }

    private static FieldList verifyVisitFields() {
        return FieldList.of(Field.of("city", StandardSQLTypeName.STRING), Field.of("zip", StandardSQLTypeName.INT64),
                Field.of("seen_at", StandardSQLTypeName.TIMESTAMP));
    }

    private static FieldValue verifyVisit(final String city, final String zip) {
        return FieldValue.of(FieldValue.Attribute.RECORD,
                FieldValueList.of(Arrays.asList(verifyCell(city), verifyCell(zip), verifyCell("1718900000.123456")), verifyVisitFields()));
    }

    private static Field verifyRepeatedStruct(final String name, final FieldList subFields) {
        return Field.newBuilder(name, StandardSQLTypeName.STRUCT, subFields).setMode(Field.Mode.REPEATED).build();
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
        final RecordHolder holder = BigQueryExecutor.toEntity(holderFields, FieldValueList
                .of(Arrays.asList(FieldValue.of(FieldValue.Attribute.REPEATED, Arrays.asList(verifyVisit("SF", "94105"), verifyVisit("LA", "90001")))),
                        holderFields),
                RecordHolder.class);
        assertEquals(Arrays.asList(new VisitRecord("SF", 94105L, expected), new VisitRecord("LA", 90001L, expected)), holder.getVisits());

        final FieldList plain = FieldList.of(Field.of("city", StandardSQLTypeName.STRING), Field.of("zip", StandardSQLTypeName.INT64));
        final FieldList plainHolderFields = FieldList.of(verifyRepeatedStruct("visits", plain));
        final RecordHolder plainHolder = BigQueryExecutor.toEntity(plainHolderFields,
                FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.REPEATED,
                        Arrays.asList(FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(Arrays.asList(verifyCell("SF"), verifyCell("94105")), plain))))),
                        plainHolderFields),
                RecordHolder.class);
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
        final FieldValue repeated = FieldValue.of(FieldValue.Attribute.REPEATED, Arrays.asList(verifyCell(cells[0]), verifyCell(cells[1]), verifyCell(cells[2])));
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

    // A Map/Collection property with typed elements of an element bean of a REPEATED STRUCT receives typed values, as it
    // did through the JSON codec before the elements were mapped through toEntity. Matches HEAD.
    @Test
    public void testRepeatedStructElementTypedContainerPropertiesMatchHead() {
        // without "pair" (STRUCT -> List<Long>), which the JSON codec could not read from an element
        final FieldList all = verifyContainerFields();
        final FieldValueList allRow = verifyContainerRow(all);
        final FieldList sub = FieldList.of(all.get(0), all.get(2), all.get(3));
        final FieldList fields = FieldList.of(verifyRepeatedStruct("items", sub));
        final FieldValue repeated = FieldValue.of(FieldValue.Attribute.REPEATED, Arrays
                .asList(FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(Arrays.asList(allRow.get(0), allRow.get(2), allRow.get(3)), sub))));

        final TypedContainers item = BigQueryExecutor.toEntity(fields, FieldValueList.of(Arrays.asList(repeated), fields), TypedContainersHolder.class)
                .getItems()
                .get(0);

        final Object count = item.getCounts().get("x");
        final Object nestedCount = item.getNested().get("m").get("x");
        assertEquals(Long.valueOf(11), count);
        assertEquals(Long.valueOf(11), nestedCount);
        assertEquals("11", item.getRaw().get("x")); // Map<String, Object> keeps the cell text
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
        check.accept(BigQueryExecutor
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

    public record VisitRecord(String city, long zip, java.time.Instant seenAt) {
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

    // ---- 2026-10-03 fixBQ ----
    // Payloads shaped exactly as google-cloud-bigquery (2.72.0, FieldValue.fromPb) decodes a result page: a REPEATED cell
    // holds a FieldValueList WITHOUT a schema, and a STRUCT inside a REPEATED value - and every STRUCT nested in it - has no
    // schema either; only a STRUCT outside any REPEATED value carries its sub-schema. Every dispatch site tested
    // `instanceof FieldValueList` before `instanceof List`, so a real REPEATED value was read as a STRUCT and failed ("No
    // schema is attached ..." / "'fields' cannot be null"), and the nested STRUCTs of REPEATED STRUCT bean elements were read
    // with the attached schema they don't have.
    private static FieldValue fixBqRepeated(final FieldValue... elements) {
        return FieldValue.of(FieldValue.Attribute.REPEATED, FieldValueList.of(Arrays.asList(elements)));
    }

    private static FieldValue fixBqRecord(final FieldValue... values) {
        return FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(Arrays.asList(values)));
    }

    private static Field fixBqRepeatedField(final String name, final StandardSQLTypeName type) {
        return Field.newBuilder(name, type).setMode(Field.Mode.REPEATED).build();
    }

    private static FieldList fixBqLocFields() {
        return FieldList.of(Field.of("lat", StandardSQLTypeName.FLOAT64), Field.of("lng", StandardSQLTypeName.FLOAT64));
    }

    private static FieldList fixBqVisitFields() {
        return FieldList.of(Field.of("city", StandardSQLTypeName.STRING), Field.of("seen_at", StandardSQLTypeName.TIMESTAMP),
                Field.of("blob", StandardSQLTypeName.BYTES), Field.newBuilder("loc", StandardSQLTypeName.STRUCT, fixBqLocFields()).build(),
                Field.newBuilder("loc_map", StandardSQLTypeName.STRUCT, fixBqLocFields()).build(),
                Field.newBuilder("counts", StandardSQLTypeName.STRUCT, Field.of("x", StandardSQLTypeName.INT64)).build(),
                fixBqRepeatedField("codes", StandardSQLTypeName.INT64), Field
                        .newBuilder("stop", StandardSQLTypeName.STRUCT, Field.of("name", StandardSQLTypeName.STRING), fixBqRepeatedField("codes", StandardSQLTypeName.INT64))
                        .build());
    }

    // One element of a REPEATED STRUCT<fixBqVisitFields> column as the client decodes it: no schema at any level, a REPEATED
    // sub-field, and a STRUCT sub-field holding a REPEATED (REPEATED in STRUCT in REPEATED).
    private static FieldValue fixBqVisit(final String city) {
        return fixBqRecord(verifyCell(city), verifyCell("1718900000.123456"), verifyCell("AQID"), fixBqRecord(verifyCell("37.5"), verifyCell("-122.25")),
                fixBqRecord(verifyCell("1.5"), verifyCell("2.5")), fixBqRecord(verifyCell("11")), fixBqRepeated(verifyCell("1"), verifyCell("2")),
                fixBqRecord(verifyCell("s1"), fixBqRepeated(verifyCell("7"), verifyCell("8"))));
    }

    private void fixBqStubResult(final FieldList fields, final FieldValueList row) throws Exception {
        when(mockTableResult.getSchema()).thenReturn(Schema.of(fields));
        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.iterateAll()).thenReturn(Arrays.asList(row));
        when(mockTableResult.getValues()).thenReturn(Arrays.asList(row));
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);
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

    // REPEATED STRUCT -> List<Bean> / Bean[] whose element bean has nested STRUCT properties (bean, Map<String, Object>,
    // Map<String, Long>), a REPEATED property and a STRUCT property holding a REPEATED: none of those records carries a
    // schema, so each is read with the sub-schema of its enclosing column. toEntity, the registered converter, list rows and
    // the bean-class Dataset.
    @Test
    public void testSdkRepeatedStructIntoBeanElementsWithNestedStructs() throws Exception {
        final FieldList fields = FieldList.of(Field.of("id", StandardSQLTypeName.INT64), verifyRepeatedStruct("visits", fixBqVisitFields()),
                verifyRepeatedStruct("visit_array", fixBqVisitFields()));
        final FieldValueList row = FieldValueList.of(
                Arrays.asList(verifyCell("5"), fixBqRepeated(fixBqVisit("SF"), fixBqVisit("LA")), fixBqRepeated(fixBqVisit("NY"))), fields);
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

        final FixBqPlace place = BigQueryExecutor.toEntity(fields, FieldValueList.of(Arrays.asList(places), fields), FixBqPlaceHolder.class)
                .getPlaces()
                .get(0);

        assertEquals("SF", place.getCity());
        assertEquals(37.5, place.getLoc().getLat());
        assertEquals("1.5", place.getLocMap().get("lat"));
        final Object count = place.getCounts().get("x");
        assertEquals(Long.valueOf(11), count);
    }

    // REPEATED and REPEATED STRUCT columns in Map rows (toMap, list(Map), Dataset of Maps / raw values), List rows and
    // Object[] rows: the element values are unwrapped, STRUCT elements as Maps with their nested STRUCT and REPEATED values.
    @Test
    public void testSdkRepeatedColumnsInMapListAndObjectArrayRows() throws Exception {
        final FieldList fields = FieldList.of(Field.of("id", StandardSQLTypeName.INT64), fixBqRepeatedField("tags", StandardSQLTypeName.STRING),
                verifyRepeatedStruct("visits", fixBqVisitFields()));
        final FieldValueList row = FieldValueList.of(Arrays.asList(verifyCell("5"), fixBqRepeated(verifyCell("a"), verifyCell("b")), fixBqRepeated(fixBqVisit("SF"))),
                fields);

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
            assertEquals(Arrays.asList(5L), ((FixBqHome) BigQueryExecutor.extractData(mockTableResult, FixBqHomeHolder.class).getColumn("home").get(0)).getCodes());
        }
    }

    // Pins (unchanged behavior): a non-repeated STRUCT carrying its schema, without REPEATED sub-fields, reads as before on
    // every path; an Object target keeps the raw record; a FieldValueList-typed property keeps the raw STRUCT or REPEATED
    // value (which is a FieldValueList as the client decodes it).
    @Test
    public void testNonRepeatedStructAndRawFieldValueListPropertiesUnchanged() throws Exception {
        final FieldList homeFields = FieldList.of(Field.of("city", StandardSQLTypeName.STRING),
                Field.newBuilder("loc", StandardSQLTypeName.STRUCT, fixBqLocFields()).build());
        final FieldList fields = FieldList.of(Field.newBuilder("home", StandardSQLTypeName.STRUCT, homeFields).build());
        final FieldValue home = FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(Arrays.asList(verifyCell("Home"),
                FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(Arrays.asList(verifyCell("1.0"), verifyCell("2.0")), fixBqLocFields()))), homeFields));
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

    public static class FixBqLoc {
        private double lat;
        private double lng;

        public double getLat() {
            return lat;
        }

        public void setLat(final double lat) {
            this.lat = lat;
        }

        public double getLng() {
            return lng;
        }

        public void setLng(final double lng) {
            this.lng = lng;
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

    public static class FixBqHome {
        private String city;
        private FixBqLoc loc;
        private List<Long> codes;

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

        public List<Long> getCodes() {
            return codes;
        }

        public void setCodes(final List<Long> codes) {
            this.codes = codes;
        }
    }

    public static class FixBqHomeHolder {
        private FixBqHome home;

        public FixBqHome getHome() {
            return home;
        }

        public void setHome(final FixBqHome home) {
            this.home = home;
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

    // ---- 2026-10-03 main (raw FieldValueList single-value reads) ----

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

    private static Field fixBq2Struct(final String name, final FieldList subFields) {
        return Field.newBuilder(name, StandardSQLTypeName.STRUCT, subFields).build();
    }

    private static FieldList fixBq2ValueFields(final StandardSQLTypeName type) {
        return FieldList.of(Field.of("value", type));
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
        final FieldList itemFields = FieldList.of(Field.of("name", StandardSQLTypeName.STRING), fixBq2Struct("box", fixBq2ValueFields(StandardSQLTypeName.INT64)),
                fixBq2Struct("seen", fixBq2ValueFields(StandardSQLTypeName.TIMESTAMP)));
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
            assertEquals(Arrays.asList(1L, 2L), new ArrayList<Object>(s.getItems()));
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
        assertEquals("5", fixBq2ValueOf((FixBq2Box<?>) holder.getRaws().get(0)));
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

    public static class FixBq2Box<T> {
        private T value;

        public T getValue() {
            return value;
        }

        public void setValue(final T value) {
            this.value = value;
        }
    }

    public static class FixBq2LongBox extends FixBq2Box<Long> {
    }

    public static class FixBq2Series<T> {
        private List<T> items;
        private Map<String, T> byKey;
        private T first;

        public List<T> getItems() {
            return items;
        }

        public void setItems(final List<T> items) {
            this.items = items;
        }

        public Map<String, T> getByKey() {
            return byKey;
        }

        public void setByKey(final Map<String, T> byKey) {
            this.byKey = byKey;
        }

        public T getFirst() {
            return first;
        }

        public void setFirst(final T first) {
            this.first = first;
        }
    }

    public static class FixBq2Item {
        private String name;
        private FixBq2Box<Long> box;
        private FixBq2LongBox longBox;
        private FixBq2Box<java.time.Instant> seen;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public FixBq2Box<Long> getBox() {
            return box;
        }

        public void setBox(final FixBq2Box<Long> box) {
            this.box = box;
        }

        public FixBq2LongBox getLongBox() {
            return longBox;
        }

        public void setLongBox(final FixBq2LongBox longBox) {
            this.longBox = longBox;
        }

        public FixBq2Box<java.time.Instant> getSeen() {
            return seen;
        }

        public void setSeen(final FixBq2Box<java.time.Instant> seen) {
            this.seen = seen;
        }
    }

    public static class FixBq2LongBoxHolder {
        private List<FixBq2LongBox> boxes;
        private FixBq2LongBox[] boxArray;
        private Set<FixBq2LongBox> boxSet;

        public List<FixBq2LongBox> getBoxes() {
            return boxes;
        }

        public void setBoxes(final List<FixBq2LongBox> boxes) {
            this.boxes = boxes;
        }

        public FixBq2LongBox[] getBoxArray() {
            return boxArray;
        }

        public void setBoxArray(final FixBq2LongBox[] boxArray) {
            this.boxArray = boxArray;
        }

        public Set<FixBq2LongBox> getBoxSet() {
            return boxSet;
        }

        public void setBoxSet(final Set<FixBq2LongBox> boxSet) {
            this.boxSet = boxSet;
        }
    }

    public static class FixBq2BoxHolder {
        private FixBq2Box<Long> box;
        private FixBq2LongBox longBox;
        private FixBq2Item item;
        private List<FixBq2Item> items;
        private FixBq2Series<Long> series;
        private List<FixBq2Series<Long>> seriesList;
        private FixBq2Box<FixBq2Box<Long>> nested;
        private List<FixBq2Box<FixBq2Box<Long>>> nestedList;

        public FixBq2Box<Long> getBox() {
            return box;
        }

        public void setBox(final FixBq2Box<Long> box) {
            this.box = box;
        }

        public FixBq2LongBox getLongBox() {
            return longBox;
        }

        public void setLongBox(final FixBq2LongBox longBox) {
            this.longBox = longBox;
        }

        public FixBq2Item getItem() {
            return item;
        }

        public void setItem(final FixBq2Item item) {
            this.item = item;
        }

        public List<FixBq2Item> getItems() {
            return items;
        }

        public void setItems(final List<FixBq2Item> items) {
            this.items = items;
        }

        public FixBq2Series<Long> getSeries() {
            return series;
        }

        public void setSeries(final FixBq2Series<Long> series) {
            this.series = series;
        }

        public List<FixBq2Series<Long>> getSeriesList() {
            return seriesList;
        }

        public void setSeriesList(final List<FixBq2Series<Long>> seriesList) {
            this.seriesList = seriesList;
        }

        public FixBq2Box<FixBq2Box<Long>> getNested() {
            return nested;
        }

        public void setNested(final FixBq2Box<FixBq2Box<Long>> nested) {
            this.nested = nested;
        }

        public List<FixBq2Box<FixBq2Box<Long>>> getNestedList() {
            return nestedList;
        }

        public void setNestedList(final List<FixBq2Box<FixBq2Box<Long>>> nestedList) {
            this.nestedList = nestedList;
        }
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

    public enum FixBq2Color {
        RED, GREEN
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
        final FieldValueList row = FieldValueList.of(Arrays.asList(fixBqRepeated(fixBqRecord(verifyCell("x"))),
                fixBqRepeated(fixBqRecord(fixBqRepeated(verifyCell("a"), verifyCell("b")))), fixBqRepeated(fixBqRecord(verifyCell("y"))),
                fixBqRecord(verifyCell("z"))), fields);

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
        final Object rowValue = fixBq2ValueOf(BigQueryExecutor.toEntity(boxFields, FieldValueList.of(Arrays.asList(verifyCell("w")), boxFields), FixBq3SbBox.class));
        assertTrue(rowValue instanceof StringBuilder, String.valueOf(rowValue == null ? null : rowValue.getClass()));
        assertEquals("w", rowValue.toString());
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
                            mismatches.add(position.name + " | " + kind.name + " | " + (sdk ? "client shape" : "plain shape") + " | " + path.getKey() + ": expected "
                                    + expected + " but was " + actual);
                        }
                    }
                }
            }
        }

        assertEquals(208, combinations);
        assertEquals(208 * 2 * 4, checks);
        assertTrue(mismatches.isEmpty(),
                mismatches.size() + " of " + checks + " checks failed (" + combinations + " position x kind combinations, 2 payload shapes, 4 read paths):\n"
                        + String.join("\n", mismatches));
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
            assertEquals(Arrays.asList(expectedRecord, expectedRecord), Arrays.asList(BigQueryExecutor.toList(mockTableResult, VisitRecord[][].class).get(0)[0]));

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
        final FieldValue wrap = FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(Arrays.asList(fixBqRepeated(verifyCell("a"), verifyCell("b"))), tagsSub));
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
        fixBqStubResult(homeField, FieldValueList.of(Arrays.asList(fixBqRecord(verifyCell("Home"), fixBqRecord(verifyCell("1.0"), verifyCell("2.0")))), homeField));
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

        assertEquals(twoOfThree, assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.toEntity(fields, row, FixBqHomeHolder.class)).getMessage());
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

    private static FieldValue mxRecord(final boolean sdk, final FieldList sub, final FieldValue... values) {
        return FieldValue.of(FieldValue.Attribute.RECORD, sdk ? FieldValueList.of(Arrays.asList(values)) : FieldValueList.of(Arrays.asList(values), sub));
    }

    private static FieldValue mxRepeated(final boolean sdk, final FieldValue... elements) {
        return FieldValue.of(FieldValue.Attribute.REPEATED, sdk ? FieldValueList.of(Arrays.asList(elements)) : new ArrayList<>(Arrays.asList(elements)));
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
        } else if (cls.getName().startsWith(BigQueryExecutorTest.class.getName() + "$") && !cls.isEnum()) {
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

    @Test
    public void testRepeatedStructBeansHonorPropertyFormats() throws Exception {
        // Direct bean mapping must keep the annotation-aware conversion formerly supplied by the JSON codec.
        final FieldList valueFields = formattedValueFields();
        final FieldList fields = FieldList.of(verifyRepeatedStruct("items", valueFields),
                Field.newBuilder("single", StandardSQLTypeName.STRUCT, valueFields).build());

        for (final boolean sdk : new boolean[] { true, false }) {
            final FieldValue item = mxRecord(sdk, valueFields, verifyCell("03/10/2026"), verifyCell("03/10/2026"),
                    verifyCell("1718900000.123456"), verifyCell("1,234.50"), verifyCell("AQID"));
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

    @Test
    public void testRejectedQueryConditionReleasesInternalSqlBuilder() throws Exception {
        final Condition invalidCondition = Filters.expr("/* only a comment */");
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final BigQueryExecutor current = new BigQueryExecutor(mockBigQuery, policy);
            for (final Collection<String> properties : Arrays.asList(null, List.of("id"))) {
                assertRejectedWithoutBuilderLeak(() -> current.query(TestEntity.class, properties, invalidCondition));
            }
        }
        org.mockito.Mockito.verifyNoInteractions(mockBigQuery);
    }

    @Test
    public void testRejectedQueryProjectionReleasesInternalSqlBuilder() throws Exception {
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final BigQueryExecutor current = new BigQueryExecutor(mockBigQuery, policy);
            assertRejectedWithoutBuilderLeak(() -> current.query(TestEntity.class, List.of("id /* invalid column */"), null));
        }
        org.mockito.Mockito.verifyNoInteractions(mockBigQuery);
    }

    @Test
    public void testRejectedUpdateConditionReleasesInternalSqlBuilder() throws Exception {
        final Condition invalidCondition = Filters.expr("/* only a comment */");
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final BigQueryExecutor current = new BigQueryExecutor(mockBigQuery, policy);
            assertRejectedWithoutBuilderLeak(() -> current.update(TestEntity.class, Map.of("name", "updated"), invalidCondition));
        }
        org.mockito.Mockito.verifyNoInteractions(mockBigQuery);
    }

    @Test
    public void testRejectedUpdatePropertyReleasesInternalSqlBuilder() throws Exception {
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final BigQueryExecutor current = new BigQueryExecutor(mockBigQuery, policy);
            assertRejectedWithoutBuilderLeak(() -> current.update(TestEntity.class, Map.of("name /* invalid column */", "updated"), Filters.eq("id", 1)));
        }
        org.mockito.Mockito.verifyNoInteractions(mockBigQuery);
    }

    @Test
    public void testRejectedDeleteConditionReleasesInternalSqlBuilder() throws Exception {
        final Condition invalidCondition = Filters.expr("/* only a comment */");
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final BigQueryExecutor current = new BigQueryExecutor(mockBigQuery, policy);
            assertRejectedWithoutBuilderLeak(() -> current.delete(TestEntity.class, invalidCondition));
        }
        org.mockito.Mockito.verifyNoInteractions(mockBigQuery);
    }

    @Test
    public void testRejectedInsertValueReleasesInternalSqlBuilder() throws Exception {
        final Condition invalidCondition = Filters.expr("/* only a comment */");
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final BigQueryExecutor current = new BigQueryExecutor(mockBigQuery, policy);
            assertRejectedWithoutBuilderLeak(() -> current.insert(TestEntity.class, Map.of("name", invalidCondition)));
        }
        org.mockito.Mockito.verifyNoInteractions(mockBigQuery);
    }

    @Test
    public void testRejectedEntityInsertValueReleasesInternalSqlBuilder() throws Exception {
        final SqlValueEntity entity = new SqlValueEntity();
        entity.setId(1L);
        entity.setValue(Filters.expr("/* only a comment */"));
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final BigQueryExecutor current = new BigQueryExecutor(mockBigQuery, policy);
            assertRejectedWithoutBuilderLeak(() -> current.insert(entity));
        }
        org.mockito.Mockito.verifyNoInteractions(mockBigQuery);
    }

    @Test
    public void testRejectedEntityUpdateValueReleasesInternalSqlBuilder() throws Exception {
        final SqlValueEntity entity = new SqlValueEntity();
        entity.setId(1L);
        entity.setValue(Filters.expr("/* only a comment */"));
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final BigQueryExecutor current = new BigQueryExecutor(mockBigQuery, policy);
            assertRejectedWithoutBuilderLeak(() -> current.update(entity, Set.of("id")));
        }
        org.mockito.Mockito.verifyNoInteractions(mockBigQuery);
    }

    private static void assertRejectedWithoutBuilderLeak(final org.junit.jupiter.api.function.Executable action) throws Exception {
        final java.lang.reflect.Field field = com.landawn.abacus.query.AbstractQueryBuilder.class.getDeclaredField("activeStringBuilderCounter");
        field.setAccessible(true);
        final java.util.concurrent.atomic.AtomicInteger counter = (java.util.concurrent.atomic.AtomicInteger) field.get(null);
        final int before = counter.get();

        assertThrows(IllegalArgumentException.class, action);
        assertEquals(before, counter.get(), "A rejected query must release the executor-owned builder");
    }

    @lombok.Data
    public static class SqlValueEntity {
        private Long id;
        private Condition value;
    }

    @lombok.Data
    public static class FormattedValueHolder {
        private List<FormattedValue> items;
        private FormattedValue single;
    }
}
