package com.landawn.abacus.da.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableResult;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.u.Nullable;
import com.landawn.abacus.util.stream.Stream;

public class BigQueryQueryTest extends BigQueryExecutorTestSupport {

    // Queries and results

    @Test
    public void testExecuteWithQueryAndParameters() throws Exception {
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        TableResult result = executor.execute("SELECT * FROM test_table WHERE id = ?", 1);

        assertEquals(mockTableResult, result);
        verify(mockBigQuery).query(any(QueryJobConfiguration.class));
    }

    @Test
    public void testExists_NullWhereClause_HasRows() throws Exception {
        when(mockTableResult.getTotalRows()).thenReturn(3L);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        assertTrue(executor.exists(TestEntity.class, (Condition) null));
    }

    @Test
    public void testExists_NullWhereClause_NoRows() throws Exception {
        // Null whereClause passes through prepareQuery without a WHERE.
        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        assertFalse(executor.exists(TestEntity.class, (Condition) null));
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
    public void testExistsWithClassAndIds() throws Exception {
        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        boolean exists = executor.exists(TestEntity.class, 1);

        assertTrue(exists);
        verify(mockBigQuery).query(any(QueryJobConfiguration.class));
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
    public void testListWithClassAndCondition_NoMatching() throws Exception {
        Field f = Field.of("id", StandardSQLTypeName.INT64);
        FieldList fields = FieldList.of(f);
        Schema schema = Schema.of(fields);

        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<>());
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        List<TestEntity> list = executor.list(TestEntity.class, Filters.eq("id", -1));
        assertNotNull(list);
        assertEquals(0, list.size());
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
    public void testQuery_NullCondition() throws Exception {
        // whereClause == null exercises the "no where clause" path in prepareQuery.
        Field f = Field.of("id", StandardSQLTypeName.INT64);
        Schema schema = Schema.of(FieldList.of(f));

        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<>());
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Dataset ds = executor.query(TestEntity.class, (Condition) null);
        assertNotNull(ds);
    }

    @Test
    public void testQuery_CamelCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.CAMEL_CASE);
        Field f = Field.of("id", StandardSQLTypeName.INT64);
        Schema schema = Schema.of(FieldList.of(f));

        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<>());
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Dataset ds = exec.query(TestEntity.class, Filters.eq("id", 1));
        assertNotNull(ds);
    }

    @Test
    public void testQuery_ScreamingSnakeCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        Field f = Field.of("id", StandardSQLTypeName.INT64);
        Schema schema = Schema.of(FieldList.of(f));

        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<>());
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Dataset ds = exec.query(TestEntity.class, Filters.eq("id", 1));
        assertNotNull(ds);
    }

    @Test
    public void testQuery_WithSelectProps_CamelCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.CAMEL_CASE);
        Field f = Field.of("name", StandardSQLTypeName.STRING);
        Schema schema = Schema.of(FieldList.of(f));

        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<>());
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Dataset ds = exec.query(TestEntity.class, Arrays.asList("name"), null);
        assertNotNull(ds);
    }

    @Test
    public void testQuery_WithSelectProps_ScreamingSnakeCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(mockBigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        Field f = Field.of("name", StandardSQLTypeName.STRING);
        Schema schema = Schema.of(FieldList.of(f));

        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockTableResult.getSchema()).thenReturn(schema);
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<>());
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Dataset ds = exec.query(TestEntity.class, Arrays.asList("name"), null);
        assertNotNull(ds);
    }

    @Test
    public void testQueryDoesNotRewriteAliasLikeTextInsideLiteral() throws Exception {
        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockTableResult.getSchema()).thenReturn(Schema.of(FieldList.of(Field.of("id", StandardSQLTypeName.INT64))));
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<>());
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        executor.query(TestEntity.class, Filters.expr("note = 'AS \"not_an_alias\"'"));

        final String sql = captureGeneratedSql();
        assertTrue(sql.contains("'AS \"not_an_alias\"'"), "quoted raw-expression text must be preserved: " + sql);
        assertTrue(sql.contains("`"), "generated aliases must still use backticks: " + sql);
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
    public void testQueryForSingleValue_ByQuery_NoRows() throws Exception {
        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Nullable<Long> result = executor.queryForSingleValue(Long.class, "SELECT COUNT(*) FROM t WHERE id = ?", 0);
        assertFalse(result.isPresent());
    }

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

    @Test
    public void testqueryForSingleValueWithEmptyResult() throws Exception {
        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Nullable<String> result = executor.queryForSingleValue(String.class, "SELECT name FROM test_table WHERE id = ?", 999);

        assertFalse(result.isPresent());
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

    // Regression: SELECT must alias columns with BigQuery backticks (`id`), not ANSI double quotes
    // ("id"). BigQuery/GoogleSQL parses a double-quoted token as a STRING LITERAL. The BigQuery
    // SqlBuilder variants are configured with IdentifierQuote.BACKTICK so no fragile SQL text
    // post-processing is needed.
    @Test
    public void testQueryGeneratesBacktickQuotedAliasesNotDoubleQuotes() throws Exception {
        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockTableResult.getSchema()).thenReturn(Schema.of(FieldList.of(Field.of("id", StandardSQLTypeName.INT64))));
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<>());
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
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<>());
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        executor.query(TestEntity.class, Filters.expr("name = \"Alice\""));

        final String sql = captureGeneratedSql();
        assertTrue(sql.contains("\"Alice\""), "double-quoted string literal must be preserved: " + sql);
        assertTrue(sql.contains("`"), "generated aliases must still use BigQuery backticks: " + sql);
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

    /** A null job configuration is not checked by the BigQuery client; reject it at the call site. */
    @Test
    public void testStreamRejectsNullQueryJobConfiguration() {
        assertThrows(IllegalArgumentException.class, () -> executor.stream((QueryJobConfiguration) null));
        assertThrows(IllegalArgumentException.class, () -> executor.stream(TestEntity.class, (QueryJobConfiguration) null));
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
    public void testStreamWithClassAndQueryJobConfiguration_InterruptedException() throws Exception {
        QueryJobConfiguration cfg = QueryJobConfiguration.newBuilder("SELECT 1").build();
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenThrow(new InterruptedException("interrupted"));

        Thread.interrupted();
        assertThrows(RuntimeException.class, () -> executor.stream(TestEntity.class, cfg));
        Thread.interrupted();
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
    public void testStreamWithQueryJobConfiguration_InterruptedException() throws Exception {
        QueryJobConfiguration cfg = QueryJobConfiguration.newBuilder("SELECT 1").build();
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenThrow(new InterruptedException("interrupted"));

        // Clear interrupt flag first (the executor sets it on InterruptedException).
        Thread.interrupted();
        assertThrows(RuntimeException.class, () -> executor.stream(cfg));
        // The catch handler re-interrupts the thread; clear it so we don't leak state to later tests.
        Thread.interrupted();
    }

    // Resource lifecycle

    @Test
    public void testExecute_InterruptedException() throws Exception {
        // execute(String, Object...) must convert InterruptedException to RuntimeException.
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenThrow(new InterruptedException("interrupted"));

        Thread.interrupted();
        assertThrows(RuntimeException.class, () -> executor.execute("SELECT 1"));
        Thread.interrupted();
    }

    // Additional contracts

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

    // ---- 2026-10-27 sliceL ----

    /**
     * A row class that N.newCollection / IntFunctions.ofMap cannot create as such (ImmutableMap, ImmutableSortedMap,
     * ImmutableList, ImmutableSet) got a mutable stand-in (HashMap, TreeMap, ArrayList, HashSet) as the row: toList/list/stream
     * returned a List&lt;ImmutableMap&gt; holding HashMaps (ClassCastException where the caller read a row), a STRUCT cell read by
     * queryForSingleValue(ImmutableList.class) was an ArrayList, and N.convert(row, ImmutableMap.class) rejected the converter's
     * own result. Each row is now of the requested class, with the raw cell values. RED on HEAD for every case.
     */
    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testImmutableRowClassesGetRowsOfThatClass_sliceL() throws Exception {
        final FieldList fields = FieldList.of(Field.of("id", StandardSQLTypeName.INT64), Field.of("name", StandardSQLTypeName.STRING));
        final FieldValueList row = FieldValueList
                .of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "n")), fields);
        when(mockTableResult.getSchema()).thenReturn(Schema.of(fields));
        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.iterateAll()).thenReturn(Arrays.asList(row));
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        final FieldList structFields = FieldList.of(Field.of("pair", StandardSQLTypeName.STRUCT, fields));
        final FieldValueList structRow = FieldValueList.of(
                Arrays.asList(FieldValue.of(FieldValue.Attribute.RECORD,
                        FieldValueList
                                .of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "m"))))),
                structFields);
        final TableResult structResult = org.mockito.Mockito.mock(TableResult.class);
        when(structResult.getSchema()).thenReturn(Schema.of(structFields));
        when(structResult.getTotalRows()).thenReturn(1L);
        when(structResult.getValues()).thenReturn(Arrays.asList(structRow));

        final Map<String, Object> expectedMap = new java.util.HashMap<>();
        expectedMap.put("id", "1");
        expectedMap.put("name", "n");

        org.junit.jupiter.api.Assertions.assertAll(() -> {
            final Object r = BigQueryExecutor.toList(mockTableResult, com.landawn.abacus.util.ImmutableMap.class).get(0);
            assertTrue(r instanceof com.landawn.abacus.util.ImmutableMap, r.getClass().getName());
            assertEquals(expectedMap, r);
        }, () -> {
            final Object r = executor.list(com.landawn.abacus.util.ImmutableSortedMap.class, "SELECT id, name FROM t").get(0);
            assertTrue(r instanceof com.landawn.abacus.util.ImmutableSortedMap, r.getClass().getName());
            assertEquals(expectedMap, r);
        }, () -> {
            final Object r = executor.list(com.landawn.abacus.util.ImmutableList.class, "SELECT id, name FROM t").get(0);
            assertTrue(r instanceof com.landawn.abacus.util.ImmutableList, r.getClass().getName());
            assertEquals(Arrays.asList("1", "n"), r);
        }, () -> {
            final Object r = ((Stream<Object>) (Stream) executor.stream(com.landawn.abacus.util.ImmutableSet.class, "SELECT id, name FROM t")).first()
                    .orElseNull();
            assertTrue(r instanceof com.landawn.abacus.util.ImmutableSet, String.valueOf(r));
            assertEquals(new java.util.HashSet<>(Arrays.asList("1", "n")), r);
        }, () -> {
            final Object r = com.landawn.abacus.util.N.convert(row, com.landawn.abacus.util.ImmutableMap.class);
            assertTrue(r instanceof com.landawn.abacus.util.ImmutableMap, r.getClass().getName());
            assertEquals(expectedMap, r);
        }, () -> {
            when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(structResult);
            final Object r = executor.queryForSingleValue(com.landawn.abacus.util.ImmutableList.class, "SELECT pair FROM t").orElseNull();
            assertTrue(r instanceof com.landawn.abacus.util.ImmutableList, r.getClass().getName());
            assertEquals(Arrays.asList("2", "m"), r);
        });
    }

    /**
     * Review 2026-10-27 r2E: the remaining row-class cases of the immutable-row fix. A NULL cell keeps its null in an immutable row;
     * a row class that the factories cannot create and N.convert cannot wrap ({@code EnumMap}, whose keys must be enums, and
     * {@code AbstractQueue}) fails with IllegalArgumentException instead of returning {@code HashMap} / {@code LinkedList} rows (RED on
     * HEAD); a mutable row class keeps the factory's own instance ({@code LinkedHashMap} in column order, {@code TreeMap},
     * {@code LinkedList}), as before.
     */
    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testRowClassesWithNullCellsStandInsAndMutableClasses_r2E() throws Exception {
        final FieldList fields = FieldList.of(Field.of("name", StandardSQLTypeName.STRING), Field.of("id", StandardSQLTypeName.INT64));
        final FieldValueList row = FieldValueList
                .of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "n"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, null)), fields);
        when(mockTableResult.getSchema()).thenReturn(Schema.of(fields));
        when(mockTableResult.getTotalRows()).thenReturn(1L);
        when(mockTableResult.iterateAll()).thenReturn(Arrays.asList(row));
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        final Map<String, Object> expectedMap = new java.util.HashMap<>();
        expectedMap.put("name", "n");
        expectedMap.put("id", null);

        org.junit.jupiter.api.Assertions.assertAll(() -> {
            final Object r = BigQueryExecutor.toList(mockTableResult, com.landawn.abacus.util.ImmutableMap.class).get(0);
            assertTrue(r instanceof com.landawn.abacus.util.ImmutableMap, r.getClass().getName());
            assertEquals(expectedMap, r);
        }, () -> {
            final Object r = executor.list(com.landawn.abacus.util.ImmutableList.class, "SELECT name, id FROM t").get(0);
            assertTrue(r instanceof com.landawn.abacus.util.ImmutableList, r.getClass().getName());
            assertEquals(Arrays.asList("n", null), r);
        }, () -> {
            final Object r = com.landawn.abacus.util.N.convert(row, com.landawn.abacus.util.ImmutableList.class);
            assertTrue(r instanceof com.landawn.abacus.util.ImmutableList, r.getClass().getName());
            assertEquals(Arrays.asList("n", null), r);
        }, () -> assertThrows(IllegalArgumentException.class, () -> BigQueryExecutor.toList(mockTableResult, java.util.EnumMap.class)),
                () -> assertThrows(IllegalArgumentException.class, () -> executor.list(java.util.AbstractQueue.class, "SELECT name, id FROM t")), () -> {
                    final Object r = BigQueryExecutor.toList(mockTableResult, java.util.LinkedHashMap.class).get(0);
                    assertEquals(java.util.LinkedHashMap.class, r.getClass());
                    assertEquals(Arrays.asList("name", "id"), new ArrayList<>(((Map) r).keySet()));
                }, () -> assertEquals(java.util.TreeMap.class, executor.list(java.util.TreeMap.class, "SELECT name, id FROM t").get(0).getClass()), () -> {
                    final Object r = executor.list(java.util.LinkedList.class, "SELECT name, id FROM t").get(0);
                    assertEquals(java.util.LinkedList.class, r.getClass());
                    assertEquals(Arrays.asList("n", null), r);
                });
    }

    /**
     * Pins the class Javadoc's Dataset and Table Naming section: the executor's own query configuration sets no default
     * dataset, and a dataset-qualified {@code @Table} name is emitted as written in the generated statements.
     */
    @Test
    public void testGeneratedStatementsUseQualifiedTableNameWithoutDefaultDataset_sliceL() throws Exception {
        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        final SliceLQualifiedCustomer customer = new SliceLQualifiedCustomer();
        customer.setId("c1");
        customer.setName("n");

        executor.insert(customer);
        executor.list(SliceLQualifiedCustomer.class, Filters.eq("name", "n"));
        executor.execute("SELECT 1");

        final ArgumentCaptor<QueryJobConfiguration> captor = ArgumentCaptor.forClass(QueryJobConfiguration.class);
        verify(mockBigQuery, org.mockito.Mockito.times(3)).query(captor.capture());
        final List<QueryJobConfiguration> configs = captor.getAllValues();

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals("INSERT INTO mydataset.customer (id, name) VALUES (?, ?)", configs.get(0).getQuery()),
                () -> assertTrue(configs.get(1).getQuery().contains(" FROM mydataset.customer WHERE "), configs.get(1).getQuery()),
                () -> assertNull(configs.get(0).getDefaultDataset()), () -> assertNull(configs.get(1).getDefaultDataset()),
                () -> assertNull(configs.get(2).getDefaultDataset()));
    }

    @com.landawn.abacus.annotation.Table("mydataset.customer")
    public static class SliceLQualifiedCustomer {
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
    }

    /**
     * Pins the class Javadoc's sub-entity paragraph (SNAKE_CASE): a bean-typed property without {@code @Column} is left out of
     * the select-all list and its dotted path renders with the sub-entity class's table name ({@code address.city}); with
     * {@code @Column} the path is {@code home_address.city} even when the annotation names another column; a backtick-quoted
     * first segment is emitted as written.
     */
    @Test
    public void testDottedPathThroughBeanPropertyRendering_sliceL() throws Exception {
        when(mockTableResult.getTotalRows()).thenReturn(0L);
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        executor.list(SliceLSubEntityPerson.class, Filters.eq("homeAddress.city", "SF"));
        executor.list(SliceLColumnPerson.class, Filters.eq("homeAddress.city", "SF"));
        executor.list(SliceLNamedColumnPerson.class, Filters.eq("homeAddress.city", "SF"));
        executor.list(SliceLNamedColumnPerson.class, Filters.eq("`home_addr`.city", "SF"));

        final ArgumentCaptor<QueryJobConfiguration> captor = ArgumentCaptor.forClass(QueryJobConfiguration.class);
        verify(mockBigQuery, org.mockito.Mockito.times(4)).query(captor.capture());
        final List<QueryJobConfiguration> configs = captor.getAllValues();

        // The sub-entity path takes the table name of the property's class (SliceLAddress), not the property name.
        final String subEntityTable = NamingPolicy.SNAKE_CASE.convert("SliceLAddress");

        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertEquals("SELECT id AS `id`, name AS `name` FROM " + NamingPolicy.SNAKE_CASE.convert("SliceLSubEntityPerson") + " WHERE "
                        + subEntityTable + ".city = ?", configs.get(0).getQuery()),
                () -> assertTrue(configs.get(1).getQuery().contains("home_address AS `homeAddress`"), configs.get(1).getQuery()),
                () -> assertTrue(configs.get(1).getQuery().endsWith(" WHERE home_address.city = ?"), configs.get(1).getQuery()),
                () -> assertTrue(configs.get(2).getQuery().contains("home_addr AS `homeAddress`"), configs.get(2).getQuery()),
                () -> assertTrue(configs.get(2).getQuery().endsWith(" WHERE home_address.city = ?"), configs.get(2).getQuery()),
                () -> assertTrue(configs.get(3).getQuery().endsWith(" WHERE `home_addr`.city = ?"), configs.get(3).getQuery()));
    }

    public static class SliceLAddress {
        private String city;

        public String getCity() {
            return city;
        }

        public void setCity(final String city) {
            this.city = city;
        }
    }

    public static class SliceLSubEntityPerson {
        @com.landawn.abacus.annotation.Id
        private String id;
        private String name;
        private SliceLAddress homeAddress;

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

        public SliceLAddress getHomeAddress() {
            return homeAddress;
        }

        public void setHomeAddress(final SliceLAddress homeAddress) {
            this.homeAddress = homeAddress;
        }
    }

    public static class SliceLColumnPerson {
        @com.landawn.abacus.annotation.Id
        private String id;
        private String name;
        @com.landawn.abacus.annotation.Column
        private SliceLAddress homeAddress;

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

        public SliceLAddress getHomeAddress() {
            return homeAddress;
        }

        public void setHomeAddress(final SliceLAddress homeAddress) {
            this.homeAddress = homeAddress;
        }
    }

    public static class SliceLNamedColumnPerson {
        @com.landawn.abacus.annotation.Id
        private String id;
        private String name;
        @com.landawn.abacus.annotation.Column("home_addr")
        private SliceLAddress homeAddress;

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

        public SliceLAddress getHomeAddress() {
            return homeAddress;
        }

        public void setHomeAddress(final SliceLAddress homeAddress) {
            this.homeAddress = homeAddress;
        }
    }
}
