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

import com.google.cloud.bigquery.BigQuery;
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
import com.landawn.abacus.util.stream.Stream;
import com.landawn.abacus.util.u.Nullable;

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
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<FieldValueList>());
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
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<FieldValueList>());
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
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<FieldValueList>());
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
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<FieldValueList>());
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
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<FieldValueList>());
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
        when(mockTableResult.iterateAll()).thenReturn(new ArrayList<FieldValueList>());
        when(mockBigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mockTableResult);

        Dataset ds = exec.query(TestEntity.class, Arrays.asList("name"), null);
        assertNotNull(ds);
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
}
