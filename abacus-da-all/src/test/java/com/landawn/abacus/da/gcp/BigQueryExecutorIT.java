package com.landawn.abacus.da.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.u.Nullable;
import com.landawn.abacus.util.stream.Stream;

/**
 * BigQuery emulator round trips. Requires the test-project emulator at localhost:9050 and creates a separate dataset for each run. Static mapping and validation contracts live in the BigQuery unit suites.
 *
 * <p>Run with {@code mvn -pl abacus-da-all -Pintegration-tests -Dit.test=BigQueryExecutorIT verify -Dgpg.skip=true}.</p>
 */
public class BigQueryExecutorIT extends TestBase {

    private static final String PROJECT = "test-project";
    private static final String ENDPOINT = "http://localhost:9050";
    private static final String DATASET = "abacus_da_test_" + java.util.UUID.randomUUID().toString().replace("-", "");
    private static final String TEST_TABLE = DATASET + ".test_entity"; // SNAKE_CASE / SCREAMING_SNAKE_CASE (case-insensitive) of TestEntity
    private static final String CAMEL_TABLE = DATASET + ".testEntity"; // CAMEL_CASE of TestEntity (no underscore -> distinct table)
    private static final String CK_TABLE = DATASET + ".composite_key_entity";

    private static BigQuery bigQuery;
    private static BigQueryExecutor executor;

    private static boolean datasetCreated;

    private static final AtomicInteger SEQ = new AtomicInteger(100_000);

    private static int uid() {
        return SEQ.incrementAndGet();
    }

    @BeforeAll
    public static void setUpClass() throws Exception {
        bigQuery = BigQueryOptions.newBuilder().setProjectId(PROJECT).setHost(ENDPOINT).setCredentials(NoCredentials.getInstance()).build().getService();
        executor = new BigQueryExecutor(bigQuery);
        bigQuery.create(DatasetInfo.newBuilder(DATASET).build());
        datasetCreated = true;
        ddl("CREATE TABLE " + TEST_TABLE + " (id INT64, name STRING)");
        ddl("CREATE TABLE " + CAMEL_TABLE + " (id INT64, name STRING)");
        ddl("CREATE TABLE " + CK_TABLE + " (id_a STRING, id_b STRING, value STRING)");
    }

    @AfterAll
    public static void tearDownClass() {
        if (bigQuery != null && datasetCreated) {
            bigQuery.delete(DATASET, BigQuery.DatasetDeleteOption.deleteContents());
        }
    }

    private static TableResult ddl(final String sql) throws Exception {
        return bigQuery.query(QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build());
    }

    /** Runs a SELECT (or any query) and returns the real {@link TableResult}. */
    private static TableResult q(final String sql) throws Exception {
        return bigQuery.query(QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build());
    }

    private static TestEntity entity(final int id, final String name) {
        final TestEntity e = new TestEntity();
        e.setId(id);
        e.setName(name);
        return e;
    }

    // ====================================================================================================
    // Conversion round-trips: real TableResult fed to the static helpers
    // ====================================================================================================

    @Test
    public void testToListWithTableResultAndClass() throws Exception {
        TableResult tr = q("SELECT 1 AS id, 'Name1' AS name UNION ALL SELECT 2 AS id, 'Name2' AS name ORDER BY id");

        List<TestEntity> list = BigQueryExecutor.toList(tr, TestEntity.class);

        assertNotNull(list);
        assertEquals(2, list.size());
        assertEquals(1, list.get(0).getId());
        assertEquals("Name1", list.get(0).getName());
        assertEquals(2, list.get(1).getId());
        assertEquals("Name2", list.get(1).getName());
    }

    @Test
    public void testToListWithEmptyTableResult() throws Exception {
        TableResult tr = q("SELECT id, name FROM " + TEST_TABLE + " WHERE id = -1");

        List<TestEntity> list = BigQueryExecutor.toList(tr, TestEntity.class);

        assertNotNull(list);
        assertEquals(0, list.size());
    }

    @Test
    public void testExtractDataWithTableResultAndClass() throws Exception {
        TableResult tr = q("SELECT 1 AS id, 'Name1' AS name");

        Dataset dataset = BigQueryExecutor.extractData(tr, TestEntity.class);

        assertNotNull(dataset);
        assertEquals(2, dataset.columnCount());
        assertEquals(1, dataset.size());
        assertTrue(dataset.columnNames().contains("id"));
        assertTrue(dataset.columnNames().contains("name"));
    }

    @Test
    public void testExtractDataWithEmptyTableResult() throws Exception {
        TableResult tr = q("SELECT id, name FROM " + TEST_TABLE + " WHERE id = -1");

        Dataset dataset = BigQueryExecutor.extractData(tr, TestEntity.class);

        assertNotNull(dataset);
        assertEquals(0, dataset.size());
    }

    @Test
    public void testExtractDataWithMapClass() throws Exception {
        TableResult tr = q("SELECT 1 AS id, 'Name1' AS name");

        Dataset dataset = BigQueryExecutor.extractData(tr, Map.class);

        assertNotNull(dataset);
        assertEquals(2, dataset.columnCount());
        assertEquals(1, dataset.size());
    }

    @Test
    public void testExtractData_AsMapClass() throws Exception {
        TableResult tr = q("SELECT 1 AS id, 'x' AS name");

        Dataset ds = BigQueryExecutor.extractData(tr, Map.class);
        assertNotNull(ds);
        assertEquals(1, ds.size());
        assertTrue(ds.containsColumn("id"));
    }

    @Test
    public void testExtractData_NullTargetClass() throws Exception {
        TableResult tr = q("SELECT 55 AS id");

        Dataset ds = BigQueryExecutor.extractData(tr, null);
        assertNotNull(ds);
        assertEquals(1, ds.size());
    }

    @Test
    public void testToListWithDifferentRowClasses() throws Exception {
        TableResult tr = q("SELECT 'Value1' AS value");

        List<String> stringList = BigQueryExecutor.toList(q("SELECT 'Value1' AS value"), String.class);
        assertEquals(1, stringList.size());
        assertEquals("Value1", stringList.get(0));

        List<Map> mapList = BigQueryExecutor.toList(q("SELECT 'Value1' AS value"), Map.class);
        assertEquals(1, mapList.size());

        List<Object[]> arrayList = BigQueryExecutor.toList(q("SELECT 'Value1' AS value"), Object[].class);
        assertEquals(1, arrayList.size());

        List<List> listList = BigQueryExecutor.toList(tr, List.class);
        assertEquals(1, listList.size());
    }

    @Test
    public void testToListWithObjectArrayPopulatesElements() throws Exception {
        TableResult tr = q("SELECT 42 AS id, 'Alice' AS name UNION ALL SELECT 43 AS id, 'Bob' AS name ORDER BY id");

        List<Object[]> arrayList = BigQueryExecutor.toList(tr, Object[].class);

        assertNotNull(arrayList);
        assertEquals(2, arrayList.size());
        assertEquals(2, arrayList.get(0).length);
        assertEquals("42", arrayList.get(0)[0]);
        assertEquals("Alice", arrayList.get(0)[1]);
        assertEquals(2, arrayList.get(1).length);
        assertEquals("43", arrayList.get(1)[0]);
        assertEquals("Bob", arrayList.get(1)[1]);
    }

    @Test
    public void testToListWithListClassPopulatesElements() throws Exception {
        TableResult tr = q("SELECT 100 AS id, 'Carol' AS name");

        List<List> listList = BigQueryExecutor.toList(tr, List.class);

        assertNotNull(listList);
        assertEquals(1, listList.size());
        assertEquals(2, listList.get(0).size());
        assertEquals("100", listList.get(0).get(0));
        assertEquals("Carol", listList.get(0).get(1));
    }

    @Test
    public void testToList_AsCollectionClass() throws Exception {
        TableResult tr = q("SELECT 1 AS id, 'n' AS name");

        @SuppressWarnings({ "unchecked", "rawtypes" })
        List<List> result = BigQueryExecutor.toList(tr, (Class) List.class);
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).size());
    }

    @Test
    public void testToList_AsMapClass() throws Exception {
        TableResult tr = q("SELECT 42 AS id");

        @SuppressWarnings({ "unchecked", "rawtypes" })
        List<Map> result = BigQueryExecutor.toList(tr, (Class) Map.class);
        assertEquals(1, result.size());
        assertEquals("42", result.get(0).get("id"));
    }

    @Test
    public void testToList_AsSingleValueClass() throws Exception {
        TableResult tr = q("SELECT 100 AS id UNION ALL SELECT 200 AS id ORDER BY id");

        List<Long> result = BigQueryExecutor.toList(tr, Long.class);
        assertEquals(2, result.size());
        assertEquals(Long.valueOf(100L), result.get(0));
        assertEquals(Long.valueOf(200L), result.get(1));
    }

    @Test
    public void testToList_WithIntegerClass() throws Exception {
        TableResult tr = q("SELECT 1 AS c UNION ALL SELECT 2 AS c ORDER BY c");

        List<Integer> values = BigQueryExecutor.toList(tr, Integer.class);
        assertEquals(2, values.size());
        assertEquals(Integer.valueOf(1), values.get(0));
        assertEquals(Integer.valueOf(2), values.get(1));
    }

    @Test
    public void testToList_ReadRow_WithFieldValueList_AsObjectArray() throws Exception {
        TableResult tr = q("SELECT STRUCT('v' AS inner_val) AS outer_val");

        List<Object[]> result = BigQueryExecutor.toList(tr, Object[].class);
        assertEquals(1, result.size());
        // The nested struct should be expanded to its own array.
        assertTrue(result.get(0)[0] instanceof Object[]);
    }

    @Test
    public void testExtractData_NestedFieldValueListInColumn() throws Exception {
        TableResult tr = q("SELECT STRUCT('x' AS a) AS nested");

        Dataset ds = BigQueryExecutor.extractData(tr, Map.class);
        assertNotNull(ds);
        assertEquals(1, ds.size());
    }

    // ----- Null-schema (DML-result) shape: a real DML TableResult has no data rows -----

    @Test
    public void testToListWithDmlResult() throws Exception {
        TableResult dml = executor.delete(TestEntity.class, Filters.eq("id", -987_654));
        List<TestEntity> list = BigQueryExecutor.toList(dml, TestEntity.class);
        assertNotNull(list);
        assertTrue(list.isEmpty());
    }

    @Test
    public void testExtractDataWithDmlResult() throws Exception {
        TableResult dml = executor.delete(TestEntity.class, Filters.eq("id", -987_653));
        Dataset ds = BigQueryExecutor.extractData(dml, TestEntity.class);
        assertNotNull(ds);
        assertEquals(0, ds.size());
    }

    @Test
    public void testExtractDataWithDmlResult_MapClass() throws Exception {
        TableResult dml = executor.delete(TestEntity.class, Filters.eq("id", -987_652));
        Dataset ds = BigQueryExecutor.extractData(dml, Map.class);
        assertNotNull(ds);
        assertEquals(0, ds.size());
    }

    // ====================================================================================================
    // Executor round-trips: real INSERT / UPDATE / DELETE / SELECT
    // ====================================================================================================

    @Test
    public void testInsertEntity() throws Exception {
        int id = uid();
        TableResult result = executor.insert(entity(id, "Test"));
        assertNotNull(result);
        assertTrue(executor.exists(TestEntity.class, id));
    }

    @Test
    public void testInsertWithClassAndProps() throws Exception {
        int id = uid();
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("id", id);
        props.put("name", "Test");

        TableResult result = executor.insert(TestEntity.class, props);
        assertNotNull(result);
        assertTrue(executor.exists(TestEntity.class, id));
    }

    @Test
    public void testUpdateEntity() throws Exception {
        int id = uid();
        executor.insert(entity(id, "old"));

        TableResult result = executor.update(entity(id, "UpdatedTest"));
        assertNotNull(result);
        assertEquals("UpdatedTest", executor.queryForSingleValue(TestEntity.class, String.class, "name", Filters.eq("id", id)).orElse(null));
    }

    @Test
    public void testUpdateEntityWithPrimaryKeyNames() throws Exception {
        int id = uid();
        executor.insert(entity(id, "old"));

        TableResult result = executor.update(entity(id, "UpdatedTest"), N.asSet("id"));
        assertNotNull(result);
        assertEquals("UpdatedTest", executor.queryForSingleValue(TestEntity.class, String.class, "name", Filters.eq("id", id)).orElse(null));
    }

    @Test
    public void testUpdateWithClassPropsAndCondition() throws Exception {
        int id = uid();
        executor.insert(entity(id, "old"));

        Map<String, Object> props = new HashMap<>();
        props.put("name", "UpdatedTest");
        TableResult result = executor.update(TestEntity.class, props, Filters.eq("id", id));
        assertNotNull(result);
        assertEquals("UpdatedTest", executor.queryForSingleValue(TestEntity.class, String.class, "name", Filters.eq("id", id)).orElse(null));
    }

    @Test
    public void testDeleteEntity() throws Exception {
        int id = uid();
        executor.insert(entity(id, "x"));

        TableResult result = executor.delete(entity(id, "x"));
        assertNotNull(result);
        assertFalse(executor.exists(TestEntity.class, id));
    }

    @Test
    public void testDeleteWithClassAndIds() throws Exception {
        int id = uid();
        executor.insert(entity(id, "x"));

        TableResult result = executor.delete(TestEntity.class, id);
        assertNotNull(result);
        assertFalse(executor.exists(TestEntity.class, id));
    }

    @Test
    public void testDeleteWithClassAndCondition() throws Exception {
        int id = uid();
        executor.insert(entity(id, "x"));

        TableResult result = executor.delete(TestEntity.class, Filters.eq("id", id));
        assertNotNull(result);
        assertFalse(executor.exists(TestEntity.class, id));
    }

    // ----- DML executes => generated SQL is positional (named ':name' SQL would fail to bind) -----

    @Test
    public void testInsertEntityGeneratesPositionalSql() throws Exception {
        int id = uid();
        // Succeeds only if the generated SQL uses positional '?' bound via setPositionalParameters.
        assertNotNull(executor.insert(entity(id, "Test")));
        assertTrue(executor.exists(TestEntity.class, id));
    }

    @Test
    public void testInsertClassPropsGeneratesPositionalSql() throws Exception {
        int id = uid();
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("id", id);
        props.put("name", "Test");
        assertNotNull(executor.insert(TestEntity.class, props));
        assertTrue(executor.exists(TestEntity.class, id));
    }

    @Test
    public void testUpdateEntityWithKeysGeneratesPositionalSql() throws Exception {
        int id = uid();
        executor.insert(entity(id, "old"));
        assertNotNull(executor.update(entity(id, "UpdatedTest"), N.asSet("id")));
        assertEquals("UpdatedTest", executor.queryForSingleValue(TestEntity.class, String.class, "name", Filters.eq("id", id)).orElse(null));
    }

    @Test
    public void testUpdateClassPropsConditionGeneratesPositionalSql() throws Exception {
        int id = uid();
        executor.insert(entity(id, "old"));
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("name", "UpdatedTest");
        assertNotNull(executor.update(TestEntity.class, props, Filters.eq("id", id)));
        assertEquals("UpdatedTest", executor.queryForSingleValue(TestEntity.class, String.class, "name", Filters.eq("id", id)).orElse(null));
    }

    @Test
    public void testDeleteClassConditionGeneratesPositionalSql() throws Exception {
        int id = uid();
        executor.insert(entity(id, "x"));
        assertNotNull(executor.delete(TestEntity.class, Filters.eq("id", id)));
        assertFalse(executor.exists(TestEntity.class, id));
    }

    // ----- exists -----

    @Test
    public void testExistsWithClassAndIds() throws Exception {
        int id = uid();
        executor.insert(entity(id, "x"));
        assertTrue(executor.exists(TestEntity.class, id));
    }

    @Test
    public void testExistsWithClassAndCondition() throws Exception {
        assertFalse(executor.exists(TestEntity.class, Filters.eq("id", -424_242)));
    }

    @Test
    public void testExists_NullWhereClause_NoRows() throws Exception {
        ddl("DELETE FROM " + TEST_TABLE + " WHERE true");
        assertFalse(executor.exists(TestEntity.class, (Condition) null));
    }

    @Test
    public void testExists_NullWhereClause_HasRows() throws Exception {
        executor.insert(entity(uid(), "x"));
        assertTrue(executor.exists(TestEntity.class, (Condition) null));
    }

    // ----- queryForSingleValue -----

    @Test
    public void testqueryForSingleValueWithTargetClass() throws Exception {
        int id = uid();
        executor.insert(entity(id, "TestName"));

        Nullable<String> result = executor.queryForSingleValue(TestEntity.class, String.class, "name", Filters.eq("id", id));
        assertTrue(result.isPresent());
        assertEquals("TestName", result.get());
    }

    @Test
    public void testqueryForSingleValueWithQuery() throws Exception {
        int id = uid();
        executor.insert(entity(id, "x"));

        Nullable<Integer> result = executor.queryForSingleValue(Integer.class, "SELECT COUNT(*) FROM " + TEST_TABLE + " WHERE id = ?", id);
        assertTrue(result.isPresent());
        assertEquals(1, result.get());
    }

    @Test
    public void testqueryForSingleValueWithEmptyResult() throws Exception {
        Nullable<String> result = executor.queryForSingleValue(String.class, "SELECT name FROM " + TEST_TABLE + " WHERE id = ?", -999);
        assertFalse(result.isPresent());
    }

    @Test
    public void testQueryForSingleValue_WithCondition_NullValue() throws Exception {
        int id = uid();
        executor.insert(entity(id, null)); // row exists but name is null

        Nullable<String> result = executor.queryForSingleValue(TestEntity.class, String.class, "name", Filters.eq("id", id));
        assertTrue(result.isPresent());
        assertNull(result.orElse(null));
    }

    @Test
    public void testQueryForSingleValue_ByQuery_NoRows() throws Exception {
        Nullable<Long> result = executor.queryForSingleValue(Long.class, "SELECT id FROM " + TEST_TABLE + " WHERE id = ?", -321);
        assertFalse(result.isPresent());
    }

    @Test
    public void testQueryForSingleValueWithDmlSql() throws Exception {
        // queryForSingleValue over a DML statement: there are no value rows -> empty (must not NoSuchElement).
        Nullable<String> result = executor.queryForSingleValue(String.class, "UPDATE " + TEST_TABLE + " SET name = ? WHERE id < ?", "x", -100_000);
        assertFalse(result.isPresent());
    }

    // ----- query / list -----

    @Test
    public void testQueryWithClassAndCondition() throws Exception {
        int id = uid();
        executor.insert(entity(id, "Name1"));

        Dataset dataset = executor.query(TestEntity.class, Filters.eq("id", id));
        assertNotNull(dataset);
        assertEquals(1, dataset.size());
    }

    @Test
    public void testQueryWithClassSelectPropsAndCondition() throws Exception {
        int id = uid();
        executor.insert(entity(id, "Name1"));

        Collection<String> selectProps = Arrays.asList("name");
        Dataset dataset = executor.query(TestEntity.class, selectProps, Filters.eq("id", id));
        assertNotNull(dataset);
        assertEquals(1, dataset.size());
        assertEquals(1, dataset.columnCount());
    }

    @Test
    public void testQueryWithClassQueryAndParameters() throws Exception {
        Dataset dataset = executor.query(TestEntity.class, "SELECT * FROM " + TEST_TABLE + " WHERE id = ?", -1);
        assertNotNull(dataset);
        assertEquals(0, dataset.size());
    }

    @Test
    public void testQuery_NullCondition() throws Exception {
        Dataset ds = executor.query(TestEntity.class, (Condition) null);
        assertNotNull(ds);
    }

    @Test
    public void testListWithClassAndCondition() throws Exception {
        int id = uid();
        executor.insert(entity(id, "Name1"));

        List<TestEntity> list = executor.list(TestEntity.class, Filters.eq("id", id));
        assertNotNull(list);
        assertEquals(1, list.size());
        assertEquals(id, list.get(0).getId());
    }

    @Test
    public void testListWithClassAndCondition_NoMatching() throws Exception {
        List<TestEntity> list = executor.list(TestEntity.class, Filters.eq("id", -1));
        assertNotNull(list);
        assertEquals(0, list.size());
    }

    @Test
    public void testListWithClassSelectPropsAndCondition() throws Exception {
        int id = uid();
        executor.insert(entity(id, "Name1"));

        Collection<String> selectProps = Arrays.asList("name");
        List<TestEntity> list = executor.list(TestEntity.class, selectProps, Filters.eq("id", id));
        assertNotNull(list);
        assertEquals(1, list.size());
    }

    @Test
    public void testListWithClassQueryAndParameters() throws Exception {
        List<TestEntity> list = executor.list(TestEntity.class, "SELECT * FROM " + TEST_TABLE + " WHERE id = ?", -1);
        assertNotNull(list);
        assertEquals(0, list.size());
    }

    // ----- naming policy (covers PSC/PAC/PLC branches via real queries) -----

    @Test
    public void testQuery_ScreamingSnakeCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(bigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        Dataset ds = exec.query(TestEntity.class, Filters.eq("id", -1));
        assertNotNull(ds);
    }

    @Test
    public void testQuery_CamelCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(bigQuery, NamingPolicy.CAMEL_CASE);
        Dataset ds = exec.query(TestEntity.class, Filters.eq("id", -1));
        assertNotNull(ds);
    }

    @Test
    public void testQuery_WithSelectProps_ScreamingSnakeCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(bigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        Dataset ds = exec.query(TestEntity.class, Arrays.asList("name"), null);
        assertNotNull(ds);
    }

    @Test
    public void testQuery_WithSelectProps_CamelCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(bigQuery, NamingPolicy.CAMEL_CASE);
        Dataset ds = exec.query(TestEntity.class, Arrays.asList("name"), null);
        assertNotNull(ds);
    }

    @Test
    public void testInsertWithMap_ScreamingSnakeCase() throws Exception {
        // Column names are case-insensitive in the emulator, so the SCREAMING_SNAKE_CASE columns (ID, NAME)
        // bind to the lower-case stored columns.
        BigQueryExecutor exec = new BigQueryExecutor(bigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        int id = uid();
        Map<String, Object> props = new HashMap<>();
        props.put("id", id);
        props.put("name", "X");

        assertNotNull(exec.insert(TestEntity.class, props));
        assertTrue(executor.exists(TestEntity.class, id));
    }

    @Test
    public void testInsertWithMap_CamelCase() throws Exception {
        // CAMEL_CASE derives the table name "testEntity" (no underscore), distinct from "test_entity";
        // seed, operate and verify all through the CAMEL executor so they target the same table.
        BigQueryExecutor exec = new BigQueryExecutor(bigQuery, NamingPolicy.CAMEL_CASE);
        int id = uid();
        Map<String, Object> props = new HashMap<>();
        props.put("id", id);
        props.put("name", "X");

        assertNotNull(exec.insert(TestEntity.class, props));
        assertTrue(exec.exists(TestEntity.class, id));
    }

    @Test
    public void testUpdateEntity_ScreamingSnakeCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(bigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        int id = uid();
        executor.insert(entity(id, "old"));
        assertNotNull(exec.update(entity(id, "X")));
    }

    @Test
    public void testUpdateEntity_CamelCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(bigQuery, NamingPolicy.CAMEL_CASE);
        int id = uid();
        exec.insert(entity(id, "old")); // CAMEL executor -> "testEntity" table
        assertNotNull(exec.update(entity(id, "X")));
    }

    @Test
    public void testUpdateWithMap_ScreamingSnakeCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(bigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        int id = uid();
        executor.insert(entity(id, "old"));
        Map<String, Object> props = new HashMap<>();
        props.put("name", "Y");
        assertNotNull(exec.update(TestEntity.class, props, Filters.eq("id", id)));
    }

    @Test
    public void testUpdateWithMap_CamelCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(bigQuery, NamingPolicy.CAMEL_CASE);
        int id = uid();
        exec.insert(entity(id, "old")); // CAMEL executor -> "testEntity" table
        Map<String, Object> props = new HashMap<>();
        props.put("name", "Y");
        assertNotNull(exec.update(TestEntity.class, props, Filters.eq("id", id)));
    }

    @Test
    public void testDelete_ScreamingSnakeCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(bigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        int id = uid();
        executor.insert(entity(id, "x"));
        assertNotNull(exec.delete(TestEntity.class, Filters.eq("id", id)));
    }

    @Test
    public void testDelete_CamelCase() throws Exception {
        BigQueryExecutor exec = new BigQueryExecutor(bigQuery, NamingPolicy.CAMEL_CASE);
        int id = uid();
        exec.insert(entity(id, "x")); // CAMEL executor -> "testEntity" table
        assertNotNull(exec.delete(TestEntity.class, Filters.eq("id", id)));
    }

    @Test
    public void testExecutorWithDifferentNamingPolicies() throws Exception {
        int id1 = uid();
        int id2 = uid();
        int id3 = uid();

        BigQueryExecutor snake = new BigQueryExecutor(bigQuery, NamingPolicy.SNAKE_CASE);
        BigQueryExecutor screaming = new BigQueryExecutor(bigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        BigQueryExecutor camel = new BigQueryExecutor(bigQuery, NamingPolicy.CAMEL_CASE);

        snake.insert(entity(id1, "a")); // -> test_entity
        screaming.insert(entity(id2, "b")); // -> TEST_ENTITY (resolves to test_entity, case-insensitive)
        camel.insert(entity(id3, "c")); // -> testEntity

        // Verify each through a matching-policy executor (each targets the table its policy derives).
        assertTrue(snake.exists(TestEntity.class, id1));
        assertTrue(screaming.exists(TestEntity.class, id2));
        assertTrue(camel.exists(TestEntity.class, id3));
    }

    // ----- stream -----

    @Test
    public void testStreamWithClassAndCondition() throws Exception {
        Stream<TestEntity> stream = executor.stream(TestEntity.class, Filters.eq("id", -1));
        assertNotNull(stream);
        assertEquals(0, stream.count());
    }

    @Test
    public void testStreamWithClassAndCondition_ReturnsRows() throws Exception {
        int id = uid();
        executor.insert(entity(id, "Eve"));

        Stream<TestEntity> stream = executor.stream(TestEntity.class, Filters.eq("id", id));
        List<TestEntity> collected = stream.toList();
        assertEquals(1, collected.size());
        assertEquals(id, collected.get(0).getId());
        assertEquals("Eve", collected.get(0).getName());
    }

    @Test
    public void testStreamWithClassSelectPropsAndCondition() throws Exception {
        Collection<String> selectProps = Arrays.asList("name");
        Stream<TestEntity> stream = executor.stream(TestEntity.class, selectProps, Filters.eq("id", -1));
        assertNotNull(stream);
        assertEquals(0, stream.count());
    }

    @Test
    public void testStreamWithClassQueryAndParameters() throws Exception {
        Stream<TestEntity> stream = executor.stream(TestEntity.class, "SELECT * FROM " + TEST_TABLE + " WHERE id = ?", -1);
        assertNotNull(stream);
        assertEquals(0, stream.count());
    }

    @Test
    public void testStreamWithClassAndQueryJobConfiguration() throws Exception {
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder("SELECT id, name FROM " + TEST_TABLE + " WHERE id = -1").build();
        Stream<TestEntity> stream = executor.stream(TestEntity.class, queryConfig);
        assertNotNull(stream);
        assertEquals(0, stream.count());
    }

    @Test
    public void testStreamWithQueryJobConfiguration() throws Exception {
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder("SELECT id, name FROM " + TEST_TABLE + " WHERE id = -1").build();
        Stream<FieldValueList> stream = executor.stream(queryConfig);
        assertNotNull(stream);
        assertEquals(0, stream.count());
    }

    @Test
    public void testStream_WithClassAndQueryConfig() throws Exception {
        int id = uid();
        executor.insert(entity(id, "John"));

        QueryJobConfiguration config = QueryJobConfiguration.newBuilder("SELECT id, name FROM " + TEST_TABLE + " WHERE id = " + id).build();
        Stream<TestEntity> stream = executor.stream(TestEntity.class, config);
        List<TestEntity> collected = stream.toList();
        assertEquals(1, collected.size());
        assertEquals(id, collected.get(0).getId());
    }

    @Test
    public void testStream_RawFieldValueList() throws Exception {
        int id = uid();
        executor.insert(entity(id, "x"));

        QueryJobConfiguration config = QueryJobConfiguration.newBuilder("SELECT id FROM " + TEST_TABLE + " WHERE id = " + id).build();
        Stream<FieldValueList> stream = executor.stream(config);
        List<FieldValueList> collected = stream.toList();
        assertEquals(1, collected.size());
    }

    // ----- stream(Class, config) readRow branches via SELECT literals -----

    @Test
    public void testStream_AsObjectArrayClass() throws Exception {
        QueryJobConfiguration cfg = QueryJobConfiguration.newBuilder("SELECT 1 AS id, 'Alice' AS name").build();
        Stream<Object[]> stream = executor.stream(Object[].class, cfg);
        List<Object[]> result = stream.toList();
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).length);
    }

    @Test
    public void testStream_AsCollectionClass() throws Exception {
        QueryJobConfiguration cfg = QueryJobConfiguration.newBuilder("SELECT 1 AS id, 'Bob' AS name").build();
        @SuppressWarnings({ "unchecked", "rawtypes" })
        Stream<List> stream = executor.stream((Class) List.class, cfg);
        List<List> result = stream.toList();
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).size());
    }

    @Test
    public void testStream_AsMapClass() throws Exception {
        QueryJobConfiguration cfg = QueryJobConfiguration.newBuilder("SELECT 1 AS id, 'Carol' AS name").build();
        @SuppressWarnings({ "unchecked", "rawtypes" })
        Stream<Map> stream = executor.stream((Class) Map.class, cfg);
        List<Map> result = stream.toList();
        assertEquals(1, result.size());
        assertNotNull(result.get(0).get("id"));
    }

    @Test
    public void testStream_AsSingleValueClass() throws Exception {
        QueryJobConfiguration cfg = QueryJobConfiguration.newBuilder("SELECT 'hello' AS v").build();
        Stream<String> stream = executor.stream(String.class, cfg);
        List<String> result = stream.toList();
        assertEquals(1, result.size());
        assertEquals("hello", result.get(0));
    }

    @Test
    public void testStream_AsSingleValueClass_MultiColumnThrows() throws Exception {
        QueryJobConfiguration cfg = QueryJobConfiguration.newBuilder("SELECT 1 AS id, 'X' AS name").build();
        Stream<String> stream = executor.stream(String.class, cfg);
        assertThrows(IllegalArgumentException.class, () -> stream.toList());
    }

    @Test
    public void testStream_AsObjectArray_NestedFieldValueList() throws Exception {
        QueryJobConfiguration cfg = QueryJobConfiguration.newBuilder("SELECT STRUCT('x' AS a) AS s").build();
        Stream<Object[]> stream = executor.stream(Object[].class, cfg);
        List<Object[]> result = stream.toList();
        assertEquals(1, result.size());
        assertTrue(result.get(0)[0] instanceof Object[]);
    }

    // ----- execute -----

    @Test
    public void testExecuteWithQueryAndParameters() throws Exception {
        int id = uid();
        executor.insert(entity(id, "x"));

        TableResult result = executor.execute("SELECT * FROM " + TEST_TABLE + " WHERE id = ?", id);
        assertNotNull(result);
        assertEquals(1, result.getTotalRows());
    }

    @Test
    public void testExecute_WithParameters() throws Exception {
        TableResult result = executor.execute("SELECT * FROM " + TEST_TABLE + " WHERE id = ?", -42);
        assertNotNull(result);
    }

    // ----- composite-key round-trips -----

    @Test
    public void testEntityToCondition_CompositeKey() throws Exception {
        CompositeKeyEntity e = new CompositeKeyEntity();
        e.setIdA("a-" + uid());
        e.setIdB("b-" + uid());
        e.setValue("v");
        // delete(entity) builds "DELETE FROM composite_key_entity WHERE id_a = ? AND id_b = ?".
        assertNotNull(executor.delete(e));
    }

    @Test
    public void testIdsToCondition_CompositeKey() throws Exception {
        assertFalse(executor.exists(CompositeKeyEntity.class, "no-such-a", "no-such-b"));
    }

    @Test
    public void testUpdate_EntityWithCompositeKey() throws Exception {
        String a = "a-" + uid();
        String b = "b-" + uid();
        ddl("INSERT INTO " + CK_TABLE + " (id_a, id_b, value) VALUES ('" + a + "', '" + b + "', 'old')");

        CompositeKeyEntity e = new CompositeKeyEntity();
        e.setIdA(a);
        e.setIdB(b);
        e.setValue("new");
        assertNotNull(executor.update(e));
        assertEquals("new", executor.queryForSingleValue(String.class, "SELECT value FROM " + CK_TABLE + " WHERE id_a = ? AND id_b = ?", a, b).orElse(null));
    }

    // ====================================================================================================
    // Entities
    // ====================================================================================================

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
