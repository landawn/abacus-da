package com.landawn.abacus.da.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
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

/**
 * No-mock counterpart of {@code BigQueryExecutorTest}: it drives {@link BigQueryExecutor} against a
 * <b>real</b> BigQuery emulator (goccy/bigquery-emulator) instead of Mockito stubs.
 *
 * <p>Tests fall into three groups:</p>
 * <ul>
 * <li><b>Pure-logic tests</b> for the static converters ({@code toEntity}/{@code toMap}/
 *     {@code buildQueryParameterValue}) built from hand-constructed {@code Field}/{@code FieldValueList},
 *     and for argument validation / unsupported naming policy (which throw before any RPC). These never used
 *     the mock and are kept as-is; they run regardless of whether the emulator is up.</li>
 * <li><b>Conversion round-trips</b> that previously stubbed {@code TableResult}. These now obtain a real
 *     {@code TableResult} from a {@code SELECT}/DML against the emulator and feed it to the static helpers.</li>
 * <li><b>Executor round-trips</b> that previously stubbed {@code BigQuery.query(...)}. These now create a real
 *     dataset/tables and run real {@code INSERT}/{@code UPDATE}/{@code DELETE}/{@code SELECT}.</li>
 * </ul>
 *
 * <h2>Running locally</h2>
 * <pre>{@code
 * docker run --rm -d --name bigquery-emulator -p 9050:9050  ghcr.io/goccy/bigquery-emulator:latest --project=test-project
 * mvn -pl abacus-da-all test -Dtest=BigQueryExecutorTest2
 * }</pre>
 *
 * <p>The Java client is pointed at the emulator with {@code setHost("http://localhost:9050")} +
 * {@link NoCredentials}. The emulator is feature-complete for this executor: it resolves unqualified table
 * names, binds positional {@code ?} parameters, treats column names case-insensitively (so
 * {@code SCREAMING_SNAKE_CASE} works against lower-case columns), and supports {@code INSERT}/{@code UPDATE}/
 * {@code DELETE} DML plus {@code STRUCT}/array results. If the emulator is unreachable, the executor/conversion
 * round-trips are <b>skipped</b> (JUnit {@link Assumptions}); the pure-logic tests still run.</p>
 *
 * <p>The three {@code InterruptedException} error-path tests from the mock suite are intentionally omitted:
 * forcing a real client to raise {@code InterruptedException} is not reliably reproducible without a mock.</p>
 */
public class BigQueryExecutorTest2 extends TestBase {

    private static final String PROJECT = "test-project";
    private static final String ENDPOINT = "http://localhost:9050";
    private static final String DATASET = "bq_test";
    private static final String TEST_TABLE = DATASET + ".test_entity"; // SNAKE_CASE / SCREAMING_SNAKE_CASE (case-insensitive) of TestEntity
    private static final String CAMEL_TABLE = DATASET + ".testEntity"; // CAMEL_CASE of TestEntity (no underscore -> distinct table)
    private static final String CK_TABLE = DATASET + ".composite_key_entity";

    private static BigQuery bigQuery;
    private static BigQueryExecutor executor;

    private static boolean available;
    private static String unavailableReason;

    private static final AtomicInteger SEQ = new AtomicInteger(100_000);

    private static int uid() {
        return SEQ.incrementAndGet();
    }

    @BeforeAll
    public static void setUpClass() {
        // Building the client does not connect; the first query does.
        bigQuery = BigQueryOptions.newBuilder().setProjectId(PROJECT).setHost(ENDPOINT).setCredentials(NoCredentials.getInstance()).build().getService();
        executor = new BigQueryExecutor(bigQuery);

        try {
            try {
                bigQuery.create(DatasetInfo.newBuilder(DATASET).build());
            } catch (final Exception ignore) {
                // dataset already exists
            }
            createTable("CREATE TABLE " + TEST_TABLE + " (id INT64, name STRING)");
            createTable("CREATE TABLE " + CAMEL_TABLE + " (id INT64, name STRING)");
            createTable("CREATE TABLE " + CK_TABLE + " (id_a STRING, id_b STRING, value STRING)");
            // Start each run from a clean slate (the emulator lacks CREATE TABLE IF NOT EXISTS).
            ddl("DELETE FROM " + TEST_TABLE + " WHERE true");
            ddl("DELETE FROM " + CAMEL_TABLE + " WHERE true");
            ddl("DELETE FROM " + CK_TABLE + " WHERE true");
            available = true;
        } catch (final Throwable t) {
            available = false;
            unavailableReason = t.toString();
            System.err.println("[BigQueryExecutorTest2] BigQuery emulator unavailable at " + ENDPOINT + " - DB-backed tests will be skipped: " + t);
        }
    }

    @AfterAll
    public static void tearDownClass() {
        if (bigQuery != null && available) {
            try {
                ddl("DROP TABLE " + TEST_TABLE);
                ddl("DROP TABLE " + CAMEL_TABLE);
                ddl("DROP TABLE " + CK_TABLE);
            } catch (final Throwable ignore) {
                // best-effort cleanup
            }
        }
    }

    private static void assumeAvailable() {
        Assumptions.assumeTrue(available, "BigQuery emulator not reachable at " + ENDPOINT + " (" + unavailableReason + ")");
    }

    private static TableResult ddl(final String sql) throws Exception {
        return bigQuery.query(QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build());
    }

    private static void createTable(final String ddl) throws Exception {
        try {
            ddl(ddl);
        } catch (final Exception e) {
            if (!String.valueOf(e.getMessage()).contains("already")) {
                throw e;
            }
        }
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
    // Pure-logic tests (no emulator required) - carried over unchanged from the mock suite
    // ====================================================================================================

    @Test
    public void testConstructorWithBigQuery() {
        BigQueryExecutor exec = new BigQueryExecutor(bigQuery);
        assertNotNull(exec);
        assertEquals(bigQuery, exec.bigQuery());
    }

    @Test
    public void testConstructorWithBigQueryAndNamingPolicy() {
        BigQueryExecutor exec = new BigQueryExecutor(bigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        assertNotNull(exec);
        assertEquals(bigQuery, exec.bigQuery());
    }

    @Test
    public void testBigQuery() {
        assertEquals(bigQuery, executor.bigQuery());
    }

    @Test
    public void testBigQuery_ReturnsSameInstance() {
        BigQueryExecutor exec = new BigQueryExecutor(bigQuery);
        assertSame(bigQuery, exec.bigQuery());
    }

    @Test
    public void testToEntityWithSchemaFieldValueListAndClass() {
        Field field1 = Field.of("id", StandardSQLTypeName.INT64);
        Field field2 = Field.of("name", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(field1, field2);
        com.google.cloud.bigquery.Schema schema = com.google.cloud.bigquery.Schema.of(fields);

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
        com.google.cloud.bigquery.Schema schema = com.google.cloud.bigquery.Schema.of(fields);

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
    public void testToEntityWithInvalidEntityClass() {
        Field field = Field.of("value", StandardSQLTypeName.STRING);
        FieldList fields = FieldList.of(field);

        FieldValueList fieldValueList = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "Value")), fields);

        assertThrows(IllegalArgumentException.class, () -> {
            BigQueryExecutor.toEntity(fields, fieldValueList, String.class);
        });
    }

    @Test
    public void testToMap_WithLinkedHashMapSupplier() {
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
    public void testToMap_UnwrapsRepeatedFieldValues() {
        final Field repeated = Field.newBuilder("tags", StandardSQLTypeName.STRING).setMode(Field.Mode.REPEATED).build();
        final FieldList fields = FieldList.of(repeated);
        final FieldValueList row = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.REPEATED,
                Arrays.asList(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "a"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "b")))), fields);

        final Map<String, Object> map = BigQueryExecutor.toMap(fields, row, com.landawn.abacus.util.IntFunctions.ofLinkedHashMap());

        assertEquals(Arrays.asList("a", "b"), map.get("tags"));
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

    // ----- buildQueryParameterValue (pure) -----

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
    public void testBuildQueryParameterValueWithSqlTypes() {
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

    @Test
    public void testBuildQueryParameterValueWithGoogleDate() {
        Object[] params = new Object[] { com.google.cloud.Date.fromYearMonthDay(2024, 1, 1) };
        List<QueryParameterValue> values = BigQueryExecutor.buildQueryParameterValue(params);
        assertEquals(1, values.size());
    }

    @Test
    public void testBuildQueryParameterValue_NullParametersArray() {
        List<QueryParameterValue> values = BigQueryExecutor.buildQueryParameterValue((Object[]) null);
        assertNotNull(values);
        assertEquals(0, values.size());
    }

    // ----- Constructor / validation (throw before any RPC) -----

    @Test
    public void testConstructor_NullBigQuery() {
        assertThrows(IllegalArgumentException.class, () -> new BigQueryExecutor(null));
    }

    @Test
    public void testConstructor_NullNamingPolicy() {
        assertThrows(IllegalArgumentException.class, () -> new BigQueryExecutor(bigQuery, null));
    }

    @Test
    public void testConstructor_NullBigQueryWithNamingPolicy() {
        assertThrows(IllegalArgumentException.class, () -> new BigQueryExecutor(null, NamingPolicy.SNAKE_CASE));
    }

    // An unsupported naming policy is now rejected eagerly at construction (fail-fast contract),
    // so no INSERT/UPDATE/DELETE/query call is ever reached.

    @Test
    public void testUnsupportedNamingPolicy() {
        assertThrows(IllegalArgumentException.class, () -> new BigQueryExecutor(bigQuery, NamingPolicy.NO_CHANGE));
    }

    @Test
    public void testInsertWithMap_UnsupportedNamingPolicy() {
        assertThrows(IllegalArgumentException.class, () -> new BigQueryExecutor(bigQuery, NamingPolicy.NO_CHANGE));
    }

    @Test
    public void testUpdateWithMap_UnsupportedNamingPolicy() {
        assertThrows(IllegalArgumentException.class, () -> new BigQueryExecutor(bigQuery, NamingPolicy.NO_CHANGE));
    }

    @Test
    public void testDelete_UnsupportedNamingPolicy() {
        assertThrows(IllegalArgumentException.class, () -> new BigQueryExecutor(bigQuery, NamingPolicy.NO_CHANGE));
    }

    @Test
    public void testQuery_UnsupportedNamingPolicy() {
        assertThrows(IllegalArgumentException.class, () -> new BigQueryExecutor(bigQuery, NamingPolicy.NO_CHANGE));
    }

    @Test
    public void testUpdateWithEmptyPrimaryKeyNames() {
        TestEntity entity = new TestEntity();
        entity.setId(1);
        Set<String> emptyKeyNames = N.toSet();
        assertThrows(IllegalArgumentException.class, () -> executor.update(entity, emptyKeyNames));
    }

    @Test
    public void testUpdateWithEmptyProps() {
        Map<String, Object> emptyProps = new HashMap<>();
        assertThrows(IllegalArgumentException.class, () -> executor.update(TestEntity.class, emptyProps, Filters.eq("id", 1)));
    }

    @Test
    public void testDelete_WithIds_MismatchCount() {
        assertThrows(IllegalArgumentException.class, () -> executor.delete(TestEntity.class, 1, 2));
    }

    @Test
    public void testDelete_WithIds_EmptyIds() {
        assertThrows(IllegalArgumentException.class, () -> executor.delete(TestEntity.class, new Object[0]));
    }

    @Test
    public void testDelete_WithIds_NullIdValue() {
        assertThrows(IllegalArgumentException.class, () -> executor.delete(TestEntity.class, (Object[]) null));
    }

    @Test
    public void testDelete_EntityNoKeyValue() {
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

    @Test
    public void testEntityToCondition_NullEntityThrows() {
        assertThrows(NullPointerException.class, () -> executor.delete((Object) null));
    }

    @Test
    public void testIdsToCondition_EmptyIdsThrows() {
        assertThrows(IllegalArgumentException.class, () -> executor.exists(TestEntity.class, new Object[0]));
    }

    @Test
    public void testIdsToCondition_TooManyIdsThrows() {
        assertThrows(IllegalArgumentException.class, () -> executor.exists(TestEntity.class, 1, 2, 3));
    }

    @Test
    public void testEntityToCondition_CompositeKey_NullValueThrows() {
        CompositeKeyEntity e = new CompositeKeyEntity();
        e.setIdA("1");
        // idB is null
        e.setValue("v");
        assertThrows(IllegalArgumentException.class, () -> executor.delete(e));
    }

    @Test
    public void testIdsToCondition_CompositeKeyMismatchThrows() {
        assertThrows(IllegalArgumentException.class, () -> executor.exists(CompositeKeyEntity.class, "1"));
    }

    // ====================================================================================================
    // Conversion round-trips: real TableResult fed to the static helpers
    // ====================================================================================================

    @Test
    public void testToListWithTableResultAndClass() throws Exception {
        assumeAvailable();
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
        assumeAvailable();
        TableResult tr = q("SELECT id, name FROM " + TEST_TABLE + " WHERE id = -1");

        List<TestEntity> list = BigQueryExecutor.toList(tr, TestEntity.class);

        assertNotNull(list);
        assertEquals(0, list.size());
    }

    @Test
    public void testExtractDataWithTableResultAndClass() throws Exception {
        assumeAvailable();
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
        assumeAvailable();
        TableResult tr = q("SELECT id, name FROM " + TEST_TABLE + " WHERE id = -1");

        Dataset dataset = BigQueryExecutor.extractData(tr, TestEntity.class);

        assertNotNull(dataset);
        assertEquals(0, dataset.size());
    }

    @Test
    public void testExtractDataWithMapClass() throws Exception {
        assumeAvailable();
        TableResult tr = q("SELECT 1 AS id, 'Name1' AS name");

        Dataset dataset = BigQueryExecutor.extractData(tr, Map.class);

        assertNotNull(dataset);
        assertEquals(2, dataset.columnCount());
        assertEquals(1, dataset.size());
    }

    @Test
    public void testExtractData_AsMapClass() throws Exception {
        assumeAvailable();
        TableResult tr = q("SELECT 1 AS id, 'x' AS name");

        Dataset ds = BigQueryExecutor.extractData(tr, Map.class);
        assertNotNull(ds);
        assertEquals(1, ds.size());
        assertTrue(ds.containsColumn("id"));
    }

    @Test
    public void testExtractData_NullTargetClass() throws Exception {
        assumeAvailable();
        TableResult tr = q("SELECT 55 AS id");

        Dataset ds = BigQueryExecutor.extractData(tr, null);
        assertNotNull(ds);
        assertEquals(1, ds.size());
    }

    @Test
    public void testToListWithDifferentRowClasses() throws Exception {
        assumeAvailable();
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
        assumeAvailable();
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
        assumeAvailable();
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
        assumeAvailable();
        TableResult tr = q("SELECT 1 AS id, 'n' AS name");

        @SuppressWarnings({ "unchecked", "rawtypes" })
        List<List> result = BigQueryExecutor.toList(tr, (Class) List.class);
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).size());
    }

    @Test
    public void testToList_AsMapClass() throws Exception {
        assumeAvailable();
        TableResult tr = q("SELECT 42 AS id");

        @SuppressWarnings({ "unchecked", "rawtypes" })
        List<Map> result = BigQueryExecutor.toList(tr, (Class) Map.class);
        assertEquals(1, result.size());
        assertEquals("42", result.get(0).get("id"));
    }

    @Test
    public void testToList_AsSingleValueClass() throws Exception {
        assumeAvailable();
        TableResult tr = q("SELECT 100 AS id UNION ALL SELECT 200 AS id ORDER BY id");

        List<Long> result = BigQueryExecutor.toList(tr, Long.class);
        assertEquals(2, result.size());
        assertEquals(Long.valueOf(100L), result.get(0));
        assertEquals(Long.valueOf(200L), result.get(1));
    }

    @Test
    public void testToList_WithIntegerClass() throws Exception {
        assumeAvailable();
        TableResult tr = q("SELECT 1 AS c UNION ALL SELECT 2 AS c ORDER BY c");

        List<Integer> values = BigQueryExecutor.toList(tr, Integer.class);
        assertEquals(2, values.size());
        assertEquals(Integer.valueOf(1), values.get(0));
        assertEquals(Integer.valueOf(2), values.get(1));
    }

    @Test
    public void testToList_ReadRow_WithFieldValueList_AsObjectArray() throws Exception {
        assumeAvailable();
        TableResult tr = q("SELECT STRUCT('v' AS inner_val) AS outer_val");

        List<Object[]> result = BigQueryExecutor.toList(tr, Object[].class);
        assertEquals(1, result.size());
        // The nested struct should be expanded to its own array.
        assertTrue(result.get(0)[0] instanceof Object[]);
    }

    @Test
    public void testExtractData_NestedFieldValueListInColumn() throws Exception {
        assumeAvailable();
        TableResult tr = q("SELECT STRUCT('x' AS a) AS nested");

        Dataset ds = BigQueryExecutor.extractData(tr, Map.class);
        assertNotNull(ds);
        assertEquals(1, ds.size());
    }

    // ----- Null-schema (DML-result) shape: a real DML TableResult has no data rows -----

    @Test
    public void testToListWithDmlResult() throws Exception {
        assumeAvailable();
        TableResult dml = executor.delete(TestEntity.class, Filters.eq("id", -987_654));
        List<TestEntity> list = BigQueryExecutor.toList(dml, TestEntity.class);
        assertNotNull(list);
        assertTrue(list.isEmpty());
    }

    @Test
    public void testExtractDataWithDmlResult() throws Exception {
        assumeAvailable();
        TableResult dml = executor.delete(TestEntity.class, Filters.eq("id", -987_653));
        Dataset ds = BigQueryExecutor.extractData(dml, TestEntity.class);
        assertNotNull(ds);
        assertEquals(0, ds.size());
    }

    @Test
    public void testExtractDataWithDmlResult_MapClass() throws Exception {
        assumeAvailable();
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
        assumeAvailable();
        int id = uid();
        TableResult result = executor.insert(entity(id, "Test"));
        assertNotNull(result);
        assertTrue(executor.exists(TestEntity.class, id));
    }

    @Test
    public void testInsertWithClassAndProps() throws Exception {
        assumeAvailable();
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
        assumeAvailable();
        int id = uid();
        executor.insert(entity(id, "old"));

        TableResult result = executor.update(entity(id, "UpdatedTest"));
        assertNotNull(result);
        assertEquals("UpdatedTest", executor.queryForSingleValue(TestEntity.class, String.class, "name", Filters.eq("id", id)).orElse(null));
    }

    @Test
    public void testUpdateEntityWithPrimaryKeyNames() throws Exception {
        assumeAvailable();
        int id = uid();
        executor.insert(entity(id, "old"));

        TableResult result = executor.update(entity(id, "UpdatedTest"), N.asSet("id"));
        assertNotNull(result);
        assertEquals("UpdatedTest", executor.queryForSingleValue(TestEntity.class, String.class, "name", Filters.eq("id", id)).orElse(null));
    }

    @Test
    public void testUpdateWithClassPropsAndCondition() throws Exception {
        assumeAvailable();
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
        assumeAvailable();
        int id = uid();
        executor.insert(entity(id, "x"));

        TableResult result = executor.delete(entity(id, "x"));
        assertNotNull(result);
        assertFalse(executor.exists(TestEntity.class, id));
    }

    @Test
    public void testDeleteWithClassAndIds() throws Exception {
        assumeAvailable();
        int id = uid();
        executor.insert(entity(id, "x"));

        TableResult result = executor.delete(TestEntity.class, id);
        assertNotNull(result);
        assertFalse(executor.exists(TestEntity.class, id));
    }

    @Test
    public void testDeleteWithClassAndCondition() throws Exception {
        assumeAvailable();
        int id = uid();
        executor.insert(entity(id, "x"));

        TableResult result = executor.delete(TestEntity.class, Filters.eq("id", id));
        assertNotNull(result);
        assertFalse(executor.exists(TestEntity.class, id));
    }

    // ----- DML executes => generated SQL is positional (named ':name' SQL would fail to bind) -----

    @Test
    public void testInsertEntityGeneratesPositionalSql() throws Exception {
        assumeAvailable();
        int id = uid();
        // Succeeds only if the generated SQL uses positional '?' bound via setPositionalParameters.
        assertNotNull(executor.insert(entity(id, "Test")));
        assertTrue(executor.exists(TestEntity.class, id));
    }

    @Test
    public void testInsertClassPropsGeneratesPositionalSql() throws Exception {
        assumeAvailable();
        int id = uid();
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("id", id);
        props.put("name", "Test");
        assertNotNull(executor.insert(TestEntity.class, props));
        assertTrue(executor.exists(TestEntity.class, id));
    }

    @Test
    public void testUpdateEntityWithKeysGeneratesPositionalSql() throws Exception {
        assumeAvailable();
        int id = uid();
        executor.insert(entity(id, "old"));
        assertNotNull(executor.update(entity(id, "UpdatedTest"), N.asSet("id")));
        assertEquals("UpdatedTest", executor.queryForSingleValue(TestEntity.class, String.class, "name", Filters.eq("id", id)).orElse(null));
    }

    @Test
    public void testUpdateClassPropsConditionGeneratesPositionalSql() throws Exception {
        assumeAvailable();
        int id = uid();
        executor.insert(entity(id, "old"));
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("name", "UpdatedTest");
        assertNotNull(executor.update(TestEntity.class, props, Filters.eq("id", id)));
        assertEquals("UpdatedTest", executor.queryForSingleValue(TestEntity.class, String.class, "name", Filters.eq("id", id)).orElse(null));
    }

    @Test
    public void testDeleteClassConditionGeneratesPositionalSql() throws Exception {
        assumeAvailable();
        int id = uid();
        executor.insert(entity(id, "x"));
        assertNotNull(executor.delete(TestEntity.class, Filters.eq("id", id)));
        assertFalse(executor.exists(TestEntity.class, id));
    }

    // ----- exists -----

    @Test
    public void testExistsWithClassAndIds() throws Exception {
        assumeAvailable();
        int id = uid();
        executor.insert(entity(id, "x"));
        assertTrue(executor.exists(TestEntity.class, id));
    }

    @Test
    public void testExistsWithClassAndCondition() throws Exception {
        assumeAvailable();
        assertFalse(executor.exists(TestEntity.class, Filters.eq("id", -424_242)));
    }

    @Test
    public void testExists_NullWhereClause_NoRows() throws Exception {
        assumeAvailable();
        ddl("DELETE FROM " + TEST_TABLE + " WHERE true");
        assertFalse(executor.exists(TestEntity.class, (Condition) null));
    }

    @Test
    public void testExists_NullWhereClause_HasRows() throws Exception {
        assumeAvailable();
        executor.insert(entity(uid(), "x"));
        assertTrue(executor.exists(TestEntity.class, (Condition) null));
    }

    // ----- queryForSingleValue -----

    @Test
    public void testqueryForSingleValueWithTargetClass() throws Exception {
        assumeAvailable();
        int id = uid();
        executor.insert(entity(id, "TestName"));

        Nullable<String> result = executor.queryForSingleValue(TestEntity.class, String.class, "name", Filters.eq("id", id));
        assertTrue(result.isPresent());
        assertEquals("TestName", result.get());
    }

    @Test
    public void testqueryForSingleValueWithQuery() throws Exception {
        assumeAvailable();
        int id = uid();
        executor.insert(entity(id, "x"));

        Nullable<Integer> result = executor.queryForSingleValue(Integer.class, "SELECT COUNT(*) FROM " + TEST_TABLE + " WHERE id = ?", id);
        assertTrue(result.isPresent());
        assertEquals(1, result.get());
    }

    @Test
    public void testqueryForSingleValueWithEmptyResult() throws Exception {
        assumeAvailable();
        Nullable<String> result = executor.queryForSingleValue(String.class, "SELECT name FROM " + TEST_TABLE + " WHERE id = ?", -999);
        assertFalse(result.isPresent());
    }

    @Test
    public void testQueryForSingleValue_WithCondition_NullValue() throws Exception {
        assumeAvailable();
        int id = uid();
        executor.insert(entity(id, null)); // row exists but name is null

        Nullable<String> result = executor.queryForSingleValue(TestEntity.class, String.class, "name", Filters.eq("id", id));
        assertTrue(result.isPresent());
        assertNull(result.orElse(null));
    }

    @Test
    public void testQueryForSingleValue_ByQuery_NoRows() throws Exception {
        assumeAvailable();
        Nullable<Long> result = executor.queryForSingleValue(Long.class, "SELECT id FROM " + TEST_TABLE + " WHERE id = ?", -321);
        assertFalse(result.isPresent());
    }

    @Test
    public void testQueryForSingleValueWithDmlSql() throws Exception {
        assumeAvailable();
        // queryForSingleValue over a DML statement: there are no value rows -> empty (must not NoSuchElement).
        Nullable<String> result = executor.queryForSingleValue(String.class, "UPDATE " + TEST_TABLE + " SET name = ? WHERE id < ?", "x", -100_000);
        assertFalse(result.isPresent());
    }

    // ----- query / list -----

    @Test
    public void testQueryWithClassAndCondition() throws Exception {
        assumeAvailable();
        int id = uid();
        executor.insert(entity(id, "Name1"));

        Dataset dataset = executor.query(TestEntity.class, Filters.eq("id", id));
        assertNotNull(dataset);
        assertEquals(1, dataset.size());
    }

    @Test
    public void testQueryWithClassSelectPropsAndCondition() throws Exception {
        assumeAvailable();
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
        assumeAvailable();
        Dataset dataset = executor.query(TestEntity.class, "SELECT * FROM " + TEST_TABLE + " WHERE id = ?", -1);
        assertNotNull(dataset);
        assertEquals(0, dataset.size());
    }

    @Test
    public void testQuery_NullCondition() throws Exception {
        assumeAvailable();
        Dataset ds = executor.query(TestEntity.class, (Condition) null);
        assertNotNull(ds);
    }

    @Test
    public void testListWithClassAndCondition() throws Exception {
        assumeAvailable();
        int id = uid();
        executor.insert(entity(id, "Name1"));

        List<TestEntity> list = executor.list(TestEntity.class, Filters.eq("id", id));
        assertNotNull(list);
        assertEquals(1, list.size());
        assertEquals(id, list.get(0).getId());
    }

    @Test
    public void testListWithClassAndCondition_NoMatching() throws Exception {
        assumeAvailable();
        List<TestEntity> list = executor.list(TestEntity.class, Filters.eq("id", -1));
        assertNotNull(list);
        assertEquals(0, list.size());
    }

    @Test
    public void testListWithClassSelectPropsAndCondition() throws Exception {
        assumeAvailable();
        int id = uid();
        executor.insert(entity(id, "Name1"));

        Collection<String> selectProps = Arrays.asList("name");
        List<TestEntity> list = executor.list(TestEntity.class, selectProps, Filters.eq("id", id));
        assertNotNull(list);
        assertEquals(1, list.size());
    }

    @Test
    public void testListWithClassQueryAndParameters() throws Exception {
        assumeAvailable();
        List<TestEntity> list = executor.list(TestEntity.class, "SELECT * FROM " + TEST_TABLE + " WHERE id = ?", -1);
        assertNotNull(list);
        assertEquals(0, list.size());
    }

    // ----- naming policy (covers PSC/PAC/PLC branches via real queries) -----

    @Test
    public void testQuery_ScreamingSnakeCase() throws Exception {
        assumeAvailable();
        BigQueryExecutor exec = new BigQueryExecutor(bigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        Dataset ds = exec.query(TestEntity.class, Filters.eq("id", -1));
        assertNotNull(ds);
    }

    @Test
    public void testQuery_CamelCase() throws Exception {
        assumeAvailable();
        BigQueryExecutor exec = new BigQueryExecutor(bigQuery, NamingPolicy.CAMEL_CASE);
        Dataset ds = exec.query(TestEntity.class, Filters.eq("id", -1));
        assertNotNull(ds);
    }

    @Test
    public void testQuery_WithSelectProps_ScreamingSnakeCase() throws Exception {
        assumeAvailable();
        BigQueryExecutor exec = new BigQueryExecutor(bigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        Dataset ds = exec.query(TestEntity.class, Arrays.asList("name"), null);
        assertNotNull(ds);
    }

    @Test
    public void testQuery_WithSelectProps_CamelCase() throws Exception {
        assumeAvailable();
        BigQueryExecutor exec = new BigQueryExecutor(bigQuery, NamingPolicy.CAMEL_CASE);
        Dataset ds = exec.query(TestEntity.class, Arrays.asList("name"), null);
        assertNotNull(ds);
    }

    @Test
    public void testInsertWithMap_ScreamingSnakeCase() throws Exception {
        assumeAvailable();
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
        assumeAvailable();
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
        assumeAvailable();
        BigQueryExecutor exec = new BigQueryExecutor(bigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        int id = uid();
        executor.insert(entity(id, "old"));
        assertNotNull(exec.update(entity(id, "X")));
    }

    @Test
    public void testUpdateEntity_CamelCase() throws Exception {
        assumeAvailable();
        BigQueryExecutor exec = new BigQueryExecutor(bigQuery, NamingPolicy.CAMEL_CASE);
        int id = uid();
        exec.insert(entity(id, "old")); // CAMEL executor -> "testEntity" table
        assertNotNull(exec.update(entity(id, "X")));
    }

    @Test
    public void testUpdateWithMap_ScreamingSnakeCase() throws Exception {
        assumeAvailable();
        BigQueryExecutor exec = new BigQueryExecutor(bigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        int id = uid();
        executor.insert(entity(id, "old"));
        Map<String, Object> props = new HashMap<>();
        props.put("name", "Y");
        assertNotNull(exec.update(TestEntity.class, props, Filters.eq("id", id)));
    }

    @Test
    public void testUpdateWithMap_CamelCase() throws Exception {
        assumeAvailable();
        BigQueryExecutor exec = new BigQueryExecutor(bigQuery, NamingPolicy.CAMEL_CASE);
        int id = uid();
        exec.insert(entity(id, "old")); // CAMEL executor -> "testEntity" table
        Map<String, Object> props = new HashMap<>();
        props.put("name", "Y");
        assertNotNull(exec.update(TestEntity.class, props, Filters.eq("id", id)));
    }

    @Test
    public void testDelete_ScreamingSnakeCase() throws Exception {
        assumeAvailable();
        BigQueryExecutor exec = new BigQueryExecutor(bigQuery, NamingPolicy.SCREAMING_SNAKE_CASE);
        int id = uid();
        executor.insert(entity(id, "x"));
        assertNotNull(exec.delete(TestEntity.class, Filters.eq("id", id)));
    }

    @Test
    public void testDelete_CamelCase() throws Exception {
        assumeAvailable();
        BigQueryExecutor exec = new BigQueryExecutor(bigQuery, NamingPolicy.CAMEL_CASE);
        int id = uid();
        exec.insert(entity(id, "x")); // CAMEL executor -> "testEntity" table
        assertNotNull(exec.delete(TestEntity.class, Filters.eq("id", id)));
    }

    @Test
    public void testExecutorWithDifferentNamingPolicies() throws Exception {
        assumeAvailable();
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
        assumeAvailable();
        Stream<TestEntity> stream = executor.stream(TestEntity.class, Filters.eq("id", -1));
        assertNotNull(stream);
        assertEquals(0, stream.count());
    }

    @Test
    public void testStreamWithClassAndCondition_ReturnsRows() throws Exception {
        assumeAvailable();
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
        assumeAvailable();
        Collection<String> selectProps = Arrays.asList("name");
        Stream<TestEntity> stream = executor.stream(TestEntity.class, selectProps, Filters.eq("id", -1));
        assertNotNull(stream);
        assertEquals(0, stream.count());
    }

    @Test
    public void testStreamWithClassQueryAndParameters() throws Exception {
        assumeAvailable();
        Stream<TestEntity> stream = executor.stream(TestEntity.class, "SELECT * FROM " + TEST_TABLE + " WHERE id = ?", -1);
        assertNotNull(stream);
        assertEquals(0, stream.count());
    }

    @Test
    public void testStreamWithClassAndQueryJobConfiguration() throws Exception {
        assumeAvailable();
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder("SELECT id, name FROM " + TEST_TABLE + " WHERE id = -1").build();
        Stream<TestEntity> stream = executor.stream(TestEntity.class, queryConfig);
        assertNotNull(stream);
        assertEquals(0, stream.count());
    }

    @Test
    public void testStreamWithQueryJobConfiguration() throws Exception {
        assumeAvailable();
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder("SELECT id, name FROM " + TEST_TABLE + " WHERE id = -1").build();
        Stream<FieldValueList> stream = executor.stream(queryConfig);
        assertNotNull(stream);
        assertEquals(0, stream.count());
    }

    @Test
    public void testStream_WithClassAndQueryConfig() throws Exception {
        assumeAvailable();
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
        assumeAvailable();
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
        assumeAvailable();
        QueryJobConfiguration cfg = QueryJobConfiguration.newBuilder("SELECT 1 AS id, 'Alice' AS name").build();
        Stream<Object[]> stream = executor.stream(Object[].class, cfg);
        List<Object[]> result = stream.toList();
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).length);
    }

    @Test
    public void testStream_AsCollectionClass() throws Exception {
        assumeAvailable();
        QueryJobConfiguration cfg = QueryJobConfiguration.newBuilder("SELECT 1 AS id, 'Bob' AS name").build();
        @SuppressWarnings({ "unchecked", "rawtypes" })
        Stream<List> stream = executor.stream((Class) List.class, cfg);
        List<List> result = stream.toList();
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).size());
    }

    @Test
    public void testStream_AsMapClass() throws Exception {
        assumeAvailable();
        QueryJobConfiguration cfg = QueryJobConfiguration.newBuilder("SELECT 1 AS id, 'Carol' AS name").build();
        @SuppressWarnings({ "unchecked", "rawtypes" })
        Stream<Map> stream = executor.stream((Class) Map.class, cfg);
        List<Map> result = stream.toList();
        assertEquals(1, result.size());
        assertNotNull(result.get(0).get("id"));
    }

    @Test
    public void testStream_AsSingleValueClass() throws Exception {
        assumeAvailable();
        QueryJobConfiguration cfg = QueryJobConfiguration.newBuilder("SELECT 'hello' AS v").build();
        Stream<String> stream = executor.stream(String.class, cfg);
        List<String> result = stream.toList();
        assertEquals(1, result.size());
        assertEquals("hello", result.get(0));
    }

    @Test
    public void testStream_AsSingleValueClass_MultiColumnThrows() throws Exception {
        assumeAvailable();
        QueryJobConfiguration cfg = QueryJobConfiguration.newBuilder("SELECT 1 AS id, 'X' AS name").build();
        Stream<String> stream = executor.stream(String.class, cfg);
        assertThrows(IllegalArgumentException.class, () -> stream.toList());
    }

    @Test
    public void testStream_AsObjectArray_NestedFieldValueList() throws Exception {
        assumeAvailable();
        QueryJobConfiguration cfg = QueryJobConfiguration.newBuilder("SELECT STRUCT('x' AS a) AS s").build();
        Stream<Object[]> stream = executor.stream(Object[].class, cfg);
        List<Object[]> result = stream.toList();
        assertEquals(1, result.size());
        assertTrue(result.get(0)[0] instanceof Object[]);
    }

    // ----- execute -----

    @Test
    public void testExecuteWithQueryAndParameters() throws Exception {
        assumeAvailable();
        int id = uid();
        executor.insert(entity(id, "x"));

        TableResult result = executor.execute("SELECT * FROM " + TEST_TABLE + " WHERE id = ?", id);
        assertNotNull(result);
        assertEquals(1, result.getTotalRows());
    }

    @Test
    public void testExecute_WithParameters() throws Exception {
        assumeAvailable();
        TableResult result = executor.execute("SELECT * FROM " + TEST_TABLE + " WHERE id = ?", -42);
        assertNotNull(result);
    }

    // ----- composite-key round-trips -----

    @Test
    public void testEntityToCondition_CompositeKey() throws Exception {
        assumeAvailable();
        CompositeKeyEntity e = new CompositeKeyEntity();
        e.setIdA("a-" + uid());
        e.setIdB("b-" + uid());
        e.setValue("v");
        // delete(entity) builds "DELETE FROM composite_key_entity WHERE id_a = ? AND id_b = ?".
        assertNotNull(executor.delete(e));
    }

    @Test
    public void testIdsToCondition_CompositeKey() throws Exception {
        assumeAvailable();
        assertFalse(executor.exists(CompositeKeyEntity.class, "no-such-a", "no-such-b"));
    }

    @Test
    public void testUpdate_EntityWithCompositeKey() throws Exception {
        assumeAvailable();
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
