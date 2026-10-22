package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.exception.DuplicateResultException;
import com.landawn.abacus.query.AbstractQueryBuilder.SP;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.ImmutableList;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.stream.Stream;
import com.landawn.abacus.util.u.Nullable;
import com.landawn.abacus.util.u.Optional;
import com.landawn.abacus.util.u.OptionalBoolean;
import com.landawn.abacus.util.u.OptionalByte;
import com.landawn.abacus.util.u.OptionalChar;
import com.landawn.abacus.util.u.OptionalDouble;
import com.landawn.abacus.util.u.OptionalFloat;
import com.landawn.abacus.util.u.OptionalInt;
import com.landawn.abacus.util.u.OptionalLong;
import com.landawn.abacus.util.u.OptionalShort;

public class CassandraExecutorBaseTest extends CassandraExecutorBaseTestSupport {

    @Test
    public void testBatchDelete() {
        // Test batch delete entities
        TestEntity e1 = new TestEntity();
        e1.setId(1L);
        TestEntity e2 = new TestEntity();
        e2.setId(2L);
        List<TestEntity> entities = Arrays.asList(e1, e2);
        TestResultSet result1 = executor.batchDelete(entities);
        assertNotNull(result1);

        // Test batch delete with propNamesToDelete
        TestResultSet result2 = executor.batchDelete(entities, Arrays.asList("name"));
        assertNotNull(result2);
    }

    @Test
    public void testBatchInsert() {
        // Test batch insert entities
        List<TestEntity> entities = Arrays.asList(new TestEntity(), new TestEntity());
        TestResultSet result1 = executor.batchInsert(entities, TestBatchType.LOGGED);
        assertNotNull(result1);

        // Test batch insert with props list
        List<Map<String, Object>> propsList = new ArrayList<>();
        propsList.add(new HashMap<>());
        propsList.add(new HashMap<>());

        TestResultSet result2 = executor.batchInsert(TestEntity.class, propsList, TestBatchType.LOGGED);
        assertNotNull(result2);
    }

    @Test
    public void testBatchInsertClausesIgnoreSemicolonsInsideRawValues() {
        final Map<String, Object> first = new LinkedHashMap<>();
        first.put("id", 1);
        first.put("value", "a;b");
        final Map<String, Object> second = new LinkedHashMap<>();
        second.put("id", 2);
        second.put("value", "c USING d");

        final String cql = CqlBuilder.Dsl.SCCB.batchInsert(Arrays.asList(first, second)).into("events").usingTTL(60).ifNotExists().build().query();

        assertEquals("BEGIN BATCH INSERT INTO events (id, value) VALUES (1, 'a;b') IF NOT EXISTS USING TTL 60;"
                + " INSERT INTO events (id, value) VALUES (2, 'c USING d') IF NOT EXISTS USING TTL 60; APPLY BATCH", cql);
    }

    @Test
    public void testBatchUpdate() {
        // Test batch update entities
        List<TestEntity> entities = Arrays.asList(new TestEntity(), new TestEntity());
        TestResultSet result1 = executor.batchUpdate(entities, TestBatchType.LOGGED);
        assertNotNull(result1);

        // Test batch update with propNamesToUpdate
        TestResultSet result2 = executor.batchUpdate(entities, Arrays.asList("name"), TestBatchType.LOGGED);
        assertNotNull(result2);

        // Test batch update with props list
        List<Map<String, Object>> propsList = new ArrayList<>();
        propsList.add(new HashMap<>());
        propsList.add(new HashMap<>());

        TestResultSet result3 = executor.batchUpdate(TestEntity.class, propsList, TestBatchType.LOGGED);
        assertNotNull(result3);

        // Test batch update with query and parameters list
        List<Object[]> parametersList = Arrays.asList(new Object[] { "updated1", 1L }, new Object[] { "updated2", 2L });

        TestResultSet result4 = executor.batchUpdate("UPDATE test SET name = ? WHERE id = ?", parametersList, TestBatchType.LOGGED);
        assertNotNull(result4);
    }

    // The constructor fails fast on a naming policy the prepare* methods can't render.
    @Test
    public void testConstructor_unsupportedNamingPolicy_throwsIAE() {
        assertThrows(IllegalArgumentException.class, () -> new TestCassandraExecutor(NamingPolicy.NO_CHANGE));
    }

    @Test
    public void testCount() {
        // Test count with condition
        long result1 = executor.count(TestEntity.class, Filters.eq("status", "active"));
        assertEquals(0L, result1);

        // Test deprecated count with query
        long result2 = executor.count("SELECT COUNT(*) FROM test WHERE status = ?", "active");
        assertEquals(0L, result2);
    }

    @Test
    public void testDelete() {
        // Test delete entity
        TestEntity entity = new TestEntity();
        entity.setId(1L);

        TestResultSet result1 = executor.delete(entity);
        assertNotNull(result1);

        // Test delete with propNamesToDelete
        TestResultSet result2 = executor.delete(entity, Arrays.asList("name"));
        assertNotNull(result2);

        // Test delete by ids
        TestResultSet result3 = executor.delete(TestEntity.class, 1L);
        assertNotNull(result3);

        // Test delete with propNamesToDelete and ids
        TestResultSet result4 = executor.delete(TestEntity.class, Arrays.asList("name"), 1L);
        assertNotNull(result4);

        // Test delete with condition
        TestResultSet result5 = executor.delete(TestEntity.class, Filters.eq("id", 1L));
        assertNotNull(result5);

        // Test delete with propNamesToDelete and condition
        TestResultSet result6 = executor.delete(TestEntity.class, Arrays.asList("name"), Filters.eq("id", 1L));
        assertNotNull(result6);
    }

    @Test
    public void testDelete_nullEntity_throwsIAE() {
        assertThrows(IllegalArgumentException.class, () -> executor.delete((Object) null));
    }

    @Test
    public void testDelete_nullEntityWithPropNames_throwsIAE() {
        assertThrows(IllegalArgumentException.class, () -> executor.delete((Object) null, java.util.Arrays.asList("name")));
    }

    @Test
    public void testEntityToCondition_collection_compositeKey() {
        CompositeKeyEntity a = new CompositeKeyEntity();
        a.setUserId("u1");
        a.setSessionId("s1");
        CompositeKeyEntity b = new CompositeKeyEntity();
        b.setUserId("u2");
        b.setSessionId("s2");

        assertThrows(IllegalArgumentException.class,
                () -> TestCassandraExecutor.exposedEntityToConditionCollection(CompositeKeyEntity.class, Arrays.asList(a, b)));
    }

    @Test
    public void testEntityToCondition_collection_emptyCollection_throwsIAE() {
        assertThrows(IllegalArgumentException.class, () -> TestCassandraExecutor.exposedEntityToConditionCollection(TestEntity.class, new ArrayList<>()));
    }

    @Test
    public void testEntityToCondition_collection_nullNonFirstEntity_throwsIAE() {
        // batchDelete validates only the first element eagerly; the per-element guard inside
        // entityToCondition must still reject a null later in the collection.
        TestEntity a = new TestEntity();
        a.setId(1L);

        assertThrows(IllegalArgumentException.class, () -> TestCassandraExecutor.exposedEntityToConditionCollection(TestEntity.class, Arrays.asList(a, null)));
    }

    @Test
    public void testEntityToCondition_collection_singleKey() {
        TestEntity a = new TestEntity();
        a.setId(1L);
        TestEntity b = new TestEntity();
        b.setId(2L);
        Condition cond = TestCassandraExecutor.exposedEntityToConditionCollection(TestEntity.class, Arrays.asList(a, b));
        assertNotNull(cond);
        assertTrue(cond.toString().toLowerCase().contains("in"));
    }

    @Test
    public void testEntityToCondition_collection_wrongEntityType_throwsIAE() {
        TestEntity entity = new TestEntity();
        entity.setId(1L);
        CompositeKeyEntity wrongType = new CompositeKeyEntity();
        wrongType.setUserId("u1");
        wrongType.setSessionId("s1");

        assertThrows(IllegalArgumentException.class,
                () -> TestCassandraExecutor.exposedEntityToConditionCollection(TestEntity.class, Arrays.asList(entity, wrongType)));
    }

    @Test
    public void testEntityToCondition_compositeKey() {
        CompositeKeyEntity e = new CompositeKeyEntity();
        e.setUserId("u1");
        e.setSessionId("s1");
        Condition cond = TestCassandraExecutor.exposedEntityToCondition(e);
        assertNotNull(cond);
        assertTrue(cond.toString().toLowerCase().contains("and"));
    }

    @Test
    public void testEntityToCondition_compositeKey_missingValue_throwsIAE() {
        CompositeKeyEntity e = new CompositeKeyEntity();
        e.setUserId("u1");
        // sessionId is null -> IAE
        assertThrows(IllegalArgumentException.class, () -> TestCassandraExecutor.exposedEntityToCondition(e));
    }

    @Test
    public void testEntityToCondition_singleKey() {
        TestEntity e = new TestEntity();
        e.setId(99L);
        Condition cond = TestCassandraExecutor.exposedEntityToCondition(e);
        assertNotNull(cond);
        assertTrue(cond.toString().contains("id"));
    }

    @Test
    public void testEntityToCondition_singleKey_nullValue_throwsIAE() {
        TestEntity e = new TestEntity();
        // id is null
        assertThrows(IllegalArgumentException.class, () -> TestCassandraExecutor.exposedEntityToCondition(e));
    }

    @Test
    public void testExecute() {
        // Test execute
        TestResultSet result1 = executor.execute("SELECT * FROM test");
        assertNotNull(result1);

        TestResultSet result2 = executor.execute("SELECT * FROM test WHERE id = ?", 1L);
        assertNotNull(result2);

        Map<String, Object> params = new HashMap<>();
        params.put("id", 1L);
        TestResultSet result3 = executor.execute("SELECT * FROM test WHERE id = :id", params);
        assertNotNull(result3);

        TestStatement stmt = new TestStatement();
        TestResultSet result4 = executor.execute(stmt);
        assertNotNull(result4);
    }

    @Test
    public void testExists() {
        // Test exists by ids
        boolean result1 = executor.exists(TestEntity.class, 1L);
        assertTrue(result1);

        // Test exists by condition
        boolean result2 = executor.exists(TestEntity.class, Filters.eq("id", 1L));
        assertTrue(result2);

        // Test exists with query
        boolean result3 = executor.exists("SELECT * FROM test WHERE id = ? LIMIT 1", 1L);
        assertTrue(result3);
    }

    @Test
    public void testFindFirst() {
        // Test findFirst with condition
        Optional<TestEntity> result1 = executor.findFirst(TestEntity.class, Filters.eq("id", 1L));
        assertTrue(result1.isPresent());

        // Test findFirst with selectPropNames and condition
        Optional<TestEntity> result2 = executor.findFirst(TestEntity.class, Arrays.asList("id", "name"), Filters.eq("id", 1L));
        assertTrue(result2.isPresent());

        // Test findFirst with query
        Optional<Map<String, Object>> result3 = executor.findFirst("SELECT * FROM test WHERE id = ?", 1L);
        assertTrue(result3.isPresent());

        // Test findFirst with targetClass and query
        Optional<TestEntity> result4 = executor.findFirst(TestEntity.class, "SELECT * FROM test WHERE id = ?", 1L);
        assertTrue(result4.isPresent());
    }

    @Test
    public void testGetKeyNames_repeatedCallReturnsSameCached() {
        ImmutableList<String> first = CassandraExecutorBase.getKeyNames(TestEntity.class);
        ImmutableList<String> second = CassandraExecutorBase.getKeyNames(TestEntity.class);
        assertNotNull(first);
        assertNotNull(second);
        assertEquals(first.size(), second.size());
    }

    @Test
    public void testGetKeyNameSet_repeatedCallReturnsSameCached() {
        java.util.Set<String> first = CassandraExecutorBase.getKeyNameSet(TestEntity.class);
        java.util.Set<String> second = CassandraExecutorBase.getKeyNameSet(TestEntity.class);
        assertEquals(first.size(), second.size());
    }

    @Test
    public void testGetKeyNameSet_returnsSet() {
        // exercise getKeyNameSet (overlap with getKeyNames, but distinct accessor)
        java.util.Set<String> set = CassandraExecutorBase.getKeyNameSet(TestEntity.class);
        assertNotNull(set);
        assertTrue(set.contains("id"));
    }

    @Test
    public void testGettWithCondition() throws DuplicateResultException {
        // Test gett with condition
        TestEntity result1 = executor.gett(TestEntity.class, Filters.eq("id", 1L));
        assertNotNull(result1);

        // Test gett with selectPropNames and condition
        TestEntity result2 = executor.gett(TestEntity.class, Arrays.asList("id", "name"), Filters.eq("id", 1L));
        assertNotNull(result2);
    }

    @Test
    public void testGettWithIds() throws DuplicateResultException {
        // Test gett with single id
        TestEntity result1 = executor.gett(TestEntity.class, 1L);
        assertNotNull(result1);

        // Test gett with selectPropNames
        TestEntity result2 = executor.gett(TestEntity.class, Arrays.asList("id", "name"), 1L);
        assertNotNull(result2);
    }

    @Test
    public void testGetWithCondition() throws DuplicateResultException {
        // Test get with condition
        Optional<TestEntity> result1 = executor.get(TestEntity.class, Filters.eq("id", 1L));
        assertNotNull(result1);

        // Test get with selectPropNames and condition
        Optional<TestEntity> result2 = executor.get(TestEntity.class, Arrays.asList("id", "name"), Filters.eq("id", 1L));
        assertNotNull(result2);
    }

    @Test
    public void testGetWithIds() throws DuplicateResultException {
        // Test get with single id
        Optional<TestEntity> result1 = executor.get(TestEntity.class, 1L);
        assertNotNull(result1);

        // Test get with selectPropNames
        Optional<TestEntity> result2 = executor.get(TestEntity.class, Arrays.asList("id", "name"), 1L);
        assertNotNull(result2);
    }

    @Test
    public void testIdsToCondition_compositeKey() {
        // Multi-key entity with matching id count -> AND condition
        Condition cond = TestCassandraExecutor.exposedIdsToCondition(CompositeKeyEntity.class, "u1", "s1");
        assertNotNull(cond);
        assertTrue(cond.toString().toLowerCase().contains("and"));
    }

    @Test
    public void testIdsToCondition_compositeKey_mismatch_throwsIAE() {
        assertThrows(IllegalArgumentException.class, () -> TestCassandraExecutor.exposedIdsToCondition(CompositeKeyEntity.class, "onlyOne"));
    }

    @Test
    public void testIdsToCondition_emptyCompositeKeyValue_throwsIAE() {
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> TestCassandraExecutor.exposedIdsToCondition(CompositeKeyEntity.class, "u1", ""));

        assertTrue(ex.getMessage().contains("sessionId"), ex.getMessage());
    }

    @Test
    public void testIdsToCondition_emptyIds_throwsIAE() {
        assertThrows(IllegalArgumentException.class, () -> TestCassandraExecutor.exposedIdsToCondition(TestEntity.class));
    }

    @Test
    public void testIdsToCondition_idCountMismatch_throwsIAE() {
        // Provide more ids than registered keys (single-key entity) -> IAE
        assertThrows(IllegalArgumentException.class, () -> TestCassandraExecutor.exposedIdsToCondition(TestEntity.class, 1L, 2L));
    }

    @Test
    public void testIdsToCondition_nullCompositeKeyValue_throwsIAE() {
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> TestCassandraExecutor.exposedIdsToCondition(CompositeKeyEntity.class, "u1", null));

        assertTrue(ex.getMessage().contains("sessionId"), ex.getMessage());
    }

    @Test
    public void testIdsToCondition_singleKey() {
        // Single key -> simple equality
        Condition cond = TestCassandraExecutor.exposedIdsToCondition(TestEntity.class, 1L);
        assertNotNull(cond);
        assertTrue(cond.toString().contains("id"));
    }

    @Test
    public void testInsert() {
        // Test insert entity
        TestEntity entity = new TestEntity();
        entity.setId(1L);
        entity.setName("test");

        TestResultSet result1 = executor.insert(entity);
        assertNotNull(result1);

        // Test insert with props
        Map<String, Object> props = new HashMap<>();
        props.put("id", 1L);
        props.put("name", "test");

        TestResultSet result2 = executor.insert(TestEntity.class, props);
        assertNotNull(result2);
    }

    @Test
    public void testInsert_nullEntity_throwsIAE() {
        assertThrows(IllegalArgumentException.class, () -> executor.insert((Object) null));
    }

    @Test
    public void testList() {
        // Test list with condition
        List<TestEntity> result1 = executor.list(TestEntity.class, Filters.eq("status", "active"));
        assertNotNull(result1);

        // Test list with selectPropNames and condition
        List<TestEntity> result2 = executor.list(TestEntity.class, Arrays.asList("id", "name"), Filters.eq("status", "active"));
        assertNotNull(result2);

        // Test list with query
        List<Map<String, Object>> result3 = executor.list("SELECT * FROM test WHERE status = ?", "active");
        assertNotNull(result3);

        // Test list with targetClass and query
        List<TestEntity> result4 = executor.list(TestEntity.class, "SELECT * FROM test WHERE status = ?", "active");
        assertNotNull(result4);
    }

    @Test
    public void testListAndStreamRejectNullTargetClassEagerly() {
        assertThrows(IllegalArgumentException.class, () -> executor.list((Class<Object>) null, "SELECT * FROM t"));
        assertThrows(IllegalArgumentException.class, () -> executor.stream((Class<Object>) null, "SELECT * FROM t"));
        assertThrows(IllegalArgumentException.class, () -> executor.stream((Class<Object>) null, new TestStatement()));
    }

    /** A null entity class / entity / SP argument is rejected with IllegalArgumentException, not NullPointerException. */
    @Test
    @SuppressWarnings("deprecation")
    public void testNullClassOrEntityArguments_throwIAE() {
        assertThrows(IllegalArgumentException.class, () -> CassandraExecutorBase.registerKeys(null, Arrays.asList("id")));
        assertThrows(IllegalArgumentException.class, () -> CassandraExecutorBase.getKeyNames(null));
        assertThrows(IllegalArgumentException.class, () -> CassandraExecutorBase.getKeyNameSet(null));
        assertThrows(IllegalArgumentException.class, () -> TestCassandraExecutor.exposedIdsToCondition(null, 1L));
        assertThrows(IllegalArgumentException.class, () -> TestCassandraExecutor.exposedEntityToCondition(null));

        assertThrows(IllegalArgumentException.class, () -> executor.get((Class<Object>) null, 1L));
        assertThrows(IllegalArgumentException.class, () -> executor.get((Class<Object>) null, (Collection<String>) null, 1L));
        assertThrows(IllegalArgumentException.class, () -> executor.gett((Class<Object>) null, 1L));
        assertThrows(IllegalArgumentException.class, () -> executor.gett((Class<Object>) null, (Collection<String>) null, 1L));
        assertThrows(IllegalArgumentException.class, () -> executor.delete((Class<?>) null, 1L));
        assertThrows(IllegalArgumentException.class, () -> executor.delete((Class<?>) null, (Collection<String>) null, 1L));
        assertThrows(IllegalArgumentException.class, () -> executor.exists((Class<?>) null, 1L));
        assertThrows(IllegalArgumentException.class, () -> executor.exists((Class<?>) null, Filters.eq("id", 1L)));
        assertThrows(IllegalArgumentException.class, () -> executor.execute((SP) null));
    }

    @Test
    public void testQuery() {
        // Test query with condition
        Dataset result1 = executor.query(TestEntity.class, Filters.eq("status", "active"));
        assertNotNull(result1);

        // Test query with selectPropNames and condition
        Dataset result2 = executor.query(TestEntity.class, Arrays.asList("id", "name"), Filters.eq("status", "active"));
        assertNotNull(result2);

        // Test query with query string
        Dataset result3 = executor.query("SELECT * FROM test WHERE status = ?", "active");
        assertNotNull(result3);

        // Test query with targetClass and query string
        Dataset result4 = executor.query(TestEntity.class, "SELECT * FROM test WHERE status = ?", "active");
        assertNotNull(result4);
    }

    @Test
    public void testQueryForPrimitiveTypes() {
        Condition condition = Filters.eq("id", 1L);

        // Test queryForBoolean
        OptionalBoolean boolResult = executor.queryForBoolean(TestEntity.class, "active", condition);
        assertNotNull(boolResult);

        // Test queryForChar
        OptionalChar charResult = executor.queryForChar(TestEntity.class, "grade", condition);
        assertNotNull(charResult);

        // Test queryForByte
        OptionalByte byteResult = executor.queryForByte(TestEntity.class, "level", condition);
        assertNotNull(byteResult);

        // Test queryForShort
        OptionalShort shortResult = executor.queryForShort(TestEntity.class, "count", condition);
        assertNotNull(shortResult);

        // Test queryForInt
        OptionalInt intResult = executor.queryForInt(TestEntity.class, "age", condition);
        assertNotNull(intResult);

        // Test queryForLong
        OptionalLong longResult = executor.queryForLong(TestEntity.class, "id", condition);
        assertNotNull(longResult);

        // Test queryForFloat
        OptionalFloat floatResult = executor.queryForFloat(TestEntity.class, "score", condition);
        assertNotNull(floatResult);

        // Test queryForDouble
        OptionalDouble doubleResult = executor.queryForDouble(TestEntity.class, "price", condition);
        assertNotNull(doubleResult);

        // Test queryForString
        Nullable<String> stringResult = executor.queryForString(TestEntity.class, "name", condition);
        assertNotNull(stringResult);

        // Test queryForDate
        Nullable<Date> dateResult = executor.queryForDate(TestEntity.class, "createdDate", condition);
        assertNotNull(dateResult);

        // Test queryForDate with valueClass
        Nullable<java.sql.Date> sqlDateResult = executor.queryForDate(TestEntity.class, java.sql.Date.class, "createdDate", condition);
        assertNotNull(sqlDateResult);
    }

    @Test
    public void testQueryForPrimitiveTypesWithQuery() {
        // Test queryForBoolean with query
        OptionalBoolean boolResult = executor.queryForBoolean("SELECT active FROM test WHERE id = ?", 1L);
        assertNotNull(boolResult);

        // Test queryForChar with query
        OptionalChar charResult = executor.queryForChar("SELECT grade FROM test WHERE id = ?", 1L);
        assertNotNull(charResult);

        // Test queryForByte with query
        OptionalByte byteResult = executor.queryForByte("SELECT level FROM test WHERE id = ?", 1L);
        assertNotNull(byteResult);

        // Test queryForShort with query
        OptionalShort shortResult = executor.queryForShort("SELECT count FROM test WHERE id = ?", 1L);
        assertNotNull(shortResult);

        // Test queryForInt with query
        OptionalInt intResult = executor.queryForInt("SELECT age FROM test WHERE id = ?", 1L);
        assertNotNull(intResult);

        // Test queryForLong with query
        OptionalLong longResult = executor.queryForLong("SELECT id FROM test WHERE id = ?", 1L);
        assertNotNull(longResult);

        // Test queryForFloat with query
        OptionalFloat floatResult = executor.queryForFloat("SELECT score FROM test WHERE id = ?", 1L);
        assertNotNull(floatResult);

        // Test queryForDouble with query
        OptionalDouble doubleResult = executor.queryForDouble("SELECT price FROM test WHERE id = ?", 1L);
        assertNotNull(doubleResult);

        // Test queryForString with query
        Nullable<String> stringResult = executor.queryForString("SELECT name FROM test WHERE id = ?", 1L);
        assertNotNull(stringResult);

        // Test queryForDate with query
        Nullable<Date> dateResult = executor.queryForDate("SELECT created_date FROM test WHERE id = ?", 1L);
        assertNotNull(dateResult);

        // Test queryForDate with valueClass and query
        Nullable<java.sql.Date> sqlDateResult = executor.queryForDate(java.sql.Date.class, "SELECT created_date FROM test WHERE id = ?", 1L);
        assertNotNull(sqlDateResult);
    }

    @Test
    public void testqueryForSingleValue() {
        // Test queryForSingleValue with condition
        Nullable<String> result1 = executor.queryForSingleValue(TestEntity.class, String.class, "name", Filters.eq("id", 1L));
        assertNotNull(result1);

        // Test queryForSingleNonNull with condition
        Optional<String> result2 = executor.queryForSingleNonNull(TestEntity.class, String.class, "name", Filters.eq("id", 1L));
        assertNotNull(result2);
    }

    @Test
    public void testqueryForSingleValueAndNonNull_nullOrEmptyPropName_throwsIAE() {
        // Regression: both queryForSingleValue and queryForSingleNonNull document
        // "@throws IllegalArgumentException if propName is null or empty". The sibling queryForSingleValue
        // already guarded propName; queryForSingleNonNull was missing the guard and threw NPE (from List.of(null))
        // for a null propName and silently built a malformed projection for an empty one.
        assertThrows(IllegalArgumentException.class, () -> executor.queryForSingleValue(TestEntity.class, String.class, null, Filters.eq("id", 1L)));
        assertThrows(IllegalArgumentException.class, () -> executor.queryForSingleValue(TestEntity.class, String.class, "", Filters.eq("id", 1L)));
        assertThrows(IllegalArgumentException.class, () -> executor.queryForSingleNonNull(TestEntity.class, String.class, null, Filters.eq("id", 1L)));
        assertThrows(IllegalArgumentException.class, () -> executor.queryForSingleNonNull(TestEntity.class, String.class, "", Filters.eq("id", 1L)));
    }

    @Test
    public void testRegisterKeys() {
        // Test deprecated registerKeys method
        CassandraExecutorBase.registerKeys(TestEntity.class, Arrays.asList("id", "name"));

        // Verify keys are registered
        ImmutableList<String> keyNames = CassandraExecutorBase.getKeyNames(TestEntity.class);
        assertTrue(keyNames.contains("id"));
        assertTrue(keyNames.contains("name"));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testRegisterKeys_nullOrBlankKeyName_throwsIAE() {
        assertThrows(IllegalArgumentException.class, () -> CassandraExecutorBase.registerKeys(TestEntity.class, Arrays.asList("id", null)));
        assertThrows(IllegalArgumentException.class, () -> CassandraExecutorBase.registerKeys(TestEntity.class, Arrays.asList("")));
        assertThrows(IllegalArgumentException.class, () -> CassandraExecutorBase.registerKeys(String.class, Arrays.asList("length")));
    }

    // the failure used to surface as the unrelated "'getSetMethod' cannot be null" from Beans.getPropNameByMethod(null).
    @Test
    @SuppressWarnings("deprecation")
    public void testRegisterKeys_unknownPropertyName_throwsIAENamingTheKey() {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> CassandraExecutorBase.registerKeys(TestEntity.class, Arrays.asList("id", "noSuchProp")));

        assertTrue(e.getMessage().contains("noSuchProp"), e.getMessage());
        assertTrue(e.getMessage().contains(TestEntity.class.getCanonicalName()), e.getMessage());
        // nothing was registered for the class
        assertTrue(!CassandraExecutorBase.entityKeyNamesMap.containsKey(TestEntity.class));
    }

    // async count/queryForXxx façades agree with their synchronous counterparts.
    @Test
    public void testSharedMappers_presentNullCollapsesToPrimitiveDefault() {
        assertEquals(0L, CassandraExecutorBase.long_secondMapper.apply(Nullable.of((Long) null)).longValue());
        assertEquals(0L, CassandraExecutorBase.long_secondMapper.apply(Nullable.<Long> empty()).longValue());
        assertEquals(7L, CassandraExecutorBase.long_secondMapper.apply(Nullable.of(7L)).longValue());

        final OptionalBoolean presentNull = CassandraExecutorBase.boolean_mapper.apply(Nullable.of((Boolean) null));
        assertTrue(presentNull.isPresent());
        assertEquals(false, presentNull.get());
        assertTrue(CassandraExecutorBase.boolean_mapper.apply(Nullable.<Boolean> empty()).isEmpty());

        final OptionalLong presentNullLong = CassandraExecutorBase.long_mapper.apply(Nullable.of((Long) null));
        assertTrue(presentNullLong.isPresent());
        assertEquals(0L, presentNullLong.get());
        assertTrue(CassandraExecutorBase.long_mapper.apply(Nullable.<Long> empty()).isEmpty());
        assertTrue(CassandraExecutorBase.exists_mapper.apply(Arrays.asList(new Object())));
        assertTrue(!CassandraExecutorBase.exists_mapper.apply(new ArrayList<>()));
    }

    // A keyless class must not be reported as having the key "[id]".
    @Test
    public void testSliceN_idsToCondition_keylessClass_messageDoesNotInventIdKey() {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> TestCassandraExecutor.exposedIdsToCondition(SliceNKeylessEntity.class, "x"));

        assertTrue(!e.getMessage().contains("[id]"), e.getMessage());
        assertTrue(e.getMessage().contains("No key names defined"), e.getMessage());
    }

    @Test
    public void testStream() {
        // Test stream with condition
        Stream<TestEntity> stream1 = executor.stream(TestEntity.class, Filters.eq("status", "active"));
        assertNotNull(stream1);

        // Test stream with selectPropNames and condition
        Stream<TestEntity> stream2 = executor.stream(TestEntity.class, Arrays.asList("id", "name"), Filters.eq("status", "active"));
        assertNotNull(stream2);

        // Test stream with query
        Stream<Object[]> stream3 = executor.stream("SELECT * FROM test WHERE status = ?", "active");
        assertNotNull(stream3);

        // Test stream with targetClass and query
        Stream<TestEntity> stream4 = executor.stream(TestEntity.class, "SELECT * FROM test WHERE status = ?", "active");
        assertNotNull(stream4);

        // Test stream with statement
        TestStatement stmt = new TestStatement();
        Stream<TestEntity> stream5 = executor.stream(TestEntity.class, stmt);
        assertNotNull(stream5);
    }

    @Test
    public void testUpdate() {
        // Test update entity
        TestEntity entity = new TestEntity();
        entity.setId(1L);
        entity.setName("test");

        TestResultSet result1 = executor.update(entity);
        assertNotNull(result1);

        // Test update with propNamesToUpdate
        TestResultSet result2 = executor.update(entity, Arrays.asList("name"));
        assertNotNull(result2);

        // Test update with props and condition
        Map<String, Object> props = new HashMap<>();
        props.put("name", "updated");

        TestResultSet result3 = executor.update(TestEntity.class, props, Filters.eq("id", 1L));
        assertNotNull(result3);

        // Test update with query
        TestResultSet result4 = executor.update("UPDATE test SET name = ? WHERE id = ?", "updated", 1L);
        assertNotNull(result4);
    }

    @Test
    public void testUpdate_nullEntity_throwsIAE() {
        assertThrows(IllegalArgumentException.class, () -> executor.update((Object) null));
    }

    @Test
    public void testUpdate_nullEntityWithPropNames_throwsIAE() {
        assertThrows(IllegalArgumentException.class, () -> executor.update((Object) null, java.util.Arrays.asList("name")));
    }

    @Test
    public void testUsingTTLRejectsNegativeValue() {
        assertThrows(IllegalArgumentException.class, () -> CqlBuilder.Dsl.PSC.insert("id").into("events").usingTTL(-1));
    }

    // ---- 2026-10-19 sliceF ----

    /** An entity whose only properties are its key (plus a computed getter-only one): nothing can be SET by an UPDATE. */
    public static class SliceFKeyOnlyEntity {
        @com.landawn.abacus.annotation.Id
        private Long id;

        public Long getId() {
            return id;
        }

        public void setId(final Long id) {
            this.id = id;
        }

        public String getDisplayName() {
            return "entity-" + id;
        }
    }

    /**
     * update(entity) / batchUpdate(entities, type) derive the update properties themselves; when the entity class has no
     * updatable non-key property they reported "'propNamesToUpdate' can't be null or empty", naming an argument the caller
     * never passed.
     */
    @Test
    public void testSliceF_updateWithoutUpdatableProperty_reportsTheEntityClass() {
        final SliceFKeyOnlyEntity entity = new SliceFKeyOnlyEntity();
        entity.setId(1L);

        final IllegalArgumentException single = assertThrows(IllegalArgumentException.class, () -> executor.update(entity));
        final IllegalArgumentException batch = assertThrows(IllegalArgumentException.class, () -> executor.batchUpdate(List.of(entity), TestBatchType.LOGGED));

        org.junit.jupiter.api.Assertions.assertAll(() -> assertTrue(!single.getMessage().contains("propNamesToUpdate"), single.getMessage()),
                () -> assertTrue(single.getMessage().contains("No updatable non-key property"), single.getMessage()),
                () -> assertTrue(single.getMessage().contains(SliceFKeyOnlyEntity.class.getSimpleName()), single.getMessage()),
                () -> assertTrue(!batch.getMessage().contains("propNamesToUpdate"), batch.getMessage()),
                () -> assertTrue(batch.getMessage().contains("No updatable non-key property"), batch.getMessage()),
                () -> assertTrue(batch.getMessage().contains(SliceFKeyOnlyEntity.class.getSimpleName()), batch.getMessage()));
    }

    /** An explicitly empty propNamesToUpdate still reports that argument. */
    @Test
    public void testSliceF_updateWithEmptyExplicitPropNames_stillNamesTheArgument() {
        final TestEntity entity = new TestEntity();
        entity.setId(1L);

        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> executor.update(entity, new ArrayList<>()));

        assertTrue(e.getMessage().contains("propNamesToUpdate"), e.getMessage());
    }
}
