package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.exception.DuplicateResultException;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.ImmutableList;
import com.landawn.abacus.util.NamingPolicy;
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
import com.landawn.abacus.util.stream.Stream;

public class CassandraExecutorBaseTest extends TestBase {

    private TestCassandraExecutor executor;

    @BeforeEach
    public void setUp() {
        executor = new TestCassandraExecutor();
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
    public void testGetWithIds() throws DuplicateResultException {
        // Test get with single id
        Optional<TestEntity> result1 = executor.get(TestEntity.class, 1L);
        assertNotNull(result1);

        // Test get with multiple ids
        Optional<TestEntity> result2 = executor.get(TestEntity.class, 1L, "name1");
        assertNotNull(result2);

        // Test get with selectPropNames
        Optional<TestEntity> result3 = executor.get(TestEntity.class, Arrays.asList("id", "name"), 1L);
        assertNotNull(result3);
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
    public void testGettWithIds() throws DuplicateResultException {
        // Test gett with single id
        TestEntity result1 = executor.gett(TestEntity.class, 1L);
        assertNotNull(result1);

        // Test gett with multiple ids
        TestEntity result2 = executor.gett(TestEntity.class, 1L, "name1");
        assertNotNull(result2);

        // Test gett with selectPropNames
        TestEntity result3 = executor.gett(TestEntity.class, Arrays.asList("id", "name"), 1L);
        assertNotNull(result3);
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
    public void testBatchDelete() {
        // Test batch delete entities
        List<TestEntity> entities = Arrays.asList(new TestEntity(), new TestEntity());
        TestResultSet result1 = executor.batchDelete(entities);
        assertNotNull(result1);

        // Test batch delete with propNamesToDelete
        TestResultSet result2 = executor.batchDelete(entities, Arrays.asList("name"));
        assertNotNull(result2);
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
    public void testCount() {
        // Test count with condition
        long result1 = executor.count(TestEntity.class, Filters.eq("status", "active"));
        assertEquals(10L, result1);

        // Test deprecated count with query
        long result2 = executor.count("SELECT COUNT(*) FROM test WHERE status = ?", "active");
        assertEquals(10L, result2);
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
    }

    @Test
    public void testQueryForSingleResult() {
        // Test queryForSingleResult with condition
        Nullable<String> result1 = executor.queryForSingleResult(TestEntity.class, String.class, "name", Filters.eq("id", 1L));
        assertNotNull(result1);

        // Test queryForSingleNonNull with condition
        Optional<String> result2 = executor.queryForSingleNonNull(TestEntity.class, String.class, "name", Filters.eq("id", 1L));
        assertNotNull(result2);
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
    public void testAsyncMethods() {
        // Test async get methods
        ContinuableFuture<Optional<TestEntity>> asyncGet1 = executor.asyncGet(TestEntity.class, 1L);
        assertNotNull(asyncGet1);

        ContinuableFuture<Optional<TestEntity>> asyncGet2 = executor.asyncGet(TestEntity.class, Arrays.asList("id"), 1L);
        assertNotNull(asyncGet2);

        ContinuableFuture<Optional<TestEntity>> asyncGet3 = executor.asyncGet(TestEntity.class, Filters.eq("id", 1L));
        assertNotNull(asyncGet3);

        ContinuableFuture<Optional<TestEntity>> asyncGet4 = executor.asyncGet(TestEntity.class, Arrays.asList("id"), Filters.eq("id", 1L));
        assertNotNull(asyncGet4);

        // Test async gett methods
        ContinuableFuture<TestEntity> asyncGett1 = executor.asyncGett(TestEntity.class, 1L);
        assertNotNull(asyncGett1);

        ContinuableFuture<TestEntity> asyncGett2 = executor.asyncGett(TestEntity.class, Arrays.asList("id"), 1L);
        assertNotNull(asyncGett2);

        ContinuableFuture<TestEntity> asyncGett3 = executor.asyncGett(TestEntity.class, Filters.eq("id", 1L));
        assertNotNull(asyncGett3);

        ContinuableFuture<TestEntity> asyncGett4 = executor.asyncGett(TestEntity.class, Arrays.asList("id"), Filters.eq("id", 1L));
        assertNotNull(asyncGett4);
    }

    @Test
    public void testAsyncCRUDOperations() {
        TestEntity entity = new TestEntity();
        entity.setId(1L);
        entity.setName("test");

        // Test async insert
        ContinuableFuture<TestResultSet> asyncInsert1 = executor.asyncInsert(entity);
        assertNotNull(asyncInsert1);

        Map<String, Object> props = new HashMap<>();
        props.put("id", 1L);
        props.put("name", "test");

        ContinuableFuture<TestResultSet> asyncInsert2 = executor.asyncInsert(TestEntity.class, props);
        assertNotNull(asyncInsert2);

        // Test async batch insert
        List<TestEntity> entities = Arrays.asList(new TestEntity(), new TestEntity());
        ContinuableFuture<TestResultSet> asyncBatchInsert1 = executor.asyncBatchInsert(entities, TestBatchType.LOGGED);
        assertNotNull(asyncBatchInsert1);

        List<Map<String, Object>> propsList = Arrays.asList(new HashMap<>(), new HashMap<>());
        ContinuableFuture<TestResultSet> asyncBatchInsert2 = executor.asyncBatchInsert(TestEntity.class, propsList, TestBatchType.LOGGED);
        assertNotNull(asyncBatchInsert2);

        // Test async update
        ContinuableFuture<TestResultSet> asyncUpdate1 = executor.asyncUpdate(entity);
        assertNotNull(asyncUpdate1);

        ContinuableFuture<TestResultSet> asyncUpdate2 = executor.asyncUpdate(entity, Arrays.asList("name"));
        assertNotNull(asyncUpdate2);

        ContinuableFuture<TestResultSet> asyncUpdate3 = executor.asyncUpdate(TestEntity.class, props, Filters.eq("id", 1L));
        assertNotNull(asyncUpdate3);

        ContinuableFuture<TestResultSet> asyncUpdate4 = executor.asyncUpdate("UPDATE test SET name = ? WHERE id = ?", "updated", 1L);
        assertNotNull(asyncUpdate4);

        // Test async batch update
        ContinuableFuture<TestResultSet> asyncBatchUpdate1 = executor.asyncBatchUpdate(entities, TestBatchType.LOGGED);
        assertNotNull(asyncBatchUpdate1);

        ContinuableFuture<TestResultSet> asyncBatchUpdate2 = executor.asyncBatchUpdate(entities, Arrays.asList("name"), TestBatchType.LOGGED);
        assertNotNull(asyncBatchUpdate2);

        ContinuableFuture<TestResultSet> asyncBatchUpdate3 = executor.asyncBatchUpdate(TestEntity.class, propsList, TestBatchType.LOGGED);
        assertNotNull(asyncBatchUpdate3);

        List<Object[]> parametersList = Arrays.asList(new Object[] { "updated1", 1L }, new Object[] { "updated2", 2L });
        ContinuableFuture<TestResultSet> asyncBatchUpdate4 = executor.asyncBatchUpdate("UPDATE test SET name = ? WHERE id = ?", parametersList,
                TestBatchType.LOGGED);
        assertNotNull(asyncBatchUpdate4);

        // Test async delete
        ContinuableFuture<TestResultSet> asyncDelete1 = executor.asyncDelete(entity);
        assertNotNull(asyncDelete1);

        ContinuableFuture<TestResultSet> asyncDelete2 = executor.asyncDelete(entity, Arrays.asList("name"));
        assertNotNull(asyncDelete2);

        ContinuableFuture<TestResultSet> asyncDelete3 = executor.asyncDelete(TestEntity.class, 1L);
        assertNotNull(asyncDelete3);

        ContinuableFuture<TestResultSet> asyncDelete4 = executor.asyncDelete(TestEntity.class, Arrays.asList("name"), 1L);
        assertNotNull(asyncDelete4);

        ContinuableFuture<TestResultSet> asyncDelete5 = executor.asyncDelete(TestEntity.class, Filters.eq("id", 1L));
        assertNotNull(asyncDelete5);

        ContinuableFuture<TestResultSet> asyncDelete6 = executor.asyncDelete(TestEntity.class, Arrays.asList("name"), Filters.eq("id", 1L));
        assertNotNull(asyncDelete6);

        // Test async batch delete
        ContinuableFuture<TestResultSet> asyncBatchDelete1 = executor.asyncBatchDelete(entities);
        assertNotNull(asyncBatchDelete1);

        ContinuableFuture<TestResultSet> asyncBatchDelete2 = executor.asyncBatchDelete(entities, Arrays.asList("name"));
        assertNotNull(asyncBatchDelete2);
    }

    @Test
    public void testAsyncQueryMethods() {
        // Test async exists
        ContinuableFuture<Boolean> asyncExists1 = executor.asyncExists(TestEntity.class, 1L);
        assertNotNull(asyncExists1);

        ContinuableFuture<Boolean> asyncExists2 = executor.asyncExists(TestEntity.class, Filters.eq("id", 1L));
        assertNotNull(asyncExists2);

        ContinuableFuture<Boolean> asyncExists3 = executor.asyncExists("SELECT * FROM test WHERE id = ? LIMIT 1", 1L);
        assertNotNull(asyncExists3);

        // Test async count
        ContinuableFuture<Long> asyncCount1 = executor.asyncCount(TestEntity.class, Filters.eq("status", "active"));
        assertNotNull(asyncCount1);

        ContinuableFuture<Long> asyncCount2 = executor.asyncCount("SELECT COUNT(*) FROM test WHERE status = ?", "active");
        assertNotNull(asyncCount2);

        // Test async list
        ContinuableFuture<List<TestEntity>> asyncList1 = executor.asyncList(TestEntity.class, Filters.eq("status", "active"));
        assertNotNull(asyncList1);

        ContinuableFuture<List<TestEntity>> asyncList2 = executor.asyncList(TestEntity.class, Arrays.asList("id", "name"), Filters.eq("status", "active"));
        assertNotNull(asyncList2);

        ContinuableFuture<List<Map<String, Object>>> asyncList3 = executor.asyncList("SELECT * FROM test WHERE status = ?", "active");
        assertNotNull(asyncList3);

        ContinuableFuture<List<TestEntity>> asyncList4 = executor.asyncList(TestEntity.class, "SELECT * FROM test WHERE status = ?", "active");
        assertNotNull(asyncList4);

        // Test async query
        ContinuableFuture<Dataset> asyncQuery1 = executor.asyncQuery(TestEntity.class, Filters.eq("status", "active"));
        assertNotNull(asyncQuery1);

        ContinuableFuture<Dataset> asyncQuery2 = executor.asyncQuery(TestEntity.class, Arrays.asList("id", "name"), Filters.eq("status", "active"));
        assertNotNull(asyncQuery2);

        ContinuableFuture<Dataset> asyncQuery3 = executor.asyncQuery("SELECT * FROM test WHERE status = ?", "active");
        assertNotNull(asyncQuery3);

        ContinuableFuture<Dataset> asyncQuery4 = executor.asyncQuery(TestEntity.class, "SELECT * FROM test WHERE status = ?", "active");
        assertNotNull(asyncQuery4);
    }

    @Test
    public void testAsyncQueryForPrimitiveTypes() {
        Condition condition = Filters.eq("id", 1L);

        // Test async queryForBoolean
        ContinuableFuture<OptionalBoolean> asyncBool1 = executor.asyncQueryForBoolean(TestEntity.class, "active", condition);
        assertNotNull(asyncBool1);

        ContinuableFuture<OptionalBoolean> asyncBool2 = executor.asyncQueryForBoolean("SELECT active FROM test WHERE id = ?", 1L);
        assertNotNull(asyncBool2);

        // Test async queryForChar
        ContinuableFuture<OptionalChar> asyncChar1 = executor.asyncQueryForChar(TestEntity.class, "grade", condition);
        assertNotNull(asyncChar1);

        ContinuableFuture<OptionalChar> asyncChar2 = executor.asyncQueryForChar("SELECT grade FROM test WHERE id = ?", 1L);
        assertNotNull(asyncChar2);

        // Test async queryForByte
        ContinuableFuture<OptionalByte> asyncByte1 = executor.asyncQueryForByte(TestEntity.class, "level", condition);
        assertNotNull(asyncByte1);

        ContinuableFuture<OptionalByte> asyncByte2 = executor.asyncQueryForByte("SELECT level FROM test WHERE id = ?", 1L);
        assertNotNull(asyncByte2);

        // Test async queryForShort
        ContinuableFuture<OptionalShort> asyncShort1 = executor.asyncQueryForShort(TestEntity.class, "count", condition);
        assertNotNull(asyncShort1);

        ContinuableFuture<OptionalShort> asyncShort2 = executor.asyncQueryForShort("SELECT count FROM test WHERE id = ?", 1L);
        assertNotNull(asyncShort2);

        // Test async queryForInt
        ContinuableFuture<OptionalInt> asyncInt1 = executor.asyncQueryForInt(TestEntity.class, "age", condition);
        assertNotNull(asyncInt1);

        ContinuableFuture<OptionalInt> asyncInt2 = executor.asyncQueryForInt("SELECT age FROM test WHERE id = ?", 1L);
        assertNotNull(asyncInt2);

        // Test async queryForLong
        ContinuableFuture<OptionalLong> asyncLong1 = executor.asyncQueryForLong(TestEntity.class, "id", condition);
        assertNotNull(asyncLong1);

        ContinuableFuture<OptionalLong> asyncLong2 = executor.asyncQueryForLong("SELECT id FROM test WHERE id = ?", 1L);
        assertNotNull(asyncLong2);

        // Test async queryForFloat
        ContinuableFuture<OptionalFloat> asyncFloat1 = executor.asyncQueryForFloat(TestEntity.class, "score", condition);
        assertNotNull(asyncFloat1);

        ContinuableFuture<OptionalFloat> asyncFloat2 = executor.asyncQueryForFloat("SELECT score FROM test WHERE id = ?", 1L);
        assertNotNull(asyncFloat2);

        // Test async queryForDouble
        ContinuableFuture<OptionalDouble> asyncDouble1 = executor.asyncQueryForDouble(TestEntity.class, "price", condition);
        assertNotNull(asyncDouble1);

        ContinuableFuture<OptionalDouble> asyncDouble2 = executor.asyncQueryForDouble("SELECT price FROM test WHERE id = ?", 1L);
        assertNotNull(asyncDouble2);

        // Test async queryForString
        ContinuableFuture<Nullable<String>> asyncString1 = executor.asyncQueryForString(TestEntity.class, "name", condition);
        assertNotNull(asyncString1);

        ContinuableFuture<Nullable<String>> asyncString2 = executor.asyncQueryForString("SELECT name FROM test WHERE id = ?", 1L);
        assertNotNull(asyncString2);

        // Test async queryForDate
        ContinuableFuture<Nullable<Date>> asyncDate1 = executor.asyncQueryForDate(TestEntity.class, "createdDate", condition);
        assertNotNull(asyncDate1);

        ContinuableFuture<Nullable<java.sql.Date>> asyncDate2 = executor.asyncQueryForDate(TestEntity.class, java.sql.Date.class, "createdDate", condition);
        assertNotNull(asyncDate2);
    }

    @Test
    public void testAsyncQueryForSingleResult() {
        // Test async queryForSingleResult
        ContinuableFuture<Nullable<String>> asyncResult1 = executor.asyncQueryForSingleResult(TestEntity.class, String.class, "name", Filters.eq("id", 1L));
        assertNotNull(asyncResult1);

        ContinuableFuture<Nullable<String>> asyncResult2 = executor.asyncQueryForSingleResult(String.class, "SELECT name FROM test WHERE id = ?", 1L);
        assertNotNull(asyncResult2);

        // Test async queryForSingleNonNull
        ContinuableFuture<Optional<String>> asyncResult3 = executor.asyncQueryForSingleNonNull(TestEntity.class, String.class, "name", Filters.eq("id", 1L));
        assertNotNull(asyncResult3);

        ContinuableFuture<Optional<String>> asyncResult4 = executor.asyncQueryForSingleNonNull(String.class, "SELECT name FROM test WHERE id = ?", 1L);
        assertNotNull(asyncResult4);
    }

    @Test
    public void testAsyncFindFirst() {
        // Test async findFirst
        ContinuableFuture<Optional<TestEntity>> asyncFind1 = executor.asyncFindFirst(TestEntity.class, Filters.eq("id", 1L));
        assertNotNull(asyncFind1);

        ContinuableFuture<Optional<TestEntity>> asyncFind2 = executor.asyncFindFirst(TestEntity.class, Arrays.asList("id", "name"), Filters.eq("id", 1L));
        assertNotNull(asyncFind2);

        ContinuableFuture<Optional<Map<String, Object>>> asyncFind3 = executor.asyncFindFirst("SELECT * FROM test WHERE id = ?", 1L);
        assertNotNull(asyncFind3);

        ContinuableFuture<Optional<TestEntity>> asyncFind4 = executor.asyncFindFirst(TestEntity.class, "SELECT * FROM test WHERE id = ?", 1L);
        assertNotNull(asyncFind4);
    }

    @Test
    public void testAsyncStream() {
        // Test async stream
        ContinuableFuture<Stream<TestEntity>> asyncStream1 = executor.asyncStream(TestEntity.class, Filters.eq("status", "active"));
        assertNotNull(asyncStream1);

        ContinuableFuture<Stream<TestEntity>> asyncStream2 = executor.asyncStream(TestEntity.class, Arrays.asList("id", "name"),
                Filters.eq("status", "active"));
        assertNotNull(asyncStream2);

        ContinuableFuture<Stream<Object[]>> asyncStream3 = executor.asyncStream("SELECT * FROM test WHERE status = ?", "active");
        assertNotNull(asyncStream3);

        ContinuableFuture<Stream<TestEntity>> asyncStream4 = executor.asyncStream(TestEntity.class, "SELECT * FROM test WHERE status = ?", "active");
        assertNotNull(asyncStream4);

        TestStatement stmt = new TestStatement();
        ContinuableFuture<Stream<TestEntity>> asyncStream5 = executor.asyncStream(TestEntity.class, stmt);
        assertNotNull(asyncStream5);
    }

    @Test
    public void testAsyncExecute() {
        // Test async execute
        ContinuableFuture<TestResultSet> asyncExec1 = executor.asyncExecute("SELECT * FROM test");
        assertNotNull(asyncExec1);

        ContinuableFuture<TestResultSet> asyncExec2 = executor.asyncExecute("SELECT * FROM test WHERE id = ?", 1L);
        assertNotNull(asyncExec2);

        Map<String, Object> params = new HashMap<>();
        params.put("id", 1L);
        ContinuableFuture<TestResultSet> asyncExec3 = executor.asyncExecute("SELECT * FROM test WHERE id = :id", params);
        assertNotNull(asyncExec3);

        TestStatement stmt = new TestStatement();
        ContinuableFuture<TestResultSet> asyncExec4 = executor.asyncExecute(stmt);
        assertNotNull(asyncExec4);
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

    // Test implementation of CassandraExecutorBase
    private static class TestCassandraExecutor extends CassandraExecutorBase<TestRow, TestResultSet, TestStatement, TestPreparedStatement, TestBatchType> {

        public TestCassandraExecutor() {
            super(null, NamingPolicy.SNAKE_CASE);
        }

        @Override
        public <T> T gett(Class<T> targetClass, Collection<String> selectPropNames, Condition whereClause) throws DuplicateResultException {
            try {
                return targetClass.newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public <E> Nullable<E> queryForSingleResult(Class<E> valueClass, String query, Object... parameters) {
            return Nullable.of((E) null);
        }

        @Override
        public <E> Optional<E> queryForSingleNonNull(Class<E> valueClass, String query, Object... parameters) {
            return Optional.empty();
        }

        @Override
        public <T> Optional<T> findFirst(Class<T> targetClass, String query, Object... parameters) {
            try {
                return Optional.of(targetClass.newInstance());
            } catch (Exception e) {
                return Optional.empty();
            }
        }

        @Override
        public TestResultSet execute(String query) {
            return new TestResultSet();
        }

        @Override
        public TestResultSet execute(String query, Object... parameters) {
            return new TestResultSet();
        }

        @Override
        public TestResultSet execute(String query, Map<String, Object> parameters) {
            return new TestResultSet();
        }

        @Override
        public TestResultSet execute(TestStatement statement) {
            return new TestResultSet();
        }

        @Override
        public <T> ContinuableFuture<Optional<T>> asyncGet(Class<T> targetClass, Collection<String> selectPropNames, Condition whereClause) {
            return ContinuableFuture.completed(Optional.empty());
        }

        @Override
        public <T> ContinuableFuture<T> asyncGett(Class<T> targetClass, Collection<String> selectPropNames, Condition whereClause) {
            try {
                return ContinuableFuture.completed(targetClass.newInstance());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public <T> ContinuableFuture<Nullable<T>> asyncQueryForSingleResult(Class<T> valueClass, String query, Object... parameters) {
            return ContinuableFuture.completed(Nullable.empty());
        }

        @Override
        public <T> ContinuableFuture<Optional<T>> asyncQueryForSingleNonNull(Class<T> valueClass, String query, Object... parameters) {
            return ContinuableFuture.completed(Optional.empty());
        }

        @Override
        public <T> ContinuableFuture<Optional<T>> asyncFindFirst(Class<T> targetClass, String query, Object... parameters) {
            return ContinuableFuture.completed(Optional.empty());
        }

        @Override
        public ContinuableFuture<Stream<Object[]>> asyncStream(String query, Object... parameters) {
            return ContinuableFuture.completed(Stream.empty());
        }

        @Override
        public ContinuableFuture<TestResultSet> asyncExecute(String query) {
            return ContinuableFuture.completed(new TestResultSet());
        }

        @Override
        public ContinuableFuture<TestResultSet> asyncExecute(String query, Object... parameters) {
            return ContinuableFuture.completed(new TestResultSet());
        }

        @Override
        public ContinuableFuture<TestResultSet> asyncExecute(String query, Map<String, Object> parameters) {
            return ContinuableFuture.completed(new TestResultSet());
        }

        @Override
        public ContinuableFuture<TestResultSet> asyncExecute(TestStatement statement) {
            return ContinuableFuture.completed(new TestResultSet());
        }

        @Override
        public void close() {
            // No-op
        }

        @Override
        protected TestStatement prepareBatchStatement(TestBatchType type) {
            return new TestStatement();
        }

        @Override
        protected TestStatement prepareBatchInsertStatement(Collection<?> entities, TestBatchType type) {
            return new TestStatement();
        }

        @Override
        protected TestStatement prepareBatchInsertStatement(Class<?> targetClass, Collection<? extends Map<String, Object>> propsList, TestBatchType type) {
            return new TestStatement();
        }

        @Override
        protected TestStatement prepareBatchUpdateStatement(Collection<?> entities, Collection<String> propNamesToUpdate, TestBatchType type) {
            return new TestStatement();
        }

        @Override
        protected TestStatement prepareBatchUpdateStatement(Class<?> targetClass, Collection<? extends Map<String, Object>> propsList, TestBatchType type) {
            return new TestStatement();
        }

        @Override
        protected TestStatement prepareBatchUpdateStatement(String query, Collection<?> parametersList, TestBatchType type) {
            return new TestStatement();
        }

        @Override
        protected TestStatement prepareStatement(String query) {
            return new TestStatement();
        }

        @Override
        protected TestStatement prepareStatement(String query, Object... parameters) {
            return new TestStatement();
        }

        @Override
        protected TestPreparedStatement prepare(String query) {
            return new TestPreparedStatement();
        }

        @Override
        protected TestStatement bind(TestPreparedStatement preStmt, Object... parameters) {
            return new TestStatement();
        }

        @Override
        protected <T> List<T> toList(Class<T> targetClass, TestResultSet execute) {
            return new ArrayList<>();
        }

        @Override
        protected Dataset extractData(Class<?> targetClass, TestResultSet execute) {
            return Dataset.empty();
        }

        @Override
        protected <T> Function<TestRow, T> createRowMapper(Class<T> targetClass) {
            return row -> {
                try {
                    return targetClass.newInstance();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            };
        }
    }

    // Test classes
    public static class TestEntity {
        private Long id;
        private String name;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    private static class TestRow {
    }

    private static class TestResultSet implements Iterable<TestRow> {
        @Override
        public Iterator<TestRow> iterator() {
            return Arrays.asList(new TestRow()).iterator();
        }
    }

    private static class TestStatement {
    }

    private static class TestPreparedStatement {
    }

    private enum TestBatchType {
        LOGGED, UNLOGGED, COUNTER
    }
}