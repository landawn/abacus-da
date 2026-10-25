package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.exception.DuplicateResultException;
import com.landawn.abacus.query.AbstractQueryBuilder.SP;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.Dataset;
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

public class AsyncCassandraExecutorBaseFacadeTest extends CassandraExecutorBaseTestSupport {

    @Test
    public void testAsyncCRUDOperations() {
        TestEntity entity = new TestEntity();
        entity.setId(1L);
        entity.setName("test");

        // Test async insert
        ContinuableFuture<TestResultSet> asyncInsert1 = executor.async().insert(entity);
        assertNotNull(asyncInsert1);

        Map<String, Object> props = new HashMap<>();
        props.put("id", 1L);
        props.put("name", "test");

        ContinuableFuture<TestResultSet> asyncInsert2 = executor.async().insert(TestEntity.class, props);
        assertNotNull(asyncInsert2);

        // Test async batch insert
        TestEntity e1 = new TestEntity();
        e1.setId(1L);
        TestEntity e2 = new TestEntity();
        e2.setId(2L);
        List<TestEntity> entities = Arrays.asList(e1, e2);
        ContinuableFuture<TestResultSet> asyncBatchInsert1 = executor.async().batchInsert(entities, TestBatchType.LOGGED);
        assertNotNull(asyncBatchInsert1);

        List<Map<String, Object>> propsList = Arrays.asList(new HashMap<>(), new HashMap<>());
        ContinuableFuture<TestResultSet> asyncBatchInsert2 = executor.async().batchInsert(TestEntity.class, propsList, TestBatchType.LOGGED);
        assertNotNull(asyncBatchInsert2);

        // Test async update
        ContinuableFuture<TestResultSet> asyncUpdate1 = executor.async().update(entity);
        assertNotNull(asyncUpdate1);

        ContinuableFuture<TestResultSet> asyncUpdate2 = executor.async().update(entity, Arrays.asList("name"));
        assertNotNull(asyncUpdate2);

        ContinuableFuture<TestResultSet> asyncUpdate3 = executor.async().update(TestEntity.class, Map.of("name", "test"), Filters.eq("id", 1L));
        assertNotNull(asyncUpdate3);

        ContinuableFuture<TestResultSet> asyncUpdate4 = executor.async().update("UPDATE test SET name = ? WHERE id = ?", "updated", 1L);
        assertNotNull(asyncUpdate4);

        // Test async batch update
        ContinuableFuture<TestResultSet> asyncBatchUpdate1 = executor.async().batchUpdate(entities, TestBatchType.LOGGED);
        assertNotNull(asyncBatchUpdate1);

        ContinuableFuture<TestResultSet> asyncBatchUpdate2 = executor.async().batchUpdate(entities, Arrays.asList("name"), TestBatchType.LOGGED);
        assertNotNull(asyncBatchUpdate2);

        ContinuableFuture<TestResultSet> asyncBatchUpdate3 = executor.async().batchUpdate(TestEntity.class, propsList, TestBatchType.LOGGED);
        assertNotNull(asyncBatchUpdate3);

        List<Object[]> parametersList = Arrays.asList(new Object[] { "updated1", 1L }, new Object[] { "updated2", 2L });
        ContinuableFuture<TestResultSet> asyncBatchUpdate4 = executor.async()
                .batchUpdate("UPDATE test SET name = ? WHERE id = ?", parametersList, TestBatchType.LOGGED);
        assertNotNull(asyncBatchUpdate4);

        // Test async delete
        ContinuableFuture<TestResultSet> asyncDelete1 = executor.async().delete(entity);
        assertNotNull(asyncDelete1);

        ContinuableFuture<TestResultSet> asyncDelete2 = executor.async().delete(entity, Arrays.asList("name"));
        assertNotNull(asyncDelete2);

        ContinuableFuture<TestResultSet> asyncDelete3 = executor.async().delete(TestEntity.class, 1L);
        assertNotNull(asyncDelete3);

        ContinuableFuture<TestResultSet> asyncDelete4 = executor.async().delete(TestEntity.class, Arrays.asList("name"), 1L);
        assertNotNull(asyncDelete4);

        ContinuableFuture<TestResultSet> asyncDelete5 = executor.async().delete(TestEntity.class, Filters.eq("id", 1L));
        assertNotNull(asyncDelete5);

        ContinuableFuture<TestResultSet> asyncDelete6 = executor.async().delete(TestEntity.class, Arrays.asList("name"), Filters.eq("id", 1L));
        assertNotNull(asyncDelete6);

        // Test async batch delete
        ContinuableFuture<TestResultSet> asyncBatchDelete1 = executor.async().batchDelete(entities);
        assertNotNull(asyncBatchDelete1);

        ContinuableFuture<TestResultSet> asyncBatchDelete2 = executor.async().batchDelete(entities, Arrays.asList("name"));
        assertNotNull(asyncBatchDelete2);
    }

    @Test
    public void testAsyncExecute() {
        // Test async execute
        ContinuableFuture<TestResultSet> asyncExec1 = executor.async().execute("SELECT * FROM test");
        assertNotNull(asyncExec1);

        ContinuableFuture<TestResultSet> asyncExec2 = executor.async().execute("SELECT * FROM test WHERE id = ?", 1L);
        assertNotNull(asyncExec2);

        Map<String, Object> params = new HashMap<>();
        params.put("id", 1L);
        ContinuableFuture<TestResultSet> asyncExec3 = executor.async().execute("SELECT * FROM test WHERE id = :id", params);
        assertNotNull(asyncExec3);

        TestStatement stmt = new TestStatement();
        ContinuableFuture<TestResultSet> asyncExec4 = executor.async().execute(stmt);
        assertNotNull(asyncExec4);
    }

    // counterpart issues (guards against a wrong overload, dropped argument or wrong limit in the async copy).
    @Test
    public void testAsyncFacadesIssueSameCqlAsSyncCounterparts() throws Exception {
        final RecordingExecutor sync = new RecordingExecutor();
        final AsyncCassandraExecutorBase<TestRow, TestResultSet, TestStatement, TestPreparedStatement, TestBatchType> async = sync.recordingAsync();

        final TestEntity entity = new TestEntity();
        entity.setId(5L);
        entity.setName("n");
        final TestEntity other = new TestEntity();
        other.setId(6L);
        final List<TestEntity> entities = Arrays.asList(entity, other);
        final Condition cond = Filters.and(Filters.eq("id", 5L), Filters.gt("name", "a"));
        final Map<String, Object> props = new LinkedHashMap<>();
        props.put("name", "x");

        final Map<String, java.util.function.Consumer<Boolean>> ops = new LinkedHashMap<>();
        ops.put("count(cond)", a -> {
            if (a) {
                async.count(TestEntity.class, cond);
            } else {
                sync.count(TestEntity.class, cond);
            }
        });
        ops.put("count(null cond)", a -> {
            if (a) {
                async.count(TestEntity.class, null);
            } else {
                sync.count(TestEntity.class, null);
            }
        });
        ops.put("count(query)", a -> {
            if (a) {
                async.count("SELECT count(*) FROM t WHERE id = ?", 1L);
            } else {
                sync.count("SELECT count(*) FROM t WHERE id = ?", 1L);
            }
        });
        ops.put("exists(ids)", a -> {
            if (a) {
                async.exists(TestEntity.class, 5L);
            } else {
                sync.exists(TestEntity.class, 5L);
            }
        });
        ops.put("exists(cond)", a -> {
            if (a) {
                async.exists(TestEntity.class, cond);
            } else {
                sync.exists(TestEntity.class, cond);
            }
        });
        ops.put("exists(null cond)", a -> {
            if (a) {
                async.exists(TestEntity.class, (Condition) null);
            } else {
                sync.exists(TestEntity.class, (Condition) null);
            }
        });
        ops.put("findFirst(cond)", a -> {
            if (a) {
                async.findFirst(TestEntity.class, cond);
            } else {
                sync.findFirst(TestEntity.class, cond);
            }
        });
        ops.put("findFirst(props, cond)", a -> {
            if (a) {
                async.findFirst(TestEntity.class, Arrays.asList("name"), cond);
            } else {
                sync.findFirst(TestEntity.class, Arrays.asList("name"), cond);
            }
        });
        ops.put("list(cond)", a -> {
            if (a) {
                async.list(TestEntity.class, cond);
            } else {
                sync.list(TestEntity.class, cond);
            }
        });
        ops.put("list(props, cond)", a -> {
            if (a) {
                async.list(TestEntity.class, Arrays.asList("name"), cond);
            } else {
                sync.list(TestEntity.class, Arrays.asList("name"), cond);
            }
        });
        ops.put("query(cond)", a -> {
            if (a) {
                async.query(TestEntity.class, cond);
            } else {
                sync.query(TestEntity.class, cond);
            }
        });
        ops.put("query(props, cond)", a -> {
            if (a) {
                async.query(TestEntity.class, Arrays.asList("name"), cond);
            } else {
                sync.query(TestEntity.class, Arrays.asList("name"), cond);
            }
        });
        ops.put("stream(cond)", a -> {
            if (a) {
                async.stream(TestEntity.class, cond);
            } else {
                sync.stream(TestEntity.class, cond);
            }
        });
        ops.put("stream(props, cond)", a -> {
            if (a) {
                async.stream(TestEntity.class, Arrays.asList("name"), cond);
            } else {
                sync.stream(TestEntity.class, Arrays.asList("name"), cond);
            }
        });
        ops.put("queryForBoolean", a -> {
            if (a) {
                async.queryForBoolean(TestEntity.class, "name", cond);
            } else {
                sync.queryForBoolean(TestEntity.class, "name", cond);
            }
        });
        ops.put("queryForChar", a -> {
            if (a) {
                async.queryForChar(TestEntity.class, "name", cond);
            } else {
                sync.queryForChar(TestEntity.class, "name", cond);
            }
        });
        ops.put("queryForByte", a -> {
            if (a) {
                async.queryForByte(TestEntity.class, "name", cond);
            } else {
                sync.queryForByte(TestEntity.class, "name", cond);
            }
        });
        ops.put("queryForShort", a -> {
            if (a) {
                async.queryForShort(TestEntity.class, "name", cond);
            } else {
                sync.queryForShort(TestEntity.class, "name", cond);
            }
        });
        ops.put("queryForInt", a -> {
            if (a) {
                async.queryForInt(TestEntity.class, "name", cond);
            } else {
                sync.queryForInt(TestEntity.class, "name", cond);
            }
        });
        ops.put("queryForLong", a -> {
            if (a) {
                async.queryForLong(TestEntity.class, "name", cond);
            } else {
                sync.queryForLong(TestEntity.class, "name", cond);
            }
        });
        ops.put("queryForFloat", a -> {
            if (a) {
                async.queryForFloat(TestEntity.class, "name", cond);
            } else {
                sync.queryForFloat(TestEntity.class, "name", cond);
            }
        });
        ops.put("queryForDouble", a -> {
            if (a) {
                async.queryForDouble(TestEntity.class, "name", cond);
            } else {
                sync.queryForDouble(TestEntity.class, "name", cond);
            }
        });
        ops.put("queryForString", a -> {
            if (a) {
                async.queryForString(TestEntity.class, "name", cond);
            } else {
                sync.queryForString(TestEntity.class, "name", cond);
            }
        });
        ops.put("queryForDate", a -> {
            if (a) {
                async.queryForDate(TestEntity.class, "name", cond);
            } else {
                sync.queryForDate(TestEntity.class, "name", cond);
            }
        });
        ops.put("queryForDate(valueClass)", a -> {
            if (a) {
                async.queryForDate(TestEntity.class, java.sql.Timestamp.class, "name", cond);
            } else {
                sync.queryForDate(TestEntity.class, java.sql.Timestamp.class, "name", cond);
            }
        });
        ops.put("queryForSingleValue", a -> {
            if (a) {
                async.queryForSingleValue(TestEntity.class, String.class, "name", cond);
            } else {
                sync.queryForSingleValue(TestEntity.class, String.class, "name", cond);
            }
        });
        ops.put("queryForSingleNonNull", a -> {
            if (a) {
                async.queryForSingleNonNull(TestEntity.class, String.class, "name", cond);
            } else {
                sync.queryForSingleNonNull(TestEntity.class, String.class, "name", cond);
            }
        });
        ops.put("insert(entity)", a -> {
            if (a) {
                async.insert(entity);
            } else {
                sync.insert(entity);
            }
        });
        ops.put("insert(class, props)", a -> {
            if (a) {
                async.insert(TestEntity.class, props);
            } else {
                sync.insert(TestEntity.class, props);
            }
        });
        ops.put("update(entity)", a -> {
            if (a) {
                async.update(entity);
            } else {
                sync.update(entity);
            }
        });
        ops.put("update(entity, props)", a -> {
            if (a) {
                async.update(entity, Arrays.asList("name"));
            } else {
                sync.update(entity, Arrays.asList("name"));
            }
        });
        ops.put("update(class, props, cond)", a -> {
            if (a) {
                async.update(TestEntity.class, props, cond);
            } else {
                sync.update(TestEntity.class, props, cond);
            }
        });
        ops.put("update(query)", a -> {
            if (a) {
                async.update("UPDATE t SET name = ? WHERE id = ?", "x", 1L);
            } else {
                sync.update("UPDATE t SET name = ? WHERE id = ?", "x", 1L);
            }
        });
        ops.put("delete(entity)", a -> {
            if (a) {
                async.delete(entity);
            } else {
                sync.delete(entity);
            }
        });
        ops.put("delete(entity, props)", a -> {
            if (a) {
                async.delete(entity, Arrays.asList("name"));
            } else {
                sync.delete(entity, Arrays.asList("name"));
            }
        });
        ops.put("delete(class, ids)", a -> {
            if (a) {
                async.delete(TestEntity.class, 5L);
            } else {
                sync.delete(TestEntity.class, 5L);
            }
        });
        ops.put("delete(class, props, ids)", a -> {
            if (a) {
                async.delete(TestEntity.class, Arrays.asList("name"), 5L);
            } else {
                sync.delete(TestEntity.class, Arrays.asList("name"), 5L);
            }
        });
        ops.put("delete(class, cond)", a -> {
            if (a) {
                async.delete(TestEntity.class, cond);
            } else {
                sync.delete(TestEntity.class, cond);
            }
        });
        ops.put("delete(class, props, cond)", a -> {
            if (a) {
                async.delete(TestEntity.class, Arrays.asList("name"), cond);
            } else {
                sync.delete(TestEntity.class, Arrays.asList("name"), cond);
            }
        });
        ops.put("batchDelete(entities)", a -> {
            if (a) {
                async.batchDelete(entities);
            } else {
                sync.batchDelete(entities);
            }
        });
        ops.put("batchDelete(entities, props)", a -> {
            if (a) {
                async.batchDelete(entities, Arrays.asList("name"));
            } else {
                sync.batchDelete(entities, Arrays.asList("name"));
            }
        });
        ops.put("exists(query)", a -> {
            if (a) {
                async.exists("SELECT id FROM t WHERE id = ? LIMIT 1", 1L);
            } else {
                sync.exists("SELECT id FROM t WHERE id = ? LIMIT 1", 1L);
            }
        });
        ops.put("list(query)", a -> {
            if (a) {
                async.list("SELECT * FROM t WHERE id = ?", 1L);
            } else {
                sync.list("SELECT * FROM t WHERE id = ?", 1L);
            }
        });
        ops.put("query(query)", a -> {
            if (a) {
                async.query("SELECT * FROM t WHERE id = ?", 1L);
            } else {
                sync.query("SELECT * FROM t WHERE id = ?", 1L);
            }
        });
        ops.put("stream(query)", a -> {
            if (a) {
                async.stream("SELECT * FROM t WHERE id = ?", 1L);
            } else {
                sync.stream("SELECT * FROM t WHERE id = ?", 1L);
            }
        });
        ops.put("findFirst(query)", a -> {
            if (a) {
                async.findFirst("SELECT * FROM t WHERE id = ?", 1L);
            } else {
                sync.findFirst("SELECT * FROM t WHERE id = ?", 1L);
            }
        });

        for (final Map.Entry<String, java.util.function.Consumer<Boolean>> op : ops.entrySet()) {
            sync.recorded.clear();
            op.getValue().accept(false);
            final List<String> fromSync = new ArrayList<>(sync.recorded);

            sync.recorded.clear();
            op.getValue().accept(true);
            final List<String> fromAsync = new ArrayList<>(sync.recorded);

            assertEquals(1, fromSync.size(), op.getKey() + " (sync) -> " + fromSync);
            assertEquals(fromSync, fromAsync, op.getKey());
        }
    }

    @Test
    public void testAsyncFindFirst() {
        // Test async findFirst
        ContinuableFuture<Optional<TestEntity>> asyncFind1 = executor.async().findFirst(TestEntity.class, Filters.eq("id", 1L));
        assertNotNull(asyncFind1);

        ContinuableFuture<Optional<TestEntity>> asyncFind2 = executor.async().findFirst(TestEntity.class, Arrays.asList("id", "name"), Filters.eq("id", 1L));
        assertNotNull(asyncFind2);

        ContinuableFuture<Optional<Map<String, Object>>> asyncFind3 = executor.async().findFirst("SELECT * FROM test WHERE id = ?", 1L);
        assertNotNull(asyncFind3);

        ContinuableFuture<Optional<TestEntity>> asyncFind4 = executor.async().findFirst(TestEntity.class, "SELECT * FROM test WHERE id = ?", 1L);
        assertNotNull(asyncFind4);
    }

    @Test
    public void testAsyncFindFirst_nullMappedRow_throwsExecutionExceptionCausedByNPE() {
        final TestCassandraExecutor exec = new TestCassandraExecutor() {
            @SuppressWarnings("unchecked")
            @Override
            protected <T> Function<TestRow, T> createRowMapper(Class<T> targetClass) {
                return row -> (T) null;
            }
        };

        // The mapping runs inside ContinuableFuture.get(), which reports the Optional.of(null) failure
        // as an ExecutionException wrapping the NullPointerException.
        final ExecutionException ex = assertThrows(ExecutionException.class,
                () -> exec.async().findFirst(String.class, "SELECT name FROM t WHERE id = ?", 1L).get());

        assertTrue(ex.getCause() instanceof NullPointerException, String.valueOf(ex.getCause()));
    }

    // and the whole-row lookup must project the same columns as the synchronous prepareQuery.
    @Test
    public void testAsyncGetAndGettUseDuplicateDetectingLimit() throws Exception {
        final RecordingExecutor sync = new RecordingExecutor();
        final AsyncCassandraExecutorBase<TestRow, TestResultSet, TestStatement, TestPreparedStatement, TestBatchType> async = sync.recordingAsync();
        final Condition cond = Filters.eq("id", 5L);
        final SP expected = sync.exposedPrepareQuery(TestEntity.class, Arrays.asList("name"), cond, 2);
        final String expectedRecord = expected.query() + " " + expected.parameters();

        for (final Runnable op : new Runnable[] { () -> async.get(TestEntity.class, Arrays.asList("name"), cond),
                () -> async.gett(TestEntity.class, Arrays.asList("name"), cond), () -> async.get(TestEntity.class, Arrays.asList("name"), 5L),
                () -> async.gett(TestEntity.class, Arrays.asList("name"), 5L) }) {
            sync.recorded.clear();
            op.run();
            assertEquals(Arrays.asList(expectedRecord), sync.recorded);
        }

        assertTrue(expected.query().endsWith("LIMIT 2"), expected.query());

        // count never carries a LIMIT (it would cap the aggregate itself)
        sync.recorded.clear();
        async.count(TestEntity.class, cond);
        assertTrue(!sync.recorded.get(0).contains("LIMIT"), sync.recorded.get(0));
        assertTrue(sync.recorded.get(0).startsWith("SELECT count(*) FROM"), sync.recorded.get(0));
    }

    @Test
    public void testAsyncGetReportsDuplicateRowsThroughTheFuture() {
        final TestCassandraExecutor twoRows = new TestCassandraExecutor() {
            @Override
            protected <T> T fetchOnlyOne(Class<T> targetClass, TestResultSet resultSet) {
                throw new DuplicateResultException();
            }
        };

        final ExecutionException e1 = assertThrows(ExecutionException.class, () -> twoRows.async().get(TestEntity.class, Filters.eq("id", 1L)).get());
        assertTrue(e1.getCause() instanceof DuplicateResultException, String.valueOf(e1.getCause()));

        final ExecutionException e2 = assertThrows(ExecutionException.class, () -> twoRows.async().gett(TestEntity.class, 1L).get());
        assertTrue(e2.getCause() instanceof DuplicateResultException, String.valueOf(e2.getCause()));
    }

    @Test
    public void testAsyncMethods() {
        // Test async get methods
        ContinuableFuture<Optional<TestEntity>> asyncGet1 = executor.async().get(TestEntity.class, 1L);
        assertNotNull(asyncGet1);

        ContinuableFuture<Optional<TestEntity>> asyncGet2 = executor.async().get(TestEntity.class, Arrays.asList("id"), 1L);
        assertNotNull(asyncGet2);

        ContinuableFuture<Optional<TestEntity>> asyncGet3 = executor.async().get(TestEntity.class, Filters.eq("id", 1L));
        assertNotNull(asyncGet3);

        ContinuableFuture<Optional<TestEntity>> asyncGet4 = executor.async().get(TestEntity.class, Arrays.asList("id"), Filters.eq("id", 1L));
        assertNotNull(asyncGet4);

        // Test async gett methods
        ContinuableFuture<TestEntity> asyncGett1 = executor.async().gett(TestEntity.class, 1L);
        assertNotNull(asyncGett1);

        ContinuableFuture<TestEntity> asyncGett2 = executor.async().gett(TestEntity.class, Arrays.asList("id"), 1L);
        assertNotNull(asyncGett2);

        ContinuableFuture<TestEntity> asyncGett3 = executor.async().gett(TestEntity.class, Filters.eq("id", 1L));
        assertNotNull(asyncGett3);

        ContinuableFuture<TestEntity> asyncGett4 = executor.async().gett(TestEntity.class, Arrays.asList("id"), Filters.eq("id", 1L));
        assertNotNull(asyncGett4);
    }

    @Test
    public void testAsyncQueryForPrimitiveTypes() {
        Condition condition = Filters.eq("id", 1L);

        // Test async queryForBoolean
        ContinuableFuture<OptionalBoolean> asyncBool1 = executor.async().queryForBoolean(TestEntity.class, "active", condition);
        assertNotNull(asyncBool1);

        ContinuableFuture<OptionalBoolean> asyncBool2 = executor.async().queryForBoolean("SELECT active FROM test WHERE id = ?", 1L);
        assertNotNull(asyncBool2);

        // Test async queryForChar
        ContinuableFuture<OptionalChar> asyncChar1 = executor.async().queryForChar(TestEntity.class, "grade", condition);
        assertNotNull(asyncChar1);

        ContinuableFuture<OptionalChar> asyncChar2 = executor.async().queryForChar("SELECT grade FROM test WHERE id = ?", 1L);
        assertNotNull(asyncChar2);

        // Test async queryForByte
        ContinuableFuture<OptionalByte> asyncByte1 = executor.async().queryForByte(TestEntity.class, "level", condition);
        assertNotNull(asyncByte1);

        ContinuableFuture<OptionalByte> asyncByte2 = executor.async().queryForByte("SELECT level FROM test WHERE id = ?", 1L);
        assertNotNull(asyncByte2);

        // Test async queryForShort
        ContinuableFuture<OptionalShort> asyncShort1 = executor.async().queryForShort(TestEntity.class, "count", condition);
        assertNotNull(asyncShort1);

        ContinuableFuture<OptionalShort> asyncShort2 = executor.async().queryForShort("SELECT count FROM test WHERE id = ?", 1L);
        assertNotNull(asyncShort2);

        // Test async queryForInt
        ContinuableFuture<OptionalInt> asyncInt1 = executor.async().queryForInt(TestEntity.class, "age", condition);
        assertNotNull(asyncInt1);

        ContinuableFuture<OptionalInt> asyncInt2 = executor.async().queryForInt("SELECT age FROM test WHERE id = ?", 1L);
        assertNotNull(asyncInt2);

        // Test async queryForLong
        ContinuableFuture<OptionalLong> asyncLong1 = executor.async().queryForLong(TestEntity.class, "id", condition);
        assertNotNull(asyncLong1);

        ContinuableFuture<OptionalLong> asyncLong2 = executor.async().queryForLong("SELECT id FROM test WHERE id = ?", 1L);
        assertNotNull(asyncLong2);

        // Test async queryForFloat
        ContinuableFuture<OptionalFloat> asyncFloat1 = executor.async().queryForFloat(TestEntity.class, "score", condition);
        assertNotNull(asyncFloat1);

        ContinuableFuture<OptionalFloat> asyncFloat2 = executor.async().queryForFloat("SELECT score FROM test WHERE id = ?", 1L);
        assertNotNull(asyncFloat2);

        // Test async queryForDouble
        ContinuableFuture<OptionalDouble> asyncDouble1 = executor.async().queryForDouble(TestEntity.class, "price", condition);
        assertNotNull(asyncDouble1);

        ContinuableFuture<OptionalDouble> asyncDouble2 = executor.async().queryForDouble("SELECT price FROM test WHERE id = ?", 1L);
        assertNotNull(asyncDouble2);

        // Test async queryForString
        ContinuableFuture<Nullable<String>> asyncString1 = executor.async().queryForString(TestEntity.class, "name", condition);
        assertNotNull(asyncString1);

        ContinuableFuture<Nullable<String>> asyncString2 = executor.async().queryForString("SELECT name FROM test WHERE id = ?", 1L);
        assertNotNull(asyncString2);

        // Test async queryForDate
        ContinuableFuture<Nullable<Date>> asyncDate1 = executor.async().queryForDate(TestEntity.class, "createdDate", condition);
        assertNotNull(asyncDate1);

        ContinuableFuture<Nullable<java.sql.Date>> asyncDate2 = executor.async().queryForDate(TestEntity.class, java.sql.Date.class, "createdDate", condition);
        assertNotNull(asyncDate2);

        ContinuableFuture<Nullable<Date>> asyncDate3 = executor.async().queryForDate("SELECT created_date FROM test WHERE id = ?", 1L);
        assertNotNull(asyncDate3);

        ContinuableFuture<Nullable<java.sql.Date>> asyncDate4 = executor.async()
                .queryForDate(java.sql.Date.class, "SELECT created_date FROM test WHERE id = ?", 1L);
        assertNotNull(asyncDate4);
    }

    @Test
    public void testAsyncqueryForSingleValue() {
        // Test async queryForSingleValue
        ContinuableFuture<Nullable<String>> asyncResult1 = executor.async().queryForSingleValue(TestEntity.class, String.class, "name", Filters.eq("id", 1L));
        assertNotNull(asyncResult1);

        ContinuableFuture<Nullable<String>> asyncResult2 = executor.async().queryForSingleValue(String.class, "SELECT name FROM test WHERE id = ?", 1L);
        assertNotNull(asyncResult2);

        // Test async queryForSingleNonNull
        ContinuableFuture<Optional<String>> asyncResult3 = executor.async().queryForSingleNonNull(TestEntity.class, String.class, "name", Filters.eq("id", 1L));
        assertNotNull(asyncResult3);

        ContinuableFuture<Optional<String>> asyncResult4 = executor.async().queryForSingleNonNull(String.class, "SELECT name FROM test WHERE id = ?", 1L);
        assertNotNull(asyncResult4);
    }

    @Test
    public void testAsyncQueryMethods() {
        // Test async exists
        ContinuableFuture<Boolean> asyncExists1 = executor.async().exists(TestEntity.class, 1L);
        assertNotNull(asyncExists1);

        ContinuableFuture<Boolean> asyncExists2 = executor.async().exists(TestEntity.class, Filters.eq("id", 1L));
        assertNotNull(asyncExists2);

        ContinuableFuture<Boolean> asyncExists3 = executor.async().exists("SELECT * FROM test WHERE id = ? LIMIT 1", 1L);
        assertNotNull(asyncExists3);

        // Test async count
        ContinuableFuture<Long> asyncCount1 = executor.async().count(TestEntity.class, Filters.eq("status", "active"));
        assertNotNull(asyncCount1);

        ContinuableFuture<Long> asyncCount2 = executor.async().count("SELECT COUNT(*) FROM test WHERE status = ?", "active");
        assertNotNull(asyncCount2);

        // Test async list
        ContinuableFuture<List<TestEntity>> asyncList1 = executor.async().list(TestEntity.class, Filters.eq("status", "active"));
        assertNotNull(asyncList1);

        ContinuableFuture<List<TestEntity>> asyncList2 = executor.async().list(TestEntity.class, Arrays.asList("id", "name"), Filters.eq("status", "active"));
        assertNotNull(asyncList2);

        ContinuableFuture<List<Map<String, Object>>> asyncList3 = executor.async().list("SELECT * FROM test WHERE status = ?", "active");
        assertNotNull(asyncList3);

        ContinuableFuture<List<TestEntity>> asyncList4 = executor.async().list(TestEntity.class, "SELECT * FROM test WHERE status = ?", "active");
        assertNotNull(asyncList4);

        // Test async query
        ContinuableFuture<Dataset> asyncQuery1 = executor.async().query(TestEntity.class, Filters.eq("status", "active"));
        assertNotNull(asyncQuery1);

        ContinuableFuture<Dataset> asyncQuery2 = executor.async().query(TestEntity.class, Arrays.asList("id", "name"), Filters.eq("status", "active"));
        assertNotNull(asyncQuery2);

        ContinuableFuture<Dataset> asyncQuery3 = executor.async().query("SELECT * FROM test WHERE status = ?", "active");
        assertNotNull(asyncQuery3);

        ContinuableFuture<Dataset> asyncQuery4 = executor.async().query(TestEntity.class, "SELECT * FROM test WHERE status = ?", "active");
        assertNotNull(asyncQuery4);
    }

    @Test
    public void testAsyncRejectsNullTargetClassOnTheCallingThread() {
        final AsyncCassandraExecutorBase<TestRow, TestResultSet, TestStatement, TestPreparedStatement, TestBatchType> async = executor.async();

        assertThrows(IllegalArgumentException.class, () -> async.list((Class<Object>) null, "SELECT * FROM t"));
        assertThrows(IllegalArgumentException.class, () -> async.stream((Class<Object>) null, "SELECT * FROM t"));
        assertThrows(IllegalArgumentException.class, () -> async.stream((Class<Object>) null, new TestStatement()));
        assertThrows(IllegalArgumentException.class, () -> async.findFirst((Class<Object>) null, "SELECT * FROM t"));
    }

    @Test
    public void testAsyncRejectsNullValueClassOnTheCallingThread() {
        final AsyncCassandraExecutorBase<TestRow, TestResultSet, TestStatement, TestPreparedStatement, TestBatchType> async = executor.async();

        assertThrows(IllegalArgumentException.class, () -> async.queryForSingleValue((Class<Object>) null, "SELECT v FROM t"));
        assertThrows(IllegalArgumentException.class, () -> async.queryForSingleNonNull((Class<Object>) null, "SELECT v FROM t"));
    }

    @Test
    public void testAsyncSingleValueFacadesEmptyAndPresentNullSemantics() throws Exception {
        // no row -> empty flavors
        final TestCassandraExecutor noRows = new TestCassandraExecutor() {
            @Override
            public TestResultSet execute(String query, Object... parameters) {
                return new TestResultSet() {
                    @Override
                    public Iterator<TestRow> iterator() {
                        return new ArrayList<TestRow>().iterator();
                    }
                };
            }
        };
        final AsyncCassandraExecutorBase<TestRow, TestResultSet, TestStatement, TestPreparedStatement, TestBatchType> asyncNoRows = new AsyncCassandraExecutorBase<>(
                noRows) {
            @Override
            public ContinuableFuture<TestResultSet> execute(String query) {
                return ContinuableFuture.completed(noRows.execute(query));
            }

            @Override
            public ContinuableFuture<TestResultSet> execute(String query, Object... parameters) {
                return ContinuableFuture.completed(noRows.execute(query, parameters));
            }

            @Override
            public ContinuableFuture<TestResultSet> execute(String query, Map<String, Object> parameters) {
                return ContinuableFuture.completed(noRows.execute(query, parameters));
            }

            @Override
            public ContinuableFuture<TestResultSet> execute(TestStatement statement) {
                return ContinuableFuture.completed(noRows.execute(statement));
            }
        };

        assertTrue(asyncNoRows.queryForBoolean("SELECT v FROM t").get().isEmpty());
        assertTrue(asyncNoRows.queryForLong("SELECT v FROM t").get().isEmpty());
        assertTrue(asyncNoRows.queryForString("SELECT v FROM t").get().isEmpty());
        assertTrue(asyncNoRows.queryForSingleValue(String.class, "SELECT v FROM t").get().isEmpty());
        assertTrue(asyncNoRows.queryForSingleNonNull(String.class, "SELECT v FROM t").get().isEmpty());
        assertTrue(asyncNoRows.findFirst("SELECT v FROM t").get().isEmpty());
        assertEquals(0L, asyncNoRows.count("SELECT count(*) FROM t").get().longValue());
        assertEquals(0L, asyncNoRows.count(TestEntity.class, Filters.eq("id", 1L)).get().longValue());
        assertEquals(false, asyncNoRows.exists("SELECT v FROM t").get());
        assertEquals(false, asyncNoRows.exists(TestEntity.class, 1L).get());

        // one row whose first column is NULL (the stub readFirstColumn returns null) -> present flavors
        final AsyncCassandraExecutorBase<TestRow, TestResultSet, TestStatement, TestPreparedStatement, TestBatchType> asyncNullRow = executor.async();
        final OptionalBoolean b = asyncNullRow.queryForBoolean("SELECT v FROM t").get();
        assertTrue(b.isPresent());
        assertEquals(false, b.get());
        final OptionalLong l = asyncNullRow.queryForLong("SELECT v FROM t").get();
        assertTrue(l.isPresent());
        assertEquals(0L, l.get());
        final Nullable<String> s = asyncNullRow.queryForString("SELECT v FROM t").get();
        assertTrue(s.isPresent());
        assertTrue(s.isNull());
        assertEquals(0L, asyncNullRow.count("SELECT count(*) FROM t").get().longValue());
        assertEquals(true, asyncNullRow.exists("SELECT v FROM t").get());

        final ExecutionException e = assertThrows(ExecutionException.class, () -> asyncNullRow.queryForSingleNonNull(String.class, "SELECT v FROM t").get());
        assertTrue(e.getCause() instanceof NullPointerException, String.valueOf(e.getCause()));
    }

    @Test
    public void testAsyncStream() {
        // Test async stream
        ContinuableFuture<Stream<TestEntity>> asyncStream1 = executor.async().stream(TestEntity.class, Filters.eq("status", "active"));
        assertNotNull(asyncStream1);

        ContinuableFuture<Stream<TestEntity>> asyncStream2 = executor.async()
                .stream(TestEntity.class, Arrays.asList("id", "name"), Filters.eq("status", "active"));
        assertNotNull(asyncStream2);

        ContinuableFuture<Stream<Object[]>> asyncStream3 = executor.async().stream("SELECT * FROM test WHERE status = ?", "active");
        assertNotNull(asyncStream3);

        ContinuableFuture<Stream<TestEntity>> asyncStream4 = executor.async().stream(TestEntity.class, "SELECT * FROM test WHERE status = ?", "active");
        assertNotNull(asyncStream4);

        TestStatement stmt = new TestStatement();
        ContinuableFuture<Stream<TestEntity>> asyncStream5 = executor.async().stream(TestEntity.class, stmt);
        assertNotNull(asyncStream5);
    }

    // ---- 2026-10-24 r2 sliceG ----

    /**
     * The async update(entity) / batchUpdate(entities, type) build their default property list themselves: for an
     * {@code @Entity}-annotated Map subclass (a bean class to Beans.isBeanClass) they used the class's getters (a bogus "empty"
     * column, the entries lost) while the sync twins set the Map's non-key entries.
     */
    @Test
    public void testR2SliceG_asyncUpdateOfMapEntitySetsItsEntriesLikeSync() {
        final R2GKeyedMapEntity entity = new R2GKeyedMapEntity();
        entity.setId(7L);
        entity.put("name", "x");
        entity.put("displayName", "Ada");

        final R2GKeyedMapEntity keyOnly = new R2GKeyedMapEntity();
        keyOnly.setId(1L);

        final RecordingExecutor recording = new RecordingExecutor() {
            @Override
            protected TestStatement prepareBatchUpdateStatement(final java.util.Collection<?> entities, final java.util.Collection<String> propNamesToUpdate,
                    final TestBatchType type) {
                for (final Object e : entities) {
                    final SP cp = prepareUpdate(e, propNamesToUpdate);
                    recorded.add("batch " + cp.query() + " " + cp.parameters());
                }

                return new TestStatement();
            }
        };
        final AsyncCassandraExecutorBase<TestRow, TestResultSet, TestStatement, TestPreparedStatement, TestBatchType> async = recording.recordingAsync();

        recording.update(entity);
        async.update(entity);
        recording.batchUpdate(List.of(entity), TestBatchType.LOGGED);
        async.batchUpdate(List.of(entity), TestBatchType.LOGGED);

        final String update = "UPDATE r2g_map_rows SET name = :name, display_name = :displayName WHERE id = :id [x, Ada, 7]";
        final IllegalArgumentException syncKeyOnly = assertThrows(IllegalArgumentException.class, () -> recording.update(keyOnly));

        // The recording async facade also logs the execution of the batch statement ("statement").
        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertEquals(List.of(update, update, "batch " + update, "batch " + update, "statement"), recording.recorded),
                () -> assertEquals(syncKeyOnly.getMessage(), assertThrows(IllegalArgumentException.class, () -> async.update(keyOnly)).getMessage()),
                () -> assertEquals(syncKeyOnly.getMessage(),
                        assertThrows(IllegalArgumentException.class, () -> async.batchUpdate(List.of(keyOnly), TestBatchType.LOGGED)).getMessage()));
    }

    /** Pins the "or is not a bean class" @throws clauses: a non-bean target class fails at the call site, before any CQL is run. */
    @Test
    public void testR2SliceG_asyncWritesRejectANonBeanTargetClassAtTheCallSite() {
        final RecordingExecutor recording = new RecordingExecutor();
        final AsyncCassandraExecutorBase<TestRow, TestResultSet, TestStatement, TestPreparedStatement, TestBatchType> async = recording.recordingAsync();
        final Map<String, Object> props = new LinkedHashMap<>();
        props.put("name", "x");
        final Condition cond = Filters.eq("id", 1L);

        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertTrue(
                        assertThrows(IllegalArgumentException.class, () -> async.insert(String.class, props)).getMessage().startsWith("Not a bean class")),
                () -> assertTrue(assertThrows(IllegalArgumentException.class, () -> async.update(String.class, props, cond)).getMessage()
                        .startsWith("Not a bean class")),
                () -> assertTrue(
                        assertThrows(IllegalArgumentException.class, () -> async.delete(String.class, cond)).getMessage().startsWith("Not a bean class")),
                () -> assertTrue(assertThrows(IllegalArgumentException.class, () -> async.delete(String.class, Arrays.asList("name"), cond)).getMessage()
                        .startsWith("Not a bean class")),
                () -> assertEquals(List.of(), recording.recorded));
    }

    @com.landawn.abacus.annotation.Entity
    @com.landawn.abacus.annotation.Table("r2g_map_rows")
    public static class R2GKeyedMapEntity extends LinkedHashMap<String, Object> {
        private static final long serialVersionUID = 1L;

        public Long getId() {
            return (Long) get("id");
        }

        public void setId(final Long id) {
            put("id", id);
        }
    }
}
