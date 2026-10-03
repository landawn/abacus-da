/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.function.Function;
import com.landawn.abacus.util.stream.Stream;
import com.landawn.abacus.util.u.Nullable;
import com.landawn.abacus.util.u.Optional;

/**
 * Mockito-based tests for the v4 driver (DataStax OSS) {@link AsyncCassandraExecutor}.
 */
public class AsyncCassandraExecutorTest extends TestBase {

    private CassandraExecutor mockExecutor;
    private CqlSession mockSession;
    private AsyncResultSet mockAsyncRS;
    private BoundStatement mockStatement;
    private AsyncCassandraExecutor async;

    @BeforeEach
    public void setUp() {
        mockExecutor = mock(CassandraExecutor.class);
        mockSession = mock(CqlSession.class);
        mockAsyncRS = mock(AsyncResultSet.class);
        mockStatement = mock(BoundStatement.class);

        when(mockExecutor.session()).thenReturn(mockSession);
        // Empty current page so ResultSets.wrap returns an empty iterable.
        when(mockAsyncRS.currentPage()).thenReturn(Collections.<Row> emptyList());
        when(mockAsyncRS.hasMorePages()).thenReturn(false);

        async = new AsyncCassandraExecutor(mockExecutor);
    }

    /** Helper: CompletionStage that completes immediately. */
    private CompletionStage<AsyncResultSet> completed(AsyncResultSet rs) {
        return CompletableFuture.completedFuture(rs);
    }

    @Test
    public void testWrappedResultSetReusesItsConsumptionCursor() {
        final Row first = mock(Row.class);
        final Row second = mock(Row.class);
        final AsyncResultSet asyncResultSet = mock(AsyncResultSet.class);
        when(asyncResultSet.currentPage()).thenReturn(Arrays.asList(first, second));
        when(asyncResultSet.hasMorePages()).thenReturn(false);

        final ResultSet resultSet = ResultSets.wrap(asyncResultSet);
        final Iterator<Row> iterator = resultSet.iterator();

        assertSame(first, iterator.next());
        assertSame(iterator, resultSet.iterator());
        assertEquals(Arrays.asList(second), resultSet.all());
    }

    @Test
    public void testWrappedResultSetRestoresInterruptWhenFetchingNextPage() {
        final AsyncResultSet asyncResultSet = mock(AsyncResultSet.class);
        when(asyncResultSet.currentPage()).thenReturn(Collections.emptyList());
        when(asyncResultSet.hasMorePages()).thenReturn(true);
        when(asyncResultSet.fetchNextPage()).thenReturn(new CompletableFuture<>());

        final ResultSet resultSet = ResultSets.wrap(asyncResultSet);

        Thread.currentThread().interrupt();

        try {
            assertThrows(RuntimeException.class, () -> resultSet.iterator().hasNext());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            // Do not leak the deliberately-set interrupt into subsequent tests.
            Thread.interrupted();
        }
    }

    @Test
    public void testSync_ReturnsUnderlyingExecutor() {
        assertSame(mockExecutor, async.sync());
    }

    @Test
    public void testConstructor_StoresExecutor() {
        AsyncCassandraExecutor a = new AsyncCassandraExecutor(mockExecutor);
        assertNotNull(a);
        assertSame(mockExecutor, a.sync());
    }

    @Test
    public void testConstructor_rejectsNullExecutor() {
        assertThrows(IllegalArgumentException.class, () -> new AsyncCassandraExecutor(null));
    }

    @Test
    public void testNullRequiredArguments_ThrowIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> async.execute((Statement<?>) null));
        assertThrows(IllegalArgumentException.class, () -> async.execute((com.landawn.abacus.query.AbstractQueryBuilder.SP) null));
        assertThrows(IllegalArgumentException.class, () -> async.stream(Object.class, (Statement<?>) null));
        assertThrows(IllegalArgumentException.class, () -> async.get((Class<Object>) null, 1L));
        assertThrows(IllegalArgumentException.class, () -> async.get((Class<Object>) null, Arrays.asList("id"), 1L));
        assertThrows(IllegalArgumentException.class, () -> async.gett((Class<Object>) null, 1L));
        assertThrows(IllegalArgumentException.class, () -> async.gett((Class<Object>) null, Arrays.asList("id"), 1L));
        assertThrows(IllegalArgumentException.class, () -> async.exists((Class<?>) null, 1L));
        assertThrows(IllegalArgumentException.class, () -> async.exists((Class<?>) null, (com.landawn.abacus.query.condition.Condition) null));
        assertThrows(IllegalArgumentException.class, () -> async.delete((Class<?>) null, 1L));
        assertThrows(IllegalArgumentException.class, () -> async.delete((Class<?>) null, Arrays.asList("name"), 1L));
    }

    @Test
    public void testExecute_StringOnly_DelegatesToSession() {
        when(mockExecutor.prepareStatement("SELECT * FROM t")).thenReturn(mockStatement);
        when(mockSession.executeAsync(mockStatement)).thenReturn(completed(mockAsyncRS));

        ContinuableFuture<ResultSet> future = async.execute("SELECT * FROM t");

        assertNotNull(future);
        verify(mockSession).executeAsync(mockStatement);
    }

    @Test
    public void testExecute_StringAndParams_DelegatesToSession() {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));

        ContinuableFuture<ResultSet> future = async.execute("SELECT * FROM t WHERE id = ?", 1);

        assertNotNull(future);
        verify(mockSession).executeAsync((Statement<?>) mockStatement);
    }

    @Test
    public void testExecute_StringAndMapParams_DelegatesToSession() {
        Map<String, Object> params = new HashMap<>();
        params.put("id", 1);
        when(mockExecutor.prepareStatement(anyString(), eq(params))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));

        ContinuableFuture<ResultSet> future = async.execute("SELECT * FROM t WHERE id = :id", params);

        assertNotNull(future);
        verify(mockSession).executeAsync((Statement<?>) mockStatement);
    }

    @Test
    public void testExecute_Statement_DelegatesToSession() {
        Statement<?> stmt = mock(Statement.class);
        when(mockSession.executeAsync(eq(stmt))).thenReturn(completed(mockAsyncRS));

        ContinuableFuture<ResultSet> future = async.execute(stmt);

        assertNotNull(future);
        verify(mockSession).executeAsync(stmt);
    }

    @Test
    public void testExecute_String_FutureGetReturnsResultSet() throws Exception {
        when(mockExecutor.prepareStatement("SELECT * FROM t")).thenReturn(mockStatement);
        when(mockSession.executeAsync(mockStatement)).thenReturn(completed(mockAsyncRS));

        ResultSet result = async.execute("SELECT * FROM t").get();
        assertNotNull(result);
    }

    @Test
    public void testStream_StringAndParams_ReturnsStreamFuture() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Object[]> mapper = (Function) (Function<Row, Object[]>) row -> new Object[] {};
        when(mockExecutor.createRowMapper(eq(Object[].class))).thenReturn(mapper);

        ContinuableFuture<Stream<Object[]>> future = async.stream("SELECT * FROM t WHERE id = ?", 1);
        assertNotNull(future);
        assertEquals(0, future.get().count());
    }

    @Test
    public void testStream_StringNoParams_ReturnsStreamFuture() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Object[]> mapper = (Function) (Function<Row, Object[]>) row -> new Object[] {};
        when(mockExecutor.createRowMapper(eq(Object[].class))).thenReturn(mapper);

        ContinuableFuture<Stream<Object[]>> future = async.stream("SELECT * FROM t");
        assertNotNull(future);
        assertEquals(0, future.get().count());
    }

    @Test
    public void testStream_WithRowMapper_String() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));

        final BiFunction<ColumnDefinitions, Row, String> rowMapper = (cd, r) -> "x";
        when(mockExecutor.createRowMapper(eq(rowMapper))).thenReturn(r -> "x");

        ContinuableFuture<Stream<String>> future = async.stream("SELECT * FROM t", rowMapper, 1);
        assertNotNull(future);
        assertEquals(0, future.get().count());
    }

    @Test
    public void testStream_WithRowMapper_Statement() throws Exception {
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        final BiFunction<ColumnDefinitions, Row, String> rowMapper = (cd, r) -> "x";
        when(mockExecutor.createRowMapper(eq(rowMapper))).thenReturn(r -> "x");

        Statement<?> stmt = mock(Statement.class);
        ContinuableFuture<Stream<String>> future = async.stream(stmt, rowMapper);
        assertNotNull(future);
        assertEquals(0, future.get().count());
    }

    @Test
    public void testStream_NullRowMapper_ThrowsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> async.stream("SELECT * FROM t", (BiFunction<ColumnDefinitions, Row, Object>) null));
        assertThrows(IllegalArgumentException.class, () -> async.stream((Statement<?>) null, (BiFunction<ColumnDefinitions, Row, Object>) null));
    }

    @Test
    public void testFindFirst_WithEmptyResultSet_ReturnsEmpty() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, String> mapper = (Function) (Function<Row, String>) row -> "v";
        when(mockExecutor.createRowMapper(eq(String.class))).thenReturn(mapper);

        Optional<String> result = async.findFirst(String.class, "SELECT * FROM t WHERE id = ?", 1).get();
        assertTrue(result.isEmpty());
    }

    @Test
    public void testFindFirst_WithRow_ReturnsValue() throws Exception {
        Row row = mock(Row.class);
        when(mockAsyncRS.currentPage()).thenReturn(Arrays.asList(row));
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, String> mapper = (Function) (Function<Row, String>) r -> "hello";
        when(mockExecutor.createRowMapper(eq(String.class))).thenReturn(mapper);

        Optional<String> result = async.findFirst(String.class, "SELECT name FROM t WHERE id = ?", 1).get();
        assertTrue(result.isPresent());
        assertEquals("hello", result.get());
    }

    @Test
    public void testQueryForSingleValue_Empty_ReturnsNullable() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Integer> mapper = (Function) (Function<Row, Integer>) row -> 42;
        when(mockExecutor.createRowMapper(eq(Integer.class))).thenReturn(mapper);

        Nullable<Integer> result = async.queryForSingleValue(Integer.class, "SELECT v FROM t WHERE id = ?", 1).get();
        assertTrue(result.isEmpty());
    }

    @Test
    public void testQueryForSingleValue_Present() throws Exception {
        Row row = mock(Row.class);
        when(mockAsyncRS.currentPage()).thenReturn(Arrays.asList(row));
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        when(mockExecutor.readFirstColumn(row, Integer.class)).thenReturn(42);

        Nullable<Integer> result = async.queryForSingleValue(Integer.class, "SELECT v FROM t WHERE id = ?", 1).get();
        assertTrue(result.isPresent());
        assertEquals(42, result.get().intValue());
    }

    @Test
    public void testQueryForSingleNonNull_Empty() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Integer> mapper = (Function) (Function<Row, Integer>) row -> 42;
        when(mockExecutor.createRowMapper(eq(Integer.class))).thenReturn(mapper);

        Optional<Integer> result = async.queryForSingleNonNull(Integer.class, "SELECT v FROM t WHERE id = ?", 1).get();
        assertTrue(result.isEmpty());
    }

    @Test
    public void testQueryForSingleNonNull_Present() throws Exception {
        Row row = mock(Row.class);
        when(mockAsyncRS.currentPage()).thenReturn(Arrays.asList(row));
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        when(mockExecutor.readFirstColumn(row, Integer.class)).thenReturn(99);

        Optional<Integer> result = async.queryForSingleNonNull(Integer.class, "SELECT v FROM t WHERE id = ?", 1).get();
        assertTrue(result.isPresent());
        assertEquals(99, result.get().intValue());
    }

    @Test
    public void testStream_RowMapperLambda_IsExercised() throws Exception {
        Row row = mock(Row.class);
        when(mockAsyncRS.currentPage()).thenReturn(Arrays.asList(row));
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));

        final BiFunction<ColumnDefinitions, Row, String> rowMapper = (cd, r) -> "X";
        when(mockExecutor.createRowMapper(eq(rowMapper))).thenReturn(r -> "X");

        ContinuableFuture<Stream<String>> future = async.stream("SELECT * FROM t WHERE id = ?", rowMapper, 1);
        Stream<String> stream = future.get();
        Iterator<String> it = stream.iterator();
        assertTrue(it.hasNext());
        assertEquals("X", it.next());
    }

    // ---------------------------------------------------------------------
    //  AsyncCassandraExecutorBase coverage tests - exercise typed queryFor*
    //  delegations and exists/count helpers.
    // ---------------------------------------------------------------------

    @Test
    public void testExists_String_DelegatesToExecute() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));

        Boolean result = async.exists("SELECT * FROM t WHERE id = ?", 1).get();
        assertNotNull(result);
        // Empty result set -> exists is false.
        assertEquals(Boolean.FALSE, result);
    }

    @Test
    public void testCount_String_DelegatesToQueryForLong() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Long> mapper = (Function) (Function<Row, Long>) row -> 0L;
        when(mockExecutor.createRowMapper(eq(Long.class))).thenReturn(mapper);

        @SuppressWarnings("deprecation")
        Long count = async.count("SELECT count(*) FROM t", 1).get();
        // No rows -> count returns 0.
        assertEquals(Long.valueOf(0L), count);
    }

    @Test
    public void testQueryForBoolean_String_DelegatesToQueryForSingleValue() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Boolean> mapper = (Function) (Function<Row, Boolean>) r -> true;
        when(mockExecutor.createRowMapper(eq(Boolean.class))).thenReturn(mapper);

        assertNotNull(async.queryForBoolean("SELECT b FROM t WHERE id = ?", 1).get());
    }

    @Test
    public void testQueryForInt_String_DelegatesToQueryForSingleValue() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Integer> mapper = (Function) (Function<Row, Integer>) r -> 1;
        when(mockExecutor.createRowMapper(eq(Integer.class))).thenReturn(mapper);

        assertNotNull(async.queryForInt("SELECT v FROM t WHERE id = ?", 1).get());
    }

    @Test
    public void testQueryForLong_String_DelegatesToQueryForSingleValue() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Long> mapper = (Function) (Function<Row, Long>) r -> 1L;
        when(mockExecutor.createRowMapper(eq(Long.class))).thenReturn(mapper);

        assertNotNull(async.queryForLong("SELECT v FROM t WHERE id = ?", 1).get());
    }

    @Test
    public void testQueryForString_String_DelegatesToQueryForSingleValue() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, String> mapper = (Function) (Function<Row, String>) r -> "x";
        when(mockExecutor.createRowMapper(eq(String.class))).thenReturn(mapper);

        assertNotNull(async.queryForString("SELECT v FROM t WHERE id = ?", 1).get());
    }

    @Test
    public void testQueryForFloat_String_DelegatesToQueryForSingleValue() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Float> mapper = (Function) (Function<Row, Float>) r -> 1.0f;
        when(mockExecutor.createRowMapper(eq(Float.class))).thenReturn(mapper);

        assertNotNull(async.queryForFloat("SELECT v FROM t WHERE id = ?", 1).get());
    }

    @Test
    public void testQueryForDouble_String_DelegatesToQueryForSingleValue() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Double> mapper = (Function) (Function<Row, Double>) r -> 1.0;
        when(mockExecutor.createRowMapper(eq(Double.class))).thenReturn(mapper);

        assertNotNull(async.queryForDouble("SELECT v FROM t WHERE id = ?", 1).get());
    }

    @Test
    public void testQueryForByte_String_DelegatesToQueryForSingleValue() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Byte> mapper = (Function) (Function<Row, Byte>) r -> (byte) 1;
        when(mockExecutor.createRowMapper(eq(Byte.class))).thenReturn(mapper);

        assertNotNull(async.queryForByte("SELECT v FROM t WHERE id = ?", 1).get());
    }

    @Test
    public void testQueryForShort_String_DelegatesToQueryForSingleValue() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Short> mapper = (Function) (Function<Row, Short>) r -> (short) 1;
        when(mockExecutor.createRowMapper(eq(Short.class))).thenReturn(mapper);

        assertNotNull(async.queryForShort("SELECT v FROM t WHERE id = ?", 1).get());
    }

    @Test
    public void testQueryForChar_String_DelegatesToQueryForSingleValue() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Character> mapper = (Function) (Function<Row, Character>) r -> 'a';
        when(mockExecutor.createRowMapper(eq(Character.class))).thenReturn(mapper);

        assertNotNull(async.queryForChar("SELECT v FROM t WHERE id = ?", 1).get());
    }

    @Test
    public void testList_String_DelegatesToExecute() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        // toList is called on the wrapped sync executor.
        when(mockExecutor.toList(eq(String.class), any(ResultSet.class))).thenReturn(Collections.<String> emptyList());

        java.util.List<String> result = async.list(String.class, "SELECT * FROM t WHERE id = ?", 1).get();
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @Test
    public void testFindFirst_String_NoTarget_DelegatesToMapClass() throws Exception {
        // findFirst(String, Object...) overload returns Optional<Map<String, Object>>
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Map<String, Object>> mapper = (Function) (Function<Row, Map<String, Object>>) r -> new HashMap<>();
        when(mockExecutor.createRowMapper(any(Class.class))).thenReturn(mapper);

        Optional<Map<String, Object>> result = async.findFirst("SELECT * FROM t WHERE id = ?", 1).get();
        assertTrue(result.isEmpty());
    }

    /**
     * Regression guard: the 4-arg Condition overloads of queryForSingleValue/queryForSingleNonNull
     * must eagerly reject a null/empty propName with IllegalArgumentException (parity with the sync
     * CassandraExecutorBase siblings); previously a null propName surfaced as an NPE from
     * List.of(propName) and an empty one produced a malformed projection.
     */
    @Test
    public void testQueryForSingleValueAndNonNull_Condition_NullOrEmptyPropName_ThrowsIAE() {
        final com.landawn.abacus.query.condition.Condition cond = com.landawn.abacus.query.Filters.eq("id", 1);

        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> async.queryForSingleValue(Map.class, String.class, null, cond));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> async.queryForSingleValue(Map.class, String.class, "", cond));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> async.queryForSingleNonNull(Map.class, String.class, null, cond));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> async.queryForSingleNonNull(Map.class, String.class, "", cond));

        // The guard fires before the query is even prepared.
        org.mockito.Mockito.verify(mockExecutor, org.mockito.Mockito.never()).prepareQuery(any(), any(), any(), org.mockito.ArgumentMatchers.anyInt());
    }

    // ---------------------------------------------------------------------------------------------
    //  Repeated get() on a returned future must yield the same result (review 2026-09-22, slice O).
    //  ContinuableFuture.map(...) re-applies its function on EVERY get(); the driver's
    //  AsyncResultSet.currentPage() hands out one shared, one-shot iterator, so an un-memoized
    //  mapping re-read an already drained page on the second get().
    // ---------------------------------------------------------------------------------------------

    /** Mirrors DefaultAsyncResultSet: every currentPage() call returns the SAME one-shot iterator. */
    private static AsyncResultSet oneShotPage(final Row... rows) {
        final AsyncResultSet rs = mock(AsyncResultSet.class);
        final Iterator<Row> iter = Arrays.asList(rows).iterator();
        when(rs.currentPage()).thenReturn(() -> iter);
        when(rs.hasMorePages()).thenReturn(false);
        return rs;
    }

    private void stubOneRowPerExecution() {
        final Row row = mock(Row.class);
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenAnswer(inv -> completed(oneShotPage(row)));
    }

    @Test
    public void testRepeatedGet_queryForSingleValueAndFindFirst_returnSameResult() throws Exception {
        stubOneRowPerExecution();
        when(mockExecutor.readFirstColumn(any(Row.class), eq(String.class))).thenReturn("v");
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, String> mapper = (Function) (Function<Row, String>) r -> "v";
        when(mockExecutor.createRowMapper(eq(String.class))).thenReturn(mapper);

        final ContinuableFuture<Nullable<String>> value = async.queryForSingleValue(String.class, "SELECT v FROM t WHERE id = ?", 1);
        assertEquals("v", value.get().orElseNull());
        assertEquals("v", value.get().orElseNull()); // was Nullable.empty(): the second get() re-read the drained page

        final ContinuableFuture<Optional<String>> first = async.findFirst(String.class, "SELECT v FROM t WHERE id = ?", 1);
        assertEquals("v", first.get().orElseNull());
        assertEquals("v", first.get().orElseNull()); // was Optional.empty()

        final ContinuableFuture<Boolean> exists = async.exists("SELECT v FROM t WHERE id = ?", 1);
        exists.get().booleanValue();
        assertTrue(exists.get());
    }

    @Test
    public void testRepeatedGet_execute_returnsSameResultSet() throws Exception {
        stubOneRowPerExecution();

        final ContinuableFuture<ResultSet> future = async.execute("SELECT id FROM t WHERE id = ?", 1);
        final ResultSet first = future.get();

        assertSame(first, future.get()); // was a fresh wrapper over the shared, drained driver iterator
        assertTrue(first.iterator().hasNext());
    }

    @Test
    public void testRepeatedGet_getAndList_returnSameResult() throws Exception {
        stubOneRowPerExecution();
        when(mockExecutor.prepareQuery(any(), any(), any(), org.mockito.ArgumentMatchers.anyInt()))
                .thenReturn(new com.landawn.abacus.query.AbstractQueryBuilder.SP("SELECT id FROM t WHERE id = ?", com.landawn.abacus.util.ImmutableList.of(1)));
        // Behave like the real fetchOnlyOne/toList: consume the result-set cursor.
        when(mockExecutor.fetchOnlyOne(eq(String.class), any(ResultSet.class)))
                .thenAnswer(inv -> {
                    final Iterator<Row> it = ((ResultSet) inv.getArgument(1)).iterator();

                    if (!it.hasNext()) {
                        return null;
                    }

                    it.next();

                    return "row";
                });
        when(mockExecutor.toList(eq(String.class), any(ResultSet.class))).thenAnswer(inv -> ((ResultSet) inv.getArgument(1)).all());

        final ContinuableFuture<Optional<String>> get = async.get(String.class, com.landawn.abacus.query.Filters.eq("id", 1));
        assertEquals("row", get.get().orElseNull());
        assertEquals("row", get.get().orElseNull()); // was Optional.empty()

        final ContinuableFuture<java.util.List<String>> list = async.list(String.class, "SELECT id FROM t WHERE id = ?", 1);
        assertEquals(1, list.get().size());
        assertSame(list.get(), list.get()); // was a second, empty list
    }

    @Test
    public void testRepeatedGet_gett_duplicateResultFailureIsReplayed() throws Exception {
        final Row row1 = mock(Row.class);
        final Row row2 = mock(Row.class);
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenAnswer(inv -> completed(oneShotPage(row1, row2)));
        when(mockExecutor.prepareQuery(any(), any(), any(), org.mockito.ArgumentMatchers.anyInt()))
                .thenReturn(new com.landawn.abacus.query.AbstractQueryBuilder.SP("SELECT id FROM t", com.landawn.abacus.util.ImmutableList.empty()));
        // Behave like the real fetchOnlyOne: read one row, then fail if another one follows.
        when(mockExecutor.fetchOnlyOne(eq(String.class), any(ResultSet.class))).thenAnswer(inv -> {
            final Iterator<Row> it = ((ResultSet) inv.getArgument(1)).iterator();

            if (!it.hasNext()) {
                return null;
            }

            it.next();

            if (it.hasNext()) {
                throw new com.landawn.abacus.exception.DuplicateResultException();
            }

            return "row";
        });

        final ContinuableFuture<String> future = async.gett(String.class, com.landawn.abacus.query.Filters.eq("status", "x"));

        for (int i = 0; i < 2; i++) { // the second get() previously returned the drained-cursor answer (null)
            final java.util.concurrent.ExecutionException ex = assertThrows(java.util.concurrent.ExecutionException.class, future::get);
            assertTrue(ex.getCause() instanceof com.landawn.abacus.exception.DuplicateResultException);
        }
    }

    // ---------------------------------------------------------------------------------------------
    //  Review 2026-09-27 (slice O)
    // ---------------------------------------------------------------------------------------------

    /**
     * An Error thrown by the result mapping (e.g. an entity class whose static initializer fails) must be
     * replayed by every later get(), like an Exception: memoize() used to cache only Exceptions, so the second
     * get() re-ran the mapping over the already consumed cursor and reported "no row" (null) instead.
     */
    @Test
    public void testRepeatedGet_gett_mappingErrorIsReplayed() throws Exception {
        final Row row = mock(Row.class);
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenAnswer(inv -> completed(oneShotPage(row)));
        when(mockExecutor.prepareQuery(any(), any(), any(), org.mockito.ArgumentMatchers.anyInt()))
                .thenReturn(new com.landawn.abacus.query.AbstractQueryBuilder.SP("SELECT id FROM t", com.landawn.abacus.util.ImmutableList.empty()));
        final ExceptionInInitializerError mappingError = new ExceptionInInitializerError("static init failed");
        // Behave like fetchOnlyOne mapping a row into a class whose initialization fails.
        when(mockExecutor.fetchOnlyOne(eq(String.class), any(ResultSet.class))).thenAnswer(inv -> {
            final Iterator<Row> it = ((ResultSet) inv.getArgument(1)).iterator();

            if (!it.hasNext()) {
                return null;
            }

            it.next();

            throw mappingError;
        });

        final ContinuableFuture<String> future = async.gett(String.class, com.landawn.abacus.query.Filters.eq("id", 1));

        for (int i = 0; i < 2; i++) { // the second get() previously returned null (the drained cursor looked empty)
            final java.util.concurrent.ExecutionException ex = assertThrows(java.util.concurrent.ExecutionException.class, future::get);
            assertSame(mappingError, ex.getCause());
        }
    }

    @Test
    public void testMemoize_cachesErrorAndDoesNotReapply() {
        final int[] calls = { 0 };
        final AssertionError error = new AssertionError("boom");
        final com.landawn.abacus.util.Throwables.Function<String, String, Exception> once = AsyncCassandraExecutorBase.memoize(s -> {
            calls[0]++;
            throw error;
        });

        assertSame(error, assertThrows(AssertionError.class, () -> once.apply("a")));
        assertSame(error, assertThrows(AssertionError.class, () -> once.apply("a")));
        assertEquals(1, calls[0]);
    }

    // ---- 2026-09-29 sliceO ----

    /**
     * The driver's default {@code PagingIterable.all()} (inherited by {@code rs.map(f)}) sizes its list with
     * {@code getAvailableWithoutFetching()}; the wrapper used to throw UnsupportedOperationException there, so
     * {@code async.execute(...).get().map(f).all()} failed on any non-empty result.
     */
    @Test
    public void testWrappedResultSet_mapAllAndAvailableWithoutFetching_sliceO() {
        final Row first = mock(Row.class);
        final Row second = mock(Row.class);
        final AsyncResultSet asyncResultSet = mock(AsyncResultSet.class);
        when(asyncResultSet.currentPage()).thenReturn(Arrays.asList(first, second));
        when(asyncResultSet.hasMorePages()).thenReturn(false);
        when(asyncResultSet.remaining()).thenReturn(2);

        final ResultSet resultSet = ResultSets.wrap(asyncResultSet);

        assertEquals(2, resultSet.getAvailableWithoutFetching());
        assertEquals(Arrays.asList(first, second), resultSet.map(java.util.function.Function.identity()).all());
    }

    // ---- 2026-10-02 sliceO ----

    /**
     * Multi-page stand-in faithful to the driver's DefaultAsyncResultSet: currentPage() always hands out the same
     * one-shot iterator, and remaining() counts down as rows are taken from it.
     */
    private static final class SliceOPage implements AsyncResultSet {
        private final ColumnDefinitions definitions;
        private final com.datastax.oss.driver.api.core.cql.ExecutionInfo executionInfo = mock(com.datastax.oss.driver.api.core.cql.ExecutionInfo.class);
        private final java.util.Deque<Row> rows;
        private final AsyncResultSet nextPage;
        private final RuntimeException nextPageFailure;
        private final Iterator<Row> iterator;

        SliceOPage(final ColumnDefinitions definitions, final AsyncResultSet nextPage, final RuntimeException nextPageFailure, final Row... rows) {
            this.definitions = definitions;
            this.rows = new java.util.ArrayDeque<>(Arrays.asList(rows));
            this.nextPage = nextPage;
            this.nextPageFailure = nextPageFailure;
            this.iterator = new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return !SliceOPage.this.rows.isEmpty();
                }

                @Override
                public Row next() {
                    if (SliceOPage.this.rows.isEmpty()) {
                        throw new java.util.NoSuchElementException();
                    }

                    return SliceOPage.this.rows.poll();
                }
            };
        }

        @Override
        public ColumnDefinitions getColumnDefinitions() {
            return definitions;
        }

        @Override
        public com.datastax.oss.driver.api.core.cql.ExecutionInfo getExecutionInfo() {
            return executionInfo;
        }

        @Override
        public Iterable<Row> currentPage() {
            return () -> iterator;
        }

        @Override
        public int remaining() {
            return rows.size();
        }

        @Override
        public boolean hasMorePages() {
            return nextPage != null || nextPageFailure != null;
        }

        @Override
        public CompletionStage<AsyncResultSet> fetchNextPage() {
            if (nextPageFailure != null) {
                final CompletableFuture<AsyncResultSet> failed = new CompletableFuture<>();
                failed.completeExceptionally(nextPageFailure);
                return failed;
            }

            return completedStage(nextPage);
        }

        private static CompletionStage<AsyncResultSet> completedStage(final AsyncResultSet page) {
            return CompletableFuture.completedFuture(page);
        }

        @Override
        public boolean wasApplied() {
            return true;
        }
    }

    private static ColumnDefinitions sliceOColumns() {
        final ColumnDefinitions columns = mock(ColumnDefinitions.class);
        final com.datastax.oss.driver.api.core.cql.ColumnDefinition column = mock(com.datastax.oss.driver.api.core.cql.ColumnDefinition.class);
        when(columns.size()).thenReturn(1);
        when(columns.get(0)).thenReturn(column);
        when(column.getName()).thenReturn(com.datastax.oss.driver.api.core.CqlIdentifier.fromInternal("v"));
        return columns;
    }

    private static Row sliceORow(final ColumnDefinitions columns, final long value) {
        final Row row = mock(Row.class);
        when(row.getColumnDefinitions()).thenReturn(columns);
        when(row.getObject(0)).thenReturn(value);
        return row;
    }

    /** Pages [1, 2], [] (empty, more to come), [3]; the last page's fetch fails when {@code lastPageFailure} is set. */
    private static AsyncResultSet sliceOPages(final ColumnDefinitions columns, final RuntimeException lastPageFailure) {
        final AsyncResultSet third = lastPageFailure == null ? new SliceOPage(columns, null, null, sliceORow(columns, 3L)) : null;
        final AsyncResultSet second = new SliceOPage(columns, third, lastPageFailure);
        return new SliceOPage(columns, second, null, sliceORow(columns, 1L), sliceORow(columns, 2L));
    }

    /** Pins the wrapper's driver ResultSet contract across pages (one/iteration/remaining/infos/isFullyFetched/map.all). */
    @Test
    public void testWrappedResultSet_multiPageContract_sliceO() {
        final ColumnDefinitions columns = sliceOColumns();
        final ResultSet resultSet = ResultSets.wrap(sliceOPages(columns, null));

        assertEquals(2, resultSet.getAvailableWithoutFetching());
        assertTrue(!resultSet.isFullyFetched());
        assertEquals(1L, resultSet.one().getObject(0));
        assertEquals(1, resultSet.getAvailableWithoutFetching()); // counts down like the driver's MultiPageResultSet

        final java.util.List<Object> rest = new java.util.ArrayList<>();
        resultSet.forEach(row -> rest.add(row.getObject(0)));

        assertEquals(Arrays.asList(2L, 3L), rest); // the empty middle page is skipped
        assertTrue(resultSet.isFullyFetched());
        assertEquals(3, resultSet.getExecutionInfos().size()); // one per fetched page
        assertSame(resultSet.getExecutionInfos().get(2), resultSet.getExecutionInfo());
        assertEquals(null, resultSet.one());

        assertEquals(Arrays.asList(1L, 2L, 3L), ResultSets.wrap(sliceOPages(columns, null)).map(row -> row.getObject(0)).all());
        assertEquals(3, java.util.stream.StreamSupport.stream(ResultSets.wrap(sliceOPages(columns, null)).spliterator(), false).count());
    }

    /**
     * Pins the async facade over a real CassandraExecutor across result pages: list/findFirst/stream read every page,
     * and a later-page fetch failure is reported by get() (list) or by the stream's terminal operation (stream) with the
     * driver failure as the cause / exception.
     */
    @Test
    public void testAsyncFacadeReadsAllPagesAndReportsPageFetchFailure_sliceO() throws Exception {
        final CqlSession session = mock(CqlSession.class);
        when(session.getContext()).thenReturn(mock(com.datastax.oss.driver.api.core.context.DriverContext.class));
        when(session.getContext().getCodecRegistry()).thenReturn(mock(com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry.class));
        final CassandraExecutor executor = new CassandraExecutor(session);
        final com.datastax.oss.driver.api.core.cql.PreparedStatement preparedStatement = mock(com.datastax.oss.driver.api.core.cql.PreparedStatement.class);
        final ColumnDefinitions noVariables = mock(ColumnDefinitions.class);
        when(noVariables.size()).thenReturn(0);
        when(preparedStatement.getVariableDefinitions()).thenReturn(noVariables);
        when(preparedStatement.bind()).thenReturn(mockStatement);
        when(session.prepare(anyString())).thenReturn(preparedStatement);

        final ColumnDefinitions columns = sliceOColumns();
        final IllegalStateException pageFailure = new IllegalStateException("page fetch failed");
        final boolean[] failLastPage = { false };
        when(session.executeAsync(any(Statement.class))).thenAnswer(inv -> completed(sliceOPages(columns, failLastPage[0] ? pageFailure : null)));

        final AsyncCassandraExecutor facade = executor.async();
        final ContinuableFuture<java.util.List<Long>> list = facade.list(Long.class, "SELECT v FROM t");

        assertEquals(Arrays.asList(1L, 2L, 3L), list.get());
        assertEquals(Arrays.asList(1L, 2L, 3L), list.get());
        assertEquals(1L, facade.findFirst(Long.class, "SELECT v FROM t").get().orElseNull());
        assertEquals(Arrays.asList(1L, 2L, 3L), facade.stream(Long.class, "SELECT v FROM t").get().toList());

        failLastPage[0] = true;

        final java.util.concurrent.ExecutionException listFailure = assertThrows(java.util.concurrent.ExecutionException.class,
                () -> facade.list(Long.class, "SELECT v FROM t").get());
        assertSame(pageFailure, listFailure.getCause());

        final Stream<Long> stream = facade.stream(Long.class, "SELECT v FROM t").get(); // the first page arrived normally
        assertSame(pageFailure, assertThrows(IllegalStateException.class, stream::toList));
    }

    // ---- 2026-10-04 coverageCS ----

    public static class CoverageCSTimeBean {
        private java.sql.Time v;

        public java.sql.Time getV() {
            return v;
        }

        public void setV(final java.sql.Time v) {
            this.v = v;
        }
    }

    private static Row coverageCSRow(final ColumnDefinitions columns, final Object value) {
        final Row row = mock(Row.class);
        when(row.getColumnDefinitions()).thenReturn(columns);
        when(row.getObject(0)).thenReturn(value);
        return row;
    }

    @Test
    public void testCoverageCS_asyncFacadeReadsTimeIntoSqlTimeOnEveryPathAndBindsSingleCalendarAsValue() throws Exception {
        // The async facade maps through the executor's helpers: a driver LocalTime with a fractional second or on a whole minute
        // (N.convert rejected both for java.sql.Time) must reach a java.sql.Time target on every async read path and page, and a
        // single Calendar parameter must be bound as one value (it was read as a bean: "Missing required parameter: 'ts'").
        final CqlSession session = mock(CqlSession.class);
        when(session.getContext()).thenReturn(mock(com.datastax.oss.driver.api.core.context.DriverContext.class));
        when(session.getContext().getCodecRegistry())
                .thenReturn(new com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry("coverage-cs-async"));
        final CassandraExecutor executor = new CassandraExecutor(session);
        final String select = "SELECT v FROM t";
        final com.datastax.oss.driver.api.core.cql.PreparedStatement selectStatement = mock(com.datastax.oss.driver.api.core.cql.PreparedStatement.class);
        final ColumnDefinitions noVariables = mock(ColumnDefinitions.class);
        when(noVariables.size()).thenReturn(0);
        when(selectStatement.getVariableDefinitions()).thenReturn(noVariables);
        when(selectStatement.bind()).thenReturn(mockStatement);
        when(selectStatement.bind(any(Object[].class))).thenReturn(mockStatement);
        when(session.prepare(select)).thenReturn(selectStatement);

        final ColumnDefinitions columns = sliceOColumns();
        final java.time.LocalTime fractional = java.time.LocalTime.of(1, 2, 3, 456_789_000);
        final java.time.LocalTime wholeMinute = java.time.LocalTime.of(12, 30);
        // Pages [fractional, wholeMinute], [] (more to come), [midnight].
        when(session.executeAsync(any(Statement.class))).thenAnswer(inv -> completed(new SliceOPage(columns,
                new SliceOPage(columns, new SliceOPage(columns, null, null, coverageCSRow(columns, java.time.LocalTime.MIDNIGHT)), null), null,
                coverageCSRow(columns, fractional), coverageCSRow(columns, wholeMinute))));
        final java.text.SimpleDateFormat timeFormat = new java.text.SimpleDateFormat("HH:mm:ss.SSS");
        final java.util.List<String> expected = Arrays.asList("01:02:03.456", "12:30:00.000", "00:00:00.000");
        final AsyncCassandraExecutor facade = executor.async();

        assertEquals(expected, facade.list(java.sql.Time.class, select).get().stream().map(timeFormat::format).toList());
        assertEquals(expected, facade.list(java.sql.Time[].class, select).get().stream().map(a -> timeFormat.format(a[0])).toList());
        assertEquals(expected, facade.stream(java.sql.Time.class, select).get().map(timeFormat::format).toList());
        assertEquals(expected, facade.list(CoverageCSTimeBean.class, select).get().stream().map(b -> timeFormat.format(b.getV())).toList());
        assertEquals(expected, facade.query(CoverageCSTimeBean.class, select).get().<java.sql.Time> getColumn("v").stream().map(timeFormat::format).toList());
        assertEquals("01:02:03.456", timeFormat.format(facade.findFirst(java.sql.Time.class, select).get().get()));
        assertEquals("01:02:03.456", timeFormat.format(facade.queryForSingleValue(java.sql.Time.class, select).get().get()));
        assertEquals("01:02:03.456", timeFormat.format(facade.queryForSingleNonNull(java.sql.Time.class, select).get().get()));

        final String update = "UPDATE t SET ts = ? WHERE id = 1";
        final com.datastax.oss.driver.api.core.cql.PreparedStatement updateStatement = mock(com.datastax.oss.driver.api.core.cql.PreparedStatement.class);
        final ColumnDefinitions variables = mock(ColumnDefinitions.class);
        final com.datastax.oss.driver.api.core.cql.ColumnDefinition tsVariable = mock(com.datastax.oss.driver.api.core.cql.ColumnDefinition.class);
        when(variables.size()).thenReturn(1);
        when(variables.get(0)).thenReturn(tsVariable);
        when(tsVariable.getName()).thenReturn(com.datastax.oss.driver.api.core.CqlIdentifier.fromInternal("ts"));
        when(tsVariable.getType()).thenReturn(com.datastax.oss.driver.api.core.type.DataTypes.TIMESTAMP);
        when(updateStatement.getVariableDefinitions()).thenReturn(variables);
        final Object[][] bound = new Object[1][];
        when(updateStatement.bind(any(Object[].class))).thenAnswer(inv -> {
            bound[0] = (Object[]) inv.getRawArguments()[0];
            return mockStatement;
        });
        when(session.prepare(update)).thenReturn(updateStatement);
        final java.util.Calendar calendar = java.util.Calendar.getInstance();
        calendar.setTimeInMillis(1_600_000_000_000L);

        facade.execute(update, calendar).get();

        assertEquals(Arrays.asList(java.time.Instant.ofEpochMilli(1_600_000_000_000L)), Arrays.asList(bound[0]));
    }

    // ---- 2026-10-04 coverageCS end ----
}
