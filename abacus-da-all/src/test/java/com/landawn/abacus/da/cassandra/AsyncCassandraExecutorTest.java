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
}
