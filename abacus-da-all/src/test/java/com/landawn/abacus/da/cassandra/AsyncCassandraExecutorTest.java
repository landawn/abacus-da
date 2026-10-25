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
import com.landawn.abacus.util.u.Nullable;
import com.landawn.abacus.util.u.Optional;
import com.landawn.abacus.util.function.Function;
import com.landawn.abacus.util.stream.Stream;

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

    // Construction and configuration

    @Test
    public void testConstructor_rejectsNullExecutor() {
        assertThrows(IllegalArgumentException.class, () -> new AsyncCassandraExecutor(null));
    }

    @Test
    public void testConstructor_StoresExecutor() {
        AsyncCassandraExecutor a = new AsyncCassandraExecutor(mockExecutor);
        assertNotNull(a);
        assertSame(mockExecutor, a.sync());
    }

    @Test
    public void testSync_ReturnsUnderlyingExecutor() {
        assertSame(mockExecutor, async.sync());
    }

    // Row and entity mapping

    @Test
    public void testFindFirst_String_NoTarget_DelegatesToMapClass() throws Exception {
        // findFirst(String, Object...) overload returns Optional<Map<String, Object>>
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Map<String, Object>> mapper = (Function<Row, Map<String, Object>>) r -> new HashMap<>();
        when(mockExecutor.createRowMapper(any(Class.class))).thenReturn(mapper);

        Optional<Map<String, Object>> result = async.findFirst("SELECT * FROM t WHERE id = ?", 1).get();
        assertTrue(result.isEmpty());
    }

    //  Review 2026-09-27 (slice O)

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

    // Updates and deletes

    @Test
    public void testExecute_String_FutureGetReturnsResultSet() throws Exception {
        when(mockExecutor.prepareStatement("SELECT * FROM t")).thenReturn(mockStatement);
        when(mockSession.executeAsync(mockStatement)).thenReturn(completed(mockAsyncRS));

        ResultSet result = async.execute("SELECT * FROM t").get();
        assertNotNull(result);
    }

    @Test
    public void testFindFirst_WithEmptyResultSet_ReturnsEmpty() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, String> mapper = (Function<Row, String>) row -> "v";
        when(mockExecutor.createRowMapper(eq(String.class))).thenReturn(mapper);

        Optional<String> result = async.findFirst(String.class, "SELECT * FROM t WHERE id = ?", 1).get();
        assertTrue(result.isEmpty());
    }

    @Test
    public void testRepeatedGet_execute_returnsSameResultSet() throws Exception {
        stubOneRowPerExecution();

        final ContinuableFuture<ResultSet> future = async.execute("SELECT id FROM t WHERE id = ?", 1);
        final ResultSet first = future.get();

        assertSame(first, future.get()); // was a fresh wrapper over the shared, drained driver iterator
        assertTrue(first.iterator().hasNext());
    }

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

    // Queries and results

    @Test
    public void testCount_String_DelegatesToQueryForLong() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Long> mapper = (Function<Row, Long>) row -> 0L;
        when(mockExecutor.createRowMapper(eq(Long.class))).thenReturn(mapper);

        @SuppressWarnings("deprecation")
        Long count = async.count("SELECT count(*) FROM t", 1).get();
        // No rows -> count returns 0.
        assertEquals(Long.valueOf(0L), count);
    }

    //  AsyncCassandraExecutorBase coverage tests - exercise typed queryFor*
    //  delegations and exists/count helpers.

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
    public void testFindFirst_WithRow_ReturnsValue() throws Exception {
        Row row = mock(Row.class);
        when(mockAsyncRS.currentPage()).thenReturn(Arrays.asList(row));
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, String> mapper = (Function<Row, String>) r -> "hello";
        when(mockExecutor.createRowMapper(eq(String.class))).thenReturn(mapper);

        Optional<String> result = async.findFirst(String.class, "SELECT name FROM t WHERE id = ?", 1).get();
        assertTrue(result.isPresent());
        assertEquals("hello", result.get());
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
    public void testQueryForBoolean_String_DelegatesToQueryForSingleValue() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Boolean> mapper = (Function<Row, Boolean>) r -> true;
        when(mockExecutor.createRowMapper(eq(Boolean.class))).thenReturn(mapper);

        assertNotNull(async.queryForBoolean("SELECT b FROM t WHERE id = ?", 1).get());
    }

    @Test
    public void testQueryForByte_String_DelegatesToQueryForSingleValue() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Byte> mapper = (Function<Row, Byte>) r -> (byte) 1;
        when(mockExecutor.createRowMapper(eq(Byte.class))).thenReturn(mapper);

        assertNotNull(async.queryForByte("SELECT v FROM t WHERE id = ?", 1).get());
    }

    @Test
    public void testQueryForChar_String_DelegatesToQueryForSingleValue() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Character> mapper = (Function<Row, Character>) r -> 'a';
        when(mockExecutor.createRowMapper(eq(Character.class))).thenReturn(mapper);

        assertNotNull(async.queryForChar("SELECT v FROM t WHERE id = ?", 1).get());
    }

    @Test
    public void testQueryForDouble_String_DelegatesToQueryForSingleValue() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Double> mapper = (Function<Row, Double>) r -> 1.0;
        when(mockExecutor.createRowMapper(eq(Double.class))).thenReturn(mapper);

        assertNotNull(async.queryForDouble("SELECT v FROM t WHERE id = ?", 1).get());
    }

    @Test
    public void testQueryForFloat_String_DelegatesToQueryForSingleValue() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Float> mapper = (Function<Row, Float>) r -> 1.0f;
        when(mockExecutor.createRowMapper(eq(Float.class))).thenReturn(mapper);

        assertNotNull(async.queryForFloat("SELECT v FROM t WHERE id = ?", 1).get());
    }

    @Test
    public void testQueryForInt_String_DelegatesToQueryForSingleValue() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Integer> mapper = (Function<Row, Integer>) r -> 1;
        when(mockExecutor.createRowMapper(eq(Integer.class))).thenReturn(mapper);

        assertNotNull(async.queryForInt("SELECT v FROM t WHERE id = ?", 1).get());
    }

    @Test
    public void testQueryForLong_String_DelegatesToQueryForSingleValue() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Long> mapper = (Function<Row, Long>) r -> 1L;
        when(mockExecutor.createRowMapper(eq(Long.class))).thenReturn(mapper);

        assertNotNull(async.queryForLong("SELECT v FROM t WHERE id = ?", 1).get());
    }

    @Test
    public void testQueryForShort_String_DelegatesToQueryForSingleValue() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Short> mapper = (Function<Row, Short>) r -> (short) 1;
        when(mockExecutor.createRowMapper(eq(Short.class))).thenReturn(mapper);

        assertNotNull(async.queryForShort("SELECT v FROM t WHERE id = ?", 1).get());
    }

    @Test
    public void testQueryForSingleNonNull_Empty() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Integer> mapper = (Function<Row, Integer>) row -> 42;
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
    public void testQueryForSingleValue_Empty_ReturnsNullable() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Integer> mapper = (Function<Row, Integer>) row -> 42;
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

    @Test
    public void testQueryForString_String_DelegatesToQueryForSingleValue() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, String> mapper = (Function<Row, String>) r -> "x";
        when(mockExecutor.createRowMapper(eq(String.class))).thenReturn(mapper);

        assertNotNull(async.queryForString("SELECT v FROM t WHERE id = ?", 1).get());
    }

    @Test
    public void testRepeatedGet_getAndList_returnSameResult() throws Exception {
        stubOneRowPerExecution();
        when(mockExecutor.prepareQuery(any(), any(), any(), org.mockito.ArgumentMatchers.anyInt()))
                .thenReturn(new com.landawn.abacus.query.AbstractQueryBuilder.SP("SELECT id FROM t WHERE id = ?", com.landawn.abacus.util.ImmutableList.of(1)));
        // Behave like the real fetchOnlyOne/toList: consume the result-set cursor.
        when(mockExecutor.fetchOnlyOne(eq(String.class), any(ResultSet.class))).thenAnswer(inv -> {
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

    @Test
    public void testRepeatedGet_queryForSingleValueAndFindFirst_returnSameResult() throws Exception {
        stubOneRowPerExecution();
        when(mockExecutor.readFirstColumn(any(Row.class), eq(String.class))).thenReturn("v");
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, String> mapper = (Function<Row, String>) r -> "v";
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
    public void testStream_NullRowMapper_ThrowsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> async.stream("SELECT * FROM t", (BiFunction<ColumnDefinitions, Row, Object>) null));
        assertThrows(IllegalArgumentException.class, () -> async.stream((Statement<?>) null, (BiFunction<ColumnDefinitions, Row, Object>) null));
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

    @Test
    public void testStream_StringAndParams_ReturnsStreamFuture() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Object[]> mapper = (Function<Row, Object[]>) row -> new Object[] {};
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
        final Function<Row, Object[]> mapper = (Function<Row, Object[]>) row -> new Object[] {};
        when(mockExecutor.createRowMapper(eq(Object[].class))).thenReturn(mapper);

        ContinuableFuture<Stream<Object[]>> future = async.stream("SELECT * FROM t");
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
    public void testStream_WithRowMapper_String() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));

        final BiFunction<ColumnDefinitions, Row, String> rowMapper = (cd, r) -> "x";
        when(mockExecutor.createRowMapper(eq(rowMapper))).thenReturn(r -> "x");

        ContinuableFuture<Stream<String>> future = async.stream("SELECT * FROM t", rowMapper, 1);
        assertNotNull(future);
        assertEquals(0, future.get().count());
    }

    // Resource lifecycle

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

    // Additional contracts

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

    @Test
    public void testExecute_Statement_DelegatesToSession() {
        Statement<?> stmt = mock(Statement.class);
        when(mockSession.executeAsync(eq(stmt))).thenReturn(completed(mockAsyncRS));

        ContinuableFuture<ResultSet> future = async.execute(stmt);

        assertNotNull(future);
        verify(mockSession).executeAsync(stmt);
    }

    @Test
    public void testExecute_StringAndMapParams_DelegatesToSession() {
        Map<String, Object> params = new HashMap<>();
        params.put("id", 1);
        when(mockExecutor.prepareStatement(anyString(), eq(params))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));

        ContinuableFuture<ResultSet> future = async.execute("SELECT * FROM t WHERE id = :id", params);

        assertNotNull(future);
        verify(mockSession).executeAsync(mockStatement);
    }

    @Test
    public void testExecute_StringAndParams_DelegatesToSession() {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(completed(mockAsyncRS));

        ContinuableFuture<ResultSet> future = async.execute("SELECT * FROM t WHERE id = ?", 1);

        assertNotNull(future);
        verify(mockSession).executeAsync(mockStatement);
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

    /** Helper: CompletionStage that completes immediately. */
    private CompletionStage<AsyncResultSet> completed(AsyncResultSet rs) {
        return CompletableFuture.completedFuture(rs);
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

    // ---- 2026-10-19 sliceG ----

    /**
     * update(entity) / batchUpdate(entities, type) on a class with no updatable non-key property must name the entity class
     * (thrown eagerly at the call site, as before), not blame a 'propNamesToUpdate' argument the caller never passed.
     */
    @Test
    public void testSliceG_updateWithoutUpdatableProperty_reportsTheEntityClass() {
        final CassandraExecutorBaseTest.SliceFKeyOnlyEntity entity = new CassandraExecutorBaseTest.SliceFKeyOnlyEntity();
        entity.setId(1L);

        final IllegalArgumentException single = assertThrows(IllegalArgumentException.class, () -> async.update(entity));
        final IllegalArgumentException batch = assertThrows(IllegalArgumentException.class,
                () -> async.batchUpdate(java.util.List.of(entity), com.datastax.oss.driver.api.core.cql.BatchType.LOGGED));
        final String className = CassandraExecutorBaseTest.SliceFKeyOnlyEntity.class.getSimpleName();

        org.junit.jupiter.api.Assertions.assertAll(() -> assertTrue(!single.getMessage().contains("propNamesToUpdate"), single.getMessage()),
                () -> assertTrue(single.getMessage().contains("No updatable non-key property"), single.getMessage()),
                () -> assertTrue(single.getMessage().contains(className), single.getMessage()),
                () -> assertTrue(!batch.getMessage().contains("propNamesToUpdate"), batch.getMessage()),
                () -> assertTrue(batch.getMessage().contains("No updatable non-key property"), batch.getMessage()),
                () -> assertTrue(batch.getMessage().contains(className), batch.getMessage()));
    }

    // ---- 2026-10-21 cassfix2 ----

    /**
     * A real v4 executor over a mocked session: every query is prepared without variables and returns one row whose first
     * column ("v") holds {@code value} ({@code null} for a NULL column), so the sync read path (readFirstColumn and its
     * conversion) is exercised through the async facade.
     */
    private static AsyncCassandraExecutor cassfix2AsyncReturning(final Object value) {
        final CqlSession session = mock(CqlSession.class);
        final com.datastax.oss.driver.api.core.context.DriverContext context = mock(com.datastax.oss.driver.api.core.context.DriverContext.class);
        when(session.getContext()).thenReturn(context);
        when(context.getCodecRegistry()).thenReturn(com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry.DEFAULT);
        final com.datastax.oss.driver.api.core.cql.PreparedStatement prepared = mock(com.datastax.oss.driver.api.core.cql.PreparedStatement.class);
        final ColumnDefinitions noVariables = mock(ColumnDefinitions.class);
        when(noVariables.size()).thenReturn(0);
        when(prepared.getVariableDefinitions()).thenReturn(noVariables);
        when(prepared.bind(any(Object[].class))).thenReturn(mock(BoundStatement.class));
        when(session.prepare(anyString())).thenReturn(prepared);
        final ColumnDefinitions columns = sliceOColumns();
        when(session.executeAsync(any(Statement.class))).thenAnswer(inv -> {
            final Row row = mock(Row.class);
            when(row.getColumnDefinitions()).thenReturn(columns);
            when(row.getObject(0)).thenReturn(value);
            return CompletableFuture.completedFuture(new SliceOPage(columns, null, null, row));
        });

        return new CassandraExecutor(session).async();
    }

    private static void cassfix2FailsWith(final Class<? extends Throwable> cause, final ContinuableFuture<?> future) {
        final java.util.concurrent.ExecutionException failure = assertThrows(java.util.concurrent.ExecutionException.class, future::get);
        org.junit.jupiter.api.Assertions.assertInstanceOf(cause, failure.getCause());
    }

    /**
     * Pins the NULL-column rule the Javadoc of queryForSingleValue/queryForSingleNonNull now states (it used to claim that
     * every reference target preserves null): a NULL column maps to the default value of the target type, that is the
     * primitive default, the empty value of an Optional-like type (which therefore never fails queryForSingleNonNull), and
     * null for a wrapper or other reference type (present-but-null Nullable; queryForSingleNonNull fails with NPE).
     */
    @Test
    @SuppressWarnings("rawtypes")
    public void cassfix2_nullColumnMapsToTargetDefault_asyncSingleValueFamily() throws Exception {
        final AsyncCassandraExecutor facade = cassfix2AsyncReturning(null);
        final String query = "SELECT v FROM t";

        final Nullable<Optional> optionalValue = facade.queryForSingleValue(Optional.class, query).get();
        final Optional<com.landawn.abacus.util.u.OptionalInt> optionalInt = facade.queryForSingleNonNull(com.landawn.abacus.util.u.OptionalInt.class, query)
                .get();
        final Nullable<Integer> wrapper = facade.queryForSingleValue(Integer.class, query).get();

        org.junit.jupiter.api.Assertions.assertAll(() -> assertTrue(optionalValue.isPresent()), () -> assertEquals(Optional.empty(), optionalValue.get()),
                () -> assertEquals(Nullable.of(Optional.empty()), optionalValue), () -> assertTrue(optionalInt.isPresent()),
                () -> assertEquals(com.landawn.abacus.util.u.OptionalInt.empty(), optionalInt.get()),
                () -> assertEquals(Optional.of(com.landawn.abacus.util.u.OptionalInt.empty()), optionalInt),
                () -> assertEquals(Nullable.of(Nullable.empty()), facade.queryForSingleValue(Nullable.class, query).get()),
                () -> assertEquals(Nullable.of(0), facade.queryForSingleValue(int.class, query).get()),
                () -> assertEquals(Optional.of(0L), facade.queryForSingleNonNull(long.class, query).get()), () -> assertTrue(wrapper.isPresent()),
                () -> org.junit.jupiter.api.Assertions.assertNull(wrapper.get()),
                () -> cassfix2FailsWith(NullPointerException.class, facade.queryForSingleNonNull(Integer.class, query)),
                () -> cassfix2FailsWith(NullPointerException.class, facade.queryForSingleNonNull(String.class, query)));
    }

    /**
     * The strict Boolean rule of the sync read path (class Javadoc of CassandraExecutorBase, "Boolean Conversion") also
     * governs the async facade: -1 and empty text are false, padded "Y" is true, and a bigint or the text "yes" fails the
     * future with an IllegalArgumentException cause.
     */
    @Test
    public void cassfix2_strictBooleanRead_asyncQueryForBooleanAndSingleValue() throws Exception {
        final String query = "SELECT v FROM t";

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(false, cassfix2AsyncReturning(-1).queryForBoolean(query).get().get()),
                () -> assertEquals(true, cassfix2AsyncReturning(" Y ").queryForBoolean(query).get().get()),
                () -> assertEquals(false, cassfix2AsyncReturning("").queryForBoolean(query).get().get()),
                () -> assertEquals(Boolean.FALSE, cassfix2AsyncReturning(-1).queryForSingleValue(Boolean.class, query).get().get()),
                () -> assertEquals(Boolean.TRUE, cassfix2AsyncReturning(" Y ").queryForSingleValue(Boolean.class, query).get().get()),
                () -> assertEquals(Boolean.FALSE, cassfix2AsyncReturning("").queryForSingleValue(Boolean.class, query).get().get()),
                () -> cassfix2FailsWith(IllegalArgumentException.class, cassfix2AsyncReturning(1L).queryForBoolean(query)),
                () -> cassfix2FailsWith(IllegalArgumentException.class, cassfix2AsyncReturning("yes").queryForBoolean(query)),
                () -> cassfix2FailsWith(IllegalArgumentException.class, cassfix2AsyncReturning(1L).queryForSingleValue(Boolean.class, query)),
                () -> cassfix2FailsWith(IllegalArgumentException.class, cassfix2AsyncReturning("yes").queryForSingleValue(Boolean.class, query)));
    }

    // ---- 2026-10-24 sliceG ----

    /**
     * A real v4 executor over a mocked session. Each execution returns a first page holding one row (v = 1) whose next page's
     * future fails with {@code failure}; with {@code failFirstPage} the execution future itself fails with it instead.
     */
    private static AsyncCassandraExecutor sliceGAsyncFailingWith(final Throwable failure, final boolean failFirstPage) {
        final CqlSession session = mock(CqlSession.class);
        final com.datastax.oss.driver.api.core.context.DriverContext context = mock(com.datastax.oss.driver.api.core.context.DriverContext.class);
        when(session.getContext()).thenReturn(context);
        when(context.getCodecRegistry()).thenReturn(com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry.DEFAULT);
        final com.datastax.oss.driver.api.core.cql.PreparedStatement prepared = mock(com.datastax.oss.driver.api.core.cql.PreparedStatement.class);
        final ColumnDefinitions noVariables = mock(ColumnDefinitions.class);
        when(noVariables.size()).thenReturn(0);
        when(prepared.getVariableDefinitions()).thenReturn(noVariables);
        when(prepared.bind()).thenReturn(mock(BoundStatement.class));
        when(session.prepare(anyString())).thenReturn(prepared);
        final ColumnDefinitions columns = sliceOColumns();
        when(session.executeAsync(any(Statement.class))).thenAnswer(inv -> {
            final CompletableFuture<AsyncResultSet> failed = new CompletableFuture<>();
            failed.completeExceptionally(failure);

            if (failFirstPage) {
                return failed;
            }

            final Iterator<Row> rows = Arrays.asList(sliceORow(columns, 1L)).iterator();
            final AsyncResultSet first = mock(AsyncResultSet.class);
            when(first.getColumnDefinitions()).thenReturn(columns);
            when(first.currentPage()).thenReturn(() -> rows);
            when(first.hasMorePages()).thenReturn(true);
            when(first.fetchNextPage()).thenReturn(failed);
            return CompletableFuture.completedFuture(first);
        });

        return new CassandraExecutor(session).async();
    }

    /**
     * An Error failing a later page must reach the caller as itself, as a first-page Error and a later-page RuntimeException
     * already did (and as the sync executor over the driver's MultiPageResultSet does): get() reports it as the
     * ExecutionException cause and a stream's terminal operation throws it. It used to arrive wrapped in a plain
     * RuntimeException.
     */
    @Test
    public void testSliceG_laterPageErrorReachesTheCallerUnwrapped() throws Exception {
        final AssertionError error = new AssertionError("page failed");
        final String query = "SELECT v FROM t";

        org.junit.jupiter.api.Assertions.assertAll(() -> {
            final ContinuableFuture<java.util.List<Long>> list = sliceGAsyncFailingWith(error, false).list(Long.class, query);
            assertSame(error, assertThrows(java.util.concurrent.ExecutionException.class, list::get).getCause());
            assertSame(error, assertThrows(java.util.concurrent.ExecutionException.class, list::get).getCause()); // replayed
        }, () -> assertSame(error,
                assertThrows(java.util.concurrent.ExecutionException.class, () -> sliceGAsyncFailingWith(error, false).query(query).get()).getCause()), () -> {
                    final Stream<Long> stream = sliceGAsyncFailingWith(error, false).stream(Long.class, query).get(); // the first page arrived
                    assertSame(error, assertThrows(AssertionError.class, stream::toList));
                }, () -> {
                    final ResultSet rs = sliceGAsyncFailingWith(error, false).execute(query).get();
                    assertEquals(1L, rs.iterator().next().getObject(0));
                    assertSame(error, assertThrows(AssertionError.class, rs::all));
                },
                // control: the same Error failing the first page is the ExecutionException cause
                () -> assertSame(error,
                        assertThrows(java.util.concurrent.ExecutionException.class, () -> sliceGAsyncFailingWith(error, true).list(Long.class, query).get())
                                .getCause()));
    }

    // ---- 2026-10-24 r2 sliceG ----

    /**
     * Pins the update(String, Object...) Javadoc: a query alone, or a query with one Collection&lt;String&gt; of values, binds to
     * update(Object) / update(Object, Collection) and fails at the call site with "Not a bean class" without preparing anything
     * (the old example claimed update("UPDATE SET") threw the driver's parse failure); a malformed query with a value does reach
     * the driver's synchronous prepare.
     */
    @Test
    public void testR2SliceG_updateWithOnlyAQueryBindsToTheEntityOverload() {
        final CqlSession session = mock(CqlSession.class);
        final com.datastax.oss.driver.api.core.context.DriverContext context = mock(com.datastax.oss.driver.api.core.context.DriverContext.class);
        when(session.getContext()).thenReturn(context);
        when(context.getCodecRegistry()).thenReturn(com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry.DEFAULT);
        final IllegalStateException prepareFailure = new IllegalStateException("prepare failed");
        when(session.prepare(anyString())).thenThrow(prepareFailure);
        final AsyncCassandraExecutor facade = new CassandraExecutor(session).async();

        final IllegalArgumentException queryOnly = assertThrows(IllegalArgumentException.class,
                () -> facade.update("UPDATE users SET name = 'x' WHERE id = 1"));
        final IllegalArgumentException queryAndStrings = assertThrows(IllegalArgumentException.class,
                () -> facade.update("UPDATE users SET name = ? WHERE id = ?", Arrays.asList("x", "1")));

        org.junit.jupiter.api.Assertions.assertAll(() -> assertTrue(queryOnly.getMessage().contains("Not a bean class"), queryOnly.getMessage()),
                () -> assertTrue(queryAndStrings.getMessage().contains("Not a bean class"), queryAndStrings.getMessage()),
                () -> org.mockito.Mockito.verify(session, org.mockito.Mockito.never()).prepare(anyString()),
                () -> assertSame(prepareFailure, assertThrows(IllegalStateException.class, () -> facade.update("UPDATE SET", 1L))),
                () -> org.mockito.Mockito.verify(session).prepare("UPDATE SET"));
    }

    /**
     * Pins the @throws IllegalArgumentException clause the v4 async overrides now state: a parameter value rejected when it is
     * converted to its column's type (1.5 for an int column) fails at the call site, while binding, before any request is sent.
     */
    @Test
    public void testR2SliceG_lossyBindValueIsRejectedAtTheCallSite() {
        final CqlSession session = mock(CqlSession.class);
        final com.datastax.oss.driver.api.core.context.DriverContext context = mock(com.datastax.oss.driver.api.core.context.DriverContext.class);
        when(session.getContext()).thenReturn(context);
        when(context.getCodecRegistry()).thenReturn(com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry.DEFAULT);
        final ColumnDefinitions variables = mock(ColumnDefinitions.class);
        final com.datastax.oss.driver.api.core.cql.ColumnDefinition variable = mock(com.datastax.oss.driver.api.core.cql.ColumnDefinition.class);
        when(variables.size()).thenReturn(1);
        when(variables.get(0)).thenReturn(variable);
        when(variable.getType()).thenReturn(com.datastax.oss.driver.api.core.type.DataTypes.INT);
        when(variable.getName()).thenReturn(com.datastax.oss.driver.api.core.CqlIdentifier.fromInternal("k"));
        final com.datastax.oss.driver.api.core.cql.PreparedStatement prepared = mock(com.datastax.oss.driver.api.core.cql.PreparedStatement.class);
        when(prepared.getVariableDefinitions()).thenReturn(variables);
        when(session.prepare(anyString())).thenReturn(prepared);
        final AsyncCassandraExecutor facade = new CassandraExecutor(session).async();
        final String query = "SELECT v FROM t WHERE k = ?";

        org.junit.jupiter.api.Assertions.assertAll(() -> r2SliceGAssertLossyBind(() -> facade.queryForSingleValue(Long.class, query, 1.5)),
                () -> r2SliceGAssertLossyBind(() -> facade.queryForSingleNonNull(Long.class, query, 1.5)),
                () -> r2SliceGAssertLossyBind(() -> facade.findFirst(Long.class, query, 1.5)), () -> r2SliceGAssertLossyBind(() -> facade.stream(query, 1.5)),
                () -> r2SliceGAssertLossyBind(() -> facade.stream(query, (defs, row) -> row, 1.5)),
                () -> r2SliceGAssertLossyBind(() -> facade.execute(query, 1.5)),
                () -> r2SliceGAssertLossyBind(() -> facade.execute(query, java.util.Collections.<String, Object> singletonMap("k", 1.5))),
                () -> org.mockito.Mockito.verify(session, org.mockito.Mockito.never()).executeAsync(any(Statement.class)));
    }

    private static void r2SliceGAssertLossyBind(final org.junit.jupiter.api.function.Executable call) {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, call);
        // The bind-conversion rejection (it names the column's CQL type), not a parameter-count or marker failure.
        assertTrue(e.getMessage().contains("CQL type int"), e.getMessage());
    }
}
