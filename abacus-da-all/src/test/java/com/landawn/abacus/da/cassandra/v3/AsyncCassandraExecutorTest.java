/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.cassandra.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
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
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.util.function.Function;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.da.cassandra.CqlMapper;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.u.Nullable;
import com.landawn.abacus.util.u.Optional;
import com.landawn.abacus.util.stream.Stream;

/**
 * Mockito-based tests for the v3 (Cassandra Driver 3.x) {@link AsyncCassandraExecutor}.
 * The underlying {@link CassandraExecutor} is final, so we rely on Mockito 5's inline mocking.
 */
public class AsyncCassandraExecutorTest extends TestBase {

    private CassandraExecutor mockExecutor;
    private Session mockSession;
    private ResultSet mockResultSet;
    private ResultSetFuture mockFuture;
    private Statement mockStatement;
    private AsyncCassandraExecutor async;

    @BeforeEach
    public void setUp() {
        mockExecutor = mock(CassandraExecutor.class);
        mockSession = mock(Session.class);
        mockResultSet = mock(ResultSet.class);
        mockFuture = mock(ResultSetFuture.class);
        mockStatement = mock(Statement.class);

        when(mockExecutor.session()).thenReturn(mockSession);
        when(mockResultSet.iterator()).thenAnswer(inv -> Collections.<Row> emptyIterator());

        async = new AsyncCassandraExecutor(mockExecutor);
    }

    private ResultSetFuture immediateFuture(final ResultSet rs) throws Exception {
        // ResultSetFuture extends Future<ResultSet>; only get() is exercised by ContinuableFuture.
        final ResultSetFuture f = mock(ResultSetFuture.class);
        when(f.get()).thenReturn(rs);
        when(f.isDone()).thenReturn(true);
        return f;
    }

    @Test
    public void testSync_ReturnsUnderlyingExecutor() {
        assertSame(mockExecutor, async.sync());
    }

    @Test
    public void testConstructor_StoresExecutor() {
        // The constructor is package-private; same-package test can call it.
        AsyncCassandraExecutor a = new AsyncCassandraExecutor(mockExecutor);
        assertNotNull(a);
        assertSame(mockExecutor, a.sync());
    }

    @Test
    public void testExecute_StringOnly_DelegatesToSession() {
        when(mockExecutor.prepareStatement("SELECT * FROM t")).thenReturn(mockStatement);
        when(mockSession.executeAsync(mockStatement)).thenReturn(mockFuture);

        ContinuableFuture<ResultSet> future = async.execute("SELECT * FROM t");

        assertNotNull(future);
        verify(mockSession).executeAsync(mockStatement);
    }

    @Test
    public void testExecute_StringAndParams_DelegatesToSession() {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(mockFuture);

        ContinuableFuture<ResultSet> future = async.execute("SELECT * FROM t WHERE id = ?", 1);

        assertNotNull(future);
        verify(mockSession).executeAsync(mockStatement);
    }

    @Test
    public void testExecute_StringAndMapParams_DelegatesToSession() {
        final Map<String, Object> params = new HashMap<>();
        params.put("id", 1);
        when(mockExecutor.prepareStatement(anyString(), eq(params))).thenReturn(mockStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(mockFuture);

        ContinuableFuture<ResultSet> future = async.execute("SELECT * FROM t WHERE id = :id", params);

        assertNotNull(future);
        verify(mockSession).executeAsync(mockStatement);
    }

    @Test
    public void testExecute_Statement_DelegatesToSession() {
        Statement stmt = new SimpleStatement("SELECT * FROM t");
        when(mockSession.executeAsync(eq(stmt))).thenReturn(mockFuture);

        ContinuableFuture<ResultSet> future = async.execute(stmt);

        assertNotNull(future);
        verify(mockSession).executeAsync(stmt);
    }

    @Test
    public void testStream_StringAndParams_ReturnsStreamFuture() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        final ResultSetFuture _f = immediateFuture(mockResultSet);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(_f);
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Object[]> mapper = (Function) (Function<Row, Object[]>) row -> new Object[] {};
        when(mockExecutor.createRowMapper(eq(Object[].class))).thenReturn(mapper);

        ContinuableFuture<Stream<Object[]>> future = async.stream("SELECT * FROM t WHERE id = ?", 1);

        assertNotNull(future);
        Stream<Object[]> stream = future.get();
        assertNotNull(stream);
        assertEquals(0, stream.count());
    }

    @Test
    public void testStream_WithRowMapper_String() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        final ResultSetFuture _f = immediateFuture(mockResultSet);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(_f);

        final BiFunction<ColumnDefinitions, Row, String> rowMapper = (cd, r) -> "x";
        when(mockExecutor.createRowMapper(eq(rowMapper))).thenReturn(r -> "x");

        ContinuableFuture<Stream<String>> future = async.stream("SELECT * FROM t", rowMapper, 1);

        assertNotNull(future);
        assertEquals(0, future.get().count());
    }

    @Test
    public void testStream_WithRowMapper_Statement() throws Exception {
        final ResultSetFuture _f = immediateFuture(mockResultSet);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(_f);

        final BiFunction<ColumnDefinitions, Row, String> rowMapper = (cd, r) -> "x";
        when(mockExecutor.createRowMapper(eq(rowMapper))).thenReturn(r -> "x");

        Statement stmt = new SimpleStatement("SELECT * FROM t");
        ContinuableFuture<Stream<String>> future = async.stream(stmt, rowMapper);

        assertNotNull(future);
        assertEquals(0, future.get().count());
    }

    @Test
    public void testStream_StringNoParams_ReturnsStreamFuture() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        final ResultSetFuture _f = immediateFuture(mockResultSet);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(_f);
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Object[]> mapper = (Function) (Function<Row, Object[]>) row -> new Object[] {};
        when(mockExecutor.createRowMapper(eq(Object[].class))).thenReturn(mapper);

        ContinuableFuture<Stream<Object[]>> future = async.stream("SELECT * FROM t");

        assertNotNull(future);
        assertEquals(0, future.get().count());
    }

    @Test
    public void testFindFirst_WithEmptyResultSet_ReturnsEmpty() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        final ResultSetFuture _f = immediateFuture(mockResultSet);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(_f);
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, String> mapper = (Function) (Function<Row, String>) row -> "v";
        when(mockExecutor.createRowMapper(eq(String.class))).thenReturn(mapper);

        ContinuableFuture<Optional<String>> future = async.findFirst(String.class, "SELECT * FROM t WHERE id = ?", 1);

        assertNotNull(future);
        Optional<String> result = future.get();
        assertTrue(result.isEmpty());
    }

    @Test
    public void testFindFirst_WithRow_ReturnsValue() throws Exception {
        final Row row = mock(Row.class);
        when(mockResultSet.iterator()).thenReturn(Arrays.asList(row).iterator());
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        final ResultSetFuture _f = immediateFuture(mockResultSet);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(_f);
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
        final ResultSetFuture _f = immediateFuture(mockResultSet);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(_f);
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Integer> mapper = (Function) (Function<Row, Integer>) row -> 42;
        when(mockExecutor.createRowMapper(eq(Integer.class))).thenReturn(mapper);

        Nullable<Integer> result = async.queryForSingleValue(Integer.class, "SELECT v FROM t WHERE id = ?", 1).get();

        assertTrue(result.isEmpty());
    }

    @Test
    public void testQueryForSingleValue_Present() throws Exception {
        final Row row = mock(Row.class);
        when(mockResultSet.iterator()).thenReturn(Arrays.asList(row).iterator());
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        final ResultSetFuture _f = immediateFuture(mockResultSet);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(_f);
        when(mockExecutor.readFirstColumn(row, Integer.class)).thenReturn(42);

        Nullable<Integer> result = async.queryForSingleValue(Integer.class, "SELECT v FROM t WHERE id = ?", 1).get();

        assertTrue(result.isPresent());
        assertEquals(42, result.get().intValue());
    }

    @Test
    public void testQueryForSingleNonNull_Empty() throws Exception {
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        final ResultSetFuture _f = immediateFuture(mockResultSet);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(_f);
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final Function<Row, Integer> mapper = (Function) (Function<Row, Integer>) row -> 42;
        when(mockExecutor.createRowMapper(eq(Integer.class))).thenReturn(mapper);

        Optional<Integer> result = async.queryForSingleNonNull(Integer.class, "SELECT v FROM t WHERE id = ?", 1).get();

        assertTrue(result.isEmpty());
    }

    @Test
    public void testQueryForSingleNonNull_Present() throws Exception {
        final Row row = mock(Row.class);
        when(mockResultSet.iterator()).thenReturn(Arrays.asList(row).iterator());
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        final ResultSetFuture _f = immediateFuture(mockResultSet);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(_f);
        when(mockExecutor.readFirstColumn(row, Integer.class)).thenReturn(99);

        Optional<Integer> result = async.queryForSingleNonNull(Integer.class, "SELECT v FROM t WHERE id = ?", 1).get();

        assertTrue(result.isPresent());
        assertEquals(99, result.get().intValue());
    }

    @Test
    public void testExecute_String_FutureGetReturnsResultSet() throws Exception {
        final ResultSetFuture _f = immediateFuture(mockResultSet);
        when(mockExecutor.prepareStatement("SELECT * FROM t")).thenReturn(mockStatement);
        when(mockSession.executeAsync(mockStatement)).thenReturn(_f);

        ResultSet result = async.execute("SELECT * FROM t").get();

        assertSame(mockResultSet, result);
    }

    @Test
    public void testToListObjectClassReturnsRawRows() {
        // Object.class is assignable from Row, so toList returns the raw driver Row objects
        // (passthrough) without mapping or first-column extraction.
        final ResultSet resultSet = mock(ResultSet.class);
        final Row row = mock(Row.class);
        when(resultSet.all()).thenReturn(Arrays.asList(row));

        final List<Object> result = CassandraExecutor.toList(resultSet, Object.class);

        assertEquals(1, result.size());
        assertSame(row, result.get(0));
    }

    @Test
    public void testStream_RowMapperLambda_IsExercised() throws Exception {
        // Cover the lambda inside stream(String, BiFunction, Object[]) -> map(resultSet -> ...)
        final Row row = mock(Row.class);
        when(mockResultSet.iterator()).thenReturn(Arrays.asList(row).iterator());
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        final ResultSetFuture _f = immediateFuture(mockResultSet);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(_f);

        final BiFunction<ColumnDefinitions, Row, String> rowMapper = (cd, r) -> "X";
        when(mockExecutor.createRowMapper(eq(rowMapper))).thenReturn(r -> "X");

        ContinuableFuture<Stream<String>> future = async.stream("SELECT * FROM t WHERE id = ?", rowMapper, 1);
        Stream<String> stream = future.get();
        Iterator<String> it = stream.iterator();
        assertTrue(it.hasNext());
        assertEquals("X", it.next());
    }

    @Test
    public void testSyncExecutorCachesResolvedCqlButReturnsFreshMutableBoundStatements() {
        final Session session = mock(Session.class);
        final Cluster cluster = mock(Cluster.class);
        final Configuration configuration = mock(Configuration.class);
        final CodecRegistry codecRegistry = mock(CodecRegistry.class);
        final ProtocolOptions protocolOptions = mock(ProtocolOptions.class);
        when(session.init()).thenReturn(session);
        when(session.getCluster()).thenReturn(cluster);
        when(cluster.getConfiguration()).thenReturn(configuration);
        when(configuration.getCodecRegistry()).thenReturn(codecRegistry);
        when(configuration.getProtocolOptions()).thenReturn(protocolOptions);
        when(protocolOptions.getProtocolVersion()).thenReturn(ProtocolVersion.V4);

        final String firstCql = "SELECT * FROM v3_mapper_cache_first";
        final String secondCql = "SELECT * FROM v3_mapper_cache_second";
        final CqlMapper mapper = new CqlMapper();
        mapper.add("lookup", firstCql);

        final PreparedStatement firstPrepared = mock(PreparedStatement.class);
        final PreparedStatement secondPrepared = mock(PreparedStatement.class);
        final BoundStatement firstBound = mock(BoundStatement.class);
        final BoundStatement nextFirstBound = mock(BoundStatement.class);
        final BoundStatement secondBound = mock(BoundStatement.class);
        when(session.prepare(firstCql)).thenReturn(firstPrepared);
        when(session.prepare(secondCql)).thenReturn(secondPrepared);
        when(firstPrepared.bind(any(Object[].class))).thenReturn(firstBound, nextFirstBound);
        when(secondPrepared.bind(any(Object[].class))).thenReturn(secondBound);

        final CassandraExecutor realExecutor = new CassandraExecutor(session, null, mapper);
        assertSame(firstBound, realExecutor.prepareStatement("lookup"));
        assertSame(nextFirstBound, realExecutor.prepareStatement("lookup"));
        assertNotSame(firstBound, nextFirstBound);

        mapper.remove("lookup");
        mapper.add("lookup", secondCql);

        assertSame(secondBound, realExecutor.prepareStatement("lookup"));
        verify(session).prepare(firstCql);
        verify(session).prepare(secondCql);
    }

    @Test
    public void testSyncExecutorRejectsParametersForParameterlessQuery() {
        final Session session = mock(Session.class);
        final Cluster cluster = mock(Cluster.class);
        final Configuration configuration = mock(Configuration.class);
        final ProtocolOptions protocolOptions = mock(ProtocolOptions.class);
        when(session.init()).thenReturn(session);
        when(session.getCluster()).thenReturn(cluster);
        when(cluster.getConfiguration()).thenReturn(configuration);
        when(configuration.getCodecRegistry()).thenReturn(mock(CodecRegistry.class));
        when(configuration.getProtocolOptions()).thenReturn(protocolOptions);
        when(protocolOptions.getProtocolVersion()).thenReturn(ProtocolVersion.V4);

        final String query = "SELECT * FROM v3_parameterless_query";
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ColumnDefinitions variables = mock(ColumnDefinitions.class);
        when(session.prepare(query)).thenReturn(preparedStatement);
        when(preparedStatement.getVariables()).thenReturn(variables);
        when(variables.size()).thenReturn(0);

        final CassandraExecutor realExecutor = new CassandraExecutor(session);
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> realExecutor.prepareStatement(query, 1L));
        assertTrue(ex.getMessage().contains("expected 0 but got 1"), ex.getMessage());
    }
}
