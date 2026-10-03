/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.cassandra.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
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
    public void testConstructor_rejectsNullExecutor() {
        assertThrows(IllegalArgumentException.class, () -> new AsyncCassandraExecutor(null));
    }

    @Test
    public void testNullStatementAndTargetClass_ThrowIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> async.execute((Statement) null));
        assertThrows(IllegalArgumentException.class, () -> async.stream(Object.class, (Statement) null));
        assertThrows(IllegalArgumentException.class, () -> async.get((Class<Object>) null, 1L));
        assertThrows(IllegalArgumentException.class, () -> async.gett((Class<Object>) null, 1L));
        assertThrows(IllegalArgumentException.class, () -> async.exists((Class<?>) null, 1L));
        assertThrows(IllegalArgumentException.class, () -> async.delete((Class<?>) null, 1L));
        assertThrows(IllegalArgumentException.class, () -> async.delete((Class<?>) null, (java.util.Collection<String>) null, 1L));
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
    public void testNullFunctionalInterfaceArguments() {
        assertThrows(IllegalArgumentException.class, () -> async.stream("SELECT * FROM t", (BiFunction<ColumnDefinitions, Row, Object>) null));
        assertThrows(IllegalArgumentException.class, () -> async.stream((Statement) null, (BiFunction<ColumnDefinitions, Row, Object>) null));

        assertThrows(IllegalArgumentException.class, () -> async.stream(mockStatement, (BiFunction<ColumnDefinitions, Row, Object>) null));

        final Session session = mock(Session.class);
        final Cluster cluster = mock(Cluster.class);
        final Configuration configuration = mock(Configuration.class);
        final ProtocolOptions protocolOptions = mock(ProtocolOptions.class);
        when(session.getCluster()).thenReturn(cluster);
        when(cluster.getConfiguration()).thenReturn(configuration);
        when(configuration.getCodecRegistry()).thenReturn(mock(CodecRegistry.class));
        when(configuration.getProtocolOptions()).thenReturn(protocolOptions);
        when(protocolOptions.getProtocolVersion()).thenReturn(ProtocolVersion.V4);

        final CassandraExecutor executor = new CassandraExecutor(session);
        assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toMap((Row) null, null));
        assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toMap(mock(Row.class), null));
        assertThrows(IllegalArgumentException.class, () -> executor.stream("SELECT * FROM t", (BiFunction<ColumnDefinitions, Row, Object>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.stream((Statement) null, (BiFunction<ColumnDefinitions, Row, Object>) null));
    }

    /**
     * A null target/value class is rejected up front as an illegal argument: it is consumed by the
     * abacus reflection/mapping layer, never handed to the driver. The guards run before any
     * statement is prepared, so no session interaction happens.
     */
    @Test
    public void testNullTargetClassArguments() {
        assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toEntity(mock(Row.class), (Class<Object>) null));
        assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toList(mock(ResultSet.class), (Class<Object>) null));

        final Session session = mock(Session.class);
        final Cluster cluster = mock(Cluster.class);
        final Configuration configuration = mock(Configuration.class);
        final ProtocolOptions protocolOptions = mock(ProtocolOptions.class);
        when(session.getCluster()).thenReturn(cluster);
        when(cluster.getConfiguration()).thenReturn(configuration);
        when(configuration.getCodecRegistry()).thenReturn(mock(CodecRegistry.class));
        when(configuration.getProtocolOptions()).thenReturn(protocolOptions);
        when(protocolOptions.getProtocolVersion()).thenReturn(ProtocolVersion.V4);

        final CassandraExecutor executor = new CassandraExecutor(session);
        assertThrows(IllegalArgumentException.class, () -> executor.findFirst((Class<Object>) null, "SELECT * FROM t"));
        assertThrows(IllegalArgumentException.class, () -> executor.queryForSingleValue((Class<Object>) null, "SELECT id FROM t"));
        assertThrows(IllegalArgumentException.class, () -> executor.queryForSingleNonNull((Class<Object>) null, "SELECT id FROM t"));
    }

    /** A null required argument of the v3 sync executor's public API is rejected with IllegalArgumentException, not NullPointerException. */
    @Test
    public void testSyncNullRequiredArgumentsThrowIAE() {
        final Session session = mock(Session.class);
        final Cluster cluster = mock(Cluster.class);
        final Configuration configuration = mock(Configuration.class);
        final ProtocolOptions protocolOptions = mock(ProtocolOptions.class);
        when(session.getCluster()).thenReturn(cluster);
        when(cluster.getConfiguration()).thenReturn(configuration);
        when(configuration.getCodecRegistry()).thenReturn(mock(CodecRegistry.class));
        when(configuration.getProtocolOptions()).thenReturn(protocolOptions);
        when(protocolOptions.getProtocolVersion()).thenReturn(ProtocolVersion.V4);

        final CassandraExecutor executor = new CassandraExecutor(session);

        assertThrows(IllegalArgumentException.class, () -> new CassandraExecutor(null, null, null, null));
        assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.extractData((ResultSet) null));
        assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.extractData((ResultSet) null, Map.class));
        assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toList((ResultSet) null, Map.class));
        assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toEntity((Row) null, Object.class));
        assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.toMap((Row) null));
        assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.registerTypeCodec((CodecRegistry) null, Map.class));
        assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.registerTypeCodec(mock(CodecRegistry.class), null));
        assertThrows(IllegalArgumentException.class, () -> executor.registerTypeCodec(null));
        assertThrows(IllegalArgumentException.class, () -> executor.mapper(null));
        assertThrows(IllegalArgumentException.class, () -> executor.execute((Statement) null));
        assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.UDTCodec.create((com.datastax.driver.core.UserType) null, Map.class));
        assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.UDTCodec.create((Cluster) null, "ks", "udt", Map.class));
        assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.UDTCodec.create(cluster, null, "udt", Map.class));
        assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.UDTCodec.create(cluster, "ks", null, Map.class));
        assertThrows(IllegalArgumentException.class, () -> CassandraExecutor.UDTCodec.create(cluster, "ks", "udt", null));
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

    // ---- sliceP 2026-09-22: ContinuableFuture.map re-applies its function on every get() ----

    @Test
    public void testStream_QueryRowMapper_RepeatedGetDoesNotReReadResultSet() throws Exception {
        final Row row = mock(Row.class);
        final ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.iterator()).thenReturn(Arrays.asList(row).iterator()); // one-shot cursor, like the driver's
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        final ResultSetFuture f = immediateFuture(resultSet);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(f);

        final BiFunction<ColumnDefinitions, Row, String> rowMapper = (cd, r) -> "X";
        when(mockExecutor.createRowMapper(eq(rowMapper))).thenReturn(r -> "X");

        final ContinuableFuture<Stream<String>> future = async.stream("SELECT * FROM t WHERE id = ?", rowMapper, 1);
        final Stream<String> first = future.get();
        final Stream<String> second = future.get();

        assertSame(first, second);
        assertEquals(Arrays.asList("X"), first.toList());
        verify(resultSet, org.mockito.Mockito.times(1)).iterator();
    }

    @Test
    public void testStream_StatementRowMapper_RepeatedGetDoesNotReReadResultSet() throws Exception {
        final Row row = mock(Row.class);
        final ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.iterator()).thenReturn(Arrays.asList(row).iterator());
        final ResultSetFuture f = immediateFuture(resultSet);
        when(mockSession.executeAsync(mockStatement)).thenReturn(f);

        final BiFunction<ColumnDefinitions, Row, String> rowMapper = (cd, r) -> "Y";
        when(mockExecutor.createRowMapper(eq(rowMapper))).thenReturn(r -> "Y");

        final ContinuableFuture<Stream<String>> future = async.stream(mockStatement, rowMapper);
        final Stream<String> first = future.get();
        final Stream<String> second = future.get();

        assertSame(first, second);
        assertEquals(Arrays.asList("Y"), first.toList());
        verify(resultSet, org.mockito.Mockito.times(1)).iterator();
    }

    // ---- sliceP 2026-09-22: BLOB <-> byte[] conversions (N.convert/PropInfo.setPropValue lose the bytes) ----

    private static CassandraExecutor newSlicePExecutor(final Session session) {
        final Cluster cluster = mock(Cluster.class);
        final Configuration configuration = mock(Configuration.class);
        final ProtocolOptions protocolOptions = mock(ProtocolOptions.class);
        when(session.init()).thenReturn(session);
        when(session.getCluster()).thenReturn(cluster);
        when(cluster.getConfiguration()).thenReturn(configuration);
        when(configuration.getCodecRegistry()).thenReturn(new CodecRegistry());
        when(configuration.getProtocolOptions()).thenReturn(protocolOptions);
        when(protocolOptions.getProtocolVersion()).thenReturn(ProtocolVersion.V4);

        return new CassandraExecutor(session);
    }

    @Test
    public void testSliceP_bindByteArrayToBlobMarkerKeepsAllBytes() {
        final Session session = mock(Session.class);
        final CassandraExecutor executor = newSlicePExecutor(session);
        final String query = "SELECT * FROM slice_p_blobs WHERE data = ?";
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ColumnDefinitions variables = mock(ColumnDefinitions.class);
        final Object[][] bound = new Object[1][];
        when(session.prepare(query)).thenReturn(preparedStatement);
        when(preparedStatement.getVariables()).thenReturn(variables);
        when(variables.size()).thenReturn(1);
        when(variables.getType(0)).thenReturn(com.datastax.driver.core.DataType.blob());
        when(variables.getName(0)).thenReturn("data");
        when(preparedStatement.bind(any(Object[].class))).thenAnswer(invocation -> {
            bound[0] = (Object[]) invocation.getRawArguments()[0];
            return mock(BoundStatement.class);
        });

        executor.prepareStatement(query, new byte[] { 1, 2, 3 });

        assertEquals(1, bound[0].length);
        assertEquals(java.nio.ByteBuffer.wrap(new byte[] { 1, 2, 3 }), bound[0][0]);
        assertEquals(3, ((java.nio.ByteBuffer) bound[0][0]).remaining());
    }

    @Test
    public void testSliceP_blobColumnReadIntoByteArrayTargetsKeepsBytes() {
        final ColumnDefinitions cols = mock(ColumnDefinitions.class);
        when(cols.size()).thenReturn(1);
        when(cols.getName(0)).thenReturn("data");
        final Row row = mock(Row.class);
        when(row.getColumnDefinitions()).thenReturn(cols);
        when(row.getObject(0)).thenAnswer(invocation -> java.nio.ByteBuffer.wrap(new byte[] { 4, 5 }));
        final ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getColumnDefinitions()).thenReturn(cols);
        when(resultSet.all()).thenReturn(Arrays.asList(row));

        // toEntity: byte[] bean property
        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[] { 4, 5 }, CassandraExecutor.toEntity(row, SlicePBlobEntity.class).getData());

        // single-value row mapping (list/stream path)
        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[] { 4, 5 }, CassandraExecutor.toList(resultSet, byte[].class).get(0));

        // extractData with an entity whose matching property is byte[]
        final com.landawn.abacus.util.Dataset ds = CassandraExecutor.extractData(resultSet, SlicePBlobEntity.class);
        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[] { 4, 5 }, (byte[]) ds.getColumn("data").get(0));

        // readFirstColumn (async queryForSingleValue family), then the sync queryForSingleValue/NonNull and findFirst
        final Session session = mock(Session.class);
        final CassandraExecutor executor = newSlicePExecutor(session);
        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[] { 4, 5 }, executor.readFirstColumn(row, byte[].class));

        final String query = "SELECT data FROM slice_p_blobs";
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final BoundStatement boundStatement = mock(BoundStatement.class);
        when(session.prepare(query)).thenReturn(preparedStatement);
        when(preparedStatement.bind(any(Object[].class))).thenReturn(boundStatement);
        when(session.execute(boundStatement)).thenAnswer(invocation -> {
            final ResultSet rs = mock(ResultSet.class);
            when(rs.one()).thenReturn(row);
            return rs;
        });

        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[] { 4, 5 }, executor.queryForSingleValue(byte[].class, query).get());
        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[] { 4, 5 }, executor.queryForSingleNonNull(byte[].class, query).get());
        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[] { 4, 5 }, executor.findFirst(byte[].class, query).get());
    }

    @Test
    public void testSliceP_udtCodecBeanDeserializeKeepsBlobBytes() {
        final com.datastax.driver.core.UserType userType = mock(com.datastax.driver.core.UserType.class);
        when(userType.getName()).thenReturn(com.datastax.driver.core.DataType.Name.UDT);
        when(userType.getFieldNames()).thenReturn(Arrays.asList("data"));
        when(userType.size()).thenReturn(1);
        final com.datastax.driver.core.UDTValue udtValue = mock(com.datastax.driver.core.UDTValue.class);
        when(udtValue.getObject(0)).thenAnswer(invocation -> java.nio.ByteBuffer.wrap(new byte[] { 6, 7 }));

        final CassandraExecutor.UDTCodec<SlicePBlobEntity> codec = CassandraExecutor.UDTCodec.create(userType, SlicePBlobEntity.class);

        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[] { 6, 7 }, codec.deserialize(udtValue).getData());
    }

    @Test
    public void testUdtBlobFieldsAcceptByteArraysInBeansMapsAndLists() throws ReflectiveOperationException {
        // Driver 3 exposes UDT metadata only through cluster discovery; construct the real metadata offline.
        final var fieldConstructor = com.datastax.driver.core.UserType.Field.class.getDeclaredConstructor(String.class, com.datastax.driver.core.DataType.class);
        fieldConstructor.setAccessible(true);
        final var field = fieldConstructor.newInstance("data", com.datastax.driver.core.DataType.blob());
        final var typeConstructor = com.datastax.driver.core.UserType.class.getDeclaredConstructor(String.class, String.class, boolean.class,
                java.util.Collection.class, ProtocolVersion.class, CodecRegistry.class);
        typeConstructor.setAccessible(true);
        final CodecRegistry registry = new CodecRegistry();
        final var userType = typeConstructor.newInstance("ks", "blob_type", false, List.of(field), ProtocolVersion.V4, registry);
        final CassandraExecutor.UDTCodec<SlicePBlobEntity> beanCodec = CassandraExecutor.UDTCodec.create(userType, SlicePBlobEntity.class);
        final CassandraExecutor.UDTCodec<Map> mapCodec = CassandraExecutor.UDTCodec.create(userType, Map.class);
        final CassandraExecutor.UDTCodec<List> listCodec = CassandraExecutor.UDTCodec.create(userType, List.class);

        for (final byte[] bytes : new byte[][] { new byte[0], { 1, 2, 3 } }) {
            final SlicePBlobEntity bean = new SlicePBlobEntity();
            bean.setData(bytes);
            org.junit.jupiter.api.Assertions.assertArrayEquals(bytes, beanCodec.deserialize(beanCodec.serialize(bean, ProtocolVersion.V4), ProtocolVersion.V4).getData());
            assertEquals(java.nio.ByteBuffer.wrap(bytes), mapCodec.deserialize(mapCodec.serialize(Map.of("data", bytes), ProtocolVersion.V4), ProtocolVersion.V4).get("data"));
            assertEquals(java.nio.ByteBuffer.wrap(bytes), listCodec.deserialize(listCodec.serialize(List.of(bytes), ProtocolVersion.V4), ProtocolVersion.V4).get(0));
            org.junit.jupiter.api.Assertions.assertArrayEquals(bytes, beanCodec.parse(beanCodec.format(bean)).getData());
        }

        final java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(new byte[] { 9, 4, 5 });
        buffer.position(1);
        assertEquals(java.nio.ByteBuffer.wrap(new byte[] { 4, 5 }), mapCodec.deserialize(mapCodec.serialize(Map.of("data", buffer), ProtocolVersion.V4), ProtocolVersion.V4).get("data"));
        assertEquals(1, buffer.position());
        assertNull(beanCodec.deserialize(beanCodec.serialize(new SlicePBlobEntity(), ProtocolVersion.V4), ProtocolVersion.V4).getData());

        final java.util.concurrent.atomic.AtomicReference<RuntimeException> codecFailure = new java.util.concurrent.atomic.AtomicReference<>();
        registry.register(new com.datastax.driver.core.TypeCodec<byte[]>(com.datastax.driver.core.DataType.blob(), byte[].class) {
            @Override
            public java.nio.ByteBuffer serialize(final byte[] value, final ProtocolVersion version) {
                if (codecFailure.get() != null) {
                    throw codecFailure.get();
                }
                return java.nio.ByteBuffer.wrap(new byte[] { 42 });
            }

            @Override
            public byte[] deserialize(final java.nio.ByteBuffer bytes, final ProtocolVersion version) {
                return new byte[] { 42 };
            }

            @Override
            public byte[] parse(final String value) {
                return new byte[] { 42 };
            }

            @Override
            public String format(final byte[] value) {
                return "0x2a";
            }
        });
        final SlicePBlobEntity customValue = new SlicePBlobEntity();
        customValue.setData(new byte[] { 1, 2, 3 });
        assertEquals(java.nio.ByteBuffer.wrap(new byte[] { 42 }), beanCodec.serialize(customValue).getBytes("data"));
        final IllegalStateException failure = new IllegalStateException("custom serializer failed");
        codecFailure.set(failure);
        assertSame(failure, assertThrows(IllegalStateException.class, () -> beanCodec.serialize(customValue)));
        final com.datastax.driver.core.exceptions.CodecNotFoundException unspecifiedType = new com.datastax.driver.core.exceptions.CodecNotFoundException(
                "custom lookup failed", com.datastax.driver.core.DataType.blob(), null);
        codecFailure.set(unspecifiedType);
        assertSame(unspecifiedType, assertThrows(com.datastax.driver.core.exceptions.CodecNotFoundException.class, () -> beanCodec.serialize(customValue)));
    }

    @Test
    public void testRegisteredBeanCodecTakesPrecedenceOverParameterExpansion() {
        final Session session = mock(Session.class);
        final CassandraExecutor codecExecutor = newSlicePExecutor(session);
        codecExecutor.registerTypeCodec(SlicePBlobEntity.class);
        final String query = "INSERT INTO custom_values (payload) VALUES (?)";
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ColumnDefinitions variables = mock(ColumnDefinitions.class);
        when(session.prepare(query)).thenReturn(preparedStatement);
        when(preparedStatement.getVariables()).thenReturn(variables);
        when(variables.size()).thenReturn(1);
        when(variables.getType(0)).thenReturn(com.datastax.driver.core.DataType.varchar());
        when(variables.getName(0)).thenReturn("payload");
        final Object[][] bound = new Object[1][];
        when(preparedStatement.bind(any(Object[].class))).thenAnswer(invocation -> {
            bound[0] = (Object[]) invocation.getRawArguments()[0];
            return mock(BoundStatement.class);
        });
        final SlicePBlobEntity value = new SlicePBlobEntity();
        value.setData(new byte[] { 1, 2, 3 });

        codecExecutor.prepareStatement(query, value);
        assertSame(value, bound[0][0]);
        codecExecutor.prepareStatement(query, List.of(value));
        assertSame(value, bound[0][0]);
        codecExecutor.prepareStatement(query, Map.of("payload", value));
        assertSame(value, bound[0][0]);
    }

    @Test
    public void testRegisteredEnumCodecTakesPrecedenceOverScalarConversion() {
        final Session session = mock(Session.class);
        final CassandraExecutor codecExecutor = newSlicePExecutor(session);
        final CodecRegistry registry = session.getCluster().getConfiguration().getCodecRegistry();
        final String query = "INSERT INTO custom_values (payload) VALUES (?)";
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ColumnDefinitions variables = mock(ColumnDefinitions.class);
        when(session.prepare(query)).thenReturn(preparedStatement);
        when(preparedStatement.getVariables()).thenReturn(variables);
        when(variables.size()).thenReturn(1);
        when(variables.getType(0)).thenReturn(com.datastax.driver.core.DataType.varchar());
        when(variables.getName(0)).thenReturn("payload");
        final Object[][] bound = new Object[1][];
        when(preparedStatement.bind(any(Object[].class))).thenAnswer(invocation -> {
            bound[0] = (Object[]) invocation.getRawArguments()[0];
            return mock(BoundStatement.class);
        });
        final java.time.DayOfWeek value = java.time.DayOfWeek.MONDAY;

        // Without a value-specific codec, the existing scalar conversion remains available.
        codecExecutor.prepareStatement(query, value);
        assertEquals("MONDAY", bound[0][0]);
        codecExecutor.prepareStatement(query, List.of(value));
        assertEquals("MONDAY", bound[0][0]);
        codecExecutor.prepareStatement(query, Map.of("payload", value));
        assertEquals("MONDAY", bound[0][0]);

        final java.util.concurrent.atomic.AtomicReference<RuntimeException> acceptanceFailure = new java.util.concurrent.atomic.AtomicReference<>();
        registry.register(new CassandraExecutor.StringCodec<java.time.DayOfWeek>(java.time.DayOfWeek.class) {
            @Override
            public boolean accepts(final Object candidate) {
                if (candidate instanceof java.time.DayOfWeek && acceptanceFailure.get() != null) {
                    throw acceptanceFailure.get();
                }
                return super.accepts(candidate);
            }
        });

        codecExecutor.prepareStatement(query, value);
        assertSame(value, bound[0][0]);
        codecExecutor.prepareStatement(query, List.of(value));
        assertSame(value, bound[0][0]);
        codecExecutor.prepareStatement(query, Map.of("payload", value));
        assertSame(value, bound[0][0]);

        // A registered codec's failure must propagate, rather than silently falling back to String.
        final IllegalStateException failure = new IllegalStateException("codec acceptance failed");
        acceptanceFailure.set(failure);
        assertSame(failure, assertThrows(IllegalStateException.class, () -> codecExecutor.prepareStatement(query, value)));
        assertSame(failure, assertThrows(IllegalStateException.class, () -> codecExecutor.prepareStatement(query, List.of(value))));
        assertSame(failure, assertThrows(IllegalStateException.class, () -> codecExecutor.prepareStatement(query, Map.of("payload", value))));
    }

    @Test
    public void testToMapPropagatesColumnDecodingFailure() {
        final Row row = mock(Row.class);
        final ColumnDefinitions columns = mock(ColumnDefinitions.class);
        when(row.getColumnDefinitions()).thenReturn(columns);
        when(columns.size()).thenReturn(1);
        when(columns.getName(0)).thenReturn("payload");
        final IllegalStateException failure = new IllegalStateException("column codec failed");
        when(row.getObject(0)).thenThrow(failure);

        assertSame(failure, assertThrows(IllegalStateException.class, () -> CassandraExecutor.toMap(row)));
        assertSame(failure, assertThrows(IllegalStateException.class, () -> CassandraExecutor.toMap(row, HashMap::new)));
    }

    @Test
    public void testEntityParameterBindingResolvesColumnAnnotatedProperty() {
        // A bean bound as the parameter source resolves a driver variable named by @Column ("cnt") through the
        // column-to-property mapping, as toEntity/extractData already do, instead of failing as a missing parameter.
        final Session session = mock(Session.class);
        final CassandraExecutor executor = newSlicePExecutor(session);
        final String query = "INSERT INTO renamed_columns (id, cnt) VALUES (?, ?)";
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ColumnDefinitions variables = mock(ColumnDefinitions.class);
        final Object[][] bound = new Object[1][];
        when(session.prepare(query)).thenReturn(preparedStatement);
        when(preparedStatement.getVariables()).thenReturn(variables);
        when(variables.size()).thenReturn(2);
        when(variables.getType(0)).thenReturn(com.datastax.driver.core.DataType.bigint());
        when(variables.getName(0)).thenReturn("id");
        when(variables.getType(1)).thenReturn(com.datastax.driver.core.DataType.cint());
        when(variables.getName(1)).thenReturn("cnt");
        when(preparedStatement.bind(any(Object[].class))).thenAnswer(invocation -> {
            bound[0] = (Object[]) invocation.getRawArguments()[0];
            return mock(BoundStatement.class);
        });
        final SlicePRenamedColumnEntity entity = new SlicePRenamedColumnEntity();
        entity.setId(7L);
        entity.setOrderCount(3);

        executor.prepareStatement(query, entity);

        assertEquals(2, bound[0].length);
        assertEquals(7L, bound[0][0]);
        assertEquals(3, bound[0][1]);

        // A variable that matches neither a property nor a @Column name is still reported as missing.
        when(variables.getName(1)).thenReturn("no_such_column");
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> executor.prepareStatement(query, entity));
        assertTrue(ex.getMessage().contains("no_such_column"), ex.getMessage());
    }

    @Test
    public void testUdtCodecDeserializeReadsCaseSensitiveFieldsByPosition() throws ReflectiveOperationException {
        // Driver 3 resolves String field names as CQL identifiers (unquoted names are lower-cased), so a quoted
        // mixed-case UDT field such as "firstName" cannot be read by its internal name; fields are read by position.
        final var fieldConstructor = com.datastax.driver.core.UserType.Field.class.getDeclaredConstructor(String.class, com.datastax.driver.core.DataType.class);
        fieldConstructor.setAccessible(true);
        final var firstName = fieldConstructor.newInstance("firstName", com.datastax.driver.core.DataType.text());
        final var lastName = fieldConstructor.newInstance("lastName", com.datastax.driver.core.DataType.text());
        final var typeConstructor = com.datastax.driver.core.UserType.class.getDeclaredConstructor(String.class, String.class, boolean.class,
                java.util.Collection.class, ProtocolVersion.class, CodecRegistry.class);
        typeConstructor.setAccessible(true);
        final var userType = typeConstructor.newInstance("ks", "full_name", false, List.of(firstName, lastName), ProtocolVersion.V4, new CodecRegistry());
        final CassandraExecutor.UDTCodec<SlicePNameEntity> beanCodec = CassandraExecutor.UDTCodec.create(userType, SlicePNameEntity.class);
        final CassandraExecutor.UDTCodec<Map> mapCodec = CassandraExecutor.UDTCodec.create(userType, Map.class);

        final SlicePNameEntity bean = new SlicePNameEntity();
        bean.setFirstName("Ada");
        bean.setLastName("Lovelace");

        final SlicePNameEntity decodedBean = beanCodec.deserialize(beanCodec.serialize(bean, ProtocolVersion.V4), ProtocolVersion.V4);
        assertEquals("Ada", decodedBean.getFirstName());
        assertEquals("Lovelace", decodedBean.getLastName());

        final Map<String, Object> source = new HashMap<>();
        source.put("firstName", "Grace");
        source.put("lastName", "Hopper");
        final Map<?, ?> decodedMap = mapCodec.deserialize(mapCodec.serialize(source, ProtocolVersion.V4), ProtocolVersion.V4);
        assertEquals("Grace", decodedMap.get("firstName"));
        assertEquals("Hopper", decodedMap.get("lastName"));
        assertEquals(2, decodedMap.size());
    }

    @Test
    public void testQueryForSingleNonNull_NullValue_GetThrowsExecutionExceptionCausedByNPE() throws Exception {
        final Row row = mock(Row.class);
        when(mockResultSet.iterator()).thenReturn(Arrays.asList(row).iterator());
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        final ResultSetFuture _f = immediateFuture(mockResultSet);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(_f);
        when(mockExecutor.readFirstColumn(row, Integer.class)).thenReturn(null);

        final ContinuableFuture<Optional<Integer>> future = async.queryForSingleNonNull(Integer.class, "SELECT v FROM t WHERE id = ?", 1);

        final java.util.concurrent.ExecutionException ex = assertThrows(java.util.concurrent.ExecutionException.class, future::get);
        assertTrue(ex.getCause() instanceof NullPointerException, String.valueOf(ex.getCause()));
    }

    // ---- sliceP 2026-09-27 regression tests ----

    @Test
    public void testNamedMapParameterForSingleMapColumnMarkerBindsTheNamedValue() {
        // execute(String, Map) documents the Map as NAME -> VALUE. For a query with one named marker whose column is itself a
        // map type, the whole Map used to be bound as the column value (a Map is assignable to the map column's Java type).
        final Session session = mock(Session.class);
        final CassandraExecutor executor = newSlicePExecutor(session);
        final String namedQuery = "UPDATE t SET m = :m WHERE id = 1";
        final String positionalQuery = "UPDATE t SET m = ? WHERE id = 1";
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ColumnDefinitions variables = mock(ColumnDefinitions.class);
        when(session.prepare(positionalQuery)).thenReturn(preparedStatement);
        when(preparedStatement.getVariables()).thenReturn(variables);
        when(variables.size()).thenReturn(1);
        when(variables.getType(0)).thenReturn(com.datastax.driver.core.DataType.map(com.datastax.driver.core.DataType.text(),
                com.datastax.driver.core.DataType.cint()));
        when(variables.getName(0)).thenReturn("m");
        final Object[][] bound = new Object[1][];
        when(preparedStatement.bind(any(Object[].class))).thenAnswer(invocation -> {
            bound[0] = (Object[]) invocation.getRawArguments()[0];
            return mock(BoundStatement.class);
        });
        final Map<String, Integer> columnValue = Map.of("a", 1);

        executor.prepareStatement(namedQuery, Map.of("m", columnValue));
        assertEquals(1, bound[0].length);
        assertEquals(columnValue, bound[0][0]);

        // A Map that is not keyed by the marker name, or a Map for a positional marker, is still bound as the column value.
        executor.prepareStatement(namedQuery, columnValue);
        assertSame(columnValue, bound[0][0]);
        final Map<String, Integer> positionalValue = Map.of("m", 5);
        executor.prepareStatement(positionalQuery, positionalValue);
        assertSame(positionalValue, bound[0][0]);
    }

    @Test
    public void testTypedArrayRowTargetConvertsColumnValuesToComponentType() {
        // Typed array row targets (documented alongside Object[]) used to store the raw driver value, so an int column
        // read into String[]/Long[] threw ArrayStoreException.
        final ColumnDefinitions cols = mock(ColumnDefinitions.class);
        when(cols.size()).thenReturn(2);
        final Row row = mock(Row.class);
        when(row.getColumnDefinitions()).thenReturn(cols);
        when(row.getObject(0)).thenReturn(1);
        when(row.getObject(1)).thenReturn(null);
        final ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getColumnDefinitions()).thenReturn(cols);
        when(resultSet.all()).thenReturn(List.of(row));

        final List<String[]> strings = CassandraExecutor.toList(resultSet, String[].class);
        assertEquals(1, strings.size());
        assertEquals(Arrays.asList("1", null), Arrays.asList(strings.get(0)));
        assertEquals(Arrays.asList(1, null), Arrays.asList(CassandraExecutor.toList(resultSet, Object[].class).get(0)));

        // Single-row path (findFirst/gett) goes through readRow.
        when(resultSet.one()).thenReturn(row);
        when(resultSet.isExhausted()).thenReturn(true);
        final Long[] longs = newSlicePExecutor(mock(Session.class)).fetchOnlyOne(Long[].class, resultSet);
        assertEquals(Arrays.asList(1L, null), Arrays.asList(longs));
    }

    @Test
    public void testStream_RowMapperError_RepeatedGetReplaysErrorWithoutReReadingResultSet() throws Exception {
        // ContinuableFuture.map reports an Error from its function through get() as well; the memoized function must replay it
        // instead of re-running and re-reading the one-shot cursor (which would make the second get() "succeed").
        final Row row = mock(Row.class);
        final ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.iterator()).thenReturn(Arrays.asList(row).iterator());
        when(mockExecutor.prepareStatement(anyString(), any(Object[].class))).thenReturn(mockStatement);
        final ResultSetFuture f = immediateFuture(resultSet);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(f);

        final BiFunction<ColumnDefinitions, Row, String> rowMapper = (cd, r) -> "X";
        final AssertionError error = new AssertionError("mapper setup failed");
        when(mockExecutor.createRowMapper(eq(rowMapper))).thenThrow(error).thenReturn(r -> "X");

        final ContinuableFuture<Stream<String>> future = async.stream("SELECT * FROM t WHERE id = ?", rowMapper, 1);

        assertSame(error, assertThrows(java.util.concurrent.ExecutionException.class, future::get).getCause());
        assertSame(error, assertThrows(java.util.concurrent.ExecutionException.class, future::get).getCause());
        verify(resultSet, org.mockito.Mockito.times(1)).iterator();
    }

    public static class SlicePRenamedColumnEntity {
        private Long id;
        @com.landawn.abacus.annotation.Column("cnt")
        private Integer orderCount;

        public Long getId() {
            return id;
        }

        public void setId(final Long id) {
            this.id = id;
        }

        public Integer getOrderCount() {
            return orderCount;
        }

        public void setOrderCount(final Integer orderCount) {
            this.orderCount = orderCount;
        }
    }

    public static class SlicePNameEntity {
        private String firstName;
        private String lastName;

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(final String firstName) {
            this.firstName = firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public void setLastName(final String lastName) {
            this.lastName = lastName;
        }
    }

    public static class SlicePBlobEntity {
        private byte[] data;

        public byte[] getData() {
            return data;
        }

        public void setData(final byte[] data) {
            this.data = data;
        }
    }

    // ---- 2026-09-29 sliceP ----

    @Test
    public void testSliceP_sortedMapWithNonStringKeysForSingleNamedMapMarkerIsBoundAsValue() {
        // The named-container check (Map keyed by the single named marker's name) called containsKey(String) on the
        // value map; a TreeMap<Integer, String> bound to a map<int, text> column threw ClassCastException from that probe.
        final Session session = mock(Session.class);
        final CassandraExecutor executor = newSlicePExecutor(session);
        final String namedQuery = "UPDATE t SET m = :m WHERE id = 1";
        final String positionalQuery = "UPDATE t SET m = ? WHERE id = 1";
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ColumnDefinitions variables = mock(ColumnDefinitions.class);
        when(session.prepare(positionalQuery)).thenReturn(preparedStatement);
        when(preparedStatement.getVariables()).thenReturn(variables);
        when(variables.size()).thenReturn(1);
        when(variables.getType(0)).thenReturn(com.datastax.driver.core.DataType.map(com.datastax.driver.core.DataType.cint(),
                com.datastax.driver.core.DataType.text()));
        when(variables.getName(0)).thenReturn("m");
        final Object[][] bound = new Object[1][];
        when(preparedStatement.bind(any(Object[].class))).thenAnswer(invocation -> {
            bound[0] = (Object[]) invocation.getRawArguments()[0];
            return mock(BoundStatement.class);
        });
        final Map<Integer, String> columnValue = new java.util.TreeMap<>(Map.of(1, "a", 2, "b"));

        executor.prepareStatement(namedQuery, columnValue);
        assertEquals(1, bound[0].length);
        assertSame(columnValue, bound[0][0]);

        // A String-keyed map naming the marker is still read as the named-parameter container.
        final Map<Integer, String> namedValue = Map.of(3, "c");
        executor.prepareStatement(namedQuery, new java.util.TreeMap<>(Map.of("m", namedValue)));
        assertSame(namedValue, bound[0][0]);
    }

    @com.landawn.abacus.annotation.Entity
    public static class SlicePReadOnlyBase {
        private String first;

        public String getFirst() {
            return first;
        }

        public void setFirst(final String first) {
            this.first = first;
        }

        public String getFullName() {
            return "full:" + first;
        }
    }

    public static class SlicePReadOnlyEntity extends SlicePReadOnlyBase {
        private int id;

        public int getId() {
            return id;
        }

        public void setId(final int id) {
            this.id = id;
        }
    }

    @Test
    public void testSliceP_getterOnlyInheritedPropertyIsSkippedOnRead() throws ReflectiveOperationException {
        // abacus-common 8.1.0 lists a getter-only property inherited from an @Entity superclass, so the generated
        // INSERT/SELECT include its column ("full_name AS \"fullName\""); reading it back used to throw
        // UnsupportedOperationException from PropInfo.setPropValue and fail the whole row.
        final ColumnDefinitions cols = mock(ColumnDefinitions.class);
        when(cols.size()).thenReturn(3);
        when(cols.getName(0)).thenReturn("id");
        when(cols.getName(1)).thenReturn("first");
        when(cols.getName(2)).thenReturn("fullName");
        final Row row = mock(Row.class);
        when(row.getColumnDefinitions()).thenReturn(cols);
        when(row.getObject(0)).thenReturn(7);
        when(row.getObject(1)).thenReturn("x");
        when(row.getObject(2)).thenReturn("full:x");

        final SlicePReadOnlyEntity entity = CassandraExecutor.toEntity(row, SlicePReadOnlyEntity.class);
        assertEquals(7, entity.getId());
        assertEquals("x", entity.getFirst());

        // Same for a UDT whose fields are mapped to the bean (serialize writes the computed value).
        final var fieldConstructor = com.datastax.driver.core.UserType.Field.class.getDeclaredConstructor(String.class,
                com.datastax.driver.core.DataType.class);
        fieldConstructor.setAccessible(true);
        final var typeConstructor = com.datastax.driver.core.UserType.class.getDeclaredConstructor(String.class, String.class, boolean.class,
                java.util.Collection.class, ProtocolVersion.class, CodecRegistry.class);
        typeConstructor.setAccessible(true);
        final var userType = typeConstructor.newInstance("ks", "person", false,
                List.of(fieldConstructor.newInstance("id", com.datastax.driver.core.DataType.cint()),
                        fieldConstructor.newInstance("first", com.datastax.driver.core.DataType.text()),
                        fieldConstructor.newInstance("fullName", com.datastax.driver.core.DataType.text())),
                ProtocolVersion.V4, new CodecRegistry());
        final CassandraExecutor.UDTCodec<SlicePReadOnlyEntity> codec = CassandraExecutor.UDTCodec.create(userType, SlicePReadOnlyEntity.class);
        final SlicePReadOnlyEntity source = new SlicePReadOnlyEntity();
        source.setId(8);
        source.setFirst("y");

        final SlicePReadOnlyEntity decoded = codec.deserialize(codec.serialize(source, ProtocolVersion.V4), ProtocolVersion.V4);
        assertEquals(8, decoded.getId());
        assertEquals("y", decoded.getFirst());
    }

    @Test
    public void testSliceP_emptyParameterContainerForParameterlessQueryBindsNothing() {
        // A single empty Map/Collection/array supplies zero values (e.g. execute(query, emptyMap)); it used to be counted
        // as one value and rejected with "Too many parameters for parameterless query: expected 0 but got 1".
        final Session session = mock(Session.class);
        final CassandraExecutor executor = newSlicePExecutor(session);
        final String query = "SELECT * FROM v3_parameterless_query";
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ColumnDefinitions variables = mock(ColumnDefinitions.class);
        final BoundStatement boundStatement = mock(BoundStatement.class);
        when(session.prepare(query)).thenReturn(preparedStatement);
        when(preparedStatement.getVariables()).thenReturn(variables);
        when(variables.size()).thenReturn(0);
        when(preparedStatement.bind(any(Object[].class))).thenReturn(boundStatement);

        assertSame(boundStatement, executor.prepareStatement(query, new HashMap<String, Object>()));
        assertSame(boundStatement, executor.prepareStatement(query, List.of()));
        assertSame(boundStatement, executor.prepareStatement(query, (Object) new Object[0]));

        // Non-empty containers and scalars are still rejected.
        assertThrows(IllegalArgumentException.class, () -> executor.prepareStatement(query, Map.of("a", 1)));
        assertThrows(IllegalArgumentException.class, () -> executor.prepareStatement(query, List.of(1)));
        assertThrows(IllegalArgumentException.class, () -> executor.prepareStatement(query, 1L));
    }

    // ---- 2026-10-02 sliceP ----

    public static class SlicePTimeEntity {
        private int id;
        private java.time.LocalTime t;

        public int getId() {
            return id;
        }

        public void setId(final int id) {
            this.id = id;
        }

        public java.time.LocalTime getT() {
            return t;
        }

        public void setT(final java.time.LocalTime t) {
            this.t = t;
        }
    }

    public static class SlicePSqlTimeEntity {
        private java.sql.Time t;

        public java.sql.Time getT() {
            return t;
        }

        public void setT(final java.sql.Time t) {
            this.t = t;
        }
    }

    @Test
    public void testSliceP_timeColumnReadIntoLocalTimeTargetsUsesNanosOfDay() throws ReflectiveOperationException {
        // Driver 3 decodes a CQL time as a Long of nanoseconds since midnight. N.convert / PropInfo.setPropValue read that Long
        // as epoch milliseconds, so every java.time.LocalTime read target got a wrong time of day (live: '10:00:00' read
        // back as 09:00 in a UTC-7 JVM).
        final java.time.LocalTime time = java.time.LocalTime.of(10, 15, 30, 123_456_789);
        final long nanos = time.toNanoOfDay();
        final ColumnDefinitions cols = mock(ColumnDefinitions.class);
        when(cols.size()).thenReturn(1);
        when(cols.getName(0)).thenReturn("t");
        when(cols.getType(0)).thenReturn(com.datastax.driver.core.DataType.time());
        final Row row = mock(Row.class);
        when(row.getColumnDefinitions()).thenReturn(cols);
        when(row.getObject(0)).thenReturn(nanos);
        final ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getColumnDefinitions()).thenReturn(cols);
        when(resultSet.all()).thenReturn(List.of(row));
        when(resultSet.one()).thenReturn(row);
        when(resultSet.isExhausted()).thenReturn(true);

        assertEquals(time, CassandraExecutor.toEntity(row, SlicePTimeEntity.class).getT());
        assertEquals(time, CassandraExecutor.toList(resultSet, java.time.LocalTime.class).get(0));
        assertEquals(time, CassandraExecutor.toList(resultSet, java.time.LocalTime[].class).get(0)[0]);
        assertEquals(time, CassandraExecutor.extractData(resultSet, SlicePTimeEntity.class).getColumn("t").get(0));

        final Session session = mock(Session.class);
        final CassandraExecutor executor = newSlicePExecutor(session);
        assertEquals(time, executor.readFirstColumn(row, java.time.LocalTime.class));
        assertEquals(time, executor.fetchOnlyOne(java.time.LocalTime.class, resultSet));
        assertEquals(time, executor.fetchOnlyOne(java.time.LocalTime[].class, resultSet)[0]);

        final String query = "SELECT t FROM slice_p_times";
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final BoundStatement boundStatement = mock(BoundStatement.class);
        when(session.prepare(query)).thenReturn(preparedStatement);
        when(preparedStatement.bind(any(Object[].class))).thenReturn(boundStatement);
        when(session.execute(boundStatement)).thenReturn(resultSet);
        assertEquals(time, executor.queryForSingleValue(java.time.LocalTime.class, query).get());
        assertEquals(time, executor.queryForSingleNonNull(java.time.LocalTime.class, query).get());
        assertEquals(time, executor.findFirst(java.time.LocalTime.class, query).get());

        // java.sql.Time targets: same time of day, to the millisecond (N.convert gave 16:37:36 for the raw Long).
        final java.text.SimpleDateFormat timeFormat = new java.text.SimpleDateFormat("HH:mm:ss.SSS");
        assertEquals("10:15:30.123", timeFormat.format(CassandraExecutor.toEntity(row, SlicePSqlTimeEntity.class).getT()));
        assertEquals("10:15:30.123", timeFormat.format(CassandraExecutor.toList(resultSet, java.sql.Time.class).get(0)));
        assertEquals("10:15:30.123", timeFormat.format(executor.readFirstColumn(row, java.sql.Time.class)));

        // Untyped targets keep the raw driver value, and a Long from a non-time column is converted as before.
        assertEquals(nanos, CassandraExecutor.toList(resultSet, Object[].class).get(0)[0]);
        when(cols.getType(0)).thenReturn(com.datastax.driver.core.DataType.bigint());
        assertEquals(com.landawn.abacus.util.N.convert(nanos, java.time.LocalTime.class), CassandraExecutor.toList(resultSet, java.time.LocalTime.class).get(0));

        // UDT time field mapped to a LocalTime bean property.
        final com.datastax.driver.core.UserType userType = slicePUserType("timed", "id", com.datastax.driver.core.DataType.cint(), "t",
                com.datastax.driver.core.DataType.time());
        final CassandraExecutor.UDTCodec<SlicePTimeEntity> codec = CassandraExecutor.UDTCodec.create(userType, SlicePTimeEntity.class);
        final SlicePTimeEntity decoded = codec.deserialize(userType.newValue().setInt(0, 7).setTime(1, nanos));
        assertEquals(7, decoded.getId());
        assertEquals(time, decoded.getT());
    }

    @Test
    public void testSliceP_udtCodecUnsupportedJavaTypeMessageNamesTheClass() throws ReflectiveOperationException {
        // The message concatenated the Class object: "Invalid Java class type: class java.lang.String. Expected: ...".
        final com.datastax.driver.core.UserType userType = slicePUserType("named", "v", com.datastax.driver.core.DataType.text());
        final CassandraExecutor.UDTCodec<String> codec = CassandraExecutor.UDTCodec.create(userType, String.class);

        final IllegalArgumentException onSerialize = assertThrows(IllegalArgumentException.class, () -> codec.serialize("x"));
        assertEquals("Invalid Java class type: java.lang.String. Expected: Collection, Map, or Bean class", onSerialize.getMessage());
        final IllegalArgumentException onDeserialize = assertThrows(IllegalArgumentException.class,
                () -> codec.deserialize(userType.newValue().setString(0, "x")));
        assertEquals("Invalid Java class type: java.lang.String. Expected: Collection, Map, or Bean class", onDeserialize.getMessage());
    }

    @Test
    public void testSliceP_singleValueTypeParameterWithGettersIsBoundAsValueNotBean() {
        // Beans.isBeanClass is true for value types such as GregorianCalendar (Calendar.getInstance()) and ByteBuffer, so a
        // single such parameter was read as a bean of named values: "Missing required parameter: 'ts'". Among two or more
        // positional values the same Calendar was converted to the column's Date.
        final Session session = mock(Session.class);
        final CassandraExecutor executor = newSlicePExecutor(session);
        final String query = "SELECT * FROM slice_p_events WHERE ts = ?";
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ColumnDefinitions variables = mock(ColumnDefinitions.class);
        when(session.prepare(query)).thenReturn(preparedStatement);
        when(preparedStatement.getVariables()).thenReturn(variables);
        when(variables.size()).thenReturn(1);
        when(variables.getType(0)).thenReturn(com.datastax.driver.core.DataType.timestamp());
        when(variables.getName(0)).thenReturn("ts");
        final Object[][] bound = new Object[1][];
        when(preparedStatement.bind(any(Object[].class))).thenAnswer(invocation -> {
            bound[0] = (Object[]) invocation.getRawArguments()[0];
            return mock(BoundStatement.class);
        });
        final java.util.Calendar calendar = java.util.Calendar.getInstance();
        calendar.setTimeInMillis(1_600_000_000_000L);

        executor.prepareStatement(query, calendar);

        assertEquals(1, bound[0].length);
        assertEquals(new java.util.Date(1_600_000_000_000L), bound[0][0]);

        // A real bean still supplies named values.
        final SlicePTimeEntity bean = new SlicePTimeEntity();
        bean.setId(3);
        final String beanQuery = "SELECT * FROM slice_p_events WHERE id = ?";
        final PreparedStatement beanStatement = mock(PreparedStatement.class);
        final ColumnDefinitions beanVariables = mock(ColumnDefinitions.class);
        when(session.prepare(beanQuery)).thenReturn(beanStatement);
        when(beanStatement.getVariables()).thenReturn(beanVariables);
        when(beanVariables.size()).thenReturn(1);
        when(beanVariables.getType(0)).thenReturn(com.datastax.driver.core.DataType.cint());
        when(beanVariables.getName(0)).thenReturn("id");
        when(beanStatement.bind(any(Object[].class))).thenAnswer(invocation -> {
            bound[0] = (Object[]) invocation.getRawArguments()[0];
            return mock(BoundStatement.class);
        });

        executor.prepareStatement(beanQuery, bean);

        assertEquals(Arrays.asList(3), Arrays.asList(bound[0]));
    }

    /** Builds a real driver-3 UserType offline (driver 3 exposes UDT metadata only through cluster discovery). */
    private static com.datastax.driver.core.UserType slicePUserType(final String typeName, final Object... namesAndTypes)
            throws ReflectiveOperationException {
        final var fieldConstructor = com.datastax.driver.core.UserType.Field.class.getDeclaredConstructor(String.class,
                com.datastax.driver.core.DataType.class);
        fieldConstructor.setAccessible(true);
        final List<com.datastax.driver.core.UserType.Field> fields = new java.util.ArrayList<>();

        for (int i = 0; i < namesAndTypes.length; i += 2) {
            fields.add(fieldConstructor.newInstance((String) namesAndTypes[i], (com.datastax.driver.core.DataType) namesAndTypes[i + 1]));
        }

        final var typeConstructor = com.datastax.driver.core.UserType.class.getDeclaredConstructor(String.class, String.class, boolean.class,
                java.util.Collection.class, ProtocolVersion.class, CodecRegistry.class);
        typeConstructor.setAccessible(true);

        return typeConstructor.newInstance("ks", typeName, false, fields, ProtocolVersion.V4, new CodecRegistry());
    }

    // ---- 2026-10-02 verifyCS ----

    public static class VerifyCSTimes {
        private java.time.LocalTime a;
        private java.time.LocalTime b;
        private java.sql.Time c;

        public java.time.LocalTime getA() {
            return a;
        }

        public void setA(final java.time.LocalTime a) {
            this.a = a;
        }

        public java.time.LocalTime getB() {
            return b;
        }

        public void setB(final java.time.LocalTime b) {
            this.b = b;
        }

        public java.sql.Time getC() {
            return c;
        }

        public void setC(final java.sql.Time c) {
            this.c = c;
        }
    }

    /** Stubs {@code session.prepare(query)} with the given variables; element 0 of the result receives the bound values. */
    private static Object[][] verifyCSPrepare(final Session session, final String query, final Object... namesAndTypes) {
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ColumnDefinitions variables = mock(ColumnDefinitions.class);
        final Object[][] bound = new Object[1][];
        when(session.prepare(query)).thenReturn(preparedStatement);
        when(preparedStatement.getVariables()).thenReturn(variables);
        when(variables.size()).thenReturn(namesAndTypes.length / 2);

        for (int i = 0; i < namesAndTypes.length; i += 2) {
            when(variables.getName(i / 2)).thenReturn((String) namesAndTypes[i]);
            when(variables.getType(i / 2)).thenReturn((com.datastax.driver.core.DataType) namesAndTypes[i + 1]);
        }

        when(preparedStatement.bind(any(Object[].class))).thenAnswer(invocation -> {
            bound[0] = (Object[]) invocation.getRawArguments()[0];
            return mock(BoundStatement.class);
        });

        return bound;
    }

    @Test
    public void testVerifyCS_dateTimeValueBoundToTimeColumnBindsTheTimeOfDayInNanos() {
        // Driver 3 binds a time column from a Long of nanoseconds since midnight, but N.convert turned a java.sql.Time, any other
        // Date/Calendar, a LocalDateTime/OffsetDateTime/ZonedDateTime or an Instant into epoch milliseconds, which was silently
        // stored as a time a few milliseconds after midnight; a LocalTime/OffsetTime could not be bound at all.
        final Session session = mock(Session.class);
        final CassandraExecutor executor = newSlicePExecutor(session);
        final java.time.LocalTime timeOfDay = java.time.LocalTime.of(10, 15, 30, 123_000_000);
        final java.sql.Time sqlTime = new java.sql.Time(java.sql.Time.valueOf(java.time.LocalTime.of(10, 15, 30)).getTime() + 123);
        final java.util.Calendar calendar = java.util.Calendar.getInstance();
        calendar.setTime(sqlTime);

        final String update = "UPDATE slice_p_times SET at = ? WHERE id = ?";
        final Object[][] updateBound = verifyCSPrepare(session, update, "at", com.datastax.driver.core.DataType.time(), "id",
                com.datastax.driver.core.DataType.cint());

        for (final Object value : List.of(sqlTime, new java.util.Date(sqlTime.getTime()), new java.sql.Timestamp(sqlTime.getTime()), calendar)) {
            executor.prepareStatement(update, value, 1);
            assertEquals(Arrays.asList(timeOfDay.toNanoOfDay(), 1), Arrays.asList(updateBound[0]), value.getClass().getName());
        }

        // java.time values: each value's own local time (nanosecond precision); an Instant in the JVM default zone.
        final java.time.LocalTime nanoTime = java.time.LocalTime.of(10, 15, 30, 123_456_789);
        final java.time.LocalDate date = java.time.LocalDate.of(2020, 1, 2);
        final java.time.ZoneOffset offset = java.time.ZoneOffset.ofHours(5);

        for (final Object value : List.of(nanoTime, java.time.LocalDateTime.of(date, nanoTime), java.time.OffsetDateTime.of(date, nanoTime, offset),
                java.time.ZonedDateTime.of(date, nanoTime, java.time.ZoneId.of("Asia/Tokyo")), java.time.OffsetTime.of(nanoTime, offset),
                java.time.ZonedDateTime.of(date, nanoTime, java.time.ZoneId.systemDefault()).toInstant())) {
            executor.prepareStatement(update, value, 1);
            assertEquals(Arrays.asList(nanoTime.toNanoOfDay(), 1), Arrays.asList(updateBound[0]), value.getClass().getName());
        }

        // A single Calendar (a value, although it has getters and setters) or LocalTime for a single time marker.
        final String select = "SELECT * FROM slice_p_times WHERE at = ?";
        final Object[][] selectBound = verifyCSPrepare(session, select, "at", com.datastax.driver.core.DataType.time());
        executor.prepareStatement(select, calendar);
        assertEquals(Arrays.asList(timeOfDay.toNanoOfDay()), Arrays.asList(selectBound[0]));
        executor.prepareStatement(select, nanoTime);
        assertEquals(Arrays.asList(nanoTime.toNanoOfDay()), Arrays.asList(selectBound[0]));

        // Unchanged: a Long is the nanosecond count itself; a Date is bound as is for a timestamp column, and a Date or a
        // LocalDateTime as epoch milliseconds for a bigint column.
        executor.prepareStatement(select, 42L);
        assertEquals(Arrays.asList(42L), Arrays.asList(selectBound[0]));
        final String other = "UPDATE slice_p_times SET ts = ?, n = ? WHERE id = ?";
        final Object[][] otherBound = verifyCSPrepare(session, other, "ts", com.datastax.driver.core.DataType.timestamp(), "n",
                com.datastax.driver.core.DataType.bigint(), "id", com.datastax.driver.core.DataType.cint());
        executor.prepareStatement(other, sqlTime, sqlTime, 1);
        assertSame(sqlTime, otherBound[0][0]);
        assertEquals(sqlTime.getTime(), otherBound[0][1]);
        final java.time.LocalDateTime localDateTime = java.time.LocalDateTime.of(date, nanoTime);
        executor.prepareStatement(other, sqlTime, localDateTime, 1);
        assertEquals(com.landawn.abacus.util.N.convert(localDateTime, Long.class), otherBound[0][1]);

        // A codec registered for the value's type still takes precedence: the value is bound as is.
        final CodecRegistry registry = new CodecRegistry().register(new com.datastax.driver.core.TypeCodec<java.time.LocalTime>(
                com.datastax.driver.core.DataType.time(), java.time.LocalTime.class) {
            @Override
            public java.nio.ByteBuffer serialize(final java.time.LocalTime value, final ProtocolVersion protocolVersion) {
                return bigint().serialize(value == null ? null : value.toNanoOfDay(), protocolVersion);
            }

            @Override
            public java.time.LocalTime deserialize(final java.nio.ByteBuffer bytes, final ProtocolVersion protocolVersion) {
                final Long nanos = bigint().deserialize(bytes, protocolVersion);
                return nanos == null ? null : java.time.LocalTime.ofNanoOfDay(nanos);
            }

            @Override
            public java.time.LocalTime parse(final String value) {
                return java.time.LocalTime.parse(value);
            }

            @Override
            public String format(final java.time.LocalTime value) {
                return String.valueOf(value);
            }
        });
        final Session codecSession = mock(Session.class);
        final Cluster cluster = mock(Cluster.class);
        final Configuration configuration = mock(Configuration.class);
        final ProtocolOptions protocolOptions = mock(ProtocolOptions.class);
        when(codecSession.init()).thenReturn(codecSession);
        when(codecSession.getCluster()).thenReturn(cluster);
        when(cluster.getConfiguration()).thenReturn(configuration);
        when(configuration.getCodecRegistry()).thenReturn(registry);
        when(configuration.getProtocolOptions()).thenReturn(protocolOptions);
        when(protocolOptions.getProtocolVersion()).thenReturn(ProtocolVersion.V4);
        final Object[][] codecBound = verifyCSPrepare(codecSession, update, "at", com.datastax.driver.core.DataType.time(), "id",
                com.datastax.driver.core.DataType.cint());
        new CassandraExecutor(codecSession).prepareStatement(update, nanoTime, 1);
        assertSame(nanoTime, codecBound[0][0]);
    }

    @Test
    public void testVerifyCS_valueTypeParametersWithGettersAreBoundAsValues() {
        final Session session = mock(Session.class);
        final CassandraExecutor executor = newSlicePExecutor(session);

        // A Calendar subclass (abacus handles every Calendar as a value) is converted to the timestamp column's Date.
        final String select = "SELECT * FROM slice_p_events WHERE ts = ?";
        final Object[][] bound = verifyCSPrepare(session, select, "ts", com.datastax.driver.core.DataType.timestamp());
        final java.util.GregorianCalendar calendarSubclass = new java.util.GregorianCalendar() {
        };
        calendarSubclass.setTimeInMillis(1_600_000_000_000L);
        executor.prepareStatement(select, calendarSubclass);
        assertEquals(Arrays.asList(new java.util.Date(1_600_000_000_000L)), Arrays.asList(bound[0]));

        // A heap or direct ByteBuffer is one positional value, not a bean of named values: too few values for two markers
        // (it was read as a bean and failed with "Missing required parameter: 'a'").
        final String twoBlobs = "SELECT * FROM slice_p_blobs WHERE a = ? AND b = ?";
        verifyCSPrepare(session, twoBlobs, "a", com.datastax.driver.core.DataType.blob(), "b", com.datastax.driver.core.DataType.blob());

        for (final java.nio.ByteBuffer buffer : List.of(java.nio.ByteBuffer.wrap(new byte[] { 1 }), java.nio.ByteBuffer.allocateDirect(1))) {
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> executor.prepareStatement(twoBlobs, buffer));
            assertTrue(e.getMessage().startsWith("Not enough parameters for parameterized query: expected 2 but got 1"), e.getMessage());
        }
    }

    @Test
    public void testVerifyCS_timeColumnEdgeValuesUdtStreamAndAsyncReadPaths() throws Exception {
        final Session session = mock(Session.class);
        final CassandraExecutor executor = newSlicePExecutor(session);
        final ColumnDefinitions cols = mock(ColumnDefinitions.class);
        when(cols.size()).thenReturn(1);
        when(cols.getName(0)).thenReturn("t");
        when(cols.getType(0)).thenReturn(com.datastax.driver.core.DataType.time());
        final java.text.SimpleDateFormat timeFormat = new java.text.SimpleDateFormat("HH:mm:ss.SSS");
        final String query = "SELECT t FROM slice_p_times";
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final BoundStatement boundStatement = mock(BoundStatement.class);
        when(session.prepare(query)).thenReturn(preparedStatement);
        when(preparedStatement.bind(any(Object[].class))).thenReturn(boundStatement);

        for (final java.time.LocalTime time : List.of(java.time.LocalTime.MIDNIGHT, java.time.LocalTime.MAX, java.time.LocalTime.NOON,
                java.time.LocalTime.of(10, 15, 30, 123_456_789))) {
            final long nanos = time.toNanoOfDay();
            final String sqlTimeText = String.format("%02d:%02d:%02d.%03d", time.getHour(), time.getMinute(), time.getSecond(), time.getNano() / 1_000_000);
            final Row row = mock(Row.class);
            when(row.getColumnDefinitions()).thenReturn(cols);
            when(row.getObject(0)).thenReturn(nanos);
            final ResultSet resultSet = mock(ResultSet.class);
            when(resultSet.getColumnDefinitions()).thenReturn(cols);
            when(resultSet.all()).thenReturn(List.of(row));
            when(resultSet.one()).thenReturn(row);
            when(resultSet.isExhausted()).thenReturn(true);
            when(resultSet.iterator()).thenAnswer(invocation -> List.of(row).iterator());
            final ResultSetFuture future = immediateFuture(resultSet);
            when(session.execute(boundStatement)).thenReturn(resultSet);
            when(session.executeAsync(boundStatement)).thenReturn(future);

            assertEquals(time, CassandraExecutor.toEntity(row, SlicePTimeEntity.class).getT());
            assertEquals(sqlTimeText, timeFormat.format(CassandraExecutor.toEntity(row, SlicePSqlTimeEntity.class).getT()));
            assertEquals(time, CassandraExecutor.toList(resultSet, java.time.LocalTime.class).get(0));
            assertEquals(sqlTimeText, timeFormat.format(CassandraExecutor.toList(resultSet, java.sql.Time[].class).get(0)[0]));
            assertEquals(sqlTimeText, timeFormat.format(executor.fetchOnlyOne(java.sql.Time.class, resultSet)));
            assertEquals(sqlTimeText, timeFormat.format((java.util.Date) CassandraExecutor.extractData(resultSet, SlicePSqlTimeEntity.class).getColumn("t").get(0)));
            assertEquals(Arrays.asList(time), executor.stream(java.time.LocalTime.class, query).toList());
            assertEquals(time, executor.async().queryForSingleValue(java.time.LocalTime.class, query).get().get());
            assertEquals(sqlTimeText, timeFormat.format(executor.async().queryForSingleNonNull(java.sql.Time.class, query).get().get()));
            assertEquals(Arrays.asList(time), executor.async().list(java.time.LocalTime.class, query).get());
        }

        // UDT fields: each field's own CQL type decides (a bigint read into a LocalTime property is converted as before).
        final com.datastax.driver.core.UserType userType = slicePUserType("verify_cs_times", "a", com.datastax.driver.core.DataType.bigint(), "b",
                com.datastax.driver.core.DataType.time(), "c", com.datastax.driver.core.DataType.time());
        final java.time.LocalTime time = java.time.LocalTime.of(10, 15, 30, 123_456_789);
        final long nanos = time.toNanoOfDay();
        final com.datastax.driver.core.UDTValue udtValue = userType.newValue().setLong(0, nanos).setTime(1, nanos).setTime(2, nanos);
        final VerifyCSTimes decoded = CassandraExecutor.UDTCodec.create(userType, VerifyCSTimes.class).deserialize(udtValue);
        assertEquals(com.landawn.abacus.util.N.convert(nanos, java.time.LocalTime.class), decoded.getA());
        assertEquals(time, decoded.getB());
        assertEquals("10:15:30.123", timeFormat.format(decoded.getC()));
        assertEquals(Map.of("a", nanos, "b", nanos, "c", nanos), CassandraExecutor.UDTCodec.create(userType, Map.class).deserialize(udtValue));

        // Non-time targets keep the raw nanosecond count, and the column type is not even resolved for them.
        final ColumnDefinitions untyped = mock(ColumnDefinitions.class);
        when(untyped.size()).thenReturn(1);
        when(untyped.getName(0)).thenReturn("t");
        final Row rawRow = mock(Row.class);
        when(rawRow.getColumnDefinitions()).thenReturn(untyped);
        when(rawRow.getObject(0)).thenReturn(nanos);
        final ResultSet rawResultSet = mock(ResultSet.class);
        when(rawResultSet.getColumnDefinitions()).thenReturn(untyped);
        when(rawResultSet.all()).thenReturn(List.of(rawRow));
        assertEquals(nanos, CassandraExecutor.toList(rawResultSet, Long.class).get(0));
        assertEquals(String.valueOf(nanos), CassandraExecutor.toList(rawResultSet, String.class).get(0));
        assertEquals(nanos, executor.readFirstColumn(rawRow, long.class));
        verify(untyped, org.mockito.Mockito.never()).getType(org.mockito.ArgumentMatchers.anyInt());
    }

    // ---- 2026-10-02 verifyCS end ----

    // ---- 2026-10-04 coverageCS ----

    public record CoverageCSKey(Integer id, String name) {
    }

    /** A result set over single-column {@code time} rows (driver 3 decodes a CQL time as a Long of nanoseconds since midnight). */
    private static ResultSet coverageCSTimeResultSet(final Long... nanosOfDay) {
        final ColumnDefinitions cols = mock(ColumnDefinitions.class);
        when(cols.size()).thenReturn(1);
        when(cols.getName(0)).thenReturn("t");
        when(cols.getType(0)).thenReturn(com.datastax.driver.core.DataType.time());
        final List<Row> rows = new java.util.ArrayList<>();

        for (final Long nanos : nanosOfDay) {
            final Row row = mock(Row.class);
            when(row.getColumnDefinitions()).thenReturn(cols);
            when(row.getObject(0)).thenReturn(nanos);
            rows.add(row);
        }

        final ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getColumnDefinitions()).thenReturn(cols);
        when(resultSet.all()).thenReturn(rows);
        when(resultSet.iterator()).thenAnswer(invocation -> rows.iterator());
        when(resultSet.isExhausted()).thenReturn(true);

        return resultSet;
    }

    @Test
    public void testCoverageCS_timeColumnRowMapperConvertsEveryRowNotOnlyTheFirst() throws Exception {
        // The single-value row mapper (toList/list/stream and their async twins) caches the first non-null value's class and
        // converts later rows - and a leading null - on a separate branch: every row of a time column must be read as nanoseconds
        // since midnight, not only the first one.
        final java.time.LocalTime first = java.time.LocalTime.of(1, 2, 3, 4_000_000);
        final java.time.LocalTime second = java.time.LocalTime.of(23, 59, 59, 999_999_999);
        final java.time.LocalTime third = java.time.LocalTime.NOON;
        final ResultSet resultSet = coverageCSTimeResultSet(null, first.toNanoOfDay(), second.toNanoOfDay(), third.toNanoOfDay());
        final List<java.time.LocalTime> expected = Arrays.asList(null, first, second, third);
        final java.text.SimpleDateFormat timeFormat = new java.text.SimpleDateFormat("HH:mm:ss.SSS");

        assertEquals(expected, CassandraExecutor.toList(resultSet, java.time.LocalTime.class));
        assertEquals(Arrays.asList(first, second, third),
                CassandraExecutor.toList(coverageCSTimeResultSet(first.toNanoOfDay(), second.toNanoOfDay(), third.toNanoOfDay()), java.time.LocalTime.class));
        assertEquals(Arrays.asList(null, "01:02:03.004", "23:59:59.999", "12:00:00.000"),
                CassandraExecutor.toList(resultSet, java.sql.Time.class).stream().map(t -> t == null ? null : timeFormat.format(t)).toList());

        final Session session = mock(Session.class);
        final CassandraExecutor executor = newSlicePExecutor(session);
        final String query = "SELECT t FROM coverage_cs_times";
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final BoundStatement boundStatement = mock(BoundStatement.class);
        when(session.prepare(query)).thenReturn(preparedStatement);
        when(preparedStatement.bind(any(Object[].class))).thenReturn(boundStatement);
        when(session.execute(boundStatement)).thenReturn(resultSet);
        final ResultSetFuture future = immediateFuture(resultSet);
        when(session.executeAsync(boundStatement)).thenReturn(future);

        assertEquals(expected, executor.list(java.time.LocalTime.class, query));
        assertEquals(expected, executor.stream(java.time.LocalTime.class, query).toList());
        assertEquals(expected, executor.async().list(java.time.LocalTime.class, query).get());
        assertEquals(expected, executor.async().stream(java.time.LocalTime.class, query).get().toList());
    }

    @Test
    public void testCoverageCS_singleValueTypeParameterThroughAsyncAndRecordStillSuppliesNamedValues() throws Exception {
        // The async facade binds through the same prepareStatement: a single Calendar is one positional value (it was read as a
        // bean of named values: "Missing required parameter: 'ts'").
        final Session session = mock(Session.class);
        final CassandraExecutor executor = newSlicePExecutor(session);
        final String select = "SELECT * FROM coverage_cs_events WHERE ts = ?";
        final Object[][] bound = verifyCSPrepare(session, select, "ts", com.datastax.driver.core.DataType.timestamp());
        final ResultSetFuture future = immediateFuture(mock(ResultSet.class));
        when(session.executeAsync(any(Statement.class))).thenReturn(future);
        final java.util.Calendar calendar = java.util.Calendar.getInstance();
        calendar.setTimeInMillis(1_600_000_000_000L);

        executor.async().execute(select, calendar).get();

        assertEquals(Arrays.asList(new java.util.Date(1_600_000_000_000L)), Arrays.asList(bound[0]));

        // Unchanged: a record (a bean for abacus too) still supplies named values.
        final String byKey = "SELECT * FROM coverage_cs_events WHERE id = ? AND name = ?";
        final Object[][] keyBound = verifyCSPrepare(session, byKey, "id", com.datastax.driver.core.DataType.cint(), "name",
                com.datastax.driver.core.DataType.text());
        executor.prepareStatement(byKey, new CoverageCSKey(8, "r"));
        assertEquals(Arrays.asList(8, "r"), Arrays.asList(keyBound[0]));
    }

    // ---- 2026-10-04 coverageCS end ----
}
