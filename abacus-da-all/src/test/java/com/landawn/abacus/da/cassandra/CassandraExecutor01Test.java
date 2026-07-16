package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.da.cassandra.CassandraExecutor.StatementSettings;
import com.landawn.abacus.exception.DuplicateResultException;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.u.Nullable;
import com.landawn.abacus.util.u.Optional;
import com.landawn.abacus.util.stream.Stream;

@ExtendWith(MockitoExtension.class)
public class CassandraExecutor01Test extends TestBase {

    @Mock
    private CqlSession mockSession;

    @Mock
    private MutableCodecRegistry mockCodecRegistry;

    @Mock
    private ResultSet mockResultSet;

    @Mock
    private AsyncResultSet mockAsyncResultSet;

    @Mock
    private Row mockRow;

    @Mock
    private PreparedStatement mockPreparedStatement;

    @Mock
    private BoundStatement mockBoundStatement;

    @Mock
    private ColumnDefinitions mockColumnDefinitions;

    @Mock
    private ColumnDefinition mockColumnDef;

    @Mock
    private DataType mockDataType;

    @Mock
    private TypeCodec<Object> mockTypeCodec;

    private CassandraExecutor executor;

    @BeforeEach
    public void setUp() {
        when(mockSession.getContext()).thenReturn(mock(com.datastax.oss.driver.api.core.context.DriverContext.class));
        when(mockSession.getContext().getCodecRegistry()).thenReturn(mockCodecRegistry);
        executor = new CassandraExecutor(mockSession);
    }

    private void stubSingleBigintParameter() {
        when(mockPreparedStatement.getVariableDefinitions()).thenReturn(mockColumnDefinitions);
        when(mockColumnDefinitions.size()).thenReturn(1);
        when(mockColumnDefinitions.get(0)).thenReturn(mockColumnDef);
        when(mockColumnDef.getType()).thenReturn(mockDataType);
        when(mockDataType.getProtocolCode()).thenReturn(ProtocolConstants.DataType.BIGINT);
    }

    @Test
    public void testConstructors() {
        // Test constructor with session only
        CassandraExecutor executor1 = new CassandraExecutor(mockSession);
        assertNotNull(executor1);

        // Test constructor with settings
        StatementSettings settings = StatementSettings.builder().fetchSize(100).build();
        CassandraExecutor executor2 = new CassandraExecutor(mockSession, settings);
        assertNotNull(executor2);

        // Test constructor with CqlMapper
        CqlMapper mapper = new CqlMapper();
        CassandraExecutor executor3 = new CassandraExecutor(mockSession, settings, mapper);
        assertNotNull(executor3);

        // Test constructor with NamingPolicy
        CassandraExecutor executor4 = new CassandraExecutor(mockSession, settings, mapper, NamingPolicy.SNAKE_CASE);
        assertNotNull(executor4);
    }

    @Test
    public void testSession() {
        assertEquals(mockSession, executor.session());
    }

    @Test
    public void testPrepareStatementWithoutRequiredParametersThrows() {
        final String query = "SELECT * FROM users WHERE id = ?";

        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> executor.prepareStatement(query));
        assertTrue(ex.getMessage().contains("expected 1"), ex.getMessage());
    }

    @Test
    public void testPrepareStatementCachesResolvedCqlButReturnsFreshBoundStatements() {
        final String firstCql = "SELECT * FROM mapper_cache_first";
        final String secondCql = "SELECT * FROM mapper_cache_second";
        final CqlMapper mapper = new CqlMapper();
        mapper.add("lookup", firstCql);

        final PreparedStatement firstPrepared = mock(PreparedStatement.class);
        final PreparedStatement secondPrepared = mock(PreparedStatement.class);
        final BoundStatement firstBound = mock(BoundStatement.class);
        final BoundStatement nextFirstBound = mock(BoundStatement.class);
        final BoundStatement secondBound = mock(BoundStatement.class);
        when(mockSession.prepare(firstCql)).thenReturn(firstPrepared);
        when(mockSession.prepare(secondCql)).thenReturn(secondPrepared);
        when(firstPrepared.bind(any(Object[].class))).thenReturn(firstBound, nextFirstBound);
        when(secondPrepared.bind(any(Object[].class))).thenReturn(secondBound);

        final CassandraExecutor mappedExecutor = new CassandraExecutor(mockSession, null, mapper);
        assertSame(firstBound, mappedExecutor.prepareStatement("lookup"));
        assertSame(nextFirstBound, mappedExecutor.prepareStatement("lookup"));
        assertNotSame(firstBound, nextFirstBound);
        verify(mockSession, times(1)).prepare(firstCql);

        mapper.remove("lookup");
        mapper.add("lookup", secondCql);

        assertSame(secondBound, mappedExecutor.prepareStatement("lookup"));
        verify(mockSession, times(1)).prepare(secondCql);
    }

    @Test
    public void testExecuteRejectsParametersForParameterlessQuery() {
        final String query = "SELECT * FROM parameterless_query";
        when(mockSession.prepare(query)).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.getVariableDefinitions()).thenReturn(mockColumnDefinitions);
        when(mockColumnDefinitions.size()).thenReturn(0);

        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> executor.execute(query, 1L));
        assertTrue(ex.getMessage().contains("expected 0 but got 1"), ex.getMessage());
    }

    @Test
    public void testExecuteRejectsTooManyPositionalParameters() {
        final String query = "SELECT * FROM one_parameter_query WHERE id = ?";
        when(mockSession.prepare(query)).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.getVariableDefinitions()).thenReturn(mockColumnDefinitions);
        when(mockColumnDefinitions.size()).thenReturn(1);

        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> executor.execute(query, 1L, 2L));
        assertTrue(ex.getMessage().contains("Too many parameters"), ex.getMessage());
        assertTrue(ex.getMessage().contains("expected 1 but got 2"), ex.getMessage());
    }

    @Test
    public void testStringCodecHonorsCqlLiteralAndNullContracts() {
        final CassandraExecutor.StringCodec<TestEntity> codec = new CassandraExecutor.StringCodec<>(TestEntity.class);
        final TestEntity entity = new TestEntity();
        entity.setId(11L);
        entity.setName("O'Brien");

        final String literal = codec.format(entity);
        assertTrue(literal.startsWith("'") && literal.endsWith("'"), literal);
        assertEquals(entity.getName(), codec.parse(literal).getName());

        final ByteBuffer encodedNull = codec.encode(null, ProtocolVersion.DEFAULT);
        assertNull(encodedNull);
        assertNull(codec.decode(null, ProtocolVersion.DEFAULT));
    }

    @Test
    public void testExtractData() {
        // Setup mock data
        List<Row> rows = Arrays.asList(mockRow);
        when(mockResultSet.all()).thenReturn(rows);
        when(mockResultSet.getColumnDefinitions()).thenReturn(mockColumnDefinitions);
        when(mockColumnDefinitions.size()).thenReturn(2);
        when(mockColumnDefinitions.get(0)).thenReturn(mock(com.datastax.oss.driver.api.core.cql.ColumnDefinition.class));
        when(mockColumnDefinitions.get(1)).thenReturn(mock(com.datastax.oss.driver.api.core.cql.ColumnDefinition.class));
        when(mockColumnDefinitions.get(0).getName()).thenReturn(com.datastax.oss.driver.api.core.CqlIdentifier.fromInternal("id"));
        when(mockColumnDefinitions.get(1).getName()).thenReturn(com.datastax.oss.driver.api.core.CqlIdentifier.fromInternal("name"));

        // Test without target class
        Dataset dataset1 = CassandraExecutor.extractData(mockResultSet);
        assertNotNull(dataset1);

        // Test with target class
        Dataset dataset2 = CassandraExecutor.extractData(mockResultSet, TestEntity.class);
        assertNotNull(dataset2);
    }

    /**
     * Regression guard for {@code extractData(ResultSet, Class)} with a non-bean, non-Map target
     * class: scalar column values must be kept raw. Previously the column class for non-bean targets
     * defaulted to {@code Object[].class}, which forced every scalar through
     * {@code N.convert(value, Object[].class)} and wrapped it into an {@code Object[]}.
     */
    @Test
    public void testExtractData_nonBeanTargetClass_keepsScalarValuesRaw() {
        when(mockResultSet.all()).thenReturn(Arrays.asList(mockRow));
        when(mockResultSet.getColumnDefinitions()).thenReturn(mockColumnDefinitions);
        when(mockColumnDefinitions.size()).thenReturn(1);
        when(mockColumnDefinitions.get(0)).thenReturn(mockColumnDef);
        when(mockColumnDef.getName()).thenReturn(com.datastax.oss.driver.api.core.CqlIdentifier.fromInternal("title"));
        when(mockRow.getObject(0)).thenReturn("abc");

        Dataset ds = CassandraExecutor.extractData(mockResultSet, String.class);

        assertEquals(1, ds.size());
        // The Dataset cursor starts at row 0; the scalar must be the raw String, not Object[].
        Object value = ds.get("title");
        assertEquals("abc", value);
    }

    @Test
    public void testToList() {
        // Setup mock data
        List<Row> rows = Arrays.asList(mockRow);
        when(mockResultSet.all()).thenReturn(rows);
        when(mockResultSet.getColumnDefinitions()).thenReturn(mockColumnDefinitions);
        when(mockColumnDefinitions.size()).thenReturn(1);
        when(mockColumnDefinitions.get(0)).thenReturn(mockColumnDef);
        when(mockColumnDef.getName()).thenReturn(com.datastax.oss.driver.api.core.CqlIdentifier.fromInternal("id"));
        when(mockRow.getColumnDefinitions()).thenReturn(mockColumnDefinitions);

        // Test with Row class
        List<Row> rowList = CassandraExecutor.toList(mockResultSet, Row.class);
        assertEquals(1, rowList.size());

        // Test with other target class
        List<TestEntity> entityList = CassandraExecutor.toList(mockResultSet, TestEntity.class);
        assertNotNull(entityList);
    }

    @Test
    public void testToListObjectClassReturnsRawRows() {
        // Object.class is assignable from Row, so toList returns the raw driver Row objects
        // (passthrough) without mapping or first-column extraction.
        when(mockResultSet.all()).thenReturn(Arrays.asList(mockRow));

        final List<Object> result = CassandraExecutor.toList(mockResultSet, Object.class);

        assertEquals(1, result.size());
        org.junit.jupiter.api.Assertions.assertSame(mockRow, result.get(0));
    }

    @Test
    public void testToEntity() {
        // Setup mock data
        when(mockRow.getColumnDefinitions()).thenReturn(mockColumnDefinitions);
        when(mockColumnDefinitions.size()).thenReturn(2);
        when(mockColumnDefinitions.get(0)).thenReturn(mock(com.datastax.oss.driver.api.core.cql.ColumnDefinition.class));
        when(mockColumnDefinitions.get(1)).thenReturn(mock(com.datastax.oss.driver.api.core.cql.ColumnDefinition.class));
        when(mockColumnDefinitions.get(0).getName()).thenReturn(com.datastax.oss.driver.api.core.CqlIdentifier.fromInternal("id"));
        when(mockColumnDefinitions.get(1).getName()).thenReturn(com.datastax.oss.driver.api.core.CqlIdentifier.fromInternal("name"));
        when(mockRow.getObject(0)).thenReturn(1L);
        when(mockRow.getObject(1)).thenReturn("test");

        TestEntity entity = CassandraExecutor.toEntity(mockRow, TestEntity.class);
        assertNotNull(entity);
    }

    @Test
    public void testToMap() {
        // Setup mock data
        when(mockRow.getColumnDefinitions()).thenReturn(mockColumnDefinitions);
        when(mockColumnDefinitions.size()).thenReturn(2);
        when(mockColumnDefinitions.get(0)).thenReturn(mock(com.datastax.oss.driver.api.core.cql.ColumnDefinition.class));
        when(mockColumnDefinitions.get(1)).thenReturn(mock(com.datastax.oss.driver.api.core.cql.ColumnDefinition.class));
        when(mockColumnDefinitions.get(0).getName()).thenReturn(com.datastax.oss.driver.api.core.CqlIdentifier.fromInternal("id"));
        when(mockColumnDefinitions.get(1).getName()).thenReturn(com.datastax.oss.driver.api.core.CqlIdentifier.fromInternal("name"));
        when(mockRow.getObject(0)).thenReturn(1L);
        when(mockRow.getObject(1)).thenReturn("test");

        // Test default toMap
        Map<String, Object> map1 = CassandraExecutor.toMap(mockRow);
        assertNotNull(map1);
        assertEquals(2, map1.size());

        // Test toMap with supplier
        Map<String, Object> map2 = CassandraExecutor.toMap(mockRow, HashMap::new);
        assertNotNull(map2);
        assertEquals(2, map2.size());
    }

    @Test
    public void testGett() throws DuplicateResultException {
        // Setup mock data
        when(mockSession.prepare(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.bind(any(Object[].class))).thenReturn(mockBoundStatement);
        stubSingleBigintParameter();
        when(mockColumnDef.getName()).thenReturn(com.datastax.oss.driver.api.core.CqlIdentifier.fromInternal("id"));
        when(mockSession.execute(any(Statement.class))).thenReturn(mockResultSet);
        when(mockResultSet.iterator()).thenReturn(Arrays.asList(mockRow).iterator());
        when(mockRow.getColumnDefinitions()).thenReturn(mockColumnDefinitions);

        // Test gett with selectPropNames and whereClause
        TestEntity result = executor.gett(TestEntity.class, Arrays.asList("id", "name"), Filters.eq("id", 1L));
        assertNotNull(result);
    }

    @Test
    public void testqueryForSingleValue() {
        // Setup mock data
        when(mockSession.prepare(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.bind(any(Object[].class))).thenReturn(mockBoundStatement);
        stubSingleBigintParameter();
        when(mockSession.execute(any(Statement.class))).thenReturn(mockResultSet);
        when(mockResultSet.one()).thenReturn(mockRow);
        when(mockRow.getObject(0)).thenReturn("test");

        Nullable<String> result = executor.queryForSingleValue(String.class, "SELECT name FROM test WHERE id = ?", 1L);
        assertTrue(result.isPresent());
        assertEquals("test", result.get());
    }

    @Test
    public void testQueryForSingleNonNull() {
        // Setup mock data
        when(mockSession.prepare(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.bind(any(Object[].class))).thenReturn(mockBoundStatement);
        stubSingleBigintParameter();
        when(mockSession.execute(any(Statement.class))).thenReturn(mockResultSet);
        when(mockResultSet.one()).thenReturn(mockRow);
        when(mockRow.getObject(0)).thenReturn("test");

        Optional<String> result = executor.queryForSingleNonNull(String.class, "SELECT name FROM test WHERE id = ?", 1L);
        assertTrue(result.isPresent());
        assertEquals("test", result.get());
    }

    @Test
    public void testFindFirst() {
        // Setup mock data
        when(mockSession.prepare(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.bind(any(Object[].class))).thenReturn(mockBoundStatement);
        stubSingleBigintParameter();
        when(mockColumnDef.getName()).thenReturn(com.datastax.oss.driver.api.core.CqlIdentifier.fromInternal("id"));
        when(mockSession.execute(any(Statement.class))).thenReturn(mockResultSet);
        when(mockResultSet.one()).thenReturn(mockRow);
        when(mockRow.getColumnDefinitions()).thenReturn(mockColumnDefinitions);

        Optional<TestEntity> result = executor.findFirst(TestEntity.class, "SELECT * FROM test WHERE id = ?", 1L);
        assertTrue(result.isPresent());
    }

    @Test
    public void testStream() {
        // Setup mock data
        when(mockSession.prepare(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.bind(any(Object[].class))).thenReturn(mockBoundStatement);
        when(mockSession.execute(any(Statement.class))).thenReturn(mockResultSet);
        when(mockResultSet.iterator()).thenReturn(Arrays.asList(mockRow).iterator());

        // Test stream with BiFunction rowMapper
        Stream<TestEntity> stream = executor.stream("SELECT * FROM test", (columnDefs, row) -> new TestEntity(), new Object[0]);
        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testStreamWithStatement() {
        // Setup mock data
        when(mockSession.execute(any(Statement.class))).thenReturn(mockResultSet);
        when(mockResultSet.iterator()).thenReturn(Arrays.asList(mockRow).iterator());

        Stream<TestEntity> stream = executor.stream(mockBoundStatement, (columnDefs, row) -> new TestEntity());
        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testExecute() {
        // Setup mock data
        when(mockSession.prepare(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.bind(any(Object[].class))).thenReturn(mockBoundStatement);
        when(mockPreparedStatement.getVariableDefinitions()).thenReturn(mockColumnDefinitions);
        when(mockColumnDefinitions.size()).thenReturn(1);
        when(mockColumnDefinitions.get(0)).thenReturn(mockColumnDef);
        when(mockColumnDef.getType()).thenReturn(mockDataType);
        when(mockDataType.getProtocolCode()).thenReturn(ProtocolConstants.DataType.BIGINT);
        when(mockCodecRegistry.codecFor(any(DataType.class))).thenReturn(mockTypeCodec);
        when(mockSession.execute(any(Statement.class))).thenReturn(mockResultSet);

        // Test execute with query only
        ResultSet result1 = executor.execute("SELECT * FROM test");
        assertNotNull(result1);

        // Test execute with parameters
        ResultSet result2 = executor.execute("SELECT * FROM test WHERE id = ?", 1L);
        assertNotNull(result2);

        // Test execute with Map parameters
        Map<String, Object> params = new HashMap<>();
        params.put("id", 1L);
        ResultSet result3 = executor.execute("SELECT * FROM test WHERE id = :id", params);
        assertNotNull(result3);

        // Test execute with Statement
        ResultSet result4 = executor.execute(mockBoundStatement);
        assertNotNull(result4);
    }

    @Test
    public void testExecuteWithSingleEntryMapStillUsesNamedBinding() {
        when(mockSession.prepare(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.getVariableDefinitions()).thenReturn(mockColumnDefinitions);
        when(mockColumnDefinitions.size()).thenReturn(2);

        final Map<String, Object> params = new HashMap<>();
        params.put("firstName", "John");

        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> executor.execute("SELECT * FROM test WHERE first_name = :firstName AND last_name = :lastName", params));

        assertTrue(ex.getMessage().contains("Missing required parameter"));
    }

    @Test
    public void testAsyncGet() {
        // Setup mock data
        when(mockSession.prepare(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.bind(any(Object[].class))).thenReturn(mockBoundStatement);
        stubSingleBigintParameter();
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(CompletableFuture.completedFuture(mockAsyncResultSet));

        ContinuableFuture<Optional<TestEntity>> future = executor.async().get(TestEntity.class, Arrays.asList("id"), Filters.eq("id", 1L));
        assertNotNull(future);
    }

    @Test
    public void testAsyncGett() {
        // Setup mock data
        when(mockSession.prepare(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.bind(any(Object[].class))).thenReturn(mockBoundStatement);
        stubSingleBigintParameter();
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(CompletableFuture.completedFuture(mockAsyncResultSet));

        ContinuableFuture<TestEntity> future = executor.async().gett(TestEntity.class, Arrays.asList("id"), Filters.eq("id", 1L));
        assertNotNull(future);
    }

    @Test
    public void testAsyncqueryForSingleValue() {
        // Setup mock data
        when(mockSession.prepare(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.bind(any(Object[].class))).thenReturn(mockBoundStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(CompletableFuture.completedFuture(mockAsyncResultSet));

        ContinuableFuture<Nullable<String>> future = executor.async().queryForSingleValue(String.class, "SELECT name FROM test", new Object[0]);
        assertNotNull(future);
    }

    @Test
    public void testAsyncQueryForSingleNonNull() {
        // Setup mock data
        when(mockSession.prepare(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.bind(any(Object[].class))).thenReturn(mockBoundStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(CompletableFuture.completedFuture(mockAsyncResultSet));

        ContinuableFuture<Optional<String>> future = executor.async().queryForSingleNonNull(String.class, "SELECT name FROM test", new Object[0]);
        assertNotNull(future);
    }

    @Test
    public void testAsyncFindFirst() {
        // Setup mock data
        when(mockSession.prepare(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.bind(any(Object[].class))).thenReturn(mockBoundStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(CompletableFuture.completedFuture(mockAsyncResultSet));

        ContinuableFuture<Optional<TestEntity>> future = executor.async().findFirst(TestEntity.class, "SELECT * FROM test", new Object[0]);
        assertNotNull(future);
    }

    @Test
    public void testAsyncStream() {
        // Setup mock data
        when(mockSession.prepare(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.bind(any(Object[].class))).thenReturn(mockBoundStatement);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(CompletableFuture.completedFuture(mockAsyncResultSet));

        // Test asyncStream with query and parameters
        ContinuableFuture<Stream<Object[]>> future1 = executor.async().stream("SELECT * FROM test", new Object[0]);
        assertNotNull(future1);

        // Test asyncStream with BiFunction rowMapper
        ContinuableFuture<Stream<TestEntity>> future2 = executor.async().stream("SELECT * FROM test", (columnDefs, row) -> new TestEntity(), new Object[0]);
        assertNotNull(future2);
    }

    @Test
    public void testAsyncStreamWithStatement() {
        // Setup mock data
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(CompletableFuture.completedFuture(mockAsyncResultSet));

        ContinuableFuture<Stream<TestEntity>> future = executor.async().stream(mockBoundStatement, (columnDefs, row) -> new TestEntity());
        assertNotNull(future);
    }

    @Test
    public void testAsyncExecute() {
        // Setup mock data
        when(mockSession.prepare(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.bind(any(Object[].class))).thenReturn(mockBoundStatement);
        when(mockPreparedStatement.getVariableDefinitions()).thenReturn(mockColumnDefinitions);
        when(mockColumnDefinitions.size()).thenReturn(1);
        when(mockColumnDefinitions.get(0)).thenReturn(mockColumnDef);
        when(mockColumnDef.getType()).thenReturn(mockDataType);
        when(mockDataType.getProtocolCode()).thenReturn(ProtocolConstants.DataType.BIGINT);
        when(mockCodecRegistry.codecFor(any(DataType.class))).thenReturn(mockTypeCodec);
        when(mockSession.executeAsync(any(Statement.class))).thenReturn(CompletableFuture.completedFuture(mockAsyncResultSet));

        // Test asyncExecute with query only
        ContinuableFuture<ResultSet> future1 = executor.async().execute("SELECT * FROM test");
        assertNotNull(future1);

        // Test asyncExecute with parameters
        ContinuableFuture<ResultSet> future2 = executor.async().execute("SELECT * FROM test WHERE id = ?", 1L);
        assertNotNull(future2);

        // Test asyncExecute with Map parameters
        Map<String, Object> params = new HashMap<>();
        params.put("id", 1L);
        ContinuableFuture<ResultSet> future3 = executor.async().execute("SELECT * FROM test WHERE id = :id", params);
        assertNotNull(future3);

        // Test asyncExecute with Statement
        ContinuableFuture<ResultSet> future4 = executor.async().execute(mockBoundStatement);
        assertNotNull(future4);
    }

    @Test
    public void testAsyncExecuteWithSingleEntryMapStillUsesNamedBinding() {
        when(mockSession.prepare(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.getVariableDefinitions()).thenReturn(mockColumnDefinitions);
        when(mockColumnDefinitions.size()).thenReturn(2);

        final Map<String, Object> params = new HashMap<>();
        params.put("firstName", "John");

        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> executor.async().execute("SELECT * FROM test WHERE first_name = :firstName AND last_name = :lastName", params));

        assertTrue(ex.getMessage().contains("Missing required parameter"));
    }

    @Test
    public void testClose() {
        when(mockSession.isClosed()).thenReturn(false);
        executor.close();
        verify(mockSession, times(1)).close();
    }

    @Test
    public void testUDTCodec() {
        // Test UDTCodec creation
        com.datastax.oss.driver.api.core.type.UserDefinedType mockUDT = mock(com.datastax.oss.driver.api.core.type.UserDefinedType.class);

        CassandraExecutor.UDTCodec<TestEntity> codec = CassandraExecutor.UDTCodec.create(mockUDT, TestEntity.class);
        assertNotNull(codec);

        // Test with Collection
        CassandraExecutor.UDTCodec<List> listCodec = CassandraExecutor.UDTCodec.create(mockUDT, List.class);
        assertNotNull(listCodec);

        // Test with Map
        CassandraExecutor.UDTCodec<Map> mapCodec = CassandraExecutor.UDTCodec.create(mockUDT, Map.class);
        assertNotNull(mapCodec);
    }

    @Test
    public void testQueryForSingleNonNull_nullValue_throwsNullPointerException() {
        when(mockSession.prepare(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.bind(any(Object[].class))).thenReturn(mockBoundStatement);
        stubSingleBigintParameter();
        when(mockSession.execute(any(Statement.class))).thenReturn(mockResultSet);
        when(mockResultSet.one()).thenReturn(mockRow);
        when(mockRow.getObject(0)).thenReturn(null);

        assertThrows(NullPointerException.class, () -> executor.queryForSingleNonNull(String.class, "SELECT name FROM test WHERE id = ?", 1L));
    }

    @Test
    public void testFindFirst_singleColumnNullValue_throwsNullPointerException() {
        // Separate column-defs mock for the result-set side, so we don't have
        // to set up bound-statement variable types.
        ColumnDefinitions rowCols = mock(ColumnDefinitions.class);
        when(rowCols.size()).thenReturn(1);

        when(mockSession.prepare(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.bind(any(Object[].class))).thenReturn(mockBoundStatement);
        stubSingleBigintParameter();
        when(mockSession.execute(any(Statement.class))).thenReturn(mockResultSet);
        when(mockResultSet.one()).thenReturn(mockRow);
        when(mockRow.getColumnDefinitions()).thenReturn(rowCols);
        when(mockRow.getObject(0)).thenReturn(null);

        assertThrows(NullPointerException.class, () -> executor.findFirst(String.class, "SELECT name FROM test WHERE id = ?", 1L));
    }

    @Test
    public void testStatementSettings() {
        // Test StatementSettings builder
        StatementSettings settings = StatementSettings.builder()
                .consistency(com.datastax.oss.driver.api.core.ConsistencyLevel.QUORUM)
                .serialConsistency(com.datastax.oss.driver.api.core.ConsistencyLevel.LOCAL_SERIAL)
                .fetchSize(100)
                .timeout(java.time.Duration.ofSeconds(10))
                .traceQuery(true)
                .build();

        assertNotNull(settings);
        assertEquals(com.datastax.oss.driver.api.core.ConsistencyLevel.QUORUM, settings.consistency());
        assertEquals(com.datastax.oss.driver.api.core.ConsistencyLevel.LOCAL_SERIAL, settings.serialConsistency());
        assertEquals(100, settings.fetchSize());
        assertEquals(java.time.Duration.ofSeconds(10), settings.timeout());
        assertTrue(settings.traceQuery());
    }

    // Test entity class
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
}
