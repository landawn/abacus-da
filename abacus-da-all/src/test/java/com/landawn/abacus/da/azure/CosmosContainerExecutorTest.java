package com.landawn.abacus.da.azure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosItemIdentity;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosPatchItemRequestOptions;
import com.azure.cosmos.models.CosmosPatchOperations;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.SqlQuerySpec;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.da.azure.CosmosContainerExecutor;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.stream.Stream;
import com.landawn.abacus.util.u.Optional;

public class CosmosContainerExecutorTest extends TestBase {

    @Mock
    private CosmosContainer mockCosmosContainer;

    @Mock
    private CosmosItemResponse<TestItem> mockItemResponse;

    @Mock
    private CosmosItemResponse<Object> mockObjectItemResponse;

    @Mock
    private CosmosPagedIterable<TestItem> mockPagedIterable;

    @Mock
    private FeedResponse<TestItem> mockFeedResponse;

    private CosmosContainerExecutor executor;
    private TestItem testItem;
    private PartitionKey partitionKey;
    private CosmosItemRequestOptions itemRequestOptions;
    private CosmosPatchItemRequestOptions patchRequestOptions;
    private CosmosQueryRequestOptions queryRequestOptions;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        executor = new CosmosContainerExecutor(mockCosmosContainer);
        testItem = new TestItem("id1", "name1");
        partitionKey = new PartitionKey("partitionKey1");
        itemRequestOptions = new CosmosItemRequestOptions();
        patchRequestOptions = new CosmosPatchItemRequestOptions();
        queryRequestOptions = new CosmosQueryRequestOptions();
    }

    @Test
    public void testConstructorWithContainer() {
        CosmosContainerExecutor executor = new CosmosContainerExecutor(mockCosmosContainer);
        assertNotNull(executor);
        assertEquals(mockCosmosContainer, executor.cosmosContainer());
    }

    @Test
    public void testConstructorWithContainerAndNamingPolicy() {
        CosmosContainerExecutor executor = new CosmosContainerExecutor(mockCosmosContainer, NamingPolicy.SCREAMING_SNAKE_CASE);
        assertNotNull(executor);
        assertEquals(mockCosmosContainer, executor.cosmosContainer());
    }

    @Test
    public void testCosmosContainer() {
        assertEquals(mockCosmosContainer, executor.cosmosContainer());
    }

    @Test
    public void testCreateItemWithItem() {
        when(mockCosmosContainer.createItem(testItem)).thenReturn(mockItemResponse);

        CosmosItemResponse<TestItem> response = executor.createItem(testItem);

        assertEquals(mockItemResponse, response);
        verify(mockCosmosContainer).createItem(testItem);
    }

    @Test
    public void testCreateItemWithItemPartitionKeyAndOptions() {
        when(mockCosmosContainer.createItem(testItem, partitionKey, itemRequestOptions)).thenReturn(mockItemResponse);

        CosmosItemResponse<TestItem> response = executor.createItem(testItem, partitionKey, itemRequestOptions);

        assertEquals(mockItemResponse, response);
        verify(mockCosmosContainer).createItem(testItem, partitionKey, itemRequestOptions);
    }

    @Test
    public void testCreateItemWithItemAndOptions() {
        when(mockCosmosContainer.createItem(testItem, itemRequestOptions)).thenReturn(mockItemResponse);

        CosmosItemResponse<TestItem> response = executor.createItem(testItem, itemRequestOptions);

        assertEquals(mockItemResponse, response);
        verify(mockCosmosContainer).createItem(testItem, itemRequestOptions);
    }

    @Test
    public void testUpsertItemWithItem() {
        when(mockCosmosContainer.upsertItem(testItem)).thenReturn(mockItemResponse);

        CosmosItemResponse<TestItem> response = executor.upsertItem(testItem);

        assertEquals(mockItemResponse, response);
        verify(mockCosmosContainer).upsertItem(testItem);
    }

    @Test
    public void testUpsertItemWithItemPartitionKeyAndOptions() {
        when(mockCosmosContainer.upsertItem(testItem, partitionKey, itemRequestOptions)).thenReturn(mockItemResponse);

        CosmosItemResponse<TestItem> response = executor.upsertItem(testItem, partitionKey, itemRequestOptions);

        assertEquals(mockItemResponse, response);
        verify(mockCosmosContainer).upsertItem(testItem, partitionKey, itemRequestOptions);
    }

    @Test
    public void testUpsertItemWithItemAndOptions() {
        when(mockCosmosContainer.upsertItem(testItem, itemRequestOptions)).thenReturn(mockItemResponse);

        CosmosItemResponse<TestItem> response = executor.upsertItem(testItem, itemRequestOptions);

        assertEquals(mockItemResponse, response);
        verify(mockCosmosContainer).upsertItem(testItem, itemRequestOptions);
    }

    @Test
    public void testReplaceItem() {
        String oldItemId = "oldId";
        when(mockCosmosContainer.replaceItem(testItem, oldItemId, partitionKey, itemRequestOptions)).thenReturn(mockItemResponse);

        CosmosItemResponse<TestItem> response = executor.replaceItem(oldItemId, partitionKey, testItem, itemRequestOptions);

        assertEquals(mockItemResponse, response);
        verify(mockCosmosContainer).replaceItem(testItem, oldItemId, partitionKey, itemRequestOptions);
    }

    @Test
    public void testPatchItemWithoutOptions() {
        String itemId = "itemId";
        CosmosPatchOperations patchOperations = CosmosPatchOperations.create();
        when(mockCosmosContainer.patchItem(itemId, partitionKey, patchOperations, TestItem.class)).thenReturn(mockItemResponse);

        CosmosItemResponse<TestItem> response = executor.patchItem(itemId, partitionKey, patchOperations, TestItem.class);

        assertEquals(mockItemResponse, response);
        verify(mockCosmosContainer).patchItem(itemId, partitionKey, patchOperations, TestItem.class);
    }

    @Test
    public void testPatchItemWithOptions() {
        String itemId = "itemId";
        CosmosPatchOperations patchOperations = CosmosPatchOperations.create();
        when(mockCosmosContainer.patchItem(itemId, partitionKey, patchOperations, patchRequestOptions, TestItem.class)).thenReturn(mockItemResponse);

        CosmosItemResponse<TestItem> response = executor.patchItem(itemId, partitionKey, patchOperations, patchRequestOptions, TestItem.class);

        assertEquals(mockItemResponse, response);
        verify(mockCosmosContainer).patchItem(itemId, partitionKey, patchOperations, patchRequestOptions, TestItem.class);
    }

    @Test
    public void testDeleteItemWithItem() {
        when(mockCosmosContainer.deleteItem(testItem, itemRequestOptions)).thenReturn(mockObjectItemResponse);

        CosmosItemResponse<Object> response = executor.deleteItem(testItem, itemRequestOptions);

        assertEquals(mockObjectItemResponse, response);
        verify(mockCosmosContainer).deleteItem(testItem, itemRequestOptions);
    }

    @Test
    public void testDeleteItemWithId() {
        String itemId = "itemId";
        when(mockCosmosContainer.deleteItem(itemId, partitionKey, itemRequestOptions)).thenReturn(mockObjectItemResponse);

        CosmosItemResponse<Object> response = executor.deleteItem(itemId, partitionKey, itemRequestOptions);

        assertEquals(mockObjectItemResponse, response);
        verify(mockCosmosContainer).deleteItem(itemId, partitionKey, itemRequestOptions);
    }

    @Test
    public void testDeleteAllItemsByPartitionKey() {
        when(mockCosmosContainer.deleteAllItemsByPartitionKey(partitionKey, itemRequestOptions)).thenReturn(mockObjectItemResponse);

        CosmosItemResponse<Object> response = executor.deleteAllItemsByPartitionKey(partitionKey, itemRequestOptions);

        assertEquals(mockObjectItemResponse, response);
        verify(mockCosmosContainer).deleteAllItemsByPartitionKey(partitionKey, itemRequestOptions);
    }

    @Test
    public void testReadItemWithoutOptions() {
        String itemId = "itemId";
        when(mockCosmosContainer.readItem(itemId, partitionKey, TestItem.class)).thenReturn(mockItemResponse);

        CosmosItemResponse<TestItem> response = executor.readItem(itemId, partitionKey, TestItem.class);

        assertEquals(mockItemResponse, response);
        verify(mockCosmosContainer).readItem(itemId, partitionKey, TestItem.class);
    }

    @Test
    public void testReadItemWithOptions() {
        String itemId = "itemId";
        when(mockCosmosContainer.readItem(itemId, partitionKey, itemRequestOptions, TestItem.class)).thenReturn(mockItemResponse);

        CosmosItemResponse<TestItem> response = executor.readItem(itemId, partitionKey, itemRequestOptions, TestItem.class);

        assertEquals(mockItemResponse, response);
        verify(mockCosmosContainer).readItem(itemId, partitionKey, itemRequestOptions, TestItem.class);
    }

    @Test
    public void testGetWithoutOptions_Found() {
        String itemId = "itemId";
        when(mockItemResponse.getItem()).thenReturn(testItem);
        when(mockCosmosContainer.readItem(itemId, partitionKey, TestItem.class)).thenReturn(mockItemResponse);

        Optional<TestItem> result = executor.get(itemId, partitionKey, TestItem.class);

        assertTrue(result.isPresent());
        assertEquals(testItem, result.get());
    }

    @Test
    public void testGetWithoutOptions_NotFoundReturnsEmpty() {
        String itemId = "missing";
        CosmosException notFound = mock(CosmosException.class);
        when(notFound.getStatusCode()).thenReturn(404);
        when(mockCosmosContainer.readItem(itemId, partitionKey, TestItem.class)).thenThrow(notFound);

        Optional<TestItem> result = executor.get(itemId, partitionKey, TestItem.class);

        assertFalse(result.isPresent());
    }

    @Test
    public void testGetWithoutOptions_Non404Propagates() {
        String itemId = "itemId";
        CosmosException throttled = mock(CosmosException.class);
        when(throttled.getStatusCode()).thenReturn(429); // Too Many Requests -> not absence, must propagate
        when(mockCosmosContainer.readItem(itemId, partitionKey, TestItem.class)).thenThrow(throttled);

        assertThrows(CosmosException.class, () -> executor.get(itemId, partitionKey, TestItem.class));
    }

    @Test
    public void testGetWithOptions_Found() {
        String itemId = "itemId";
        when(mockItemResponse.getItem()).thenReturn(testItem);
        when(mockCosmosContainer.readItem(itemId, partitionKey, itemRequestOptions, TestItem.class)).thenReturn(mockItemResponse);

        Optional<TestItem> result = executor.get(itemId, partitionKey, itemRequestOptions, TestItem.class);

        assertTrue(result.isPresent());
        assertEquals(testItem, result.get());
    }

    @Test
    public void testGetWithOptions_NotFoundReturnsEmpty() {
        String itemId = "missing";
        CosmosException notFound = mock(CosmosException.class);
        when(notFound.getStatusCode()).thenReturn(404);
        when(mockCosmosContainer.readItem(itemId, partitionKey, itemRequestOptions, TestItem.class)).thenThrow(notFound);

        Optional<TestItem> result = executor.get(itemId, partitionKey, itemRequestOptions, TestItem.class);

        assertFalse(result.isPresent());
    }

    @Test
    public void testReadManyWithoutSessionToken() {
        List<CosmosItemIdentity> itemIdentityList = Arrays.asList(new CosmosItemIdentity(partitionKey, "id1"), new CosmosItemIdentity(partitionKey, "id2"));
        when(mockCosmosContainer.readMany(itemIdentityList, TestItem.class)).thenReturn(mockFeedResponse);

        FeedResponse<TestItem> response = executor.readMany(itemIdentityList, TestItem.class);

        assertEquals(mockFeedResponse, response);
        verify(mockCosmosContainer).readMany(itemIdentityList, TestItem.class);
    }

    @Test
    public void testReadManyWithSessionToken() {
        List<CosmosItemIdentity> itemIdentityList = Arrays.asList(new CosmosItemIdentity(partitionKey, "id1"), new CosmosItemIdentity(partitionKey, "id2"));
        String sessionToken = "sessionToken";
        when(mockCosmosContainer.readMany(itemIdentityList, sessionToken, TestItem.class)).thenReturn(mockFeedResponse);

        FeedResponse<TestItem> response = executor.readMany(itemIdentityList, sessionToken, TestItem.class);

        assertEquals(mockFeedResponse, response);
        verify(mockCosmosContainer).readMany(itemIdentityList, sessionToken, TestItem.class);
    }

    @Test
    public void testReadAllItemsWithoutOptions() {
        when(mockCosmosContainer.readAllItems(partitionKey, TestItem.class)).thenReturn(mockPagedIterable);

        CosmosPagedIterable<TestItem> response = executor.readAllItems(partitionKey, TestItem.class);

        assertEquals(mockPagedIterable, response);
        verify(mockCosmosContainer).readAllItems(partitionKey, TestItem.class);
    }

    @Test
    public void testReadAllItemsWithOptions() {
        when(mockCosmosContainer.readAllItems(partitionKey, queryRequestOptions, TestItem.class)).thenReturn(mockPagedIterable);

        CosmosPagedIterable<TestItem> response = executor.readAllItems(partitionKey, queryRequestOptions, TestItem.class);

        assertEquals(mockPagedIterable, response);
        verify(mockCosmosContainer).readAllItems(partitionKey, queryRequestOptions, TestItem.class);
    }

    @Test
    public void testStreamAllItemsWithoutOptions() {
        List<TestItem> items = Arrays.asList(new TestItem("1", "Item1"), new TestItem("2", "Item2"));
        when(mockCosmosContainer.readAllItems(partitionKey, TestItem.class)).thenReturn(mockPagedIterable);
        when(mockPagedIterable.stream()).thenReturn(items.stream());

        Stream<TestItem> stream = executor.streamAllItems(partitionKey, TestItem.class);

        assertNotNull(stream);
        List<TestItem> result = stream.toList();
        assertEquals(2, result.size());
        assertEquals("1", result.get(0).id);
        verify(mockCosmosContainer).readAllItems(partitionKey, TestItem.class);
    }

    @Test
    public void testStreamAllItemsWithOptions() {
        List<TestItem> items = Arrays.asList(new TestItem("1", "Item1"), new TestItem("2", "Item2"));
        when(mockCosmosContainer.readAllItems(partitionKey, queryRequestOptions, TestItem.class)).thenReturn(mockPagedIterable);
        when(mockPagedIterable.stream()).thenReturn(items.stream());

        Stream<TestItem> stream = executor.streamAllItems(partitionKey, queryRequestOptions, TestItem.class);

        assertNotNull(stream);
        List<TestItem> result = stream.toList();
        assertEquals(2, result.size());
        assertEquals("1", result.get(0).id);
        verify(mockCosmosContainer).readAllItems(partitionKey, queryRequestOptions, TestItem.class);
    }

    @Test
    public void testQueryItemsWithStringQuery() {
        String query = "SELECT * FROM c WHERE c.id = 'id1'";
        when(mockCosmosContainer.queryItems(query, null, TestItem.class)).thenReturn(mockPagedIterable);

        CosmosPagedIterable<TestItem> response = executor.queryItems(query, TestItem.class);

        assertEquals(mockPagedIterable, response);
        verify(mockCosmosContainer).queryItems(query, null, TestItem.class);
    }

    @Test
    public void testQueryItemsWithStringQueryAndOptions() {
        String query = "SELECT * FROM c WHERE c.id = 'id1'";
        when(mockCosmosContainer.queryItems(query, queryRequestOptions, TestItem.class)).thenReturn(mockPagedIterable);

        CosmosPagedIterable<TestItem> response = executor.queryItems(query, queryRequestOptions, TestItem.class);

        assertEquals(mockPagedIterable, response);
        verify(mockCosmosContainer).queryItems(query, queryRequestOptions, TestItem.class);
    }

    @Test
    public void testQueryItemsWithSqlQuerySpec() {
        SqlQuerySpec querySpec = new SqlQuerySpec("SELECT * FROM c WHERE c.id = @id");
        when(mockCosmosContainer.queryItems(querySpec, null, TestItem.class)).thenReturn(mockPagedIterable);

        CosmosPagedIterable<TestItem> response = executor.queryItems(querySpec, TestItem.class);

        assertEquals(mockPagedIterable, response);
        verify(mockCosmosContainer).queryItems(querySpec, null, TestItem.class);
    }

    @Test
    public void testQueryItemsWithSqlQuerySpecAndOptions() {
        SqlQuerySpec querySpec = new SqlQuerySpec("SELECT * FROM c WHERE c.id = @id");
        when(mockCosmosContainer.queryItems(querySpec, queryRequestOptions, TestItem.class)).thenReturn(mockPagedIterable);

        CosmosPagedIterable<TestItem> response = executor.queryItems(querySpec, queryRequestOptions, TestItem.class);

        assertEquals(mockPagedIterable, response);
        verify(mockCosmosContainer).queryItems(querySpec, queryRequestOptions, TestItem.class);
    }

    @Test
    public void testStreamItemsWithStringQuery() {
        String query = "SELECT * FROM c";
        List<TestItem> items = Arrays.asList(new TestItem("1", "Item1"), new TestItem("2", "Item2"));
        when(mockCosmosContainer.queryItems(query, null, TestItem.class)).thenReturn(mockPagedIterable);
        when(mockPagedIterable.stream()).thenReturn(items.stream());

        Stream<TestItem> stream = executor.streamItems(query, TestItem.class);

        assertNotNull(stream);
        List<TestItem> result = stream.toList();
        assertEquals(2, result.size());
        verify(mockCosmosContainer).queryItems(query, null, TestItem.class);
    }

    @Test
    public void testStreamItemsWithStringQueryAndOptions() {
        String query = "SELECT * FROM c";
        List<TestItem> items = Arrays.asList(new TestItem("1", "Item1"), new TestItem("2", "Item2"));
        when(mockCosmosContainer.queryItems(query, queryRequestOptions, TestItem.class)).thenReturn(mockPagedIterable);
        when(mockPagedIterable.stream()).thenReturn(items.stream());

        Stream<TestItem> stream = executor.streamItems(query, queryRequestOptions, TestItem.class);

        assertNotNull(stream);
        List<TestItem> result = stream.toList();
        assertEquals(2, result.size());
        verify(mockCosmosContainer).queryItems(query, queryRequestOptions, TestItem.class);
    }

    @Test
    public void testStreamItemsWithSqlQuerySpec() {
        SqlQuerySpec querySpec = new SqlQuerySpec("SELECT * FROM c");
        List<TestItem> items = Arrays.asList(new TestItem("1", "Item1"), new TestItem("2", "Item2"));
        when(mockCosmosContainer.queryItems(querySpec, null, TestItem.class)).thenReturn(mockPagedIterable);
        when(mockPagedIterable.stream()).thenReturn(items.stream());

        Stream<TestItem> stream = executor.streamItems(querySpec, TestItem.class);

        assertNotNull(stream);
        List<TestItem> result = stream.toList();
        assertEquals(2, result.size());
        verify(mockCosmosContainer).queryItems(querySpec, null, TestItem.class);
    }

    @Test
    public void testStreamItemsWithSqlQuerySpecAndOptions() {
        SqlQuerySpec querySpec = new SqlQuerySpec("SELECT * FROM c");
        List<TestItem> items = Arrays.asList(new TestItem("1", "Item1"), new TestItem("2", "Item2"));
        when(mockCosmosContainer.queryItems(querySpec, queryRequestOptions, TestItem.class)).thenReturn(mockPagedIterable);
        when(mockPagedIterable.stream()).thenReturn(items.stream());

        Stream<TestItem> stream = executor.streamItems(querySpec, queryRequestOptions, TestItem.class);

        assertNotNull(stream);
        List<TestItem> result = stream.toList();
        assertEquals(2, result.size());
        verify(mockCosmosContainer).queryItems(querySpec, queryRequestOptions, TestItem.class);
    }

    @Test
    public void testStreamItemsWithCondition() {
        // Test with different naming policies
        testStreamItemsWithConditionAndNamingPolicy(NamingPolicy.SNAKE_CASE);
        testStreamItemsWithConditionAndNamingPolicy(NamingPolicy.SCREAMING_SNAKE_CASE);
        testStreamItemsWithConditionAndNamingPolicy(NamingPolicy.CAMEL_CASE);
    }

    private void testStreamItemsWithConditionAndNamingPolicy(NamingPolicy namingPolicy) {
        CosmosContainerExecutor executorWithPolicy = new CosmosContainerExecutor(mockCosmosContainer, namingPolicy);
        List<TestItem> items = Arrays.asList(new TestItem("1", "Item1"), new TestItem("2", "Item2"));
        when(mockPagedIterable.stream()).thenReturn(items.stream());
        when(mockCosmosContainer.queryItems(any(SqlQuerySpec.class), any(), eq(TestItem.class))).thenReturn(mockPagedIterable);

        Stream<TestItem> stream = executorWithPolicy.streamItems(Filters.eq("id", "1"), TestItem.class);

        assertNotNull(stream);
        List<TestItem> result = stream.toList();
        assertEquals(2, result.size());
    }

    @Test
    public void testStreamItemsWithConditionGeneratesAliasQualifiedCosmosSql() {
        // Regression: Cosmos DB SQL requires every property reference to be bound to the FROM
        // source/alias. prepareQuery used to emit bare identifiers (e.g. "FROM test_item WHERE
        // id = '1'"), which Cosmos rejects at query enumeration. It must now qualify them via "c".
        List<TestItem> items = Arrays.asList(new TestItem("1", "Item1"));
        when(mockPagedIterable.stream()).thenReturn(items.stream());
        final org.mockito.ArgumentCaptor<SqlQuerySpec> specCaptor = org.mockito.ArgumentCaptor.forClass(SqlQuerySpec.class);
        when(mockCosmosContainer.queryItems(specCaptor.capture(), any(), eq(TestItem.class))).thenReturn(mockPagedIterable);

        // default executor uses NamingPolicy.SNAKE_CASE -> table name "test_item"
        executor.streamItems(Filters.and(Filters.eq("id", "1"), Filters.eq("name", "a")), TestItem.class).toList();

        final String sql = specCaptor.getValue().getQueryText();
        assertEquals("SELECT * FROM test_item c WHERE (c.id = @p0) AND (c.name = @p1)", sql);
        assertTrue(sql.contains("FROM test_item c"), "FROM clause must declare the alias 'c': " + sql);
        assertTrue(sql.contains("c.id"), "id must be alias-qualified: " + sql);
        assertTrue(sql.contains("c.name"), "name must be alias-qualified: " + sql);
        // Old broken form had the table immediately followed by WHERE with unqualified columns.
        assertTrue(!sql.contains("FROM test_item WHERE"), "WHERE columns must be alias-qualified: " + sql);

        assertEquals(2, specCaptor.getValue().getParameters().size());
        assertEquals("@p0", specCaptor.getValue().getParameters().get(0).getName());
        assertEquals("1", specCaptor.getValue().getParameters().get(0).getValue(String.class));
        assertEquals("@p1", specCaptor.getValue().getParameters().get(1).getName());
        assertEquals("a", specCaptor.getValue().getParameters().get(1).getValue(String.class));
    }

    @Test
    public void testStreamItemsWithFunctionExpressionNullConditionUsesCosmosSyntax() {
        when(mockPagedIterable.stream()).thenAnswer(invocation -> java.util.stream.Stream.empty());
        final org.mockito.ArgumentCaptor<SqlQuerySpec> specCaptor = org.mockito.ArgumentCaptor.forClass(SqlQuerySpec.class);
        when(mockCosmosContainer.queryItems(specCaptor.capture(), any(), eq(TestItem.class))).thenReturn(mockPagedIterable);

        executor.streamItems(Filters.isNull("ARRAY_LENGTH(tags)"), TestItem.class).toList();

        assertEquals("SELECT * FROM test_item c WHERE IS_NULL(ARRAY_LENGTH(c.tags))", specCaptor.getValue().getQueryText());
    }

    @Test
    public void testStreamItemsWithArithmeticNullExpressionsUsesCosmosSyntax() {
        when(mockPagedIterable.stream()).thenAnswer(invocation -> java.util.stream.Stream.empty());
        final org.mockito.ArgumentCaptor<SqlQuerySpec> specCaptor = org.mockito.ArgumentCaptor.forClass(SqlQuerySpec.class);
        when(mockCosmosContainer.queryItems(specCaptor.capture(), any(), eq(TestItem.class))).thenReturn(mockPagedIterable);

        executor.streamItems(Filters.expr("price + tax IS NULL"), TestItem.class).toList();
        executor.streamItems(Filters.expr("price + tax IS NOT NULL"), TestItem.class).toList();

        assertEquals("SELECT * FROM test_item c WHERE IS_NULL(c.price + c.tax)", specCaptor.getAllValues().get(0).getQueryText());
        assertEquals("SELECT * FROM test_item c WHERE NOT IS_NULL(c.price + c.tax)", specCaptor.getAllValues().get(1).getQueryText());
    }

    @Test
    public void testStreamItemsWithNullExpressionAsFunctionArgumentUsesCosmosSyntax() {
        when(mockPagedIterable.stream()).thenReturn(java.util.stream.Stream.empty());
        final org.mockito.ArgumentCaptor<SqlQuerySpec> specCaptor = org.mockito.ArgumentCaptor.forClass(SqlQuerySpec.class);
        when(mockCosmosContainer.queryItems(specCaptor.capture(), any(), eq(TestItem.class))).thenReturn(mockPagedIterable);

        executor.streamItems(Filters.expr("IIF(flag, value IS NULL, false)"), TestItem.class).toList();

        assertEquals("SELECT * FROM test_item c WHERE IIF(c.flag, IS_NULL(c.value), false)", specCaptor.getValue().getQueryText());
    }

    @Test
    public void testRawExpressionQualifiesFieldsAfterLogicalKeywordsAndPreservesExponent() {
        when(mockPagedIterable.stream()).thenReturn(java.util.stream.Stream.empty());
        final org.mockito.ArgumentCaptor<SqlQuerySpec> specCaptor = org.mockito.ArgumentCaptor.forClass(SqlQuerySpec.class);
        when(mockCosmosContainer.queryItems(specCaptor.capture(), any(), eq(TestItem.class))).thenReturn(mockPagedIterable);

        executor.streamItems(Filters.expr("active AND score > 1e2"), TestItem.class).toList();

        assertEquals("SELECT * FROM test_item c WHERE c.active AND c.score > 1e2", specCaptor.getValue().getQueryText());
    }

    @Test
    public void testKeywordNamedPropertiesAreStillAliasQualified() {
        when(mockPagedIterable.stream()).thenAnswer(invocation -> java.util.stream.Stream.empty());
        final org.mockito.ArgumentCaptor<SqlQuerySpec> specCaptor = org.mockito.ArgumentCaptor.forClass(SqlQuerySpec.class);
        when(mockCosmosContainer.queryItems(specCaptor.capture(), any(), eq(TestItem.class))).thenReturn(mockPagedIterable);

        executor.streamItems(Filters.eq("order", 1), TestItem.class).toList();
        executor.streamItems(Filters.isNull("group"), TestItem.class).toList();
        executor.streamItems(Filters.expr("score > order"), TestItem.class).toList();
        executor.streamItems(Filters.expr("order NOT IN (1, 2)"), TestItem.class).toList();
        executor.streamItems(Filters.eq("c", 1), TestItem.class).toList();
        executor.streamItems(Filters.expr("score = in"), TestItem.class).toList();
        executor.streamItems(Filters.expr("score = null"), TestItem.class).toList();
        executor.streamItems(Filters.expr("IS_DEFINED(order)"), TestItem.class).toList();
        executor.streamItems(Filters.expr("order AND active"), TestItem.class).toList();
        executor.streamItems(Filters.expr("value is null"), TestItem.class).toList();

        assertEquals("SELECT * FROM test_item c WHERE c.order = @p0", specCaptor.getAllValues().get(0).getQueryText());
        assertEquals("SELECT * FROM test_item c WHERE IS_NULL(c.group)", specCaptor.getAllValues().get(1).getQueryText());
        assertEquals("SELECT * FROM test_item c WHERE c.score > c.order", specCaptor.getAllValues().get(2).getQueryText());
        assertEquals("SELECT * FROM test_item c WHERE c.order NOT IN (1, 2)", specCaptor.getAllValues().get(3).getQueryText());
        assertEquals("SELECT * FROM test_item c WHERE c.c = @p0", specCaptor.getAllValues().get(4).getQueryText());
        assertEquals("SELECT * FROM test_item c WHERE c.score = c.in", specCaptor.getAllValues().get(5).getQueryText());
        assertEquals("SELECT * FROM test_item c WHERE c.score = null", specCaptor.getAllValues().get(6).getQueryText());
        assertEquals("SELECT * FROM test_item c WHERE IS_DEFINED(c.order)", specCaptor.getAllValues().get(7).getQueryText());
        assertEquals("SELECT * FROM test_item c WHERE c.order AND c.active", specCaptor.getAllValues().get(8).getQueryText());
        assertEquals("SELECT * FROM test_item c WHERE IS_NULL(c.value)", specCaptor.getAllValues().get(9).getQueryText());
    }

    @Test
    public void testIsNullOperandScanHonoursLowercaseKeywordsAndNot() {
        when(mockPagedIterable.stream()).thenAnswer(invocation -> java.util.stream.Stream.empty());
        final org.mockito.ArgumentCaptor<SqlQuerySpec> specCaptor = org.mockito.ArgumentCaptor.forClass(SqlQuerySpec.class);
        when(mockCosmosContainer.queryItems(specCaptor.capture(), any(), eq(TestItem.class))).thenReturn(mockPagedIterable);

        // Clause boundaries must be recognized case-insensitively: a lower/mixed-case "and"/"or" in a
        // caller-supplied raw expression must not let the operand scan swallow the preceding predicate.
        executor.streamItems(Filters.expr("score = 1 or group IS NULL"), TestItem.class).toList();
        executor.streamItems(Filters.expr("score is null or group is null"), TestItem.class).toList();
        executor.streamItems(Filters.expr("score is null And group is null"), TestItem.class).toList();
        // "NOT" is a boundary too, so the negation stays outside IS_NULL(...).
        executor.streamItems(Filters.expr("NOT group IS NULL"), TestItem.class).toList();
        executor.streamItems(Filters.expr("score > 1 AND NOT group IS NULL"), TestItem.class).toList();
        // The whole-token "IS NOT NULL" form is unaffected.
        executor.streamItems(Filters.expr("group IS NOT NULL"), TestItem.class).toList();

        assertEquals("SELECT * FROM test_item c WHERE c.score = 1 or IS_NULL(c.group)", specCaptor.getAllValues().get(0).getQueryText());
        assertEquals("SELECT * FROM test_item c WHERE IS_NULL(c.score) or IS_NULL(c.group)", specCaptor.getAllValues().get(1).getQueryText());
        // The query builder normalizes the identifier-like "And" token to lower case before the Cosmos
        // rewrite runs; the point of the assertion is that the boundary is still recognized.
        assertEquals("SELECT * FROM test_item c WHERE IS_NULL(c.score) and IS_NULL(c.group)", specCaptor.getAllValues().get(2).getQueryText());
        assertEquals("SELECT * FROM test_item c WHERE NOT IS_NULL(c.group)", specCaptor.getAllValues().get(3).getQueryText());
        assertEquals("SELECT * FROM test_item c WHERE c.score > 1 AND NOT IS_NULL(c.group)", specCaptor.getAllValues().get(4).getQueryText());
        assertEquals("SELECT * FROM test_item c WHERE NOT IS_NULL(c.group)", specCaptor.getAllValues().get(5).getQueryText());
    }

    @Test
    public void testStreamItemsWithConditionAndOptions() {
        List<TestItem> items = Arrays.asList(new TestItem("1", "Item1"), new TestItem("2", "Item2"));
        when(mockPagedIterable.stream()).thenReturn(items.stream());
        when(mockCosmosContainer.queryItems(any(SqlQuerySpec.class), eq(queryRequestOptions), eq(TestItem.class))).thenReturn(mockPagedIterable);

        Stream<TestItem> stream = executor.streamItems(Filters.eq("id", "1"), queryRequestOptions, TestItem.class);

        assertNotNull(stream);
        List<TestItem> result = stream.toList();
        assertEquals(2, result.size());
    }

    @Test
    public void testStreamItemsWithSelectAndCondition() {
        Collection<String> selectProps = Arrays.asList("id", "name");
        List<TestItem> items = Arrays.asList(new TestItem("1", "Item1"), new TestItem("2", "Item2"));
        when(mockPagedIterable.stream()).thenReturn(items.stream());
        final org.mockito.ArgumentCaptor<SqlQuerySpec> specCaptor = org.mockito.ArgumentCaptor.forClass(SqlQuerySpec.class);
        when(mockCosmosContainer.queryItems(specCaptor.capture(), any(), eq(TestItem.class))).thenReturn(mockPagedIterable);

        Stream<TestItem> stream = executor.streamItems(selectProps, Filters.eq("id", "1"), TestItem.class);

        assertNotNull(stream);
        List<TestItem> result = stream.toList();
        assertEquals(2, result.size());
        assertEquals("SELECT VALUE { " + '"' + "id" + '"' + ": c.id, " + '"' + "name" + '"' + ": c.name } FROM test_item c WHERE c.id = @p0",
                specCaptor.getValue().getQueryText());
    }

    /** Projection and non-projection overloads must reject a null result type consistently. */
    @Test
    public void testStreamItemsWithProjectionRejectsNullTargetClass() {
        assertThrows(IllegalArgumentException.class, () -> executor.streamItems(List.of("id"), null, (Class<TestItem>) null));
    }

    @Test
    public void testStreamItemsWithSelectConditionAndOptions() {
        Collection<String> selectProps = Arrays.asList("id", "name");
        List<TestItem> items = Arrays.asList(new TestItem("1", "Item1"), new TestItem("2", "Item2"));
        when(mockPagedIterable.stream()).thenReturn(items.stream());
        when(mockCosmosContainer.queryItems(any(SqlQuerySpec.class), eq(queryRequestOptions), eq(TestItem.class))).thenReturn(mockPagedIterable);

        Stream<TestItem> stream = executor.streamItems(selectProps, Filters.eq("id", "1"), queryRequestOptions, TestItem.class);

        assertNotNull(stream);
        List<TestItem> result = stream.toList();
        assertEquals(2, result.size());
    }

    @Test
    public void testStreamItemsWithNullCondition() {
        List<TestItem> items = Arrays.asList(new TestItem("1", "Item1"), new TestItem("2", "Item2"));
        when(mockPagedIterable.stream()).thenReturn(items.stream());
        when(mockCosmosContainer.queryItems(any(SqlQuerySpec.class), any(), eq(TestItem.class))).thenReturn(mockPagedIterable);

        Stream<TestItem> stream = executor.streamItems((Collection<String>) null, null, TestItem.class);

        assertNotNull(stream);
        List<TestItem> result = stream.toList();
        assertEquals(2, result.size());
    }

    @Test
    public void testStreamItemsWithEmptySelectProps() {
        Collection<String> emptySelectProps = Arrays.asList();
        List<TestItem> items = Arrays.asList(new TestItem("1", "Item1"), new TestItem("2", "Item2"));
        when(mockPagedIterable.stream()).thenReturn(items.stream());
        when(mockCosmosContainer.queryItems(any(SqlQuerySpec.class), any(), eq(TestItem.class))).thenReturn(mockPagedIterable);

        Stream<TestItem> stream = executor.streamItems(emptySelectProps, Filters.eq("id", "1"), TestItem.class);

        assertNotNull(stream);
        List<TestItem> result = stream.toList();
        assertEquals(2, result.size());
    }

    @Test
    public void testRewritePositionalParametersRejectsExtraPlaceholders() throws Exception {
        final Method method = CosmosContainerExecutor.class.getDeclaredMethod("rewritePositionalParameters", String.class, int.class);
        method.setAccessible(true);

        final InvocationTargetException ex = assertThrows(InvocationTargetException.class,
                () -> method.invoke(null, "SELECT * FROM c WHERE c.id = ? AND c.name = ?", 1));

        assertTrue(ex.getCause() instanceof IllegalArgumentException);
        assertTrue(ex.getCause().getMessage().contains("expected 1 placeholders but found 2"));
    }

    // Additional rewritePositionalParameters branch coverage: in-quote skipping, escaped quotes, multi-placeholder.

    @Test
    public void testRewritePositionalParameters_NoPlaceholders() throws Exception {
        final Method method = CosmosContainerExecutor.class.getDeclaredMethod("rewritePositionalParameters", String.class, int.class);
        method.setAccessible(true);
        final String result = (String) method.invoke(null, "SELECT * FROM c WHERE c.id = 'static'", 0);
        assertEquals("SELECT * FROM c WHERE c.id = 'static'", result);
    }

    @Test
    public void testRewritePositionalParameters_MultiPlaceholder() throws Exception {
        final Method method = CosmosContainerExecutor.class.getDeclaredMethod("rewritePositionalParameters", String.class, int.class);
        method.setAccessible(true);
        final String result = (String) method.invoke(null, "SELECT * FROM c WHERE c.a = ? AND c.b = ? AND c.c = ?", 3);
        assertEquals("SELECT * FROM c WHERE c.a = @p0 AND c.b = @p1 AND c.c = @p2", result);
    }

    @Test
    public void testRewritePositionalParameters_QuestionMarkInsideSingleQuotes() throws Exception {
        // The '?' inside a single-quoted string literal must be preserved, not rewritten.
        final Method method = CosmosContainerExecutor.class.getDeclaredMethod("rewritePositionalParameters", String.class, int.class);
        method.setAccessible(true);
        final String result = (String) method.invoke(null, "SELECT * FROM c WHERE c.name = 'a?b' AND c.id = ?", 1);
        assertEquals("SELECT * FROM c WHERE c.name = 'a?b' AND c.id = @p0", result);
    }

    @Test
    public void testRewritePositionalParameters_QuestionMarkInsideDoubleQuotes() throws Exception {
        // Cosmos accepts both quote styles for string literals; neither may be scanned as a placeholder.
        final Method method = CosmosContainerExecutor.class.getDeclaredMethod("rewritePositionalParameters", String.class, int.class);
        method.setAccessible(true);
        final String result = (String) method.invoke(null, "SELECT * FROM c WHERE c.name = \"a?b\" AND c.id = ?", 1);
        assertEquals("SELECT * FROM c WHERE c.name = \"a?b\" AND c.id = @p0", result);
    }

    @Test
    public void testRewritePositionalParameters_BackslashEscapedQuote() throws Exception {
        final Method method = CosmosContainerExecutor.class.getDeclaredMethod("rewritePositionalParameters", String.class, int.class);
        method.setAccessible(true);
        final String result = (String) method.invoke(null, "SELECT * FROM c WHERE c.name = \"a\\\"?b\" AND c.id = ?", 1);
        assertEquals("SELECT * FROM c WHERE c.name = \"a\\\"?b\" AND c.id = @p0", result);
    }

    @Test
    public void testRewritePositionalParameters_EscapedSingleQuote() throws Exception {
        // '' is the SQL escape for a single quote and should not toggle the in-quote state.
        final Method method = CosmosContainerExecutor.class.getDeclaredMethod("rewritePositionalParameters", String.class, int.class);
        method.setAccessible(true);
        final String result = (String) method.invoke(null, "SELECT * FROM c WHERE c.name = 'O''Brien' AND c.id = ?", 1);
        assertEquals("SELECT * FROM c WHERE c.name = 'O''Brien' AND c.id = @p0", result);
    }

    @Test
    public void testRewritePositionalParameters_FewerParametersThanPlaceholders() throws Exception {
        // When parameterCount < total '?' found, the extras stay as '?' and we hit the mismatch error.
        final Method method = CosmosContainerExecutor.class.getDeclaredMethod("rewritePositionalParameters", String.class, int.class);
        method.setAccessible(true);
        final InvocationTargetException ex = assertThrows(InvocationTargetException.class,
                () -> method.invoke(null, "SELECT * FROM c WHERE a=? AND b=? AND c=?", 2));
        assertTrue(ex.getCause() instanceof IllegalArgumentException);
        assertTrue(ex.getCause().getMessage().contains("expected 2 placeholders but found 3"));
    }

    @Test
    public void testStreamItemsWithSelectAndConditionAndNamingPolicy_ScreamingSnake() {
        // Exercise SCREAMING_SNAKE_CASE branch in prepareQuery.
        CosmosContainerExecutor executorWithPolicy = new CosmosContainerExecutor(mockCosmosContainer, NamingPolicy.SCREAMING_SNAKE_CASE);
        List<TestItem> items = Arrays.asList(new TestItem("1", "Item1"));
        when(mockPagedIterable.stream()).thenReturn(items.stream());
        when(mockCosmosContainer.queryItems(any(SqlQuerySpec.class), any(), eq(TestItem.class))).thenReturn(mockPagedIterable);

        Stream<TestItem> stream = executorWithPolicy.streamItems(Arrays.asList("id"), Filters.eq("id", "1"), TestItem.class);

        assertNotNull(stream);
        assertEquals(1, stream.toList().size());
    }

    @Test
    public void testStreamItemsWithSelectAndConditionAndNamingPolicy_CamelCase() {
        // Exercise CAMEL_CASE branch in prepareQuery.
        CosmosContainerExecutor executorWithPolicy = new CosmosContainerExecutor(mockCosmosContainer, NamingPolicy.CAMEL_CASE);
        List<TestItem> items = Arrays.asList(new TestItem("1", "Item1"));
        when(mockPagedIterable.stream()).thenReturn(items.stream());
        when(mockCosmosContainer.queryItems(any(SqlQuerySpec.class), any(), eq(TestItem.class))).thenReturn(mockPagedIterable);

        Stream<TestItem> stream = executorWithPolicy.streamItems(Arrays.asList("id"), Filters.eq("id", "1"), TestItem.class);

        assertNotNull(stream);
        assertEquals(1, stream.toList().size());
    }

    @Test
    public void testConstructorRejectsNullContainer() {
        assertThrows(IllegalArgumentException.class, () -> new CosmosContainerExecutor(null));
    }

    @Test
    public void testConstructorWithPolicyRejectsNullContainer() {
        assertThrows(IllegalArgumentException.class, () -> new CosmosContainerExecutor(null, NamingPolicy.SNAKE_CASE));
    }

    @Test
    public void testConstructorWithPolicyRejectsNullPolicy() {
        assertThrows(IllegalArgumentException.class, () -> new CosmosContainerExecutor(mockCosmosContainer, (NamingPolicy) null));
    }

    @Test
    public void testUnsupportedNamingPolicy() {
        // The constructor now fails fast on an unsupported policy (was a deferred ISE on the
        // first Condition-based query).
        assertThrows(IllegalArgumentException.class, () -> new CosmosContainerExecutor(mockCosmosContainer, NamingPolicy.NO_CHANGE));
    }

    // ------------------------------------------------------------------------------------------------
    // Null-argument guards. The Cosmos SDK does not reject these eagerly: a null id is reported as a
    // 404, a null partition key as an UnsupportedOperationException, a null result type only fails on
    // getItem(), and the query/read-all methods return a lazy iterable/stream. They are checked here.
    // ------------------------------------------------------------------------------------------------

    @Test
    public void testReadItemRejectsNullArguments() {
        assertThrows(IllegalArgumentException.class, () -> executor.readItem(null, partitionKey, TestItem.class));
        assertThrows(IllegalArgumentException.class, () -> executor.readItem("id1", null, TestItem.class));
        assertThrows(IllegalArgumentException.class, () -> executor.readItem("id1", partitionKey, (Class<TestItem>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.readItem(null, partitionKey, itemRequestOptions, TestItem.class));
        assertThrows(IllegalArgumentException.class, () -> executor.readItem("id1", null, itemRequestOptions, TestItem.class));
        assertThrows(IllegalArgumentException.class, () -> executor.readItem("id1", partitionKey, itemRequestOptions, (Class<TestItem>) null));
    }

    /** A null id must not be swallowed by the 404 handling and reported as an absent item. */
    @Test
    public void testGetAndGettRejectNullItemId() {
        assertThrows(IllegalArgumentException.class, () -> executor.get(null, partitionKey, TestItem.class));
        assertThrows(IllegalArgumentException.class, () -> executor.get(null, partitionKey, itemRequestOptions, TestItem.class));
        assertThrows(IllegalArgumentException.class, () -> executor.gett(null, partitionKey, TestItem.class));
        assertThrows(IllegalArgumentException.class, () -> executor.gett(null, partitionKey, itemRequestOptions, TestItem.class));
    }

    @Test
    public void testDeleteRejectsNullArguments() {
        assertThrows(IllegalArgumentException.class, () -> executor.deleteItem((Object) null, itemRequestOptions));
        assertThrows(IllegalArgumentException.class, () -> executor.deleteItem(null, partitionKey, itemRequestOptions));
        assertThrows(IllegalArgumentException.class, () -> executor.deleteItem("id1", null, itemRequestOptions));
        assertThrows(IllegalArgumentException.class, () -> executor.deleteAllItemsByPartitionKey(null, itemRequestOptions));
    }

    /** The SDK would reject a null item with a NullPointerException; the executor rejects it up front with an IllegalArgumentException. */
    @Test
    public void testCreateAndUpsertRejectNullItem() {
        assertThrows(IllegalArgumentException.class, () -> executor.createItem(null));
        assertThrows(IllegalArgumentException.class, () -> executor.createItem(null, partitionKey, itemRequestOptions));
        assertThrows(IllegalArgumentException.class, () -> executor.createItem(null, itemRequestOptions));
        assertThrows(IllegalArgumentException.class, () -> executor.upsertItem(null));
        assertThrows(IllegalArgumentException.class, () -> executor.upsertItem(null, partitionKey, itemRequestOptions));
        assertThrows(IllegalArgumentException.class, () -> executor.upsertItem(null, itemRequestOptions));
        verifyNoInteractions(mockCosmosContainer);
    }

    @Test
    public void testReplaceAndPatchRejectNullArguments() {
        final CosmosPatchOperations patchOperations = CosmosPatchOperations.create();

        assertThrows(IllegalArgumentException.class, () -> executor.replaceItem(null, partitionKey, testItem, itemRequestOptions));
        assertThrows(IllegalArgumentException.class, () -> executor.replaceItem("id1", partitionKey, null, itemRequestOptions));
        assertThrows(IllegalArgumentException.class, () -> executor.patchItem(null, partitionKey, patchOperations, TestItem.class));
        assertThrows(IllegalArgumentException.class, () -> executor.patchItem("id1", null, patchOperations, TestItem.class));
        assertThrows(IllegalArgumentException.class, () -> executor.patchItem("id1", partitionKey, null, TestItem.class));
        assertThrows(IllegalArgumentException.class, () -> executor.patchItem("id1", partitionKey, patchOperations, (Class<TestItem>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.patchItem(null, partitionKey, patchOperations, patchRequestOptions, TestItem.class));
        assertThrows(IllegalArgumentException.class, () -> executor.patchItem("id1", null, patchOperations, patchRequestOptions, TestItem.class));
        assertThrows(IllegalArgumentException.class, () -> executor.patchItem("id1", partitionKey, null, patchRequestOptions, TestItem.class));
        assertThrows(IllegalArgumentException.class,
                () -> executor.patchItem("id1", partitionKey, patchOperations, patchRequestOptions, (Class<TestItem>) null));
        verifyNoInteractions(mockCosmosContainer);
    }

    @Test
    public void testReadManyRejectsNullArguments() {
        assertThrows(IllegalArgumentException.class, () -> executor.readMany(null, TestItem.class));
        assertThrows(IllegalArgumentException.class, () -> executor.readMany(List.<CosmosItemIdentity> of(), (Class<TestItem>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.readMany(null, "sessionToken", TestItem.class));
        assertThrows(IllegalArgumentException.class, () -> executor.readMany(List.<CosmosItemIdentity> of(), "sessionToken", (Class<TestItem>) null));
    }

    @Test
    public void testReadAllAndStreamAllItemsRejectNullArguments() {
        assertThrows(IllegalArgumentException.class, () -> executor.readAllItems(null, TestItem.class));
        assertThrows(IllegalArgumentException.class, () -> executor.readAllItems(partitionKey, (Class<TestItem>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.readAllItems(null, queryRequestOptions, TestItem.class));
        assertThrows(IllegalArgumentException.class, () -> executor.readAllItems(partitionKey, queryRequestOptions, (Class<TestItem>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.streamAllItems(null, TestItem.class));
        assertThrows(IllegalArgumentException.class, () -> executor.streamAllItems(partitionKey, (Class<TestItem>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.streamAllItems(null, queryRequestOptions, TestItem.class));
        assertThrows(IllegalArgumentException.class, () -> executor.streamAllItems(partitionKey, queryRequestOptions, (Class<TestItem>) null));
    }

    /** queryItems/streamItems hand back a lazy iterable/stream, so a null query must fail at the call site. */
    @Test
    public void testQueryAndStreamItemsRejectNullArguments() {
        final SqlQuerySpec spec = new SqlQuerySpec("SELECT * FROM c");

        assertThrows(IllegalArgumentException.class, () -> executor.queryItems((String) null, TestItem.class));
        assertThrows(IllegalArgumentException.class, () -> executor.queryItems("SELECT * FROM c", (Class<TestItem>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.queryItems((SqlQuerySpec) null, TestItem.class));
        assertThrows(IllegalArgumentException.class, () -> executor.queryItems(spec, (Class<TestItem>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.streamItems((String) null, TestItem.class));
        assertThrows(IllegalArgumentException.class, () -> executor.streamItems("SELECT * FROM c", (Class<TestItem>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.streamItems((SqlQuerySpec) null, TestItem.class));
        assertThrows(IllegalArgumentException.class, () -> executor.streamItems(spec, (Class<TestItem>) null));
    }

    private List<String> captureConditionQueries(final CosmosContainerExecutor cosmosExecutor, final com.landawn.abacus.query.condition.Condition... conditions) {
        when(mockPagedIterable.stream()).thenAnswer(invocation -> java.util.stream.Stream.empty());
        final org.mockito.ArgumentCaptor<SqlQuerySpec> specCaptor = org.mockito.ArgumentCaptor.forClass(SqlQuerySpec.class);
        when(mockCosmosContainer.queryItems(specCaptor.capture(), any(), eq(TestItem.class))).thenReturn(mockPagedIterable);

        for (final com.landawn.abacus.query.condition.Condition condition : conditions) {
            cosmosExecutor.streamItems(condition, TestItem.class).toList();
        }

        return specCaptor.getAllValues().stream().map(SqlQuerySpec::getQueryText).toList();
    }

    /**
     * Regression: abacus-query 4.9.x renders Boolean IS/IS NOT conditions as inline truth-value keywords
     * ({@code x IS TRUE}) instead of binding a parameter ({@code x IS ?}). Cosmos DB has no IS TRUE/IS FALSE
     * syntax (400 Bad Request when the stream is consumed), so they must be rewritten to comparisons.
     */
    @Test
    public void testBooleanIsPredicatesUseCosmosComparisonSyntax() {
        final List<String> queries = captureConditionQueries(executor, Filters.isTrue("active"), Filters.isFalse("active"), Filters.is("active", true),
                Filters.isNot("active", true), Filters.isNot("active", false), Filters.and(Filters.isTrue("active"), Filters.eq("id", "1")));

        assertEquals("SELECT * FROM test_item c WHERE c.active = true", queries.get(0));
        assertEquals("SELECT * FROM test_item c WHERE c.active = false", queries.get(1));
        assertEquals("SELECT * FROM test_item c WHERE c.active = true", queries.get(2));
        assertEquals("SELECT * FROM test_item c WHERE c.active != true", queries.get(3));
        assertEquals("SELECT * FROM test_item c WHERE c.active != false", queries.get(4));
        assertEquals("SELECT * FROM test_item c WHERE (c.active = true) AND (c.id = @p0)", queries.get(5));
    }

    /**
     * Regression: Cosmos DB literals ({@code true}, {@code false}, {@code null}, {@code undefined}) are case-sensitive
     * and must be lower case, but SCREAMING_SNAKE_CASE upper-cases every token of a raw expression. A keyword-named
     * property is still alias-qualified and keeps its spelling.
     */
    @Test
    public void testLiteralKeywordsAreLowerCasedForCosmos() {
        final CosmosContainerExecutor screamingExecutor = new CosmosContainerExecutor(mockCosmosContainer, NamingPolicy.SCREAMING_SNAKE_CASE);

        final List<String> queries = captureConditionQueries(screamingExecutor, Filters.expr("active = true AND deleted = false"),
                Filters.expr("score = null OR score = undefined"), Filters.isTrue("active"), Filters.isNull("score"), Filters.eq("null", 1));

        assertEquals("SELECT * FROM TEST_ITEM c WHERE c.ACTIVE = true AND c.DELETED = false", queries.get(0));
        assertEquals("SELECT * FROM TEST_ITEM c WHERE c.SCORE = null OR c.SCORE = undefined", queries.get(1));
        assertEquals("SELECT * FROM TEST_ITEM c WHERE c.ACTIVE = true", queries.get(2));
        assertEquals("SELECT * FROM TEST_ITEM c WHERE IS_NULL(c.SCORE)", queries.get(3));
        assertEquals("SELECT * FROM TEST_ITEM c WHERE c.NULL = @p0", queries.get(4));
    }

    /** Regression: a Cosmos user-defined-function call ({@code udf.name(...)}) must not be alias-qualified as a property path. */
    @Test
    public void testUdfCallsAreNotAliasQualified() {
        final List<String> queries = captureConditionQueries(executor, Filters.expr("udf.tax(price) > 1"), Filters.expr("udf.x IS NULL"));

        assertEquals("SELECT * FROM test_item c WHERE udf.tax(c.price) > 1", queries.get(0));
        // Without a call, "udf" is an ordinary property path.
        assertEquals("SELECT * FROM test_item c WHERE IS_NULL(c.udf.x)", queries.get(1));
    }

    @Test
    public void testStreamItemsWithSetRangeAndPatternConditionsUseNumberedCosmosParameters() {
        when(mockPagedIterable.stream()).thenAnswer(invocation -> java.util.stream.Stream.empty());
        final org.mockito.ArgumentCaptor<SqlQuerySpec> specCaptor = org.mockito.ArgumentCaptor.forClass(SqlQuerySpec.class);
        when(mockCosmosContainer.queryItems(specCaptor.capture(), any(), eq(TestItem.class))).thenReturn(mockPagedIterable);

        executor.streamItems(Filters.or(Filters.in("name", Arrays.asList("a", "b")), Filters.between("id", "1", "5")), TestItem.class).toList();
        executor.streamItems(Filters.and(Filters.notIn("name", Arrays.asList("a", "b")), Filters.notLike("id", "x%")), TestItem.class).toList();
        executor.streamItems(Filters.not(Filters.like("name", "a%")), TestItem.class).toList();
        executor.streamItems(Arrays.asList("id"), Filters.and(Filters.isNull("name"), Filters.ne("id", "1")), TestItem.class).toList();

        final List<SqlQuerySpec> specs = specCaptor.getAllValues();
        assertEquals(4, specs.size());

        // Every positional placeholder is renamed to @p<index> in order, including those inside IN (...) and BETWEEN ... AND ...
        assertEquals("SELECT * FROM test_item c WHERE (c.name IN (@p0, @p1)) OR (c.id BETWEEN @p2 AND @p3)", specs.get(0).getQueryText());
        assertEquals(Arrays.asList("@p0", "@p1", "@p2", "@p3"), specs.get(0).getParameters().stream().map(p -> p.getName()).toList());
        assertEquals(Arrays.asList("a", "b", "1", "5"), specs.get(0).getParameters().stream().map(p -> p.getValue(String.class)).toList());

        assertEquals("SELECT * FROM test_item c WHERE (c.name NOT IN (@p0, @p1)) AND (c.id NOT LIKE @p2)", specs.get(1).getQueryText());
        assertEquals(Arrays.asList("a", "b", "x%"), specs.get(1).getParameters().stream().map(p -> p.getValue(String.class)).toList());

        assertEquals("SELECT * FROM test_item c WHERE NOT (c.name LIKE @p0)", specs.get(2).getQueryText());
        assertEquals("a%", specs.get(2).getParameters().get(0).getValue(String.class));

        // A projection keeps the Cosmos VALUE form while the WHERE clause is still rewritten and parameterised.
        assertEquals("SELECT VALUE { \"id\": c.id } FROM test_item c WHERE (IS_NULL(c.name)) AND (c.id != @p0)", specs.get(3).getQueryText());
        assertEquals(1, specs.get(3).getParameters().size());
        assertEquals("1", specs.get(3).getParameters().get(0).getValue(String.class));
    }

    /**
     * Pins the documented {@code @throws IllegalArgumentException} for a blank or comment-only raw expression (abacus-query 4.9.4 rejects
     * comment-only expressions as blank predicates): the call fails eagerly, before any Cosmos request is issued.
     */
    @Test
    public void testBlankOrCommentOnlyExpressionConditionIsRejectedBeforeQuerying() {
        assertThrows(IllegalArgumentException.class, () -> executor.streamItems(Filters.expr("  "), TestItem.class));
        assertThrows(IllegalArgumentException.class, () -> executor.streamItems(Filters.expr("/* only a comment */"), TestItem.class));
        assertThrows(IllegalArgumentException.class, () -> executor.streamItems(List.of("id"), Filters.expr("/* only a comment */"), TestItem.class));
        verifyNoInteractions(mockCosmosContainer);
    }

    // ---- 2026-10-02 sliceT ----

    /**
     * Regression: the Cosmos coalesce operator {@code ??} in a raw expression was counted as two positional placeholders, so combining it
     * with any parameterized condition failed with "Query parameter count mismatch: expected 1 placeholders but found 3".
     */
    @Test
    public void testCoalesceOperatorIsNotCountedAsPlaceholders_sliceT() throws Exception {
        when(mockPagedIterable.stream()).thenAnswer(invocation -> java.util.stream.Stream.empty());
        final org.mockito.ArgumentCaptor<SqlQuerySpec> specCaptor = org.mockito.ArgumentCaptor.forClass(SqlQuerySpec.class);
        when(mockCosmosContainer.queryItems(specCaptor.capture(), any(), eq(TestItem.class))).thenReturn(mockPagedIterable);

        executor.streamItems(Filters.and(Filters.eq("id", "1"), Filters.expr("(score ?? 0) > 1")), TestItem.class).toList();
        executor.streamItems(Filters.and(Filters.expr("(score ?? 0) > 1"), Filters.in("name", Arrays.asList("a", "b"))), TestItem.class).toList();

        final List<SqlQuerySpec> specs = specCaptor.getAllValues();
        assertEquals("SELECT * FROM test_item c WHERE (c.id = @p0) AND ((c.score ?? 0) > 1)", specs.get(0).getQueryText());
        assertEquals("1", specs.get(0).getParameters().get(0).getValue(String.class));
        assertEquals("SELECT * FROM test_item c WHERE ((c.score ?? 0) > 1) AND (c.name IN (@p0, @p1))", specs.get(1).getQueryText());
        assertEquals(Arrays.asList("a", "b"), specs.get(1).getParameters().stream().map(p -> p.getValue(String.class)).toList());

        final Method method = CosmosContainerExecutor.class.getDeclaredMethod("rewritePositionalParameters", String.class, int.class);
        method.setAccessible(true);
        assertEquals("SELECT * FROM c WHERE c.a = @p0 AND (c.b??c.d) > @p1", method.invoke(null, "SELECT * FROM c WHERE c.a = ? AND (c.b??c.d) > ?", 2));
    }

    /** Regression: the {@code ESCAPE} keyword of a raw Cosmos {@code LIKE ... ESCAPE '!'} predicate was alias-qualified as {@code c.escape}. */
    @Test
    public void testLikeEscapeKeywordIsNotAliasQualified_sliceT() {
        final List<String> queries = captureConditionQueries(executor, Filters.expr("name LIKE 'a!%' ESCAPE '!'"),
                Filters.expr("name NOT LIKE 'a!_' escape '!' AND id = '1'"), Filters.eq("escape", 1), Filters.isNull("escape"),
                Filters.expr("IS_DEFINED(escape) AND id = escape"), Filters.expr("id = '1' AND NOT escape"));

        // SNAKE_CASE lower-cases the keyword token; Cosmos DB keywords are case-insensitive.
        assertEquals("SELECT * FROM test_item c WHERE c.name LIKE 'a!%' escape '!'", queries.get(0));
        assertEquals("SELECT * FROM test_item c WHERE c.name NOT LIKE 'a!_' escape '!' AND c.id = '1'", queries.get(1));
        // A property that is merely named "escape" is still qualified.
        assertEquals("SELECT * FROM test_item c WHERE c.escape = @p0", queries.get(2));
        assertEquals("SELECT * FROM test_item c WHERE IS_NULL(c.escape)", queries.get(3));
        assertEquals("SELECT * FROM test_item c WHERE IS_DEFINED(c.escape) AND c.id = c.escape", queries.get(4));
        assertEquals("SELECT * FROM test_item c WHERE c.id = '1' AND NOT c.escape", queries.get(5));
    }

    // ---- 2026-10-02 verifyCN ----

    /**
     * Edge cases of the coalesce rule: adjacent question marks pair left to right ({@code ???} is {@code ??} plus one placeholder,
     * {@code ????} is two coalesce operators), spaced or comma-joined placeholders are still counted, a quoted {@code ??} is ignored,
     * and a lone {@code ??} never satisfies a parameter.
     */
    @Test
    public void testCoalescePairingAndPlaceholderCountingEdgeCases_verifyCN() throws Exception {
        final Method method = CosmosContainerExecutor.class.getDeclaredMethod("rewritePositionalParameters", String.class, int.class);
        method.setAccessible(true);

        assertEquals("(c.t ??@p0) = 1", method.invoke(null, "(c.t ???) = 1", 1));
        assertEquals("(c.t ????) = @p0", method.invoke(null, "(c.t ????) = ?", 1));
        assertEquals("c.a ?? @p0 ?? c.b", method.invoke(null, "c.a ?? ? ?? c.b", 1));
        assertEquals("@p0 @p1", method.invoke(null, "? ?", 2));
        assertEquals("(@p0,@p1)", method.invoke(null, "(?,?)", 2));
        assertEquals("c.n = 'a''??' AND c.d = \"x\\\"??\" AND c.a = @p0", method.invoke(null, "c.n = 'a''??' AND c.d = \"x\\\"??\" AND c.a = ?", 1));

        final InvocationTargetException e = assertThrows(InvocationTargetException.class, () -> method.invoke(null, "c.a = ??", 1));
        assertTrue(e.getCause() instanceof IllegalArgumentException);
        assertEquals("Query parameter count mismatch: expected 1 placeholders but found 0", e.getCause().getMessage());
    }

    /**
     * A raw sub-query whose user-written bindings sit next to a coalesce ({@code ???}) is counted the same way abacus-query counts its
     * bindings, so the parameters line up; a raw {@code ?} that is not a binding (a ternary) is still rejected as a count mismatch.
     */
    @Test
    public void testCoalesceNextToRawSubQueryBindingAndRawQuestionMarkMismatch_verifyCN() {
        when(mockPagedIterable.stream()).thenAnswer(invocation -> java.util.stream.Stream.empty());
        final org.mockito.ArgumentCaptor<SqlQuerySpec> specCaptor = org.mockito.ArgumentCaptor.forClass(SqlQuerySpec.class);
        when(mockCosmosContainer.queryItems(specCaptor.capture(), any(), eq(TestItem.class))).thenReturn(mockPagedIterable);

        executor.streamItems(Filters.and(Filters.eq("id", "1"),
                Filters.in("name", Filters.subQuery("SELECT VALUE t FROM t IN c.tags WHERE (t ???) = 1", Arrays.asList("a")))), TestItem.class)
                .toList();

        final SqlQuerySpec spec = specCaptor.getValue();
        assertTrue(spec.getQueryText().startsWith("SELECT * FROM test_item c WHERE (c.id = @p0) AND "), spec.getQueryText());
        assertTrue(spec.getQueryText().contains("??@p1) = 1"), spec.getQueryText());
        assertEquals(Arrays.asList("@p0", "@p1"), spec.getParameters().stream().map(p -> p.getName()).toList());
        assertEquals(Arrays.asList("1", "a"), spec.getParameters().stream().map(p -> p.getValue(String.class)).toList());

        assertThrows(IllegalArgumentException.class,
                () -> executor.streamItems(Filters.and(Filters.eq("id", "1"), Filters.expr("(score > 0 ? score ?? 0 : 1) > 0")), TestItem.class));
    }

    /** The ESCAPE keyword rule with extra whitespace or a double-quoted escape literal, next to a bound parameter, and under every naming policy. */
    @Test
    public void testLikeEscapeKeywordSpacingAndNamingPolicies_verifyCN() {
        final List<String> queries = captureConditionQueries(executor, Filters.expr("name LIKE 'a!%' ESCAPE    '!'"),
                Filters.expr("name LIKE 'a!%' ESCAPE\"!\""), Filters.and(Filters.eq("id", "1"), Filters.expr("name LIKE '%!_x' ESCAPE '!'")));

        assertEquals("SELECT * FROM test_item c WHERE c.name LIKE 'a!%' escape '!'", queries.get(0));
        assertEquals("SELECT * FROM test_item c WHERE c.name LIKE 'a!%' escape\"!\"", queries.get(1));
        assertEquals("SELECT * FROM test_item c WHERE (c.id = @p0) AND (c.name LIKE '%!_x' escape '!')", queries.get(2));

        final List<String> screaming = captureConditionQueries(new CosmosContainerExecutor(mockCosmosContainer, NamingPolicy.SCREAMING_SNAKE_CASE),
                Filters.expr("name LIKE 'a!%' ESCAPE '!'"), Filters.expr("name like 'a!%' escape '!'"), Filters.eq("escape", 1));

        assertEquals("SELECT * FROM TEST_ITEM c WHERE c.NAME LIKE 'a!%' ESCAPE '!'", screaming.get(0));
        assertEquals("SELECT * FROM TEST_ITEM c WHERE c.NAME LIKE 'a!%' ESCAPE '!'", screaming.get(1));
        assertEquals("SELECT * FROM TEST_ITEM c WHERE c.ESCAPE = @p0", screaming.get(2));

        final List<String> camel = captureConditionQueries(new CosmosContainerExecutor(mockCosmosContainer, NamingPolicy.CAMEL_CASE),
                Filters.expr("name LIKE 'a!%' ESCAPE '!'"), Filters.expr("escape = 'x'"));

        assertEquals("SELECT * FROM testItem c WHERE c.name LIKE 'a!%' escape '!'", camel.get(0));
        assertEquals("SELECT * FROM testItem c WHERE c.escape = 'x'", camel.get(1));
    }

    // ---- 2026-10-04 coverageDC ----

    /**
     * Every ESCAPE shape compared as one list, so each is proven on its own (the sliceT/verifyCN tests stop at their first assertion
     * on HEAD, which qualified every keyword below as {@code c.escape}): single- and double-quoted escape literal, mixed keyword case,
     * NOT LIKE, two LIKE ... ESCAPE predicates in one expression, and next to a bound parameter. The last two entries pin that a
     * property named {@code escape} that is not followed by a string literal is still qualified.
     */
    @Test
    public void testLikeEscapeKeywordEveryShapeIsNotAliasQualified_coverageDC() {
        final List<String> queries = captureConditionQueries(executor, Filters.expr("name LIKE 'a!%' ESCAPE '!'"),
                Filters.expr("name LIKE 'a!%' ESCAPE \"!\""), Filters.expr("name LIKE 'a!%' Escape '!'"), Filters.expr("name NOT LIKE 'a!_' escape '!'"),
                Filters.expr("(name LIKE 'a!%' ESCAPE '!') OR (id LIKE 'b!_%' ESCAPE '!')"),
                Filters.and(Filters.eq("id", "1"), Filters.expr("name LIKE 'a!%' ESCAPE '!'")), Filters.expr("name = escape"),
                Filters.expr("escape LIKE 'a%'"));

        assertEquals(List.of("SELECT * FROM test_item c WHERE c.name LIKE 'a!%' escape '!'", //
                "SELECT * FROM test_item c WHERE c.name LIKE 'a!%' escape \"!\"", //
                "SELECT * FROM test_item c WHERE c.name LIKE 'a!%' escape '!'", //
                "SELECT * FROM test_item c WHERE c.name NOT LIKE 'a!_' escape '!'", //
                "SELECT * FROM test_item c WHERE (c.name LIKE 'a!%' escape '!') OR (c.id LIKE 'b!_%' escape '!')", //
                "SELECT * FROM test_item c WHERE (c.id = @p0) AND (c.name LIKE 'a!%' escape '!')", //
                "SELECT * FROM test_item c WHERE c.name = c.escape", //
                "SELECT * FROM test_item c WHERE c.escape LIKE 'a%'"), queries);
    }

    /**
     * Every coalesce shape that is combined with a bound parameter, rendered one by one so that each is proven on its own (HEAD
     * counted each {@code ??} as two placeholders and rejected every one of them with "Query parameter count mismatch"): a coalesce
     * before or after the bound condition, a chained coalesce, no spaces around {@code ??}, and several bound values. The last entry
     * pins that a coalesce without any bound parameter was, and still is, passed through unchanged.
     */
    @Test
    public void testCoalesceWithBoundParametersEveryShape_coverageDC() {
        final List<String> rendered = new java.util.ArrayList<>();

        for (final com.landawn.abacus.query.condition.Condition condition : List.of(
                Filters.and(Filters.eq("id", "1"), Filters.expr("(score ?? 0) > 1")),
                Filters.and(Filters.expr("(score ?? 0) > 1"), Filters.eq("id", "1")),
                Filters.and(Filters.eq("id", "1"), Filters.expr("(score ?? bonus ?? 0) > 1")),
                Filters.and(Filters.eq("id", "1"), Filters.expr("(score??0) > 1")),
                Filters.and(Filters.between("id", "a", "b"), Filters.expr("(name ?? '') != ''"), Filters.in("name", Arrays.asList("x", "y"))),
                Filters.expr("(score ?? 0) > 1"))) {
            rendered.add(renderQueryOrFailure_coverageDC(condition));
        }

        assertEquals(List.of("SELECT * FROM test_item c WHERE (c.id = @p0) AND ((c.score ?? 0) > 1) [1]", //
                "SELECT * FROM test_item c WHERE ((c.score ?? 0) > 1) AND (c.id = @p0) [1]", //
                "SELECT * FROM test_item c WHERE (c.id = @p0) AND ((c.score ?? c.bonus ?? 0) > 1) [1]", //
                "SELECT * FROM test_item c WHERE (c.id = @p0) AND ((c.score??0) > 1) [1]", //
                "SELECT * FROM test_item c WHERE (c.id BETWEEN @p0 AND @p1) AND ((c.name ?? '') != '') AND (c.name IN (@p2, @p3)) [a, b, x, y]", //
                "SELECT * FROM test_item c WHERE (c.score ?? 0) > 1 []"), rendered);
    }

    private String renderQueryOrFailure_coverageDC(final com.landawn.abacus.query.condition.Condition condition) {
        when(mockPagedIterable.stream()).thenAnswer(invocation -> java.util.stream.Stream.empty());
        final org.mockito.ArgumentCaptor<SqlQuerySpec> specCaptor = org.mockito.ArgumentCaptor.forClass(SqlQuerySpec.class);
        when(mockCosmosContainer.queryItems(specCaptor.capture(), any(), eq(TestItem.class))).thenReturn(mockPagedIterable);

        try {
            executor.streamItems(condition, TestItem.class).toList();
        } catch (final IllegalArgumentException e) {
            return "IllegalArgumentException: " + e.getMessage();
        }

        final SqlQuerySpec spec = specCaptor.getValue();
        return spec.getQueryText() + " " + spec.getParameters().stream().map(p -> p.getValue(Object.class)).toList();
    }

    /** Regression: a keyword-named Boolean property also needs its alias after a logical operator. */
    @Test
    @Tag("base-test")
    public void testKeywordBooleanPropertiesAfterLogicalOperators() {
        final List<String> queries = captureConditionQueries(executor, Filters.expr("active AND order"), Filters.expr("active OR group"),
                Filters.expr("NOT having"));

        assertEquals("SELECT * FROM test_item c WHERE c.active AND c.order", queries.get(0));
        assertEquals("SELECT * FROM test_item c WHERE c.active OR c.group", queries.get(1));
        assertEquals("SELECT * FROM test_item c WHERE NOT c.having", queries.get(2));
    }

    /** Logical token recognition is case-insensitive and applies at each operand in a compound expression. */
    @Test
    @Tag("base-test")
    public void testKeywordBooleanPropertiesInMixedCaseAndNestedExpressions() {
        final List<String> queries = captureConditionQueries(executor, Filters.expr("active And order"), Filters.expr("Not group"),
                Filters.expr("order AND group OR NOT having"), Filters.expr("active AND NOT (order OR group)"));

        assertEquals("SELECT * FROM test_item c WHERE c.active and c.order", queries.get(0));
        assertEquals("SELECT * FROM test_item c WHERE not c.group", queries.get(1));
        assertEquals("SELECT * FROM test_item c WHERE c.order AND c.group OR NOT c.having", queries.get(2));
        assertEquals("SELECT * FROM test_item c WHERE c.active AND NOT (c.order OR c.group)", queries.get(3));
    }

    /** Property spelling follows the selected naming policy while logical operators retain their role. */
    @Test
    @Tag("base-test")
    public void testKeywordBooleanPropertiesUnderEveryNamingPolicy() {
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final CosmosContainerExecutor current = new CosmosContainerExecutor(mockCosmosContainer, policy);
            final List<String> queries = captureConditionQueries(current, Filters.expr("active AND order OR NOT group"));
            final String table = policy == NamingPolicy.SCREAMING_SNAKE_CASE ? "TEST_ITEM" : policy == NamingPolicy.CAMEL_CASE ? "testItem" : "test_item";
            final String predicate = policy == NamingPolicy.SCREAMING_SNAKE_CASE ? "c.ACTIVE AND c.ORDER OR NOT c.GROUP"
                    : "c.active AND c.order OR NOT c.group";

            assertEquals("SELECT * FROM " + table + " c WHERE " + predicate, queries.get(0), policy.toString());
        }
    }

    /** Adding logical operand positions must preserve literals, real operators, existing aliases, and quoted text. */
    @Test
    @Tag("base-test")
    public void testLogicalOperandQualificationPreservesOtherExpressionTokens() {
        final List<String> queries = captureConditionQueries(executor, Filters.expr("active AND true OR NOT false"),
                Filters.expr("active AND null OR undefined"), Filters.expr("order NOT IN (1, 2) AND group IS NOT NULL"),
                Filters.expr("active AND c.order"), Filters.expr("name = 'AND order OR group NOT having' AND active"));

        assertEquals("SELECT * FROM test_item c WHERE c.active AND true OR NOT false", queries.get(0));
        assertEquals("SELECT * FROM test_item c WHERE c.active AND null OR undefined", queries.get(1));
        assertEquals("SELECT * FROM test_item c WHERE c.order NOT IN (1, 2) AND NOT IS_NULL(c.group)", queries.get(2));
        assertEquals("SELECT * FROM test_item c WHERE c.active AND c.order", queries.get(3));
        assertEquals("SELECT * FROM test_item c WHERE c.name = 'AND order OR group NOT having' AND c.active", queries.get(4));
    }

    /** Regression: the coalesce operator creates operand positions on both sides without changing literal semantics. */
    @Test
    @Tag("base-test")
    public void testKeywordPropertiesAroundCoalesceOperators() {
        final List<String> queries = captureConditionQueries(executor, Filters.expr("order ?? false"), Filters.expr("active ?? group"),
                Filters.expr("(order ?? group) ?? having"), Filters.expr("active AND (order ?? false) OR (group ?? having)"),
                Filters.expr("null ?? undefined ?? true ?? false"));

        assertEquals("SELECT * FROM test_item c WHERE c.order ?? false", queries.get(0));
        assertEquals("SELECT * FROM test_item c WHERE c.active ?? c.group", queries.get(1));
        assertEquals("SELECT * FROM test_item c WHERE (c.order ?? c.group) ?? c.having", queries.get(2));
        assertEquals("SELECT * FROM test_item c WHERE c.active AND (c.order ?? false) OR (c.group ?? c.having)", queries.get(3));
        assertEquals("SELECT * FROM test_item c WHERE null ?? undefined ?? true ?? false", queries.get(4));
    }

    /** Regression: BETWEEN accepts property expressions for both bounds, including keyword-named properties. */
    @Test
    @Tag("base-test")
    public void testKeywordPropertiesAsBetweenBounds() {
        final List<String> queries = captureConditionQueries(executor, Filters.expr("score BETWEEN order AND group"),
                Filters.expr("score NOT BETWEEN order AND group"), Filters.expr("score Between order And group"),
                Filters.expr("score BETWEEN 1 AND 10"));

        assertEquals("SELECT * FROM test_item c WHERE c.score BETWEEN c.order AND c.group", queries.get(0));
        assertEquals("SELECT * FROM test_item c WHERE c.score NOT BETWEEN c.order AND c.group", queries.get(1));
        assertEquals("SELECT * FROM test_item c WHERE c.score between c.order and c.group", queries.get(2));
        assertEquals("SELECT * FROM test_item c WHERE c.score BETWEEN 1 AND 10", queries.get(3));
    }

    @Test
    public void testRejectedConditionReleasesInternalSqlBuilder() throws Exception {
        final com.landawn.abacus.query.condition.Condition invalidCondition = Filters.expr("/* only a comment */");
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final CosmosContainerExecutor current = new CosmosContainerExecutor(mockCosmosContainer, policy);
            for (final Collection<String> properties : Arrays.asList(null, List.of("id"))) {
                assertRejectedWithoutBuilderLeak(() -> current.streamItems(properties, invalidCondition, TestItem.class));
            }
        }
        verifyNoInteractions(mockCosmosContainer);
    }

    @Test
    public void testRejectedProjectionReleasesInternalSqlBuilder() throws Exception {
        for (final NamingPolicy policy : List.of(NamingPolicy.SNAKE_CASE, NamingPolicy.SCREAMING_SNAKE_CASE, NamingPolicy.CAMEL_CASE)) {
            final CosmosContainerExecutor current = new CosmosContainerExecutor(mockCosmosContainer, policy);
            assertRejectedWithoutBuilderLeak(() -> current.streamItems(List.of("id /* invalid column */"), null, TestItem.class));
        }
        verifyNoInteractions(mockCosmosContainer);
    }

    private static void assertRejectedWithoutBuilderLeak(final Runnable action) throws Exception {
        final java.lang.reflect.Field field = com.landawn.abacus.query.AbstractQueryBuilder.class.getDeclaredField("activeStringBuilderCounter");
        field.setAccessible(true);
        final java.util.concurrent.atomic.AtomicInteger counter = (java.util.concurrent.atomic.AtomicInteger) field.get(null);
        final int before = counter.get();

        assertThrows(IllegalArgumentException.class, action::run);
        assertEquals(before, counter.get(), "A rejected query must release the executor-owned builder");
    }

    // Test data class
    public static class TestItem {
        public String id;
        public String name;

        public TestItem() {
        }

        public TestItem(String id, String name) {
            this.id = id;
            this.name = name;
        }
    }
}
