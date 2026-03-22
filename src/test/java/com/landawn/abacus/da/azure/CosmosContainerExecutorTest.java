package com.landawn.abacus.da.azure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.azure.cosmos.CosmosContainer;
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
        when(mockCosmosContainer.queryItems(any(String.class), any(), eq(TestItem.class))).thenReturn(mockPagedIterable);

        Stream<TestItem> stream = executorWithPolicy.streamItems(Filters.eq("id", "1"), TestItem.class);

        assertNotNull(stream);
        List<TestItem> result = stream.toList();
        assertEquals(2, result.size());
    }

    @Test
    public void testStreamItemsWithConditionAndOptions() {
        List<TestItem> items = Arrays.asList(new TestItem("1", "Item1"), new TestItem("2", "Item2"));
        when(mockPagedIterable.stream()).thenReturn(items.stream());
        when(mockCosmosContainer.queryItems(any(String.class), eq(queryRequestOptions), eq(TestItem.class))).thenReturn(mockPagedIterable);

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
        when(mockCosmosContainer.queryItems(any(String.class), any(), eq(TestItem.class))).thenReturn(mockPagedIterable);

        Stream<TestItem> stream = executor.streamItems(selectProps, Filters.eq("id", "1"), TestItem.class);

        assertNotNull(stream);
        List<TestItem> result = stream.toList();
        assertEquals(2, result.size());
    }

    @Test
    public void testStreamItemsWithSelectConditionAndOptions() {
        Collection<String> selectProps = Arrays.asList("id", "name");
        List<TestItem> items = Arrays.asList(new TestItem("1", "Item1"), new TestItem("2", "Item2"));
        when(mockPagedIterable.stream()).thenReturn(items.stream());
        when(mockCosmosContainer.queryItems(any(String.class), eq(queryRequestOptions), eq(TestItem.class))).thenReturn(mockPagedIterable);

        Stream<TestItem> stream = executor.streamItems(selectProps, Filters.eq("id", "1"), queryRequestOptions, TestItem.class);

        assertNotNull(stream);
        List<TestItem> result = stream.toList();
        assertEquals(2, result.size());
    }

    @Test
    public void testStreamItemsWithNullCondition() {
        List<TestItem> items = Arrays.asList(new TestItem("1", "Item1"), new TestItem("2", "Item2"));
        when(mockPagedIterable.stream()).thenReturn(items.stream());
        when(mockCosmosContainer.queryItems(any(String.class), any(), eq(TestItem.class))).thenReturn(mockPagedIterable);

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
        when(mockCosmosContainer.queryItems(any(String.class), any(), eq(TestItem.class))).thenReturn(mockPagedIterable);

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

    @Test
    public void testUnsupportedNamingPolicy() {
        // Create a mock naming policy that's not supported
        NamingPolicy unsupportedPolicy = NamingPolicy.NO_CHANGE;
        CosmosContainerExecutor executorWithUnsupportedPolicy = new CosmosContainerExecutor(mockCosmosContainer, unsupportedPolicy);

        List<TestItem> items = Arrays.asList(new TestItem("1", "Item1"));
        when(mockPagedIterable.stream()).thenReturn(items.stream());

        // This should throw RuntimeException for unsupported naming policy
        assertThrows(RuntimeException.class, () -> {
            executorWithUnsupportedPolicy.streamItems(Filters.eq("id", "1"), TestItem.class);
        });
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
