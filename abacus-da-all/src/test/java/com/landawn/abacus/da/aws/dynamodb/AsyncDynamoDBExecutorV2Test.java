package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.da.aws.dynamodb.v2.AsyncDynamoDBExecutor;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.stream.Stream;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public class AsyncDynamoDBExecutorV2Test extends TestBase {

    @Mock
    private DynamoDbAsyncClient mockDynamoDbAsyncClient;

    private AsyncDynamoDBExecutor asyncExecutor;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        asyncExecutor = new AsyncDynamoDBExecutor(mockDynamoDbAsyncClient);
    }

    @Test
    public void testDynamoDBAsyncClient() {
        DynamoDbAsyncClient client = asyncExecutor.dynamoDBAsyncClient();
        assertNotNull(client);
        assertEquals(mockDynamoDbAsyncClient, client);
    }

    @Test
    public void testMapperResultConversionFailuresCompleteFuturesExceptionally() {
        final AsyncDynamoDBExecutor.Mapper<NumericResultEntity> mapper = asyncExecutor.mapper(NumericResultEntity.class, "TestTable", NamingPolicy.CAMEL_CASE);
        final NumericResultEntity key = new NumericResultEntity();
        key.setId("1");
        final Map<String, AttributeValue> invalidItem = Map.of("id", AttributeValue.builder().s("1").build(),
                "count", AttributeValue.builder().s("not-a-number").build());
        when(mockDynamoDbAsyncClient.getItem(any(GetItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(GetItemResponse.builder().item(invalidItem).build()));
        when(mockDynamoDbAsyncClient.batchGetItem(any(BatchGetItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(BatchGetItemResponse.builder().responses(Map.of("TestTable", List.of(invalidItem))).build()));

        // Even an already completed SDK response must report mapper failures through its future.
        final List<CompletableFuture<?>> results = List.of(mapper.getItem(key), mapper.getItem(key, true),
                mapper.batchGetItem(List.of(key)), mapper.batchGetItem(List.of(key), "TOTAL"));
        for (final CompletableFuture<?> result : results) {
            assertTrue(result.isCompletedExceptionally());
            assertTrue(assertThrows(ExecutionException.class, result::get).getCause() instanceof RuntimeException);
        }
    }

    public static class NumericResultEntity {
        private String id;
        private int count;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public int getCount() {
            return count;
        }

        public void setCount(final int count) {
            this.count = count;
        }
    }

    @Test
    public void testMapperWithTargetEntityClass() {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);
        assertNotNull(mapper);

        // Test caching - should return same instance
        AsyncDynamoDBExecutor.Mapper<TestEntity> secondMapper = asyncExecutor.mapper(TestEntity.class);
        assertSame(mapper, secondMapper);
    }

    @Test
    public void testMapperWithTargetEntityClassNoTableAnnotation() {
        assertThrows(IllegalArgumentException.class, () -> {
            asyncExecutor.mapper(NoTableEntity.class);
        });
    }

    @Test
    public void testMapperWithTableNameAndNamingPolicy() {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class, "TestTable", NamingPolicy.CAMEL_CASE);
        assertNotNull(mapper);
    }

    /**
     * Null mapper inputs are caller errors and must fail synchronously with the documented
     * IllegalArgumentException instead of an incidental NullPointerException.
     */
    @Test
    public void testMapperNullArgumentsThrowIllegalArgumentException() {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);

        assertThrows(IllegalArgumentException.class, () -> mapper.getItem((TestEntity) null));
        assertThrows(IllegalArgumentException.class, () -> mapper.batchGetItem((Collection<TestEntity>) null));
        assertThrows(IllegalArgumentException.class, () -> mapper.batchPutItem((Collection<TestEntity>) null));
        assertThrows(IllegalArgumentException.class, () -> mapper.batchPutItem(java.util.Arrays.asList((TestEntity) null)));
        assertThrows(IllegalArgumentException.class, () -> mapper.batchDeleteItem((Collection<TestEntity>) null));

        assertThrows(IllegalArgumentException.class, () -> mapper.getItem((GetItemRequest) null));
        assertThrows(IllegalArgumentException.class, () -> mapper.batchGetItem((BatchGetItemRequest) null));
        assertThrows(IllegalArgumentException.class, () -> mapper.batchWriteItem((BatchWriteItemRequest) null));
        assertThrows(IllegalArgumentException.class, () -> mapper.putItem((PutItemRequest) null));
        assertThrows(IllegalArgumentException.class, () -> mapper.updateItem((UpdateItemRequest) null));
        assertThrows(IllegalArgumentException.class, () -> mapper.deleteItem((DeleteItemRequest) null));
        assertThrows(IllegalArgumentException.class, () -> mapper.list((QueryRequest) null));
        assertThrows(IllegalArgumentException.class, () -> mapper.scan((ScanRequest) null));
    }

    @Test
    public void testGetItemWithTableNameAndKey() throws ExecutionException, InterruptedException {
        String tableName = "TestTable";
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", AttributeValue.builder().s("123").build());

        GetItemResponse response = GetItemResponse.builder()
                .item(Map.of("id", AttributeValue.builder().s("123").build(), "name", AttributeValue.builder().s("Test").build()))
                .build();

        when(mockDynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Map<String, Object>> future = asyncExecutor.getItem(tableName, key);
        Map<String, Object> result = future.get();

        assertNotNull(result);
        assertEquals("123", result.get("id"));
        assertEquals("Test", result.get("name"));
    }

    @Test
    public void testGetItemWithConsistentRead() throws ExecutionException, InterruptedException {
        String tableName = "TestTable";
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", AttributeValue.builder().s("123").build());
        Boolean consistentRead = true;

        GetItemResponse response = GetItemResponse.builder().item(Map.of("id", AttributeValue.builder().s("123").build())).build();

        when(mockDynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Map<String, Object>> future = asyncExecutor.getItem(tableName, key, consistentRead);
        Map<String, Object> result = future.get();

        assertNotNull(result);
        assertEquals("123", result.get("id"));
    }

    @Test
    public void testGetItemWithRequest() throws ExecutionException, InterruptedException {
        GetItemRequest request = GetItemRequest.builder().tableName("TestTable").key(Map.of("id", AttributeValue.builder().s("123").build())).build();

        GetItemResponse response = GetItemResponse.builder().item(Map.of("id", AttributeValue.builder().s("123").build())).build();

        when(mockDynamoDbAsyncClient.getItem(request)).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Map<String, Object>> future = asyncExecutor.getItem(request);
        Map<String, Object> result = future.get();

        assertNotNull(result);
        assertEquals("123", result.get("id"));
    }

    @Test
    public void testGetItemWithTargetClass() throws ExecutionException, InterruptedException {
        String tableName = "TestTable";
        Map<String, AttributeValue> key = Map.of("id", AttributeValue.builder().s("123").build());

        GetItemResponse response = GetItemResponse.builder()
                .item(Map.of("id", AttributeValue.builder().s("123").build(), "name", AttributeValue.builder().s("Test").build()))
                .build();

        when(mockDynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<TestEntity> future = asyncExecutor.getItem(tableName, key, TestEntity.class);
        TestEntity result = future.get();

        assertNotNull(result);
        assertEquals("123", result.getId());
        assertEquals("Test", result.getName());
    }

    @Test
    public void testMapperSupportsCompositePrimaryKeyAndRejectsMoreThanTwoIds() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<CompositeKeyEntity> mapper = asyncExecutor.mapper(CompositeKeyEntity.class);
        CompositeKeyEntity entity = new CompositeKeyEntity();
        entity.setPartitionId("partition-1");
        entity.setSortId("sort-1");

        when(mockDynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(GetItemResponse.builder().build()));

        mapper.getItem(entity).get();

        ArgumentCaptor<GetItemRequest> requestCaptor = ArgumentCaptor.forClass(GetItemRequest.class);
        verify(mockDynamoDbAsyncClient).getItem(requestCaptor.capture());
        assertEquals("partition-1", requestCaptor.getValue().key().get("partitionId").s());
        assertEquals("sort-1", requestCaptor.getValue().key().get("sortId").s());
        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.mapper(ThreeKeyEntity.class));
        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.mapper(CompositeKeyNameCollisionEntity.class, "TestTable", NamingPolicy.SNAKE_CASE));
    }

    @Test
    public void testBatchGetItem() throws ExecutionException, InterruptedException {
        Map<String, KeysAndAttributes> requestItems = new HashMap<>();
        List<Map<String, AttributeValue>> keys = new ArrayList<>();
        keys.add(Map.of("id", AttributeValue.builder().s("1").build()));
        keys.add(Map.of("id", AttributeValue.builder().s("2").build()));

        requestItems.put("TestTable", KeysAndAttributes.builder().keys(keys).build());

        Map<String, List<Map<String, AttributeValue>>> responses = new HashMap<>();
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        items.add(Map.of("id", AttributeValue.builder().s("1").build(), "name", AttributeValue.builder().s("Item1").build()));
        items.add(Map.of("id", AttributeValue.builder().s("2").build(), "name", AttributeValue.builder().s("Item2").build()));
        responses.put("TestTable", items);

        BatchGetItemResponse response = BatchGetItemResponse.builder().responses(responses).build();

        when(mockDynamoDbAsyncClient.batchGetItem(any(BatchGetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Map<String, List<Map<String, Object>>>> future = asyncExecutor.batchGetItem(requestItems);
        Map<String, List<Map<String, Object>>> result = future.get();

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey("TestTable"));
        assertEquals(2, result.get("TestTable").size());
    }

    @Test
    public void testBatchGetItemWithReturnConsumedCapacity() throws ExecutionException, InterruptedException {
        Map<String, KeysAndAttributes> requestItems = new HashMap<>();
        requestItems.put("TestTable", KeysAndAttributes.builder().build());
        String returnConsumedCapacity = "TOTAL";

        BatchGetItemResponse response = BatchGetItemResponse.builder().responses(new HashMap<>()).build();

        when(mockDynamoDbAsyncClient.batchGetItem(any(BatchGetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Map<String, List<Map<String, Object>>>> future = asyncExecutor.batchGetItem(requestItems, returnConsumedCapacity);
        Map<String, List<Map<String, Object>>> result = future.get();

        assertNotNull(result);
    }

    @Test
    public void testPutItem() throws ExecutionException, InterruptedException {
        String tableName = "TestTable";
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("123").build());
        item.put("name", AttributeValue.builder().s("Test").build());

        PutItemResponse response = PutItemResponse.builder().build();

        when(mockDynamoDbAsyncClient.putItem(any(PutItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<PutItemResponse> future = asyncExecutor.putItem(tableName, item);
        PutItemResponse result = future.get();

        assertNotNull(result);
    }

    @Test
    public void testPutItemWithReturnValues() throws ExecutionException, InterruptedException {
        String tableName = "TestTable";
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("123").build());
        String returnValues = "ALL_OLD";

        PutItemResponse response = PutItemResponse.builder().build();

        when(mockDynamoDbAsyncClient.putItem(any(PutItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<PutItemResponse> future = asyncExecutor.putItem(tableName, item, returnValues);
        PutItemResponse result = future.get();

        assertNotNull(result);
    }

    @Test
    public void testBatchWriteItem() throws ExecutionException, InterruptedException {
        Map<String, List<WriteRequest>> requestItems = new HashMap<>();
        List<WriteRequest> writeRequests = new ArrayList<>();
        writeRequests
                .add(WriteRequest.builder().putRequest(PutRequest.builder().item(Map.of("id", AttributeValue.builder().s("123").build())).build()).build());
        requestItems.put("TestTable", writeRequests);

        BatchWriteItemResponse response = BatchWriteItemResponse.builder().build();

        when(mockDynamoDbAsyncClient.batchWriteItem(any(BatchWriteItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<BatchWriteItemResponse> future = asyncExecutor.batchWriteItem(requestItems);
        BatchWriteItemResponse result = future.get();

        assertNotNull(result);
    }

    @Test
    public void testUpdateItem() throws ExecutionException, InterruptedException {
        String tableName = "TestTable";
        Map<String, AttributeValue> key = Map.of("id", AttributeValue.builder().s("123").build());
        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
        attributeUpdates.put("name", AttributeValueUpdate.builder().value(AttributeValue.builder().s("Updated").build()).action(AttributeAction.PUT).build());

        UpdateItemResponse response = UpdateItemResponse.builder().build();

        when(mockDynamoDbAsyncClient.updateItem(any(UpdateItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<UpdateItemResponse> future = asyncExecutor.updateItem(tableName, key, attributeUpdates);
        UpdateItemResponse result = future.get();

        assertNotNull(result);
    }

    @Test
    public void testUpdateItemWithReturnValues() throws ExecutionException, InterruptedException {
        String tableName = "TestTable";
        Map<String, AttributeValue> key = Map.of("id", AttributeValue.builder().s("123").build());
        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
        String returnValues = "ALL_NEW";

        UpdateItemResponse response = UpdateItemResponse.builder().build();

        when(mockDynamoDbAsyncClient.updateItem(any(UpdateItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<UpdateItemResponse> future = asyncExecutor.updateItem(tableName, key, attributeUpdates, returnValues);
        UpdateItemResponse result = future.get();

        assertNotNull(result);
    }

    @Test
    public void testDeleteItem() throws ExecutionException, InterruptedException {
        String tableName = "TestTable";
        Map<String, AttributeValue> key = Map.of("id", AttributeValue.builder().s("123").build());

        DeleteItemResponse response = DeleteItemResponse.builder().build();

        when(mockDynamoDbAsyncClient.deleteItem(any(DeleteItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<DeleteItemResponse> future = asyncExecutor.deleteItem(tableName, key);
        DeleteItemResponse result = future.get();

        assertNotNull(result);
    }

    @Test
    public void testDeleteItemWithReturnValues() throws ExecutionException, InterruptedException {
        String tableName = "TestTable";
        Map<String, AttributeValue> key = Map.of("id", AttributeValue.builder().s("123").build());
        String returnValues = "ALL_OLD";

        DeleteItemResponse response = DeleteItemResponse.builder().build();

        when(mockDynamoDbAsyncClient.deleteItem(any(DeleteItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<DeleteItemResponse> future = asyncExecutor.deleteItem(tableName, key, returnValues);
        DeleteItemResponse result = future.get();

        assertNotNull(result);
    }

    @Test
    public void testList() throws ExecutionException, InterruptedException {
        QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();

        QueryResponse response = QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("123").build()))).build();

        when(mockDynamoDbAsyncClient.query(any(QueryRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<List<Map<String, Object>>> future = asyncExecutor.list(queryRequest);
        List<Map<String, Object>> result = future.get();

        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testListWithTargetClass() throws ExecutionException, InterruptedException {
        QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();

        QueryResponse response = QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("123").build()))).build();

        when(mockDynamoDbAsyncClient.query(any(QueryRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<List<TestEntity>> future = asyncExecutor.list(queryRequest, TestEntity.class);
        List<TestEntity> result = future.get();

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("123", result.get(0).getId());
    }

    @Test
    public void testQuery() throws ExecutionException, InterruptedException {
        QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();

        QueryResponse response = QueryResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("123").build(), "name", AttributeValue.builder().s("Test").build())))
                .build();

        when(mockDynamoDbAsyncClient.query(any(QueryRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Dataset> future = asyncExecutor.query(queryRequest);
        Dataset result = future.get();

        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testStream() throws ExecutionException, InterruptedException {
        QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();

        QueryResponse response = QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("123").build()))).build();

        when(mockDynamoDbAsyncClient.query(any(QueryRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Stream<Map<String, Object>>> future = asyncExecutor.stream(queryRequest);
        Stream<Map<String, Object>> stream = future.get();

        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testScanWithTableNameAndAttributesToGet() throws ExecutionException, InterruptedException {
        String tableName = "TestTable";
        List<String> attributesToGet = List.of("id", "name");

        ScanResponse response = ScanResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("123").build()))).build();

        when(mockDynamoDbAsyncClient.scan(any(ScanRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Stream<Map<String, Object>>> future = asyncExecutor.scan(tableName, attributesToGet);
        Stream<Map<String, Object>> stream = future.get();

        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testScanWithTableNameAndScanFilter() throws ExecutionException, InterruptedException {
        String tableName = "TestTable";
        Map<String, Condition> scanFilter = new HashMap<>();
        scanFilter.put("status",
                Condition.builder().comparisonOperator(ComparisonOperator.EQ).attributeValueList(AttributeValue.builder().s("active").build()).build());

        ScanResponse response = ScanResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("123").build()))).build();

        when(mockDynamoDbAsyncClient.scan(any(ScanRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Stream<Map<String, Object>>> future = asyncExecutor.scan(tableName, scanFilter);
        Stream<Map<String, Object>> stream = future.get();

        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testScanWithAllParameters() throws ExecutionException, InterruptedException {
        String tableName = "TestTable";
        List<String> attributesToGet = List.of("id", "name");
        Map<String, Condition> scanFilter = new HashMap<>();

        ScanResponse response = ScanResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("123").build()))).build();

        when(mockDynamoDbAsyncClient.scan(any(ScanRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Stream<Map<String, Object>>> future = asyncExecutor.scan(tableName, attributesToGet, scanFilter);
        Stream<Map<String, Object>> stream = future.get();

        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testScanWithRequest() throws ExecutionException, InterruptedException {
        ScanRequest scanRequest = ScanRequest.builder().tableName("TestTable").build();

        ScanResponse response = ScanResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("123").build()))).build();

        when(mockDynamoDbAsyncClient.scan(scanRequest)).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Stream<Map<String, Object>>> future = asyncExecutor.scan(scanRequest);
        Stream<Map<String, Object>> stream = future.get();

        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testClose() {
        // Test that close method doesn't throw exception
        asyncExecutor.close();
        verify(mockDynamoDbAsyncClient, times(1)).close();
    }

    // Test for Mapper inner class
    @Test
    public void testMapperGetItem() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);

        TestEntity entity = new TestEntity();
        entity.setId("123");

        GetItemResponse response = GetItemResponse.builder()
                .item(Map.of("id", AttributeValue.builder().s("123").build(), "name", AttributeValue.builder().s("Test").build()))
                .build();

        when(mockDynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<TestEntity> future = mapper.getItem(entity);
        TestEntity result = future.get();

        assertNotNull(result);
        assertEquals("123", result.getId());
        assertEquals("Test", result.getName());
    }

    @Test
    public void testMapperPutItem() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);

        TestEntity entity = new TestEntity();
        entity.setId("123");
        entity.setName("Test");

        PutItemResponse response = PutItemResponse.builder().build();

        when(mockDynamoDbAsyncClient.putItem(any(PutItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<PutItemResponse> future = mapper.putItem(entity);
        PutItemResponse result = future.get();

        assertNotNull(result);
    }

    @Test
    public void testMapperUpdateItem() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);

        TestEntity entity = new TestEntity();
        entity.setId("123");
        entity.setName("Updated");

        UpdateItemResponse response = UpdateItemResponse.builder().build();

        when(mockDynamoDbAsyncClient.updateItem(any(UpdateItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<UpdateItemResponse> future = mapper.updateItem(entity);
        UpdateItemResponse result = future.get();

        assertNotNull(result);

        ArgumentCaptor<UpdateItemRequest> requestCaptor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(mockDynamoDbAsyncClient).updateItem(requestCaptor.capture());

        UpdateItemRequest request = requestCaptor.getValue();
        assertTrue(request.key().containsKey("id"));
        assertTrue(!request.attributeUpdates().containsKey("id"));
        assertTrue(request.attributeUpdates().containsKey("name"));
    }

    @Test
    public void testMapperDeleteItem() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);

        TestEntity entity = new TestEntity();
        entity.setId("123");

        DeleteItemResponse response = DeleteItemResponse.builder().build();

        when(mockDynamoDbAsyncClient.deleteItem(any(DeleteItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<DeleteItemResponse> future = mapper.deleteItem(entity);
        DeleteItemResponse result = future.get();

        assertNotNull(result);
    }

    @Test
    public void testMapperBatchGetItem() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);

        List<TestEntity> entities = new ArrayList<>();
        TestEntity entity1 = new TestEntity();
        entity1.setId("1");
        entities.add(entity1);
        TestEntity entity2 = new TestEntity();
        entity2.setId("2");
        entities.add(entity2);

        Map<String, List<Map<String, AttributeValue>>> responses = new HashMap<>();
        responses.put("TestTable", List.of(Map.of("id", AttributeValue.builder().s("1").build()), Map.of("id", AttributeValue.builder().s("2").build())));

        BatchGetItemResponse response = BatchGetItemResponse.builder().responses(responses).build();

        when(mockDynamoDbAsyncClient.batchGetItem(any(BatchGetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<List<TestEntity>> future = mapper.batchGetItem(entities);
        List<TestEntity> result = future.get();

        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    public void testMapperBatchPutItem() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);

        List<TestEntity> entities = new ArrayList<>();
        TestEntity entity = new TestEntity();
        entity.setId("123");
        entities.add(entity);

        BatchWriteItemResponse response = BatchWriteItemResponse.builder().build();

        when(mockDynamoDbAsyncClient.batchWriteItem(any(BatchWriteItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<BatchWriteItemResponse> future = mapper.batchPutItem(entities);
        BatchWriteItemResponse result = future.get();

        assertNotNull(result);
    }

    @Test
    public void testMapperBatchDeleteItem() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);

        List<TestEntity> entities = new ArrayList<>();
        TestEntity entity = new TestEntity();
        entity.setId("123");
        entities.add(entity);

        BatchWriteItemResponse response = BatchWriteItemResponse.builder().build();

        when(mockDynamoDbAsyncClient.batchWriteItem(any(BatchWriteItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<BatchWriteItemResponse> future = mapper.batchDeleteItem(entities);
        BatchWriteItemResponse result = future.get();

        assertNotNull(result);
    }

    @Test
    public void testMapperList() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);

        QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();

        QueryResponse response = QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("123").build()))).build();

        when(mockDynamoDbAsyncClient.query(any(QueryRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<List<TestEntity>> future = mapper.list(queryRequest);
        List<TestEntity> result = future.get();

        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testMapperQuery() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);

        QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();

        QueryResponse response = QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("123").build()))).build();

        when(mockDynamoDbAsyncClient.query(any(QueryRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Dataset> future = mapper.query(queryRequest);
        Dataset result = future.get();

        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testMapperStream() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);

        QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();

        QueryResponse response = QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("123").build()))).build();

        when(mockDynamoDbAsyncClient.query(any(QueryRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Stream<TestEntity>> future = mapper.stream(queryRequest);
        Stream<TestEntity> stream = future.get();

        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testMapperScan() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);

        ScanRequest scanRequest = ScanRequest.builder().tableName("TestTable").build();

        ScanResponse response = ScanResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("123").build()))).build();

        when(mockDynamoDbAsyncClient.scan(any(ScanRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Stream<TestEntity>> future = mapper.scan(scanRequest);
        Stream<TestEntity> stream = future.get();

        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    /**
     * Regression test: when an async Query result is paginated (non-empty LastEvaluatedKey) the
     * {@code query(QueryRequest, Class)} Map branch must aggregate subsequent pages. AWS SDK v2's
     * {@code QueryResponse.items()} returns an immutable list, so the previous implementation threw
     * {@link UnsupportedOperationException} (wrapped in the future) when calling {@code addAll}.
     * The fix copies the items into a mutable list before aggregating additional pages.
     */
    @Test
    public void testQueryWithPaginationDoesNotThrowOnImmutableItems() throws ExecutionException, InterruptedException {
        QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();

        QueryResponse page1 = QueryResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("1").build())))
                .lastEvaluatedKey(Map.of("id", AttributeValue.builder().s("1").build()))
                .build();

        QueryResponse page2 = QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("2").build()))).build();

        when(mockDynamoDbAsyncClient.query(any(QueryRequest.class))).thenReturn(CompletableFuture.completedFuture(page1),
                CompletableFuture.completedFuture(page2));

        CompletableFuture<Dataset> future = asyncExecutor.query(queryRequest, Map.class);
        Dataset result = future.get();

        assertNotNull(result);
        assertEquals(2, result.size());
    }

    /**
     * Regression test mirroring {@link #testQueryWithPaginationDoesNotThrowOnImmutableItems()} for
     * the untyped async {@code query(QueryRequest)} entry point.
     */
    @Test
    public void testQueryUntypedWithPaginationDoesNotThrowOnImmutableItems() throws ExecutionException, InterruptedException {
        QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();

        QueryResponse page1 = QueryResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("a").build())))
                .lastEvaluatedKey(Map.of("id", AttributeValue.builder().s("a").build()))
                .build();

        QueryResponse page2 = QueryResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("b").build()), Map.of("id", AttributeValue.builder().s("c").build())))
                .build();

        when(mockDynamoDbAsyncClient.query(any(QueryRequest.class))).thenReturn(CompletableFuture.completedFuture(page1),
                CompletableFuture.completedFuture(page2));

        CompletableFuture<Dataset> future = asyncExecutor.query(queryRequest);
        Dataset result = future.get();

        assertNotNull(result);
        assertEquals(3, result.size());
    }

    @Test
    public void testListPaginationDoesNotBlockOnFutureGet() throws ExecutionException, InterruptedException {
        final QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();
        final QueryResponse page1 = QueryResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("1").build())))
                .lastEvaluatedKey(Map.of("id", AttributeValue.builder().s("1").build()))
                .build();
        final QueryResponse page2 = QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("2").build()))).build();
        final CompletableFuture<QueryResponse> page2Future = new CompletableFuture<>() {
            @Override
            public QueryResponse get() {
                throw new AssertionError("Pagination must compose the future instead of blocking on get()");
            }
        };
        page2Future.complete(page2);

        when(mockDynamoDbAsyncClient.query(any(QueryRequest.class))).thenReturn(CompletableFuture.completedFuture(page1), page2Future);

        assertEquals(2, asyncExecutor.list(queryRequest, Map.class).get().size());
    }

    @Test
    public void testQuery_NullArgsThrowEagerly() {
        // Aligned with the sync twin: both queryRequest and targetClass are validated at the call site
        // (before any CompletableFuture is built), so these throw IAE synchronously rather than completing exceptionally.
        QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();
        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.query(queryRequest, (Class<?>) null));
        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.query(null, Map.class));

        // Regression: list(QueryRequest, Class) was the only request+class entry point without the
        // eager guards its query/stream/scan siblings (and the sync twin's list) have.
        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.list((QueryRequest) null, Map.class));
        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.list(queryRequest, (Class<?>) null));

        // Regression: batchGetItem(BatchGetItemRequest, Class) validated targetClass only lazily,
        // AFTER the network call had already been issued; both args are now guarded eagerly.
        BatchGetItemRequest batchGetItemRequest = BatchGetItemRequest.builder().build();
        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.batchGetItem((BatchGetItemRequest) null, Map.class));
        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.batchGetItem(batchGetItemRequest, (Class<?>) null));
    }

    @Test
    public void testRequestOverloads_NullRequestThrowsIllegalArgumentExceptionEagerly() {
        // A null request object is rejected synchronously (before any future is built) with IAE — the same
        // exception type as the batchGetItem/list/query/stream/scan request overloads and the Mapper request overloads.
        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.getItem((GetItemRequest) null));
        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.getItem((GetItemRequest) null, Map.class));
        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.batchGetItem((BatchGetItemRequest) null));
        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.putItem((PutItemRequest) null));
        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.batchWriteItem((BatchWriteItemRequest) null));
        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.updateItem((UpdateItemRequest) null));
        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.deleteItem((DeleteItemRequest) null));
        verifyNoInteractions(mockDynamoDbAsyncClient);
    }

    @Test
    public void testStreamAndScan_NullArgsThrowEagerly() {
        QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();
        ScanRequest scanRequest = ScanRequest.builder().tableName("TestTable").build();

        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.stream((QueryRequest) null, Map.class));
        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.stream(queryRequest, (Class<?>) null));
        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.scan((ScanRequest) null, Map.class));
        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.scan(scanRequest, (Class<?>) null));

        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.scan((String) null, List.of("id")));
        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.scan((String) null, Map.<String, Condition> of()));
        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.scan((String) null, List.of("id"), Map.<String, Condition> of()));
        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.scan((String) null, List.of("id"), Map.class));
        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.scan((String) null, Map.<String, Condition> of(), Map.class));
        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.scan((String) null, List.of("id"), Map.<String, Condition> of(), Map.class));

        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.scan("TestTable", List.of("id"), (Class<?>) null));
        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.scan("TestTable", Map.<String, Condition> of(), (Class<?>) null));
        assertThrows(IllegalArgumentException.class, () -> asyncExecutor.scan("TestTable", List.of("id"), Map.<String, Condition> of(), (Class<?>) null));
    }

    @Test
    public void testScanEmptyAttributesToGetOmitsLegacyProjection() throws ExecutionException, InterruptedException {
        when(mockDynamoDbAsyncClient.scan(any(ScanRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(ScanResponse.builder().items(List.of()).build()));

        assertEquals(0, asyncExecutor.scan("TestTable", List.of()).get().count());
        assertEquals(0, asyncExecutor.scan("TestTable", List.of(), Map.<String, Condition> of()).get().count());
        assertEquals(0, asyncExecutor.scan("TestTable", List.of(), Map.class).get().count());
        assertEquals(0, asyncExecutor.scan("TestTable", List.of(), Map.<String, Condition> of(), Map.class).get().count());

        final ArgumentCaptor<ScanRequest> requestCaptor = ArgumentCaptor.forClass(ScanRequest.class);
        verify(mockDynamoDbAsyncClient, times(4)).scan(requestCaptor.capture());
        requestCaptor.getAllValues().forEach(request -> assertFalse(request.hasAttributesToGet()));
    }

    /**
     * Regression test: {@code stream(QueryRequest, Class)} must not terminate prematurely when an
     * intermediate page returns zero items but a non-empty LastEvaluatedKey. AWS SDK v2's
     * {@code QueryResponse.hasItems()} returns {@code true} even for an empty (but present) items
     * list, so the previous {@code if (queryResult.hasItems())} check broke out of the pagination
     * loop and dropped all subsequent pages.
     */
    @Test
    public void testStreamSkipsEmptyIntermediatePageAndContinuesPagination() throws ExecutionException, InterruptedException {
        QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();

        QueryResponse page1 = QueryResponse.builder().items(List.of()).lastEvaluatedKey(Map.of("id", AttributeValue.builder().s("k1").build())).build();
        QueryResponse page2 = QueryResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("1").build()), Map.of("id", AttributeValue.builder().s("2").build())))
                .build();

        when(mockDynamoDbAsyncClient.query(any(QueryRequest.class))).thenReturn(CompletableFuture.completedFuture(page1),
                CompletableFuture.completedFuture(page2));

        CompletableFuture<Stream<Map<String, Object>>> future = asyncExecutor.stream(queryRequest);
        Stream<Map<String, Object>> stream = future.get();

        assertNotNull(stream);
        assertEquals(2, stream.count());
    }

    /**
     * Regression test mirroring {@link #testStreamSkipsEmptyIntermediatePageAndContinuesPagination()}
     * for the scan stream pagination path.
     */
    @Test
    public void testScanStreamSkipsEmptyIntermediatePageAndContinuesPagination() throws ExecutionException, InterruptedException {
        ScanRequest scanRequest = ScanRequest.builder().tableName("TestTable").build();

        ScanResponse page1 = ScanResponse.builder().items(List.of()).lastEvaluatedKey(Map.of("id", AttributeValue.builder().s("k1").build())).build();
        ScanResponse page2 = ScanResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("1").build()), Map.of("id", AttributeValue.builder().s("2").build()),
                        Map.of("id", AttributeValue.builder().s("3").build())))
                .build();

        when(mockDynamoDbAsyncClient.scan(any(ScanRequest.class))).thenReturn(CompletableFuture.completedFuture(page1),
                CompletableFuture.completedFuture(page2));

        CompletableFuture<Stream<Map<String, Object>>> future = asyncExecutor.scan(scanRequest);
        Stream<Map<String, Object>> stream = future.get();

        assertNotNull(stream);
        assertEquals(3, stream.count());
    }

    /**
     * A lazy query stream blocks in Future.get while loading a page. Java interruption policy
     * requires code that translates InterruptedException to preserve the interrupt signal.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testQueryStreamPreservesInterruptStatus() throws Exception {
        CompletableFuture<QueryResponse> interruptedFuture = mock(CompletableFuture.class);
        when(interruptedFuture.get()).thenThrow(new InterruptedException("query page interrupted"));
        when(mockDynamoDbAsyncClient.query(any(QueryRequest.class))).thenReturn(interruptedFuture);

        Stream<Map<String, Object>> stream = asyncExecutor.stream(QueryRequest.builder().tableName("TestTable").build()).get();
        Thread.interrupted(); // clear any stale status so this regression test owns the signal

        try {
            assertThrows(RuntimeException.class, stream::count);
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted(); // do not leak the interrupt to the JUnit worker
        }
    }

    /** Regression coverage for the equivalent lazy scan iterator. */
    @SuppressWarnings("unchecked")
    @Test
    public void testScanStreamPreservesInterruptStatus() throws Exception {
        CompletableFuture<ScanResponse> interruptedFuture = mock(CompletableFuture.class);
        when(interruptedFuture.get()).thenThrow(new InterruptedException("scan page interrupted"));
        when(mockDynamoDbAsyncClient.scan(any(ScanRequest.class))).thenReturn(interruptedFuture);

        Stream<Map> stream = asyncExecutor.scan(ScanRequest.builder().tableName("TestTable").build(), Map.class).get();
        Thread.interrupted();

        try {
            assertThrows(RuntimeException.class, stream::count);
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void testMapperBatchPutItemAppliesNamingPolicy() throws InterruptedException, ExecutionException {
        AsyncDynamoDBExecutor.Mapper<NamingPolicyEntity> mapper = asyncExecutor.mapper(NamingPolicyEntity.class, "TestTable", NamingPolicy.SNAKE_CASE);

        NamingPolicyEntity entity = new NamingPolicyEntity();
        entity.setId("123");
        entity.setUserName("Alice");

        when(mockDynamoDbAsyncClient.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(BatchWriteItemResponse.builder().build()));

        org.mockito.ArgumentCaptor<BatchWriteItemRequest> captor = org.mockito.ArgumentCaptor.forClass(BatchWriteItemRequest.class);

        mapper.batchPutItem(List.of(entity)).get();

        verify(mockDynamoDbAsyncClient).batchWriteItem(captor.capture());

        Map<String, AttributeValue> item = captor.getValue().requestItems().get("TestTable").get(0).putRequest().item();

        // With SNAKE_CASE the "userName" property must be written as "user_name", not the default camelCase.
        assertTrue(item.containsKey("user_name"));
        assertEquals("Alice", item.get("user_name").s());
    }

    /**
     * Regression: when the async Mapper is configured with a non-CAMEL_CASE NamingPolicy and the @Id
     * field has no explicit @Column annotation, the key built by getItem/deleteItem/updateItem/
     * batchGetItem must use the policy-converted attribute name (e.g. "userId" -> "user_id") so it
     * matches what putItem writes via toItem(entity, namingPolicy). Previously the key was built
     * from the raw Java property name, causing every key-based operation to look up the wrong
     * attribute.
     */
    @Test
    public void testMapperGetItemAppliesNamingPolicyToKey() throws InterruptedException, ExecutionException {
        AsyncDynamoDBExecutor.Mapper<NamingPolicyKeyEntity> mapper = asyncExecutor.mapper(NamingPolicyKeyEntity.class, "TestTable", NamingPolicy.SNAKE_CASE);

        NamingPolicyKeyEntity entity = new NamingPolicyKeyEntity();
        entity.setUserId("u-1");

        when(mockDynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(GetItemResponse.builder().build()));

        org.mockito.ArgumentCaptor<GetItemRequest> captor = org.mockito.ArgumentCaptor.forClass(GetItemRequest.class);

        mapper.getItem(entity).get();

        verify(mockDynamoDbAsyncClient).getItem(captor.capture());

        Map<String, AttributeValue> key = captor.getValue().key();
        // With SNAKE_CASE the "userId" id must be mapped to "user_id" key, mirroring what putItem writes.
        assertTrue(key.containsKey("user_id"), "key should contain converted attribute 'user_id', actual keys: " + key.keySet());
        assertEquals("u-1", key.get("user_id").s());
    }

    /** Invalid entity IDs fail synchronously, before an async SDK request is created. */
    @Test
    public void testMapperRejectsMissingAndEmptyKeyValues() {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);
        TestEntity entity = new TestEntity();

        assertThrows(IllegalArgumentException.class, () -> mapper.getItem(entity));
        assertThrows(IllegalArgumentException.class, () -> mapper.putItem(entity));
        assertThrows(IllegalArgumentException.class, () -> mapper.batchPutItem(List.of(entity)));

        entity.setId("");
        assertThrows(IllegalArgumentException.class, () -> mapper.getItem(entity));
        assertThrows(IllegalArgumentException.class, () -> mapper.putItem(entity));

        entity.setId("valid-id");
        assertThrows(IllegalArgumentException.class, () -> mapper.updateItem(entity));
    }

    // Additional coverage for v2 async overloads.
    @Test
    public void testGetItemWithConsistentReadAndTargetClass() throws ExecutionException, InterruptedException {
        Map<String, AttributeValue> key = Map.of("id", AttributeValue.builder().s("1").build());
        GetItemResponse response = GetItemResponse.builder().item(Map.of("id", AttributeValue.builder().s("1").build())).build();
        when(mockDynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<TestEntity> future = asyncExecutor.getItem("TestTable", key, true, TestEntity.class);
        TestEntity result = future.get();
        assertNotNull(result);
        assertEquals("1", result.getId());
    }

    @Test
    public void testGetItemWithRequestAndTargetClass() throws ExecutionException, InterruptedException {
        GetItemRequest request = GetItemRequest.builder().tableName("TestTable").key(Map.of("id", AttributeValue.builder().s("1").build())).build();
        GetItemResponse response = GetItemResponse.builder().item(Map.of("id", AttributeValue.builder().s("1").build())).build();
        when(mockDynamoDbAsyncClient.getItem(request)).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<TestEntity> future = asyncExecutor.getItem(request, TestEntity.class);
        TestEntity result = future.get();
        assertNotNull(result);
        assertEquals("1", result.getId());
    }

    @Test
    public void testBatchGetItemWithTargetClass() throws ExecutionException, InterruptedException {
        Map<String, KeysAndAttributes> requestItems = Map.of("TestTable",
                KeysAndAttributes.builder().keys(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build());
        BatchGetItemResponse response = BatchGetItemResponse.builder()
                .responses(Map.of("TestTable", List.of(Map.of("id", AttributeValue.builder().s("1").build()))))
                .build();
        when(mockDynamoDbAsyncClient.batchGetItem(any(BatchGetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Map<String, List<TestEntity>>> future = asyncExecutor.batchGetItem(requestItems, TestEntity.class);
        Map<String, List<TestEntity>> result = future.get();
        assertNotNull(result);
        assertEquals(1, result.get("TestTable").size());
    }

    @Test
    public void testBatchGetItemWithReturnConsumedCapacityAndTargetClass() throws ExecutionException, InterruptedException {
        Map<String, KeysAndAttributes> requestItems = Map.of("TestTable", KeysAndAttributes.builder().build());
        BatchGetItemResponse response = BatchGetItemResponse.builder().responses(new HashMap<>()).build();
        when(mockDynamoDbAsyncClient.batchGetItem(any(BatchGetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Map<String, List<TestEntity>>> future = asyncExecutor.batchGetItem(requestItems, "TOTAL", TestEntity.class);
        assertNotNull(future.get());
    }

    @Test
    public void testBatchGetItemWithRequest() throws ExecutionException, InterruptedException {
        BatchGetItemRequest request = BatchGetItemRequest.builder()
                .requestItems(Map.of("TestTable", KeysAndAttributes.builder().keys(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build()))
                .build();
        BatchGetItemResponse response = BatchGetItemResponse.builder()
                .responses(Map.of("TestTable", List.of(Map.of("id", AttributeValue.builder().s("1").build()))))
                .build();
        when(mockDynamoDbAsyncClient.batchGetItem(request)).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Map<String, List<Map<String, Object>>>> future = asyncExecutor.batchGetItem(request);
        Map<String, List<Map<String, Object>>> result = future.get();
        assertNotNull(result);
        assertEquals(1, result.get("TestTable").size());
    }

    @Test
    public void testBatchGetItemWithRequestAndTargetClass() throws ExecutionException, InterruptedException {
        BatchGetItemRequest request = BatchGetItemRequest.builder()
                .requestItems(Map.of("TestTable", KeysAndAttributes.builder().keys(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build()))
                .build();
        BatchGetItemResponse response = BatchGetItemResponse.builder()
                .responses(Map.of("TestTable", List.of(Map.of("id", AttributeValue.builder().s("1").build()))))
                .build();
        when(mockDynamoDbAsyncClient.batchGetItem(request)).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Map<String, List<TestEntity>>> future = asyncExecutor.batchGetItem(request, TestEntity.class);
        Map<String, List<TestEntity>> result = future.get();
        assertNotNull(result);
        assertEquals(1, result.get("TestTable").size());
    }

    @Test
    public void testPutItemWithRequest() throws ExecutionException, InterruptedException {
        PutItemRequest request = PutItemRequest.builder().tableName("TestTable").item(Map.of("id", AttributeValue.builder().s("1").build())).build();
        when(mockDynamoDbAsyncClient.putItem(request)).thenReturn(CompletableFuture.completedFuture(PutItemResponse.builder().build()));

        CompletableFuture<PutItemResponse> future = asyncExecutor.putItem(request);
        assertNotNull(future.get());
    }

    @Test
    public void testBatchWriteItemWithRequest() throws ExecutionException, InterruptedException {
        BatchWriteItemRequest request = BatchWriteItemRequest.builder()
                .requestItems(Map.of("TestTable", List.of(
                        WriteRequest.builder().putRequest(PutRequest.builder().item(Map.of("id", AttributeValue.builder().s("1").build())).build()).build())))
                .build();
        when(mockDynamoDbAsyncClient.batchWriteItem(request)).thenReturn(CompletableFuture.completedFuture(BatchWriteItemResponse.builder().build()));

        CompletableFuture<BatchWriteItemResponse> future = asyncExecutor.batchWriteItem(request);
        assertNotNull(future.get());
    }

    @Test
    public void testUpdateItemWithRequest() throws ExecutionException, InterruptedException {
        UpdateItemRequest request = UpdateItemRequest.builder().tableName("TestTable").key(Map.of("id", AttributeValue.builder().s("1").build())).build();
        when(mockDynamoDbAsyncClient.updateItem(request)).thenReturn(CompletableFuture.completedFuture(UpdateItemResponse.builder().build()));

        CompletableFuture<UpdateItemResponse> future = asyncExecutor.updateItem(request);
        assertNotNull(future.get());
    }

    @Test
    public void testDeleteItemWithRequest() throws ExecutionException, InterruptedException {
        DeleteItemRequest request = DeleteItemRequest.builder().tableName("TestTable").key(Map.of("id", AttributeValue.builder().s("1").build())).build();
        when(mockDynamoDbAsyncClient.deleteItem(request)).thenReturn(CompletableFuture.completedFuture(DeleteItemResponse.builder().build()));

        CompletableFuture<DeleteItemResponse> future = asyncExecutor.deleteItem(request);
        assertNotNull(future.get());
    }

    @Test
    public void testStreamWithTargetClass() throws ExecutionException, InterruptedException {
        QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();
        QueryResponse response = QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build();
        when(mockDynamoDbAsyncClient.query(any(QueryRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Stream<TestEntity>> future = asyncExecutor.stream(queryRequest, TestEntity.class);
        Stream<TestEntity> stream = future.get();
        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testScanWithAttributesToGetAndTargetClass() throws ExecutionException, InterruptedException {
        ScanResponse response = ScanResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build();
        when(mockDynamoDbAsyncClient.scan(any(ScanRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Stream<TestEntity>> future = asyncExecutor.scan("TestTable", List.of("id"), TestEntity.class);
        Stream<TestEntity> stream = future.get();
        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testScanWithScanFilterAndTargetClass() throws ExecutionException, InterruptedException {
        ScanResponse response = ScanResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build();
        when(mockDynamoDbAsyncClient.scan(any(ScanRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        Map<String, Condition> scanFilter = new HashMap<>();
        CompletableFuture<Stream<TestEntity>> future = asyncExecutor.scan("TestTable", scanFilter, TestEntity.class);
        Stream<TestEntity> stream = future.get();
        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testScanWithAttrsFilterAndTargetClass() throws ExecutionException, InterruptedException {
        ScanResponse response = ScanResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build();
        when(mockDynamoDbAsyncClient.scan(any(ScanRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        Map<String, Condition> scanFilter = new HashMap<>();
        CompletableFuture<Stream<TestEntity>> future = asyncExecutor.scan("TestTable", List.of("id"), scanFilter, TestEntity.class);
        Stream<TestEntity> stream = future.get();
        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testScanWithRequestAndTargetClass() throws ExecutionException, InterruptedException {
        ScanRequest scanRequest = ScanRequest.builder().tableName("TestTable").build();
        ScanResponse response = ScanResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build();
        when(mockDynamoDbAsyncClient.scan(any(ScanRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Stream<TestEntity>> future = asyncExecutor.scan(scanRequest, TestEntity.class);
        Stream<TestEntity> stream = future.get();
        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    // Mapper overload tests.
    @Test
    public void testMapperGetItemWithConsistentRead() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);
        TestEntity entity = new TestEntity();
        entity.setId("1");

        GetItemResponse response = GetItemResponse.builder().item(Map.of("id", AttributeValue.builder().s("1").build())).build();
        when(mockDynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<TestEntity> future = mapper.getItem(entity, true);
        TestEntity result = future.get();
        assertNotNull(result);
        assertEquals("1", result.getId());
    }

    @Test
    public void testMapperGetItemWithKey() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);
        Map<String, AttributeValue> key = Map.of("id", AttributeValue.builder().s("1").build());

        GetItemResponse response = GetItemResponse.builder().item(Map.of("id", AttributeValue.builder().s("1").build())).build();
        when(mockDynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<TestEntity> future = mapper.getItem(key);
        TestEntity result = future.get();
        assertNotNull(result);
        assertEquals("1", result.getId());
    }

    @Test
    public void testMapperGetItemWithRequest() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);
        GetItemRequest request = GetItemRequest.builder().tableName("TestTable").key(Map.of("id", AttributeValue.builder().s("1").build())).build();

        GetItemResponse response = GetItemResponse.builder().item(Map.of("id", AttributeValue.builder().s("1").build())).build();
        when(mockDynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<TestEntity> future = mapper.getItem(request);
        TestEntity result = future.get();
        assertNotNull(result);
        assertEquals("1", result.getId());
    }

    @Test
    public void testMapperBatchGetItemWithReturnConsumedCapacity() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);
        TestEntity entity = new TestEntity();
        entity.setId("1");

        BatchGetItemResponse response = BatchGetItemResponse.builder()
                .responses(Map.of("TestTable", List.of(Map.of("id", AttributeValue.builder().s("1").build()))))
                .build();
        when(mockDynamoDbAsyncClient.batchGetItem(any(BatchGetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<List<TestEntity>> future = mapper.batchGetItem(List.of(entity), "TOTAL");
        List<TestEntity> result = future.get();
        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testMapperPutItemWithReturnValues() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);
        TestEntity entity = new TestEntity();
        entity.setId("1");

        when(mockDynamoDbAsyncClient.putItem(any(PutItemRequest.class))).thenReturn(CompletableFuture.completedFuture(PutItemResponse.builder().build()));

        CompletableFuture<PutItemResponse> future = mapper.putItem(entity, "ALL_OLD");
        assertNotNull(future.get());
    }

    @Test
    public void testMapperUpdateItemWithReturnValues() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);
        TestEntity entity = new TestEntity();
        entity.setId("1");
        entity.setName("Updated");

        when(mockDynamoDbAsyncClient.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(UpdateItemResponse.builder().build()));

        CompletableFuture<UpdateItemResponse> future = mapper.updateItem(entity, "ALL_NEW");
        assertNotNull(future.get());
    }

    @Test
    public void testMapperDeleteItemWithReturnValues() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);
        TestEntity entity = new TestEntity();
        entity.setId("1");

        when(mockDynamoDbAsyncClient.deleteItem(any(DeleteItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(DeleteItemResponse.builder().build()));

        CompletableFuture<DeleteItemResponse> future = mapper.deleteItem(entity, "ALL_OLD");
        assertNotNull(future.get());
    }

    @Test
    public void testMapperDeleteItemWithKey() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);
        Map<String, AttributeValue> key = Map.of("id", AttributeValue.builder().s("1").build());

        when(mockDynamoDbAsyncClient.deleteItem(any(DeleteItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(DeleteItemResponse.builder().build()));

        CompletableFuture<DeleteItemResponse> future = mapper.deleteItem(key);
        assertNotNull(future.get());
    }

    @Test
    public void testMapperPutItemWithRequest() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);
        PutItemRequest request = PutItemRequest.builder().tableName("TestTable").item(Map.of("id", AttributeValue.builder().s("1").build())).build();

        when(mockDynamoDbAsyncClient.putItem(any(PutItemRequest.class))).thenReturn(CompletableFuture.completedFuture(PutItemResponse.builder().build()));

        CompletableFuture<PutItemResponse> future = mapper.putItem(request);
        assertNotNull(future.get());
    }

    @Test
    public void testMapperUpdateItemWithRequest() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);
        UpdateItemRequest request = UpdateItemRequest.builder().tableName("TestTable").key(Map.of("id", AttributeValue.builder().s("1").build())).build();

        when(mockDynamoDbAsyncClient.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(UpdateItemResponse.builder().build()));

        CompletableFuture<UpdateItemResponse> future = mapper.updateItem(request);
        assertNotNull(future.get());
    }

    @Test
    public void testMapperDeleteItemWithRequest() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);
        DeleteItemRequest request = DeleteItemRequest.builder().tableName("TestTable").key(Map.of("id", AttributeValue.builder().s("1").build())).build();

        when(mockDynamoDbAsyncClient.deleteItem(any(DeleteItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(DeleteItemResponse.builder().build()));

        CompletableFuture<DeleteItemResponse> future = mapper.deleteItem(request);
        assertNotNull(future.get());
    }

    @Test
    public void testMapperBatchGetItemWithRequest() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);
        BatchGetItemRequest request = BatchGetItemRequest.builder()
                .requestItems(Map.of("TestTable", KeysAndAttributes.builder().keys(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build()))
                .build();
        BatchGetItemResponse response = BatchGetItemResponse.builder()
                .responses(Map.of("TestTable", List.of(Map.of("id", AttributeValue.builder().s("1").build()))))
                .build();
        when(mockDynamoDbAsyncClient.batchGetItem(any(BatchGetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<List<TestEntity>> future = mapper.batchGetItem(request);
        List<TestEntity> result = future.get();
        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testMapperBatchWriteItemWithRequest() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);
        BatchWriteItemRequest request = BatchWriteItemRequest.builder()
                .requestItems(Map.of("TestTable", List.of(
                        WriteRequest.builder().putRequest(PutRequest.builder().item(Map.of("id", AttributeValue.builder().s("1").build())).build()).build())))
                .build();

        when(mockDynamoDbAsyncClient.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(BatchWriteItemResponse.builder().build()));

        CompletableFuture<BatchWriteItemResponse> future = mapper.batchWriteItem(request);
        assertNotNull(future.get());
    }

    @Test
    public void testMapperScanWithAttributesToGet() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);
        ScanResponse response = ScanResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build();
        when(mockDynamoDbAsyncClient.scan(any(ScanRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Stream<TestEntity>> future = mapper.scan(List.of("id"));
        Stream<TestEntity> stream = future.get();
        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testMapperScanWithScanFilter() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);
        ScanResponse response = ScanResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build();
        when(mockDynamoDbAsyncClient.scan(any(ScanRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        Map<String, Condition> scanFilter = new HashMap<>();
        CompletableFuture<Stream<TestEntity>> future = mapper.scan(scanFilter);
        Stream<TestEntity> stream = future.get();
        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testMapperScanWithAttrsAndScanFilter() throws ExecutionException, InterruptedException {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);
        ScanResponse response = ScanResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build();
        when(mockDynamoDbAsyncClient.scan(any(ScanRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        Map<String, Condition> scanFilter = new HashMap<>();
        CompletableFuture<Stream<TestEntity>> future = mapper.scan(List.of("id"), scanFilter);
        Stream<TestEntity> stream = future.get();
        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testMapperGetItemWithWrongTableName() {
        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);
        GetItemRequest request = GetItemRequest.builder().tableName("WrongTable").key(Map.of("id", AttributeValue.builder().s("1").build())).build();
        assertThrows(IllegalArgumentException.class, () -> mapper.getItem(request));
    }

    // --- sliceD 2026-09-22: pins for documented behavior ---

    /**
     * A class with no {@code @Id} field falls back to its {@code id} property as the partition key
     * (documented on mapper(..)/Mapper since the "zero @Id fields -> IAE" wording was corrected).
     */
    @Test
    public void testMapperFallsBackToIdPropertyWithoutIdAnnotation() throws Exception {
        final AsyncDynamoDBExecutor.Mapper<NoTableEntity> mapper = asyncExecutor.mapper(NoTableEntity.class, "FallbackTable", null);

        when(mockDynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(GetItemResponse.builder().build()));

        final NoTableEntity key = new NoTableEntity();
        key.setId("k1");
        mapper.getItem(key).get();

        final ArgumentCaptor<GetItemRequest> captor = ArgumentCaptor.forClass(GetItemRequest.class);
        verify(mockDynamoDbAsyncClient).getItem(captor.capture());
        assertEquals("FallbackTable", captor.getValue().tableName());
        assertEquals(Map.of("id", AttributeValue.fromS("k1")), captor.getValue().key());
    }

    /**
     * The async executor converts rows through the sync v2 helpers, so a B attribute read into a
     * ByteBuffer must be readable (position 0, remaining == byte count), not an exhausted buffer.
     */
    @Test
    public void testGetItemBinaryAttributeIntoByteBufferIsReadable() throws Exception {
        final byte[] bytes = { 1, 2, 3 };
        when(mockDynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(
                GetItemResponse.builder().item(Map.of("data", AttributeValue.fromB(software.amazon.awssdk.core.SdkBytes.fromByteArray(bytes)))).build()));

        final java.nio.ByteBuffer buf = asyncExecutor.getItem("T", Map.of("id", AttributeValue.fromS("1")), java.nio.ByteBuffer.class).get();

        assertEquals(bytes.length, buf.remaining());
        final byte[] read = new byte[buf.remaining()];
        buf.get(read);
        assertTrue(java.util.Arrays.equals(bytes, read));
    }

    @com.landawn.abacus.annotation.Table(name = "TestTable")
    private static class TestEntity {
        @com.landawn.abacus.annotation.Id
        private String id;
        private String name;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    private static class NoTableEntity {
        private String id;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }
    }

    @com.landawn.abacus.annotation.Table(name = "TestTable")
    private static class CompositeKeyEntity {
        @com.landawn.abacus.annotation.Id
        private String partitionId;
        @com.landawn.abacus.annotation.Id
        private String sortId;

        public String getPartitionId() {
            return partitionId;
        }

        public void setPartitionId(String partitionId) {
            this.partitionId = partitionId;
        }

        public String getSortId() {
            return sortId;
        }

        public void setSortId(String sortId) {
            this.sortId = sortId;
        }
    }

    @com.landawn.abacus.annotation.Table(name = "TestTable")
    private static class ThreeKeyEntity {
        @com.landawn.abacus.annotation.Id
        private String firstId;
        @com.landawn.abacus.annotation.Id
        private String secondId;
        @com.landawn.abacus.annotation.Id
        private String thirdId;

        public String getFirstId() {
            return firstId;
        }

        public void setFirstId(String firstId) {
            this.firstId = firstId;
        }

        public String getSecondId() {
            return secondId;
        }

        public void setSecondId(String secondId) {
            this.secondId = secondId;
        }

        public String getThirdId() {
            return thirdId;
        }

        public void setThirdId(String thirdId) {
            this.thirdId = thirdId;
        }
    }

    private static class NamingPolicyEntity {
        @com.landawn.abacus.annotation.Id
        private String id;
        private String userName;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }
    }

    private static class NamingPolicyKeyEntity {
        @com.landawn.abacus.annotation.Id
        private String userId;
        private String userName;

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }
    }

    private static class CompositeKeyNameCollisionEntity {
        @com.landawn.abacus.annotation.Id
        private String partitionKey;
        @com.landawn.abacus.annotation.Id
        private String partition_key;

        public String getPartitionKey() {
            return partitionKey;
        }

        public void setPartitionKey(final String partitionKey) {
            this.partitionKey = partitionKey;
        }

        public String getPartition_key() {
            return partition_key;
        }

        public void setPartition_key(final String partition_key) {
            this.partition_key = partition_key;
        }
    }

    // ===== Documented contract: getItem(tableName, key[, consistentRead], targetClass) on a missing item =====

    @Test
    public void testGetItemByKeyMissingItemReturnsDefaultForPrimitiveTargetClass() throws Exception {
        when(mockDynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(GetItemResponse.builder().build()));
        final Map<String, AttributeValue> key = Map.of("id", AttributeValue.fromS("missing"));

        assertEquals(0, (int) asyncExecutor.getItem("TestTable", key, int.class).get());
        assertEquals(false, asyncExecutor.getItem("TestTable", key, true, boolean.class).get());
        assertNull(asyncExecutor.getItem("TestTable", key, Integer.class).get());
        assertNull(asyncExecutor.getItem("TestTable", key, false, TestEntity.class).get());
        assertNull(asyncExecutor.getItem("TestTable", key).get());
        verify(mockDynamoDbAsyncClient, times(5)).getItem(any(GetItemRequest.class));
    }

    // ===== sliceD (2026-09-27): async conversion paths delegate to the sync v2 helpers =====

    /**
     * Pins the documented query(QueryRequest) example: Map rows hold N attributes as their raw number
     * String, so a {@code (Number)} cast fails and the value must be parsed.
     */
    @Test
    public void testQueryMapRowsHoldNumericAttributesAsRawNumberStrings() throws Exception {
        final QueryResponse response = QueryResponse.builder()
                .items(List.of(Map.of("productId", AttributeValue.fromS("p1"), "amount", AttributeValue.fromN("12.5")),
                        Map.of("productId", AttributeValue.fromS("p1"), "amount", AttributeValue.fromN("2.5"))))
                .build();
        when(mockDynamoDbAsyncClient.query(any(QueryRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        final Dataset dataset = asyncExecutor.query(QueryRequest.builder().tableName("Sales").build()).get();

        assertEquals("12.5", dataset.moveToRow(0).get("amount"));
        final Dataset grouped = dataset.groupBy("productId", "amount", "totalAmount",
                com.landawn.abacus.util.stream.Collectors.summingDouble(v -> Double.parseDouble((String) v)));
        assertEquals(15.0, ((Number) grouped.moveToRow(0).get("totalAmount")).doubleValue(), 0.0);
    }

    /**
     * The async typed read paths (getItem/list/query/stream) convert through the sync v2 helpers, so a
     * String[] property stored as JSON-array text (an S attribute) is parsed element-wise, not wrapped whole.
     */
    @Test
    public void testTypedReadsParseStringArrayPropertyFromJsonArrayText() throws Exception {
        final Map<String, AttributeValue> item = Map.of("id", AttributeValue.fromS("1"), "tags", AttributeValue.fromS("[\"a\",\"b\"]"));
        when(mockDynamoDbAsyncClient.getItem(any(GetItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(GetItemResponse.builder().item(item).build()));
        when(mockDynamoDbAsyncClient.query(any(QueryRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(QueryResponse.builder().items(List.of(item)).build()));
        final QueryRequest queryRequest = QueryRequest.builder().tableName("T").build();

        final List<StringArrayEntity> fromReads = new ArrayList<>();
        fromReads.add(asyncExecutor.getItem("T", Map.of("id", AttributeValue.fromS("1")), StringArrayEntity.class).get());
        fromReads.addAll(asyncExecutor.list(queryRequest, StringArrayEntity.class).get());
        fromReads.addAll(asyncExecutor.stream(queryRequest, StringArrayEntity.class).get().toList());
        fromReads.addAll(asyncExecutor.query(queryRequest, StringArrayEntity.class).get().toList(StringArrayEntity.class));

        assertEquals(4, fromReads.size());
        for (final StringArrayEntity e : fromReads) {
            assertTrue(java.util.Arrays.equals(new String[] { "a", "b" }, e.getTags()), java.util.Arrays.toString(e.getTags()));
        }
    }

    public static class StringArrayEntity {
        private String id;
        private String[] tags;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String[] getTags() {
            return tags;
        }

        public void setTags(final String[] tags) {
            this.tags = tags;
        }
    }

    // ---- 2026-09-29 sliceD ----

    /**
     * The async read paths convert through the sync v2 helpers, so they inherit the sync fix that skips a getter-only
     * property inherited from an {@code @Entity} superclass (the mapper writes its computed value; reading it back used to
     * fail the whole item with UnsupportedOperationException).
     */
    @Test
    public void testAsyncReadsSkipReadOnlyInheritedPropertyAttribute() throws Exception {
        final AsyncDynamoDBExecutor.Mapper<AsyncReadOnlyPropSubEntity> mapper = asyncExecutor.mapper(AsyncReadOnlyPropSubEntity.class);
        final AsyncReadOnlyPropSubEntity source = new AsyncReadOnlyPropSubEntity();
        source.setId("id-1");
        source.setName("n");

        when(mockDynamoDbAsyncClient.putItem(any(PutItemRequest.class))).thenReturn(CompletableFuture.completedFuture(PutItemResponse.builder().build()));
        mapper.putItem(source).get();
        final ArgumentCaptor<PutItemRequest> putCaptor = ArgumentCaptor.forClass(PutItemRequest.class);
        verify(mockDynamoDbAsyncClient).putItem(putCaptor.capture());
        final Map<String, AttributeValue> item = putCaptor.getValue().item();
        assertEquals("computed:id-1", item.get("computed").s());

        when(mockDynamoDbAsyncClient.getItem(any(GetItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(GetItemResponse.builder().item(item).build()));
        when(mockDynamoDbAsyncClient.query(any(QueryRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(QueryResponse.builder().items(List.of(item)).build()));
        when(mockDynamoDbAsyncClient.batchGetItem(any(BatchGetItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(BatchGetItemResponse.builder().responses(Map.of("AsyncReadOnlyTable", List.of(item))).build()));

        final List<AsyncReadOnlyPropSubEntity> reads = new ArrayList<>();
        reads.add(mapper.getItem(source).get());
        reads.addAll(mapper.list(QueryRequest.builder().build()).get());
        reads.addAll(mapper.stream(QueryRequest.builder().build()).get().toList());
        reads.addAll(mapper.batchGetItem(List.of(source)).get());

        assertEquals(4, reads.size());
        for (final AsyncReadOnlyPropSubEntity e : reads) {
            assertEquals("id-1", e.getId());
            assertEquals("n", e.getName());
            assertEquals("computed:id-1", e.getComputed());
        }
    }

    /**
     * The async read paths inherit the sync fix for bracketed plain text (not a JSON array) read into a String[] property:
     * it keeps the lenient single-element conversion instead of failing the item with a ParsingException.
     */
    @Test
    public void testAsyncReadsKeepBracketedNonJsonTextAsSingleStringArrayElement() throws Exception {
        final Map<String, AttributeValue> item = Map.of("id", AttributeValue.fromS("1"), "tags", AttributeValue.fromS("[a] and [b]"));
        when(mockDynamoDbAsyncClient.getItem(any(GetItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(GetItemResponse.builder().item(item).build()));
        when(mockDynamoDbAsyncClient.scan(any(ScanRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(ScanResponse.builder().items(List.of(item)).build()));

        final List<StringArrayEntity> reads = new ArrayList<>();
        reads.add(asyncExecutor.getItem("T", Map.of("id", AttributeValue.fromS("1")), StringArrayEntity.class).get());
        reads.addAll(asyncExecutor.scan("T", (List<String>) null, StringArrayEntity.class).get().toList());

        assertEquals(2, reads.size());
        for (final StringArrayEntity e : reads) {
            assertTrue(java.util.Arrays.equals(new String[] { "[a] and [b]" }, e.getTags()), java.util.Arrays.toString(e.getTags()));
        }
    }

    @com.landawn.abacus.annotation.Entity
    public static class AsyncReadOnlyPropBaseEntity {
        @com.landawn.abacus.annotation.Id
        private String id;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getComputed() {
            return "computed:" + id;
        }
    }

    @com.landawn.abacus.annotation.Table(name = "AsyncReadOnlyTable")
    public static class AsyncReadOnlyPropSubEntity extends AsyncReadOnlyPropBaseEntity {
        private String name;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    // ---- 2026-10-02 sliceD ----

    /**
     * mapper(Class) without {@code @Table} and the Mapper constructor for a non-bean class concatenated the Class object,
     * so the messages read "Entity class class com.x.Foo must ..." and "class java.lang.Integer is not an entity class".
     */
    @Test
    public void testMapper_ErrorMessagesNameClassOnce() {
        final IllegalArgumentException noTable = assertThrows(IllegalArgumentException.class, () -> asyncExecutor.mapper(NoTableEntity.class));
        assertTrue(noTable.getMessage().startsWith("Entity class " + com.landawn.abacus.util.ClassUtil.getCanonicalClassName(NoTableEntity.class) + " must"),
                noTable.getMessage());
        assertFalse(noTable.getMessage().contains("class class"), noTable.getMessage());

        final IllegalArgumentException notBean = assertThrows(IllegalArgumentException.class, () -> asyncExecutor.mapper(Integer.class, "t", null));
        assertTrue(notBean.getMessage().startsWith("java.lang.Integer is not an entity class"), notBean.getMessage());
    }

    /**
     * Cancelling (or timing out) the future returned by list/query must stop the auto-pagination. The cancellation of a
     * dependent stage never reaches the page chain, which used to keep requesting every remaining page in the background.
     */
    @Test
    public void testListAndQueryStopPaginatingOnceReturnedFutureIsCancelled() throws Exception {
        final QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();
        final Map<String, AttributeValue> row = Map.of("id", AttributeValue.fromS("1"));

        for (int variant = 0; variant < 3; variant++) {
            final DynamoDbAsyncClient client = mock(DynamoDbAsyncClient.class);
            final AsyncDynamoDBExecutor executor = new AsyncDynamoDBExecutor(client);
            final CompletableFuture<QueryResponse> secondPage = new CompletableFuture<>();
            when(client.query(any(QueryRequest.class))).thenReturn(
                    CompletableFuture.completedFuture(QueryResponse.builder().items(List.of(row)).lastEvaluatedKey(Map.of("id", AttributeValue.fromS("1"))).build()),
                    secondPage, CompletableFuture.completedFuture(QueryResponse.builder().items(List.of(row)).build()));

            final CompletableFuture<?> result = variant == 0 ? executor.list(queryRequest, StringArrayEntity.class)
                    : variant == 1 ? executor.query(queryRequest) : executor.query(queryRequest, StringArrayEntity.class);

            verify(client, org.mockito.Mockito.timeout(5000).times(2)).query(any(QueryRequest.class)); // page 2 is in flight
            assertTrue(result.cancel(true));

            secondPage.complete(QueryResponse.builder().items(List.of(row)).lastEvaluatedKey(Map.of("id", AttributeValue.fromS("2"))).build());

            verify(client, org.mockito.Mockito.after(1000).times(2)).query(any(QueryRequest.class)); // no page 3 request
            assertTrue(result.isCancelled(), "variant " + variant);
        }
    }

    /**
     * The result future of list/query is completed from the page chain, so a page failure still reaches get() as the
     * unwrapped cause and exceptionally(...) as a CompletionException, and an uncancelled multi-page query returns every page.
     */
    @Test
    public void testListAndQueryForwardPagesAndFailuresToReturnedFuture() throws Exception {
        final QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();
        final Map<String, AttributeValue> row = Map.of("id", AttributeValue.fromS("1"));
        final QueryResponse firstPage = QueryResponse.builder().items(List.of(row)).lastEvaluatedKey(Map.of("id", AttributeValue.fromS("1"))).build();
        final software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException failure = software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
                .builder()
                .message("gone")
                .build();

        when(mockDynamoDbAsyncClient.query(any(QueryRequest.class))).thenReturn(CompletableFuture.completedFuture(firstPage),
                CompletableFuture.completedFuture(firstPage.copy(b -> b.lastEvaluatedKey(Map.of("id", AttributeValue.fromS("2"))))),
                CompletableFuture.completedFuture(QueryResponse.builder().items(List.of(row)).build()), CompletableFuture.completedFuture(firstPage),
                CompletableFuture.failedFuture(failure), CompletableFuture.completedFuture(firstPage), CompletableFuture.failedFuture(failure));

        assertEquals(3, asyncExecutor.list(queryRequest, StringArrayEntity.class).get().size());

        final CompletableFuture<Dataset> failedQuery = asyncExecutor.query(queryRequest);
        assertSame(failure, assertThrows(ExecutionException.class, failedQuery::get).getCause());
        final Throwable seen = failedQuery.handle((r, e) -> e).get();
        assertTrue(seen instanceof java.util.concurrent.CompletionException && seen.getCause() == failure, String.valueOf(seen));

        assertSame(failure, assertThrows(ExecutionException.class, () -> asyncExecutor.query(queryRequest, StringArrayEntity.class).get()).getCause());
    }

    // ---- 2026-10-02 verifyDD ----

    private static final List<String> PAGINATING_QUERY_METHODS_verifyDD = List.of("list", "list(Entity)", "query", "query(LinkedHashMap)", "query(Entity)",
            "mapper.list", "mapper.query");

    private static CompletableFuture<?> invokePaginatingQuery_verifyDD(final AsyncDynamoDBExecutor executor, final String method,
            final QueryRequest queryRequest) {
        return switch (method) {
            case "list" -> executor.list(queryRequest);
            case "list(Entity)" -> executor.list(queryRequest, StringArrayEntity.class);
            case "query" -> executor.query(queryRequest);
            case "query(LinkedHashMap)" -> executor.query(queryRequest, java.util.LinkedHashMap.class);
            case "query(Entity)" -> executor.query(queryRequest, StringArrayEntity.class);
            case "mapper.list" -> executor.mapper(TestEntity.class).list(queryRequest);
            case "mapper.query" -> executor.mapper(TestEntity.class).query(queryRequest);
            default -> throw new IllegalArgumentException(method);
        };
    }

    private static long queryCalls_verifyDD(final DynamoDbAsyncClient client) {
        return org.mockito.Mockito.mockingDetails(client).getInvocations().stream().filter(i -> i.getMethod().getName().equals("query")).count();
    }

    // Waits until the page chain has run to its end. CompletableFuture runs async stages in the common pool when it has more
    // than one worker (otherwise on a new thread per task, which awaitQuiescence cannot see, so fall back to a fixed wait).
    private static void awaitPageChain_verifyDD() throws InterruptedException {
        if (java.util.concurrent.ForkJoinPool.getCommonPoolParallelism() > 1) {
            java.util.concurrent.ForkJoinPool.commonPool().awaitQuiescence(5, java.util.concurrent.TimeUnit.SECONDS);
        } else {
            Thread.sleep(500);
        }
    }

    /**
     * Every auto-paginating entry point (list/query, Map and entity rows, executor and Mapper) stops requesting pages once the
     * returned future is completed early: cancelled before the first page arrives, or timed out with orTimeout(...) /
     * completeOnTimeout(...) while page 2 is in flight. Before the fix only a cancel that beat the first page of list/raw-Map
     * query stopped it (the returned stage was that page's direct dependent); otherwise the remaining pages were still
     * requested. Without early completion every page is still fetched and returned.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testEveryPaginatingListAndQueryStopsWhenReturnedFutureCompletesEarly_verifyDD() throws Exception {
        final QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();
        final Map<String, AttributeValue> row = Map.of("id", AttributeValue.fromS("1"));
        final QueryResponse pageWithMore = QueryResponse.builder().items(List.of(row)).lastEvaluatedKey(Map.of("id", AttributeValue.fromS("1"))).build();
        final QueryResponse lastPage = QueryResponse.builder().items(List.of(row)).build();
        final List<String> wrong = new ArrayList<>();

        for (final String method : PAGINATING_QUERY_METHODS_verifyDD) {
            for (final String mode : List.of("cancelBeforeFirstPage", "orTimeoutDuringPage2", "completeOnTimeoutDuringPage2", "notCompletedEarly")) {
                final DynamoDbAsyncClient client = mock(DynamoDbAsyncClient.class);
                final AsyncDynamoDBExecutor executor = new AsyncDynamoDBExecutor(client);
                final CompletableFuture<QueryResponse> firstPage = new CompletableFuture<>();
                final CompletableFuture<QueryResponse> secondPage = new CompletableFuture<>();
                when(client.query(any(QueryRequest.class))).thenReturn(firstPage, secondPage, CompletableFuture.completedFuture(lastPage));

                final CompletableFuture<Object> result = (CompletableFuture<Object>) invokePaginatingQuery_verifyDD(executor, method, queryRequest);
                final long expectedCalls;

                if (mode.equals("cancelBeforeFirstPage")) {
                    assertTrue(result.cancel(true));
                    firstPage.complete(pageWithMore);
                    expectedCalls = 1;
                } else {
                    firstPage.complete(pageWithMore);
                    verify(client, org.mockito.Mockito.timeout(5000).times(2)).query(any(QueryRequest.class)); // page 2 is in flight

                    if (mode.equals("orTimeoutDuringPage2")) {
                        final ExecutionException e = assertThrows(ExecutionException.class,
                                () -> result.orTimeout(1, java.util.concurrent.TimeUnit.MILLISECONDS).get(5, java.util.concurrent.TimeUnit.SECONDS));
                        assertTrue(e.getCause() instanceof java.util.concurrent.TimeoutException, method + ": " + e.getCause());
                        expectedCalls = 2;
                    } else if (mode.equals("completeOnTimeoutDuringPage2")) {
                        assertEquals("fallback",
                                result.completeOnTimeout("fallback", 1, java.util.concurrent.TimeUnit.MILLISECONDS).get(5, java.util.concurrent.TimeUnit.SECONDS));
                        expectedCalls = 2;
                    } else {
                        expectedCalls = 3;
                    }

                    secondPage.complete(pageWithMore);
                }

                awaitPageChain_verifyDD();

                if (queryCalls_verifyDD(client) != expectedCalls) {
                    wrong.add(method + "/" + mode + ": " + queryCalls_verifyDD(client) + " query requests, expected " + expectedCalls);
                }

                if (mode.equals("notCompletedEarly")) {
                    final Object value = result.get(5, java.util.concurrent.TimeUnit.SECONDS);
                    assertEquals(3, value instanceof Dataset ? ((Dataset) value).size() : ((List<?>) value).size(), method);
                } else if (mode.equals("cancelBeforeFirstPage")) {
                    assertTrue(result.isCancelled(), method);
                }
            }
        }

        assertTrue(wrong.isEmpty(), wrong.toString());
    }

    /**
     * Pins that list/query report every failure point exactly like the plain thenComposeAsync stage they returned before the
     * result future was separated from the page chain: a client that throws on the first request makes the call itself throw;
     * a failed first or later page, a later page request that throws, and a row-conversion failure complete the future so that
     * get() throws ExecutionException(cause), join() throws CompletionException(cause), and exceptionally/handle/whenComplete
     * see that same CompletionException instance.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testListAndQueryFailureShapesMatchPlainDependentStage_verifyDD() throws Exception {
        final QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();
        final QueryResponse pageWithMore = QueryResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.fromS("1"))))
                .lastEvaluatedKey(Map.of("id", AttributeValue.fromS("1")))
                .build();
        final software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException failure = software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
                .builder()
                .message("gone")
                .build();
        final IllegalStateException thrown = new IllegalStateException("client threw");

        for (final String method : PAGINATING_QUERY_METHODS_verifyDD) {
            final DynamoDbAsyncClient syncThrowing = mock(DynamoDbAsyncClient.class);
            when(syncThrowing.query(any(QueryRequest.class))).thenThrow(thrown);
            assertSame(thrown,
                    assertThrows(IllegalStateException.class, () -> invokePaginatingQuery_verifyDD(new AsyncDynamoDBExecutor(syncThrowing), method, queryRequest)),
                    method);

            for (final String failurePoint : List.of("firstPageFails", "thirdPageFails", "secondRequestThrows")) {
                final DynamoDbAsyncClient client = mock(DynamoDbAsyncClient.class);

                if (failurePoint.equals("firstPageFails")) {
                    when(client.query(any(QueryRequest.class))).thenReturn(CompletableFuture.failedFuture(failure));
                } else if (failurePoint.equals("thirdPageFails")) {
                    when(client.query(any(QueryRequest.class))).thenReturn(CompletableFuture.completedFuture(pageWithMore),
                            CompletableFuture.completedFuture(pageWithMore), CompletableFuture.failedFuture(failure));
                } else {
                    when(client.query(any(QueryRequest.class))).thenReturn(CompletableFuture.completedFuture(pageWithMore)).thenThrow(thrown);
                }

                final Throwable cause = failurePoint.equals("secondRequestThrows") ? thrown : failure;
                final CompletableFuture<Object> result = (CompletableFuture<Object>) invokePaginatingQuery_verifyDD(new AsyncDynamoDBExecutor(client), method,
                        queryRequest);
                final String where = method + "/" + failurePoint;

                assertSame(cause, assertThrows(ExecutionException.class, () -> result.get(5, java.util.concurrent.TimeUnit.SECONDS)).getCause(), where);
                final java.util.concurrent.CompletionException joined = assertThrows(java.util.concurrent.CompletionException.class, result::join, where);
                assertSame(cause, joined.getCause(), where);
                assertSame(joined, result.exceptionally(e -> e).join(), where);
                assertSame(joined, result.handle((r, e) -> e).join(), where);
                final Throwable[] seen = new Throwable[1];
                result.whenComplete((r, e) -> seen[0] = e).exceptionally(e -> null).join();
                assertSame(joined, seen[0], where);
                assertTrue(result.isCompletedExceptionally() && !result.isCancelled(), where);
            }
        }

        // A row that cannot be converted on a later page fails the future the same way (single-value row type, 2 columns).
        final DynamoDbAsyncClient client = mock(DynamoDbAsyncClient.class);
        when(client.query(any(QueryRequest.class))).thenReturn(CompletableFuture.completedFuture(pageWithMore), CompletableFuture
                .completedFuture(QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.fromS("2"), "x", AttributeValue.fromS("y")))).build()));
        final CompletableFuture<List<String>> converted = new AsyncDynamoDBExecutor(client).list(queryRequest, String.class);
        final Throwable conversionFailure = assertThrows(ExecutionException.class, () -> converted.get(5, java.util.concurrent.TimeUnit.SECONDS)).getCause();
        assertTrue(conversionFailure instanceof IllegalArgumentException && conversionFailure.getMessage().startsWith("Column count must be 1"),
                String.valueOf(conversionFailure));
        assertSame(conversionFailure, assertThrows(java.util.concurrent.CompletionException.class, converted::join).getCause());
    }

    // ---- 2026-10-04 coverageDC ----

    // The Mapper-constructor message on its own: testMapper_ErrorMessagesNameClassOnce fails at its first (@Table) assertion on
    // HEAD, so it never proved this second site, which rendered "class java.lang.Integer is not an entity class ...".
    @Test
    public void testMapperConstructor_NonBeanMessageNamesClassOnce_coverageDC() {
        final IllegalArgumentException notBean = assertThrows(IllegalArgumentException.class, () -> asyncExecutor.mapper(Integer.class, "t", null));
        assertEquals("java.lang.Integer is not an entity class with getter/setter method", notBean.getMessage());
    }

    // The async Mapper builds entity keys with the sync v2 toKeyAttributeValue, so an empty byte[] key is named "byte[]" (was "[B")
    // and rejected before any request is sent.
    @Test
    public void testMapperEmptyBinaryKeyMessageNamesByteArrayReadably_coverageDC() {
        final BinaryKeyEntity_coverageDC entity = new BinaryKeyEntity_coverageDC();
        entity.setId(new byte[0]);

        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> asyncExecutor.mapper(BinaryKeyEntity_coverageDC.class, "t", NamingPolicy.CAMEL_CASE).getItem(entity));
        assertEquals("DynamoDB key attribute 'id' must be a non-empty String, finite Number, or non-empty binary value; received byte[]", e.getMessage());
        verifyNoInteractions(mockDynamoDbAsyncClient);
    }

    /**
     * Pins the unchanged single-page contract next to the separated result future: a request that already carries an
     * exclusiveStartKey is answered with exactly that page, even when the page has a LastEvaluatedKey, on every paginating entry point.
     */
    @Test
    public void testListAndQueryWithExclusiveStartKeyStillReturnOnlyThatPage_coverageDC() throws Exception {
        final QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").exclusiveStartKey(Map.of("id", AttributeValue.fromS("0"))).build();
        final QueryResponse pageWithMore = QueryResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.fromS("1"))))
                .lastEvaluatedKey(Map.of("id", AttributeValue.fromS("1")))
                .build();

        for (final String method : PAGINATING_QUERY_METHODS_verifyDD) {
            final DynamoDbAsyncClient client = mock(DynamoDbAsyncClient.class);
            when(client.query(any(QueryRequest.class))).thenReturn(CompletableFuture.completedFuture(pageWithMore),
                    CompletableFuture.completedFuture(QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.fromS("2")))).build()));

            final Object value = invokePaginatingQuery_verifyDD(new AsyncDynamoDBExecutor(client), method, queryRequest).get(5, java.util.concurrent.TimeUnit.SECONDS);

            assertEquals(1, value instanceof Dataset ? ((Dataset) value).size() : ((List<?>) value).size(), method);
            verify(client, times(1)).query(any(QueryRequest.class));
        }
    }

    public static class BinaryKeyEntity_coverageDC {
        @com.landawn.abacus.annotation.Id
        private byte[] id;

        public byte[] getId() {
            return id;
        }

        public void setId(final byte[] id) {
            this.id = id;
        }
    }
}
