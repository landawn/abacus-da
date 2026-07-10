package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
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
}
