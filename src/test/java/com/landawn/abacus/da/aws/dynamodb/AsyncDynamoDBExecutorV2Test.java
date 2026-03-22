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
        // Create a test entity class
        @com.landawn.abacus.annotation.Table(name = "TestTable")
        class TestEntity {
            @com.landawn.abacus.annotation.Id
            private String id;

            public String getId() {
                return id;
            }

            public void setId(String id) {
                this.id = id;
            }
        }

        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);
        assertNotNull(mapper);

        // Test caching - should return same instance
        AsyncDynamoDBExecutor.Mapper<TestEntity> secondMapper = asyncExecutor.mapper(TestEntity.class);
        assertSame(mapper, secondMapper);
    }

    @Test
    public void testMapperWithTargetEntityClassNoTableAnnotation() {
        class NoTableEntity {
            private String id;
        }

        assertThrows(IllegalArgumentException.class, () -> {
            asyncExecutor.mapper(NoTableEntity.class);
        });
    }

    @Test
    public void testMapperWithTableNameAndNamingPolicy() {
        class TestEntity {
            @com.landawn.abacus.annotation.Id
            private String id;

            public String getId() {
                return id;
            }

            public void setId(String id) {
                this.id = id;
            }
        }

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
        class TestEntity {
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
        class TestEntity {
            private String id;

            public String getId() {
                return id;
            }

            public void setId(String id) {
                this.id = id;
            }
        }

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
        @com.landawn.abacus.annotation.Table(name = "TestTable")
        class TestEntity {
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
        @com.landawn.abacus.annotation.Table(name = "TestTable")
        class TestEntity {
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
        @com.landawn.abacus.annotation.Table(name = "TestTable")
        class TestEntity {
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

        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);

        TestEntity entity = new TestEntity();
        entity.setId("123");
        entity.setName("Updated");

        UpdateItemResponse response = UpdateItemResponse.builder().build();

        when(mockDynamoDbAsyncClient.updateItem(any(UpdateItemRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<UpdateItemResponse> future = mapper.updateItem(entity);
        UpdateItemResponse result = future.get();

        assertNotNull(result);
    }

    @Test
    public void testMapperDeleteItem() throws ExecutionException, InterruptedException {
        @com.landawn.abacus.annotation.Table(name = "TestTable")
        class TestEntity {
            @com.landawn.abacus.annotation.Id
            private String id;

            public String getId() {
                return id;
            }

            public void setId(String id) {
                this.id = id;
            }
        }

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
        @com.landawn.abacus.annotation.Table(name = "TestTable")
        class TestEntity {
            @com.landawn.abacus.annotation.Id
            private String id;

            public String getId() {
                return id;
            }

            public void setId(String id) {
                this.id = id;
            }
        }

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
        @com.landawn.abacus.annotation.Table(name = "TestTable")
        class TestEntity {
            @com.landawn.abacus.annotation.Id
            private String id;

            public String getId() {
                return id;
            }

            public void setId(String id) {
                this.id = id;
            }
        }

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
        @com.landawn.abacus.annotation.Table(name = "TestTable")
        class TestEntity {
            @com.landawn.abacus.annotation.Id
            private String id;

            public String getId() {
                return id;
            }

            public void setId(String id) {
                this.id = id;
            }
        }

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
        @com.landawn.abacus.annotation.Table(name = "TestTable")
        class TestEntity {
            @com.landawn.abacus.annotation.Id
            private String id;

            public String getId() {
                return id;
            }

            public void setId(String id) {
                this.id = id;
            }
        }

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
        @com.landawn.abacus.annotation.Table(name = "TestTable")
        class TestEntity {
            @com.landawn.abacus.annotation.Id
            private String id;

            public String getId() {
                return id;
            }

            public void setId(String id) {
                this.id = id;
            }
        }

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
        @com.landawn.abacus.annotation.Table(name = "TestTable")
        class TestEntity {
            @com.landawn.abacus.annotation.Id
            private String id;

            public String getId() {
                return id;
            }

            public void setId(String id) {
                this.id = id;
            }
        }

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
        @com.landawn.abacus.annotation.Table(name = "TestTable")
        class TestEntity {
            @com.landawn.abacus.annotation.Id
            private String id;

            public String getId() {
                return id;
            }

            public void setId(String id) {
                this.id = id;
            }
        }

        AsyncDynamoDBExecutor.Mapper<TestEntity> mapper = asyncExecutor.mapper(TestEntity.class);

        ScanRequest scanRequest = ScanRequest.builder().tableName("TestTable").build();

        ScanResponse response = ScanResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("123").build()))).build();

        when(mockDynamoDbAsyncClient.scan(any(ScanRequest.class))).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<Stream<TestEntity>> future = mapper.scan(scanRequest);
        Stream<TestEntity> stream = future.get();

        assertNotNull(stream);
        assertEquals(1, stream.count());
    }
}