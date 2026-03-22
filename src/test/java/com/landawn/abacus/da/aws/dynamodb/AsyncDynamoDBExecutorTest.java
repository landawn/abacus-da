package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Throwables;
import com.landawn.abacus.util.stream.Stream;

public class AsyncDynamoDBExecutorTest extends TestBase {

    @Mock
    private DynamoDBExecutor mockDynamoDBExecutor;

    @Mock
    private AsyncExecutor mockAsyncExecutor;

    private AsyncDynamoDBExecutor asyncExecutor;

    @BeforeEach
    public void setUp() throws InterruptedException, ExecutionException {
        MockitoAnnotations.openMocks(this);
        asyncExecutor = new AsyncDynamoDBExecutor(mockDynamoDBExecutor, mockAsyncExecutor);

        // Configure mock async executor to execute synchronously for testing
        when(mockAsyncExecutor.execute(any(Throwables.Runnable.class))).thenAnswer(invocation -> {
            Runnable task = invocation.getArgument(0);
            task.run();
            return ContinuableFuture.completed(null);
        });
    }

    @Test
    public void testSync() throws InterruptedException, ExecutionException {
        DynamoDBExecutor syncExecutor = asyncExecutor.sync();
        assertNotNull(syncExecutor);
        assertEquals(mockDynamoDBExecutor, syncExecutor);
    }

    @Test
    public void testGetItemWithTableNameAndKey() throws InterruptedException, ExecutionException {
        String tableName = "TestTable";
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", new AttributeValue().withS("123"));

        Map<String, Object> expectedResult = new HashMap<>();
        expectedResult.put("id", "123");
        expectedResult.put("name", "Test");

        when(mockDynamoDBExecutor.getItem(tableName, key)).thenReturn(expectedResult);

        ContinuableFuture<Map<String, Object>> future = asyncExecutor.getItem(tableName, key);
        Map<String, Object> result = future.get();

        assertNotNull(result);
        assertEquals("123", result.get("id"));
        assertEquals("Test", result.get("name"));
        verify(mockDynamoDBExecutor, times(1)).getItem(tableName, key);
    }

    @Test
    public void testGetItemWithConsistentRead() throws InterruptedException, ExecutionException {
        String tableName = "TestTable";
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", new AttributeValue().withS("123"));
        Boolean consistentRead = true;

        Map<String, Object> expectedResult = new HashMap<>();
        expectedResult.put("id", "123");

        when(mockDynamoDBExecutor.getItem(tableName, key, consistentRead)).thenReturn(expectedResult);

        ContinuableFuture<Map<String, Object>> future = asyncExecutor.getItem(tableName, key, consistentRead);
        Map<String, Object> result = future.get();

        assertNotNull(result);
        assertEquals("123", result.get("id"));
        verify(mockDynamoDBExecutor, times(1)).getItem(tableName, key, consistentRead);
    }

    @Test
    public void testGetItemWithRequest() throws InterruptedException, ExecutionException {
        GetItemRequest request = new GetItemRequest().withTableName("TestTable").withKey(Map.of("id", new AttributeValue().withS("123")));

        Map<String, Object> expectedResult = new HashMap<>();
        expectedResult.put("id", "123");

        when(mockDynamoDBExecutor.getItem(request)).thenReturn(expectedResult);

        ContinuableFuture<Map<String, Object>> future = asyncExecutor.getItem(request);
        Map<String, Object> result = future.get();

        assertNotNull(result);
        assertEquals("123", result.get("id"));
        verify(mockDynamoDBExecutor, times(1)).getItem(request);
    }

    @Test
    public void testGetItemWithTargetClass() throws InterruptedException, ExecutionException {
        class TestEntity {
            private String id;
            private String name;

            public String getId() throws InterruptedException, ExecutionException {
                return id;
            }

            public void setId(String id) {
                this.id = id;
            }

            public String getName() throws InterruptedException, ExecutionException {
                return name;
            }

            public void setName(String name) {
                this.name = name;
            }
        }

        String tableName = "TestTable";
        Map<String, AttributeValue> key = Map.of("id", new AttributeValue().withS("123"));

        TestEntity expectedEntity = new TestEntity();
        expectedEntity.setId("123");
        expectedEntity.setName("Test");

        when(mockDynamoDBExecutor.getItem(tableName, key, TestEntity.class)).thenReturn(expectedEntity);

        ContinuableFuture<TestEntity> future = asyncExecutor.getItem(tableName, key, TestEntity.class);
        TestEntity result = future.get();

        assertNotNull(result);
        assertEquals("123", result.getId());
        assertEquals("Test", result.getName());
        verify(mockDynamoDBExecutor, times(1)).getItem(tableName, key, TestEntity.class);
    }

    @Test
    public void testGetItemWithConsistentReadAndTargetClass() throws InterruptedException, ExecutionException {
        class TestEntity {
            private String id;

            public String getId() throws InterruptedException, ExecutionException {
                return id;
            }

            public void setId(String id) {
                this.id = id;
            }
        }

        String tableName = "TestTable";
        Map<String, AttributeValue> key = Map.of("id", new AttributeValue().withS("123"));
        Boolean consistentRead = true;

        TestEntity expectedEntity = new TestEntity();
        expectedEntity.setId("123");

        when(mockDynamoDBExecutor.getItem(tableName, key, consistentRead, TestEntity.class)).thenReturn(expectedEntity);

        ContinuableFuture<TestEntity> future = asyncExecutor.getItem(tableName, key, consistentRead, TestEntity.class);
        TestEntity result = future.get();

        assertNotNull(result);
        assertEquals("123", result.getId());
        verify(mockDynamoDBExecutor, times(1)).getItem(tableName, key, consistentRead, TestEntity.class);
    }

    @Test
    public void testGetItemWithRequestAndTargetClass() throws InterruptedException, ExecutionException {
        class TestEntity {
            private String id;

            public String getId() throws InterruptedException, ExecutionException {
                return id;
            }

            public void setId(String id) {
                this.id = id;
            }
        }

        GetItemRequest request = new GetItemRequest().withTableName("TestTable").withKey(Map.of("id", new AttributeValue().withS("123")));

        TestEntity expectedEntity = new TestEntity();
        expectedEntity.setId("123");

        when(mockDynamoDBExecutor.getItem(request, TestEntity.class)).thenReturn(expectedEntity);

        ContinuableFuture<TestEntity> future = asyncExecutor.getItem(request, TestEntity.class);
        TestEntity result = future.get();

        assertNotNull(result);
        assertEquals("123", result.getId());
        verify(mockDynamoDBExecutor, times(1)).getItem(request, TestEntity.class);
    }

    @Test
    public void testBatchGetItem() throws InterruptedException, ExecutionException {
        Map<String, KeysAndAttributes> requestItems = new HashMap<>();
        KeysAndAttributes keysAndAttributes = new KeysAndAttributes();
        keysAndAttributes.setKeys(List.of(Map.of("id", new AttributeValue().withS("1"))));
        requestItems.put("TestTable", keysAndAttributes);

        Map<String, List<Map<String, Object>>> expectedResult = new HashMap<>();
        expectedResult.put("TestTable", List.of(Map.of("id", "1")));

        when(mockDynamoDBExecutor.batchGetItem(requestItems)).thenReturn(expectedResult);

        ContinuableFuture<Map<String, List<Map<String, Object>>>> future = asyncExecutor.batchGetItem(requestItems);
        Map<String, List<Map<String, Object>>> result = future.get();

        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey("TestTable"));
        verify(mockDynamoDBExecutor, times(1)).batchGetItem(requestItems);
    }

    @Test
    public void testBatchGetItemWithReturnConsumedCapacity() throws InterruptedException, ExecutionException {
        Map<String, KeysAndAttributes> requestItems = new HashMap<>();
        requestItems.put("TestTable", new KeysAndAttributes());
        String returnConsumedCapacity = "TOTAL";

        Map<String, List<Map<String, Object>>> expectedResult = new HashMap<>();

        when(mockDynamoDBExecutor.batchGetItem(requestItems, returnConsumedCapacity)).thenReturn(expectedResult);

        ContinuableFuture<Map<String, List<Map<String, Object>>>> future = asyncExecutor.batchGetItem(requestItems, returnConsumedCapacity);
        Map<String, List<Map<String, Object>>> result = future.get();

        assertNotNull(result);
        verify(mockDynamoDBExecutor, times(1)).batchGetItem(requestItems, returnConsumedCapacity);
    }

    @Test
    public void testBatchGetItemWithRequest() throws InterruptedException, ExecutionException {
        BatchGetItemRequest request = new BatchGetItemRequest();

        Map<String, List<Map<String, Object>>> expectedResult = new HashMap<>();

        when(mockDynamoDBExecutor.batchGetItem(request)).thenReturn(expectedResult);

        ContinuableFuture<Map<String, List<Map<String, Object>>>> future = asyncExecutor.batchGetItem(request);
        Map<String, List<Map<String, Object>>> result = future.get();

        assertNotNull(result);
        verify(mockDynamoDBExecutor, times(1)).batchGetItem(request);
    }

    @Test
    public void testBatchGetItemWithTargetClass() throws InterruptedException, ExecutionException {
        class TestEntity {
            private String id;

            public String getId() throws InterruptedException, ExecutionException {
                return id;
            }

            public void setId(String id) {
                this.id = id;
            }
        }

        Map<String, KeysAndAttributes> requestItems = new HashMap<>();
        requestItems.put("TestTable", new KeysAndAttributes());

        Map<String, List<TestEntity>> expectedResult = new HashMap<>();

        when(mockDynamoDBExecutor.batchGetItem(requestItems, TestEntity.class)).thenReturn(expectedResult);

        ContinuableFuture<Map<String, List<TestEntity>>> future = asyncExecutor.batchGetItem(requestItems, TestEntity.class);
        Map<String, List<TestEntity>> result = future.get();

        assertNotNull(result);
        verify(mockDynamoDBExecutor, times(1)).batchGetItem(requestItems, TestEntity.class);
    }

    @Test
    public void testPutItem() throws InterruptedException, ExecutionException {
        String tableName = "TestTable";
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue().withS("123"));

        PutItemResult expectedResult = new PutItemResult();

        when(mockDynamoDBExecutor.putItem(tableName, item)).thenReturn(expectedResult);

        ContinuableFuture<PutItemResult> future = asyncExecutor.putItem(tableName, item);
        PutItemResult result = future.get();

        assertNotNull(result);
        verify(mockDynamoDBExecutor, times(1)).putItem(tableName, item);
    }

    @Test
    public void testPutItemWithReturnValues() throws InterruptedException, ExecutionException {
        String tableName = "TestTable";
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue().withS("123"));
        String returnValues = "ALL_OLD";

        PutItemResult expectedResult = new PutItemResult();

        when(mockDynamoDBExecutor.putItem(tableName, item, returnValues)).thenReturn(expectedResult);

        ContinuableFuture<PutItemResult> future = asyncExecutor.putItem(tableName, item, returnValues);
        PutItemResult result = future.get();

        assertNotNull(result);
        verify(mockDynamoDBExecutor, times(1)).putItem(tableName, item, returnValues);
    }

    @Test
    public void testPutItemWithRequest() throws InterruptedException, ExecutionException {
        PutItemRequest request = new PutItemRequest();

        PutItemResult expectedResult = new PutItemResult();

        when(mockDynamoDBExecutor.putItem(request)).thenReturn(expectedResult);

        ContinuableFuture<PutItemResult> future = asyncExecutor.putItem(request);
        PutItemResult result = future.get();

        assertNotNull(result);
        verify(mockDynamoDBExecutor, times(1)).putItem(request);
    }

    @Test
    public void testBatchWriteItem() throws InterruptedException, ExecutionException {
        Map<String, List<WriteRequest>> requestItems = new HashMap<>();

        BatchWriteItemResult expectedResult = new BatchWriteItemResult();

        when(mockDynamoDBExecutor.batchWriteItem(requestItems)).thenReturn(expectedResult);

        ContinuableFuture<BatchWriteItemResult> future = asyncExecutor.batchWriteItem(requestItems);
        BatchWriteItemResult result = future.get();

        assertNotNull(result);
        verify(mockDynamoDBExecutor, times(1)).batchWriteItem(requestItems);
    }

    @Test
    public void testBatchWriteItemWithRequest() throws InterruptedException, ExecutionException {
        BatchWriteItemRequest request = new BatchWriteItemRequest();

        BatchWriteItemResult expectedResult = new BatchWriteItemResult();

        when(mockDynamoDBExecutor.batchWriteItem(request)).thenReturn(expectedResult);

        ContinuableFuture<BatchWriteItemResult> future = asyncExecutor.batchWriteItem(request);
        BatchWriteItemResult result = future.get();

        assertNotNull(result);
        verify(mockDynamoDBExecutor, times(1)).batchWriteItem(request);
    }

    @Test
    public void testUpdateItem() throws InterruptedException, ExecutionException {
        String tableName = "TestTable";
        Map<String, AttributeValue> key = Map.of("id", new AttributeValue().withS("123"));
        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();

        UpdateItemResult expectedResult = new UpdateItemResult();

        when(mockDynamoDBExecutor.updateItem(tableName, key, attributeUpdates)).thenReturn(expectedResult);

        ContinuableFuture<UpdateItemResult> future = asyncExecutor.updateItem(tableName, key, attributeUpdates);
        UpdateItemResult result = future.get();

        assertNotNull(result);
        verify(mockDynamoDBExecutor, times(1)).updateItem(tableName, key, attributeUpdates);
    }

    @Test
    public void testUpdateItemWithReturnValues() throws InterruptedException, ExecutionException {
        String tableName = "TestTable";
        Map<String, AttributeValue> key = Map.of("id", new AttributeValue().withS("123"));
        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
        String returnValues = "ALL_NEW";

        UpdateItemResult expectedResult = new UpdateItemResult();

        when(mockDynamoDBExecutor.updateItem(tableName, key, attributeUpdates, returnValues)).thenReturn(expectedResult);

        ContinuableFuture<UpdateItemResult> future = asyncExecutor.updateItem(tableName, key, attributeUpdates, returnValues);
        UpdateItemResult result = future.get();

        assertNotNull(result);
        verify(mockDynamoDBExecutor, times(1)).updateItem(tableName, key, attributeUpdates, returnValues);
    }

    @Test
    public void testUpdateItemWithRequest() throws InterruptedException, ExecutionException {
        UpdateItemRequest request = new UpdateItemRequest();

        UpdateItemResult expectedResult = new UpdateItemResult();

        when(mockDynamoDBExecutor.updateItem(request)).thenReturn(expectedResult);

        ContinuableFuture<UpdateItemResult> future = asyncExecutor.updateItem(request);
        UpdateItemResult result = future.get();

        assertNotNull(result);
        verify(mockDynamoDBExecutor, times(1)).updateItem(request);
    }

    @Test
    public void testDeleteItem() throws InterruptedException, ExecutionException {
        String tableName = "TestTable";
        Map<String, AttributeValue> key = Map.of("id", new AttributeValue().withS("123"));

        DeleteItemResult expectedResult = new DeleteItemResult();

        when(mockDynamoDBExecutor.deleteItem(tableName, key)).thenReturn(expectedResult);

        ContinuableFuture<DeleteItemResult> future = asyncExecutor.deleteItem(tableName, key);
        DeleteItemResult result = future.get();

        assertNotNull(result);
        verify(mockDynamoDBExecutor, times(1)).deleteItem(tableName, key);
    }

    @Test
    public void testDeleteItemWithReturnValues() throws InterruptedException, ExecutionException {
        String tableName = "TestTable";
        Map<String, AttributeValue> key = Map.of("id", new AttributeValue().withS("123"));
        String returnValues = "ALL_OLD";

        DeleteItemResult expectedResult = new DeleteItemResult();

        when(mockDynamoDBExecutor.deleteItem(tableName, key, returnValues)).thenReturn(expectedResult);

        ContinuableFuture<DeleteItemResult> future = asyncExecutor.deleteItem(tableName, key, returnValues);
        DeleteItemResult result = future.get();

        assertNotNull(result);
        verify(mockDynamoDBExecutor, times(1)).deleteItem(tableName, key, returnValues);
    }

    @Test
    public void testDeleteItemWithRequest() throws InterruptedException, ExecutionException {
        DeleteItemRequest request = new DeleteItemRequest();

        DeleteItemResult expectedResult = new DeleteItemResult();

        when(mockDynamoDBExecutor.deleteItem(request)).thenReturn(expectedResult);

        ContinuableFuture<DeleteItemResult> future = asyncExecutor.deleteItem(request);
        DeleteItemResult result = future.get();

        assertNotNull(result);
        verify(mockDynamoDBExecutor, times(1)).deleteItem(request);
    }

    @Test
    public void testList() throws InterruptedException, ExecutionException {
        QueryRequest queryRequest = new QueryRequest();

        List<Map<String, Object>> expectedResult = new ArrayList<>();
        expectedResult.add(Map.of("id", "123"));

        when(mockDynamoDBExecutor.list(queryRequest)).thenReturn(expectedResult);

        ContinuableFuture<List<Map<String, Object>>> future = asyncExecutor.list(queryRequest);
        List<Map<String, Object>> result = future.get();

        assertNotNull(result);
        assertEquals(1, result.size());
        verify(mockDynamoDBExecutor, times(1)).list(queryRequest);
    }

    @Test
    public void testListWithTargetClass() throws InterruptedException, ExecutionException {
        class TestEntity {
            private String id;

            public String getId() throws InterruptedException, ExecutionException {
                return id;
            }

            public void setId(String id) {
                this.id = id;
            }
        }

        QueryRequest queryRequest = new QueryRequest();

        List<TestEntity> expectedResult = new ArrayList<>();
        TestEntity entity = new TestEntity();
        entity.setId("123");
        expectedResult.add(entity);

        when(mockDynamoDBExecutor.list(queryRequest, TestEntity.class)).thenReturn(expectedResult);

        ContinuableFuture<List<TestEntity>> future = asyncExecutor.list(queryRequest, TestEntity.class);
        List<TestEntity> result = future.get();

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("123", result.get(0).getId());
        verify(mockDynamoDBExecutor, times(1)).list(queryRequest, TestEntity.class);
    }

    @Test
    public void testQuery() throws InterruptedException, ExecutionException {
        QueryRequest queryRequest = new QueryRequest();

        Dataset expectedResult = N.newEmptyDataset();

        when(mockDynamoDBExecutor.query(queryRequest)).thenReturn(expectedResult);

        ContinuableFuture<Dataset> future = asyncExecutor.query(queryRequest);
        Dataset result = future.get();

        assertNotNull(result);
        verify(mockDynamoDBExecutor, times(1)).query(queryRequest);
    }

    @Test
    public void testQueryWithTargetClass() throws InterruptedException, ExecutionException {
        class TestEntity {
            private String id;

            public String getId() throws InterruptedException, ExecutionException {
                return id;
            }

            public void setId(String id) {
                this.id = id;
            }
        }

        QueryRequest queryRequest = new QueryRequest();

        Dataset expectedResult = N.newEmptyDataset();

        when(mockDynamoDBExecutor.query(queryRequest, TestEntity.class)).thenReturn(expectedResult);

        ContinuableFuture<Dataset> future = asyncExecutor.query(queryRequest, TestEntity.class);
        Dataset result = future.get();

        assertNotNull(result);
        verify(mockDynamoDBExecutor, times(1)).query(queryRequest, TestEntity.class);
    }

    @Test
    public void testStream() throws InterruptedException, ExecutionException {
        QueryRequest queryRequest = new QueryRequest();

        Stream<Map<String, Object>> expectedResult = Stream.just(Map.of("id", "123"));

        when(mockDynamoDBExecutor.stream(queryRequest)).thenReturn(expectedResult);

        ContinuableFuture<Stream<Map<String, Object>>> future = asyncExecutor.stream(queryRequest);
        Stream<Map<String, Object>> result = future.get();

        assertNotNull(result);
        assertEquals(1, result.count());
        verify(mockDynamoDBExecutor, times(1)).stream(queryRequest);
    }

    @Test
    public void testStreamWithTargetClass() throws InterruptedException, ExecutionException {
        class TestEntity {
            private String id;

            public String getId() throws InterruptedException, ExecutionException {
                return id;
            }

            public void setId(String id) {
                this.id = id;
            }
        }

        QueryRequest queryRequest = new QueryRequest();

        TestEntity entity = new TestEntity();
        entity.setId("123");
        Stream<TestEntity> expectedResult = Stream.of(entity);

        when(mockDynamoDBExecutor.stream(queryRequest, TestEntity.class)).thenReturn(expectedResult);

        ContinuableFuture<Stream<TestEntity>> future = asyncExecutor.stream(queryRequest, TestEntity.class);
        Stream<TestEntity> result = future.get();

        assertNotNull(result);
        assertEquals(1, result.count());
        verify(mockDynamoDBExecutor, times(1)).stream(queryRequest, TestEntity.class);
    }

    @Test
    public void testScanWithTableNameAndAttributesToGet() throws InterruptedException, ExecutionException {
        String tableName = "TestTable";
        List<String> attributesToGet = List.of("id", "name");

        Stream<Map<String, Object>> expectedResult = Stream.just(Map.of("id", "123"));

        when(mockDynamoDBExecutor.scan(tableName, attributesToGet)).thenReturn(expectedResult);

        ContinuableFuture<Stream<Map<String, Object>>> future = asyncExecutor.scan(tableName, attributesToGet);
        Stream<Map<String, Object>> result = future.get();

        assertNotNull(result);
        assertEquals(1, result.count());
        verify(mockDynamoDBExecutor, times(1)).scan(tableName, attributesToGet);
    }

    @Test
    public void testScanWithTableNameAndScanFilter() throws InterruptedException, ExecutionException {
        String tableName = "TestTable";
        Map<String, Condition> scanFilter = new HashMap<>();

        Stream<Map<String, Object>> expectedResult = Stream.just(Map.of("id", "123"));

        when(mockDynamoDBExecutor.scan(tableName, scanFilter)).thenReturn(expectedResult);

        ContinuableFuture<Stream<Map<String, Object>>> future = asyncExecutor.scan(tableName, scanFilter);
        Stream<Map<String, Object>> result = future.get();

        assertNotNull(result);
        assertEquals(1, result.count());
        verify(mockDynamoDBExecutor, times(1)).scan(tableName, scanFilter);
    }

    @Test
    public void testScanWithAllParameters() throws InterruptedException, ExecutionException {
        String tableName = "TestTable";
        List<String> attributesToGet = List.of("id", "name");
        Map<String, Condition> scanFilter = new HashMap<>();

        Stream<Map<String, Object>> expectedResult = Stream.just(Map.of("id", "123"));

        when(mockDynamoDBExecutor.scan(tableName, attributesToGet, scanFilter)).thenReturn(expectedResult);

        ContinuableFuture<Stream<Map<String, Object>>> future = asyncExecutor.scan(tableName, attributesToGet, scanFilter);
        Stream<Map<String, Object>> result = future.get();

        assertNotNull(result);
        assertEquals(1, result.count());
        verify(mockDynamoDBExecutor, times(1)).scan(tableName, attributesToGet, scanFilter);
    }

    @Test
    public void testScanWithRequest() throws InterruptedException, ExecutionException {
        ScanRequest scanRequest = new ScanRequest();

        Stream<Map<String, Object>> expectedResult = Stream.just(Map.of("id", "123"));

        when(mockDynamoDBExecutor.scan(scanRequest)).thenReturn(expectedResult);

        ContinuableFuture<Stream<Map<String, Object>>> future = asyncExecutor.scan(scanRequest);
        Stream<Map<String, Object>> result = future.get();

        assertNotNull(result);
        assertEquals(1, result.count());
        verify(mockDynamoDBExecutor, times(1)).scan(scanRequest);
    }

    @Test
    public void testScanWithTargetClass() throws InterruptedException, ExecutionException {
        class TestEntity {
            private String id;

            public String getId() throws InterruptedException, ExecutionException {
                return id;
            }

            public void setId(String id) {
                this.id = id;
            }
        }

        String tableName = "TestTable";
        List<String> attributesToGet = List.of("id");

        TestEntity entity = new TestEntity();
        entity.setId("123");
        Stream<TestEntity> expectedResult = Stream.of(entity);

        when(mockDynamoDBExecutor.scan(tableName, attributesToGet, TestEntity.class)).thenReturn(expectedResult);

        ContinuableFuture<Stream<TestEntity>> future = asyncExecutor.scan(tableName, attributesToGet, TestEntity.class);
        Stream<TestEntity> result = future.get();

        assertNotNull(result);
        assertEquals(1, result.count());
        verify(mockDynamoDBExecutor, times(1)).scan(tableName, attributesToGet, TestEntity.class);
    }
}