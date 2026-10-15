package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.landawn.abacus.da.aws.dynamodb.DynamoDBExecutor.Filters;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.stream.Stream;

public class DynamoDBExecutorTest extends DynamoDBExecutorTestSupport {

    @Mock
    private DynamoDBMapperConfig mockMapperConfig;

    // Construction and configuration

    @org.junit.jupiter.api.Test
    public void test_streamScan_nullRequest_throwIAE() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> executor.stream((QueryRequest) null, com.landawn.abacus.da.Account.class));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> executor.scan((ScanRequest) null, com.landawn.abacus.da.Account.class));
    }

    @Test
    public void testConstructor_NullConfigOk() {
        AsyncExecutor exec = new AsyncExecutor(1, 2, 60L, TimeUnit.SECONDS);
        DynamoDBExecutor e = new DynamoDBExecutor(mockDynamoDBClient, null, exec);
        assertNotNull(e);
        assertNotNull(e.dynamoDBMapper());
    }

    @Test
    public void testConstructor_NullAsyncExecutorThrows() {
        assertThrows(IllegalArgumentException.class, () -> new DynamoDBExecutor(mockDynamoDBClient, mockMapperConfig, null));
    }

    @Test
    public void testConstructor_NullClientThrows() {
        assertThrows(IllegalArgumentException.class, () -> new DynamoDBExecutor(null));
    }

    @Test
    public void testConstructor_NullClientWithConfigAndAsyncThrows() {
        AsyncExecutor exec = new AsyncExecutor(1, 2, 60L, TimeUnit.SECONDS);
        assertThrows(IllegalArgumentException.class, () -> new DynamoDBExecutor(null, mockMapperConfig, exec));
    }

    @Test
    public void testConstructor_NullClientWithConfigThrows() {
        assertThrows(IllegalArgumentException.class, () -> new DynamoDBExecutor(null, mockMapperConfig));
    }

    @Test
    public void testConstructorWithAsyncExecutor() {
        AsyncExecutor asyncExecutor = new AsyncExecutor(2, 4, 60L, TimeUnit.SECONDS);
        DynamoDBExecutor executorWithAsync = new DynamoDBExecutor(mockDynamoDBClient, mockMapperConfig, asyncExecutor);
        assertNotNull(executorWithAsync);
    }

    @Test
    public void testConstructorWithConfig() {
        DynamoDBExecutor executorWithConfig = new DynamoDBExecutor(mockDynamoDBClient, mockMapperConfig);
        assertNotNull(executorWithConfig);
    }

    @Test
    public void testDynamoDBClient() {
        AmazonDynamoDBClient client = executor.dynamoDBClient();
        assertNotNull(client);
        assertEquals(mockDynamoDBClient, client);
    }

    // Attribute and key conversion

    @Test
    public void testtoAttributeValueUpdate() {
        AttributeValueUpdate result = DynamoDBExecutor.toAttributeValueUpdate("test");
        assertNotNull(result);
        assertEquals("test", result.getValue().getS());
        assertEquals(AttributeAction.PUT.toString(), result.getAction());
    }

    @Test
    public void testtoAttributeValueUpdateWithAction() {
        AttributeValueUpdate result = DynamoDBExecutor.toAttributeValueUpdate("test", AttributeAction.DELETE);
        assertNotNull(result);
        assertEquals("test", result.getValue().getS());
        assertEquals(AttributeAction.DELETE.toString(), result.getAction());

        result = DynamoDBExecutor.toAttributeValueUpdate(null, AttributeAction.DELETE);
        assertNotNull(result);
        assertNull(result.getValue());
        assertEquals(AttributeAction.DELETE.toString(), result.getAction());
    }

    // Row and entity mapping

    @Test
    public void testGetItem_ReadRowNullItem_EntityClass() {
        Map<String, AttributeValue> key = Map.of("id", new AttributeValue().withS("missing"));
        when(mockDynamoDBClient.getItem(eq("T"), any(Map.class))).thenReturn(new GetItemResult());

        TestEntity result = executor.getItem("T", key, TestEntity.class);
        assertNull(result);
    }

    @Test
    public void testGetItem_ReadRowNullItem_PrimitiveClass() {
        // null row + primitive rowClass should yield default value (0 for int)
        Map<String, AttributeValue> key = Map.of("id", new AttributeValue().withS("missing"));
        when(mockDynamoDBClient.getItem(eq("T"), any(Map.class))).thenReturn(new GetItemResult());

        Integer result = executor.getItem("T", key, int.class);
        assertEquals(0, result);
    }

    @Test
    public void testGetItem_ReadRowAsCollectionClass() {
        Map<String, AttributeValue> key = Map.of("id", new AttributeValue().withS("1"));
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("a", new AttributeValue().withS("x"));
        item.put("b", new AttributeValue().withS("y"));
        when(mockDynamoDBClient.getItem(eq("T"), any(Map.class))).thenReturn(new GetItemResult().withItem(item));

        List<?> result = executor.<List<?>> getItem("T", key, (Class<List<?>>) (Class<?>) List.class);
        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    public void testGetItem_ReadRowAsMapClass() {
        Map<String, AttributeValue> key = Map.of("id", new AttributeValue().withS("1"));
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("a", new AttributeValue().withS("x"));
        when(mockDynamoDBClient.getItem(eq("T"), any(Map.class))).thenReturn(new GetItemResult().withItem(item));

        Map<?, ?> result = executor.<Map<?, ?>> getItem("T", key, (Class<Map<?, ?>>) (Class<?>) Map.class);
        assertNotNull(result);
        assertEquals("x", result.get("a"));
    }

    @Test
    public void testGetItem_ReadRowAsObjectArrayClass() {
        Map<String, AttributeValue> key = Map.of("id", new AttributeValue().withS("1"));
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("a", new AttributeValue().withS("x"));
        item.put("b", new AttributeValue().withS("y"));
        when(mockDynamoDBClient.getItem(eq("T"), any(Map.class))).thenReturn(new GetItemResult().withItem(item));

        Object[] result = executor.getItem("T", key, Object[].class);
        assertNotNull(result);
        assertEquals(2, result.length);
    }

    @Test
    public void testGetItem_ReadRowAsSingleValueClass() {
        Map<String, AttributeValue> key = Map.of("id", new AttributeValue().withS("1"));
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("v", new AttributeValue().withS("hello"));
        when(mockDynamoDBClient.getItem(eq("T"), any(Map.class))).thenReturn(new GetItemResult().withItem(item));

        String result = executor.getItem("T", key, String.class);
        assertEquals("hello", result);
    }

    @Test
    public void testGetItem_ReadRowAsSingleValueClass_MultiColumnThrows() {
        Map<String, AttributeValue> key = Map.of("id", new AttributeValue().withS("1"));
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("a", new AttributeValue().withS("1"));
        item.put("b", new AttributeValue().withS("2"));
        when(mockDynamoDBClient.getItem(eq("T"), any(Map.class))).thenReturn(new GetItemResult().withItem(item));

        assertThrows(IllegalArgumentException.class, () -> executor.getItem("T", key, String.class));
    }

    // Consistent-read overload + ReadRow branches
    @Test
    public void testGetItemConsistentRead_ReadRowAsObjectArrayClass() {
        Map<String, AttributeValue> key = Map.of("id", new AttributeValue().withS("1"));
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("a", new AttributeValue().withS("x"));
        when(mockDynamoDBClient.getItem(eq("T"), any(Map.class), eq(Boolean.TRUE))).thenReturn(new GetItemResult().withItem(item));

        Object[] result = executor.getItem("T", key, Boolean.TRUE, Object[].class);
        assertNotNull(result);
        assertEquals(1, result.length);
    }

    // GetItemRequest overload + ReadRow Collection branch
    @Test
    public void testGetItemWithRequest_ReadRowAsCollectionClass() {
        GetItemRequest req = new GetItemRequest().withTableName("T");
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("a", new AttributeValue().withS("x"));
        when(mockDynamoDBClient.getItem(any(GetItemRequest.class))).thenReturn(new GetItemResult().withItem(item));

        List<?> result = executor.<List<?>> getItem(req, (Class<List<?>>) (Class<?>) List.class);
        assertEquals(1, result.size());
    }

    // Factories and insert operations

    @Test
    public void testPutItem() {
        String tableName = "TestTable";
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue().withS("123"));

        PutItemResult putItemResult = new PutItemResult();

        when(mockDynamoDBClient.putItem(tableName, item)).thenReturn(putItemResult);

        PutItemResult result = executor.putItem(tableName, item);

        assertNotNull(result);
    }

    @Test
    public void testPutItem_StringMap_PackagePrivate() {
        when(mockDynamoDBClient.putItem(eq("T"), any(Map.class))).thenReturn(new PutItemResult());
        PutItemResult result = executor.putItem("T", Map.of("id", new AttributeValue().withS("1")));
        assertNotNull(result);
    }

    @Test
    public void testPutItem_StringObjectEntity() {
        TestEntity entity = new TestEntity();
        entity.setId("po1");
        when(mockDynamoDBClient.putItem(eq("TestTable"), any(Map.class))).thenReturn(new PutItemResult());

        PutItemResult result = executor.putItem("TestTable", (Object) entity);
        assertNotNull(result);
    }

    @Test
    public void testPutItem_StringObjectEntityReturnValues() {
        TestEntity entity = new TestEntity();
        entity.setId("po2");
        when(mockDynamoDBClient.putItem(eq("TestTable"), any(Map.class), eq("ALL_OLD"))).thenReturn(new PutItemResult());

        PutItemResult result = executor.putItem("TestTable", (Object) entity, "ALL_OLD");
        assertNotNull(result);
    }

    // put/update/delete overloads taking request objects
    @Test
    public void testPutItemWithRequest() {
        PutItemRequest req = new PutItemRequest().withTableName("TestTable").withItem(Map.of("id", new AttributeValue().withS("1")));
        PutItemResult res = new PutItemResult();
        when(mockDynamoDBClient.putItem(req)).thenReturn(res);

        PutItemResult result = executor.putItem(req);
        assertNotNull(result);
    }

    @Test
    public void testPutItemWithReturnValues() {
        Map<String, AttributeValue> item = Map.of("id", new AttributeValue().withS("1"));
        PutItemResult res = new PutItemResult();
        when(mockDynamoDBClient.putItem("TestTable", item, "ALL_OLD")).thenReturn(res);

        PutItemResult result = executor.putItem("TestTable", item, "ALL_OLD");
        assertNotNull(result);
    }

    // Updates and deletes

    @Test
    public void testDeleteItem() {
        String tableName = "TestTable";
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", new AttributeValue().withS("123"));

        DeleteItemResult deleteItemResult = new DeleteItemResult();

        when(mockDynamoDBClient.deleteItem(tableName, key)).thenReturn(deleteItemResult);

        DeleteItemResult result = executor.deleteItem(tableName, key);

        assertNotNull(result);
    }

    @Test
    public void testDeleteItem_StringMap_PackagePrivate() {
        when(mockDynamoDBClient.deleteItem(eq("T"), any(Map.class))).thenReturn(new DeleteItemResult());
        DeleteItemResult result = executor.deleteItem("T", Map.of("id", new AttributeValue().withS("1")));
        assertNotNull(result);
    }

    @Test
    public void testDeleteItemWithRequest() {
        DeleteItemRequest req = new DeleteItemRequest().withTableName("TestTable").withKey(Map.of("id", new AttributeValue().withS("1")));
        DeleteItemResult res = new DeleteItemResult();
        when(mockDynamoDBClient.deleteItem(req)).thenReturn(res);

        DeleteItemResult result = executor.deleteItem(req);
        assertNotNull(result);
    }

    @Test
    public void testDeleteItemWithReturnValues() {
        Map<String, AttributeValue> key = Map.of("id", new AttributeValue().withS("1"));
        DeleteItemResult res = new DeleteItemResult();
        when(mockDynamoDBClient.deleteItem("TestTable", key, "ALL_OLD")).thenReturn(res);

        DeleteItemResult result = executor.deleteItem("TestTable", key, "ALL_OLD");
        assertNotNull(result);
    }

    @Test
    public void testUpdateItem() {
        String tableName = "TestTable";
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", new AttributeValue().withS("123"));
        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
        attributeUpdates.put("name", new AttributeValueUpdate().withValue(new AttributeValue().withS("updated")).withAction(AttributeAction.PUT));

        UpdateItemResult updateItemResult = new UpdateItemResult();

        when(mockDynamoDBClient.updateItem(tableName, key, attributeUpdates)).thenReturn(updateItemResult);

        UpdateItemResult result = executor.updateItem(tableName, key, attributeUpdates);

        assertNotNull(result);
    }

    @Test
    public void testUpdateItem_StringMapMap_PackagePrivate() {
        when(mockDynamoDBClient.updateItem(eq("T"), any(Map.class), any(Map.class))).thenReturn(new UpdateItemResult());
        UpdateItemResult result = executor.updateItem("T", Map.of("id", new AttributeValue().withS("1")),
                Map.of("name", new AttributeValueUpdate().withValue(new AttributeValue().withS("x"))));
        assertNotNull(result);
    }

    @Test
    public void testUpdateItemWithRequest() {
        UpdateItemRequest req = new UpdateItemRequest().withTableName("TestTable").withKey(Map.of("id", new AttributeValue().withS("1")));
        UpdateItemResult res = new UpdateItemResult();
        when(mockDynamoDBClient.updateItem(req)).thenReturn(res);

        UpdateItemResult result = executor.updateItem(req);
        assertNotNull(result);
    }

    @Test
    public void testUpdateItemWithReturnValues() {
        Map<String, AttributeValue> key = Map.of("id", new AttributeValue().withS("1"));
        Map<String, AttributeValueUpdate> upd = Map.of("name",
                new AttributeValueUpdate().withValue(new AttributeValue().withS("x")).withAction(AttributeAction.PUT));
        UpdateItemResult res = new UpdateItemResult();
        when(mockDynamoDBClient.updateItem("TestTable", key, upd, "ALL_NEW")).thenReturn(res);

        UpdateItemResult result = executor.updateItem("TestTable", key, upd, "ALL_NEW");
        assertNotNull(result);
    }

    // Queries and results

    @Test
    public void testBatchGetItem() {
        Map<String, KeysAndAttributes> requestItems = new HashMap<>();
        KeysAndAttributes keysAndAttributes = new KeysAndAttributes();
        List<Map<String, AttributeValue>> keys = new ArrayList<>();
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", new AttributeValue().withS("1"));
        keys.add(key);
        keysAndAttributes.setKeys(keys);
        requestItems.put("TestTable", keysAndAttributes);

        BatchGetItemResult batchGetItemResult = new BatchGetItemResult();
        Map<String, List<Map<String, AttributeValue>>> responses = new HashMap<>();
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue().withS("1"));
        items.add(item);
        responses.put("TestTable", items);
        batchGetItemResult.setResponses(responses);

        when(mockDynamoDBClient.batchGetItem(requestItems)).thenReturn(batchGetItemResult);

        Map<String, List<Map<String, Object>>> result = executor.batchGetItem(requestItems);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(1, result.get("TestTable").size());
    }

    @Test
    public void testBatchGetItem_NullArgsThrowIAE() {
        final Map<String, KeysAndAttributes> requestItems = Map.of("T",
                new KeysAndAttributes().withKeys(List.of(Map.of("id", new AttributeValue().withS("1")))));

        assertThrows(IllegalArgumentException.class, () -> executor.batchGetItem(requestItems, (Class<TestEntity>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.batchGetItem(requestItems, "TOTAL", (Class<TestEntity>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.batchGetItem((BatchGetItemRequest) null));
        assertThrows(IllegalArgumentException.class, () -> executor.batchGetItem((BatchGetItemRequest) null, TestEntity.class));
        assertThrows(IllegalArgumentException.class, () -> executor.batchGetItem(new BatchGetItemRequest(), (Class<TestEntity>) null));
    }

    @Test
    public void testBatchGetItem_WithClassDoesNotThrowOnEmpty() {
        Map<String, KeysAndAttributes> req = Map.of("TestTable", new KeysAndAttributes().withKeys(List.of(Map.of("id", new AttributeValue().withS("1")))));
        when(mockDynamoDBClient.batchGetItem(req)).thenReturn(new BatchGetItemResult());

        Map<String, List<TestEntity>> result = executor.batchGetItem(req, TestEntity.class);
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @Test
    public void testBatchGetItemMapWithReturnConsumedCapacityAndClass() {
        Map<String, KeysAndAttributes> requestItems = Map.of("TestTable",
                new KeysAndAttributes().withKeys(List.of(Map.of("id", new AttributeValue().withS("1")))));
        BatchGetItemResult res = new BatchGetItemResult().withResponses(Map.of("TestTable", List.of(Map.of("id", new AttributeValue().withS("1")))));
        when(mockDynamoDBClient.batchGetItem(requestItems, "TOTAL")).thenReturn(res);

        Map<String, List<TestEntity>> result = executor.batchGetItem(requestItems, "TOTAL", TestEntity.class);
        assertNotNull(result);
        assertEquals(1, result.get("TestTable").size());
        assertEquals("1", result.get("TestTable").get(0).getId());
    }

    @Test
    public void testBatchGetItemWithRequest() {
        BatchGetItemRequest req = new BatchGetItemRequest()
                .withRequestItems(Map.of("TestTable", new KeysAndAttributes().withKeys(List.of(Map.of("id", new AttributeValue().withS("1"))))));
        BatchGetItemResult res = new BatchGetItemResult().withResponses(Map.of("TestTable", List.of(Map.of("id", new AttributeValue().withS("1")))));
        when(mockDynamoDBClient.batchGetItem(req)).thenReturn(res);

        Map<String, List<Map<String, Object>>> result = executor.batchGetItem(req);
        assertNotNull(result);
        assertEquals(1, result.get("TestTable").size());
    }

    @Test
    public void testBatchGetItemWithRequestAndClass() {
        BatchGetItemRequest req = new BatchGetItemRequest()
                .withRequestItems(Map.of("TestTable", new KeysAndAttributes().withKeys(List.of(Map.of("id", new AttributeValue().withS("1"))))));
        BatchGetItemResult res = new BatchGetItemResult().withResponses(Map.of("TestTable", List.of(Map.of("id", new AttributeValue().withS("1")))));
        when(mockDynamoDBClient.batchGetItem(req)).thenReturn(res);

        Map<String, List<TestEntity>> result = executor.batchGetItem(req, TestEntity.class);
        assertNotNull(result);
        assertEquals(1, result.get("TestTable").size());
        assertEquals("1", result.get("TestTable").get(0).getId());
    }

    // batchGetItem overloads
    @Test
    public void testBatchGetItemWithReturnConsumedCapacity() {
        Map<String, KeysAndAttributes> requestItems = Map.of("TestTable",
                new KeysAndAttributes().withKeys(List.of(Map.of("id", new AttributeValue().withS("1")))));
        BatchGetItemResult res = new BatchGetItemResult().withResponses(Map.of("TestTable", List.of(Map.of("id", new AttributeValue().withS("1")))));
        when(mockDynamoDBClient.batchGetItem(requestItems, "TOTAL")).thenReturn(res);

        Map<String, List<Map<String, Object>>> result = executor.batchGetItem(requestItems, "TOTAL");
        assertNotNull(result);
        assertEquals(1, result.get("TestTable").size());
    }

    @Test
    public void testGetItem() {
        String tableName = "TestTable";
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", new AttributeValue().withS("123"));

        GetItemResult getItemResult = new GetItemResult();
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue().withS("123"));
        item.put("name", new AttributeValue().withS("test"));
        getItemResult.setItem(item);

        when(mockDynamoDBClient.getItem(tableName, key)).thenReturn(getItemResult);

        Map<String, Object> result = executor.getItem(tableName, key);

        assertNotNull(result);
        assertEquals("123", result.get("id"));
        assertEquals("test", result.get("name"));
    }

    @Test
    public void testGetItem_NullTargetClassThrowsIAE() {
        final Map<String, AttributeValue> key = Map.of("id", new AttributeValue().withS("1"));

        assertThrows(IllegalArgumentException.class, () -> executor.getItem("T", key, (Class<TestEntity>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.getItem("T", key, Boolean.TRUE, (Class<TestEntity>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.getItem(new GetItemRequest(), (Class<TestEntity>) null));
    }

    @Test
    public void testGetItem_SingleBinaryAttributeToByteArray() {
        final Map<String, AttributeValue> key = Map.of("id", new AttributeValue().withS("1"));
        final Map<String, AttributeValue> item = Map.of("data", new AttributeValue().withB(ByteBuffer.wrap(new byte[] { 7, 8 })));
        when(mockDynamoDBClient.getItem("T", key)).thenReturn(new GetItemResult().withItem(item));

        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[] { 7, 8 }, executor.getItem("T", key, byte[].class));
        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[] { 7, 8 }, DynamoDBExecutor.toValue(item.get("data"), byte[].class));
    }

    @Test
    public void testGetItemWithConsistentRead() {
        String tableName = "TestTable";
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", new AttributeValue().withS("123"));
        Boolean consistentRead = true;

        GetItemResult getItemResult = new GetItemResult();
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue().withS("123"));
        getItemResult.setItem(item);

        when(mockDynamoDBClient.getItem(tableName, key, consistentRead)).thenReturn(getItemResult);

        Map<String, Object> result = executor.getItem(tableName, key, consistentRead);

        assertNotNull(result);
        assertEquals("123", result.get("id"));
    }

    @Test
    public void testGetItemWithConsistentReadAndClass() {
        Map<String, AttributeValue> key = Map.of("id", new AttributeValue().withS("1"));
        GetItemResult res = new GetItemResult().withItem(Map.of("id", new AttributeValue().withS("1")));
        when(mockDynamoDBClient.getItem("TestTable", key, true)).thenReturn(res);

        TestEntity result = executor.getItem("TestTable", key, true, TestEntity.class);
        assertNotNull(result);
        assertEquals("1", result.getId());
    }

    // GetItemRequest-based overloads
    @Test
    public void testGetItemWithGetItemRequest() {
        GetItemRequest req = new GetItemRequest().withTableName("TestTable").withKey(Map.of("id", new AttributeValue().withS("1")));
        GetItemResult res = new GetItemResult().withItem(Map.of("id", new AttributeValue().withS("1"), "name", new AttributeValue().withS("n")));
        when(mockDynamoDBClient.getItem(req)).thenReturn(res);

        Map<String, Object> result = executor.getItem(req);
        assertNotNull(result);
        assertEquals("1", result.get("id"));
        assertEquals("n", result.get("name"));
    }

    @Test
    public void testGetItemWithGetItemRequestAndClass() {
        GetItemRequest req = new GetItemRequest().withTableName("TestTable").withKey(Map.of("id", new AttributeValue().withS("1")));
        GetItemResult res = new GetItemResult().withItem(Map.of("id", new AttributeValue().withS("1"), "name", new AttributeValue().withS("n")));
        when(mockDynamoDBClient.getItem(req)).thenReturn(res);

        TestEntity result = executor.getItem(req, TestEntity.class);
        assertNotNull(result);
        assertEquals("1", result.getId());
        assertEquals("n", result.getName());
    }

    @Test
    public void testGetItemWithTargetClass() {
        String tableName = "TestTable";
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", new AttributeValue().withS("123"));

        GetItemResult getItemResult = new GetItemResult();
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue().withS("123"));
        item.put("name", new AttributeValue().withS("test"));
        getItemResult.setItem(item);

        when(mockDynamoDBClient.getItem(tableName, key)).thenReturn(getItemResult);

        TestEntity result = executor.getItem(tableName, key, TestEntity.class);

        assertNotNull(result);
        assertEquals("123", result.getId());
        assertEquals("test", result.getName());
    }

    @Test
    public void testList() {
        QueryRequest queryRequest = new QueryRequest().withTableName("TestTable");

        QueryResult queryResult = new QueryResult();
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue().withS("123"));
        items.add(item);
        queryResult.setItems(items);

        when(mockDynamoDBClient.query(queryRequest)).thenReturn(queryResult);

        List<Map<String, Object>> result = executor.list(queryRequest);

        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testListQueryStream_NullArgsThrowIAE() {
        assertThrows(IllegalArgumentException.class, () -> executor.list((QueryRequest) null));
        assertThrows(IllegalArgumentException.class, () -> executor.list((QueryRequest) null, TestEntity.class));
        assertThrows(IllegalArgumentException.class, () -> executor.list(new QueryRequest(), (Class<TestEntity>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.query((QueryRequest) null));
        assertThrows(IllegalArgumentException.class, () -> executor.query((QueryRequest) null, TestEntity.class));
        assertThrows(IllegalArgumentException.class, () -> executor.stream((QueryRequest) null));
        assertThrows(IllegalArgumentException.class, () -> executor.stream(new QueryRequest(), (Class<TestEntity>) null));
    }

    @Test
    public void testListWithClassPaginates() {
        // list(QueryRequest, Class) covers the pagination branch as well.
        QueryRequest req = new QueryRequest().withTableName("TestTable");
        QueryResult page1 = new QueryResult().withItems(List.of(Map.of("id", new AttributeValue().withS("1"))))
                .withLastEvaluatedKey(Map.of("id", new AttributeValue().withS("1")));
        QueryResult page2 = new QueryResult().withItems(List.of(Map.of("id", new AttributeValue().withS("2"))));

        when(mockDynamoDBClient.query(any(QueryRequest.class))).thenReturn(page1, page2);

        List<TestEntity> result = executor.list(req, TestEntity.class);
        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    public void testQuery() {
        QueryRequest queryRequest = new QueryRequest().withTableName("TestTable");

        QueryResult queryResult = new QueryResult();
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue().withS("123"));
        items.add(item);
        queryResult.setItems(items);

        when(mockDynamoDBClient.query(queryRequest)).thenReturn(queryResult);

        Dataset result = executor.query(queryRequest);

        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testQueryWithClass_MapBranch() {
        QueryRequest req = new QueryRequest().withTableName("TestTable");
        QueryResult res = new QueryResult().withItems(List.of(Map.of("id", new AttributeValue().withS("1"))));
        when(mockDynamoDBClient.query(any(QueryRequest.class))).thenReturn(res);

        Dataset ds = executor.query(req, com.landawn.abacus.util.Clazz.PROPS_MAP);
        assertNotNull(ds);
        assertEquals(1, ds.size());
    }

    @Test
    public void testQueryWithClassEntityBranch() {
        // query(QueryRequest, Class) with a non-Map class goes through list(...) path.
        QueryRequest req = new QueryRequest().withTableName("TestTable");
        QueryResult res = new QueryResult().withItems(List.of(Map.of("id", new AttributeValue().withS("1"))));
        when(mockDynamoDBClient.query(any(QueryRequest.class))).thenReturn(res);

        Dataset ds = executor.query(req, TestEntity.class);
        assertNotNull(ds);
        assertEquals(1, ds.size());
    }

    @Test
    public void testScan() {
        String tableName = "TestTable";
        List<String> attributesToGet = List.of("id", "name");

        ScanResult scanResult = new ScanResult();
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue().withS("123"));
        items.add(item);
        scanResult.setItems(items);

        when(mockDynamoDBClient.scan(any(ScanRequest.class))).thenReturn(scanResult);

        Stream<Map<String, Object>> stream = executor.scan(tableName, attributesToGet);

        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    // AWS SDK v1 marshals an explicitly set empty AttributesToGet list as "[]", which DynamoDB rejects with a
    // ValidationException (the member must have at least one element). Mirrors the v2 executor: null and empty both
    // mean "retrieve all attributes".
    @Test
    public void testScan_EmptyAttributesToGetOmitsLegacyProjection() {
        when(mockDynamoDBClient.scan(any(ScanRequest.class))).thenReturn(new ScanResult().withItems(new ArrayList<>()));

        assertEquals(0, executor.scan("TestTable", List.of()).count());
        assertEquals(0, executor.scan("TestTable", new ArrayList<>(), Filters.eq("status", "active")).count());
        assertEquals(0, executor.scan("TestTable", List.of(), TestEntity.class).count());
        assertEquals(0, executor.scan("TestTable", new ArrayList<>(), Filters.eq("status", "active"), TestEntity.class).count());

        ArgumentCaptor<ScanRequest> captor = ArgumentCaptor.forClass(ScanRequest.class);
        verify(mockDynamoDBClient, times(4)).scan(captor.capture());

        for (ScanRequest sent : captor.getAllValues()) {
            assertEquals("TestTable", sent.getTableName());
            assertNull(sent.getAttributesToGet(), "empty attributesToGet must not be sent as an empty AttributesToGet list");
        }

        assertNull(captor.getAllValues().get(0).getScanFilter());
        assertNotNull(captor.getAllValues().get(1).getScanFilter());
        assertEquals(ComparisonOperator.EQ.toString(), captor.getAllValues().get(1).getScanFilter().get("status").getComparisonOperator());
    }

    @Test
    public void testScan_NullAndNonEmptyAttributesToGetPreserved() {
        when(mockDynamoDBClient.scan(any(ScanRequest.class))).thenReturn(new ScanResult().withItems(new ArrayList<>()));

        assertEquals(0, executor.scan("TestTable", (List<String>) null).count());
        assertEquals(0, executor.scan("TestTable", List.of("id", "name"), TestEntity.class).count());

        ArgumentCaptor<ScanRequest> captor = ArgumentCaptor.forClass(ScanRequest.class);
        verify(mockDynamoDBClient, times(2)).scan(captor.capture());

        assertNull(captor.getAllValues().get(0).getAttributesToGet());
        assertEquals(List.of("id", "name"), captor.getAllValues().get(1).getAttributesToGet());
    }

    @Test
    public void testScan_NullArgsThrowIAE() {
        assertThrows(IllegalArgumentException.class, () -> executor.scan((String) null, (List<String>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.scan((String) null, (Map<String, Condition>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.scan((String) null, (List<String>) null, (Map<String, Condition>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.scan((ScanRequest) null));
        assertThrows(IllegalArgumentException.class, () -> executor.scan("T", (List<String>) null, (Class<TestEntity>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.scan("T", (Map<String, Condition>) null, (Class<TestEntity>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.scan("T", (List<String>) null, (Map<String, Condition>) null, (Class<TestEntity>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.scan(new ScanRequest(), (Class<TestEntity>) null));
    }

    // scan overloads
    @Test
    public void testScanByTableNameAndScanFilter() {
        ScanResult res = new ScanResult().withItems(List.of(Map.of("id", new AttributeValue().withS("1"))));
        when(mockDynamoDBClient.scan(any(ScanRequest.class))).thenReturn(res);

        Stream<Map<String, Object>> stream = executor.scan("TestTable", Filters.eq("status", "active"));
        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testScanByTableNameAttrsAndScanFilter() {
        ScanResult res = new ScanResult().withItems(List.of(Map.of("id", new AttributeValue().withS("1"))));
        when(mockDynamoDBClient.scan(any(ScanRequest.class))).thenReturn(res);

        Stream<Map<String, Object>> stream = executor.scan("TestTable", List.of("id"), Filters.eq("status", "active"));
        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testScanWithAttrsAndClass() {
        ScanResult res = new ScanResult().withItems(List.of(Map.of("id", new AttributeValue().withS("1"))));
        when(mockDynamoDBClient.scan(any(ScanRequest.class))).thenReturn(res);

        Stream<TestEntity> stream = executor.scan("TestTable", List.of("id"), TestEntity.class);
        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testScanWithAttrsFilterAndClass() {
        ScanResult res = new ScanResult().withItems(List.of(Map.of("id", new AttributeValue().withS("1"))));
        when(mockDynamoDBClient.scan(any(ScanRequest.class))).thenReturn(res);

        Stream<TestEntity> stream = executor.scan("TestTable", List.of("id"), Filters.eq("status", "active"), TestEntity.class);
        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testScanWithFilterAndClass() {
        ScanResult res = new ScanResult().withItems(List.of(Map.of("id", new AttributeValue().withS("1"))));
        when(mockDynamoDBClient.scan(any(ScanRequest.class))).thenReturn(res);

        Stream<TestEntity> stream = executor.scan("TestTable", Filters.eq("status", "active"), TestEntity.class);
        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testStream() {
        QueryRequest queryRequest = new QueryRequest().withTableName("TestTable");

        QueryResult queryResult = new QueryResult();
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue().withS("123"));
        items.add(item);
        queryResult.setItems(items);

        when(mockDynamoDBClient.query(any(QueryRequest.class))).thenReturn(queryResult);

        Stream<Map<String, Object>> stream = executor.stream(queryRequest);

        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testStreamWithClass() {
        QueryRequest req = new QueryRequest().withTableName("TestTable");
        QueryResult res = new QueryResult().withItems(List.of(Map.of("id", new AttributeValue().withS("1"))));
        when(mockDynamoDBClient.query(any(QueryRequest.class))).thenReturn(res);

        Stream<TestEntity> stream = executor.stream(req, TestEntity.class);
        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    // Resource lifecycle

    @Test
    public void testClose() {
        executor.close();
        verify(mockDynamoDBClient, times(1)).shutdown();
    }

    // Additional contracts

    @Test
    public void testAsync() {
        AsyncDynamoDBExecutor asyncExecutor = executor.async();
        assertNotNull(asyncExecutor);
    }

    @Test
    public void testBatchWriteItem() {
        Map<String, List<WriteRequest>> requestItems = new HashMap<>();
        List<WriteRequest> writeRequests = new ArrayList<>();
        WriteRequest writeRequest = new WriteRequest();
        PutRequest putRequest = new PutRequest();
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue().withS("123"));
        putRequest.setItem(item);
        writeRequest.setPutRequest(putRequest);
        writeRequests.add(writeRequest);
        requestItems.put("TestTable", writeRequests);

        BatchWriteItemResult batchWriteItemResult = new BatchWriteItemResult();

        when(mockDynamoDBClient.batchWriteItem(requestItems)).thenReturn(batchWriteItemResult);

        BatchWriteItemResult result = executor.batchWriteItem(requestItems);

        assertNotNull(result);
    }

    @Test
    public void testBatchWriteItemWithRequest() {
        BatchWriteItemRequest req = new BatchWriteItemRequest()
                .withRequestItems(Map.of("TestTable", List.of(new WriteRequest(new PutRequest(Map.of("id", new AttributeValue().withS("1")))))));
        BatchWriteItemResult res = new BatchWriteItemResult();
        when(mockDynamoDBClient.batchWriteItem(req)).thenReturn(res);

        BatchWriteItemResult result = executor.batchWriteItem(req);
        assertNotNull(result);
    }

    @Test
    public void testDynamoDBMapper() {
        DynamoDBMapper mapper = executor.dynamoDBMapper();
        assertNotNull(mapper);
    }

    @Test
    public void testDynamoDBMapperWithConfig() {
        DynamoDBMapper mapper = executor.dynamoDBMapper(mockMapperConfig);
        assertNotNull(mapper);
    }

    @Test
    public void testRequestObjectOverloads_NullRequestThrowsIAEBeforeClientCall() {
        assertThrows(IllegalArgumentException.class, () -> executor.getItem((GetItemRequest) null));
        assertThrows(IllegalArgumentException.class, () -> executor.getItem((GetItemRequest) null, TestEntity.class));
        assertThrows(IllegalArgumentException.class, () -> executor.putItem((PutItemRequest) null));
        assertThrows(IllegalArgumentException.class, () -> executor.batchWriteItem((BatchWriteItemRequest) null));
        assertThrows(IllegalArgumentException.class, () -> executor.updateItem((UpdateItemRequest) null));
        assertThrows(IllegalArgumentException.class, () -> executor.deleteItem((DeleteItemRequest) null));

        org.mockito.Mockito.verifyNoInteractions(mockDynamoDBClient);
    }

    @Test
    public void testStaticHelpers_NullRequiredArgumentThrowsIAE() {
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.extractData((QueryResult) null));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.extractData((QueryResult) null, -1, 1));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.extractData((ScanResult) null));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.extractData((ScanResult) null, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toAttributeValueUpdate("v", null));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toAttributeValueUpdate(null, null));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toItem((java.util.Collection<?>) null));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toItem((java.util.Collection<?>) null, NamingPolicy.CAMEL_CASE));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toUpdateItem((java.util.Collection<?>) null));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toUpdateItem((java.util.Collection<?>) null, NamingPolicy.CAMEL_CASE));
    }
}
