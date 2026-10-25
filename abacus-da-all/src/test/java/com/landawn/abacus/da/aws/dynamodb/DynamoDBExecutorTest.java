package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
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

        PutItemResult result = executor.putItem("TestTable", entity);
        assertNotNull(result);
    }

    @Test
    public void testPutItem_StringObjectEntityReturnValues() {
        TestEntity entity = new TestEntity();
        entity.setId("po2");
        when(mockDynamoDBClient.putItem(eq("TestTable"), any(Map.class), eq("ALL_OLD"))).thenReturn(new PutItemResult());

        PutItemResult result = executor.putItem("TestTable", entity, "ALL_OLD");
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

    // -- asProps normal cases --

    @Test
    public void testasPropsWithKeyValuePairs() {
        Object[] input = new Object[] { "name", "John", "age", 30 };
        Map<String, Object> props = DynamoDBExecutorBase.asProps(input);

        assertNotNull(props);
        assertEquals(2, props.size());
        assertEquals("John", props.get("name"));
        assertEquals(30, props.get("age"));
    }

    @Test
    public void testasPropsPreservesInsertionOrder() {
        // Implementation uses LinkedHashMap so insertion order is preserved
        Object[] input = new Object[] { "z", 1, "a", 2, "m", 3 };
        Map<String, Object> props = DynamoDBExecutorBase.asProps(input);

        assertTrue(props instanceof LinkedHashMap);
        assertEquals(3, props.size());

        Object[] keys = props.keySet().toArray();
        assertEquals("z", keys[0]);
        assertEquals("a", keys[1]);
        assertEquals("m", keys[2]);
    }

    @Test
    public void testasPropsWithEmptyArray() {
        Map<String, Object> props = DynamoDBExecutorBase.asProps(new Object[0]);
        assertNotNull(props);
        assertEquals(0, props.size());
    }

    @Test
    public void testasPropsWithNullArray() {
        // null array returns an empty LinkedHashMap (not null)
        Map<String, Object> props = DynamoDBExecutorBase.asProps(null);
        assertNotNull(props);
        assertEquals(0, props.size());
        assertTrue(props instanceof LinkedHashMap);
    }

    // -- asProps error and edge cases --

    @Test
    public void testasPropsWithOddLengthThrows() {
        Object[] input = new Object[] { "name", "John", "age" };
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutorBase.asProps(input));
    }

    @Test
    public void testasPropsWithNullValue() {
        // null value is allowed and preserved
        Object[] input = new Object[] { "key", null };
        Map<String, Object> props = DynamoDBExecutorBase.asProps(input);

        assertEquals(1, props.size());
        assertTrue(props.containsKey("key"));
        assertNull(props.get("key"));
    }

    @Test
    public void testasPropsWithNonStringKeyThrows() {
        // Property names must be Strings; a non-String name (here an Integer at index 0) is rejected.
        Object[] input = new Object[] { 100, "hundred", true, "yes" };
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutorBase.asProps(input));
    }

    @Test
    public void testasPropsWithMixedValueTypes() {
        Object[] input = new Object[] { "str", "abc", "int", 42, "bool", true, "dbl", 3.14 };
        Map<String, Object> props = DynamoDBExecutorBase.asProps(input);

        assertEquals(4, props.size());
        assertEquals("abc", props.get("str"));
        assertEquals(42, props.get("int"));
        assertEquals(true, props.get("bool"));
        assertEquals(3.14, props.get("dbl"));
    }

    @Test
    public void testasPropsSingleEntry() {
        Object[] input = new Object[] { "only", "value" };
        Map<String, Object> props = DynamoDBExecutorBase.asProps(input);

        assertEquals(1, props.size());
        assertEquals("value", props.get("only"));
    }

    // ---- 2026-10-19 sliceA ----

    @Test
    public void testList_ConvertsEveryPageInOrderIncludingItemlessPages() {
        // list(QueryRequest, Class) converts pages straight into one result list; a page without an Items member
        // (null, as for Select=COUNT) contributes nothing and the client's first response is not modified.
        final QueryResult page1 = new QueryResult().withItems(List.of(Map.of("id", new AttributeValue().withS("1"))))
                .withLastEvaluatedKey(Map.of("id", new AttributeValue().withS("1")));
        final QueryResult page2 = new QueryResult().withLastEvaluatedKey(Map.of("id", new AttributeValue().withS("1b")));
        final QueryResult page3 = new QueryResult()
                .withItems(List.of(Map.of("id", new AttributeValue().withS("2")), Map.of("id", new AttributeValue().withS("3"))));
        when(mockDynamoDBClient.query(any(QueryRequest.class))).thenReturn(page1, page2, page3);

        final List<TestEntity> result = executor.list(new QueryRequest().withTableName("TestTable"), TestEntity.class);

        assertEquals(List.of("1", "2", "3"), result.stream().map(TestEntity::getId).toList());
        assertEquals(1, page1.getItems().size());
        result.add(new TestEntity()); // the aggregated result stays mutable
        verify(mockDynamoDBClient, times(3)).query(any(QueryRequest.class));
    }

    @Test
    public void testList_SinglePageWithoutItemsIsEmptyAndMutable() {
        when(mockDynamoDBClient.query(any(QueryRequest.class))).thenReturn(new QueryResult());

        final List<Map<String, Object>> result = executor.list(new QueryRequest().withTableName("TestTable"));

        assertTrue(result.isEmpty());
        result.add(new HashMap<>());
        assertEquals(1, result.size());
    }

    @Test
    public void testToItemAndToUpdateItem_MapInputKeepsOrderBeyondDefaultCapacity() {
        final Map<String, Object> source = new LinkedHashMap<>();

        for (int i = 20; i > 0; i--) {
            source.put("attr" + i, i);
        }

        final Map<String, AttributeValue> item = DynamoDBExecutor.toItem(source);
        final Map<String, AttributeValueUpdate> updates = DynamoDBExecutor.toUpdateItem(source, NamingPolicy.SCREAMING_SNAKE_CASE);

        assertEquals(new ArrayList<>(source.keySet()), new ArrayList<>(item.keySet()));
        assertEquals("20", item.get("attr20").getN());
        assertEquals(20, updates.size());
        assertEquals("ATTR1", new ArrayList<>(updates.keySet()).get(19));
        item.put("extra", new AttributeValue().withS("x")); // results stay mutable
        assertEquals(21, item.size());
    }

    @Test
    public void testToItem_BeanSkipsNullPropertiesAndAppliesNamingPolicy() {
        final TestEntity entity = new TestEntity();
        entity.setId("1");
        entity.setFirstName("Ann");

        final Map<String, AttributeValue> item = DynamoDBExecutor.toItem(entity, NamingPolicy.SNAKE_CASE);

        assertEquals(List.of("id", "first_name"), new ArrayList<>(item.keySet()));
        assertEquals("Ann", item.get("first_name").getS());
    }

    @Test
    public void testToMap_NestedMapKeepsEntryOrderAndIsMutable() {
        final Map<String, AttributeValue> nested = new LinkedHashMap<>();

        for (int i = 15; i > 0; i--) {
            nested.put("k" + i, new AttributeValue().withN(String.valueOf(i)));
        }

        final Map<String, Object> map = DynamoDBExecutor.toMap(Map.of("nested", new AttributeValue().withM(nested)));
        @SuppressWarnings("unchecked")
        final Map<String, Object> converted = (Map<String, Object>) map.get("nested");

        assertEquals(new ArrayList<>(nested.keySet()), new ArrayList<>(converted.keySet()));
        assertEquals("15", converted.get("k15"));
        converted.put("added", 1);
        assertEquals(15, nested.size());
    }

    @Test
    public void testToAttributeValue_RejectsNonFiniteValuesOfEveryNumericType() {
        final java.util.concurrent.atomic.DoubleAdder adder = new java.util.concurrent.atomic.DoubleAdder();
        adder.add(Double.NEGATIVE_INFINITY);
        final List<Object> nonFinite = List.of(Double.NaN, Float.POSITIVE_INFINITY, com.landawn.abacus.util.MutableDouble.of(Double.NaN),
                com.landawn.abacus.util.MutableFloat.of(Float.POSITIVE_INFINITY), adder);

        // HEAD only checked Double/Float, so the mutable and adder types were written as N=NaN / N=Infinity.
        org.junit.jupiter.api.Assertions.assertAll(nonFinite.stream().<org.junit.jupiter.api.function.Executable> map(value -> () -> {
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toAttributeValue(value));
            assertTrue(e.getMessage().startsWith("DynamoDB numbers must be finite"), e.getMessage());
            assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("k", value));
            assertThrows(IllegalArgumentException.class, () -> Filters.eq("k", value));
        }).toList());

        assertEquals("1.5", DynamoDBExecutor.toAttributeValue(com.landawn.abacus.util.MutableDouble.of(1.5)).getN());
        assertEquals("-2.5", DynamoDBExecutor.toAttributeValue(-2.5d).getN());
    }

    @Test
    public void testExtractData_ColumnsAreUnionOfSelectedRowsWithNullCells() {
        // Map.of iteration order is unspecified, so the multi-key row uses a LinkedHashMap to make the column order deterministic.
        final Map<String, AttributeValue> lastItem = new java.util.LinkedHashMap<>();
        lastItem.put("a", new AttributeValue().withS("3"));
        lastItem.put("c", new AttributeValue().withBOOL(true));
        final List<Map<String, AttributeValue>> items = List.of(Map.of("a", new AttributeValue().withS("1")), Map.of("b", new AttributeValue().withN("2")),
                lastItem);

        final Dataset ds = DynamoDBExecutor.extractData(new QueryResult().withItems(items), 1, 2);

        assertEquals(List.of("b", "a", "c"), ds.columnNames());
        assertEquals(2, ds.size());
        assertEquals(java.util.Arrays.asList("2", null), ds.getColumn("b"));
        assertEquals(java.util.Arrays.asList(null, "3"), ds.getColumn("a"));
        assertEquals(java.util.Arrays.asList(null, true), ds.getColumn("c"));
        ds.addColumn("d", java.util.Arrays.asList(1, 2)); // the Dataset owns mutable storage
        assertEquals(4, ds.columnCount());
    }

    // ---- 2026-10-19 ddbcopy ----

    public static class DdbCopyBean {
        private List<Object> objs;
        @SuppressWarnings("rawtypes")
        private java.util.Collection raw;
        private Map<String, Object> props;

        public List<Object> getObjs() {
            return objs;
        }

        public void setObjs(final List<Object> objs) {
            this.objs = objs;
        }

        @SuppressWarnings("rawtypes")
        public java.util.Collection getRaw() {
            return raw;
        }

        @SuppressWarnings("rawtypes")
        public void setRaw(final java.util.Collection raw) {
            this.raw = raw;
        }

        public Map<String, Object> getProps() {
            return props;
        }

        public void setProps(final Map<String, Object> props) {
            this.props = props;
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testToValue_SetListAndEmptyContainerValuesAreFreshMutableCopies() {
        // HEAD returned the AttributeValue's own SS/NS/BS list and its own empty L/M, so mutating a converted value
        // mutated the source item (and every other entity/map converted from it).
        final AttributeValue ss = new AttributeValue().withSS("a", "b");
        final AttributeValue ns = new AttributeValue().withNS("1", "2.5");
        final AttributeValue bs = new AttributeValue().withBS(ByteBuffer.wrap(new byte[] { 1 }));
        final AttributeValue emptyL = new AttributeValue().withL(new ArrayList<>());
        final AttributeValue emptyM = new AttributeValue().withM(new HashMap<>());
        final AttributeValue emptySS = new AttributeValue().withSS(new ArrayList<String>());
        final AttributeValue nestedL = new AttributeValue().withL(new AttributeValue().withSS("n"));
        final AttributeValue nestedM = new AttributeValue().withM(Map.of("k", new AttributeValue().withNS("7")));

        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("ss", ss);
        item.put("ns", ns);
        item.put("bs", bs);
        item.put("emptyL", emptyL);
        item.put("emptyM", emptyM);
        item.put("emptySS", emptySS);
        item.put("nestedL", nestedL);
        item.put("nestedM", nestedM);

        final Map<String, Object> map = DynamoDBExecutor.toMap(item);
        ((List) map.get("ss")).add("x");
        ((List) map.get("ns")).add("9");
        ((List) map.get("bs")).add(ByteBuffer.wrap(new byte[] { 2 }));
        ((List) map.get("emptyL")).add("x");
        ((Map) map.get("emptyM")).put("x", 1);
        ((List) map.get("emptySS")).add("x");
        ((List) ((List) map.get("nestedL")).get(0)).add("x");
        ((List) ((Map) map.get("nestedM")).get("k")).add("8");

        org.junit.jupiter.api.Assertions.assertAll( //
                () -> assertEquals(List.of("a", "b"), ss.getSS()), //
                () -> assertEquals(List.of("1", "2.5"), ns.getNS()), // NS elements stay number text
                () -> assertEquals(List.of("1", "2.5", "9"), map.get("ns")), //
                () -> assertEquals(1, bs.getBS().size()), //
                () -> assertEquals(ByteBuffer.wrap(new byte[] { 1 }), ((List) map.get("bs")).get(0)), //
                () -> assertTrue(emptyL.getL().isEmpty()), //
                () -> assertTrue(emptyM.getM().isEmpty()), //
                () -> assertTrue(emptySS.getSS().isEmpty()), //
                () -> assertEquals(List.of("n"), nestedL.getL().get(0).getSS()), //
                () -> assertEquals(List.of("7"), nestedM.getM().get("k").getNS()));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testToEntity_EntitiesMappedFromSameItemDoNotShareCollections() {
        final AttributeValue ss = new AttributeValue().withSS("a", "b");
        final AttributeValue ns = new AttributeValue().withNS("1");
        final AttributeValue emptyM = new AttributeValue().withM(new HashMap<>());
        final Map<String, AttributeValue> item = new HashMap<>();
        item.put("objs", ss);
        item.put("raw", ns);
        item.put("props", emptyM);

        final DdbCopyBean first = DynamoDBExecutor.toEntity(item, DdbCopyBean.class);
        final DdbCopyBean second = DynamoDBExecutor.toEntity(item, DdbCopyBean.class);
        first.getObjs().add("x");
        first.getRaw().add("2");
        first.getProps().put("x", 1);

        org.junit.jupiter.api.Assertions.assertAll( //
                () -> assertEquals(List.of("a", "b", "x"), first.getObjs()), //
                () -> assertEquals(List.of("a", "b"), second.getObjs()), //
                () -> assertEquals(List.of("a", "b"), ss.getSS()), //
                () -> assertEquals(List.of("1"), new ArrayList<>(second.getRaw())), //
                () -> assertEquals(List.of("1"), ns.getNS()), //
                () -> assertTrue(second.getProps().isEmpty()), //
                () -> assertTrue(emptyM.getM().isEmpty()));
    }

    // ---- 2026-10-20 fixC ----

    /**
     * list(QueryRequest, Class) converts each response page directly instead of through toList(QueryResult, Class). A null
     * response, which only an un-stubbed client produces, must keep the former toList behaviour: an empty (mutable) list, and
     * no further page is requested.
     */
    @Test
    public void testListWithClass_NullResponseIsEmptyAndRequestsNoFurtherPage_fixC() {
        QueryRequest req = new QueryRequest().withTableName("TestTable");
        when(mockDynamoDBClient.query(any(QueryRequest.class))).thenReturn(null);

        List<TestEntity> result = executor.list(req, TestEntity.class);

        assertNotNull(result);
        assertTrue(result.isEmpty());
        result.add(new TestEntity());
        assertEquals(1, result.size());
        verify(mockDynamoDBClient, times(1)).query(any(QueryRequest.class));
    }

    // ---- 2026-10-21 fixP ----

    /**
     * A null LATER page (page 1 has rows and a LastEvaluatedKey, page 2 is null) must end the pagination the same way a null
     * first page does: the result holds page 1's rows, exactly two requests are made and nothing is thrown (the loop used to
     * dereference the null page and fail with a NullPointerException).
     */
    @Test
    public void testListWithClass_NullLaterPageEndsPaginationWithRowsSoFar_fixP() {
        QueryRequest req = new QueryRequest().withTableName("TestTable");
        QueryResult page1 = new QueryResult().withItems(List.of(Map.of("id", new AttributeValue().withS("1")), Map.of("id", new AttributeValue().withS("2"))))
                .withLastEvaluatedKey(Map.of("id", new AttributeValue().withS("2")));
        when(mockDynamoDBClient.query(any(QueryRequest.class))).thenReturn(page1, (QueryResult) null);

        List<TestEntity> result = executor.list(req, TestEntity.class);

        assertEquals(List.of("1", "2"), result.stream().map(TestEntity::getId).toList());
        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(mockDynamoDBClient, times(2)).query(captor.capture());
        assertNull(captor.getAllValues().get(0).getExclusiveStartKey());
        assertEquals("2", captor.getAllValues().get(1).getExclusiveStartKey().get("id").getS());
    }

    // ---- 2026-10-24 apply-ddb ----

    /**
     * A number type whose text is not a decimal number (abacus Fraction renders "3/4"; DynamoDB rejects such an N value) is
     * written as S holding that text, by every write path (toAttributeValue, asKey, asItem, toAttributeValueUpdate, toItem),
     * and the S text reads back into the Fraction property and a Fraction single value unchanged.
     */
    @Test
    public void testToAttributeValue_NonDecimalNumberTextIsStoredAsSAndReadsBack_applyDdb() {
        final com.landawn.abacus.util.Fraction threeQuarters = com.landawn.abacus.util.Fraction.of(3, 4);
        final com.landawn.abacus.util.Fraction oneThird = com.landawn.abacus.util.Fraction.of(1, 3);
        final AttributeValue fraction = DynamoDBExecutor.toAttributeValue(threeQuarters);
        final FractionEntity_applyDdb entity = new FractionEntity_applyDdb();
        entity.setId(oneThird);
        entity.setRatio(threeQuarters);
        entity.setCount(5);
        final Map<String, AttributeValue> item = DynamoDBExecutor.toItem(entity);

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals("3/4", fraction.getS()), () -> assertNull(fraction.getN()),
                () -> assertEquals("2/1", DynamoDBExecutor.toAttributeValue(com.landawn.abacus.util.Fraction.of(2, 1)).getS()),
                () -> assertEquals("1/3", DynamoDBExecutor.asKey("id", oneThird).get("id").getS()),
                () -> assertEquals("3/4", DynamoDBExecutor.asItem("ratio", threeQuarters).get("ratio").getS()),
                () -> assertEquals("3/4", DynamoDBExecutor.toAttributeValueUpdate(threeQuarters).getValue().getS()),
                () -> assertEquals("1,234", DynamoDBExecutor.toAttributeValue(new GroupedDigitsNumber_applyDdb()).getS()),
                () -> assertEquals("1/3", item.get("id").getS()), () -> assertEquals("3/4", item.get("ratio").getS()),
                () -> assertNull(item.get("ratio").getN()), () -> assertEquals("5", item.get("count").getN()),
                () -> assertEquals(threeQuarters, DynamoDBExecutor.toEntity(item, FractionEntity_applyDdb.class).getRatio()),
                () -> assertEquals(oneThird, DynamoDBExecutor.toEntity(item, FractionEntity_applyDdb.class).getId()),
                () -> assertEquals(threeQuarters, DynamoDBExecutor.toValue(fraction, com.landawn.abacus.util.Fraction.class)));
    }

    /**
     * Mapper keys and items go through the same conversion: a Fraction key is sent as S, the stored item is written with S, and
     * an item stored that way maps back to the entity's Fraction properties.
     */
    @Test
    public void testMapperFractionKeyAndItemUseSAndReadBack_applyDdb() {
        final DynamoDBExecutor.Mapper<FractionEntity_applyDdb> mapper = executor.mapper(FractionEntity_applyDdb.class, "FractionTable",
                NamingPolicy.CAMEL_CASE);
        final FractionEntity_applyDdb entity = new FractionEntity_applyDdb();
        entity.setId(com.landawn.abacus.util.Fraction.of(1, 3));
        entity.setRatio(com.landawn.abacus.util.Fraction.of(3, 4));
        final Map<String, AttributeValue> stored = new LinkedHashMap<>();
        stored.put("id", new AttributeValue().withS("1/3"));
        stored.put("ratio", new AttributeValue().withS("3/4"));
        when(mockDynamoDBClient.getItem(eq("FractionTable"), any())).thenReturn(new GetItemResult().withItem(stored));

        final FractionEntity_applyDdb read = mapper.getItem(entity);
        mapper.putItem(entity);

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Map<String, AttributeValue>> keyCaptor = ArgumentCaptor.forClass(Map.class);
        verify(mockDynamoDBClient).getItem(eq("FractionTable"), keyCaptor.capture());
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Map<String, AttributeValue>> itemCaptor = ArgumentCaptor.forClass(Map.class);
        verify(mockDynamoDBClient).putItem(eq("FractionTable"), itemCaptor.capture());
        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(new AttributeValue().withS("1/3"), keyCaptor.getValue().get("id")),
                () -> assertEquals(new AttributeValue().withS("1/3"), itemCaptor.getValue().get("id")),
                () -> assertEquals(new AttributeValue().withS("3/4"), itemCaptor.getValue().get("ratio")),
                () -> assertEquals(com.landawn.abacus.util.Fraction.of(1, 3), read.getId()),
                () -> assertEquals(com.landawn.abacus.util.Fraction.of(3, 4), read.getRatio()));
    }

    /**
     * Guard for the Fraction change: every number type whose text is a decimal number is still written as N with exactly the
     * text it had before (the type's own string form, exponent forms included), and non-finite values are still rejected.
     */
    @Test
    public void testToAttributeValue_DecimalNumbersUnchangedAndNonFiniteRejected_applyDdb() {
        final java.util.concurrent.atomic.DoubleAdder adder = new java.util.concurrent.atomic.DoubleAdder();
        adder.add(0.25);
        final List<Object> decimals = List.of((byte) -1, (short) 7, 42, Integer.MIN_VALUE, -42L, Long.MAX_VALUE, 1.5f, Float.MIN_VALUE, -0.0, 1.0E-7, 1.0E21,
                Double.MIN_VALUE, -Double.MAX_VALUE, new java.math.BigInteger("123456789012345678901234567890"), new java.math.BigDecimal("1E+10"),
                new java.math.BigDecimal("0E-10"), new java.math.BigDecimal("-0.000001"), new java.math.BigDecimal("12345678901234567890.123456789"),
                java.math.BigDecimal.valueOf(1e-7), java.math.BigDecimal.valueOf(12, -5), com.landawn.abacus.util.MutableInt.of(3),
                com.landawn.abacus.util.MutableLong.of(-9L), com.landawn.abacus.util.MutableDouble.of(2.5), com.landawn.abacus.util.MutableFloat.of(0.1f),
                adder);
        final List<org.junit.jupiter.api.function.Executable> checks = new ArrayList<>();

        for (final Object number : decimals) {
            @SuppressWarnings("unchecked")
            final String text = com.landawn.abacus.util.N.typeOf((Class<Object>) number.getClass()).stringOf(number);
            checks.add(() -> assertEquals(new AttributeValue().withN(text), DynamoDBExecutor.toAttributeValue(number), text));
            checks.add(() -> assertEquals(new AttributeValue().withN(text), DynamoDBExecutor.asKey("k", number).get("k"), text));
        }

        for (final Object nonFinite : List.of(Double.NaN, Double.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY,
                com.landawn.abacus.util.MutableDouble.of(Double.NaN), com.landawn.abacus.util.MutableFloat.of(Float.POSITIVE_INFINITY))) {
            checks.add(() -> assertTrue(assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toAttributeValue(nonFinite)).getMessage()
                    .startsWith("DynamoDB numbers must be finite"), String.valueOf(nonFinite)));
        }

        org.junit.jupiter.api.Assertions.assertAll(checks);
    }

    /**
     * query(QueryRequest, Class) builds a Dataset only from Map or bean rows. Any other row type is rejected with an
     * IllegalArgumentException before the request is sent - no page is read or paid for - whatever the query would return,
     * an empty result included. null, Map types (an @Entity-annotated Map subclass too) and bean classes still query.
     */
    @Test
    public void testQueryWithClass_UnsupportedRowTypeRejectedBeforeAnyRequest_applyDdb() {
        final QueryRequest req = new QueryRequest().withTableName("TestTable");
        final QueryResult oneItem = new QueryResult().withItems(List.of(Map.of("id", new AttributeValue().withS("1"))));
        when(mockDynamoDBClient.query(any(QueryRequest.class))).thenReturn(oneItem, new QueryResult().withItems(new ArrayList<>()));
        final List<Class<?>> unsupported = List.of(String.class, Integer.class, int.class, Object.class, Object[].class, String[].class, List.class,
                java.util.Set.class, java.util.Optional.class, com.landawn.abacus.util.Holder.class, com.landawn.abacus.util.MutableBoolean.class);
        final List<org.junit.jupiter.api.function.Executable> checks = new ArrayList<>();

        for (final Class<?> rowType : unsupported) {
            checks.add(() -> {
                final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> executor.query(req, rowType), rowType.getName());
                assertTrue(e.getMessage().startsWith("Unsupported row type: " + com.landawn.abacus.util.ClassUtil.getCanonicalClassName(rowType)),
                        e.getMessage());
            });
        }

        org.junit.jupiter.api.Assertions.assertAll(checks);
        verify(mockDynamoDBClient, org.mockito.Mockito.never()).query(any(QueryRequest.class));

        when(mockDynamoDBClient.query(any(QueryRequest.class))).thenReturn(oneItem);
        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(1, executor.query(req, (Class<?>) null).size()),
                () -> assertEquals(1, executor.query(req, Map.class).size()), () -> assertEquals(1, executor.query(req, AnnotatedMapRow_applyDdb.class).size()),
                () -> assertEquals(1, executor.query(req, TestEntity.class).size()));
        verify(mockDynamoDBClient, times(4)).query(any(QueryRequest.class));
    }

    @com.landawn.abacus.annotation.Table(name = "FractionTable")
    public static class FractionEntity_applyDdb {
        @com.landawn.abacus.annotation.Id
        private com.landawn.abacus.util.Fraction id;
        private com.landawn.abacus.util.Fraction ratio;
        private Integer count;

        public com.landawn.abacus.util.Fraction getId() {
            return id;
        }

        public void setId(final com.landawn.abacus.util.Fraction id) {
            this.id = id;
        }

        public com.landawn.abacus.util.Fraction getRatio() {
            return ratio;
        }

        public void setRatio(final com.landawn.abacus.util.Fraction ratio) {
            this.ratio = ratio;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(final Integer count) {
            this.count = count;
        }
    }

    /** A number type whose text is not a decimal literal. */
    public static final class GroupedDigitsNumber_applyDdb extends Number {
        private static final long serialVersionUID = 1L;

        @Override
        public int intValue() {
            return 1234;
        }

        @Override
        public long longValue() {
            return 1234;
        }

        @Override
        public float floatValue() {
            return 1234;
        }

        @Override
        public double doubleValue() {
            return 1234;
        }

        @Override
        public String toString() {
            return "1,234";
        }
    }

    @com.landawn.abacus.annotation.Entity
    public static class AnnotatedMapRow_applyDdb extends LinkedHashMap<String, Object> {
        private static final long serialVersionUID = 1L;
    }

    // ---- 2026-10-24 r2 sliceA ----

    /**
     * An @Entity-annotated Map subclass is read as a Map everywhere (readRow, the row mappers, query(QueryRequest, Class)), but
     * toItem/toUpdateItem took it as a bean (Beans.isBeanClass accepts it) and wrote the Map's own getters (keySet, values,
     * entrySet, size, empty) instead of its entries. It is now written like any other Map (naming policy and NULL for a null
     * value included), and the written item reads back into an equal instance of the subclass.
     */
    @Test
    public void testToItem_EntityAnnotatedMapSubclassIsWrittenAsMap_r2sliceA() {
        final AnnotatedMapRow_applyDdb row = new AnnotatedMapRow_applyDdb();
        row.put("id", "1");
        row.put("userName", "x");
        row.put("note", null);

        final Map<String, AttributeValue> expectedItem = new LinkedHashMap<>();
        expectedItem.put("id", new AttributeValue().withS("1"));
        expectedItem.put("userName", new AttributeValue().withS("x"));
        expectedItem.put("note", new AttributeValue().withNULL(true));
        final Map<String, AttributeValueUpdate> expectedUpdate = new LinkedHashMap<>();
        expectedUpdate.put("id", new AttributeValueUpdate(new AttributeValue().withS("1"), AttributeAction.PUT));
        expectedUpdate.put("user_name", new AttributeValueUpdate(new AttributeValue().withS("x"), AttributeAction.PUT));
        expectedUpdate.put("note", new AttributeValueUpdate(new AttributeValue().withNULL(true), AttributeAction.PUT));

        final Map<String, AttributeValue> item = DynamoDBExecutor.toItem(row);
        final Object readBack = DynamoDBExecutor.toList(new QueryResult().withItems(List.of(item)), AnnotatedMapRow_applyDdb.class).get(0);

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(expectedItem, item),
                () -> assertEquals(List.of("id", "userName", "note"), new ArrayList<>(item.keySet())),
                () -> assertEquals(List.of("id", "user_name", "note"), new ArrayList<>(DynamoDBExecutor.toItem(row, NamingPolicy.SNAKE_CASE).keySet())),
                () -> assertEquals(expectedUpdate, DynamoDBExecutor.toUpdateItem(row, NamingPolicy.SNAKE_CASE)),
                () -> assertEquals(List.of("id", "userName", "note"), new ArrayList<>(DynamoDBExecutor.toUpdateItem(row).keySet())),
                () -> assertTrue(readBack instanceof AnnotatedMapRow_applyDdb, String.valueOf(readBack)), () -> assertEquals(row, readBack));
    }

    // ---- 2026-10-24 r2verify1 ----

    /**
     * Beans.isBeanClass accepts an @Entity-annotated Map subclass, so both public toEntity overloads mapped an item to it as a
     * bean whose only properties are its Map accessors (and its own fields): every attribute was silently dropped and an empty
     * map returned (or UnsupportedOperationException for a class without fields). They now reject every Map type with the
     * IllegalArgumentException a plain Map already got; a null item still returns null, and the row readers still fill such a
     * class with the item's attributes.
     */
    @Test
    public void testToEntityRejectsEntityAnnotatedMapSubclass_r2verify1() {
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("id", new AttributeValue().withS("1"));
        item.put("name", new AttributeValue().withS("x"));
        final Map<String, Object> expectedRow = new LinkedHashMap<>();
        expectedRow.put("id", "1");
        expectedRow.put("name", "x");
        final String reason = " - Map implementations are never treated as beans";
        final String withIdMessage = "Not a bean class: " + EntityMapWithId_r2verify1.class.getCanonicalName() + reason;

        final Object row = DynamoDBExecutor.toList(new QueryResult().withItems(List.of(item)), EntityMapWithId_r2verify1.class).get(0);

        org.junit.jupiter.api.Assertions
                .assertAll(
                        () -> assertEquals(withIdMessage,
                                assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toEntity(item, EntityMapWithId_r2verify1.class))
                                        .getMessage()),
                        () -> assertEquals(withIdMessage,
                                assertThrows(IllegalArgumentException.class,
                                        () -> DynamoDBExecutor.toEntity(new GetItemResult().withItem(item), EntityMapWithId_r2verify1.class)).getMessage()),
                        () -> assertEquals("Not a bean class: " + AnnotatedMapRow_applyDdb.class.getCanonicalName() + reason,
                                assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toEntity(item, AnnotatedMapRow_applyDdb.class))
                                        .getMessage()),
                        // Unchanged: a plain Map type fails alike, and a missing item is null.
                        () -> assertEquals("Not a bean class: java.util.LinkedHashMap" + reason,
                                assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toEntity(item, LinkedHashMap.class)).getMessage()),
                        () -> assertNull(DynamoDBExecutor.toEntity((Map<String, AttributeValue>) null, EntityMapWithId_r2verify1.class)),
                        () -> assertNull(DynamoDBExecutor.toEntity(new GetItemResult(), EntityMapWithId_r2verify1.class)),
                        () -> assertTrue(row instanceof EntityMapWithId_r2verify1, String.valueOf(row)), () -> assertEquals(expectedRow, row));
    }

    @com.landawn.abacus.annotation.Entity
    public static class EntityMapWithId_r2verify1 extends LinkedHashMap<String, Object> {
        private static final long serialVersionUID = 1L;

        @com.landawn.abacus.annotation.Id
        private String id;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }
    }

}
