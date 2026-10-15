package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
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
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.stream.Stream;

public class DynamoDBMapperTest extends DynamoDBExecutorTestSupport {

    // Construction and configuration

    /**
     * Regression test for bug in DynamoDBExecutor.Mapper.createBatchPutRequest:
     * it previously used toItem(entity) (always CAMEL_CASE) instead of
     * toItem(entity, namingPolicy), so batchPutItem ignored the mapper's configured
     * NamingPolicy while putItem/updateItem honored it. This produced inconsistent
     * attribute names (e.g. "firstName" vs "first_name") for the same entity.
     */
    @org.junit.jupiter.api.Test
    public void test_mapper_nullEntityArgs_throwIAE() {
        final DynamoDBExecutor.Mapper<com.landawn.abacus.da.Account> mapper = executor.mapper(com.landawn.abacus.da.Account.class, "account",
                com.landawn.abacus.util.NamingPolicy.CAMEL_CASE);

        // Entity methods: guarded at the createKey/toItem chokepoints (was NPE).
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> mapper.getItem((com.landawn.abacus.da.Account) null));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> mapper.putItem((com.landawn.abacus.da.Account) null));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> mapper.updateItem((com.landawn.abacus.da.Account) null));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> mapper.deleteItem((com.landawn.abacus.da.Account) null));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> mapper.batchGetItem((List<com.landawn.abacus.da.Account>) null));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> mapper.batchPutItem((List<com.landawn.abacus.da.Account>) null));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> mapper.batchDeleteItem((List<com.landawn.abacus.da.Account>) null));
    }

    @org.junit.jupiter.api.Test
    public void test_mapper_nullRequestArgs_throwIAE() {
        final DynamoDBExecutor.Mapper<com.landawn.abacus.da.Account> mapper = executor.mapper(com.landawn.abacus.da.Account.class, "account",
                com.landawn.abacus.util.NamingPolicy.CAMEL_CASE);

        // Request-object methods: guarded at the check* chokepoints (was NPE).
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> mapper.getItem((com.amazonaws.services.dynamodbv2.model.GetItemRequest) null));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> mapper.stream((QueryRequest) null));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> mapper.scan((ScanRequest) null));
    }

    @Test
    public void testMapperBatchPutItemRespectsNamingPolicy() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class, "TestTable", NamingPolicy.SNAKE_CASE);

        TestEntity entity = new TestEntity();
        entity.setId("123");
        entity.setFirstName("John");

        // Mapper.batchPutItem -> createBatchPutRequest returns a Map, so the Map overload of batchWriteItem is invoked.
        when(mockDynamoDBClient.batchWriteItem(any(Map.class))).thenReturn(new BatchWriteItemResult());

        mapper.batchPutItem(List.of(entity));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, List<WriteRequest>>> captor = ArgumentCaptor.forClass(Map.class);
        verify(mockDynamoDBClient).batchWriteItem(captor.capture());

        Map<String, AttributeValue> writtenItem = captor.getValue().get("TestTable").get(0).getPutRequest().getItem();

        // With SNAKE_CASE the attribute must be "first_name", not the CAMEL_CASE "firstName".
        assertTrue(writtenItem.containsKey("first_name"), "Expected snake_case attribute name 'first_name' but got: " + writtenItem.keySet());
        assertEquals("John", writtenItem.get("first_name").getS());
        assertEquals("123", writtenItem.get("id").getS());
    }

    // The Mapper-constructor message on its own: testMapper_ErrorMessagesNameClassOnce fails at its first (@Table) assertion on
    // HEAD, so it never proved this second site, which rendered "class java.lang.Integer is not an entity class ...".
    @Test
    public void testMapperConstructor_NonBeanMessageNamesClassOnce_coverageDC() {
        final IllegalArgumentException notBean = assertThrows(IllegalArgumentException.class, () -> executor.mapper(Integer.class, "t", null));
        assertEquals("java.lang.Integer is not an entity class with getter/setter method", notBean.getMessage());

        final IllegalArgumentException nested = assertThrows(IllegalArgumentException.class,
                () -> executor.mapper(BlankKeyValue_coverageDC.class, "t", NamingPolicy.CAMEL_CASE));
        assertEquals(BlankKeyValue_coverageDC.class.getCanonicalName() + " is not an entity class with getter/setter method", nested.getMessage());
    }

    /**
     * Regression test for bug in DynamoDBExecutor.Mapper.createKey: it computed the
     * key attribute name as the raw Java field name (or @Column override) without
     * applying the configured NamingPolicy. Mapper.putItem honored NamingPolicy via
     * toItem(entity, namingPolicy), so a SNAKE_CASE mapper wrote "user_id" but the
     * subsequent getItem/deleteItem/updateItem/batch* looked up "userId" — the row
     * was unreachable through the mapper API once persisted.
     */
    @Test
    public void testMapperCreateKeyRespectsNamingPolicy() {
        DynamoDBExecutor.Mapper<TestEntityWithUserId> mapper = executor.mapper(TestEntityWithUserId.class, "TestTable", NamingPolicy.SNAKE_CASE);

        TestEntityWithUserId entity = new TestEntityWithUserId();
        entity.setUserId("u123");

        when(mockDynamoDBClient.getItem(any(String.class), any(Map.class))).thenReturn(new GetItemResult());

        mapper.getItem(entity);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, AttributeValue>> keyCaptor = ArgumentCaptor.forClass(Map.class);
        verify(mockDynamoDBClient).getItem(any(String.class), keyCaptor.capture());

        Map<String, AttributeValue> capturedKey = keyCaptor.getValue();

        // With SNAKE_CASE the key attribute must be "user_id" (matching what putItem writes),
        // not the raw field name "userId". Before the fix, the captured key was "userId".
        assertTrue(capturedKey.containsKey("user_id"), "Expected snake_case key attribute 'user_id' but got: " + capturedKey.keySet());
        assertEquals("u123", capturedKey.get("user_id").getS());
    }

    // Factories and insert operations

    @Test
    public void testMapperBatchPutItem() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        List<TestEntity> entities = new ArrayList<>();
        TestEntity entity = new TestEntity();
        entity.setId("123");
        entities.add(entity);

        BatchWriteItemResult batchWriteItemResult = new BatchWriteItemResult();

        when(mockDynamoDBClient.batchWriteItem(any(Map.class))).thenReturn(batchWriteItemResult);

        BatchWriteItemResult result = mapper.batchPutItem(entities);

        assertNotNull(result);
    }

    @Test
    public void testMapperGetItem_StringArrayPropRoundTripsThroughPutItem() {
        DynamoDBExecutor.Mapper<ObjectArrayPropsEntity> mapper = executor.mapper(ObjectArrayPropsEntity.class);
        ObjectArrayPropsEntity entity = new ObjectArrayPropsEntity();
        entity.setId("id-1");
        entity.setTags(new String[] { "red", "green" });

        when(mockDynamoDBClient.putItem(eq("TestTable"), any())).thenReturn(new PutItemResult());
        mapper.putItem(entity);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, AttributeValue>> itemCaptor = ArgumentCaptor.forClass(Map.class);
        verify(mockDynamoDBClient).putItem(eq("TestTable"), itemCaptor.capture());

        when(mockDynamoDBClient.getItem(eq("TestTable"), any())).thenReturn(new GetItemResult().withItem(itemCaptor.getValue()));

        assertArrayEquals(new String[] { "red", "green" }, mapper.getItem(entity).getTags());
    }

    @Test
    public void testMapperPutItem() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        TestEntity entity = new TestEntity();
        entity.setId("123");
        entity.setName("Test");

        PutItemResult putItemResult = new PutItemResult();

        when(mockDynamoDBClient.putItem(eq("TestTable"), any(Map.class))).thenReturn(putItemResult);

        PutItemResult result = mapper.putItem(entity);

        assertNotNull(result);
    }

    // Mapper.putItem(PutItemRequest) with no tableName set
    @Test
    public void testMapperPutItemRequest_NoTableNameSet() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        PutItemRequest req = new PutItemRequest().withItem(Map.of("id", new AttributeValue().withS("p2")));
        when(mockDynamoDBClient.putItem(any(PutItemRequest.class))).thenReturn(new PutItemResult());

        PutItemResult result = mapper.putItem(req);
        assertNotNull(result);
        assertEquals("TestTable", req.getTableName());
    }

    // Mapper.putItem(entity, returnValues)
    @Test
    public void testMapperPutItemWithReturnValues() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        TestEntity entity = new TestEntity();
        entity.setId("p1");
        when(mockDynamoDBClient.putItem(eq("TestTable"), any(Map.class), eq("ALL_OLD"))).thenReturn(new PutItemResult());

        PutItemResult result = mapper.putItem(entity, "ALL_OLD");
        assertNotNull(result);
    }

    // Updates and deletes

    @Test
    public void testMapperBatchDeleteItem() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        List<TestEntity> entities = new ArrayList<>();
        TestEntity entity = new TestEntity();
        entity.setId("123");
        entities.add(entity);

        BatchWriteItemResult batchWriteItemResult = new BatchWriteItemResult();

        when(mockDynamoDBClient.batchWriteItem(any(Map.class))).thenReturn(batchWriteItemResult);

        BatchWriteItemResult result = mapper.batchDeleteItem(entities);

        assertNotNull(result);
    }

    // Mapper.batchWriteItem(BatchWriteItemRequest)
    @Test
    public void testMapperBatchWriteItemRequest_NoTableNameSet() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        BatchWriteItemRequest req = new BatchWriteItemRequest().withRequestItems(
                Map.of("TestTable", List.of(new WriteRequest().withPutRequest(new PutRequest().withItem(Map.of("id", new AttributeValue().withS("b1")))))));
        when(mockDynamoDBClient.batchWriteItem(req)).thenReturn(new BatchWriteItemResult());

        BatchWriteItemResult result = mapper.batchWriteItem(req);
        assertNotNull(result);
    }

    @Test
    public void testMapperDeleteItem() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        TestEntity entity = new TestEntity();
        entity.setId("123");

        DeleteItemResult deleteItemResult = new DeleteItemResult();

        when(mockDynamoDBClient.deleteItem(eq("TestTable"), any(Map.class))).thenReturn(deleteItemResult);

        DeleteItemResult result = mapper.deleteItem(entity);

        assertNotNull(result);
    }

    // Mapper.deleteItem(Map<String, AttributeValue>)
    @Test
    public void testMapperDeleteItemByKey() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        Map<String, AttributeValue> key = Map.of("id", new AttributeValue().withS("d2"));
        when(mockDynamoDBClient.deleteItem(eq("TestTable"), eq(key))).thenReturn(new DeleteItemResult());

        DeleteItemResult result = mapper.deleteItem(key);
        assertNotNull(result);
    }

    // Mapper.deleteItem(DeleteItemRequest)
    @Test
    public void testMapperDeleteItemRequest_NoTableNameSet() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        DeleteItemRequest req = new DeleteItemRequest().withKey(Map.of("id", new AttributeValue().withS("d3")));
        when(mockDynamoDBClient.deleteItem(any(DeleteItemRequest.class))).thenReturn(new DeleteItemResult());

        DeleteItemResult result = mapper.deleteItem(req);
        assertNotNull(result);
        assertEquals("TestTable", req.getTableName());
    }

    // Mapper.deleteItem(entity, returnValues)
    @Test
    public void testMapperDeleteItemWithReturnValues() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        TestEntity entity = new TestEntity();
        entity.setId("d1");
        when(mockDynamoDBClient.deleteItem(eq("TestTable"), any(Map.class), eq("ALL_OLD"))).thenReturn(new DeleteItemResult());

        DeleteItemResult result = mapper.deleteItem(entity, "ALL_OLD");
        assertNotNull(result);
    }

    // Mapper.getItem(GetItemRequest) with no tableName set (defaults to mapper's tableName)
    @Test
    public void testMapperGetItemRequest_NoTableNameSet() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        GetItemRequest req = new GetItemRequest().withKey(Map.of("id", new AttributeValue().withS("x")));
        when(mockDynamoDBClient.getItem(any(GetItemRequest.class))).thenReturn(new GetItemResult().withItem(Map.of("id", new AttributeValue().withS("x"))));

        TestEntity result = mapper.getItem(req);
        assertNotNull(result);
        assertEquals("TestTable", req.getTableName());
    }

    @Test
    public void testMapperUpdateItem() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        TestEntity entity = new TestEntity();
        entity.setId("123");
        entity.setName("Updated");

        UpdateItemResult updateItemResult = new UpdateItemResult();

        when(mockDynamoDBClient.updateItem(eq("TestTable"), any(Map.class), any(Map.class))).thenReturn(updateItemResult);

        UpdateItemResult result = mapper.updateItem(entity);
        assertNotNull(result);

        ArgumentCaptor<Map> keyCaptor = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<Map> updatesCaptor = ArgumentCaptor.forClass(Map.class);
        verify(mockDynamoDBClient).updateItem(eq("TestTable"), keyCaptor.capture(), updatesCaptor.capture());

        Map<String, AttributeValue> key = keyCaptor.getValue();
        Map<String, AttributeValueUpdate> updates = updatesCaptor.getValue();
        assertTrue(key.containsKey("id"));
        assertTrue(!updates.containsKey("id"));
        assertTrue(updates.containsKey("name"));
    }

    // Mapper.updateItem(UpdateItemRequest) with no tableName set
    @Test
    public void testMapperUpdateItemRequest_NoTableNameSet() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        UpdateItemRequest req = new UpdateItemRequest().withKey(Map.of("id", new AttributeValue().withS("u2")));
        when(mockDynamoDBClient.updateItem(any(UpdateItemRequest.class))).thenReturn(new UpdateItemResult());

        UpdateItemResult result = mapper.updateItem(req);
        assertNotNull(result);
        assertEquals("TestTable", req.getTableName());
    }

    // Mapper.updateItem(entity, returnValues)
    @Test
    public void testMapperUpdateItemWithReturnValues() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        TestEntity entity = new TestEntity();
        entity.setId("u1");
        entity.setName("updated");
        when(mockDynamoDBClient.updateItem(eq("TestTable"), any(Map.class), any(Map.class), eq("ALL_NEW"))).thenReturn(new UpdateItemResult());

        UpdateItemResult result = mapper.updateItem(entity, "ALL_NEW");
        assertNotNull(result);

        ArgumentCaptor<Map> keyCaptor = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<Map> updatesCaptor = ArgumentCaptor.forClass(Map.class);
        verify(mockDynamoDBClient).updateItem(eq("TestTable"), keyCaptor.capture(), updatesCaptor.capture(), eq("ALL_NEW"));

        Map<String, AttributeValue> key = keyCaptor.getValue();
        Map<String, AttributeValueUpdate> updates = updatesCaptor.getValue();
        assertTrue(key.containsKey("id"));
        assertTrue(!updates.containsKey("id"));
        assertTrue(updates.containsKey("name"));
    }

    // Queries and results

    @Test
    public void testMapperBatchGetItem() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        List<TestEntity> entities = new ArrayList<>();
        TestEntity entity1 = new TestEntity();
        entity1.setId("1");
        entities.add(entity1);
        TestEntity entity2 = new TestEntity();
        entity2.setId("2");
        entities.add(entity2);

        BatchGetItemResult batchGetItemResult = new BatchGetItemResult();
        Map<String, List<Map<String, AttributeValue>>> responses = new HashMap<>();
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("id", new AttributeValue().withS("1"));
        items.add(item1);
        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("id", new AttributeValue().withS("2"));
        items.add(item2);
        responses.put("TestTable", items);
        batchGetItemResult.setResponses(responses);

        when(mockDynamoDBClient.batchGetItem(any(Map.class))).thenReturn(batchGetItemResult);

        List<TestEntity> result = mapper.batchGetItem(entities);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("1", result.get(0).getId());
        assertEquals("2", result.get(1).getId());
    }

    // Mapper.batchGetItem with empty responses returns empty list
    @Test
    public void testMapperBatchGetItem_EmptyResponses() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        BatchGetItemResult emptyRes = new BatchGetItemResult();
        when(mockDynamoDBClient.batchGetItem(any(Map.class))).thenReturn(emptyRes);

        List<TestEntity> result = mapper.batchGetItem(List.of());
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    // Mapper.batchGetItem(BatchGetItemRequest)
    @Test
    public void testMapperBatchGetItemRequest() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        BatchGetItemRequest req = new BatchGetItemRequest()
                .withRequestItems(Map.of("TestTable", new KeysAndAttributes().withKeys(List.of(Map.of("id", new AttributeValue().withS("b1"))))));
        BatchGetItemResult res = new BatchGetItemResult().withResponses(Map.of("TestTable", List.of(Map.of("id", new AttributeValue().withS("b1")))));
        when(mockDynamoDBClient.batchGetItem(req)).thenReturn(res);

        List<TestEntity> result = mapper.batchGetItem(req);
        assertNotNull(result);
        assertEquals(1, result.size());
    }

    // Mapper.batchGetItem(Collection, returnConsumedCapacity)
    @Test
    public void testMapperBatchGetItemWithReturnConsumedCapacity() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        TestEntity e1 = new TestEntity();
        e1.setId("1");

        BatchGetItemResult res = new BatchGetItemResult().withResponses(Map.of("TestTable", List.of(Map.of("id", new AttributeValue().withS("1")))));
        when(mockDynamoDBClient.batchGetItem(any(Map.class), eq("TOTAL"))).thenReturn(res);

        List<TestEntity> result = mapper.batchGetItem(List.of(e1), "TOTAL");
        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testMapperGetItem() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        TestEntity entity = new TestEntity();
        entity.setId("123");

        GetItemResult getItemResult = new GetItemResult();
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue().withS("123"));
        item.put("name", new AttributeValue().withS("Test"));
        getItemResult.setItem(item);

        when(mockDynamoDBClient.getItem(eq("TestTable"), any(Map.class))).thenReturn(getItemResult);

        TestEntity result = mapper.getItem(entity);

        assertNotNull(result);
        assertEquals("123", result.getId());
        assertEquals("Test", result.getName());
    }

    // Mapper.getItem(Map<String, AttributeValue>)
    @Test
    public void testMapperGetItemByKey() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        Map<String, AttributeValue> key = Map.of("id", new AttributeValue().withS("abc"));
        GetItemResult res = new GetItemResult().withItem(Map.of("id", new AttributeValue().withS("abc")));
        when(mockDynamoDBClient.getItem(eq("TestTable"), eq(key))).thenReturn(res);

        TestEntity result = mapper.getItem(key);
        assertNotNull(result);
        assertEquals("abc", result.getId());
    }

    // Mapper.getItem(entity, consistentRead) - new overload not yet tested
    @Test
    public void testMapperGetItemWithConsistentRead() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        TestEntity entity = new TestEntity();
        entity.setId("123");

        GetItemResult res = new GetItemResult().withItem(Map.of("id", new AttributeValue().withS("123")));
        when(mockDynamoDBClient.getItem(eq("TestTable"), any(Map.class), eq(Boolean.TRUE))).thenReturn(res);

        TestEntity result = mapper.getItem(entity, Boolean.TRUE);
        assertNotNull(result);
        assertEquals("123", result.getId());
    }

    @Test
    public void testMapperList() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        QueryRequest queryRequest = new QueryRequest().withTableName("TestTable");

        QueryResult queryResult = new QueryResult();
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue().withS("123"));
        items.add(item);
        queryResult.setItems(items);

        when(mockDynamoDBClient.query(queryRequest)).thenReturn(queryResult);

        List<TestEntity> result = mapper.list(queryRequest);

        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testMapperQuery() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        QueryRequest queryRequest = new QueryRequest().withTableName("TestTable");

        QueryResult queryResult = new QueryResult();
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue().withS("123"));
        items.add(item);
        queryResult.setItems(items);

        when(mockDynamoDBClient.query(queryRequest)).thenReturn(queryResult);

        Dataset result = mapper.query(queryRequest);

        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testMapperScan() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        ScanRequest scanRequest = new ScanRequest().withTableName("TestTable");

        ScanResult scanResult = new ScanResult();
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue().withS("123"));
        items.add(item);
        scanResult.setItems(items);

        when(mockDynamoDBClient.scan(any(ScanRequest.class))).thenReturn(scanResult);

        Stream<TestEntity> stream = mapper.scan(scanRequest);

        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testMapperScan_EmptyAttributesToGetOmitsLegacyProjection() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        when(mockDynamoDBClient.scan(any(ScanRequest.class))).thenReturn(new ScanResult().withItems(new ArrayList<>()));

        assertEquals(0, mapper.scan(List.of()).count());
        assertEquals(0, mapper.scan(List.of(), Filters.notNull("id")).count());

        ArgumentCaptor<ScanRequest> captor = ArgumentCaptor.forClass(ScanRequest.class);
        verify(mockDynamoDBClient, times(2)).scan(captor.capture());

        for (ScanRequest sent : captor.getAllValues()) {
            assertEquals("TestTable", sent.getTableName());
            assertNull(sent.getAttributesToGet());
        }
    }

    // Mapper.scan(attributesToGet) overload
    @Test
    public void testMapperScanWithAttributes() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        ScanResult res = new ScanResult().withItems(List.of(Map.of("id", new AttributeValue().withS("s1"))));
        when(mockDynamoDBClient.scan(any(ScanRequest.class))).thenReturn(res);

        Stream<TestEntity> stream = mapper.scan(List.of("id"));
        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    // Mapper.scan(attributesToGet, scanFilter)
    @Test
    public void testMapperScanWithAttrsAndFilter() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        ScanResult res = new ScanResult().withItems(List.of(Map.of("id", new AttributeValue().withS("s3"))));
        when(mockDynamoDBClient.scan(any(ScanRequest.class))).thenReturn(res);

        Stream<TestEntity> stream = mapper.scan(List.of("id"), Filters.eq("status", "active"));
        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    // Mapper.scan(scanFilter)
    @Test
    public void testMapperScanWithFilter() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        ScanResult res = new ScanResult().withItems(List.of(Map.of("id", new AttributeValue().withS("s2"))));
        when(mockDynamoDBClient.scan(any(ScanRequest.class))).thenReturn(res);

        Stream<TestEntity> stream = mapper.scan(Filters.eq("status", "active"));
        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testMapperStream() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        QueryRequest queryRequest = new QueryRequest().withTableName("TestTable");

        QueryResult queryResult = new QueryResult();
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue().withS("123"));
        items.add(item);
        queryResult.setItems(items);

        when(mockDynamoDBClient.query(any(QueryRequest.class))).thenReturn(queryResult);

        Stream<TestEntity> stream = mapper.stream(queryRequest);

        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    // Additional contracts

    /** Invalid entity IDs must fail locally instead of producing a rejected DynamoDB request. */
    @Test
    public void testMapperRejectsMissingAndEmptyKeyValues() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
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

    @Test
    public void testMapper_NullKeyMapThrowsIAE() {
        final DynamoDBExecutor.Mapper<TestEntityWithUserId> mapper = executor.mapper(TestEntityWithUserId.class);

        assertThrows(IllegalArgumentException.class, () -> mapper.getItem((Map<String, AttributeValue>) null));
        assertThrows(IllegalArgumentException.class, () -> mapper.deleteItem((Map<String, AttributeValue>) null));
    }

    // The mapper() messages concatenated the Class object itself, rendering "Entity class class com...$NoTableEntity" and
    // "class java.lang.Integer is not an entity class"; they now name the class by its canonical name.
    @Test
    public void testMapper_ErrorMessagesNameClassOnce() {
        final IllegalArgumentException noTable = assertThrows(IllegalArgumentException.class, () -> executor.mapper(NoTableEntity.class));
        assertTrue(noTable.getMessage().startsWith("Entity class " + com.landawn.abacus.util.ClassUtil.getCanonicalClassName(NoTableEntity.class) + " must"),
                noTable.getMessage());
        org.junit.jupiter.api.Assertions.assertFalse(noTable.getMessage().contains("class class"), noTable.getMessage());

        final IllegalArgumentException notBean = assertThrows(IllegalArgumentException.class, () -> executor.mapper(Integer.class, "t", null));
        assertTrue(notBean.getMessage().startsWith("java.lang.Integer is not an entity class"), notBean.getMessage());
    }

    @Test
    public void testMapper_NoTableAnnotationThrows() {
        assertThrows(IllegalArgumentException.class, () -> executor.mapper(NoTableEntity.class));
    }

    // Tests for Mapper inner class
    @Test
    public void testMapperCreation() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        assertNotNull(mapper);
    }

    @Test
    public void testMapperSupportsCompositePrimaryKeyAndRejectsMoreThanTwoIds() {
        final DynamoDBExecutor.Mapper<CompositeKeyEntity> mapper = executor.mapper(CompositeKeyEntity.class);
        final CompositeKeyEntity entity = new CompositeKeyEntity();
        entity.setPartitionId("tenant-1");
        entity.setSortId("item-9");
        when(mockDynamoDBClient.getItem(eq("TestTable"), any(Map.class))).thenReturn(new GetItemResult());

        mapper.getItem(entity);

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Map<String, AttributeValue>> keyCaptor = ArgumentCaptor.forClass(Map.class);
        verify(mockDynamoDBClient).getItem(eq("TestTable"), keyCaptor.capture());
        assertEquals("tenant-1", keyCaptor.getValue().get("partitionId").getS());
        assertEquals("item-9", keyCaptor.getValue().get("sortId").getS());
        assertThrows(IllegalArgumentException.class, () -> executor.mapper(ThreeKeyEntity.class));
        assertThrows(IllegalArgumentException.class, () -> executor.mapper(CompositeKeyNameCollisionEntity.class, "TestTable", NamingPolicy.SNAKE_CASE));
    }

    @Test
    public void testMapperWithWrongTableName() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        GetItemRequest request = new GetItemRequest().withTableName("WrongTable").withKey(Map.of("id", new AttributeValue().withS("123")));

        assertThrows(IllegalArgumentException.class, () -> {
            mapper.getItem(request);
        });
    }

    // Entity used to exercise mapper() failure when @Table is missing
    public static class NoTableEntity {
        @com.landawn.abacus.annotation.Id
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

        public void setPartitionId(final String partitionId) {
            this.partitionId = partitionId;
        }

        public String getSortId() {
            return sortId;
        }

        public void setSortId(final String sortId) {
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

        public void setFirstId(final String firstId) {
            this.firstId = firstId;
        }

        public String getSecondId() {
            return secondId;
        }

        public void setSecondId(final String secondId) {
            this.secondId = secondId;
        }

        public String getThirdId() {
            return thirdId;
        }

        public void setThirdId(final String thirdId) {
            this.thirdId = thirdId;
        }
    }

    @com.landawn.abacus.annotation.Table(name = "TestTable")
    private static class TestEntityWithUserId {
        @com.landawn.abacus.annotation.Id
        private String userId;
        private String firstName;

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
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
}
