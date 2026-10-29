package com.landawn.abacus.da.aws.dynamodb.v2;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.Filters;
import com.landawn.abacus.util.Clazz;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.stream.Stream;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
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

public class DynamoDBExecutorTest extends DynamoDBExecutorTestSupport {

    // Construction and configuration

    @Test
    public void testConstructor_NullClientThrows() {
        assertThrows(IllegalArgumentException.class, () -> new DynamoDBExecutor(null));
    }

    @Test
    public void testDynamoDBClient() {
        DynamoDbClient client = executor.dynamoDBClient();
        assertNotNull(client);
        assertEquals(mockDynamoDbClient, client);
    }

    // Metadata and validation

    @Test
    public void testRegisteredAttributeValueConverter_StringArrayTargetParsesJsonText() {
        assertNotNull(executor); // the executor class (and so its AttributeValue converter) is initialized

        assertArrayEquals(new String[] { "p", "q" }, com.landawn.abacus.util.N.convert(AttributeValue.fromS("[\"p\", \"q\"]"), String[].class));
    }

    // Row and entity mapping

    @Test
    public void testGetItemRequest_ReadRowEmptyResponse_DefaultForPrimitive() {
        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().build());

        GetItemRequest req = GetItemRequest.builder().tableName("T").key(Map.of("id", AttributeValue.builder().s("x").build())).build();
        Integer result = executor.getItem(req, int.class);
        assertEquals(0, result);
    }

    @Test
    public void testGetItemRequest_ReadRowEmptyResponse_NullForBean() {
        // GetItemResponse with no item triggers the "row == null" branch on readRow
        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().build());

        GetItemRequest req = GetItemRequest.builder().tableName("T").key(Map.of("id", AttributeValue.builder().s("x").build())).build();
        TestEntity result = executor.getItem(req, TestEntity.class);
        assertNull(result);
    }

    @Test
    public void testGetItemRequest_ReadRowAsCollectionClass() {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("a", AttributeValue.builder().s("x").build());
        item.put("b", AttributeValue.builder().s("y").build());
        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(item).build());

        GetItemRequest req = GetItemRequest.builder().tableName("T").key(Map.of("id", AttributeValue.builder().s("1").build())).build();
        List<?> result = executor.<List<?>> getItem(req, (Class<List<?>>) (Class<?>) List.class);
        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    public void testGetItemRequest_ReadRowAsMapClass() {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("a", AttributeValue.builder().s("x").build());
        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(item).build());

        GetItemRequest req = GetItemRequest.builder().tableName("T").key(Map.of("id", AttributeValue.builder().s("1").build())).build();
        Map<?, ?> result = executor.<Map<?, ?>> getItem(req, (Class<Map<?, ?>>) (Class<?>) Map.class);
        assertNotNull(result);
        assertEquals("x", result.get("a"));
    }

    @Test
    public void testGetItemRequest_ReadRowAsObjectArrayClass() {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("a", AttributeValue.builder().s("x").build());
        item.put("b", AttributeValue.builder().s("y").build());
        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(item).build());

        GetItemRequest req = GetItemRequest.builder().tableName("T").key(Map.of("id", AttributeValue.builder().s("1").build())).build();
        Object[] result = executor.getItem(req, Object[].class);
        assertNotNull(result);
        assertEquals(2, result.length);
    }

    @Test
    public void testGetItemRequest_ReadRowAsSingleValueClass() {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("v", AttributeValue.builder().s("hello").build());
        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(item).build());

        GetItemRequest req = GetItemRequest.builder().tableName("T").key(Map.of("id", AttributeValue.builder().s("1").build())).build();
        String result = executor.getItem(req, String.class);
        assertEquals("hello", result);
    }

    @Test
    public void testGetItemRequest_ReadRowAsSingleValueClass_MultiColumnThrows() {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("a", AttributeValue.builder().s("1").build());
        item.put("b", AttributeValue.builder().s("2").build());
        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(item).build());

        GetItemRequest req = GetItemRequest.builder().tableName("T").key(Map.of("id", AttributeValue.builder().s("1").build())).build();
        assertThrows(IllegalArgumentException.class, () -> executor.getItem(req, String.class));
    }

    @Test
    public void testNumericAttributesDecodeAsRawStringsAndConvertForTypedTargets() {
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("count", AttributeValue.fromN("30"));
        item.put("price", AttributeValue.fromN("1.50"));
        item.put("ids", AttributeValue.fromNs(List.of("7", "8")));

        // Untyped: the raw decimal Strings are preserved exactly (no precision loss / reformatting)
        final Map<String, Object> map = DynamoDBExecutor.toMap(item);
        assertEquals("30", map.get("count"));
        assertEquals("1.50", map.get("price"));
        assertEquals(List.of("7", "8"), map.get("ids"));

        // Typed single-column rows convert the N String to the requested Java number type
        when(mockDynamoDbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().item(Map.of("count", AttributeValue.fromN("30"))).build());
        final GetItemRequest request = GetItemRequest.builder().tableName("TestTable").key(DynamoDBExecutor.asKey("id", "1")).build();
        assertEquals(Long.valueOf(30L), executor.getItem(request, Long.class));
        assertEquals(new java.math.BigDecimal("30"), executor.getItem(request, java.math.BigDecimal.class));
        assertEquals("30", executor.getItem(request, String.class));

        // Entity properties convert too, including the elements of a native NS set
        final TypedCollectionEntity entity = DynamoDBExecutor
                .toEntity(Map.of("id", AttributeValue.fromS("1"), "tags", AttributeValue.fromNs(List.of("7", "8"))), TypedCollectionEntity.class);
        assertNotNull(entity.getTags());
        assertTrue(entity.getTags().containsAll(List.of(7L, 8L)), "expected typed elements, got " + entity.getTags());
    }

    // Factories and insert operations

    @Test
    public void testPutItem() {
        String tableName = "TestTable";
        Map<String, AttributeValue> item = Map.of("id", AttributeValue.builder().s("123").build());

        PutItemResponse response = PutItemResponse.builder().build();

        when(mockDynamoDbClient.putItem(any(PutItemRequest.class))).thenReturn(response);

        PutItemResponse result = executor.putItem(tableName, item);

        assertNotNull(result);
    }

    // TODO: v2 toItem(Collection), toUpdateItem(Collection), putItem(String, Object) are package-private
    // and inaccessible from this test package - exercised indirectly via Mapper.batchPutItem/batchPutItem
    @Test
    public void testPutItem_StringMap_PackagePrivate() {
        when(mockDynamoDbClient.putItem(any(PutItemRequest.class))).thenReturn(PutItemResponse.builder().build());
        PutItemResponse result = executor.putItem("T", Map.of("id", AttributeValue.builder().s("1").build()));
        assertNotNull(result);
    }

    // put/update/delete overloads
    @Test
    public void testPutItemWithRequest() {
        PutItemRequest req = PutItemRequest.builder().tableName("TestTable").item(Map.of("id", AttributeValue.builder().s("1").build())).build();
        PutItemResponse res = PutItemResponse.builder().build();
        when(mockDynamoDbClient.putItem(req)).thenReturn(res);

        PutItemResponse result = executor.putItem(req);
        assertNotNull(result);
    }

    @Test
    public void testPutItemWithReturnValues() {
        Map<String, AttributeValue> item = Map.of("id", AttributeValue.builder().s("1").build());
        PutItemResponse res = PutItemResponse.builder().build();
        when(mockDynamoDbClient.putItem(any(PutItemRequest.class))).thenReturn(res);

        PutItemResponse result = executor.putItem("TestTable", item, "ALL_OLD");
        assertNotNull(result);
    }

    // Updates and deletes

    @Test
    public void testDeleteItem() {
        String tableName = "TestTable";
        Map<String, AttributeValue> key = Map.of("id", AttributeValue.builder().s("123").build());

        DeleteItemResponse response = DeleteItemResponse.builder().build();

        when(mockDynamoDbClient.deleteItem(any(DeleteItemRequest.class))).thenReturn(response);

        DeleteItemResponse result = executor.deleteItem(tableName, key);

        assertNotNull(result);
    }

    @Test
    public void testDeleteItem_StringMap_PackagePrivate() {
        when(mockDynamoDbClient.deleteItem(any(DeleteItemRequest.class))).thenReturn(DeleteItemResponse.builder().build());
        DeleteItemResponse result = executor.deleteItem("T", Map.of("id", AttributeValue.builder().s("1").build()));
        assertNotNull(result);
    }

    @Test
    public void testDeleteItemWithRequest() {
        DeleteItemRequest req = DeleteItemRequest.builder().tableName("TestTable").key(Map.of("id", AttributeValue.builder().s("1").build())).build();
        DeleteItemResponse res = DeleteItemResponse.builder().build();
        when(mockDynamoDbClient.deleteItem(req)).thenReturn(res);

        DeleteItemResponse result = executor.deleteItem(req);
        assertNotNull(result);
    }

    @Test
    public void testDeleteItemWithReturnValues() {
        Map<String, AttributeValue> key = Map.of("id", AttributeValue.builder().s("1").build());
        DeleteItemResponse res = DeleteItemResponse.builder().build();
        when(mockDynamoDbClient.deleteItem(any(DeleteItemRequest.class))).thenReturn(res);

        DeleteItemResponse result = executor.deleteItem("TestTable", key, "ALL_OLD");
        assertNotNull(result);
    }

    @Test
    public void testUpdateItem() {
        String tableName = "TestTable";
        Map<String, AttributeValue> key = Map.of("id", AttributeValue.builder().s("123").build());
        Map<String, AttributeValueUpdate> attributeUpdates = Map.of("name",
                AttributeValueUpdate.builder().value(AttributeValue.builder().s("updated").build()).action(AttributeAction.PUT).build());

        UpdateItemResponse response = UpdateItemResponse.builder().build();

        when(mockDynamoDbClient.updateItem(any(UpdateItemRequest.class))).thenReturn(response);

        UpdateItemResponse result = executor.updateItem(tableName, key, attributeUpdates);

        assertNotNull(result);
    }

    @Test
    public void testUpdateItem_StringMapMap_PackagePrivate() {
        when(mockDynamoDbClient.updateItem(any(UpdateItemRequest.class))).thenReturn(UpdateItemResponse.builder().build());
        UpdateItemResponse result = executor.updateItem("T", Map.of("id", AttributeValue.builder().s("1").build()),
                Map.of("name", AttributeValueUpdate.builder().value(AttributeValue.builder().s("x").build()).build()));
        assertNotNull(result);
    }

    @Test
    public void testUpdateItemWithRequest() {
        UpdateItemRequest req = UpdateItemRequest.builder().tableName("TestTable").key(Map.of("id", AttributeValue.builder().s("1").build())).build();
        UpdateItemResponse res = UpdateItemResponse.builder().build();
        when(mockDynamoDbClient.updateItem(req)).thenReturn(res);

        UpdateItemResponse result = executor.updateItem(req);
        assertNotNull(result);
    }

    @Test
    public void testUpdateItemWithReturnValues() {
        Map<String, AttributeValue> key = Map.of("id", AttributeValue.builder().s("1").build());
        Map<String, AttributeValueUpdate> upd = Map.of("name",
                AttributeValueUpdate.builder().value(AttributeValue.builder().s("x").build()).action(AttributeAction.PUT).build());
        UpdateItemResponse res = UpdateItemResponse.builder().build();
        when(mockDynamoDbClient.updateItem(any(UpdateItemRequest.class))).thenReturn(res);

        UpdateItemResponse result = executor.updateItem("TestTable", key, upd, "ALL_NEW");
        assertNotNull(result);
    }

    // Queries and results

    @Test
    public void testBatchGetItem() {
        Map<String, KeysAndAttributes> requestItems = new HashMap<>();
        requestItems.put("TestTable", KeysAndAttributes.builder().keys(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build());

        BatchGetItemResponse response = BatchGetItemResponse.builder()
                .responses(Map.of("TestTable", List.of(Map.of("id", AttributeValue.builder().s("1").build()))))
                .build();

        when(mockDynamoDbClient.batchGetItem(any(BatchGetItemRequest.class))).thenReturn(response);

        Map<String, List<Map<String, Object>>> result = executor.batchGetItem(requestItems);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(1, result.get("TestTable").size());
    }

    // TODO: putItem(String, Object) overloads are package-private and cannot be invoked from this test package.

    // batchGetItem with empty response paths
    @Test
    public void testBatchGetItem_NullResponses() {
        Map<String, KeysAndAttributes> req = Map.of("TestTable",
                KeysAndAttributes.builder().keys(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build());
        when(mockDynamoDbClient.batchGetItem(any(BatchGetItemRequest.class))).thenReturn(BatchGetItemResponse.builder().build());

        Map<String, List<TestEntity>> result = executor.batchGetItem(req, TestEntity.class);
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @Test
    public void testBatchGetItemWithRequest() {
        BatchGetItemRequest req = BatchGetItemRequest.builder()
                .requestItems(Map.of("TestTable", KeysAndAttributes.builder().keys(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build()))
                .build();
        BatchGetItemResponse res = BatchGetItemResponse.builder()
                .responses(Map.of("TestTable", List.of(Map.of("id", AttributeValue.builder().s("1").build()))))
                .build();
        when(mockDynamoDbClient.batchGetItem(req)).thenReturn(res);

        Map<String, List<Map<String, Object>>> result = executor.batchGetItem(req);
        assertNotNull(result);
        assertEquals(1, result.get("TestTable").size());
    }

    @Test
    public void testBatchGetItemWithRequestAndClass() {
        BatchGetItemRequest req = BatchGetItemRequest.builder()
                .requestItems(Map.of("TestTable", KeysAndAttributes.builder().keys(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build()))
                .build();
        BatchGetItemResponse res = BatchGetItemResponse.builder()
                .responses(Map.of("TestTable", List.of(Map.of("id", AttributeValue.builder().s("1").build()))))
                .build();
        when(mockDynamoDbClient.batchGetItem(req)).thenReturn(res);

        Map<String, List<TestEntity>> result = executor.batchGetItem(req, TestEntity.class);
        assertNotNull(result);
        assertEquals(1, result.get("TestTable").size());
        assertEquals("1", result.get("TestTable").get(0).getId());
    }

    @Test
    public void testBatchGetItemWithRequestAndClass_NullArgsThrowEagerly() {
        // Regression: batchGetItem(BatchGetItemRequest, Class) validated targetClass only lazily
        // (inside createRowMapper), AFTER the billable network call had already been issued — and
        // not at all when the response carried no Responses field. Both args are now guarded
        // eagerly, matching the scan/list/query/stream family.
        BatchGetItemRequest req = BatchGetItemRequest.builder().build();
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> executor.batchGetItem((BatchGetItemRequest) null, Map.class));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> executor.batchGetItem(req, (Class<?>) null));
        org.mockito.Mockito.verify(mockDynamoDbClient, org.mockito.Mockito.never()).batchGetItem(any(BatchGetItemRequest.class));
    }

    // batchGetItem overloads
    @Test
    public void testBatchGetItemWithReturnConsumedCapacity() {
        Map<String, KeysAndAttributes> requestItems = Map.of("TestTable",
                KeysAndAttributes.builder().keys(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build());
        BatchGetItemResponse res = BatchGetItemResponse.builder()
                .responses(Map.of("TestTable", List.of(Map.of("id", AttributeValue.builder().s("1").build()))))
                .build();
        when(mockDynamoDbClient.batchGetItem(any(BatchGetItemRequest.class))).thenReturn(res);

        Map<String, List<Map<String, Object>>> result = executor.batchGetItem(requestItems, "TOTAL");
        assertNotNull(result);
        assertEquals(1, result.get("TestTable").size());
    }

    @Test
    public void testGetItem() {
        String tableName = "TestTable";
        Map<String, AttributeValue> key = Map.of("id", AttributeValue.builder().s("123").build());

        GetItemResponse response = GetItemResponse.builder()
                .item(Map.of("id", AttributeValue.builder().s("123").build(), "name", AttributeValue.builder().s("test").build()))
                .build();

        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(response);

        Map<String, Object> result = executor.getItem(tableName, key);

        assertNotNull(result);
        assertEquals("123", result.get("id"));
        assertEquals("test", result.get("name"));
    }

    @Test
    public void testGetItem_ByteBufferTargetClass_IsReadable() {
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("buf", AttributeValue.fromB(software.amazon.awssdk.core.SdkBytes.fromByteArray(new byte[] { 4, 5 })));
        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(item).build());

        final ByteBuffer buf = executor.getItem(GetItemRequest.builder().tableName("t").key(DynamoDBExecutor.asKey("id", "1")).build(), ByteBuffer.class);

        assertEquals(0, buf.position());
        assertEquals(2, buf.remaining());
        assertEquals(4, buf.get(0));
        assertEquals(5, buf.get(1));
    }

    @Test
    public void testGetItem_StringMapWithConsistentRead_PackagePrivate() {
        Map<String, AttributeValue> item = Map.of("id", AttributeValue.builder().s("1").build());
        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(item).build());
        Map<String, Object> result = executor.getItem("T", Map.of("id", AttributeValue.builder().s("1").build()), Boolean.TRUE);
        assertNotNull(result);
    }

    @Test
    public void testGetItemByKeyMissingItemReturnsDefaultForPrimitiveTargetClass() {
        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().build());
        final Map<String, AttributeValue> key = DynamoDBExecutor.asKey("id", "missing");

        assertEquals(0, (int) executor.getItem("TestTable", key, int.class));
        assertEquals(false, executor.getItem("TestTable", key, true, boolean.class));
        assertNull(executor.getItem("TestTable", key, Integer.class));
        assertNull(executor.getItem("TestTable", key, false, TestEntity.class));
        assertNull(executor.getItem("TestTable", key));
        verify(mockDynamoDbClient, times(5)).getItem(any(GetItemRequest.class));
    }

    @Test
    public void testGetItemRequest_NullTargetClassThrowsIAE() {
        // Regression: getItem(GetItemRequest, Class) documents "@throws IllegalArgumentException if
        // any parameter is null" and every sibling read API guards targetClass, but this chokepoint
        // silently fell through to readRow's null-tolerant Object[] branch.
        GetItemRequest req = GetItemRequest.builder().tableName("T").key(Map.of("id", AttributeValue.builder().s("1").build())).build();
        assertThrows(IllegalArgumentException.class, () -> executor.getItem(req, null));
        verify(mockDynamoDbClient, never()).getItem(any(GetItemRequest.class));
    }

    @Test
    public void testGetItemWithConsistentRead() {
        String tableName = "TestTable";
        Map<String, AttributeValue> key = Map.of("id", AttributeValue.builder().s("123").build());
        Boolean consistentRead = true;

        GetItemResponse response = GetItemResponse.builder().item(Map.of("id", AttributeValue.builder().s("123").build())).build();

        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(response);

        Map<String, Object> result = executor.getItem(tableName, key, consistentRead);

        assertNotNull(result);
        assertEquals("123", result.get("id"));
    }

    @Test
    public void testGetItemWithConsistentReadAndClass() {
        Map<String, AttributeValue> key = Map.of("id", AttributeValue.builder().s("1").build());
        GetItemResponse res = GetItemResponse.builder().item(Map.of("id", AttributeValue.builder().s("1").build())).build();
        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(res);

        TestEntity result = executor.getItem("TestTable", key, true, TestEntity.class);
        assertNotNull(result);
        assertEquals("1", result.getId());
    }

    // GetItemRequest entry points
    @Test
    public void testGetItemWithGetItemRequest() {
        GetItemRequest req = GetItemRequest.builder().tableName("TestTable").key(Map.of("id", AttributeValue.builder().s("1").build())).build();
        GetItemResponse res = GetItemResponse.builder()
                .item(Map.of("id", AttributeValue.builder().s("1").build(), "name", AttributeValue.builder().s("n").build()))
                .build();
        when(mockDynamoDbClient.getItem(req)).thenReturn(res);

        Map<String, Object> result = executor.getItem(req);
        assertNotNull(result);
        assertEquals("1", result.get("id"));
        assertEquals("n", result.get("name"));
    }

    @Test
    public void testGetItemWithGetItemRequestAndClass() {
        GetItemRequest req = GetItemRequest.builder().tableName("TestTable").key(Map.of("id", AttributeValue.builder().s("1").build())).build();
        GetItemResponse res = GetItemResponse.builder()
                .item(Map.of("id", AttributeValue.builder().s("1").build(), "name", AttributeValue.builder().s("n").build()))
                .build();
        when(mockDynamoDbClient.getItem(req)).thenReturn(res);

        TestEntity result = executor.getItem(req, TestEntity.class);
        assertNotNull(result);
        assertEquals("1", result.getId());
        assertEquals("n", result.getName());
    }

    @Test
    public void testGetItemWithTargetClass() {
        String tableName = "TestTable";
        Map<String, AttributeValue> key = Map.of("id", AttributeValue.builder().s("123").build());

        GetItemResponse response = GetItemResponse.builder()
                .item(Map.of("id", AttributeValue.builder().s("123").build(), "name", AttributeValue.builder().s("test").build()))
                .build();

        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(response);

        TestEntity result = executor.getItem(tableName, key, TestEntity.class);

        assertNotNull(result);
        assertEquals("123", result.getId());
        assertEquals("test", result.getName());
    }

    @Test
    public void testList() {
        QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();

        QueryResponse response = QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("123").build()))).build();

        when(mockDynamoDbClient.query(queryRequest)).thenReturn(response);

        List<Map<String, Object>> result = executor.list(queryRequest);

        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testListWithClassPaginates() {
        QueryRequest req = QueryRequest.builder().tableName("TestTable").build();
        QueryResponse page1 = QueryResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("1").build())))
                .lastEvaluatedKey(Map.of("id", AttributeValue.builder().s("1").build()))
                .build();
        QueryResponse page2 = QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("2").build()))).build();

        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(page1, page2);

        List<TestEntity> result = executor.list(req, TestEntity.class);
        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    public void testQuery() {
        QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();

        QueryResponse response = QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("123").build()))).build();

        when(mockDynamoDbClient.query(queryRequest)).thenReturn(response);

        Dataset result = executor.query(queryRequest);

        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testQuery_NullQueryRequestThrows() {
        // query(QueryRequest, Class) eagerly rejects a null request (matches its documented @throws IAE).
        assertThrows(IllegalArgumentException.class, () -> executor.query(null, TestEntity.class));
        // The no-class overload delegates to the same path.
        assertThrows(IllegalArgumentException.class, () -> executor.query((QueryRequest) null));
    }

    @Test
    public void testQuery_NullTargetClassThrows() {
        // targetClass must not be null; pass a Map subtype for raw-attribute rows (matches list/stream + async twin).
        QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();
        assertThrows(IllegalArgumentException.class, () -> executor.query(queryRequest, (Class<?>) null));
    }

    /**
     * Regression test mirroring {@link #testQueryWithPaginationDoesNotThrowOnImmutableItems()} for
     * the untyped {@code query(QueryRequest)} entry point.
     */
    @Test
    public void testQueryUntypedWithPaginationDoesNotThrowOnImmutableItems() {
        QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();

        QueryResponse page1 = QueryResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("a").build())))
                .lastEvaluatedKey(Map.of("id", AttributeValue.builder().s("a").build()))
                .build();

        QueryResponse page2 = QueryResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("b").build()), Map.of("id", AttributeValue.builder().s("c").build())))
                .build();

        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(page1, page2);

        Dataset result = executor.query(queryRequest);

        assertNotNull(result);
        assertEquals(3, result.size());
    }

    // query(QueryRequest, Class) with a non-bean (Map) class
    @Test
    public void testQueryWithClass_MapBranch() {
        QueryRequest req = QueryRequest.builder().tableName("TestTable").build();
        QueryResponse res = QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build();
        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(res);

        Dataset ds = executor.query(req, Clazz.PROPS_MAP);
        assertNotNull(ds);
        assertEquals(1, ds.size());
    }

    /**
     * Regression test: when a Query result is paginated (has a non-empty LastEvaluatedKey) the
     * {@code query(QueryRequest, Class)} Map branch must aggregate subsequent pages. AWS SDK v2's
     * {@code QueryResponse.items()} returns an immutable list, so the previous implementation threw
     * {@link UnsupportedOperationException} when calling {@code addAll} on it. The fix copies the
     * items into a mutable list before aggregating additional pages.
     */
    @Test
    public void testQueryWithPaginationDoesNotThrowOnImmutableItems() {
        QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();

        QueryResponse page1 = QueryResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("1").build())))
                .lastEvaluatedKey(Map.of("id", AttributeValue.builder().s("1").build()))
                .build();

        QueryResponse page2 = QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("2").build()))).build();

        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(page1, page2);

        Dataset result = executor.query(queryRequest, Map.class);

        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    public void testScan() {
        String tableName = "TestTable";
        List<String> attributesToGet = List.of("id", "name");

        ScanResponse response = ScanResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("123").build()))).build();

        when(mockDynamoDbClient.scan(any(ScanRequest.class))).thenReturn(response);

        Stream<Map<String, Object>> stream = executor.scan(tableName, attributesToGet);

        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testScan_emptyAttributesToGetOmitsLegacyProjection() {
        when(mockDynamoDbClient.scan(any(ScanRequest.class))).thenReturn(ScanResponse.builder().items(List.of()).build());

        assertEquals(0, executor.scan("TestTable", List.of()).count());

        final ArgumentCaptor<ScanRequest> requestCaptor = ArgumentCaptor.forClass(ScanRequest.class);
        verify(mockDynamoDbClient).scan(requestCaptor.capture());
        assertFalse(requestCaptor.getValue().hasAttributesToGet());
    }

    // scan overloads
    @Test
    public void testScanByTableNameAndScanFilter() {
        ScanResponse res = ScanResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build();
        when(mockDynamoDbClient.scan(any(ScanRequest.class))).thenReturn(res);

        Stream<Map<String, Object>> stream = executor.scan("TestTable", Filters.eq("status", "active"));
        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testScanByTableNameAttrsAndScanFilter() {
        ScanResponse res = ScanResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build();
        when(mockDynamoDbClient.scan(any(ScanRequest.class))).thenReturn(res);

        Stream<Map<String, Object>> stream = executor.scan("TestTable", List.of("id"), Filters.eq("status", "active"));
        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    /**
     * Regression test mirroring {@link #testStreamSkipsEmptyIntermediatePageAndContinuesPagination()}
     * for the scan stream pagination path.
     */
    @Test
    public void testScanStreamSkipsEmptyIntermediatePageAndContinuesPagination() {
        ScanRequest scanRequest = ScanRequest.builder().tableName("TestTable").build();

        ScanResponse page1 = ScanResponse.builder().items(List.of()).lastEvaluatedKey(Map.of("id", AttributeValue.builder().s("k1").build())).build();
        ScanResponse page2 = ScanResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("1").build()), Map.of("id", AttributeValue.builder().s("2").build()),
                        Map.of("id", AttributeValue.builder().s("3").build())))
                .build();

        when(mockDynamoDbClient.scan(any(ScanRequest.class))).thenReturn(page1, page2);

        Stream<Map<String, Object>> stream = executor.scan(scanRequest);

        assertNotNull(stream);
        assertEquals(3, stream.count());
    }

    @Test
    public void testScanWithAttrsAndClass() {
        ScanResponse res = ScanResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build();
        when(mockDynamoDbClient.scan(any(ScanRequest.class))).thenReturn(res);

        Stream<TestEntity> stream = executor.scan("TestTable", List.of("id"), TestEntity.class);
        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testScanWithAttrsFilterAndClass() {
        ScanResponse res = ScanResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build();
        when(mockDynamoDbClient.scan(any(ScanRequest.class))).thenReturn(res);

        Stream<TestEntity> stream = executor.scan("TestTable", List.of("id"), Filters.eq("status", "active"), TestEntity.class);
        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testScanWithFilterAndClass() {
        ScanResponse res = ScanResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build();
        when(mockDynamoDbClient.scan(any(ScanRequest.class))).thenReturn(res);

        Stream<TestEntity> stream = executor.scan("TestTable", Filters.eq("status", "active"), TestEntity.class);
        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testStream() {
        QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();

        QueryResponse response = QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("123").build()))).build();

        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(response);

        Stream<Map<String, Object>> stream = executor.stream(queryRequest);

        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testStreamAndScan_NullArgsThrow() {
        QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();
        ScanRequest scanRequest = ScanRequest.builder().tableName("TestTable").build();

        assertThrows(IllegalArgumentException.class, () -> executor.stream((QueryRequest) null, Map.class));
        assertThrows(IllegalArgumentException.class, () -> executor.stream(queryRequest, (Class<?>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.scan((ScanRequest) null, Map.class));
        assertThrows(IllegalArgumentException.class, () -> executor.scan(scanRequest, (Class<?>) null));

        assertThrows(IllegalArgumentException.class, () -> executor.scan((String) null, List.of("id")));
        assertThrows(IllegalArgumentException.class, () -> executor.scan((String) null, Map.<String, Condition> of()));
        assertThrows(IllegalArgumentException.class, () -> executor.scan((String) null, List.of("id"), Map.<String, Condition> of()));
        assertThrows(IllegalArgumentException.class, () -> executor.scan((String) null, List.of("id"), Map.class));
        assertThrows(IllegalArgumentException.class, () -> executor.scan((String) null, Map.<String, Condition> of(), Map.class));
        assertThrows(IllegalArgumentException.class, () -> executor.scan((String) null, List.of("id"), Map.<String, Condition> of(), Map.class));

        assertThrows(IllegalArgumentException.class, () -> executor.scan("TestTable", List.of("id"), (Class<?>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.scan("TestTable", Map.<String, Condition> of(), (Class<?>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.scan("TestTable", List.of("id"), Map.<String, Condition> of(), (Class<?>) null));
    }

    /**
     * Regression test: {@code stream(QueryRequest, Class)} must not terminate prematurely when an
     * intermediate page returns zero items but a non-empty LastEvaluatedKey (common with filter
     * expressions). AWS SDK v2's {@code QueryResponse.hasItems()} returns {@code true} even for an
     * empty (but present) items list, so the previous {@code if (queryResult.hasItems())} check
     * broke out of the pagination loop and dropped all subsequent pages.
     */
    @Test
    public void testStreamSkipsEmptyIntermediatePageAndContinuesPagination() {
        QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();

        // Page 1: empty items but has LastEvaluatedKey -> must continue paginating, not stop.
        QueryResponse page1 = QueryResponse.builder().items(List.of()).lastEvaluatedKey(Map.of("id", AttributeValue.builder().s("k1").build())).build();
        // Page 2: real data, no more pages.
        QueryResponse page2 = QueryResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("1").build()), Map.of("id", AttributeValue.builder().s("2").build())))
                .build();

        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(page1, page2);

        Stream<Map<String, Object>> stream = executor.stream(queryRequest);

        assertNotNull(stream);
        assertEquals(2, stream.count());
    }

    @Test
    public void testStreamWithClass() {
        QueryRequest req = QueryRequest.builder().tableName("TestTable").build();
        QueryResponse res = QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build();
        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(res);

        Stream<TestEntity> stream = executor.stream(req, TestEntity.class);
        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    // Resource lifecycle

    @Test
    public void testClose() {
        executor.close();
        verify(mockDynamoDbClient, times(1)).close();
    }

    // close() should not throw when client is non-null
    @Test
    public void testClose_NoThrow() {
        DynamoDBExecutor exec = new DynamoDBExecutor(mockDynamoDbClient);
        assertDoesNotThrow(exec::close);
    }

    @Test
    public void testExecutorNullRequestObjectsThrowIllegalArgumentExceptionBeforeCallingClient() {
        assertThrows(IllegalArgumentException.class, () -> executor.getItem((GetItemRequest) null));
        assertThrows(IllegalArgumentException.class, () -> executor.getItem((GetItemRequest) null, TestEntity.class));
        assertThrows(IllegalArgumentException.class, () -> executor.putItem((PutItemRequest) null));
        assertThrows(IllegalArgumentException.class, () -> executor.batchWriteItem((BatchWriteItemRequest) null));
        assertThrows(IllegalArgumentException.class, () -> executor.updateItem((UpdateItemRequest) null));
        assertThrows(IllegalArgumentException.class, () -> executor.deleteItem((DeleteItemRequest) null));

        org.mockito.Mockito.verifyNoInteractions(mockDynamoDbClient);
    }

    // Additional contracts

    @Test
    public void testBatchWriteItem() {
        Map<String, List<WriteRequest>> requestItems = new HashMap<>();
        List<WriteRequest> writeRequests = List
                .of(WriteRequest.builder().putRequest(PutRequest.builder().item(Map.of("id", AttributeValue.builder().s("123").build())).build()).build());
        requestItems.put("TestTable", writeRequests);

        BatchWriteItemResponse response = BatchWriteItemResponse.builder().build();

        when(mockDynamoDbClient.batchWriteItem(any(BatchWriteItemRequest.class))).thenReturn(response);

        BatchWriteItemResponse result = executor.batchWriteItem(requestItems);

        assertNotNull(result);
    }

    // ---- 2026-10-19 sliceC ----

    @Test
    public void testList_ConvertsEveryPageInOrderIncludingItemlessPages_sliceC() {
        final QueryRequest req = QueryRequest.builder().tableName("TestTable").build();
        final QueryResponse page1 = QueryResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.fromS("1")), Map.of("id", AttributeValue.fromS("2"))))
                .lastEvaluatedKey(Map.of("id", AttributeValue.fromS("2")))
                .build();
        // An item-less page (no items member at all) that still continues the pagination.
        final QueryResponse page2 = QueryResponse.builder().lastEvaluatedKey(Map.of("id", AttributeValue.fromS("2b"))).build();
        final QueryResponse page3 = QueryResponse.builder().items(List.of()).lastEvaluatedKey(Map.of("id", AttributeValue.fromS("2c"))).build();
        final QueryResponse page4 = QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.fromS("3")))).build();

        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(page1, page2, page3, page4);

        final List<TestEntity> entities = executor.list(req, TestEntity.class);
        final ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(mockDynamoDbClient, times(4)).query(captor.capture());

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(List.of("1", "2", "3"), entities.stream().map(TestEntity::getId).toList()),
                () -> assertDoesNotThrow(() -> entities.add(new TestEntity())), // the aggregated result stays mutable
                () -> assertNull(captor.getAllValues().get(0).exclusiveStartKey().get("id")),
                () -> assertEquals("2", captor.getAllValues().get(1).exclusiveStartKey().get("id").s()),
                () -> assertEquals("2b", captor.getAllValues().get(2).exclusiveStartKey().get("id").s()),
                () -> assertEquals("2c", captor.getAllValues().get(3).exclusiveStartKey().get("id").s()));

        // A non-bean row type uses the same per-page conversion.
        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(page1, page2, page3, page4);
        assertEquals(List.of("1", "2", "3"), executor.list(req, String.class));
    }

    // ---- 2026-10-20 fixC ----

    /**
     * list(QueryRequest, Class) converts each response page directly instead of through toList(QueryResponse, Class). A null
     * response, which only an un-stubbed client produces, must keep the former toList behaviour: an empty (mutable) list, and
     * no further page is requested.
     */
    @Test
    public void testListWithClass_NullResponseIsEmptyAndRequestsNoFurtherPage_fixC() {
        QueryRequest req = QueryRequest.builder().tableName("TestTable").build();
        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(null);

        List<TestEntity> result = executor.list(req, TestEntity.class);

        assertNotNull(result);
        assertTrue(result.isEmpty());
        assertDoesNotThrow(() -> result.add(new TestEntity()));
        verify(mockDynamoDbClient, times(1)).query(any(QueryRequest.class));
    }

    // ---- 2026-10-21 fixP ----

    /**
     * A null LATER page (page 1 has rows and a lastEvaluatedKey, page 2 is null) must end the pagination the same way a null
     * first page does: the result holds page 1's rows, exactly two requests are made and nothing is thrown (the do/while loop
     * used to dereference the null page and fail with a NullPointerException).
     */
    @Test
    public void testListWithClass_NullLaterPageEndsPaginationWithRowsSoFar_fixP() {
        QueryRequest req = QueryRequest.builder().tableName("TestTable").build();
        QueryResponse page1 = QueryResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.fromS("1")), Map.of("id", AttributeValue.fromS("2"))))
                .lastEvaluatedKey(Map.of("id", AttributeValue.fromS("2")))
                .build();
        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(page1, (QueryResponse) null);

        List<TestEntity> result = executor.list(req, TestEntity.class);

        assertEquals(List.of("1", "2"), result.stream().map(TestEntity::getId).toList());
        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(mockDynamoDbClient, times(2)).query(captor.capture());
        assertTrue(captor.getAllValues().get(0).exclusiveStartKey().isEmpty());
        assertEquals("2", captor.getAllValues().get(1).exclusiveStartKey().get("id").s());
    }

    // ---- 2026-10-24 sliceC ----

    /**
     * Pins the documented row-type rule of query(QueryRequest, Class): a Map type builds the Dataset from the raw items and a
     * bean class from its properties, but an array, collection or single-value row type has no column names, so it is rejected
     * before the request is sent (since the 2026-10-24 apply round; it used to be rejected once the query returned an item),
     * while list(QueryRequest, Class) accepts it. An item-less result no longer makes such a type succeed.
     */
    @Test
    public void testQueryWithClass_NonBeanNonMapRowTypeRejectedBeforeAnyRequest_sliceC() {
        final QueryRequest req = QueryRequest.builder().tableName("TestTable").build();
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("id", AttributeValue.fromS("1"));
        item.put("name", AttributeValue.fromS("n"));
        final QueryResponse twoColumns = QueryResponse.builder().items(List.of(item)).build();
        final QueryResponse oneColumn = QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.fromS("1")))).build();

        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(twoColumns);
        org.junit.jupiter.api.Assertions.assertAll(() -> assertThrows(IllegalArgumentException.class, () -> executor.query(req, Object[].class)),
                () -> assertThrows(IllegalArgumentException.class, () -> executor.query(req, List.class)),
                () -> assertEquals(1, executor.list(req, Object[].class).size()),
                () -> assertEquals(List.of("id", "name"), executor.query(req, Map.class).columnNames()),
                () -> assertEquals("n", executor.query(req, TestEntity.class).toList(TestEntity.class).get(0).getName()));

        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(oneColumn);
        org.junit.jupiter.api.Assertions.assertAll(() -> assertThrows(IllegalArgumentException.class, () -> executor.query(req, String.class)),
                () -> assertEquals(List.of("1"), executor.list(req, String.class)));

        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().build());
        assertThrows(IllegalArgumentException.class, () -> executor.query(req, Object[].class));
    }

    /**
     * Pins the documented Dataset rule for items without attributes (a projection of attributes the items lack): such an item
     * is a row of nulls next to an item that has an attribute, but when no selected item has one the Dataset has no column and
     * therefore no row, although the list conversions still return one (empty) map per item.
     */
    @Test
    public void testExtractData_ItemsWithoutAttributesHaveNoRowsUnlessAnItemHasOne_sliceC() {
        final QueryResponse allEmpty = QueryResponse.builder().items(List.of(new HashMap<>(), new HashMap<>())).build();
        final QueryResponse mixed = QueryResponse.builder().items(List.of(Map.of("a", AttributeValue.fromS("x")), new HashMap<>())).build();
        final ScanResponse scanAllEmpty = ScanResponse.builder().items(List.of(new HashMap<>())).build();
        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(allEmpty);

        final Dataset mixedDataset = DynamoDBExecutor.extractData(mixed);

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(0, DynamoDBExecutor.extractData(allEmpty).size()),
                () -> assertEquals(0, DynamoDBExecutor.extractData(scanAllEmpty, 0, 1).size()),
                () -> assertEquals(0, executor.query(QueryRequest.builder().tableName("TestTable").build()).size()),
                () -> assertEquals(2, DynamoDBExecutor.toList(allEmpty, Map.class).size()), () -> assertEquals(2, mixedDataset.size()),
                () -> assertEquals(java.util.Arrays.asList("x", null), new java.util.ArrayList<>(mixedDataset.getColumn("a"))));
    }

    /**
     * Pins the documented difference between the paging entry points: list(QueryRequest, Class) returns only the requested page
     * when the request sets exclusiveStartKey, while stream(QueryRequest, Class) and scan(ScanRequest, Class) start from that key
     * and continue to the last page; a limit on the request is the page size, not a cap on the streamed rows.
     */
    @Test
    public void testStreamAndScanWithExclusiveStartKey_ContinueToTheLastPage_sliceC() {
        final Map<String, AttributeValue> startKey = Map.of("id", AttributeValue.fromS("0"));
        final QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").limit(1).exclusiveStartKey(startKey).build();
        final QueryResponse queryPage1 = QueryResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.fromS("1"))))
                .lastEvaluatedKey(Map.of("id", AttributeValue.fromS("1")))
                .build();
        final QueryResponse queryPage2 = QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.fromS("2")))).build();
        final ScanRequest scanRequest = ScanRequest.builder().tableName("TestTable").limit(1).exclusiveStartKey(startKey).build();
        final ScanResponse scanPage1 = ScanResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.fromS("1"))))
                .lastEvaluatedKey(Map.of("id", AttributeValue.fromS("1")))
                .build();
        final ScanResponse scanPage2 = ScanResponse.builder().items(List.of(Map.of("id", AttributeValue.fromS("2")))).build();

        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(queryPage1, queryPage2);
        final List<String> listed = executor.list(queryRequest, String.class);
        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(queryPage1, queryPage2);
        final List<String> streamed = executor.stream(queryRequest, String.class).toList();
        when(mockDynamoDbClient.scan(any(ScanRequest.class))).thenReturn(scanPage1, scanPage2);
        final List<String> scanned = executor.scan(scanRequest, String.class).toList();

        final ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(mockDynamoDbClient, times(3)).query(captor.capture());

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(List.of("1"), listed), () -> assertEquals(List.of("1", "2"), streamed),
                () -> assertEquals(List.of("1", "2"), scanned), () -> assertEquals("0", captor.getAllValues().get(1).exclusiveStartKey().get("id").s()),
                () -> assertEquals("1", captor.getAllValues().get(2).exclusiveStartKey().get("id").s()),
                () -> verify(mockDynamoDbClient, times(2)).scan(any(ScanRequest.class)));
    }

    /**
     * Pins the documented missing-item result of the typed getItem overloads: the default value of the target type, which is an
     * empty value (not null) for Optional-like wrappers and a Holder holding null, besides null for reference types and the
     * primitive default.
     */
    @Test
    public void testTypedGetItem_MissingItemYieldsTargetTypeDefaultIncludingEmptyWrappers_sliceC() {
        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().build());
        final Map<String, AttributeValue> key = Map.of("id", AttributeValue.fromS("missing"));
        final GetItemRequest request = GetItemRequest.builder().tableName("TestTable").key(key).build();

        org.junit.jupiter.api.Assertions.assertAll(() -> assertNull(executor.getItem("TestTable", key, TestEntity.class)),
                () -> assertNull(executor.getItem("TestTable", key, Integer.class)), () -> assertEquals(0, executor.getItem("TestTable", key, int.class)),
                () -> assertEquals(java.util.Optional.empty(), executor.getItem("TestTable", key, java.util.Optional.class)),
                () -> assertEquals(java.util.OptionalInt.empty(), executor.getItem("TestTable", key, true, java.util.OptionalInt.class)),
                () -> assertFalse(executor.getItem("TestTable", key, com.landawn.abacus.util.u.Optional.class).isPresent()),
                () -> assertFalse(executor.getItem(request, com.landawn.abacus.util.u.Nullable.class).isPresent()),
                () -> assertNull(executor.getItem(request, com.landawn.abacus.util.Holder.class).value()));
    }

    /**
     * Pins the documented null scanFilter of Mapper.scan(Map) and Mapper.scan(List, Map): it means no filter, so the request is
     * sent without a scan filter instead of failing.
     */
    @Test
    public void testMapperScan_NullScanFilterSendsNoFilter_sliceC() {
        when(mockDynamoDbClient.scan(any(ScanRequest.class)))
                .thenReturn(ScanResponse.builder().items(List.of(Map.of("id", AttributeValue.fromS("1")))).build());
        final DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        final List<TestEntity> byFilter = mapper.scan((Map<String, Condition>) null).toList();
        final List<TestEntity> byAttributesAndFilter = mapper.scan(List.of("id"), (Map<String, Condition>) null).toList();

        final ArgumentCaptor<ScanRequest> captor = ArgumentCaptor.forClass(ScanRequest.class);
        verify(mockDynamoDbClient, times(2)).scan(captor.capture());
        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals("1", byFilter.get(0).getId()),
                () -> assertEquals("1", byAttributesAndFilter.get(0).getId()), () -> assertFalse(captor.getAllValues().get(0).hasScanFilter()),
                () -> assertFalse(captor.getAllValues().get(1).hasScanFilter()),
                () -> assertEquals(List.of("id"), captor.getAllValues().get(1).attributesToGet()));
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

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(AttributeValue.fromS("3/4"), fraction),
                () -> assertEquals(AttributeValue.fromS("2/1"), DynamoDBExecutor.toAttributeValue(com.landawn.abacus.util.Fraction.of(2, 1))),
                () -> assertEquals(AttributeValue.fromS("1/3"), DynamoDBExecutor.asKey("id", oneThird).get("id")),
                () -> assertEquals(AttributeValue.fromS("3/4"), DynamoDBExecutor.asItem("ratio", threeQuarters).get("ratio")),
                () -> assertEquals(AttributeValue.fromS("3/4"), DynamoDBExecutor.toAttributeValueUpdate(threeQuarters).value()),
                () -> assertEquals(AttributeValue.fromS("1,234"), DynamoDBExecutor.toAttributeValue(new GroupedDigitsNumber_applyDdb())),
                () -> assertEquals(AttributeValue.fromS("1/3"), item.get("id")), () -> assertEquals(AttributeValue.fromS("3/4"), item.get("ratio")),
                () -> assertEquals(AttributeValue.fromN("5"), item.get("count")),
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
                com.landawn.abacus.util.NamingPolicy.CAMEL_CASE);
        final FractionEntity_applyDdb entity = new FractionEntity_applyDdb();
        entity.setId(com.landawn.abacus.util.Fraction.of(1, 3));
        entity.setRatio(com.landawn.abacus.util.Fraction.of(3, 4));
        final Map<String, AttributeValue> stored = new LinkedHashMap<>();
        stored.put("id", AttributeValue.fromS("1/3"));
        stored.put("ratio", AttributeValue.fromS("3/4"));
        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(stored).build());

        final FractionEntity_applyDdb read = mapper.getItem(entity);
        mapper.putItem(entity);

        final ArgumentCaptor<GetItemRequest> getCaptor = ArgumentCaptor.forClass(GetItemRequest.class);
        verify(mockDynamoDbClient).getItem(getCaptor.capture());
        final ArgumentCaptor<PutItemRequest> putCaptor = ArgumentCaptor.forClass(PutItemRequest.class);
        verify(mockDynamoDbClient).putItem(putCaptor.capture());
        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(AttributeValue.fromS("1/3"), getCaptor.getValue().key().get("id")),
                () -> assertEquals(AttributeValue.fromS("1/3"), putCaptor.getValue().item().get("id")),
                () -> assertEquals(AttributeValue.fromS("3/4"), putCaptor.getValue().item().get("ratio")),
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
        final List<org.junit.jupiter.api.function.Executable> checks = new java.util.ArrayList<>();

        for (final Object number : decimals) {
            @SuppressWarnings("unchecked")
            final String text = com.landawn.abacus.util.N.typeOf((Class<Object>) number.getClass()).stringOf(number);
            checks.add(() -> assertEquals(AttributeValue.fromN(text), DynamoDBExecutor.toAttributeValue(number), text));
            checks.add(() -> assertEquals(AttributeValue.fromN(text), DynamoDBExecutor.asKey("k", number).get("k"), text));
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
     * an empty result included. Map types (an @Entity-annotated Map subclass too) and bean classes still query; null is still
     * rejected as a null argument.
     */
    @Test
    public void testQueryWithClass_UnsupportedRowTypeRejectedBeforeAnyRequest_applyDdb() {
        final QueryRequest req = QueryRequest.builder().tableName("TestTable").build();
        final QueryResponse oneItem = QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.fromS("1")))).build();
        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(oneItem, QueryResponse.builder().items(List.of()).build());
        final List<Class<?>> unsupported = List.of(String.class, Integer.class, int.class, Object.class, Object[].class, String[].class, List.class,
                java.util.Set.class, java.util.Optional.class, com.landawn.abacus.util.Holder.class, com.landawn.abacus.util.MutableBoolean.class);
        final List<org.junit.jupiter.api.function.Executable> checks = new java.util.ArrayList<>();

        for (final Class<?> rowType : unsupported) {
            checks.add(() -> {
                final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> executor.query(req, rowType), rowType.getName());
                assertTrue(e.getMessage().startsWith("Unsupported row type: " + com.landawn.abacus.util.ClassUtil.getCanonicalClassName(rowType)),
                        e.getMessage());
            });
        }

        org.junit.jupiter.api.Assertions.assertAll(checks);
        assertThrows(IllegalArgumentException.class, () -> executor.query(req, (Class<?>) null));
        verify(mockDynamoDbClient, never()).query(any(QueryRequest.class));

        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(oneItem);
        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(1, executor.query(req, Map.class).size()),
                () -> assertEquals(1, executor.query(req, AnnotatedMapRow_applyDdb.class).size()),
                () -> assertEquals(1, executor.query(req, TestEntity.class).size()));
        verify(mockDynamoDbClient, times(3)).query(any(QueryRequest.class));
    }

    // ---- 2026-10-24 r2 sliceC ----

    /**
     * An @Entity-annotated Map subclass (Beans.isBeanClass accepts it) is written as the Map it is by toItem and toUpdateItem, in
     * both naming-policy forms, and by putItem(String, Object). It used to take the bean path, whose only properties are the Map
     * getters, so every entry was dropped and keySet/values/entrySet/size/empty were written instead. A row that
     * list(QueryRequest, Class) reads into that class (as a Map of the item's attributes) now writes back the item it came from.
     */
    @Test
    public void testToItem_EntityAnnotatedMapSubclassIsWrittenAsMap_r2sliceC() {
        final AnnotatedMapRow_applyDdb entity = new AnnotatedMapRow_applyDdb();
        entity.put("id", "a1");
        entity.put("userName", "n");
        final Map<String, AttributeValue> expected = new LinkedHashMap<>();
        expected.put("id", AttributeValue.fromS("a1"));
        expected.put("userName", AttributeValue.fromS("n"));
        final Map<String, AttributeValue> snakeExpected = new LinkedHashMap<>();
        snakeExpected.put("id", AttributeValue.fromS("a1"));
        snakeExpected.put("user_name", AttributeValue.fromS("n"));
        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().items(List.of(expected)).build());

        final AnnotatedMapRow_applyDdb readBack = executor.list(QueryRequest.builder().tableName("TestTable").build(), AnnotatedMapRow_applyDdb.class).get(0);
        executor.putItem("TestTable", (Object) entity);

        final ArgumentCaptor<PutItemRequest> putCaptor = ArgumentCaptor.forClass(PutItemRequest.class);
        verify(mockDynamoDbClient).putItem(putCaptor.capture());
        final Map<String, AttributeValueUpdate> update = DynamoDBExecutor.toUpdateItem(entity);
        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(expected, DynamoDBExecutor.toItem(entity)),
                () -> assertEquals(snakeExpected, DynamoDBExecutor.toItem(entity, com.landawn.abacus.util.NamingPolicy.SNAKE_CASE)),
                () -> assertEquals(List.of("id", "userName"), new java.util.ArrayList<>(update.keySet())),
                () -> assertEquals(AttributeValue.fromS("n"), update.get("userName").value()),
                () -> assertEquals(List.of("id", "user_name"),
                        new java.util.ArrayList<>(DynamoDBExecutor.toUpdateItem(entity, com.landawn.abacus.util.NamingPolicy.SNAKE_CASE).keySet())),
                () -> assertEquals(expected, putCaptor.getValue().item()), () -> assertEquals(expected, DynamoDBExecutor.toItem(readBack)));
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

    // ---- 2026-10-24 r2verify1 ----

    /**
     * Beans.isBeanClass accepts an @Entity-annotated Map subclass, so both public toEntity overloads mapped an item to it as a
     * bean whose only properties are its Map accessors (and its own fields): every attribute was silently dropped and an empty
     * map returned (or UnsupportedOperationException for a class without fields). They now reject every Map type with the
     * IllegalArgumentException a plain Map already got; a missing item still returns null, and the row readers still fill such a
     * class with the item's attributes.
     */
    @Test
    public void testToEntityRejectsEntityAnnotatedMapSubclass_r2verify1() {
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("id", AttributeValue.fromS("1"));
        item.put("name", AttributeValue.fromS("x"));
        final Map<String, Object> expectedRow = new LinkedHashMap<>();
        expectedRow.put("id", "1");
        expectedRow.put("name", "x");
        final String reason = " - Map implementations are never treated as beans";
        final String withIdMessage = "Not a bean class: " + EntityMapWithId_r2verify1.class.getCanonicalName() + reason;

        final Object row = DynamoDBExecutor.toList(QueryResponse.builder().items(List.of(item)).build(), EntityMapWithId_r2verify1.class).get(0);

        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertEquals(withIdMessage,
                        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toEntity(item, EntityMapWithId_r2verify1.class)).getMessage()),
                () -> assertEquals(withIdMessage,
                        assertThrows(IllegalArgumentException.class,
                                () -> DynamoDBExecutor.toEntity(GetItemResponse.builder().item(item).build(), EntityMapWithId_r2verify1.class)).getMessage()),
                () -> assertEquals("Not a bean class: " + AnnotatedMapRow_applyDdb.class.getCanonicalName() + reason,
                        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toEntity(item, AnnotatedMapRow_applyDdb.class)).getMessage()),
                // Unchanged: a plain Map type fails alike, and a missing item is null.
                () -> assertEquals("Not a bean class: java.util.LinkedHashMap" + reason,
                        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toEntity(item, LinkedHashMap.class)).getMessage()),
                () -> assertNull(DynamoDBExecutor.toEntity((Map<String, AttributeValue>) null, EntityMapWithId_r2verify1.class)),
                () -> assertNull(DynamoDBExecutor.toEntity(GetItemResponse.builder().build(), EntityMapWithId_r2verify1.class)),
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

    // ---- 2026-10-27 sliceC ----

    /**
     * Pins three documented contracts (doc-only changes, GREEN before and after): Mapper.batchGetItem silently omits the items of
     * keys DynamoDB returns as unprocessed; query(QueryRequest, beanClass) and Mapper.query return a Dataset without any columns
     * when nothing matches; and toAttributeValueUpdate never builds a set value, so a Java Set for ADD/DELETE becomes S JSON text.
     */
    @Test
    public void testMapperBatchGetUnprocessedKeysEmptyBeanDatasetAndSetUpdates_sliceC() {
        final Map<String, AttributeValue> found = new LinkedHashMap<>();
        found.put("id", AttributeValue.fromS("2"));
        final Map<String, AttributeValue> unprocessed = new LinkedHashMap<>();
        unprocessed.put("id", AttributeValue.fromS("1"));
        when(mockDynamoDbClient.batchGetItem(any(BatchGetItemRequest.class))).thenReturn(BatchGetItemResponse.builder()
                .responses(Map.of("TestTable", List.of(found)))
                .unprocessedKeys(Map.of("TestTable", KeysAndAttributes.builder().keys(List.of(unprocessed)).build()))
                .build());
        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().build());

        final TestEntity first = new TestEntity();
        first.setId("1");
        final TestEntity second = new TestEntity();
        second.setId("2");
        final List<TestEntity> fetched = executor.mapper(TestEntity.class).batchGetItem(List.of(first, second));
        final Dataset empty = executor.query(QueryRequest.builder().tableName("TestTable").build(), TestEntity.class);
        final Dataset emptyFromMapper = executor.mapper(TestEntity.class).query(QueryRequest.builder().build());
        final AttributeValueUpdate add = DynamoDBExecutor.toAttributeValueUpdate(new java.util.LinkedHashSet<>(List.of("a", "b")), AttributeAction.ADD);
        final AttributeValueUpdate delete = DynamoDBExecutor.toAttributeValueUpdate(new java.util.LinkedHashSet<>(List.of("a")), AttributeAction.DELETE);

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(1, fetched.size()), () -> assertEquals("2", fetched.get(0).getId()),
                () -> assertEquals(0, empty.size()), () -> assertTrue(empty.columnNames().isEmpty(), String.valueOf(empty.columnNames())),
                () -> assertThrows(IllegalArgumentException.class, () -> empty.getColumn("name")),
                () -> assertTrue(emptyFromMapper.columnNames().isEmpty(), String.valueOf(emptyFromMapper.columnNames())),
                () -> assertEquals(AttributeValue.fromS("[\"a\", \"b\"]"), add.value()), () -> assertFalse(add.value().hasSs()),
                () -> assertEquals(AttributeValue.fromS("[\"a\"]"), delete.value()));
    }

    /** async(null) used to report the async executor constructor's parameter ("dynamoDBClient"), which this method does not have. */
    @Test
    public void testAsyncBridgeNullClientMessageNamesItsOwnParameter_sliceC() {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> executor.async(null));

        assertTrue(e.getMessage().contains("dynamoDBAsyncClient"), e.getMessage());
    }

    /**
     * An immutable row class (abacus ImmutableMap/ImmutableList/ImmutableSet/ImmutableSortedMap) used to get the mutable stand-in
     * that N.newCollection/IntFunctions.ofMap create for it (HashMap, ArrayList, HashSet, TreeMap), so a List of that row type held
     * rows of another class. Every reader (toList, list, getItem) now returns rows of the requested class with unchanged values;
     * a mutable row class still gets its own instance.
     */
    @Test
    public void testImmutableRowClassesGetRowsOfThatClass_sliceC() {
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("a", AttributeValue.fromS("1"));
        item.put("b", AttributeValue.fromN("2"));
        final QueryResponse response = QueryResponse.builder().items(List.of(item)).build();
        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(response);
        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(item).build());
        final QueryRequest request = QueryRequest.builder().tableName("TestTable").build();
        final Map<String, AttributeValue> key = DynamoDBExecutor.asKey("a", "1");

        final Object mapRow = DynamoDBExecutor.toList(response, com.landawn.abacus.util.ImmutableMap.class).get(0);
        final Object listRow = executor.list(request, com.landawn.abacus.util.ImmutableList.class).get(0);
        final Object setRow = DynamoDBExecutor.toList(response, com.landawn.abacus.util.ImmutableSet.class).get(0);
        final Object sortedRow = executor.list(request, com.landawn.abacus.util.ImmutableSortedMap.class).get(0);
        final Object fetched = executor.getItem("TestTable", key, com.landawn.abacus.util.ImmutableMap.class);
        final Object fetchedList = executor.getItem("TestTable", key, com.landawn.abacus.util.ImmutableList.class);
        final Object mutableRow = DynamoDBExecutor.toList(response, LinkedHashMap.class).get(0);

        org.junit.jupiter.api.Assertions.assertAll(() -> assertTrue(mapRow instanceof com.landawn.abacus.util.ImmutableMap, mapRow.getClass().getName()),
                () -> assertEquals(Map.of("a", "1", "b", "2"), mapRow),
                () -> assertTrue(listRow instanceof com.landawn.abacus.util.ImmutableList, listRow.getClass().getName()),
                () -> assertEquals(List.of("1", "2"), listRow),
                () -> assertTrue(setRow instanceof com.landawn.abacus.util.ImmutableSet, setRow.getClass().getName()),
                () -> assertEquals(java.util.Set.of("1", "2"), setRow),
                () -> assertTrue(sortedRow instanceof com.landawn.abacus.util.ImmutableSortedMap, sortedRow.getClass().getName()),
                () -> assertEquals(Map.of("a", "1", "b", "2"), sortedRow),
                () -> assertTrue(fetched instanceof com.landawn.abacus.util.ImmutableMap, fetched.getClass().getName()),
                () -> assertTrue(fetchedList instanceof com.landawn.abacus.util.ImmutableList, fetchedList.getClass().getName()),
                () -> assertEquals(LinkedHashMap.class, mutableRow.getClass()));
    }

    // ---- 2026-10-27 r2D ----

    /**
     * The immutable-row fix on the readers the sliceC test does not reach: stream and scan, batchGetItem, toList over a scan
     * response, readRow's Collection branch with a request, and the sorted and navigable immutable kinds. A creatable row class
     * keeps the factory's instance. EnumMap and AbstractQueue row classes, which the stand-in cannot be converted to, now fail with
     * IllegalArgumentException (HEAD returned HashMap/LinkedList rows typed as them); ImmutableBiMap still fails as before. A null
     * element of a hand-built item list stays a null Map row, as in the v1 executor, whose toRowClass has the same null guard.
     */
    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testEveryRowReaderReturnsTheRequestedRowClass_r2D() {
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("id", AttributeValue.fromS("a"));
        item.put("n", AttributeValue.fromN("2"));
        final Map<String, Object> expectedMap = new HashMap<>();
        expectedMap.put("id", "a");
        expectedMap.put("n", "2");
        final Map<String, KeysAndAttributes> keys = new LinkedHashMap<>();
        keys.put("TestTable", KeysAndAttributes.builder().keys(List.of(DynamoDBExecutor.asKey("id", "a"))).build());
        final QueryResponse queryResponse = QueryResponse.builder().items(List.of(item)).build();
        final GetItemRequest getItemRequest = GetItemRequest.builder().tableName("TestTable").key(DynamoDBExecutor.asKey("id", "a")).build();
        final List<Map<String, AttributeValue>> itemsWithNull = new java.util.ArrayList<>();
        itemsWithNull.add(null);
        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(queryResponse);
        when(mockDynamoDbClient.scan(any(ScanRequest.class))).thenReturn(ScanResponse.builder().items(List.of(item)).build());
        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(item).build());
        when(mockDynamoDbClient.batchGetItem(any(BatchGetItemRequest.class)))
                .thenReturn(BatchGetItemResponse.builder().responses(Map.of("TestTable", List.of(item))).build());

        org.junit.jupiter.api.Assertions.assertAll(() -> {
            final Object row = executor.stream(QueryRequest.builder().tableName("TestTable").build(), com.landawn.abacus.util.ImmutableMap.class)
                    .first()
                    .orElseNull();
            assertTrue(row instanceof com.landawn.abacus.util.ImmutableMap, row.getClass().getName());
            assertEquals(expectedMap, row);
        }, () -> {
            final Object row = executor.scan(ScanRequest.builder().tableName("TestTable").build(), com.landawn.abacus.util.ImmutableSet.class)
                    .first()
                    .orElseNull();
            assertTrue(row instanceof com.landawn.abacus.util.ImmutableSet, row.getClass().getName());
            assertEquals(java.util.Set.of("a", "2"), row);
        }, () -> {
            final Object row = executor.batchGetItem(keys, com.landawn.abacus.util.ImmutableList.class).get("TestTable").get(0);
            assertTrue(row instanceof com.landawn.abacus.util.ImmutableList, row.getClass().getName());
            assertEquals(List.of("a", "2"), row);
        }, () -> {
            final Object row = DynamoDBExecutor.toList(ScanResponse.builder().items(List.of(item)).build(), com.landawn.abacus.util.ImmutableNavigableMap.class)
                    .get(0);
            assertTrue(row instanceof com.landawn.abacus.util.ImmutableNavigableMap, row.getClass().getName());
            assertEquals(List.of("id", "n"), new java.util.ArrayList<>(((Map<?, ?>) row).keySet()));
        }, () -> {
            final Object row = executor.getItem(getItemRequest, com.landawn.abacus.util.ImmutableSortedSet.class);
            assertTrue(row instanceof com.landawn.abacus.util.ImmutableSortedSet, row.getClass().getName());
            assertEquals(List.of("2", "a"), new java.util.ArrayList<>((java.util.Collection<?>) row));
        }, () -> {
            final Object row = DynamoDBExecutor.toList(queryResponse, 0, 1, com.landawn.abacus.util.ImmutableNavigableSet.class).get(0);
            assertTrue(row instanceof com.landawn.abacus.util.ImmutableNavigableSet, row.getClass().getName());
            assertEquals(List.of("2", "a"), new java.util.ArrayList<>((java.util.Collection<?>) row));
        }, () -> assertEquals(java.util.TreeMap.class, executor.getItem(getItemRequest, java.util.TreeMap.class).getClass()),
                () -> assertEquals(java.util.LinkedList.class, DynamoDBExecutor.toList(queryResponse, java.util.LinkedList.class).get(0).getClass()),
                () -> assertEquals(java.util.TreeSet.class,
                        executor.list(QueryRequest.builder().tableName("TestTable").build(), java.util.TreeSet.class).get(0).getClass()),
                () -> assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toList(queryResponse, java.util.EnumMap.class)),
                () -> assertThrows(IllegalArgumentException.class, () -> executor.getItem(getItemRequest, java.util.AbstractQueue.class)),
                () -> assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toList(queryResponse, com.landawn.abacus.util.ImmutableBiMap.class)),
                () -> assertNull(DynamoDBExecutor.toList(itemsWithNull, 0, 1, com.landawn.abacus.util.ImmutableMap.class).get(0)),
                () -> assertNull(DynamoDBExecutor.toList(itemsWithNull, 0, 1, LinkedHashMap.class).get(0)));
    }
}
