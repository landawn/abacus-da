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
}
