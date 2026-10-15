package com.landawn.abacus.da.aws.dynamodb.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.Filters;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.stream.Stream;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
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

public class DynamoDBMapperTest extends DynamoDBExecutorTestSupport {

    // Construction and configuration

    /**
     * Regression: same as {@link #testMapperGetItemAppliesNamingPolicyToKey()} but for the
     * batch-delete path which goes through createBatchDeleteRequest -> createKey.
     */
    @Test
    public void testMapperBatchDeleteItemAppliesNamingPolicyToKey() {
        DynamoDBExecutor.Mapper<TestEntityWithUserId> mapper = executor.mapper(TestEntityWithUserId.class, "TestTable", NamingPolicy.SNAKE_CASE);

        TestEntityWithUserId entity = new TestEntityWithUserId();
        entity.setUserId("u-42");

        when(mockDynamoDbClient.batchWriteItem(any(BatchWriteItemRequest.class))).thenReturn(BatchWriteItemResponse.builder().build());

        org.mockito.ArgumentCaptor<BatchWriteItemRequest> captor = org.mockito.ArgumentCaptor.forClass(BatchWriteItemRequest.class);

        mapper.batchDeleteItem(List.of(entity));

        verify(mockDynamoDbClient).batchWriteItem(captor.capture());

        Map<String, AttributeValue> key = captor.getValue().requestItems().get("TestTable").get(0).deleteRequest().key();
        assertTrue(key.containsKey("user_id"), "delete key should contain converted attribute 'user_id', actual keys: " + key.keySet());
        assertEquals("u-42", key.get("user_id").s());
    }

    @Test
    public void testMapperBatchPutItemAppliesNamingPolicy() {
        DynamoDBExecutor.Mapper<NamingPolicyEntity> mapper = executor.mapper(NamingPolicyEntity.class, "TestTable", NamingPolicy.SNAKE_CASE);

        NamingPolicyEntity entity = new NamingPolicyEntity();
        entity.setId("123");
        entity.setUserName("Alice");

        when(mockDynamoDbClient.batchWriteItem(any(BatchWriteItemRequest.class))).thenReturn(BatchWriteItemResponse.builder().build());

        org.mockito.ArgumentCaptor<BatchWriteItemRequest> captor = org.mockito.ArgumentCaptor.forClass(BatchWriteItemRequest.class);

        mapper.batchPutItem(List.of(entity));

        verify(mockDynamoDbClient).batchWriteItem(captor.capture());

        Map<String, AttributeValue> item = captor.getValue().requestItems().get("TestTable").get(0).putRequest().item();

        // With SNAKE_CASE the "userName" property must be written as "user_name", not the default camelCase.
        assertTrue(item.containsKey("user_name"));
        assertEquals("Alice", item.get("user_name").s());
    }

    // The Mapper-constructor message on its own: testMapper_ErrorMessagesNameClassOnce fails at its first (@Table) assertion on
    // HEAD, so it never proved this second site, which rendered "class java.lang.Integer is not an entity class ...".
    @Test
    public void testMapperConstructor_NonBeanMessageNamesClassOnce_coverageDC() {
        final IllegalArgumentException notBean = assertThrows(IllegalArgumentException.class, () -> executor.mapper(Integer.class, "t", null));
        assertEquals("java.lang.Integer is not an entity class with getter/setter method", notBean.getMessage());
    }

    /**
     * Regression: when the Mapper is configured with a non-CAMEL_CASE NamingPolicy and the @Id field
     * has no explicit @Column annotation, the key built by getItem/deleteItem/updateItem/batchGetItem
     * must use the policy-converted attribute name (e.g. "userId" -> "user_id") so it matches what
     * putItem writes via toItem(entity, namingPolicy). Previously the key was built from the raw
     * Java property name, causing every key-based operation to lookup the wrong attribute.
     */
    @Test
    public void testMapperGetItemAppliesNamingPolicyToKey() {
        DynamoDBExecutor.Mapper<TestEntityWithUserId> mapper = executor.mapper(TestEntityWithUserId.class, "TestTable", NamingPolicy.SNAKE_CASE);

        TestEntityWithUserId entity = new TestEntityWithUserId();
        entity.setUserId("u-1");

        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().build());

        org.mockito.ArgumentCaptor<GetItemRequest> captor = org.mockito.ArgumentCaptor.forClass(GetItemRequest.class);

        mapper.getItem(entity);

        verify(mockDynamoDbClient).getItem(captor.capture());

        Map<String, AttributeValue> key = captor.getValue().key();
        // With SNAKE_CASE the "userId" id must be mapped to "user_id" key, mirroring what putItem writes.
        assertTrue(key.containsKey("user_id"), "key should contain converted attribute 'user_id', actual keys: " + key.keySet());
        assertEquals("u-1", key.get("user_id").s());
    }

    // Factories and insert operations

    @Test
    public void testMapperBatchPutItem() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        List<TestEntity> entities = new ArrayList<>();
        TestEntity entity = new TestEntity();
        entity.setId("123");
        entities.add(entity);

        BatchWriteItemResponse response = BatchWriteItemResponse.builder().build();

        when(mockDynamoDbClient.batchWriteItem(any(BatchWriteItemRequest.class))).thenReturn(response);

        BatchWriteItemResponse result = mapper.batchPutItem(entities);

        assertNotNull(result);
    }

    @Test
    public void testMapperPutItem() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        TestEntity entity = new TestEntity();
        entity.setId("123");
        entity.setName("Test");

        PutItemResponse response = PutItemResponse.builder().build();

        when(mockDynamoDbClient.putItem(any(PutItemRequest.class))).thenReturn(response);

        PutItemResponse result = mapper.putItem(entity);

        assertNotNull(result);
    }

    // Mapper request-based overload tests for additional coverage.
    @Test
    public void testMapperPutItemWithRequest() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        PutItemRequest request = PutItemRequest.builder().tableName("TestTable").item(Map.of("id", AttributeValue.builder().s("123").build())).build();

        when(mockDynamoDbClient.putItem(any(PutItemRequest.class))).thenReturn(PutItemResponse.builder().build());

        PutItemResponse result = mapper.putItem(request);
        assertNotNull(result);
    }

    @Test
    public void testMapperPutItemWithRequest_NoTableName() {
        // When PutItemRequest omits the table name, the mapper should inject its own.
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        PutItemRequest request = PutItemRequest.builder().item(Map.of("id", AttributeValue.builder().s("123").build())).build();

        when(mockDynamoDbClient.putItem(any(PutItemRequest.class))).thenReturn(PutItemResponse.builder().build());

        PutItemResponse result = mapper.putItem(request);
        assertNotNull(result);
    }

    @Test
    public void testMapperPutItemWithRequest_WrongTableName() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        PutItemRequest request = PutItemRequest.builder().tableName("WrongTable").item(Map.of("id", AttributeValue.builder().s("1").build())).build();
        assertThrows(IllegalArgumentException.class, () -> mapper.putItem(request));
    }

    @Test
    public void testMapperPutItemWithReturnValues() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        TestEntity entity = new TestEntity();
        entity.setId("123");

        PutItemResponse response = PutItemResponse.builder().build();

        when(mockDynamoDbClient.putItem(any(PutItemRequest.class))).thenReturn(response);

        PutItemResponse result = mapper.putItem(entity, "ALL_OLD");

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

        BatchWriteItemResponse response = BatchWriteItemResponse.builder().build();

        when(mockDynamoDbClient.batchWriteItem(any(BatchWriteItemRequest.class))).thenReturn(response);

        BatchWriteItemResponse result = mapper.batchDeleteItem(entities);

        assertNotNull(result);
    }

    @Test
    public void testMapperDeleteItem() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        TestEntity entity = new TestEntity();
        entity.setId("123");

        DeleteItemResponse response = DeleteItemResponse.builder().build();

        when(mockDynamoDbClient.deleteItem(any(DeleteItemRequest.class))).thenReturn(response);

        DeleteItemResponse result = mapper.deleteItem(entity);

        assertNotNull(result);
    }

    @Test
    public void testMapperDeleteItemWithKey() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        Map<String, AttributeValue> key = Map.of("id", AttributeValue.builder().s("123").build());

        DeleteItemResponse response = DeleteItemResponse.builder().build();

        when(mockDynamoDbClient.deleteItem(any(DeleteItemRequest.class))).thenReturn(response);

        DeleteItemResponse result = mapper.deleteItem(key);

        assertNotNull(result);
    }

    @Test
    public void testMapperDeleteItemWithRequest() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        DeleteItemRequest request = DeleteItemRequest.builder().tableName("TestTable").key(Map.of("id", AttributeValue.builder().s("123").build())).build();

        when(mockDynamoDbClient.deleteItem(any(DeleteItemRequest.class))).thenReturn(DeleteItemResponse.builder().build());

        DeleteItemResponse result = mapper.deleteItem(request);
        assertNotNull(result);
    }

    @Test
    public void testMapperDeleteItemWithRequest_WrongTableName() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        DeleteItemRequest request = DeleteItemRequest.builder().tableName("WrongTable").key(Map.of("id", AttributeValue.builder().s("1").build())).build();
        assertThrows(IllegalArgumentException.class, () -> mapper.deleteItem(request));
    }

    @Test
    public void testMapperDeleteItemWithReturnValues() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        TestEntity entity = new TestEntity();
        entity.setId("123");

        DeleteItemResponse response = DeleteItemResponse.builder().build();

        when(mockDynamoDbClient.deleteItem(any(DeleteItemRequest.class))).thenReturn(response);

        DeleteItemResponse result = mapper.deleteItem(entity, "ALL_OLD");

        assertNotNull(result);
    }

    @Test
    public void testMapperUpdateItem() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        TestEntity entity = new TestEntity();
        entity.setId("123");
        entity.setName("Updated");

        UpdateItemResponse response = UpdateItemResponse.builder().build();

        when(mockDynamoDbClient.updateItem(any(UpdateItemRequest.class))).thenReturn(response);

        UpdateItemResponse result = mapper.updateItem(entity);

        assertNotNull(result);

        ArgumentCaptor<UpdateItemRequest> requestCaptor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(mockDynamoDbClient).updateItem(requestCaptor.capture());

        UpdateItemRequest request = requestCaptor.getValue();
        assertTrue(request.key().containsKey("id"));
        assertTrue(!request.attributeUpdates().containsKey("id"));
        assertTrue(request.attributeUpdates().containsKey("name"));
    }

    @Test
    public void testMapperUpdateItemWithRequest() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        UpdateItemRequest request = UpdateItemRequest.builder().tableName("TestTable").key(Map.of("id", AttributeValue.builder().s("123").build())).build();

        when(mockDynamoDbClient.updateItem(any(UpdateItemRequest.class))).thenReturn(UpdateItemResponse.builder().build());

        UpdateItemResponse result = mapper.updateItem(request);
        assertNotNull(result);
    }

    @Test
    public void testMapperUpdateItemWithRequest_WrongTableName() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        UpdateItemRequest request = UpdateItemRequest.builder().tableName("WrongTable").key(Map.of("id", AttributeValue.builder().s("1").build())).build();
        assertThrows(IllegalArgumentException.class, () -> mapper.updateItem(request));
    }

    @Test
    public void testMapperUpdateItemWithReturnValues() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        TestEntity entity = new TestEntity();
        entity.setId("123");
        entity.setName("Updated");

        UpdateItemResponse response = UpdateItemResponse.builder().build();

        when(mockDynamoDbClient.updateItem(any(UpdateItemRequest.class))).thenReturn(response);

        UpdateItemResponse result = mapper.updateItem(entity, "ALL_NEW");

        assertNotNull(result);

        ArgumentCaptor<UpdateItemRequest> requestCaptor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(mockDynamoDbClient).updateItem(requestCaptor.capture());

        UpdateItemRequest request = requestCaptor.getValue();
        assertTrue(request.key().containsKey("id"));
        assertTrue(!request.attributeUpdates().containsKey("id"));
        assertTrue(request.attributeUpdates().containsKey("name"));
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

        BatchGetItemResponse response = BatchGetItemResponse.builder()
                .responses(Map.of("TestTable",
                        List.of(Map.of("id", AttributeValue.builder().s("1").build()), Map.of("id", AttributeValue.builder().s("2").build()))))
                .build();

        when(mockDynamoDbClient.batchGetItem(any(BatchGetItemRequest.class))).thenReturn(response);

        List<TestEntity> result = mapper.batchGetItem(entities);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("1", result.get(0).getId());
        assertEquals("2", result.get(1).getId());
    }

    @Test
    public void testMapperBatchGetItem_EmptyResponse() {
        // When the underlying batch response is empty the mapper returns an empty list, not null.
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        TestEntity entity = new TestEntity();
        entity.setId("1");

        when(mockDynamoDbClient.batchGetItem(any(BatchGetItemRequest.class))).thenReturn(BatchGetItemResponse.builder().responses(new HashMap<>()).build());

        List<TestEntity> result = mapper.batchGetItem(List.of(entity));
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @Test
    public void testMapperBatchGetItemWithRequest() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        BatchGetItemRequest request = BatchGetItemRequest.builder()
                .requestItems(Map.of("TestTable", KeysAndAttributes.builder().keys(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build()))
                .build();

        when(mockDynamoDbClient.batchGetItem(any(BatchGetItemRequest.class))).thenReturn(
                BatchGetItemResponse.builder().responses(Map.of("TestTable", List.of(Map.of("id", AttributeValue.builder().s("1").build())))).build());

        List<TestEntity> result = mapper.batchGetItem(request);
        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testMapperBatchGetItemWithReturnConsumedCapacity() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        List<TestEntity> entities = List.of(new TestEntity());
        entities.get(0).setId("123");

        BatchGetItemResponse response = BatchGetItemResponse.builder()
                .responses(Map.of("TestTable", List.of(Map.of("id", AttributeValue.builder().s("123").build()))))
                .build();

        when(mockDynamoDbClient.batchGetItem(any(BatchGetItemRequest.class))).thenReturn(response);

        List<TestEntity> result = mapper.batchGetItem(entities, "TOTAL");

        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testMapperGetItem() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        TestEntity entity = new TestEntity();
        entity.setId("123");

        GetItemResponse response = GetItemResponse.builder()
                .item(Map.of("id", AttributeValue.builder().s("123").build(), "name", AttributeValue.builder().s("Test").build()))
                .build();

        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(response);

        TestEntity result = mapper.getItem(entity);

        assertNotNull(result);
        assertEquals("123", result.getId());
        assertEquals("Test", result.getName());
    }

    @Test
    public void testMapperGetItemWithConsistentRead() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        TestEntity entity = new TestEntity();
        entity.setId("123");

        GetItemResponse response = GetItemResponse.builder().item(Map.of("id", AttributeValue.builder().s("123").build())).build();

        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(response);

        TestEntity result = mapper.getItem(entity, true);

        assertNotNull(result);
        assertEquals("123", result.getId());
    }

    @Test
    public void testMapperGetItemWithKey() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        Map<String, AttributeValue> key = Map.of("id", AttributeValue.builder().s("123").build());

        GetItemResponse response = GetItemResponse.builder().item(Map.of("id", AttributeValue.builder().s("123").build())).build();

        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(response);

        TestEntity result = mapper.getItem(key);

        assertNotNull(result);
        assertEquals("123", result.getId());
    }

    @Test
    public void testMapperGetItemWithRequest() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        GetItemRequest request = GetItemRequest.builder().tableName("TestTable").key(Map.of("id", AttributeValue.builder().s("123").build())).build();

        GetItemResponse response = GetItemResponse.builder().item(Map.of("id", AttributeValue.builder().s("123").build())).build();

        when(mockDynamoDbClient.getItem(request)).thenReturn(response);

        TestEntity result = mapper.getItem(request);

        assertNotNull(result);
        assertEquals("123", result.getId());
    }

    @Test
    public void testMapperGetItemWithRequest_NoTableName() {
        // Mapper should auto-inject table name when request omits it.
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        GetItemRequest request = GetItemRequest.builder().key(Map.of("id", AttributeValue.builder().s("1").build())).build();

        when(mockDynamoDbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().item(Map.of("id", AttributeValue.builder().s("1").build())).build());

        TestEntity result = mapper.getItem(request);
        assertNotNull(result);
        assertEquals("1", result.getId());
    }

    @Test
    public void testMapperList() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();

        QueryResponse response = QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("123").build()))).build();

        when(mockDynamoDbClient.query(queryRequest)).thenReturn(response);

        List<TestEntity> result = mapper.list(queryRequest);

        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testMapperListWithWrongTableName() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        QueryRequest queryRequest = QueryRequest.builder().tableName("WrongTable").build();
        assertThrows(IllegalArgumentException.class, () -> mapper.list(queryRequest));
    }

    @Test
    public void testMapperQuery() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();

        QueryResponse response = QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("123").build()))).build();

        when(mockDynamoDbClient.query(queryRequest)).thenReturn(response);

        Dataset result = mapper.query(queryRequest);

        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testMapperScanWithAllParameters() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        List<String> attributesToGet = List.of("id");
        Map<String, Condition> scanFilter = Filters.eq("status", "active");

        ScanResponse response = ScanResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("123").build()))).build();

        when(mockDynamoDbClient.scan(any(ScanRequest.class))).thenReturn(response);

        Stream<TestEntity> stream = mapper.scan(attributesToGet, scanFilter);

        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testMapperScanWithAttributesToGet() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        List<String> attributesToGet = List.of("id");

        ScanResponse response = ScanResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("123").build()))).build();

        when(mockDynamoDbClient.scan(any(ScanRequest.class))).thenReturn(response);

        Stream<TestEntity> stream = mapper.scan(attributesToGet);

        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testMapperScanWithRequest() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        ScanRequest scanRequest = ScanRequest.builder().tableName("TestTable").build();

        ScanResponse response = ScanResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("123").build()))).build();

        when(mockDynamoDbClient.scan(any(ScanRequest.class))).thenReturn(response);

        Stream<TestEntity> stream = mapper.scan(scanRequest);

        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testMapperScanWithScanFilter() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        Map<String, Condition> scanFilter = Filters.eq("status", "active");

        ScanResponse response = ScanResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("123").build()))).build();

        when(mockDynamoDbClient.scan(any(ScanRequest.class))).thenReturn(response);

        Stream<TestEntity> stream = mapper.scan(scanFilter);

        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testMapperScanWithWrongTableName() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        ScanRequest scanRequest = ScanRequest.builder().tableName("WrongTable").build();
        assertThrows(IllegalArgumentException.class, () -> mapper.scan(scanRequest));
    }

    @Test
    public void testMapperStream() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();

        QueryResponse response = QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("123").build()))).build();

        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(response);

        Stream<TestEntity> stream = mapper.stream(queryRequest);

        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    // Resource lifecycle

    /**
     * The v2 mapper promises argument validation just like the v1 mapper. These entry points used
     * to dereference null inside private request builders and leak implementation-detail NPEs.
     */
    @Test
    public void testMapperNullArgumentsThrowIllegalArgumentException() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

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
    public void testMapperRejectsNonFinitePropertyValuesBeforeSendingWrites() {
        final DynamoDBExecutor.Mapper<NonFiniteValueEntity> mapper = executor.mapper(NonFiniteValueEntity.class, "TestTable", NamingPolicy.CAMEL_CASE);
        final NonFiniteValueEntity entity = new NonFiniteValueEntity();
        entity.setId("1");
        for (final double value : new double[] { Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY }) {
            entity.setValue(value);
            assertThrows(IllegalArgumentException.class, () -> mapper.putItem(entity));
            assertThrows(IllegalArgumentException.class, () -> mapper.putItem(entity, "ALL_OLD"));
            assertThrows(IllegalArgumentException.class, () -> mapper.batchPutItem(List.of(entity)));
            assertThrows(IllegalArgumentException.class, () -> mapper.updateItem(entity));
            assertThrows(IllegalArgumentException.class, () -> mapper.updateItem(entity, "ALL_NEW"));
        }
        org.mockito.Mockito.verifyNoInteractions(mockDynamoDbClient);
    }

    // The mapper() messages concatenated the Class object itself, rendering "Entity class class com...$V2NoTableEntity" and
    // "class java.lang.Integer is not an entity class"; they now name the class by its canonical name (v1 twin: slice A).
    @Test
    public void testMapper_ErrorMessagesNameClassOnce() {
        final IllegalArgumentException noTable = assertThrows(IllegalArgumentException.class, () -> executor.mapper(V2NoTableEntity.class));
        assertTrue(noTable.getMessage().startsWith("Entity class " + com.landawn.abacus.util.ClassUtil.getCanonicalClassName(V2NoTableEntity.class) + " must"),
                noTable.getMessage());
        assertFalse(noTable.getMessage().contains("class class"), noTable.getMessage());

        final IllegalArgumentException notBean = assertThrows(IllegalArgumentException.class, () -> executor.mapper(Integer.class, "t", null));
        assertTrue(notBean.getMessage().startsWith("java.lang.Integer is not an entity class"), notBean.getMessage());
    }

    @Test
    public void testMapper_NoTableAnnotationThrows() {
        assertThrows(IllegalArgumentException.class, () -> executor.mapper(V2NoTableEntity.class));
    }

    @Test
    public void testMapperBatchWriteItemWithRequest() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        BatchWriteItemRequest request = BatchWriteItemRequest.builder()
                .requestItems(Map.of("TestTable", List.of(
                        WriteRequest.builder().putRequest(PutRequest.builder().item(Map.of("id", AttributeValue.builder().s("1").build())).build()).build())))
                .build();

        when(mockDynamoDbClient.batchWriteItem(any(BatchWriteItemRequest.class))).thenReturn(BatchWriteItemResponse.builder().build());

        BatchWriteItemResponse result = mapper.batchWriteItem(request);
        assertNotNull(result);
    }

    @Test
    public void testMapperBatchWriteItemWithRequest_WrongTableName() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        BatchWriteItemRequest request = BatchWriteItemRequest.builder()
                .requestItems(Map.of("WrongTable", List.of(
                        WriteRequest.builder().putRequest(PutRequest.builder().item(Map.of("id", AttributeValue.builder().s("1").build())).build()).build())))
                .build();

        assertThrows(IllegalArgumentException.class, () -> mapper.batchWriteItem(request));
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
        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().build());

        mapper.getItem(entity);

        final ArgumentCaptor<GetItemRequest> requestCaptor = ArgumentCaptor.forClass(GetItemRequest.class);
        verify(mockDynamoDbClient).getItem(requestCaptor.capture());
        assertEquals("tenant-1", requestCaptor.getValue().key().get("partitionId").s());
        assertEquals("item-9", requestCaptor.getValue().key().get("sortId").s());
        assertThrows(IllegalArgumentException.class, () -> executor.mapper(ThreeKeyEntity.class));
        assertThrows(IllegalArgumentException.class, () -> executor.mapper(CompositeKeyNameCollisionEntity.class, "TestTable", NamingPolicy.SNAKE_CASE));
    }

    @Test
    public void testMapperWithInvalidEntity() {
        assertThrows(IllegalArgumentException.class, () -> {
            executor.mapper(InvalidEntity.class, "TestTable", NamingPolicy.CAMEL_CASE);
        });
    }

    @Test
    public void testMapperWithWrongTableName() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        GetItemRequest request = GetItemRequest.builder().tableName("WrongTable").key(Map.of("id", AttributeValue.builder().s("123").build())).build();

        assertThrows(IllegalArgumentException.class, () -> {
            mapper.getItem(request);
        });
    }

    public static class NonFiniteValueEntity {
        private String id;
        private double value;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public double getValue() {
            return value;
        }

        public void setValue(final double value) {
            this.value = value;
        }
    }

    // Entity with no @Table to test mapper failure path
    public static class V2NoTableEntity {
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

    private static class TestEntityWithUserId {
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

    private static class InvalidEntity {
        // No @Id annotation and no field named "id" so no implicit id is detected
        private String firstName;
        private String lastName;

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public void setLastName(String lastName) {
            this.lastName = lastName;
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
