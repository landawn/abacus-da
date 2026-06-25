package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.da.aws.AnyUtil;
import com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor;
import com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.ConditionBuilder;
import com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.Filters;
import com.landawn.abacus.util.Clazz;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.stream.Stream;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
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

public class DynamoDBExecutorV2Test extends TestBase {

    @Mock
    private DynamoDbClient mockDynamoDbClient;

    private DynamoDBExecutor executor;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        executor = new DynamoDBExecutor(mockDynamoDbClient);
    }

    @Test
    public void testDynamoDBClient() {
        DynamoDbClient client = executor.dynamoDBClient();
        assertNotNull(client);
        assertEquals(mockDynamoDbClient, client);
    }

    @Test
    public void testAttrValueOfNull() {
        AttributeValue result = DynamoDBExecutor.toAttributeValue(null);
        assertNotNull(result);
        assertTrue(result.nul());
    }

    @Test
    public void testAttrValueOfString() {
        AttributeValue result = DynamoDBExecutor.toAttributeValue("test");
        assertNotNull(result);
        assertEquals("test", result.s());
    }

    @Test
    public void testAttrValueOfNumber() {
        AttributeValue result = DynamoDBExecutor.toAttributeValue(123);
        assertNotNull(result);
        assertEquals("123", result.n());
    }

    @Test
    public void testAttrValueOfBoolean() {
        AttributeValue result = DynamoDBExecutor.toAttributeValue(true);
        assertNotNull(result);
        assertTrue(result.bool());
    }

    @Test
    public void testAttrValueOfByteArray() {
        byte[] bytes = { 1, 2, 3 };
        AttributeValue result = DynamoDBExecutor.toAttributeValue(bytes);
        assertNotNull(result);
        assertNotNull(result.b());
    }

    @Test
    public void testAttrValueOfByteBuffer() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[] { 1, 2, 3 });
        AttributeValue result = DynamoDBExecutor.toAttributeValue(buffer);
        assertNotNull(result);
        assertNotNull(result.b());
    }

    @Test
    public void testAttrValueUpdateOf() {
        AttributeValueUpdate result = DynamoDBExecutor.toAttributeValueUpdate("test");
        assertNotNull(result);
        assertEquals("test", result.value().s());
        assertEquals(AttributeAction.PUT, result.action());
    }

    @Test
    public void testAttrValueUpdateOfWithAction() {
        AttributeValueUpdate result = DynamoDBExecutor.toAttributeValueUpdate("test", AttributeAction.DELETE);
        assertNotNull(result);
        assertEquals("test", result.value().s());
        assertEquals(AttributeAction.DELETE, result.action());

        result = DynamoDBExecutor.toAttributeValueUpdate(null, AttributeAction.DELETE);
        assertNotNull(result);
        assertNull(result.value());
        assertEquals(AttributeAction.DELETE, result.action());
    }

    @Test
    public void testAsKey() {
        Map<String, AttributeValue> result = DynamoDBExecutor.asKey("id", "123");
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("123", result.get("id").s());
    }

    @Test
    public void testAsKeyWithTwoParameters() {
        Map<String, AttributeValue> result = DynamoDBExecutor.asKey("id", "123", "name", "test");
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("123", result.get("id").s());
        assertEquals("test", result.get("name").s());
    }

    @Test
    public void testAsKeyWithThreeParameters() {
        Map<String, AttributeValue> result = DynamoDBExecutor.asKey("id", "123", "name", "test", "age", 25);
        assertNotNull(result);
        assertEquals(3, result.size());
        assertEquals("123", result.get("id").s());
        assertEquals("test", result.get("name").s());
        assertEquals("25", result.get("age").n());
    }

    @Test
    public void testAsKeyVarargs() {
        Map<String, AttributeValue> result = DynamoDBExecutor.asKey("id", "123", "name", "test");
        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    public void testAsKeyVarargsInvalidArgumentCount() {
        assertThrows(IllegalArgumentException.class, () -> {
            DynamoDBExecutor.asKey("id", "123", "name");
        });
    }

    @Test
    public void testAsItem() {
        Map<String, AttributeValue> result = DynamoDBExecutor.asItem("id", "123");
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("123", result.get("id").s());
    }

    @Test
    public void testAsUpdateItem() {
        Map<String, AttributeValueUpdate> result = DynamoDBExecutor.asUpdateItem("name", "test");
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("test", result.get("name").value().s());
    }

    @Test
    public void testToItemWithEntity() {
        TestEntity entity = new TestEntity();
        entity.setId("123");
        entity.setName("test");
        Map<String, AttributeValue> result = DynamoDBExecutor.toItem(entity);

        assertNotNull(result);
        assertEquals("123", result.get("id").s());
        assertEquals("test", result.get("name").s());
    }

    @Test
    public void testToItemWithMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("id", "123");
        map.put("name", "test");

        Map<String, AttributeValue> result = DynamoDBExecutor.toItem(map);

        assertNotNull(result);
        assertEquals("123", result.get("id").s());
        assertEquals("test", result.get("name").s());
    }

    @Test
    public void testToItemWithArray() {
        Object[] array = { "id", "123", "name", "test" };
        Map<String, AttributeValue> result = DynamoDBExecutor.toItem(array);

        assertNotNull(result);
        assertEquals("123", result.get("id").s());
        assertEquals("test", result.get("name").s());
    }

    @Test
    public void testToItemWithNamingPolicy() {
        TestEntity entity = new TestEntity();
        entity.setFirstName("John");
        Map<String, AttributeValue> result = DynamoDBExecutor.toItem(entity, NamingPolicy.SNAKE_CASE);

        assertNotNull(result);
        assertEquals("John", result.get("first_name").s());
    }

    @Test
    public void testToUpdateItem() {
        TestEntity entity = new TestEntity();
        entity.setId("123");
        entity.setName("test");
        Map<String, AttributeValueUpdate> result = DynamoDBExecutor.toUpdateItem(entity);

        assertNotNull(result);
        assertEquals("123", result.get("id").value().s());
        assertEquals("test", result.get("name").value().s());
    }

    @Test
    public void testToMap() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("123").build());
        item.put("count", AttributeValue.builder().n("5").build());

        Map<String, Object> result = DynamoDBExecutor.toMap(item);

        assertNotNull(result);
        assertEquals("123", result.get("id"));
        assertEquals("5", result.get("count"));
    }

    @Test
    public void testToMapWithObjectArray() {
        Object[] propNameAndValues = { "id", "123", "name", "test" };
        Map<String, Object> result = AnyUtil.array2Props(propNameAndValues);

        assertNotNull(result);
        assertEquals("123", result.get("id"));
        assertEquals("test", result.get("name"));
    }

    @Test
    public void testToMapWithNull() {
        Map<String, Object> result = DynamoDBExecutor.toMap((Map<String, AttributeValue>) null);
        assertNull(result);
    }

    @Test
    public void testToMapWithEmptyStringAttribute() {
        // Regression: an empty-string ("") S attribute is a legal DynamoDB value.
        // toValue() must return it rather than throwing "Unsupported Attribute type".
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("emptyStr", AttributeValue.builder().s("").build());
        item.put("normalStr", AttributeValue.builder().s("abc").build());

        Map<String, Object> result = DynamoDBExecutor.toMap(item);

        assertNotNull(result);
        assertEquals("", result.get("emptyStr"));
        assertEquals("abc", result.get("normalStr"));
    }

    @Test
    public void testToEntityWithGetItemResponse() {
        GetItemResponse response = GetItemResponse.builder()
                .item(Map.of("id", AttributeValue.builder().s("123").build(), "name", AttributeValue.builder().s("test").build()))
                .build();

        TestEntity result = DynamoDBExecutor.toEntity(response, TestEntity.class);

        assertNotNull(result);
        assertEquals("123", result.getId());
        assertEquals("test", result.getName());
    }

    @Test
    public void testToEntityWithNullResponse() {
        TestEntity result = DynamoDBExecutor.toEntity((GetItemResponse) null, TestEntity.class);
        assertNull(result);
    }

    @Test
    public void testToEntityWithEmptyResponse() {
        GetItemResponse response = GetItemResponse.builder().build();
        TestEntity result = DynamoDBExecutor.toEntity(response, TestEntity.class);
        assertNull(result);
    }

    @Test
    public void testToList() {
        QueryResponse queryResponse = QueryResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("1").build()), Map.of("id", AttributeValue.builder().s("2").build())))
                .build();

        List<TestEntity> result = DynamoDBExecutor.toList(queryResponse, TestEntity.class);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("1", result.get(0).getId());
        assertEquals("2", result.get(1).getId());
    }

    @Test
    public void testToListWithOffset() {
        QueryResponse queryResponse = QueryResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("1").build()), Map.of("id", AttributeValue.builder().s("2").build()),
                        Map.of("id", AttributeValue.builder().s("3").build())))
                .build();

        List<Map<String, Object>> result = DynamoDBExecutor.toList(queryResponse, 1, 2, Clazz.ofMap(String.class, Object.class));

        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("2", result.get(0).get("id"));
        assertEquals("3", result.get(1).get("id"));
    }

    @Test
    public void testExtractData() {
        QueryResponse queryResponse = QueryResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("1").build(), "name", AttributeValue.builder().s("Test1").build()),
                        Map.of("id", AttributeValue.builder().s("2").build(), "name", AttributeValue.builder().s("Test2").build())))
                .build();

        Dataset result = DynamoDBExecutor.extractData(queryResponse);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.containsColumn("id"));
        assertTrue(result.containsColumn("name"));
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

    @Test
    public void testPutItem() {
        String tableName = "TestTable";
        Map<String, AttributeValue> item = Map.of("id", AttributeValue.builder().s("123").build());

        PutItemResponse response = PutItemResponse.builder().build();

        when(mockDynamoDbClient.putItem(any(PutItemRequest.class))).thenReturn(response);

        PutItemResponse result = executor.putItem(tableName, item);

        assertNotNull(result);
    }

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
    public void testDeleteItem() {
        String tableName = "TestTable";
        Map<String, AttributeValue> key = Map.of("id", AttributeValue.builder().s("123").build());

        DeleteItemResponse response = DeleteItemResponse.builder().build();

        when(mockDynamoDbClient.deleteItem(any(DeleteItemRequest.class))).thenReturn(response);

        DeleteItemResponse result = executor.deleteItem(tableName, key);

        assertNotNull(result);
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
    public void testStream() {
        QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();

        QueryResponse response = QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("123").build()))).build();

        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(response);

        Stream<Map<String, Object>> stream = executor.stream(queryRequest);

        assertNotNull(stream);
        assertEquals(1, stream.count());
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
    public void testClose() {
        executor.close();
        verify(mockDynamoDbClient, times(1)).close();
    }

    // Tests for Filters class
    @Test
    public void testFiltersEq() {
        Map<String, Condition> result = Filters.eq("name", "test");
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(ComparisonOperator.EQ, result.get("name").comparisonOperator());
    }

    @Test
    public void testFiltersNe() {
        Map<String, Condition> result = Filters.ne("name", "test");
        assertNotNull(result);
        assertEquals(ComparisonOperator.NE, result.get("name").comparisonOperator());
    }

    @Test
    public void testFiltersGt() {
        Map<String, Condition> result = Filters.gt("age", 18);
        assertNotNull(result);
        assertEquals(ComparisonOperator.GT, result.get("age").comparisonOperator());
    }

    @Test
    public void testFiltersGe() {
        Map<String, Condition> result = Filters.ge("age", 18);
        assertNotNull(result);
        assertEquals(ComparisonOperator.GE, result.get("age").comparisonOperator());
    }

    @Test
    public void testFiltersLt() {
        Map<String, Condition> result = Filters.lt("age", 65);
        assertNotNull(result);
        assertEquals(ComparisonOperator.LT, result.get("age").comparisonOperator());
    }

    @Test
    public void testFiltersLe() {
        Map<String, Condition> result = Filters.le("age", 65);
        assertNotNull(result);
        assertEquals(ComparisonOperator.LE, result.get("age").comparisonOperator());
    }

    @Test
    public void testFiltersBt() {
        Map<String, Condition> result = Filters.bt("age", 18, 65);
        assertNotNull(result);
        assertEquals(ComparisonOperator.BETWEEN, result.get("age").comparisonOperator());
        assertEquals(2, result.get("age").attributeValueList().size());
    }

    @Test
    public void testFiltersIsNull() {
        Map<String, Condition> result = Filters.isNull("optional");
        assertNotNull(result);
        assertEquals(ComparisonOperator.NULL, result.get("optional").comparisonOperator());
    }

    @Test
    public void testFiltersNotNull() {
        Map<String, Condition> result = Filters.notNull("required");
        assertNotNull(result);
        assertEquals(ComparisonOperator.NOT_NULL, result.get("required").comparisonOperator());
    }

    @Test
    public void testFiltersContains() {
        Map<String, Condition> result = Filters.contains("tags", "java");
        assertNotNull(result);
        assertEquals(ComparisonOperator.CONTAINS, result.get("tags").comparisonOperator());
    }

    @Test
    public void testFiltersNotContains() {
        Map<String, Condition> result = Filters.notContains("tags", "python");
        assertNotNull(result);
        assertEquals(ComparisonOperator.NOT_CONTAINS, result.get("tags").comparisonOperator());
    }

    @Test
    public void testFiltersBeginsWith() {
        Map<String, Condition> result = Filters.beginsWith("name", "John");
        assertNotNull(result);
        assertEquals(ComparisonOperator.BEGINS_WITH, result.get("name").comparisonOperator());
    }

    @Test
    public void testFiltersInVarargs() {
        Map<String, Condition> result = Filters.in("status", "active", "pending", "approved");
        assertNotNull(result);
        assertEquals(ComparisonOperator.IN, result.get("status").comparisonOperator());
        assertEquals(3, result.get("status").attributeValueList().size());
    }

    @Test
    public void testFiltersInCollection() {
        List<String> values = List.of("active", "pending", "approved");
        Map<String, Condition> result = Filters.in("status", values);
        assertNotNull(result);
        assertEquals(ComparisonOperator.IN, result.get("status").comparisonOperator());
        assertEquals(3, result.get("status").attributeValueList().size());
    }

    @Test
    public void testFiltersBuilder() {
        ConditionBuilder builder = Filters.builder();
        assertNotNull(builder);
    }

    // Tests for ConditionBuilder class
    @Test
    public void testConditionBuilderEq() {
        Map<String, Condition> result = ConditionBuilder.create().eq("name", "test").build();
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(ComparisonOperator.EQ, result.get("name").comparisonOperator());
    }

    @Test
    public void testConditionBuilderMultipleConditions() {
        Map<String, Condition> result = Filters.builder().eq("status", "active").gt("age", 18).lt("score", 65).build();
        assertNotNull(result);
        assertEquals(3, result.size());
    }

    @Test
    public void testConditionBuilderAllOperations() {
        Map<String, Condition> result = Filters.builder()
                .eq("field1", "value1")
                .ne("field2", "value2")
                .gt("field3", 10)
                .ge("field4", 20)
                .lt("field5", 30)
                .le("field6", 40)
                .bt("field7", 1, 100)
                .isNull("field8")
                .notNull("field9")
                .contains("field10", "substring")
                .notContains("field11", "excluded")
                .beginsWith("field12", "prefix")
                .in("field13", "a", "b", "c")
                .build();

        assertNotNull(result);
        assertEquals(13, result.size());
        assertEquals(ComparisonOperator.EQ, result.get("field1").comparisonOperator());
        assertEquals(ComparisonOperator.NE, result.get("field2").comparisonOperator());
        assertEquals(ComparisonOperator.GT, result.get("field3").comparisonOperator());
        assertEquals(ComparisonOperator.GE, result.get("field4").comparisonOperator());
        assertEquals(ComparisonOperator.LT, result.get("field5").comparisonOperator());
        assertEquals(ComparisonOperator.LE, result.get("field6").comparisonOperator());
        assertEquals(ComparisonOperator.BETWEEN, result.get("field7").comparisonOperator());
        assertEquals(ComparisonOperator.NULL, result.get("field8").comparisonOperator());
        assertEquals(ComparisonOperator.NOT_NULL, result.get("field9").comparisonOperator());
        assertEquals(ComparisonOperator.CONTAINS, result.get("field10").comparisonOperator());
        assertEquals(ComparisonOperator.NOT_CONTAINS, result.get("field11").comparisonOperator());
        assertEquals(ComparisonOperator.BEGINS_WITH, result.get("field12").comparisonOperator());
        assertEquals(ComparisonOperator.IN, result.get("field13").comparisonOperator());
    }

    @Test
    public void testConditionBuilderInWithCollection() {
        List<Integer> values = List.of(1, 2, 3, 4, 5);
        Map<String, Condition> result = Filters.builder().in("numbers", values).build();

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(ComparisonOperator.IN, result.get("numbers").comparisonOperator());
        assertEquals(5, result.get("numbers").attributeValueList().size());
    }

    // Individual ConditionBuilder operator coverage tests.
    @Test
    public void testConditionBuilderNe() {
        Map<String, Condition> result = Filters.builder().ne("name", "test").build();
        assertEquals(ComparisonOperator.NE, result.get("name").comparisonOperator());
    }

    @Test
    public void testConditionBuilderGt() {
        Map<String, Condition> result = Filters.builder().gt("age", 18).build();
        assertEquals(ComparisonOperator.GT, result.get("age").comparisonOperator());
    }

    @Test
    public void testConditionBuilderGe() {
        Map<String, Condition> result = Filters.builder().ge("age", 18).build();
        assertEquals(ComparisonOperator.GE, result.get("age").comparisonOperator());
    }

    @Test
    public void testConditionBuilderLt() {
        Map<String, Condition> result = Filters.builder().lt("age", 65).build();
        assertEquals(ComparisonOperator.LT, result.get("age").comparisonOperator());
    }

    @Test
    public void testConditionBuilderLe() {
        Map<String, Condition> result = Filters.builder().le("age", 65).build();
        assertEquals(ComparisonOperator.LE, result.get("age").comparisonOperator());
    }

    @Test
    public void testConditionBuilderBt() {
        Map<String, Condition> result = Filters.builder().bt("age", 18, 65).build();
        assertEquals(ComparisonOperator.BETWEEN, result.get("age").comparisonOperator());
        assertEquals(2, result.get("age").attributeValueList().size());
    }

    @Test
    public void testConditionBuilderIsNull() {
        Map<String, Condition> result = Filters.builder().isNull("optional").build();
        assertEquals(ComparisonOperator.NULL, result.get("optional").comparisonOperator());
    }

    @Test
    public void testConditionBuilderNotNull() {
        Map<String, Condition> result = Filters.builder().notNull("required").build();
        assertEquals(ComparisonOperator.NOT_NULL, result.get("required").comparisonOperator());
    }

    @Test
    public void testConditionBuilderContains() {
        Map<String, Condition> result = Filters.builder().contains("tags", "java").build();
        assertEquals(ComparisonOperator.CONTAINS, result.get("tags").comparisonOperator());
    }

    @Test
    public void testConditionBuilderNotContains() {
        Map<String, Condition> result = Filters.builder().notContains("tags", "python").build();
        assertEquals(ComparisonOperator.NOT_CONTAINS, result.get("tags").comparisonOperator());
    }

    @Test
    public void testConditionBuilderBeginsWith() {
        Map<String, Condition> result = Filters.builder().beginsWith("name", "John").build();
        assertEquals(ComparisonOperator.BEGINS_WITH, result.get("name").comparisonOperator());
    }

    @Test
    public void testConditionBuilderInVarargsSingle() {
        Map<String, Condition> result = Filters.builder().in("status", "active").build();
        assertEquals(ComparisonOperator.IN, result.get("status").comparisonOperator());
        assertEquals(1, result.get("status").attributeValueList().size());
    }

    @Test
    public void testFiltersInCollection_Empty() {
        Map<String, Condition> result = Filters.in("status", List.of());
        assertNotNull(result);
        assertEquals(ComparisonOperator.IN, result.get("status").comparisonOperator());
        assertEquals(0, result.get("status").attributeValueList().size());
    }

    @Test
    public void testFiltersEq_AttributeValueContents() {
        Map<String, Condition> result = Filters.eq("name", "abc");
        assertEquals(1, result.get("name").attributeValueList().size());
        assertEquals("abc", result.get("name").attributeValueList().get(0).s());
    }

    @Test
    public void testFiltersBt_AttributeValueContents() {
        Map<String, Condition> result = Filters.bt("age", 18, 65);
        assertEquals("18", result.get("age").attributeValueList().get(0).n());
        assertEquals("65", result.get("age").attributeValueList().get(1).n());
    }

    @Test
    public void testConditionBuilderBuild_NullifiesInternalState() {
        // After build(), the internal map is nulled; calling build() again returns null.
        ConditionBuilder b = Filters.builder().eq("x", 1);
        Map<String, Condition> first = b.build();
        assertNotNull(first);
        assertEquals(1, first.size());
        assertNull(b.build());
    }

    // Tests for Mapper inner class
    @Test
    public void testMapperCreation() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        assertNotNull(mapper);
    }

    @Test
    public void testMapperWithInvalidEntity() {
        assertThrows(IllegalArgumentException.class, () -> {
            executor.mapper(InvalidEntity.class, "TestTable", NamingPolicy.CAMEL_CASE);
        });
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
    public void testMapperDeleteItemWithKey() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        Map<String, AttributeValue> key = Map.of("id", AttributeValue.builder().s("123").build());

        DeleteItemResponse response = DeleteItemResponse.builder().build();

        when(mockDynamoDbClient.deleteItem(any(DeleteItemRequest.class))).thenReturn(response);

        DeleteItemResponse result = mapper.deleteItem(key);

        assertNotNull(result);
    }

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
    public void testMapperStream() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        QueryRequest queryRequest = QueryRequest.builder().tableName("TestTable").build();

        QueryResponse response = QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("123").build()))).build();

        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(response);

        Stream<TestEntity> stream = mapper.stream(queryRequest);

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
    public void testMapperWithWrongTableName() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        GetItemRequest request = GetItemRequest.builder().tableName("WrongTable").key(Map.of("id", AttributeValue.builder().s("123").build())).build();

        assertThrows(IllegalArgumentException.class, () -> {
            mapper.getItem(request);
        });
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
    public void testMapperUpdateItemWithRequest() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        UpdateItemRequest request = UpdateItemRequest.builder().tableName("TestTable").key(Map.of("id", AttributeValue.builder().s("123").build())).build();

        when(mockDynamoDbClient.updateItem(any(UpdateItemRequest.class))).thenReturn(UpdateItemResponse.builder().build());

        UpdateItemResponse result = mapper.updateItem(request);
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
    public void testMapperPutItemWithRequest_WrongTableName() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        PutItemRequest request = PutItemRequest.builder().tableName("WrongTable").item(Map.of("id", AttributeValue.builder().s("1").build())).build();
        assertThrows(IllegalArgumentException.class, () -> mapper.putItem(request));
    }

    @Test
    public void testMapperUpdateItemWithRequest_WrongTableName() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        UpdateItemRequest request = UpdateItemRequest.builder().tableName("WrongTable").key(Map.of("id", AttributeValue.builder().s("1").build())).build();
        assertThrows(IllegalArgumentException.class, () -> mapper.updateItem(request));
    }

    @Test
    public void testMapperDeleteItemWithRequest_WrongTableName() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        DeleteItemRequest request = DeleteItemRequest.builder().tableName("WrongTable").key(Map.of("id", AttributeValue.builder().s("1").build())).build();
        assertThrows(IllegalArgumentException.class, () -> mapper.deleteItem(request));
    }

    @Test
    public void testMapperListWithWrongTableName() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        QueryRequest queryRequest = QueryRequest.builder().tableName("WrongTable").build();
        assertThrows(IllegalArgumentException.class, () -> mapper.list(queryRequest));
    }

    @Test
    public void testMapperScanWithWrongTableName() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        ScanRequest scanRequest = ScanRequest.builder().tableName("WrongTable").build();
        assertThrows(IllegalArgumentException.class, () -> mapper.scan(scanRequest));
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

    // -- Additional coverage tests below --

    @Test
    public void testAsUpdateItemTwoPairs() {
        Map<String, AttributeValueUpdate> result = DynamoDBExecutor.asUpdateItem("name", "alice", "age", 30);
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("alice", result.get("name").value().s());
        assertEquals("30", result.get("age").value().n());
    }

    @Test
    public void testAsUpdateItemThreePairs() {
        Map<String, AttributeValueUpdate> result = DynamoDBExecutor.asUpdateItem("a", "1", "b", "2", "c", "3");
        assertNotNull(result);
        assertEquals(3, result.size());
        assertEquals("1", result.get("a").value().s());
        assertEquals("2", result.get("b").value().s());
        assertEquals("3", result.get("c").value().s());
    }

    @Test
    public void testAsUpdateItemVarargs() {
        Map<String, AttributeValueUpdate> result = DynamoDBExecutor.asUpdateItem(new Object[] { "a", 1, "b", 2 });
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("1", result.get("a").value().n());
    }

    @Test
    public void testAsUpdateItemVarargs_OddCountThrows() {
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asUpdateItem(new Object[] { "a", 1, "b" }));
    }

    @Test
    public void testAsItemVarargs() {
        Map<String, AttributeValue> result = DynamoDBExecutor.asItem(new Object[] { "a", "x", "b", "y" });
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("x", result.get("a").s());
        assertEquals("y", result.get("b").s());
    }

    @Test
    public void testToListFromScanResponse() {
        ScanResponse scanResponse = ScanResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("a").build()), Map.of("id", AttributeValue.builder().s("b").build())))
                .build();

        List<TestEntity> result = DynamoDBExecutor.toList(scanResponse, TestEntity.class);
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("a", result.get(0).getId());
    }

    @Test
    public void testToListFromScanResponseWithOffsetCount() {
        ScanResponse scanResponse = ScanResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("a").build()), Map.of("id", AttributeValue.builder().s("b").build()),
                        Map.of("id", AttributeValue.builder().s("c").build())))
                .build();

        List<TestEntity> result = DynamoDBExecutor.toList(scanResponse, 1, 1, TestEntity.class);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("b", result.get(0).getId());
    }

    @Test
    public void testExtractDataFromScanResponse() {
        ScanResponse scanResponse = ScanResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("1").build()), Map.of("id", AttributeValue.builder().s("2").build())))
                .build();

        Dataset result = DynamoDBExecutor.extractData(scanResponse);
        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.containsColumn("id"));
    }

    @Test
    public void testExtractDataFromScanResponseWithOffsetCount() {
        ScanResponse scanResponse = ScanResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("1").build()), Map.of("id", AttributeValue.builder().s("2").build()),
                        Map.of("id", AttributeValue.builder().s("3").build())))
                .build();

        Dataset result = DynamoDBExecutor.extractData(scanResponse, 1, 1);
        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testExtractDataFromQueryResponseWithOffsetCount() {
        QueryResponse queryResponse = QueryResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("1").build()), Map.of("id", AttributeValue.builder().s("2").build()),
                        Map.of("id", AttributeValue.builder().s("3").build())))
                .build();

        Dataset result = DynamoDBExecutor.extractData(queryResponse, 0, 2);
        assertNotNull(result);
        assertEquals(2, result.size());
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
    public void testGetItemWithConsistentReadAndClass() {
        Map<String, AttributeValue> key = Map.of("id", AttributeValue.builder().s("1").build());
        GetItemResponse res = GetItemResponse.builder().item(Map.of("id", AttributeValue.builder().s("1").build())).build();
        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(res);

        TestEntity result = executor.getItem("TestTable", key, true, TestEntity.class);
        assertNotNull(result);
        assertEquals("1", result.getId());
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

    @Test
    public void testScanWithAttrsAndClass() {
        ScanResponse res = ScanResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build();
        when(mockDynamoDbClient.scan(any(ScanRequest.class))).thenReturn(res);

        Stream<TestEntity> stream = executor.scan("TestTable", List.of("id"), TestEntity.class);
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
    public void testScanWithAttrsFilterAndClass() {
        ScanResponse res = ScanResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build();
        when(mockDynamoDbClient.scan(any(ScanRequest.class))).thenReturn(res);

        Stream<TestEntity> stream = executor.scan("TestTable", List.of("id"), Filters.eq("status", "active"), TestEntity.class);
        assertNotNull(stream);
        assertEquals(1, stream.count());
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
    public void testStreamWithClass() {
        QueryRequest req = QueryRequest.builder().tableName("TestTable").build();
        QueryResponse res = QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build();
        when(mockDynamoDbClient.query(any(QueryRequest.class))).thenReturn(res);

        Stream<TestEntity> stream = executor.stream(req, TestEntity.class);
        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    // ===== Coverage gap fillers: toValue / toItem / toUpdateItem / createRowMapper / extras =====

    @Test
    public void testToValue_BoolViaToMap() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("flag", AttributeValue.builder().bool(true).build());
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        assertEquals(Boolean.TRUE, result.get("flag"));
    }

    @Test
    public void testToValue_BinaryViaToMap() {
        software.amazon.awssdk.core.SdkBytes bytes = software.amazon.awssdk.core.SdkBytes.fromByteArray(new byte[] { 1, 2, 3 });
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("b", AttributeValue.builder().b(bytes).build());
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        byte[] arr = (byte[]) result.get("b");
        assertEquals(3, arr.length);
    }

    @Test
    public void testToValue_StringSetViaToMap() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ss", AttributeValue.builder().ss("a", "b", "c").build());
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        assertEquals(3, ((List<?>) result.get("ss")).size());
    }

    @Test
    public void testToValue_NumberSetViaToMap() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ns", AttributeValue.builder().ns("1", "2").build());
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        assertEquals(2, ((List<?>) result.get("ns")).size());
    }

    @Test
    public void testToValue_BinarySetViaToMap() {
        software.amazon.awssdk.core.SdkBytes b1 = software.amazon.awssdk.core.SdkBytes.fromByteArray(new byte[] { 1 });
        software.amazon.awssdk.core.SdkBytes b2 = software.amazon.awssdk.core.SdkBytes.fromByteArray(new byte[] { 2 });
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("bs", AttributeValue.builder().bs(b1, b2).build());
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        @SuppressWarnings("unchecked")
        List<byte[]> bsList = (List<byte[]>) result.get("bs");
        assertEquals(2, bsList.size());
    }

    @Test
    public void testToValue_ListViaToMap() {
        AttributeValue listAttr = AttributeValue.builder().l(AttributeValue.builder().s("x").build(), AttributeValue.builder().n("5").build()).build();
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("l", listAttr);
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        List<?> list = (List<?>) result.get("l");
        assertEquals(2, list.size());
    }

    @Test
    public void testToValue_MapViaToMap() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("s", AttributeValue.builder().s("hello").build());
        final Map<String, AttributeValue> child = new HashMap<>();
        child.put("child", AttributeValue.builder().s("value").build());
        item.put("m", AttributeValue.builder().m(child).build());
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        assertEquals("hello", result.get("s"));

        @SuppressWarnings("unchecked")
        final Map<String, Object> nested = (Map<String, Object>) result.get("m");
        assertEquals("value", nested.get("child"));
    }

    @Test
    public void testToValue_NullViaToMap() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("n", AttributeValue.builder().nul(true).build());
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        assertNull(result.get("n"));
    }

    // toItem / toUpdateItem from Map and Object[]
    @Test
    public void testToItem_FromMapSnakeCase() {
        Map<String, Object> map = new java.util.LinkedHashMap<>();
        map.put("firstName", "John");
        Map<String, AttributeValue> result = DynamoDBExecutor.toItem(map, NamingPolicy.SNAKE_CASE);
        assertEquals("John", result.get("first_name").s());
    }

    @Test
    public void testToItem_UnsupportedTypeThrows() {
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toItem("notSupported", NamingPolicy.CAMEL_CASE));
    }

    @Test
    public void testToUpdateItem_FromMapSnakeCase() {
        Map<String, Object> map = new java.util.LinkedHashMap<>();
        map.put("firstName", "John");
        Map<String, AttributeValueUpdate> result = DynamoDBExecutor.toUpdateItem(map, NamingPolicy.SNAKE_CASE);
        assertEquals("John", result.get("first_name").value().s());
    }

    @Test
    public void testToUpdateItem_FromObjectArray() {
        Object[] arr = { "name", "Alice", "age", 30 };
        Map<String, AttributeValueUpdate> result = DynamoDBExecutor.toUpdateItem(arr);
        assertEquals(2, result.size());
        assertEquals("Alice", result.get("name").value().s());
        assertEquals("30", result.get("age").value().n());
    }

    @Test
    public void testToUpdateItem_UnsupportedTypeThrows() {
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toUpdateItem("notSupported"));
    }

    // toEntity null branches
    @Test
    public void testToEntity_NullItemReturnsNull() {
        TestEntity result = DynamoDBExecutor.toEntity((Map<String, AttributeValue>) null, TestEntity.class);
        assertNull(result);
    }

    @Test
    public void testToEntity_NullGetItemResponseReturnsNull() {
        TestEntity result = DynamoDBExecutor.toEntity((GetItemResponse) null, TestEntity.class);
        assertNull(result);
    }

    // toList branches: Object[], Collection, Map, single-value
    @Test
    public void testToList_AsObjectArrayClass() {
        Map<String, AttributeValue> r = new java.util.LinkedHashMap<>();
        r.put("a", AttributeValue.builder().s("x").build());
        r.put("b", AttributeValue.builder().s("y").build());
        QueryResponse qr = QueryResponse.builder().items(List.of(r)).build();

        List<Object[]> result = DynamoDBExecutor.toList(qr, Object[].class);
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).length);
    }

    @Test
    public void testToList_AsMapClass() {
        Map<String, AttributeValue> r = new java.util.LinkedHashMap<>();
        r.put("a", AttributeValue.builder().s("x").build());
        QueryResponse qr = QueryResponse.builder().items(List.of(r)).build();

        @SuppressWarnings({ "unchecked", "rawtypes" })
        List<Map> result = DynamoDBExecutor.toList(qr, (Class) Map.class);
        assertEquals(1, result.size());
        assertEquals("x", result.get(0).get("a"));
    }

    @Test
    public void testToList_AsCollectionClass() {
        Map<String, AttributeValue> r = new java.util.LinkedHashMap<>();
        r.put("a", AttributeValue.builder().s("x").build());
        r.put("b", AttributeValue.builder().s("y").build());
        QueryResponse qr = QueryResponse.builder().items(List.of(r)).build();

        @SuppressWarnings({ "unchecked", "rawtypes" })
        List<List> result = DynamoDBExecutor.toList(qr, (Class) List.class);
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).size());
    }

    @Test
    public void testToList_AsSingleValueClass() {
        Map<String, AttributeValue> r = new java.util.LinkedHashMap<>();
        r.put("v", AttributeValue.builder().s("hello").build());
        QueryResponse qr = QueryResponse.builder().items(List.of(r)).build();

        List<String> result = DynamoDBExecutor.toList(qr, String.class);
        assertEquals(1, result.size());
        assertEquals("hello", result.get(0));
    }

    @Test
    public void testToList_SingleValueMultiColumnThrows() {
        Map<String, AttributeValue> r = new java.util.LinkedHashMap<>();
        r.put("a", AttributeValue.builder().s("1").build());
        r.put("b", AttributeValue.builder().s("2").build());
        QueryResponse qr = QueryResponse.builder().items(List.of(r)).build();

        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toList(qr, String.class));
    }

    // extractData edge cases
    @Test
    public void testExtractData_EmptyItems() {
        Dataset ds = DynamoDBExecutor.extractData(QueryResponse.builder().items(new ArrayList<>()).build());
        assertNotNull(ds);
        assertEquals(0, ds.size());
    }

    @Test
    public void testExtractData_NegativeOffsetThrows() {
        QueryResponse qr = QueryResponse.builder().items(List.of(Map.of("id", AttributeValue.builder().s("1").build()))).build();
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.extractData(qr, -1, 1));
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

    // close() should not throw when client is non-null
    @Test
    public void testClose_NoThrow() {
        DynamoDBExecutor exec = new DynamoDBExecutor(mockDynamoDbClient);
        assertDoesNotThrow(exec::close);
    }

    // ===== readRow branches via getItem(GetItemRequest, Class) - exercise private readRow paths =====
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
    public void testGetItemRequest_ReadRowEmptyResponse_NullForBean() {
        // GetItemResponse with no item triggers the "row == null" branch on readRow
        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().build());

        GetItemRequest req = GetItemRequest.builder().tableName("T").key(Map.of("id", AttributeValue.builder().s("x").build())).build();
        TestEntity result = executor.getItem(req, TestEntity.class);
        assertNull(result);
    }

    @Test
    public void testGetItemRequest_ReadRowEmptyResponse_DefaultForPrimitive() {
        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().build());

        GetItemRequest req = GetItemRequest.builder().tableName("T").key(Map.of("id", AttributeValue.builder().s("x").build())).build();
        Integer result = executor.getItem(req, int.class);
        assertEquals(0, result);
    }

    // ===== toEntity dot-notation property branch =====
    @Test
    public void testToEntity_DotNotationPropName() {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("id", AttributeValue.builder().s("1").build());
        item.put("unknown.path", AttributeValue.builder().s("val").build());

        TestEntity result = DynamoDBExecutor.toEntity(item, TestEntity.class);
        assertNotNull(result);
        assertEquals("1", result.getId());
    }

    @Test
    public void testToEntity_UnknownPropIgnored() {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("id", AttributeValue.builder().s("1").build());
        item.put("noSuchField", AttributeValue.builder().s("x").build());

        TestEntity result = DynamoDBExecutor.toEntity(item, TestEntity.class);
        assertNotNull(result);
        assertEquals("1", result.getId());
    }

    // ===== toValue fallthrough branches (empty string s/n, hasM) =====
    @Test
    public void testToValue_EmptyStringS_FallsThrough() {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("k", AttributeValue.builder().s("").build());
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        assertEquals("", result.get("k"));
    }

    @Test
    public void testToValue_EmptyNumberN_FallsThrough() {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("k", AttributeValue.builder().n("").build());
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        assertEquals("", result.get("k"));
    }

    /**
     * Regression test for toValue's NUL=false fallback: a hand-built AttributeValue with NUL=false
     * (DynamoDB itself only ever emits NUL=true, which is handled by the top null-check) previously
     * fell through every branch and threw IllegalArgumentException ("Unsupported Attribute type").
     * It now mirrors the v1 getNULL() fallback and yields Boolean.FALSE.
     */
    @Test
    public void testToValue_NulFalseViaToMap_YieldsFalseInsteadOfThrowing() {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("flag", AttributeValue.builder().nul(false).build());

        Map<String, Object> result = assertDoesNotThrow(() -> DynamoDBExecutor.toMap(item));

        assertEquals(Boolean.FALSE, result.get("flag"));
    }

    // TODO: testToValue_NestedMapBranch - AttributeValue.builder().m() wraps as UnmodifiableMap which N.newMap
    // cannot instantiate. This branch is reached via the public API path but not testable in isolation.

    @Test
    public void testToValue_TargetClassConversion() {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("count", AttributeValue.builder().n("42").build());
        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(item).build());

        GetItemRequest req = GetItemRequest.builder().tableName("T").key(Map.of("id", AttributeValue.builder().s("1").build())).build();
        Integer result = executor.getItem(req, Integer.class);
        assertEquals(42, result);
    }

    // ===== Constructor validation =====
    @Test
    public void testConstructor_NullClientThrows() {
        assertThrows(IllegalArgumentException.class, () -> new DynamoDBExecutor(null));
    }

    // ===== mapper() with no @Table annotation should throw =====
    @Test
    public void testMapper_NoTableAnnotationThrows() {
        assertThrows(IllegalArgumentException.class, () -> executor.mapper(V2NoTableEntity.class));
    }

    // TODO: v2 checkEntityClass is package-private and inaccessible from this test package
    // TODO: v2 toItem(Collection), toUpdateItem(Collection), putItem(String, Object) are package-private
    // and inaccessible from this test package - exercised indirectly via Mapper.batchPutItem/batchPutItem

    // ===== Package-private static putItem/updateItem/deleteItem(String, Map ...) =====
    @Test
    public void testPutItem_StringMap_PackagePrivate() {
        when(mockDynamoDbClient.putItem(any(PutItemRequest.class))).thenReturn(PutItemResponse.builder().build());
        PutItemResponse result = executor.putItem("T", Map.of("id", AttributeValue.builder().s("1").build()));
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
    public void testDeleteItem_StringMap_PackagePrivate() {
        when(mockDynamoDbClient.deleteItem(any(DeleteItemRequest.class))).thenReturn(DeleteItemResponse.builder().build());
        DeleteItemResponse result = executor.deleteItem("T", Map.of("id", AttributeValue.builder().s("1").build()));
        assertNotNull(result);
    }

    // ===== package-private getItem(String, Map, Boolean) static overload =====
    @Test
    public void testGetItem_StringMapWithConsistentRead_PackagePrivate() {
        Map<String, AttributeValue> item = Map.of("id", AttributeValue.builder().s("1").build());
        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(item).build());
        Map<String, Object> result = executor.getItem("T", Map.of("id", AttributeValue.builder().s("1").build()), Boolean.TRUE);
        assertNotNull(result);
    }

    // ===== toList(QueryResponse, offset, count, Class) and toList(ScanResponse, offset, count, Class) =====
    @Test
    public void testToList_QueryResponseOffsetCount() {
        QueryResponse qr = QueryResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("1").build()), Map.of("id", AttributeValue.builder().s("2").build()),
                        Map.of("id", AttributeValue.builder().s("3").build())))
                .build();
        List<TestEntity> result = DynamoDBExecutor.toList(qr, 1, 2, TestEntity.class);
        assertEquals(2, result.size());
        assertEquals("2", result.get(0).getId());
    }

    @Test
    public void testToList_ScanResponseOffsetCount() {
        ScanResponse sr = ScanResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("a").build()), Map.of("id", AttributeValue.builder().s("b").build()),
                        Map.of("id", AttributeValue.builder().s("c").build())))
                .build();
        List<TestEntity> result = DynamoDBExecutor.toList(sr, 1, 1, TestEntity.class);
        assertEquals(1, result.size());
        assertEquals("b", result.get(0).getId());
    }

    // TODO: toList(List<Map>, offset, count, Class) is package-private in v2

    // ===== asKey(Object[]) with mixed Object array =====
    @Test
    public void testAsKey_VarargsMixed() {
        Map<String, AttributeValue> result = DynamoDBExecutor.asKey("k1", "v1", "k2", 42);
        assertEquals(2, result.size());
        assertEquals("v1", result.get("k1").s());
        assertEquals("42", result.get("k2").n());
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
    private static class TestEntity {
        @com.landawn.abacus.annotation.Id
        private String id;
        private String name;
        private String firstName;

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

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
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
}
