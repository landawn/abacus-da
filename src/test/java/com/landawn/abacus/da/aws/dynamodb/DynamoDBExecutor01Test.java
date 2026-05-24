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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

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
import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.da.aws.dynamodb.DynamoDBExecutor.ConditionBuilder;
import com.landawn.abacus.da.aws.dynamodb.DynamoDBExecutor.Filters;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.stream.Stream;

public class DynamoDBExecutor01Test extends TestBase {

    @Mock
    private AmazonDynamoDBClient mockDynamoDBClient;

    @Mock
    private DynamoDBMapperConfig mockMapperConfig;

    private DynamoDBExecutor executor;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        executor = new DynamoDBExecutor(mockDynamoDBClient);
    }

    @Test
    public void testConstructorWithConfig() {
        DynamoDBExecutor executorWithConfig = new DynamoDBExecutor(mockDynamoDBClient, mockMapperConfig);
        assertNotNull(executorWithConfig);
    }

    @Test
    public void testConstructorWithAsyncExecutor() {
        AsyncExecutor asyncExecutor = new AsyncExecutor(2, 4, 60L, TimeUnit.SECONDS);
        DynamoDBExecutor executorWithAsync = new DynamoDBExecutor(mockDynamoDBClient, mockMapperConfig, asyncExecutor);
        assertNotNull(executorWithAsync);
    }

    @Test
    public void testDynamoDBClient() {
        AmazonDynamoDBClient client = executor.dynamoDBClient();
        assertNotNull(client);
        assertEquals(mockDynamoDBClient, client);
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
    public void testAsync() {
        AsyncDynamoDBExecutor asyncExecutor = executor.async();
        assertNotNull(asyncExecutor);
    }

    @Test
    public void testAttrValueOfNull() {
        AttributeValue result = DynamoDBExecutor.toAttributeValue(null);
        assertNotNull(result);
        assertTrue(result.getNULL());
    }

    @Test
    public void testAttrValueOfString() {
        AttributeValue result = DynamoDBExecutor.toAttributeValue("test");
        assertNotNull(result);
        assertEquals("test", result.getS());
    }

    @Test
    public void testAttrValueOfNumber() {
        AttributeValue result = DynamoDBExecutor.toAttributeValue(123);
        assertNotNull(result);
        assertEquals("123", result.getN());
    }

    @Test
    public void testAttrValueOfBoolean() {
        AttributeValue result = DynamoDBExecutor.toAttributeValue(true);
        assertNotNull(result);
        assertTrue(result.getBOOL());
    }

    @Test
    public void testAttrValueOfByteBuffer() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[] { 1, 2, 3 });
        AttributeValue result = DynamoDBExecutor.toAttributeValue(buffer);
        assertNotNull(result);
        assertEquals(buffer, result.getB());
    }

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
    }

    @Test
    public void testAsKey() {
        Map<String, AttributeValue> result = DynamoDBExecutor.asKey("id", "123");
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("123", result.get("id").getS());
    }

    @Test
    public void testAsKeyWithTwoParameters() {
        Map<String, AttributeValue> result = DynamoDBExecutor.asKey("id", "123", "name", "test");
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("123", result.get("id").getS());
        assertEquals("test", result.get("name").getS());
    }

    @Test
    public void testAsKeyWithThreeParameters() {
        Map<String, AttributeValue> result = DynamoDBExecutor.asKey("id", "123", "name", "test", "age", 25);
        assertNotNull(result);
        assertEquals(3, result.size());
        assertEquals("123", result.get("id").getS());
        assertEquals("test", result.get("name").getS());
        assertEquals("25", result.get("age").getN());
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
        assertEquals("123", result.get("id").getS());
    }

    @Test
    public void testAsUpdateItem() {
        Map<String, AttributeValueUpdate> result = DynamoDBExecutor.asUpdateItem("name", "test");
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("test", result.get("name").getValue().getS());
    }

    @Test
    public void testToItemWithEntity() {
        TestEntity entity = new TestEntity();
        entity.setId("123");
        entity.setName("test");
        Map<String, AttributeValue> result = DynamoDBExecutor.toItem(entity);

        assertNotNull(result);
        assertEquals("123", result.get("id").getS());
        assertEquals("test", result.get("name").getS());
    }

    @Test
    public void testToItemWithMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("id", "123");
        map.put("name", "test");

        Map<String, AttributeValue> result = DynamoDBExecutor.toItem(map);

        assertNotNull(result);
        assertEquals("123", result.get("id").getS());
        assertEquals("test", result.get("name").getS());
    }

    @Test
    public void testToItemWithArray() {
        Object[] array = { "id", "123", "name", "test" };
        Map<String, AttributeValue> result = DynamoDBExecutor.toItem(array);

        assertNotNull(result);
        assertEquals("123", result.get("id").getS());
        assertEquals("test", result.get("name").getS());
    }

    @Test
    public void testToItemWithNamingPolicy() {
        TestEntity entity = new TestEntity();
        entity.setFirstName("John");
        Map<String, AttributeValue> result = DynamoDBExecutor.toItem(entity, NamingPolicy.SNAKE_CASE);

        assertNotNull(result);
        assertEquals("John", result.get("first_name").getS());
    }

    @Test
    public void testToUpdateItem() {
        TestEntity entity = new TestEntity();
        entity.setId("123");
        entity.setName("test");
        Map<String, AttributeValueUpdate> result = DynamoDBExecutor.toUpdateItem(entity);

        assertNotNull(result);
        assertEquals("123", result.get("id").getValue().getS());
        assertEquals("test", result.get("name").getValue().getS());
    }

    @Test
    public void testToMap() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue().withS("123"));
        item.put("count", new AttributeValue().withN("5"));

        Map<String, Object> result = DynamoDBExecutor.toMap(item);

        assertNotNull(result);
        assertEquals("123", result.get("id"));
        assertEquals("5", result.get("count"));
    }

    @Test
    public void testToMapWithObjectArray() {
        Object[] propNameAndValues = { "id", "123", "name", "test" };
        Map<String, Object> result = DynamoDBExecutor.toMap(propNameAndValues);

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
    public void testToEntity() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue().withS("123"));
        item.put("name", new AttributeValue().withS("test"));

        TestEntity result = DynamoDBExecutor.toEntity(item, TestEntity.class);

        assertNotNull(result);
        assertEquals("123", result.getId());
        assertEquals("test", result.getName());
    }

    @Test
    public void testToList() {
        QueryResult queryResult = new QueryResult();
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("id", new AttributeValue().withS("1"));
        items.add(item1);
        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("id", new AttributeValue().withS("2"));
        items.add(item2);
        queryResult.setItems(items);

        List<TestEntity> result = DynamoDBExecutor.toList(queryResult, TestEntity.class);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("1", result.get(0).getId());
        assertEquals("2", result.get(1).getId());
    }

    @Test
    public void testExtractData() {
        QueryResult queryResult = new QueryResult();
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("id", new AttributeValue().withS("1"));
        item1.put("name", new AttributeValue().withS("Test1"));
        items.add(item1);
        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("id", new AttributeValue().withS("2"));
        item2.put("name", new AttributeValue().withS("Test2"));
        items.add(item2);
        queryResult.setItems(items);

        Dataset result = DynamoDBExecutor.extractData(queryResult);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.containsColumn("id"));
        assertTrue(result.containsColumn("name"));
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

    @Test
    public void testClose() {
        executor.close();
        verify(mockDynamoDBClient, times(1)).shutdown();
    }

    // Tests for Filters class
    @Test
    public void testFiltersEq() {
        Map<String, Condition> result = Filters.eq("name", "test");
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(ComparisonOperator.EQ.toString(), result.get("name").getComparisonOperator());
    }

    @Test
    public void testFiltersNe() {
        Map<String, Condition> result = Filters.ne("name", "test");
        assertNotNull(result);
        assertEquals(ComparisonOperator.NE.toString(), result.get("name").getComparisonOperator());
    }

    @Test
    public void testFiltersGt() {
        Map<String, Condition> result = Filters.gt("age", 18);
        assertNotNull(result);
        assertEquals(ComparisonOperator.GT.toString(), result.get("age").getComparisonOperator());
    }

    @Test
    public void testFiltersGe() {
        Map<String, Condition> result = Filters.ge("age", 18);
        assertNotNull(result);
        assertEquals(ComparisonOperator.GE.toString(), result.get("age").getComparisonOperator());
    }

    @Test
    public void testFiltersLt() {
        Map<String, Condition> result = Filters.lt("age", 65);
        assertNotNull(result);
        assertEquals(ComparisonOperator.LT.toString(), result.get("age").getComparisonOperator());
    }

    @Test
    public void testFiltersLe() {
        Map<String, Condition> result = Filters.le("age", 65);
        assertNotNull(result);
        assertEquals(ComparisonOperator.LE.toString(), result.get("age").getComparisonOperator());
    }

    @Test
    public void testFiltersBt() {
        Map<String, Condition> result = Filters.bt("age", 18, 65);
        assertNotNull(result);
        assertEquals(ComparisonOperator.BETWEEN.toString(), result.get("age").getComparisonOperator());
        assertEquals(2, result.get("age").getAttributeValueList().size());
    }

    @Test
    public void testFiltersIsNull() {
        Map<String, Condition> result = Filters.isNull("optional");
        assertNotNull(result);
        assertEquals(ComparisonOperator.NULL.toString(), result.get("optional").getComparisonOperator());
    }

    @Test
    public void testFiltersNotNull() {
        Map<String, Condition> result = Filters.notNull("required");
        assertNotNull(result);
        assertEquals(ComparisonOperator.NOT_NULL.toString(), result.get("required").getComparisonOperator());
    }

    @Test
    public void testFiltersContains() {
        Map<String, Condition> result = Filters.contains("tags", "java");
        assertNotNull(result);
        assertEquals(ComparisonOperator.CONTAINS.toString(), result.get("tags").getComparisonOperator());
    }

    @Test
    public void testFiltersNotContains() {
        Map<String, Condition> result = Filters.notContains("tags", "python");
        assertNotNull(result);
        assertEquals(ComparisonOperator.NOT_CONTAINS.toString(), result.get("tags").getComparisonOperator());
    }

    @Test
    public void testFiltersBeginsWith() {
        Map<String, Condition> result = Filters.beginsWith("name", "John");
        assertNotNull(result);
        assertEquals(ComparisonOperator.BEGINS_WITH.toString(), result.get("name").getComparisonOperator());
    }

    @Test
    public void testFiltersInVarargs() {
        Map<String, Condition> result = Filters.in("status", "active", "pending", "approved");
        assertNotNull(result);
        assertEquals(ComparisonOperator.IN.toString(), result.get("status").getComparisonOperator());
        assertEquals(3, result.get("status").getAttributeValueList().size());
    }

    @Test
    public void testFiltersInCollection() {
        List<String> values = List.of("active", "pending", "approved");
        Map<String, Condition> result = Filters.in("status", values);
        assertNotNull(result);
        assertEquals(ComparisonOperator.IN.toString(), result.get("status").getComparisonOperator());
        assertEquals(3, result.get("status").getAttributeValueList().size());
    }

    @Test
    public void testFiltersBuilder() {
        ConditionBuilder builder = Filters.builder();
        assertNotNull(builder);
    }

    // Tests for ConditionBuilder class
    @Test
    public void testConditionBuilderCreate() {
        ConditionBuilder builder = ConditionBuilder.create();
        assertNotNull(builder);
    }

    @Test
    public void testConditionBuilderEq() {
        Map<String, Condition> result = new ConditionBuilder().eq("name", "test").build();
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(ComparisonOperator.EQ.toString(), result.get("name").getComparisonOperator());
    }

    @Test
    public void testConditionBuilderAllOperations() {
        Map<String, Condition> result = new ConditionBuilder().eq("field1", "value1")
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
    }

    // Tests for Mapper inner class
    @Test
    public void testMapperCreation() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        assertNotNull(mapper);
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
    public void testMapperWithWrongTableName() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);

        GetItemRequest request = new GetItemRequest().withTableName("WrongTable").withKey(Map.of("id", new AttributeValue().withS("123")));

        assertThrows(IllegalArgumentException.class, () -> {
            mapper.getItem(request);
        });
    }

    /**
     * Regression test for bug in DynamoDBExecutor.Mapper.createBatchPutRequest:
     * it previously used toItem(entity) (always CAMEL_CASE) instead of
     * toItem(entity, namingPolicy), so batchPutItem ignored the mapper's configured
     * NamingPolicy while putItem/updateItem honored it. This produced inconsistent
     * attribute names (e.g. "firstName" vs "first_name") for the same entity.
     */
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

    // -- Additional coverage tests below --

    @Test
    public void testAsUpdateItemTwoPairs() {
        Map<String, AttributeValueUpdate> result = DynamoDBExecutor.asUpdateItem("name", "alice", "age", 30);
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("alice", result.get("name").getValue().getS());
        assertEquals("30", result.get("age").getValue().getN());
    }

    @Test
    public void testAsUpdateItemThreePairs() {
        Map<String, AttributeValueUpdate> result = DynamoDBExecutor.asUpdateItem("a", "1", "b", "2", "c", "3");
        assertNotNull(result);
        assertEquals(3, result.size());
        assertEquals("1", result.get("a").getValue().getS());
        assertEquals("2", result.get("b").getValue().getS());
        assertEquals("3", result.get("c").getValue().getS());
    }

    @Test
    public void testAsUpdateItemVarargs() {
        Map<String, AttributeValueUpdate> result = DynamoDBExecutor.asUpdateItem(new Object[] { "a", 1, "b", 2 });
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("1", result.get("a").getValue().getN());
        assertEquals("2", result.get("b").getValue().getN());
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
        assertEquals("x", result.get("a").getS());
        assertEquals("y", result.get("b").getS());
    }

    @Test
    public void testAsKeyVarargs_Explicit() {
        Map<String, AttributeValue> result = DynamoDBExecutor.asKey(new Object[] { "k1", "v1", "k2", "v2" });
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("v1", result.get("k1").getS());
        assertEquals("v2", result.get("k2").getS());
    }

    @Test
    public void testToItemCollection() {
        TestEntity e1 = new TestEntity();
        e1.setId("1");
        TestEntity e2 = new TestEntity();
        e2.setId("2");

        // Drives Mapper.batchPutItem which internally calls package-private toItem(Collection, NamingPolicy)
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class, "TestTable", NamingPolicy.SNAKE_CASE);
        when(mockDynamoDBClient.batchWriteItem(any(Map.class))).thenReturn(new BatchWriteItemResult());

        mapper.batchPutItem(List.of(e1, e2));

        ArgumentCaptor<Map<String, List<WriteRequest>>> captor = ArgumentCaptor.forClass(Map.class);
        verify(mockDynamoDBClient).batchWriteItem(captor.capture());
        assertEquals(2, captor.getValue().get("TestTable").size());
    }

    @Test
    public void testToListWithOffsetCount() {
        QueryResult queryResult = new QueryResult();
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        items.add(Map.of("id", new AttributeValue().withS("1")));
        items.add(Map.of("id", new AttributeValue().withS("2")));
        items.add(Map.of("id", new AttributeValue().withS("3")));
        queryResult.setItems(items);

        List<TestEntity> result = DynamoDBExecutor.toList(queryResult, 1, 2, TestEntity.class);
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("2", result.get(0).getId());
        assertEquals("3", result.get(1).getId());
    }

    @Test
    public void testToListFromScanResult() {
        ScanResult scanResult = new ScanResult();
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        items.add(Map.of("id", new AttributeValue().withS("a")));
        items.add(Map.of("id", new AttributeValue().withS("b")));
        scanResult.setItems(items);

        List<TestEntity> result = DynamoDBExecutor.toList(scanResult, TestEntity.class);
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("a", result.get(0).getId());
    }

    @Test
    public void testToListFromScanResultWithOffsetCount() {
        ScanResult scanResult = new ScanResult();
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        items.add(Map.of("id", new AttributeValue().withS("a")));
        items.add(Map.of("id", new AttributeValue().withS("b")));
        items.add(Map.of("id", new AttributeValue().withS("c")));
        scanResult.setItems(items);

        List<TestEntity> result = DynamoDBExecutor.toList(scanResult, 1, 1, TestEntity.class);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("b", result.get(0).getId());
    }

    @Test
    public void testExtractDataFromScanResult() {
        ScanResult scanResult = new ScanResult();
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        items.add(Map.of("id", new AttributeValue().withS("1"), "name", new AttributeValue().withS("n1")));
        items.add(Map.of("id", new AttributeValue().withS("2"), "name", new AttributeValue().withS("n2")));
        scanResult.setItems(items);

        Dataset result = DynamoDBExecutor.extractData(scanResult);
        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.containsColumn("id"));
    }

    @Test
    public void testExtractDataFromScanResultWithOffsetCount() {
        ScanResult scanResult = new ScanResult();
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        items.add(Map.of("id", new AttributeValue().withS("1")));
        items.add(Map.of("id", new AttributeValue().withS("2")));
        items.add(Map.of("id", new AttributeValue().withS("3")));
        scanResult.setItems(items);

        Dataset result = DynamoDBExecutor.extractData(scanResult, 1, 1);
        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testExtractDataFromQueryResultWithOffsetCount() {
        QueryResult queryResult = new QueryResult();
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        items.add(Map.of("id", new AttributeValue().withS("1")));
        items.add(Map.of("id", new AttributeValue().withS("2")));
        items.add(Map.of("id", new AttributeValue().withS("3")));
        queryResult.setItems(items);

        Dataset result = DynamoDBExecutor.extractData(queryResult, 0, 2);
        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    public void testToMapWithMapSupplier() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue().withS("123"));
        item.put("count", new AttributeValue().withN("5"));

        Map<String, Object> result = DynamoDBExecutor.toMap(item, com.landawn.abacus.util.IntFunctions.ofLinkedHashMap());
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("123", result.get("id"));
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
    public void testGetItemWithConsistentReadAndClass() {
        Map<String, AttributeValue> key = Map.of("id", new AttributeValue().withS("1"));
        GetItemResult res = new GetItemResult().withItem(Map.of("id", new AttributeValue().withS("1")));
        when(mockDynamoDBClient.getItem("TestTable", key, true)).thenReturn(res);

        TestEntity result = executor.getItem("TestTable", key, true, TestEntity.class);
        assertNotNull(result);
        assertEquals("1", result.getId());
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
    public void testScanWithFilterAndClass() {
        ScanResult res = new ScanResult().withItems(List.of(Map.of("id", new AttributeValue().withS("1"))));
        when(mockDynamoDBClient.scan(any(ScanRequest.class))).thenReturn(res);

        Stream<TestEntity> stream = executor.scan("TestTable", Filters.eq("status", "active"), TestEntity.class);
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
    public void testStreamWithClass() {
        QueryRequest req = new QueryRequest().withTableName("TestTable");
        QueryResult res = new QueryResult().withItems(List.of(Map.of("id", new AttributeValue().withS("1"))));
        when(mockDynamoDBClient.query(any(QueryRequest.class))).thenReturn(res);

        Stream<TestEntity> stream = executor.stream(req, TestEntity.class);
        assertNotNull(stream);
        assertEquals(1, stream.count());
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
}
