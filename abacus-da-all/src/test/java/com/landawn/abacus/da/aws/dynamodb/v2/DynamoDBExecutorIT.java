package com.landawn.abacus.da.aws.dynamodb.v2;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.Filters;
import com.landawn.abacus.util.Clazz;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.stream.Stream;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/**
 * DynamoDB Local round trips, pagination, and mapper behavior. Requires localhost:8000 and unused TestTable, UserTable, and RangeTable names. Only tables created by this suite are removed.
 *
 * <p>Run with {@code mvn -pl abacus-da-all -Pintegration-tests -Dit.test=DynamoDBExecutorIT verify -Dgpg.skip=true}.</p>
 */
public class DynamoDBExecutorIT extends TestBase {

    private static final String ENDPOINT = "http://localhost:8000";

    private static final String TEST_TABLE = "TestTable"; // hash key: id (matches @Table on TestEntity)
    private static final String USER_TABLE = "UserTable"; // hash key: user_id (for SNAKE_CASE @Id userId)
    private static final String RANGE_TABLE = "RangeTable"; // hash: pk, range: sk (for real query pagination)

    private static DynamoDbClient client;
    private static DynamoDBExecutor executor;

    private static final java.util.Set<String> createdTables = new java.util.LinkedHashSet<>();

    @BeforeAll
    public static void setUpClass() {
        client = DynamoDbClient.builder()
                .endpointOverride(URI.create(ENDPOINT))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
                .build();
        executor = new DynamoDBExecutor(client);

        try {
            client.listTables(ListTablesRequest.builder().limit(1).build()); // connectivity probe
            createSimpleTable(TEST_TABLE, "id");
            createSimpleTable(USER_TABLE, "user_id");
            createCompositeTable(RANGE_TABLE, "pk", "sk");

        } catch (final Exception e) {
            throw new IllegalStateException("DynamoDB integration setup failed at " + ENDPOINT, e);
        }
    }

    @AfterAll
    public static void tearDownClass() {
        if (client != null) {
            try {
                for (final String table : createdTables) {
                    client.deleteTable(DeleteTableRequest.builder().tableName(table).build());
                }
            } finally {
                client.close();
                createdTables.clear();
            }
        }
    }

    private static boolean tableExists(final String name) {
        try {
            client.describeTable(DescribeTableRequest.builder().tableName(name).build());
            return true;
        } catch (final ResourceNotFoundException e) {
            return false;
        }
    }

    private static void createSimpleTable(final String name, final String hashKey) {
        if (tableExists(name)) {
            throw new IllegalStateException("Integration tests require an unused table name: " + name);
        }
        client.createTable(CreateTableRequest.builder()
                .tableName(name)
                .keySchema(KeySchemaElement.builder().attributeName(hashKey).keyType(KeyType.HASH).build())
                .attributeDefinitions(AttributeDefinition.builder().attributeName(hashKey).attributeType(ScalarAttributeType.S).build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build());
        createdTables.add(name);
        client.waiter().waitUntilTableExists(DescribeTableRequest.builder().tableName(name).build());
    }

    private static void createCompositeTable(final String name, final String hashKey, final String rangeKey) {
        if (tableExists(name)) {
            throw new IllegalStateException("Integration tests require an unused table name: " + name);
        }
        client.createTable(CreateTableRequest.builder()
                .tableName(name)
                .keySchema(KeySchemaElement.builder().attributeName(hashKey).keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName(rangeKey).keyType(KeyType.RANGE).build())
                .attributeDefinitions(AttributeDefinition.builder().attributeName(hashKey).attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName(rangeKey).attributeType(ScalarAttributeType.S).build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build());
        createdTables.add(name);
        client.waiter().waitUntilTableExists(DescribeTableRequest.builder().tableName(name).build());
    }

    private static String id() {
        return UUID.randomUUID().toString();
    }

    @Test
    public void testClose_NoThrow() {
        // Use a throwaway client so the shared one stays open for the rest of the suite.
        DynamoDbClient throwaway = DynamoDbClient.builder()
                .endpointOverride(URI.create(ENDPOINT))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
                .build();
        DynamoDBExecutor exec = new DynamoDBExecutor(throwaway);
        assertDoesNotThrow(exec::close);
    }

    // ====================================================================================================
    // Round-trip tests against DynamoDB Local
    // ====================================================================================================

    @Test
    public void testGetItem() {
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id, "name", "test"));

        Map<String, Object> result = executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id));

        assertNotNull(result);
        assertEquals(id, result.get("id"));
        assertEquals("test", result.get("name"));
    }

    @Test
    public void testGetItemWithConsistentRead() {
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id));

        Map<String, Object> result = executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id), true);

        assertNotNull(result);
        assertEquals(id, result.get("id"));
    }

    @Test
    public void testGetItem_StringMapWithConsistentRead() {
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id));

        Map<String, Object> result = executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id), Boolean.TRUE);
        assertNotNull(result);
        assertEquals(id, result.get("id"));
    }

    @Test
    public void testGetItemWithTargetClass() {
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id, "name", "test"));

        TestEntity result = executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id), TestEntity.class);

        assertNotNull(result);
        assertEquals(id, result.getId());
        assertEquals("test", result.getName());
    }

    @Test
    public void testGetItemWithConsistentReadAndClass() {
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id));

        TestEntity result = executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id), true, TestEntity.class);
        assertNotNull(result);
        assertEquals(id, result.getId());
    }

    @Test
    public void testGetItemWithGetItemRequest() {
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id, "name", "n"));

        GetItemRequest req = GetItemRequest.builder().tableName(TEST_TABLE).key(DynamoDBExecutor.asKey("id", id)).build();
        Map<String, Object> result = executor.getItem(req);
        assertNotNull(result);
        assertEquals(id, result.get("id"));
        assertEquals("n", result.get("name"));
    }

    @Test
    public void testGetItemWithGetItemRequestAndClass() {
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id, "name", "n"));

        GetItemRequest req = GetItemRequest.builder().tableName(TEST_TABLE).key(DynamoDBExecutor.asKey("id", id)).build();
        TestEntity result = executor.getItem(req, TestEntity.class);
        assertNotNull(result);
        assertEquals(id, result.getId());
        assertEquals("n", result.getName());
    }

    @Test
    public void testGetItemRequest_ReadRowAsObjectArrayClass() {
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id, "a", "x", "b", "y"));

        // projectionExpression limits the returned columns so the row shape is deterministic.
        GetItemRequest req = GetItemRequest.builder().tableName(TEST_TABLE).key(DynamoDBExecutor.asKey("id", id)).projectionExpression("a, b").build();
        Object[] result = executor.getItem(req, Object[].class);
        assertNotNull(result);
        assertEquals(2, result.length);
    }

    @Test
    public void testGetItemRequest_ReadRowAsCollectionClass() {
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id, "a", "x", "b", "y"));

        GetItemRequest req = GetItemRequest.builder().tableName(TEST_TABLE).key(DynamoDBExecutor.asKey("id", id)).projectionExpression("a, b").build();
        List<?> result = executor.<List<?>> getItem(req, (Class<List<?>>) (Class<?>) List.class);
        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    public void testGetItemRequest_ReadRowAsMapClass() {
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id, "a", "x"));

        GetItemRequest req = GetItemRequest.builder().tableName(TEST_TABLE).key(DynamoDBExecutor.asKey("id", id)).projectionExpression("a").build();
        Map<?, ?> result = executor.<Map<?, ?>> getItem(req, (Class<Map<?, ?>>) (Class<?>) Map.class);
        assertNotNull(result);
        assertEquals("x", result.get("a"));
    }

    @Test
    public void testGetItemRequest_ReadRowAsSingleValueClass() {
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id, "v", "hello"));

        GetItemRequest req = GetItemRequest.builder().tableName(TEST_TABLE).key(DynamoDBExecutor.asKey("id", id)).projectionExpression("v").build();
        String result = executor.getItem(req, String.class);
        assertEquals("hello", result);
    }

    @Test
    public void testGetItemRequest_ReadRowAsSingleValueClass_MultiColumnThrows() {
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id, "a", "1", "b", "2"));

        GetItemRequest req = GetItemRequest.builder().tableName(TEST_TABLE).key(DynamoDBExecutor.asKey("id", id)).projectionExpression("a, b").build();
        assertThrows(IllegalArgumentException.class, () -> executor.getItem(req, String.class));
    }

    @Test
    public void testGetItemRequest_ReadRowEmptyResponse_NullForBean() {
        GetItemRequest req = GetItemRequest.builder().tableName(TEST_TABLE).key(DynamoDBExecutor.asKey("id", id())).build();
        TestEntity result = executor.getItem(req, TestEntity.class);
        assertNull(result);
    }

    @Test
    public void testGetItemRequest_ReadRowEmptyResponse_DefaultForPrimitive() {
        GetItemRequest req = GetItemRequest.builder().tableName(TEST_TABLE).key(DynamoDBExecutor.asKey("id", id())).build();
        Integer result = executor.getItem(req, int.class);
        assertEquals(0, result);
    }

    @Test
    public void testToValue_TargetClassConversion() {
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id, "num", 42));

        GetItemRequest req = GetItemRequest.builder().tableName(TEST_TABLE).key(DynamoDBExecutor.asKey("id", id)).projectionExpression("num").build();
        Integer result = executor.getItem(req, Integer.class);
        assertEquals(42, result);
    }

    @Test
    public void testBatchGetItem() {
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id));

        Map<String, KeysAndAttributes> requestItems = Map.of(TEST_TABLE, KeysAndAttributes.builder().keys(List.of(DynamoDBExecutor.asKey("id", id))).build());

        Map<String, List<Map<String, Object>>> result = executor.batchGetItem(requestItems);

        assertNotNull(result);
        assertEquals(1, result.get(TEST_TABLE).size());
        assertEquals(id, result.get(TEST_TABLE).get(0).get("id"));
    }

    @Test
    public void testBatchGetItemWithReturnConsumedCapacity() {
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id));

        Map<String, KeysAndAttributes> requestItems = Map.of(TEST_TABLE, KeysAndAttributes.builder().keys(List.of(DynamoDBExecutor.asKey("id", id))).build());

        Map<String, List<Map<String, Object>>> result = executor.batchGetItem(requestItems, "TOTAL");
        assertNotNull(result);
        assertEquals(1, result.get(TEST_TABLE).size());
    }

    @Test
    public void testBatchGetItemWithRequest() {
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id));

        BatchGetItemRequest req = BatchGetItemRequest.builder()
                .requestItems(Map.of(TEST_TABLE, KeysAndAttributes.builder().keys(List.of(DynamoDBExecutor.asKey("id", id))).build()))
                .build();
        Map<String, List<Map<String, Object>>> result = executor.batchGetItem(req);
        assertNotNull(result);
        assertEquals(1, result.get(TEST_TABLE).size());
    }

    @Test
    public void testBatchGetItemWithClass() {
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id, "name", "n"));

        Map<String, KeysAndAttributes> requestItems = Map.of(TEST_TABLE, KeysAndAttributes.builder().keys(List.of(DynamoDBExecutor.asKey("id", id))).build());
        Map<String, List<TestEntity>> result = executor.batchGetItem(requestItems, TestEntity.class);
        assertNotNull(result);
        assertEquals(1, result.get(TEST_TABLE).size());
        assertEquals(id, result.get(TEST_TABLE).get(0).getId());
    }

    @Test
    public void testBatchGetItemWithRequestAndClass() {
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id));

        BatchGetItemRequest req = BatchGetItemRequest.builder()
                .requestItems(Map.of(TEST_TABLE, KeysAndAttributes.builder().keys(List.of(DynamoDBExecutor.asKey("id", id))).build()))
                .build();
        Map<String, List<TestEntity>> result = executor.batchGetItem(req, TestEntity.class);
        assertNotNull(result);
        assertEquals(id, result.get(TEST_TABLE).get(0).getId());
    }

    @Test
    public void testBatchGetItem_NullResponses() {
        // Request a key that does not exist -> no responses for the table.
        Map<String, KeysAndAttributes> req = Map.of(TEST_TABLE, KeysAndAttributes.builder().keys(List.of(DynamoDBExecutor.asKey("id", id()))).build());

        Map<String, List<TestEntity>> result = executor.batchGetItem(req, TestEntity.class);
        assertNotNull(result);
        // No matching items -> empty list (or absent table key).
        assertTrue(result.getOrDefault(TEST_TABLE, List.of()).isEmpty());
    }

    @Test
    public void testPutItem() {
        String id = id();
        PutItemResponse result = executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id, "name", "test"));
        assertNotNull(result);

        // Verify it really landed.
        assertEquals("test", executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)).get("name"));
    }

    @Test
    public void testPutItemWithRequest() {
        String id = id();
        PutItemRequest req = PutItemRequest.builder().tableName(TEST_TABLE).item(DynamoDBExecutor.asItem("id", id)).build();
        PutItemResponse result = executor.putItem(req);
        assertNotNull(result);
        assertNotNull(executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)));
    }

    @Test
    public void testPutItemWithReturnValues() {
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id, "name", "v1"));

        // ALL_OLD returns the previous item that the put replaced.
        PutItemResponse result = executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id, "name", "v2"), "ALL_OLD");
        assertNotNull(result);
        assertEquals("v1", result.attributes().get("name").s());
        assertEquals("v2", executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)).get("name"));
    }

    @Test
    public void testBatchWriteItem() {
        String id = id();
        Map<String, List<WriteRequest>> requestItems = Map.of(TEST_TABLE,
                List.of(WriteRequest.builder().putRequest(PutRequest.builder().item(DynamoDBExecutor.asItem("id", id, "name", "bw")).build()).build()));

        BatchWriteItemResponse result = executor.batchWriteItem(requestItems);
        assertNotNull(result);
        assertEquals("bw", executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)).get("name"));
    }

    @Test
    public void testUpdateItem() {
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id, "name", "old"));

        Map<String, AttributeValueUpdate> updates = Map.of("name",
                AttributeValueUpdate.builder().value(AttributeValue.builder().s("updated").build()).action(AttributeAction.PUT).build());
        UpdateItemResponse result = executor.updateItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id), updates);
        assertNotNull(result);
        assertEquals("updated", executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)).get("name"));
    }

    @Test
    public void testUpdateItemWithRequest() {
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id, "name", "old"));

        UpdateItemRequest req = UpdateItemRequest.builder()
                .tableName(TEST_TABLE)
                .key(DynamoDBExecutor.asKey("id", id))
                .attributeUpdates(Map.of("name",
                        AttributeValueUpdate.builder().value(AttributeValue.builder().s("req-updated").build()).action(AttributeAction.PUT).build()))
                .build();
        UpdateItemResponse result = executor.updateItem(req);
        assertNotNull(result);
        assertEquals("req-updated", executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)).get("name"));
    }

    @Test
    public void testUpdateItemWithReturnValues() {
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id, "name", "old"));

        Map<String, AttributeValueUpdate> upd = Map.of("name",
                AttributeValueUpdate.builder().value(AttributeValue.builder().s("new").build()).action(AttributeAction.PUT).build());
        UpdateItemResponse result = executor.updateItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id), upd, "ALL_NEW");
        assertNotNull(result);
        assertEquals("new", result.attributes().get("name").s());
    }

    @Test
    public void testUpdateItem_StringMapMap() {
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id));
        UpdateItemResponse result = executor.updateItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id),
                Map.of("name", AttributeValueUpdate.builder().value(AttributeValue.builder().s("x").build()).build()));
        assertNotNull(result);
    }

    @Test
    public void testDeleteItem() {
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id));

        DeleteItemResponse result = executor.deleteItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id));
        assertNotNull(result);
        assertNull(executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)));
    }

    @Test
    public void testDeleteItemWithRequest() {
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id));

        DeleteItemRequest req = DeleteItemRequest.builder().tableName(TEST_TABLE).key(DynamoDBExecutor.asKey("id", id)).build();
        DeleteItemResponse result = executor.deleteItem(req);
        assertNotNull(result);
        assertNull(executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)));
    }

    @Test
    public void testDeleteItemWithReturnValues() {
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id, "name", "gone"));

        DeleteItemResponse result = executor.deleteItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id), "ALL_OLD");
        assertNotNull(result);
        assertEquals("gone", result.attributes().get("name").s());
    }

    // ----- query / list / stream (composite-key table for real pagination) -----

    private String seedRange(int count) {
        String pk = id();
        for (int i = 0; i < count; i++) {
            executor.putItem(RANGE_TABLE, DynamoDBExecutor.asItem("pk", pk, "sk", "sk-" + i, "name", "n" + i));
        }
        return pk;
    }

    @Test
    public void testList() {
        String pk = seedRange(3);
        QueryRequest req = QueryRequest.builder().tableName(RANGE_TABLE).keyConditions(Filters.eq("pk", pk)).build();
        List<Map<String, Object>> result = executor.list(req);
        assertNotNull(result);
        assertEquals(3, result.size());
    }

    @Test
    public void testListWithClassPaginates() {
        String pk = seedRange(3);
        // limit(1) forces DynamoDB to return one item per page; list(...) must aggregate all pages.
        QueryRequest req = QueryRequest.builder().tableName(RANGE_TABLE).keyConditions(Filters.eq("pk", pk)).limit(1).build();
        List<RangeEntity> result = executor.list(req, RangeEntity.class);
        assertNotNull(result);
        assertEquals(3, result.size());
    }

    @Test
    public void testQuery() {
        String pk = seedRange(2);
        QueryRequest req = QueryRequest.builder().tableName(RANGE_TABLE).keyConditions(Filters.eq("pk", pk)).build();
        Dataset result = executor.query(req);
        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    public void testQueryWithClass_MapBranch() {
        String pk = seedRange(1);
        QueryRequest req = QueryRequest.builder().tableName(RANGE_TABLE).keyConditions(Filters.eq("pk", pk)).build();
        Dataset ds = executor.query(req, Clazz.PROPS_MAP);
        assertNotNull(ds);
        assertEquals(1, ds.size());
    }

    @Test
    public void testQueryWithPaginationAggregatesPages() {
        String pk = seedRange(3);
        QueryRequest req = QueryRequest.builder().tableName(RANGE_TABLE).keyConditions(Filters.eq("pk", pk)).limit(1).build();
        Dataset result = executor.query(req, Map.class);
        assertNotNull(result);
        assertEquals(3, result.size());
    }

    @Test
    public void testStream() {
        String pk = seedRange(2);
        QueryRequest req = QueryRequest.builder().tableName(RANGE_TABLE).keyConditions(Filters.eq("pk", pk)).build();
        Stream<Map<String, Object>> stream = executor.stream(req);
        assertNotNull(stream);
        assertEquals(2, stream.count());
    }

    @Test
    public void testStreamWithClass() {
        String pk = seedRange(2);
        QueryRequest req = QueryRequest.builder().tableName(RANGE_TABLE).keyConditions(Filters.eq("pk", pk)).build();
        Stream<RangeEntity> stream = executor.stream(req, RangeEntity.class);
        assertNotNull(stream);
        assertEquals(2, stream.count());
    }

    @Test
    public void testStreamPaginatesAcrossPages() {
        String pk = seedRange(3);
        QueryRequest req = QueryRequest.builder().tableName(RANGE_TABLE).keyConditions(Filters.eq("pk", pk)).limit(1).build();
        Stream<Map<String, Object>> stream = executor.stream(req);
        assertNotNull(stream);
        assertEquals(3, stream.count());
    }

    // ----- scan -----

    /** Writes {@code count} items to TEST_TABLE all sharing a unique {@code grp} marker; returns the marker. */
    private String seedScanGroup(int count) {
        String grp = id();
        for (int i = 0; i < count; i++) {
            executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id(), "grp", grp, "name", "n" + i));
        }
        return grp;
    }

    @Test
    public void testScanByTableNameAndScanFilter() {
        String grp = seedScanGroup(3);
        Stream<Map<String, Object>> stream = executor.scan(TEST_TABLE, Filters.eq("grp", grp));
        assertEquals(3, stream.count());
    }

    @Test
    public void testScanByTableNameAttrsAndScanFilter() {
        String grp = seedScanGroup(2);
        Stream<Map<String, Object>> stream = executor.scan(TEST_TABLE, List.of("id", "grp"), Filters.eq("grp", grp));
        assertEquals(2, stream.count());
    }

    @Test
    public void testScanWithAttrsAndClass() {
        String grp = seedScanGroup(2);
        // Scope via a filtered ScanRequest is not available on this overload; assert our rows are included.
        Stream<TestEntity> stream = executor.scan(TEST_TABLE, List.of("id"), TestEntity.class);
        assertTrue(stream.count() >= 2);
        // Filtered variant for a precise count:
        assertEquals(2, executor.scan(TEST_TABLE, Filters.eq("grp", grp), TestEntity.class).count());
    }

    @Test
    public void testScanWithFilterAndClass() {
        String grp = seedScanGroup(2);
        Stream<TestEntity> stream = executor.scan(TEST_TABLE, Filters.eq("grp", grp), TestEntity.class);
        assertEquals(2, stream.count());
    }

    @Test
    public void testScanWithAttrsFilterAndClass() {
        String grp = seedScanGroup(2);
        Stream<TestEntity> stream = executor.scan(TEST_TABLE, List.of("id", "grp"), Filters.eq("grp", grp), TestEntity.class);
        assertEquals(2, stream.count());
    }

    @Test
    public void testScanWithRequest() {
        String grp = seedScanGroup(2);
        ScanRequest req = ScanRequest.builder().tableName(TEST_TABLE).scanFilter(Filters.eq("grp", grp)).build();
        Stream<Map<String, Object>> stream = executor.scan(req);
        assertEquals(2, stream.count());
    }

    @Test
    public void testScanPaginatesAndSkipsEmptyPages() {
        String grp = seedScanGroup(3);
        // limit(1) + a filter forces many single-item (and filtered-empty) pages; scan must paginate through all.
        ScanRequest req = ScanRequest.builder().tableName(TEST_TABLE).scanFilter(Filters.eq("grp", grp)).limit(1).build();
        Stream<Map<String, Object>> stream = executor.scan(req);
        assertEquals(3, stream.count());
    }

    // ====================================================================================================
    // Mapper round-trips
    // ====================================================================================================

    private TestEntity newEntity(String id, String name) {
        TestEntity e = new TestEntity();
        e.setId(id);
        e.setName(name);
        return e;
    }

    @Test
    public void testMapperPutAndGetItem() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        TestEntity entity = newEntity(id(), "Test");
        assertNotNull(mapper.putItem(entity));

        TestEntity result = mapper.getItem(entity);
        assertNotNull(result);
        assertEquals(entity.getId(), result.getId());
        assertEquals("Test", result.getName());
    }

    @Test
    public void testMapperGetItemWithConsistentRead() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        TestEntity entity = newEntity(id(), "Test");
        mapper.putItem(entity);

        TestEntity result = mapper.getItem(entity, true);
        assertNotNull(result);
        assertEquals(entity.getId(), result.getId());
    }

    @Test
    public void testMapperGetItemWithKey() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        mapper.putItem(newEntity(id, "Test"));

        TestEntity result = mapper.getItem(DynamoDBExecutor.asKey("id", id));
        assertNotNull(result);
        assertEquals(id, result.getId());
    }

    @Test
    public void testMapperGetItemWithRequest() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        mapper.putItem(newEntity(id, "Test"));

        GetItemRequest request = GetItemRequest.builder().tableName(TEST_TABLE).key(DynamoDBExecutor.asKey("id", id)).build();
        TestEntity result = mapper.getItem(request);
        assertNotNull(result);
        assertEquals(id, result.getId());
    }

    @Test
    public void testMapperGetItemWithRequest_NoTableName() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        mapper.putItem(newEntity(id, "Test"));

        // Mapper injects its own table name when the request omits it.
        GetItemRequest request = GetItemRequest.builder().key(DynamoDBExecutor.asKey("id", id)).build();
        TestEntity result = mapper.getItem(request);
        assertNotNull(result);
        assertEquals(id, result.getId());
    }

    @Test
    public void testMapperBatchGetItem() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        TestEntity e1 = newEntity(id(), "a");
        TestEntity e2 = newEntity(id(), "b");
        mapper.putItem(e1);
        mapper.putItem(e2);

        List<TestEntity> result = mapper.batchGetItem(List.of(e1, e2));
        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    public void testMapperBatchGetItemWithReturnConsumedCapacity() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        TestEntity e = newEntity(id(), "a");
        mapper.putItem(e);

        List<TestEntity> result = mapper.batchGetItem(List.of(e), "TOTAL");
        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testMapperBatchGetItemWithRequest() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        mapper.putItem(newEntity(id, "a"));

        BatchGetItemRequest request = BatchGetItemRequest.builder()
                .requestItems(Map.of(TEST_TABLE, KeysAndAttributes.builder().keys(List.of(DynamoDBExecutor.asKey("id", id))).build()))
                .build();
        List<TestEntity> result = mapper.batchGetItem(request);
        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testMapperBatchGetItem_EmptyResponse() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        TestEntity entity = newEntity(id(), "x"); // not stored -> empty result

        List<TestEntity> result = mapper.batchGetItem(List.of(entity));
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @Test
    public void testMapperPutItemWithReturnValues() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        mapper.putItem(newEntity(id, "v1"));

        PutItemResponse result = mapper.putItem(newEntity(id, "v2"), "ALL_OLD");
        assertNotNull(result);
        assertEquals("v1", result.attributes().get("name").s());
    }

    @Test
    public void testMapperPutItemWithRequest() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        PutItemRequest request = PutItemRequest.builder().tableName(TEST_TABLE).item(DynamoDBExecutor.asItem("id", id)).build();
        assertNotNull(mapper.putItem(request));
        assertNotNull(executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)));
    }

    @Test
    public void testMapperPutItemWithRequest_NoTableName() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        PutItemRequest request = PutItemRequest.builder().item(DynamoDBExecutor.asItem("id", id)).build();
        assertNotNull(mapper.putItem(request));
        assertNotNull(executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)));
    }

    @Test
    public void testMapperBatchPutItem() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        BatchWriteItemResponse result = mapper.batchPutItem(List.of(newEntity(id, "x")));
        assertNotNull(result);
        assertNotNull(executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)));
    }

    @Test
    public void testMapperUpdateItem() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        mapper.putItem(newEntity(id, "old"));

        UpdateItemResponse result = mapper.updateItem(newEntity(id, "Updated"));
        assertNotNull(result);
        // The id is the key and must not be part of the attribute updates; name is updated.
        assertEquals("Updated", executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)).get("name"));
    }

    @Test
    public void testMapperUpdateItemWithReturnValues() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        mapper.putItem(newEntity(id, "old"));

        UpdateItemResponse result = mapper.updateItem(newEntity(id, "Updated"), "ALL_NEW");
        assertNotNull(result);
        assertEquals("Updated", result.attributes().get("name").s());
    }

    @Test
    public void testMapperUpdateItemWithRequest() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        mapper.putItem(newEntity(id, "old"));

        UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(TEST_TABLE)
                .key(DynamoDBExecutor.asKey("id", id))
                .attributeUpdates(
                        Map.of("name", AttributeValueUpdate.builder().value(AttributeValue.builder().s("req").build()).action(AttributeAction.PUT).build()))
                .build();
        UpdateItemResponse result = mapper.updateItem(request);
        assertNotNull(result);
        assertEquals("req", executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)).get("name"));
    }

    @Test
    public void testMapperDeleteItem() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        mapper.putItem(newEntity(id, "x"));

        DeleteItemResponse result = mapper.deleteItem(newEntity(id, "x"));
        assertNotNull(result);
        assertNull(executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)));
    }

    @Test
    public void testMapperDeleteItemWithReturnValues() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        mapper.putItem(newEntity(id, "gone"));

        DeleteItemResponse result = mapper.deleteItem(newEntity(id, "gone"), "ALL_OLD");
        assertNotNull(result);
        assertEquals("gone", result.attributes().get("name").s());
    }

    @Test
    public void testMapperDeleteItemWithKey() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        mapper.putItem(newEntity(id, "x"));

        DeleteItemResponse result = mapper.deleteItem(DynamoDBExecutor.asKey("id", id));
        assertNotNull(result);
        assertNull(executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)));
    }

    @Test
    public void testMapperDeleteItemWithRequest() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        mapper.putItem(newEntity(id, "x"));

        DeleteItemRequest request = DeleteItemRequest.builder().tableName(TEST_TABLE).key(DynamoDBExecutor.asKey("id", id)).build();
        DeleteItemResponse result = mapper.deleteItem(request);
        assertNotNull(result);
        assertNull(executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)));
    }

    @Test
    public void testMapperBatchDeleteItem() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        mapper.putItem(newEntity(id, "x"));

        BatchWriteItemResponse result = mapper.batchDeleteItem(List.of(newEntity(id, "x")));
        assertNotNull(result);
        assertNull(executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)));
    }

    @Test
    public void testMapperBatchWriteItemWithRequest() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        BatchWriteItemRequest request = BatchWriteItemRequest.builder()
                .requestItems(Map.of(TEST_TABLE,
                        List.of(WriteRequest.builder().putRequest(PutRequest.builder().item(DynamoDBExecutor.asItem("id", id)).build()).build())))
                .build();
        BatchWriteItemResponse result = mapper.batchWriteItem(request);
        assertNotNull(result);
        assertNotNull(executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)));
    }

    @Test
    public void testMapperList() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        mapper.putItem(newEntity(id, "q"));

        QueryRequest queryRequest = QueryRequest.builder().tableName(TEST_TABLE).keyConditions(Filters.eq("id", id)).build();
        List<TestEntity> result = mapper.list(queryRequest);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(id, result.get(0).getId());
    }

    @Test
    public void testMapperQuery() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        mapper.putItem(newEntity(id, "q"));

        QueryRequest queryRequest = QueryRequest.builder().tableName(TEST_TABLE).keyConditions(Filters.eq("id", id)).build();
        Dataset result = mapper.query(queryRequest);
        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testMapperStream() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        mapper.putItem(newEntity(id, "q"));

        QueryRequest queryRequest = QueryRequest.builder().tableName(TEST_TABLE).keyConditions(Filters.eq("id", id)).build();
        Stream<TestEntity> stream = mapper.stream(queryRequest);
        assertNotNull(stream);
        assertEquals(1, stream.count());
    }

    @Test
    public void testMapperScanWithScanFilter() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String name = "scan-" + id();
        mapper.putItem(newEntity(id(), name));

        Stream<TestEntity> stream = mapper.scan(Filters.eq("name", name));
        assertEquals(1, stream.count());
    }

    @Test
    public void testMapperScanWithAttributesAndFilter() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String name = "scan-" + id();
        mapper.putItem(newEntity(id(), name));

        Stream<TestEntity> stream = mapper.scan(List.of("id", "name"), Filters.eq("name", name));
        assertEquals(1, stream.count());
    }

    @Test
    public void testMapperScanWithRequest() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String name = "scan-" + id();
        mapper.putItem(newEntity(id(), name));

        ScanRequest scanRequest = ScanRequest.builder().tableName(TEST_TABLE).scanFilter(Filters.eq("name", name)).build();
        Stream<TestEntity> stream = mapper.scan(scanRequest);
        assertEquals(1, stream.count());
    }

    // ----- Mapper naming policy applied to real round-trips -----

    @Test
    public void testMapperBatchPutItemAppliesNamingPolicy() {
        DynamoDBExecutor.Mapper<NamingPolicyEntity> mapper = executor.mapper(NamingPolicyEntity.class, TEST_TABLE, NamingPolicy.SNAKE_CASE);

        String id = id();
        NamingPolicyEntity entity = new NamingPolicyEntity();
        entity.setId(id);
        entity.setUserName("Alice");
        mapper.batchPutItem(List.of(entity));

        // Read back the raw item: SNAKE_CASE must have stored "userName" as "user_name".
        Map<String, Object> stored = executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id));
        assertNotNull(stored);
        assertEquals("Alice", stored.get("user_name"));
    }

    @Test
    public void testMapperGetItemAppliesNamingPolicyToKey() {
        DynamoDBExecutor.Mapper<TestEntityWithUserId> mapper = executor.mapper(TestEntityWithUserId.class, USER_TABLE, NamingPolicy.SNAKE_CASE);

        TestEntityWithUserId entity = new TestEntityWithUserId();
        entity.setUserId("u-" + id());
        entity.setUserName("Bob");
        mapper.putItem(entity);

        // getItem(entity) must build the key as "user_id" (snake_case of @Id userId) to find the row.
        TestEntityWithUserId result = mapper.getItem(entity);
        assertNotNull(result);
        assertEquals(entity.getUserId(), result.getUserId());
        assertEquals("Bob", result.getUserName());
    }

    @Test
    public void testMapperBatchDeleteItemAppliesNamingPolicyToKey() {
        DynamoDBExecutor.Mapper<TestEntityWithUserId> mapper = executor.mapper(TestEntityWithUserId.class, USER_TABLE, NamingPolicy.SNAKE_CASE);

        TestEntityWithUserId entity = new TestEntityWithUserId();
        entity.setUserId("u-" + id());
        mapper.putItem(entity);

        mapper.batchDeleteItem(List.of(entity));

        // The delete key must use "user_id"; if it did, the row is gone.
        assertNull(executor.getItem(USER_TABLE, DynamoDBExecutor.asKey("user_id", entity.getUserId())));
    }

    // ---- sliceC 2026-09-27: a String[] property is written as JSON text (S) and must read back element-wise ----
    @Test
    public void testMapperPutGetRoundTripsStringArrayProperty() {
        final DynamoDBExecutor.Mapper<StringArrayEntity> mapper = executor.mapper(StringArrayEntity.class, TEST_TABLE, NamingPolicy.CAMEL_CASE);

        final StringArrayEntity entity = new StringArrayEntity();
        entity.setId(id());
        entity.setTags(new String[] { "red", "green, blue" });
        mapper.putItem(entity);

        final StringArrayEntity read = mapper.getItem(entity);

        assertNotNull(read);
        org.junit.jupiter.api.Assertions.assertArrayEquals(new String[] { "red", "green, blue" }, read.getTags());
    }

    // ---- 2026-10-04 coverageDC ----

    private static final List<String> PAGINATING_ASYNC_METHODS_coverageDC = List.of("list", "list(Entity)", "query", "query(LinkedHashMap)", "query(Entity)",
            "mapper.list", "mapper.query");

    /**
     * Live end-to-end check of the v2 AsyncDynamoDBExecutor early-completion fix against DynamoDB Local. A 6-item partition read with
     * Limit 1 is a 7-request pagination (the 6th page still carries a LastEvaluatedKey). For every auto-paginating entry point
     * (executor list/query with Map, LinkedHashMap and entity rows; Mapper list/query) the real page-2 response is held back while the
     * returned future is cancelled or timed out with orTimeout(...); once page 2 is released, no page-3 request may follow (HEAD kept
     * requesting every remaining page). Without early completion every entry point still returns all 6 items over 7 real requests.
     */
    @Test
    public void testAsyncListAndQueryStopPaginatingWhenReturnedFutureCompletesEarly_coverageDC() throws Exception {
        final String pk = seedRange(6);
        final DynamoDbAsyncClient realAsyncClient = DynamoDbAsyncClient.builder()
                .endpointOverride(URI.create(ENDPOINT))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
                .build();

        try {
            final QueryRequest request = QueryRequest.builder().tableName(RANGE_TABLE).keyConditions(Filters.eq("pk", pk)).limit(1).build();
            final List<String> wrong = new ArrayList<>();

            for (final String method : PAGINATING_ASYNC_METHODS_coverageDC) {
                for (final String mode : List.of("cancel", "orTimeout", "notCompletedEarly")) {
                    final String where = method + "/" + mode;
                    final AtomicInteger queryRequests = new AtomicInteger();
                    final java.util.concurrent.atomic.AtomicReference<ObservedPageFuture<software.amazon.awssdk.services.dynamodb.model.QueryResponse>> firstPage = new java.util.concurrent.atomic.AtomicReference<>();
                    final CompletableFuture<Void> page2Gate = mode.equals("notCompletedEarly") ? CompletableFuture.completedFuture(null)
                            : new CompletableFuture<>();
                    final CompletableFuture<CompletableFuture<?>> page2Response = new CompletableFuture<>();
                    final AsyncDynamoDBExecutor asyncExecutor = new AsyncDynamoDBExecutor(
                            gatedQueryClient_coverageDC(realAsyncClient, queryRequests, page2Gate, page2Response, firstPage));
                    final CompletableFuture<?> result = invokePaginatingAsyncQuery_coverageDC(asyncExecutor, method, request);

                    if (mode.equals("notCompletedEarly")) {
                        final Object value = result.get(30, TimeUnit.SECONDS);
                        assertEquals(6, value instanceof Dataset ? ((Dataset) value).size() : ((List<?>) value).size(), where);
                        assertEquals(7, queryRequests.get(), where);
                        continue;
                    }

                    // Page 2 has been requested and its real response (with a LastEvaluatedKey) has arrived, but is held back.
                    page2Response.get(30, TimeUnit.SECONDS).get(30, TimeUnit.SECONDS);

                    if (mode.equals("cancel")) {
                        assertTrue(result.cancel(true), where);
                    } else {
                        final ExecutionException e = assertThrows(ExecutionException.class,
                                () -> result.orTimeout(1, TimeUnit.MILLISECONDS).get(30, TimeUnit.SECONDS), where);
                        assertTrue(e.getCause() instanceof java.util.concurrent.TimeoutException, where + ": " + e.getCause());
                    }

                    page2Gate.complete(null); // hand the real page-2 response to the page chain
                    firstPage.get().awaitPageChain(30, TimeUnit.SECONDS);

                    if (queryRequests.get() != 2) {
                        wrong.add(where + ": " + queryRequests.get() + " query requests, expected 2");
                    }
                }
            }

            assertTrue(wrong.isEmpty(), wrong.toString());
        } finally {
            realAsyncClient.close();

            for (int i = 0; i < 6; i++) {
                executor.deleteItem(RANGE_TABLE, DynamoDBExecutor.asKey("pk", pk, "sk", "sk-" + i));
            }
        }
    }

    /**
     * Delegates every call to the real async client; counts query requests, publishes the response future of the 2nd one and holds
     * that response back until {@code page2Gate} completes.
     */
    private static DynamoDbAsyncClient gatedQueryClient_coverageDC(final DynamoDbAsyncClient delegate, final AtomicInteger queryRequests,
            final CompletableFuture<Void> page2Gate, final CompletableFuture<CompletableFuture<?>> page2Response,
            final java.util.concurrent.atomic.AtomicReference<ObservedPageFuture<software.amazon.awssdk.services.dynamodb.model.QueryResponse>> firstPage) {
        return (DynamoDbAsyncClient) java.lang.reflect.Proxy.newProxyInstance(DynamoDbAsyncClient.class.getClassLoader(),
                new Class<?>[] { DynamoDbAsyncClient.class }, (proxy, method, args) -> {
                    if ("query".equals(method.getName()) && args != null && args.length == 1 && args[0] instanceof QueryRequest) {
                        final CompletableFuture<software.amazon.awssdk.services.dynamodb.model.QueryResponse> response = delegate.query((QueryRequest) args[0]);

                        final int pageNumber = queryRequests.incrementAndGet();
                        final ObservedPageFuture<software.amazon.awssdk.services.dynamodb.model.QueryResponse> delivered = new ObservedPageFuture<>();
                        if (pageNumber == 1) {
                            firstPage.set(delivered);
                        }
                        final CompletableFuture<software.amazon.awssdk.services.dynamodb.model.QueryResponse> released;
                        if (pageNumber == 2) {
                            page2Response.complete(response);
                            released = response.thenCombine(page2Gate, (page, ignored) -> page);
                        } else {
                            released = response;
                        }
                        released.whenComplete((page, failure) -> {
                            if (failure == null) {
                                delivered.complete(page);
                            } else {
                                delivered.completeExceptionally(failure);
                            }
                        });
                        return delivered;
                    }

                    try {
                        return method.invoke(delegate, args);
                    } catch (final java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private static CompletableFuture<?> invokePaginatingAsyncQuery_coverageDC(final AsyncDynamoDBExecutor asyncExecutor, final String method,
            final QueryRequest request) {
        return switch (method) {
            case "list" -> asyncExecutor.list(request);
            case "list(Entity)" -> asyncExecutor.list(request, RangeEntity.class);
            case "query" -> asyncExecutor.query(request);
            case "query(LinkedHashMap)" -> asyncExecutor.query(request, LinkedHashMap.class);
            case "query(Entity)" -> asyncExecutor.query(request, RangeEntity.class);
            case "mapper.list" -> asyncExecutor.mapper(RangeKeyEntity_coverageDC.class, RANGE_TABLE, NamingPolicy.CAMEL_CASE).list(request);
            case "mapper.query" -> asyncExecutor.mapper(RangeKeyEntity_coverageDC.class, RANGE_TABLE, NamingPolicy.CAMEL_CASE).query(request);
            default -> throw new IllegalArgumentException(method);
        };
    }

    /** RangeTable entity with its partition and sort keys declared as ID properties (required by a Mapper). */
    public static class RangeKeyEntity_coverageDC {
        @com.landawn.abacus.annotation.Id
        private String pk;
        @com.landawn.abacus.annotation.Id
        private String sk;
        private String name;

        public String getPk() {
            return pk;
        }

        public void setPk(final String pk) {
            this.pk = pk;
        }

        public String getSk() {
            return sk;
        }

        public void setSk(final String sk) {
            this.sk = sk;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    // ====================================================================================================
    // Entities
    // ====================================================================================================

    public static class StringArrayEntity {
        @com.landawn.abacus.annotation.Id
        private String id;
        private String[] tags;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String[] getTags() {
            return tags;
        }

        public void setTags(String[] tags) {
            this.tags = tags;
        }
    }

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
    public static class TestEntity {
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

    @com.landawn.abacus.annotation.Table(name = "RangeTable")
    public static class RangeEntity {
        private String pk;
        private String sk;
        private String name;

        public String getPk() {
            return pk;
        }

        public void setPk(String pk) {
            this.pk = pk;
        }

        public String getSk() {
            return sk;
        }

        public void setSk(String sk) {
            this.sk = sk;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class TestEntityWithUserId {
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

    public static class NamingPolicyEntity {
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

    public static class InvalidEntity {
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
