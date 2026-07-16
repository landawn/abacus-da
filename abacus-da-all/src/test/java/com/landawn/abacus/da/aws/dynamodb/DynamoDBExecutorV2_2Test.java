package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.da.aws.AnyUtil;
import com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor;
import com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.ConditionBuilder;
import com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.Filters;
import com.landawn.abacus.util.Clazz;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.stream.Stream;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;
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
 * No-mock counterpart of {@code DynamoDBExecutorV2Test}: it exercises {@link DynamoDBExecutor} against a
 * <b>real</b> DynamoDB Local instance instead of a Mockito {@code DynamoDbClient}.
 *
 * <p>Two kinds of tests live here:</p>
 * <ul>
 * <li><b>Pure-logic tests</b> for the static converters ({@code toAttributeValue}/{@code asKey}/{@code toItem}/
 *     {@code toMap}/{@code toEntity}/{@code toList}/{@code extractData}), the {@code Filters}/
 *     {@code ConditionBuilder} DSL, and argument validation. These never touched the mock and are kept
 *     as-is; they run regardless of whether DynamoDB Local is up.</li>
 * <li><b>Round-trip tests</b> that previously stubbed {@code DynamoDbClient}. These now create real tables,
 *     write real items, and read them back, asserting actual DynamoDB behavior (stored attribute names,
 *     {@code ALL_OLD}/{@code ALL_NEW} return values, real query/scan pagination, etc.).</li>
 * </ul>
 *
 * <h2>Running locally</h2>
 * <pre>{@code
 * docker run --name dynamodb-local -p 8000:8000 -d amazon/dynamodb-local
 * mvn -pl abacus-da-all test -Dtest=DynamoDBExecutorV2_2Test
 * }</pre>
 *
 * <p>DynamoDB Local accepts any credentials and ignores the region; this test uses placeholder values and
 * an endpoint override of {@code http://localhost:8000}. If the instance is unreachable, the round-trip
 * tests are <b>skipped</b> (via JUnit {@link Assumptions}) rather than failed; the pure-logic tests still
 * run. The class is intentionally not named {@code *Test}-only-friendly differently &mdash; it follows the
 * standard {@code *Test} suffix, so it is picked up by {@code mvn test}; when DynamoDB Local is down its
 * DB-backed tests simply skip.</p>
 */
public class DynamoDBExecutorV2_2Test extends TestBase {

    private static final String ENDPOINT = "http://localhost:8000";

    private static final String TEST_TABLE = "TestTable"; // hash key: id (matches @Table on TestEntity)
    private static final String USER_TABLE = "UserTable"; // hash key: user_id (for SNAKE_CASE @Id userId)
    private static final String RANGE_TABLE = "RangeTable"; // hash: pk, range: sk (for real query pagination)

    private static DynamoDbClient client;
    private static DynamoDBExecutor executor;

    private static boolean available;
    private static String unavailableReason;

    @BeforeAll
    public static void setUpClass() {
        // The client builds lazily (no network yet), so the executor is always available for the pure-logic
        // and Mapper-construction tests even when DynamoDB Local is down.
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
            available = true;
        } catch (final Exception e) {
            available = false;
            unavailableReason = e.toString();
            System.err.println("[DynamoDBExecutorV2_2Test] DynamoDB Local unavailable at " + ENDPOINT + " - DB-backed tests will be skipped: " + e);
        }
    }

    @AfterAll
    public static void tearDownClass() {
        if (client != null) {
            if (available) {
                for (final String table : List.of(TEST_TABLE, USER_TABLE, RANGE_TABLE)) {
                    try {
                        client.deleteTable(DeleteTableRequest.builder().tableName(table).build());
                    } catch (final Exception ignore) {
                        // best-effort cleanup
                    }
                }
            }
            client.close();
        }
    }

    private static void assumeAvailable() {
        Assumptions.assumeTrue(available, "DynamoDB Local not reachable at " + ENDPOINT + " (" + unavailableReason + ")");
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
            return;
        }
        client.createTable(CreateTableRequest.builder()
                .tableName(name)
                .keySchema(KeySchemaElement.builder().attributeName(hashKey).keyType(KeyType.HASH).build())
                .attributeDefinitions(AttributeDefinition.builder().attributeName(hashKey).attributeType(ScalarAttributeType.S).build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build());
        client.waiter().waitUntilTableExists(DescribeTableRequest.builder().tableName(name).build());
    }

    private static void createCompositeTable(final String name, final String hashKey, final String rangeKey) {
        if (tableExists(name)) {
            return;
        }
        client.createTable(CreateTableRequest.builder()
                .tableName(name)
                .keySchema(KeySchemaElement.builder().attributeName(hashKey).keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName(rangeKey).keyType(KeyType.RANGE).build())
                .attributeDefinitions(AttributeDefinition.builder().attributeName(hashKey).attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName(rangeKey).attributeType(ScalarAttributeType.S).build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build());
        client.waiter().waitUntilTableExists(DescribeTableRequest.builder().tableName(name).build());
    }

    private static String id() {
        return UUID.randomUUID().toString();
    }

    // ====================================================================================================
    // Pure-logic tests (no DynamoDB connection required) - carried over unchanged from the mock suite
    // ====================================================================================================

    @Test
    public void testDynamoDBClient() {
        DynamoDbClient c = executor.dynamoDBClient();
        assertNotNull(c);
        assertEquals(client, c);
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
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("id", "123", "name", "test", "age", 25));
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
    public void testAsKey_VarargsMixed() {
        Map<String, AttributeValue> result = DynamoDBExecutor.asKey("k1", "v1", "k2", 42);
        assertEquals(2, result.size());
        assertEquals("v1", result.get("k1").s());
        assertEquals("42", result.get("k2").n());
    }

    @Test
    public void testAsItem() {
        Map<String, AttributeValue> result = DynamoDBExecutor.asItem("id", "123");
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("123", result.get("id").s());
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
    public void testAsUpdateItem() {
        Map<String, AttributeValueUpdate> result = DynamoDBExecutor.asUpdateItem("name", "test");
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("test", result.get("name").value().s());
    }

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
    public void testToItem_FromMapSnakeCase() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("firstName", "John");
        Map<String, AttributeValue> result = DynamoDBExecutor.toItem(map, NamingPolicy.SNAKE_CASE);
        assertEquals("John", result.get("first_name").s());
    }

    @Test
    public void testToItem_UnsupportedTypeThrows() {
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toItem("notSupported", NamingPolicy.CAMEL_CASE));
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
    public void testToUpdateItem_FromMapSnakeCase() {
        Map<String, Object> map = new LinkedHashMap<>();
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
        Map<String, Object> result = AnyUtil.asProps(propNameAndValues);

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

    @Test
    public void testToEntityWithGetItemResponse() {
        software.amazon.awssdk.services.dynamodb.model.GetItemResponse response = software.amazon.awssdk.services.dynamodb.model.GetItemResponse.builder()
                .item(Map.of("id", AttributeValue.builder().s("123").build(), "name", AttributeValue.builder().s("test").build()))
                .build();

        TestEntity result = DynamoDBExecutor.toEntity(response, TestEntity.class);

        assertNotNull(result);
        assertEquals("123", result.getId());
        assertEquals("test", result.getName());
    }

    @Test
    public void testToEntityWithNullResponse() {
        TestEntity result = DynamoDBExecutor.toEntity((software.amazon.awssdk.services.dynamodb.model.GetItemResponse) null, TestEntity.class);
        assertNull(result);
    }

    @Test
    public void testToEntityWithEmptyResponse() {
        software.amazon.awssdk.services.dynamodb.model.GetItemResponse response = software.amazon.awssdk.services.dynamodb.model.GetItemResponse.builder()
                .build();
        TestEntity result = DynamoDBExecutor.toEntity(response, TestEntity.class);
        assertNull(result);
    }

    @Test
    public void testToEntity_NullItemReturnsNull() {
        TestEntity result = DynamoDBExecutor.toEntity((Map<String, AttributeValue>) null, TestEntity.class);
        assertNull(result);
    }

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

    @Test
    public void testToList() {
        software.amazon.awssdk.services.dynamodb.model.QueryResponse queryResponse = software.amazon.awssdk.services.dynamodb.model.QueryResponse.builder()
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
        software.amazon.awssdk.services.dynamodb.model.QueryResponse queryResponse = software.amazon.awssdk.services.dynamodb.model.QueryResponse.builder()
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
    public void testToListFromScanResponse() {
        software.amazon.awssdk.services.dynamodb.model.ScanResponse scanResponse = software.amazon.awssdk.services.dynamodb.model.ScanResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("a").build()), Map.of("id", AttributeValue.builder().s("b").build())))
                .build();

        List<TestEntity> result = DynamoDBExecutor.toList(scanResponse, TestEntity.class);
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("a", result.get(0).getId());
    }

    @Test
    public void testToListFromScanResponseWithOffsetCount() {
        software.amazon.awssdk.services.dynamodb.model.ScanResponse scanResponse = software.amazon.awssdk.services.dynamodb.model.ScanResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("a").build()), Map.of("id", AttributeValue.builder().s("b").build()),
                        Map.of("id", AttributeValue.builder().s("c").build())))
                .build();

        List<TestEntity> result = DynamoDBExecutor.toList(scanResponse, 1, 1, TestEntity.class);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("b", result.get(0).getId());
    }

    @Test
    public void testToList_QueryResponseOffsetCount() {
        software.amazon.awssdk.services.dynamodb.model.QueryResponse qr = software.amazon.awssdk.services.dynamodb.model.QueryResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("1").build()), Map.of("id", AttributeValue.builder().s("2").build()),
                        Map.of("id", AttributeValue.builder().s("3").build())))
                .build();
        List<TestEntity> result = DynamoDBExecutor.toList(qr, 1, 2, TestEntity.class);
        assertEquals(2, result.size());
        assertEquals("2", result.get(0).getId());
    }

    @Test
    public void testToList_ScanResponseOffsetCount() {
        software.amazon.awssdk.services.dynamodb.model.ScanResponse sr = software.amazon.awssdk.services.dynamodb.model.ScanResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("a").build()), Map.of("id", AttributeValue.builder().s("b").build()),
                        Map.of("id", AttributeValue.builder().s("c").build())))
                .build();
        List<TestEntity> result = DynamoDBExecutor.toList(sr, 1, 1, TestEntity.class);
        assertEquals(1, result.size());
        assertEquals("b", result.get(0).getId());
    }

    @Test
    public void testToList_AsObjectArrayClass() {
        Map<String, AttributeValue> r = new LinkedHashMap<>();
        r.put("a", AttributeValue.builder().s("x").build());
        r.put("b", AttributeValue.builder().s("y").build());
        software.amazon.awssdk.services.dynamodb.model.QueryResponse qr = software.amazon.awssdk.services.dynamodb.model.QueryResponse.builder()
                .items(List.of(r))
                .build();

        List<Object[]> result = DynamoDBExecutor.toList(qr, Object[].class);
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).length);
    }

    @Test
    public void testToList_AsMapClass() {
        Map<String, AttributeValue> r = new LinkedHashMap<>();
        r.put("a", AttributeValue.builder().s("x").build());
        software.amazon.awssdk.services.dynamodb.model.QueryResponse qr = software.amazon.awssdk.services.dynamodb.model.QueryResponse.builder()
                .items(List.of(r))
                .build();

        @SuppressWarnings({ "unchecked", "rawtypes" })
        List<Map> result = DynamoDBExecutor.toList(qr, (Class) Map.class);
        assertEquals(1, result.size());
        assertEquals("x", result.get(0).get("a"));
    }

    @Test
    public void testToList_AsCollectionClass() {
        Map<String, AttributeValue> r = new LinkedHashMap<>();
        r.put("a", AttributeValue.builder().s("x").build());
        r.put("b", AttributeValue.builder().s("y").build());
        software.amazon.awssdk.services.dynamodb.model.QueryResponse qr = software.amazon.awssdk.services.dynamodb.model.QueryResponse.builder()
                .items(List.of(r))
                .build();

        @SuppressWarnings({ "unchecked", "rawtypes" })
        List<List> result = DynamoDBExecutor.toList(qr, (Class) List.class);
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).size());
    }

    @Test
    public void testToList_AsSingleValueClass() {
        Map<String, AttributeValue> r = new LinkedHashMap<>();
        r.put("v", AttributeValue.builder().s("hello").build());
        software.amazon.awssdk.services.dynamodb.model.QueryResponse qr = software.amazon.awssdk.services.dynamodb.model.QueryResponse.builder()
                .items(List.of(r))
                .build();

        List<String> result = DynamoDBExecutor.toList(qr, String.class);
        assertEquals(1, result.size());
        assertEquals("hello", result.get(0));
    }

    @Test
    public void testToList_SingleValueMultiColumnThrows() {
        Map<String, AttributeValue> r = new LinkedHashMap<>();
        r.put("a", AttributeValue.builder().s("1").build());
        r.put("b", AttributeValue.builder().s("2").build());
        software.amazon.awssdk.services.dynamodb.model.QueryResponse qr = software.amazon.awssdk.services.dynamodb.model.QueryResponse.builder()
                .items(List.of(r))
                .build();

        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toList(qr, String.class));
    }

    @Test
    public void testExtractData() {
        software.amazon.awssdk.services.dynamodb.model.QueryResponse queryResponse = software.amazon.awssdk.services.dynamodb.model.QueryResponse.builder()
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
    public void testExtractDataFromScanResponse() {
        software.amazon.awssdk.services.dynamodb.model.ScanResponse scanResponse = software.amazon.awssdk.services.dynamodb.model.ScanResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("1").build()), Map.of("id", AttributeValue.builder().s("2").build())))
                .build();

        Dataset result = DynamoDBExecutor.extractData(scanResponse);
        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.containsColumn("id"));
    }

    @Test
    public void testExtractDataFromScanResponseWithOffsetCount() {
        software.amazon.awssdk.services.dynamodb.model.ScanResponse scanResponse = software.amazon.awssdk.services.dynamodb.model.ScanResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("1").build()), Map.of("id", AttributeValue.builder().s("2").build()),
                        Map.of("id", AttributeValue.builder().s("3").build())))
                .build();

        Dataset result = DynamoDBExecutor.extractData(scanResponse, 1, 1);
        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testExtractDataFromQueryResponseWithOffsetCount() {
        software.amazon.awssdk.services.dynamodb.model.QueryResponse queryResponse = software.amazon.awssdk.services.dynamodb.model.QueryResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("1").build()), Map.of("id", AttributeValue.builder().s("2").build()),
                        Map.of("id", AttributeValue.builder().s("3").build())))
                .build();

        Dataset result = DynamoDBExecutor.extractData(queryResponse, 0, 2);
        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    public void testExtractData_EmptyItems() {
        Dataset ds = DynamoDBExecutor.extractData(software.amazon.awssdk.services.dynamodb.model.QueryResponse.builder().items(new ArrayList<>()).build());
        assertNotNull(ds);
        assertEquals(0, ds.size());
    }

    @Test
    public void testExtractData_NegativeOffsetThrows() {
        software.amazon.awssdk.services.dynamodb.model.QueryResponse qr = software.amazon.awssdk.services.dynamodb.model.QueryResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.builder().s("1").build())))
                .build();
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.extractData(qr, -1, 1));
    }

    // ----- Filters DSL (pure) -----

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
    public void testFiltersBt_AttributeValueContents() {
        Map<String, Condition> result = Filters.bt("age", 18, 65);
        assertEquals("18", result.get("age").attributeValueList().get(0).n());
        assertEquals("65", result.get("age").attributeValueList().get(1).n());
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
    public void testFiltersBuilder() {
        ConditionBuilder builder = Filters.builder();
        assertNotNull(builder);
    }

    // ----- ConditionBuilder (pure) -----

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
    public void testConditionBuilderBuild_NullifiesInternalState() {
        // After build(), the internal map is nulled; calling build() again returns null.
        ConditionBuilder b = Filters.builder().eq("x", 1);
        Map<String, Condition> first = b.build();
        assertNotNull(first);
        assertEquals(1, first.size());
        assertNull(b.build());
    }

    // ----- Construction / Mapper validation (no client calls) -----

    @Test
    public void testConstructor_NullClientThrows() {
        assertThrows(IllegalArgumentException.class, () -> new DynamoDBExecutor(null));
    }

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
    public void testMapper_NoTableAnnotationThrows() {
        assertThrows(IllegalArgumentException.class, () -> executor.mapper(V2NoTableEntity.class));
    }

    @Test
    public void testMapperWithWrongTableName() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        GetItemRequest request = GetItemRequest.builder().tableName("WrongTable").key(Map.of("id", AttributeValue.builder().s("123").build())).build();
        assertThrows(IllegalArgumentException.class, () -> mapper.getItem(request));
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
    public void testMapperBatchWriteItemWithRequest_WrongTableName() {
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        BatchWriteItemRequest request = BatchWriteItemRequest.builder()
                .requestItems(Map.of("WrongTable", List.of(
                        WriteRequest.builder().putRequest(PutRequest.builder().item(Map.of("id", AttributeValue.builder().s("1").build())).build()).build())))
                .build();
        assertThrows(IllegalArgumentException.class, () -> mapper.batchWriteItem(request));
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
        assumeAvailable();
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id, "name", "test"));

        Map<String, Object> result = executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id));

        assertNotNull(result);
        assertEquals(id, result.get("id"));
        assertEquals("test", result.get("name"));
    }

    @Test
    public void testGetItemWithConsistentRead() {
        assumeAvailable();
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id));

        Map<String, Object> result = executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id), true);

        assertNotNull(result);
        assertEquals(id, result.get("id"));
    }

    @Test
    public void testGetItem_StringMapWithConsistentRead() {
        assumeAvailable();
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id));

        Map<String, Object> result = executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id), Boolean.TRUE);
        assertNotNull(result);
        assertEquals(id, result.get("id"));
    }

    @Test
    public void testGetItemWithTargetClass() {
        assumeAvailable();
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id, "name", "test"));

        TestEntity result = executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id), TestEntity.class);

        assertNotNull(result);
        assertEquals(id, result.getId());
        assertEquals("test", result.getName());
    }

    @Test
    public void testGetItemWithConsistentReadAndClass() {
        assumeAvailable();
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id));

        TestEntity result = executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id), true, TestEntity.class);
        assertNotNull(result);
        assertEquals(id, result.getId());
    }

    @Test
    public void testGetItemWithGetItemRequest() {
        assumeAvailable();
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
        assumeAvailable();
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
        assumeAvailable();
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
        assumeAvailable();
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id, "a", "x", "b", "y"));

        GetItemRequest req = GetItemRequest.builder().tableName(TEST_TABLE).key(DynamoDBExecutor.asKey("id", id)).projectionExpression("a, b").build();
        List<?> result = executor.<List<?>> getItem(req, (Class<List<?>>) (Class<?>) List.class);
        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    public void testGetItemRequest_ReadRowAsMapClass() {
        assumeAvailable();
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id, "a", "x"));

        GetItemRequest req = GetItemRequest.builder().tableName(TEST_TABLE).key(DynamoDBExecutor.asKey("id", id)).projectionExpression("a").build();
        Map<?, ?> result = executor.<Map<?, ?>> getItem(req, (Class<Map<?, ?>>) (Class<?>) Map.class);
        assertNotNull(result);
        assertEquals("x", result.get("a"));
    }

    @Test
    public void testGetItemRequest_ReadRowAsSingleValueClass() {
        assumeAvailable();
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id, "v", "hello"));

        GetItemRequest req = GetItemRequest.builder().tableName(TEST_TABLE).key(DynamoDBExecutor.asKey("id", id)).projectionExpression("v").build();
        String result = executor.getItem(req, String.class);
        assertEquals("hello", result);
    }

    @Test
    public void testGetItemRequest_ReadRowAsSingleValueClass_MultiColumnThrows() {
        assumeAvailable();
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id, "a", "1", "b", "2"));

        GetItemRequest req = GetItemRequest.builder().tableName(TEST_TABLE).key(DynamoDBExecutor.asKey("id", id)).projectionExpression("a, b").build();
        assertThrows(IllegalArgumentException.class, () -> executor.getItem(req, String.class));
    }

    @Test
    public void testGetItemRequest_ReadRowEmptyResponse_NullForBean() {
        assumeAvailable();
        GetItemRequest req = GetItemRequest.builder().tableName(TEST_TABLE).key(DynamoDBExecutor.asKey("id", id())).build();
        TestEntity result = executor.getItem(req, TestEntity.class);
        assertNull(result);
    }

    @Test
    public void testGetItemRequest_ReadRowEmptyResponse_DefaultForPrimitive() {
        assumeAvailable();
        GetItemRequest req = GetItemRequest.builder().tableName(TEST_TABLE).key(DynamoDBExecutor.asKey("id", id())).build();
        Integer result = executor.getItem(req, int.class);
        assertEquals(0, result);
    }

    @Test
    public void testToValue_TargetClassConversion() {
        assumeAvailable();
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id, "num", 42));

        GetItemRequest req = GetItemRequest.builder().tableName(TEST_TABLE).key(DynamoDBExecutor.asKey("id", id)).projectionExpression("num").build();
        Integer result = executor.getItem(req, Integer.class);
        assertEquals(42, result);
    }

    @Test
    public void testBatchGetItem() {
        assumeAvailable();
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
        assumeAvailable();
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id));

        Map<String, KeysAndAttributes> requestItems = Map.of(TEST_TABLE, KeysAndAttributes.builder().keys(List.of(DynamoDBExecutor.asKey("id", id))).build());

        Map<String, List<Map<String, Object>>> result = executor.batchGetItem(requestItems, "TOTAL");
        assertNotNull(result);
        assertEquals(1, result.get(TEST_TABLE).size());
    }

    @Test
    public void testBatchGetItemWithRequest() {
        assumeAvailable();
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
        assumeAvailable();
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
        assumeAvailable();
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
        assumeAvailable();
        // Request a key that does not exist -> no responses for the table.
        Map<String, KeysAndAttributes> req = Map.of(TEST_TABLE, KeysAndAttributes.builder().keys(List.of(DynamoDBExecutor.asKey("id", id()))).build());

        Map<String, List<TestEntity>> result = executor.batchGetItem(req, TestEntity.class);
        assertNotNull(result);
        // No matching items -> empty list (or absent table key).
        assertTrue(result.getOrDefault(TEST_TABLE, List.of()).isEmpty());
    }

    @Test
    public void testPutItem() {
        assumeAvailable();
        String id = id();
        PutItemResponse result = executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id, "name", "test"));
        assertNotNull(result);

        // Verify it really landed.
        assertEquals("test", executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)).get("name"));
    }

    @Test
    public void testPutItemWithRequest() {
        assumeAvailable();
        String id = id();
        PutItemRequest req = PutItemRequest.builder().tableName(TEST_TABLE).item(DynamoDBExecutor.asItem("id", id)).build();
        PutItemResponse result = executor.putItem(req);
        assertNotNull(result);
        assertNotNull(executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)));
    }

    @Test
    public void testPutItemWithReturnValues() {
        assumeAvailable();
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
        assumeAvailable();
        String id = id();
        Map<String, List<WriteRequest>> requestItems = Map.of(TEST_TABLE,
                List.of(WriteRequest.builder().putRequest(PutRequest.builder().item(DynamoDBExecutor.asItem("id", id, "name", "bw")).build()).build()));

        BatchWriteItemResponse result = executor.batchWriteItem(requestItems);
        assertNotNull(result);
        assertEquals("bw", executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)).get("name"));
    }

    @Test
    public void testUpdateItem() {
        assumeAvailable();
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
        assumeAvailable();
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
        assumeAvailable();
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
        assumeAvailable();
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id));
        UpdateItemResponse result = executor.updateItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id),
                Map.of("name", AttributeValueUpdate.builder().value(AttributeValue.builder().s("x").build()).build()));
        assertNotNull(result);
    }

    @Test
    public void testDeleteItem() {
        assumeAvailable();
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id));

        DeleteItemResponse result = executor.deleteItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id));
        assertNotNull(result);
        assertNull(executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)));
    }

    @Test
    public void testDeleteItemWithRequest() {
        assumeAvailable();
        String id = id();
        executor.putItem(TEST_TABLE, DynamoDBExecutor.asItem("id", id));

        DeleteItemRequest req = DeleteItemRequest.builder().tableName(TEST_TABLE).key(DynamoDBExecutor.asKey("id", id)).build();
        DeleteItemResponse result = executor.deleteItem(req);
        assertNotNull(result);
        assertNull(executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)));
    }

    @Test
    public void testDeleteItemWithReturnValues() {
        assumeAvailable();
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
        assumeAvailable();
        String pk = seedRange(3);
        QueryRequest req = QueryRequest.builder().tableName(RANGE_TABLE).keyConditions(Filters.eq("pk", pk)).build();
        List<Map<String, Object>> result = executor.list(req);
        assertNotNull(result);
        assertEquals(3, result.size());
    }

    @Test
    public void testListWithClassPaginates() {
        assumeAvailable();
        String pk = seedRange(3);
        // limit(1) forces DynamoDB to return one item per page; list(...) must aggregate all pages.
        QueryRequest req = QueryRequest.builder().tableName(RANGE_TABLE).keyConditions(Filters.eq("pk", pk)).limit(1).build();
        List<RangeEntity> result = executor.list(req, RangeEntity.class);
        assertNotNull(result);
        assertEquals(3, result.size());
    }

    @Test
    public void testQuery() {
        assumeAvailable();
        String pk = seedRange(2);
        QueryRequest req = QueryRequest.builder().tableName(RANGE_TABLE).keyConditions(Filters.eq("pk", pk)).build();
        Dataset result = executor.query(req);
        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    public void testQueryWithClass_MapBranch() {
        assumeAvailable();
        String pk = seedRange(1);
        QueryRequest req = QueryRequest.builder().tableName(RANGE_TABLE).keyConditions(Filters.eq("pk", pk)).build();
        Dataset ds = executor.query(req, Clazz.PROPS_MAP);
        assertNotNull(ds);
        assertEquals(1, ds.size());
    }

    @Test
    public void testQueryWithPaginationAggregatesPages() {
        assumeAvailable();
        String pk = seedRange(3);
        QueryRequest req = QueryRequest.builder().tableName(RANGE_TABLE).keyConditions(Filters.eq("pk", pk)).limit(1).build();
        Dataset result = executor.query(req, Map.class);
        assertNotNull(result);
        assertEquals(3, result.size());
    }

    @Test
    public void testStream() {
        assumeAvailable();
        String pk = seedRange(2);
        QueryRequest req = QueryRequest.builder().tableName(RANGE_TABLE).keyConditions(Filters.eq("pk", pk)).build();
        Stream<Map<String, Object>> stream = executor.stream(req);
        assertNotNull(stream);
        assertEquals(2, stream.count());
    }

    @Test
    public void testStreamWithClass() {
        assumeAvailable();
        String pk = seedRange(2);
        QueryRequest req = QueryRequest.builder().tableName(RANGE_TABLE).keyConditions(Filters.eq("pk", pk)).build();
        Stream<RangeEntity> stream = executor.stream(req, RangeEntity.class);
        assertNotNull(stream);
        assertEquals(2, stream.count());
    }

    @Test
    public void testStreamPaginatesAcrossPages() {
        assumeAvailable();
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
        assumeAvailable();
        String grp = seedScanGroup(3);
        Stream<Map<String, Object>> stream = executor.scan(TEST_TABLE, Filters.eq("grp", grp));
        assertEquals(3, stream.count());
    }

    @Test
    public void testScanByTableNameAttrsAndScanFilter() {
        assumeAvailable();
        String grp = seedScanGroup(2);
        Stream<Map<String, Object>> stream = executor.scan(TEST_TABLE, List.of("id", "grp"), Filters.eq("grp", grp));
        assertEquals(2, stream.count());
    }

    @Test
    public void testScanWithAttrsAndClass() {
        assumeAvailable();
        String grp = seedScanGroup(2);
        // Scope via a filtered ScanRequest is not available on this overload; assert our rows are included.
        Stream<TestEntity> stream = executor.scan(TEST_TABLE, List.of("id"), TestEntity.class);
        assertTrue(stream.count() >= 2);
        // Filtered variant for a precise count:
        assertEquals(2, executor.scan(TEST_TABLE, Filters.eq("grp", grp), TestEntity.class).count());
    }

    @Test
    public void testScanWithFilterAndClass() {
        assumeAvailable();
        String grp = seedScanGroup(2);
        Stream<TestEntity> stream = executor.scan(TEST_TABLE, Filters.eq("grp", grp), TestEntity.class);
        assertEquals(2, stream.count());
    }

    @Test
    public void testScanWithAttrsFilterAndClass() {
        assumeAvailable();
        String grp = seedScanGroup(2);
        Stream<TestEntity> stream = executor.scan(TEST_TABLE, List.of("id", "grp"), Filters.eq("grp", grp), TestEntity.class);
        assertEquals(2, stream.count());
    }

    @Test
    public void testScanWithRequest() {
        assumeAvailable();
        String grp = seedScanGroup(2);
        ScanRequest req = ScanRequest.builder().tableName(TEST_TABLE).scanFilter(Filters.eq("grp", grp)).build();
        Stream<Map<String, Object>> stream = executor.scan(req);
        assertEquals(2, stream.count());
    }

    @Test
    public void testScanPaginatesAndSkipsEmptyPages() {
        assumeAvailable();
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
        assumeAvailable();
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
        assumeAvailable();
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        TestEntity entity = newEntity(id(), "Test");
        mapper.putItem(entity);

        TestEntity result = mapper.getItem(entity, true);
        assertNotNull(result);
        assertEquals(entity.getId(), result.getId());
    }

    @Test
    public void testMapperGetItemWithKey() {
        assumeAvailable();
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        mapper.putItem(newEntity(id, "Test"));

        TestEntity result = mapper.getItem(DynamoDBExecutor.asKey("id", id));
        assertNotNull(result);
        assertEquals(id, result.getId());
    }

    @Test
    public void testMapperGetItemWithRequest() {
        assumeAvailable();
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
        assumeAvailable();
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
        assumeAvailable();
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
        assumeAvailable();
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        TestEntity e = newEntity(id(), "a");
        mapper.putItem(e);

        List<TestEntity> result = mapper.batchGetItem(List.of(e), "TOTAL");
        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    public void testMapperBatchGetItemWithRequest() {
        assumeAvailable();
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
        assumeAvailable();
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        TestEntity entity = newEntity(id(), "x"); // not stored -> empty result

        List<TestEntity> result = mapper.batchGetItem(List.of(entity));
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @Test
    public void testMapperPutItemWithReturnValues() {
        assumeAvailable();
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        mapper.putItem(newEntity(id, "v1"));

        PutItemResponse result = mapper.putItem(newEntity(id, "v2"), "ALL_OLD");
        assertNotNull(result);
        assertEquals("v1", result.attributes().get("name").s());
    }

    @Test
    public void testMapperPutItemWithRequest() {
        assumeAvailable();
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        PutItemRequest request = PutItemRequest.builder().tableName(TEST_TABLE).item(DynamoDBExecutor.asItem("id", id)).build();
        assertNotNull(mapper.putItem(request));
        assertNotNull(executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)));
    }

    @Test
    public void testMapperPutItemWithRequest_NoTableName() {
        assumeAvailable();
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        PutItemRequest request = PutItemRequest.builder().item(DynamoDBExecutor.asItem("id", id)).build();
        assertNotNull(mapper.putItem(request));
        assertNotNull(executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)));
    }

    @Test
    public void testMapperBatchPutItem() {
        assumeAvailable();
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        BatchWriteItemResponse result = mapper.batchPutItem(List.of(newEntity(id, "x")));
        assertNotNull(result);
        assertNotNull(executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)));
    }

    @Test
    public void testMapperUpdateItem() {
        assumeAvailable();
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
        assumeAvailable();
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        mapper.putItem(newEntity(id, "old"));

        UpdateItemResponse result = mapper.updateItem(newEntity(id, "Updated"), "ALL_NEW");
        assertNotNull(result);
        assertEquals("Updated", result.attributes().get("name").s());
    }

    @Test
    public void testMapperUpdateItemWithRequest() {
        assumeAvailable();
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
        assumeAvailable();
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        mapper.putItem(newEntity(id, "x"));

        DeleteItemResponse result = mapper.deleteItem(newEntity(id, "x"));
        assertNotNull(result);
        assertNull(executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)));
    }

    @Test
    public void testMapperDeleteItemWithReturnValues() {
        assumeAvailable();
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        mapper.putItem(newEntity(id, "gone"));

        DeleteItemResponse result = mapper.deleteItem(newEntity(id, "gone"), "ALL_OLD");
        assertNotNull(result);
        assertEquals("gone", result.attributes().get("name").s());
    }

    @Test
    public void testMapperDeleteItemWithKey() {
        assumeAvailable();
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        mapper.putItem(newEntity(id, "x"));

        DeleteItemResponse result = mapper.deleteItem(DynamoDBExecutor.asKey("id", id));
        assertNotNull(result);
        assertNull(executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)));
    }

    @Test
    public void testMapperDeleteItemWithRequest() {
        assumeAvailable();
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
        assumeAvailable();
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String id = id();
        mapper.putItem(newEntity(id, "x"));

        BatchWriteItemResponse result = mapper.batchDeleteItem(List.of(newEntity(id, "x")));
        assertNotNull(result);
        assertNull(executor.getItem(TEST_TABLE, DynamoDBExecutor.asKey("id", id)));
    }

    @Test
    public void testMapperBatchWriteItemWithRequest() {
        assumeAvailable();
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
        assumeAvailable();
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
        assumeAvailable();
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
        assumeAvailable();
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
        assumeAvailable();
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String name = "scan-" + id();
        mapper.putItem(newEntity(id(), name));

        Stream<TestEntity> stream = mapper.scan(Filters.eq("name", name));
        assertEquals(1, stream.count());
    }

    @Test
    public void testMapperScanWithAttributesAndFilter() {
        assumeAvailable();
        DynamoDBExecutor.Mapper<TestEntity> mapper = executor.mapper(TestEntity.class);
        String name = "scan-" + id();
        mapper.putItem(newEntity(id(), name));

        Stream<TestEntity> stream = mapper.scan(List.of("id", "name"), Filters.eq("name", name));
        assertEquals(1, stream.count());
    }

    @Test
    public void testMapperScanWithRequest() {
        assumeAvailable();
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
        assumeAvailable();
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
        assumeAvailable();
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
        assumeAvailable();
        DynamoDBExecutor.Mapper<TestEntityWithUserId> mapper = executor.mapper(TestEntityWithUserId.class, USER_TABLE, NamingPolicy.SNAKE_CASE);

        TestEntityWithUserId entity = new TestEntityWithUserId();
        entity.setUserId("u-" + id());
        mapper.putItem(entity);

        mapper.batchDeleteItem(List.of(entity));

        // The delete key must use "user_id"; if it did, the row is gone.
        assertNull(executor.getItem(USER_TABLE, DynamoDBExecutor.asKey("user_id", entity.getUserId())));
    }

    // ====================================================================================================
    // Entities
    // ====================================================================================================

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
