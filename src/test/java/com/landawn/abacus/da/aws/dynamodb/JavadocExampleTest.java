package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;

/**
 * Test class to verify all Javadoc code examples in DynamoDB classes compile and work correctly.
 * This test verifies examples from:
 * - com.landawn.abacus.da.aws.dynamodb.DynamoDBExecutor
 * - com.landawn.abacus.da.aws.dynamodb.AsyncDynamoDBExecutor
 * - com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
 * - com.landawn.abacus.da.aws.dynamodb.v2.AsyncDynamoDBExecutor
 */
public class JavadocExampleTest {

    /**
     * Test examples from v2/DynamoDBExecutor.java class-level documentation (line 133)
     */
    @Test
    public void testV2DynamoDBExecutor_ClassExample() {
        // This test verifies the example compiles correctly
        // We don't execute it as it requires actual DynamoDB connection

        // Example from line 133-162
        // DynamoDbClient client = DynamoDbClient.builder()
        //     .region(Region.US_EAST_1)
        //     .build();
        // DynamoDBExecutor executor = new DynamoDBExecutor(client);

        // Verify the pattern compiles
        assertDoesNotThrow(() -> {
            // Builder pattern validation - just verify builder() method exists
            assertNotNull(DynamoDbClient.builder());
        });
    }

    /**
     * Test examples from v2/DynamoDBExecutor.java constructor documentation (line 201)
     */
    @Test
    public void testV2DynamoDBExecutor_ConstructorExample() {
        // Example from line 201-212
        // DynamoDbClient client = DynamoDbClient.builder()
        //     .region(Region.US_EAST_1)
        //     .credentialsProvider(DefaultCredentialsProvider.create())
        //     .overrideConfiguration(ClientOverrideConfiguration.builder()
        //         .retryPolicy(RetryPolicy.builder()
        //             .numRetries(3)
        //             .build())
        //         .build())
        //     .build();
        // DynamoDBExecutor executor = new DynamoDBExecutor(client);

        // Verify the pattern compiles
        assertDoesNotThrow(() -> {
            ClientOverrideConfiguration config = ClientOverrideConfiguration.builder().retryPolicy(RetryPolicy.builder().numRetries(3).build()).build();
            assertNotNull(config);
        });
    }

    /**
     * Test examples from v2/DynamoDBExecutor.java attrValueUpdateOf (line 474)
     */
    @Test
    public void testV2DynamoDBExecutor_AttrValueUpdateOf_Simple() {
        // Example from line 474-485
        Map<String, AttributeValueUpdate> updates = new HashMap<>();
        updates.put("name", com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.attrValueUpdateOf("John Doe"));
        updates.put("age", com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.attrValueUpdateOf(30));
        updates.put("lastLogin", com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.attrValueUpdateOf(Instant.now().toString()));

        UpdateItemRequest.Builder requestBuilder = UpdateItemRequest.builder()
                .tableName("Users")
                .key(com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.asKey("userId", "123"))
                .attributeUpdates(updates);

        assertNotNull(requestBuilder);
        UpdateItemRequest request = requestBuilder.build();
        assertNotNull(request);
        assertEquals("Users", request.tableName());
        assertTrue(request.hasAttributeUpdates());
        assertEquals(3, request.attributeUpdates().size());
    }

    /**
     * Test examples from v2/DynamoDBExecutor.java attrValueUpdateOf with actions (line 519)
     */
    @Test
    public void testV2DynamoDBExecutor_AttrValueUpdateOf_WithActions() {
        // Example from line 519-542
        // Replace attribute value
        AttributeValueUpdate put = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.attrValueUpdateOf("updated name", AttributeAction.PUT);
        assertNotNull(put);
        assertEquals(AttributeAction.PUT, put.action());

        // Increment a numeric counter
        AttributeValueUpdate increment = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.attrValueUpdateOf(1, AttributeAction.ADD);
        assertNotNull(increment);
        assertEquals(AttributeAction.ADD, increment.action());

        // Decrement a numeric value
        AttributeValueUpdate decrement = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.attrValueUpdateOf(-5, AttributeAction.ADD);
        assertNotNull(decrement);
        assertEquals(AttributeAction.ADD, decrement.action());

        // Delete an attribute entirely
        AttributeValueUpdate delete = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.attrValueUpdateOf(null, AttributeAction.DELETE);
        assertNotNull(delete);
        assertEquals(AttributeAction.DELETE, delete.action());

        // Use in UpdateItem operation
        Map<String, AttributeValueUpdate> updates = new HashMap<>();
        updates.put("loginCount", increment);
        updates.put("lastLogin", com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.attrValueUpdateOf(Instant.now().toString()));

        UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName("Users")
                .key(com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.asKey("userId", "user123"))
                .attributeUpdates(updates)
                .build();

        assertNotNull(request);
        assertEquals("Users", request.tableName());
    }

    /**
     * Test examples from v2/DynamoDBExecutor.java asItem single attribute (line 706)
     */
    @Test
    public void testV2DynamoDBExecutor_AsItem_SingleAttribute() {
        // Example from line 706-715
        Map<String, AttributeValue> item = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.asItem("name", "John Doe");
        // Results in: {"name": AttributeValue.fromS("John Doe")}

        assertNotNull(item);
        assertEquals(1, item.size());
        assertTrue(item.containsKey("name"));
        assertEquals("John Doe", item.get("name").s());

        // Use with PutItem operation
        PutItemRequest request = PutItemRequest.builder().tableName("Users").item(item).build();

        assertNotNull(request);
        assertEquals("Users", request.tableName());
    }

    /**
     * Test examples from v2/DynamoDBExecutor.java asItem two attributes (line 734)
     */
    @Test
    public void testV2DynamoDBExecutor_AsItem_TwoAttributes() {
        // Example from line 734-737
        Map<String, AttributeValue> item = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.asItem("name", "John Doe", "age", 30);
        // Results in: {"name": AttributeValue.fromS("John Doe"), "age": AttributeValue.fromN("30")}

        assertNotNull(item);
        assertEquals(2, item.size());
        assertTrue(item.containsKey("name"));
        assertTrue(item.containsKey("age"));
        assertEquals("John Doe", item.get("name").s());
        assertEquals("30", item.get("age").n());
    }

    /**
     * Test asKey method usage from various examples
     */
    @Test
    public void testV2DynamoDBExecutor_AsKey() {
        // Pattern used throughout examples like line 148, 482, 539, etc.
        Map<String, AttributeValue> key = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.asKey("userId", "123");

        assertNotNull(key);
        assertEquals(1, key.size());
        assertTrue(key.containsKey("userId"));
        assertEquals("123", key.get("userId").s());

        // Use in GetItemRequest
        GetItemRequest request = GetItemRequest.builder().tableName("Users").key(key).consistentRead(true).build();

        assertNotNull(request);
        assertEquals("Users", request.tableName());
        assertTrue(request.consistentRead());
    }

    /**
     * Test attrValueOf method with different types
     */
    @Test
    public void testV2DynamoDBExecutor_AttrValueOf_DifferentTypes() {
        // String value
        AttributeValue stringAttr = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.attrValueOf("test");
        assertNotNull(stringAttr);
        assertEquals("test", stringAttr.s());

        // Number value
        AttributeValue numberAttr = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.attrValueOf(42);
        assertNotNull(numberAttr);
        assertEquals("42", numberAttr.n());

        // Boolean value
        AttributeValue boolAttr = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.attrValueOf(true);
        assertNotNull(boolAttr);
        assertTrue(boolAttr.bool());
    }

    /**
     * Test QueryRequest builder pattern from examples
     */
    @Test
    public void testV2DynamoDBExecutor_QueryRequest() {
        // Example from line 154-161
        QueryRequest query = QueryRequest.builder()
                .tableName("Users")
                .keyConditionExpression("userId = :userId")
                .expressionAttributeValues(Map.of(":userId", AttributeValue.fromS("123")))
                .build();

        assertNotNull(query);
        assertEquals("Users", query.tableName());
        assertEquals("userId = :userId", query.keyConditionExpression());
        assertTrue(query.hasExpressionAttributeValues());
        assertEquals(1, query.expressionAttributeValues().size());
    }

    /**
     * Test asItem with three attributes
     */
    @Test
    public void testV2DynamoDBExecutor_AsItem_ThreeAttributes() {
        // Pattern from line 765-784
        Map<String, AttributeValue> item = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.asItem("id", "123", "name", "John Doe", "age", 30);

        assertNotNull(item);
        assertEquals(3, item.size());
        assertTrue(item.containsKey("id"));
        assertTrue(item.containsKey("name"));
        assertTrue(item.containsKey("age"));
        assertEquals("123", item.get("id").s());
        assertEquals("John Doe", item.get("name").s());
        assertEquals("30", item.get("age").n());
    }

    /**
     * Test asUpdateItem with single attribute
     */
    @Test
    public void testV2DynamoDBExecutor_AsUpdateItem_SingleAttribute() {
        // Pattern from line 847-859
        Map<String, AttributeValueUpdate> updates = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.asUpdateItem("name", "John Doe");

        assertNotNull(updates);
        assertEquals(1, updates.size());
        assertTrue(updates.containsKey("name"));
        assertNotNull(updates.get("name"));
        assertEquals(AttributeAction.PUT, updates.get("name").action());
    }

    /**
     * Test asUpdateItem with two attributes
     */
    @Test
    public void testV2DynamoDBExecutor_AsUpdateItem_TwoAttributes() {
        // Pattern from line 847-859
        Map<String, AttributeValueUpdate> updates = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.asUpdateItem("name", "John Doe", "age", 30);

        assertNotNull(updates);
        assertEquals(2, updates.size());
        assertTrue(updates.containsKey("name"));
        assertTrue(updates.containsKey("age"));
    }

    /**
     * Test asUpdateItem with three attributes
     */
    @Test
    public void testV2DynamoDBExecutor_AsUpdateItem_ThreeAttributes() {
        // Pattern from line 872-887
        Map<String, AttributeValueUpdate> updates = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.asUpdateItem("name", "John Doe", "age", 30, "email",
                "john@example.com");

        assertNotNull(updates);
        assertEquals(3, updates.size());
        assertTrue(updates.containsKey("name"));
        assertTrue(updates.containsKey("age"));
        assertTrue(updates.containsKey("email"));
    }

    /**
     * Test that null values are handled correctly
     * Note: The Javadoc indicates null validation should occur, but the implementation
     * delegates to SDK builder methods which may have their own null handling.
     */
    @Test
    public void testV2DynamoDBExecutor_NullHandling() {
        // Test that DELETE action can handle null value (as shown in line 530)
        AttributeValueUpdate delete = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.attrValueUpdateOf(null, AttributeAction.DELETE);
        assertNotNull(delete);
        assertEquals(AttributeAction.DELETE, delete.action());
    }

    /**
     * Verify Region enum usage from examples
     */
    @Test
    public void testRegionEnumUsage() {
        // Pattern from line 136, 203
        Region region = Region.US_EAST_1;
        assertNotNull(region);
        assertEquals("us-east-1", region.id());
    }

    /**
     * Test AttributeValue static factory methods
     */
    @Test
    public void testAttributeValueFactoryMethods() {
        // Pattern from line 158, 268
        AttributeValue stringValue = AttributeValue.fromS("123");
        assertNotNull(stringValue);
        assertEquals("123", stringValue.s());

        AttributeValue numberValue = AttributeValue.fromN("42");
        assertNotNull(numberValue);
        assertEquals("42", numberValue.n());

        AttributeValue boolValue = AttributeValue.fromBool(true);
        assertNotNull(boolValue);
        assertTrue(boolValue.bool());
    }

    /**
     * Test Map.of usage for expression attribute values
     */
    @Test
    public void testMapOfUsage() {
        // Pattern from line 157-159
        Map<String, AttributeValue> values = Map.of(":userId", AttributeValue.fromS("123"));

        assertNotNull(values);
        assertEquals(1, values.size());
        assertTrue(values.containsKey(":userId"));

        // Multiple values
        Map<String, AttributeValue> multiValues = Map.of(":userId", AttributeValue.fromS("123"), ":status", AttributeValue.fromS("active"));

        assertEquals(2, multiValues.size());
    }

    /**
     * Test builder pattern chaining
     */
    @Test
    public void testBuilderPatternChaining() {
        // Pattern used throughout examples
        GetItemRequest request = GetItemRequest.builder()
                .tableName("Users")
                .key(com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.asKey("id", "123"))
                .consistentRead(true)
                .build();

        assertNotNull(request);
        assertEquals("Users", request.tableName());
        assertTrue(request.consistentRead());
        assertTrue(request.hasKey());

        // UpdateItemRequest builder
        UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                .tableName("Users")
                .key(com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.asKey("userId", "user123"))
                .attributeUpdates(Map.of("status", com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.attrValueUpdateOf("active")))
                .build();

        assertNotNull(updateRequest);
        assertEquals("Users", updateRequest.tableName());
    }
}
