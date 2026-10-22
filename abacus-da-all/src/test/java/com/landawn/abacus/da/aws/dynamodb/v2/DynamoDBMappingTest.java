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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.aws.dynamodb.DynamoDBExecutorBase;
import com.landawn.abacus.util.Clazz;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.NamingPolicy;

import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

public class DynamoDBMappingTest extends DynamoDBExecutorTestSupport {

    // Construction and configuration

    @Test
    public void testToItemWithNamingPolicy() {
        TestEntity entity = new TestEntity();
        entity.setFirstName("John");
        Map<String, AttributeValue> result = DynamoDBExecutor.toItem(entity, NamingPolicy.SNAKE_CASE);

        assertNotNull(result);
        assertEquals("John", result.get("first_name").s());
    }

    // Attribute and key conversion

    @Test
    public void testAsItem() {
        Map<String, AttributeValue> result = DynamoDBExecutor.asItem("id", "123");
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("123", result.get("id").s());
    }

    @Test
    public void testAsItem_NullOrEmptyAttrNameThrowsIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asItem(null, "v"));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asItem("", "v"));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asItem("a", "v", null, "v2"));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asItem("a", "v", "b", "v2", null, "v3"));

        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asUpdateItem(null, "v"));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asUpdateItem("", "v"));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asUpdateItem("a", "v", null, "v2"));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asUpdateItem("a", "v", "b", "v2", null, "v3"));
    }

    @Test
    public void testAsItemVarargs() {
        Map<String, AttributeValue> result = DynamoDBExecutor.asItem(new Object[] { "a", "x", "b", "y" });
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("x", result.get("a").s());
        assertEquals("y", result.get("b").s());
    }

    // with IllegalArgumentException rather than letting an incidental NullPointerException escape.
    @Test
    public void testAsItemVarargs_NullArrayThrowsIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asItem((Object[]) null));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asUpdateItem((Object[]) null));
    }

    @Test
    public void testAsKey() {
        Map<String, AttributeValue> result = DynamoDBExecutor.asKey("id", "123");
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("123", result.get("id").s());
    }

    /** DynamoDB keys must be complete, scalar, non-empty, and contain at most two distinct attributes. */
    @Test
    public void testAsKeyRejectsInvalidDynamoDBKeys() {
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("", "123"));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("id", null));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("id", ""));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("id", true));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("id", List.of("nested")));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("id", Map.of("nested", "value")));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("id", Double.POSITIVE_INFINITY));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("id", new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("id", "1", "id", "2"));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey((Object[]) null));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey(new Object[0]));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey(new Object[] { "a", 1, "b", 2, "c", 3 }));
    }

    // Twin of testAsKey_NonScalarMessageNamesArrayTypeReadably: v2 accepts byte[] keys, so an EMPTY byte[] reaches the second
    // toKeyAttributeValue message, which still used Class.getName() and read "received [B" (also via a Mapper's entity key).
    @Test
    public void testAsKey_EmptyBinaryKeyMessageNamesByteArrayReadably_coverageDC() {
        final String prefix = "DynamoDB key attribute 'id' must be a non-empty String, finite Number, or non-empty binary value; received ";

        assertEquals(prefix + "byte[]", assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("id", new byte[0])).getMessage());
        assertEquals(prefix.replace("'id'", "'sk'") + "byte[]",
                assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("id", "1", "sk", new byte[0])).getMessage());

        final BinaryKeyEntity_coverageDC entity = new BinaryKeyEntity_coverageDC();
        entity.setId(new byte[0]);
        assertEquals(prefix + "byte[]", assertThrows(IllegalArgumentException.class,
                () -> executor.mapper(BinaryKeyEntity_coverageDC.class, "t", NamingPolicy.CAMEL_CASE).getItem(entity)).getMessage());
        verify(mockDynamoDbClient, never()).getItem(any(GetItemRequest.class));

        // Nested classes are named canonically; top-level classes read the same as before.
        assertEquals(prefix + BlankKeyValue_coverageDC.class.getCanonicalName(),
                assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("id", new BlankKeyValue_coverageDC())).getMessage());
        assertEquals(prefix + "java.lang.String", assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("id", "")).getMessage());
        assertEquals(prefix + "java.lang.Boolean", assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("id", true)).getMessage());
        assertEquals(prefix + "java.nio.HeapByteBuffer",
                assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("id", ByteBuffer.allocate(0))).getMessage());
    }

    // An array key value was named by Class.getName(): "must be scalar, not [Ljava.lang.String;".
    @Test
    public void testAsKey_NonScalarMessageNamesArrayTypeReadably() {
        final IllegalArgumentException array = assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("id", new String[] { "a" }));
        assertEquals("DynamoDB key attribute 'id' must be scalar, not java.lang.String[]", array.getMessage());

        final IllegalArgumentException list = assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("id", new java.util.ArrayList<>()));
        assertEquals("DynamoDB key attribute 'id' must be scalar, not java.util.ArrayList", list.getMessage());
    }

    // TODO: toList(List<Map>, offset, count, Class) is package-private in v2
    @Test
    public void testAsKey_VarargsMixed() {
        Map<String, AttributeValue> result = DynamoDBExecutor.asKey("k1", "v1", "k2", 42);
        assertEquals(2, result.size());
        assertEquals("v1", result.get("k1").s());
        assertEquals("42", result.get("k2").n());
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
    public void testAsKeyWithThreeParameters() {
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("id", "123", "name", "test", "age", 25));
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
    public void testAsUpdateItem() {
        Map<String, AttributeValueUpdate> result = DynamoDBExecutor.asUpdateItem("name", "test");
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("test", result.get("name").value().s());
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
    public void testAttrValueOfNull() {
        AttributeValue result = DynamoDBExecutor.toAttributeValue(null);
        assertNotNull(result);
        assertTrue(result.nul());
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
    public void testAttrValueOfNumber() {
        AttributeValue result = DynamoDBExecutor.toAttributeValue(123);
        assertNotNull(result);
        assertEquals("123", result.n());

        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toAttributeValue(Double.NEGATIVE_INFINITY));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toAttributeValue(Float.NaN));
    }

    @Test
    public void testAttrValueOfString() {
        AttributeValue result = DynamoDBExecutor.toAttributeValue("test");
        assertNotNull(result);
        assertEquals("test", result.s());
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
    public void testToAttributeValueSerializesContainersAndBeansAsJsonString() {
        final AttributeValue listAttr = DynamoDBExecutor.toAttributeValue(List.of(1, 2));
        assertFalse(listAttr.hasL());
        assertFalse(listAttr.hasNs());
        assertEquals(List.of(1, 2), com.landawn.abacus.util.N.fromJson(listAttr.s(), List.class));

        final AttributeValue mapAttr = DynamoDBExecutor.toAttributeValue(Map.of("k", 1));
        assertFalse(mapAttr.hasM());
        assertEquals(Map.of("k", 1), com.landawn.abacus.util.N.fromJson(mapAttr.s(), Map.class));

        final AttributeValue arrayAttr = DynamoDBExecutor.toAttributeValue(new String[] { "a", "b" });
        assertFalse(arrayAttr.hasSs());
        assertEquals(List.of("a", "b"), com.landawn.abacus.util.N.fromJson(arrayAttr.s(), List.class));

        final TestEntity bean = new TestEntity();
        bean.setId("u1");
        bean.setName("Alice");
        final AttributeValue beanAttr = DynamoDBExecutor.toAttributeValue(bean);
        assertFalse(beanAttr.hasM());
        final TestEntity parsed = com.landawn.abacus.util.N.fromJson(beanAttr.s(), TestEntity.class);
        assertEquals("u1", parsed.getId());
        assertEquals("Alice", parsed.getName());

        // byte[] is the one array type that stays native (B), not an S string
        final AttributeValue binaryAttr = DynamoDBExecutor.toAttributeValue(new byte[] { 1, 2 });
        assertNull(binaryAttr.s());
        assertArrayEquals(new byte[] { 1, 2 }, binaryAttr.b().asByteArray());
    }

    @Test
    public void testToItem_NullArgumentsThrowIllegalArgument() {
        TestEntity entity = new TestEntity();
        entity.setId("1");

        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toItem(null));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toItem(null, NamingPolicy.CAMEL_CASE));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toItem(entity, null));
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

    // toItem/toUpdateItem also accept Object[] name-value pairs, but their rejection message listed only "Entity or Map<String, Object>
    // classes with getter/setter methods" (v1 already names all three accepted shapes).
    @Test
    public void testToItem_UnsupportedTypeMessageListsAcceptedShapes() {
        final IllegalArgumentException item = assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toItem(5));
        assertTrue(item.getMessage().startsWith("Unsupported type: java.lang.Integer. "), item.getMessage());
        assertTrue(item.getMessage().contains("Object[] name-value pairs"), item.getMessage());

        final IllegalArgumentException update = assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toUpdateItem("text"));
        assertTrue(update.getMessage().contains("Object[] name-value pairs"), update.getMessage());
    }

    // Exact text of both "Unsupported type" sites and both overloads of each (testToItem_UnsupportedTypeMessageListsAcceptedShapes fails
    // at its toItem assertion on HEAD, so the toUpdateItem site was never proven on its own).
    @Test
    public void testToItemAndToUpdateItem_UnsupportedTypeMessagesListAcceptedShapes_coverageDC() {
        final String suffix = ". Only entity classes with getter/setter methods, Map<String, Object>, or Object[] name-value pairs are supported";

        assertEquals(
                List.of("Unsupported type: java.lang.String" + suffix, "Unsupported type: java.lang.Integer" + suffix,
                        "Unsupported type: java.lang.Integer" + suffix, "Unsupported type: java.lang.String" + suffix),
                List.of(assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toUpdateItem("text")).getMessage(),
                        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toUpdateItem(5, NamingPolicy.SNAKE_CASE)).getMessage(),
                        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toItem(5)).getMessage(),
                        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toItem("text", NamingPolicy.SNAKE_CASE)).getMessage()));
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
    public void testToUpdateItem_NullArgumentsThrowIllegalArgument() {
        TestEntity entity = new TestEntity();
        entity.setId("1");

        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toUpdateItem(null));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toUpdateItem(null, NamingPolicy.CAMEL_CASE));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toUpdateItem(entity, null));
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

    @Test
    public void testToValue_EmptyNumberN_FallsThrough() {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("k", AttributeValue.builder().n("").build());
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        assertEquals("", result.get("k"));
    }

    @Test
    public void testToValue_EmptyStringS_FallsThrough() {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("k", AttributeValue.builder().s("").build());
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        assertEquals("", result.get("k"));
    }

    @Test
    public void testToValue_NullViaToMap() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("n", AttributeValue.builder().nul(true).build());
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        assertNull(result.get("n"));
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
    public void testToValue_BinaryViaToMap() {
        software.amazon.awssdk.core.SdkBytes bytes = software.amazon.awssdk.core.SdkBytes.fromByteArray(new byte[] { 1, 2, 3 });
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("b", AttributeValue.builder().b(bytes).build());
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        byte[] arr = (byte[]) result.get("b");
        assertEquals(3, arr.length);
    }

    @Test
    public void testToValue_BoolViaToMap() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("flag", AttributeValue.builder().bool(true).build());
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        assertEquals(Boolean.TRUE, result.get("flag"));
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

    @Test
    public void testToValue_NumberSetViaToMap() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ns", AttributeValue.builder().ns("1", "2").build());
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        assertEquals(2, ((List<?>) result.get("ns")).size());
    }

    @Test
    public void testToValue_StringSetViaToMap() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ss", AttributeValue.builder().ss("a", "b", "c").build());
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        assertEquals(3, ((List<?>) result.get("ss")).size());
    }

    @Test
    public void testToValue_SetResultsAreMutableCopies() {
        // SS/NS must behave like L/M/BS: a mutable Java collection detached from the immutable SDK attribute.
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("ss", AttributeValue.builder().ss("a", "b").build());
        item.put("ns", AttributeValue.builder().ns("1", "2").build());

        Map<String, Object> result = DynamoDBExecutor.toMap(item);

        @SuppressWarnings("unchecked")
        final List<String> ss = (List<String>) result.get("ss");
        @SuppressWarnings("unchecked")
        final List<String> ns = (List<String>) result.get("ns");

        ss.add("c");
        ns.add("3");

        assertEquals(List.of("a", "b", "c"), ss);
        assertEquals(List.of("1", "2", "3"), ns);
        assertEquals(2, item.get("ss").ss().size());
        assertEquals(2, item.get("ns").ns().size());
    }

    @Test
    public void testToValue_TargetClassConversion() {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("count", AttributeValue.builder().n("42").build());
        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(item).build());

        GetItemRequest req = GetItemRequest.builder().tableName("T").key(Map.of("id", AttributeValue.builder().s("1").build())).build();
        Integer result = executor.getItem(req, Integer.class);
        assertEquals(42, result);
    }

    // Row and entity mapping

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
    public void testExtractDataNullResponseThrowsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.extractData((QueryResponse) null));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.extractData((QueryResponse) null, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.extractData((ScanResponse) null));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.extractData((ScanResponse) null, 0, 1));
        // The null response is reported before the offset/count range check (signature order).
        assertTrue(assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.extractData((QueryResponse) null, -1, 1)).getMessage()
                .contains("queryResult"));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.extractData((ScanResponse) null, 0, -1)).getMessage()
                .contains("scanResult"));
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
    public void testToEntity_EmptyBinaryContainersRemainEmpty() {
        final AttributeValue emptyList = AttributeValue.fromL(List.of());
        final AttributeValue emptyMap = AttributeValue.fromM(Map.of());

        final BinaryContainerEntity entity = DynamoDBExecutor.toEntity(Map.of("bytes", emptyList, "buffers", emptyList, "byteArray", emptyList, "bufferArray",
                emptyList, "bufferSet", emptyList, "byteMap", emptyMap, "bufferMap", emptyMap), BinaryContainerEntity.class);

        assertTrue(entity.getBytes().isEmpty());
        assertTrue(entity.getBuffers().isEmpty());
        assertEquals(0, entity.getByteArray().length);
        assertEquals(0, entity.getBufferArray().length);
        assertTrue(entity.getBufferSet().isEmpty());
        assertTrue(entity.getByteMap().isEmpty());
        assertTrue(entity.getBufferMap().isEmpty());
    }

    @Test
    public void testToEntity_NullGetItemResponseReturnsNull() {
        TestEntity result = DynamoDBExecutor.toEntity((GetItemResponse) null, TestEntity.class);
        assertNull(result);
    }

    // toEntity null branches
    @Test
    public void testToEntity_NullItemReturnsNull() {
        TestEntity result = DynamoDBExecutor.toEntity((Map<String, AttributeValue>) null, TestEntity.class);
        assertNull(result);
    }

    @Test
    public void testToEntity_NullTargetClassThrowsIllegalArgument() {
        Map<String, AttributeValue> item = Map.of("id", AttributeValue.builder().s("1").build());

        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toEntity(item, null));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toEntity((Map<String, AttributeValue>) null, null));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toEntity(GetItemResponse.builder().item(item).build(), null));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toEntity((GetItemResponse) null, null));
    }

    // abacus-common's N.convert(byte[], ByteBuffer.class) returns a buffer positioned at its end
    // (remaining() == 0), so a get -> put round trip used to write an EMPTY binary attribute.

    @Test
    public void testToEntity_BinaryAttributeToByteBufferProperty_IsReadable() {
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("id", AttributeValue.fromS("1"));
        item.put("buf", AttributeValue.fromB(software.amazon.awssdk.core.SdkBytes.fromByteArray(new byte[] { 1, 2, 3 })));

        final ByteBufferEntity entity = DynamoDBExecutor.toEntity(item, ByteBufferEntity.class);

        assertEquals(0, entity.getBuf().position());
        assertEquals(3, entity.getBuf().remaining());

        // read-modify-write must not lose the binary content
        final AttributeValue written = DynamoDBExecutor.toItem(entity).get("buf");
        assertTrue(java.util.Arrays.equals(new byte[] { 1, 2, 3 }, written.b().asByteArray()));
    }

    // isJsonArrayText only checks the brackets: plain text such as "[a] and [b]" read into a String[]/Object[] property used to
    // throw a ParsingException (failing the whole item) instead of keeping the lenient single-element conversion.
    @Test
    public void testToEntity_BracketedNonJsonTextIntoStringArrayKeepsSingleElement() {
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("tags", AttributeValue.fromS("[a] and [b]"));
        item.put("values", AttributeValue.fromS("[\"unterminated]"));

        final StringArrayEntity result = DynamoDBExecutor.toEntity(item, StringArrayEntity.class);

        assertArrayEquals(new String[] { "[a] and [b]" }, result.getTags());
        assertArrayEquals(new Object[] { "[\"unterminated]" }, result.getValues());
        assertArrayEquals(new String[] { "[x] [y]" }, com.landawn.abacus.util.N.convert(AttributeValue.fromS("[x] [y]"), String[].class));
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
    public void testToEntity_NativeBinaryListsAndMapsPreserveNullAndEmptyValues() {
        final AttributeValue binary = AttributeValue.fromB(software.amazon.awssdk.core.SdkBytes.fromByteArray(new byte[] { 4, 5 }));
        final AttributeValue missing = AttributeValue.fromNul(true);
        final AttributeValue empty = AttributeValue.fromB(software.amazon.awssdk.core.SdkBytes.fromByteArray(new byte[0]));
        final AttributeValue binaryList = AttributeValue.fromL(List.of(binary, missing, empty));
        final AttributeValue binaryMap = AttributeValue.fromM(Map.of("7", binary, "8", missing, "9", empty));

        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("bytes", binaryList);
        item.put("buffers", binaryList);
        item.put("byteArray", binaryList);
        item.put("bufferArray", binaryList);
        item.put("byteMap", binaryMap);
        item.put("bufferMap", binaryMap);

        final BinaryContainerEntity entity = DynamoDBExecutor.toEntity(item, BinaryContainerEntity.class);
        assertArrayEquals(new byte[] { 4, 5 }, entity.getBytes().get(0));
        assertEquals(ByteBuffer.wrap(new byte[] { 4, 5 }), entity.getBuffers().get(0));
        assertNull(entity.getBytes().get(1));
        assertNull(entity.getBuffers().get(1));
        assertArrayEquals(new byte[0], entity.getBytes().get(2));
        assertEquals(0, entity.getBuffers().get(2).remaining());
        assertArrayEquals(new byte[] { 4, 5 }, entity.getByteArray()[0]);
        assertNull(entity.getByteArray()[1]);
        assertArrayEquals(new byte[0], entity.getByteArray()[2]);
        assertEquals(ByteBuffer.wrap(new byte[] { 4, 5 }), entity.getBufferArray()[0]);
        assertNull(entity.getBufferArray()[1]);
        assertEquals(0, entity.getBufferArray()[2].remaining());
        assertArrayEquals(new byte[] { 4, 5 }, entity.getByteMap().get(7));
        assertNull(entity.getByteMap().get(8));
        assertArrayEquals(new byte[0], entity.getByteMap().get(9));
        assertEquals(ByteBuffer.wrap(new byte[] { 4, 5 }), entity.getBufferMap().get("7"));
        assertNull(entity.getBufferMap().get("8"));
        assertEquals(0, entity.getBufferMap().get("9").remaining());

    }

    @Test
    public void testToEntity_NativeBinarySetsPreserveTypedElementsAndArrays() {
        final ByteBuffer source = ByteBuffer.wrap(new byte[] { 9, 1, 2, -3, 8 }).asReadOnlyBuffer();
        source.position(1);
        source.limit(4);
        final AttributeValue binarySet = AttributeValue.fromBs(List.of(software.amazon.awssdk.core.SdkBytes.fromByteBuffer(source)));

        final Map<String, AttributeValue> item = new LinkedHashMap<>();

        for (final String name : List.of("bytes", "buffers", "byteArray", "bufferArray", "bufferSet", "rawValues", "rawArray")) {
            item.put(name, binarySet);
        }

        final BinaryContainerEntity entity = DynamoDBExecutor.toEntity(item, BinaryContainerEntity.class);
        final byte[] expected = { 1, 2, -3 };
        assertArrayEquals(expected, entity.getBytes().get(0));
        assertEquals(ByteBuffer.wrap(expected), entity.getBuffers().get(0));
        assertArrayEquals(expected, entity.getByteArray()[0]);
        assertEquals(ByteBuffer.wrap(expected), entity.getBufferArray()[0]);
        assertEquals(java.util.Set.of(ByteBuffer.wrap(expected)), entity.getBufferSet());
        assertEquals(1, source.position());
        assertEquals(4, source.limit());

        assertEquals(0, entity.getBuffers().get(0).position());
        assertEquals(0, entity.getBufferArray()[0].position());
        assertArrayEquals(expected, (byte[]) entity.getRawValues().get(0));
        assertArrayEquals(expected, (byte[]) entity.getRawArray()[0]);
    }

    // Regression for the Object-typed-slot guard: a Map<String,Object> property must receive the raw
    // container as-is. Rebuilding it through the JSON codec would re-parse a byte[] value (from a
    // native B sub-attribute) under element type Object, turning it into a List of numbers.
    @Test
    public void testToEntity_ObjectValuedMap_PreservesByteArray() {
        final byte[] blob = new byte[] { 1, 2, -3 };

        final Map<String, AttributeValue> nested = new LinkedHashMap<>();
        nested.put("blob", AttributeValue.builder().b(software.amazon.awssdk.core.SdkBytes.fromByteArray(blob)).build());
        nested.put("name", AttributeValue.builder().s("n1").build());

        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("id", AttributeValue.builder().s("u1").build());
        item.put("attrs", AttributeValue.builder().m(nested).build());

        final TypedCollectionEntity result = DynamoDBExecutor.toEntity(item, TypedCollectionEntity.class);

        assertNotNull(result);
        assertNotNull(result.getAttrs());
        assertEquals("n1", result.getAttrs().get("name"));

        final Object blobValue = result.getAttrs().get("blob");
        assertEquals(byte[].class, blobValue == null ? null : blobValue.getClass(),
                "blob should stay byte[], was " + (blobValue == null ? "null" : blobValue.getClass()));
        assertTrue(java.util.Arrays.equals(blob, (byte[]) blobValue));
    }

    // A getter-only property inherited from an @Entity superclass is written by toItem; reading the item back used to fail
    // the whole entity with UnsupportedOperationException from PropInfo.setPropValue.
    @Test
    public void testToEntity_ReadOnlyInheritedPropertyAttributeIsSkipped() {
        final ReadOnlyPropSubEntity source = new ReadOnlyPropSubEntity();
        source.setId("id-1");
        source.setName("n");

        final Map<String, AttributeValue> item = DynamoDBExecutor.toItem(source);
        assertEquals("computed:id-1", item.get("computed").s());

        final ReadOnlyPropSubEntity result = DynamoDBExecutor.toEntity(item, ReadOnlyPropSubEntity.class);
        assertEquals("id-1", result.getId());
        assertEquals("n", result.getName());
        assertEquals("computed:id-1", result.getComputed());

        when(mockDynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(item).build());
        final ReadOnlyPropSubEntity fromMapper = executor.mapper(ReadOnlyPropSubEntity.class).getItem(source);
        assertEquals("n", fromMapper.getName());
        assertEquals("computed:id-1", fromMapper.getComputed());
    }

    @Test
    public void testToEntity_StringArrayPropertyFromEmptyJsonArrayAndPlainText() {
        assertEquals(0, DynamoDBExecutor.toEntity(Map.of("tags", AttributeValue.fromS("[]")), StringArrayEntity.class).getTags().length);

        // A plain (non-JSON-array) S value keeps the lenient single-element conversion.
        assertArrayEquals(new String[] { "plain, text" },
                DynamoDBExecutor.toEntity(Map.of("tags", AttributeValue.fromS("plain, text")), StringArrayEntity.class).getTags());
    }

    // toItem stores arrays as JSON text; N.convert(String, String[].class) wrapped that whole text as ONE element.

    @Test
    public void testToEntity_StringArrayPropertyRoundTripsThroughJsonText() {
        final StringArrayEntity source = new StringArrayEntity();
        source.setId("1");
        source.setTags(new String[] { "a", "b,c" });
        source.setValues(new Object[] { "x", 1 });
        source.setNames(new CharSequence[] { "n" });

        final Map<String, AttributeValue> item = DynamoDBExecutor.toItem(source);
        assertEquals("[\"a\", \"b,c\"]", item.get("tags").s());

        final StringArrayEntity read = DynamoDBExecutor.toEntity(item, StringArrayEntity.class);

        assertArrayEquals(new String[] { "a", "b,c" }, read.getTags());
        assertArrayEquals(new Object[] { "x", 1 }, read.getValues());
        assertArrayEquals(new CharSequence[] { "n" }, read.getNames());
    }

    // Regression: v2 toEntity previously called toValue(propValue, propInfo.clazz) for every property,
    // so native NS/L/M attributes populated typed collection/map properties (List<Integer>, Set<Long>,
    // Map<String,Long>) with raw String elements — heap pollution surfacing as a ClassCastException on
    // read. v1 had the parameterized-Type rebuild; v2 was missing it. The fix ports v1's logic to v2.
    @Test
    public void testToEntity_TypedCollectionFromNsAndL_NoHeapPollution() {
        final Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("id", AttributeValue.builder().s("u1").build());
        // NS -> Set<Long> tags; L -> List<Integer> scores; M -> Map<String,Long> ratings.
        item.put("tags", AttributeValue.builder().ns("1", "2", "3").build());
        item.put("scores", AttributeValue.builder().l(AttributeValue.builder().n("10").build(), AttributeValue.builder().n("20").build()).build());
        item.put("ratings",
                AttributeValue.builder().m(Map.of("a", AttributeValue.builder().n("7").build(), "b", AttributeValue.builder().n("8").build())).build());

        final TypedCollectionEntity result = DynamoDBExecutor.toEntity(item, TypedCollectionEntity.class);

        assertNotNull(result);
        assertEquals("u1", result.getId());

        assertNotNull(result.getTags());
        assertEquals(3, result.getTags().size());
        // Elements must be Long, not String (heap pollution before the fix).
        for (final Long tag : result.getTags()) {
            assertEquals(Long.class, tag.getClass(), "tag element should be Long, was " + (tag == null ? "null" : tag.getClass()));
        }
        assertTrue(result.getTags().contains(1L));
        assertTrue(result.getTags().contains(2L));
        assertTrue(result.getTags().contains(3L));

        assertNotNull(result.getScores());
        assertEquals(2, result.getScores().size());
        for (final Integer score : result.getScores()) {
            assertEquals(Integer.class, score.getClass(), "score element should be Integer, was " + (score == null ? "null" : score.getClass()));
        }
        assertEquals(Integer.valueOf(10), result.getScores().get(0));
        assertEquals(Integer.valueOf(20), result.getScores().get(1));

        assertNotNull(result.getRatings());
        assertEquals(2, result.getRatings().size());
        // Values must be Long, not String (Map branch of the rebuild).
        for (final Long rating : result.getRatings().values()) {
            assertEquals(Long.class, rating.getClass(), "rating value should be Long, was " + (rating == null ? "null" : rating.getClass()));
        }
        assertEquals(Long.valueOf(7), result.getRatings().get("a"));
        assertEquals(Long.valueOf(8), result.getRatings().get("b"));
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
    public void testToEntityWithEmptyResponse() {
        GetItemResponse response = GetItemResponse.builder().build();
        TestEntity result = DynamoDBExecutor.toEntity(response, TestEntity.class);
        assertNull(result);
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
    public void testToList_AsMapClass() {
        Map<String, AttributeValue> r = new java.util.LinkedHashMap<>();
        r.put("a", AttributeValue.builder().s("x").build());
        QueryResponse qr = QueryResponse.builder().items(List.of(r)).build();

        @SuppressWarnings({ "unchecked", "rawtypes" })
        List<Map> result = DynamoDBExecutor.toList(qr, (Class) Map.class);
        assertEquals(1, result.size());
        assertEquals("x", result.get(0).get("a"));
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
    public void testToList_AsSingleValueClass() {
        Map<String, AttributeValue> r = new java.util.LinkedHashMap<>();
        r.put("v", AttributeValue.builder().s("hello").build());
        QueryResponse qr = QueryResponse.builder().items(List.of(r)).build();

        List<String> result = DynamoDBExecutor.toList(qr, String.class);
        assertEquals(1, result.size());
        assertEquals("hello", result.get(0));
    }

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
    public void testToList_QueryResponseValidatesRangeBeforeEmptyResult() {
        final QueryResponse empty = QueryResponse.builder().build();

        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toList(empty, -1, 1, TestEntity.class));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toList((QueryResponse) null, 0, -1, TestEntity.class));
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

    @Test
    public void testToList_ScanResponseValidatesRangeBeforeEmptyResult() {
        final ScanResponse empty = ScanResponse.builder().build();

        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toList(empty, -1, 1, TestEntity.class));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toList((ScanResponse) null, 0, -1, TestEntity.class));
    }

    @Test
    public void testToList_SingleValueMultiColumnThrows() {
        Map<String, AttributeValue> r = new java.util.LinkedHashMap<>();
        r.put("a", AttributeValue.builder().s("1").build());
        r.put("b", AttributeValue.builder().s("2").build());
        QueryResponse qr = QueryResponse.builder().items(List.of(r)).build();

        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toList(qr, String.class));
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
    public void testToMapWithNull() {
        Map<String, Object> result = DynamoDBExecutor.toMap((Map<String, AttributeValue>) null);
        assertNull(result);
    }

    @Test
    public void testToMapWithNullMapSupplier() {
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toMap(new HashMap<>(), null));
    }

    @Test
    public void testToMapWithObjectArray() {
        Object[] propNameAndValues = { "id", "123", "name", "test" };
        Map<String, Object> result = DynamoDBExecutorBase.asProps(propNameAndValues);

        assertNotNull(result);
        assertEquals("123", result.get("id"));
        assertEquals("test", result.get("name"));
    }

    // ---- 2026-10-19 sliceC ----

    @Test
    public void testToAttributeValue_RejectsNonFiniteValuesOfEveryNumericType_sliceC() {
        final java.util.concurrent.atomic.DoubleAdder nanAdder = new java.util.concurrent.atomic.DoubleAdder();
        nanAdder.add(Double.NaN);

        org.junit.jupiter.api.Assertions.assertAll(
                // MutableDouble/MutableFloat/DoubleAdder are numeric types, but only Double and Float were checked: they produced N=NaN /
                // N=Infinity, which DynamoDB rejects service-side instead of the documented IllegalArgumentException.
                () -> assertThrows(IllegalArgumentException.class,
                        () -> DynamoDBExecutor.toAttributeValue(com.landawn.abacus.util.MutableDouble.of(Double.NaN))),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> DynamoDBExecutor.toAttributeValue(com.landawn.abacus.util.MutableDouble.of(Double.POSITIVE_INFINITY))),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> DynamoDBExecutor.toAttributeValue(com.landawn.abacus.util.MutableFloat.of(Float.NEGATIVE_INFINITY))),
                () -> assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toAttributeValue(nanAdder)),
                () -> assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("k", com.landawn.abacus.util.MutableDouble.of(Double.NaN))),
                () -> assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.Filters.eq("a", com.landawn.abacus.util.MutableFloat.of(Float.NaN))),
                // Unchanged: Double/Float non-finite values keep their message; finite values of every numeric type are N attributes.
                () -> assertEquals("DynamoDB numbers must be finite: NaN",
                        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toAttributeValue(Double.NaN)).getMessage()),
                () -> assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toAttributeValue(Float.POSITIVE_INFINITY)),
                () -> assertEquals("1.5", DynamoDBExecutor.toAttributeValue(com.landawn.abacus.util.MutableDouble.of(1.5)).n()),
                () -> assertEquals("-0.0", DynamoDBExecutor.toAttributeValue(-0.0d).n()),
                () -> assertEquals("1E+3", DynamoDBExecutor.toAttributeValue(new java.math.BigDecimal("1E+3")).n()),
                // A wrapped NaN is not a numeric attribute: it is still stored as its S text.
                () -> assertEquals("NaN", DynamoDBExecutor.toAttributeValue(java.util.Optional.of(Double.NaN)).s()));
    }

    @Test
    public void testAsKey_NonEmptyBinaryKeysAreAcceptedWithoutConsumingBuffers_sliceC() {
        final ByteBuffer buffer = ByteBuffer.wrap(new byte[] { 7, 8, 9 });
        buffer.get();

        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertArrayEquals(new byte[] { 1 }, DynamoDBExecutor.asKey("k", new byte[] { 1 }).get("k").b().asByteArray()),
                () -> assertArrayEquals(new byte[] { 8, 9 }, DynamoDBExecutor.asKey("k", buffer).get("k").b().asByteArray()),
                () -> assertEquals(1, buffer.position()),
                () -> assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("k", ByteBuffer.allocate(0))),
                () -> assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("k", new byte[0])));
    }

    @Test
    public void testToItem_SizedResultsKeepEveryAttributeInOrder_sliceC() {
        final Map<String, Object> map = new LinkedHashMap<>();

        for (int i = 0; i < 40; i++) {
            map.put("a" + i, i);
        }

        final Map<String, AttributeValue> item = DynamoDBExecutor.toItem(map);
        final Map<String, AttributeValueUpdate> updateItem = DynamoDBExecutor.toUpdateItem(map, NamingPolicy.SCREAMING_SNAKE_CASE);

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(new ArrayList<>(map.keySet()), new ArrayList<>(item.keySet())),
                () -> assertEquals("39", item.get("a39").n()), () -> assertEquals(40, updateItem.size()),
                () -> assertEquals("A39", new ArrayList<>(updateItem.keySet()).get(39)), () -> assertTrue(DynamoDBExecutor.toItem(new HashMap<>()).isEmpty()),
                // The returned maps stay mutable (Mapper.updateItem removes key attributes from them).
                () -> assertDoesNotThrow(() -> DynamoDBExecutor.toItem(new HashMap<>()).put("x", AttributeValue.fromS("y"))),
                () -> assertThrows(ClassCastException.class, () -> DynamoDBExecutor.toItem(Map.of(1, 2))));
    }

    public static class StringArrayEntity {
        private String id;
        private String[] tags;
        private Object[] values;
        private CharSequence[] names;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String[] getTags() {
            return tags;
        }

        public void setTags(final String[] tags) {
            this.tags = tags;
        }

        public Object[] getValues() {
            return values;
        }

        public void setValues(final Object[] values) {
            this.values = values;
        }

        public CharSequence[] getNames() {
            return names;
        }

        public void setNames(final CharSequence[] names) {
            this.names = names;
        }
    }

    public static class BinaryContainerEntity {
        private List<byte[]> bytes;
        private List<ByteBuffer> buffers;
        private byte[][] byteArray;
        private ByteBuffer[] bufferArray;
        private java.util.Set<ByteBuffer> bufferSet;
        private Map<Integer, byte[]> byteMap;
        private Map<String, ByteBuffer> bufferMap;
        private List<Object> rawValues;
        private Object[] rawArray;

        public List<byte[]> getBytes() {
            return bytes;
        }

        public void setBytes(final List<byte[]> bytes) {
            this.bytes = bytes;
        }

        public List<ByteBuffer> getBuffers() {
            return buffers;
        }

        public void setBuffers(final List<ByteBuffer> buffers) {
            this.buffers = buffers;
        }

        public byte[][] getByteArray() {
            return byteArray;
        }

        public void setByteArray(final byte[][] byteArray) {
            this.byteArray = byteArray;
        }

        public ByteBuffer[] getBufferArray() {
            return bufferArray;
        }

        public void setBufferArray(final ByteBuffer[] bufferArray) {
            this.bufferArray = bufferArray;
        }

        public java.util.Set<ByteBuffer> getBufferSet() {
            return bufferSet;
        }

        public void setBufferSet(final java.util.Set<ByteBuffer> bufferSet) {
            this.bufferSet = bufferSet;
        }

        public Map<Integer, byte[]> getByteMap() {
            return byteMap;
        }

        public void setByteMap(final Map<Integer, byte[]> byteMap) {
            this.byteMap = byteMap;
        }

        public Map<String, ByteBuffer> getBufferMap() {
            return bufferMap;
        }

        public void setBufferMap(final Map<String, ByteBuffer> bufferMap) {
            this.bufferMap = bufferMap;
        }

        public List<Object> getRawValues() {
            return rawValues;
        }

        public void setRawValues(final List<Object> rawValues) {
            this.rawValues = rawValues;
        }

        public Object[] getRawArray() {
            return rawArray;
        }

        public void setRawArray(final Object[] rawArray) {
            this.rawArray = rawArray;
        }
    }

    private static class ByteBufferEntity {
        private String id;
        private ByteBuffer buf;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public ByteBuffer getBuf() {
            return buf;
        }

        public void setBuf(final ByteBuffer buf) {
            this.buf = buf;
        }
    }

    @com.landawn.abacus.annotation.Entity
    public static class ReadOnlyPropBaseEntity {
        @com.landawn.abacus.annotation.Id
        private String id;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public String getComputed() {
            return "computed:" + id;
        }
    }

    @com.landawn.abacus.annotation.Table(name = "TestTable")
    public static class ReadOnlyPropSubEntity extends ReadOnlyPropBaseEntity {
        private String name;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    /** A value type whose string form is empty, so it converts to an empty {@code S} key attribute. */
    public static final class BlankKeyValue_coverageDC {
        @Override
        public String toString() {
            return "";
        }
    }

    public static class BinaryKeyEntity_coverageDC {
        @com.landawn.abacus.annotation.Id
        private byte[] id;

        public byte[] getId() {
            return id;
        }

        public void setId(final byte[] id) {
            this.id = id;
        }
    }

    // ---- 2026-10-20 ddbfix ----

    /**
     * Pins the documented ClassCastException of toItem/toUpdateItem: a Map entity's keys become the attribute names as they
     * are (or converted by the naming policy), never converted to text, so a non-String key fails on both overloads.
     */
    @Test
    public void testToItemAndToUpdateItem_NonStringMapKeyThrowsClassCastException_ddbfix() {
        final Map<Object, Object> nonStringKey = new HashMap<>();
        nonStringKey.put(1, "a");

        assertThrows(ClassCastException.class, () -> DynamoDBExecutor.toItem(nonStringKey));
        assertThrows(ClassCastException.class, () -> DynamoDBExecutor.toItem(nonStringKey, NamingPolicy.SNAKE_CASE));
        assertThrows(ClassCastException.class, () -> DynamoDBExecutor.toUpdateItem(nonStringKey));
        assertThrows(ClassCastException.class, () -> DynamoDBExecutor.toUpdateItem(nonStringKey, NamingPolicy.SNAKE_CASE));

        // String keys are attribute names, converted only by the naming policy.
        assertEquals(Map.of("first_name", AttributeValue.fromS("a")), DynamoDBExecutor.toItem(Map.of("firstName", "a"), NamingPolicy.SNAKE_CASE));
        assertEquals(Map.of("firstName", AttributeValue.fromS("a")), DynamoDBExecutor.toItem(Map.of("firstName", "a")));
    }

    /**
     * extractData hands its freshly built columns to RowDataset.fromOwnedColumns (no defensive copy): the returned Dataset
     * must still be a mutable, independent result, and two calls on the same response must not share column lists.
     */
    @Test
    public void testExtractData_ReturnsMutableIndependentDataset_ddbfix() {
        final QueryResponse queryResponse = QueryResponse.builder()
                .items(List.of(Map.of("id", AttributeValue.fromS("1"), "name", AttributeValue.fromS("Test1")),
                        Map.of("id", AttributeValue.fromS("2"), "name", AttributeValue.fromS("Test2"))))
                .build();
        final ScanResponse scanResponse = ScanResponse.builder().items(queryResponse.items()).build();

        final Dataset first = DynamoDBExecutor.extractData(queryResponse);
        final Dataset second = DynamoDBExecutor.extractData(queryResponse);
        final Dataset scanned = DynamoDBExecutor.extractData(scanResponse);

        // Column order follows the first item's attribute iteration order (a Map.of), so address columns by name.
        final int idColumn = first.getColumnIndex("id");
        final int nameColumn = first.getColumnIndex("name");
        final Object[] newRow = new Object[2];
        newRow[idColumn] = "3";
        newRow[nameColumn] = "Test3";

        assertFalse(first.isFrozen());
        first.set(0, idColumn, "changed");
        first.addRow(newRow);
        first.addColumn("extra", List.of("x", "y", "z"));

        assertEquals(3, first.size());
        assertEquals("changed", first.get(0, idColumn));
        assertEquals("3", first.get(2, idColumn));
        assertEquals(3, first.columnCount());

        assertEquals(2, second.size());
        assertEquals("1", second.get(0, second.getColumnIndex("id")));
        assertEquals(2, second.columnCount());
        assertEquals(List.of("1", "2"), second.getColumn("id"));
        assertEquals(List.of("1", "2"), scanned.getColumn("id"));
        assertFalse(scanned.isFrozen());
        scanned.set(1, scanned.getColumnIndex("name"), "other");
        assertEquals("Test2", second.get(1, second.getColumnIndex("name")));
    }

    /**
     * Pins toList(QueryResponse/ScanResponse, Class) on responses without rows: no Items member and an empty Items list give
     * an empty list, and a hand-built response holding a null item (which the SDK stores as an empty map, so no NPE can
     * occur in the row mappers) yields an attribute-less bean, Map, array or collection, and is rejected for a single-value
     * row type because it has no column; extractData gives that row null cells.
     */
    @Test
    public void testToList_NoItemsEmptyItemsAndAttributeLessItem_ddbfix() {
        assertEquals(List.of(), DynamoDBExecutor.toList(QueryResponse.builder().build(), TestEntity.class));
        assertEquals(List.of(), DynamoDBExecutor.toList(QueryResponse.builder().items(List.of()).build(), TestEntity.class));
        assertEquals(List.of(), DynamoDBExecutor.toList(ScanResponse.builder().build(), String.class));
        assertEquals(List.of(), DynamoDBExecutor.toList(ScanResponse.builder().items(new ArrayList<>()).build(), Map.class));

        final List<Map<String, AttributeValue>> items = new ArrayList<>();
        items.add(Map.of("id", AttributeValue.fromS("1")));
        items.add(null);
        final QueryResponse queryResponse = QueryResponse.builder().items(items).build();
        final ScanResponse scanResponse = ScanResponse.builder().items(items).build();

        assertEquals(2, queryResponse.items().size());
        assertNotNull(queryResponse.items().get(1));
        assertTrue(queryResponse.items().get(1).isEmpty());

        final List<TestEntity> beans = DynamoDBExecutor.toList(queryResponse, TestEntity.class);
        assertEquals(2, beans.size());
        assertEquals("1", beans.get(0).getId());
        assertNotNull(beans.get(1));
        assertNull(beans.get(1).getId());

        assertEquals(List.of(Map.of("id", "1"), Map.of()), DynamoDBExecutor.toList(scanResponse, Map.class));
        assertEquals(0, ((Object[]) DynamoDBExecutor.toList(queryResponse, Object[].class).get(1)).length);
        assertEquals(List.of(), DynamoDBExecutor.toList(queryResponse, List.class).get(1));
        assertEquals(List.of("1"), DynamoDBExecutor.toList(queryResponse, 0, 1, String.class));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toList(queryResponse, String.class)).getMessage()
                .contains("the row has 0 columns"));

        final Dataset dataset = DynamoDBExecutor.extractData(queryResponse);
        assertEquals(2, dataset.size());
        assertEquals("1", dataset.get(0, 0));
        assertNull(dataset.get(1, 0));
    }
}
