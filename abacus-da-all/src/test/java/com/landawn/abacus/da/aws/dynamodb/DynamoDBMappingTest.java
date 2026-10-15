package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.NamingPolicy;

public class DynamoDBMappingTest extends DynamoDBExecutorTestSupport {

    // Construction and configuration

    @Test
    public void testToItem_CollectionDefaultNamingPolicy() {
        TestEntity e1 = new TestEntity();
        e1.setId("1");
        TestEntity e2 = new TestEntity();
        e2.setId("2");

        List<Map<String, AttributeValue>> result = DynamoDBExecutor.toItem(List.of(e1, e2));
        assertEquals(2, result.size());
        assertEquals("1", result.get(0).get("id").getS());
        assertEquals("2", result.get(1).get("id").getS());
    }

    @Test
    public void testToItem_CollectionWithNamingPolicy_SnakeCase() {
        TestEntity e = new TestEntity();
        e.setFirstName("John");
        List<Map<String, AttributeValue>> result = DynamoDBExecutor.toItem(List.of(e), NamingPolicy.SNAKE_CASE);
        assertEquals(1, result.size());
        assertEquals("John", result.get(0).get("first_name").getS());
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
    public void testToUpdateItem_CollectionDefaultNamingPolicy() {
        TestEntity e1 = new TestEntity();
        e1.setId("1");
        TestEntity e2 = new TestEntity();
        e2.setId("2");

        List<Map<String, AttributeValueUpdate>> result = DynamoDBExecutor.toUpdateItem(List.of(e1, e2));
        assertEquals(2, result.size());
        assertEquals("1", result.get(0).get("id").getValue().getS());
        assertEquals("2", result.get(1).get("id").getValue().getS());
    }

    @Test
    public void testToUpdateItem_CollectionWithNamingPolicy_SnakeCase() {
        TestEntity e = new TestEntity();
        e.setFirstName("John");
        List<Map<String, AttributeValueUpdate>> result = DynamoDBExecutor.toUpdateItem(List.of(e), NamingPolicy.SNAKE_CASE);
        assertEquals(1, result.size());
        assertEquals("John", result.get(0).get("first_name").getValue().getS());
    }

    // Attribute and key conversion

    @Test
    public void testAsItem() {
        Map<String, AttributeValue> result = DynamoDBExecutor.asItem("id", "123");
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("123", result.get("id").getS());
    }

    @Test
    public void testAsItemAsUpdateItem_NullOrEmptyAttrNameThrowsIAE() {
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asItem(null, "v"));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asItem("", "v"));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asItem("a", 1, null, 2));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asItem("a", 1, "b", 2, "", 3));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asItem((Object[]) null));

        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asUpdateItem(null, "v"));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asUpdateItem("", "v"));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asUpdateItem("a", 1, null, 2));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asUpdateItem("a", 1, "b", 2, "", 3));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asUpdateItem((Object[]) null));
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
    public void testAsKey() {
        Map<String, AttributeValue> result = DynamoDBExecutor.asKey("id", "123");
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("123", result.get("id").getS());
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
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("id", Double.NaN));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("id", ByteBuffer.allocate(0)));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("id", "1", "id", "2"));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey((Object[]) null));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey(new Object[0]));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey(new Object[] { "a", 1, "b", 2, "c", 3 }));
    }

    // Twin of testAsKey_NonScalarMessageNamesArrayTypeReadably: the second toKeyAttributeValue message (a scalar converting to an empty
    // or non-key attribute) still used Class.getName(), so a nested value class read "received ...DynamoDBExecutorTest$BlankKeyValue_coverageDC".
    @Test
    public void testAsKey_EmptyOrNonKeyValueMessageNamesClassReadably_coverageDC() {
        final String prefix = "DynamoDB key attribute 'id' must be a non-empty String, finite Number, or non-empty binary value; received ";

        assertEquals(prefix + BlankKeyValue_coverageDC.class.getCanonicalName(),
                assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("id", new BlankKeyValue_coverageDC())).getMessage());
        // The composite-key overload validates its sort key through the same check.
        assertEquals(prefix.replace("'id'", "'sk'") + BlankKeyValue_coverageDC.class.getCanonicalName(),
                assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.asKey("id", "1", "sk", new BlankKeyValue_coverageDC())).getMessage());

        // Top-level classes read the same as before.
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
    public void testAsKeyVarargs_Explicit() {
        Map<String, AttributeValue> result = DynamoDBExecutor.asKey(new Object[] { "k1", "v1", "k2", "v2" });
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("v1", result.get("k1").getS());
        assertEquals("v2", result.get("k2").getS());
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
        assertEquals("123", result.get("id").getS());
        assertEquals("test", result.get("name").getS());
    }

    @Test
    public void testAsUpdateItem() {
        Map<String, AttributeValueUpdate> result = DynamoDBExecutor.asUpdateItem("name", "test");
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("test", result.get("name").getValue().getS());
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
    public void testAttrValueOfNull() {
        AttributeValue result = DynamoDBExecutor.toAttributeValue(null);
        assertNotNull(result);
        assertTrue(result.getNULL());
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
    public void testAttrValueOfNumber() {
        AttributeValue result = DynamoDBExecutor.toAttributeValue(123);
        assertNotNull(result);
        assertEquals("123", result.getN());

        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toAttributeValue(Double.NaN));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toAttributeValue(Float.POSITIVE_INFINITY));
    }

    @Test
    public void testAttrValueOfString() {
        AttributeValue result = DynamoDBExecutor.toAttributeValue("test");
        assertNotNull(result);
        assertEquals("test", result.getS());
    }

    // toItem writes a String[]/Object[] property as its JSON text; N.convert(String, String[].class) wraps that whole
    // text as a single element instead of parsing it, so the array came back as {"[\"a\", \"b\"]"}.
    @Test
    public void testToEntity_StringAndObjectArrayPropsRoundTripThroughToItem() {
        ObjectArrayPropsEntity source = new ObjectArrayPropsEntity();
        source.setId("id-1");
        source.setTags(new String[] { "a", "b, c" });
        source.setValues(new Object[] { "s", 2 });
        source.setNums(new Integer[] { 1, 2 });
        source.setEmptyTags(new String[0]);

        Map<String, AttributeValue> item = DynamoDBExecutor.toItem(source);
        assertEquals("[\"a\", \"b, c\"]", item.get("tags").getS());

        ObjectArrayPropsEntity result = DynamoDBExecutor.toEntity(item, ObjectArrayPropsEntity.class);

        assertArrayEquals(new String[] { "a", "b, c" }, result.getTags());
        assertArrayEquals(new Object[] { "s", 2 }, result.getValues());
        assertArrayEquals(new Integer[] { 1, 2 }, result.getNums());
        assertArrayEquals(new String[0], result.getEmptyTags());

        Map<String, AttributeValue> padded = new LinkedHashMap<>();
        padded.put("tags", new AttributeValue().withS(" [\"x\", \"y\"] "));
        assertArrayEquals(new String[] { "x", "y" }, DynamoDBExecutor.toEntity(padded, ObjectArrayPropsEntity.class).getTags());
    }

    // toItem(Object, NamingPolicy) Map branch (camel-case and non-camel-case branches)
    @Test
    public void testToItem_FromMapWithSnakeCase() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("firstName", "John");
        map.put("lastName", "Doe");
        Map<String, AttributeValue> result = DynamoDBExecutor.toItem(map, NamingPolicy.SNAKE_CASE);
        assertEquals("John", result.get("first_name").getS());
        assertEquals("Doe", result.get("last_name").getS());
    }

    @Test
    public void testToItem_UnsupportedTypeThrows() {
        // String entity is not a bean, not a Map, not an Object[]
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toItem("notSupported", NamingPolicy.CAMEL_CASE));
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
    public void testToItemToUpdateItemToEntity_NullArgsThrowIAE() {
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toItem((Object) null));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toItem((Object) null, NamingPolicy.CAMEL_CASE));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toItem((Object) new TestEntity(), null));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toUpdateItem((Object) null));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toUpdateItem((Object) null, NamingPolicy.CAMEL_CASE));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toUpdateItem((Object) new TestEntity(), null));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toEntity((GetItemResult) null, (Class<TestEntity>) null));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toEntity((Map<String, AttributeValue>) null, (Class<TestEntity>) null));
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
    public void testToUpdateItem() {
        TestEntity entity = new TestEntity();
        entity.setId("123");
        entity.setName("test");
        Map<String, AttributeValueUpdate> result = DynamoDBExecutor.toUpdateItem(entity);

        assertNotNull(result);
        assertEquals("123", result.get("id").getValue().getS());
        assertEquals("test", result.get("name").getValue().getS());
    }

    // toUpdateItem: bean + map + array + unsupported branches
    @Test
    public void testToUpdateItem_FromMapCamelCase() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("name", "Alice");
        Map<String, AttributeValueUpdate> result = DynamoDBExecutor.toUpdateItem(map);
        assertEquals("Alice", result.get("name").getValue().getS());
    }

    @Test
    public void testToUpdateItem_FromMapSnakeCase() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("firstName", "John");
        Map<String, AttributeValueUpdate> result = DynamoDBExecutor.toUpdateItem(map, NamingPolicy.SNAKE_CASE);
        assertEquals("John", result.get("first_name").getValue().getS());
    }

    @Test
    public void testToUpdateItem_FromObjectArray() {
        Object[] arr = { "key1", "v1", "key2", 2 };
        Map<String, AttributeValueUpdate> result = DynamoDBExecutor.toUpdateItem(arr);
        assertEquals(2, result.size());
        assertEquals("v1", result.get("key1").getValue().getS());
        assertEquals("2", result.get("key2").getValue().getN());
    }

    @Test
    public void testToUpdateItem_UnsupportedTypeThrows() {
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toUpdateItem("notSupported"));
    }

    @Test
    public void testToValue_EmptyBS_FallsThrough() {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        AttributeValue v = new AttributeValue();
        v.setBS(new ArrayList<>());
        item.put("k", v);
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        assertNotNull(result.get("k"));
    }

    @Test
    public void testToValue_EmptyL_FallsThrough() {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        AttributeValue v = new AttributeValue();
        v.setL(new ArrayList<>());
        item.put("k", v);
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        assertNotNull(result.get("k"));
    }

    @Test
    public void testToValue_EmptyM_FallsThrough() {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        AttributeValue v = new AttributeValue();
        v.setM(new HashMap<>());
        item.put("k", v);
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        assertNotNull(result.get("k"));
    }

    @Test
    public void testToValue_EmptyNS_FallsThrough() {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        AttributeValue v = new AttributeValue();
        v.setNS(new ArrayList<>());
        item.put("k", v);
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        assertNotNull(result.get("k"));
    }

    @Test
    public void testToValue_EmptyNumberN_FallsThrough() {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        AttributeValue v = new AttributeValue();
        v.setN("");
        item.put("k", v);
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        assertEquals("", result.get("k"));
    }

    @Test
    public void testToValue_EmptySS_FallsThrough() {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        AttributeValue v = new AttributeValue();
        v.setSS(new ArrayList<>()); // empty list (not null) => N.notEmpty false, getSS!=null true
        item.put("k", v);
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        assertNotNull(result.get("k"));
    }

    @Test
    public void testToValue_EmptyStringS_FallsThroughToNotNullBranch() {
        // empty string => Strings.isNotEmpty fails, getS()!=null is true (line 1444 branch)
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("k", new AttributeValue().withS(""));
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        assertEquals("", result.get("k"));
    }

    @Test
    public void testToValue_NullAttributeViaToMap() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("nullVal", new AttributeValue().withNULL(Boolean.TRUE));
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        assertNull(result.get("nullVal"));
    }

    @Test
    public void testToValue_BinaryByteBufferViaToMap() {
        ByteBuffer buf = ByteBuffer.wrap(new byte[] { 9, 8, 7 });
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("b", new AttributeValue().withB(buf));
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        assertEquals(buf, result.get("b"));
    }

    // toValue: BOOL, SS, NS, BS, L, M, and null with target class default-value branches.
    @Test
    public void testToValue_BoolViaToMap() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("flag", new AttributeValue().withBOOL(Boolean.TRUE));
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        assertEquals(Boolean.TRUE, result.get("flag"));
    }

    /**
     * Regression test for toValue's M branch: it previously instantiated the result map from the
     * input map's runtime class via N.newMap(attrMap.getClass(), size). A caller-built AttributeValue
     * holding an immutable map (e.g. java.util.Map.of(...)) made that instantiation throw; the branch
     * now copies into a LinkedHashMap regardless of the input map's concrete class.
     */
    @Test
    public void testToValue_ImmutableMapInMAttribute_NoLongerThrows() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("m", new AttributeValue().withM(Map.of("a", new AttributeValue("x"))));

        Map<String, Object> result = assertDoesNotThrow(() -> DynamoDBExecutor.toMap(item));

        Map<?, ?> inner = (Map<?, ?>) result.get("m");
        assertNotNull(inner);
        assertEquals("x", inner.get("a"));
    }

    @Test
    public void testToValue_ListViaToMap() {
        AttributeValue listAttr = new AttributeValue().withL(new AttributeValue().withS("x"), new AttributeValue().withN("5"));
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("l", listAttr);
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        List<?> list = (List<?>) result.get("l");
        assertEquals(2, list.size());
        assertEquals("x", list.get(0));
    }

    @Test
    public void testToValue_MapViaToMap() {
        Map<String, AttributeValue> nested = new HashMap<>();
        nested.put("k", new AttributeValue().withS("v"));
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("m", new AttributeValue().withM(nested));
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        Map<?, ?> innerMap = (Map<?, ?>) result.get("m");
        assertEquals("v", innerMap.get("k"));
    }

    @Test
    public void testToValue_NumberSetViaToMap() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ns", new AttributeValue().withNS("1", "2", "3"));
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        assertEquals(3, ((List<?>) result.get("ns")).size());
    }

    @Test
    public void testToValue_StringSetViaToMap() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ss", new AttributeValue().withSS("a", "b"));
        Map<String, Object> result = DynamoDBExecutor.toMap(item);
        assertNotNull(result.get("ss"));
        assertEquals(2, ((List<?>) result.get("ss")).size());
    }

    @Test
    public void testToValue_TargetClassConversion() {
        // Single-column ret is String "42" - targetClass Integer => N.convert
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("count", new AttributeValue().withN("42"));

        when(mockDynamoDBClient.getItem(eq("T"), any(Map.class))).thenReturn(new GetItemResult().withItem(item));
        Integer result = executor.getItem("T", Map.of("id", new AttributeValue().withS("1")), Integer.class);
        assertEquals(42, result);
    }

    // Row and entity mapping

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

    // extractData edge cases
    @Test
    public void testExtractData_EmptyItems() {
        Dataset ds = DynamoDBExecutor.extractData(new QueryResult().withItems(new ArrayList<>()));
        assertNotNull(ds);
        assertEquals(0, ds.size());
    }

    @Test
    public void testExtractData_NegativeOffsetThrows() {
        QueryResult qr = new QueryResult().withItems(List.of(Map.of("id", new AttributeValue().withS("1"))));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.extractData(qr, -1, 1));
    }

    @Test
    public void testExtractData_OffsetBeyondSize() {
        QueryResult qr = new QueryResult();
        qr.setItems(List.of(Map.of("id", new AttributeValue().withS("1"))));
        Dataset ds = DynamoDBExecutor.extractData(qr, 5, 10);
        assertNotNull(ds);
        assertEquals(0, ds.size());
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
    public void testToEntity_EmptyBinaryContainersRemainEmpty() {
        final AttributeValue emptyList = new AttributeValue().withL(List.<AttributeValue> of());
        final AttributeValue emptyMap = new AttributeValue().withM(Map.of());

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
    public void testToEntity_NullGetItemResultReturnsNull() {
        assertNull(DynamoDBExecutor.toEntity((GetItemResult) null, TestEntity.class));
    }

    // toEntity: null item branch and column-name mapping branches
    @Test
    public void testToEntity_NullItemReturnsNull() {
        TestEntity result = DynamoDBExecutor.toEntity((Map<String, AttributeValue>) null, TestEntity.class);
        assertNull(result);
    }

    // N.convert(ByteBuffer, byte[].class) returns null in abacus-common, so a binary attribute read into a
    // byte[] property / byte[] target used to be silently dropped.
    @Test
    public void testToEntity_BinaryAttributeToByteArrayProperty() {
        final ByteBuffer buf = ByteBuffer.wrap(new byte[] { 1, 2, -3 });
        final Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue().withS("1"));
        item.put("data", new AttributeValue().withB(buf));

        final BinaryEntity entity = DynamoDBExecutor.toEntity(item, BinaryEntity.class);

        assertEquals("1", entity.getId());
        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[] { 1, 2, -3 }, entity.getData());
        assertEquals(0, buf.position()); // the attribute's buffer is not consumed
    }

    // isJsonArrayText only checks the brackets: plain text such as "[a] and [b]" read into a String[] property used to throw
    // a ParsingException (failing the whole item) instead of keeping the lenient single-element conversion.
    @Test
    public void testToEntity_BracketedNonJsonTextIntoStringArrayKeepsSingleElement() {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("tags", new AttributeValue().withS("[a] and [b]"));
        item.put("values", new AttributeValue().withS("[\"unterminated]"));

        ObjectArrayPropsEntity result = DynamoDBExecutor.toEntity(item, ObjectArrayPropsEntity.class);

        assertArrayEquals(new String[] { "[a] and [b]" }, result.getTags());
        assertArrayEquals(new Object[] { "[\"unterminated]" }, result.getValues());
        assertArrayEquals(new String[] { "[x] [y]" }, DynamoDBExecutor.toValue(new AttributeValue().withS("[x] [y]"), String[].class));
    }

    @Test
    public void testToEntity_DotNotationPropName() {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("id", new AttributeValue().withS("1"));
        // dot in name forces the entityInfo.setPropValue branch
        item.put("unknown.path", new AttributeValue().withS("val"));

        TestEntity result = DynamoDBExecutor.toEntity(item, TestEntity.class);
        assertNotNull(result);
        assertEquals("1", result.getId());
    }

    // toEntity(GetItemResult, Class): mirrors the v2 GetItemResponse overload
    @Test
    public void testToEntity_GetItemResult() {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("id", new AttributeValue().withS("123"));
        item.put("name", new AttributeValue().withS("test"));

        TestEntity result = DynamoDBExecutor.toEntity(new GetItemResult().withItem(item), TestEntity.class);

        assertNotNull(result);
        assertEquals("123", result.getId());
        assertEquals("test", result.getName());
    }

    @Test
    public void testToEntity_GetItemResultNoItemReturnsNull() {
        // No item set -> getItem() is null -> null entity (key-not-found case)
        assertNull(DynamoDBExecutor.toEntity(new GetItemResult(), TestEntity.class));
    }

    @Test
    public void testToEntity_LAttributeOfNIntoTypedList_convertsElements() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue("e2"));
        item.put("counts", new AttributeValue().withL(new AttributeValue().withN("5"), new AttributeValue().withN("7")));

        GenericPropsEntity entity = DynamoDBExecutor.toEntity(item, GenericPropsEntity.class);

        assertNotNull(entity.getCounts());
        assertEquals(2, entity.getCounts().size());
        assertEquals(Long.valueOf(5L), entity.getCounts().get(0)); // would throw ClassCastException before the fix
        assertEquals(Long.valueOf(7L), entity.getCounts().get(1));
    }

    @Test
    public void testToEntity_MAttributeIntoTypedMap_convertsValues() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue("e3"));
        Map<String, AttributeValue> ratings = new HashMap<>();
        ratings.put("quality", new AttributeValue().withN("4"));
        item.put("ratings", new AttributeValue().withM(ratings));

        GenericPropsEntity entity = DynamoDBExecutor.toEntity(item, GenericPropsEntity.class);

        assertNotNull(entity.getRatings());
        assertEquals(Integer.valueOf(4), entity.getRatings().get("quality")); // ClassCastException before the fix
    }

    @Test
    public void testToEntity_NativeBinaryListsAndMapsPreserveNullAndEmptyValues() {
        final AttributeValue binary = new AttributeValue().withB(ByteBuffer.wrap(new byte[] { 4, 5 }));
        final AttributeValue missing = new AttributeValue().withNULL(true);
        final AttributeValue empty = new AttributeValue().withB(ByteBuffer.allocate(0));
        final AttributeValue binaryList = new AttributeValue().withL(binary, missing, empty);
        final AttributeValue binaryMap = new AttributeValue().withM(Map.of("7", binary, "8", missing, "9", empty));

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
        final AttributeValue binarySet = new AttributeValue().withBS(source);

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

        assertSame(source, entity.getRawValues().get(0));
        assertSame(source, entity.getRawArray()[0]);
    }

    /**
     * Regression tests for toEntity's container conversion: elements of native NS/L/M attributes were
     * previously left as Strings inside generically-typed properties (Set&lt;Integer&gt;, List&lt;Long&gt;,
     * Map&lt;String, Integer&gt;) — heap pollution surfacing as ClassCastException at the call site.
     * Values are now rebuilt through the property's full parameterized type.
     */
    @Test
    public void testToEntity_NsAttributeIntoTypedSet_convertsElements() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue("e1"));
        item.put("scores", new AttributeValue().withNS("1", "2"));

        GenericPropsEntity entity = DynamoDBExecutor.toEntity(item, GenericPropsEntity.class);

        assertNotNull(entity.getScores());
        assertTrue(entity.getScores().contains(Integer.valueOf(1)));
        assertTrue(entity.getScores().contains(Integer.valueOf(2)));

        int sum = 0;
        for (Integer score : entity.getScores()) { // would throw ClassCastException before the fix
            sum += score;
        }
        assertEquals(3, sum);
    }

    // Regression for the Object-typed-slot guard: a Map<String,Object> property must receive the raw
    // container as-is. Rebuilding it through the JSON codec would mangle a ByteBuffer value (from a
    // native B sub-attribute) that the codec can't re-type under element type Object.
    @Test
    public void testToEntity_ObjectValuedMap_PreservesBinaryValue() {
        byte[] blob = new byte[] { 1, 2, -3 };

        Map<String, AttributeValue> nested = new LinkedHashMap<>();
        nested.put("blob", new AttributeValue().withB(ByteBuffer.wrap(blob)));
        nested.put("name", new AttributeValue("n1"));

        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue("e4"));
        item.put("attrs", new AttributeValue().withM(nested));

        GenericPropsEntity entity = DynamoDBExecutor.toEntity(item, GenericPropsEntity.class);

        assertNotNull(entity.getAttrs());
        assertEquals("n1", entity.getAttrs().get("name"));

        Object blobValue = entity.getAttrs().get("blob");
        assertTrue(blobValue instanceof ByteBuffer, "blob should stay ByteBuffer, was " + (blobValue == null ? "null" : blobValue.getClass()));
        assertEquals(ByteBuffer.wrap(blob), blobValue);
    }

    // A getter-only property inherited from an @Entity superclass is written by toItem; reading the item back used to fail
    // the whole entity with UnsupportedOperationException from PropInfo.setPropValue.
    @Test
    public void testToEntity_ReadOnlyInheritedPropertyAttributeIsSkipped() {
        ReadOnlyPropSubEntity source = new ReadOnlyPropSubEntity();
        source.setId("id-1");
        source.setName("n");

        Map<String, AttributeValue> item = DynamoDBExecutor.toItem(source);
        assertEquals("computed:id-1", item.get("computed").getS());

        ReadOnlyPropSubEntity result = DynamoDBExecutor.toEntity(item, ReadOnlyPropSubEntity.class);
        assertEquals("id-1", result.getId());
        assertEquals("n", result.getName());

        when(mockDynamoDBClient.getItem(eq("TestTable"), any())).thenReturn(new GetItemResult().withItem(item));
        ReadOnlyPropSubEntity fromMapper = executor.mapper(ReadOnlyPropSubEntity.class).getItem(source);
        assertEquals("n", fromMapper.getName());
        assertEquals("computed:id-1", fromMapper.getComputed());
    }

    @Test
    public void testToEntity_StringArrayPropFromPlainOrNativeAttributeUnchanged() {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("tags", new AttributeValue().withS("a, b")); // not JSON text: kept as a single element, as before
        item.put("values", new AttributeValue().withSS("x", "y"));

        ObjectArrayPropsEntity result = DynamoDBExecutor.toEntity(item, ObjectArrayPropsEntity.class);

        assertArrayEquals(new String[] { "a, b" }, result.getTags());
        assertArrayEquals(new Object[] { "x", "y" }, result.getValues());
    }

    @Test
    public void testToEntity_UnknownPropIgnored() {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        item.put("id", new AttributeValue().withS("1"));
        item.put("noSuchField", new AttributeValue().withS("x"));

        TestEntity result = DynamoDBExecutor.toEntity(item, TestEntity.class);
        assertNotNull(result);
        assertEquals("1", result.getId());
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
    public void testToList_AsCollectionClass() {
        QueryResult qr = new QueryResult();
        Map<String, AttributeValue> r = new LinkedHashMap<>();
        r.put("a", new AttributeValue().withS("x"));
        r.put("b", new AttributeValue().withS("y"));
        qr.setItems(List.of(r));

        @SuppressWarnings({ "unchecked", "rawtypes" })
        List<List> result = DynamoDBExecutor.toList(qr, (Class) List.class);
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).size());
    }

    @Test
    public void testToList_AsMapClass() {
        QueryResult qr = new QueryResult();
        Map<String, AttributeValue> r = new LinkedHashMap<>();
        r.put("a", new AttributeValue().withS("x"));
        qr.setItems(List.of(r));

        @SuppressWarnings({ "unchecked", "rawtypes" })
        List<Map> result = DynamoDBExecutor.toList(qr, (Class) Map.class);
        assertEquals(1, result.size());
        assertEquals("x", result.get(0).get("a"));
    }

    // readRow / createRowMapper branches: Object[], Collection, Map, single-value
    @Test
    public void testToList_AsObjectArrayClass() {
        QueryResult qr = new QueryResult();
        Map<String, AttributeValue> r = new LinkedHashMap<>();
        r.put("a", new AttributeValue().withS("x"));
        r.put("b", new AttributeValue().withS("y"));
        qr.setItems(List.of(r));

        List<Object[]> result = DynamoDBExecutor.toList(qr, Object[].class);
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).length);
    }

    @Test
    public void testToList_AsSingleValueClass() {
        QueryResult qr = new QueryResult();
        Map<String, AttributeValue> r = new LinkedHashMap<>();
        r.put("v", new AttributeValue().withS("hello"));
        qr.setItems(List.of(r));

        List<String> result = DynamoDBExecutor.toList(qr, String.class);
        assertEquals(1, result.size());
        assertEquals("hello", result.get(0));
    }

    // toList with negative offset/count
    @Test
    public void testToList_NegativeOffsetThrows() {
        QueryResult qr = new QueryResult();
        qr.setItems(new ArrayList<>());
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toList(qr, -1, 0, TestEntity.class));
    }

    @Test
    public void testToList_SingleValueClassMultiColumn_Throws() {
        QueryResult qr = new QueryResult();
        Map<String, AttributeValue> r = new LinkedHashMap<>();
        r.put("a", new AttributeValue().withS("1"));
        r.put("b", new AttributeValue().withS("2"));
        qr.setItems(List.of(r));

        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toList(qr, String.class));
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
    public void testToListQueryResultNullStillValidatesRange() {
        assertTrue(DynamoDBExecutor.toList((QueryResult) null, 0, 1, TestEntity.class).isEmpty());
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toList((QueryResult) null, -1, 1, TestEntity.class));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toList((QueryResult) null, 0, -1, TestEntity.class));
    }

    @Test
    public void testToListScanResultNullStillValidatesRange() {
        assertTrue(DynamoDBExecutor.toList((ScanResult) null, 0, 1, TestEntity.class).isEmpty());
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toList((ScanResult) null, -1, 1, TestEntity.class));
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toList((ScanResult) null, 0, -1, TestEntity.class));
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
    public void testToMapWithNull() {
        Map<String, Object> result = DynamoDBExecutor.toMap((Map<String, AttributeValue>) null);
        assertNull(result);
    }

    @Test
    public void testToMapWithNullMapSupplier() {
        assertThrows(IllegalArgumentException.class, () -> DynamoDBExecutor.toMap(new HashMap<>(), null));
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

    public static class BinaryEntity {
        @com.landawn.abacus.annotation.Id
        private String id;
        private byte[] data;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }

        public byte[] getData() {
            return data;
        }

        public void setData(final byte[] data) {
            this.data = data;
        }
    }

    @com.landawn.abacus.annotation.Table(name = "TestTable")
    public static class GenericPropsEntity {
        @com.landawn.abacus.annotation.Id
        private String id;
        private java.util.Set<Integer> scores;
        private List<Long> counts;
        private Map<String, Integer> ratings;
        private Map<String, Object> attrs;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public java.util.Set<Integer> getScores() {
            return scores;
        }

        public void setScores(java.util.Set<Integer> scores) {
            this.scores = scores;
        }

        public List<Long> getCounts() {
            return counts;
        }

        public void setCounts(List<Long> counts) {
            this.counts = counts;
        }

        public Map<String, Integer> getRatings() {
            return ratings;
        }

        public void setRatings(Map<String, Integer> ratings) {
            this.ratings = ratings;
        }

        public Map<String, Object> getAttrs() {
            return attrs;
        }

        public void setAttrs(Map<String, Object> attrs) {
            this.attrs = attrs;
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
}
