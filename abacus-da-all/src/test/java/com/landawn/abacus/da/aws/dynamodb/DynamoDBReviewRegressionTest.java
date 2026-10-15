package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;

import software.amazon.awssdk.core.SdkBytes;

public class DynamoDBReviewRegressionTest {

    // Row and entity mapping

    @Test
    public void legacyTypedMapKeysDoNotConvertObjectBinaryValuesToJson() {
        final ByteBuffer source = sourceBuffer();
        final AttributeValue value = new AttributeValue().withM(Map.of("7", new AttributeValue().withB(source)));

        final NestedContainerEntity entity = DynamoDBExecutor.toEntity(Map.of("keyedObjects", value), NestedContainerEntity.class);

        assertEquals(source, assertInstanceOf(ByteBuffer.class, entity.getKeyedObjects().get(7)));
    }

    @Test
    public void nativeNestedNumericContainersRetainGenericTypes() {
        final AttributeValue legacyNumbers = new AttributeValue().withM(Map.of("7", new AttributeValue().withNS("11", "12")));
        final var v2Numbers = software.amazon.awssdk.services.dynamodb.model.AttributeValue
                .fromM(Map.of("7", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromNs(List.of("11", "12"))));

        assertEquals(Map.of(7, List.of(11L, 12L)), DynamoDBExecutor.toEntity(Map.of("numbers", legacyNumbers), NestedContainerEntity.class).getNumbers());
        assertEquals(Map.of(7, List.of(11L, 12L)),
                com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(Map.of("numbers", v2Numbers), NestedContainerEntity.class).getNumbers());
    }

    @Test
    public void parameterizedBeanElementsRetainGenericBindings() {
        final AttributeValue legacyLeaves = new AttributeValue().withL(new AttributeValue().withM(Map.of("value", new AttributeValue().withN("11"))));
        final var v2Leaves = software.amazon.awssdk.services.dynamodb.model.AttributeValue
                .fromL(List.of(software.amazon.awssdk.services.dynamodb.model.AttributeValue
                        .fromM(Map.of("value", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromN("11")))));

        assertEquals(11L, DynamoDBExecutor.toEntity(Map.of("genericLeaves", legacyLeaves), NestedContainerEntity.class).getGenericLeaves().get(0).getValue());
        assertEquals(11L,
                com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(Map.of("genericLeaves", v2Leaves), NestedContainerEntity.class)
                        .getGenericLeaves()
                        .get(0)
                        .getValue());

        final AttributeValue legacyJsonLeaves = new AttributeValue().withL(new AttributeValue().withS("{\"value\":11}"));
        final var v2JsonLeaves = software.amazon.awssdk.services.dynamodb.model.AttributeValue
                .fromL(List.of(software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("{\"value\":11}")));
        assertEquals(11L,
                DynamoDBExecutor.toEntity(Map.of("genericLeaves", legacyJsonLeaves), NestedContainerEntity.class).getGenericLeaves().get(0).getValue());
        assertEquals(11L,
                com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(Map.of("genericLeaves", v2JsonLeaves), NestedContainerEntity.class)
                        .getGenericLeaves()
                        .get(0)
                        .getValue());
    }

    @Test
    public void parameterizedBeanPropertyRetainsGenericBindingAfterRoundTrip() {
        final NestedContainerEntity original = new NestedContainerEntity();
        final GenericLeaf<Long> leaf = new GenericLeaf<>();
        leaf.setValue(11L);
        original.setGenericNumberLeaf(leaf);

        assertEquals(11L, DynamoDBExecutor.toEntity(DynamoDBExecutor.toItem(original), NestedContainerEntity.class).getGenericNumberLeaf().getValue());
        assertEquals(11L,
                com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                        .toEntity(com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toItem(original), NestedContainerEntity.class)
                        .getGenericNumberLeaf()
                        .getValue());
    }

    @Test
    public void parameterizedMapKeysAndArrayElementsRetainGenericBindings() {
        final Map<String, AttributeValue> legacyItem = Map.of("beanKeys",
                new AttributeValue().withM(Map.of("{\"value\":11}", new AttributeValue().withS("payload"))), "listKeys",
                new AttributeValue().withM(Map.of("[11]", new AttributeValue().withS("payload"))), "genericArrays",
                new AttributeValue().withL(new AttributeValue().withS("[{\"value\":11}]")));
        assertParameterizedKeysAndArrays(DynamoDBExecutor.toEntity(legacyItem, NestedContainerEntity.class));

        final var v2Item = Map.of("beanKeys",
                software.amazon.awssdk.services.dynamodb.model.AttributeValue
                        .fromM(Map.of("{\"value\":11}", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("payload"))),
                "listKeys",
                software.amazon.awssdk.services.dynamodb.model.AttributeValue
                        .fromM(Map.of("[11]", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("payload"))),
                "genericArrays", software.amazon.awssdk.services.dynamodb.model.AttributeValue
                        .fromL(List.of(software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("[{\"value\":11}]"))));
        assertParameterizedKeysAndArrays(com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(v2Item, NestedContainerEntity.class));
    }

    @Test
    public void v2TypedMapKeysDoNotConvertObjectBinaryValuesToJson() {
        final var value = software.amazon.awssdk.services.dynamodb.model.AttributeValue
                .fromM(Map.of("7", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromB(SdkBytes.fromByteBuffer(sourceBuffer()))));

        final NestedContainerEntity entity = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(Map.of("keyedObjects", value),
                NestedContainerEntity.class);

        assertArrayEquals(new byte[] { 1, 2, -3 }, assertInstanceOf(byte[].class, entity.getKeyedObjects().get(7)));
    }

    // Queries and results

    @Test
    public void legacyPaginatedQueryDoesNotModifyClientResponse() {
        final AmazonDynamoDBClient client = mock(AmazonDynamoDBClient.class);
        final QueryResult first = new QueryResult().withItems(Map.of("id", new AttributeValue("1")))
                .withLastEvaluatedKey(Map.of("id", new AttributeValue("1")));
        final QueryResult last = new QueryResult().withItems(Map.of("id", new AttributeValue("2")));
        when(client.query(any(QueryRequest.class))).thenReturn(first, last, first, last);
        final DynamoDBExecutor executor = new DynamoDBExecutor(client);

        assertEquals(2, executor.query(new QueryRequest("table")).size());
        assertEquals(1, first.getItems().size());
        assertEquals(2, executor.query(new QueryRequest("table")).size());
    }

    // Additional contracts

    @Test
    public void legacyNativeNestedBeansPreserveBinaryProperties() {
        final AttributeValue binary = new AttributeValue().withB(sourceBuffer());
        final AttributeValue leaf = new AttributeValue().withM(Map.of("buffer", binary, "bytes", binary));
        final NestedContainerEntity entity = DynamoDBExecutor.toEntity(Map.of("leaf", leaf, "leaves", new AttributeValue().withL(leaf)),
                NestedContainerEntity.class);

        assertBinaryLeaf(entity.getLeaf());
        assertBinaryLeaf(entity.getLeaves().get(0));
    }

    @Test
    public void legacyNestedArraysAndRawContainersPreserveBinaryValues() {
        final AttributeValue binary = new AttributeValue().withB(sourceBuffer());
        final AttributeValue list = new AttributeValue().withL(binary);
        final AttributeValue arrays = new AttributeValue().withL(list);
        final AttributeValue maps = new AttributeValue().withL(new AttributeValue().withM(Map.of("blob", binary)));
        final NestedContainerEntity entity = DynamoDBExecutor.toEntity(Map.of("arrays", arrays, "rawLists", arrays, "rawMaps", maps),
                NestedContainerEntity.class);

        assertEquals(sourceBuffer(), assertInstanceOf(ByteBuffer.class, entity.getArrays().get(0)[0]));
        assertEquals(sourceBuffer(), assertInstanceOf(ByteBuffer.class, entity.getRawLists().get(0).get(0)));
        assertEquals(sourceBuffer(), assertInstanceOf(ByteBuffer.class, entity.getRawMaps().get(0).get("blob")));
    }

    @Test
    public void legacyNestedObjectContainersPreserveBinaryValues() {
        final ByteBuffer source = sourceBuffer();
        final AttributeValue binary = new AttributeValue().withB(source);
        final AttributeValue value = new AttributeValue().withL(new AttributeValue().withM(Map.of("blob", binary)));

        final NestedContainerEntity entity = DynamoDBExecutor.toEntity(Map.of("objects", value), NestedContainerEntity.class);

        assertEquals(source, assertInstanceOf(ByteBuffer.class, entity.getObjects().get(0).get("blob")));
        assertEquals(1, source.position());
        assertEquals(4, source.limit());
    }

    @Test
    public void legacyNestedTypedContainersPreserveBinaryValuesAndNulls() {
        final ByteBuffer source = sourceBuffer();
        final AttributeValue values = new AttributeValue().withL(new AttributeValue().withB(source), new AttributeValue().withNULL(true),
                new AttributeValue().withB(ByteBuffer.allocate(0)));
        final Map<String, AttributeValue> item = Map.of("buffers", new AttributeValue().withM(Map.of("7", values)), "bytes",
                new AttributeValue().withL(values));

        assertTypedBinaryValues(DynamoDBExecutor.toEntity(item, NestedContainerEntity.class));
        assertEquals(1, source.position());
        assertEquals(4, source.limit());
    }

    @Test
    public void legacyParameterizedBeansPreserveBinaryTypeBindings() {
        final AttributeValue binary = new AttributeValue().withB(sourceBuffer());
        final AttributeValue leaf = new AttributeValue().withM(Map.of("value", binary));
        final AttributeValue leaves = new AttributeValue().withL(leaf);
        final NestedContainerEntity entity = DynamoDBExecutor.toEntity(
                Map.of("genericBytes", leaves, "genericBuffers", leaves, "genericByteLeaf", leaf, "genericBufferLeaf", leaf), NestedContainerEntity.class);

        assertGenericBinaryValues(entity);
    }

    @Test
    public void v2NativeNestedBeansPreserveBinaryProperties() {
        final var binary = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromB(SdkBytes.fromByteBuffer(sourceBuffer()));
        final var leaf = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(Map.of("buffer", binary, "bytes", binary));
        final NestedContainerEntity entity = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(
                Map.of("leaf", leaf, "leaves", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List.of(leaf))),
                NestedContainerEntity.class);

        assertBinaryLeaf(entity.getLeaf());
        assertBinaryLeaf(entity.getLeaves().get(0));
    }

    @Test
    public void v2NestedArraysAndRawContainersPreserveBinaryValues() {
        final var binary = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromB(SdkBytes.fromByteBuffer(sourceBuffer()));
        final var list = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List.of(binary));
        final var arrays = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List.of(list));
        final var maps = software.amazon.awssdk.services.dynamodb.model.AttributeValue
                .fromL(List.of(software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(Map.of("blob", binary))));
        final NestedContainerEntity entity = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                .toEntity(Map.of("arrays", arrays, "rawLists", arrays, "rawMaps", maps), NestedContainerEntity.class);

        assertArrayEquals(new byte[] { 1, 2, -3 }, assertInstanceOf(byte[].class, entity.getArrays().get(0)[0]));
        assertArrayEquals(new byte[] { 1, 2, -3 }, assertInstanceOf(byte[].class, entity.getRawLists().get(0).get(0)));
        assertArrayEquals(new byte[] { 1, 2, -3 }, assertInstanceOf(byte[].class, entity.getRawMaps().get(0).get("blob")));
    }

    @Test
    public void v2NestedObjectContainersPreserveBinaryValues() {
        final var binary = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromB(SdkBytes.fromByteBuffer(sourceBuffer()));
        final var value = software.amazon.awssdk.services.dynamodb.model.AttributeValue
                .fromL(List.of(software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(Map.of("blob", binary))));

        final NestedContainerEntity entity = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(Map.of("objects", value),
                NestedContainerEntity.class);

        assertArrayEquals(new byte[] { 1, 2, -3 }, assertInstanceOf(byte[].class, entity.getObjects().get(0).get("blob")));
    }

    @Test
    public void v2NestedTypedContainersPreserveBinaryValuesAndNulls() {
        final var values = software.amazon.awssdk.services.dynamodb.model.AttributeValue
                .fromL(List.of(software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromB(SdkBytes.fromByteBuffer(sourceBuffer())),
                        software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromNul(true),
                        software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromB(SdkBytes.fromByteArray(new byte[0]))));
        final var item = Map.of("buffers", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(Map.of("7", values)), "bytes",
                software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List.of(values)));

        assertTypedBinaryValues(com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(item, NestedContainerEntity.class));
    }

    @Test
    public void v2ParameterizedBeansPreserveBinaryTypeBindings() {
        final var binary = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromB(SdkBytes.fromByteBuffer(sourceBuffer()));
        final var leaf = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(Map.of("value", binary));
        final var leaves = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List.of(leaf));
        final NestedContainerEntity entity = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(
                Map.of("genericBytes", leaves, "genericBuffers", leaves, "genericByteLeaf", leaf, "genericBufferLeaf", leaf), NestedContainerEntity.class);

        assertGenericBinaryValues(entity);
    }

    private static ByteBuffer sourceBuffer() {
        final ByteBuffer result = ByteBuffer.wrap(new byte[] { 9, 1, 2, -3, 8 }).asReadOnlyBuffer();
        result.position(1);
        result.limit(4);
        return result;
    }

    private static void assertTypedBinaryValues(final NestedContainerEntity entity) {
        final List<ByteBuffer> buffers = entity.getBuffers().get(7);
        assertEquals(ByteBuffer.wrap(new byte[] { 1, 2, -3 }), buffers.get(0));
        assertEquals(3, buffers.get(0).remaining());
        assertNull(buffers.get(1));
        assertEquals(0, buffers.get(2).remaining());
        final List<byte[]> bytes = entity.getBytes().get(0);
        assertArrayEquals(new byte[] { 1, 2, -3 }, bytes.get(0));
        assertNull(bytes.get(1));
        assertArrayEquals(new byte[0], bytes.get(2));
    }

    private static void assertBinaryLeaf(final BinaryLeaf leaf) {
        assertEquals(ByteBuffer.wrap(new byte[] { 1, 2, -3 }), leaf.getBuffer());
        assertArrayEquals(new byte[] { 1, 2, -3 }, leaf.getBytes());
    }

    private static void assertGenericBinaryValues(final NestedContainerEntity entity) {
        assertArrayEquals(new byte[] { 1, 2, -3 }, entity.getGenericBytes().get(0).getValue());
        assertEquals(ByteBuffer.wrap(new byte[] { 1, 2, -3 }), entity.getGenericBuffers().get(0).getValue());
        assertArrayEquals(new byte[] { 1, 2, -3 }, entity.getGenericByteLeaf().getValue());
        assertEquals(ByteBuffer.wrap(new byte[] { 1, 2, -3 }), entity.getGenericBufferLeaf().getValue());
    }

    private static void assertParameterizedKeysAndArrays(final NestedContainerEntity entity) {
        assertEquals(11L, entity.getBeanKeys().keySet().iterator().next().getValue());
        assertEquals(List.of(11L), entity.getListKeys().keySet().iterator().next());
        assertEquals(11L, entity.getGenericArrays().get(0)[0].getValue());
        assertEquals("payload", entity.getBeanKeys().values().iterator().next());
        assertEquals("payload", entity.getListKeys().values().iterator().next());
    }

    @SuppressWarnings("rawtypes")
    public static class NestedContainerEntity {
        private List<Map<String, Object>> objects;
        private Map<Integer, List<ByteBuffer>> buffers;
        private List<List<byte[]>> bytes;
        private Map<Integer, Object> keyedObjects;
        private List<Object[]> arrays;
        private List<Map> rawMaps;
        private List<List> rawLists;
        private Map<Integer, List<Long>> numbers;
        private BinaryLeaf leaf;
        private List<BinaryLeaf> leaves;
        private List<GenericLeaf<Long>> genericLeaves;
        private List<GenericLeaf<byte[]>> genericBytes;
        private List<GenericLeaf<ByteBuffer>> genericBuffers;
        private GenericLeaf<byte[]> genericByteLeaf;
        private GenericLeaf<ByteBuffer> genericBufferLeaf;
        private Map<GenericLeaf<Long>, Object> beanKeys;
        private Map<List<Long>, Object> listKeys;
        private List<GenericLeaf<Long>[]> genericArrays;
        private GenericLeaf<Long> genericNumberLeaf;

        public List<Map<String, Object>> getObjects() {
            return objects;
        }

        public void setObjects(final List<Map<String, Object>> objects) {
            this.objects = objects;
        }

        public Map<Integer, List<ByteBuffer>> getBuffers() {
            return buffers;
        }

        public void setBuffers(final Map<Integer, List<ByteBuffer>> buffers) {
            this.buffers = buffers;
        }

        public List<List<byte[]>> getBytes() {
            return bytes;
        }

        public void setBytes(final List<List<byte[]>> bytes) {
            this.bytes = bytes;
        }

        public Map<Integer, Object> getKeyedObjects() {
            return keyedObjects;
        }

        public void setKeyedObjects(final Map<Integer, Object> keyedObjects) {
            this.keyedObjects = keyedObjects;
        }

        public List<Object[]> getArrays() {
            return arrays;
        }

        public void setArrays(final List<Object[]> arrays) {
            this.arrays = arrays;
        }

        public List<Map> getRawMaps() {
            return rawMaps;
        }

        public void setRawMaps(final List<Map> rawMaps) {
            this.rawMaps = rawMaps;
        }

        public List<List> getRawLists() {
            return rawLists;
        }

        public void setRawLists(final List<List> rawLists) {
            this.rawLists = rawLists;
        }

        public Map<Integer, List<Long>> getNumbers() {
            return numbers;
        }

        public void setNumbers(final Map<Integer, List<Long>> numbers) {
            this.numbers = numbers;
        }

        public BinaryLeaf getLeaf() {
            return leaf;
        }

        public void setLeaf(final BinaryLeaf leaf) {
            this.leaf = leaf;
        }

        public List<BinaryLeaf> getLeaves() {
            return leaves;
        }

        public void setLeaves(final List<BinaryLeaf> leaves) {
            this.leaves = leaves;
        }

        public List<GenericLeaf<Long>> getGenericLeaves() {
            return genericLeaves;
        }

        public void setGenericLeaves(final List<GenericLeaf<Long>> genericLeaves) {
            this.genericLeaves = genericLeaves;
        }

        public List<GenericLeaf<byte[]>> getGenericBytes() {
            return genericBytes;
        }

        public void setGenericBytes(final List<GenericLeaf<byte[]>> genericBytes) {
            this.genericBytes = genericBytes;
        }

        public List<GenericLeaf<ByteBuffer>> getGenericBuffers() {
            return genericBuffers;
        }

        public void setGenericBuffers(final List<GenericLeaf<ByteBuffer>> genericBuffers) {
            this.genericBuffers = genericBuffers;
        }

        public GenericLeaf<byte[]> getGenericByteLeaf() {
            return genericByteLeaf;
        }

        public void setGenericByteLeaf(final GenericLeaf<byte[]> genericByteLeaf) {
            this.genericByteLeaf = genericByteLeaf;
        }

        public GenericLeaf<ByteBuffer> getGenericBufferLeaf() {
            return genericBufferLeaf;
        }

        public void setGenericBufferLeaf(final GenericLeaf<ByteBuffer> genericBufferLeaf) {
            this.genericBufferLeaf = genericBufferLeaf;
        }

        public Map<GenericLeaf<Long>, Object> getBeanKeys() {
            return beanKeys;
        }

        public void setBeanKeys(final Map<GenericLeaf<Long>, Object> beanKeys) {
            this.beanKeys = beanKeys;
        }

        public Map<List<Long>, Object> getListKeys() {
            return listKeys;
        }

        public void setListKeys(final Map<List<Long>, Object> listKeys) {
            this.listKeys = listKeys;
        }

        public List<GenericLeaf<Long>[]> getGenericArrays() {
            return genericArrays;
        }

        public void setGenericArrays(final List<GenericLeaf<Long>[]> genericArrays) {
            this.genericArrays = genericArrays;
        }

        public GenericLeaf<Long> getGenericNumberLeaf() {
            return genericNumberLeaf;
        }

        public void setGenericNumberLeaf(final GenericLeaf<Long> genericNumberLeaf) {
            this.genericNumberLeaf = genericNumberLeaf;
        }
    }

    public static class BinaryLeaf {
        private ByteBuffer buffer;
        private byte[] bytes;

        public ByteBuffer getBuffer() {
            return buffer;
        }

        public void setBuffer(final ByteBuffer buffer) {
            this.buffer = buffer;
        }

        public byte[] getBytes() {
            return bytes;
        }

        public void setBytes(final byte[] bytes) {
            this.bytes = bytes;
        }
    }

    public static class GenericLeaf<T> {
        private T value;

        public T getValue() {
            return value;
        }

        public void setValue(final T value) {
            this.value = value;
        }
    }
}
