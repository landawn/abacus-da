package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.landawn.abacus.util.Holder;
import com.landawn.abacus.util.u.Nullable;

import software.amazon.awssdk.core.SdkBytes;

public class DynamoDBNativeWrapperRegressionTest {
    @Test
    public void legacyNativeWrappersPreserveBinarySlicesAndEmptyValues() {
        for (final boolean empty : new boolean[] { false, true }) {
            final ByteBuffer source = sourceBuffer(empty);
            final int position = source.position();
            final int limit = source.limit();
            final AttributeValue binary = new AttributeValue().withB(source);
            final AttributeValue bean = new AttributeValue().withM(Map.of("buffer", binary, "bytes", binary));
            final Map<String, AttributeValue> leafItem = Map.of("bytes", binary, "buffer", binary, "bean", bean, "abacus", binary, "nullable", binary, "held",
                    bean, "nested", binary, "bufferList", new AttributeValue().withL(binary), "bytesMap", new AttributeValue().withM(Map.of("key", binary)));
            final AttributeValue leaf = new AttributeValue().withM(leafItem);
            final WrapperHolder result = DynamoDBExecutor.toEntity(
                    Map.of("leaf", leaf, "leaves", new AttributeValue().withL(leaf), "bufferElements", new AttributeValue().withL(binary), "beanValues",
                            new AttributeValue().withM(Map.of("key", bean)), "nestedBytes", new AttributeValue().withL(binary)),
                    WrapperHolder.class);

            final byte[] expected = empty ? new byte[0] : new byte[] { 1, 2, -3 };
            assertLeaf(DynamoDBExecutor.toEntity(leafItem, WrapperLeaf.class), expected);
            assertHolder(result, expected);
            assertEquals(position, source.position());
            assertEquals(limit, source.limit());
        }
    }

    @Test
    public void v2NativeWrappersPreserveBinarySlicesAndEmptyValues() {
        for (final boolean empty : new boolean[] { false, true }) {
            final ByteBuffer source = sourceBuffer(empty);
            final var binary = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromB(SdkBytes.fromByteBuffer(source));
            final var bean = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(Map.of("buffer", binary, "bytes", binary));
            final var leafItem = Map.of("bytes", binary, "buffer", binary, "bean", bean, "abacus", binary, "nullable", binary, "held", bean, "nested", binary,
                    "bufferList", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List.of(binary)), "bytesMap",
                    software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(Map.of("key", binary)));
            final var leaf = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(leafItem);
            final WrapperHolder result = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                    .toEntity(Map.of("leaf", leaf, "leaves", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List.of(leaf)),
                            "bufferElements", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List.of(binary)), "beanValues",
                            software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(Map.of("key", bean)), "nestedBytes",
                            software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List.of(binary))), WrapperHolder.class);

            final byte[] expected = empty ? new byte[0] : new byte[] { 1, 2, -3 };
            assertLeaf(com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(leafItem, WrapperLeaf.class), expected);
            assertHolder(result, expected);
        }
    }

    @Test
    public void legacyNativeNullWrappersRetainPropertyAndContainerDefaults() {
        final AttributeValue nil = new AttributeValue().withNULL(true);
        final Map<String, AttributeValue> leafItem = Map.of("bytes", nil, "buffer", nil, "bean", nil, "abacus", nil, "nullable", nil, "held", nil, "nested",
                nil, "bufferList", nil, "bytesMap", nil);
        final AttributeValue leaf = new AttributeValue().withM(leafItem);
        final AttributeValue list = new AttributeValue().withL(nil);
        final WrapperHolder holder = DynamoDBExecutor.toEntity(Map.of("leaf", leaf, "leaves", new AttributeValue().withL(leaf), "bufferElements", list,
                "abacusElements", list, "nullableElements", list, "heldElements", list, "beanValues", new AttributeValue().withM(Map.of("key", nil)),
                "nestedBytes", list, "heldValues", new AttributeValue().withM(Map.of("key", nil))), WrapperHolder.class);

        assertNullLeaf(DynamoDBExecutor.toEntity(leafItem, WrapperLeaf.class));
        assertNullHolder(holder);
    }

    @Test
    public void v2NativeNullWrappersRetainPropertyAndContainerDefaults() {
        final var nil = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromNul(true);
        final var leafItem = Map.of("bytes", nil, "buffer", nil, "bean", nil, "abacus", nil, "nullable", nil, "held", nil, "nested", nil, "bufferList", nil,
                "bytesMap", nil);
        final var leaf = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(leafItem);
        final var list = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List.of(nil));
        final WrapperHolder holder = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                .toEntity(Map.of("leaf", leaf, "leaves", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List.of(leaf)), "bufferElements",
                        list, "abacusElements", list, "nullableElements", list, "heldElements", list, "beanValues",
                        software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(Map.of("key", nil)), "nestedBytes", list, "heldValues",
                        software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(Map.of("key", nil))), WrapperHolder.class);

        assertNullLeaf(com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(leafItem, WrapperLeaf.class));
        assertNullHolder(holder);
    }

    @Test
    public void legacyNullHolderArrayElementsRetainNativeAndJsonContexts() {
        final AttributeValue list = new AttributeValue().withL(new AttributeValue().withNULL(true));
        final WrapperHolder nativeResult = DynamoDBExecutor.toEntity(Map.of("holderArray", list, "holderArrays", new AttributeValue().withL(list)),
                WrapperHolder.class);
        final WrapperHolder serializedResult = DynamoDBExecutor.toEntity(Map.of("holderArray", new AttributeValue().withS("[null]")), WrapperHolder.class);

        assertNull(nativeResult.getHolderArray()[0].value());
        assertNull(nativeResult.getHolderArrays().get(0)[0]);
        assertNull(serializedResult.getHolderArray()[0]);
    }

    @Test
    public void v2NullHolderArrayElementsRetainNativeAndJsonContexts() {
        final var list = software.amazon.awssdk.services.dynamodb.model.AttributeValue
                .fromL(List.of(software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromNul(true)));
        final WrapperHolder nativeResult = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(
                Map.of("holderArray", list, "holderArrays", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List.of(list))),
                WrapperHolder.class);
        final WrapperHolder serializedResult = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                .toEntity(Map.of("holderArray", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("[null]")), WrapperHolder.class);

        assertNull(nativeResult.getHolderArray()[0].value());
        assertNull(nativeResult.getHolderArrays().get(0)[0]);
        assertNull(serializedResult.getHolderArray()[0]);
    }

    private static ByteBuffer sourceBuffer(final boolean empty) {
        final ByteBuffer result = ByteBuffer.wrap(empty ? new byte[0] : new byte[] { 9, 1, 2, -3, 8 }).asReadOnlyBuffer();
        if (!empty) {
            result.position(1);
            result.limit(4);
        }
        return result;
    }

    private static void assertHolder(final WrapperHolder holder, final byte[] expected) {
        assertLeaf(holder.getLeaf(), expected);
        assertLeaf(holder.getLeaves().get(0), expected);
        assertEquals(Optional.of(ByteBuffer.wrap(expected)), holder.getBufferElements().get(0));
        assertBean(holder.getBeanValues().get("key").orElseThrow(), expected);
        assertArrayEquals(expected, holder.getNestedBytes().get(0).orElseThrow().orElseThrow());
    }

    private static void assertLeaf(final WrapperLeaf leaf, final byte[] expected) {
        assertArrayEquals(expected, leaf.getBytes().orElseThrow());
        assertEquals(Optional.of(ByteBuffer.wrap(expected)), leaf.getBuffer());
        assertBean(leaf.getBean().orElseThrow(), expected);
        assertArrayEquals(expected, leaf.getAbacus().get());
        assertEquals(ByteBuffer.wrap(expected), leaf.getNullable().get());
        assertBean(leaf.getHeld().value(), expected);
        assertEquals(Optional.of(Optional.of(ByteBuffer.wrap(expected))), leaf.getNested());
        assertEquals(Optional.of(List.of(ByteBuffer.wrap(expected))), leaf.getBufferList());
        assertArrayEquals(expected, leaf.getBytesMap().orElseThrow().get("key"));
    }

    private static void assertBean(final BinaryBean bean, final byte[] expected) {
        assertEquals(ByteBuffer.wrap(expected), bean.getBuffer());
        assertArrayEquals(expected, bean.getBytes());
    }

    private static void assertNullHolder(final WrapperHolder holder) {
        assertNullLeaf(holder.getLeaf());
        assertNullLeaf(holder.getLeaves().get(0));
        assertEquals(Optional.empty(), holder.getBufferElements().get(0));
        assertEquals(com.landawn.abacus.util.u.Optional.empty(), holder.getAbacusElements().get(0));
        assertEquals(Nullable.empty(), holder.getNullableElements().get(0));
        assertNull(holder.getHeldElements().get(0));
        assertEquals(1, holder.getHeldValues().size());
        assertNull(holder.getHeldValues().get("key"));
        assertEquals(Optional.empty(), holder.getBeanValues().get("key"));
        assertEquals(Optional.empty(), holder.getNestedBytes().get(0));
    }

    private static void assertNullLeaf(final WrapperLeaf leaf) {
        assertEquals(Optional.empty(), leaf.getBytes());
        assertEquals(Optional.empty(), leaf.getBuffer());
        assertEquals(Optional.empty(), leaf.getBean());
        assertEquals(com.landawn.abacus.util.u.Optional.empty(), leaf.getAbacus());
        assertEquals(Nullable.empty(), leaf.getNullable());
        assertNull(leaf.getHeld().value());
        assertEquals(Optional.empty(), leaf.getNested());
        assertEquals(Optional.empty(), leaf.getBufferList());
        assertEquals(Optional.empty(), leaf.getBytesMap());
    }

    public static class WrapperHolder {
        private WrapperLeaf leaf;
        private List<WrapperLeaf> leaves;
        private List<Optional<ByteBuffer>> bufferElements;
        private Map<String, Optional<BinaryBean>> beanValues;
        private List<Optional<Optional<byte[]>>> nestedBytes;
        private List<com.landawn.abacus.util.u.Optional<byte[]>> abacusElements;
        private List<Nullable<byte[]>> nullableElements;
        private List<Holder<byte[]>> heldElements;
        private Map<String, Holder<byte[]>> heldValues;
        private Holder<byte[]>[] holderArray;
        private List<Holder<byte[]>[]> holderArrays;

        public WrapperLeaf getLeaf() {
            return leaf;
        }

        public void setLeaf(final WrapperLeaf value) {
            leaf = value;
        }

        public List<WrapperLeaf> getLeaves() {
            return leaves;
        }

        public void setLeaves(final List<WrapperLeaf> value) {
            leaves = value;
        }

        public List<Optional<ByteBuffer>> getBufferElements() {
            return bufferElements;
        }

        public void setBufferElements(final List<Optional<ByteBuffer>> value) {
            bufferElements = value;
        }

        public Map<String, Optional<BinaryBean>> getBeanValues() {
            return beanValues;
        }

        public void setBeanValues(final Map<String, Optional<BinaryBean>> value) {
            beanValues = value;
        }

        public List<Optional<Optional<byte[]>>> getNestedBytes() {
            return nestedBytes;
        }

        public void setNestedBytes(final List<Optional<Optional<byte[]>>> value) {
            nestedBytes = value;
        }

        public List<com.landawn.abacus.util.u.Optional<byte[]>> getAbacusElements() {
            return abacusElements;
        }

        public void setAbacusElements(final List<com.landawn.abacus.util.u.Optional<byte[]>> value) {
            abacusElements = value;
        }

        public List<Nullable<byte[]>> getNullableElements() {
            return nullableElements;
        }

        public void setNullableElements(final List<Nullable<byte[]>> value) {
            nullableElements = value;
        }

        public List<Holder<byte[]>> getHeldElements() {
            return heldElements;
        }

        public void setHeldElements(final List<Holder<byte[]>> value) {
            heldElements = value;
        }

        public Map<String, Holder<byte[]>> getHeldValues() {
            return heldValues;
        }

        public void setHeldValues(final Map<String, Holder<byte[]>> value) {
            heldValues = value;
        }

        public Holder<byte[]>[] getHolderArray() {
            return holderArray;
        }

        public void setHolderArray(final Holder<byte[]>[] value) {
            holderArray = value;
        }

        public List<Holder<byte[]>[]> getHolderArrays() {
            return holderArrays;
        }

        public void setHolderArrays(final List<Holder<byte[]>[]> value) {
            holderArrays = value;
        }
    }

    public static class WrapperLeaf {
        private Optional<byte[]> bytes;
        private Optional<ByteBuffer> buffer;
        private Optional<BinaryBean> bean;
        private com.landawn.abacus.util.u.Optional<byte[]> abacus;
        private Nullable<ByteBuffer> nullable;
        private Holder<BinaryBean> held;
        private Optional<Optional<ByteBuffer>> nested;
        private Optional<List<ByteBuffer>> bufferList;
        private Optional<Map<String, byte[]>> bytesMap;

        public Optional<byte[]> getBytes() {
            return bytes;
        }

        public void setBytes(final Optional<byte[]> value) {
            bytes = value;
        }

        public Optional<ByteBuffer> getBuffer() {
            return buffer;
        }

        public void setBuffer(final Optional<ByteBuffer> value) {
            buffer = value;
        }

        public Optional<BinaryBean> getBean() {
            return bean;
        }

        public void setBean(final Optional<BinaryBean> value) {
            bean = value;
        }

        public com.landawn.abacus.util.u.Optional<byte[]> getAbacus() {
            return abacus;
        }

        public void setAbacus(final com.landawn.abacus.util.u.Optional<byte[]> value) {
            abacus = value;
        }

        public Nullable<ByteBuffer> getNullable() {
            return nullable;
        }

        public void setNullable(final Nullable<ByteBuffer> value) {
            nullable = value;
        }

        public Holder<BinaryBean> getHeld() {
            return held;
        }

        public void setHeld(final Holder<BinaryBean> value) {
            held = value;
        }

        public Optional<Optional<ByteBuffer>> getNested() {
            return nested;
        }

        public void setNested(final Optional<Optional<ByteBuffer>> value) {
            nested = value;
        }

        public Optional<List<ByteBuffer>> getBufferList() {
            return bufferList;
        }

        public void setBufferList(final Optional<List<ByteBuffer>> value) {
            bufferList = value;
        }

        public Optional<Map<String, byte[]>> getBytesMap() {
            return bytesMap;
        }

        public void setBytesMap(final Optional<Map<String, byte[]>> value) {
            bytesMap = value;
        }
    }

    public static class BinaryBean {
        private ByteBuffer buffer;
        private byte[] bytes;

        public ByteBuffer getBuffer() {
            return buffer;
        }

        public void setBuffer(final ByteBuffer value) {
            buffer = value;
        }

        public byte[] getBytes() {
            return bytes;
        }

        public void setBytes(final byte[] value) {
            bytes = value;
        }
    }
}
