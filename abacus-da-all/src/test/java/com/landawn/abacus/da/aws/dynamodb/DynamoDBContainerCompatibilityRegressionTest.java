package com.landawn.abacus.da.aws.dynamodb;

import static com.landawn.abacus.da.aws.dynamodb.DynamoDBTypedMappingRegressionTest.forEachSdk;
import static com.landawn.abacus.da.aws.dynamodb.DynamoDBTypedMappingRegressionTest.nullableMap;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.annotation.JsonXmlField;
import com.landawn.abacus.da.aws.dynamodb.DynamoDBTypedMappingRegressionTest.Sdk;
import com.landawn.abacus.util.ImmutableBiMap;
import com.landawn.abacus.util.ImmutableCollection;

public class DynamoDBContainerCompatibilityRegressionTest {
    @Test
    public void immutableCollectionAndBiMapSupportPopulatedAndEmptyNativeValues() {
        forEachSdk(sdk -> assertAll(() -> assertImmutableValues(sdk, false), () -> assertImmutableValues(sdk, true)));
    }

    @Test
    public void immutableCollectionAndBiMapPreserveNestedBinaryValues() {
        forEachSdk(sdk -> {
            for (final boolean emptyBinary : new boolean[] { false, true }) {
                final ByteBuffer source = sourceBuffer(emptyBinary);
                final Map<String, Object> bean = Map.of("name", "alpha", "buffer", source, "bytes", source);
                final ContainerRoot result = sdk.read(Map.of("values", List.of(bean), "index", Map.of("a", bean)), ContainerRoot.class);
                final byte[] expected = emptyBinary ? new byte[0] : new byte[] { 1, 2, -3 };

                assertBinaryBean(result.getValues().iterator().next(), expected);
                assertBinaryBean(result.getIndex().get("a"), expected);
                assertSourceUnchanged(source, emptyBinary);
            }
        });
    }

    @Test
    public void nullAndAbsentImmutablePropertiesRemainNull() {
        forEachSdk(sdk -> {
            for (final Map<String, Object> item : List.of(nullableMap("values", "index"), Map.<String, Object> of())) {
                final ContainerRoot result = sdk.read(item, ContainerRoot.class);
                assertNull(result.getValues());
                assertNull(result.getIndex());
            }
        });
    }

    @Test
    public void emptyBooleanMapKeysRemainDistinctFromFalseInJsonContexts() {
        forEachSdk(sdk -> {
            final ByteBuffer source = sourceBuffer(false);
            final Map<String, Object> values = new LinkedHashMap<>();
            values.put("", Map.of("name", "empty", "buffer", source));
            values.put("false", Map.of("name", "false", "buffer", source));
            assertAll(() -> assertBooleanKeys(sdk.read(Map.of("booleanKeys", values), ContainerRoot.class).getBooleanKeys()), () -> assertBooleanKeys(
                    sdk.read(Map.of("list", List.of(Map.of("booleanKeys", values))), ContainerRoot.class).getList().get(0).getBooleanKeys()));
            assertSourceUnchanged(source, false);
        });
    }

    @Test
    public void directNativeMapsRejectConvertedKeyCollisions() {
        forEachSdk(sdk -> {
            final Map<String, Object> keys = collidingLongKeys();
            assertAll(() -> assertCollision(() -> sdk.read(Map.of("direct", Map.of("longKeys", keys)), ContainerRoot.class)),
                    () -> assertCollision(() -> sdk.read(Map.of("array", List.of(Map.of("longKeys", keys))), ContainerRoot.class)));
        });
    }

    @Test
    public void jsonMapsRetainTheirExistingConvertedKeyReplacementBehavior() {
        forEachSdk(sdk -> {
            final Map<String, Object> keys = collidingLongKeys();
            final ContainerRoot result = sdk.read(Map.of("longKeys", keys, "list", List.of(Map.of("longKeys", keys))), ContainerRoot.class);
            for (final Map<Long, Bean> values : List.of(result.getLongKeys(), result.getList().get(0).getLongKeys())) {
                assertEquals(1, values.size());
                assertEquals("zero-one", values.get(1L).getName());
            }
        });
    }

    @Test
    public void directNativeConcurrentMapsReportRejectedEntries() {
        forEachSdk(sdk -> {
            final Map<String, Object> leaf = Map.of("concurrent", nullableMap("nil"));
            assertAll(() -> assertRejectedEntry(() -> sdk.read(Map.of("direct", leaf), ContainerRoot.class)),
                    () -> assertRejectedEntry(() -> sdk.read(Map.of("array", List.of(leaf)), ContainerRoot.class)));
        });
    }

    @Test
    public void directNativeBlockingQueuesRetainTypedElementsAndEmptyValues() {
        forEachSdk(sdk -> {
            for (final List<Map<String, Object>> values : List.of(List.of(Map.<String, Object> of("name", "first"), Map.<String, Object> of("name", "second")),
                    List.<Map<String, Object>> of())) {
                final ContainerRoot result = sdk.read(Map.of("direct", Map.of("queue", values), "array", List.of(Map.of("queue", values))),
                        ContainerRoot.class);
                for (final NativeLeaf leaf : List.of(result.getDirect(), result.getArray()[0])) {
                    assertInstanceOf(ArrayBlockingQueue.class, leaf.getQueue());
                    assertEquals(values.isEmpty() ? List.of() : List.of("first", "second"), leaf.getQueue().stream().map(Bean::getName).toList());
                }
            }
        });
    }

    @Test
    public void directNativeBlockingQueuesPreserveBinaryLeavesAndPropertyDefaults() {
        forEachSdk(sdk -> {
            final ByteBuffer source = sourceBuffer(false);
            final Map<String, Object> bean = Map.of("buffer", source, "bytes", source);
            final ContainerRoot result = sdk.read(Map.of("direct", Map.of("queue", List.of(bean))), ContainerRoot.class);
            assertBinaryBean(result.getDirect().getQueue().element(), new byte[] { 1, 2, -3 });
            assertSourceUnchanged(source, false);

            assertNull(sdk.read(Map.of("direct", nullableMap("queue")), ContainerRoot.class).getDirect().getQueue());
            assertNull(sdk.read(Map.of("direct", Map.of()), ContainerRoot.class).getDirect().getQueue());
        });
    }

    @Test
    public void scalarStringCollectionsKeepNativeSingletonAndJsonParsingContexts() {
        forEachSdk(sdk -> {
            for (final String text : List.of("plain", "[\"a\",\"b\"]", "null", "", "\"quoted\"")) {
                final Map<String, Object> leaf = Map.of("strings", text);
                final ContainerRoot result = sdk.read(Map.of("direct", leaf, "array", List.of(leaf), "list", List.of(leaf)), ContainerRoot.class);
                assertEquals(List.of(text), result.getDirect().getStrings());
                assertEquals(List.of(text), result.getArray()[0].getStrings());
                final List<String> expectedJson = switch (text) {
                    case "[\"a\",\"b\"]" -> List.of("a", "b");
                    case "null" -> Arrays.asList((String) null);
                    case "" -> null;
                    case "\"quoted\"" -> List.of("quoted");
                    default -> List.of(text);
                };
                assertEquals(expectedJson, result.getList().get(0).getStrings());
            }
        });
    }

    @Test
    public void nativeStringCollectionsPreserveEmptyNullAndAbsentProperties() {
        forEachSdk(sdk -> {
            for (final Map<String, Object> leaf : List.of(Map.<String, Object> of("strings", List.of("a", "b")), Map.<String, Object> of("strings", List.of()),
                    nullableMap("strings"), Map.<String, Object> of())) {
                final ContainerRoot result = sdk.read(Map.of("direct", leaf, "array", List.of(leaf), "list", List.of(leaf)), ContainerRoot.class);
                for (final NativeLeaf value : List.of(result.getDirect(), result.getArray()[0], result.getList().get(0))) {
                    assertEquals(leaf.get("strings"), value.getStrings());
                }
            }
        });
    }

    @Test
    public void abstractNumberInferenceDoesNotIgnoreInvalidDateFormats() {
        forEachSdk(sdk -> {
            for (final Object value : List.of(20, "20")) {
                final UnsupportedOperationException failure = assertThrows(UnsupportedOperationException.class,
                        () -> sdk.read(Map.of("formatted", List.of(Map.of("value", value))), ContainerRoot.class));
                // Only the annotation attribute is matched: the rest of the wording belongs to abacus-common.
                assertTrue(failure.getMessage().toLowerCase(java.util.Locale.ROOT).contains("dateformat"), failure.getMessage());
            }
        });
    }

    @Test
    public void topLevelCopyOnWriteListsPreserveTypeOrderDuplicatesAndNulls() {
        forEachSdk(sdk -> {
            for (final List<Object> expected : List.of(Arrays.<Object> asList("a", null, "a", "b"), List.<Object> of())) {
                final CopyOnWriteArrayList<Object> actual = sdk.read(Map.of("copies", expected), ContainerRoot.class).getCopies();
                assertEquals(CopyOnWriteArrayList.class, actual.getClass());
                assertEquals(expected, actual);
            }
            assertNull(sdk.read(nullableMap("copies"), ContainerRoot.class).getCopies());
            assertNull(sdk.read(Map.of(), ContainerRoot.class).getCopies());
        });
    }

    private static void assertImmutableValues(final Sdk sdk, final boolean empty) {
        final Map<String, Object> bean = Map.of("name", "alpha");
        final ContainerRoot result = sdk.read(Map.of("values", empty ? List.of() : List.of(bean), "index", empty ? Map.of() : Map.of("a", bean)),
                ContainerRoot.class);
        assertInstanceOf(ImmutableCollection.class, result.getValues());
        assertInstanceOf(ImmutableBiMap.class, result.getIndex());
        assertEquals(empty ? 0 : 1, result.getValues().size());
        assertEquals(empty ? 0 : 1, result.getIndex().size());
        if (!empty) {
            assertEquals("alpha", result.getValues().iterator().next().getName());
            assertEquals("alpha", result.getIndex().get("a").getName());
        }
    }

    private static void assertBooleanKeys(final Map<Boolean, Bean> values) {
        assertEquals(2, values.size());
        assertEquals("empty", values.get(null).getName());
        assertEquals("false", values.get(Boolean.FALSE).getName());
        assertEquals(ByteBuffer.wrap(new byte[] { 1, 2, -3 }), values.get(null).getBuffer());
        assertEquals(ByteBuffer.wrap(new byte[] { 1, 2, -3 }), values.get(Boolean.FALSE).getBuffer());
    }

    private static Map<String, Object> collidingLongKeys() {
        final Map<String, Object> result = new LinkedHashMap<>();
        result.put("1", Map.of("name", "one"));
        result.put("01", Map.of("name", "zero-one"));
        return result;
    }

    // Exception types, not abacus-common's message wording, identify these failures. The exact type excludes
    // NumberFormatException, an IllegalArgumentException subclass that a failed key conversion would throw.
    private static void assertCollision(final org.junit.jupiter.api.function.Executable action) {
        final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, action);
        assertEquals(IllegalArgumentException.class, failure.getClass(), failure.getMessage());
        assertNull(failure.getCause(), failure.getMessage());
    }

    private static void assertRejectedEntry(final org.junit.jupiter.api.function.Executable action) {
        final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, action);
        assertEquals(IllegalArgumentException.class, failure.getClass(), failure.getMessage());
        assertInstanceOf(NullPointerException.class, failure.getCause());
    }

    private static ByteBuffer sourceBuffer(final boolean empty) {
        final ByteBuffer result = ByteBuffer.wrap(empty ? new byte[0] : new byte[] { 9, 1, 2, -3, 8 }).asReadOnlyBuffer();
        if (!empty) {
            result.position(1);
            result.limit(4);
        }
        return result;
    }

    private static void assertSourceUnchanged(final ByteBuffer source, final boolean empty) {
        assertEquals(empty ? 0 : 1, source.position());
        assertEquals(empty ? 0 : 4, source.limit());
    }

    private static void assertBinaryBean(final Bean bean, final byte[] expected) {
        assertEquals(ByteBuffer.wrap(expected), bean.getBuffer());
        assertArrayEquals(expected, bean.getBytes());
    }

    public static class ContainerRoot {
        private ImmutableCollection<Bean> values;
        private ImmutableBiMap<String, Bean> index;
        private Map<Boolean, Bean> booleanKeys;
        private TreeMap<Long, Bean> longKeys;
        private NativeLeaf direct;
        private NativeLeaf[] array;
        private List<NativeLeaf> list;
        private List<FormattedNumber> formatted;
        private CopyOnWriteArrayList<Object> copies;

        public ImmutableCollection<Bean> getValues() {
            return values;
        }

        public void setValues(final ImmutableCollection<Bean> value) {
            values = value;
        }

        public ImmutableBiMap<String, Bean> getIndex() {
            return index;
        }

        public void setIndex(final ImmutableBiMap<String, Bean> value) {
            index = value;
        }

        public Map<Boolean, Bean> getBooleanKeys() {
            return booleanKeys;
        }

        public void setBooleanKeys(final Map<Boolean, Bean> value) {
            booleanKeys = value;
        }

        public TreeMap<Long, Bean> getLongKeys() {
            return longKeys;
        }

        public void setLongKeys(final TreeMap<Long, Bean> value) {
            longKeys = value;
        }

        public NativeLeaf getDirect() {
            return direct;
        }

        public void setDirect(final NativeLeaf value) {
            direct = value;
        }

        public NativeLeaf[] getArray() {
            return array;
        }

        public void setArray(final NativeLeaf[] value) {
            array = value;
        }

        public List<NativeLeaf> getList() {
            return list;
        }

        public void setList(final List<NativeLeaf> value) {
            list = value;
        }

        public List<FormattedNumber> getFormatted() {
            return formatted;
        }

        public void setFormatted(final List<FormattedNumber> value) {
            formatted = value;
        }

        public CopyOnWriteArrayList<Object> getCopies() {
            return copies;
        }

        public void setCopies(final CopyOnWriteArrayList<Object> value) {
            copies = value;
        }
    }

    public static class NativeLeaf {
        private Map<Boolean, Bean> booleanKeys;
        private TreeMap<Long, Bean> longKeys;
        private ConcurrentHashMap<String, Bean> concurrent;
        private ArrayBlockingQueue<Bean> queue;
        private List<String> strings;

        public Map<Boolean, Bean> getBooleanKeys() {
            return booleanKeys;
        }

        public void setBooleanKeys(final Map<Boolean, Bean> value) {
            booleanKeys = value;
        }

        public TreeMap<Long, Bean> getLongKeys() {
            return longKeys;
        }

        public void setLongKeys(final TreeMap<Long, Bean> value) {
            longKeys = value;
        }

        public ConcurrentHashMap<String, Bean> getConcurrent() {
            return concurrent;
        }

        public void setConcurrent(final ConcurrentHashMap<String, Bean> value) {
            concurrent = value;
        }

        public ArrayBlockingQueue<Bean> getQueue() {
            return queue;
        }

        public void setQueue(final ArrayBlockingQueue<Bean> value) {
            queue = value;
        }

        public List<String> getStrings() {
            return strings;
        }

        public void setStrings(final List<String> value) {
            strings = value;
        }
    }

    public static class Bean {
        private String name;
        private ByteBuffer buffer;
        private byte[] bytes;

        public String getName() {
            return name;
        }

        public void setName(final String value) {
            name = value;
        }

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

    public static class FormattedNumber {
        @JsonXmlField(dateFormat = "yyyy-MM-dd")
        private Number value;

        public Number getValue() {
            return value;
        }

        public void setValue(final Number value) {
            this.value = value;
        }
    }
}
