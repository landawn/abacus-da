package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryResult;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

/**
 * A {@code B} attribute read into a type that cannot hold a binary value (a String, number, date, UUID, enum, bean, map or
 * primitive array). SDK v1 hands the attribute out as a read-mode {@link ByteBuffer}, which abacus-common converts only through
 * its (empty) write-mode text form, so the v1 executor silently produced {@code ""}, {@code null} or a default; SDK v2's
 * {@code byte[]} converts from its bytes. {@code DynamoDBExecutorBase.convertValue} documents one shared rule for both SDKs.
 */
public class DynamoDBExecutorBaseDeepReview2026BinaryScalarTest {

    // ---- 2026-10-24 sliceB ----

    private static final byte[] BYTES = { 1, 2, 3 };

    private static AttributeValue v1Binary() {
        return new AttributeValue().withB(ByteBuffer.wrap(BYTES.clone()));
    }

    private static software.amazon.awssdk.services.dynamodb.model.AttributeValue v2Binary() {
        return software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().b(SdkBytes.fromByteArray(BYTES.clone())).build();
    }

    private static Target v1Item(final String property) {
        return DynamoDBExecutor.toEntity(Map.of(property, v1Binary()), Target.class);
    }

    private static Target v2Item(final String property) {
        return com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(Map.of(property, v2Binary()), Target.class);
    }

    private static <T> T v1Single(final Class<T> targetClass) {
        return DynamoDBExecutor.toList(new QueryResult().withItems(List.of(Map.of("v", v1Binary()))), targetClass).get(0);
    }

    private static <T> T v2Single(final Class<T> targetClass) {
        return com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                .toList(QueryResponse.builder().items(List.of(Map.of("v", v2Binary()))).build(), targetClass)
                .get(0);
    }

    /** Renders a result or the failure's class, so that both SDKs' outcomes can be compared. */
    private static String outcome(final Supplier<?> read) {
        try {
            final Object value = read.get();
            return value instanceof final long[] array ? "long" + java.util.Arrays.toString(array)
                    : value instanceof final byte[] array ? "byte" + java.util.Arrays.toString(array)
                            : value instanceof final Object[] array ? java.util.Arrays.toString(array) : String.valueOf(value);
        } catch (final RuntimeException e) {
            return e.getClass().getName();
        }
    }

    @Test
    public void itemPropertiesThatCannotHoldBinaryFailInsteadOfReadingNullOrEmptyText() {
        assertAll(() -> assertThrows(NumberFormatException.class, () -> v1Item("boxedLong")),
                () -> assertThrows(NumberFormatException.class, () -> v1Item("primitiveInt")),
                () -> assertThrows(NumberFormatException.class, () -> v1Item("decimal")),
                () -> assertThrows(IllegalArgumentException.class, () -> v1Item("date")),
                () -> assertThrows(IllegalArgumentException.class, () -> v1Item("uuid")),
                () -> assertThrows(IllegalArgumentException.class, () -> v1Item("color")),
                () -> assertThrows(NumberFormatException.class, () -> v1Item("optionalLong")),
                () -> assertThrows(RuntimeException.class, () -> v1Item("nested")),
                // A String receives the bytes' text, as from SDK v2's byte[], instead of an empty String.
                () -> assertEquals("[1, 2, 3]", v1Item("text").getText()), () -> assertEquals("[1, 2, 3]", v2Item("text").getText()),
                () -> assertArrayEquals(new long[] { 1, 2, 3 }, v1Item("longs").getLongs()));
    }

    @Test
    public void directAndJsonContextBeansApplyTheSameRule() {
        final AttributeValue nested = new AttributeValue().withM(Map.of("value", v1Binary()));
        final AttributeValue nestedText = new AttributeValue().withM(Map.of("text", v1Binary()));

        assertAll(
                // M attribute -> bean (direct map-to-bean conversion).
                () -> assertThrows(NumberFormatException.class, () -> DynamoDBExecutor.toEntity(Map.of("nested", nested), Target.class)),
                () -> assertEquals("[1, 2, 3]", DynamoDBExecutor.toEntity(Map.of("nested", nestedText), Target.class).getNested().getText()),
                // Element of an L attribute (JSON-context bean).
                () -> assertThrows(NumberFormatException.class,
                        () -> DynamoDBExecutor.toEntity(Map.of("nestedList", new AttributeValue().withL(nested)), Target.class)),
                () -> assertEquals("[1, 2, 3]",
                        DynamoDBExecutor.toEntity(Map.of("nestedList", new AttributeValue().withL(nestedText)), Target.class)
                                .getNestedList()
                                .get(0)
                                .getText()));
    }

    @Test
    public void singleValueTargetsMatchSdkV2() {
        final List<Class<?>> targets = List.of(String.class, Integer.class, int.class, Long.class, long.class, double.class, BigDecimal.class, Character.class,
                Date.class, java.time.Instant.class, UUID.class, java.util.OptionalLong.class, java.util.concurrent.atomic.AtomicLong.class, Color.class,
                String[].class, long[].class, byte[].class);
        final List<org.junit.jupiter.api.function.Executable> checks = new ArrayList<>();

        for (final Class<?> target : targets) {
            checks.add(() -> assertEquals(outcome(() -> v2Single(target)), outcome(() -> v1Single(target)), target.getName()));
        }

        // Before, every one of these but byte[] and boolean-like failures read null, 0, "" or an empty OptionalLong under SDK v1.
        checks.add(() -> assertThrows(NumberFormatException.class, () -> v1Single(Long.class)));
        checks.add(() -> assertThrows(NumberFormatException.class, () -> v1Single(int.class)));
        checks.add(() -> assertEquals("[1, 2, 3]", v1Single(String.class)));
        assertAll(checks);
    }

    @Test
    public void typesThatCanHoldTheBufferKeepItAndTheSourceBufferIsNotConsumed() {
        final AttributeValue binary = v1Binary();
        final ByteBuffer source = binary.getB();

        final Object object = DynamoDBExecutor.toList(new QueryResult().withItems(List.of(Map.of("v", binary))), Object.class).get(0);
        final Comparable<?> comparable = DynamoDBExecutor.toList(new QueryResult().withItems(List.of(Map.of("v", binary))), Comparable.class).get(0);
        final List<?> list = DynamoDBExecutor.toList(new QueryResult().withItems(List.of(Map.of("v", binary))), List.class).get(0);
        final Object[] array = DynamoDBExecutor.toList(new QueryResult().withItems(List.of(Map.of("v", binary))), Object[].class).get(0);
        final byte[] bytes = DynamoDBExecutor.toList(new QueryResult().withItems(List.of(Map.of("v", binary))), byte[].class).get(0);
        assertThrows(NumberFormatException.class, () -> DynamoDBExecutor.toList(new QueryResult().withItems(List.of(Map.of("v", binary))), Long.class));

        assertAll(() -> assertSame(source, object), () -> assertSame(source, comparable), () -> assertEquals(List.of(source), list),
                () -> assertSame(source, array[0]), () -> assertArrayEquals(BYTES, bytes), //
                () -> assertEquals(0, source.position()), () -> assertEquals(3, source.remaining()),
                () -> assertTrue(v1Item("buffer").getBuffer().equals(ByteBuffer.wrap(BYTES))), () -> assertArrayEquals(BYTES, v1Item("bytes").getBytes()));
    }

    // ---- 2026-10-24 verify1 ----

    private static AttributeValue v1BinarySet() {
        return new AttributeValue().withBS(ByteBuffer.wrap(new byte[] { 1 }), ByteBuffer.wrap(new byte[] { 2, 3 }));
    }

    private static software.amazon.awssdk.services.dynamodb.model.AttributeValue v2BinarySet() {
        return software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder()
                .bs(SdkBytes.fromByteArray(new byte[] { 1 }), SdkBytes.fromByteArray(new byte[] { 2, 3 }))
                .build();
    }

    private static AttributeValue v1ListOf(final AttributeValue... elements) {
        return new AttributeValue().withL(elements);
    }

    private static software.amazon.awssdk.services.dynamodb.model.AttributeValue v2ListOf(
            final software.amazon.awssdk.services.dynamodb.model.AttributeValue... elements) {
        return software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().l(elements).build();
    }

    private static AttributeValue v1MapOf(final String key, final AttributeValue value) {
        return new AttributeValue().withM(Map.of(key, value));
    }

    private static software.amazon.awssdk.services.dynamodb.model.AttributeValue v2MapOf(final String key,
            final software.amazon.awssdk.services.dynamodb.model.AttributeValue value) {
        return software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().m(Map.of(key, value)).build();
    }

    private static Elements v1Elements(final String name, final AttributeValue value) {
        return DynamoDBExecutor.toEntity(Map.of(name, value), Elements.class);
    }

    private static Elements v2Elements(final String name, final software.amazon.awssdk.services.dynamodb.model.AttributeValue value) {
        return com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(Map.of(name, value), Elements.class);
    }

    private static Parent v1Parent(final String name, final AttributeValue value) {
        return DynamoDBExecutor.toEntity(Map.of(name, value), Parent.class);
    }

    private static Parent v2Parent(final String name, final software.amazon.awssdk.services.dynamodb.model.AttributeValue value) {
        return com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(Map.of(name, value), Parent.class);
    }

    /** SDK v1 reads the attribute exactly as SDK v2 reads the same attribute, with the expected outcome (value text or failure class). */
    private static org.junit.jupiter.api.function.Executable sameAsV2(final String expected, final Supplier<?> v1, final Supplier<?> v2) {
        return () -> {
            final String v1Outcome = outcome(v1);
            assertEquals(outcome(v2), v1Outcome, "SDK v1 vs SDK v2");
            assertEquals(expected, v1Outcome);
        };
    }

    /**
     * The buffers inside BS, L and M attributes are converted like a top-level B attribute: an element of a typed object
     * container receives its bytes' text, or fails the whole item for a number type, as SDK v2's byte[] elements do. Before,
     * SDK v1 silently stored "" or null for each element.
     */
    @Test
    public void binaryElementsOfSetsListsAndMapsConvertAsInSdkV2() {
        final String numberFormat = NumberFormatException.class.getName();

        assertAll(
                // BS attribute into typed object containers.
                sameAsV2("[[1], [2, 3]]", () -> v1Elements("strings", v1BinarySet()).getStrings(), () -> v2Elements("strings", v2BinarySet()).getStrings()),
                sameAsV2("[[1], [2, 3]]", () -> v1Elements("stringArray", v1BinarySet()).getStringArray(),
                        () -> v2Elements("stringArray", v2BinarySet()).getStringArray()),
                sameAsV2(numberFormat, () -> v1Elements("longs", v1BinarySet()), () -> v2Elements("longs", v2BinarySet())),
                // Binary elements of L and M attributes for String and Long targets.
                sameAsV2("[[1, 2, 3]]", () -> v1Elements("strings", v1ListOf(v1Binary())).getStrings(),
                        () -> v2Elements("strings", v2ListOf(v2Binary())).getStrings()),
                sameAsV2(numberFormat, () -> v1Elements("longs", v1ListOf(v1Binary())), () -> v2Elements("longs", v2ListOf(v2Binary()))),
                sameAsV2("{k=[1, 2, 3]}", () -> v1Elements("textMap", v1MapOf("k", v1Binary())).getTextMap(),
                        () -> v2Elements("textMap", v2MapOf("k", v2Binary())).getTextMap()),
                sameAsV2(numberFormat, () -> v1Elements("longMap", v1MapOf("k", v1Binary())), () -> v2Elements("longMap", v2MapOf("k", v2Binary()))),
                // A direct bean (M attribute) and a JSON-context bean (element of an L attribute).
                sameAsV2("[[1], [2, 3]]", () -> v1Parent("direct", v1MapOf("strings", v1BinarySet())).getDirect().getStrings(),
                        () -> v2Parent("direct", v2MapOf("strings", v2BinarySet())).getDirect().getStrings()),
                sameAsV2("[[1, 2, 3]]", () -> v1Parent("children", v1ListOf(v1MapOf("strings", v1ListOf(v1Binary())))).getChildren().get(0).getStrings(),
                        () -> v2Parent("children", v2ListOf(v2MapOf("strings", v2ListOf(v2Binary())))).getChildren().get(0).getStrings()),
                sameAsV2(numberFormat, () -> v1Parent("children", v1ListOf(v1MapOf("longMap", v1MapOf("k", v1Binary())))),
                        () -> v2Parent("children", v2ListOf(v2MapOf("longMap", v2MapOf("k", v2Binary()))))),
                // A raw Optional row class.
                sameAsV2("Optional[[1, 2, 3]]", () -> v1Single(java.util.Optional.class), () -> v2Single(java.util.Optional.class)));
    }

    /**
     * An attribute named by a dotted path ("direct.text") is converted like the same property at top level. The path setter of
     * abacus-common applied only its raw conversion: an SDK v1 buffer became "" or null, an SDK v2 byte[] for a ByteBuffer an
     * empty write-mode buffer, and number text stayed String elements of a List&lt;Long&gt;.
     */
    @Test
    public void dottedAttributeNamesReadLikeTheSamePropertyAtTopLevel() {
        final AttributeValue v1Numbers = v1ListOf(new AttributeValue().withN("1"), new AttributeValue().withN("2"));
        final software.amazon.awssdk.services.dynamodb.model.AttributeValue v2Numbers = v2ListOf(
                software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromN("1"),
                software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromN("2"));

        assertAll(() -> assertEquals("[1, 2, 3]", v1Parent("direct.text", v1Binary()).getDirect().getText()),
                () -> assertEquals(v1Elements("text", v1Binary()).getText(), v1Parent("direct.text", v1Binary()).getDirect().getText()),
                () -> assertArrayEquals(BYTES, v1Parent("direct.bytes", v1Binary()).getDirect().getBytes()),
                () -> assertThrows(NumberFormatException.class, () -> v1Parent("direct.number", v1Binary())),
                // A collection on the path: the value goes to its (created) first element.
                () -> assertEquals("[1, 2, 3]", v1Parent("children.text", v1Binary()).getChildren().get(0).getText()),
                () -> assertEquals(ByteBuffer.wrap(BYTES), v2Parent("direct.buffer", v2Binary()).getDirect().getBuffer()),
                () -> assertEquals(0, v2Parent("direct.buffer", v2Binary()).getDirect().getBuffer().position()),
                () -> assertEquals(List.of(1L, 2L), v1Parent("direct.longs", v1Numbers).getDirect().getLongs()),
                () -> assertEquals(List.of(1L, 2L), v2Parent("direct.longs", v2Numbers).getDirect().getLongs()));
    }

    /** Paths that the setter ignores (read-only last property, unknown property) still create nothing and do not fail. */
    @Test
    public void dottedPathsTheSetterIgnoresStayIgnored() {
        final AttributeValue text = new AttributeValue().withS("v");

        assertAll(() -> org.junit.jupiter.api.Assertions.assertNull(v1Parent("direct.fixed", text).getDirect()),
                () -> org.junit.jupiter.api.Assertions.assertNull(v1Parent("direct.unknown", text).getDirect()),
                () -> org.junit.jupiter.api.Assertions.assertNull(v1Parent("unknown.text", text).getDirect()),
                () -> assertEquals("v", v1Parent("direct.text", text).getDirect().getText()));
    }

    public static class Elements {
        private final String fixed = "fixed";
        private List<String> strings;
        private String[] stringArray;
        private List<Long> longs;
        private Map<String, String> textMap;
        private Map<String, Long> longMap;
        private String text;
        private Long number;
        private byte[] bytes;
        private ByteBuffer buffer;

        public String getFixed() {
            return fixed;
        }

        public List<String> getStrings() {
            return strings;
        }

        public void setStrings(final List<String> strings) {
            this.strings = strings;
        }

        public String[] getStringArray() {
            return stringArray;
        }

        public void setStringArray(final String[] stringArray) {
            this.stringArray = stringArray;
        }

        public List<Long> getLongs() {
            return longs;
        }

        public void setLongs(final List<Long> longs) {
            this.longs = longs;
        }

        public Map<String, String> getTextMap() {
            return textMap;
        }

        public void setTextMap(final Map<String, String> textMap) {
            this.textMap = textMap;
        }

        public Map<String, Long> getLongMap() {
            return longMap;
        }

        public void setLongMap(final Map<String, Long> longMap) {
            this.longMap = longMap;
        }

        public String getText() {
            return text;
        }

        public void setText(final String text) {
            this.text = text;
        }

        public Long getNumber() {
            return number;
        }

        public void setNumber(final Long number) {
            this.number = number;
        }

        public byte[] getBytes() {
            return bytes;
        }

        public void setBytes(final byte[] bytes) {
            this.bytes = bytes;
        }

        public ByteBuffer getBuffer() {
            return buffer;
        }

        public void setBuffer(final ByteBuffer buffer) {
            this.buffer = buffer;
        }
    }

    public static class Parent {
        private Elements direct;
        private List<Elements> children;

        public Elements getDirect() {
            return direct;
        }

        public void setDirect(final Elements direct) {
            this.direct = direct;
        }

        public List<Elements> getChildren() {
            return children;
        }

        public void setChildren(final List<Elements> children) {
            this.children = children;
        }
    }

    public enum Color {
        RED
    }

    public static class Nested {
        private Long value;
        private String text;

        public Long getValue() {
            return value;
        }

        public void setValue(final Long value) {
            this.value = value;
        }

        public String getText() {
            return text;
        }

        public void setText(final String text) {
            this.text = text;
        }
    }

    public static class Target {
        private Long boxedLong;
        private int primitiveInt;
        private BigDecimal decimal;
        private Date date;
        private UUID uuid;
        private Color color;
        private String text;
        private Optional<Long> optionalLong;
        private long[] longs;
        private byte[] bytes;
        private ByteBuffer buffer;
        private Nested nested;
        private List<Nested> nestedList;

        public Long getBoxedLong() {
            return boxedLong;
        }

        public void setBoxedLong(final Long boxedLong) {
            this.boxedLong = boxedLong;
        }

        public int getPrimitiveInt() {
            return primitiveInt;
        }

        public void setPrimitiveInt(final int primitiveInt) {
            this.primitiveInt = primitiveInt;
        }

        public BigDecimal getDecimal() {
            return decimal;
        }

        public void setDecimal(final BigDecimal decimal) {
            this.decimal = decimal;
        }

        public Date getDate() {
            return date;
        }

        public void setDate(final Date date) {
            this.date = date;
        }

        public UUID getUuid() {
            return uuid;
        }

        public void setUuid(final UUID uuid) {
            this.uuid = uuid;
        }

        public Color getColor() {
            return color;
        }

        public void setColor(final Color color) {
            this.color = color;
        }

        public String getText() {
            return text;
        }

        public void setText(final String text) {
            this.text = text;
        }

        public Optional<Long> getOptionalLong() {
            return optionalLong;
        }

        public void setOptionalLong(final Optional<Long> optionalLong) {
            this.optionalLong = optionalLong;
        }

        public long[] getLongs() {
            return longs;
        }

        public void setLongs(final long[] longs) {
            this.longs = longs;
        }

        public byte[] getBytes() {
            return bytes;
        }

        public void setBytes(final byte[] bytes) {
            this.bytes = bytes;
        }

        public ByteBuffer getBuffer() {
            return buffer;
        }

        public void setBuffer(final ByteBuffer buffer) {
            this.buffer = buffer;
        }

        public Nested getNested() {
            return nested;
        }

        public void setNested(final Nested nested) {
            this.nested = nested;
        }

        public List<Nested> getNestedList() {
            return nestedList;
        }

        public void setNestedList(final List<Nested> nestedList) {
            this.nestedList = nestedList;
        }
    }
}
