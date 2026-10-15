package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.landawn.abacus.annotation.JsonXmlField;
import com.landawn.abacus.util.EnumType;
import com.landawn.abacus.util.Holder;
import com.landawn.abacus.util.Pair;
import com.landawn.abacus.util.u.Nullable;

import software.amazon.awssdk.core.SdkBytes;

public class DynamoDBTypedMappingRegressionTest {
    private static final String DATE_TEXT = "2026-10-14T12:34:56.123Z";

    @Test
    public void nativeBeansAndArraysPreserveScalarWrapperTypeArguments() {
        forEachSdk(sdk -> forEachContext(context -> assertScalarWrappers(readLeaf(sdk, context,
                Map.of("optionalLong", 12, "optionalDate", DATE_TEXT, "optionalEnum", "SECOND", "abacusLong", 12, "nullableLong", 12, "heldLong", 12)))));
    }

    @Test
    public void nativeAndSerializedPairsAndEntriesPreserveBothTypeArguments() {
        forEachSdk(sdk -> forEachContext(
                context -> assertAll(() -> assertPairs(readLeaf(sdk, context, Map.of("pair", List.of(11, 12), "entry", Map.of("11", 12)))),
                        () -> assertPairs(readLeaf(sdk, context, Map.of("pair", "[11,12]", "entry", "{\"11\":12}"))))));
    }

    @Test
    public void enumCodesDoNotFallBackToOrdinalsInNativeBeansOrArrays() {
        forEachSdk(sdk -> forEachContext(context -> assertAll(() -> assertSame(Status.SECOND, readLeaf(sdk, context, Map.of("coded", 20)).getCoded()),
                () -> assertThrows(IllegalArgumentException.class, () -> readLeaf(sdk, context, Map.of("coded", 1))))));
    }

    @Test
    public void resolvedGenericNumberPropertiesUseNumericInference() {
        forEachSdk(sdk -> forEachContext(context -> assertAll(
                () -> assertEquals(12, assertInstanceOf(Integer.class, readNumber(sdk, context, Map.of("value", 12)).getValue())),
                () -> assertEquals(1.25d, assertInstanceOf(Double.class, readNumber(sdk, context, Map.of("value", 1.25d)).getValue())),
                () -> assertNull(readNumber(sdk, context, nullableMap("value")).getValue()), () -> assertNull(readNumber(sdk, context, Map.of()).getValue()))));
    }

    @Test
    public void wrapperPropertiesDistinguishExplicitNullFromAbsentValues() {
        forEachSdk(sdk -> forEachContext(context -> {
            final TypedLeaf nil = readLeaf(sdk, context, nullableMap("optionalLong", "optionalDate", "optionalEnum", "abacusLong", "nullableLong", "heldLong"));
            assertAll(() -> assertEquals(Optional.empty(), nil.getOptionalLong()), () -> assertEquals(Optional.empty(), nil.getOptionalDate()),
                    () -> assertEquals(Optional.empty(), nil.getOptionalEnum()),
                    () -> assertEquals(com.landawn.abacus.util.u.Optional.empty(), nil.getAbacusLong()),
                    () -> assertEquals(Nullable.empty(), nil.getNullableLong()), () -> assertNull(nil.getHeldLong().value()));

            final TypedLeaf absent = readLeaf(sdk, context, Map.of());
            assertAll(() -> assertNull(absent.getOptionalLong()), () -> assertNull(absent.getOptionalDate()), () -> assertNull(absent.getOptionalEnum()),
                    () -> assertNull(absent.getAbacusLong()), () -> assertNull(absent.getNullableLong()), () -> assertNull(absent.getHeldLong()));
        }));
    }

    @Test
    public void nestedNativeCollectionsRetainDatabaseAndJsonExposureRules() {
        forEachSdk(sdk -> forEachContext(context -> {
            final TypedLeaf leaf = readLeaf(sdk, context, Map.of("children", List.of(Map.of("readOnly", "from-database"))));
            assertEquals(context == Context.LIST ? "default" : "from-database", leaf.getChildren().get(0).getReadOnly());
        }));
    }

    @Test
    public void topLevelWrapperRoundTripsAndSerializedBeansRetainTheirDeclaredTypes() {
        forEachSdk(sdk -> {
            final SimpleBean bean = new SimpleBean();
            bean.setName("alpha");
            final WrapperRoot original = new WrapperRoot();
            original.setNumber(Optional.of(123L));
            original.setBean(Optional.of(bean));

            assertAll(() -> assertTopLevelWrappers(sdk.roundTrip(original, WrapperRoot.class)),
                    () -> assertTopLevelWrappers(sdk.read(Map.of("number", "123", "bean", "{\"name\":\"alpha\"}"), WrapperRoot.class)),
                    () -> assertTopLevelWrappers(sdk.read(Map.of("number", 123, "bean", Map.of("name", "alpha")), WrapperRoot.class)));
        });
    }

    @Test
    public void topLevelWrapperNullEmptyAndAbsentValuesKeepTheirDefaults() {
        forEachSdk(sdk -> {
            for (final Map<String, Object> item : List.of(nullableMap("number", "bean"), Map.<String, Object> of("number", "", "bean", ""))) {
                final WrapperRoot empty = sdk.read(item, WrapperRoot.class);
                assertEquals(Optional.empty(), empty.getNumber());
                assertEquals(Optional.empty(), empty.getBean());
            }
            final WrapperRoot absent = sdk.read(Map.of(), WrapperRoot.class);
            assertNull(absent.getNumber());
            assertNull(absent.getBean());
        });
    }

    @Test
    public void wrappedBeansRetainJsonMetadataForNativeTextAndRoundTripValues() {
        forEachSdk(sdk -> {
            final SimpleBean bean = new SimpleBean();
            bean.setDate(LocalDate.of(2026, 12, 31));
            bean.setCoded(Status.SECOND);
            bean.setReadOnly("stored");
            final WrapperRoot original = new WrapperRoot();
            original.setBean(Optional.of(bean));

            final Map<String, Object> nativeBean = Map.of("date", "31/12/2026", "coded", 20, "readOnly", "stored");
            final String serializedBean = "{\"date\":\"31/12/2026\",\"coded\":20,\"readOnly\":\"stored\"}";
            assertAll(() -> assertWrappedMetadata(sdk.read(Map.of("bean", nativeBean), WrapperRoot.class)),
                    () -> assertWrappedMetadata(sdk.read(Map.of("bean", serializedBean), WrapperRoot.class)),
                    () -> assertWrappedMetadata(sdk.roundTrip(original, WrapperRoot.class)));
        });
    }

    static void forEachSdk(final Consumer<Sdk> action) {
        assertAll("both DynamoDB SDKs", Arrays.stream(Sdk.values()).map(sdk -> () -> assertAll(sdk.name(), () -> action.accept(sdk))));
    }

    private static void forEachContext(final Consumer<Context> action) {
        assertAll("native bean, native array, and JSON collection contexts",
                Arrays.stream(Context.values()).map(context -> () -> assertAll(context.name(), () -> action.accept(context))));
    }

    static Map<String, Object> nullableMap(final String... keys) {
        final Map<String, Object> result = new LinkedHashMap<>();
        for (final String key : keys) {
            result.put(key, null);
        }
        return result;
    }

    private static TypedLeaf readLeaf(final Sdk sdk, final Context context, final Map<String, Object> leaf) {
        final TypedRoot result = sdk.read(Map.of(context.property, context == Context.DIRECT ? leaf : List.of(leaf)), TypedRoot.class);
        return switch (context) {
            case DIRECT -> result.getDirect();
            case ARRAY -> result.getArray()[0];
            case LIST -> result.getList().get(0);
        };
    }

    private static GenericValue<Number> readNumber(final Sdk sdk, final Context context, final Map<String, Object> leaf) {
        final NumberRoot result = sdk.read(Map.of(context.property, context == Context.DIRECT ? leaf : List.of(leaf)), NumberRoot.class);
        return switch (context) {
            case DIRECT -> result.getDirect();
            case ARRAY -> result.getArray()[0];
            case LIST -> result.getList().get(0);
        };
    }

    private static void assertScalarWrappers(final TypedLeaf leaf) {
        assertAll(() -> assertEquals(12L, leaf.getOptionalLong().orElseThrow().longValue()),
                () -> assertEquals(Instant.parse(DATE_TEXT).toEpochMilli(), leaf.getOptionalDate().orElseThrow().getTime()),
                () -> assertSame(Status.SECOND, leaf.getOptionalEnum().orElseThrow()), () -> assertEquals(12L, leaf.getAbacusLong().get().longValue()),
                () -> assertEquals(12L, leaf.getNullableLong().get().longValue()), () -> assertEquals(12L, leaf.getHeldLong().value().longValue()));
    }

    private static void assertPairs(final TypedLeaf leaf) {
        assertAll(() -> assertEquals(11L, leaf.getPair().getKey().longValue()), () -> assertEquals(12L, leaf.getPair().getValue().longValue()),
                () -> assertEquals(11L, leaf.getEntry().getKey().longValue()), () -> assertEquals(12L, leaf.getEntry().getValue().longValue()));
    }

    private static void assertTopLevelWrappers(final WrapperRoot root) {
        assertAll(() -> assertEquals(123L, root.getNumber().orElseThrow().longValue()), () -> assertEquals("alpha", root.getBean().orElseThrow().getName()));
    }

    private static void assertWrappedMetadata(final WrapperRoot root) {
        final SimpleBean bean = root.getBean().orElseThrow();
        assertEquals(LocalDate.of(2026, 12, 31), bean.getDate());
        assertSame(Status.SECOND, bean.getCoded());
        assertEquals("default", bean.getReadOnly());
    }

    private enum Context {
        DIRECT("direct"), ARRAY("array"), LIST("list");

        private final String property;

        Context(final String property) {
            this.property = property;
        }
    }

    enum Sdk {
        V1, V2;

        <T> T read(final Map<String, Object> item, final Class<T> targetClass) {
            if (this == V1) {
                return DynamoDBExecutor.toEntity(legacyItem(item), targetClass);
            }
            return com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(v2Item(item), targetClass);
        }

        <T> T roundTrip(final T bean, final Class<T> targetClass) {
            if (this == V1) {
                return DynamoDBExecutor.toEntity(DynamoDBExecutor.toItem(bean), targetClass);
            }
            return com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toItem(bean),
                    targetClass);
        }

        private static Map<String, AttributeValue> legacyItem(final Map<String, Object> item) {
            final Map<String, AttributeValue> result = new LinkedHashMap<>();
            item.forEach((key, value) -> result.put(key, legacyValue(value)));
            return result;
        }

        @SuppressWarnings("unchecked")
        private static AttributeValue legacyValue(final Object value) {
            if (value == null) {
                return new AttributeValue().withNULL(true);
            } else if (value instanceof Map<?, ?> map) {
                return new AttributeValue().withM(legacyItem((Map<String, Object>) map));
            } else if (value instanceof List<?> list) {
                return new AttributeValue().withL(list.stream().map(Sdk::legacyValue).toList());
            } else if (value instanceof Number number) {
                return new AttributeValue().withN(number.toString());
            } else if (value instanceof ByteBuffer buffer) {
                return new AttributeValue().withB(buffer);
            }
            return new AttributeValue().withS((String) value);
        }

        private static Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> v2Item(final Map<String, Object> item) {
            final Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> result = new LinkedHashMap<>();
            item.forEach((key, value) -> result.put(key, v2Value(value)));
            return result;
        }

        @SuppressWarnings("unchecked")
        private static software.amazon.awssdk.services.dynamodb.model.AttributeValue v2Value(final Object value) {
            if (value == null) {
                return software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromNul(true);
            } else if (value instanceof Map<?, ?> map) {
                return software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(v2Item((Map<String, Object>) map));
            } else if (value instanceof List<?> list) {
                return software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(list.stream().map(Sdk::v2Value).toList());
            } else if (value instanceof Number number) {
                return software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromN(number.toString());
            } else if (value instanceof ByteBuffer buffer) {
                return software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromB(SdkBytes.fromByteBuffer(buffer));
            }
            return software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS((String) value);
        }
    }

    public enum Status {
        FIRST(10), SECOND(20);

        private final int code;

        Status(final int code) {
            this.code = code;
        }

        public int code() {
            return code;
        }
    }

    public static class TypedRoot {
        private TypedLeaf direct;
        private TypedLeaf[] array;
        private List<TypedLeaf> list;

        public TypedLeaf getDirect() {
            return direct;
        }

        public void setDirect(final TypedLeaf value) {
            direct = value;
        }

        public TypedLeaf[] getArray() {
            return array;
        }

        public void setArray(final TypedLeaf[] value) {
            array = value;
        }

        public List<TypedLeaf> getList() {
            return list;
        }

        public void setList(final List<TypedLeaf> value) {
            list = value;
        }
    }

    public static class TypedLeaf {
        private Optional<Long> optionalLong;
        private Optional<Date> optionalDate;
        private Optional<Status> optionalEnum;
        private com.landawn.abacus.util.u.Optional<Long> abacusLong;
        private Nullable<Long> nullableLong;
        private Holder<Long> heldLong;
        private Pair<Long, Long> pair;
        private Map.Entry<Long, Long> entry;
        @JsonXmlField(enumerated = EnumType.CODE)
        private Status coded;
        private List<ExposureLeaf> children;

        public Optional<Long> getOptionalLong() {
            return optionalLong;
        }

        public void setOptionalLong(final Optional<Long> value) {
            optionalLong = value;
        }

        public Optional<Date> getOptionalDate() {
            return optionalDate;
        }

        public void setOptionalDate(final Optional<Date> value) {
            optionalDate = value;
        }

        public Optional<Status> getOptionalEnum() {
            return optionalEnum;
        }

        public void setOptionalEnum(final Optional<Status> value) {
            optionalEnum = value;
        }

        public com.landawn.abacus.util.u.Optional<Long> getAbacusLong() {
            return abacusLong;
        }

        public void setAbacusLong(final com.landawn.abacus.util.u.Optional<Long> value) {
            abacusLong = value;
        }

        public Nullable<Long> getNullableLong() {
            return nullableLong;
        }

        public void setNullableLong(final Nullable<Long> value) {
            nullableLong = value;
        }

        public Holder<Long> getHeldLong() {
            return heldLong;
        }

        public void setHeldLong(final Holder<Long> value) {
            heldLong = value;
        }

        public Pair<Long, Long> getPair() {
            return pair;
        }

        public void setPair(final Pair<Long, Long> value) {
            pair = value;
        }

        public Map.Entry<Long, Long> getEntry() {
            return entry;
        }

        public void setEntry(final Map.Entry<Long, Long> value) {
            entry = value;
        }

        public Status getCoded() {
            return coded;
        }

        public void setCoded(final Status value) {
            coded = value;
        }

        public List<ExposureLeaf> getChildren() {
            return children;
        }

        public void setChildren(final List<ExposureLeaf> value) {
            children = value;
        }
    }

    public static class ExposureLeaf {
        @JsonXmlField(direction = JsonXmlField.Direction.SERIALIZE_ONLY)
        private String readOnly = "default";

        public String getReadOnly() {
            return readOnly;
        }

        public void setReadOnly(final String value) {
            readOnly = value;
        }
    }

    public static class NumberRoot {
        private GenericValue<Number> direct;
        private GenericValue<Number>[] array;
        private List<GenericValue<Number>> list;

        public GenericValue<Number> getDirect() {
            return direct;
        }

        public void setDirect(final GenericValue<Number> value) {
            direct = value;
        }

        public GenericValue<Number>[] getArray() {
            return array;
        }

        public void setArray(final GenericValue<Number>[] value) {
            array = value;
        }

        public List<GenericValue<Number>> getList() {
            return list;
        }

        public void setList(final List<GenericValue<Number>> value) {
            list = value;
        }
    }

    public static class GenericValue<T> {
        private T value;

        public T getValue() {
            return value;
        }

        public void setValue(final T value) {
            this.value = value;
        }
    }

    public static class WrapperRoot {
        private Optional<Long> number;
        private Optional<SimpleBean> bean;

        public Optional<Long> getNumber() {
            return number;
        }

        public void setNumber(final Optional<Long> value) {
            number = value;
        }

        public Optional<SimpleBean> getBean() {
            return bean;
        }

        public void setBean(final Optional<SimpleBean> value) {
            bean = value;
        }
    }

    public static class SimpleBean {
        private String name;
        @JsonXmlField(dateFormat = "dd/MM/uuuu")
        private LocalDate date;
        @JsonXmlField(enumerated = EnumType.CODE)
        private Status coded;
        @JsonXmlField(direction = JsonXmlField.Direction.SERIALIZE_ONLY)
        private String readOnly = "default";

        public String getName() {
            return name;
        }

        public void setName(final String value) {
            name = value;
        }

        public LocalDate getDate() {
            return date;
        }

        public void setDate(final LocalDate value) {
            date = value;
        }

        public Status getCoded() {
            return coded;
        }

        public void setCoded(final Status value) {
            coded = value;
        }

        public String getReadOnly() {
            return readOnly;
        }

        public void setReadOnly(final String value) {
            readOnly = value;
        }
    }
}
