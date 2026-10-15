package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.landawn.abacus.annotation.JsonXmlConfig;
import com.landawn.abacus.annotation.JsonXmlField;
import com.landawn.abacus.util.EnumType;
import com.landawn.abacus.util.Holder;
import com.landawn.abacus.util.N;

public class DynamoDBMetadataRegressionTest {
    @Test
    public void nativeNestedBeansHonorReadMetadataWithAndWithoutBinaryValues() {
        for (final boolean withBinary : new boolean[] { false, true }) {
            final Map<String, Object> data = metadataValues();
            if (withBinary) {
                data.put("buffer", ByteBuffer.wrap(new byte[] { 1, 2, -3 }));
            }

            final Map<String, AttributeValue> legacyLeaf = DynamoDBExecutor.toItem(data);
            legacyLeaf.put("raw", new AttributeValue().withM(Map.of("a", new AttributeValue().withN("1"))));
            legacyLeaf.put("optionalNumbers", new AttributeValue().withL(new AttributeValue().withN("11")));
            final AttributeValue legacyLeaves = new AttributeValue().withL(new AttributeValue().withM(legacyLeaf));
            assertMetadata(DynamoDBExecutor.toEntity(Map.of("leaves", legacyLeaves), MetadataHolder.class).getLeaves().get(0), withBinary);

            final var v2Leaf = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toItem(data);
            v2Leaf.put("raw", software.amazon.awssdk.services.dynamodb.model.AttributeValue
                    .fromM(Map.of("a", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromN("1"))));
            v2Leaf.put("optionalNumbers", software.amazon.awssdk.services.dynamodb.model.AttributeValue
                    .fromL(List.of(software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromN("11"))));
            final var v2Leaves = software.amazon.awssdk.services.dynamodb.model.AttributeValue
                    .fromL(List.of(software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(v2Leaf)));
            assertMetadata(com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(Map.of("leaves", v2Leaves), MetadataHolder.class).getLeaves().get(0),
                    withBinary);
        }
    }

    @Test
    public void nativeNestedBeansHonorClassLevelFormats() {
        final Map<String, Object> data = Map.of("date", "31/12/2026", "number", "1,234.50", "status", 20);
        final AttributeValue legacyLeaves = new AttributeValue().withL(new AttributeValue().withM(DynamoDBExecutor.toItem(data)));
        assertClassMetadata(DynamoDBExecutor.toEntity(Map.of("configured", legacyLeaves), MetadataHolder.class).getConfigured().get(0));

        final var v2Leaves = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List
                .of(software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toItem(data))));
        assertClassMetadata(
                com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(Map.of("configured", v2Leaves), MetadataHolder.class).getConfigured().get(0));
    }

    @Test
    public void topLevelDatabaseMappingRetainsExistingFieldExposure() {
        final Map<String, Object> data = Map.of("readOnly", "from-database");
        assertEquals("from-database", DynamoDBExecutor.toEntity(DynamoDBExecutor.toItem(data), MetadataLeaf.class).getReadOnly());
        assertEquals("from-database",
                com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                        .toEntity(com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toItem(data), MetadataLeaf.class)
                        .getReadOnly());
    }

    @Test
    public void legacyNativeNestedNumberWrappersRetainJsonScalarInference() {
        for (final boolean numericAttribute : new boolean[] { false, true }) {
            for (final String value : List.of("20", "1.25")) {
                final AttributeValue attribute = numericAttribute ? new AttributeValue().withN(value) : new AttributeValue().withS(value);
                final AttributeValue leaf = new AttributeValue()
                        .withM(Map.of("optional", attribute, "abacus", attribute, "nullable", attribute, "held", attribute, "nested", attribute));
                final NumberHolder result = DynamoDBExecutor.toEntity(Map.of("leaves", new AttributeValue().withL(leaf)), NumberHolder.class);
                assertNumberWrappers(result.getLeaves().get(0), value);
            }
        }

        for (final AttributeValue attribute : List.of(new AttributeValue().withS("null"), new AttributeValue().withS(""),
                new AttributeValue().withNULL(true))) {
            final AttributeValue leaf = new AttributeValue().withM(Map.of("optional", attribute));
            assertEquals(Optional.empty(),
                    DynamoDBExecutor.toEntity(Map.of("leaves", new AttributeValue().withL(leaf)), NumberHolder.class).getLeaves().get(0).getOptional());
        }
    }

    @Test
    public void v2NativeNestedNumberWrappersRetainJsonScalarInference() {
        for (final boolean numericAttribute : new boolean[] { false, true }) {
            for (final String value : List.of("20", "1.25")) {
                final var attribute = numericAttribute ? software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromN(value)
                        : software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS(value);
                final var leaf = software.amazon.awssdk.services.dynamodb.model.AttributeValue
                        .fromM(Map.of("optional", attribute, "abacus", attribute, "nullable", attribute, "held", attribute, "nested", attribute));
                final NumberHolder result = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                        .toEntity(Map.of("leaves", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List.of(leaf))), NumberHolder.class);
                assertNumberWrappers(result.getLeaves().get(0), value);
            }
        }

        for (final var attribute : List.of(software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("null"),
                software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS(""),
                software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromNul(true))) {
            final var leaf = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(Map.of("optional", attribute));
            assertEquals(Optional.empty(),
                    com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                            .toEntity(Map.of("leaves", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List.of(leaf))), NumberHolder.class)
                            .getLeaves()
                            .get(0)
                            .getOptional());
        }
    }

    @Test
    public void legacyAbstractNumberMapKeysRetainInferenceWithoutReencodingValues() {
        for (final String key : List.of("20", "1.25", "null", "")) {
            for (final boolean binaryValue : new boolean[] { false, true }) {
                final AttributeValue value = binaryValue ? new AttributeValue().withB(ByteBuffer.wrap(new byte[] { 1, -2 }))
                        : new AttributeValue().withS("value");
                final AttributeValue map = new AttributeValue().withM(Map.of(key, value));
                final NumberHolder result = DynamoDBExecutor.toEntity(Map.of("numberKeys", map, "optionalKeys", map), NumberHolder.class);
                assertNumberKeys(result, key);
                final Object expected = binaryValue ? ByteBuffer.wrap(new byte[] { 1, -2 }) : "value";
                assertEquals(expected, result.getNumberKeys().values().iterator().next());
                assertEquals(expected, result.getOptionalKeys().values().iterator().next());
            }
        }
    }

    @Test
    public void v2AbstractNumberMapKeysRetainInferenceWithoutReencodingValues() {
        for (final String key : List.of("20", "1.25", "null", "")) {
            for (final boolean binaryValue : new boolean[] { false, true }) {
                final var value = binaryValue
                        ? software.amazon.awssdk.services.dynamodb.model.AttributeValue
                                .fromB(software.amazon.awssdk.core.SdkBytes.fromByteArray(new byte[] { 1, -2 }))
                        : software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("value");
                final var map = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(Map.of(key, value));
                final NumberHolder result = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(Map.of("numberKeys", map, "optionalKeys", map),
                        NumberHolder.class);
                assertNumberKeys(result, key);
                for (final Object actual : List.of(result.getNumberKeys().values().iterator().next(), result.getOptionalKeys().values().iterator().next())) {
                    if (binaryValue) {
                        assertArrayEquals(new byte[] { 1, -2 }, assertInstanceOf(byte[].class, actual));
                    } else {
                        assertEquals("value", actual);
                    }
                }
            }
        }
    }

    private static void assertNumberWrappers(final NumberLeaf leaf, final String value) {
        final Number expected = value.equals("20") ? (Number) Integer.valueOf(20) : Double.valueOf(1.25);
        assertEquals(Optional.of(expected), leaf.getOptional());
        assertEquals(com.landawn.abacus.util.u.Optional.of(expected), leaf.getAbacus());
        assertEquals(com.landawn.abacus.util.u.Nullable.of(expected), leaf.getNullable());
        assertEquals(expected, leaf.getHeld().value());
        assertEquals(Optional.of(Optional.of(expected)), leaf.getNested());
    }

    private static void assertNumberKeys(final NumberHolder holder, final String value) {
        final Number expected = value.isEmpty() || value.equals("null") ? null : value.equals("20") ? (Number) Integer.valueOf(20) : Double.valueOf(1.25);
        assertEquals(expected, holder.getNumberKeys().keySet().iterator().next());
        assertEquals(Optional.ofNullable(expected), holder.getOptionalKeys().keySet().iterator().next());
    }

    @Test
    public void abstractNumberMapKeysStillRejectMalformedNumbers() {
        for (final String key : List.of(" ", "oops", "1.2.3", "NaN")) {
            final AttributeValue legacy = new AttributeValue().withM(Map.of(key, new AttributeValue().withS("value")));
            final var v2 = software.amazon.awssdk.services.dynamodb.model.AttributeValue
                    .fromM(Map.of(key, software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("value")));
            for (final String property : List.of("numberKeys", "optionalKeys")) {
                assertThrows(NumberFormatException.class, () -> DynamoDBExecutor.toEntity(Map.of(property, legacy), NumberHolder.class));
                assertThrows(NumberFormatException.class,
                        () -> com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(Map.of(property, v2), NumberHolder.class));
            }
        }
    }

    private static Map<String, Object> metadataValues() {
        final Map<String, Object> data = new LinkedHashMap<>();
        data.put("date", "31/12/2026");
        data.put("timestamp", "2026-12-31 00:00");
        data.put("amount", "1,234.50");
        data.put("formattedNumber", "1,234.50");
        data.put("number", "1.25");
        data.put("status", 20);
        data.put("readOnly", "overwritten");
        data.put("typed", "20");
        data.put("untyped", 20);
        data.put("optionalBoolean", true);
        data.put("rawScalar", "text \"quoted\"");
        data.put("alias", "renamed");
        data.put("ignored", "overwritten");
        data.put("nullable", null);
        data.put("primitive", null);
        return data;
    }

    private static void assertMetadata(final MetadataLeaf leaf, final boolean withBinary) {
        assertEquals(LocalDate.of(2026, 12, 31), leaf.getDate());
        assertEquals(Instant.parse("2026-12-30T22:00:00Z"), leaf.getTimestamp().toInstant());
        assertEquals(new BigDecimal("1234.50"), leaf.getAmount());
        assertEquals(1234.5d, leaf.getFormattedNumber());
        assertEquals(1.25d, leaf.getNumber());
        assertEquals(Status.SECOND, leaf.getStatus());
        assertEquals("default", leaf.getReadOnly());
        assertEquals(20L, leaf.getTyped());
        assertEquals("20", leaf.getUntyped());
        assertEquals(Optional.of(List.of(11L)), leaf.getOptionalNumbers());
        assertEquals(Optional.of(true), leaf.getOptionalBoolean());
        assertEquals(Map.of("a", "1"), N.fromJson(leaf.getRaw(), Map.class));
        assertEquals("text \"quoted\"", leaf.getRawScalar());
        assertEquals("renamed", leaf.getRenamed());
        assertEquals("overwritten", leaf.getIgnored());
        assertNull(leaf.getNullable());
        assertEquals(0, leaf.getPrimitive());
        assertEquals("default", leaf.getOmitted());
        assertEquals(withBinary ? ByteBuffer.wrap(new byte[] { 1, 2, -3 }) : null, leaf.getBuffer());
    }

    private static void assertClassMetadata(final ConfiguredLeaf leaf) {
        assertEquals(LocalDate.of(2026, 12, 31), leaf.getDate());
        assertEquals(1234.5d, leaf.getNumber());
        assertEquals(Status.SECOND, leaf.getStatus());
    }

    public static class MetadataHolder {
        private List<MetadataLeaf> leaves;
        private List<ConfiguredLeaf> configured;

        public List<MetadataLeaf> getLeaves() {
            return leaves;
        }

        public void setLeaves(final List<MetadataLeaf> value) {
            leaves = value;
        }

        public List<ConfiguredLeaf> getConfigured() {
            return configured;
        }

        public void setConfigured(final List<ConfiguredLeaf> value) {
            configured = value;
        }
    }

    public static class NumberHolder {
        private List<NumberLeaf> leaves;
        private Map<Number, Object> numberKeys;
        private Map<Optional<Number>, Object> optionalKeys;

        public List<NumberLeaf> getLeaves() {
            return leaves;
        }

        public void setLeaves(final List<NumberLeaf> value) {
            leaves = value;
        }

        public Map<Number, Object> getNumberKeys() {
            return numberKeys;
        }

        public void setNumberKeys(final Map<Number, Object> value) {
            numberKeys = value;
        }

        public Map<Optional<Number>, Object> getOptionalKeys() {
            return optionalKeys;
        }

        public void setOptionalKeys(final Map<Optional<Number>, Object> value) {
            optionalKeys = value;
        }
    }

    public static class NumberLeaf {
        private Optional<Number> optional;
        private com.landawn.abacus.util.u.Optional<Number> abacus;
        private com.landawn.abacus.util.u.Nullable<Number> nullable;
        private Holder<Number> held;
        private Optional<Optional<Number>> nested;

        public Optional<Number> getOptional() {
            return optional;
        }

        public void setOptional(final Optional<Number> value) {
            optional = value;
        }

        public com.landawn.abacus.util.u.Optional<Number> getAbacus() {
            return abacus;
        }

        public void setAbacus(final com.landawn.abacus.util.u.Optional<Number> value) {
            abacus = value;
        }

        public com.landawn.abacus.util.u.Nullable<Number> getNullable() {
            return nullable;
        }

        public void setNullable(final com.landawn.abacus.util.u.Nullable<Number> value) {
            nullable = value;
        }

        public Holder<Number> getHeld() {
            return held;
        }

        public void setHeld(final Holder<Number> value) {
            held = value;
        }

        public Optional<Optional<Number>> getNested() {
            return nested;
        }

        public void setNested(final Optional<Optional<Number>> value) {
            nested = value;
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

    public static class MetadataLeaf {
        @JsonXmlField(dateFormat = "dd/MM/uuuu")
        private LocalDate date;
        @JsonXmlField(dateFormat = "yyyy-MM-dd HH:mm", timeZone = "GMT+02:00")
        private Date timestamp;
        @JsonXmlField(numberFormat = "#,##0.00")
        private BigDecimal amount;
        @JsonXmlField(numberFormat = "#,##0.00")
        private Number formattedNumber;
        private Number number;
        @JsonXmlField(enumerated = EnumType.CODE)
        private Status status;
        @JsonXmlField(direction = JsonXmlField.Direction.SERIALIZE_ONLY)
        private String readOnly = "default";
        @JsonXmlField(type = "java.lang.Long")
        private Object typed;
        private Object untyped;
        private Optional<List<Long>> optionalNumbers;
        private Optional<Boolean> optionalBoolean;
        @JsonXmlField(isJsonRawValue = true)
        private String raw;
        @JsonXmlField(isJsonRawValue = true)
        private String rawScalar;
        @JsonXmlField(name = "canonical", aliases = { "alias" })
        private String renamed;
        @JsonXmlField(ignore = true)
        private String ignored = "default";
        private String nullable = "default";
        private int primitive = 7;
        private String omitted = "default";
        private ByteBuffer buffer;

        public LocalDate getDate() {
            return date;
        }

        public void setDate(final LocalDate value) {
            date = value;
        }

        public Date getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(final Date value) {
            timestamp = value;
        }

        public BigDecimal getAmount() {
            return amount;
        }

        public void setAmount(final BigDecimal value) {
            amount = value;
        }

        public Number getFormattedNumber() {
            return formattedNumber;
        }

        public void setFormattedNumber(final Number value) {
            formattedNumber = value;
        }

        public Number getNumber() {
            return number;
        }

        public void setNumber(final Number value) {
            number = value;
        }

        public Status getStatus() {
            return status;
        }

        public void setStatus(final Status value) {
            status = value;
        }

        public String getReadOnly() {
            return readOnly;
        }

        public void setReadOnly(final String value) {
            readOnly = value;
        }

        public Object getTyped() {
            return typed;
        }

        public void setTyped(final Object value) {
            typed = value;
        }

        public Object getUntyped() {
            return untyped;
        }

        public void setUntyped(final Object value) {
            untyped = value;
        }

        public Optional<List<Long>> getOptionalNumbers() {
            return optionalNumbers;
        }

        public void setOptionalNumbers(final Optional<List<Long>> value) {
            optionalNumbers = value;
        }

        public Optional<Boolean> getOptionalBoolean() {
            return optionalBoolean;
        }

        public void setOptionalBoolean(final Optional<Boolean> value) {
            optionalBoolean = value;
        }

        public String getRaw() {
            return raw;
        }

        public void setRaw(final String value) {
            raw = value;
        }

        public String getRawScalar() {
            return rawScalar;
        }

        public void setRawScalar(final String value) {
            rawScalar = value;
        }

        public String getRenamed() {
            return renamed;
        }

        public void setRenamed(final String value) {
            renamed = value;
        }

        public String getIgnored() {
            return ignored;
        }

        public void setIgnored(final String value) {
            ignored = value;
        }

        public String getNullable() {
            return nullable;
        }

        public void setNullable(final String value) {
            nullable = value;
        }

        public int getPrimitive() {
            return primitive;
        }

        public void setPrimitive(final int value) {
            primitive = value;
        }

        public String getOmitted() {
            return omitted;
        }

        public void setOmitted(final String value) {
            omitted = value;
        }

        public ByteBuffer getBuffer() {
            return buffer;
        }

        public void setBuffer(final ByteBuffer value) {
            buffer = value;
        }
    }

    @JsonXmlConfig(dateFormat = "dd/MM/uuuu", numberFormat = "#,##0.00", enumerated = EnumType.CODE)
    public static class ConfiguredLeaf {
        private LocalDate date;
        private Number number;
        private Status status;

        public LocalDate getDate() {
            return date;
        }

        public void setDate(final LocalDate value) {
            date = value;
        }

        public Number getNumber() {
            return number;
        }

        public void setNumber(final Number value) {
            number = value;
        }

        public Status getStatus() {
            return status;
        }

        public void setStatus(final Status value) {
            status = value;
        }
    }
}
