package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.landawn.abacus.annotation.JsonXmlConfig;
import com.landawn.abacus.annotation.JsonXmlField;
import com.landawn.abacus.util.Holder;
import com.landawn.abacus.util.u.Nullable;

public class LegacyDynamoDBExecutorDeepReview2026NumericWrapperTest {
    private static final String FORMATTED_NUMBER = "1,234.50";

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void fieldFormattedNumericWrappersUseNumericElementConversion(final boolean jsonBacked) {
        final Map<String, Object> values = Map.of("directLong", FORMATTED_NUMBER, "optionalLong", FORMATTED_NUMBER, "abacusLong", FORMATTED_NUMBER,
                "nullableLong", FORMATTED_NUMBER, "heldLong", FORMATTED_NUMBER, "number", FORMATTED_NUMBER, "nestedNumber", FORMATTED_NUMBER);

        final NestedRoot root = DynamoDBExecutor.toEntity(Map.of("fieldLeaves", nestedLeaf(values, jsonBacked)), NestedRoot.class);
        final FieldFormattedLeaf leaf = root.getFieldLeaves().get(0);

        assertEquals(1234L, leaf.getDirectLong());
        assertEquals(Optional.of(1234L), leaf.getOptionalLong());
        assertEquals(1234L, leaf.getAbacusLong().get());
        assertEquals(1234L, leaf.getNullableLong().get());
        assertEquals(1234L, leaf.getHeldLong().value());
        assertEquals(Optional.of(1234.5d), leaf.getNumber());
        assertEquals(Optional.of(1234.5d), leaf.getNestedNumber().value());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void classAndExplicitFieldFormatsApplyToTheirDeclaredTargets(final boolean jsonBacked) {
        final Map<String, Object> values = Map.of("directNumber", FORMATTED_NUMBER, "optionalNumber", FORMATTED_NUMBER, "heldLong", FORMATTED_NUMBER);

        final NestedRoot root = DynamoDBExecutor.toEntity(Map.of("classLeaves", nestedLeaf(values, jsonBacked)), NestedRoot.class);
        final ClassFormattedLeaf leaf = root.getClassLeaves().get(0);

        assertEquals(1234.5d, leaf.getDirectNumber());
        assertEquals(Optional.of(1234.5d), leaf.getOptionalNumber());
        assertEquals(1234L, leaf.getHeldLong().value());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void formattedIntegralWrappersRetainPrecisionBeyondDouble(final boolean jsonBacked) {
        final String exactNumber = "9,007,199,254,740,993.00";
        final long expected = 9007199254740993L;
        final Map<String, Object> values = Map.of("directLong", exactNumber, "optionalLong", exactNumber, "abacusLong", exactNumber, "nullableLong",
                exactNumber, "heldLong", exactNumber);

        final NestedRoot root = DynamoDBExecutor.toEntity(Map.of("fieldLeaves", nestedLeaf(values, jsonBacked)), NestedRoot.class);
        final FieldFormattedLeaf leaf = root.getFieldLeaves().get(0);

        assertEquals(expected, leaf.getDirectLong());
        assertEquals(Optional.of(expected), leaf.getOptionalLong());
        assertEquals(expected, leaf.getAbacusLong().get());
        assertEquals(expected, leaf.getNullableLong().get());
        assertEquals(expected, leaf.getHeldLong().value());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void formattedNumericWrapperRejectsTrailingContent(final boolean jsonBacked) {
        final Map<String, Object> values = Map.of("optionalLong", FORMATTED_NUMBER + " trailing");

        assertThrows(com.landawn.abacus.exception.ParsingException.class,
                () -> DynamoDBExecutor.toEntity(Map.of("fieldLeaves", nestedLeaf(values, jsonBacked)), NestedRoot.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void explicitWrapperFormatsSupportAllNumericTypesAndFieldOverrides(final boolean jsonBacked) {
        final ClassFormattedLeaf leaf = classFormattedLeaf(Map.of("integer", "2,147,483,647.99", "shortNumber", "32,767.99", "byteNumber", "-128.99",
                "floatNumber", FORMATTED_NUMBER, "doubleNumber", FORMATTED_NUMBER, "bigDecimal", "12,345,678,901,234,567,890.123456789", "bigInteger",
                "12,345,678,901,234,567,890.99", "currency", "USD 1234.50"), jsonBacked);

        assertEquals(Optional.of(Integer.MAX_VALUE), leaf.getInteger());
        assertEquals(Optional.of(Short.MAX_VALUE), leaf.getShortNumber());
        assertEquals(Optional.of(Byte.MIN_VALUE), leaf.getByteNumber());
        assertEquals(Optional.of(1234.5f), leaf.getFloatNumber());
        assertEquals(Optional.of(1234.5d), leaf.getDoubleNumber());
        assertEquals(Optional.of(new BigDecimal("12345678901234567890.123456789")), leaf.getBigDecimal());
        assertEquals(Optional.of(new BigInteger("12345678901234567890")), leaf.getBigInteger());
        assertEquals(Optional.of(new BigDecimal("1234.50")), leaf.getCurrency());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void sharedFormatPatternsPreserveExactValuesAndNegativeZero(final boolean jsonBacked) {
        final Map<String, Object> values = Map.of("bigDecimal", "9,007,199,254,740,993.01", "bigInteger", "12,345,678,901,234,567,890.00", "floatNumber",
                "-0.00", "doubleNumber", "-0.00", "optionalNumber", "-0.00");

        for (int i = 0; i < 2; i++) {
            final ClassFormattedLeaf leaf = classFormattedLeaf(values, jsonBacked);

            assertEquals(Optional.of(new BigDecimal("9007199254740993.01")), leaf.getBigDecimal());
            assertEquals(Optional.of(new BigInteger("12345678901234567890")), leaf.getBigInteger());
            assertEquals(Optional.of(-0.0f), leaf.getFloatNumber());
            assertEquals(Optional.of(-0.0d), leaf.getDoubleNumber());
            assertEquals(Optional.of(-0.0d), leaf.getOptionalNumber());
        }
    }

    @Test
    public void formattedNumberParsingIsIsolatedAcrossThreads() throws Exception {
        final CountDownLatch start = new CountDownLatch(1);
        final List<Future<?>> futures = new ArrayList<>();

        try (ExecutorService executor = Executors.newFixedThreadPool(4)) {
            for (int i = 0; i < 4; i++) {
                final Map<String, Object> values = Map.of("bigDecimal", "9,007,199,254,740,993.0" + i, "doubleNumber", "1,00" + i + ".50");
                final BigDecimal expectedDecimal = new BigDecimal("9007199254740993.0" + i);
                final double expectedDouble = 1000.5d + i;
                futures.add(executor.submit(() -> {
                    assertTrue(start.await(5, TimeUnit.SECONDS), "Mapping workers did not start");

                    for (int j = 0; j < 100; j++) {
                        final ClassFormattedLeaf leaf = classFormattedLeaf(values, (j & 1) == 0);
                        assertEquals(Optional.of(expectedDecimal), leaf.getBigDecimal());
                        assertEquals(Optional.of(expectedDouble), leaf.getDoubleNumber());
                    }

                    return null;
                }));
            }

            start.countDown();

            for (final Future<?> future : futures) {
                future.get(20, TimeUnit.SECONDS);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void classNumberFormatLeavesWrappersConsistentWithJsonRoundTrips(final boolean numbersAsText) {
        final ClassFormatDefaultsLeaf original = new ClassFormatDefaultsLeaf();
        original.directLong = 1234L;
        original.optionalLong = Optional.of(1234L);
        original.optionalNumber = Optional.of(1234);
        original.abacusLong = com.landawn.abacus.util.u.Optional.of(1234L);
        original.nullableLong = Nullable.of(1234L);
        original.nestedNumber = Holder.of(Optional.of(1234));
        original.emptyFormat = Optional.of(1234L);
        original.blankFormat = Optional.of(1234L);
        final NestedRoot originalRoot = new NestedRoot();
        originalRoot.setDefaultLeaves(List.of(original));

        // toItem writes the nested list as JSON text. Equivalent native L/M values must follow the same format rules.
        final ClassFormatDefaultsLeaf jsonLeaf = DynamoDBExecutor.toEntity(DynamoDBExecutor.toItem(originalRoot), NestedRoot.class).getDefaultLeaves().get(0);
        final Object number = numbersAsText ? "1234" : 1234L;
        final Map<String, Object> values = Map.of("directLong", "USD 1234.00", "optionalLong", number, "optionalNumber", number, "abacusLong", number,
                "nullableLong", number, "nestedNumber", number, "emptyFormat", number, "blankFormat", number);
        final ClassFormatDefaultsLeaf nativeLeaf = DynamoDBExecutor.toEntity(Map.of("defaultLeaves", nestedLeaf(values)), NestedRoot.class)
                .getDefaultLeaves()
                .get(0);

        for (final ClassFormatDefaultsLeaf leaf : List.of(jsonLeaf, nativeLeaf)) {
            assertEquals(1234L, leaf.directLong);
            assertEquals(Optional.of(1234L), leaf.optionalLong);
            assertEquals(1234L, leaf.optionalNumber.orElseThrow().longValue());
            assertEquals(1234L, leaf.abacusLong.get());
            assertEquals(1234L, leaf.nullableLong.get());
            assertEquals(1234L, leaf.nestedNumber.value().orElseThrow().longValue());
            assertEquals(Optional.of(1234L), leaf.emptyFormat);
            assertEquals(Optional.of(1234L), leaf.blankFormat);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void wrapperFormatsTrimPatternWhitespaceAndPreserveQuotedSpaces(final boolean jsonBacked) {
        final Map<String, Object> values = Map.of("directLong", FORMATTED_NUMBER, "optionalLong", FORMATTED_NUMBER, "nestedDecimal", "9,007,199,254,740,993.01",
                "doubleValue", "-0.00", "currency", "USD 1234.50", "spacedValue", " 12.50 ");
        final PaddedFormatLeaf leaf = DynamoDBExecutor.toEntity(Map.of("paddedLeaves", nestedLeaf(values, jsonBacked)), NestedRoot.class)
                .getPaddedLeaves()
                .get(0);

        assertEquals(1234L, leaf.directLong);
        assertEquals(Optional.of(1234L), leaf.optionalLong);
        assertEquals(Optional.of(new BigDecimal("9007199254740993.01")), leaf.nestedDecimal.value());
        assertEquals(Optional.of(-0.0d), leaf.doubleValue);
        assertEquals(Optional.of(new BigDecimal("1234.50")), leaf.currency);
        assertEquals(Optional.of(new BigDecimal("12.50")), leaf.spacedValue);
    }

    @ParameterizedTest
    @ValueSource(strings = { "currency", "spacedValue" })
    public void trimmingPatternsDoesNotRemoveRequiredQuotedAffixes(final String property) {
        final String value = property.equals("currency") ? "USD1234.50" : "12.50";
        assertThrows(com.landawn.abacus.exception.ParsingException.class,
                () -> DynamoDBExecutor.toEntity(Map.of("paddedLeaves", nestedLeaf(Map.of(property, value))), NestedRoot.class));
    }

    @ParameterizedTest
    @ValueSource(strings = { "optionalLong", "optionalNumber", "abacusLong", "nullableLong", "heldLong", "nestedNumber", "paddedDate", "dateOnly" })
    public void dateFormatsTakePrecedenceOverNumberFormatsInNativeAndJsonContainers(final String property) {
        for (final String value : List.of(FORMATTED_NUMBER, "not a number")) {
            final Map<String, Object> values = Map.of(property, value);
            // The same nested bean may arrive as native L/M attributes or as the JSON text written by toItem.
            final UnsupportedOperationException jsonFailure = assertThrows(UnsupportedOperationException.class,
                    () -> DynamoDBExecutor.toEntity(DynamoDBExecutor.toItem(Map.of("leaves", List.of(values))), FormatPrecedenceRoot.class));
            final UnsupportedOperationException nativeFailure = assertThrows(UnsupportedOperationException.class,
                    () -> DynamoDBExecutor.toEntity(Map.of("leaves", nestedLeaf(values)), FormatPrecedenceRoot.class));

            assertTrue(jsonFailure.getMessage().contains("DateFormat"));
            assertEquals(jsonFailure.getMessage(), nativeFailure.getMessage());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void blankDateFormatDoesNotSuppressWrapperNumberFormat(final boolean jsonBacked) {
        final FormatPrecedenceRoot root = DynamoDBExecutor.toEntity(Map.of("leaves", nestedLeaf(Map.of("blankDate", FORMATTED_NUMBER), jsonBacked)),
                FormatPrecedenceRoot.class);

        assertEquals(Optional.of(1234L), root.leaves.get(0).blankDate);
    }

    @Test
    public void jsonMapsArraysWrappersAndNestedBeansShareFormattedNumericReads() {
        final Map<String, Object> leaf = Map.of("optionalLong", FORMATTED_NUMBER, "nestedDecimal", "9,007,199,254,740,993.01");
        final JsonContainerRoot root = DynamoDBExecutor.toEntity(
                DynamoDBExecutor.toItem(Map.of("byName", Map.of("first", leaf), "array", List.of(leaf), "optional", leaf, "nested", leaf, "children",
                        List.of(Map.of("child", leaf, "array", List.of(leaf))), "generic", List.of(Map.of("value", FORMATTED_NUMBER)))),
                JsonContainerRoot.class);

        for (final PaddedFormatLeaf result : List.of(root.byName.get("first"), root.array[0], root.optional.orElseThrow(), root.nested.value().orElseThrow(),
                root.children.get(0).child, root.children.get(0).array[0])) {
            assertEquals(Optional.of(1234L), result.optionalLong);
            assertEquals(Optional.of(new BigDecimal("9007199254740993.01")), result.nestedDecimal.value());
        }
        assertEquals(Optional.of(1234L), root.generic.get(0).value);
    }

    @Test
    public void formattedJsonBeansPreserveOtherScalarTokensAndPropertyMetadata() {
        final String json = "[{\"amount\":\"1,234.50\",\"decimal\":9007199254740993.0100,\"optionalDecimal\":9007199254740993.0100,"
                + "\"date\":1,\"character\":65,\"negativeZero\":-0.0,\"formattedInteger\":1234.5,\"raw\":{\"n\":9007199254740993.0100},"
                + "\"alias\":\"1,234.50\",\"readOnly\":\"overwrite\",\"ignored.child\":1}]";
        final JsonMixedLeaf leaf = DynamoDBExecutor.toEntity(DynamoDBExecutor.toItem(Map.of("leaves", json)), JsonMixedRoot.class).leaves.get(0);

        assertEquals(Optional.of(1234L), leaf.amount);
        assertEquals(new BigDecimal("9007199254740993.0100"), leaf.decimal);
        assertEquals(Optional.of(new BigDecimal("9007199254740993.0100")), leaf.optionalDecimal);
        assertEquals(new java.util.Date(1), leaf.date);
        assertEquals('A', leaf.character);
        assertEquals(-0.0d, leaf.negativeZero);
        assertEquals(1234, leaf.formattedInteger);
        assertEquals(Optional.of(1234L), leaf.renamed);
        assertEquals("default", leaf.readOnly);
        assertTrue(leaf.raw.contains("9007199254740993.0100"), leaf.raw);
    }

    @Test
    public void formattedJsonNullsAndRecursiveBeansRetainTheirDeclaredContents() {
        final String json = "[{\"amount\":null,\"held\":null,\"child\":{\"amount\":\"1,234.50\"}}]";
        final JsonRecursiveRoot root = DynamoDBExecutor.toEntity(DynamoDBExecutor.toItem(Map.of("leaves", json)), JsonRecursiveRoot.class);
        final JsonRecursiveLeaf leaf = root.leaves.get(0);
        assertEquals(Optional.empty(), leaf.amount);
        final JsonRecursiveLeaf jsonDefaults = com.landawn.abacus.util.N.fromJson("{\"amount\":null,\"held\":null}", JsonRecursiveLeaf.class);
        assertEquals(jsonDefaults.held, leaf.held);
        assertEquals(Optional.of(1234L), leaf.child.amount);
    }

    @Test
    public void formattedJsonRejectsMalformedNumbersBooleansAndJson() {
        for (final String json : List.of("[{\"amount\":\"1,234.50 trailing\"}]", "[{\"amount\":true}]", "[{\"amount\":\"1,234.50\"} trailing]")) {
            assertThrows(com.landawn.abacus.exception.ParsingException.class,
                    () -> DynamoDBExecutor.toEntity(DynamoDBExecutor.toItem(Map.of("leaves", json)), JsonMixedRoot.class), json);
        }
    }

    @Test
    public void formattedJsonAliasesRespectDuplicatePropertyOrder() {
        final String json = "[{\"canonical\":\"1,234.50\",\"alias\":\"2,345.50\",\"canonical\":\"3,456.50\"}]";
        final JsonMixedLeaf leaf = DynamoDBExecutor.toEntity(DynamoDBExecutor.toItem(Map.of("leaves", json)), JsonMixedRoot.class).leaves.get(0);
        assertEquals(Optional.of(3456L), leaf.renamed);
    }

    @Test
    public void recursiveGenericJsonTypesResolveFormattedWrappers() {
        final String json = "[{\"value\":\"1,234.50\",\"child\":{\"value\":\"2,345.50\"}}]";
        final JsonGenericCycleRoot root = DynamoDBExecutor.toEntity(DynamoDBExecutor.toItem(Map.of("leaves", json)), JsonGenericCycleRoot.class);
        assertEquals(1234L, root.leaves.get(0).value);
        assertEquals(Optional.of(2345L), root.leaves.get(0).child.value);
    }

    @Test
    public void jsonNumericTokensUseWrapperFormatsAndStringContainersKeepFieldFormatValidation() {
        final JsonMixedLeaf leaf = DynamoDBExecutor.toEntity(DynamoDBExecutor.toItem(Map.of("leaves", "[{\"amount\":1234.5}]")), JsonMixedRoot.class).leaves
                .get(0);
        assertEquals(Optional.of(1234L), leaf.amount);

        final Map<String, Object> child = Map.of("leaf", "{\"optionalLong\":\"1,234.50\"}");
        assertThrows(UnsupportedOperationException.class,
                () -> DynamoDBExecutor.toEntity(DynamoDBExecutor.toItem(Map.of("leaves", List.of(child))), JsonContainerFormatRoot.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void recursiveGenericFormatsFollowChangingTypeArguments(final boolean jsonBacked) {
        final Map<String, Object> node = Map.of("child",
                Map.of("child", Map.of("amount", "9,007,199,254,740,993.50", "child", Map.of("child", Map.of("amount", "2,345.50")))));
        final JsonShiftingCycleRoot root = DynamoDBExecutor.toEntity(Map.of("leaves", recursiveLeaves(node, jsonBacked)), JsonShiftingCycleRoot.class);

        // The formatted property becomes Optional<Long> only on the second recurrence, then Optional<Optional<Long>>.
        assertEquals(Optional.of(9007199254740993L), root.leaves.get(0).child.child.amount);
        assertEquals(Optional.of(Optional.of(2345L)), root.leaves.get(0).child.child.child.child.amount);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void recursiveGenericFormatsMayAppearAfterSeveralRecurrences(final boolean jsonBacked) {
        final Map<String, Object> node = Map.of("child", Map.of("child", Map.of("child", Map.of("amount", FORMATTED_NUMBER))));
        final JsonRotatingCycleRoot root = DynamoDBExecutor.toEntity(Map.of("leaves", recursiveLeaves(node, jsonBacked)), JsonRotatingCycleRoot.class);

        assertEquals(Optional.of(1234L), root.leaves.get(0).child.child.child.amount);
    }

    @Test
    public void recursiveSchemasWithoutWrapperFormatsTerminateAndRetainTypedJsonValues() {
        final String growing = "[{\"value\":9007199254740993.0100,\"child\":{\"value\":[9007199254740993.0100],"
                + "\"child\":{\"value\":[[9007199254740993.0100]]}}}]";
        final JsonUnformattedCycleRoot root = DynamoDBExecutor.toEntity(
                DynamoDBExecutor.toItem(Map.of("growing", growing, "stable", "[{\"value\":7,\"child\":{\"value\":8}}]")), JsonUnformattedCycleRoot.class);
        final BigDecimal exact = new BigDecimal("9007199254740993.0100");

        assertEquals(exact, root.growing.get(0).value);
        assertEquals(List.of(exact), root.growing.get(0).child.value);
        assertEquals(List.of(List.of(exact)), root.growing.get(0).child.child.value);
        assertEquals(7L, root.stable.get(0).value);
        assertEquals(8L, root.stable.get(0).child.value);
    }

    @ParameterizedTest
    @ValueSource(strings = { "jacksonLong", "jacksonNested", "jacksonNumber", "jacksonDateOnly" })
    public void jacksonDateFormatsRetainPrecedenceOverNumericWrapperFormats(final String property) {
        for (final String value : List.of(FORMATTED_NUMBER, "not a number")) {
            final UnsupportedOperationException expected = assertThrows(UnsupportedOperationException.class,
                    () -> com.landawn.abacus.parser.ParserUtil.getBeanInfo(FormatPrecedenceLeaf.class).getPropInfo(property).readPropValue(value));

            for (final boolean jsonBacked : List.of(false, true)) {
                final UnsupportedOperationException actual = assertThrows(UnsupportedOperationException.class,
                        () -> DynamoDBExecutor.toEntity(Map.of("leaves", nestedLeaf(Map.of(property, value), jsonBacked)), FormatPrecedenceRoot.class));
                assertEquals(expected.getMessage(), actual.getMessage());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void blankDateOverridesStillAllowNumericWrapperFormats(final boolean jsonBacked) {
        final Map<String, Object> values = Map.of("blankJacksonDate", FORMATTED_NUMBER, "blankDateOverridesJackson", FORMATTED_NUMBER);
        final FormatPrecedenceRoot root = DynamoDBExecutor.toEntity(Map.of("leaves", nestedLeaf(values, jsonBacked)), FormatPrecedenceRoot.class);

        assertEquals(Optional.of(1234L), root.leaves.get(0).blankJacksonDate);
        assertEquals(Optional.of(1234L), root.leaves.get(0).blankDateOverridesJackson);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void controlOnlyDatePatternsDisableDateFormatsAfterAnnotationSelection(final boolean jsonBacked) {
        // String.trim removes U+0001, even though String.isBlank does not. A nonempty field override
        // must still suppress the lower-priority Jackson format before the selected pattern is trimmed.
        final Map<String, Object> values = Map.of("controlJacksonDate", FORMATTED_NUMBER, "controlDateOverridesJackson", FORMATTED_NUMBER);
        final FormatPrecedenceRoot root = DynamoDBExecutor.toEntity(Map.of("leaves", nestedLeaf(values, jsonBacked)), FormatPrecedenceRoot.class);

        assertEquals(Optional.of(1234L), root.leaves.get(0).controlJacksonDate);
        assertEquals(Optional.of(1234L), root.leaves.get(0).controlDateOverridesJackson);
    }

    @ParameterizedTest
    @ValueSource(strings = { "unicodeJacksonDate", "unicodeFieldDate" })
    public void unicodeWhitespaceDatePatternsRetainDateFormatValidation(final String property) {
        // U+2000 is whitespace to String.isBlank, but String.trim preserves it as an effective date pattern.
        final UnsupportedOperationException expected = assertThrows(UnsupportedOperationException.class,
                () -> com.landawn.abacus.parser.ParserUtil.getBeanInfo(FormatPrecedenceLeaf.class).getPropInfo(property).readPropValue(FORMATTED_NUMBER));

        for (final boolean jsonBacked : List.of(false, true)) {
            final UnsupportedOperationException actual = assertThrows(UnsupportedOperationException.class,
                    () -> DynamoDBExecutor.toEntity(Map.of("leaves", nestedLeaf(Map.of(property, FORMATTED_NUMBER), jsonBacked)), FormatPrecedenceRoot.class));
            assertEquals(expected.getMessage(), actual.getMessage());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "fastjsonLong", "fastjsonNested", "fastjsonOverridesJackson", "emptyFastjsonFallsThrough", "unicodeFastjsonDate",
            "fieldDateOverridesFastjson" })
    public void fastjsonDateFormatsRetainAnnotationPrecedence(final String property) {
        for (final String value : List.of(FORMATTED_NUMBER, "not a number")) {
            final UnsupportedOperationException expected = assertThrows(UnsupportedOperationException.class,
                    () -> com.landawn.abacus.parser.ParserUtil.getBeanInfo(FormatPrecedenceLeaf.class).getPropInfo(property).readPropValue(value));

            for (final boolean jsonBacked : List.of(false, true)) {
                final UnsupportedOperationException actual = assertThrows(UnsupportedOperationException.class,
                        () -> DynamoDBExecutor.toEntity(Map.of("leaves", nestedLeaf(Map.of(property, value), jsonBacked)), FormatPrecedenceRoot.class));
                assertEquals(expected.getMessage(), actual.getMessage());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "blankFastjsonDate", "controlFastjsonDate", "blankFieldOverridesFastjson", "controlFieldOverridesFastjson", "emptyFastjsonDate" })
    public void fastjsonEmptyAndTrimEmptyDatePatternsAllowWrapperNumberFormats(final String property) {
        // A raw empty pattern falls through; a nonempty trim-empty pattern overrides Jackson's effective date format.
        for (final boolean jsonBacked : List.of(false, true)) {
            final FormatPrecedenceRoot root = DynamoDBExecutor.toEntity(Map.of("leaves", nestedLeaf(Map.of(property, FORMATTED_NUMBER), jsonBacked)),
                    FormatPrecedenceRoot.class);
            assertEquals(Optional.of(1234L),
                    com.landawn.abacus.parser.ParserUtil.getBeanInfo(FormatPrecedenceLeaf.class).getPropValue(root.leaves.get(0), property));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void recursiveGenericArgumentsRetainFormatsFromReachableBeans(final boolean jsonBacked) {
        // The cycle itself has no number-format annotation. The formatted bean is first read after two rotations.
        final Map<String, Object> rotated = Map.of("child", Map.of("child", Map.of("value", Map.of("amount", FORMATTED_NUMBER))));
        // This bean type is introduced by the recursive declaration, rather than supplied in the root's type arguments.
        final Map<String, Object> introduced = Map.of("child", Map.of("value", Map.of("amount", FORMATTED_NUMBER)));
        final JsonArgumentCycleRoot root = DynamoDBExecutor.toEntity(
                Map.of("rotated", recursiveLeaves(rotated, jsonBacked), "introduced", recursiveLeaves(introduced, jsonBacked)), JsonArgumentCycleRoot.class);

        assertEquals(Optional.of(1234L), root.rotated.get(0).child.child.value.amount);
        assertEquals(Optional.of(1234L), root.introduced.get(0).child.value.amount);
    }

    public static class JsonArgumentCycleRoot {
        public List<JsonArgumentCycle<String, Long, JsonRecursiveLeaf>> rotated;
        public List<JsonIntroducedCycle<String>> introduced;
    }

    public static class JsonArgumentCycle<A, B, C> {
        public A value;
        public JsonArgumentCycle<B, C, List<A>> child;
    }

    public static class JsonIntroducedCycle<T> {
        public T value;
        public JsonIntroducedCycle<JsonRecursiveLeaf> child;
    }

    public static class JsonShiftingCycleRoot {
        public List<JsonShiftingCycle<Long, String>> leaves;
    }

    public static class JsonShiftingCycle<A, B> {
        @JsonXmlField(numberFormat = "#,##0.00")
        public A amount;
        public JsonShiftingCycle<B, Optional<A>> child;
    }

    public static class JsonRotatingCycleRoot {
        public List<JsonRotatingCycle<Long, String, BigDecimal>> leaves;
    }

    public static class JsonRotatingCycle<A, B, C> {
        @JsonXmlField(numberFormat = "#,##0.00")
        public A amount;
        public JsonRotatingCycle<B, C, Optional<A>> child;
    }

    public static class JsonUnformattedCycleRoot {
        public List<JsonGrowingCycle<BigDecimal>> growing;
        public List<JsonStableCycle<Long>> stable;
    }

    public static class JsonGrowingCycle<T> {
        public T value;
        public JsonGrowingCycle<List<T>> child;
    }

    public static class JsonStableCycle<T> {
        public T value;
        public JsonStableCycle<T> child;
    }

    private static AttributeValue recursiveLeaves(final Map<String, Object> node, final boolean jsonBacked) {
        return jsonBacked ? nestedLeaf(node, true) : new AttributeValue().withL(nativeRecursiveNode(node));
    }

    @SuppressWarnings("unchecked")
    private static AttributeValue nativeRecursiveNode(final Map<String, Object> node) {
        final Map<String, AttributeValue> attributes = new java.util.LinkedHashMap<>();
        node.forEach((name, value) -> attributes.put(name,
                value instanceof Map ? nativeRecursiveNode((Map<String, Object>) value) : DynamoDBExecutor.toItem(Map.of(name, value)).get(name)));
        return new AttributeValue().withM(attributes);
    }

    public static class JsonGenericCycleRoot {
        public List<JsonGenericCycle<Long>> leaves;
    }

    public static class JsonGenericCycle<T> {
        @JsonXmlField(numberFormat = "#,##0.00")
        public T value;
        public JsonGenericCycle<Optional<T>> child;
    }

    public static class JsonContainerFormatRoot {
        public List<JsonContainerFormatLeaf> leaves;
    }

    public static class JsonContainerFormatLeaf {
        @JsonXmlField(dateFormat = "long")
        public PaddedFormatLeaf leaf;
    }

    public static class JsonContainerRoot {
        public Map<String, PaddedFormatLeaf> byName;
        public PaddedFormatLeaf[] array;
        public Optional<PaddedFormatLeaf> optional;
        public Holder<Optional<PaddedFormatLeaf>> nested;
        public List<JsonContainerChild> children;
        public List<JsonGenericLeaf<Long>> generic;
    }

    public static class JsonContainerChild {
        public PaddedFormatLeaf child;
        public PaddedFormatLeaf[] array;
    }

    public static class JsonGenericLeaf<T extends Number> {
        @JsonXmlField(numberFormat = "#,##0.00")
        public Optional<T> value;
    }

    public static class JsonMixedRoot {
        public List<JsonMixedLeaf> leaves;
    }

    public static class JsonMixedLeaf {
        @JsonXmlField(numberFormat = "#,##0.00")
        public Optional<Long> amount;
        public BigDecimal decimal;
        public Optional<BigDecimal> optionalDecimal;
        public java.util.Date date;
        public char character;
        public double negativeZero;
        @JsonXmlField(numberFormat = "#,##0.00")
        public int formattedInteger;
        @JsonXmlField(isJsonRawValue = true)
        public String raw;
        @JsonXmlField(name = "canonical", aliases = { "alias" }, numberFormat = "#,##0.00")
        public Optional<Long> renamed;
        @JsonXmlField(direction = JsonXmlField.Direction.SERIALIZE_ONLY)
        public String readOnly = "default";
    }

    public static class JsonRecursiveRoot {
        public List<JsonRecursiveLeaf> leaves;
    }

    public static class JsonRecursiveLeaf {
        @JsonXmlField(numberFormat = "#,##0.00")
        public Optional<Long> amount;
        @JsonXmlField(numberFormat = "#,##0.00")
        public Holder<Optional<BigDecimal>> held;
        public JsonRecursiveLeaf child;
    }

    private static ClassFormattedLeaf classFormattedLeaf(final Map<String, Object> values, final boolean jsonBacked) {
        return DynamoDBExecutor.toEntity(Map.of("classLeaves", nestedLeaf(values, jsonBacked)), NestedRoot.class).getClassLeaves().get(0);
    }

    private static AttributeValue nestedLeaf(final Map<String, Object> values, final boolean jsonBacked) {
        // toItem stores lists as JSON text; exercise the same payload through both supported representations.
        return jsonBacked ? DynamoDBExecutor.toItem(Map.of("leaves", List.of(values))).get("leaves") : nestedLeaf(values);
    }

    private static AttributeValue nestedLeaf(final Map<String, Object> values) {
        return new AttributeValue().withL(new AttributeValue().withM(DynamoDBExecutor.toItem(values)));
    }

    public static class FormatPrecedenceRoot {
        public List<FormatPrecedenceLeaf> leaves;
    }

    public static class FormatPrecedenceLeaf {
        @JsonXmlField(numberFormat = "#,##0.00")
        @com.alibaba.fastjson2.annotation.JSONField(format = "long")
        public Optional<Long> fastjsonLong;
        @JsonXmlField(numberFormat = "#,##0.00")
        @com.alibaba.fastjson2.annotation.JSONField(format = " \tlong\n ")
        public Holder<Optional<Number>> fastjsonNested;
        @JsonXmlField(numberFormat = "#,##0.00")
        @com.alibaba.fastjson2.annotation.JSONField(format = "long")
        @com.fasterxml.jackson.annotation.JsonFormat(pattern = " \t\n ")
        public Optional<Long> fastjsonOverridesJackson;
        @JsonXmlField(numberFormat = "#,##0.00")
        @com.alibaba.fastjson2.annotation.JSONField(format = "")
        @com.fasterxml.jackson.annotation.JsonFormat(pattern = "long")
        public Optional<Long> emptyFastjsonFallsThrough;
        @JsonXmlField(numberFormat = "#,##0.00")
        @com.alibaba.fastjson2.annotation.JSONField(format = "\u2000")
        public Optional<Long> unicodeFastjsonDate;
        @JsonXmlField(numberFormat = "#,##0.00", dateFormat = "long")
        @com.alibaba.fastjson2.annotation.JSONField(format = " \t\n ")
        public Optional<Long> fieldDateOverridesFastjson;
        @JsonXmlField(numberFormat = "#,##0.00")
        @com.alibaba.fastjson2.annotation.JSONField(format = " \t\n ")
        @com.fasterxml.jackson.annotation.JsonFormat(pattern = "long")
        public Optional<Long> blankFastjsonDate;
        @JsonXmlField(numberFormat = "#,##0.00")
        @com.alibaba.fastjson2.annotation.JSONField(format = "\u0001")
        @com.fasterxml.jackson.annotation.JsonFormat(pattern = "long")
        public Optional<Long> controlFastjsonDate;
        @JsonXmlField(numberFormat = "#,##0.00", dateFormat = " \t\n ")
        @com.alibaba.fastjson2.annotation.JSONField(format = "long")
        public Optional<Long> blankFieldOverridesFastjson;
        @JsonXmlField(numberFormat = "#,##0.00", dateFormat = "\u0001")
        @com.alibaba.fastjson2.annotation.JSONField(format = "long")
        public Optional<Long> controlFieldOverridesFastjson;
        @JsonXmlField(numberFormat = "#,##0.00")
        @com.alibaba.fastjson2.annotation.JSONField(format = "")
        public Optional<Long> emptyFastjsonDate;
        @JsonXmlField(numberFormat = "#,##0.00")
        @com.fasterxml.jackson.annotation.JsonFormat(pattern = "long")
        public Optional<Long> jacksonLong;
        @JsonXmlField(numberFormat = "#,##0.00")
        @com.fasterxml.jackson.annotation.JsonFormat(pattern = " \tlong\n ")
        public Holder<Optional<Number>> jacksonNested;
        @JsonXmlField(numberFormat = "#,##0.00")
        @com.fasterxml.jackson.annotation.JsonFormat(pattern = "long")
        public Optional<Number> jacksonNumber;
        @com.fasterxml.jackson.annotation.JsonFormat(pattern = "long")
        public Optional<Number> jacksonDateOnly;
        @JsonXmlField(numberFormat = "#,##0.00")
        @com.fasterxml.jackson.annotation.JsonFormat(pattern = " \t\n ")
        public Optional<Long> blankJacksonDate;
        @JsonXmlField(numberFormat = "#,##0.00", dateFormat = " \t\n ")
        @com.fasterxml.jackson.annotation.JsonFormat(pattern = "long")
        public Optional<Long> blankDateOverridesJackson;
        @JsonXmlField(numberFormat = "#,##0.00")
        @com.fasterxml.jackson.annotation.JsonFormat(pattern = "\u0001")
        public Optional<Long> controlJacksonDate;
        @JsonXmlField(numberFormat = "#,##0.00", dateFormat = "\u0001")
        @com.fasterxml.jackson.annotation.JsonFormat(pattern = "long")
        public Optional<Long> controlDateOverridesJackson;
        @JsonXmlField(numberFormat = "#,##0.00")
        @com.fasterxml.jackson.annotation.JsonFormat(pattern = "\u2000")
        public Optional<Long> unicodeJacksonDate;
        @JsonXmlField(numberFormat = "#,##0.00", dateFormat = "\u2000")
        public Optional<Long> unicodeFieldDate;
        @JsonXmlField(numberFormat = "#,##0.00", dateFormat = "long")
        public Optional<Long> optionalLong;
        @JsonXmlField(numberFormat = "#,##0.00", dateFormat = "long")
        public Optional<Number> optionalNumber;
        @JsonXmlField(numberFormat = "#,##0.00", dateFormat = "long")
        public com.landawn.abacus.util.u.Optional<Long> abacusLong;
        @JsonXmlField(numberFormat = "#,##0.00", dateFormat = "long")
        public Nullable<Long> nullableLong;
        @JsonXmlField(numberFormat = "#,##0.00", dateFormat = "long")
        public Holder<Long> heldLong;
        @JsonXmlField(numberFormat = "#,##0.00", dateFormat = "long")
        public Holder<Optional<Number>> nestedNumber;
        @JsonXmlField(numberFormat = "#,##0.00", dateFormat = " \tlong\n ")
        public Optional<Long> paddedDate;
        @JsonXmlField(dateFormat = "long")
        public Optional<Long> dateOnly;
        @JsonXmlField(numberFormat = "#,##0.00", dateFormat = " \t\n ")
        public Optional<Long> blankDate;
    }

    public static class NestedRoot {
        private List<FieldFormattedLeaf> fieldLeaves;
        private List<ClassFormattedLeaf> classLeaves;
        private List<ClassFormatDefaultsLeaf> defaultLeaves;
        private List<PaddedFormatLeaf> paddedLeaves;

        public List<FieldFormattedLeaf> getFieldLeaves() {
            return fieldLeaves;
        }

        public void setFieldLeaves(final List<FieldFormattedLeaf> value) {
            fieldLeaves = value;
        }

        public List<ClassFormattedLeaf> getClassLeaves() {
            return classLeaves;
        }

        public void setClassLeaves(final List<ClassFormattedLeaf> value) {
            classLeaves = value;
        }

        public List<ClassFormatDefaultsLeaf> getDefaultLeaves() {
            return defaultLeaves;
        }

        public void setDefaultLeaves(final List<ClassFormatDefaultsLeaf> value) {
            defaultLeaves = value;
        }

        public List<PaddedFormatLeaf> getPaddedLeaves() {
            return paddedLeaves;
        }

        public void setPaddedLeaves(final List<PaddedFormatLeaf> value) {
            paddedLeaves = value;
        }
    }

    public static class FieldFormattedLeaf {
        @JsonXmlField(numberFormat = "#,##0.00")
        private Long directLong;
        @JsonXmlField(numberFormat = "#,##0.00")
        private Optional<Long> optionalLong;
        @JsonXmlField(numberFormat = "#,##0.00")
        private com.landawn.abacus.util.u.Optional<Long> abacusLong;
        @JsonXmlField(numberFormat = "#,##0.00")
        private Nullable<Long> nullableLong;
        @JsonXmlField(numberFormat = "#,##0.00")
        private Holder<Long> heldLong;
        @JsonXmlField(numberFormat = "#,##0.00")
        private Optional<Number> number;
        @JsonXmlField(numberFormat = "#,##0.00")
        private Holder<Optional<Number>> nestedNumber;

        public Long getDirectLong() {
            return directLong;
        }

        public void setDirectLong(final Long value) {
            directLong = value;
        }

        public Optional<Long> getOptionalLong() {
            return optionalLong;
        }

        public void setOptionalLong(final Optional<Long> value) {
            optionalLong = value;
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

        public Optional<Number> getNumber() {
            return number;
        }

        public void setNumber(final Optional<Number> value) {
            number = value;
        }

        public Holder<Optional<Number>> getNestedNumber() {
            return nestedNumber;
        }

        public void setNestedNumber(final Holder<Optional<Number>> value) {
            nestedNumber = value;
        }
    }

    @JsonXmlConfig(numberFormat = "'USD '0.00")
    public static class ClassFormatDefaultsLeaf {
        public Long directLong;
        public Optional<Long> optionalLong;
        public Optional<Number> optionalNumber;
        public com.landawn.abacus.util.u.Optional<Long> abacusLong;
        public Nullable<Long> nullableLong;
        public Holder<Optional<Number>> nestedNumber;
        @JsonXmlField(numberFormat = "")
        public Optional<Long> emptyFormat;
        @JsonXmlField(numberFormat = " \t ")
        public Optional<Long> blankFormat;
    }

    public static class PaddedFormatLeaf {
        @JsonXmlField(numberFormat = " \t#,##0.00 \n")
        public Long directLong;
        @JsonXmlField(numberFormat = " \t#,##0.00 \n")
        public Optional<Long> optionalLong;
        @JsonXmlField(numberFormat = " #,##0.00 ")
        public Holder<Optional<BigDecimal>> nestedDecimal;
        @JsonXmlField(numberFormat = " #,##0.00 ")
        public Optional<Double> doubleValue;
        @JsonXmlField(numberFormat = " 'USD '0.00 ")
        public Optional<BigDecimal> currency;
        @JsonXmlField(numberFormat = " ' '0.00' ' ")
        public Optional<BigDecimal> spacedValue;
    }

    @JsonXmlConfig(numberFormat = "#,##0.00")
    public static class ClassFormattedLeaf {
        private Number directNumber;
        // The class default applies to directNumber; generic wrappers need explicit field formats.
        @JsonXmlField(numberFormat = "#,##0.00")
        private Optional<Number> optionalNumber;
        @JsonXmlField(numberFormat = "#,##0.00")
        private Holder<Long> heldLong;
        @JsonXmlField(numberFormat = "#,##0.00")
        private Optional<Integer> integer;
        @JsonXmlField(numberFormat = "#,##0.00")
        private Optional<Short> shortNumber;
        @JsonXmlField(numberFormat = "#,##0.00")
        private Optional<Byte> byteNumber;
        @JsonXmlField(numberFormat = "#,##0.00")
        private Optional<Float> floatNumber;
        @JsonXmlField(numberFormat = "#,##0.00")
        private Optional<Double> doubleNumber;
        @JsonXmlField(numberFormat = "#,##0.00")
        private Optional<BigDecimal> bigDecimal;
        @JsonXmlField(numberFormat = "#,##0.00")
        private Optional<BigInteger> bigInteger;
        @JsonXmlField(numberFormat = "'USD '0.00")
        private Optional<BigDecimal> currency;

        public Number getDirectNumber() {
            return directNumber;
        }

        public void setDirectNumber(final Number value) {
            directNumber = value;
        }

        public Optional<Number> getOptionalNumber() {
            return optionalNumber;
        }

        public void setOptionalNumber(final Optional<Number> value) {
            optionalNumber = value;
        }

        public Holder<Long> getHeldLong() {
            return heldLong;
        }

        public void setHeldLong(final Holder<Long> value) {
            heldLong = value;
        }

        public Optional<Integer> getInteger() {
            return integer;
        }

        public void setInteger(final Optional<Integer> value) {
            integer = value;
        }

        public Optional<Short> getShortNumber() {
            return shortNumber;
        }

        public void setShortNumber(final Optional<Short> value) {
            shortNumber = value;
        }

        public Optional<Byte> getByteNumber() {
            return byteNumber;
        }

        public void setByteNumber(final Optional<Byte> value) {
            byteNumber = value;
        }

        public Optional<Float> getFloatNumber() {
            return floatNumber;
        }

        public void setFloatNumber(final Optional<Float> value) {
            floatNumber = value;
        }

        public Optional<Double> getDoubleNumber() {
            return doubleNumber;
        }

        public void setDoubleNumber(final Optional<Double> value) {
            doubleNumber = value;
        }

        public Optional<BigDecimal> getBigDecimal() {
            return bigDecimal;
        }

        public void setBigDecimal(final Optional<BigDecimal> value) {
            bigDecimal = value;
        }

        public Optional<BigInteger> getBigInteger() {
            return bigInteger;
        }

        public void setBigInteger(final Optional<BigInteger> value) {
            bigInteger = value;
        }

        public Optional<BigDecimal> getCurrency() {
            return currency;
        }

        public void setCurrency(final Optional<BigDecimal> value) {
            currency = value;
        }
    }
}
