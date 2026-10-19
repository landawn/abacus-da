package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.landawn.abacus.annotation.JsonXmlConfig;
import com.landawn.abacus.annotation.JsonXmlField;
import com.landawn.abacus.exception.ParsingException;

public class DynamoDBGenericFormatCacheTest {
    private static final long DATE_TOKEN = 20240102L;
    private static final Date FORMATTED_DATE = Date.from(Instant.parse("2024-01-02T00:00:00Z"));

    @BeforeEach
    void startWithColdFormatCaches() throws Exception {
        // Each ordering must start cold: an earlier parameterized invocation must not decide its cache history.
        // The format caches are declared by the shared abstract parent, so one clear covers both executors.
        for (final String name : List.of("jsonScalarFormatPool", "jsonDateFormatPool")) {
            final Field field = DynamoDBExecutorBase.class.getDeclaredField(name);
            field.setAccessible(true);
            ((Map<?, ?>) field.get(null)).clear();
        }
    }

    @ParameterizedTest
    @CsvSource({ "false, false, false", "false, false, true", "false, true, false", "false, true, true", "true, false, false", "true, false, true",
            "true, true, false", "true, true, true" })
    void classDateFormatsAreIndependentOfGenericReadOrder(final boolean sdkV2, final boolean wrapperFirst, final boolean nativeWarmup) {
        if (wrapperFirst) {
            assertDateWrapper(sdkV2, nativeWarmup);
            assertFormattedDate(sdkV2, false);
        } else {
            assertFormattedDate(sdkV2, false);
            assertDateWrapper(sdkV2, nativeWarmup);
        }

        // A numeric JSON token must use the date pattern, rather than being treated as epoch milliseconds.
        // Native warmup reaches the date cache without populating the scalar cache; JSON warmup reaches both.
        for (final boolean nativeContainers : List.of(false, true)) {
            assertFormattedDate(sdkV2, nativeContainers);
            assertDateWrapper(sdkV2, nativeContainers);
        }
    }

    @ParameterizedTest
    @CsvSource({ "false, false, false", "false, false, true", "false, true, false", "false, true, true", "true, false, false", "true, false, true",
            "true, true, false", "true, true, true" })
    void classNumberFormatsAreIndependentOfGenericReadOrder(final boolean sdkV2, final boolean wrapperFirst, final boolean nativeWarmup) {
        if (wrapperFirst) {
            assertNumberWrapper(sdkV2, nativeWarmup);
            assertMissingPercentRejected(sdkV2);
        } else {
            assertMissingPercentRejected(sdkV2);
            assertNumberWrapper(sdkV2, nativeWarmup);
        }

        for (final boolean nativeContainers : List.of(false, true)) {
            final NumberRoot root = read(sdkV2, NumberRoot.class, "numbers", "50%", nativeContainers);
            assertEquals(0.5d, root.numbers.get(0).value);
            assertNumberWrapper(sdkV2, nativeContainers);
        }
        assertMissingPercentRejected(sdkV2);
    }

    @ParameterizedTest
    @CsvSource({ "false, false", "false, true", "true, false", "true, true" })
    void inheritedPropertiesUseTheOwningBeansFormatConfiguration(final boolean sdkV2, final boolean formattedFirst) {
        // These properties share both their reflected field and resolved Date type, but their owners have different formats.
        if (formattedFirst) {
            assertInheritedFormattedDate(sdkV2);
            assertInheritedUnformattedDate(sdkV2);
        } else {
            assertInheritedUnformattedDate(sdkV2);
            assertInheritedFormattedDate(sdkV2);
        }
        assertInheritedFormattedDate(sdkV2);
        assertInheritedUnformattedDate(sdkV2);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void genericFormatsRemainIndependentDuringConcurrentCacheInitialization(final boolean sdkV2) throws Exception {
        final var workers = Executors.newFixedThreadPool(2);
        final CountDownLatch start = new CountDownLatch(1);
        try {
            final var dates = workers.submit(() -> {
                start.await();
                for (int i = 0; i < 50; i++) {
                    assertFormattedDate(sdkV2, false);
                }
                return null;
            });
            final var wrappers = workers.submit(() -> {
                start.await();
                for (int i = 0; i < 50; i++) {
                    assertDateWrapper(sdkV2, false);
                }
                return null;
            });
            start.countDown();
            dates.get(10, TimeUnit.SECONDS);
            wrappers.get(10, TimeUnit.SECONDS);
        } finally {
            workers.shutdownNow();
            assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS), "Cache initialization workers must stop before the next cold-cache test");
        }
    }

    private static void assertFormattedDate(final boolean sdkV2, final boolean nativeContainers) {
        final DateRoot root = read(sdkV2, DateRoot.class, "dates", DATE_TOKEN, nativeContainers);
        assertEquals(FORMATTED_DATE, root.dates.get(0).value);
    }

    private static void assertDateWrapper(final boolean sdkV2, final boolean nativeContainers) {
        final DateRoot root = read(sdkV2, DateRoot.class, "wrappers", 7L, nativeContainers);
        assertEquals(Optional.of(7L), root.wrappers.get(0).value);
    }

    private static void assertNumberWrapper(final boolean sdkV2, final boolean nativeContainers) {
        final NumberRoot root = read(sdkV2, NumberRoot.class, "wrappers", 50L, nativeContainers);
        assertEquals(Optional.of(50L), root.wrappers.get(0).value);
    }

    private static void assertMissingPercentRejected(final boolean sdkV2) {
        // Class number formats apply to Double but not Optional<Long>. A false scalar-cache hit would accept this token as 50.
        assertThrows(ParsingException.class, () -> read(sdkV2, NumberRoot.class, "numbers", 50L, false));
    }

    private static void assertInheritedFormattedDate(final boolean sdkV2) {
        final InheritedRoot root = read(sdkV2, InheritedRoot.class, "formatted", DATE_TOKEN, false);
        assertEquals(FORMATTED_DATE, root.formatted.get(0).value);
    }

    private static void assertInheritedUnformattedDate(final boolean sdkV2) {
        final InheritedRoot root = read(sdkV2, InheritedRoot.class, "unformatted", DATE_TOKEN, false);
        assertEquals(new Date(DATE_TOKEN), root.unformatted.get(0).value);
    }

    private static <T> T read(final boolean sdkV2, final Class<T> target, final String property, final Object value, final boolean nativeContainers) {
        final Map<String, Object> leaf = Map.of("value", value);
        if (sdkV2) {
            final var item = nativeContainers ? Map.of(property,
                    software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder()
                            .l(software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder()
                                    .m(com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toItem(leaf))
                                    .build())
                            .build())
                    : com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toItem(Map.of(property, List.of(leaf)));
            return com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor.toEntity(item, target);
        }

        final var item = nativeContainers
                ? Map.of(property,
                        new com.amazonaws.services.dynamodbv2.model.AttributeValue()
                                .withL(new com.amazonaws.services.dynamodbv2.model.AttributeValue().withM(DynamoDBExecutor.toItem(leaf))))
                : DynamoDBExecutor.toItem(Map.of(property, List.of(leaf)));
        return DynamoDBExecutor.toEntity(item, target);
    }

    public static class DateRoot {
        public List<DateBox<Date>> dates;
        public List<DateBox<Optional<Long>>> wrappers;
    }

    @JsonXmlConfig(dateFormat = "yyyyMMdd", timeZone = "UTC")
    public static class DateBox<T> {
        public T value;
        // A formatted wrapper makes JSON containers use the executor's custom reader even when this property is absent.
        @JsonXmlField(numberFormat = "#,##0.00")
        public Optional<Long> formatted;
    }

    public static class NumberRoot {
        public List<NumberBox<Double>> numbers;
        public List<NumberBox<Optional<Long>>> wrappers;
    }

    @JsonXmlConfig(numberFormat = "0%")
    public static class NumberBox<T> {
        public T value;
        @JsonXmlField(numberFormat = "#,##0.00")
        public Optional<Long> formatted;
    }

    public static class InheritedRoot {
        public List<FormattedDateBox> formatted;
        public List<UnformattedDateBox> unformatted;
    }

    public static class BaseDateBox {
        public Date value;
        @JsonXmlField(numberFormat = "#,##0.00")
        public Optional<Long> formatted;
    }

    @JsonXmlConfig(dateFormat = "yyyyMMdd", timeZone = "UTC")
    public static class FormattedDateBox extends BaseDateBox {
    }

    public static class UnformattedDateBox extends BaseDateBox {
    }
}
