package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class MongoCollectionScalarQueryTest extends MongoCollectionExecutorTestSupport {

    @Test
    public void testBinaryScalarQueriesPreserveReadablePayloads() {
        final byte[] bytes = { 1, 2, 3 };
        final ByteBuffer expected = ByteBuffer.wrap(bytes);
        final Bson filter = new Document();
        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);

        for (final Object value : new Object[] { bytes, new Binary(bytes) }) {
            stubEmits(mockFindPublisher, new Document("value", value));

            StepVerifier.create(executor.queryForSingleValue("value", filter, ByteBuffer.class)).expectNext(expected).verifyComplete();
            StepVerifier.create(executor.findFirst(Arrays.asList("value"), filter, ByteBuffer.class)).expectNext(expected).verifyComplete();
            StepVerifier.create(executor.list(Arrays.asList("value"), filter, ByteBuffer.class)).expectNext(expected).verifyComplete();
        }
    }

    @Test
    public void testQueryForBoolean() {
        String propName = "isActive";
        Bson filter = new Document("id", 1);
        Document doc = new Document(propName, true);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Boolean> result = executor.queryForBoolean(propName, filter);

        StepVerifier.create(result).expectNext(true).verifyComplete();
    }

    @Test
    public void testQueryForByte() {
        String propName = "level";
        Bson filter = new Document("id", 1);
        Document doc = new Document(propName, (byte) 5);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Byte> result = executor.queryForByte(propName, filter);

        StepVerifier.create(result).expectNext((byte) 5).verifyComplete();
    }

    @Test
    public void testQueryForChar() {
        String propName = "grade";
        Bson filter = new Document("id", 1);
        Document doc = new Document(propName, 'A');

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Character> result = executor.queryForChar(propName, filter);

        StepVerifier.create(result).expectNext('A').verifyComplete();
    }

    @Test
    public void testQueryForDate() {
        String propName = "createdAt";
        Bson filter = new Document("id", 1);
        Date date = new Date();
        Document doc = new Document(propName, date);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Date> result = executor.queryForDate(propName, filter);

        StepVerifier.create(result).expectNext(date).verifyComplete();
    }

    @Test
    public void testQueryForDateWithRowType() {
        String propName = "timestamp";
        Bson filter = new Document("id", 1);
        Date date = new Date();
        Document doc = new Document(propName, date);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Date> result = executor.queryForDate(propName, filter, Date.class);

        StepVerifier.create(result).expectNext(date).verifyComplete();
    }

    @Test
    public void testQueryForDouble() {
        String propName = "price";
        Bson filter = new Document("id", 1);
        Document doc = new Document(propName, 19.99);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Double> result = executor.queryForDouble(propName, filter);

        StepVerifier.create(result).expectNext(19.99).verifyComplete();
    }

    @Test
    public void testQueryForFloat() {
        String propName = "score";
        Bson filter = new Document("id", 1);
        Document doc = new Document(propName, 98.5f);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Float> result = executor.queryForFloat(propName, filter);

        StepVerifier.create(result).expectNext(98.5f).verifyComplete();
    }

    @Test
    public void testQueryForInt() {
        String propName = "age";
        Bson filter = new Document("id", 1);
        Document doc = new Document(propName, 25);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Integer> result = executor.queryForInt(propName, filter);

        StepVerifier.create(result).expectNext(25).verifyComplete();
    }

    @Test
    public void testQueryForLong() {
        String propName = "timestamp";
        Bson filter = new Document("id", 1);
        Document doc = new Document(propName, 1234567890L);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Long> result = executor.queryForLong(propName, filter);

        StepVerifier.create(result).expectNext(1234567890L).verifyComplete();
    }

    @Test
    public void testQueryForShort() {
        String propName = "count";
        Bson filter = new Document("id", 1);
        Document doc = new Document(propName, (short) 100);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Short> result = executor.queryForShort(propName, filter);

        StepVerifier.create(result).expectNext((short) 100).verifyComplete();
    }

    // field is missing — a wrapper valueType completes empty, a primitive valueType emits its default.
    @Test
    public void testQueryForSingleValueMissingFieldWrapperVsPrimitive_sliceF() {
        Bson filter = new Document("id", 1);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, new Document("_id", new ObjectId()));

        StepVerifier.create(executor.queryForSingleValue("age", filter, Integer.class)).verifyComplete();
        StepVerifier.create(executor.queryForSingleValue("age", filter, int.class)).expectNext(0).verifyComplete();
    }

    @Test
    public void testQueryForSingleValueWithFoundDocumentButNullPropertyCompletesEmpty() {
        // Regression: a matching document is found but the requested property is absent/null.
        // N.convert(null, String.class) returns null and Mono.just(null) throws NPE.
        // The reactive contract requires completing empty (no value) instead.
        String propName = "name";
        Bson filter = new Document("id", 1);
        Document doc = new Document("otherField", "value"); // no "name" property

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<String> result = executor.queryForSingleValue(propName, filter, String.class);

        StepVerifier.create(result).verifyComplete();
    }

    @Test
    public void testQueryForSingleValueWithNullValueTypeThrowsIAESynchronously() {
        // Regression: a null valueType is now validated eagerly, so IllegalArgumentException is thrown
        // at the call site (before any Mono is returned) instead of erroring on subscription.
        Bson filter = new Document("id", 1);

        assertThrows(IllegalArgumentException.class, () -> executor.queryForSingleValue("prop", filter, null));

        verify(mockCollection, org.mockito.Mockito.never()).find(any(Bson.class));
    }

    @Test
    public void testQueryForString() {
        String propName = "name";
        Bson filter = new Document("id", 1);
        Document doc = new Document(propName, "John Doe");

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<String> result = executor.queryForString(propName, filter);

        StepVerifier.create(result).expectNext("John Doe").verifyComplete();
    }

    @Test
    public void testScalarQueryNumericOverflowIsDeferredUntilSubscription() {
        final Bson filter = new Document();
        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, new Document("value", Long.MAX_VALUE));

        final Mono<Integer> result = executor.queryForInt("value", filter);
        StepVerifier.create(result).expectError(ArithmeticException.class).verify();
        StepVerifier.create(executor.queryForSingleValue("value", filter, Integer.class)).expectError(ArithmeticException.class).verify();
        StepVerifier.create(executor.findFirst(Arrays.asList("value"), filter, Integer.class)).expectError(ArithmeticException.class).verify();
        StepVerifier.create(executor.list(Arrays.asList("value"), filter, Integer.class)).expectError(ArithmeticException.class).verify();
    }

    @Test
    public void testqueryForSingleValue() {
        String propName = "value";
        Bson filter = new Document("id", 1);
        Document doc = new Document(propName, 42);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Integer> result = executor.queryForSingleValue(propName, filter, Integer.class);

        StepVerifier.create(result).expectNext(42).verifyComplete();
    }

    @Test
    public void testqueryForSingleValueWithEmptyDocument() {
        String propName = "value";
        Bson filter = new Document("id", 999);
        Document emptyDoc = new Document();

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, emptyDoc);

        Mono<String> result = executor.queryForSingleValue(propName, filter, String.class);

        StepVerifier.create(result).verifyComplete();
    }

    // ---- 2026-10-24 r2 sliceU ----

    // Pins the queryForSingleValue Javadoc: bracketed text read as String[]/Object[] is parsed as JSON array text (also verified
    // live on the reactive path), text that does not parse is the single element.
    @Test
    public void testQueryForSingleValueParsesBracketedTextIntoTextArrays_r2sliceU() {
        final Bson filter = new Document("id", 1);
        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);

        final Object[][] cases = { { "[a, b]", String[].class, new String[] { "a", "b" } }, { "[]", String[].class, new String[0] },
                { "[x", String[].class, new String[] { "[x" } }, { "[\"a\", \"b, c\"]", Object[].class, new Object[] { "a", "b, c" } },
                { "plain", Object[].class, new Object[] { "plain" } } };
        final java.util.List<org.junit.jupiter.api.function.Executable> checks = new java.util.ArrayList<>();

        for (final Object[] c : cases) {
            stubEmits(mockFindPublisher, new Document("id", 1).append("value", c[0]));
            final Object actual = executor.queryForSingleValue("value", filter, (Class<?>) c[1]).block();
            checks.add(() -> org.junit.jupiter.api.Assertions.assertArrayEquals((Object[]) c[2], (Object[]) actual, String.valueOf(c[0])));
            checks.add(() -> org.junit.jupiter.api.Assertions.assertEquals(c[1], actual.getClass(), String.valueOf(c[0])));
        }

        org.junit.jupiter.api.Assertions.assertAll(checks);
    }
}
