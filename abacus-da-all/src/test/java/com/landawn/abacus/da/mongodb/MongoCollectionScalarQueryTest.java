package com.landawn.abacus.da.mongodb;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.util.stream.Stream;
import com.landawn.abacus.util.u.Nullable;
import com.landawn.abacus.util.u.Optional;
import com.landawn.abacus.util.u.OptionalBoolean;
import com.landawn.abacus.util.u.OptionalByte;
import com.landawn.abacus.util.u.OptionalChar;
import com.landawn.abacus.util.u.OptionalDouble;
import com.landawn.abacus.util.u.OptionalFloat;
import com.landawn.abacus.util.u.OptionalInt;
import com.landawn.abacus.util.u.OptionalLong;
import com.landawn.abacus.util.u.OptionalShort;

public class MongoCollectionScalarQueryTest extends MongoCollectionExecutorTestSupport {

    @Test
    public void testBinaryScalarQueriesPreserveReadablePayloads() {
        final byte[] bytes = { 1, 2, 3 };
        final ByteBuffer expected = ByteBuffer.wrap(bytes);
        final Bson filter = new Document();

        for (final Object value : new Object[] { bytes, new Binary(bytes) }) {
            final Document row = new Document("value", value);
            when(mockFindIterable.first()).thenReturn(row);
            when(mockFindIterable.into(any())).thenReturn(Arrays.asList(row));

            Assertions.assertEquals(expected, executor.queryForSingleValue("value", filter, ByteBuffer.class).get());
            Assertions.assertEquals(expected, executor.queryForSingleNonNull("value", filter, ByteBuffer.class).get());
            Assertions.assertEquals(expected, executor.findFirst(Arrays.asList("value"), filter, ByteBuffer.class).get());
            Assertions.assertEquals(Arrays.asList(expected), executor.list(Arrays.asList("value"), filter, ByteBuffer.class));

            when(mockCursor.hasNext()).thenReturn(true, false);
            when(mockCursor.next()).thenReturn(row);
            try (Stream<ByteBuffer> stream = executor.stream(Arrays.asList("value"), filter, ByteBuffer.class)) {
                Assertions.assertEquals(Arrays.asList(expected), stream.toList());
            }
        }
    }

    @Test
    public void testQueryForBoolean() {
        String propName = "isActive";
        Document filter = new Document("id", 1);
        Document doc = new Document(propName, true);
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        OptionalBoolean result = executor.queryForBoolean(propName, filter);
        Assertions.assertTrue(result.orElseThrow());
    }

    @Test
    public void testQueryForByte() {
        String propName = "level";
        Document filter = new Document("id", 1);
        Document doc = new Document(propName, (byte) 5);
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        OptionalByte result = executor.queryForByte(propName, filter);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals((byte) 5, result.get());
    }

    @Test
    public void testQueryForChar() {
        String propName = "grade";
        Document filter = new Document("id", 1);
        Document doc = new Document(propName, 'A');
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        OptionalChar result = executor.queryForChar(propName, filter);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals('A', result.get());
    }

    @Test
    public void testQueryForDate() {
        String propName = "createdAt";
        Document filter = new Document("id", 1);
        Date date = new Date();
        Document doc = new Document(propName, date);
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        Nullable<Date> result = executor.queryForDate(propName, filter);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(date, result.get());
    }

    @Test
    public void testQueryForDateBasic() {
        Document filter = new Document("id", 1);
        Date d = new Date();
        Document doc = new Document("date", d);
        when(mockFindIterable.first()).thenReturn(doc);
        Nullable<Date> result = executor.queryForDate("date", filter);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(d, result.get());
    }

    @Test
    public void testQueryForDateWithSqlDateType() {
        Document filter = new Document("id", 1);
        Date d = new Date();
        Document doc = new Document("date", d);
        when(mockFindIterable.first()).thenReturn(doc);
        Nullable<java.sql.Date> result = executor.queryForDate("date", filter, java.sql.Date.class);
        Assertions.assertTrue(result.isPresent());
    }

    @Test
    public void testQueryForDouble() {
        String propName = "amount";
        Document filter = new Document("id", 1);
        Document doc = new Document(propName, 99.99);
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        OptionalDouble result = executor.queryForDouble(propName, filter);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(99.99, result.get(), 0.01);
    }

    @Test
    public void testQueryForFloat() {
        String propName = "price";
        Document filter = new Document("id", 1);
        Document doc = new Document(propName, 19.99f);
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        OptionalFloat result = executor.queryForFloat(propName, filter);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(19.99f, result.get(), 0.01f);
    }

    @Test
    public void testQueryForInt() {
        String propName = "age";
        Document filter = new Document("id", 1);
        Document doc = new Document(propName, 25);
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        OptionalInt result = executor.queryForInt(propName, filter);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(25, result.get());
    }

    @Test
    public void testQueryForLong() {
        String propName = "timestamp";
        Document filter = new Document("id", 1);
        Document doc = new Document(propName, 1234567890L);
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        OptionalLong result = executor.queryForLong(propName, filter);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(1234567890L, result.get());
    }

    @Test
    public void testQueryForShort() {
        String propName = "count";
        Document filter = new Document("id", 1);
        Document doc = new Document(propName, (short) 100);
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        OptionalShort result = executor.queryForShort(propName, filter);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals((short) 100, result.get());
    }

    @Test
    public void testQueryForSingleNonNull() {
        String propName = "value";
        Document filter = new Document("id", 1);
        Document doc = new Document(propName, "result");
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        Optional<String> result = executor.queryForSingleNonNull(propName, filter, String.class);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals("result", result.get());
    }

    @Test
    public void testQueryForSingleNonNullThrowsOnMissingPropertyValue() {
        // When the matched document has no value for the requested property, doc.get(propName) is
        // null and Optional.of(null) rejects the payload, so NullPointerException is thrown.
        Document filter = new Document("id", 1);
        Document doc = new Document("otherProp", "x"); // does not contain "value"
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        Assertions.assertThrows(NullPointerException.class, () -> executor.queryForSingleNonNull("value", filter, String.class));
    }

    @Test
    public void testQueryForSingleNonNullWhenNoDoc() {
        Document filter = new Document("id", 1);
        when(mockFindIterable.first()).thenReturn(null);
        Optional<String> result = executor.queryForSingleNonNull("v", filter, String.class);
        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testQueryForSingleNonNullWithMatchedDocAndNonNullValue() {
        Document filter = new Document("id", 1);
        Document doc = new Document("v", 42);
        when(mockFindIterable.first()).thenReturn(doc);
        Optional<Integer> r = executor.queryForSingleNonNull("v", filter, Integer.class);
        Assertions.assertTrue(r.isPresent());
        Assertions.assertEquals(42, r.get());
    }

    @Test
    public void testQueryForSingleNonNullWithNullValue() {
        String propName = "value";
        Document filter = new Document("id", 1);
        Document doc = new Document(propName, null);
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        // When a document is matched but the field value is null, Optional.of(null) rejects the
        // payload and NullPointerException is thrown.
        Assertions.assertThrows(NullPointerException.class, () -> executor.queryForSingleNonNull(propName, filter, String.class));
    }

    @Test
    public void testqueryForSingleValue() {
        String propName = "value";
        Document filter = new Document("id", 1);
        Document doc = new Document(propName, "result");
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        Nullable<String> result = executor.queryForSingleValue(propName, filter, String.class);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals("result", result.get());
    }

    @Test
    public void testQueryForSingleValueWhenNoDoc() {
        Document filter = new Document("id", 1);
        when(mockFindIterable.first()).thenReturn(null);
        Nullable<String> result = executor.queryForSingleValue("v", filter, String.class);
        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testQueryForSingleValueWithDottedPathReadsNestedValue() {
        // Regression: a dotted path like "address.city" is a valid projection; the server returns
        // {address: {city: ...}}. Previously the flat doc.get("address.city") lookup yielded a
        // present-but-null result; the nested value must be resolved.
        Document filter = new Document("id", 1);
        Document doc = new Document("address", new Document("city", "Berlin"));
        when(mockFindIterable.first()).thenReturn(doc);

        Nullable<String> result = executor.queryForSingleValue("address.city", filter, String.class);

        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals("Berlin", result.get());
    }

    @Test
    public void testQueryForSingleValueWithMatchedDocAndNonNullValue() {
        Document filter = new Document("id", 1);
        Document doc = new Document("v", 42);
        when(mockFindIterable.first()).thenReturn(doc);
        Nullable<Integer> r = executor.queryForSingleValue("v", filter, Integer.class);
        Assertions.assertTrue(r.isPresent());
        Assertions.assertEquals(42, r.get());
    }

    @Test
    public void testQueryForString() {
        String propName = "name";
        Document filter = new Document("id", 1);
        Document doc = new Document(propName, "John");
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        Nullable<String> result = executor.queryForString(propName, filter);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals("John", result.get());
    }

    @Test
    public void testScalarQueriesPreserveNumericOverflowFailures() {
        final Document row = new Document("value", Long.MAX_VALUE);
        final Bson filter = new Document();
        when(mockFindIterable.first()).thenReturn(row);
        when(mockFindIterable.into(any())).thenReturn(Arrays.asList(row));

        Assertions.assertThrows(ArithmeticException.class, () -> executor.queryForInt("value", filter));
        Assertions.assertThrows(ArithmeticException.class, () -> executor.queryForSingleValue("value", filter, Integer.class));
        Assertions.assertThrows(ArithmeticException.class, () -> executor.queryForSingleNonNull("value", filter, Integer.class));
        Assertions.assertThrows(ArithmeticException.class, () -> executor.findFirst(Arrays.asList("value"), filter, Integer.class));
        Assertions.assertThrows(ArithmeticException.class, () -> executor.list(Arrays.asList("value"), filter, Integer.class));
    }

    @Test
    public void testSingleValueQueriesRejectNullValueTypeBeforeQuery() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.queryForSingleValue("name", new Document("active", true), null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.queryForSingleNonNull("name", new Document("active", true), null));

        verify(mockCollection, never()).find(any(Bson.class));
    }
}
