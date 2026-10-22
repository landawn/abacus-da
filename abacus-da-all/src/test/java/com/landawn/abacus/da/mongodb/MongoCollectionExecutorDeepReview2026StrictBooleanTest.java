package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.u.OptionalBoolean;

/**
 * Pins the strict Boolean conversion documented on {@code queryForBoolean} (see the "Boolean Conversion" section of
 * {@link MongoDBBase}) for the blocking executor, the blocking mapper (which delegates to it) and the async executor: only a
 * BSON boolean, an int32 1/0/-1 and the texts 1/0/-1/y/n/true/false (any case, surrounding whitespace ignored) convert; blank
 * text is {@code false}; every other value fails with {@link IllegalArgumentException}.
 */
public class MongoCollectionExecutorDeepReview2026StrictBooleanTest extends MongoCollectionExecutorTestSupport {

    private static final Bson FILTER = new Document("id", 1);

    /** {stored value, expected Boolean}. */
    static List<Object[]> accepted() {
        return List.of(new Object[] { "", false }, new Object[] { "  ", false }, new Object[] { " Y ", true }, new Object[] { "TRUE", true },
                new Object[] { "n", false }, new Object[] { -1, false }, new Object[] { 1, true }, new Object[] { 0, false }, new Object[] { true, true },
                new Object[] { false, false });
    }

    static List<Object> rejected() {
        return List.of("yes", 2, 1L, 1.0d, Decimal128.parse("1"), "1.0", "2");
    }

    @Test
    public void syncQueryForBooleanConvertsOnlyTheDocumentedValues() {
        for (final Object[] c : accepted()) {
            when(mockFindIterable.first()).thenReturn(new Document("v", c[0]));

            final OptionalBoolean result = executor.queryForBoolean("v", FILTER);
            assertTrue(result.isPresent(), String.valueOf(c[0]));
            assertEquals(c[1], result.get(), "stored " + describe(c[0]));
        }

        for (final Object value : rejected()) {
            when(mockFindIterable.first()).thenReturn(new Document("v", value));

            assertThrows(IllegalArgumentException.class, () -> executor.queryForBoolean("v", FILTER), "stored " + describe(value));
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void mapperQueryForBooleanConvertsOnlyTheDocumentedValues() {
        final MongoCollectionMapper<Map> mapper = new MongoCollectionMapper<>(executor, Map.class);

        for (final Object[] c : accepted()) {
            when(mockFindIterable.first()).thenReturn(new Document("v", c[0]));

            final OptionalBoolean result = mapper.queryForBoolean("v", FILTER);
            assertTrue(result.isPresent(), String.valueOf(c[0]));
            assertEquals(c[1], result.get(), "stored " + describe(c[0]));
        }

        for (final Object value : rejected()) {
            when(mockFindIterable.first()).thenReturn(new Document("v", value));

            assertThrows(IllegalArgumentException.class, () -> mapper.queryForBoolean("v", FILTER), "stored " + describe(value));
        }
    }

    @Test
    public void asyncQueryForBooleanFutureFailsWithTheIllegalArgumentException() throws Exception {
        final AsyncMongoCollectionExecutor async = new AsyncMongoCollectionExecutor(executor, new AsyncExecutor(Runnable::run));

        for (final Object[] c : accepted()) {
            when(mockFindIterable.first()).thenReturn(new Document("v", c[0]));

            final OptionalBoolean result = async.queryForBoolean("v", FILTER).get();
            assertTrue(result.isPresent(), String.valueOf(c[0]));
            assertEquals(c[1], result.get(), "stored " + describe(c[0]));
        }

        for (final Object value : rejected()) {
            when(mockFindIterable.first()).thenReturn(new Document("v", value));

            // The call returns the future; the conversion failure fails it.
            final ContinuableFuture<OptionalBoolean> future = async.queryForBoolean("v", FILTER);
            final ExecutionException e = assertThrows(ExecutionException.class, future::get, "stored " + describe(value));
            assertInstanceOf(IllegalArgumentException.class, e.getCause(), "stored " + describe(value));
        }
    }

    static String describe(final Object value) {
        return value == null ? "null" : value.getClass().getSimpleName() + " '" + value + "'";
    }
}
