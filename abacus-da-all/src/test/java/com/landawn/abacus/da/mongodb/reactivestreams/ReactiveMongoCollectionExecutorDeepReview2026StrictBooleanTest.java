package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import reactor.test.StepVerifier;

/**
 * Pins the strict Boolean conversion documented on the reactive {@code queryForBoolean} (executor and mapper): only a BSON
 * boolean, an int32 1/0/-1 and the texts 1/0/-1/y/n/true/false (any case, surrounding whitespace ignored) convert; blank text
 * emits {@code false}; every other value is signalled as an {@link IllegalArgumentException} through the publisher.
 */
public class ReactiveMongoCollectionExecutorDeepReview2026StrictBooleanTest extends MongoCollectionExecutorTestSupport {

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

    @BeforeEach
    public void stubSingleValueFind() {
        when(mockCollection.find(FILTER)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
    }

    @Test
    public void executorQueryForBooleanConvertsOnlyTheDocumentedValues() {
        for (final Object[] c : accepted()) {
            stubEmits(mockFindPublisher, new Document("v", c[0]));

            StepVerifier.create(executor.queryForBoolean("v", FILTER)).expectNext((Boolean) c[1]).as("stored " + describe(c[0])).verifyComplete();
        }

        for (final Object value : rejected()) {
            stubEmits(mockFindPublisher, new Document("v", value));

            StepVerifier.create(executor.queryForBoolean("v", FILTER)).as("stored " + describe(value)).expectError(IllegalArgumentException.class).verify();
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void mapperQueryForBooleanConvertsOnlyTheDocumentedValues() {
        final MongoCollectionMapper<Map> mapper = new MongoCollectionMapper<>(executor, Map.class);

        for (final Object[] c : accepted()) {
            stubEmits(mockFindPublisher, new Document("v", c[0]));

            StepVerifier.create(mapper.queryForBoolean("v", FILTER)).expectNext((Boolean) c[1]).as("stored " + describe(c[0])).verifyComplete();
        }

        for (final Object value : rejected()) {
            stubEmits(mockFindPublisher, new Document("v", value));

            StepVerifier.create(mapper.queryForBoolean("v", FILTER)).as("stored " + describe(value)).expectError(IllegalArgumentException.class).verify();
        }
    }

    static String describe(final Object value) {
        return value == null ? "null" : value.getClass().getSimpleName() + " '" + value + "'";
    }
}
