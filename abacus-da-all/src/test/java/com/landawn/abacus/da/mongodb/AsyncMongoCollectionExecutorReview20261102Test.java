package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.stream.Stream;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;

/**
 * Pins two statements of the {@link AsyncMongoCollectionExecutor} Javadoc against a real sync executor over a mocked
 * collection: a stream-returning method has read its {@code selectPropNames} once the stream is delivered, and the
 * 6-arg {@code list} overloads read an object-array row type from the sole non-{@code _id} field while {@code findFirst}
 * returns the field values positionally.
 */
public class AsyncMongoCollectionExecutorReview20261102Test extends MongoCollectionExecutorTestSupport {

    private static final Bson FILTER = Filters.eq("k", 1);

    private AsyncMongoCollectionExecutor async;

    @BeforeEach
    public void createAsync() {
        async = new AsyncMongoCollectionExecutor(executor, new AsyncExecutor(Runnable::run));
    }

    @Test
    public void testStreamDoesNotReadSelectPropNamesAfterDelivery() throws Exception {
        stubFindRows(Arrays.asList(new Document("_id", 1).append("x", 10).append("y", "a"), new Document("_id", 2).append("x", 20).append("y", "b")));

        final List<String> selectPropNames = new ArrayList<>(List.of("x"));

        try (Stream<Integer> stream = async.stream(selectPropNames, FILTER, null, Integer.class).get()) {
            // Changed after delivery but before consumption: the single-field extraction already captured "x".
            selectPropNames.set(0, "y");
            selectPropNames.add("x");

            assertEquals(Arrays.asList(10, 20), stream.toList());
        }
    }

    @Test
    public void testSixArgListReadsArrayRowTypeFromSoleFieldWhileFindFirstIsPositional() throws Exception {
        final Document row = new Document("_id", 7).append("a", 1);
        stubFindRows(Arrays.asList(row));
        when(mockFindIterable.first()).thenReturn(row);

        assertAll(() -> assertArrayEquals(new Object[] { 1 }, async.list(List.of("a"), FILTER, null, 0, 10, Object[].class).get().get(0)),
                () -> assertArrayEquals(new Object[] { 1 }, async.list(Projections.include("a"), FILTER, null, 0, 10, Object[].class).get().get(0)),
                () -> assertArrayEquals(new Object[] { 7, 1 }, async.findFirst(List.of("a"), FILTER, null, Object[].class).get().get()));
    }
}
