package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;

/**
 * Pins the documented row-shape difference for object-array and collection row types: {@code list} reads such a type from
 * each document's sole non-{@code _id} field (as {@code MongoDBBase.toList} does), while {@code findFirst} and {@code stream}
 * return each document's field values positionally.
 */
public class MongoCollectionExecutorReview20261102Test extends MongoCollectionExecutorTestSupport {

    private static final Bson FILTER = Filters.eq("k", 1);

    private void stubRows(final Document row) {
        MongoIterableStubs.stubRows(mockFindIterable, Arrays.asList(row));
        when(mockFindIterable.first()).thenReturn(row);
    }

    @Test
    public void testListReadsArrayAndCollectionRowTypesFromTheSoleField() {
        stubRows(new Document("_id", 7).append("tags", Arrays.asList("x", "y")));

        assertAll(() -> assertArrayEquals(new Object[] { "x", "y" }, executor.list(FILTER, Object[].class).get(0)),
                () -> assertEquals(Arrays.asList("x", "y"), executor.list(FILTER, List.class).get(0)),
                () -> assertEquals(Arrays.asList("x", "y"), executor.list(List.of("tags"), FILTER, List.class).get(0)),
                () -> assertEquals(Arrays.asList("x", "y"), executor.list(Projections.include("tags"), FILTER, null, List.class).get(0)));
    }

    @Test
    public void testListRejectsMultiFieldDocumentsForArrayRowTypesWhileFindFirstAndStreamArePositional() {
        stubRows(new Document("_id", 7).append("a", 1).append("b", 2));

        assertAll(() -> assertThrows(IllegalArgumentException.class, () -> executor.list(FILTER, Object[].class)),
                () -> assertThrows(IllegalArgumentException.class, () -> executor.list(Projections.include("a", "b"), FILTER, null, List.class)),
                () -> assertArrayEquals(new Object[] { 7, 1, 2 }, executor.findFirst(FILTER, Object[].class).orElseThrow()),
                () -> assertEquals(Arrays.asList(7, 1, 2), executor.findFirst(Projections.include("a", "b"), FILTER, null, List.class).orElseThrow()),
                () -> assertArrayEquals(new Object[] { 7, 1, 2 }, executor.stream(FILTER, Object[].class).first().orElseThrow()),
                () -> assertEquals(Arrays.asList(7, 1, 2), executor.stream(FILTER, List.class).first().orElseThrow()));
    }
}
