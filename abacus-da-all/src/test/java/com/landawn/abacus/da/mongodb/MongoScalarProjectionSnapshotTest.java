package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.util.stream.Stream;

class MongoScalarProjectionSnapshotTest extends MongoCollectionExecutorTestSupport {

    @Test
    void scalarStreamKeepsTheOriginalFieldWhenCallerReusesProjectionList() {
        final List<String> fields = new ArrayList<>(List.of("address.city"));
        stubFindRows(List.of(new Document("address", new Document("city", "Paris")), new Document("address", new Document("city", "Rome"))));

        try (Stream<String> rows = executor.stream(fields, new Document(), String.class)) {
            // The query has already captured its projection; changing the caller's list must not change the row decoder.
            fields.set(0, "other.city");
            assertEquals(List.of("Paris", "Rome"), rows.toList());
        }
    }

    @Test
    void scalarStreamKeepsTheOriginalShapeWhenProjectionListIsCleared() {
        final List<String> fields = new ArrayList<>(List.of("address.city"));
        stubFindRows(List.of(new Document("address", new Document("city", "Paris"))));

        try (Stream<String> rows = executor.stream(fields, new Document(), String.class)) {
            fields.clear();
            assertEquals(List.of("Paris"), rows.toList());
        }
    }

    @Test
    void scalarListWithPreparedPathPreservesMissingAndNullValues() {
        stubFindRows(
                List.of(new Document("address", new Document("city", "Paris")), new Document("address", new Document("city", null)), new Document("_id", 3)));

        assertEquals(Arrays.asList("Paris", null, null), executor.list(List.of("address.city"), new Document(), String.class));
    }
}
