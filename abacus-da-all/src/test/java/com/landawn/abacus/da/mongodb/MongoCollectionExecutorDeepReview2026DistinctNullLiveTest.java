package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.util.stream.Stream;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

/**
 * Pins the documented behaviour of the blocking {@code distinct} against a live MongoDB (skipped when none is reachable): a
 * BSON {@code null} stored in the field is included in the Stream (unlike the reactive executor, which drops it; see
 * {@code ReactiveMongoCollectionExecutorDeepReview2026DistinctNullLiveTest}). A document that lacks the field contributes
 * nothing.
 */
public class MongoCollectionExecutorDeepReview2026DistinctNullLiveTest {

    @Test
    public void syncDistinctKeepsBsonNull() {
        try (MongoClient client = MongoLiveTestSupport.documentsClient()) {
            final MongoCollection<Document> coll = client.getDatabase("test").getCollection(collectionName());

            try {
                coll.insertMany(List.of(new Document("v", "a"), new Document("v", null), new Document("v", "b"), new Document("w", 1)));

                final MongoCollectionExecutor executor = new MongoCollectionExecutor(coll, MongoDBBase.DEFAULT_ASYNC_EXECUTOR);

                try (Stream<String> values = executor.distinct("v", String.class)) {
                    final List<String> list = values.toList();
                    assertEquals(3, list.size(), String.valueOf(list));
                    assertTrue(list.contains(null), String.valueOf(list));
                    assertEquals(new HashSet<>(Arrays.asList("a", null, "b")), new HashSet<>(list));
                }

                try (Stream<String> values = executor.distinct("v", Filters.ne("v", "a"), String.class)) {
                    assertEquals(new HashSet<>(Arrays.asList(null, "b")), new HashSet<>(values.toList()));
                }

                try (Stream<String> values = executor.distinct("v", Filters.eq("v", null), String.class)) {
                    assertEquals(Arrays.asList((String) null), values.toList());
                }
            } finally {
                coll.drop();
            }
        }
    }

    private static String collectionName() {
        return "abacus_da_distinct_null_" + UUID.randomUUID().toString().replace("-", "");
    }
}
