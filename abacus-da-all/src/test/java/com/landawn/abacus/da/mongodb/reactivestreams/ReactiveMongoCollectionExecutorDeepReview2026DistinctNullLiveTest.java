package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.mongodb.MongoLiveTestSupport;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Filters;
import com.mongodb.reactivestreams.client.MongoClients;

/**
 * Pins the documented behaviour of the reactive {@code distinct} against a live MongoDB (skipped when none is reachable): a
 * BSON {@code null} stored in the field is reported by the server as a distinct value (the blocking executor's Stream includes
 * it, see {@code MongoCollectionExecutorDeepReview2026DistinctNullLiveTest}) but the reactive Flux does not emit it, because
 * Reactive Streams forbids {@code null} elements. A document that lacks the field contributes nothing.
 */
public class ReactiveMongoCollectionExecutorDeepReview2026DistinctNullLiveTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Test
    public void reactiveDistinctDropsTheBsonNullTheServerReports() {
        try (MongoClient syncClient = MongoLiveTestSupport.documentsClient();
             com.mongodb.reactivestreams.client.MongoClient reactiveClient = MongoClients.create(MongoLiveTestSupport.clientSettings())) {
            final String name = collectionName();
            final com.mongodb.client.MongoCollection<Document> syncColl = syncClient.getDatabase("test").getCollection(name);

            try {
                syncColl.insertMany(List.of(new Document("v", "a"), new Document("v", null), new Document("v", "b"), new Document("w", 1)));

                // The server reports the null as a distinct value (the blocking driver cursor yields it).
                final List<String> raw = syncColl.distinct("v", String.class).into(new ArrayList<>());
                assertTrue(raw.contains(null), String.valueOf(raw));
                assertEquals(3, raw.size());

                final MongoCollectionExecutor executor = new MongoCollectionExecutor(reactiveClient.getDatabase("test").getCollection(name));

                final List<String> values = executor.distinct("v", String.class).collectList().block(TIMEOUT);
                assertEquals(Set.of("a", "b"), new HashSet<>(values));
                assertEquals(2, values.size());
                assertFalse(values.contains(null));

                assertEquals(List.of("b"), executor.distinct("v", Filters.ne("v", "a"), String.class).collectList().block(TIMEOUT));

                // A field stored only as null yields an empty Flux.
                assertEquals(List.of(), executor.distinct("v", Filters.eq("v", null), String.class).collectList().block(TIMEOUT));
            } finally {
                syncColl.drop();
            }
        }
    }

    private static String collectionName() {
        return "abacus_da_reactive_distinct_null_" + UUID.randomUUID().toString().replace("-", "");
    }
}
