package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.mongodb.MongoLiveTestSupport;
import com.mongodb.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;

import reactor.core.publisher.Flux;

/**
 * Server-side counterpart of the reactive {@code $ifNull} group-leaf pipeline-shape tests: against a live MongoDB (skipped
 * when none is reachable) the multi-field grouping overloads must put documents that lack a group field and documents
 * holding {@code null} in it into ONE group whose key carries the field with {@code null}.
 */
public class ReactiveMongoCollectionExecutorDeepReview2026GroupNullKeyLiveTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final List<String> FIELDS = List.of("a", "b");

    @Test
    public void missingAndNullGroupFieldsFormOneGroupWithNullInTheKey() {
        // documentsClient() performs the reachability probe (and the JUnit skip); the reactive client reuses its settings.
        try (MongoClient syncClient = MongoLiveTestSupport.documentsClient();
             com.mongodb.reactivestreams.client.MongoClient reactiveClient = MongoClients.create(MongoLiveTestSupport.clientSettings())) {
            final String name = collectionName();
            final com.mongodb.client.MongoCollection<Document> syncColl = syncClient.getDatabase("test").getCollection(name);

            try {
                syncColl.insertMany(List.of(new Document("a", 1).append("b", 2), new Document("a", 1), new Document("a", 1).append("b", null),
                        new Document("a", 2).append("b", 2)));

                final MongoCollectionExecutor executor = new MongoCollectionExecutor(reactiveClient.getDatabase("test").getCollection(name));
                final MongoCollectionMapper<Map> mapMapper = new MongoCollectionMapper<>(executor, Map.class);
                final MongoCollectionMapper<Document> documentMapper = new MongoCollectionMapper<>(executor, Document.class);

                final Document key12 = new Document("a", 1).append("b", 2);
                final Document key1Null = new Document("a", 1).append("b", null);
                final Document key22 = new Document("a", 2).append("b", 2);

                final Set<Document> documentGroups = Set.of(new Document("_id", key12), new Document("_id", key1Null), new Document("_id", key22));
                final Set<Document> documentCounts = Set.of(new Document("_id", key12).append("count", 1), new Document("_id", key1Null).append("count", 2),
                        new Document("_id", key22).append("count", 1));
                final Set<Document> mapGroups = Set.of(key12, key1Null, key22);
                final Set<Document> mapCounts = Set.of(new Document(key12).append("count", 1), new Document(key1Null).append("count", 2),
                        new Document(key22).append("count", 1));

                assertEquals(documentGroups, toSet(executor.groupBy(FIELDS)));
                assertEquals(documentCounts, toSet(executor.groupByAndCount(FIELDS)));
                assertEquals(mapGroups, toSet(executor.groupBy(FIELDS, Map.class)));
                assertEquals(mapCounts, toSet(executor.groupByAndCount(FIELDS, Map.class)));

                assertEquals(mapGroups, toSet(mapMapper.groupBy(FIELDS)));
                assertEquals(mapCounts, toSet(mapMapper.groupByAndCount(FIELDS)));
                assertEquals(documentGroups, toSet(documentMapper.groupBy(FIELDS)));
                assertEquals(documentCounts, toSet(documentMapper.groupByAndCount(FIELDS)));

                // The null-key group is one row whose key holds b explicitly (not a row without b, and not two rows).
                final List<Document> nullKeyRows = executor.groupByAndCount(FIELDS, Map.class)
                        .map(row -> new Document((Map<String, Object>) row))
                        .filter(row -> !Integer.valueOf(2).equals(row.get("b")))
                        .collectList()
                        .block(TIMEOUT);
                assertEquals(1, nullKeyRows.size());
                assertTrue(nullKeyRows.get(0).containsKey("b"));
                assertNull(nullKeyRows.get(0).get("b"));
                assertEquals(2, nullKeyRows.get(0).get("count"));
            } finally {
                syncColl.drop();
            }
        }
    }

    @Test
    public void nestedGroupFieldsAppearAsNestedDocumentsWithNullLeaves() {
        try (MongoClient syncClient = MongoLiveTestSupport.documentsClient();
             com.mongodb.reactivestreams.client.MongoClient reactiveClient = MongoClients.create(MongoLiveTestSupport.clientSettings())) {
            final String name = collectionName();
            final com.mongodb.client.MongoCollection<Document> syncColl = syncClient.getDatabase("test").getCollection(name);

            try {
                syncColl.insertMany(List.of(new Document("a", 1).append("n", new Document("m", 2)), new Document("a", 1),
                        new Document("a", 1).append("n", new Document("m", null)), new Document("a", 1).append("n", null)));

                final MongoCollectionExecutor executor = new MongoCollectionExecutor(reactiveClient.getDatabase("test").getCollection(name));
                final List<String> fields = List.of("a", "n.m");

                final Document key2 = new Document("a", 1).append("n", new Document("m", 2));
                final Document keyNull = new Document("a", 1).append("n", new Document("m", null));

                assertEquals(Set.of(new Document("_id", key2), new Document("_id", keyNull)), toSet(executor.groupBy(fields)));
                assertEquals(Set.of(new Document("_id", key2).append("count", 1), new Document("_id", keyNull).append("count", 3)),
                        toSet(executor.groupByAndCount(fields)));
                assertEquals(Set.of(key2, keyNull), toSet(executor.groupBy(fields, Map.class)));
                assertEquals(Set.of(new Document(key2).append("count", 1), new Document(keyNull).append("count", 3)),
                        toSet(executor.groupByAndCount(fields, Map.class)));
            } finally {
                syncColl.drop();
            }
        }
    }

    private static String collectionName() {
        return "abacus_da_reactive_group_null_key_" + UUID.randomUUID().toString().replace("-", "");
    }

    private static Set<Document> toSet(final Flux<?> rows) {
        return toSet(rows.collectList().block(TIMEOUT));
    }

    @SuppressWarnings("unchecked")
    private static Set<Document> toSet(final Collection<?> rows) {
        final Set<Document> result = new HashSet<>();

        for (final Object row : rows) {
            // Normalize Map rows (Document or any other Map) so the comparison is by entries, null values included.
            result.add(new Document((Map<String, Object>) row));
        }

        return result;
    }
}
