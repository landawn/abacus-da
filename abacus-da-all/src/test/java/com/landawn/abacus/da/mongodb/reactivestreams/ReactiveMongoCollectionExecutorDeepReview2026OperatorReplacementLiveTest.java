package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.mongodb.MongoLiveTestSupport;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.reactivestreams.client.MongoClients;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Reactive counterpart of the documented rejection of a replacement whose top-level field name starts with {@code $},
 * against a live MongoDB (skipped when none is reachable): every replaceOne/findOneAndReplace overload (executor and mapper)
 * returns its Mono normally and the driver's {@link IllegalArgumentException} is signalled through it (on block() or to a
 * StepVerifier), the stored document is untouched, and a {@code $}-prefixed name nested below the top level is accepted.
 */
public class ReactiveMongoCollectionExecutorDeepReview2026OperatorReplacementLiveTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void topLevelOperatorNamesAreSignalledThroughThePublisherAndNestedOnesAccepted() {
        try (MongoClient syncClient = MongoLiveTestSupport.documentsClient();
             com.mongodb.reactivestreams.client.MongoClient reactiveClient = MongoClients.create(MongoLiveTestSupport.clientSettings())) {
            final String name = collectionName();
            final com.mongodb.client.MongoCollection<Document> syncColl = syncClient.getDatabase("test").getCollection(name);

            try {
                final ObjectId oid = new ObjectId();
                final Document stored = new Document("_id", oid).append("a", 1);
                syncColl.insertOne(stored);

                final MongoCollectionExecutor executor = new MongoCollectionExecutor(reactiveClient.getDatabase("test").getCollection(name));
                final MongoCollectionMapper<Document> documentMapper = new MongoCollectionMapper<>(executor, Document.class);
                final MongoCollectionMapper<Map> mapMapper = new MongoCollectionMapper<>(executor, Map.class);
                final Bson filter = Filters.eq("_id", oid);

                final Document operatorDocument = new Document("$set", new Document("a", 2));
                final Map<String, Object> operatorMap = new LinkedHashMap<>();
                operatorMap.put("$set", 1);

                for (final Object replacement : List.of(operatorDocument, operatorMap)) {
                    // The call itself returns the Mono; the rejection is a signal, not an exception at call time.
                    assertSignalsRejection(executor.replaceOne(oid.toHexString(), replacement));
                    assertSignalsRejection(executor.replaceOne(oid, replacement));
                    assertSignalsRejection(executor.replaceOne(filter, replacement));
                    assertSignalsRejection(executor.replaceOne(filter, replacement, new ReplaceOptions()));
                    assertSignalsRejection(executor.findOneAndReplace(filter, replacement));
                    assertSignalsRejection(executor.findOneAndReplace(filter, replacement, Map.class));
                    assertSignalsRejection(executor.findOneAndReplace(filter, replacement, new FindOneAndReplaceOptions()));
                    assertSignalsRejection(executor.findOneAndReplace(filter, replacement, new FindOneAndReplaceOptions(), Document.class));
                }

                assertSignalsRejection(documentMapper.replaceOne(oid.toHexString(), operatorDocument));
                assertSignalsRejection(documentMapper.replaceOne(oid, operatorDocument));
                assertSignalsRejection(documentMapper.replaceOne(filter, operatorDocument));
                assertSignalsRejection(documentMapper.replaceOne(filter, operatorDocument, new ReplaceOptions()));
                assertSignalsRejection(documentMapper.findOneAndReplace(filter, operatorDocument));
                assertSignalsRejection(documentMapper.findOneAndReplace(filter, operatorDocument, new FindOneAndReplaceOptions()));
                assertSignalsRejection(mapMapper.replaceOne(filter, operatorMap));
                assertSignalsRejection(mapMapper.findOneAndReplace(filter, operatorMap));

                // Rejected before the command is sent: the stored document is untouched.
                assertEquals(stored, syncColl.find(filter).first());

                // A $-prefixed name below the top level is not an update operator and is accepted.
                assertEquals(1, executor.replaceOne(filter, new Document("a", new Document("$set", 1))).block(TIMEOUT).getMatchedCount());
                assertEquals(new Document("_id", oid).append("a", new Document("$set", 1)), syncColl.find(filter).first());

                final Map<String, Object> nestedMap = new LinkedHashMap<>();
                nestedMap.put("a", Map.of("$set", 2));
                assertEquals(1, executor.replaceOne(filter, nestedMap).block(TIMEOUT).getMatchedCount());
                assertEquals(new Document("_id", oid).append("a", new Document("$set", 2)), syncColl.find(filter).first());
            } finally {
                syncColl.drop();
            }
        }
    }

    private static void assertSignalsRejection(final Mono<?> mono) {
        StepVerifier.create(mono).expectErrorSatisfies(e -> {
            assertInstanceOf(IllegalArgumentException.class, e);
            assertTrue(e.getMessage().contains("$set"), e.getMessage());
        }).verify(TIMEOUT);

        // block() rethrows the signalled RuntimeException as-is.
        final IllegalArgumentException blocked = assertThrows(IllegalArgumentException.class, () -> mono.block(TIMEOUT));
        assertTrue(blocked.getMessage().contains("$set"), blocked.getMessage());
    }

    private static String collectionName() {
        return "abacus_da_reactive_operator_replacement_" + UUID.randomUUID().toString().replace("-", "");
    }
}
