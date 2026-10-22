package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import com.landawn.abacus.util.ContinuableFuture;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.UpdateResult;

/**
 * Pins the documented rejection of a replacement whose top-level field name starts with {@code $}, against a live MongoDB
 * (skipped when none is reachable): every blocking replaceOne/findOneAndReplace overload (executor and mapper) throws the
 * driver's {@link IllegalArgumentException} at the call, the async executor's future fails with it, the stored document is
 * untouched, and a {@code $}-prefixed name nested below the top level is accepted.
 */
public class MongoCollectionExecutorDeepReview2026OperatorReplacementLiveTest {

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void topLevelOperatorNamesAreRejectedAtTheCallAndNestedOnesAccepted() throws Exception {
        try (MongoClient client = MongoLiveTestSupport.documentsClient()) {
            final MongoCollection<Document> coll = client.getDatabase("test").getCollection(collectionName());

            try {
                final ObjectId oid = new ObjectId();
                final Document stored = new Document("_id", oid).append("a", 1);
                coll.insertOne(stored);

                final MongoCollectionExecutor executor = new MongoCollectionExecutor(coll, MongoDBBase.DEFAULT_ASYNC_EXECUTOR);
                final AsyncMongoCollectionExecutor async = new AsyncMongoCollectionExecutor(executor, MongoDBBase.DEFAULT_ASYNC_EXECUTOR);
                final MongoCollectionMapper<Document> documentMapper = new MongoCollectionMapper<>(executor, Document.class);
                final MongoCollectionMapper<Map> mapMapper = new MongoCollectionMapper<>(executor, Map.class);
                final Bson filter = Filters.eq("_id", oid);

                final Document operatorDocument = new Document("$set", new Document("a", 2));
                final Map<String, Object> operatorMap = new LinkedHashMap<>();
                operatorMap.put("$set", 1);

                for (final Object replacement : List.of(operatorDocument, operatorMap)) {
                    assertRejected(() -> executor.replaceOne(oid.toHexString(), replacement));
                    assertRejected(() -> executor.replaceOne(oid, replacement));
                    assertRejected(() -> executor.replaceOne(filter, replacement));
                    assertRejected(() -> executor.replaceOne(filter, replacement, new ReplaceOptions()));
                    assertRejected(() -> executor.findOneAndReplace(filter, replacement));
                    assertRejected(() -> executor.findOneAndReplace(filter, replacement, Map.class));
                    assertRejected(() -> executor.findOneAndReplace(filter, replacement, new FindOneAndReplaceOptions()));
                    assertRejected(() -> executor.findOneAndReplace(filter, replacement, new FindOneAndReplaceOptions(), Document.class));

                    assertFutureRejected(async.replaceOne(oid.toHexString(), replacement));
                    assertFutureRejected(async.replaceOne(oid, replacement));
                    assertFutureRejected(async.replaceOne(filter, replacement));
                    assertFutureRejected(async.replaceOne(filter, replacement, new ReplaceOptions()));
                    assertFutureRejected(async.findOneAndReplace(filter, replacement));
                    assertFutureRejected(async.findOneAndReplace(filter, replacement, new FindOneAndReplaceOptions()));
                }

                assertRejected(() -> documentMapper.replaceOne(oid.toHexString(), operatorDocument));
                assertRejected(() -> documentMapper.replaceOne(oid, operatorDocument));
                assertRejected(() -> documentMapper.replaceOne(filter, operatorDocument));
                assertRejected(() -> documentMapper.replaceOne(filter, operatorDocument, new ReplaceOptions()));
                assertRejected(() -> documentMapper.findOneAndReplace(filter, operatorDocument));
                assertRejected(() -> documentMapper.findOneAndReplace(filter, operatorDocument, new FindOneAndReplaceOptions()));
                assertRejected(() -> mapMapper.replaceOne(filter, operatorMap));
                assertRejected(() -> mapMapper.findOneAndReplace(filter, operatorMap));

                // Rejected before the command is sent: the stored document is untouched.
                assertEquals(stored, coll.find(filter).first());

                // A $-prefixed name below the top level is not an update operator and is accepted.
                final Document nested = new Document("a", new Document("$set", 1));
                final UpdateResult accepted = executor.replaceOne(filter, nested);
                assertEquals(1, accepted.getMatchedCount());
                assertEquals(new Document("_id", oid).append("a", new Document("$set", 1)), coll.find(filter).first());

                final Map<String, Object> nestedMap = new LinkedHashMap<>();
                nestedMap.put("a", Map.of("$set", 2));
                assertEquals(1, executor.replaceOne(filter, nestedMap).getMatchedCount());
                assertEquals(new Document("_id", oid).append("a", new Document("$set", 2)), coll.find(filter).first());
            } finally {
                coll.drop();
            }
        }
    }

    private static void assertRejected(final Executable call) {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, call);
        assertTrue(e.getMessage().contains("$set"), e.getMessage());
    }

    private static void assertFutureRejected(final ContinuableFuture<?> future) {
        final ExecutionException e = assertThrows(ExecutionException.class, future::get);
        final IllegalArgumentException cause = assertInstanceOf(IllegalArgumentException.class, e.getCause());
        assertTrue(cause.getMessage().contains("$set"), cause.getMessage());
    }

    private static String collectionName() {
        return "abacus_da_operator_replacement_" + UUID.randomUUID().toString().replace("-", "");
    }
}
