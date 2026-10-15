package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.UpdateResult;

import reactor.core.publisher.Mono;

class MongoUpdateBoundaryTest {

    @ParameterizedTest(name = "reactive={0}")
    @ValueSource(booleans = { false, true })
    void legacyDocumentsRemoveImmutableIdsFromACopy(final boolean reactive) {
        final Target target = target(reactive);
        final BasicDBObject source = new BasicDBObject("_id", "original").append("value", 7);
        final BasicDBObject snapshot = new BasicDBObject(source);
        assertSame(target.result(), target.update().apply(source));
        assertEquals(new Document("$set", new BasicDBObject("value", 7)), target.sent().get());
        assertEquals(snapshot, source);
    }

    @ParameterizedTest(name = "reactive={0}")
    @ValueSource(booleans = { false, true })
    void completeLegacyOperatorUpdatesArePassedThrough(final boolean reactive) {
        final Target target = target(reactive);
        final BasicDBObject source = new BasicDBObject("$inc", new BasicDBObject("value", 1));
        assertSame(target.result(), target.update().apply(source));
        assertSame(source, target.sent().get());
    }

    @ParameterizedTest(name = "reactive={0}")
    @ValueSource(booleans = { false, true })
    void idOnlyAndNullNamedUpdatesFailBeforeWriting(final boolean reactive) {
        final Target target = target(reactive);
        final BasicDBObject idOnly = new BasicDBObject("_id", "original");
        assertThrows(IllegalArgumentException.class, () -> target.update().apply(idOnly));
        assertEquals(new BasicDBObject("_id", "original"), idOnly);
        assertThrows(IllegalArgumentException.class, () -> target.update().apply(new Document(null, 1)));
        assertThrows(IllegalArgumentException.class, () -> target.update().apply(new Document("value", 1).append(null, 2)));
        assertNull(target.sent().get());
    }

    @SuppressWarnings("unchecked")
    private static Target target(final boolean reactive) {
        final AtomicReference<Bson> sent = new AtomicReference<>();
        final UpdateResult result = UpdateResult.acknowledged(1, 1L, null);
        final Bson filter = new Document("_id", "original");
        if (reactive) {
            final com.mongodb.reactivestreams.client.MongoCollection<Document> collection = mock(com.mongodb.reactivestreams.client.MongoCollection.class);
            when(collection.updateOne(any(Bson.class), any(Bson.class))).thenAnswer(invocation -> {
                assertSame(filter, invocation.getArgument(0));
                sent.set(invocation.getArgument(1));
                return Mono.just(result);
            });
            final var executor = new com.landawn.abacus.da.mongodb.reactivestreams.MongoDB(mock(com.mongodb.reactivestreams.client.MongoDatabase.class))
                    .collectionExecutor(collection);
            return new Target(update -> executor.updateOne(filter, update).block(), sent, result);
        }
        final MongoCollection<Document> collection = mock(MongoCollection.class);
        when(collection.updateOne(any(Bson.class), any(Bson.class))).thenAnswer(invocation -> {
            assertSame(filter, invocation.getArgument(0));
            sent.set(invocation.getArgument(1));
            return result;
        });
        final MongoCollectionExecutor executor = new MongoCollectionExecutor(collection, new com.landawn.abacus.util.AsyncExecutor(Runnable::run));
        return new Target(update -> executor.updateOne(filter, update), sent, result);
    }

    private record Target(Function<Object, UpdateResult> update, AtomicReference<Bson> sent, UpdateResult result) {
    }
}
