package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import com.mongodb.client.result.UpdateResult;

import reactor.test.StepVerifier;

public class ReactiveMongoCollectionExecutorDeepReview2026BsonDocumentUpdateTest extends MongoCollectionExecutorTestSupport {

    @Test
    public void testPlainBsonDocumentIsWrappedWithoutMutatingCaller() {
        final Bson filter = new Document("id", 1);
        final BsonDocument callerUpdate = new BsonDocument("_id", new BsonInt32(1)).append("status", new BsonString("active"));
        final UpdateResult updateResult = org.mockito.Mockito.mock(UpdateResult.class);
        when(mockCollection.updateOne(eq(filter), any(Bson.class))).thenReturn(reactor.core.publisher.Mono.just(updateResult));

        StepVerifier.create(executor.updateOne(filter, callerUpdate)).expectNext(updateResult).verifyComplete();

        final org.mockito.ArgumentCaptor<Bson> updateCaptor = org.mockito.ArgumentCaptor.forClass(Bson.class);
        verify(mockCollection).updateOne(eq(filter), updateCaptor.capture());
        final Document sent = (Document) updateCaptor.getValue();
        assertEquals(new BsonDocument("status", new BsonString("active")), sent.get("$set"));
        assertEquals(new BsonDocument("_id", new BsonInt32(1)).append("status", new BsonString("active")), callerUpdate);
    }

    @Test
    public void testPlainRawBsonDocumentIsWrappedWithoutMutatingCaller() {
        final Bson filter = new Document("id", 1);
        final RawBsonDocument callerUpdate = RawBsonDocument.parse("{_id: 1, status: 'active'}");
        final UpdateResult updateResult = org.mockito.Mockito.mock(UpdateResult.class);
        when(mockCollection.updateOne(eq(filter), any(Bson.class))).thenReturn(reactor.core.publisher.Mono.just(updateResult));

        StepVerifier.create(executor.updateOne(filter, callerUpdate)).expectNext(updateResult).verifyComplete();

        final org.mockito.ArgumentCaptor<Bson> updateCaptor = org.mockito.ArgumentCaptor.forClass(Bson.class);
        verify(mockCollection).updateOne(eq(filter), updateCaptor.capture());
        final Document sent = (Document) updateCaptor.getValue();
        assertEquals(new BsonDocument("status", new BsonString("active")), sent.get("$set"));
        assertEquals(RawBsonDocument.parse("{_id: 1, status: 'active'}"), callerUpdate);
    }

    @Test
    public void testOperatorBsonDocumentPassesThrough() {
        final Bson filter = new Document("id", 1);
        final BsonDocument update = new BsonDocument("$set", new BsonDocument("status", new BsonString("active")));
        final UpdateResult updateResult = org.mockito.Mockito.mock(UpdateResult.class);
        when(mockCollection.updateOne(filter, update)).thenReturn(reactor.core.publisher.Mono.just(updateResult));

        StepVerifier.create(executor.updateOne(filter, update)).expectNext(updateResult).verifyComplete();

        verify(mockCollection).updateOne(filter, update);
    }

    @Test
    public void testInvalidBsonDocumentUpdatesAreRejectedBeforeDriverCall() {
        final Bson filter = new Document("id", 1);
        final BsonDocument mixed = new BsonDocument("$set", new BsonDocument("name", new BsonString("alice"))).append("status", new BsonString("active"));

        assertThrows(IllegalArgumentException.class, () -> executor.updateOne(filter, mixed));
        assertThrows(IllegalArgumentException.class, () -> executor.updateOne(filter, new BsonDocument()));
        assertThrows(IllegalArgumentException.class, () -> executor.updateOne(filter, new BsonDocument("_id", new BsonInt32(1))));
        assertThrows(IllegalArgumentException.class, () -> executor.updateOne(filter, RawBsonDocument.parse("{_id: 1}")));
        verify(mockCollection, org.mockito.Mockito.never()).updateOne(any(Bson.class), any(Bson.class));
    }
}
