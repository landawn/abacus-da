package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;

import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class MongoNullValidationTest {

    @Test
    void selectedNullPropertyFailureIsDeferredUntilScalarRowIsEmitted() {
        final MongoCollection<Document> collection = mock(MongoCollection.class);
        final FindPublisher<Document> publisher = mock(FindPublisher.class, RETURNS_SELF);
        when(collection.find(any(Bson.class))).thenReturn(publisher);
        final Document row = new Document("value", "test");
        doAnswer(invocation -> {
            Flux.just(row).subscribe(invocation.<Subscriber<? super Document>>getArgument(0));
            return null;
        }).when(publisher).subscribe(any());
        final MongoCollectionExecutor executor = new MongoCollectionExecutor(collection);

        final Mono<String> result = executor.findFirst(Collections.singletonList(null), new Document(), String.class);
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, result::block);
        assertTrue(exception.getMessage().contains("propName"), exception.getMessage());
        assertSame(row, executor.findFirst(Collections.singletonList(null), new Document(), Document.class).block());

        doAnswer(invocation -> {
            Flux.<Document>empty().subscribe(invocation.<Subscriber<? super Document>>getArgument(0));
            return null;
        }).when(publisher).subscribe(any());
        assertNull(executor.findFirst(Collections.singletonList(null), new Document(), String.class).block());
    }
}
