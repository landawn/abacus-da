package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import reactor.test.StepVerifier;

class ReactiveMongoScalarProjectionPathTest extends MongoCollectionExecutorTestSupport {

    @Test
    void preparedDottedPathKeepsValuesAndSkipsMissingOrNullFields() {
        when(mockCollection.find(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any())).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, new Document("address", new Document("city", "Paris")), new Document("address", new Document("city", null)),
                new Document("_id", 3), new Document("address", new Document("city", "Rome")));

        StepVerifier.create(executor.list(List.of("address.city"), new Document(), String.class)).expectNext("Paris", "Rome").verifyComplete();
    }
}
