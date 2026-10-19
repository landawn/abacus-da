package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class ReactiveMongoCollectionExecutorDeepReview2026SingletonScalarGroupTest extends MongoCollectionExecutorTestSupport {

    @Test
    public void testSingletonAndRepeatedDottedFieldsEmitScalarGroupKeys() {
        final List<List<? extends Bson>> pipelines = new ArrayList<>();

        when(mockCollection.aggregate(anyList(), eq(Document.class))).thenAnswer(invocation -> {
            pipelines.add(invocation.getArgument(0));
            return mockAggregatePublisher;
        });
        doAnswer(invocation -> {
            final List<? extends Bson> pipeline = pipelines.get(pipelines.size() - 1);
            final Document projection = ((Document) pipeline.get(1)).get("$project", Document.class);
            final Document row = projection.containsKey("n.m") ? new Document("n", new Document("m", 7)) : new Document("_id", 7);
            Flux.just(row).subscribe(invocation.<Subscriber<? super Document>> getArgument(0));
            return null;
        }).when(mockAggregatePublisher).subscribe(any());

        StepVerifier.create(executor.groupBy(List.of("n.m"), Integer.class)).expectNext(7).verifyComplete();
        StepVerifier.create(executor.groupBy(Arrays.asList("n.m", "n.m"), Integer.class)).expectNext(7).verifyComplete();

        assertEquals(2, pipelines.size());
        for (final List<? extends Bson> pipeline : pipelines) {
            assertEquals(new Document("$group", new Document("_id", new Document("n", new Document("m", "$n.m")))), pipeline.get(0));
            assertEquals(new Document("$project", new Document("_id", "$_id.n.m")), pipeline.get(1));
        }
    }
}
