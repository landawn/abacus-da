package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import com.landawn.abacus.da.MockitoTestBase;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class ReactiveMongoMapperDeepReview2026PrimitiveDistinctTest extends MockitoTestBase {

    @Mock
    private MongoCollectionExecutor mockExecutor;

    @Test
    public void testNullGroupIsRejectedBeforePrimitiveDefaultConversion() {
        final List<List<? extends Bson>> pipelines = new ArrayList<>();
        when(mockExecutor.aggregate(anyList(), eq(int.class))).thenAnswer(invocation -> {
            pipelines.add(invocation.getArgument(0));
            return Flux.empty();
        });

        final MongoCollectionMapper<Integer> mapper = new MongoCollectionMapper<>(mockExecutor, int.class);
        final Bson filter = new Document("active", true);

        StepVerifier.create(mapper.distinct("age")).verifyComplete();
        StepVerifier.create(mapper.distinct("age", filter)).verifyComplete();

        final Document group = new Document("$group", new Document("_id", "$age"));
        final Document nonNullGroup = new Document("$match", new Document("_id", new Document("$ne", null)));
        assertEquals(List.of(group, nonNullGroup), pipelines.get(0));
        assertEquals(3, pipelines.get(1).size());
        assertEquals(group, pipelines.get(1).get(1));
        assertEquals(nonNullGroup, pipelines.get(1).get(2));
    }
}
