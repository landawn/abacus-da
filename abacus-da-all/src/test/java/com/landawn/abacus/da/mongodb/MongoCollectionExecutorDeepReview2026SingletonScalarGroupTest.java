package com.landawn.abacus.da.mongodb;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCursor;

public class MongoCollectionExecutorDeepReview2026SingletonScalarGroupTest extends MongoCollectionExecutorTestSupport {

    @SuppressWarnings("unchecked")
    @Test
    public void testSingletonAndRepeatedDottedFieldsEmitScalarGroupKeys() {
        final List<List<? extends Bson>> pipelines = new ArrayList<>();

        when(mockCollection.aggregate(anyList(), eq(Document.class))).thenAnswer(invocation -> {
            final List<? extends Bson> pipeline = invocation.getArgument(0);
            pipelines.add(pipeline);

            final Document projection = ((Document) pipeline.get(1)).get("$project", Document.class);
            final Document row = projection.containsKey("n.m") ? new Document("n", new Document("m", 7)) : new Document("_id", 7);
            final AggregateIterable<Document> aggregateIterable = mock(AggregateIterable.class);
            final MongoCursor<Document> cursor = mock(MongoCursor.class);
            when(aggregateIterable.iterator()).thenReturn(cursor);
            when(cursor.hasNext()).thenReturn(true, false);
            when(cursor.next()).thenReturn(row);
            return aggregateIterable;
        });

        Assertions.assertEquals(List.of(7), executor.groupBy(List.of("n.m"), Integer.class).toList());
        Assertions.assertEquals(List.of(7), executor.groupBy(Arrays.asList("n.m", "n.m"), Integer.class).toList());

        Assertions.assertEquals(2, pipelines.size());
        for (final List<? extends Bson> pipeline : pipelines) {
            Assertions.assertEquals(new Document("$group", new Document("_id", new Document("n", new Document("m", "$n.m")))), pipeline.get(0));
            Assertions.assertEquals(new Document("$project", new Document("_id", "$_id.n.m")), pipeline.get(1));
        }
    }
}
