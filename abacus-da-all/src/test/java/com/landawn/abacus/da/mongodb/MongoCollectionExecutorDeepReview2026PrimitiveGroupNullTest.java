package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCursor;

public class MongoCollectionExecutorDeepReview2026PrimitiveGroupNullTest extends MongoCollectionExecutorTestSupport {

    @SuppressWarnings("unchecked")
    @Test
    public void testNullGroupsSkipPrimitivesWithoutChangingWrappersOrRealDefaultValues() {
        final List<List<? extends Bson>> pipelines = new ArrayList<>();
        final List<Object> keys = Arrays.asList(null, null, null, null, null, null, null, 0, false, 0, false, 0, false);

        when(mockCollection.aggregate(anyList(), eq(Document.class))).thenAnswer(invocation -> {
            final List<? extends Bson> pipeline = invocation.getArgument(0);
            final Object key = keys.get(pipelines.size());
            pipelines.add(pipeline);

            final Document groupRow = groupRow(pipeline, key);
            final boolean emit = matches(groupRow, pipeline);
            final AggregateIterable<Document> aggregateIterable = mock(AggregateIterable.class);
            final MongoCursor<Document> cursor = mock(MongoCursor.class);
            when(aggregateIterable.iterator()).thenReturn(cursor);

            if (emit) {
                when(cursor.hasNext()).thenReturn(true, false);
                when(cursor.next()).thenReturn(resultRow(pipeline, key));
            } else {
                when(cursor.hasNext()).thenReturn(false);
            }

            return aggregateIterable;
        });

        assertEquals(List.of(), executor.groupBy("age", int.class).toList());
        assertEquals(List.of(), executor.groupBy("active", boolean.class).toList());
        assertEquals(List.of(), executor.groupBy(List.of("n"), int.class).toList());
        assertEquals(List.of(), executor.groupBy(Arrays.asList("n", "n"), boolean.class).toList());
        assertEquals(List.of(), executor.groupBy(List.of("n.m"), int.class).toList());
        assertEquals(List.of(), executor.groupBy(Arrays.asList("n.m", "n.m"), boolean.class).toList());
        assertEquals(Arrays.asList((Integer) null), executor.groupBy("age", Integer.class).toList());
        assertEquals(List.of(0), executor.groupBy("age", int.class).toList());
        assertEquals(List.of(false), executor.groupBy("active", boolean.class).toList());
        assertEquals(List.of(0), executor.groupBy(List.of("n"), int.class).toList());
        assertEquals(List.of(false), executor.groupBy(Arrays.asList("n", "n"), boolean.class).toList());
        assertEquals(List.of(0), executor.groupBy(List.of("n.m"), int.class).toList());
        assertEquals(List.of(false), executor.groupBy(Arrays.asList("n.m", "n.m"), boolean.class).toList());

        final Document ageGroup = new Document("$group", new Document("_id", "$age"));
        final Document activeGroup = new Document("$group", new Document("_id", "$active"));
        final Document collectionGroup = new Document("$group", new Document("_id", new Document("n", "$n")));
        final Document collectionProjection = new Document("$project", new Document("_id", 0).append("n", "$_id.n"));
        final Document dottedGroup = new Document("$group", new Document("_id", new Document("n", new Document("m", "$n.m"))));
        final Document dottedProjection = new Document("$project", new Document("_id", "$_id.n.m"));
        final Document idMatch = nonNullMatch("_id");
        final Document collectionMatch = nonNullMatch("_id.n");
        final Document dottedMatch = nonNullMatch("_id.n.m");

        assertEquals(13, pipelines.size());
        assertEquals(List.of(ageGroup, idMatch), pipelines.get(0));
        assertEquals(List.of(activeGroup, idMatch), pipelines.get(1));
        assertEquals(List.of(collectionGroup, collectionMatch, collectionProjection), pipelines.get(2));
        assertEquals(List.of(collectionGroup, collectionMatch, collectionProjection), pipelines.get(3));
        assertEquals(List.of(dottedGroup, dottedMatch, dottedProjection), pipelines.get(4));
        assertEquals(List.of(dottedGroup, dottedMatch, dottedProjection), pipelines.get(5));
        assertEquals(List.of(ageGroup), pipelines.get(6));
        assertEquals(List.of(ageGroup, idMatch), pipelines.get(7));
        assertEquals(List.of(activeGroup, idMatch), pipelines.get(8));
        assertEquals(List.of(collectionGroup, collectionMatch, collectionProjection), pipelines.get(9));
        assertEquals(List.of(collectionGroup, collectionMatch, collectionProjection), pipelines.get(10));
        assertEquals(List.of(dottedGroup, dottedMatch, dottedProjection), pipelines.get(11));
        assertEquals(List.of(dottedGroup, dottedMatch, dottedProjection), pipelines.get(12));
    }

    private static Document nonNullMatch(final String path) {
        return new Document("$match", new Document(path, new Document("$ne", null)));
    }

    private static Document groupRow(final List<? extends Bson> pipeline, final Object key) {
        final Object id = ((Document) pipeline.get(0)).get("$group", Document.class).get("_id");
        return new Document("_id", id instanceof Document ? keyDocument((Document) id, key) : key);
    }

    private static Document keyDocument(final Document shape, final Object key) {
        final Document result = new Document();

        for (final Map.Entry<String, Object> entry : shape.entrySet()) {
            result.append(entry.getKey(), entry.getValue() instanceof Document ? keyDocument((Document) entry.getValue(), key) : key);
        }

        return result;
    }

    private static boolean matches(final Document groupRow, final List<? extends Bson> pipeline) {
        for (final Bson stage : pipeline) {
            final Document match = ((Document) stage).get("$match", Document.class);

            if (match != null) {
                for (final Map.Entry<String, Object> entry : match.entrySet()) {
                    final Document condition = (Document) entry.getValue();

                    if (condition.get("$ne") == null && getPath(groupRow, entry.getKey()) == null) {
                        return false;
                    }
                }
            }
        }

        return true;
    }

    private static Object getPath(final Document document, final String path) {
        return path.indexOf('.') < 0 ? document.get(path) : document.getEmbedded(Arrays.asList(path.split("\\.")), Object.class);
    }

    private static Document resultRow(final List<? extends Bson> pipeline, final Object key) {
        for (final Bson stage : pipeline) {
            final Document projection = ((Document) stage).get("$project", Document.class);

            if (projection != null) {
                if (projection.size() == 1 && projection.get("_id") instanceof String) {
                    return new Document("_id", key);
                }

                for (final String fieldName : projection.keySet()) {
                    if (!"_id".equals(fieldName)) {
                        return new Document(fieldName, key);
                    }
                }
            }
        }

        return new Document("_id", key);
    }
}
