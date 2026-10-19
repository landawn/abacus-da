package com.landawn.abacus.da.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.da.MockitoTestBase;
import com.landawn.abacus.util.AsyncExecutor;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

public class MongoCollectionMapperDeepReview2026PrimitiveDistinctTest extends MockitoTestBase {

    @Test
    public void testNullGroupsAreSkippedOnlyForPrimitivesAndRealDefaultsSurvive() {
        final MongoCollection<Document> collection = mock(MongoCollection.class);
        final MongoCollectionExecutor executor = new MongoCollectionExecutor(collection, mock(AsyncExecutor.class));
        final List<List<? extends Bson>> pipelines = new ArrayList<>();
        // A null group key represents both an explicit BSON null and a field missing from the grouped documents.
        final List<List<Document>> rowsByCall = List.of(Arrays.asList(new Document("_id", null), new Document("_id", 0)),
                Arrays.asList(new Document("_id", null), new Document("_id", 0)), Arrays.asList(new Document("_id", null), new Document("_id", false)),
                Arrays.asList(new Document("_id", null), new Document("_id", 0)), Arrays.asList(new Document("_id", null), new Document("_id", "real")));

        when(collection.aggregate(anyList(), eq(Document.class))).thenAnswer(invocation -> {
            final List<? extends Bson> pipeline = invocation.getArgument(0);
            final int call = pipelines.size();
            pipelines.add(pipeline);
            final Iterator<Document> iterator = rowsAfterNullMatch(pipeline, rowsByCall.get(call)).iterator();
            final AggregateIterable<Document> aggregateIterable = mock(AggregateIterable.class);
            final MongoCursor<Document> cursor = mock(MongoCursor.class);
            when(aggregateIterable.iterator()).thenReturn(cursor);
            when(cursor.hasNext()).thenAnswer(ignored -> iterator.hasNext());
            when(cursor.next()).thenAnswer(ignored -> iterator.next());
            return aggregateIterable;
        });

        final MongoCollectionMapper<Integer> intMapper = new MongoCollectionMapper<>(executor, int.class);
        final MongoCollectionMapper<Boolean> booleanMapper = new MongoCollectionMapper<>(executor, boolean.class);
        final MongoCollectionMapper<Integer> wrapperMapper = new MongoCollectionMapper<>(executor, Integer.class);
        final MongoCollectionMapper<String> referenceMapper = new MongoCollectionMapper<>(executor, String.class);
        final Bson filter = new Document("enabled", true);

        assertEquals(List.of(0), intMapper.distinct("age").toList());
        assertEquals(List.of(0), intMapper.distinct("age", filter).toList());
        assertEquals(List.of(false), booleanMapper.distinct("active").toList());
        assertEquals(Arrays.asList((Integer) null, 0), wrapperMapper.distinct("age").toList());
        assertEquals(Arrays.asList((String) null, "real"), referenceMapper.distinct("name").toList());

        final Document ageGroup = new Document("$group", new Document("_id", "$age"));
        final Document activeGroup = new Document("$group", new Document("_id", "$active"));
        final Document nameGroup = new Document("$group", new Document("_id", "$name"));
        final Document nonNullMatch = new Document("$match", nonNullCondition());

        assertEquals(5, pipelines.size());
        assertEquals(List.of(ageGroup, nonNullMatch), pipelines.get(0));
        assertEquals(3, pipelines.get(1).size());
        assertEquals(ageGroup, pipelines.get(1).get(1));
        assertEquals(nonNullMatch, pipelines.get(1).get(2));
        assertEquals(List.of(activeGroup, nonNullMatch), pipelines.get(2));
        assertEquals(List.of(ageGroup), pipelines.get(3));
        assertEquals(List.of(nameGroup), pipelines.get(4));
    }

    private static List<Document> rowsAfterNullMatch(final List<? extends Bson> pipeline, final List<Document> rows) {
        final Document nonNullCondition = nonNullCondition();
        boolean rejectNull = false;

        for (final Bson stage : pipeline) {
            if (stage instanceof final Document document && nonNullCondition.equals(document.get("$match"))) {
                rejectNull = true;
                break;
            }
        }

        if (!rejectNull) {
            return rows;
        }

        final List<Document> result = new ArrayList<>();

        for (final Document row : rows) {
            if (row.get("_id") != null) {
                result.add(row);
            }
        }

        return result;
    }

    private static Document nonNullCondition() {
        return new Document("_id", new Document("$ne", null));
    }
}
