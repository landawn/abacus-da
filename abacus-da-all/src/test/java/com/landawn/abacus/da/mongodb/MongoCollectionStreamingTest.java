package com.landawn.abacus.da.mongodb;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.util.stream.Stream;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCursor;

public class MongoCollectionStreamingTest extends MongoCollectionExecutorTestSupport {

    @Test
    public void testAggregate() {
        List<Document> pipeline = Arrays.asList(new Document("$match", new Document("status", "active")));
        when(mockCollection.aggregate(pipeline, Document.class)).thenReturn(mock());

        Stream<Document> result = executor.aggregate(pipeline);
        Assertions.assertNotNull(result);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testAggregateDefaultDocumentRowTypePreservesRowContent() {
        // The default aggregate(pipeline) decodes to Document and maps each row through toEntity,
        // which hands Document-assignable row types the raw row; this pins the content-preserving
        // pass-through of multi-field aggregate rows for the default rowType.
        final AggregateIterable<Document> aggregateIterable = mock(AggregateIterable.class);
        final MongoCursor<Document> cursor = mock(MongoCursor.class);

        final Document row = new Document("_id", "sales").append("count", 2).append("region", "east");

        when(mockCollection.aggregate(anyList(), eq(Document.class))).thenReturn(aggregateIterable);
        when(aggregateIterable.iterator()).thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(true, false);
        when(cursor.next()).thenReturn(row);

        final List<Document> pipeline = Arrays.asList(new Document("$group", new Document("_id", "$department").append("count", new Document("$sum", 1))));
        final List<Document> rows = executor.aggregate(pipeline).toList();

        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(row, rows.get(0));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testAggregateWithDocumentAssignableRowTypeReturnsRawDocuments() {
        // Regression: aggregate(pipeline, Object.class) sent every row through readRow's scalar fallback,
        // which throws for a multi-field row and silently dropped _id for a single-field row. Result types
        // that can hold a Document (Object, Bson, Map, Document) must receive the raw row, matching
        // list/stream/findFirst.
        final AggregateIterable<Document> aggregateIterable = mock(AggregateIterable.class);
        final MongoCursor<Document> cursor = mock(MongoCursor.class);
        final Document row = new Document("_id", "sales").append("count", 2).append("region", "east");
        final List<Document> pipeline = Arrays.asList(new Document("$group", new Document("_id", "$department")));

        when(mockCollection.aggregate(anyList(), eq(Document.class))).thenReturn(aggregateIterable);
        when(aggregateIterable.iterator()).thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(true, false);
        when(cursor.next()).thenReturn(row);

        final List<Object> objects = executor.aggregate(pipeline, Object.class).toList();

        Assertions.assertEquals(1, objects.size());
        Assertions.assertSame(row, objects.get(0));

        when(cursor.hasNext()).thenReturn(true, false);
        final List<Bson> bsons = executor.aggregate(pipeline, Bson.class).toList();
        Assertions.assertSame(row, bsons.get(0));

        when(cursor.hasNext()).thenReturn(true, false);
        final List<Document> documents = executor.aggregate(pipeline, Document.class).toList();
        Assertions.assertSame(row, documents.get(0));
    }

    @Test
    public void testAggregateWithNullRowTypeThrowsIAE() {
        // Regression: a null rowType is now rejected eagerly with IllegalArgumentException, before any
        // aggregation cursor is opened.
        List<Document> pipeline = Arrays.asList(new Document("$match", new Document("status", "active")));

        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.aggregate(pipeline, (Class<Document>) null));

        verify(mockCollection, never()).aggregate(anyList(), eq(Document.class));
    }

    @Test
    public void testDistinct() {
        String fieldName = "category";
        MongoCursor<String> cursor = mock(MongoCursor.class);
        when(mockCollection.distinct(fieldName, String.class)).thenReturn(mock());

        Stream<String> result = executor.distinct(fieldName, String.class);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testDistinctWithFilter() {
        String fieldName = "category";
        Document filter = new Document("active", true);
        when(mockCollection.distinct(eq(fieldName), eq(filter), eq(String.class))).thenReturn(mock());

        Stream<String> result = executor.distinct(fieldName, filter, String.class);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testGroupBy() {
        String fieldName = "category";
        when(mockCollection.aggregate(anyList(), eq(Document.class))).thenReturn(mock());

        Stream<Document> result = executor.groupBy(fieldName);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testGroupByAndCount() {
        String fieldName = "category";
        when(mockCollection.aggregate(anyList(), eq(Document.class))).thenReturn(mock());

        Stream<Document> result = executor.groupByAndCount(fieldName);
        Assertions.assertNotNull(result);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testGroupByAndCountCountFieldRejectedForEveryNonDocumentRowType() {
        // Same rule and message as the reactive executor: only Document rows (key kept under _id) accept a group field
        // named "count"; every other row type is projected as {<field>: key, count: n}, a Document subclass included.
        final List<List<? extends Bson>> pipelines = new java.util.ArrayList<>();
        when(mockCollection.aggregate(anyList(), eq(Document.class))).thenAnswer(invocation -> {
            pipelines.add(invocation.getArgument(0));
            final AggregateIterable<Document> aggregateIterable = mock(AggregateIterable.class);
            when(aggregateIterable.iterator()).thenReturn(mock(MongoCursor.class));
            return aggregateIterable;
        });

        for (final Class rowType : Arrays.<Class> asList(java.util.Map.class, java.util.LinkedHashMap.class, Object.class, Bson.class, CountDocument.class,
                GroupRow.class, Long.class, long.class, String.class)) {
            Assertions.assertEquals(COUNT_COLUMN_CONFLICT,
                    Assertions.assertThrows(IllegalArgumentException.class, () -> executor.groupByAndCount("count", rowType)).getMessage(), rowType.getName());
            Assertions.assertEquals(COUNT_COLUMN_CONFLICT,
                    Assertions.assertThrows(IllegalArgumentException.class, () -> executor.groupByAndCount(Arrays.asList("department", "count"), rowType))
                            .getMessage(),
                    rowType.getName());
        }

        Assertions.assertTrue(pipelines.isEmpty());

        // Document rows, other field names, and groupBy (no count column) are unaffected.
        executor.groupByAndCount("count", Document.class).count();
        executor.groupByAndCount(Arrays.asList("department", "count"), Document.class).count();
        executor.groupByAndCount("department", java.util.Map.class).count();
        executor.groupBy("count", CountDocument.class).count();
        executor.groupBy(Arrays.asList("department", "count"), java.util.Map.class).count();
        Assertions.assertEquals(5, pipelines.size());
        Assertions.assertEquals(new Document("$project", new Document("_id", 0).append("department", "$_id").append("count", 1)), pipelines.get(2).get(1));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGroupByAndCountRejectsGroupFieldNamedCountForNonDocumentRows() {
        // Regression: the {fieldName: "$_id", count: 1} projection overwrote a group field literally named "count"
        // with the count, silently losing the group key (live: groupByAndCount("count", Map.class) -> [{count=2}, {count=1}]).
        final AggregateIterable<Document> aggregateIterable = mock(AggregateIterable.class);
        final MongoCursor<Document> cursor = mock(MongoCursor.class);
        when(aggregateIterable.iterator()).thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(false);
        when(mockCollection.aggregate(anyList(), eq(Document.class))).thenReturn(aggregateIterable);

        final String expectedMessage = "Group field name 'count' conflicts with the count column of groupByAndCount; use Document as the row type";

        Assertions.assertEquals(expectedMessage,
                Assertions.assertThrows(IllegalArgumentException.class, () -> executor.groupByAndCount("count", java.util.Map.class)).getMessage());
        Assertions.assertEquals(expectedMessage,
                Assertions.assertThrows(IllegalArgumentException.class, () -> executor.groupByAndCount(Arrays.asList("count", "cat"), GroupRow.class))
                        .getMessage());
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.groupByAndCount("count", Long.class));
        verify(mockCollection, never()).aggregate(anyList(), eq(Document.class));

        // Document rows keep the key in _id, so they are unaffected; groupBy (no count column) is unaffected too.
        Assertions.assertEquals(0, executor.groupByAndCount("count").count());
        Assertions.assertEquals(0, executor.groupByAndCount(Arrays.asList("count", "cat")).count());
        Assertions.assertEquals(0, executor.groupBy("count", java.util.Map.class).count());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGroupByAndCountTypedMapsSingleGroupKey() {
        final AggregateIterable<Document> aggregateIterable = mock(AggregateIterable.class);
        final MongoCursor<Document> cursor = mock(MongoCursor.class);

        when(mockCollection.aggregate(anyList(), eq(Document.class))).thenAnswer(invocation -> {
            final List<?> pipeline = invocation.getArgument(0);
            final Document row = pipeline.size() > 1 ? new Document("department", "sales").append("count", 2) : new Document("_id", "sales").append("count", 2);

            when(aggregateIterable.iterator()).thenReturn(cursor);
            when(cursor.hasNext()).thenReturn(true, false);
            when(cursor.next()).thenReturn(row);
            return aggregateIterable;
        });

        final List<GroupRow> rows = executor.groupByAndCount("department", GroupRow.class).toList();

        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("sales", rows.get(0).getDepartment());
        Assertions.assertEquals(2, rows.get(0).getCount());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGroupByDottedFieldIntoScalarTypeEmitsGroupKeys() {
        // Regression: groupBy("a.b", String.class) re-projected the key as {"a.b": "$_id"}, which MongoDB
        // materializes as {a: {b: key}}; the scalar conversion then received the embedded document
        // ({"b": "v1"} as JSON text, or NumberFormatException for Integer) instead of the key.
        final List<List<? extends Bson>> pipelines = new java.util.ArrayList<>();

        when(mockCollection.aggregate(anyList(), eq(Document.class))).thenAnswer(invocation -> {
            final List<? extends Bson> pipeline = invocation.getArgument(0);
            pipelines.add(pipeline);

            // Emit what MongoDB returns for the pipeline (live-verified): $group alone yields {_id: key},
            // while a trailing $project {"a.b": "$_id", _id: 0} yields {a: {b: key}}.
            final boolean groupsByNM = "$n.m".equals(((Document) pipeline.get(0)).get("$group", Document.class).get("_id"));
            final Object key = groupsByNM ? (Object) 7 : "v1";
            final Document row = pipeline.size() == 1 ? new Document("_id", key)
                    : groupsByNM ? new Document("n", new Document("m", key)) : new Document("a", new Document("b", key));

            final AggregateIterable<Document> aggregateIterable = mock(AggregateIterable.class);
            final MongoCursor<Document> cursor = mock(MongoCursor.class);
            when(aggregateIterable.iterator()).thenReturn(cursor);
            when(cursor.hasNext()).thenReturn(true, false);
            when(cursor.next()).thenReturn(row);
            return aggregateIterable;
        });

        Assertions.assertEquals(Arrays.asList("v1"), executor.groupBy("a.b", String.class).toList());
        Assertions.assertEquals(1, pipelines.get(pipelines.size() - 1).size());

        Assertions.assertEquals(Arrays.asList(7), executor.groupBy("n.m", Integer.class).toList());
        Assertions.assertEquals(1, pipelines.get(pipelines.size() - 1).size());

        // Map/bean/Object results keep the named-field projection.
        Assertions.assertEquals(Arrays.asList(new Document("a", new Document("b", "v1"))), executor.groupBy("a.b", java.util.Map.class).toList());
        Assertions.assertEquals(2, pipelines.get(pipelines.size() - 1).size());
    }

    @Test
    public void testGroupByWithMultipleFields() {
        Collection<String> fieldNames = Arrays.asList("category", "status");
        when(mockCollection.aggregate(anyList(), eq(Document.class))).thenReturn(mock());

        Stream<Document> result = executor.groupBy(fieldNames);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testGroupByWithNullRowTypeThrowsIAE() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.groupBy("category", (Class<Document>) null));

        verify(mockCollection, never()).aggregate(anyList(), eq(Document.class));
    }

    @Test
    public void testMapReduce() {
        String mapFunction = "function() { emit(this.name, 1); }";
        String reduceFunction = "function(key, values) { return Array.sum(values); }";
        when(mockCollection.mapReduce(mapFunction, reduceFunction, Document.class)).thenReturn(mock());

        Stream<Document> result = executor.mapReduce(mapFunction, reduceFunction);
        Assertions.assertNotNull(result);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMapReduceWithDocumentAssignableRowTypeReturnsRawDocuments() {
        final com.mongodb.client.MapReduceIterable<Document> mapReduceIterable = mock(com.mongodb.client.MapReduceIterable.class);
        final MongoCursor<Document> cursor = mock(MongoCursor.class);
        final Document row = new Document("_id", "sales").append("value", 2).append("extra", "x");

        when(mockCollection.mapReduce(any(String.class), any(String.class), eq(Document.class))).thenReturn(mapReduceIterable);
        when(mapReduceIterable.iterator()).thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(true, false);
        when(cursor.next()).thenReturn(row);

        final List<java.util.Map> rows = executor
                .mapReduce("function() { emit(this.name, 1); }", "function(k, v) { return Array.sum(v); }", java.util.Map.class)
                .toList();

        Assertions.assertEquals(1, rows.size());
        Assertions.assertSame(row, rows.get(0));
    }

    @Test
    public void testStream() {
        when(mockCollection.find()).thenReturn(mockFindIterable);

        Stream<Document> result = executor.stream();
        Assertions.assertNotNull(result);
    }

    @Test
    public void testStreamRejectsNullRowTypeBeforeOpeningCursor() {
        // Regression: previously, stream(null) would open a cursor and then throw NPE on
        // `rowType.isAssignableFrom(...)`, leaking the cursor. The contract says null rowType
        // must throw IllegalArgumentException, and no cursor should be opened.
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.stream((Class<Document>) null));

        verify(mockCollection, org.mockito.Mockito.never()).find();
    }

    @Test
    public void testStreamWithBsonProjectionFilterSort() {
        Bson projection = com.mongodb.client.model.Projections.include("name");
        Document filter = new Document("status", "active");
        Document sort = new Document("name", 1);
        when(mockFindIterable.iterator()).thenReturn(mockCursor);

        Stream<Document> result = executor.stream(projection, filter, sort, Document.class);
        Assertions.assertNotNull(result);
        result.close();
    }

    @Test
    public void testStreamWithBsonProjectionRejectsNullRowTypeBeforeOpeningCursor() {
        // Regression: cursor-leak fix for the Bson-projection stream overload.
        final Bson projection = com.mongodb.client.model.Projections.include("name");
        final Bson filter = new Document("status", "active");
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.stream(projection, filter, (Bson) null, 0, 10, (Class<Document>) null));

        verify(mockCollection, org.mockito.Mockito.never()).find(any(Bson.class));
        verify(mockCollection, org.mockito.Mockito.never()).find();
    }

    @Test
    public void testStreamWithCollectionProjectionRejectsNullRowTypeBeforeOpeningCursor() {
        // Regression: cursor-leak fix for the multi-arg stream overload.
        final Collection<String> selectPropNames = java.util.Collections.singletonList("name");
        final Document filter = new Document("status", "active");
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.stream(selectPropNames, filter, (Bson) null, 0, 10, (Class<Document>) null));

        verify(mockCollection, org.mockito.Mockito.never()).find(any(Bson.class));
        verify(mockCollection, org.mockito.Mockito.never()).find();
    }

    @Test
    public void testStreamWithDocumentClassCallsFindNoArgs() {
        when(mockCollection.find()).thenReturn(mockFindIterable);
        when(mockFindIterable.iterator()).thenReturn(mockCursor);
        Stream<Document> s = executor.stream(Document.class);
        Assertions.assertNotNull(s);
        s.close();
    }

    @Test
    public void testStreamWithFilter() {
        Document filter = new Document("status", "active");
        when(mockFindIterable.iterator()).thenReturn(mockCursor);

        Stream<Document> result = executor.stream(filter);
        Assertions.assertNotNull(result);
        result.close();
    }

    @Test
    public void testStreamWithFilterAndRowType() {
        Document filter = new Document("status", "active");
        when(mockFindIterable.iterator()).thenReturn(mockCursor);

        Stream<Document> result = executor.stream(filter, Document.class);
        Assertions.assertNotNull(result);
        result.close();
    }

    @Test
    public void testStreamWithFilterOffsetCountAndClass() {
        Document filter = new Document("a", 1);
        when(mockFindIterable.iterator()).thenReturn(mockCursor);
        Stream<Document> s = executor.stream(filter, 0, 10, Document.class);
        Assertions.assertNotNull(s);
        s.close();
    }

    @Test
    public void testStreamWithFilterOffsetCountAndNonDocumentClass() {
        Document filter = new Document("a", 1);
        when(mockFindIterable.iterator()).thenReturn(mockCursor);
        when(mockCursor.hasNext()).thenReturn(false);
        Stream<TestEntity> s = executor.stream(filter, 0, 10, TestEntity.class);
        Assertions.assertNotNull(s);
        s.close();
    }

    @Test
    public void testStreamWithNonDocumentClassUsesToEntityMapper() {
        when(mockCollection.find()).thenReturn(mockFindIterable);
        when(mockFindIterable.iterator()).thenReturn(mockCursor);
        when(mockCursor.hasNext()).thenReturn(false);
        Stream<TestEntity> s = executor.stream(TestEntity.class);
        Assertions.assertNotNull(s);
        s.close();
    }

    @Test
    public void testStreamWithObjectRowTypeReturnsRawDocuments() {
        final Document row = new Document("_id", new ObjectId()).append("a", 1).append("b", 2);
        when(mockCollection.find()).thenReturn(mockFindIterable);
        when(mockCursor.hasNext()).thenReturn(true, false);
        when(mockCursor.next()).thenReturn(row);

        try (Stream<Object> stream = executor.stream(Object.class)) {
            Assertions.assertSame(row, stream.first().get());
        }
    }

    @Test
    public void testStreamWithProjectionFilterSortAndClass() {
        Bson projection = com.mongodb.client.model.Projections.include("name");
        Document filter = new Document("a", 1);
        Document sort = new Document("a", 1);
        when(mockFindIterable.iterator()).thenReturn(mockCursor);
        Stream<Document> s = executor.stream(projection, filter, sort, Document.class);
        Assertions.assertNotNull(s);
        s.close();
    }

    @Test
    public void testStreamWithProjectionFilterSortOffsetCountAndNonDocumentClass() {
        Bson projection = com.mongodb.client.model.Projections.include("name");
        Document filter = new Document("a", 1);
        Document sort = new Document("a", 1);
        when(mockFindIterable.iterator()).thenReturn(mockCursor);
        when(mockCursor.hasNext()).thenReturn(false);
        Stream<TestEntity> s = executor.stream(projection, filter, sort, 0, 10, TestEntity.class);
        Assertions.assertNotNull(s);
        s.close();
    }

    @Test
    public void testStreamWithRowType() {
        when(mockCollection.find()).thenReturn(mockFindIterable);

        Stream<Document> result = executor.stream(Document.class);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testStreamWithSelectFilterSortAndClass() {
        Document filter = new Document("a", 1);
        Document sort = new Document("a", 1);
        when(mockFindIterable.iterator()).thenReturn(mockCursor);
        Stream<Document> s = executor.stream(Arrays.asList("a"), filter, sort, Document.class);
        Assertions.assertNotNull(s);
        s.close();
    }

    @Test
    public void testStreamWithSelectFilterSortOffsetCountAndClass() {
        Document filter = new Document("a", 1);
        Document sort = new Document("a", 1);
        when(mockFindIterable.iterator()).thenReturn(mockCursor);
        Stream<Document> s = executor.stream(Arrays.asList("a"), filter, sort, 0, 10, Document.class);
        Assertions.assertNotNull(s);
        s.close();
    }

    @Test
    public void testStreamWithSelectPropNamesAndFilter() {
        Document filter = new Document("status", "active");
        when(mockFindIterable.iterator()).thenReturn(mockCursor);

        Stream<Document> result = executor.stream(Arrays.asList("name"), filter, Document.class);
        Assertions.assertNotNull(result);
        result.close();
    }

    @Test
    public void testWatch() {
        ChangeStreamIterable<Document> changeStream = mock(ChangeStreamIterable.class);
        when(mockCollection.watch()).thenReturn(changeStream);

        ChangeStreamIterable<Document> result = executor.watch();
        Assertions.assertSame(changeStream, result);
    }

    @Test
    public void testWatchWithPipeline() {
        List<Document> pipeline = Arrays.asList(new Document("$match", new Document()));
        ChangeStreamIterable<Document> changeStream = mock(ChangeStreamIterable.class);
        when(mockCollection.watch(pipeline)).thenReturn(changeStream);

        ChangeStreamIterable<Document> result = executor.watch(pipeline);
        Assertions.assertSame(changeStream, result);
    }

    @Test
    public void testWatchWithPipelineAndRowType() {
        List<Document> pipeline = Arrays.asList(new Document("$match", new Document()));
        ChangeStreamIterable<Document> changeStream = mock(ChangeStreamIterable.class);
        when(mockCollection.watch(pipeline, Document.class)).thenReturn(changeStream);

        ChangeStreamIterable<Document> result = executor.watch(pipeline, Document.class);
        Assertions.assertSame(changeStream, result);
    }

    @Test
    public void testWatchWithRowType() {
        ChangeStreamIterable<TestEntity> changeStream = mock(ChangeStreamIterable.class);
        when(mockCollection.watch(TestEntity.class)).thenReturn(changeStream);

        ChangeStreamIterable<TestEntity> result = executor.watch(TestEntity.class);
        Assertions.assertSame(changeStream, result);
    }
}
