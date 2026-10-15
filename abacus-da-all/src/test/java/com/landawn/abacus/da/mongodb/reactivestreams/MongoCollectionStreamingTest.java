package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;

import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class MongoCollectionStreamingTest extends MongoCollectionExecutorTestSupport {

    @Test
    public void testAggregate() {
        List<Bson> pipeline = Arrays.asList(new Document("$match", new Document("status", "active")), new Document("$group", new Document("_id", "$category")));
        List<Document> aggregateResults = Arrays.asList(new Document("_id", "electronics"), new Document("_id", "books"));
        when(mockCollection.aggregate(pipeline, Document.class)).thenReturn(mockAggregatePublisher);
        // when(mockAggregatePublisher.iterator()).thenReturn(aggregateResults.iterator());

        Flux<Document> result = executor.aggregate(pipeline);

        assertNotNull(result);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAggregateSingleValueRowTypeSkipsDocumentsWithoutAValue() {
        // Pins the behavior documented on aggregate(List, Class) (and the mapper's aggregate) this round: for a single-value
        // rowType an output document that yields no value (an empty document, or BSON null for a non-primitive rowType) is
        // skipped, as Reactive Streams forbids null elements; a primitive rowType receives its default for BSON null.
        final org.mockito.MockSettings lenient = org.mockito.Mockito.withSettings().strictness(org.mockito.quality.Strictness.LENIENT);
        final MongoCollection<Document> collection = mock(MongoCollection.class, lenient);
        final AggregatePublisher<Document> publisher = mock(AggregatePublisher.class, lenient);
        when(collection.aggregate(anyList(), eq(Document.class))).thenReturn(publisher);
        doAnswer(invocation -> {
            Flux.just(new Document(), new Document("v", null), new Document("_id", 1).append("v", 5))
                    .subscribe((Subscriber<? super Document>) invocation.getArgument(0));
            return null;
        }).when(publisher).subscribe(any());

        final MongoCollectionExecutor exec = new MongoCollectionExecutor(collection);
        final List<Document> pipeline = Arrays.asList(new Document("$project", new Document("v", 1)));

        assertEquals(Arrays.asList(5L), exec.aggregate(pipeline, Long.class).collectList().block());
        assertEquals(Arrays.asList("5"), exec.aggregate(pipeline, String.class).collectList().block());
        assertEquals(Arrays.asList(0L, 5L), exec.aggregate(pipeline, long.class).collectList().block());
    }

    @Test
    public void testAggregateWithNullRowTypeThrowsIAE() {
        // Regression: aggregate(List, Class) documents "@throws IAE if pipeline or rowType is null"
        // (matching the sync executor) but previously had no rowType guard and silently produced
        // Object[] rows for a null rowType. The guard now throws eagerly at the call site.
        List<Bson> pipeline = Arrays.asList(new Document("$match", new Document("status", "active")));
        assertThrows(IllegalArgumentException.class, () -> executor.aggregate(pipeline, null));
    }

    // Document (same short-circuit as list/findFirst), not rejected by readRow's scalar fallback.
    @Test
    public void testAggregateWithObjectRowTypeReturnsRawMultiFieldDocument() {
        final List<Bson> pipeline = Arrays.asList(new Document("$match", new Document("status", "active")));
        final Document wideDoc = new Document("_id", new ObjectId()).append("name", "John").append("age", 30);

        when(mockCollection.aggregate(pipeline, Document.class)).thenReturn(mockAggregatePublisher);
        stubEmits(mockAggregatePublisher, wideDoc);

        StepVerifier.create(executor.aggregate(pipeline, Object.class)).expectNext(wideDoc).verifyComplete();
        StepVerifier.create(executor.aggregate(pipeline, Bson.class)).expectNext(wideDoc).verifyComplete();
    }

    @Test
    public void testAggregateWithRowType() {
        List<Bson> pipeline = Arrays.asList(new Document("$match", new Document("status", "active")), new Document("$group", new Document("_id", "$category")));
        List<Document> aggregateResults = Arrays.asList(new Document("_id", "electronics"), new Document("_id", "books"));
        when(mockCollection.aggregate(pipeline, Document.class)).thenReturn(mockAggregatePublisher);
        // when(mockAggregatePublisher.iterator()).thenReturn(aggregateResults.iterator());

        Flux<Document> result = executor.aggregate(pipeline, Document.class);

        assertNotNull(result);
    }

    @Test
    public void testDistinct() {
        String fieldName = "category";
        when(mockCollection.distinct(fieldName, String.class)).thenReturn(mockDistinctPublisher);

        Flux<String> result = executor.distinct(fieldName, String.class);

        assertNotNull(result);
        verify(mockCollection).distinct(fieldName, String.class);
    }

    @Test
    public void testDistinctWithFilter() {
        String fieldName = "category";
        Bson filter = new Document("active", true);
        when(mockCollection.distinct(fieldName, filter, String.class)).thenReturn(mockDistinctPublisher);

        Flux<String> result = executor.distinct(fieldName, filter, String.class);

        assertNotNull(result);
        verify(mockCollection).distinct(fieldName, filter, String.class);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testGroupByAndCountCountFieldRejectedForEveryNonDocumentRowType() {
        // Same rule and message as the sync executor, thrown at call time (no aggregate, no subscription): only Document
        // rows (key kept under _id) accept a group field named "count"; a Document subclass is projected like a Map.
        final List<List<? extends Bson>> pipelines = new ArrayList<>();
        when(mockCollection.aggregate(anyList(), eq(Document.class))).thenAnswer(invocation -> {
            pipelines.add(invocation.getArgument(0));
            return mockAggregatePublisher;
        });

        for (final Class rowType : Arrays.<Class> asList(Map.class, java.util.LinkedHashMap.class, Object.class, Bson.class, CountDocument.class,
                GroupRow.class, Long.class, long.class, String.class)) {
            assertEquals(COUNT_COLUMN_CONFLICT, assertThrows(IllegalArgumentException.class, () -> executor.groupByAndCount("count", rowType)).getMessage(),
                    rowType.getName());
            assertEquals(COUNT_COLUMN_CONFLICT,
                    assertThrows(IllegalArgumentException.class, () -> executor.groupByAndCount(Arrays.asList("department", "count"), rowType)).getMessage(),
                    rowType.getName());
        }

        assertTrue(pipelines.isEmpty());

        // Document rows, other field names, and groupBy (no count column) are unaffected.
        executor.groupByAndCount("count", Document.class);
        executor.groupByAndCount(Arrays.asList("department", "count"), Document.class);
        executor.groupByAndCount("department", Map.class);
        executor.groupBy("count", CountDocument.class);
        executor.groupBy(Arrays.asList("department", "count"), Map.class);
        assertEquals(5, pipelines.size());
        assertEquals(new Document("$project", new Document("_id", 0).append("department", "$_id").append("count", 1)), pipelines.get(2).get(1));
    }

    // It must be rejected at call time, while the Document row type ({_id: key, count: n}) keeps working.
    @Test
    public void testGroupByAndCountRejectsGroupFieldNamedCountForNonDocumentRowType_sliceF() {
        final List<List<? extends Bson>> pipelines = new ArrayList<>();
        when(mockCollection.aggregate(anyList(), eq(Document.class))).thenAnswer(invocation -> {
            pipelines.add(invocation.getArgument(0));
            return mockAggregatePublisher;
        });

        for (final Class<?> rowType : Arrays.asList(Map.class, Object.class, GroupRow.class)) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> executor.groupByAndCount("count", rowType));
            assertTrue(e.getMessage().contains("'count'"), e.getMessage());
            e = assertThrows(IllegalArgumentException.class, () -> executor.groupByAndCount(Arrays.asList("department", "count"), rowType));
            assertTrue(e.getMessage().contains("'count'"), e.getMessage());
        }

        assertTrue(pipelines.isEmpty());

        // The Document row type keeps the key in _id and is unaffected; groupBy (no count column) is unaffected too.
        assertNotNull(executor.groupByAndCount("count"));
        assertNotNull(executor.groupByAndCount(Arrays.asList("department", "count")));
        assertNotNull(executor.groupBy("count", Map.class));
        assertEquals(3, pipelines.size());
    }

    @Test
    public void testGroupByAndCountTypedMapsSingleGroupKey() {
        when(mockCollection.aggregate(anyList(), eq(Document.class))).thenAnswer(invocation -> {
            final List<?> pipeline = invocation.getArgument(0);
            final Document row = pipeline.size() > 1 ? new Document("department", "sales").append("count", 2) : new Document("_id", "sales").append("count", 2);

            stubEmits(mockAggregatePublisher, row);
            return mockAggregatePublisher;
        });

        StepVerifier.create(executor.groupByAndCount("department", GroupRow.class))
                .expectNextMatches(row -> "sales".equals(row.getDepartment()) && row.getCount() == 2)
                .verifyComplete();
    }

    @Test
    public void testGroupByAndCountWithFieldName() {
        String fieldName = "category";
        List<Document> groupResults = Arrays.asList(new Document("_id", "electronics").append("count", 10), new Document("_id", "books").append("count", 5));
        when(mockCollection.aggregate(anyList(), eq(Document.class))).thenReturn(mockAggregatePublisher);
        // when(mockAggregatePublisher.iterator()).thenReturn(groupResults.iterator());

        Flux<Document> result = executor.groupByAndCount(fieldName);

        assertNotNull(result);
    }

    @Test
    public void testGroupByAndCountWithFieldNameAndRowType() {
        String fieldName = "category";
        List<Document> groupResults = Arrays.asList(new Document("_id", "electronics").append("count", 10), new Document("_id", "books").append("count", 5));
        when(mockCollection.aggregate(anyList(), eq(Document.class))).thenReturn(mockAggregatePublisher);
        // when(mockAggregatePublisher.iterator()).thenReturn(groupResults.iterator());

        Flux<Document> result = executor.groupByAndCount(fieldName, Document.class);

        assertNotNull(result);
    }

    @Test
    public void testGroupByAndCountWithFieldNames() {
        Collection<String> fieldNames = Arrays.asList("category", "brand");
        List<Document> groupResults = Arrays
                .asList(new Document("_id", new Document("category", "electronics").append("brand", "Samsung")).append("count", 10));
        when(mockCollection.aggregate(anyList(), eq(Document.class))).thenReturn(mockAggregatePublisher);
        // when(mockAggregatePublisher.iterator()).thenReturn(groupResults.iterator());

        Flux<Document> result = executor.groupByAndCount(fieldNames);

        assertNotNull(result);
    }

    @Test
    public void testGroupByAndCountWithFieldNamesAndRowType() {
        Collection<String> fieldNames = Arrays.asList("category", "brand");
        List<Document> groupResults = Arrays
                .asList(new Document("_id", new Document("category", "electronics").append("brand", "Samsung")).append("count", 10));
        when(mockCollection.aggregate(anyList(), eq(Document.class))).thenReturn(mockAggregatePublisher);
        // when(mockAggregatePublisher.iterator()).thenReturn(groupResults.iterator());

        Flux<Document> result = executor.groupByAndCount(fieldNames, Document.class);

        assertNotNull(result);
    }

    // its JSON text ('{"b": "v1"}') for String (NumberFormatException for Integer). Scalar results must be the keys.
    @Test
    public void testGroupByDottedFieldIntoScalarTypeEmitsGroupKeys_sliceF() {
        final List<List<? extends Bson>> pipelines = new ArrayList<>();
        when(mockCollection.aggregate(anyList(), eq(Document.class))).thenAnswer(invocation -> {
            pipelines.add(invocation.getArgument(0));
            return mockAggregatePublisher;
        });
        doAnswer(invocation -> {
            // Emit what MongoDB returns for the pipeline: $group alone yields {_id: key}, while a trailing
            // $project {"a.b": "$_id", _id: 0} yields {a: {b: key}} (live-verified against MongoDB).
            final Document row = pipelines.get(pipelines.size() - 1).size() == 1 ? new Document("_id", "v1") : new Document("a", new Document("b", "v1"));
            Flux.just(row).subscribe(invocation.<Subscriber<? super Document>> getArgument(0));
            return null;
        }).when(mockAggregatePublisher).subscribe(any());

        StepVerifier.create(executor.groupBy("a.b", String.class)).expectNext("v1").verifyComplete();

        // Map/bean results keep the named-field projection.
        StepVerifier.create(executor.groupBy("a.b", Map.class)).expectNext(new Document("a", new Document("b", "v1"))).verifyComplete();
        assertEquals(2, pipelines.get(pipelines.size() - 1).size());
    }

    @Test
    public void testGroupByWithFieldName() {
        String fieldName = "category";
        List<Document> groupResults = Arrays.asList(new Document("_id", "electronics"), new Document("_id", "books"));
        when(mockCollection.aggregate(anyList(), eq(Document.class))).thenReturn(mockAggregatePublisher);
        // when(mockAggregatePublisher.iterator()).thenReturn(groupResults.iterator());

        Flux<Document> result = executor.groupBy(fieldName);

        assertNotNull(result);
    }

    @Test
    public void testGroupByWithFieldNameAndRowType() {
        String fieldName = "category";
        List<Document> groupResults = Arrays.asList(new Document("_id", "electronics"), new Document("_id", "books"));
        when(mockCollection.aggregate(anyList(), eq(Document.class))).thenReturn(mockAggregatePublisher);
        // when(mockAggregatePublisher.iterator()).thenReturn(groupResults.iterator());

        Flux<Document> result = executor.groupBy(fieldName, Document.class);

        assertNotNull(result);
    }

    @Test
    public void testGroupByWithFieldNames() {
        Collection<String> fieldNames = Arrays.asList("category", "brand");
        List<Document> groupResults = Arrays.asList(new Document("_id", new Document("category", "electronics").append("brand", "Samsung")));
        when(mockCollection.aggregate(anyList(), eq(Document.class))).thenReturn(mockAggregatePublisher);
        // when(mockAggregatePublisher.iterator()).thenReturn(groupResults.iterator());

        Flux<Document> result = executor.groupBy(fieldNames);

        assertNotNull(result);
    }

    @Test
    public void testGroupByWithFieldNamesAndRowType() {
        Collection<String> fieldNames = Arrays.asList("category", "brand");
        List<Document> groupResults = Arrays.asList(new Document("_id", new Document("category", "electronics").append("brand", "Samsung")));
        when(mockCollection.aggregate(anyList(), eq(Document.class))).thenReturn(mockAggregatePublisher);
        // when(mockAggregatePublisher.iterator()).thenReturn(groupResults.iterator());

        Flux<Document> result = executor.groupBy(fieldNames, Document.class);

        assertNotNull(result);
    }

    @Test
    public void testMapReduce() {
        String mapFunction = "function() { emit(this.category, 1); }";
        String reduceFunction = "function(key, values) { return Array.sum(values); }";
        List<Document> mapReduceResults = Arrays.asList(new Document("_id", "electronics").append("value", 10),
                new Document("_id", "books").append("value", 5));
        when(mockCollection.mapReduce(mapFunction, reduceFunction, Document.class)).thenReturn(mockMapReducePublisher);
        // when(mockMapReducePublisher.iterator()).thenReturn(mapReduceResults.iterator());

        Flux<Document> result = executor.mapReduce(mapFunction, reduceFunction);

        assertNotNull(result);
    }

    @Test
    public void testMapReduceWithNullRowTypeThrowsIAE() {
        assertThrows(IllegalArgumentException.class,
                () -> executor.mapReduce("function() { emit(this.category, 1); }", "function(key, values) { return Array.sum(values); }", null));
    }

    @Test
    public void testMapReduceWithRowType() {
        String mapFunction = "function() { emit(this.category, 1); }";
        String reduceFunction = "function(key, values) { return Array.sum(values); }";
        List<Document> mapReduceResults = Arrays.asList(new Document("_id", "electronics").append("value", 10),
                new Document("_id", "books").append("value", 5));
        when(mockCollection.mapReduce(mapFunction, reduceFunction, Document.class)).thenReturn(mockMapReducePublisher);
        // when(mockMapReducePublisher.iterator()).thenReturn(mapReduceResults.iterator());

        Flux<Document> result = executor.mapReduce(mapFunction, reduceFunction, Document.class);

        assertNotNull(result);
    }

    @Test
    public void testWatch() {
        when(mockCollection.watch()).thenReturn(mockChangeStreamPublisher);

        ChangeStreamPublisher<Document> result = executor.watch();

        assertSame(mockChangeStreamPublisher, result);
    }

    @Test
    public void testWatchWithPipeline() {
        List<Bson> pipeline = Arrays.asList(new Document("$match", new Document("operationType", "insert")));
        when(mockCollection.watch(pipeline)).thenReturn(mockChangeStreamPublisher);

        ChangeStreamPublisher<Document> result = executor.watch(pipeline);

        assertSame(mockChangeStreamPublisher, result);
    }

    @Test
    public void testWatchWithPipelineAndRowType() {
        List<Bson> pipeline = Arrays.asList(new Document("$match", new Document("operationType", "update")));
        ChangeStreamPublisher<Map> mockMapPublisher = mock(ChangeStreamPublisher.class);
        when(mockCollection.watch(pipeline, Map.class)).thenReturn(mockMapPublisher);

        ChangeStreamPublisher<Map> result = executor.watch(pipeline, Map.class);

        assertSame(mockMapPublisher, result);
    }

    @Test
    public void testWatchWithRowType() {
        ChangeStreamPublisher<String> mockStringPublisher = mock(ChangeStreamPublisher.class);
        when(mockCollection.watch(String.class)).thenReturn(mockStringPublisher);

        ChangeStreamPublisher<String> result = executor.watch(String.class);

        assertSame(mockStringPublisher, result);
    }
}
