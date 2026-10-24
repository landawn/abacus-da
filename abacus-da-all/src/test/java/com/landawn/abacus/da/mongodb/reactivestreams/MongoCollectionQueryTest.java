package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import com.landawn.abacus.util.Dataset;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.model.Projections;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class MongoCollectionQueryTest extends MongoCollectionExecutorTestSupport {

    @Test
    public void testCount() {
        Publisher<Long> countPublisher = Mono.just(100L);
        when(mockCollection.countDocuments()).thenReturn(countPublisher);

        Mono<Long> result = executor.count();

        StepVerifier.create(result).expectNext(100L).verifyComplete();
    }

    @Test
    public void testCountWithFilter() {
        Bson filter = new Document("status", "active");
        Publisher<Long> countPublisher = Mono.just(50L);
        when(mockCollection.countDocuments(filter)).thenReturn(countPublisher);

        Mono<Long> result = executor.count(filter);

        StepVerifier.create(result).expectNext(50L).verifyComplete();
    }

    @Test
    public void testCountWithFilterAndNullOptions() {
        Bson filter = new Document("status", "active");
        Publisher<Long> countPublisher = Mono.just(25L);
        when(mockCollection.countDocuments(filter)).thenReturn(countPublisher);

        Mono<Long> result = executor.count(filter, null);

        StepVerifier.create(result).expectNext(25L).verifyComplete();
    }

    @Test
    public void testCountWithFilterAndOptions() {
        Bson filter = new Document("status", "active");
        CountOptions options = new CountOptions().limit(10);
        Publisher<Long> countPublisher = Mono.just(10L);
        when(mockCollection.countDocuments(filter, options)).thenReturn(countPublisher);

        Mono<Long> result = executor.count(filter, options);

        StepVerifier.create(result).expectNext(10L).verifyComplete();
    }

    @Test
    public void testDottedSelectNameCheckToleratesNullNames() {
        // Pin: the dotted-name check runs when the Mono is assembled, so it must be null-safe: a null select name still fails
        // through the publisher with the Dataset's IllegalArgumentException (as before the fix), whichever position it has.
        final MongoCollectionExecutor exec = new MongoCollectionExecutor(
                mockCollectionReturning(new Document("_id", 1).append("address", new Document("city", "Paris"))));
        final Bson filter = new Document();

        for (final List<String> names : Arrays.asList(Arrays.asList(null, "address.city"), Arrays.asList("address.city", null),
                Arrays.asList("department", null))) {
            StepVerifier.create(exec.query(names, filter, Map.class)).expectError(IllegalArgumentException.class).verify();
        }
    }

    @Test
    public void testEstimatedDocumentCount() {
        Publisher<Long> countPublisher = Mono.just(1000L);
        when(mockCollection.estimatedDocumentCount()).thenReturn(countPublisher);

        Mono<Long> result = executor.estimatedDocumentCount();

        StepVerifier.create(result).expectNext(1000L).verifyComplete();
    }

    @Test
    public void testEstimatedDocumentCountWithNullOptionsUsesDefaults() {
        // Null options fall back to the no-options driver call (family convention),
        // instead of the driver's notNull IAE.
        Publisher<Long> countPublisher = Mono.just(42L);
        when(mockCollection.estimatedDocumentCount()).thenReturn(countPublisher);

        Mono<Long> result = executor.estimatedDocumentCount((EstimatedDocumentCountOptions) null);

        StepVerifier.create(result).expectNext(42L).verifyComplete();
    }

    @Test
    public void testEstimatedDocumentCountWithOptions() {
        EstimatedDocumentCountOptions options = new EstimatedDocumentCountOptions();
        Publisher<Long> countPublisher = Mono.just(2000L);
        when(mockCollection.estimatedDocumentCount(options)).thenReturn(countPublisher);

        Mono<Long> result = executor.estimatedDocumentCount(options);

        StepVerifier.create(result).expectNext(2000L).verifyComplete();
    }

    @Test
    public void testExistsWithEmptyStringObjectId() {
        assertThrows(IllegalArgumentException.class, () -> {
            executor.exists("");
        });
    }

    @Test
    public void testExistsWithFilter() {
        Bson filter = new Document("name", "test");
        Publisher<Long> countPublisher = Mono.just(2L);
        when(mockCollection.countDocuments(any(Bson.class), any(CountOptions.class))).thenReturn(countPublisher);

        Mono<Boolean> result = executor.exists(filter);

        StepVerifier.create(result).expectNext(true).verifyComplete();
    }

    @Test
    public void testExistsWithNullStringObjectId() {
        assertThrows(IllegalArgumentException.class, () -> {
            executor.exists((String) null);
        });
    }

    @Test
    public void testExistsWithObjectId() {
        ObjectId objectId = new ObjectId();
        Publisher<Long> countPublisher = Mono.just(0L);
        when(mockCollection.countDocuments(any(Bson.class), any(CountOptions.class))).thenReturn(countPublisher);

        Mono<Boolean> result = executor.exists(objectId);

        StepVerifier.create(result).expectNext(false).verifyComplete();
    }

    @Test
    public void testExistsWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        Publisher<Long> countPublisher = Mono.just(1L);
        when(mockCollection.countDocuments(any(Bson.class), any(CountOptions.class))).thenReturn(countPublisher);

        Mono<Boolean> result = executor.exists(objectId);

        StepVerifier.create(result).expectNext(true).verifyComplete();
    }

    @Test
    public void testFindFirstBsonProjectionObjectRowTypeReturnsRawDocument() {
        Bson projection = Projections.include("email");
        Bson filter = new Document("active", true);
        Bson sort = new Document("name", 1);
        Document doc = new Document("_id", new ObjectId()).append("email", "a@b.c").append("name", "John");

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(projection)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.sort(sort)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        StepVerifier.create(executor.findFirst(projection, filter, sort, Object.class)).expectNext(doc).verifyComplete();
    }

    @Test
    public void testFindFirstSingleDottedScalarProjectionReadsNestedValue() {
        Collection<String> selectPropNames = Arrays.asList("address.city");
        Bson filter = new Document("active", true);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, new Document("_id", new ObjectId()).append("address", new Document("city", "Paris")));

        StepVerifier.create(executor.findFirst(selectPropNames, filter, String.class)).expectNext("Paris").verifyComplete();
    }

    @Test
    public void testFindFirstSingleProjectionObjectRowTypeReturnsRawDocument() {
        Collection<String> selectPropNames = Arrays.asList("email");
        Bson filter = new Document("active", true);
        Document doc = new Document("_id", new ObjectId()).append("email", "a@b.c");

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        StepVerifier.create(executor.findFirst(selectPropNames, filter, Object.class)).expectNext(doc).verifyComplete();
    }

    @Test
    public void testFindFirstSingleScalarProjectionDoesNotFallBackToObjectId() {
        Collection<String> selectPropNames = Arrays.asList("name");
        Bson filter = new Document("active", true);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, new Document("_id", new ObjectId()));

        StepVerifier.create(executor.findFirst(selectPropNames, filter, String.class)).verifyComplete();
    }

    /**
     * Regression test for empty scalar projections in the private {@code toEntity(Class)}
     * mapper. Document.class keeps an empty document as a real result, but scalar row types
     * must still treat a field-less document as no projected value.
     *
     * <p>{@code toEntity(Class)} maps an empty Document to {@code null}. The reactive
     * pipeline uses {@code mapNotNull(...)} (not {@code map(...)}), so a {@code null}
     * mapped value completes the {@code Mono} empty — matching the documented "empty if
     * no match" contract and the synchronous sibling — rather than emitting a fabricated
     * {@code 0} or surfacing a {@link NullPointerException}.</p>
     */
    @Test
    public void testFindFirstTypedWithEmptyDocumentDoesNotProduceDefaultValue() {
        Bson filter = new Document("id", 999);
        Document emptyDoc = new Document();

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, emptyDoc);

        Mono<Integer> result = executor.findFirst(filter, Integer.class);

        // An empty document produces neither a bogus 0 (default value of Integer) nor an
        // error: mapNotNull completes the Mono empty.
        StepVerifier.create(result).verifyComplete();
    }

    /**
     * Companion positive test: a non-empty document is still mapped correctly after
     * adding the {@code N.isEmpty(doc)} guard to {@code toEntity(Class)}.
     */
    @Test
    public void testFindFirstTypedWithNonEmptyDocumentStillMaps() {
        Bson filter = new Document("id", 1);
        Document doc = new Document("value", 42);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Integer> result = executor.findFirst(filter, Integer.class);

        StepVerifier.create(result).expectNext(42).verifyComplete();
    }

    @Test
    public void testFindFirstWithAllParameters() {
        Collection<String> selectPropNames = Arrays.asList("name");
        Bson filter = new Document("active", true);
        Bson sort = new Document("createdAt", -1);
        Document doc = new Document("name", "test");

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.sort(sort)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Document> result = executor.findFirst(selectPropNames, filter, sort, Document.class);

        StepVerifier.create(result).expectNext(doc).verifyComplete();
    }

    // MongoDB document for Document.class and must not be treated as "no document".
    @Test
    public void testFindFirstWithEmptyDocumentReturnsDocument() {
        Collection<String> selectPropNames = Arrays.asList("name");
        Bson filter = new Document("active", true);
        Document emptyDoc = new Document();

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, emptyDoc);

        Mono<Document> result = executor.findFirst(selectPropNames, filter, Document.class);

        StepVerifier.create(result).expectNext(emptyDoc).verifyComplete();
    }

    @Test
    public void testFindFirstWithFilter() {
        Bson filter = new Document("name", "test");
        Document doc = new Document("name", "test");

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Document> result = executor.findFirst(filter);

        StepVerifier.create(result).expectNext(doc).verifyComplete();
    }

    @Test
    public void testFindFirstWithFilterAndRowType() {
        Bson filter = new Document("value", 123);
        Document doc = new Document("value", 123);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Integer> result = executor.findFirst(filter, Integer.class);

        StepVerifier.create(result).expectNext(123).verifyComplete();
    }

    @Test
    public void testFindFirstWithNullRowTypeThrowsIAE() {
        // Regression: the two terminal reactive findFirst overloads previously had no rowType guard, so a
        // null rowType silently produced an empty Mono instead of the eager IllegalArgumentException thrown
        // by the synchronous sibling (and by every other reactive read). The guard now throws at the call site.
        final Bson filter = new Document("_id", "x");

        // findFirst(Collection, Bson, Bson, Class) terminal (routes through query(...)).
        assertThrows(IllegalArgumentException.class, () -> executor.findFirst(Arrays.asList("name"), filter, (Bson) null, (Class<Document>) null));
        // findFirst(Bson, Bson, Bson, Class) terminal (routes through executeQuery(...)).
        assertThrows(IllegalArgumentException.class, () -> executor.findFirst((Bson) new Document(), filter, (Bson) null, (Class<Document>) null));
        // The delegating convenience overload inherits the guard.
        assertThrows(IllegalArgumentException.class, () -> executor.findFirst(filter, (Class<Document>) null));
    }

    @Test
    public void testFindFirstWithProjection() {
        Bson projection = Projections.include("name", "status");
        Bson filter = new Document("active", true);
        Bson sort = new Document("name", 1);
        Document doc = new Document("name", "test").append("status", "active");

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(projection)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.sort(sort)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Document> result = executor.findFirst(projection, filter, sort, Document.class);

        StepVerifier.create(result).expectNext(doc).verifyComplete();
    }

    @Test
    public void testFindFirstWithSelectPropNamesFilterAndRowType() {
        Collection<String> selectPropNames = Arrays.asList("name", "age");
        Bson filter = new Document("active", true);
        Document doc = new Document("name", "John").append("age", 30);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Document> result = executor.findFirst(selectPropNames, filter, Document.class);

        StepVerifier.create(result).expectNext(doc).verifyComplete();
    }

    @Test
    public void testGetWithInvalidObjectIdString() {
        String invalidObjectId = "invalid";
        assertThrows(IllegalArgumentException.class, () -> {
            executor.get(invalidObjectId);
        });
    }

    @Test
    public void testGetWithObjectId() {
        ObjectId objectId = new ObjectId();
        Document doc = new Document("_id", objectId).append("name", "test");

        when(mockCollection.find(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Document> result = executor.get(objectId);

        StepVerifier.create(result).expectNext(doc).verifyComplete();
    }

    @Test
    public void testGetWithObjectIdAndRowType() {
        ObjectId objectId = new ObjectId();
        Map<String, Object> data = new HashMap<>();
        data.put("name", "test");

        Document doc = new Document("_id", objectId).append("name", "test");

        when(mockCollection.find(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Map> result = executor.get(objectId, Map.class);

        StepVerifier.create(result).expectNextMatches(map -> map.get("name").equals("test")).verifyComplete();
    }

    @Test
    public void testGetWithObjectIdSelectPropNamesAndRowType() {
        ObjectId objectId = new ObjectId();
        Collection<String> selectPropNames = Arrays.asList("name");
        Document doc = new Document("name", "test");

        when(mockCollection.find(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Document> result = executor.get(objectId, selectPropNames, Document.class);

        StepVerifier.create(result).expectNext(doc).verifyComplete();
    }

    @Test
    public void testGetWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        Document doc = new Document("_id", new ObjectId(objectId)).append("name", "test");

        when(mockCollection.find(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Document> result = executor.get(objectId);

        StepVerifier.create(result).expectNext(doc).verifyComplete();
    }

    @Test
    public void testGetWithStringObjectIdAndRowType() {
        String objectId = "507f1f77bcf86cd799439011";
        Document doc = new Document("_id", new ObjectId(objectId)).append("value", "test");

        when(mockCollection.find(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<String> result = executor.get(objectId, String.class);

        StepVerifier.create(result).expectNext("test").verifyComplete();
    }

    @Test
    public void testGetWithStringObjectIdSelectPropNamesAndRowType() {
        String objectId = "507f1f77bcf86cd799439011";
        Collection<String> selectPropNames = Arrays.asList("name", "status");
        Document doc = new Document("name", "test").append("status", "active");

        when(mockCollection.find(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Document> result = executor.get(objectId, selectPropNames, Document.class);

        StepVerifier.create(result).expectNext(doc).verifyComplete();
    }

    // Regression: a field-less document in a multi-result stream is still a Document.class result.
    @Test
    public void testListIncludesEmptyDocumentForDocumentRowType() {
        Bson filter = new Document("active", true);
        int offset = 10;
        int count = 5;
        Document populated = new Document("name", "doc1");
        Document emptyDoc = new Document();

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.skip(offset)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(count)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, populated, emptyDoc);

        Flux<Document> result = executor.list(filter, offset, count, Document.class);

        StepVerifier.create(result).expectNext(populated).expectNext(emptyDoc).verifyComplete();
    }

    @Test
    public void testListWithAllParameters() {
        Collection<String> selectPropNames = Arrays.asList("name");
        Bson filter = new Document("active", true);
        Bson sort = new Document("createdAt", -1);
        int offset = 5;
        int count = 10;

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.sort(sort)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.skip(offset)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(count)).thenReturn(mockFindPublisher);

        Flux<Document> result = executor.list(selectPropNames, filter, sort, offset, count, Document.class);

        assertNotNull(result);
    }

    @Test
    public void testListWithDocumentRowTypeReturnsRawDocumentInstances() {
        // Regression: when the caller asks for Document, the raw documents must be returned untouched
        // (same instances), not rebuilt through readRow.
        Collection<String> selectPropNames = Arrays.asList("name");
        Bson filter = new Document("active", true);
        Bson sort = new Document("name", 1);
        int offset = 2;
        int count = 10;
        Document doc1 = new Document("name", "a").append("value", 1).append("extra", true);
        Document doc2 = new Document("name", "b").append("value", 2).append("extra", false);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.sort(sort)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.skip(offset)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(count)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc1, doc2);

        Flux<Document> result = executor.list(selectPropNames, filter, sort, offset, count, Document.class);

        StepVerifier.create(result).assertNext(d -> assertSame(doc1, d)).assertNext(d -> assertSame(doc2, d)).verifyComplete();
    }

    @Test
    public void testListWithEmptySelectPropNames() {
        Collection<String> selectPropNames = Collections.emptyList();
        Bson filter = new Document("active", true);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);

        Flux<Document> result = executor.list(selectPropNames, filter, Document.class);

        assertNotNull(result);
    }

    @Test
    public void testListWithFilter() {
        Bson filter = new Document("status", "active");
        List<Document> docs = Arrays.asList(new Document("name", "doc1"), new Document("name", "doc2"));

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, docs.get(0), docs.get(1));

        Flux<Document> result = executor.list(filter);

        StepVerifier.create(result).expectNext(docs.get(0)).expectNext(docs.get(1)).verifyComplete();
    }

    @Test
    public void testListWithFilterAndRowType() {
        Bson filter = new Document("type", "number");
        List<Document> docs = Arrays.asList(new Document("value", 1), new Document("value", 2));

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, docs.get(0), docs.get(1));

        Flux<Integer> result = executor.list(filter, Integer.class);

        StepVerifier.create(result).expectNext(1).expectNext(2).verifyComplete();
    }

    @Test
    public void testListWithFilterOffsetCountAndRowType() {
        Bson filter = new Document("active", true);
        int offset = 10;
        int count = 5;
        List<Document> docs = Arrays.asList(new Document("name", "doc1"), new Document("name", "doc2"));

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.skip(offset)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(count)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, docs.get(0), docs.get(1));

        Flux<Document> result = executor.list(filter, offset, count, Document.class);

        StepVerifier.create(result).expectNext(docs.get(0)).expectNext(docs.get(1)).verifyComplete();
    }

    @Test
    public void testListWithNegativeCount() {
        Bson filter = new Document("name", "test");
        assertThrows(IllegalArgumentException.class, () -> {
            executor.list(filter, 0, -1, Document.class);
        });
    }

    @Test
    public void testListWithNegativeOffset() {
        Bson filter = new Document("name", "test");
        assertThrows(IllegalArgumentException.class, () -> {
            executor.list(filter, -1, 10, Document.class);
        });
    }

    @Test
    public void testListWithNullRowTypeThrowsIAE() {
        final Bson filter = new Document("active", true);

        assertThrows(IllegalArgumentException.class, () -> executor.list(filter, (Class<Document>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.list(Arrays.asList("name"), filter, null, 0, 1, (Class<Document>) null));
        assertThrows(IllegalArgumentException.class, () -> executor.list((Bson) new Document("name", 1), filter, null, 0, 1, (Class<Document>) null));
    }

    @Test
    public void testListWithObjectRowTypeNoLongerThrowsForWideDocuments() {
        // Regression: list(..., Object.class) previously rebuilt each row through readRow, which threw
        // for documents with more than two fields. Object.class now short-circuits to the raw documents.
        Bson projection = Projections.include("a", "b", "c");
        Bson filter = new Document("active", true);
        Bson sort = new Document("a", 1);
        Document wideDoc = new Document("a", 1).append("b", 2).append("c", 3);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(projection)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.sort(sort)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(10)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, wideDoc);

        Flux<Object> result = executor.list(projection, filter, sort, 0, 10, Object.class);

        StepVerifier.create(result).assertNext(o -> assertSame(wideDoc, o)).verifyComplete();
    }

    @Test
    public void testListWithProjection() {
        Bson projection = Projections.include("name", "status");
        Bson filter = new Document("active", true);
        Bson sort = new Document("name", 1);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(projection)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.sort(sort)).thenReturn(mockFindPublisher);

        Flux<Document> result = executor.list(projection, filter, sort, Document.class);

        assertNotNull(result);
    }

    // Covers list(Bson projection, Bson filter, Bson sort, int offset, int count, Class) overload.
    @Test
    public void testListWithProjectionAndOffsetCount() {
        Bson projection = Projections.include("name");
        Bson filter = new Document("active", true);
        Bson sort = new Document("name", 1);
        int offset = 5;
        int count = 10;
        Document doc = new Document("name", "test");

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(projection)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.sort(sort)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.skip(offset)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(count)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Flux<Document> result = executor.list(projection, filter, sort, offset, count, Document.class);

        StepVerifier.create(result).expectNext(doc).verifyComplete();
    }

    @Test
    public void testListWithSelectPropNamesAsList() {
        List<String> selectPropNames = Arrays.asList("name", "value");
        Bson filter = new Document("active", true);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);

        Flux<Document> result = executor.list(selectPropNames, filter, Document.class);

        assertNotNull(result);
    }

    // by passing selectPropNames as a non-List Collection (Set).
    @Test
    public void testListWithSelectPropNamesAsNonListCollection() {
        Collection<String> selectPropNames = new java.util.LinkedHashSet<>(Arrays.asList("name", "value"));
        Bson filter = new Document("active", true);
        Document doc = new Document("name", "x").append("value", 1);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Flux<Document> result = executor.list(selectPropNames, filter, Document.class);

        StepVerifier.create(result).expectNext(doc).verifyComplete();
    }

    @Test
    public void testListWithSelectPropNamesFilterAndRowType() {
        Collection<String> selectPropNames = Arrays.asList("name", "email");
        Bson filter = new Document("active", true);
        List<Document> docs = Arrays.asList(new Document("name", "John").append("email", "john@example.com"),
                new Document("name", "Jane").append("email", "jane@example.com"));

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, docs.get(0), docs.get(1));

        Flux<Document> result = executor.list(selectPropNames, filter, Document.class);

        StepVerifier.create(result).expectNext(docs.get(0)).expectNext(docs.get(1)).verifyComplete();
    }

    // Cover list(Collection, Bson, Bson, Class) overload (with sort and projection collection).
    @Test
    public void testListWithSelectPropNamesFilterSortAndRowType() {
        Collection<String> selectPropNames = Arrays.asList("name", "score");
        Bson filter = new Document("active", true);
        Bson sort = new Document("score", -1);
        List<Document> docs = Arrays.asList(new Document("name", "a").append("score", 10), new Document("name", "b").append("score", 5));

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.sort(sort)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, docs.get(0), docs.get(1));

        Flux<Document> result = executor.list(selectPropNames, filter, sort, Document.class);

        StepVerifier.create(result).expectNext(docs.get(0)).expectNext(docs.get(1)).verifyComplete();
    }

    @Test
    public void testListWithZeroCountReturnsEmptyAndDoesNotUseLimitZero() {
        // Regression: limit(0) is treated by the MongoDB driver as "no limit" (return ALL docs).
        // A user-supplied count of 0 must yield zero results, not the whole collection.
        Bson filter = new Document("status", "active");
        Document alwaysFalse = new Document("$expr", false);
        when(mockCollection.find(eq(alwaysFalse))).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher);

        Flux<Document> result = executor.list(filter, 0, 0, Document.class);

        StepVerifier.create(result).verifyComplete();
        verify(mockCollection).find(eq(alwaysFalse));
        verify(mockFindPublisher, org.mockito.Mockito.never()).limit(0);
    }

    @Test
    public void testQueryDatasetDottedSelectNameThroughArrayFailsOnEveryOverload() {
        final Bson filter = new Document("status", "active");
        final Bson sort = new Document("_id", 1);
        stubFindChain(filter);
        stubEmits(mockFindPublisher, new Document("_id", 1).append("department", "eng").append("address", new Document("city", "Paris")),
                new Document("_id", 2).append("department", "ops").append("address", Arrays.asList(new Document("city", "Rome"))));
        final List<String> names = Arrays.asList("department", "address.city");

        StepVerifier.create(executor.query(names, filter, Map.class)).expectError(ClassCastException.class).verify();
        StepVerifier.create(executor.query(names, filter, 0, 5, Document.class)).expectError(ClassCastException.class).verify();
        StepVerifier.create(executor.query(names, filter, sort, GroupRow.class)).expectError(ClassCastException.class).verify();
        StepVerifier.create(executor.query(names, filter, sort, 1, 5, Map.class)).expectError(ClassCastException.class).verify();

        // Without a dotted name the same rows load as before: the array is just a column value.
        final Dataset plain = executor.query(Arrays.asList("department", "address"), filter, Map.class).block();
        assertEquals(Arrays.asList("eng", "ops"), plain.getColumn("department"));
        assertEquals(Arrays.asList(new Document("city", "Rome")), plain.getColumn("address").get(1));
    }

    @Test
    public void testQueryDatasetDottedSelectNamesOnEveryOverloadKeepOrderAndRawNestedValues() {
        // Every Collection-projection Dataset overload resolves dotted names from the nested documents (they were all-null
        // columns): requested order kept, null for a missing/null parent or leaf, raw nested value (Integer zip although
        // GroupRow declares no address), plain columns converted as before; an empty result still emits an empty Dataset.
        final Bson filter = new Document("status", "active");
        final Bson sort = new Document("_id", 1);
        stubFindChain(filter);
        stubEmits(mockFindPublisher, new Document("_id", 1).append("department", "eng").append("address", new Document("city", "Paris").append("zip", 75001)),
                new Document("_id", 2).append("department", "ops"),
                new Document("_id", 3).append("department", "hr").append("address", new Document("city", null)),
                new Document("_id", 4).append("department", "qa").append("address", null));
        final List<String> names = Arrays.asList("address.zip", "department", "address.city");

        for (final Class<?> rowType : Arrays.asList(Map.class, java.util.LinkedHashMap.class, Document.class, GroupRow.class)) {
            for (final Mono<Dataset> mono : Arrays.asList(executor.query(names, filter, rowType), executor.query(names, filter, 1, 3, rowType),
                    executor.query(names, filter, sort, rowType), executor.query(names, filter, sort, 1, 3, rowType))) {
                final Dataset ds = mono.block();
                assertEquals(names, ds.columnNames(), rowType.getName());
                assertEquals(Arrays.asList("eng", "ops", "hr", "qa"), ds.getColumn("department"), rowType.getName());
                assertEquals(Arrays.asList(75001, null, null, null), ds.getColumn("address.zip"), rowType.getName());
                assertEquals(Arrays.asList("Paris", null, null, null), ds.getColumn("address.city"), rowType.getName());
            }
        }

        final Dataset withId = executor.query(new java.util.LinkedHashSet<>(Arrays.asList("_id", "address.city")), filter, Map.class).block();
        assertEquals(Arrays.asList("_id", "address.city"), withId.columnNames());
        assertEquals(Arrays.asList(1, 2, 3, 4), withId.getColumn("_id"));

        stubEmits(mockFindPublisher);
        assertEquals(0, executor.query(names, filter, sort, 0, 10, Map.class).block().size());
    }

    // ({address: {city: ...}}) while Dataset columns are read by flat map key / bean property, so the column was all null.
    @Test
    public void testQueryDatasetResolvesDottedSelectNamesFromNestedDocuments_sliceF() {
        final Bson filter = new Document("status", "active");
        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, new Document("_id", 1).append("department", "eng").append("address", new Document("city", "Paris")),
                new Document("_id", 2).append("department", "ops"));

        final List<String> selectPropNames = Arrays.asList("department", "address.city");

        for (final Class<?> rowType : Arrays.asList(Map.class, Document.class, GroupRow.class)) {
            final Dataset dataset = executor.query(selectPropNames, filter, rowType).block();

            assertEquals(selectPropNames, dataset.columnNames());
            assertEquals(Arrays.asList("eng", "ops"), dataset.getColumn("department"));
            assertEquals(Arrays.asList("Paris", null), dataset.getColumn("address.city"));
        }

        // A dotted path through a non-Document value fails through the publisher, as for list/findFirst.
        stubEmits(mockFindPublisher, new Document("_id", 3).append("department", "x").append("address", Arrays.asList(new Document("city", "Rome"))));
        StepVerifier.create(executor.query(selectPropNames, filter, Map.class)).expectError(ClassCastException.class).verify();
    }

    @Test
    public void testQueryDatasetWithInvalidRowTypeThrowsIAESynchronously() {
        // Regression: an invalid rowType (not a bean / Map / Document) is rejected synchronously at the
        // call site (checkResultClass), mirroring the sync executor, instead of producing a malformed
        // Dataset on subscription.
        Collection<String> selectPropNames = Arrays.asList("name");
        Bson projection = Projections.include("name");
        Bson filter = new Document("active", true);
        Bson sort = new Document("name", 1);

        assertThrows(IllegalArgumentException.class, () -> executor.query(selectPropNames, filter, sort, 0, 10, String.class));
        assertThrows(IllegalArgumentException.class, () -> executor.query(projection, filter, sort, 0, 10, String.class));

        verify(mockCollection, org.mockito.Mockito.never()).find(any(Bson.class));
    }

    @Test
    public void testQueryWithAllParametersForDataset() {
        Collection<String> selectPropNames = Arrays.asList("name", "value");
        Bson filter = new Document("active", true);
        Bson sort = new Document("createdAt", -1);
        int offset = 10;
        int count = 20;

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.sort(sort)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.skip(offset)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(count)).thenReturn(mockFindPublisher);

        Mono<Dataset> result = executor.query(selectPropNames, filter, sort, offset, count, Document.class);

        assertNotNull(result);
    }

    @Test
    public void testQueryWithFilter() {
        Bson filter = new Document("status", "active");
        List<Document> docs = Arrays.asList(new Document("name", "doc1"), new Document("name", "doc2"));

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);

        Mono<Dataset> result = executor.query(filter);

        assertNotNull(result);
    }

    @Test
    public void testQueryWithFilterAndRowType() {
        Bson filter = new Document("type", "test");

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);

        Mono<Dataset> result = executor.query(filter, Document.class);

        assertNotNull(result);
    }

    // Covers query(Bson filter, int offset, int count, Class) -> Dataset overload.
    @Test
    public void testQueryWithFilterOffsetCountForDataset() {
        Bson filter = new Document("status", "active");
        int offset = 0;
        int count = 5;
        Document doc = new Document("name", "x").append("value", 1);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(count)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Dataset> result = executor.query(filter, offset, count, Document.class);

        StepVerifier.create(result).expectNextMatches(ds -> ds.size() == 1).verifyComplete();
    }

    @Test
    public void testQueryWithNegativeCount() {
        Bson filter = new Document("name", "test");
        assertThrows(IllegalArgumentException.class, () -> {
            executor.query(filter, 0, -1, Document.class);
        });
    }

    @Test
    public void testQueryWithNegativeOffset() {
        Bson filter = new Document("name", "test");
        assertThrows(IllegalArgumentException.class, () -> {
            executor.query(filter, -1, 10, Document.class);
        });
    }

    // Covers query(Bson projection, Bson filter, Bson sort, int offset, int count, Class) -> Dataset overload.
    @Test
    public void testQueryWithProjectionAllParametersForDataset() {
        Bson projection = Projections.include("name", "value");
        Bson filter = new Document("active", true);
        Bson sort = new Document("name", 1);
        int offset = 2;
        int count = 8;
        Document doc = new Document("name", "x").append("value", 1);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(projection)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.sort(sort)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.skip(offset)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(count)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Dataset> result = executor.query(projection, filter, sort, offset, count, Document.class);

        StepVerifier.create(result).expectNextMatches(ds -> ds.size() == 1).verifyComplete();
    }

    // Covers the empty-results path for query(Bson projection, ...) -> Dataset.
    @Test
    public void testQueryWithProjectionEmptyResults() {
        Bson projection = Projections.include("name");
        Bson filter = new Document("active", true);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(projection)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher);

        Mono<Dataset> result = executor.query(projection, filter, null, Document.class);

        StepVerifier.create(result).expectNextMatches(ds -> ds.size() == 0).verifyComplete();
    }

    // Covers query(Bson projection, Bson filter, Bson sort, Class) -> Dataset overload.
    @Test
    public void testQueryWithProjectionFilterSortForDataset() {
        Bson projection = Projections.include("name");
        Bson filter = new Document("active", true);
        Bson sort = new Document("name", 1);
        Document doc = new Document("name", "x");

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(projection)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.sort(sort)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Dataset> result = executor.query(projection, filter, sort, Document.class);

        StepVerifier.create(result).expectNextMatches(ds -> ds.size() == 1).verifyComplete();
    }

    // Covers query(Collection, Bson, Class) -> Dataset overload.
    @Test
    public void testQueryWithSelectPropNamesAndFilterForDataset() {
        Collection<String> selectPropNames = Arrays.asList("name", "value");
        Bson filter = new Document("active", true);
        Document doc = new Document("name", "x").append("value", 1);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Dataset> result = executor.query(selectPropNames, filter, Document.class);

        StepVerifier.create(result).expectNextMatches(ds -> ds.size() == 1).verifyComplete();
    }

    // Covers the empty-results path: extractData on an empty rowList yields an empty Dataset.
    @Test
    public void testQueryWithSelectPropNamesEmptyResults() {
        Collection<String> selectPropNames = Arrays.asList("name");
        Bson filter = new Document("active", true);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher);

        Mono<Dataset> result = executor.query(selectPropNames, filter, Document.class);

        StepVerifier.create(result).expectNextMatches(ds -> ds.size() == 0).verifyComplete();
    }

    // Covers query(Collection, Bson, int, int, Class) -> Dataset overload.
    @Test
    public void testQueryWithSelectPropNamesFilterOffsetCountForDataset() {
        Collection<String> selectPropNames = Arrays.asList("name");
        Bson filter = new Document("active", true);
        int offset = 0;
        int count = 10;
        Document doc = new Document("name", "x");

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(count)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Dataset> result = executor.query(selectPropNames, filter, offset, count, Document.class);

        StepVerifier.create(result).expectNextMatches(ds -> ds.size() == 1).verifyComplete();
    }

    // Covers query(Collection, Bson, Bson, Class) -> Dataset overload.
    @Test
    public void testQueryWithSelectPropNamesFilterSortForDataset() {
        Collection<String> selectPropNames = Arrays.asList("name");
        Bson filter = new Document("active", true);
        Bson sort = new Document("name", 1);
        Document doc = new Document("name", "x");

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.sort(sort)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Dataset> result = executor.query(selectPropNames, filter, sort, Document.class);

        StepVerifier.create(result).expectNextMatches(ds -> ds.size() == 1).verifyComplete();
    }

    // uses an always-false {$expr: false} filter instead of limit(0).
    @Test
    public void testQueryWithZeroCount_ReturnsEmpty() {
        Bson filter = new Document("active", true);
        Bson sort = new Document("name", 1);

        when(mockCollection.find(any(Document.class))).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher);

        Mono<Dataset> result = executor.query(Projections.include("name"), filter, sort, 0, 0, Document.class);

        StepVerifier.create(result).expectNextMatches(ds -> ds.size() == 0).verifyComplete();
    }

    // ---- 2026-10-20 sliceQ (reactive twin of the blocking regression test) ----

    @Test
    public void testQueryDottedSelectNamesResultStaysMutableAndKeepsDeepPathsAndPlainColumns() {
        // Dotted columns are overwritten in place on the extracted Dataset instead of rebuilding (copying) every column:
        // the result must keep its values, column order, and mutability, and multi-level paths must still resolve.
        final Bson filter = new Document("status", "active");
        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher,
                new Document("_id", 1).append("name", "n1")
                        .append("a", new Document("b", new Document("c", 7)))
                        .append("address", new Document("city", "Paris")),
                new Document("_id", 2).append("name", "n2").append("a", new Document("b", new Document())));

        final Dataset ds = executor.query(Arrays.asList("a.b.c", "name", "address.city"), filter, Map.class).block();

        assertEquals(Arrays.asList("a.b.c", "name", "address.city"), ds.columnNames());
        assertEquals(2, ds.size());
        assertEquals(Arrays.asList(7, null), ds.getColumn("a.b.c"));
        assertEquals(Arrays.asList("n1", "n2"), ds.getColumn("name"));
        assertEquals(Arrays.asList("Paris", null), ds.getColumn("address.city"));
        assertEquals(false, ds.isFrozen());

        // "a.b" is the scalar 8 here, so traversing to "c" is not a Document path: signalled through the publisher.
        stubEmits(mockFindPublisher, new Document("_id", 1).append("a", new Document("b", 8)));
        StepVerifier.create(executor.query(Arrays.asList("a.b.c"), filter, Map.class)).expectError(ClassCastException.class).verify();

        stubEmits(mockFindPublisher, new Document("_id", 1).append("name", "n1").append("a", new Document("b", new Document("c", 7))));
        final Dataset single = executor.query(Arrays.asList("a.b.c", "name"), filter, Map.class).block();
        assertEquals(Arrays.asList(7), single.getColumn("a.b.c"));

        single.set(0, single.getColumnIndex("a.b.c"), 9);
        single.set(0, single.getColumnIndex("name"), "renamed");
        assertEquals(Arrays.asList(9), single.getColumn("a.b.c"));
        assertEquals(Arrays.asList("renamed"), single.getColumn("name"));
    }

    // ---- 2026-10-24 sliceU ----

    @Test
    public void testQueryDatasetKeepsSelectNamesAsOfTheCallWhenTheCallerChangesThemBeforeSubscribing() {
        // The projection is built when query(...) is called, but the Dataset columns were read from the caller's collection on
        // each subscription: changing the collection in between gave columns that no longer matched the projected fields.
        final Bson filter = new Document("status", "active");
        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, new Document("_id", 1).append("department", "eng").append("address", new Document("city", "Paris")),
                new Document("_id", 2).append("department", "ops"));

        final List<String> dottedNames = new java.util.ArrayList<>(Arrays.asList("department", "address.city"));
        final Mono<Dataset> dotted = executor.query(dottedNames, filter, Map.class);
        dottedNames.set(1, "address.zip");
        dottedNames.add("extra");

        final java.util.Set<String> plainNames = new java.util.LinkedHashSet<>(Arrays.asList("department"));
        final Mono<Dataset> plain = executor.query(plainNames, filter, GroupRow.class);
        plainNames.add("status");

        final Dataset dottedResult = dotted.block();
        final Dataset dottedAgain = dotted.block();
        final Dataset plainResult = plain.block();

        org.junit.jupiter.api.Assertions.assertAll(() -> assertEquals(Arrays.asList("department", "address.city"), dottedResult.columnNames()),
                () -> assertEquals(Arrays.asList("Paris", null), dottedResult.getColumn("address.city")),
                () -> assertEquals(Arrays.asList("department", "address.city"), dottedAgain.columnNames()),
                () -> assertEquals(Arrays.asList("Paris", null), dottedAgain.getColumn("address.city")),
                () -> assertEquals(Arrays.asList("department"), plainResult.columnNames()),
                () -> assertEquals(Arrays.asList("eng", "ops"), plainResult.getColumn("department")));
    }
}
