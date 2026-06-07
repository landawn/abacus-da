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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.util.Dataset;
import com.mongodb.BasicDBObject;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.DistinctPublisher;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MapReducePublisher;
import com.mongodb.reactivestreams.client.MongoCollection;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
public class MongoCollectionExecutorTest extends TestBase {

    @Mock
    private MongoCollection<Document> mockCollection;

    @Mock
    private FindPublisher<Document> mockFindPublisher;

    @Mock
    private AggregatePublisher<Document> mockAggregatePublisher;

    @Mock
    private DistinctPublisher<String> mockDistinctPublisher;

    @Mock
    private MapReducePublisher<Document> mockMapReducePublisher;

    @Mock
    private ChangeStreamPublisher<Document> mockChangeStreamPublisher;

    private MongoCollectionExecutor executor;

    @BeforeEach
    public void setUp() {
        executor = new MongoCollectionExecutor(mockCollection);
    }

    @Test
    public void testColl() {
        MongoCollection<Document> result = executor.coll();
        assertSame(mockCollection, result);
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
    public void testExistsWithObjectId() {
        ObjectId objectId = new ObjectId();
        Publisher<Long> countPublisher = Mono.just(0L);
        when(mockCollection.countDocuments(any(Bson.class), any(CountOptions.class))).thenReturn(countPublisher);

        Mono<Boolean> result = executor.exists(objectId);

        StepVerifier.create(result).expectNext(false).verifyComplete();
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
    public void testCountWithFilterAndOptions() {
        Bson filter = new Document("status", "active");
        CountOptions options = new CountOptions().limit(10);
        Publisher<Long> countPublisher = Mono.just(10L);
        when(mockCollection.countDocuments(filter, options)).thenReturn(countPublisher);

        Mono<Long> result = executor.count(filter, options);

        StepVerifier.create(result).expectNext(10L).verifyComplete();
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
    public void testEstimatedDocumentCount() {
        Publisher<Long> countPublisher = Mono.just(1000L);
        when(mockCollection.estimatedDocumentCount()).thenReturn(countPublisher);

        Mono<Long> result = executor.estimatedDocumentCount();

        StepVerifier.create(result).expectNext(1000L).verifyComplete();
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

    // Regression: an empty document (e.g. from an all-excluding projection) is still a matching
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

    @Test
    public void testQueryForBoolean() {
        String propName = "isActive";
        Bson filter = new Document("id", 1);
        Document doc = new Document(propName, true);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Boolean> result = executor.queryForBoolean(propName, filter);

        StepVerifier.create(result).expectNext(true).verifyComplete();
    }

    @Test
    public void testQueryForChar() {
        String propName = "grade";
        Bson filter = new Document("id", 1);
        Document doc = new Document(propName, 'A');

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Character> result = executor.queryForChar(propName, filter);

        StepVerifier.create(result).expectNext('A').verifyComplete();
    }

    @Test
    public void testQueryForByte() {
        String propName = "level";
        Bson filter = new Document("id", 1);
        Document doc = new Document(propName, (byte) 5);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Byte> result = executor.queryForByte(propName, filter);

        StepVerifier.create(result).expectNext((byte) 5).verifyComplete();
    }

    @Test
    public void testQueryForShort() {
        String propName = "count";
        Bson filter = new Document("id", 1);
        Document doc = new Document(propName, (short) 100);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Short> result = executor.queryForShort(propName, filter);

        StepVerifier.create(result).expectNext((short) 100).verifyComplete();
    }

    @Test
    public void testQueryForInt() {
        String propName = "age";
        Bson filter = new Document("id", 1);
        Document doc = new Document(propName, 25);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Integer> result = executor.queryForInt(propName, filter);

        StepVerifier.create(result).expectNext(25).verifyComplete();
    }

    @Test
    public void testQueryForLong() {
        String propName = "timestamp";
        Bson filter = new Document("id", 1);
        Document doc = new Document(propName, 1234567890L);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Long> result = executor.queryForLong(propName, filter);

        StepVerifier.create(result).expectNext(1234567890L).verifyComplete();
    }

    @Test
    public void testQueryForFloat() {
        String propName = "score";
        Bson filter = new Document("id", 1);
        Document doc = new Document(propName, 98.5f);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Float> result = executor.queryForFloat(propName, filter);

        StepVerifier.create(result).expectNext(98.5f).verifyComplete();
    }

    @Test
    public void testQueryForDouble() {
        String propName = "price";
        Bson filter = new Document("id", 1);
        Document doc = new Document(propName, 19.99);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Double> result = executor.queryForDouble(propName, filter);

        StepVerifier.create(result).expectNext(19.99).verifyComplete();
    }

    @Test
    public void testQueryForString() {
        String propName = "name";
        Bson filter = new Document("id", 1);
        Document doc = new Document(propName, "John Doe");

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<String> result = executor.queryForString(propName, filter);

        StepVerifier.create(result).expectNext("John Doe").verifyComplete();
    }

    @Test
    public void testQueryForDate() {
        String propName = "createdAt";
        Bson filter = new Document("id", 1);
        Date date = new Date();
        Document doc = new Document(propName, date);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Date> result = executor.queryForDate(propName, filter);

        StepVerifier.create(result).expectNext(date).verifyComplete();
    }

    @Test
    public void testQueryForDateWithRowType() {
        String propName = "timestamp";
        Bson filter = new Document("id", 1);
        Date date = new Date();
        Document doc = new Document(propName, date);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Date> result = executor.queryForDate(propName, filter, Date.class);

        StepVerifier.create(result).expectNext(date).verifyComplete();
    }

    @Test
    public void testqueryForSingleValue() {
        String propName = "value";
        Bson filter = new Document("id", 1);
        Document doc = new Document(propName, 42);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<Integer> result = executor.queryForSingleValue(propName, filter, Integer.class);

        StepVerifier.create(result).expectNext(42).verifyComplete();
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
    public void testWatch() {
        when(mockCollection.watch()).thenReturn(mockChangeStreamPublisher);

        ChangeStreamPublisher<Document> result = executor.watch();

        assertSame(mockChangeStreamPublisher, result);
    }

    @Test
    public void testWatchWithRowType() {
        ChangeStreamPublisher<String> mockStringPublisher = mock(ChangeStreamPublisher.class);
        when(mockCollection.watch(String.class)).thenReturn(mockStringPublisher);

        ChangeStreamPublisher<String> result = executor.watch(String.class);

        assertSame(mockStringPublisher, result);
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
    public void testInsertOne() {
        Document doc = new Document("name", "test");
        InsertOneResult insertResult = mock(InsertOneResult.class);
        Publisher<InsertOneResult> publisher = Mono.just(insertResult);
        when(mockCollection.insertOne(doc)).thenReturn(publisher);

        Mono<InsertOneResult> result = executor.insertOne(doc);

        StepVerifier.create(result).expectNext(insertResult).verifyComplete();
    }

    @Test
    public void testInsertOneWithOptions() {
        Document doc = new Document("name", "test");
        InsertOneOptions options = new InsertOneOptions();
        InsertOneResult insertResult = mock(InsertOneResult.class);
        Publisher<InsertOneResult> publisher = Mono.just(insertResult);
        when(mockCollection.insertOne(doc, options)).thenReturn(publisher);

        Mono<InsertOneResult> result = executor.insertOne(doc, options);

        StepVerifier.create(result).expectNext(insertResult).verifyComplete();
    }

    @Test
    public void testInsertMany() {
        List<Document> docs = Arrays.asList(new Document("name", "doc1"), new Document("name", "doc2"));
        InsertManyResult insertResult = mock(InsertManyResult.class);
        Publisher<InsertManyResult> publisher = Mono.just(insertResult);
        when(mockCollection.insertMany(docs)).thenReturn(publisher);

        Mono<InsertManyResult> result = executor.insertMany(docs);

        StepVerifier.create(result).expectNext(insertResult).verifyComplete();
    }

    @Test
    public void testInsertManyWithOptions() {
        List<Document> docs = Arrays.asList(new Document("name", "doc1"), new Document("name", "doc2"));
        InsertManyOptions options = new InsertManyOptions();
        InsertManyResult insertResult = mock(InsertManyResult.class);
        Publisher<InsertManyResult> publisher = Mono.just(insertResult);
        when(mockCollection.insertMany(docs, options)).thenReturn(publisher);

        Mono<InsertManyResult> result = executor.insertMany(docs, options);

        StepVerifier.create(result).expectNext(insertResult).verifyComplete();
    }

    @Test
    public void testUpdateOneWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        Document update = new Document("status", "updated");
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateOne(any(Bson.class), any(Bson.class))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateOne(objectId, update);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testUpdateOneWithObjectId() {
        ObjectId objectId = new ObjectId();
        Document update = new Document("status", "updated");
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateOne(any(Bson.class), any(Bson.class))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateOne(objectId, update);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testUpdateOneWithFilter() {
        Bson filter = new Document("name", "test");
        Document update = new Document("status", "updated");
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateOne(eq(filter), any(Bson.class))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateOne(filter, update);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testUpdateOneWithFilterAndOptions() {
        Bson filter = new Document("name", "test");
        Document update = new Document("status", "updated");
        UpdateOptions options = new UpdateOptions();
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateOne(eq(filter), any(Bson.class), eq(options))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateOne(filter, update, options);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testUpdateOneWithFilterAndList() {
        Bson filter = new Document("name", "test");
        List<Bson> updates = Arrays.asList(new Document("$set", new Document("status", "active")), new Document("$inc", new Document("count", 1)));
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateOne(eq(filter), eq(updates))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateOne(filter, updates);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testUpdateOneWithFilterListAndOptions() {
        Bson filter = new Document("name", "test");
        List<Bson> updates = Arrays.asList(new Document("$set", new Document("status", "active")));
        UpdateOptions options = new UpdateOptions();
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateOne(eq(filter), eq(updates), eq(options))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateOne(filter, updates, options);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testUpdateMany() {
        Bson filter = new Document("status", "pending");
        Document update = new Document("status", "processed");
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateMany(eq(filter), any(Bson.class))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateMany(filter, update);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testUpdateManyWithOptions() {
        Bson filter = new Document("status", "pending");
        Document update = new Document("status", "processed");
        UpdateOptions options = new UpdateOptions();
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateMany(eq(filter), any(Bson.class), eq(options))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateMany(filter, update, options);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testUpdateManyWithList() {
        Bson filter = new Document("status", "pending");
        List<Bson> updates = Arrays.asList(new Document("$set", new Document("status", "processed")));
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateMany(eq(filter), eq(updates))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateMany(filter, updates);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testUpdateManyWithListAndOptions() {
        Bson filter = new Document("status", "pending");
        List<Bson> updates = Arrays.asList(new Document("$set", new Document("status", "processed")));
        UpdateOptions options = new UpdateOptions();
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateMany(eq(filter), eq(updates), eq(options))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateMany(filter, updates, options);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testReplaceOneWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        Document replacement = new Document("name", "new name").append("status", "active");
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.replaceOne(any(Bson.class), eq(replacement))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.replaceOne(objectId, replacement);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testReplaceOneWithObjectId() {
        ObjectId objectId = new ObjectId();
        Document replacement = new Document("name", "new name").append("status", "active");
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.replaceOne(any(Bson.class), eq(replacement))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.replaceOne(objectId, replacement);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testReplaceOneWithFilter() {
        Bson filter = new Document("name", "old name");
        Document replacement = new Document("name", "new name").append("status", "active");
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.replaceOne(eq(filter), eq(replacement))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.replaceOne(filter, replacement);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testReplaceOneWithFilterAndOptions() {
        Bson filter = new Document("name", "old name");
        Document replacement = new Document("name", "new name").append("status", "active");
        ReplaceOptions options = new ReplaceOptions();
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.replaceOne(eq(filter), eq(replacement), eq(options))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.replaceOne(filter, replacement, options);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testDeleteOneWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        DeleteResult deleteResult = mock(DeleteResult.class);
        Publisher<DeleteResult> publisher = Mono.just(deleteResult);
        when(mockCollection.deleteOne(any(Bson.class))).thenReturn(publisher);

        Mono<DeleteResult> result = executor.deleteOne(objectId);

        StepVerifier.create(result).expectNext(deleteResult).verifyComplete();
    }

    @Test
    public void testDeleteOneWithObjectId() {
        ObjectId objectId = new ObjectId();
        DeleteResult deleteResult = mock(DeleteResult.class);
        Publisher<DeleteResult> publisher = Mono.just(deleteResult);
        when(mockCollection.deleteOne(any(Bson.class))).thenReturn(publisher);

        Mono<DeleteResult> result = executor.deleteOne(objectId);

        StepVerifier.create(result).expectNext(deleteResult).verifyComplete();
    }

    @Test
    public void testDeleteOneWithFilter() {
        Bson filter = new Document("name", "test");
        DeleteResult deleteResult = mock(DeleteResult.class);
        Publisher<DeleteResult> publisher = Mono.just(deleteResult);
        when(mockCollection.deleteOne(filter)).thenReturn(publisher);

        Mono<DeleteResult> result = executor.deleteOne(filter);

        StepVerifier.create(result).expectNext(deleteResult).verifyComplete();
    }

    @Test
    public void testDeleteOneWithFilterAndOptions() {
        Bson filter = new Document("name", "test");
        DeleteOptions options = new DeleteOptions();
        DeleteResult deleteResult = mock(DeleteResult.class);
        Publisher<DeleteResult> publisher = Mono.just(deleteResult);
        when(mockCollection.deleteOne(filter, options)).thenReturn(publisher);

        Mono<DeleteResult> result = executor.deleteOne(filter, options);

        StepVerifier.create(result).expectNext(deleteResult).verifyComplete();
    }

    @Test
    public void testDeleteOneWithNullOptionsUsesDefaultOverload() {
        Bson filter = new Document("name", "test");
        DeleteResult deleteResult = mock(DeleteResult.class);
        Publisher<DeleteResult> publisher = Mono.just(deleteResult);
        when(mockCollection.deleteOne(filter)).thenReturn(publisher);

        Mono<DeleteResult> result = executor.deleteOne(filter, null);

        StepVerifier.create(result).expectNext(deleteResult).verifyComplete();
        verify(mockCollection).deleteOne(filter);
    }

    @Test
    public void testDeleteMany() {
        Bson filter = new Document("status", "deleted");
        DeleteResult deleteResult = mock(DeleteResult.class);
        Publisher<DeleteResult> publisher = Mono.just(deleteResult);
        when(mockCollection.deleteMany(filter)).thenReturn(publisher);

        Mono<DeleteResult> result = executor.deleteMany(filter);

        StepVerifier.create(result).expectNext(deleteResult).verifyComplete();
    }

    @Test
    public void testDeleteManyWithOptions() {
        Bson filter = new Document("status", "deleted");
        DeleteOptions options = new DeleteOptions();
        DeleteResult deleteResult = mock(DeleteResult.class);
        Publisher<DeleteResult> publisher = Mono.just(deleteResult);
        when(mockCollection.deleteMany(filter, options)).thenReturn(publisher);

        Mono<DeleteResult> result = executor.deleteMany(filter, options);

        StepVerifier.create(result).expectNext(deleteResult).verifyComplete();
    }

    @Test
    public void testDeleteManyWithNullOptionsUsesDefaultOverload() {
        Bson filter = new Document("status", "deleted");
        DeleteResult deleteResult = mock(DeleteResult.class);
        Publisher<DeleteResult> publisher = Mono.just(deleteResult);
        when(mockCollection.deleteMany(filter)).thenReturn(publisher);

        Mono<DeleteResult> result = executor.deleteMany(filter, null);

        StepVerifier.create(result).expectNext(deleteResult).verifyComplete();
        verify(mockCollection).deleteMany(filter);
    }

    @Test
    public void testBulkInsert() {
        List<Document> entities = Arrays.asList(new Document("name", "doc1"), new Document("name", "doc2"));
        BulkWriteResult bulkResult = mock(BulkWriteResult.class);
        when(bulkResult.getInsertedCount()).thenReturn(2);
        Publisher<BulkWriteResult> publisher = Mono.just(bulkResult);
        when(mockCollection.bulkWrite(anyList())).thenReturn(publisher);

        Mono<Integer> result = executor.bulkInsert(entities);

        StepVerifier.create(result).expectNext(2).verifyComplete();
    }

    @Test
    public void testBulkInsertWithOptions() {
        List<Document> entities = Arrays.asList(new Document("name", "doc1"), new Document("name", "doc2"));
        BulkWriteOptions options = new BulkWriteOptions();
        BulkWriteResult bulkResult = mock(BulkWriteResult.class);
        when(bulkResult.getInsertedCount()).thenReturn(2);
        Publisher<BulkWriteResult> publisher = Mono.just(bulkResult);
        when(mockCollection.bulkWrite(anyList(), eq(options))).thenReturn(publisher);

        Mono<Integer> result = executor.bulkInsert(entities, options);

        StepVerifier.create(result).expectNext(2).verifyComplete();
    }

    @Test
    public void testBulkInsertRejectsNullEntities() {
        assertThrows(IllegalArgumentException.class, () -> executor.bulkInsert(null));
    }

    @Test
    public void testBulkInsertRejectsEmptyEntities() {
        assertThrows(IllegalArgumentException.class, () -> executor.bulkInsert(List.of()));
    }

    @Test
    public void testBulkWrite() {
        List<WriteModel<Document>> requests = Arrays.asList(new InsertOneModel<>(new Document("name", "doc1")));
        BulkWriteResult bulkResult = mock(BulkWriteResult.class);
        Publisher<BulkWriteResult> publisher = Mono.just(bulkResult);
        when(mockCollection.bulkWrite(requests)).thenReturn(publisher);

        Mono<BulkWriteResult> result = executor.bulkWrite(requests);

        StepVerifier.create(result).expectNext(bulkResult).verifyComplete();
    }

    @Test
    public void testBulkWriteWithOptions() {
        List<WriteModel<Document>> requests = Arrays.asList(new InsertOneModel<>(new Document("name", "doc1")));
        BulkWriteOptions options = new BulkWriteOptions();
        BulkWriteResult bulkResult = mock(BulkWriteResult.class);
        Publisher<BulkWriteResult> publisher = Mono.just(bulkResult);
        when(mockCollection.bulkWrite(requests, options)).thenReturn(publisher);

        Mono<BulkWriteResult> result = executor.bulkWrite(requests, options);

        StepVerifier.create(result).expectNext(bulkResult).verifyComplete();
    }

    @Test
    public void testBulkWriteRejectsNullRequests() {
        assertThrows(IllegalArgumentException.class, () -> executor.bulkWrite(null));
    }

    @Test
    public void testBulkWriteRejectsEmptyRequests() {
        assertThrows(IllegalArgumentException.class, () -> executor.bulkWrite(List.of()));
    }

    @Test
    public void testFindOneAndUpdateWithFilter() {
        Bson filter = new Document("name", "test");
        Document update = new Document("status", "updated");
        Document resultDoc = new Document("name", "test").append("status", "updated");
        Publisher<Document> publisher = Mono.just(resultDoc);
        when(mockCollection.findOneAndUpdate(eq(filter), any(Bson.class))).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndUpdate(filter, update);

        StepVerifier.create(result).expectNext(resultDoc).verifyComplete();
    }

    @Test
    public void testFindOneAndUpdateWithFilterAndRowType() {
        Bson filter = new Document("name", "test");
        Document update = new Document("value", 123);
        Document resultDoc = new Document("value", 123);
        Publisher<Document> publisher = Mono.just(resultDoc);
        when(mockCollection.findOneAndUpdate(eq(filter), any(Bson.class))).thenReturn(publisher);

        Mono<Integer> result = executor.findOneAndUpdate(filter, update, Integer.class);

        StepVerifier.create(result).expectNext(123).verifyComplete();
    }

    @Test
    public void testFindOneAndUpdateWithFilterUpdateAndOptions() {
        Bson filter = new Document("name", "test");
        Document update = new Document("status", "updated");
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        Document resultDoc = new Document("name", "test").append("status", "updated");
        Publisher<Document> publisher = Mono.just(resultDoc);
        when(mockCollection.findOneAndUpdate(eq(filter), any(Bson.class), eq(options))).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndUpdate(filter, update, options);

        StepVerifier.create(result).expectNext(resultDoc).verifyComplete();
    }

    @Test
    public void testFindOneAndUpdateWithAllParameters() {
        Bson filter = new Document("name", "test");
        Document update = new Document("status", "updated");
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        Document resultDoc = new Document("name", "test").append("status", "updated");
        Publisher<Document> publisher = Mono.just(resultDoc);
        when(mockCollection.findOneAndUpdate(eq(filter), any(Bson.class), eq(options))).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndUpdate(filter, update, options, Document.class);

        StepVerifier.create(result).expectNext(resultDoc).verifyComplete();
    }

    @Test
    public void testFindOneAndUpdateWithFilterAndList() {
        Bson filter = new Document("name", "test");
        List<Bson> updates = Arrays.asList(new Document("$set", new Document("status", "active")));
        Document resultDoc = new Document("name", "test").append("status", "active");
        Publisher<Document> publisher = Mono.just(resultDoc);
        when(mockCollection.findOneAndUpdate(eq(filter), eq(updates))).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndUpdate(filter, updates);

        StepVerifier.create(result).expectNext(resultDoc).verifyComplete();
    }

    @Test
    public void testFindOneAndUpdateWithFilterAndListRejectsNullFilter() {
        List<Bson> updates = Arrays.asList(new Document("$set", new Document("status", "active")));

        assertThrows(IllegalArgumentException.class, () -> executor.findOneAndUpdate(null, updates));
    }

    @Test
    public void testFindOneAndUpdateWithFilterAndListRejectsEmptyUpdates() {
        Bson filter = new Document("name", "test");

        assertThrows(IllegalArgumentException.class, () -> executor.findOneAndUpdate(filter, List.of()));
    }

    @Test
    public void testFindOneAndUpdateWithFilterListAndRowType() {
        Bson filter = new Document("name", "test");
        List<Bson> updates = Arrays.asList(new Document("$set", new Document("status", "active")));
        Document resultDoc = new Document("name", "test").append("status", "active");
        Publisher<Document> publisher = Mono.just(resultDoc);
        when(mockCollection.findOneAndUpdate(eq(filter), eq(updates))).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndUpdate(filter, updates, Document.class);

        StepVerifier.create(result).expectNext(resultDoc).verifyComplete();
    }

    @Test
    public void testFindOneAndUpdateWithFilterListAndOptions() {
        Bson filter = new Document("name", "test");
        List<Bson> updates = Arrays.asList(new Document("$set", new Document("status", "active")));
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        Document resultDoc = new Document("name", "test").append("status", "active");
        Publisher<Document> publisher = Mono.just(resultDoc);
        when(mockCollection.findOneAndUpdate(eq(filter), eq(updates), eq(options))).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndUpdate(filter, updates, options);

        StepVerifier.create(result).expectNext(resultDoc).verifyComplete();
    }

    @Test
    public void testFindOneAndUpdateWithFilterListOptionsAndRowType() {
        Bson filter = new Document("name", "test");
        List<Bson> updates = Arrays.asList(new Document("$set", new Document("status", "active")));
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        Document resultDoc = new Document("name", "test").append("status", "active");
        Publisher<Document> publisher = Mono.just(resultDoc);
        when(mockCollection.findOneAndUpdate(eq(filter), eq(updates), eq(options))).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndUpdate(filter, updates, options, Document.class);

        StepVerifier.create(result).expectNext(resultDoc).verifyComplete();
    }

    @Test
    public void testFindOneAndReplace() {
        Bson filter = new Document("name", "oldName");
        Document replacement = new Document("name", "newName");
        Publisher<Document> publisher = Mono.just(replacement);
        when(mockCollection.findOneAndReplace(filter, replacement)).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndReplace(filter, replacement);

        StepVerifier.create(result).expectNext(replacement).verifyComplete();
    }

    @Test
    public void testFindOneAndReplaceWithRowType() {
        Bson filter = new Document("name", "oldName");
        Document replacement = new Document("name", "newName");
        Publisher<Document> publisher = Mono.just(replacement);
        when(mockCollection.findOneAndReplace(filter, replacement)).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndReplace(filter, replacement, Document.class);

        StepVerifier.create(result).expectNext(replacement).verifyComplete();
    }

    @Test
    public void testFindOneAndReplaceWithOptions() {
        Bson filter = new Document("name", "oldName");
        Document replacement = new Document("name", "newName");
        FindOneAndReplaceOptions options = new FindOneAndReplaceOptions();
        Publisher<Document> publisher = Mono.just(replacement);
        when(mockCollection.findOneAndReplace(filter, replacement, options)).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndReplace(filter, replacement, options);

        StepVerifier.create(result).expectNext(replacement).verifyComplete();
    }

    @Test
    public void testFindOneAndReplaceWithOptionsAndRowType() {
        Bson filter = new Document("name", "oldName");
        Document replacement = new Document("name", "newName");
        FindOneAndReplaceOptions options = new FindOneAndReplaceOptions();
        Publisher<Document> publisher = Mono.just(replacement);
        when(mockCollection.findOneAndReplace(filter, replacement, options)).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndReplace(filter, replacement, options, Document.class);

        StepVerifier.create(result).expectNext(replacement).verifyComplete();
    }

    @Test
    public void testFindOneAndDelete() {
        Bson filter = new Document("name", "test");
        Document deletedDoc = new Document("name", "test").append("_id", new ObjectId());
        Publisher<Document> publisher = Mono.just(deletedDoc);
        when(mockCollection.findOneAndDelete(filter)).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndDelete(filter);

        StepVerifier.create(result).expectNext(deletedDoc).verifyComplete();
    }

    @Test
    public void testFindOneAndDeleteWithRowType() {
        Bson filter = new Document("value", 123);
        Document deletedDoc = new Document("value", 123);
        Publisher<Document> publisher = Mono.just(deletedDoc);
        when(mockCollection.findOneAndDelete(filter)).thenReturn(publisher);

        Mono<Integer> result = executor.findOneAndDelete(filter, Integer.class);

        StepVerifier.create(result).expectNext(123).verifyComplete();
    }

    @Test
    public void testFindOneAndDeleteWithOptions() {
        Bson filter = new Document("name", "test");
        FindOneAndDeleteOptions options = new FindOneAndDeleteOptions();
        Document deletedDoc = new Document("name", "test");
        Publisher<Document> publisher = Mono.just(deletedDoc);
        when(mockCollection.findOneAndDelete(filter, options)).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndDelete(filter, options);

        StepVerifier.create(result).expectNext(deletedDoc).verifyComplete();
    }

    @Test
    public void testFindOneAndDeleteWithOptionsAndRowType() {
        Bson filter = new Document("name", "test");
        FindOneAndDeleteOptions options = new FindOneAndDeleteOptions();
        Document deletedDoc = new Document("name", "test");
        Publisher<Document> publisher = Mono.just(deletedDoc);
        when(mockCollection.findOneAndDelete(filter, options)).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndDelete(filter, options, Document.class);

        StepVerifier.create(result).expectNext(deletedDoc).verifyComplete();
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
    public void testAggregateWithRowType() {
        List<Bson> pipeline = Arrays.asList(new Document("$match", new Document("status", "active")), new Document("$group", new Document("_id", "$category")));
        List<Document> aggregateResults = Arrays.asList(new Document("_id", "electronics"), new Document("_id", "books"));
        when(mockCollection.aggregate(pipeline, Document.class)).thenReturn(mockAggregatePublisher);
        // when(mockAggregatePublisher.iterator()).thenReturn(aggregateResults.iterator());

        Flux<Document> result = executor.aggregate(pipeline, Document.class);

        assertNotNull(result);
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
    public void testGroupByAndCountTypedMapsSingleGroupKey() {
        when(mockCollection.aggregate(anyList(), eq(Document.class))).thenAnswer(invocation -> {
            final List<?> pipeline = invocation.getArgument(0);
            final Document row = pipeline.size() > 1 ? new Document("department", "sales").append("count", 2)
                    : new Document("_id", "sales").append("count", 2);

            stubEmits(mockAggregatePublisher, row);
            return mockAggregatePublisher;
        });

        StepVerifier.create(executor.groupByAndCount("department", GroupRow.class))
                .expectNextMatches(row -> "sales".equals(row.getDepartment()) && row.getCount() == 2)
                .verifyComplete();
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
    public void testExistsWithNullStringObjectId() {
        assertThrows(IllegalArgumentException.class, () -> {
            executor.exists((String) null);
        });
    }

    @Test
    public void testExistsWithEmptyStringObjectId() {
        assertThrows(IllegalArgumentException.class, () -> {
            executor.exists("");
        });
    }

    @Test
    public void testGetWithInvalidObjectIdString() {
        String invalidObjectId = "invalid";
        assertThrows(IllegalArgumentException.class, () -> {
            executor.get(invalidObjectId);
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
    public void testListWithNegativeCount() {
        Bson filter = new Document("name", "test");
        assertThrows(IllegalArgumentException.class, () -> {
            executor.list(filter, 0, -1, Document.class);
        });
    }

    @Test
    public void testQueryWithNegativeOffset() {
        Bson filter = new Document("name", "test");
        assertThrows(IllegalArgumentException.class, () -> {
            executor.query(filter, -1, 10, Document.class);
        });
    }

    @Test
    public void testQueryWithNegativeCount() {
        Bson filter = new Document("name", "test");
        assertThrows(IllegalArgumentException.class, () -> {
            executor.query(filter, 0, -1, Document.class);
        });
    }

    @Test
    public void testqueryForSingleValueWithEmptyDocument() {
        String propName = "value";
        Bson filter = new Document("id", 999);
        Document emptyDoc = new Document();

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, emptyDoc);

        Mono<String> result = executor.queryForSingleValue(propName, filter, String.class);

        StepVerifier.create(result).verifyComplete();
    }

    @Test
    public void testInsertOneWithNonDocumentObject() {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "test");
        map.put("value", 123);

        InsertOneResult insertResult = mock(InsertOneResult.class);
        Publisher<InsertOneResult> publisher = Mono.just(insertResult);
        when(mockCollection.insertOne(any(Document.class))).thenReturn(publisher);

        Mono<InsertOneResult> result = executor.insertOne(map);

        StepVerifier.create(result).expectNext(insertResult).verifyComplete();
    }

    @Test
    public void testInsertManyWithNonDocumentObjects() {
        List<Map<String, Object>> mapList = new ArrayList<>();
        Map<String, Object> map1 = new HashMap<>();
        map1.put("name", "test1");
        Map<String, Object> map2 = new HashMap<>();
        map2.put("name", "test2");
        mapList.add(map1);
        mapList.add(map2);

        InsertManyResult insertResult = mock(InsertManyResult.class);
        Publisher<InsertManyResult> publisher = Mono.just(insertResult);
        when(mockCollection.insertMany(anyList())).thenReturn(publisher);

        Mono<InsertManyResult> result = executor.insertMany(mapList);

        StepVerifier.create(result).expectNext(insertResult).verifyComplete();
    }

    @Test
    public void testUpdateOneWithBsonUpdate() {
        Bson filter = new Document("name", "test");
        Bson update = new Document("$set", new Document("status", "updated"));
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateOne(eq(filter), eq(update))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateOne(filter, update);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testUpdateOneWithBasicDBObject() {
        Bson filter = new Document("name", "test");
        BasicDBObject update = new BasicDBObject("$set", new BasicDBObject("status", "updated"));
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateOne(eq(filter), eq(update))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateOne(filter, update);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testReplaceOneWithNonDocumentObject() {
        Bson filter = new Document("name", "oldName");
        Map<String, Object> replacement = new HashMap<>();
        replacement.put("name", "newName");
        replacement.put("status", "active");

        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.replaceOne(eq(filter), any(Document.class))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.replaceOne(filter, replacement);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testBulkInsertWithNonDocumentEntities() {
        List<Map<String, Object>> entities = new ArrayList<>();
        Map<String, Object> entity1 = new HashMap<>();
        entity1.put("name", "entity1");
        Map<String, Object> entity2 = new HashMap<>();
        entity2.put("name", "entity2");
        entities.add(entity1);
        entities.add(entity2);

        BulkWriteResult bulkResult = mock(BulkWriteResult.class);
        when(bulkResult.getInsertedCount()).thenReturn(2);
        Publisher<BulkWriteResult> publisher = Mono.just(bulkResult);
        when(mockCollection.bulkWrite(anyList())).thenReturn(publisher);

        Mono<Integer> result = executor.bulkInsert(entities);

        StepVerifier.create(result).expectNext(2).verifyComplete();
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
    public void testListWithSelectPropNamesAsList() {
        List<String> selectPropNames = Arrays.asList("name", "value");
        Bson filter = new Document("active", true);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);

        Flux<Document> result = executor.list(selectPropNames, filter, Document.class);

        assertNotNull(result);
    }

    @Test
    public void testUpdateOneWithCollectionAsList() {
        Bson filter = new Document("name", "test");
        List<Bson> updates = Arrays.asList(new Document("$set", new Document("status", "active")));
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateOne(eq(filter), eq(updates))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateOne(filter, updates);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testUpdateManyWithNonBsonCollection() {
        Bson filter = new Document("status", "pending");
        List<Map<String, Object>> updates = new ArrayList<>();
        Map<String, Object> update = new HashMap<>();
        update.put("$set", new Document("status", "active"));
        updates.add(update);

        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateMany(eq(filter), anyList())).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateMany(filter, updates);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testFindOneAndUpdateWithNonBsonCollection() {
        Bson filter = new Document("name", "test");
        List<Map<String, Object>> updates = new ArrayList<>();
        Map<String, Object> update = new HashMap<>();
        update.put("$set", new Document("status", "active"));
        updates.add(update);

        Document resultDoc = new Document("name", "test").append("status", "active");
        Publisher<Document> publisher = Mono.just(resultDoc);
        when(mockCollection.findOneAndUpdate(eq(filter), anyList())).thenReturn(publisher);

        Mono<Document> result = executor.findOneAndUpdate(filter, updates);

        StepVerifier.create(result).expectNext(resultDoc).verifyComplete();
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
    public void testUpdateOneWithDriverBuiltBsonNotWrappedInSet() {
        // Regression: a driver-built Bson (Updates.set(...)) is neither Document nor BasicDBObject.
        // It must be passed through as-is, not re-wrapped in {$set: ...} which corrupts the update.
        Bson filter = new Document("name", "test");
        Bson update = com.mongodb.client.model.Updates.set("verified", true);
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateOne(eq(filter), eq(update))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateOne(filter, update);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @Test
    public void testQueryForSingleValueWithFoundDocumentButNullPropertyCompletesEmpty() {
        // Regression: a matching document is found but the requested property is absent/null.
        // N.convert(null, String.class) returns null and Mono.just(null) throws NPE.
        // The reactive contract requires completing empty (no value) instead.
        String propName = "name";
        Bson filter = new Document("id", 1);
        Document doc = new Document("otherField", "value"); // no "name" property

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        stubEmits(mockFindPublisher, doc);

        Mono<String> result = executor.queryForSingleValue(propName, filter, String.class);

        StepVerifier.create(result).verifyComplete();
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

    // Covers the private query(Collection,...) -> executeQuery(Projections.include(List)) branch
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

    // Covers toBson(Collection) pass-through path: when ALL elements are already Bson
    // and the collection IS a List, the list is returned as-is (no copy).
    @Test
    public void testUpdateOneWithAllBsonListPassthrough() {
        Bson filter = new Document("name", "test");
        List<Bson> updates = Arrays.asList(new Document("$set", new Document("a", 1)), new Document("$set", new Document("b", 2)));
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateOne(eq(filter), eq(updates))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateOne(filter, updates);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
        verify(mockCollection).updateOne(eq(filter), eq(updates));
    }

    // Covers toBson(Collection) all-Bson-but-non-List path: copies into a new ArrayList.
    @Test
    public void testUpdateOneWithAllBsonNonListCollection() {
        Bson filter = new Document("name", "test");
        Collection<Bson> updates = new java.util.LinkedHashSet<>(
                Arrays.asList(new Document("$set", new Document("a", 1)), new Document("$set", new Document("b", 2))));
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateOne(eq(filter), anyList())).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateOne(filter, updates);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
        verify(mockCollection).updateOne(eq(filter), anyList());
    }

    // Covers toBson(Collection) empty-collection guard: must throw IllegalArgumentException.
    @Test
    public void testUpdateOneWithEmptyCollection_EdgeCase() {
        Bson filter = new Document("name", "test");
        assertThrows(IllegalArgumentException.class, () -> {
            executor.updateOne(filter, Collections.emptyList());
        });
    }

    // Covers toDocument(Collection) all-Document path for a non-List collection.
    @Test
    public void testInsertManyWithAllDocumentsNonListCollection() {
        Collection<Document> docs = new java.util.LinkedHashSet<>(Arrays.asList(new Document("name", "a"), new Document("name", "b")));
        InsertManyResult insertResult = mock(InsertManyResult.class);
        Publisher<InsertManyResult> publisher = Mono.just(insertResult);
        when(mockCollection.insertMany(anyList())).thenReturn(publisher);

        Mono<InsertManyResult> result = executor.insertMany(docs);

        StepVerifier.create(result).expectNext(insertResult).verifyComplete();
    }

    // Sanity: executor.coll() returns the wrapped collection (additional assertion form).
    @Test
    public void testCollReturnsWrappedCollection() {
        assertNotNull(executor.coll());
        assertEquals(mockCollection, executor.coll());
    }

    // Confirms executor created via constructor with a non-null collection is usable.
    @Test
    public void testConstructorAcceptsCollection() {
        MongoCollectionExecutor exec = new MongoCollectionExecutor(mockCollection);
        assertNotNull(exec);
        assertSame(mockCollection, exec.coll());
        assertTrue(exec.coll() == mockCollection);
    }

    // Covers executeQuery(projection, filter, sort, offset, 0) zero-count path which
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

    // Covers toBson(Object) for a driver-built update Bson (e.g. com.mongodb.client.model.Updates).
    // These are not Document / BasicDBObject so must be returned as-is (no {$set: ...} wrapping).
    @Test
    public void testUpdateOneWithDriverBuiltUpdateBson() {
        Bson filter = new Document("name", "test");
        Bson update = com.mongodb.client.model.Updates.set("status", "active");
        UpdateResult updateResult = mock(UpdateResult.class);
        Publisher<UpdateResult> publisher = Mono.just(updateResult);
        when(mockCollection.updateOne(eq(filter), eq(update))).thenReturn(publisher);

        Mono<UpdateResult> result = executor.updateOne(filter, update);

        StepVerifier.create(result).expectNext(updateResult).verifyComplete();
    }

    @SuppressWarnings("unchecked")
    @SafeVarargs
    private static <T> void stubEmits(final Publisher<T> publisher, final T... items) {
        // The executor consumes these publishers via Flux.from(publisher), which calls
        // publisher.subscribe(...). Stubbing publisher.first()/limit()/etc. is useless —
        // those are never invoked. We must stub subscribe() itself so the subscriber
        // actually receives signals, otherwise StepVerifier hangs forever.
        doAnswer(invocation -> {
            Flux.fromArray(items).subscribe((Subscriber<? super T>) invocation.getArgument(0));
            return null;
        }).when(publisher).subscribe(any());
    }

    // ========= Regression: null update/replacement must throw IllegalArgumentException eagerly, not NPE =========

    @Test
    public void testUpdateOneWithNullUpdateThrowsIAE() {
        Document filter = new Document("id", 1);
        assertThrows(IllegalArgumentException.class, () -> executor.updateOne(filter, (Object) null));
    }

    @Test
    public void testUpdateManyWithNullUpdateThrowsIAE() {
        Document filter = new Document("id", 1);
        assertThrows(IllegalArgumentException.class, () -> executor.updateMany(filter, (Object) null));
    }

    @Test
    public void testReplaceOneWithNullReplacementThrowsIAE() {
        Document filter = new Document("id", 1);
        assertThrows(IllegalArgumentException.class, () -> executor.replaceOne(filter, (Object) null));
    }

    @Test
    public void testFindOneAndUpdateWithNullUpdateThrowsIAE() {
        Document filter = new Document("id", 1);
        assertThrows(IllegalArgumentException.class, () -> executor.findOneAndUpdate(filter, (Object) null));
    }

    @Test
    public void testFindOneAndReplaceWithNullReplacementThrowsIAE() {
        Document filter = new Document("id", 1);
        assertThrows(IllegalArgumentException.class, () -> executor.findOneAndReplace(filter, (Object) null));
    }

    @Test
    public void testFindOneAndDeleteWithNullFilterThrowsIAE() {
        // Regression: a null filter must be rejected eagerly with IllegalArgumentException at the call site (before a
        // Mono is built), matching every sibling deleteOne/findOneAndUpdate/findOneAndReplace. Before the fix the null
        // filter reached the driver, which silently deletes an arbitrary document.
        final com.mongodb.client.model.FindOneAndDeleteOptions options = new com.mongodb.client.model.FindOneAndDeleteOptions();

        assertThrows(IllegalArgumentException.class, () -> executor.findOneAndDelete((Bson) null));
        assertThrows(IllegalArgumentException.class, () -> executor.findOneAndDelete((Bson) null, Integer.class));
        assertThrows(IllegalArgumentException.class, () -> executor.findOneAndDelete((Bson) null, options));
        assertThrows(IllegalArgumentException.class, () -> executor.findOneAndDelete((Bson) null, options, Document.class));
    }

    @Test
    public void testReadMethodsRejectNullFilterWithIAE() {
        // Regression: findFirst/list/query taking a Bson filter must reject a null filter eagerly with
        // IllegalArgumentException at the call site (before a Mono/Flux is built). Previously a null filter
        // was silently treated as "match all". The driver must NOT be invoked.
        final Collection<String> selectPropNames = Arrays.asList("name");
        final Bson sort = new Document("name", 1);
        final Bson projection = new Document("name", 1);

        // findFirst family
        assertThrows(IllegalArgumentException.class, () -> executor.findFirst((Bson) null));
        assertThrows(IllegalArgumentException.class, () -> executor.findFirst((Bson) null, Document.class));
        assertThrows(IllegalArgumentException.class, () -> executor.findFirst(selectPropNames, (Bson) null, Document.class));
        assertThrows(IllegalArgumentException.class, () -> executor.findFirst(selectPropNames, (Bson) null, sort, Document.class));
        assertThrows(IllegalArgumentException.class, () -> executor.findFirst(projection, (Bson) null, sort, Document.class));

        // list family
        assertThrows(IllegalArgumentException.class, () -> executor.list((Bson) null));
        assertThrows(IllegalArgumentException.class, () -> executor.list((Bson) null, Document.class));
        assertThrows(IllegalArgumentException.class, () -> executor.list((Bson) null, 0, 10, Document.class));
        assertThrows(IllegalArgumentException.class, () -> executor.list(selectPropNames, (Bson) null, Document.class));
        assertThrows(IllegalArgumentException.class, () -> executor.list(selectPropNames, (Bson) null, sort, 0, 10, Document.class));
        assertThrows(IllegalArgumentException.class, () -> executor.list(projection, (Bson) null, sort, 0, 10, Document.class));

        // query family
        assertThrows(IllegalArgumentException.class, () -> executor.query((Bson) null));
        assertThrows(IllegalArgumentException.class, () -> executor.query((Bson) null, Document.class));
        assertThrows(IllegalArgumentException.class, () -> executor.query((Bson) null, 0, 10, Document.class));
        assertThrows(IllegalArgumentException.class, () -> executor.query(selectPropNames, (Bson) null, Document.class));
        assertThrows(IllegalArgumentException.class, () -> executor.query(selectPropNames, (Bson) null, sort, 0, 10, Document.class));
        assertThrows(IllegalArgumentException.class, () -> executor.query(projection, (Bson) null, sort, 0, 10, Document.class));

        // count / exists / queryForSingleValue / distinct take a Bson filter too and must reject null consistently.
        assertThrows(IllegalArgumentException.class, () -> executor.count((Bson) null));
        assertThrows(IllegalArgumentException.class, () -> executor.count((Bson) null, new CountOptions()));
        assertThrows(IllegalArgumentException.class, () -> executor.exists((Bson) null));
        assertThrows(IllegalArgumentException.class, () -> executor.queryForSingleValue("name", (Bson) null, String.class));
        assertThrows(IllegalArgumentException.class, () -> executor.distinct("name", (Bson) null, String.class));

        // The eager guard short-circuits before the driver is touched.
        verify(mockCollection, org.mockito.Mockito.never()).find(any(Bson.class));
        verify(mockCollection, org.mockito.Mockito.never()).countDocuments(any(Bson.class));
    }

    public static class GroupRow {
        private String department;
        private int count;

        public String getDepartment() {
            return department;
        }

        public void setDepartment(String department) {
            this.department = department;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
    }
}
