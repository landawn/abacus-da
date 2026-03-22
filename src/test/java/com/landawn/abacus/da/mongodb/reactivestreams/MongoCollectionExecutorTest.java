package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
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
        when(mockFindPublisher.projection(any())).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.first()).thenReturn(Mono.just(doc));

        Mono<Document> result = executor.get(objectId);

        StepVerifier.create(result).expectNext(doc).verifyComplete();
    }

    @Test
    public void testGetWithObjectId() {
        ObjectId objectId = new ObjectId();
        Document doc = new Document("_id", objectId).append("name", "test");

        when(mockCollection.find(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any())).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.first()).thenReturn(Mono.just(doc));

        Mono<Document> result = executor.get(objectId);

        StepVerifier.create(result).expectNext(doc).verifyComplete();
    }

    @Test
    public void testGetWithStringObjectIdAndRowType() {
        String objectId = "507f1f77bcf86cd799439011";
        Document doc = new Document("_id", new ObjectId(objectId)).append("value", "test");

        when(mockCollection.find(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any())).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.first()).thenReturn(Mono.just(doc));

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
        when(mockFindPublisher.projection(any())).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.first()).thenReturn(Mono.just(doc));

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
        when(mockFindPublisher.first()).thenReturn(Mono.just(doc));

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
        when(mockFindPublisher.first()).thenReturn(Mono.just(doc));

        Mono<Document> result = executor.get(objectId, selectPropNames, Document.class);

        StepVerifier.create(result).expectNext(doc).verifyComplete();
    }

    @Test
    public void testFindFirstWithFilter() {
        Bson filter = new Document("name", "test");
        Document doc = new Document("name", "test");

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.first()).thenReturn(Mono.just(doc));

        Mono<Document> result = executor.findFirst(filter);

        StepVerifier.create(result).expectNext(doc).verifyComplete();
    }

    @Test
    public void testFindFirstWithFilterAndRowType() {
        Bson filter = new Document("value", 123);
        Document doc = new Document("value", 123);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.first()).thenReturn(Mono.just(doc));

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
        when(mockFindPublisher.first()).thenReturn(Mono.just(doc));

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
        when(mockFindPublisher.first()).thenReturn(Mono.just(doc));

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
        when(mockFindPublisher.first()).thenReturn(Mono.just(doc));

        Mono<Document> result = executor.findFirst(projection, filter, sort, Document.class);

        StepVerifier.create(result).expectNext(doc).verifyComplete();
    }

    @Test
    public void testListWithFilter() {
        Bson filter = new Document("status", "active");
        List<Document> docs = Arrays.asList(new Document("name", "doc1"), new Document("name", "doc2"));

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.skip(0)).thenReturn(mockFindPublisher);

        Flux<Document> result = executor.list(filter);

        StepVerifier.create(result).expectNext(docs.get(0)).expectNext(docs.get(1)).verifyComplete();
    }

    @Test
    public void testListWithFilterAndRowType() {
        Bson filter = new Document("type", "number");
        List<Document> docs = Arrays.asList(new Document("value", 1), new Document("value", 2));

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.skip(0)).thenReturn(mockFindPublisher);

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
        when(mockFindPublisher.skip(0)).thenReturn(mockFindPublisher);

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
        when(mockFindPublisher.skip(0)).thenReturn(mockFindPublisher);

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
        when(mockFindPublisher.first()).thenReturn(Mono.just(doc));

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
        when(mockFindPublisher.first()).thenReturn(Mono.just(doc));

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
        when(mockFindPublisher.first()).thenReturn(Mono.just(doc));

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
        when(mockFindPublisher.first()).thenReturn(Mono.just(doc));

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
        when(mockFindPublisher.first()).thenReturn(Mono.just(doc));

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
        when(mockFindPublisher.first()).thenReturn(Mono.just(doc));

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
        when(mockFindPublisher.first()).thenReturn(Mono.just(doc));

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
        when(mockFindPublisher.first()).thenReturn(Mono.just(doc));

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
        when(mockFindPublisher.first()).thenReturn(Mono.just(doc));

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
        when(mockFindPublisher.first()).thenReturn(Mono.just(doc));

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
        when(mockFindPublisher.first()).thenReturn(Mono.just(doc));

        Mono<Date> result = executor.queryForDate(propName, filter, Date.class);

        StepVerifier.create(result).expectNext(date).verifyComplete();
    }

    @Test
    public void testQueryForSingleResult() {
        String propName = "value";
        Bson filter = new Document("id", 1);
        Document doc = new Document(propName, 42);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.first()).thenReturn(Mono.just(doc));

        Mono<Integer> result = executor.queryForSingleResult(propName, filter, Integer.class);

        StepVerifier.create(result).expectNext(42).verifyComplete();
    }

    @Test
    public void testQueryWithFilter() {
        Bson filter = new Document("status", "active");
        List<Document> docs = Arrays.asList(new Document("name", "doc1"), new Document("name", "doc2"));

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.skip(0)).thenReturn(mockFindPublisher);

        Mono<Dataset> result = executor.query(filter);

        assertNotNull(result);
    }

    @Test
    public void testQueryWithFilterAndRowType() {
        Bson filter = new Document("type", "test");

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.skip(0)).thenReturn(mockFindPublisher);

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
    public void testQueryForSingleResultWithEmptyDocument() {
        String propName = "value";
        Bson filter = new Document("id", 999);
        Document emptyDoc = new Document();

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(1)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.first()).thenReturn(Mono.just(emptyDoc));

        Mono<String> result = executor.queryForSingleResult(propName, filter, String.class);

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
        when(mockFindPublisher.skip(0)).thenReturn(mockFindPublisher);

        Flux<Document> result = executor.list(selectPropNames, filter, Document.class);

        assertNotNull(result);
    }

    @Test
    public void testListWithSelectPropNamesAsList() {
        List<String> selectPropNames = Arrays.asList("name", "value");
        Bson filter = new Document("active", true);

        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.skip(0)).thenReturn(mockFindPublisher);

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
}
