package com.landawn.abacus.da.mongodb;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.u.Optional;
import com.landawn.abacus.util.u.OptionalBoolean;
import com.landawn.abacus.util.stream.Stream;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

public class AsyncMongoCollectionExecutorTest extends TestBase {

    @Mock
    private MongoCollectionExecutor mockCollExecutor;

    @Mock
    private AsyncExecutor mockAsyncExecutor;

    private AsyncMongoCollectionExecutor asyncExecutor;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        asyncExecutor = new AsyncMongoCollectionExecutor(mockCollExecutor, mockAsyncExecutor);
    }

    @Test
    public void testSync() {
        MongoCollectionExecutor result = asyncExecutor.sync();
        Assertions.assertSame(mockCollExecutor, result);
    }

    @Test
    public void testExistsWithStringObjectId() throws Exception {
        String objectId = "507f1f77bcf86cd799439011";
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(true));

        ContinuableFuture<Boolean> result = asyncExecutor.exists(objectId);
        Assertions.assertTrue(result.get());
    }

    @Test
    public void testExistsWithObjectId() throws Exception {
        ObjectId objectId = new ObjectId();
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(true));

        ContinuableFuture<Boolean> result = asyncExecutor.exists(objectId);
        Assertions.assertTrue(result.get());
    }

    @Test
    public void testExistsWithBsonFilter() throws Exception {
        Document filter = new Document("name", "test");
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(false));

        ContinuableFuture<Boolean> result = asyncExecutor.exists(filter);
        Assertions.assertFalse(result.get());
    }

    @Test
    public void testCount() throws Exception {
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(10L));

        ContinuableFuture<Long> result = asyncExecutor.count();
        Assertions.assertEquals(10L, result.get());
    }

    @Test
    public void testCountWithFilter() throws Exception {
        Document filter = new Document("status", "active");
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(5L));

        ContinuableFuture<Long> result = asyncExecutor.count(filter);
        Assertions.assertEquals(5L, result.get());
    }

    @Test
    public void testCountWithFilterAndOptions() throws Exception {
        Document filter = new Document("status", "active");
        CountOptions options = new CountOptions().limit(100);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(3L));

        ContinuableFuture<Long> result = asyncExecutor.count(filter, options);
        Assertions.assertEquals(3L, result.get());
    }

    @Test
    public void testGetWithStringObjectId() throws Exception {
        String objectId = "507f1f77bcf86cd799439011";
        Document doc = new Document("_id", objectId);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(Optional.of(doc)));

        ContinuableFuture<Optional<Document>> result = asyncExecutor.get(objectId);
        Assertions.assertTrue(result.get().isPresent());
    }

    @Test
    public void testGetWithObjectId() throws Exception {
        ObjectId objectId = new ObjectId();
        Document doc = new Document("_id", objectId);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(Optional.of(doc)));

        ContinuableFuture<Optional<Document>> result = asyncExecutor.get(objectId);
        Assertions.assertTrue(result.get().isPresent());
    }

    @Test
    public void testGetWithStringObjectIdAndRowType() throws Exception {
        String objectId = "507f1f77bcf86cd799439011";
        TestEntity entity = new TestEntity();
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(Optional.of(entity)));

        ContinuableFuture<Optional<TestEntity>> result = asyncExecutor.get(objectId, TestEntity.class);
        Assertions.assertTrue(result.get().isPresent());
    }

    @Test
    public void testGettWithStringObjectId() throws Exception {
        String objectId = "507f1f77bcf86cd799439011";
        Document doc = new Document("_id", objectId);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(doc));

        ContinuableFuture<Document> result = asyncExecutor.gett(objectId);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testFindFirst() throws Exception {
        Document filter = new Document("name", "test");
        Document doc = new Document("name", "test");
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(Optional.of(doc)));

        ContinuableFuture<Optional<Document>> result = asyncExecutor.findFirst(filter);
        Assertions.assertTrue(result.get().isPresent());
    }

    @Test
    public void testList() throws Exception {
        Document filter = new Document("status", "active");
        List<Document> docs = Arrays.asList(new Document("id", 1), new Document("id", 2));
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(docs));

        ContinuableFuture<List<Document>> result = asyncExecutor.list(filter);
        Assertions.assertEquals(2, result.get().size());
    }

    @Test
    public void testQueryForBoolean() throws Exception {
        String propName = "isActive";
        Document filter = new Document("id", 1);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(OptionalBoolean.of(true)));

        ContinuableFuture<OptionalBoolean> result = asyncExecutor.queryForBoolean(propName, filter);
        Assertions.assertTrue(result.get().orElseThrow());
    }

    @Test
    public void testQuery() throws Exception {
        Document filter = new Document("status", "active");
        Dataset dataset = N.newEmptyDataset(Arrays.asList("id", "name"));
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(dataset));

        ContinuableFuture<Dataset> result = asyncExecutor.query(filter);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testStream() throws Exception {
        Document filter = new Document("status", "active");
        Stream<Document> stream = Stream.just(new Document("id", 1));
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(stream));

        ContinuableFuture<Stream<Document>> result = asyncExecutor.stream(filter);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testWatch() throws Exception {
        ChangeStreamIterable<Document> changeStream = mock(ChangeStreamIterable.class);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(changeStream));

        ContinuableFuture<ChangeStreamIterable<Document>> result = asyncExecutor.watch();
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testInsertOne() throws Exception {
        TestEntity entity = new TestEntity();
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(null));

        ContinuableFuture<Void> result = asyncExecutor.insertOne(entity);
        result.get(); // Should complete without exception
    }

    @Test
    public void testInsertMany() throws Exception {
        List<TestEntity> entities = Arrays.asList(new TestEntity(), new TestEntity());
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(null));

        ContinuableFuture<Void> result = asyncExecutor.insertMany(entities);
        result.get(); // Should complete without exception
    }

    @Test
    public void testUpdateOne() throws Exception {
        String objectId = "507f1f77bcf86cd799439011";
        Document update = new Document("$set", new Document("name", "updated"));
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(updateResult));

        ContinuableFuture<UpdateResult> result = asyncExecutor.updateOne(objectId, update);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testUpdateMany() throws Exception {
        Document filter = new Document("status", "active");
        Document update = new Document("$set", new Document("status", "inactive"));
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(updateResult));

        ContinuableFuture<UpdateResult> result = asyncExecutor.updateMany(filter, update);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testReplaceOne() throws Exception {
        String objectId = "507f1f77bcf86cd799439011";
        Document replacement = new Document("name", "replaced");
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(updateResult));

        ContinuableFuture<UpdateResult> result = asyncExecutor.replaceOne(objectId, replacement);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testDeleteOne() throws Exception {
        String objectId = "507f1f77bcf86cd799439011";
        DeleteResult deleteResult = mock(DeleteResult.class);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(deleteResult));

        ContinuableFuture<DeleteResult> result = asyncExecutor.deleteOne(objectId);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testDeleteMany() throws Exception {
        Document filter = new Document("status", "inactive");
        DeleteResult deleteResult = mock(DeleteResult.class);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(deleteResult));

        ContinuableFuture<DeleteResult> result = asyncExecutor.deleteMany(filter);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testBulkInsert() throws Exception {
        List<Document> documents = Arrays.asList(new Document("id", 1), new Document("id", 2));
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(2));

        ContinuableFuture<Integer> result = asyncExecutor.bulkInsert(documents);
        Assertions.assertEquals(2, result.get());
    }

    @Test
    public void testBulkWrite() throws Exception {
        List<WriteModel<Document>> requests = Arrays.asList();
        BulkWriteResult bulkWriteResult = mock(BulkWriteResult.class);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(bulkWriteResult));

        ContinuableFuture<BulkWriteResult> result = asyncExecutor.bulkWrite(requests);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testFindOneAndUpdate() throws Exception {
        Document filter = new Document("id", 1);
        Document update = new Document("$set", new Document("name", "updated"));
        Document updatedDoc = new Document("id", 1).append("name", "updated");
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(updatedDoc));

        ContinuableFuture<Document> result = asyncExecutor.findOneAndUpdate(filter, update);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testFindOneAndReplace() throws Exception {
        Document filter = new Document("id", 1);
        Document replacement = new Document("id", 1).append("name", "replaced");
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(replacement));

        ContinuableFuture<Document> result = asyncExecutor.findOneAndReplace(filter, replacement);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testFindOneAndDelete() throws Exception {
        Document filter = new Document("id", 1);
        Document deletedDoc = new Document("id", 1);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(deletedDoc));

        ContinuableFuture<Document> result = asyncExecutor.findOneAndDelete(filter);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testDistinct() throws Exception {
        String fieldName = "category";
        Stream<String> stream = Stream.of("cat1", "cat2");
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(stream));

        ContinuableFuture<Stream<String>> result = asyncExecutor.distinct(fieldName, String.class);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testAggregate() throws Exception {
        List<Document> pipeline = Arrays.asList(new Document("$match", new Document("status", "active")));
        Stream<Document> stream = Stream.just(new Document("id", 1));
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(stream));

        ContinuableFuture<Stream<Document>> result = asyncExecutor.aggregate(pipeline);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testMapReduce() throws Exception {
        String mapFunction = "function() { emit(this.name, 1); }";
        String reduceFunction = "function(key, values) { return Array.sum(values); }";
        Stream<Document> stream = Stream.just(new Document("_id", "test"));
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(stream));

        ContinuableFuture<Stream<Document>> result = asyncExecutor.mapReduce(mapFunction, reduceFunction);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testGroupBy() throws Exception {
        String fieldName = "category";
        Stream<Document> stream = Stream.just(new Document("_id", "cat1"));
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(stream));

        ContinuableFuture<Stream<Document>> result = asyncExecutor.groupBy(fieldName);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testGroupByAndCount() throws Exception {
        String fieldName = "category";
        Stream<Document> stream = Stream.just(new Document("_id", "cat1").append("count", 5));
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(stream));

        ContinuableFuture<Stream<Document>> result = asyncExecutor.groupByAndCount(fieldName);
        Assertions.assertNotNull(result.get());
    }

    // Test entity class for testing
    private static class TestEntity {
        private String id;
        private String name;

        // Getters and setters
        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}