package com.landawn.abacus.da.mongodb;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
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
import com.landawn.abacus.util.u.Nullable;
import com.landawn.abacus.util.u.Optional;
import com.landawn.abacus.util.u.OptionalBoolean;
import com.landawn.abacus.util.u.OptionalByte;
import com.landawn.abacus.util.u.OptionalChar;
import com.landawn.abacus.util.u.OptionalDouble;
import com.landawn.abacus.util.u.OptionalFloat;
import com.landawn.abacus.util.u.OptionalInt;
import com.landawn.abacus.util.u.OptionalLong;
import com.landawn.abacus.util.u.OptionalShort;
import com.landawn.abacus.util.stream.Stream;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
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

    // ---- Additional coverage: get/gett with rowType variants ----

    @Test
    public void testGetWithObjectIdAndRowType() throws Exception {
        ObjectId objectId = new ObjectId();
        TestEntity entity = new TestEntity();
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(Optional.of(entity)));

        ContinuableFuture<Optional<TestEntity>> result = asyncExecutor.get(objectId, TestEntity.class);
        Assertions.assertTrue(result.get().isPresent());
    }

    @Test
    public void testGetWithStringObjectIdSelectPropsAndRowType() throws Exception {
        String objectId = "507f1f77bcf86cd799439011";
        TestEntity entity = new TestEntity();
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(Optional.of(entity)));

        ContinuableFuture<Optional<TestEntity>> result = asyncExecutor.get(objectId, Arrays.asList("name"), TestEntity.class);
        Assertions.assertTrue(result.get().isPresent());
    }

    @Test
    public void testGetWithObjectIdSelectPropsAndRowType() throws Exception {
        ObjectId objectId = new ObjectId();
        TestEntity entity = new TestEntity();
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(Optional.of(entity)));

        ContinuableFuture<Optional<TestEntity>> result = asyncExecutor.get(objectId, Arrays.asList("name"), TestEntity.class);
        Assertions.assertTrue(result.get().isPresent());
    }

    @Test
    public void testGettWithObjectId() throws Exception {
        ObjectId objectId = new ObjectId();
        Document doc = new Document("_id", objectId);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(doc));

        ContinuableFuture<Document> result = asyncExecutor.gett(objectId);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testGettWithStringObjectIdAndRowType() throws Exception {
        String objectId = "507f1f77bcf86cd799439011";
        TestEntity entity = new TestEntity();
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(entity));

        ContinuableFuture<TestEntity> result = asyncExecutor.gett(objectId, TestEntity.class);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testGettWithObjectIdAndRowType() throws Exception {
        ObjectId objectId = new ObjectId();
        TestEntity entity = new TestEntity();
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(entity));

        ContinuableFuture<TestEntity> result = asyncExecutor.gett(objectId, TestEntity.class);
        Assertions.assertNotNull(result.get());
    }

    // ---- findFirst variants ----

    @Test
    public void testFindFirstWithFilterAndRowType() throws Exception {
        Document filter = new Document("status", "active");
        TestEntity entity = new TestEntity();
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(Optional.of(entity)));

        ContinuableFuture<Optional<TestEntity>> result = asyncExecutor.findFirst(filter, TestEntity.class);
        Assertions.assertTrue(result.get().isPresent());
    }

    @Test
    public void testFindFirstWithSelectPropsFilterAndRowType() throws Exception {
        Document filter = new Document("status", "active");
        TestEntity entity = new TestEntity();
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(Optional.of(entity)));

        ContinuableFuture<Optional<TestEntity>> result = asyncExecutor.findFirst(Arrays.asList("name"), filter, TestEntity.class);
        Assertions.assertTrue(result.get().isPresent());
    }

    @Test
    public void testFindFirstWithSelectPropsFilterSortAndRowType() throws Exception {
        Document filter = new Document("status", "active");
        Document sort = new Document("name", 1);
        TestEntity entity = new TestEntity();
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(Optional.of(entity)));

        ContinuableFuture<Optional<TestEntity>> result = asyncExecutor.findFirst(Arrays.asList("name"), filter, sort, TestEntity.class);
        Assertions.assertTrue(result.get().isPresent());
    }

    @Test
    public void testFindFirstWithProjectionFilterSortAndRowType() throws Exception {
        Document projection = new Document("name", 1);
        Document filter = new Document("status", "active");
        Document sort = new Document("name", 1);
        TestEntity entity = new TestEntity();
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(Optional.of(entity)));

        ContinuableFuture<Optional<TestEntity>> result = asyncExecutor.findFirst(projection, filter, sort, TestEntity.class);
        Assertions.assertTrue(result.get().isPresent());
    }

    // ---- list variants with rowType / projection / pagination ----

    @Test
    public void testListWithFilterAndRowType() throws Exception {
        Document filter = new Document("status", "active");
        List<TestEntity> entities = Arrays.asList(new TestEntity(), new TestEntity());
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(entities));

        ContinuableFuture<List<TestEntity>> result = asyncExecutor.list(filter, TestEntity.class);
        Assertions.assertEquals(2, result.get().size());
    }

    @Test
    public void testListWithFilterOffsetCountAndRowType() throws Exception {
        Document filter = new Document("status", "active");
        List<TestEntity> entities = Collections.singletonList(new TestEntity());
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(entities));

        ContinuableFuture<List<TestEntity>> result = asyncExecutor.list(filter, 0, 10, TestEntity.class);
        Assertions.assertEquals(1, result.get().size());
    }

    @Test
    public void testListWithSelectPropsFilterAndRowType() throws Exception {
        Document filter = new Document("status", "active");
        List<TestEntity> entities = Collections.singletonList(new TestEntity());
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(entities));

        ContinuableFuture<List<TestEntity>> result = asyncExecutor.list(Arrays.asList("name"), filter, TestEntity.class);
        Assertions.assertEquals(1, result.get().size());
    }

    @Test
    public void testListWithSelectPropsFilterOffsetCountAndRowType() throws Exception {
        Document filter = new Document("status", "active");
        List<TestEntity> entities = Collections.emptyList();
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(entities));

        ContinuableFuture<List<TestEntity>> result = asyncExecutor.list(Arrays.asList("name"), filter, 0, 10, TestEntity.class);
        Assertions.assertEquals(0, result.get().size());
    }

    @Test
    public void testListWithSelectPropsFilterSortAndRowType() throws Exception {
        Document filter = new Document("status", "active");
        Document sort = new Document("name", 1);
        List<TestEntity> entities = Collections.singletonList(new TestEntity());
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(entities));

        ContinuableFuture<List<TestEntity>> result = asyncExecutor.list(Arrays.asList("name"), filter, sort, TestEntity.class);
        Assertions.assertEquals(1, result.get().size());
    }

    @Test
    public void testListWithSelectPropsFilterSortOffsetCountAndRowType() throws Exception {
        Document filter = new Document("status", "active");
        Document sort = new Document("name", 1);
        List<TestEntity> entities = Collections.singletonList(new TestEntity());
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(entities));

        ContinuableFuture<List<TestEntity>> result = asyncExecutor.list(Arrays.asList("name"), filter, sort, 0, 10, TestEntity.class);
        Assertions.assertEquals(1, result.get().size());
    }

    @Test
    public void testListWithProjectionFilterSortAndRowType() throws Exception {
        Document projection = new Document("name", 1);
        Document filter = new Document("status", "active");
        Document sort = new Document("name", 1);
        List<TestEntity> entities = Collections.singletonList(new TestEntity());
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(entities));

        ContinuableFuture<List<TestEntity>> result = asyncExecutor.list(projection, filter, sort, TestEntity.class);
        Assertions.assertEquals(1, result.get().size());
    }

    @Test
    public void testListWithProjectionFilterSortOffsetCountAndRowType() throws Exception {
        Document projection = new Document("name", 1);
        Document filter = new Document("status", "active");
        Document sort = new Document("name", 1);
        List<TestEntity> entities = Collections.singletonList(new TestEntity());
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(entities));

        ContinuableFuture<List<TestEntity>> result = asyncExecutor.list(projection, filter, sort, 0, 10, TestEntity.class);
        Assertions.assertEquals(1, result.get().size());
    }

    // ---- queryFor* primitive variants ----

    @Test
    public void testQueryForChar() throws Exception {
        Document filter = new Document("id", 1);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(OptionalChar.of('A')));

        ContinuableFuture<OptionalChar> result = asyncExecutor.queryForChar("grade", filter);
        Assertions.assertEquals('A', result.get().get());
    }

    @Test
    public void testQueryForByte() throws Exception {
        Document filter = new Document("id", 1);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(OptionalByte.of((byte) 1)));

        ContinuableFuture<OptionalByte> result = asyncExecutor.queryForByte("level", filter);
        Assertions.assertEquals((byte) 1, result.get().get());
    }

    @Test
    public void testQueryForShort() throws Exception {
        Document filter = new Document("id", 1);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(OptionalShort.of((short) 10)));

        ContinuableFuture<OptionalShort> result = asyncExecutor.queryForShort("count", filter);
        Assertions.assertEquals((short) 10, result.get().get());
    }

    @Test
    public void testQueryForInt() throws Exception {
        Document filter = new Document("id", 1);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(OptionalInt.of(42)));

        ContinuableFuture<OptionalInt> result = asyncExecutor.queryForInt("age", filter);
        Assertions.assertEquals(42, result.get().get());
    }

    @Test
    public void testQueryForLong() throws Exception {
        Document filter = new Document("id", 1);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(OptionalLong.of(123L)));

        ContinuableFuture<OptionalLong> result = asyncExecutor.queryForLong("ts", filter);
        Assertions.assertEquals(123L, result.get().get());
    }

    @Test
    public void testQueryForFloat() throws Exception {
        Document filter = new Document("id", 1);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(OptionalFloat.of(1.5f)));

        ContinuableFuture<OptionalFloat> result = asyncExecutor.queryForFloat("price", filter);
        Assertions.assertEquals(1.5f, result.get().get(), 0.001f);
    }

    @Test
    public void testQueryForDouble() throws Exception {
        Document filter = new Document("id", 1);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(OptionalDouble.of(2.5)));

        ContinuableFuture<OptionalDouble> result = asyncExecutor.queryForDouble("amount", filter);
        Assertions.assertEquals(2.5, result.get().get(), 0.001);
    }

    @Test
    public void testQueryForString() throws Exception {
        Document filter = new Document("id", 1);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(Nullable.of("hello")));

        ContinuableFuture<Nullable<String>> result = asyncExecutor.queryForString("name", filter);
        Assertions.assertEquals("hello", result.get().get());
    }

    @Test
    public void testQueryForDate() throws Exception {
        Document filter = new Document("id", 1);
        Date now = new Date();
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(Nullable.of(now)));

        ContinuableFuture<Nullable<Date>> result = asyncExecutor.queryForDate("createdAt", filter);
        Assertions.assertEquals(now, result.get().get());
    }

    // ---- query / stream variants ----

    @Test
    public void testQueryWithFilterAndRowType() throws Exception {
        Document filter = new Document("status", "active");
        Dataset dataset = N.newEmptyDataset(Arrays.asList("id", "name"));
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(dataset));

        ContinuableFuture<Dataset> result = asyncExecutor.query(filter, TestEntity.class);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testQueryWithFilterOffsetCountAndRowType() throws Exception {
        Document filter = new Document("status", "active");
        Dataset dataset = N.newEmptyDataset(Arrays.asList("id", "name"));
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(dataset));

        ContinuableFuture<Dataset> result = asyncExecutor.query(filter, 0, 10, TestEntity.class);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testStreamWithFilterAndRowType() throws Exception {
        Document filter = new Document("status", "active");
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(stream));

        ContinuableFuture<Stream<TestEntity>> result = asyncExecutor.stream(filter, TestEntity.class);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testStreamWithFilterOffsetCountAndRowType() throws Exception {
        Document filter = new Document("status", "active");
        Stream<TestEntity> stream = Stream.empty();
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(stream));

        ContinuableFuture<Stream<TestEntity>> result = asyncExecutor.stream(filter, 0, 10, TestEntity.class);
        Assertions.assertNotNull(result.get());
    }

    // ---- watch variants ----

    @Test
    public void testWatchWithRowType() throws Exception {
        ChangeStreamIterable<TestEntity> changeStream = mock(ChangeStreamIterable.class);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(changeStream));

        ContinuableFuture<ChangeStreamIterable<TestEntity>> result = asyncExecutor.watch(TestEntity.class);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testWatchWithPipeline() throws Exception {
        ChangeStreamIterable<Document> changeStream = mock(ChangeStreamIterable.class);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(changeStream));

        ContinuableFuture<ChangeStreamIterable<Document>> result = asyncExecutor.watch(Arrays.asList(new Document("$match", new Document())));
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testWatchWithPipelineAndRowType() throws Exception {
        ChangeStreamIterable<TestEntity> changeStream = mock(ChangeStreamIterable.class);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(changeStream));

        ContinuableFuture<ChangeStreamIterable<TestEntity>> result =
                asyncExecutor.watch(Arrays.asList(new Document("$match", new Document())), TestEntity.class);
        Assertions.assertNotNull(result.get());
    }

    // ---- insert/update/delete with options ----

    @Test
    public void testInsertOneWithOptions() throws Exception {
        TestEntity entity = new TestEntity();
        InsertOneOptions options = new InsertOneOptions();
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(null));

        ContinuableFuture<Void> result = asyncExecutor.insertOne(entity, options);
        Assertions.assertNull(result.get());
    }

    @Test
    public void testInsertManyWithOptions() throws Exception {
        List<TestEntity> entities = Arrays.asList(new TestEntity());
        InsertManyOptions options = new InsertManyOptions();
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(null));

        ContinuableFuture<Void> result = asyncExecutor.insertMany(entities, options);
        Assertions.assertNull(result.get());
    }

    @Test
    public void testUpdateOneWithFilterAndUpdate() throws Exception {
        Document filter = new Document("status", "active");
        Document update = new Document("$set", new Document("name", "x"));
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(updateResult));

        ContinuableFuture<UpdateResult> result = asyncExecutor.updateOne(filter, (Object) update);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testUpdateOneWithFilterUpdateAndOptions() throws Exception {
        Document filter = new Document("status", "active");
        Document update = new Document("$set", new Document("name", "x"));
        UpdateOptions options = new UpdateOptions().upsert(true);
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(updateResult));

        ContinuableFuture<UpdateResult> result = asyncExecutor.updateOne(filter, update, options);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testUpdateOneWithFilterAndObjList() throws Exception {
        Document filter = new Document("status", "active");
        List<Document> updates = Arrays.asList(new Document("$set", new Document("a", 1)));
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(updateResult));

        ContinuableFuture<UpdateResult> result = asyncExecutor.updateOne(filter, updates);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testUpdateOneWithFilterObjListAndOptions() throws Exception {
        Document filter = new Document("status", "active");
        List<Document> updates = Arrays.asList(new Document("$set", new Document("a", 1)));
        UpdateOptions options = new UpdateOptions();
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(updateResult));

        ContinuableFuture<UpdateResult> result = asyncExecutor.updateOne(filter, updates, options);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testUpdateManyWithFilterUpdateAndOptions() throws Exception {
        Document filter = new Document("status", "active");
        Document update = new Document("$set", new Document("a", 1));
        UpdateOptions options = new UpdateOptions();
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(updateResult));

        ContinuableFuture<UpdateResult> result = asyncExecutor.updateMany(filter, update, options);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testUpdateManyWithFilterAndObjList() throws Exception {
        Document filter = new Document("status", "active");
        List<Document> updates = Arrays.asList(new Document("$set", new Document("a", 1)));
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(updateResult));

        ContinuableFuture<UpdateResult> result = asyncExecutor.updateMany(filter, updates);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testUpdateManyWithFilterObjListAndOptions() throws Exception {
        Document filter = new Document("status", "active");
        List<Document> updates = Arrays.asList(new Document("$set", new Document("a", 1)));
        UpdateOptions options = new UpdateOptions();
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(updateResult));

        ContinuableFuture<UpdateResult> result = asyncExecutor.updateMany(filter, updates, options);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testReplaceOneWithFilterReplacementAndOptions() throws Exception {
        Document filter = new Document("id", 1);
        Document replacement = new Document("name", "x");
        ReplaceOptions options = new ReplaceOptions();
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(updateResult));

        ContinuableFuture<UpdateResult> result = asyncExecutor.replaceOne(filter, replacement, options);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testDeleteOneWithFilterAndOptions() throws Exception {
        Document filter = new Document("status", "x");
        DeleteOptions options = new DeleteOptions();
        DeleteResult deleteResult = mock(DeleteResult.class);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(deleteResult));

        ContinuableFuture<DeleteResult> result = asyncExecutor.deleteOne(filter, options);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testDeleteManyWithFilterAndOptions() throws Exception {
        Document filter = new Document("status", "x");
        DeleteOptions options = new DeleteOptions();
        DeleteResult deleteResult = mock(DeleteResult.class);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(deleteResult));

        ContinuableFuture<DeleteResult> result = asyncExecutor.deleteMany(filter, options);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testBulkInsertWithOptions() throws Exception {
        List<TestEntity> entities = Arrays.asList(new TestEntity());
        BulkWriteOptions options = new BulkWriteOptions();
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(1));

        ContinuableFuture<Integer> result = asyncExecutor.bulkInsert(entities, options);
        Assertions.assertEquals(1, result.get());
    }

    @Test
    public void testBulkWriteWithOptions() throws Exception {
        List<WriteModel<Document>> requests = Arrays.asList();
        BulkWriteOptions options = new BulkWriteOptions();
        BulkWriteResult bulkWriteResult = mock(BulkWriteResult.class);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(bulkWriteResult));

        ContinuableFuture<BulkWriteResult> result = asyncExecutor.bulkWrite(requests, options);
        Assertions.assertNotNull(result.get());
    }

    // ---- findOneAnd* variants ----

    @Test
    public void testFindOneAndUpdateWithRowType() throws Exception {
        Document filter = new Document("id", 1);
        Document update = new Document("$set", new Document("a", 1));
        TestEntity entity = new TestEntity();
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(entity));

        ContinuableFuture<TestEntity> result = asyncExecutor.findOneAndUpdate(filter, update, TestEntity.class);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testFindOneAndUpdateWithOptions() throws Exception {
        Document filter = new Document("id", 1);
        Document update = new Document("$set", new Document("a", 1));
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        Document doc = new Document("a", 1);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(doc));

        ContinuableFuture<Document> result = asyncExecutor.findOneAndUpdate(filter, update, options);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testFindOneAndUpdateWithOptionsAndRowType() throws Exception {
        Document filter = new Document("id", 1);
        Document update = new Document("$set", new Document("a", 1));
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        TestEntity entity = new TestEntity();
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(entity));

        ContinuableFuture<TestEntity> result = asyncExecutor.findOneAndUpdate(filter, update, options, TestEntity.class);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testFindOneAndUpdateWithObjList() throws Exception {
        Document filter = new Document("id", 1);
        List<Document> updates = Arrays.asList(new Document("$set", new Document("a", 1)));
        Document doc = new Document("a", 1);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(doc));

        ContinuableFuture<Document> result = asyncExecutor.findOneAndUpdate(filter, updates);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testFindOneAndUpdateWithObjListAndRowType() throws Exception {
        Document filter = new Document("id", 1);
        List<Document> updates = Arrays.asList(new Document("$set", new Document("a", 1)));
        TestEntity entity = new TestEntity();
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(entity));

        ContinuableFuture<TestEntity> result = asyncExecutor.findOneAndUpdate(filter, updates, TestEntity.class);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testFindOneAndUpdateWithObjListAndOptions() throws Exception {
        Document filter = new Document("id", 1);
        List<Document> updates = Arrays.asList(new Document("$set", new Document("a", 1)));
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        Document doc = new Document("a", 1);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(doc));

        ContinuableFuture<Document> result = asyncExecutor.findOneAndUpdate(filter, updates, options);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testFindOneAndUpdateWithObjListOptionsAndRowType() throws Exception {
        Document filter = new Document("id", 1);
        List<Document> updates = Arrays.asList(new Document("$set", new Document("a", 1)));
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        TestEntity entity = new TestEntity();
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(entity));

        ContinuableFuture<TestEntity> result = asyncExecutor.findOneAndUpdate(filter, updates, options, TestEntity.class);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testFindOneAndReplaceWithRowType() throws Exception {
        Document filter = new Document("id", 1);
        Document replacement = new Document("name", "x");
        TestEntity entity = new TestEntity();
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(entity));

        ContinuableFuture<TestEntity> result = asyncExecutor.findOneAndReplace(filter, replacement, TestEntity.class);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testFindOneAndReplaceWithOptions() throws Exception {
        Document filter = new Document("id", 1);
        Document replacement = new Document("name", "x");
        FindOneAndReplaceOptions options = new FindOneAndReplaceOptions();
        Document doc = new Document("name", "x");
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(doc));

        ContinuableFuture<Document> result = asyncExecutor.findOneAndReplace(filter, replacement, options);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testFindOneAndReplaceWithOptionsAndRowType() throws Exception {
        Document filter = new Document("id", 1);
        Document replacement = new Document("name", "x");
        FindOneAndReplaceOptions options = new FindOneAndReplaceOptions();
        TestEntity entity = new TestEntity();
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(entity));

        ContinuableFuture<TestEntity> result = asyncExecutor.findOneAndReplace(filter, replacement, options, TestEntity.class);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testFindOneAndDeleteWithRowType() throws Exception {
        Document filter = new Document("id", 1);
        TestEntity entity = new TestEntity();
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(entity));

        ContinuableFuture<TestEntity> result = asyncExecutor.findOneAndDelete(filter, TestEntity.class);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testFindOneAndDeleteWithOptions() throws Exception {
        Document filter = new Document("id", 1);
        FindOneAndDeleteOptions options = new FindOneAndDeleteOptions();
        Document doc = new Document("id", 1);
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(doc));

        ContinuableFuture<Document> result = asyncExecutor.findOneAndDelete(filter, options);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testFindOneAndDeleteWithOptionsAndRowType() throws Exception {
        Document filter = new Document("id", 1);
        FindOneAndDeleteOptions options = new FindOneAndDeleteOptions();
        TestEntity entity = new TestEntity();
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(entity));

        ContinuableFuture<TestEntity> result = asyncExecutor.findOneAndDelete(filter, options, TestEntity.class);
        Assertions.assertNotNull(result.get());
    }

    // ---- distinct/aggregate/groupBy multi-field/mapReduce-with-rowType ----

    @Test
    public void testDistinctWithFilter() throws Exception {
        Document filter = new Document("active", true);
        Stream<String> stream = Stream.of("a", "b");
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(stream));

        ContinuableFuture<Stream<String>> result = asyncExecutor.distinct("category", filter, String.class);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testAggregateWithRowType() throws Exception {
        List<Document> pipeline = Arrays.asList(new Document("$match", new Document()));
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(stream));

        ContinuableFuture<Stream<TestEntity>> result = asyncExecutor.aggregate(pipeline, TestEntity.class);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testGroupByWithMultipleFields() throws Exception {
        Stream<Document> stream = Stream.<Document>of(new Document("_id", "x"));
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(stream));

        ContinuableFuture<Stream<Document>> result = asyncExecutor.groupBy(Arrays.asList("category", "status"));
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testGroupByAndCountWithMultipleFields() throws Exception {
        Stream<Document> stream = Stream.<Document>of(new Document("_id", "x").append("count", 1));
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(stream));

        ContinuableFuture<Stream<Document>> result = asyncExecutor.groupByAndCount(Arrays.asList("category", "status"));
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testMapReduceWithRowType() throws Exception {
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(stream));

        ContinuableFuture<Stream<TestEntity>> result = asyncExecutor.mapReduce("map", "reduce", TestEntity.class);
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