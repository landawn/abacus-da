package com.landawn.abacus.da.mongodb;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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

        ContinuableFuture<ChangeStreamIterable<TestEntity>> result = asyncExecutor.watch(Arrays.asList(new Document("$match", new Document())),
                TestEntity.class);
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
        Stream<Document> stream = Stream.<Document> of(new Document("_id", "x"));
        when(mockAsyncExecutor.execute(any(Callable.class))).thenReturn(ContinuableFuture.completed(stream));

        ContinuableFuture<Stream<Document>> result = asyncExecutor.groupBy(Arrays.asList("category", "status"));
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testGroupByAndCountWithMultipleFields() throws Exception {
        Stream<Document> stream = Stream.<Document> of(new Document("_id", "x").append("count", 1));
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

    // ---- lambda coverage: actually run the Callable so the inner lambda bodies execute ----

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void stubRunCallable() {
        doAnswer(invocation -> {
            Callable callable = invocation.getArgument(0);
            return ContinuableFuture.completed(callable.call());
        }).when(mockAsyncExecutor).execute(any(Callable.class));
    }

    @Test
    public void testExistsWithStringObjectId_LambdaRuns() throws Exception {
        stubRunCallable();
        String objectId = "507f1f77bcf86cd799439011";
        when(mockCollExecutor.exists(objectId)).thenReturn(true);

        ContinuableFuture<Boolean> result = asyncExecutor.exists(objectId);
        Assertions.assertTrue(result.get());
        verify(mockCollExecutor).exists(objectId);
    }

    @Test
    public void testExistsWithObjectId_LambdaRuns() throws Exception {
        stubRunCallable();
        ObjectId objectId = new ObjectId();
        when(mockCollExecutor.exists(objectId)).thenReturn(false);

        ContinuableFuture<Boolean> result = asyncExecutor.exists(objectId);
        Assertions.assertFalse(result.get());
    }

    @Test
    public void testExistsWithFilter_LambdaRuns() throws Exception {
        stubRunCallable();
        Document filter = new Document("name", "test");
        when(mockCollExecutor.exists(filter)).thenReturn(true);

        ContinuableFuture<Boolean> result = asyncExecutor.exists(filter);
        Assertions.assertTrue(result.get());
    }

    @Test
    public void testCount_LambdaRuns() throws Exception {
        stubRunCallable();
        when(mockCollExecutor.count()).thenReturn(42L);

        ContinuableFuture<Long> result = asyncExecutor.count();
        Assertions.assertEquals(42L, result.get());
    }

    @Test
    public void testCountWithFilter_LambdaRuns() throws Exception {
        stubRunCallable();
        Document filter = new Document("x", 1);
        when(mockCollExecutor.count(filter)).thenReturn(7L);

        ContinuableFuture<Long> result = asyncExecutor.count(filter);
        Assertions.assertEquals(7L, result.get());
    }

    @Test
    public void testCountWithFilterAndOptions_LambdaRuns() throws Exception {
        stubRunCallable();
        Document filter = new Document("x", 1);
        CountOptions options = new CountOptions();
        when(mockCollExecutor.count(filter, options)).thenReturn(3L);

        ContinuableFuture<Long> result = asyncExecutor.count(filter, options);
        Assertions.assertEquals(3L, result.get());
    }

    @Test
    public void testGetStringObjectId_LambdaRuns() throws Exception {
        stubRunCallable();
        String objectId = "507f1f77bcf86cd799439011";
        when(mockCollExecutor.get(objectId)).thenReturn(Optional.<Document> empty());

        ContinuableFuture<Optional<Document>> result = asyncExecutor.get(objectId);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testGetObjectId_LambdaRuns() throws Exception {
        stubRunCallable();
        ObjectId objectId = new ObjectId();
        when(mockCollExecutor.get(objectId)).thenReturn(Optional.<Document> empty());

        ContinuableFuture<Optional<Document>> result = asyncExecutor.get(objectId);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testGetStringObjectIdRowType_LambdaRuns() throws Exception {
        stubRunCallable();
        String objectId = "507f1f77bcf86cd799439011";
        when(mockCollExecutor.get(objectId, TestEntity.class)).thenReturn(Optional.<TestEntity> empty());

        ContinuableFuture<Optional<TestEntity>> result = asyncExecutor.get(objectId, TestEntity.class);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testGetObjectIdRowType_LambdaRuns() throws Exception {
        stubRunCallable();
        ObjectId objectId = new ObjectId();
        when(mockCollExecutor.get(objectId, TestEntity.class)).thenReturn(Optional.<TestEntity> empty());

        ContinuableFuture<Optional<TestEntity>> result = asyncExecutor.get(objectId, TestEntity.class);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testGetStringObjectIdSelectPropsRowType_LambdaRuns() throws Exception {
        stubRunCallable();
        String objectId = "507f1f77bcf86cd799439011";
        List<String> selectProps = Arrays.asList("name");
        when(mockCollExecutor.get(objectId, selectProps, TestEntity.class)).thenReturn(Optional.<TestEntity> empty());

        ContinuableFuture<Optional<TestEntity>> result = asyncExecutor.get(objectId, selectProps, TestEntity.class);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testGetObjectIdSelectPropsRowType_LambdaRuns() throws Exception {
        stubRunCallable();
        ObjectId objectId = new ObjectId();
        List<String> selectProps = Arrays.asList("name");
        when(mockCollExecutor.get(objectId, selectProps, TestEntity.class)).thenReturn(Optional.<TestEntity> empty());

        ContinuableFuture<Optional<TestEntity>> result = asyncExecutor.get(objectId, selectProps, TestEntity.class);
        Assertions.assertNotNull(result.get());
    }

    @Test
    public void testGettStringObjectId_LambdaRuns() throws Exception {
        stubRunCallable();
        String objectId = "507f1f77bcf86cd799439011";
        when(mockCollExecutor.gett(objectId)).thenReturn(null);

        ContinuableFuture<Document> result = asyncExecutor.gett(objectId);
        Assertions.assertNull(result.get());
    }

    @Test
    public void testGettObjectId_LambdaRuns() throws Exception {
        stubRunCallable();
        ObjectId objectId = new ObjectId();
        when(mockCollExecutor.gett(objectId)).thenReturn(null);

        ContinuableFuture<Document> result = asyncExecutor.gett(objectId);
        Assertions.assertNull(result.get());
    }

    @Test
    public void testGettStringObjectIdRowType_LambdaRuns() throws Exception {
        stubRunCallable();
        String objectId = "507f1f77bcf86cd799439011";
        TestEntity entity = new TestEntity();
        when(mockCollExecutor.gett(objectId, TestEntity.class)).thenReturn(entity);

        ContinuableFuture<TestEntity> result = asyncExecutor.gett(objectId, TestEntity.class);
        Assertions.assertSame(entity, result.get());
    }

    @Test
    public void testGettObjectIdRowType_LambdaRuns() throws Exception {
        stubRunCallable();
        ObjectId objectId = new ObjectId();
        TestEntity entity = new TestEntity();
        when(mockCollExecutor.gett(objectId, TestEntity.class)).thenReturn(entity);

        ContinuableFuture<TestEntity> result = asyncExecutor.gett(objectId, TestEntity.class);
        Assertions.assertSame(entity, result.get());
    }

    @Test
    public void testGettStringObjectIdSelectPropsRowType_LambdaRuns() throws Exception {
        stubRunCallable();
        String objectId = "507f1f77bcf86cd799439011";
        List<String> selectProps = Arrays.asList("name");
        TestEntity entity = new TestEntity();
        when(mockCollExecutor.gett(objectId, selectProps, TestEntity.class)).thenReturn(entity);

        ContinuableFuture<TestEntity> result = asyncExecutor.gett(objectId, selectProps, TestEntity.class);
        Assertions.assertSame(entity, result.get());
    }

    @Test
    public void testGettObjectIdSelectPropsRowType_LambdaRuns() throws Exception {
        stubRunCallable();
        ObjectId objectId = new ObjectId();
        List<String> selectProps = Arrays.asList("name");
        TestEntity entity = new TestEntity();
        when(mockCollExecutor.gett(objectId, selectProps, TestEntity.class)).thenReturn(entity);

        ContinuableFuture<TestEntity> result = asyncExecutor.gett(objectId, selectProps, TestEntity.class);
        Assertions.assertSame(entity, result.get());
    }

    @Test
    public void testQueryForPrimitives_LambdaRuns() throws Exception {
        stubRunCallable();
        Document filter = new Document("k", "v");
        when(mockCollExecutor.queryForBoolean("p", filter)).thenReturn(OptionalBoolean.of(true));
        when(mockCollExecutor.queryForChar("p", filter)).thenReturn(OptionalChar.of('a'));
        when(mockCollExecutor.queryForByte("p", filter)).thenReturn(OptionalByte.of((byte) 1));
        when(mockCollExecutor.queryForShort("p", filter)).thenReturn(OptionalShort.of((short) 1));
        when(mockCollExecutor.queryForInt("p", filter)).thenReturn(OptionalInt.of(1));
        when(mockCollExecutor.queryForLong("p", filter)).thenReturn(OptionalLong.of(1L));
        when(mockCollExecutor.queryForFloat("p", filter)).thenReturn(OptionalFloat.of(1f));
        when(mockCollExecutor.queryForDouble("p", filter)).thenReturn(OptionalDouble.of(1d));
        when(mockCollExecutor.queryForString("p", filter)).thenReturn(Nullable.of("s"));

        Assertions.assertTrue(asyncExecutor.queryForBoolean("p", filter).get().orElse(false));
        Assertions.assertEquals('a', asyncExecutor.queryForChar("p", filter).get().orElse('z'));
        Assertions.assertEquals((byte) 1, asyncExecutor.queryForByte("p", filter).get().orElse((byte) 0));
        Assertions.assertEquals((short) 1, asyncExecutor.queryForShort("p", filter).get().orElse((short) 0));
        Assertions.assertEquals(1, asyncExecutor.queryForInt("p", filter).get().orElse(0));
        Assertions.assertEquals(1L, asyncExecutor.queryForLong("p", filter).get().orElse(0L));
        Assertions.assertEquals(1f, asyncExecutor.queryForFloat("p", filter).get().orElse(0f));
        Assertions.assertEquals(1d, asyncExecutor.queryForDouble("p", filter).get().orElse(0d));
        Assertions.assertEquals("s", asyncExecutor.queryForString("p", filter).get().orElse(null));
    }

    @Test
    public void testQueryForDate_LambdaRuns() throws Exception {
        stubRunCallable();
        Document filter = new Document("k", "v");
        Date d = new Date();
        when(mockCollExecutor.queryForDate("p", filter)).thenReturn(Nullable.of(d));

        Assertions.assertSame(d, asyncExecutor.queryForDate("p", filter).get().orElse(null));
    }

    @Test
    public void testQueryForDateWithValueType_LambdaRuns() throws Exception {
        stubRunCallable();
        Document filter = new Document("k", "v");
        java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
        when(mockCollExecutor.queryForDate("p", filter, java.sql.Timestamp.class)).thenReturn(Nullable.of(ts));

        Assertions.assertSame(ts, asyncExecutor.queryForDate("p", filter, java.sql.Timestamp.class).get().orElse(null));
    }

    @Test
    public void testQueryForSingleValue_LambdaRuns() throws Exception {
        stubRunCallable();
        Document filter = new Document("k", "v");
        when(mockCollExecutor.queryForSingleValue("p", filter, String.class)).thenReturn(Nullable.of("hello"));

        Assertions.assertEquals("hello", asyncExecutor.queryForSingleValue("p", filter, String.class).get().orElse(null));
    }

    @Test
    public void testQueryForSingleNonNull_LambdaRuns() throws Exception {
        stubRunCallable();
        Document filter = new Document("k", "v");
        when(mockCollExecutor.queryForSingleNonNull("p", filter, String.class)).thenReturn(Optional.of("hello"));

        Assertions.assertEquals("hello", asyncExecutor.queryForSingleNonNull("p", filter, String.class).get().orElse(null));
    }

    @Test
    public void testQueryWithProjFilterSortRowType_LambdaRuns() throws Exception {
        stubRunCallable();
        Document proj = new Document("name", 1);
        Document filter = new Document("k", "v");
        Document sort = new Document("name", 1);
        Dataset dataset = N.newEmptyDataset(Arrays.asList("id"));
        when(mockCollExecutor.query(proj, filter, sort, TestEntity.class)).thenReturn(dataset);

        ContinuableFuture<Dataset> result = asyncExecutor.query(proj, filter, sort, TestEntity.class);
        Assertions.assertSame(dataset, result.get());
    }

    @Test
    public void testQueryWithProjFilterSortOffsetCountRowType_LambdaRuns() throws Exception {
        stubRunCallable();
        Document proj = new Document("name", 1);
        Document filter = new Document("k", "v");
        Document sort = new Document("name", 1);
        Dataset dataset = N.newEmptyDataset(Arrays.asList("id"));
        when(mockCollExecutor.query(proj, filter, sort, 0, 5, TestEntity.class)).thenReturn(dataset);

        ContinuableFuture<Dataset> result = asyncExecutor.query(proj, filter, sort, 0, 5, TestEntity.class);
        Assertions.assertSame(dataset, result.get());
    }

    @Test
    public void testQueryWithSelectFilterRowType_LambdaRuns() throws Exception {
        stubRunCallable();
        List<String> select = Arrays.asList("id");
        Document filter = new Document("k", "v");
        Dataset dataset = N.newEmptyDataset(Arrays.asList("id"));
        when(mockCollExecutor.query(select, filter, TestEntity.class)).thenReturn(dataset);

        ContinuableFuture<Dataset> result = asyncExecutor.query(select, filter, TestEntity.class);
        Assertions.assertSame(dataset, result.get());
    }

    @Test
    public void testQueryWithSelectFilterOffsetCountRowType_LambdaRuns() throws Exception {
        stubRunCallable();
        List<String> select = Arrays.asList("id");
        Document filter = new Document("k", "v");
        Dataset dataset = N.newEmptyDataset(Arrays.asList("id"));
        when(mockCollExecutor.query(select, filter, 0, 5, TestEntity.class)).thenReturn(dataset);

        ContinuableFuture<Dataset> result = asyncExecutor.query(select, filter, 0, 5, TestEntity.class);
        Assertions.assertSame(dataset, result.get());
    }

    @Test
    public void testQueryWithSelectFilterSortRowType_LambdaRuns() throws Exception {
        stubRunCallable();
        List<String> select = Arrays.asList("id");
        Document filter = new Document("k", "v");
        Document sort = new Document("k", 1);
        Dataset dataset = N.newEmptyDataset(Arrays.asList("id"));
        when(mockCollExecutor.query(select, filter, sort, TestEntity.class)).thenReturn(dataset);

        ContinuableFuture<Dataset> result = asyncExecutor.query(select, filter, sort, TestEntity.class);
        Assertions.assertSame(dataset, result.get());
    }

    @Test
    public void testQueryWithSelectFilterSortOffsetCountRowType_LambdaRuns() throws Exception {
        stubRunCallable();
        List<String> select = Arrays.asList("id");
        Document filter = new Document("k", "v");
        Document sort = new Document("k", 1);
        Dataset dataset = N.newEmptyDataset(Arrays.asList("id"));
        when(mockCollExecutor.query(select, filter, sort, 0, 5, TestEntity.class)).thenReturn(dataset);

        ContinuableFuture<Dataset> result = asyncExecutor.query(select, filter, sort, 0, 5, TestEntity.class);
        Assertions.assertSame(dataset, result.get());
    }

    @Test
    public void testStreamWithSelectFilterRowType_LambdaRuns() throws Exception {
        stubRunCallable();
        List<String> select = Arrays.asList("id");
        Document filter = new Document("k", "v");
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        when(mockCollExecutor.stream(select, filter, TestEntity.class)).thenReturn(stream);

        ContinuableFuture<Stream<TestEntity>> result = asyncExecutor.stream(select, filter, TestEntity.class);
        Assertions.assertSame(stream, result.get());
    }

    @Test
    public void testStreamWithSelectFilterOffsetCountRowType_LambdaRuns() throws Exception {
        stubRunCallable();
        List<String> select = Arrays.asList("id");
        Document filter = new Document("k", "v");
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        when(mockCollExecutor.stream(select, filter, 0, 5, TestEntity.class)).thenReturn(stream);

        ContinuableFuture<Stream<TestEntity>> result = asyncExecutor.stream(select, filter, 0, 5, TestEntity.class);
        Assertions.assertSame(stream, result.get());
    }

    @Test
    public void testStreamWithSelectFilterSortRowType_LambdaRuns() throws Exception {
        stubRunCallable();
        List<String> select = Arrays.asList("id");
        Document filter = new Document("k", "v");
        Document sort = new Document("k", 1);
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        when(mockCollExecutor.stream(select, filter, sort, TestEntity.class)).thenReturn(stream);

        ContinuableFuture<Stream<TestEntity>> result = asyncExecutor.stream(select, filter, sort, TestEntity.class);
        Assertions.assertSame(stream, result.get());
    }

    @Test
    public void testStreamWithSelectFilterSortOffsetCountRowType_LambdaRuns() throws Exception {
        stubRunCallable();
        List<String> select = Arrays.asList("id");
        Document filter = new Document("k", "v");
        Document sort = new Document("k", 1);
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        when(mockCollExecutor.stream(select, filter, sort, 0, 5, TestEntity.class)).thenReturn(stream);

        ContinuableFuture<Stream<TestEntity>> result = asyncExecutor.stream(select, filter, sort, 0, 5, TestEntity.class);
        Assertions.assertSame(stream, result.get());
    }

    @Test
    public void testStreamWithProjFilterSortRowType_LambdaRuns() throws Exception {
        stubRunCallable();
        Document proj = new Document("k", 1);
        Document filter = new Document("k", "v");
        Document sort = new Document("k", 1);
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        when(mockCollExecutor.stream(proj, filter, sort, TestEntity.class)).thenReturn(stream);

        ContinuableFuture<Stream<TestEntity>> result = asyncExecutor.stream(proj, filter, sort, TestEntity.class);
        Assertions.assertSame(stream, result.get());
    }

    @Test
    public void testStreamWithProjFilterSortOffsetCountRowType_LambdaRuns() throws Exception {
        stubRunCallable();
        Document proj = new Document("k", 1);
        Document filter = new Document("k", "v");
        Document sort = new Document("k", 1);
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        when(mockCollExecutor.stream(proj, filter, sort, 0, 5, TestEntity.class)).thenReturn(stream);

        ContinuableFuture<Stream<TestEntity>> result = asyncExecutor.stream(proj, filter, sort, 0, 5, TestEntity.class);
        Assertions.assertSame(stream, result.get());
    }

    @Test
    public void testInsertOne_LambdaRuns() throws Exception {
        stubRunCallable();
        TestEntity entity = new TestEntity();

        ContinuableFuture<Void> result = asyncExecutor.insertOne(entity);
        result.get();
        verify(mockCollExecutor).insertOne(entity);
    }

    @Test
    public void testInsertOneWithOptions_LambdaRuns() throws Exception {
        stubRunCallable();
        TestEntity entity = new TestEntity();
        InsertOneOptions opts = new InsertOneOptions();

        ContinuableFuture<Void> result = asyncExecutor.insertOne(entity, opts);
        result.get();
        verify(mockCollExecutor).insertOne(entity, opts);
    }

    @Test
    public void testInsertMany_LambdaRuns() throws Exception {
        stubRunCallable();
        List<TestEntity> entities = Arrays.asList(new TestEntity());

        ContinuableFuture<Void> result = asyncExecutor.insertMany(entities);
        result.get();
        verify(mockCollExecutor).insertMany(entities);
    }

    @Test
    public void testInsertManyWithOptions_LambdaRuns() throws Exception {
        stubRunCallable();
        List<TestEntity> entities = Arrays.asList(new TestEntity());
        InsertManyOptions opts = new InsertManyOptions();

        ContinuableFuture<Void> result = asyncExecutor.insertMany(entities, opts);
        result.get();
        verify(mockCollExecutor).insertMany(entities, opts);
    }

    @Test
    public void testUpdateOneWithFilterAndOptions_LambdaRuns() throws Exception {
        stubRunCallable();
        Document filter = new Document("k", "v");
        Document update = new Document("$set", new Document("k", "w"));
        UpdateOptions opts = new UpdateOptions();
        UpdateResult ur = mock(UpdateResult.class);
        when(mockCollExecutor.updateOne(filter, update, opts)).thenReturn(ur);

        Assertions.assertSame(ur, asyncExecutor.updateOne(filter, update, opts).get());
    }

    @Test
    public void testUpdateOneWithFilterAndCollection_LambdaRuns() throws Exception {
        stubRunCallable();
        Document filter = new Document("k", "v");
        List<TestEntity> objList = Arrays.asList(new TestEntity());
        UpdateResult ur = mock(UpdateResult.class);
        when(mockCollExecutor.updateOne(filter, objList)).thenReturn(ur);

        Assertions.assertSame(ur, asyncExecutor.updateOne(filter, objList).get());
    }

    @Test
    public void testUpdateOneWithFilterCollectionAndOptions_LambdaRuns() throws Exception {
        stubRunCallable();
        Document filter = new Document("k", "v");
        List<TestEntity> objList = Arrays.asList(new TestEntity());
        UpdateOptions opts = new UpdateOptions();
        UpdateResult ur = mock(UpdateResult.class);
        when(mockCollExecutor.updateOne(filter, objList, opts)).thenReturn(ur);

        Assertions.assertSame(ur, asyncExecutor.updateOne(filter, objList, opts).get());
    }

    @Test
    public void testUpdateManyWithOptions_LambdaRuns() throws Exception {
        stubRunCallable();
        Document filter = new Document("k", "v");
        Document update = new Document("$set", new Document("k", "w"));
        UpdateOptions opts = new UpdateOptions();
        UpdateResult ur = mock(UpdateResult.class);
        when(mockCollExecutor.updateMany(filter, update, opts)).thenReturn(ur);

        Assertions.assertSame(ur, asyncExecutor.updateMany(filter, update, opts).get());
    }

    @Test
    public void testUpdateManyWithCollection_LambdaRuns() throws Exception {
        stubRunCallable();
        Document filter = new Document("k", "v");
        List<TestEntity> objList = Arrays.asList(new TestEntity());
        UpdateResult ur = mock(UpdateResult.class);
        when(mockCollExecutor.updateMany(filter, objList)).thenReturn(ur);

        Assertions.assertSame(ur, asyncExecutor.updateMany(filter, objList).get());
    }

    @Test
    public void testUpdateManyWithCollectionAndOptions_LambdaRuns() throws Exception {
        stubRunCallable();
        Document filter = new Document("k", "v");
        List<TestEntity> objList = Arrays.asList(new TestEntity());
        UpdateOptions opts = new UpdateOptions();
        UpdateResult ur = mock(UpdateResult.class);
        when(mockCollExecutor.updateMany(filter, objList, opts)).thenReturn(ur);

        Assertions.assertSame(ur, asyncExecutor.updateMany(filter, objList, opts).get());
    }

    @Test
    public void testReplaceOneWithObjectId_LambdaRuns() throws Exception {
        stubRunCallable();
        ObjectId objectId = new ObjectId();
        Document replacement = new Document("k", "v");
        UpdateResult ur = mock(UpdateResult.class);
        when(mockCollExecutor.replaceOne(objectId, replacement)).thenReturn(ur);

        Assertions.assertSame(ur, asyncExecutor.replaceOne(objectId, replacement).get());
    }

    @Test
    public void testReplaceOneWithFilterAndOptions_LambdaRuns() throws Exception {
        stubRunCallable();
        Document filter = new Document("k", "v");
        Document replacement = new Document("k", "v");
        ReplaceOptions opts = new ReplaceOptions();
        UpdateResult ur = mock(UpdateResult.class);
        when(mockCollExecutor.replaceOne(filter, replacement, opts)).thenReturn(ur);

        Assertions.assertSame(ur, asyncExecutor.replaceOne(filter, replacement, opts).get());
    }

    @Test
    public void testDeleteOneWithFilterAndOptions_LambdaRuns() throws Exception {
        stubRunCallable();
        Document filter = new Document("k", "v");
        DeleteOptions opts = new DeleteOptions();
        DeleteResult dr = mock(DeleteResult.class);
        when(mockCollExecutor.deleteOne(filter, opts)).thenReturn(dr);

        Assertions.assertSame(dr, asyncExecutor.deleteOne(filter, opts).get());
    }

    @Test
    public void testDeleteManyWithFilterAndOptions_LambdaRuns() throws Exception {
        stubRunCallable();
        Document filter = new Document("k", "v");
        DeleteOptions opts = new DeleteOptions();
        DeleteResult dr = mock(DeleteResult.class);
        when(mockCollExecutor.deleteMany(filter, opts)).thenReturn(dr);

        Assertions.assertSame(dr, asyncExecutor.deleteMany(filter, opts).get());
    }

    @Test
    public void testBulkWrite_LambdaRuns() throws Exception {
        stubRunCallable();
        List<WriteModel<? extends Document>> requests = Collections.emptyList();
        BulkWriteResult br = mock(BulkWriteResult.class);
        when(mockCollExecutor.bulkWrite(requests)).thenReturn(br);

        Assertions.assertSame(br, asyncExecutor.bulkWrite(requests).get());
    }

    @Test
    public void testBulkWriteWithOptions_LambdaRuns() throws Exception {
        stubRunCallable();
        List<WriteModel<? extends Document>> requests = Collections.emptyList();
        BulkWriteOptions opts = new BulkWriteOptions();
        BulkWriteResult br = mock(BulkWriteResult.class);
        when(mockCollExecutor.bulkWrite(requests, opts)).thenReturn(br);

        Assertions.assertSame(br, asyncExecutor.bulkWrite(requests, opts).get());
    }

    @Test
    public void testFindOneAndUpdateRowType_LambdaRuns() throws Exception {
        stubRunCallable();
        Document filter = new Document("k", "v");
        Document update = new Document("$set", new Document("k", "w"));
        TestEntity entity = new TestEntity();
        when(mockCollExecutor.findOneAndUpdate(filter, update, TestEntity.class)).thenReturn(entity);

        Assertions.assertSame(entity, asyncExecutor.findOneAndUpdate(filter, update, TestEntity.class).get());
    }

    @Test
    public void testFindOneAndUpdateOptionsRowType_LambdaRuns() throws Exception {
        stubRunCallable();
        Document filter = new Document("k", "v");
        Document update = new Document("$set", new Document("k", "w"));
        FindOneAndUpdateOptions opts = new FindOneAndUpdateOptions();
        TestEntity entity = new TestEntity();
        when(mockCollExecutor.findOneAndUpdate(filter, update, opts, TestEntity.class)).thenReturn(entity);

        Assertions.assertSame(entity, asyncExecutor.findOneAndUpdate(filter, update, opts, TestEntity.class).get());
    }

    @Test
    public void testFindOneAndUpdateWithCollection_LambdaRuns() throws Exception {
        stubRunCallable();
        Document filter = new Document("k", "v");
        List<TestEntity> objList = Arrays.asList(new TestEntity());
        Document doc = new Document();
        when(mockCollExecutor.findOneAndUpdate(filter, objList)).thenReturn(doc);

        Assertions.assertSame(doc, asyncExecutor.findOneAndUpdate(filter, objList).get());
    }

    @Test
    public void testFindOneAndUpdateWithCollectionRowType_LambdaRuns() throws Exception {
        stubRunCallable();
        Document filter = new Document("k", "v");
        List<TestEntity> objList = Arrays.asList(new TestEntity());
        TestEntity entity = new TestEntity();
        when(mockCollExecutor.findOneAndUpdate(filter, objList, TestEntity.class)).thenReturn(entity);

        Assertions.assertSame(entity, asyncExecutor.findOneAndUpdate(filter, objList, TestEntity.class).get());
    }

    @Test
    public void testFindOneAndUpdateWithCollectionAndOptions_LambdaRuns() throws Exception {
        stubRunCallable();
        Document filter = new Document("k", "v");
        List<TestEntity> objList = Arrays.asList(new TestEntity());
        FindOneAndUpdateOptions opts = new FindOneAndUpdateOptions();
        Document doc = new Document();
        when(mockCollExecutor.findOneAndUpdate(filter, objList, opts)).thenReturn(doc);

        Assertions.assertSame(doc, asyncExecutor.findOneAndUpdate(filter, objList, opts).get());
    }

    @Test
    public void testFindOneAndUpdateWithCollectionOptionsAndRowType_LambdaRuns() throws Exception {
        stubRunCallable();
        Document filter = new Document("k", "v");
        List<TestEntity> objList = Arrays.asList(new TestEntity());
        FindOneAndUpdateOptions opts = new FindOneAndUpdateOptions();
        TestEntity entity = new TestEntity();
        when(mockCollExecutor.findOneAndUpdate(filter, objList, opts, TestEntity.class)).thenReturn(entity);

        Assertions.assertSame(entity, asyncExecutor.findOneAndUpdate(filter, objList, opts, TestEntity.class).get());
    }

    @Test
    public void testFindOneAndReplaceRowType_LambdaRuns() throws Exception {
        stubRunCallable();
        Document filter = new Document("k", "v");
        Document replacement = new Document("k", "x");
        TestEntity entity = new TestEntity();
        when(mockCollExecutor.findOneAndReplace(filter, replacement, TestEntity.class)).thenReturn(entity);

        Assertions.assertSame(entity, asyncExecutor.findOneAndReplace(filter, replacement, TestEntity.class).get());
    }

    @Test
    public void testFindOneAndReplaceOptionsRowType_LambdaRuns() throws Exception {
        stubRunCallable();
        Document filter = new Document("k", "v");
        Document replacement = new Document("k", "x");
        FindOneAndReplaceOptions opts = new FindOneAndReplaceOptions();
        TestEntity entity = new TestEntity();
        when(mockCollExecutor.findOneAndReplace(filter, replacement, opts, TestEntity.class)).thenReturn(entity);

        Assertions.assertSame(entity, asyncExecutor.findOneAndReplace(filter, replacement, opts, TestEntity.class).get());
    }

    @Test
    public void testFindOneAndDeleteOptionsRowType_LambdaRuns() throws Exception {
        stubRunCallable();
        Document filter = new Document("k", "v");
        FindOneAndDeleteOptions opts = new FindOneAndDeleteOptions();
        TestEntity entity = new TestEntity();
        when(mockCollExecutor.findOneAndDelete(filter, opts, TestEntity.class)).thenReturn(entity);

        Assertions.assertSame(entity, asyncExecutor.findOneAndDelete(filter, opts, TestEntity.class).get());
    }

    @Test
    public void testDistinct_LambdaRuns() throws Exception {
        stubRunCallable();
        Stream<String> s = Stream.of("a", "b");
        when(mockCollExecutor.distinct("f", String.class)).thenReturn(s);

        Assertions.assertSame(s, asyncExecutor.distinct("f", String.class).get());
    }

    @Test
    public void testWatch_LambdaRuns() throws Exception {
        stubRunCallable();
        ChangeStreamIterable<Document> csi = mock(ChangeStreamIterable.class);
        when(mockCollExecutor.watch()).thenReturn(csi);

        Assertions.assertSame(csi, asyncExecutor.watch().get());
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