package com.landawn.abacus.da.mongodb;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.Dataset;
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
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

public class MongoCollectionExecutorTest extends TestBase {

    @Mock
    private MongoCollection<Document> mockCollection;

    @Mock
    private AsyncExecutor mockAsyncExecutor;

    @Mock
    private FindIterable<Document> mockFindIterable;

    @Mock
    private MongoCursor<Document> mockCursor;

    private MongoCollectionExecutor executor;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        executor = new MongoCollectionExecutor(mockCollection, mockAsyncExecutor);

        when(mockCollection.find(any(Bson.class))).thenReturn(mockFindIterable);
        // executeQuery() returns the iterable from coll.find(...) then chains fluent
        // modifiers (projection/sort/skip/limit) — unstubbed they return null and break
        // toEntity(...). Have each chained call return the same mock iterable.
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.sort(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.skip(anyInt())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);
        when(mockFindIterable.iterator()).thenReturn(mockCursor);
    }

    @Test
    public void testColl() {
        MongoCollection<Document> result = executor.mongoCollection();
        Assertions.assertSame(mockCollection, result);
    }

    @Test
    public void testAsync() {
        AsyncMongoCollectionExecutor asyncExecutor = executor.async();
        Assertions.assertNotNull(asyncExecutor);
    }

    @Test
    public void testExistsWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        when(mockCollection.countDocuments(any(), any(CountOptions.class))).thenReturn(1L);

        boolean result = executor.exists(objectId);
        Assertions.assertTrue(result);
    }

    @Test
    public void testExistsWithObjectId() {
        ObjectId objectId = new ObjectId();
        when(mockCollection.countDocuments(any(), any(CountOptions.class))).thenReturn(0L);

        boolean result = executor.exists(objectId);
        Assertions.assertFalse(result);
    }

    @Test
    public void testExistsWithBsonFilter() {
        Document filter = new Document("name", "test");
        when(mockCollection.countDocuments(any(), any(CountOptions.class))).thenReturn(1L);

        boolean result = executor.exists(filter);
        Assertions.assertTrue(result);
    }

    @Test
    public void testCount() {
        when(mockCollection.countDocuments()).thenReturn(100L);

        long result = executor.count();
        Assertions.assertEquals(100L, result);
    }

    @Test
    public void testCountWithFilter() {
        Document filter = new Document("status", "active");
        when(mockCollection.countDocuments(filter)).thenReturn(50L);

        long result = executor.count(filter);
        Assertions.assertEquals(50L, result);
    }

    @Test
    public void testCountWithFilterAndOptions() {
        Document filter = new Document("status", "active");
        CountOptions options = new CountOptions().limit(10);
        when(mockCollection.countDocuments(filter, options)).thenReturn(10L);

        long result = executor.count(filter, options);
        Assertions.assertEquals(10L, result);
    }

    @Test
    public void testEstimatedDocumentCount() {
        when(mockCollection.estimatedDocumentCount()).thenReturn(1000L);

        long result = executor.estimatedDocumentCount();
        Assertions.assertEquals(1000L, result);
    }

    @Test
    public void testEstimatedDocumentCountWithOptions() {
        EstimatedDocumentCountOptions options = new EstimatedDocumentCountOptions();
        when(mockCollection.estimatedDocumentCount(options)).thenReturn(1000L);

        long result = executor.estimatedDocumentCount(options);
        Assertions.assertEquals(1000L, result);
    }

    @Test
    public void testGetWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        Document doc = new Document("_id", new ObjectId(objectId));
        when(mockFindIterable.first()).thenReturn(doc);

        Optional<Document> result = executor.get(objectId);
        Assertions.assertTrue(result.isPresent());
    }

    @Test
    public void testGetWithObjectId() {
        ObjectId objectId = new ObjectId();
        when(mockFindIterable.first()).thenReturn(null);

        Optional<Document> result = executor.get(objectId);
        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testGetWithStringObjectIdAndRowType() {
        String objectId = "507f1f77bcf86cd799439011";
        Document doc = new Document("_id", new ObjectId(objectId)).append("name", "test");
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);

        Optional<TestEntity> result = executor.get(objectId, TestEntity.class);
        Assertions.assertTrue(result.isPresent());
    }

    @Test
    public void testGettWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        Document doc = new Document("_id", new ObjectId(objectId));
        when(mockFindIterable.first()).thenReturn(doc);

        Document result = executor.gett(objectId);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testGettWithObjectId() {
        ObjectId objectId = new ObjectId();
        when(mockFindIterable.first()).thenReturn(null);

        Document result = executor.gett(objectId);
        Assertions.assertNull(result);
    }

    @Test
    public void testFindFirst() {
        Document filter = new Document("name", "test");
        Document doc = new Document("name", "test");
        when(mockFindIterable.first()).thenReturn(doc);

        Optional<Document> result = executor.findFirst(filter);
        Assertions.assertTrue(result.isPresent());
    }

    @Test
    public void testFindFirstWithRowType() {
        Document filter = new Document("name", "test");
        Document doc = new Document("name", "test");
        when(mockFindIterable.first()).thenReturn(doc);

        Optional<Document> result = executor.findFirst(filter, Document.class);
        Assertions.assertTrue(result.isPresent());
    }

    @Test
    public void testList() {
        Document filter = new Document("status", "active");
        List<Document> docs = Arrays.asList(new Document("id", 1), new Document("id", 2));
        when(mockFindIterable.into(any())).thenReturn(docs);

        List<Document> result = executor.list(filter);
        Assertions.assertEquals(2, result.size());
    }

    @Test
    public void testListWithRowType() {
        Document filter = new Document("status", "active");
        List<Document> docs = Arrays.asList(new Document("id", 1), new Document("id", 2));
        when(mockFindIterable.into(any())).thenReturn(docs);

        List<Document> result = executor.list(filter, Document.class);
        Assertions.assertEquals(2, result.size());
    }

    @Test
    public void testQueryForBoolean() {
        String propName = "isActive";
        Document filter = new Document("id", 1);
        Document doc = new Document(propName, true);
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        OptionalBoolean result = executor.queryForBoolean(propName, filter);
        Assertions.assertTrue(result.orElseThrow());
    }

    @Test
    public void testQueryForChar() {
        String propName = "grade";
        Document filter = new Document("id", 1);
        Document doc = new Document(propName, 'A');
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        OptionalChar result = executor.queryForChar(propName, filter);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals('A', result.get());
    }

    @Test
    public void testQueryForByte() {
        String propName = "level";
        Document filter = new Document("id", 1);
        Document doc = new Document(propName, (byte) 5);
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        OptionalByte result = executor.queryForByte(propName, filter);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals((byte) 5, result.get());
    }

    @Test
    public void testQueryForShort() {
        String propName = "count";
        Document filter = new Document("id", 1);
        Document doc = new Document(propName, (short) 100);
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        OptionalShort result = executor.queryForShort(propName, filter);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals((short) 100, result.get());
    }

    @Test
    public void testQueryForInt() {
        String propName = "age";
        Document filter = new Document("id", 1);
        Document doc = new Document(propName, 25);
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        OptionalInt result = executor.queryForInt(propName, filter);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(25, result.get());
    }

    @Test
    public void testQueryForLong() {
        String propName = "timestamp";
        Document filter = new Document("id", 1);
        Document doc = new Document(propName, 1234567890L);
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        OptionalLong result = executor.queryForLong(propName, filter);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(1234567890L, result.get());
    }

    @Test
    public void testQueryForFloat() {
        String propName = "price";
        Document filter = new Document("id", 1);
        Document doc = new Document(propName, 19.99f);
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        OptionalFloat result = executor.queryForFloat(propName, filter);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(19.99f, result.get(), 0.01f);
    }

    @Test
    public void testQueryForDouble() {
        String propName = "amount";
        Document filter = new Document("id", 1);
        Document doc = new Document(propName, 99.99);
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        OptionalDouble result = executor.queryForDouble(propName, filter);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(99.99, result.get(), 0.01);
    }

    @Test
    public void testQueryForString() {
        String propName = "name";
        Document filter = new Document("id", 1);
        Document doc = new Document(propName, "John");
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        Nullable<String> result = executor.queryForString(propName, filter);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals("John", result.get());
    }

    @Test
    public void testQueryForDate() {
        String propName = "createdAt";
        Document filter = new Document("id", 1);
        Date date = new Date();
        Document doc = new Document(propName, date);
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        Nullable<Date> result = executor.queryForDate(propName, filter);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(date, result.get());
    }

    @Test
    public void testqueryForSingleValue() {
        String propName = "value";
        Document filter = new Document("id", 1);
        Document doc = new Document(propName, "result");
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        Nullable<String> result = executor.queryForSingleValue(propName, filter, String.class);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals("result", result.get());
    }

    @Test
    public void testQueryForSingleNonNull() {
        String propName = "value";
        Document filter = new Document("id", 1);
        Document doc = new Document(propName, "result");
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        Optional<String> result = executor.queryForSingleNonNull(propName, filter, String.class);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals("result", result.get());
    }

    @Test
    public void testQueryForSingleNonNullWithNullValue() {
        String propName = "value";
        Document filter = new Document("id", 1);
        Document doc = new Document(propName, null);
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        Optional<String> result = executor.queryForSingleNonNull(propName, filter, String.class);
        Assertions.assertFalse(result.isPresent());
    }

    @Test
    public void testQuery() {
        Document filter = new Document("status", "active");
        List<Document> docs = Arrays.asList(new Document("id", 1), new Document("id", 2));
        when(mockFindIterable.into(any())).thenReturn(docs);

        Dataset result = executor.query(filter);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testStream() {
        when(mockCollection.find()).thenReturn(mockFindIterable);

        Stream<Document> result = executor.stream();
        Assertions.assertNotNull(result);
    }

    @Test
    public void testStreamWithRowType() {
        when(mockCollection.find()).thenReturn(mockFindIterable);

        Stream<Document> result = executor.stream(Document.class);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testWatch() {
        ChangeStreamIterable<Document> changeStream = mock(ChangeStreamIterable.class);
        when(mockCollection.watch()).thenReturn(changeStream);

        ChangeStreamIterable<Document> result = executor.watch();
        Assertions.assertNotNull(result);
    }

    @Test
    public void testWatchWithRowType() {
        ChangeStreamIterable<TestEntity> changeStream = mock(ChangeStreamIterable.class);
        when(mockCollection.watch(TestEntity.class)).thenReturn(changeStream);

        ChangeStreamIterable<TestEntity> result = executor.watch(TestEntity.class);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testInsertOne() {
        Document doc = new Document("name", "test");

        executor.insertOne(doc);
        verify(mockCollection).insertOne(doc);
    }

    @Test
    public void testInsertOneWithOptions() {
        Document doc = new Document("name", "test");
        InsertOneOptions options = new InsertOneOptions();

        executor.insertOne(doc, options);
        verify(mockCollection).insertOne(doc, options);
    }

    @Test
    public void testInsertMany() {
        List<Document> docs = Arrays.asList(new Document("id", 1), new Document("id", 2));

        executor.insertMany(docs);
        verify(mockCollection).insertMany(docs);
    }

    @Test
    public void testInsertManyWithOptions() {
        List<Document> docs = Arrays.asList(new Document("id", 1), new Document("id", 2));
        InsertManyOptions options = new InsertManyOptions();

        executor.insertMany(docs, options);
        verify(mockCollection).insertMany(docs, options);
    }

    @Test
    public void testUpdateOneWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        Document update = new Document("name", "updated");
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockCollection.updateOne(any(Bson.class), any(Bson.class))).thenReturn(updateResult);

        UpdateResult result = executor.updateOne(objectId, update);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testUpdateMany() {
        Document filter = new Document("status", "active");
        Document update = new Document("status", "inactive");
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockCollection.updateMany(any(Bson.class), any(Bson.class))).thenReturn(updateResult);

        UpdateResult result = executor.updateMany(filter, update);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testReplaceOne() {
        String objectId = "507f1f77bcf86cd799439011";
        Document replacement = new Document("name", "replaced");
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockCollection.replaceOne(any(), any())).thenReturn(updateResult);

        UpdateResult result = executor.replaceOne(objectId, replacement);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testDeleteOne() {
        String objectId = "507f1f77bcf86cd799439011";
        DeleteResult deleteResult = mock(DeleteResult.class);
        when(mockCollection.deleteOne(any())).thenReturn(deleteResult);

        DeleteResult result = executor.deleteOne(objectId);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testDeleteMany() {
        Document filter = new Document("status", "inactive");
        DeleteResult deleteResult = mock(DeleteResult.class);
        when(mockCollection.deleteMany(filter)).thenReturn(deleteResult);

        DeleteResult result = executor.deleteMany(filter);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testBulkInsert() {
        List<Document> docs = Arrays.asList(new Document("id", 1), new Document("id", 2));
        BulkWriteResult bulkWriteResult = mock(BulkWriteResult.class);
        when(bulkWriteResult.getInsertedCount()).thenReturn(2);
        when(mockCollection.bulkWrite(anyList())).thenReturn(bulkWriteResult);

        int result = executor.bulkInsert(docs);
        Assertions.assertEquals(2, result);
    }

    @Test
    public void testBulkWrite() {
        List<WriteModel<Document>> requests = Arrays.asList(mock(WriteModel.class));
        BulkWriteResult bulkWriteResult = mock(BulkWriteResult.class);
        when(mockCollection.bulkWrite(requests)).thenReturn(bulkWriteResult);

        BulkWriteResult result = executor.bulkWrite(requests);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testBulkInsertRejectsNullEntities() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.bulkInsert(null));
    }

    @Test
    public void testBulkInsertRejectsEmptyEntities() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.bulkInsert(List.of()));
    }

    @Test
    public void testBulkWriteRejectsNullRequests() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.bulkWrite(null));
    }

    @Test
    public void testBulkWriteRejectsEmptyRequests() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.bulkWrite(List.of()));
    }

    @Test
    public void testFindOneAndUpdate() {
        Document filter = new Document("id", 1);
        Document update = new Document("name", "updated");
        Document updatedDoc = new Document("id", 1).append("name", "updated");
        when(mockCollection.findOneAndUpdate(any(Bson.class), any(Bson.class))).thenReturn(updatedDoc);

        Document result = executor.findOneAndUpdate(filter, update);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testFindOneAndUpdateWithObjListRejectsNullFilter() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.findOneAndUpdate(null, List.of(new Document("$set", new Document("a", 1)))));
    }

    @Test
    public void testFindOneAndUpdateWithObjListRejectsEmptyUpdates() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.findOneAndUpdate(new Document("id", 1), List.of()));
    }

    @Test
    public void testFindOneAndReplace() {
        Document filter = new Document("id", 1);
        Document replacement = new Document("id", 1).append("name", "replaced");
        when(mockCollection.findOneAndReplace(any(), any())).thenReturn(replacement);

        Document result = executor.findOneAndReplace(filter, replacement);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testFindOneAndDelete() {
        Document filter = new Document("id", 1);
        Document deletedDoc = new Document("id", 1);
        when(mockCollection.findOneAndDelete(filter)).thenReturn(deletedDoc);

        Document result = executor.findOneAndDelete(filter);
        Assertions.assertNotNull(result);
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
    public void testAggregate() {
        List<Document> pipeline = Arrays.asList(new Document("$match", new Document("status", "active")));
        when(mockCollection.aggregate(pipeline, Document.class)).thenReturn(mock());

        Stream<Document> result = executor.aggregate(pipeline);
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
    public void testGroupByWithMultipleFields() {
        Collection<String> fieldNames = Arrays.asList("category", "status");
        when(mockCollection.aggregate(anyList(), eq(Document.class))).thenReturn(mock());

        Stream<Document> result = executor.groupBy(fieldNames);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testGroupByAndCount() {
        String fieldName = "category";
        when(mockCollection.aggregate(anyList(), eq(Document.class))).thenReturn(mock());

        Stream<Document> result = executor.groupByAndCount(fieldName);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testMapReduce() {
        String mapFunction = "function() { emit(this.name, 1); }";
        String reduceFunction = "function(key, values) { return Array.sum(values); }";
        when(mockCollection.mapReduce(mapFunction, reduceFunction, Document.class)).thenReturn(mock());

        Stream<Document> result = executor.mapReduce(mapFunction, reduceFunction);
        Assertions.assertNotNull(result);
    }

    // Test entity class
    @Test
    public void testListWithZeroCountReturnsEmptyAndDoesNotUseLimitZero() {
        // Regression: limit(0) is treated by the MongoDB driver as "no limit" (return ALL docs).
        // A user-supplied count of 0 must yield zero results, not the whole collection.
        Document filter = new Document("status", "active");
        org.mockito.ArgumentCaptor<Bson> findCaptor = org.mockito.ArgumentCaptor.forClass(Bson.class);
        when(mockFindIterable.into(any())).thenReturn(Arrays.asList());

        List<Document> result = executor.list(filter, 0, 0, Document.class);

        Assertions.assertTrue(result.isEmpty());
        verify(mockCollection).find(findCaptor.capture());
        Assertions.assertEquals(new Document("$expr", false), findCaptor.getValue());
        // limit(0) must never be applied (it would mean "no limit").
        verify(mockFindIterable, org.mockito.Mockito.never()).limit(0);
    }

    @Test
    public void testUpdateOneWithDriverBuiltBsonNotWrappedInSet() {
        // Regression: a driver-built Bson (Updates.set(...)) is neither Document nor BasicDBObject.
        // It must be passed through as-is, not re-wrapped in {$set: ...} which corrupts the update.
        Document filter = new Document("name", "test");
        Bson update = com.mongodb.client.model.Updates.set("verified", true);
        UpdateResult updateResult = mock(UpdateResult.class);
        org.mockito.ArgumentCaptor<Bson> updateCaptor = org.mockito.ArgumentCaptor.forClass(Bson.class);
        when(mockCollection.updateOne(any(Bson.class), any(Bson.class))).thenReturn(updateResult);

        executor.updateOne(filter, update);

        verify(mockCollection).updateOne(any(Bson.class), updateCaptor.capture());
        Assertions.assertSame(update, updateCaptor.getValue());
        Assertions.assertFalse(updateCaptor.getValue() instanceof Document);
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
    public void testStreamWithCollectionProjectionRejectsNullRowTypeBeforeOpeningCursor() {
        // Regression: cursor-leak fix for the multi-arg stream overload.
        final Collection<String> selectPropNames = java.util.Collections.singletonList("name");
        final Document filter = new Document("status", "active");
        Assertions.assertThrows(IllegalArgumentException.class, () -> executor.stream(selectPropNames, filter, (Bson) null, 0, 10, (Class<Document>) null));

        verify(mockCollection, org.mockito.Mockito.never()).find(any(Bson.class));
        verify(mockCollection, org.mockito.Mockito.never()).find();
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
    public void testQueryForSingleNonNullReturnsEmptyOnMissingPropertyValue() {
        // When the matched document has no value for the requested property (vs. the value being null),
        // queryForSingleNonNull must return Optional.empty().
        Document filter = new Document("id", 1);
        Document doc = new Document("otherProp", "x"); // does not contain "value"
        when(mockFindIterable.first()).thenReturn(doc);
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);

        Optional<String> result = executor.queryForSingleNonNull("value", filter, String.class);

        Assertions.assertFalse(result.isPresent());
    }

    // ---- Additional coverage: list/query/stream overloads with selectPropNames / projection / pagination / sort ----

    @Test
    public void testListWithSelectPropNamesAndFilter() {
        Document filter = new Document("status", "active");
        List<Document> docs = Arrays.asList(new Document("name", "a"), new Document("name", "b"));
        when(mockFindIterable.into(any())).thenReturn(docs);

        List<Document> result = executor.list(Arrays.asList("name"), filter, Document.class);
        Assertions.assertEquals(2, result.size());
    }

    @Test
    public void testListWithSelectPropNamesFilterOffsetCount() {
        Document filter = new Document("status", "active");
        List<Document> docs = Arrays.asList(new Document("name", "a"));
        when(mockFindIterable.into(any())).thenReturn(docs);

        List<Document> result = executor.list(Arrays.asList("name"), filter, 0, 10, Document.class);
        Assertions.assertEquals(1, result.size());
    }

    @Test
    public void testListWithSelectPropNamesFilterAndSort() {
        Document filter = new Document("status", "active");
        Document sort = new Document("name", 1);
        List<Document> docs = Arrays.asList(new Document("name", "a"));
        when(mockFindIterable.into(any())).thenReturn(docs);

        List<Document> result = executor.list(Arrays.asList("name"), filter, sort, Document.class);
        Assertions.assertEquals(1, result.size());
    }

    @Test
    public void testListWithSelectPropNamesFilterSortOffsetCount() {
        Document filter = new Document("status", "active");
        Document sort = new Document("name", 1);
        List<Document> docs = Arrays.asList(new Document("name", "a"));
        when(mockFindIterable.into(any())).thenReturn(docs);

        List<Document> result = executor.list(Arrays.asList("name"), filter, sort, 0, 10, Document.class);
        Assertions.assertEquals(1, result.size());
    }

    @Test
    public void testListWithBsonProjectionFilterAndSort() {
        Bson projection = com.mongodb.client.model.Projections.include("name");
        Document filter = new Document("status", "active");
        Document sort = new Document("name", 1);
        List<Document> docs = Arrays.asList(new Document("name", "a"));
        when(mockFindIterable.into(any())).thenReturn(docs);

        List<Document> result = executor.list(projection, filter, sort, Document.class);
        Assertions.assertEquals(1, result.size());
    }

    @Test
    public void testListWithBsonProjectionFilterSortOffsetCount() {
        Bson projection = com.mongodb.client.model.Projections.include("name");
        Document filter = new Document("status", "active");
        Document sort = new Document("name", 1);
        List<Document> docs = Arrays.asList(new Document("name", "a"));
        when(mockFindIterable.into(any())).thenReturn(docs);

        List<Document> result = executor.list(projection, filter, sort, 0, 10, Document.class);
        Assertions.assertEquals(1, result.size());
    }

    @Test
    public void testQueryWithFilterAndRowType() {
        Document filter = new Document("status", "active");
        List<Document> docs = Arrays.asList(new Document("id", 1));
        when(mockFindIterable.into(any())).thenReturn(docs);

        Dataset result = executor.query(filter, Document.class);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testQueryWithFilterOffsetCountAndRowType() {
        Document filter = new Document("status", "active");
        List<Document> docs = Arrays.asList(new Document("id", 1));
        when(mockFindIterable.into(any())).thenReturn(docs);

        Dataset result = executor.query(filter, 0, 10, Document.class);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testQueryWithSelectPropNamesAndFilter() {
        Document filter = new Document("status", "active");
        List<Document> docs = Arrays.asList(new Document("id", 1));
        when(mockFindIterable.into(any())).thenReturn(docs);

        Dataset result = executor.query(Arrays.asList("id"), filter, Document.class);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testQueryWithBsonProjectionFilterSort() {
        Bson projection = com.mongodb.client.model.Projections.include("name");
        Document filter = new Document("status", "active");
        Document sort = new Document("name", 1);
        List<Document> docs = Arrays.asList(new Document("name", "a"));
        when(mockFindIterable.into(any())).thenReturn(docs);

        Dataset result = executor.query(projection, filter, sort, Document.class);
        Assertions.assertNotNull(result);
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
    public void testStreamWithSelectPropNamesAndFilter() {
        Document filter = new Document("status", "active");
        when(mockFindIterable.iterator()).thenReturn(mockCursor);

        Stream<Document> result = executor.stream(Arrays.asList("name"), filter, Document.class);
        Assertions.assertNotNull(result);
        result.close();
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

    // ---- findOneAndUpdate / Replace / Delete with options & rowType ----

    @Test
    public void testFindOneAndUpdateWithRowType() {
        Document filter = new Document("id", 1);
        Document update = new Document("$set", new Document("a", 1));
        Document returnedDoc = new Document("a", 1);
        when(mockCollection.findOneAndUpdate(any(Bson.class), any(Bson.class))).thenReturn(returnedDoc);

        Document result = executor.findOneAndUpdate(filter, update, Document.class);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testFindOneAndUpdateWithOptions() {
        Document filter = new Document("id", 1);
        Document update = new Document("$set", new Document("a", 1));
        com.mongodb.client.model.FindOneAndUpdateOptions options = new com.mongodb.client.model.FindOneAndUpdateOptions();
        Document returnedDoc = new Document("a", 1);
        when(mockCollection.findOneAndUpdate(any(Bson.class), any(Bson.class), any(com.mongodb.client.model.FindOneAndUpdateOptions.class)))
                .thenReturn(returnedDoc);

        Document result = executor.findOneAndUpdate(filter, update, options);
        Assertions.assertNotNull(result);
        verify(mockCollection).findOneAndUpdate(any(Bson.class), any(Bson.class), eq(options));
    }

    @Test
    public void testFindOneAndUpdateWithOptionsAndRowType() {
        Document filter = new Document("id", 1);
        Document update = new Document("$set", new Document("a", 1));
        com.mongodb.client.model.FindOneAndUpdateOptions options = new com.mongodb.client.model.FindOneAndUpdateOptions();
        Document returnedDoc = new Document("a", 1);
        when(mockCollection.findOneAndUpdate(any(Bson.class), any(Bson.class), any(com.mongodb.client.model.FindOneAndUpdateOptions.class)))
                .thenReturn(returnedDoc);

        Document result = executor.findOneAndUpdate(filter, update, options, Document.class);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testFindOneAndUpdateWithCollection() {
        Document filter = new Document("id", 1);
        List<Document> updates = Arrays.asList(new Document("$set", new Document("a", 1)));
        Document returnedDoc = new Document("a", 1);
        when(mockCollection.findOneAndUpdate(any(Bson.class), anyList())).thenReturn(returnedDoc);

        Document result = executor.findOneAndUpdate(filter, updates);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testFindOneAndUpdateWithCollectionAndOptions() {
        Document filter = new Document("id", 1);
        List<Document> updates = Arrays.asList(new Document("$set", new Document("a", 1)));
        com.mongodb.client.model.FindOneAndUpdateOptions options = new com.mongodb.client.model.FindOneAndUpdateOptions();
        Document returnedDoc = new Document("a", 1);
        when(mockCollection.findOneAndUpdate(any(Bson.class), anyList(), any(com.mongodb.client.model.FindOneAndUpdateOptions.class))).thenReturn(returnedDoc);

        Document result = executor.findOneAndUpdate(filter, updates, options);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testFindOneAndUpdateWithCollectionOptionsAndRowType() {
        Document filter = new Document("id", 1);
        List<Document> updates = Arrays.asList(new Document("$set", new Document("a", 1)));
        com.mongodb.client.model.FindOneAndUpdateOptions options = new com.mongodb.client.model.FindOneAndUpdateOptions();
        Document returnedDoc = new Document("a", 1);
        when(mockCollection.findOneAndUpdate(any(Bson.class), anyList(), any(com.mongodb.client.model.FindOneAndUpdateOptions.class))).thenReturn(returnedDoc);

        Document result = executor.findOneAndUpdate(filter, updates, options, Document.class);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testFindOneAndReplaceWithRowType() {
        Document filter = new Document("id", 1);
        Document replacement = new Document("name", "x");
        Document returnedDoc = new Document("name", "x");
        when(mockCollection.findOneAndReplace(any(Bson.class), any(Document.class))).thenReturn(returnedDoc);

        Document result = executor.findOneAndReplace(filter, replacement, Document.class);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testFindOneAndReplaceWithOptions() {
        Document filter = new Document("id", 1);
        Document replacement = new Document("name", "x");
        com.mongodb.client.model.FindOneAndReplaceOptions options = new com.mongodb.client.model.FindOneAndReplaceOptions();
        Document returnedDoc = new Document("name", "x");
        when(mockCollection.findOneAndReplace(any(Bson.class), any(Document.class), any(com.mongodb.client.model.FindOneAndReplaceOptions.class)))
                .thenReturn(returnedDoc);

        Document result = executor.findOneAndReplace(filter, replacement, options);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testFindOneAndReplaceWithOptionsAndRowType() {
        Document filter = new Document("id", 1);
        Document replacement = new Document("name", "x");
        com.mongodb.client.model.FindOneAndReplaceOptions options = new com.mongodb.client.model.FindOneAndReplaceOptions();
        Document returnedDoc = new Document("name", "x");
        when(mockCollection.findOneAndReplace(any(Bson.class), any(Document.class), any(com.mongodb.client.model.FindOneAndReplaceOptions.class)))
                .thenReturn(returnedDoc);

        Document result = executor.findOneAndReplace(filter, replacement, options, Document.class);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testFindOneAndDeleteWithRowType() {
        Document filter = new Document("id", 1);
        Document returnedDoc = new Document("id", 1);
        when(mockCollection.findOneAndDelete(any(Bson.class))).thenReturn(returnedDoc);

        Document result = executor.findOneAndDelete(filter, Document.class);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testFindOneAndDeleteWithOptions() {
        Document filter = new Document("id", 1);
        com.mongodb.client.model.FindOneAndDeleteOptions options = new com.mongodb.client.model.FindOneAndDeleteOptions();
        Document returnedDoc = new Document("id", 1);
        when(mockCollection.findOneAndDelete(any(Bson.class), any(com.mongodb.client.model.FindOneAndDeleteOptions.class))).thenReturn(returnedDoc);

        Document result = executor.findOneAndDelete(filter, options);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testFindOneAndDeleteWithOptionsAndRowType() {
        Document filter = new Document("id", 1);
        com.mongodb.client.model.FindOneAndDeleteOptions options = new com.mongodb.client.model.FindOneAndDeleteOptions();
        Document returnedDoc = new Document("id", 1);
        when(mockCollection.findOneAndDelete(any(Bson.class), any(com.mongodb.client.model.FindOneAndDeleteOptions.class))).thenReturn(returnedDoc);

        Document result = executor.findOneAndDelete(filter, options, Document.class);
        Assertions.assertNotNull(result);
    }

    // ---- distinct/update/delete/get with collection projection ----

    @Test
    public void testDistinctWithFilter() {
        String fieldName = "category";
        Document filter = new Document("active", true);
        when(mockCollection.distinct(eq(fieldName), eq(filter), eq(String.class))).thenReturn(mock());

        Stream<String> result = executor.distinct(fieldName, filter, String.class);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testGetWithSelectPropNamesAndRowType() {
        String objectId = "507f1f77bcf86cd799439011";
        Document doc = new Document("_id", new ObjectId(objectId)).append("name", "test");
        when(mockFindIterable.first()).thenReturn(doc);

        Optional<TestEntity> result = executor.get(objectId, Arrays.asList("name"), TestEntity.class);
        Assertions.assertTrue(result.isPresent());
    }

    @Test
    public void testGettWithSelectPropNamesAndRowType() {
        String objectId = "507f1f77bcf86cd799439011";
        Document doc = new Document("_id", new ObjectId(objectId)).append("name", "test");
        when(mockFindIterable.first()).thenReturn(doc);

        TestEntity result = executor.gett(objectId, Arrays.asList("name"), TestEntity.class);
        Assertions.assertNotNull(result);
    }

    // ---- toBson collection path: collection of non-Bson entities triggers conversion via MongoDBBase ----

    @Test
    public void testUpdateOneWithCollection() {
        Document filter = new Document("id", 1);
        List<Document> updates = Arrays.asList(new Document("$set", new Document("a", 1)));
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockCollection.updateOne(any(Bson.class), anyList())).thenReturn(updateResult);

        UpdateResult result = executor.updateOne(filter, updates);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testUpdateOneWithCollectionAndOptions() {
        Document filter = new Document("id", 1);
        List<Document> updates = Arrays.asList(new Document("$set", new Document("a", 1)));
        com.mongodb.client.model.UpdateOptions options = new com.mongodb.client.model.UpdateOptions();
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockCollection.updateOne(any(Bson.class), anyList(), any(com.mongodb.client.model.UpdateOptions.class))).thenReturn(updateResult);

        UpdateResult result = executor.updateOne(filter, updates, options);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testUpdateManyWithCollection() {
        Document filter = new Document("id", 1);
        List<Document> updates = Arrays.asList(new Document("$set", new Document("a", 1)));
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockCollection.updateMany(any(Bson.class), anyList())).thenReturn(updateResult);

        UpdateResult result = executor.updateMany(filter, updates);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testUpdateManyWithCollectionAndOptions() {
        Document filter = new Document("id", 1);
        List<Document> updates = Arrays.asList(new Document("$set", new Document("a", 1)));
        com.mongodb.client.model.UpdateOptions options = new com.mongodb.client.model.UpdateOptions();
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockCollection.updateMany(any(Bson.class), anyList(), any(com.mongodb.client.model.UpdateOptions.class))).thenReturn(updateResult);

        UpdateResult result = executor.updateMany(filter, updates, options);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testDeleteOneWithFilterAndOptions() {
        Document filter = new Document("status", "x");
        com.mongodb.client.model.DeleteOptions options = new com.mongodb.client.model.DeleteOptions();
        DeleteResult deleteResult = mock(DeleteResult.class);
        when(mockCollection.deleteOne(any(Bson.class), any(com.mongodb.client.model.DeleteOptions.class))).thenReturn(deleteResult);

        DeleteResult result = executor.deleteOne(filter, options);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testDeleteManyWithFilterAndOptions() {
        Document filter = new Document("status", "x");
        com.mongodb.client.model.DeleteOptions options = new com.mongodb.client.model.DeleteOptions();
        DeleteResult deleteResult = mock(DeleteResult.class);
        when(mockCollection.deleteMany(any(Bson.class), any(com.mongodb.client.model.DeleteOptions.class))).thenReturn(deleteResult);

        DeleteResult result = executor.deleteMany(filter, options);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testWatchWithPipeline() {
        List<Document> pipeline = Arrays.asList(new Document("$match", new Document()));
        ChangeStreamIterable<Document> changeStream = mock(ChangeStreamIterable.class);
        when(mockCollection.watch(pipeline)).thenReturn(changeStream);

        ChangeStreamIterable<Document> result = executor.watch(pipeline);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testWatchWithPipelineAndRowType() {
        List<Document> pipeline = Arrays.asList(new Document("$match", new Document()));
        ChangeStreamIterable<Document> changeStream = mock(ChangeStreamIterable.class);
        when(mockCollection.watch(pipeline, Document.class)).thenReturn(changeStream);

        ChangeStreamIterable<Document> result = executor.watch(pipeline, Document.class);
        Assertions.assertNotNull(result);
    }

    private static class TestEntity {
        private String id;
        private String name;

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
