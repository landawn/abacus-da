package com.landawn.abacus.da.mongodb;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.landawn.abacus.da.TestBase;
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
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;

public class MongoCollectionMapperTest extends TestBase {

    @Mock
    private MongoCollectionExecutor mockCollExecutor;

    private MongoCollectionMapper<TestEntity> mapper;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        mapper = new MongoCollectionMapper<>(mockCollExecutor, TestEntity.class);
    }

    @Test
    public void testCollExecutor() {
        MongoCollectionExecutor result = mapper.collectionExecutor();
        Assertions.assertSame(mockCollExecutor, result);
    }

    @Test
    public void testExistsWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        when(mockCollExecutor.exists(objectId)).thenReturn(true);

        boolean result = mapper.exists(objectId);
        Assertions.assertTrue(result);
        verify(mockCollExecutor).exists(objectId);
    }

    @Test
    public void testExistsWithObjectId() {
        ObjectId objectId = new ObjectId();
        when(mockCollExecutor.exists(objectId)).thenReturn(false);

        boolean result = mapper.exists(objectId);
        Assertions.assertFalse(result);
        verify(mockCollExecutor).exists(objectId);
    }

    @Test
    public void testExistsWithBsonFilter() {
        Document filter = new Document("name", "test");
        when(mockCollExecutor.exists(filter)).thenReturn(true);

        boolean result = mapper.exists(filter);
        Assertions.assertTrue(result);
        verify(mockCollExecutor).exists(filter);
    }

    @Test
    public void testCount() {
        when(mockCollExecutor.count()).thenReturn(100L);

        long result = mapper.count();
        Assertions.assertEquals(100L, result);
        verify(mockCollExecutor).count();
    }

    @Test
    public void testCountWithFilter() {
        Document filter = new Document("status", "active");
        when(mockCollExecutor.count(filter)).thenReturn(50L);

        long result = mapper.count(filter);
        Assertions.assertEquals(50L, result);
        verify(mockCollExecutor).count(filter);
    }

    @Test
    public void testCountWithFilterAndOptions() {
        Document filter = new Document("status", "active");
        CountOptions options = new CountOptions();
        when(mockCollExecutor.count(filter, options)).thenReturn(25L);

        long result = mapper.count(filter, options);
        Assertions.assertEquals(25L, result);
        verify(mockCollExecutor).count(filter, options);
    }

    @Test
    public void testGetWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        TestEntity entity = new TestEntity();
        when(mockCollExecutor.get(objectId, TestEntity.class)).thenReturn(Optional.of(entity));

        Optional<TestEntity> result = mapper.get(objectId);
        Assertions.assertTrue(result.isPresent());
        verify(mockCollExecutor).get(objectId, TestEntity.class);
    }

    @Test
    public void testGetWithObjectId() {
        ObjectId objectId = new ObjectId();
        when(mockCollExecutor.get(objectId, TestEntity.class)).thenReturn(Optional.empty());

        Optional<TestEntity> result = mapper.get(objectId);
        Assertions.assertFalse(result.isPresent());
        verify(mockCollExecutor).get(objectId, TestEntity.class);
    }

    @Test
    public void testGetWithSelectPropNames() {
        String objectId = "507f1f77bcf86cd799439011";
        Collection<String> selectPropNames = Arrays.asList("name", "status");
        TestEntity entity = new TestEntity();
        when(mockCollExecutor.get(objectId, selectPropNames, TestEntity.class)).thenReturn(Optional.of(entity));

        Optional<TestEntity> result = mapper.get(objectId, selectPropNames);
        Assertions.assertTrue(result.isPresent());
        verify(mockCollExecutor).get(objectId, selectPropNames, TestEntity.class);
    }

    @Test
    public void testGettWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        TestEntity entity = new TestEntity();
        when(mockCollExecutor.gett(objectId, TestEntity.class)).thenReturn(entity);

        TestEntity result = mapper.gett(objectId);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).gett(objectId, TestEntity.class);
    }

    @Test
    public void testFindFirst() {
        Document filter = new Document("name", "test");
        TestEntity entity = new TestEntity();
        when(mockCollExecutor.findFirst(filter, TestEntity.class)).thenReturn(Optional.of(entity));

        Optional<TestEntity> result = mapper.findFirst(filter);
        Assertions.assertTrue(result.isPresent());
        verify(mockCollExecutor).findFirst(filter, TestEntity.class);
    }

    @Test
    public void testList() {
        Document filter = new Document("status", "active");
        List<TestEntity> entities = Arrays.asList(new TestEntity(), new TestEntity());
        when(mockCollExecutor.list(filter, TestEntity.class)).thenReturn(entities);

        List<TestEntity> result = mapper.list(filter);
        Assertions.assertEquals(2, result.size());
        verify(mockCollExecutor).list(filter, TestEntity.class);
    }

    @Test
    public void testListWithPagination() {
        Document filter = new Document("status", "active");
        int offset = 10;
        int count = 5;
        List<TestEntity> entities = Arrays.asList(new TestEntity());
        when(mockCollExecutor.list(filter, offset, count, TestEntity.class)).thenReturn(entities);

        List<TestEntity> result = mapper.list(filter, offset, count);
        Assertions.assertEquals(1, result.size());
        verify(mockCollExecutor).list(filter, offset, count, TestEntity.class);
    }

    @Test
    public void testQueryForBoolean() {
        String propName = "isActive";
        Document filter = new Document("id", 1);
        when(mockCollExecutor.queryForBoolean(propName, filter)).thenReturn(OptionalBoolean.of(true));

        OptionalBoolean result = mapper.queryForBoolean(propName, filter);
        Assertions.assertTrue(result.orElseThrow());
        verify(mockCollExecutor).queryForBoolean(propName, filter);
    }

    @Test
    public void testQueryForChar() {
        String propName = "grade";
        Document filter = new Document("id", 1);
        when(mockCollExecutor.queryForChar(propName, filter)).thenReturn(OptionalChar.of('A'));

        OptionalChar result = mapper.queryForChar(propName, filter);
        Assertions.assertEquals('A', result.get());
        verify(mockCollExecutor).queryForChar(propName, filter);
    }

    @Test
    public void testQueryForByte() {
        String propName = "level";
        Document filter = new Document("id", 1);
        when(mockCollExecutor.queryForByte(propName, filter)).thenReturn(OptionalByte.of((byte) 5));

        OptionalByte result = mapper.queryForByte(propName, filter);
        Assertions.assertEquals((byte) 5, result.get());
        verify(mockCollExecutor).queryForByte(propName, filter);
    }

    @Test
    public void testQueryForShort() {
        String propName = "count";
        Document filter = new Document("id", 1);
        when(mockCollExecutor.queryForShort(propName, filter)).thenReturn(OptionalShort.of((short) 100));

        OptionalShort result = mapper.queryForShort(propName, filter);
        Assertions.assertEquals((short) 100, result.get());
        verify(mockCollExecutor).queryForShort(propName, filter);
    }

    @Test
    public void testQueryForInt() {
        String propName = "age";
        Document filter = new Document("id", 1);
        when(mockCollExecutor.queryForInt(propName, filter)).thenReturn(OptionalInt.of(25));

        OptionalInt result = mapper.queryForInt(propName, filter);
        Assertions.assertEquals(25, result.get());
        verify(mockCollExecutor).queryForInt(propName, filter);
    }

    @Test
    public void testQueryForLong() {
        String propName = "timestamp";
        Document filter = new Document("id", 1);
        when(mockCollExecutor.queryForLong(propName, filter)).thenReturn(OptionalLong.of(1234567890L));

        OptionalLong result = mapper.queryForLong(propName, filter);
        Assertions.assertEquals(1234567890L, result.get());
        verify(mockCollExecutor).queryForLong(propName, filter);
    }

    @Test
    public void testQueryForFloat() {
        String propName = "price";
        Document filter = new Document("id", 1);
        when(mockCollExecutor.queryForFloat(propName, filter)).thenReturn(OptionalFloat.of(19.99f));

        OptionalFloat result = mapper.queryForFloat(propName, filter);
        Assertions.assertEquals(19.99f, result.get(), 0.01f);
        verify(mockCollExecutor).queryForFloat(propName, filter);
    }

    @Test
    public void testQueryForDouble() {
        String propName = "amount";
        Document filter = new Document("id", 1);
        when(mockCollExecutor.queryForDouble(propName, filter)).thenReturn(OptionalDouble.of(99.99));

        OptionalDouble result = mapper.queryForDouble(propName, filter);
        Assertions.assertEquals(99.99, result.get(), 0.01);
        verify(mockCollExecutor).queryForDouble(propName, filter);
    }

    @Test
    public void testQueryForString() {
        String propName = "name";
        Document filter = new Document("id", 1);
        when(mockCollExecutor.queryForString(propName, filter)).thenReturn(Nullable.of("John"));

        Nullable<String> result = mapper.queryForString(propName, filter);
        Assertions.assertEquals("John", result.get());
        verify(mockCollExecutor).queryForString(propName, filter);
    }

    @Test
    public void testQueryForDate() {
        String propName = "createdAt";
        Document filter = new Document("id", 1);
        Date date = new Date();
        when(mockCollExecutor.queryForDate(propName, filter)).thenReturn(Nullable.of(date));

        Nullable<Date> result = mapper.queryForDate(propName, filter);
        Assertions.assertEquals(date, result.get());
        verify(mockCollExecutor).queryForDate(propName, filter);
    }

    @Test
    public void testqueryForSingleValue() {
        String propName = "value";
        Document filter = new Document("id", 1);
        when(mockCollExecutor.queryForSingleValue(propName, filter, String.class)).thenReturn(Nullable.of("result"));

        Nullable<String> result = mapper.queryForSingleValue(propName, filter, String.class);
        Assertions.assertEquals("result", result.get());
        verify(mockCollExecutor).queryForSingleValue(propName, filter, String.class);
    }

    @Test
    public void testqueryForSingleNonNull() {
        String propName = "value";
        Document filter = new Document("id", 1);
        when(mockCollExecutor.queryForSingleNonNull(propName, filter, String.class)).thenReturn(Optional.of("result"));

        Optional<String> result = mapper.queryForSingleNonNull(propName, filter, String.class);
        Assertions.assertEquals("result", result.get());
        verify(mockCollExecutor).queryForSingleNonNull(propName, filter, String.class);
    }

    @Test
    public void testQuery() {
        Document filter = new Document("status", "active");
        Dataset dataset = N.newEmptyDataset(Arrays.asList("id", "name"));
        when(mockCollExecutor.query(filter, TestEntity.class)).thenReturn(dataset);

        Dataset result = mapper.query(filter);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).query(filter, TestEntity.class);
    }

    @Test
    public void testStream() {
        Document filter = new Document("status", "active");
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        when(mockCollExecutor.stream(filter, TestEntity.class)).thenReturn(stream);

        Stream<TestEntity> result = mapper.stream(filter);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).stream(filter, TestEntity.class);
    }

    @Test
    public void testInsertOne() {
        TestEntity entity = new TestEntity();
        InsertOneResult insertResult = mock(InsertOneResult.class);
        when(mockCollExecutor.insertOne(entity)).thenReturn(insertResult);

        InsertOneResult result = mapper.insertOne(entity);
        verify(mockCollExecutor).insertOne(entity);
        Assertions.assertSame(insertResult, result);
    }

    @Test
    public void testInsertOneWithOptions() {
        TestEntity entity = new TestEntity();
        InsertOneOptions options = new InsertOneOptions();
        InsertOneResult insertResult = mock(InsertOneResult.class);
        when(mockCollExecutor.insertOne(entity, options)).thenReturn(insertResult);

        InsertOneResult result = mapper.insertOne(entity, options);
        verify(mockCollExecutor).insertOne(entity, options);
        Assertions.assertSame(insertResult, result);
    }

    @Test
    public void testInsertMany() {
        Collection<TestEntity> entities = Arrays.asList(new TestEntity(), new TestEntity());
        InsertManyResult insertResult = mock(InsertManyResult.class);
        when(mockCollExecutor.insertMany(entities)).thenReturn(insertResult);

        InsertManyResult result = mapper.insertMany(entities);
        verify(mockCollExecutor).insertMany(entities);
        Assertions.assertSame(insertResult, result);
    }

    @Test
    public void testInsertManyWithOptions() {
        Collection<TestEntity> entities = Arrays.asList(new TestEntity(), new TestEntity());
        InsertManyOptions options = new InsertManyOptions();
        InsertManyResult insertResult = mock(InsertManyResult.class);
        when(mockCollExecutor.insertMany(entities, options)).thenReturn(insertResult);

        InsertManyResult result = mapper.insertMany(entities, options);
        verify(mockCollExecutor).insertMany(entities, options);
        Assertions.assertSame(insertResult, result);
    }

    @Test
    public void testUpdateOneWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        TestEntity update = new TestEntity();
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockCollExecutor.updateOne(objectId, update)).thenReturn(updateResult);

        UpdateResult result = mapper.updateOne(objectId, update);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).updateOne(objectId, update);
    }

    @Test
    public void testUpdateOneWithObjectId() {
        ObjectId objectId = new ObjectId();
        TestEntity update = new TestEntity();
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockCollExecutor.updateOne(objectId, update)).thenReturn(updateResult);

        UpdateResult result = mapper.updateOne(objectId, update);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).updateOne(objectId, update);
    }

    @Test
    public void testUpdateOneWithFilter() {
        Document filter = new Document("id", 1);
        TestEntity update = new TestEntity();
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockCollExecutor.updateOne(filter, update)).thenReturn(updateResult);

        UpdateResult result = mapper.updateOne(filter, update);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).updateOne(filter, update);
    }

    @Test
    public void testUpdateOneWithFilterAndOptions() {
        Document filter = new Document("id", 1);
        TestEntity update = new TestEntity();
        UpdateOptions options = new UpdateOptions();
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockCollExecutor.updateOne(filter, update, options)).thenReturn(updateResult);

        UpdateResult result = mapper.updateOne(filter, update, options);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).updateOne(filter, update, options);
    }

    @Test
    public void testUpdateOneWithFilterAndCollection() {
        Document filter = new Document("id", 1);
        Collection<TestEntity> objList = Arrays.asList(new TestEntity());
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockCollExecutor.updateOne(filter, objList)).thenReturn(updateResult);

        UpdateResult result = mapper.updateOne(filter, objList);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).updateOne(filter, objList);
    }

    @Test
    public void testUpdateMany() {
        Document filter = new Document("status", "active");
        TestEntity update = new TestEntity();
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockCollExecutor.updateMany(filter, update)).thenReturn(updateResult);

        UpdateResult result = mapper.updateMany(filter, update);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).updateMany(filter, update);
    }

    @Test
    public void testUpdateManyWithOptions() {
        Document filter = new Document("status", "active");
        TestEntity update = new TestEntity();
        UpdateOptions options = new UpdateOptions();
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockCollExecutor.updateMany(filter, update, options)).thenReturn(updateResult);

        UpdateResult result = mapper.updateMany(filter, update, options);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).updateMany(filter, update, options);
    }

    @Test
    public void testReplaceOneWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        TestEntity replacement = new TestEntity();
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockCollExecutor.replaceOne(objectId, replacement)).thenReturn(updateResult);

        UpdateResult result = mapper.replaceOne(objectId, replacement);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).replaceOne(objectId, replacement);
    }

    @Test
    public void testReplaceOneWithFilter() {
        Document filter = new Document("id", 1);
        TestEntity replacement = new TestEntity();
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockCollExecutor.replaceOne(filter, replacement)).thenReturn(updateResult);

        UpdateResult result = mapper.replaceOne(filter, replacement);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).replaceOne(filter, replacement);
    }

    @Test
    public void testReplaceOneWithOptions() {
        Document filter = new Document("id", 1);
        TestEntity replacement = new TestEntity();
        ReplaceOptions options = new ReplaceOptions();
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockCollExecutor.replaceOne(filter, replacement, options)).thenReturn(updateResult);

        UpdateResult result = mapper.replaceOne(filter, replacement, options);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).replaceOne(filter, replacement, options);
    }

    @Test
    public void testDeleteOneWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        DeleteResult deleteResult = mock(DeleteResult.class);
        when(mockCollExecutor.deleteOne(objectId)).thenReturn(deleteResult);

        DeleteResult result = mapper.deleteOne(objectId);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).deleteOne(objectId);
    }

    @Test
    public void testDeleteOneWithObjectId() {
        ObjectId objectId = new ObjectId();
        DeleteResult deleteResult = mock(DeleteResult.class);
        when(mockCollExecutor.deleteOne(objectId)).thenReturn(deleteResult);

        DeleteResult result = mapper.deleteOne(objectId);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).deleteOne(objectId);
    }

    @Test
    public void testDeleteOneWithFilter() {
        Document filter = new Document("id", 1);
        DeleteResult deleteResult = mock(DeleteResult.class);
        when(mockCollExecutor.deleteOne(filter)).thenReturn(deleteResult);

        DeleteResult result = mapper.deleteOne(filter);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).deleteOne(filter);
    }

    @Test
    public void testDeleteMany() {
        Document filter = new Document("status", "inactive");
        DeleteResult deleteResult = mock(DeleteResult.class);
        when(mockCollExecutor.deleteMany(filter)).thenReturn(deleteResult);

        DeleteResult result = mapper.deleteMany(filter);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).deleteMany(filter);
    }

    @Test
    public void testBulkInsert() {
        Collection<TestEntity> entities = Arrays.asList(new TestEntity(), new TestEntity());
        BulkWriteResult bulkWriteResult = mock(BulkWriteResult.class);
        when(mockCollExecutor.bulkInsert(entities)).thenReturn(bulkWriteResult);

        BulkWriteResult result = mapper.bulkInsert(entities);
        Assertions.assertSame(bulkWriteResult, result);
        verify(mockCollExecutor).bulkInsert(entities);
    }

    @Test
    public void testBulkInsertWithOptions() {
        Collection<TestEntity> entities = Arrays.asList(new TestEntity(), new TestEntity());
        BulkWriteOptions options = new BulkWriteOptions();
        BulkWriteResult bulkWriteResult = mock(BulkWriteResult.class);
        when(mockCollExecutor.bulkInsert(entities, options)).thenReturn(bulkWriteResult);

        BulkWriteResult result = mapper.bulkInsert(entities, options);
        Assertions.assertSame(bulkWriteResult, result);
        verify(mockCollExecutor).bulkInsert(entities, options);
    }

    @Test
    public void testBulkWrite() {
        List<WriteModel<Document>> requests = Arrays.asList();
        BulkWriteResult bulkWriteResult = mock(BulkWriteResult.class);
        when(mockCollExecutor.bulkWrite(requests)).thenReturn(bulkWriteResult);

        BulkWriteResult result = mapper.bulkWrite(requests);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).bulkWrite(requests);
    }

    @Test
    public void testFindOneAndUpdate() {
        Document filter = new Document("id", 1);
        TestEntity update = new TestEntity();
        TestEntity updatedEntity = new TestEntity();
        when(mockCollExecutor.findOneAndUpdate(filter, update, TestEntity.class)).thenReturn(updatedEntity);

        TestEntity result = mapper.findOneAndUpdate(filter, update);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).findOneAndUpdate(filter, update, TestEntity.class);
    }

    @Test
    public void testFindOneAndUpdateWithOptions() {
        Document filter = new Document("id", 1);
        TestEntity update = new TestEntity();
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        TestEntity updatedEntity = new TestEntity();
        when(mockCollExecutor.findOneAndUpdate(filter, update, options, TestEntity.class)).thenReturn(updatedEntity);

        TestEntity result = mapper.findOneAndUpdate(filter, update, options);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).findOneAndUpdate(filter, update, options, TestEntity.class);
    }

    @Test
    public void testFindOneAndReplace() {
        Document filter = new Document("id", 1);
        TestEntity replacement = new TestEntity();
        TestEntity replacedEntity = new TestEntity();
        when(mockCollExecutor.findOneAndReplace(filter, replacement, TestEntity.class)).thenReturn(replacedEntity);

        TestEntity result = mapper.findOneAndReplace(filter, replacement);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).findOneAndReplace(filter, replacement, TestEntity.class);
    }

    @Test
    public void testFindOneAndDelete() {
        Document filter = new Document("id", 1);
        TestEntity deletedEntity = new TestEntity();
        when(mockCollExecutor.findOneAndDelete(filter, TestEntity.class)).thenReturn(deletedEntity);

        TestEntity result = mapper.findOneAndDelete(filter);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).findOneAndDelete(filter, TestEntity.class);
    }

    @Test
    public void testDistinct() {
        // distinct routes through a $group/$project pipeline so distinct scalar values come back as
        // {fieldName: value} documents decodable into the mapped entity type (the driver's native
        // distinct(field, entityClass) decodes raw scalar VALUES with the entity codec and throws).
        String fieldName = "category";
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        List<org.bson.conversions.Bson> expectedPipeline = Arrays.asList(new Document("$group", new Document("_id", "$" + fieldName)),
                new Document("$project", new Document("_id", 0).append(fieldName, "$_id")));
        when(mockCollExecutor.aggregate(expectedPipeline, TestEntity.class)).thenReturn(stream);

        Stream<TestEntity> result = mapper.distinct(fieldName);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).aggregate(expectedPipeline, TestEntity.class);
    }

    @Test
    public void testDistinctWithFilter() {
        String fieldName = "category";
        Document filter = new Document("status", "active");
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        when(mockCollExecutor.aggregate(org.mockito.ArgumentMatchers.anyList(), org.mockito.ArgumentMatchers.eq(TestEntity.class))).thenReturn(stream);

        Stream<TestEntity> result = mapper.distinct(fieldName, filter);
        Assertions.assertNotNull(result);

        @SuppressWarnings("rawtypes")
        org.mockito.ArgumentCaptor<List> pipelineCaptor = org.mockito.ArgumentCaptor.forClass(List.class);
        verify(mockCollExecutor).aggregate(pipelineCaptor.capture(), org.mockito.ArgumentMatchers.eq(TestEntity.class));

        List<?> pipeline = pipelineCaptor.getValue();
        Assertions.assertEquals(3, pipeline.size());
        // First stage matches the filter, then $group on the field, then $project surfacing the value.
        Assertions.assertEquals(new Document("$group", new Document("_id", "$" + fieldName)), pipeline.get(1));
        Assertions.assertEquals(new Document("$project", new Document("_id", 0).append(fieldName, "$_id")), pipeline.get(2));
    }

    @Test
    public void testDistinctWithFilter_nullFilter_throws() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> mapper.distinct("category", null));
    }

    @Test
    public void testDistinct_nullOrEmptyFieldName_throws() {
        // Regression: distinct(...) documents "@throws IAE if fieldName is null or empty", but previously
        // routed straight through the $group/$project pipeline without validating fieldName (a null
        // fieldName degenerated to a "$null" group key, an empty one to an invalid "$" path).
        Assertions.assertThrows(IllegalArgumentException.class, () -> mapper.distinct((String) null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> mapper.distinct(""));
        Assertions.assertThrows(IllegalArgumentException.class, () -> mapper.distinct(null, new Document("status", "active")));
        Assertions.assertThrows(IllegalArgumentException.class, () -> mapper.distinct("", new Document("status", "active")));
        // The underlying executor must not be reached when fieldName is invalid.
        verify(mockCollExecutor, org.mockito.Mockito.never()).aggregate(org.mockito.ArgumentMatchers.anyList(), org.mockito.ArgumentMatchers.any());
    }

    @Test
    public void testAggregate() {
        List<Document> pipeline = Arrays.asList(new Document("$match", new Document("status", "active")));
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        when(mockCollExecutor.aggregate(pipeline, TestEntity.class)).thenReturn(stream);

        Stream<TestEntity> result = mapper.aggregate(pipeline);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).aggregate(pipeline, TestEntity.class);
    }

    @Test
    public void testGroupBy() {
        String fieldName = "category";
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        when(mockCollExecutor.groupBy(fieldName, TestEntity.class)).thenReturn(stream);

        Stream<TestEntity> result = mapper.groupBy(fieldName);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).groupBy(fieldName, TestEntity.class);
    }

    @Test
    public void testGroupByWithMultipleFields() {
        Collection<String> fieldNames = Arrays.asList("category", "status");
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        when(mockCollExecutor.groupBy(fieldNames, TestEntity.class)).thenReturn(stream);

        Stream<TestEntity> result = mapper.groupBy(fieldNames);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).groupBy(fieldNames, TestEntity.class);
    }

    @Test
    public void testGroupByAndCount() {
        String fieldName = "category";
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        when(mockCollExecutor.groupByAndCount(fieldName, TestEntity.class)).thenReturn(stream);

        Stream<TestEntity> result = mapper.groupByAndCount(fieldName);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).groupByAndCount(fieldName, TestEntity.class);
    }

    @Test
    public void testGroupByAndCountWithMultipleFields() {
        Collection<String> fieldNames = Arrays.asList("category", "status");
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        when(mockCollExecutor.groupByAndCount(fieldNames, TestEntity.class)).thenReturn(stream);

        Stream<TestEntity> result = mapper.groupByAndCount(fieldNames);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).groupByAndCount(fieldNames, TestEntity.class);
    }

    @Test
    public void testMapReduce() {
        String mapFunction = "function() { emit(this.name, 1); }";
        String reduceFunction = "function(key, values) { return Array.sum(values); }";
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        when(mockCollExecutor.mapReduce(mapFunction, reduceFunction, TestEntity.class)).thenReturn(stream);

        Stream<TestEntity> result = mapper.mapReduce(mapFunction, reduceFunction);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).mapReduce(mapFunction, reduceFunction, TestEntity.class);
    }

    // ---- coverage gaps: get/gett with projection, findFirst/list/query/stream variants, etc. ----

    @Test
    public void testGetWithObjectIdAndSelectPropNames() {
        ObjectId objectId = new ObjectId();
        Collection<String> select = Arrays.asList("name");
        when(mockCollExecutor.get(objectId, select, TestEntity.class)).thenReturn(com.landawn.abacus.util.u.Optional.<TestEntity> empty());

        com.landawn.abacus.util.u.Optional<TestEntity> result = mapper.get(objectId, select);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).get(objectId, select, TestEntity.class);
    }

    @Test
    public void testGettWithObjectId() {
        ObjectId objectId = new ObjectId();
        TestEntity entity = new TestEntity();
        when(mockCollExecutor.gett(objectId, TestEntity.class)).thenReturn(entity);

        TestEntity result = mapper.gett(objectId);
        Assertions.assertSame(entity, result);
        verify(mockCollExecutor).gett(objectId, TestEntity.class);
    }

    @Test
    public void testGettWithStringObjectIdAndSelectPropNames() {
        String objectId = "507f1f77bcf86cd799439011";
        Collection<String> select = Arrays.asList("name");
        TestEntity entity = new TestEntity();
        when(mockCollExecutor.gett(objectId, select, TestEntity.class)).thenReturn(entity);

        TestEntity result = mapper.gett(objectId, select);
        Assertions.assertSame(entity, result);
        verify(mockCollExecutor).gett(objectId, select, TestEntity.class);
    }

    @Test
    public void testGettWithObjectIdAndSelectPropNames() {
        ObjectId objectId = new ObjectId();
        Collection<String> select = Arrays.asList("name");
        TestEntity entity = new TestEntity();
        when(mockCollExecutor.gett(objectId, select, TestEntity.class)).thenReturn(entity);

        TestEntity result = mapper.gett(objectId, select);
        Assertions.assertSame(entity, result);
        verify(mockCollExecutor).gett(objectId, select, TestEntity.class);
    }

    @Test
    public void testFindFirstWithSelectAndFilter() {
        Collection<String> select = Arrays.asList("name");
        Document filter = new Document("a", 1);
        when(mockCollExecutor.findFirst(select, filter, TestEntity.class)).thenReturn(com.landawn.abacus.util.u.Optional.<TestEntity> empty());

        com.landawn.abacus.util.u.Optional<TestEntity> result = mapper.findFirst(select, filter);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).findFirst(select, filter, TestEntity.class);
    }

    @Test
    public void testFindFirstWithSelectFilterAndSort() {
        Collection<String> select = Arrays.asList("name");
        Document filter = new Document("a", 1);
        Document sort = new Document("a", 1);
        when(mockCollExecutor.findFirst(select, filter, sort, TestEntity.class)).thenReturn(com.landawn.abacus.util.u.Optional.<TestEntity> empty());

        com.landawn.abacus.util.u.Optional<TestEntity> result = mapper.findFirst(select, filter, sort);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).findFirst(select, filter, sort, TestEntity.class);
    }

    @Test
    public void testFindFirstWithProjectionFilterAndSort() {
        Document proj = new Document("name", 1);
        Document filter = new Document("a", 1);
        Document sort = new Document("a", 1);
        when(mockCollExecutor.findFirst(proj, filter, sort, TestEntity.class)).thenReturn(com.landawn.abacus.util.u.Optional.<TestEntity> empty());

        com.landawn.abacus.util.u.Optional<TestEntity> result = mapper.findFirst(proj, filter, sort);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).findFirst(proj, filter, sort, TestEntity.class);
    }

    @Test
    public void testListWithSelectAndFilter() {
        Collection<String> select = Arrays.asList("name");
        Document filter = new Document("a", 1);
        List<TestEntity> result = Arrays.asList(new TestEntity());
        when(mockCollExecutor.list(select, filter, TestEntity.class)).thenReturn(result);

        Assertions.assertSame(result, mapper.list(select, filter));
        verify(mockCollExecutor).list(select, filter, TestEntity.class);
    }

    @Test
    public void testListWithSelectFilterOffsetCount() {
        Collection<String> select = Arrays.asList("name");
        Document filter = new Document("a", 1);
        List<TestEntity> result = Arrays.asList(new TestEntity());
        when(mockCollExecutor.list(select, filter, 0, 10, TestEntity.class)).thenReturn(result);

        Assertions.assertSame(result, mapper.list(select, filter, 0, 10));
        verify(mockCollExecutor).list(select, filter, 0, 10, TestEntity.class);
    }

    @Test
    public void testListWithSelectFilterAndSort() {
        Collection<String> select = Arrays.asList("name");
        Document filter = new Document("a", 1);
        Document sort = new Document("a", 1);
        List<TestEntity> result = Arrays.asList(new TestEntity());
        when(mockCollExecutor.list(select, filter, sort, TestEntity.class)).thenReturn(result);

        Assertions.assertSame(result, mapper.list(select, filter, sort));
        verify(mockCollExecutor).list(select, filter, sort, TestEntity.class);
    }

    @Test
    public void testListWithSelectFilterSortOffsetCount() {
        Collection<String> select = Arrays.asList("name");
        Document filter = new Document("a", 1);
        Document sort = new Document("a", 1);
        List<TestEntity> result = Arrays.asList(new TestEntity());
        when(mockCollExecutor.list(select, filter, sort, 0, 10, TestEntity.class)).thenReturn(result);

        Assertions.assertSame(result, mapper.list(select, filter, sort, 0, 10));
        verify(mockCollExecutor).list(select, filter, sort, 0, 10, TestEntity.class);
    }

    @Test
    public void testListWithProjectionFilterAndSort() {
        Document proj = new Document("name", 1);
        Document filter = new Document("a", 1);
        Document sort = new Document("a", 1);
        List<TestEntity> result = Arrays.asList(new TestEntity());
        when(mockCollExecutor.list(proj, filter, sort, TestEntity.class)).thenReturn(result);

        Assertions.assertSame(result, mapper.list(proj, filter, sort));
        verify(mockCollExecutor).list(proj, filter, sort, TestEntity.class);
    }

    @Test
    public void testListWithProjectionFilterSortOffsetCount() {
        Document proj = new Document("name", 1);
        Document filter = new Document("a", 1);
        Document sort = new Document("a", 1);
        List<TestEntity> result = Arrays.asList(new TestEntity());
        when(mockCollExecutor.list(proj, filter, sort, 0, 10, TestEntity.class)).thenReturn(result);

        Assertions.assertSame(result, mapper.list(proj, filter, sort, 0, 10));
        verify(mockCollExecutor).list(proj, filter, sort, 0, 10, TestEntity.class);
    }

    @Test
    public void testQueryForDateWithValueType() {
        Document filter = new Document("a", 1);
        java.sql.Timestamp ts = new java.sql.Timestamp(System.currentTimeMillis());
        when(mockCollExecutor.queryForDate("p", filter, java.sql.Timestamp.class)).thenReturn(Nullable.of(ts));

        Nullable<java.sql.Timestamp> result = mapper.queryForDate("p", filter, java.sql.Timestamp.class);
        Assertions.assertSame(ts, result.orElse(null));
        verify(mockCollExecutor).queryForDate("p", filter, java.sql.Timestamp.class);
    }

    @Test
    public void testQueryWithFilterOffsetCount() {
        Document filter = new Document("a", 1);
        Dataset ds = N.newEmptyDataset(Arrays.asList("id"));
        when(mockCollExecutor.query(filter, 0, 10, TestEntity.class)).thenReturn(ds);

        Assertions.assertSame(ds, mapper.query(filter, 0, 10));
        verify(mockCollExecutor).query(filter, 0, 10, TestEntity.class);
    }

    @Test
    public void testQueryWithSelectAndFilter() {
        Collection<String> select = Arrays.asList("name");
        Document filter = new Document("a", 1);
        Dataset ds = N.newEmptyDataset(Arrays.asList("id"));
        when(mockCollExecutor.query(select, filter, TestEntity.class)).thenReturn(ds);

        Assertions.assertSame(ds, mapper.query(select, filter));
        verify(mockCollExecutor).query(select, filter, TestEntity.class);
    }

    @Test
    public void testQueryWithSelectFilterOffsetCount() {
        Collection<String> select = Arrays.asList("name");
        Document filter = new Document("a", 1);
        Dataset ds = N.newEmptyDataset(Arrays.asList("id"));
        when(mockCollExecutor.query(select, filter, 0, 10, TestEntity.class)).thenReturn(ds);

        Assertions.assertSame(ds, mapper.query(select, filter, 0, 10));
        verify(mockCollExecutor).query(select, filter, 0, 10, TestEntity.class);
    }

    @Test
    public void testQueryWithSelectFilterAndSort() {
        Collection<String> select = Arrays.asList("name");
        Document filter = new Document("a", 1);
        Document sort = new Document("a", 1);
        Dataset ds = N.newEmptyDataset(Arrays.asList("id"));
        when(mockCollExecutor.query(select, filter, sort, TestEntity.class)).thenReturn(ds);

        Assertions.assertSame(ds, mapper.query(select, filter, sort));
        verify(mockCollExecutor).query(select, filter, sort, TestEntity.class);
    }

    @Test
    public void testQueryWithSelectFilterSortOffsetCount() {
        Collection<String> select = Arrays.asList("name");
        Document filter = new Document("a", 1);
        Document sort = new Document("a", 1);
        Dataset ds = N.newEmptyDataset(Arrays.asList("id"));
        when(mockCollExecutor.query(select, filter, sort, 0, 10, TestEntity.class)).thenReturn(ds);

        Assertions.assertSame(ds, mapper.query(select, filter, sort, 0, 10));
        verify(mockCollExecutor).query(select, filter, sort, 0, 10, TestEntity.class);
    }

    @Test
    public void testQueryWithProjectionFilterAndSort() {
        Document proj = new Document("name", 1);
        Document filter = new Document("a", 1);
        Document sort = new Document("a", 1);
        Dataset ds = N.newEmptyDataset(Arrays.asList("id"));
        when(mockCollExecutor.query(proj, filter, sort, TestEntity.class)).thenReturn(ds);

        Assertions.assertSame(ds, mapper.query(proj, filter, sort));
        verify(mockCollExecutor).query(proj, filter, sort, TestEntity.class);
    }

    @Test
    public void testQueryWithProjectionFilterSortOffsetCount() {
        Document proj = new Document("name", 1);
        Document filter = new Document("a", 1);
        Document sort = new Document("a", 1);
        Dataset ds = N.newEmptyDataset(Arrays.asList("id"));
        when(mockCollExecutor.query(proj, filter, sort, 0, 10, TestEntity.class)).thenReturn(ds);

        Assertions.assertSame(ds, mapper.query(proj, filter, sort, 0, 10));
        verify(mockCollExecutor).query(proj, filter, sort, 0, 10, TestEntity.class);
    }

    @Test
    public void testStreamWithFilterOffsetCount() {
        Document filter = new Document("a", 1);
        Stream<TestEntity> s = Stream.of(new TestEntity());
        when(mockCollExecutor.stream(filter, 0, 10, TestEntity.class)).thenReturn(s);

        Assertions.assertSame(s, mapper.stream(filter, 0, 10));
        verify(mockCollExecutor).stream(filter, 0, 10, TestEntity.class);
    }

    @Test
    public void testStreamWithSelectAndFilter() {
        Collection<String> select = Arrays.asList("name");
        Document filter = new Document("a", 1);
        Stream<TestEntity> s = Stream.of(new TestEntity());
        when(mockCollExecutor.stream(select, filter, TestEntity.class)).thenReturn(s);

        Assertions.assertSame(s, mapper.stream(select, filter));
        verify(mockCollExecutor).stream(select, filter, TestEntity.class);
    }

    @Test
    public void testStreamWithSelectFilterOffsetCount() {
        Collection<String> select = Arrays.asList("name");
        Document filter = new Document("a", 1);
        Stream<TestEntity> s = Stream.of(new TestEntity());
        when(mockCollExecutor.stream(select, filter, 0, 10, TestEntity.class)).thenReturn(s);

        Assertions.assertSame(s, mapper.stream(select, filter, 0, 10));
        verify(mockCollExecutor).stream(select, filter, 0, 10, TestEntity.class);
    }

    @Test
    public void testStreamWithSelectFilterAndSort() {
        Collection<String> select = Arrays.asList("name");
        Document filter = new Document("a", 1);
        Document sort = new Document("a", 1);
        Stream<TestEntity> s = Stream.of(new TestEntity());
        when(mockCollExecutor.stream(select, filter, sort, TestEntity.class)).thenReturn(s);

        Assertions.assertSame(s, mapper.stream(select, filter, sort));
        verify(mockCollExecutor).stream(select, filter, sort, TestEntity.class);
    }

    @Test
    public void testStreamWithSelectFilterSortOffsetCount() {
        Collection<String> select = Arrays.asList("name");
        Document filter = new Document("a", 1);
        Document sort = new Document("a", 1);
        Stream<TestEntity> s = Stream.of(new TestEntity());
        when(mockCollExecutor.stream(select, filter, sort, 0, 10, TestEntity.class)).thenReturn(s);

        Assertions.assertSame(s, mapper.stream(select, filter, sort, 0, 10));
        verify(mockCollExecutor).stream(select, filter, sort, 0, 10, TestEntity.class);
    }

    @Test
    public void testStreamWithProjectionFilterAndSort() {
        Document proj = new Document("name", 1);
        Document filter = new Document("a", 1);
        Document sort = new Document("a", 1);
        Stream<TestEntity> s = Stream.of(new TestEntity());
        when(mockCollExecutor.stream(proj, filter, sort, TestEntity.class)).thenReturn(s);

        Assertions.assertSame(s, mapper.stream(proj, filter, sort));
        verify(mockCollExecutor).stream(proj, filter, sort, TestEntity.class);
    }

    @Test
    public void testStreamWithProjectionFilterSortOffsetCount() {
        Document proj = new Document("name", 1);
        Document filter = new Document("a", 1);
        Document sort = new Document("a", 1);
        Stream<TestEntity> s = Stream.of(new TestEntity());
        when(mockCollExecutor.stream(proj, filter, sort, 0, 10, TestEntity.class)).thenReturn(s);

        Assertions.assertSame(s, mapper.stream(proj, filter, sort, 0, 10));
        verify(mockCollExecutor).stream(proj, filter, sort, 0, 10, TestEntity.class);
    }

    @Test
    public void testUpdateOneWithCollectionAndOptions() {
        Document filter = new Document("a", 1);
        Collection<TestEntity> objList = Arrays.asList(new TestEntity());
        UpdateOptions options = new UpdateOptions();
        UpdateResult ur = mock(UpdateResult.class);
        when(mockCollExecutor.updateOne(filter, objList, options)).thenReturn(ur);

        Assertions.assertSame(ur, mapper.updateOne(filter, objList, options));
        verify(mockCollExecutor).updateOne(filter, objList, options);
    }

    @Test
    public void testUpdateManyWithCollection() {
        Document filter = new Document("a", 1);
        Collection<TestEntity> objList = Arrays.asList(new TestEntity());
        UpdateResult ur = mock(UpdateResult.class);
        when(mockCollExecutor.updateMany(filter, objList)).thenReturn(ur);

        Assertions.assertSame(ur, mapper.updateMany(filter, objList));
        verify(mockCollExecutor).updateMany(filter, objList);
    }

    @Test
    public void testUpdateManyWithCollectionAndOptions() {
        Document filter = new Document("a", 1);
        Collection<TestEntity> objList = Arrays.asList(new TestEntity());
        UpdateOptions options = new UpdateOptions();
        UpdateResult ur = mock(UpdateResult.class);
        when(mockCollExecutor.updateMany(filter, objList, options)).thenReturn(ur);

        Assertions.assertSame(ur, mapper.updateMany(filter, objList, options));
        verify(mockCollExecutor).updateMany(filter, objList, options);
    }

    @Test
    public void testReplaceOneWithObjectId() {
        ObjectId objectId = new ObjectId();
        TestEntity replacement = new TestEntity();
        UpdateResult ur = mock(UpdateResult.class);
        when(mockCollExecutor.replaceOne(objectId, replacement)).thenReturn(ur);

        Assertions.assertSame(ur, mapper.replaceOne(objectId, replacement));
        verify(mockCollExecutor).replaceOne(objectId, replacement);
    }

    @Test
    public void testDeleteOneWithFilterAndOptions() {
        Document filter = new Document("a", 1);
        DeleteOptions opts = new DeleteOptions();
        DeleteResult dr = mock(DeleteResult.class);
        when(mockCollExecutor.deleteOne(filter, opts)).thenReturn(dr);

        Assertions.assertSame(dr, mapper.deleteOne(filter, opts));
        verify(mockCollExecutor).deleteOne(filter, opts);
    }

    @Test
    public void testDeleteManyWithFilterAndOptions() {
        Document filter = new Document("a", 1);
        DeleteOptions opts = new DeleteOptions();
        DeleteResult dr = mock(DeleteResult.class);
        when(mockCollExecutor.deleteMany(filter, opts)).thenReturn(dr);

        Assertions.assertSame(dr, mapper.deleteMany(filter, opts));
        verify(mockCollExecutor).deleteMany(filter, opts);
    }

    @Test
    public void testBulkWriteWithOptions() {
        List<WriteModel<? extends Document>> requests = Collections.<WriteModel<? extends Document>> emptyList();
        BulkWriteOptions opts = new BulkWriteOptions();
        BulkWriteResult br = mock(BulkWriteResult.class);
        when(mockCollExecutor.bulkWrite(requests, opts)).thenReturn(br);

        Assertions.assertSame(br, mapper.bulkWrite(requests, opts));
        verify(mockCollExecutor).bulkWrite(requests, opts);
    }

    @Test
    public void testFindOneAndUpdateWithCollection() {
        Document filter = new Document("a", 1);
        Collection<TestEntity> objList = Arrays.asList(new TestEntity());
        TestEntity entity = new TestEntity();
        when(mockCollExecutor.findOneAndUpdate(filter, objList, TestEntity.class)).thenReturn(entity);

        Assertions.assertSame(entity, mapper.findOneAndUpdate(filter, objList));
        verify(mockCollExecutor).findOneAndUpdate(filter, objList, TestEntity.class);
    }

    @Test
    public void testFindOneAndUpdateWithCollectionAndOptions() {
        Document filter = new Document("a", 1);
        Collection<TestEntity> objList = Arrays.asList(new TestEntity());
        FindOneAndUpdateOptions opts = new FindOneAndUpdateOptions();
        TestEntity entity = new TestEntity();
        when(mockCollExecutor.findOneAndUpdate(filter, objList, opts, TestEntity.class)).thenReturn(entity);

        Assertions.assertSame(entity, mapper.findOneAndUpdate(filter, objList, opts));
        verify(mockCollExecutor).findOneAndUpdate(filter, objList, opts, TestEntity.class);
    }

    @Test
    public void testFindOneAndReplaceWithOptions() {
        Document filter = new Document("a", 1);
        TestEntity replacement = new TestEntity();
        FindOneAndReplaceOptions opts = new FindOneAndReplaceOptions();
        TestEntity entity = new TestEntity();
        when(mockCollExecutor.findOneAndReplace(filter, replacement, opts, TestEntity.class)).thenReturn(entity);

        Assertions.assertSame(entity, mapper.findOneAndReplace(filter, replacement, opts));
        verify(mockCollExecutor).findOneAndReplace(filter, replacement, opts, TestEntity.class);
    }

    @Test
    public void testFindOneAndDeleteWithOptions() {
        Document filter = new Document("a", 1);
        FindOneAndDeleteOptions opts = new FindOneAndDeleteOptions();
        TestEntity entity = new TestEntity();
        when(mockCollExecutor.findOneAndDelete(filter, opts, TestEntity.class)).thenReturn(entity);

        Assertions.assertSame(entity, mapper.findOneAndDelete(filter, opts));
        verify(mockCollExecutor).findOneAndDelete(filter, opts, TestEntity.class);
    }

    // Test entity class
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