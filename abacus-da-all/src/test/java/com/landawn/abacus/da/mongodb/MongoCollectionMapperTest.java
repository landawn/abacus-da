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

import com.landawn.abacus.da.MockitoTestBase;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.stream.Stream;
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

public class MongoCollectionMapperTest extends MockitoTestBase {

    @Mock
    private MongoCollectionExecutor mockCollExecutor;

    private MongoCollectionMapper<TestEntity> mapper;

    @BeforeEach
    public void setUp() {
        mapper = new MongoCollectionMapper<>(mockCollExecutor, TestEntity.class);
    }

    @Test
    public void testDecodedValueConversionsOnMapperReadPaths() {
        // The mapper reads through the executor with its entity class as row type, so the MongoDBBase read-side conversions
        // (typed container elements, java.time Local* in UTC, generic bean type arguments, records incl. _id into the record's
        // id component) must hold for mapped entities too. Fixtures are shared with MongoCollectionExecutorTest (*);
        // each read path is asserted on its own.
        final MongoCollectionMapper<MongoMappingFixtures.MappedEntity> entityMapper = mapperReturning(MongoMappingFixtures.entityDoc(),
                MongoMappingFixtures.MappedEntity.class);
        final MongoCollectionMapper<MongoMappingFixtures.MappedRecord> recordMapper = mapperReturning(MongoMappingFixtures.recordDoc(),
                MongoMappingFixtures.MappedRecord.class);
        final MongoCollectionMapper<MongoMappingFixtures.LongBox> boxMapper = mapperReturning(MongoMappingFixtures.longBoxDoc(),
                MongoMappingFixtures.LongBox.class);
        final Document filter = new Document();
        final List<Document> pipeline = Arrays.asList(new Document("$match", new Document()));
        final ObjectId oid = new ObjectId(MongoMappingFixtures.OID_HEX);

        Assertions.assertAll(() -> MongoMappingFixtures.assertEntity("list", entityMapper.list(filter).get(0)),
                () -> MongoMappingFixtures.assertEntity("list(Collection)", entityMapper.list(Arrays.asList("nums", "day"), filter).get(0)),
                () -> MongoMappingFixtures.assertEntity("findFirst", entityMapper.findFirst(filter).get()),
                () -> MongoMappingFixtures.assertEntity("get(ObjectId)", entityMapper.get(oid).get()),
                () -> MongoMappingFixtures.assertEntity("gett(String)", entityMapper.gett(MongoMappingFixtures.OID_HEX)), () -> {
                    try (Stream<MongoMappingFixtures.MappedEntity> stream = entityMapper.stream(filter)) {
                        MongoMappingFixtures.assertEntity("stream", stream.toList().get(0));
                    }
                }, () -> {
                    try (Stream<MongoMappingFixtures.MappedEntity> stream = entityMapper.aggregate(pipeline)) {
                        MongoMappingFixtures.assertEntity("aggregate", stream.toList().get(0));
                    }
                }, () -> MongoMappingFixtures.assertEntity("findOneAndDelete", entityMapper.findOneAndDelete(filter)),
                () -> MongoMappingFixtures.assertEntityDataset("query(Bson)", entityMapper.query(filter)),
                () -> Assertions.assertEquals(MongoMappingFixtures.LOCAL_DAY, entityMapper.queryForSingleValue("day", filter, java.time.LocalDate.class).get(),
                        "queryForSingleValue LocalDate"),
                () -> Assertions.assertEquals(MongoMappingFixtures.expectedRecord(), recordMapper.get(oid).get(), "record get(ObjectId)"),
                () -> Assertions.assertEquals(MongoMappingFixtures.expectedRecord(), recordMapper.list(filter).get(0), "record list"),
                () -> Assertions.assertEquals((Object) 9L, boxMapper.list(filter).get(0).getValue(), "LongBox list"),
                () -> Assertions.assertEquals((Object) 9L, boxMapper.findFirst(filter).get().getValue(), "LongBox findFirst"));
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
    public void testDeleteMany() {
        Document filter = new Document("status", "inactive");
        DeleteResult deleteResult = mock(DeleteResult.class);
        when(mockCollExecutor.deleteMany(filter)).thenReturn(deleteResult);

        DeleteResult result = mapper.deleteMany(filter);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).deleteMany(filter);
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
    public void testDeleteOneWithFilter() {
        Document filter = new Document("id", 1);
        DeleteResult deleteResult = mock(DeleteResult.class);
        when(mockCollExecutor.deleteOne(filter)).thenReturn(deleteResult);

        DeleteResult result = mapper.deleteOne(filter);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).deleteOne(filter);
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
    public void testDeleteOneWithObjectId() {
        ObjectId objectId = new ObjectId();
        DeleteResult deleteResult = mock(DeleteResult.class);
        when(mockCollExecutor.deleteOne(objectId)).thenReturn(deleteResult);

        DeleteResult result = mapper.deleteOne(objectId);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).deleteOne(objectId);
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
    public void testFindOneAndDelete() {
        Document filter = new Document("id", 1);
        TestEntity deletedEntity = new TestEntity();
        when(mockCollExecutor.findOneAndDelete(filter, TestEntity.class)).thenReturn(deletedEntity);

        TestEntity result = mapper.findOneAndDelete(filter);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).findOneAndDelete(filter, TestEntity.class);
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
    public void testListWithSelectFilterOffsetCount() {
        Collection<String> select = Arrays.asList("name");
        Document filter = new Document("a", 1);
        List<TestEntity> result = Arrays.asList(new TestEntity());
        when(mockCollExecutor.list(select, filter, 0, 10, TestEntity.class)).thenReturn(result);

        Assertions.assertSame(result, mapper.list(select, filter, 0, 10));
        verify(mockCollExecutor).list(select, filter, 0, 10, TestEntity.class);
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
    public void testQueryWithFilterOffsetCount() {
        Document filter = new Document("a", 1);
        Dataset ds = N.newEmptyDataset(Arrays.asList("id"));
        when(mockCollExecutor.query(filter, 0, 10, TestEntity.class)).thenReturn(ds);

        Assertions.assertSame(ds, mapper.query(filter, 0, 10));
        verify(mockCollExecutor).query(filter, 0, 10, TestEntity.class);
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
    public void testQueryWithSelectFilterOffsetCount() {
        Collection<String> select = Arrays.asList("name");
        Document filter = new Document("a", 1);
        Dataset ds = N.newEmptyDataset(Arrays.asList("id"));
        when(mockCollExecutor.query(select, filter, 0, 10, TestEntity.class)).thenReturn(ds);

        Assertions.assertSame(ds, mapper.query(select, filter, 0, 10));
        verify(mockCollExecutor).query(select, filter, 0, 10, TestEntity.class);
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
    public void testStreamWithFilterOffsetCount() {
        Document filter = new Document("a", 1);
        Stream<TestEntity> s = Stream.of(new TestEntity());
        when(mockCollExecutor.stream(filter, 0, 10, TestEntity.class)).thenReturn(s);

        Assertions.assertSame(s, mapper.stream(filter, 0, 10));
        verify(mockCollExecutor).stream(filter, 0, 10, TestEntity.class);
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
    public void testStreamWithSelectFilterOffsetCount() {
        Collection<String> select = Arrays.asList("name");
        Document filter = new Document("a", 1);
        Stream<TestEntity> s = Stream.of(new TestEntity());
        when(mockCollExecutor.stream(select, filter, 0, 10, TestEntity.class)).thenReturn(s);

        Assertions.assertSame(s, mapper.stream(select, filter, 0, 10));
        verify(mockCollExecutor).stream(select, filter, 0, 10, TestEntity.class);
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
    public void testAggregate() {
        List<Document> pipeline = Arrays.asList(new Document("$match", new Document("status", "active")));
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        when(mockCollExecutor.aggregate(pipeline, TestEntity.class)).thenReturn(stream);

        Stream<TestEntity> result = mapper.aggregate(pipeline);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).aggregate(pipeline, TestEntity.class);
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

    // -- 2026-09-27 review (slice H) regressions --

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testDistinctDottedFieldIntoScalarRowTypeSendsGroupOnlyPipelineAndReturnsValues() {
        // Regression: a single-value mapper type must read each distinct value from the {_id: value} group rows. A trailing
        // {$project: {"address.city": "$_id"}} makes the server nest the value ({address: {city: v}}), which a String
        // mapper then returned as '{"city": "Paris"}' and an Integer mapper failed with NumberFormatException (live-probed).
        final com.mongodb.client.MongoCollection<Document> collection = mock(com.mongodb.client.MongoCollection.class);
        final com.mongodb.client.AggregateIterable<Document> iterable = mock(com.mongodb.client.AggregateIterable.class);
        when(collection.aggregate(org.mockito.ArgumentMatchers.anyList(), org.mockito.ArgumentMatchers.eq(Document.class))).thenReturn(iterable);
        when(iterable.iterator()).thenAnswer(invocation -> {
            final java.util.Iterator<Document> it = Arrays.asList(new Document("_id", "Paris"), new Document("_id", "Rome")).iterator();
            final com.mongodb.client.MongoCursor<Document> cursor = mock(com.mongodb.client.MongoCursor.class);
            when(cursor.hasNext()).thenAnswer(i -> it.hasNext());
            when(cursor.next()).thenAnswer(i -> it.next());
            return cursor;
        });

        final MongoCollectionExecutor collExecutor = new MongoCollectionExecutor(collection, mock(com.landawn.abacus.util.AsyncExecutor.class));
        final MongoCollectionMapper<String> stringMapper = new MongoCollectionMapper<>(collExecutor, String.class);
        final Document filter = new Document("active", true);

        Assertions.assertEquals(Arrays.asList("Paris", "Rome"), stringMapper.distinct("address.city").toList());
        Assertions.assertEquals(Arrays.asList("Paris", "Rome"), stringMapper.distinct("address.city", filter).toList());

        final org.mockito.ArgumentCaptor<List> pipelineCaptor = org.mockito.ArgumentCaptor.forClass(List.class);
        verify(collection, org.mockito.Mockito.times(2)).aggregate(pipelineCaptor.capture(), org.mockito.ArgumentMatchers.eq(Document.class));
        Assertions.assertEquals(Arrays.asList(new Document("$group", new Document("_id", "$address.city"))), pipelineCaptor.getAllValues().get(0));
        final List<?> filtered = pipelineCaptor.getAllValues().get(1);
        Assertions.assertEquals(2, filtered.size());
        Assertions.assertEquals(new Document("$group", new Document("_id", "$address.city")), filtered.get(1));

        // A bean mapper still gets the $project stage that surfaces the value under the field name.
        final MongoCollectionMapper<TestEntity> beanMapper = new MongoCollectionMapper<>(collExecutor, TestEntity.class);
        beanMapper.distinct("name").toList();

        verify(collection, org.mockito.Mockito.times(3)).aggregate(pipelineCaptor.capture(), org.mockito.ArgumentMatchers.eq(Document.class));
        final List<?> beanPipeline = pipelineCaptor.getAllValues().get(pipelineCaptor.getAllValues().size() - 1);
        Assertions.assertEquals(2, beanPipeline.size());
        Assertions.assertEquals(new Document("$project", new Document("_id", 0).append("name", "$_id")), beanPipeline.get(1));
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
    public void testExistsWithBsonFilter() {
        Document filter = new Document("name", "test");
        when(mockCollExecutor.exists(filter)).thenReturn(true);

        boolean result = mapper.exists(filter);
        Assertions.assertTrue(result);
        verify(mockCollExecutor).exists(filter);
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
    public void testExistsWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        when(mockCollExecutor.exists(objectId)).thenReturn(true);

        boolean result = mapper.exists(objectId);
        Assertions.assertTrue(result);
        verify(mockCollExecutor).exists(objectId);
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
    public void testGettWithObjectId() {
        ObjectId objectId = new ObjectId();
        TestEntity entity = new TestEntity();
        when(mockCollExecutor.gett(objectId, TestEntity.class)).thenReturn(entity);

        TestEntity result = mapper.gett(objectId);
        Assertions.assertSame(entity, result);
        verify(mockCollExecutor).gett(objectId, TestEntity.class);
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
    public void testGettWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        TestEntity entity = new TestEntity();
        when(mockCollExecutor.gett(objectId, TestEntity.class)).thenReturn(entity);

        TestEntity result = mapper.gett(objectId);
        Assertions.assertNotNull(result);
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
    public void testGetWithObjectId() {
        ObjectId objectId = new ObjectId();
        when(mockCollExecutor.get(objectId, TestEntity.class)).thenReturn(Optional.empty());

        Optional<TestEntity> result = mapper.get(objectId);
        Assertions.assertFalse(result.isPresent());
        verify(mockCollExecutor).get(objectId, TestEntity.class);
    }

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
    public void testGetWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        TestEntity entity = new TestEntity();
        when(mockCollExecutor.get(objectId, TestEntity.class)).thenReturn(Optional.of(entity));

        Optional<TestEntity> result = mapper.get(objectId);
        Assertions.assertTrue(result.isPresent());
        verify(mockCollExecutor).get(objectId, TestEntity.class);
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
    public void testGroupByAndCount() {
        String fieldName = "category";
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        when(mockCollExecutor.groupByAndCount(fieldName, TestEntity.class)).thenReturn(stream);

        Stream<TestEntity> result = mapper.groupByAndCount(fieldName);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).groupByAndCount(fieldName, TestEntity.class);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGroupByAndCountCountFieldMessageAndDocumentMapper() {
        final com.mongodb.client.MongoCollection<Document> collection = mock(com.mongodb.client.MongoCollection.class);
        final com.mongodb.client.AggregateIterable<Document> aggregateIterable = mock(com.mongodb.client.AggregateIterable.class);
        when(aggregateIterable.iterator()).thenReturn(mock(com.mongodb.client.MongoCursor.class));
        when(collection.aggregate(org.mockito.ArgumentMatchers.anyList(), org.mockito.ArgumentMatchers.eq(Document.class))).thenReturn(aggregateIterable);
        final MongoCollectionExecutor collExecutor = new MongoCollectionExecutor(collection, mock(com.landawn.abacus.util.AsyncExecutor.class));

        final MongoCollectionMapper<DottedEntity> entityMapper = new MongoCollectionMapper<>(collExecutor, DottedEntity.class);
        Assertions.assertEquals("Group field name 'count' conflicts with the count column of groupByAndCount; use Document as the row type",
                Assertions.assertThrows(IllegalArgumentException.class, () -> entityMapper.groupByAndCount("count")).getMessage());
        org.mockito.Mockito.verifyNoInteractions(collection);

        // A Document mapper keeps the key under _id, so the same field name is accepted.
        final MongoCollectionMapper<Document> documentMapper = new MongoCollectionMapper<>(collExecutor, Document.class);
        Assertions.assertEquals(0, documentMapper.groupByAndCount("count").count());
        Assertions.assertEquals(0, documentMapper.groupByAndCount(Arrays.asList("name", "count")).count());
        verify(collection, org.mockito.Mockito.times(2)).aggregate(org.mockito.ArgumentMatchers.anyList(), org.mockito.ArgumentMatchers.eq(Document.class));
    }

    @Test
    public void testGroupByAndCountOnCountFieldRejectedForBeanMapper() {
        // Pins the documented IllegalArgumentException: an entity mapper's groupByAndCount rows are projected as
        // {<field>: key, count: n}, so a group field named "count" would be overwritten by the count (no query is sent).
        final com.mongodb.client.MongoCollection<Document> collection = mock(com.mongodb.client.MongoCollection.class);
        final MongoCollectionExecutor collExecutor = new MongoCollectionExecutor(collection, mock(com.landawn.abacus.util.AsyncExecutor.class));
        final MongoCollectionMapper<TestEntity> beanMapper = new MongoCollectionMapper<>(collExecutor, TestEntity.class);

        Assertions.assertThrows(IllegalArgumentException.class, () -> beanMapper.groupByAndCount("count"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> beanMapper.groupByAndCount(Arrays.asList("name", "count")));
        org.mockito.Mockito.verifyNoInteractions(collection);
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
    public void testGroupByWithMultipleFields() {
        Collection<String> fieldNames = Arrays.asList("category", "status");
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        when(mockCollExecutor.groupBy(fieldNames, TestEntity.class)).thenReturn(stream);

        Stream<TestEntity> result = mapper.groupBy(fieldNames);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).groupBy(fieldNames, TestEntity.class);
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
    public void testListWithSelectAndFilter() {
        Collection<String> select = Arrays.asList("name");
        Document filter = new Document("a", 1);
        List<TestEntity> result = Arrays.asList(new TestEntity());
        when(mockCollExecutor.list(select, filter, TestEntity.class)).thenReturn(result);

        Assertions.assertSame(result, mapper.list(select, filter));
        verify(mockCollExecutor).list(select, filter, TestEntity.class);
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
    public void testQuery() {
        Document filter = new Document("status", "active");
        Dataset dataset = N.newEmptyDataset(Arrays.asList("id", "name"));
        when(mockCollExecutor.query(filter, TestEntity.class)).thenReturn(dataset);

        Dataset result = mapper.query(filter);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).query(filter, TestEntity.class);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testQueryDottedSelectNameThroughMapperFillsNestedColumn() {
        // The mapper's Collection-projection Dataset overloads delegate with the entity class as row type, so a dotted
        // select name must come back as the nested value (it was an all-null column) and a path through an array fails.
        final com.mongodb.client.MongoCollection<Document> collection = mock(com.mongodb.client.MongoCollection.class);
        final com.mongodb.client.FindIterable<Document> findIterable = mock(com.mongodb.client.FindIterable.class);
        when(collection.find(org.mockito.ArgumentMatchers.any(org.bson.conversions.Bson.class))).thenReturn(findIterable);
        when(findIterable.projection(org.mockito.ArgumentMatchers.any())).thenReturn(findIterable);
        when(findIterable.sort(org.mockito.ArgumentMatchers.any())).thenReturn(findIterable);
        when(findIterable.skip(org.mockito.ArgumentMatchers.anyInt())).thenReturn(findIterable);
        when(findIterable.limit(org.mockito.ArgumentMatchers.anyInt())).thenReturn(findIterable);
        final List<Document> rows = new java.util.ArrayList<>(Arrays.asList(
                new Document("_id", "1").append("name", "n1").append("address", new Document("city", "Paris")), new Document("_id", "2").append("name", "n2")));
        org.mockito.Mockito.doAnswer(invocation -> {
            final Collection<Object> target = invocation.getArgument(0);
            target.addAll(rows);
            return target;
        }).when(findIterable).into(org.mockito.ArgumentMatchers.any());
        // 2026-10-24 sliceT: the dotted query fetches through iterator() (MongoDBBase.fetchAll), so stub it too.
        MongoIterableStubs.stubRows(findIterable, rows);

        final MongoCollectionMapper<DottedEntity> entityMapper = new MongoCollectionMapper<>(
                new MongoCollectionExecutor(collection, mock(com.landawn.abacus.util.AsyncExecutor.class)), DottedEntity.class);
        final List<String> names = Arrays.asList("name", "address.city");
        final Document filter = new Document();
        final Document sort = new Document("_id", 1);

        for (final Dataset ds : Arrays.asList(entityMapper.query(names, filter), entityMapper.query(names, filter, 1, 5),
                entityMapper.query(names, filter, sort), entityMapper.query(names, filter, sort, 1, 5))) {
            Assertions.assertEquals(names, ds.columnNames());
            Assertions.assertEquals(Arrays.asList("n1", "n2"), ds.getColumn("name"));
            Assertions.assertEquals(Arrays.asList("Paris", null), ds.getColumn("address.city"));
        }

        rows.add(new Document("_id", "3").append("name", "n3").append("address", Arrays.asList(new Document("city", "Rome"))));
        Assertions.assertThrows(ClassCastException.class, () -> entityMapper.query(names, filter));
        Assertions.assertThrows(ClassCastException.class, () -> entityMapper.query(names, filter, sort, 1, 5));
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
    public void testQueryForByte() {
        String propName = "level";
        Document filter = new Document("id", 1);
        when(mockCollExecutor.queryForByte(propName, filter)).thenReturn(OptionalByte.of((byte) 5));

        OptionalByte result = mapper.queryForByte(propName, filter);
        Assertions.assertEquals((byte) 5, result.get());
        verify(mockCollExecutor).queryForByte(propName, filter);
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
    public void testQueryForDateWithValueType() {
        Document filter = new Document("a", 1);
        java.sql.Timestamp ts = java.sql.Timestamp.from(java.time.Instant.parse("2026-01-02T03:04:05Z"));
        when(mockCollExecutor.queryForDate("p", filter, java.sql.Timestamp.class)).thenReturn(Nullable.of(ts));

        Nullable<java.sql.Timestamp> result = mapper.queryForDate("p", filter, java.sql.Timestamp.class);
        Assertions.assertSame(ts, result.orElse(null));
        verify(mockCollExecutor).queryForDate("p", filter, java.sql.Timestamp.class);
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
    public void testQueryForFloat() {
        String propName = "price";
        Document filter = new Document("id", 1);
        when(mockCollExecutor.queryForFloat(propName, filter)).thenReturn(OptionalFloat.of(19.99f));

        OptionalFloat result = mapper.queryForFloat(propName, filter);
        Assertions.assertEquals(19.99f, result.get(), 0.01f);
        verify(mockCollExecutor).queryForFloat(propName, filter);
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
    public void testQueryForShort() {
        String propName = "count";
        Document filter = new Document("id", 1);
        when(mockCollExecutor.queryForShort(propName, filter)).thenReturn(OptionalShort.of((short) 100));

        OptionalShort result = mapper.queryForShort(propName, filter);
        Assertions.assertEquals((short) 100, result.get());
        verify(mockCollExecutor).queryForShort(propName, filter);
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
    public void testqueryForSingleValue() {
        String propName = "value";
        Document filter = new Document("id", 1);
        when(mockCollExecutor.queryForSingleValue(propName, filter, String.class)).thenReturn(Nullable.of("result"));

        Nullable<String> result = mapper.queryForSingleValue(propName, filter, String.class);
        Assertions.assertEquals("result", result.get());
        verify(mockCollExecutor).queryForSingleValue(propName, filter, String.class);
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
    public void testQueryWithSelectAndFilter() {
        Collection<String> select = Arrays.asList("name");
        Document filter = new Document("a", 1);
        Dataset ds = N.newEmptyDataset(Arrays.asList("id"));
        when(mockCollExecutor.query(select, filter, TestEntity.class)).thenReturn(ds);

        Assertions.assertSame(ds, mapper.query(select, filter));
        verify(mockCollExecutor).query(select, filter, TestEntity.class);
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
    public void testStream() {
        Document filter = new Document("status", "active");
        Stream<TestEntity> stream = Stream.of(new TestEntity());
        when(mockCollExecutor.stream(filter, TestEntity.class)).thenReturn(stream);

        Stream<TestEntity> result = mapper.stream(filter);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).stream(filter, TestEntity.class);
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
    public void testStreamWithSelectAndFilter() {
        Collection<String> select = Arrays.asList("name");
        Document filter = new Document("a", 1);
        Stream<TestEntity> s = Stream.of(new TestEntity());
        when(mockCollExecutor.stream(select, filter, TestEntity.class)).thenReturn(s);

        Assertions.assertSame(s, mapper.stream(select, filter));
        verify(mockCollExecutor).stream(select, filter, TestEntity.class);
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
    public void testBulkWrite() {
        List<WriteModel<Document>> requests = Arrays.asList();
        BulkWriteResult bulkWriteResult = mock(BulkWriteResult.class);
        when(mockCollExecutor.bulkWrite(requests)).thenReturn(bulkWriteResult);

        BulkWriteResult result = mapper.bulkWrite(requests);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).bulkWrite(requests);
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
    public void testCollExecutor() {
        MongoCollectionExecutor result = mapper.collectionExecutor();
        Assertions.assertSame(mockCollExecutor, result);
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
    public void testReplaceOneWithStringObjectId() {
        String objectId = "507f1f77bcf86cd799439011";
        TestEntity replacement = new TestEntity();
        UpdateResult updateResult = mock(UpdateResult.class);
        when(mockCollExecutor.replaceOne(objectId, replacement)).thenReturn(updateResult);

        UpdateResult result = mapper.replaceOne(objectId, replacement);
        Assertions.assertNotNull(result);
        verify(mockCollExecutor).replaceOne(objectId, replacement);
    }

    // ---- 2026-10-24 sliceS ----

    @Test
    @SuppressWarnings("unchecked")
    public void testGroupByAndCountRowShapesPerMappedType_sliceS() {
        // Pins the groupByAndCount contract documented on both mapper overloads: a Document mapper receives the raw
        // {_id: key, count: n} group documents, while a single-value mapper cannot hold a key-plus-count row and fails
        // with IllegalArgumentException once the stream is consumed (the call itself succeeds).
        final com.mongodb.client.MongoCollection<Document> collection = mock(com.mongodb.client.MongoCollection.class);
        final MongoCollectionExecutor executor = new MongoCollectionExecutor(collection, mock(com.landawn.abacus.util.AsyncExecutor.class));
        final List<List<?>> pipelines = new java.util.ArrayList<>();

        when(collection.aggregate(org.mockito.ArgumentMatchers.anyList(), org.mockito.ArgumentMatchers.eq(Document.class))).thenAnswer(invocation -> {
            final List<?> pipeline = invocation.getArgument(0);
            pipelines.add(pipeline);
            final com.mongodb.client.AggregateIterable<Document> iterable = mock(com.mongodb.client.AggregateIterable.class);
            // A $project stage after $group turns {_id: key, count: n} into {category: key, count: n}, as the server does.
            MongoIterableStubs.stubRows(iterable, pipeline.size() == 1 ? Arrays.asList(new Document("_id", "a").append("count", 2))
                    : Arrays.asList(new Document("category", "a").append("count", 2)));
            return iterable;
        });

        final MongoCollectionMapper<Document> documentMapper = new MongoCollectionMapper<>(executor, Document.class);
        final MongoCollectionMapper<String> stringMapper = new MongoCollectionMapper<>(executor, String.class);

        Assertions.assertAll(
                () -> Assertions.assertEquals(Arrays.asList(new Document("_id", "a").append("count", 2)), documentMapper.groupByAndCount("category").toList()),
                () -> Assertions.assertEquals(1, pipelines.get(0).size()), () -> {
                    final Stream<String> stream = stringMapper.groupByAndCount("category");
                    Assertions.assertThrows(IllegalArgumentException.class, stream::toList);
                }, () -> {
                    final Stream<String> stream = stringMapper.groupByAndCount(Arrays.asList("category"));
                    Assertions.assertThrows(IllegalArgumentException.class, stream::toList);
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSingleValueRowsForBsonProjectionAndMultiFieldGroupBy_sliceS() {
        // Pins the single-value-T rules documented on the mapper: a Bson-projection read converts the sole non-_id field,
        // and an _id-only document (projected field absent) has its _id converted, except that list() turns it into null
        // when another returned document has the field; groupBy over two distinct fields cannot fill a single-value T.
        final Document withField = new Document("_id", 7).append("n", 1);
        final Document idOnly = new Document("_id", 8);
        final Document otherIdOnly = new Document("_id", 9);
        final org.bson.conversions.Bson projection = new Document("n", 1);
        final Document filter = new Document();

        final MongoCollectionMapper<String> mixed = stringMapperFinding(Arrays.asList(withField, idOnly));
        final MongoCollectionMapper<String> idsOnly = stringMapperFinding(Arrays.asList(idOnly, otherIdOnly));

        final com.mongodb.client.MongoCollection<Document> groupCollection = mock(com.mongodb.client.MongoCollection.class);
        final com.mongodb.client.AggregateIterable<Document> groupRows = mock(com.mongodb.client.AggregateIterable.class);
        MongoIterableStubs.stubRows(groupRows, Arrays.asList(new Document("a", "x").append("b", "y")));
        when(groupCollection.aggregate(org.mockito.ArgumentMatchers.anyList(), org.mockito.ArgumentMatchers.eq(Document.class))).thenReturn(groupRows);
        final MongoCollectionMapper<String> groupMapper = new MongoCollectionMapper<>(
                new MongoCollectionExecutor(groupCollection, mock(com.landawn.abacus.util.AsyncExecutor.class)), String.class);

        Assertions.assertAll(() -> Assertions.assertEquals(Optional.of("8"), stringMapperFinding(Arrays.asList(idOnly)).findFirst(projection, filter, null)),
                () -> Assertions.assertEquals(Arrays.asList("1", "8"), mixed.stream(projection, filter, null).toList()),
                () -> Assertions.assertEquals(Arrays.asList("1", "8"), mixed.stream(projection, filter, null, 0, 10).toList()),
                () -> Assertions.assertEquals(Arrays.asList("1", null), mixed.list(projection, filter, null)),
                () -> Assertions.assertEquals(Arrays.asList("1", null), mixed.list(projection, filter, null, 0, 10)),
                () -> Assertions.assertEquals(Arrays.asList("8", "9"), idsOnly.list(projection, filter, null)), () -> {
                    final Stream<String> stream = groupMapper.groupBy(Arrays.asList("a", "b"));
                    Assertions.assertThrows(IllegalArgumentException.class, stream::toList);
                });
    }

    @SuppressWarnings("unchecked")
    private static MongoCollectionMapper<String> stringMapperFinding(final List<Document> rows) {
        final com.mongodb.client.MongoCollection<Document> collection = mock(com.mongodb.client.MongoCollection.class);
        final com.mongodb.client.FindIterable<Document> findIterable = mock(com.mongodb.client.FindIterable.class);
        when(collection.find(org.mockito.ArgumentMatchers.any(org.bson.conversions.Bson.class))).thenReturn(findIterable);
        when(findIterable.projection(org.mockito.ArgumentMatchers.any())).thenReturn(findIterable);
        when(findIterable.sort(org.mockito.ArgumentMatchers.any())).thenReturn(findIterable);
        when(findIterable.skip(org.mockito.ArgumentMatchers.anyInt())).thenReturn(findIterable);
        when(findIterable.limit(org.mockito.ArgumentMatchers.anyInt())).thenReturn(findIterable);
        when(findIterable.first()).thenReturn(rows.get(0));
        MongoIterableStubs.stubRows(findIterable, rows);

        return new MongoCollectionMapper<>(new MongoCollectionExecutor(collection, mock(com.landawn.abacus.util.AsyncExecutor.class)), String.class);
    }

    // ---- 2026-10-24 verify4 ----

    @Test
    @SuppressWarnings("unchecked")
    public void testMultiFieldGroupByUnderOneTopLevelFieldConvertsTheNestedDocument_verify4() {
        // Pins the exception documented on MongoCollectionExecutor.groupBy(Collection, Class) and MongoCollectionMapper.groupBy(Collection):
        // dotted group fields under one top-level field come back (live-verified) as the single nested field {address: {city, zip}},
        // which a single-value type converts as a whole (String -> its JSON text) instead of failing; fields under two top-level
        // fields still fail with IllegalArgumentException once the stream is consumed.
        final Document nested = new Document("address", new Document("city", "Paris").append("zip", "75"));
        final Document twoFields = new Document("address", new Document("city", "Rome")).append("g", "y");
        final com.mongodb.client.MongoCollection<Document> collection = mock(com.mongodb.client.MongoCollection.class);

        when(collection.aggregate(org.mockito.ArgumentMatchers.anyList(), org.mockito.ArgumentMatchers.eq(Document.class))).thenAnswer(invocation -> {
            final String pipeline = String.valueOf(invocation.getArgument(0, List.class));
            final com.mongodb.client.AggregateIterable<Document> iterable = mock(com.mongodb.client.AggregateIterable.class);
            MongoIterableStubs.stubRows(iterable, Arrays.asList(pipeline.contains("zip") ? nested : twoFields));
            return iterable;
        });

        final MongoCollectionExecutor executor = new MongoCollectionExecutor(collection, mock(com.landawn.abacus.util.AsyncExecutor.class));
        final MongoCollectionMapper<String> mapper = new MongoCollectionMapper<>(executor, String.class);
        final List<String> sameParent = Arrays.asList("address.city", "address.zip");

        Assertions.assertAll(
                () -> Assertions.assertEquals(Arrays.asList("{\"city\": \"Paris\", \"zip\": \"75\"}"), executor.groupBy(sameParent, String.class).toList()),
                () -> Assertions.assertEquals(Arrays.asList("{\"city\": \"Paris\", \"zip\": \"75\"}"), mapper.groupBy(sameParent).toList()), () -> {
                    final Stream<String> stream = executor.groupBy(Arrays.asList("address.city", "g"), String.class);
                    Assertions.assertThrows(IllegalArgumentException.class, stream::toList);
                }, () -> {
                    final Stream<String> stream = mapper.groupBy(Arrays.asList("address.city", "g"));
                    Assertions.assertThrows(IllegalArgumentException.class, stream::toList);
                });
    }

    // ---- end 2026-10-24 verify4 ----

    // ---- 2026-10-27 sliceS ----

    @Test
    @SuppressWarnings("unchecked")
    public void testIdPropertyIsAddressedAsUnderscoreIdAndOtherIdTypesStayOrdinaryFields_sliceS() {
        // Pins the "Property <-> Field Mapping" class doc of MongoCollectionMapper and the "Important" class doc of MongoDB:
        // a propName is a BSON field name and a String/ObjectId id property is stored only as _id, so the matched document
        // (projecting "id" returns just its _id, as the server does - verified live) has no "id" field to read, while
        // "_id" reads the id; an id property of any other type (long) is written as an ordinary "id" field, not as _id.
        final ObjectId oid = new ObjectId("507f1f77bcf86cd799439011");
        final com.mongodb.client.MongoCollection<Document> collection = mock(com.mongodb.client.MongoCollection.class);
        final com.mongodb.client.FindIterable<Document> findIterable = mock(com.mongodb.client.FindIterable.class);
        when(collection.find(org.mockito.ArgumentMatchers.any(org.bson.conversions.Bson.class))).thenReturn(findIterable);
        when(findIterable.projection(org.mockito.ArgumentMatchers.any())).thenReturn(findIterable);
        when(findIterable.limit(org.mockito.ArgumentMatchers.anyInt())).thenReturn(findIterable);
        when(findIterable.first()).thenReturn(new Document("_id", oid));
        final MongoCollectionMapper<DottedEntity> idMapper = new MongoCollectionMapper<>(
                new MongoCollectionExecutor(collection, mock(com.landawn.abacus.util.AsyncExecutor.class)), DottedEntity.class);

        final LongIdEntity longIdEntity = new LongIdEntity();
        longIdEntity.setId(7L);
        longIdEntity.setName("seven");
        final Document stored = MongoDBBase.toDocument(longIdEntity);

        Assertions.assertAll(() -> Assertions.assertTrue(idMapper.queryForString("id", new Document()).isPresent()),
                () -> Assertions.assertNull(idMapper.queryForString("id", new Document()).orElse("not-null")),
                () -> Assertions.assertEquals(oid.toHexString(), idMapper.queryForString("_id", new Document()).orElse(null)),
                () -> Assertions.assertEquals(7L, stored.get("id")), () -> Assertions.assertFalse(stored.containsKey("_id"), stored::toJson));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDocumentMapperOperatorDocumentUpdateIsSentVerbatim_sliceS() {
        // Pins the updateOne(String, T) doc: a Document mapper (which cannot take a driver-built Updates.set(...)) writes an
        // explicit null through an operator document, which reaches the driver unchanged; a plain Document is wrapped in $set
        // without its immutable _id.
        final com.mongodb.client.MongoCollection<Document> collection = mock(com.mongodb.client.MongoCollection.class);
        final MongoCollectionMapper<Document> documentMapper = new MongoCollectionMapper<>(
                new MongoCollectionExecutor(collection, mock(com.landawn.abacus.util.AsyncExecutor.class)), Document.class);
        final Document operatorUpdate = new Document("$set", new Document("age", null));
        final org.mockito.ArgumentCaptor<org.bson.conversions.Bson> update = org.mockito.ArgumentCaptor.forClass(org.bson.conversions.Bson.class);

        documentMapper.updateOne("507f1f77bcf86cd799439011", operatorUpdate);
        documentMapper.updateOne("507f1f77bcf86cd799439011", new Document("_id", new ObjectId()).append("age", 3));
        verify(collection, org.mockito.Mockito.times(2)).updateOne(org.mockito.ArgumentMatchers.any(org.bson.conversions.Bson.class), update.capture());

        Assertions.assertAll(() -> Assertions.assertSame(operatorUpdate, update.getAllValues().get(0)),
                () -> Assertions.assertEquals(new Document("$set", new Document("age", null)), update.getAllValues().get(0)),
                () -> Assertions.assertEquals(new Document("$set", new Document("age", 3)), update.getAllValues().get(1)));
    }

    @Test
    public void testAggregateRejectsNullStageFromThisCall_sliceS() {
        // Pins the aggregate(List) @throws: the driver rejects a null pipeline stage with IllegalArgumentException while it builds
        // the operation, before server selection, so this call throws it. No server is involved: the client points at an unused
        // port that is never reached (a regression would surface as a MongoTimeoutException after 1 ms instead).
        try (com.mongodb.client.MongoClient client = com.mongodb.client.MongoClients.create("mongodb://localhost:1/?serverSelectionTimeoutMS=1")) {
            final MongoCollectionMapper<Document> documentMapper = new MongoCollectionMapper<>(
                    new MongoCollectionExecutor(client.getDatabase("sliceS").getCollection("c"), mock(com.landawn.abacus.util.AsyncExecutor.class)),
                    Document.class);
            final org.bson.conversions.Bson match = new Document("$match", new Document());

            Assertions.assertAll(() -> Assertions.assertThrows(IllegalArgumentException.class, () -> documentMapper.aggregate(Arrays.asList(match, null))),
                    () -> Assertions.assertThrows(IllegalArgumentException.class, () -> documentMapper.aggregate(Arrays.asList(null, match))));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testFindOneAndWriteConvertsOnlyAfterTheDriverWrite_sliceS() {
        // Pins the findOneAndUpdate/Replace/Delete docs: the returned document is converted after the driver call returns,
        // so a conversion failure (strict Boolean: "yes" is rejected) is thrown once the write has already been sent and
        // applied (live: the document was updated/replaced/deleted, and a retried findOneAndDelete deleted another one).
        final com.mongodb.client.MongoCollection<Document> collection = mock(com.mongodb.client.MongoCollection.class);
        final Document unconvertible = new Document("flag", "yes");
        when(collection.findOneAndUpdate(org.mockito.ArgumentMatchers.any(org.bson.conversions.Bson.class),
                org.mockito.ArgumentMatchers.any(org.bson.conversions.Bson.class))).thenReturn(unconvertible);
        when(collection.findOneAndReplace(org.mockito.ArgumentMatchers.any(org.bson.conversions.Bson.class), org.mockito.ArgumentMatchers.any(Document.class)))
                .thenReturn(unconvertible);
        when(collection.findOneAndDelete(org.mockito.ArgumentMatchers.any(org.bson.conversions.Bson.class))).thenReturn(unconvertible);
        final MongoCollectionMapper<FlagEntity> flagMapper = new MongoCollectionMapper<>(
                new MongoCollectionExecutor(collection, mock(com.landawn.abacus.util.AsyncExecutor.class)), FlagEntity.class);
        final FlagEntity payload = new FlagEntity();
        payload.setName("x");
        final Document filter = new Document("name", "a");

        Assertions.assertAll(() -> Assertions.assertThrows(IllegalArgumentException.class, () -> flagMapper.findOneAndUpdate(filter, payload)),
                () -> Assertions.assertThrows(IllegalArgumentException.class, () -> flagMapper.findOneAndReplace(filter, payload)),
                () -> Assertions.assertThrows(IllegalArgumentException.class, () -> flagMapper.findOneAndDelete(filter)),
                () -> verify(collection).findOneAndUpdate(org.mockito.ArgumentMatchers.eq(filter),
                        org.mockito.ArgumentMatchers.any(org.bson.conversions.Bson.class)),
                () -> verify(collection).findOneAndReplace(org.mockito.ArgumentMatchers.eq(filter), org.mockito.ArgumentMatchers.any(Document.class)),
                () -> verify(collection).findOneAndDelete(filter));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDistinctOfBsonValueRowTypeReadsDottedAndEmbeddedIdPaths_sliceS() {
        // MongoDBBase.readRow reads a Binary/Code/Symbol/BsonValue row type as a single value although Binary/Code's Type reports a bean.
        // distinct must then send the group-only pipeline it sends for String: re-projecting a dotted name nests the value
        // ({a: {b: v}}, {_id: {part: v}}) and the single-value read received the embedded document instead of v. The stub returns
        // the document shape the server returns for each pipeline (the server would also turn a projected Code into its text; the
        // _r2S tests stub that too).
        final org.bson.types.Binary binary = new org.bson.types.Binary(new byte[] { 1, 2 });
        final org.bson.types.Code code = new org.bson.types.Code("x");
        final List<List<?>> pipelines = new java.util.ArrayList<>();
        final java.util.function.Function<Class<?>, MongoCollectionMapper<?>> mapperOf = rowType -> {
            final com.mongodb.client.MongoCollection<Document> collection = mock(com.mongodb.client.MongoCollection.class);
            final Object value = rowType == org.bson.types.Code.class ? code : binary;

            when(collection.aggregate(org.mockito.ArgumentMatchers.anyList(), org.mockito.ArgumentMatchers.eq(Document.class))).thenAnswer(invocation -> {
                final List<?> pipeline = invocation.getArgument(0);
                pipelines.add(pipeline);
                final Document group = (Document) pipeline.stream()
                        .filter(stage -> stage instanceof Document && ((Document) stage).containsKey("$group"))
                        .findFirst()
                        .get();
                final String field = ((String) ((Document) group.get("$group")).get("_id")).substring(1);
                final boolean projected = pipeline.stream().anyMatch(stage -> stage instanceof Document && ((Document) stage).containsKey("$project"));
                final Document row;

                if (!projected) {
                    row = new Document("_id", value);
                } else if (field.indexOf('.') < 0) {
                    row = new Document(field, value);
                } else {
                    final String[] path = field.split("\\.");
                    row = new Document(path[0], new Document(path[1], value));
                }

                final com.mongodb.client.AggregateIterable<Document> iterable = mock(com.mongodb.client.AggregateIterable.class);
                MongoIterableStubs.stubRows(iterable, Arrays.asList(row));
                return iterable;
            });

            return new MongoCollectionMapper<>(new MongoCollectionExecutor(collection, mock(com.landawn.abacus.util.AsyncExecutor.class)), rowType);
        };

        Assertions
                .assertAll(() -> Assertions.assertEquals(Arrays.asList(binary), mapperOf.apply(org.bson.types.Binary.class).distinct("a").toList()),
                        () -> Assertions.assertEquals(Arrays.asList(binary), mapperOf.apply(org.bson.types.Binary.class).distinct("a.b").toList()),
                        () -> Assertions.assertEquals(Arrays.asList(binary), mapperOf.apply(
                                org.bson.types.Binary.class).distinct("_id.part").toList()),
                        () -> Assertions.assertEquals(Arrays.asList(code), mapperOf.apply(org.bson.types.Code.class).distinct("a.b", new Document()).toList()),
                        () -> Assertions.assertTrue(
                                pipelines.stream()
                                        .allMatch(pipeline -> pipeline.stream()
                                                .noneMatch(stage -> stage instanceof Document && ((Document) stage).containsKey("$project"))),
                                () -> String.valueOf(pipelines)));
    }

    public static class FlagEntity {
        private String id;
        private Boolean flag;
        private String name;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public Boolean getFlag() {
            return flag;
        }

        public void setFlag(Boolean flag) {
            this.flag = flag;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class LongIdEntity {
        private long id;
        private String name;

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    // ---- end 2026-10-27 sliceS ----

    // ---- 2026-10-27 r2S ----

    @Test
    public void testDistinctReadsEveryBsonValueRowTypeFromTheGroupRows_r2S() {
        // isSingleValueRowType follows MongoDBBase.isBsonValueClass: Binary, Code (and its subclass CodeWithScope), Symbol and a scalar
        // BsonValue are read as the single value of each {_id: value} group row, for a top-level, a dotted and an embedded-_id field,
        // with and without a filter. The stub answers each pipeline as the server does (verified live): a $project nests a dotted path
        // ({n: {v: value}}, {_id: {part: value}}) and returns a Code or Symbol as plain text, which the single-value read cannot turn
        // back into the value.
        final Object[][] cases = { { org.bson.types.Binary.class, new org.bson.types.Binary(new byte[] { 1, 2 }) },
                { org.bson.types.Code.class, new org.bson.types.Code("f1") },
                { org.bson.types.Code.class, new org.bson.types.CodeWithScope("g1", new Document("x", 1)) },
                { org.bson.types.CodeWithScope.class, new org.bson.types.CodeWithScope("g2", new Document("x", 2)) },
                { org.bson.types.Symbol.class, new org.bson.types.Symbol("s1") }, { org.bson.BsonTimestamp.class, new org.bson.BsonTimestamp(10, 1) },
                { org.bson.BsonRegularExpression.class, new org.bson.BsonRegularExpression("a+", "i") },
                { org.bson.BsonValue.class, new org.bson.BsonTimestamp(20, 2) } };
        final List<org.junit.jupiter.api.function.Executable> checks = new java.util.ArrayList<>();

        for (final Object[] c : cases) {
            for (final String field : Arrays.asList("v", "n.v", "_id.part")) {
                for (final boolean filtered : new boolean[] { false, true }) {
                    final String label = ((Class<?>) c[0]).getSimpleName() + " <- " + c[1] + ", distinct(" + field + (filtered ? ", filter)" : ")");
                    final List<List<?>> pipelines = new java.util.ArrayList<>();
                    final MongoCollectionMapper<?> distinctMapper = new MongoCollectionMapper<>(
                            new MongoCollectionExecutor(serverLikeDistinctCollection(c[1], pipelines), mock(com.landawn.abacus.util.AsyncExecutor.class)),
                            (Class<?>) c[0]);

                    checks.add(() -> {
                        final List<?> values = filtered ? distinctMapper.distinct(field, new Document("ok", true)).toList()
                                : distinctMapper.distinct(field).toList();

                        Assertions.assertEquals(Arrays.asList(c[1]), values, label);
                        Assertions.assertEquals(new Document("$group", new Document("_id", "$" + field)), N.lastOrNullIfEmpty(pipelines.get(0)), label);
                    });
                }
            }
        }

        Assertions.assertAll(checks);
    }

    @Test
    public void testDistinctPipelineShapeOfNonBsonValueRowTypesIsUnchanged_r2S() {
        // Guards the isSingleValueRowType rule against over-reach: String/Integer still read the group rows directly (int also drops the
        // null group), while a bean, Map, Document, Bson, Object and the BSON containers BsonDocument (a Map) and BsonArray (a List),
        // which isBsonValueClass excludes, still get the $project stage that surfaces the value under the field name.
        final Object[][] cases = { { String.class, "none" }, { Integer.class, "none" }, { int.class, "match" }, { DottedEntity.class, "project" },
                { java.util.Map.class, "project" }, { Document.class, "project" }, { org.bson.conversions.Bson.class, "project" }, { Object.class, "project" },
                { org.bson.BsonDocument.class, "project" }, { org.bson.BsonArray.class, "project" } };
        final List<org.junit.jupiter.api.function.Executable> checks = new java.util.ArrayList<>();

        for (final Object[] c : cases) {
            for (final String field : Arrays.asList("name", "address.city", "_id.part")) {
                final List<List<?>> pipelines = new java.util.ArrayList<>();
                final MongoCollectionMapper<?> distinctMapper = new MongoCollectionMapper<>(
                        new MongoCollectionExecutor(serverLikeDistinctCollection(null, pipelines), mock(com.landawn.abacus.util.AsyncExecutor.class)),
                        (Class<?>) c[0]);

                checks.add(() -> {
                    distinctMapper.distinct(field).toList();
                    distinctMapper.distinct(field, new Document("ok", true)).toList();

                    for (final List<?> pipeline : pipelines) {
                        final Object last = N.lastOrNullIfEmpty(pipeline);
                        final String label = ((Class<?>) c[0]).getSimpleName() + " " + field + " " + pipeline;

                        switch ((String) c[1]) {
                            case "none" -> Assertions.assertEquals(new Document("$group", new Document("_id", "$" + field)), last, label);
                            case "match" -> Assertions.assertEquals(new Document("$match", new Document("_id", new Document("$ne", null))), last, label);
                            default -> Assertions.assertTrue(((Document) last).containsKey("$project"), label);
                        }
                    }

                    Assertions.assertEquals(2, pipelines.size());
                });
            }
        }

        Assertions.assertAll(checks);
    }

    /**
     * A collection whose {@code aggregate} answers a distinct pipeline as the server does for a stored {@code value}: the group row
     * {@code {_id: value}} without a {@code $project}; with one, the value under the (nested) field name, a {@code Code} (not a
     * {@code CodeWithScope}) or {@code Symbol} as its text. A {@code null} value answers no rows.
     */
    @SuppressWarnings("unchecked")
    private static com.mongodb.client.MongoCollection<Document> serverLikeDistinctCollection(final Object value, final List<List<?>> pipelines) {
        final com.mongodb.client.MongoCollection<Document> collection = mock(com.mongodb.client.MongoCollection.class);

        when(collection.aggregate(org.mockito.ArgumentMatchers.anyList(), org.mockito.ArgumentMatchers.eq(Document.class))).thenAnswer(invocation -> {
            final List<?> pipeline = invocation.getArgument(0);
            pipelines.add(pipeline);
            final List<Document> rows = new java.util.ArrayList<>();

            if (value != null) {
                final Document group = (Document) pipeline.stream()
                        .filter(stage -> stage instanceof Document && ((Document) stage).containsKey("$group"))
                        .findFirst()
                        .get();
                final String[] path = ((String) ((Document) group.get("$group")).get("_id")).substring(1).split("\\.");
                final boolean projected = pipeline.stream().anyMatch(stage -> stage instanceof Document && ((Document) stage).containsKey("$project"));

                if (projected) {
                    final Object shown = value instanceof org.bson.types.Code && !(value instanceof org.bson.types.CodeWithScope)
                            ? ((org.bson.types.Code) value).getCode()
                            : value instanceof org.bson.types.Symbol ? ((org.bson.types.Symbol) value).getSymbol() : value;
                    rows.add(path.length == 1 ? new Document(path[0], shown) : new Document(path[0], new Document(path[1], shown)));
                } else {
                    rows.add(new Document("_id", value));
                }
            }

            final com.mongodb.client.AggregateIterable<Document> iterable = mock(com.mongodb.client.AggregateIterable.class);
            MongoIterableStubs.stubRows(iterable, rows);
            return iterable;
        });

        return collection;
    }

    // ---- end 2026-10-27 r2S ----

    public static class DottedEntity {
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

    private static <E> MongoCollectionMapper<E> mapperReturning(final Document row, final Class<E> entityClass) {
        return new MongoCollectionMapper<>(
                new MongoCollectionExecutor(MongoMappingFixtures.mockCollectionReturning(row), mock(com.landawn.abacus.util.AsyncExecutor.class)), entityClass);
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
